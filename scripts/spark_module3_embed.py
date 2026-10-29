import json
import os
import sys
import time

import numpy as np
import pandas as pd
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, FloatType, IntegerType, StringType,
                               StructField, StructType)

sys.stdout.reconfigure(encoding="utf-8")

HDFS = "hdfs://localhost:9000"
# EDUINSIGHT_SCOPE=subset (default, CS development subset) or full (whole MOOCCubeX corpus)
FULL = os.environ.get("EDUINSIGHT_SCOPE", "subset") == "full"
PROC = HDFS + ("/eduinsight/processed_full" if FULL else "/eduinsight/processed")
SUB = HDFS + ("/eduinsight/full" if FULL else "/eduinsight/subset_cs")
MODEL = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"
STATS_PATH = "/home/aryan/module3_stats%s.json" % ("_full" if FULL else "")
EMBED_PARTITIONS = 2
VALIDATION_SAMPLE = 6000   # pairwise validation uses at most this many videos (a full N x N matrix would not fit in memory)

spark = (SparkSession.builder.appName("EduInsight-Module3-Embeddings" + ("-full" if FULL else ""))
         .master(os.environ.get("EDUINSIGHT_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "64" if FULL else "8")
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("WARN")
stats = {"scope": "full" if FULL else "subset"}
t0 = time.time()


# ---------- Stage 1: chunk captions + embed chunks on the GPU (Spark mapPartitions) ----------
def embed_partition(rows):
    import torch
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(MODEL, device="cuda")
    model.half()
    tok = model.tokenizer
    max_len = model.max_seq_length - 2  # room for the special tokens
    texts, meta = [], []

    def flush():
        if not texts:
            return []
        embs = model.encode(texts, batch_size=64, convert_to_numpy=True,
                            normalize_embeddings=True, show_progress_bar=False)
        out = [Row(ccid=m[0], chunk_idx=m[1], chunk_text=t, embedding=[float(x) for x in e])
               for m, t, e in zip(meta, texts, embs)]
        texts.clear()
        meta.clear()
        return out

    for r in rows:
        segs = [s for s in (r["segments"] or []) if s]
        if not segs:
            continue
        lens = [len(x) for x in tok(segs, add_special_tokens=False)["input_ids"]]
        cur, cur_len, idx = [], 0, 0
        for s, l in zip(segs, lens):
            if cur and cur_len + l > max_len:
                texts.append(" ".join(cur)); meta.append((r["ccid"], idx)); idx += 1
                cur, cur_len = [], 0
            cur.append(s)
            cur_len += l
        if cur:
            texts.append(" ".join(cur)); meta.append((r["ccid"], idx))
        if len(texts) >= 256:
            yield from flush()
    yield from flush()


chunk_schema = StructType([
    StructField("ccid", StringType()), StructField("chunk_idx", IntegerType()),
    StructField("chunk_text", StringType()), StructField("embedding", ArrayType(FloatType())),
])

videos = spark.read.parquet(PROC + "/videos_tokenized").select("ccid", "name", "segments")
t1 = time.time()
chunks = spark.createDataFrame(videos.repartition(EMBED_PARTITIONS).rdd.mapPartitions(embed_partition),
                               chunk_schema)
chunks.write.mode("overwrite").parquet(PROC + "/chunk_embeddings")
chunks = spark.read.parquet(PROC + "/chunk_embeddings")
n_chunks = chunks.count()
stats["chunks"] = n_chunks
stats["embed_seconds"] = round(time.time() - t1, 1)
stats["chunks_per_second"] = round(n_chunks / (time.time() - t1), 1)

# ---------- Stage 2: mean-pool chunk vectors into one vector per video (Spark groupBy) ----------
pool_schema = StructType([
    StructField("ccid", StringType()), StructField("n_chunks", IntegerType()),
    StructField("embedding", ArrayType(FloatType())),
])


def pool(pdf):
    arr = np.stack([np.asarray(e, dtype="float32") for e in pdf["embedding"]])
    v = arr.mean(axis=0)
    v = v / (np.linalg.norm(v) + 1e-12)
    return pd.DataFrame({"ccid": [pdf["ccid"].iloc[0]], "n_chunks": [len(pdf)], "embedding": [v]})


video_emb = chunks.select("ccid", "embedding").groupBy("ccid").applyInPandas(pool, pool_schema)
video_emb.write.mode("overwrite").parquet(PROC + "/video_embeddings")
video_emb = spark.read.parquet(PROC + "/video_embeddings")
stats["videos_embedded"] = video_emb.count()
stats["avg_chunks_per_video"] = round(n_chunks / stats["videos_embedded"], 1)
stats["embedding_dim"] = len(video_emb.first()["embedding"])

# ---------- Stage 3: validation on the driver ----------
ve = video_emb.select("ccid", "embedding").toPandas()
names = (spark.read.parquet(PROC + "/videos_tokenized").select("ccid", "name").toPandas()
         .set_index("ccid")["name"].to_dict())
E = np.stack(ve["embedding"].apply(lambda x: np.asarray(x, dtype="float32")).values)
ccids = ve["ccid"].tolist()
stats["norm_check_min_max"] = [round(float(np.linalg.norm(E, axis=1).min()), 4),
                               round(float(np.linalg.norm(E, axis=1).max()), 4)]

# ccid -> primary course, using the same bridge join as the subset build
courses = spark.read.json(SUB + "/course.json").select(
    F.col("id").alias("course_id"), F.explode("resource").alias("res"))
courses = courses.select("course_id", F.col("res.resource_id").alias("rid")).filter(F.col("rid").startswith("V_"))
bridge = (sc.textFile(SUB + "/video_id-ccid.txt").map(lambda l: l.rstrip("\n").split("\t"))
            .filter(lambda p: len(p) == 2).toDF(["rid", "ccid"]))
cmap = (courses.join(bridge, "rid").groupBy("ccid").agg(F.min("course_id").alias("course_id"))
        .toPandas().set_index("ccid")["course_id"].to_dict())
course_of = np.array([cmap.get(c, "?" + c) for c in ccids])   # videos in no course count as their own course

rng = np.random.default_rng(42)
sel = np.arange(len(ccids)) if len(ccids) <= VALIDATION_SAMPLE else np.sort(rng.choice(len(ccids), VALIDATION_SAMPLE, replace=False))
stats["validation_sample_size"] = int(len(sel))
Es, cs = E[sel], course_of[sel]
S = Es @ Es.T
same = cs[:, None] == cs[None, :]
np.fill_diagonal(same, False)
diff = cs[:, None] != cs[None, :]
stats["mean_cosine_same_course"] = round(float(S[same].mean()), 3)
stats["mean_cosine_different_course"] = round(float(S[diff].mean()), 3)
S2 = S.copy()
np.fill_diagonal(S2, -1)
nn = S2.argmax(axis=1)
stats["nearest_neighbour_same_course_pct"] = round(100 * float((cs[nn] == cs).mean()), 1)
del S, S2, same, diff

# nearest neighbours of a sample lecture (searched across ALL videos)
sample_ccid = "AC1E9080F4785AB99C33DC5901307461"
if sample_ccid in ccids:
    i = ccids.index(sample_ccid)
    sims_i = E @ E[i]
    sims_i[i] = -1
    top = np.argsort(-sims_i)[:5]
    stats["neighbours_of_sample"] = {"video": names.get(sample_ccid),
                                     "top5": [(names.get(ccids[j]), round(float(sims_i[j]), 3)) for j in top]}

# English queries against Chinese lecture embeddings (cross-lingual retrieval preview)
import torch
from sentence_transformers import SentenceTransformer
qm = SentenceTransformer(MODEL, device="cuda")
qm.half()
queries = ["dynamic programming", "process scheduling in an operating system", "linked list",
           "TCP congestion control", "sorting algorithms", "pointers in C"]
Q = qm.encode(queries, normalize_embeddings=True, convert_to_numpy=True).astype("float32")
stats["query_demo"] = {}
for q, qv in zip(queries, Q):
    sims = E @ qv
    top = np.argsort(-sims)[:5]
    stats["query_demo"][q] = [(names.get(ccids[j]), cmap.get(ccids[j]), round(float(sims[j]), 3)) for j in top]

stats["total_runtime_sec"] = round(time.time() - t0, 1)
with open(STATS_PATH, "w", encoding="utf-8") as f:
    json.dump(stats, f, ensure_ascii=False, indent=2)

print("=" * 60)
print("MODULE 3 COMPLETE | scope:", stats["scope"])
for k in ["videos_embedded", "chunks", "avg_chunks_per_video", "embedding_dim", "norm_check_min_max",
          "embed_seconds", "chunks_per_second", "validation_sample_size", "mean_cosine_same_course",
          "mean_cosine_different_course", "nearest_neighbour_same_course_pct", "total_runtime_sec"]:
    print(f"{k}: {stats[k]}")
print("neighbours of sample:", json.dumps(stats.get("neighbours_of_sample"), ensure_ascii=False))
for q, res in stats["query_demo"].items():
    print(f"QUERY {q!r}:")
    for r in res:
        print("   ", r)
print("=" * 60)
spark.stop()

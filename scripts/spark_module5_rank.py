import json
import os
import sys
import time

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType)
from pyspark.sql.window import Window

sys.stdout.reconfigure(encoding="utf-8")

HDFS = "hdfs://localhost:9000"
PROC = HDFS + "/eduinsight/processed"
SUB = HDFS + "/eduinsight/subset_cs"
MODEL = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"
STATS_PATH = "/home/aryan/module5_stats.json"
CAND_PATH = "/home/aryan/module5_candidates_BC.json"
RANK_PATH = "/home/aryan/module5_rankings_BC.json"
JUDGE_PATH = "/home/aryan/module5_judgments.json"
ASSETS = "/home/aryan/search_assets"

CONCEPT_SIM_FLOOR = 0.35   # concept similarities below this are treated as "no match"
TRANS_K, TRANS_TAU = 3, 0.40   # query translation: up to 3 nearest concept names with cosine >= 0.40 (fixed a priori)
RRF_K = 60                 # standard reciprocal-rank-fusion constant

# Set A (automatic): English query -> Chinese concept whose linked videos are the relevant set
A = [
    ("data structures", "数据结构"), ("compiler", "编译器"), ("CPU registers", "寄存器"),
    ("IP address", "IP地址"), ("system call", "系统调用"), ("file system", "文件系统"),
    ("constructor", "构造函数"), ("root node of a tree", "根节点"), ("time complexity", "时间复杂度"),
    ("sorting algorithm", "排序算法"), ("routing table", "路由表"), ("transport layer", "传输层"),
    ("MapReduce", "mapreduce"), ("red-black tree", "红黑树"), ("multithreading", "多线程"),
    ("subnet mask", "子网掩码"), ("semaphore", "信号量"), ("hash table", "哈希表"),
    ("recursive function call", "递归调用"), ("function pointer", "函数指针"), ("deep learning", "深度学习"),
    ("cloud computing", "云计算"), ("Turing machine", "图灵机"), ("scheduling algorithm", "调度算法"),
    ("memory management", "内存管理"), ("abstract class", "抽象类"), ("regular language", "正则语言"),
    ("assembly language", "汇编语言"), ("floating-point numbers", "浮点数"), ("variable scope", "作用域"),
]
# Set B (manual, DEVELOPMENT set: these queries were examined while designing the fix)
B = ["dynamic programming", "linked list", "binary search tree", "recursion", "deadlock in operating systems",
     "TCP congestion control", "SQL queries", "linear regression", "pointers in C",
     "how does a CPU execute instructions"]
# Set C (manual, HELD-OUT test set: fixed before any result was seen)
C = ["binary search", "virtual memory and page replacement", "inheritance in object-oriented programming",
     "shortest path algorithm", "cache memory", "DNS domain name system", "database normalization",
     "neural networks", "stack and queue", "exception handling", "polymorphism and virtual functions",
     "dynamic memory allocation"]

queries = []  # (qid, text)
for i, (en, zh) in enumerate(A, 1):
    queries += [(f"A{i:02d}_en", en), (f"A{i:02d}_zh", zh)]
queries += [(f"B{i:02d}_en", q) for i, q in enumerate(B, 1)]
queries += [(f"C{i:02d}_en", q) for i, q in enumerate(C, 1)]
qids = [q[0] for q in queries]
NQ = len(queries)

spark = (SparkSession.builder.appName("EduInsight-Module5-Ranking")
         .master("local[4]")
         .config("spark.driver.memory", "6g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         .config("spark.sql.execution.arrow.maxRecordsPerBatch", "2000")
         .config("spark.sql.parquet.columnarReaderBatchSize", "1024")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("WARN")
t0 = time.time()
stats = {"queries_total": NQ, "set_A_queries": len(A), "set_B_queries": len(B), "set_C_queries": len(C),
         "translation_rule": f"up to {TRANS_K} nearest concept names with cosine >= {TRANS_TAU}",
         "rrf_k": RRF_K}


def py(o):
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.floating):
        return float(o)
    raise TypeError(type(o))


# ---------- 1. encode queries and concept names on the GPU ----------
import torch
from sentence_transformers import SentenceTransformer

model = SentenceTransformer(MODEL, device="cuda")
model.half()
Q = model.encode([q[1] for q in queries], batch_size=64, normalize_embeddings=True,
                 convert_to_numpy=True).astype("float32")

cc = spark.read.parquet(PROC + "/concepts_clean").select("name").distinct()
vc = spark.read.parquet(PROC + "/video_concepts")
used_names = vc.select(F.explode("concepts").alias("name")).distinct()
cnames = sorted(r["name"] for r in cc.join(used_names, "name").collect())
t = time.time()
Cemb = model.encode(cnames, batch_size=256, normalize_embeddings=True, convert_to_numpy=True).astype("float32")
stats["concept_names_embedded"] = len(cnames)
stats["concept_embed_seconds"] = round(time.time() - t, 1)
spark.createDataFrame(pd.DataFrame({"name": cnames, "embedding": [c.tolist() for c in Cemb]})) \
    .write.mode("overwrite").parquet(PROC + "/concept_embeddings")
del model
torch.cuda.empty_cache()

Qb, qidsb = sc.broadcast(Q), sc.broadcast(qids)


# ---------- 2. distributed scoring of every query against every chunk / video / concept ----------
def chunk_batches(batches):
    for pdf in batches:
        M = np.stack(pdf["embedding"].to_numpy()).astype(np.float32)
        S = M @ Qb.value.T                       # (rows, queries)
        n = S.shape[0]
        yield pd.DataFrame({"qid": np.tile(np.array(qidsb.value, dtype=object), n),
                            "ccid": np.repeat(pdf["ccid"].to_numpy(), NQ),
                            "chunk_idx": np.repeat(pdf["chunk_idx"].to_numpy(), NQ).astype("int32"),
                            "score": S.ravel().astype("float64")})


def video_batches(batches):
    for pdf in batches:
        M = np.stack(pdf["embedding"].to_numpy()).astype(np.float32)
        S = M @ Qb.value.T
        n = S.shape[0]
        yield pd.DataFrame({"qid": np.tile(np.array(qidsb.value, dtype=object), n),
                            "ccid": np.repeat(pdf["ccid"].to_numpy(), NQ),
                            "video_cos": S.ravel().astype("float64")})


def concept_batches(batches):
    qarr = np.array(qidsb.value, dtype=object)
    for pdf in batches:
        M = np.stack(pdf["embedding"].to_numpy()).astype(np.float32)
        S = M @ Qb.value.T
        ii, jj = np.where(S >= CONCEPT_SIM_FLOOR)
        yield pd.DataFrame({"qid": qarr[jj], "name": pdf["name"].to_numpy()[ii],
                            "sim": S[ii, jj].astype("float64")})


t = time.time()
chunks = spark.read.parquet(PROC + "/chunk_embeddings").select("ccid", "chunk_idx", "embedding")
cs_schema = StructType([StructField("qid", StringType()), StructField("ccid", StringType()),
                        StructField("chunk_idx", IntegerType()), StructField("score", DoubleType())])
chunk_scores = chunks.mapInPandas(chunk_batches, cs_schema)
chunk_best = (chunk_scores.groupBy("qid", "ccid")
              .agg(F.max("score").alias("chunk_max"), F.max_by("chunk_idx", "score").alias("best_chunk")))

vs_schema = StructType([StructField("qid", StringType()), StructField("ccid", StringType()),
                        StructField("video_cos", DoubleType())])
video_scores = (spark.read.parquet(PROC + "/video_embeddings").select("ccid", "embedding")
                .mapInPandas(video_batches, vs_schema))

cn_schema = StructType([StructField("qid", StringType()), StructField("name", StringType()),
                        StructField("sim", DoubleType())])
concept_sims = spark.read.parquet(PROC + "/concept_embeddings").mapInPandas(concept_batches, cn_schema).cache()
vlinks = vc.select("ccid", F.explode("concepts").alias("name"))
concept_best = concept_sims.join(vlinks, "name").groupBy("qid", "ccid").agg(F.max("sim").alias("concept_sim"))

# ---------- 3. query translation: English query -> nearest Chinese concept names ----------
wq = Window.partitionBy("qid").orderBy(F.desc("sim"))
trans = (concept_sims.filter(F.col("qid").endswith("_en")).withColumn("rn", F.row_number().over(wq))
         .filter((F.col("rn") <= TRANS_K) & (F.col("sim") >= TRANS_TAU))
         .select("qid", "name", "sim", "rn").toPandas())
translations = {}
for r in trans.sort_values(["qid", "rn"]).itertuples():
    translations.setdefault(r.qid, []).append((r.name, round(float(r.sim), 3)))
stats["translations"] = translations
stats["queries_with_no_translation"] = sorted(q for q in qids if q.endswith("_en") and q not in translations)
a_hits = {}
for i, (en, zh) in enumerate(A, 1):
    names_ = [n for n, _ in translations.get(f"A{i:02d}_en", [])]
    a_hits[f"A{i:02d}"] = {"target": zh, "found": zh in names_, "rank": (names_.index(zh) + 1) if zh in names_ else None,
                           "got": names_}
stats["translation_hit_at_3_setA"] = round(sum(v["found"] for v in a_hits.values()) / len(A), 3)
stats["translation_hit_at_1_setA"] = round(sum(v["rank"] == 1 for v in a_hits.values()) / len(A), 3)
stats["translation_setA_detail"] = a_hits

# ---------- 4. BM25 keyword search (Spark): baseline (Chinese term) and translated-query version ----------
import jieba
import stopwordsiso
jieba.setLogLevel(60)
STOP = set(stopwordsiso.stopwords("zh")) | set(stopwordsiso.stopwords("en"))


def terms_of(text):
    toks = [w.strip().lower() for w in jieba.lcut(text)]
    toks = [w for w in toks if len(w) >= 2 and w not in STOP]
    return toks or [text.strip().lower()]


zh_terms = [(f"A{i:02d}_zh", w, 1.0) for i, (_, zh) in enumerate(A, 1) for w in set(terms_of(zh))]
tq = {}
for qid, lst in translations.items():
    for name, sim in lst:
        for w in set(terms_of(name)):
            tq[(qid, w)] = max(tq.get((qid, w), 0.0), sim)
tr_terms = [(q, w, s) for (q, w), s in tq.items()]
stats["bm25_query_tokens_setA_zh"] = {f"A{i:02d}": sorted({t_[1] for t_ in zh_terms if t_[0] == f"A{i:02d}_zh"})
                                      for i in range(1, len(A) + 1)}

vt = spark.read.parquet(PROC + "/videos_tokenized").select("ccid", "name", "n_tokens", "tokens").cache()
N = vt.count()
avgdl = float(vt.agg(F.avg("n_tokens")).first()[0])
ts = spark.read.parquet(PROC + "/term_stats").select("term", "df")
K1, B_ = 1.5, 0.75


def bm25_spark(term_rows, out_col):
    qt = spark.createDataFrame(term_rows, ["qid", "term", "w"])
    tf = (vt.select("ccid", "n_tokens", F.explode("tokens").alias("term"))
            .join(F.broadcast(qt.select("term").distinct()), "term")
            .groupBy("ccid", "term", "n_tokens").agg(F.count("*").alias("tf")))
    return (tf.join(qt, "term").join(ts, "term")
            .withColumn("idf_b", F.log(1 + (F.lit(float(N)) - F.col("df") + 0.5) / (F.col("df") + 0.5)))
            .withColumn("s", F.col("w") * F.col("idf_b") * F.col("tf") * (K1 + 1) /
                        (F.col("tf") + K1 * (1 - B_ + B_ * F.col("n_tokens") / F.lit(avgdl))))
            .groupBy("qid", "ccid").agg(F.sum("s").alias(out_col)))


bm25_zh = bm25_spark(zh_terms, "bm25")
terms_en = bm25_spark(tr_terms, "terms_bm25")

# ---------- 5. collect the score tables (queries x 4,071 videos) ----------
grid = (chunk_best.join(video_scores, ["qid", "ccid"], "left")
        .join(concept_best, ["qid", "ccid"], "left")
        .join(bm25_zh, ["qid", "ccid"], "left")
        .join(terms_en, ["qid", "ccid"], "left"))
grid.write.mode("overwrite").parquet(PROC + "/ranking_scores")
df = spark.read.parquet(PROC + "/ranking_scores").toPandas()
stats["score_rows"] = int(len(df))
stats["scoring_seconds"] = round(time.time() - t, 1)

a_names = [zh for _, zh in A]
rel_rows = vlinks.filter(F.col("name").isin(a_names)).groupBy("name").agg(F.collect_set("ccid").alias("c")).collect()
rel = {r["name"]: set(r["c"]) for r in rel_rows}
stats["set_A_relevant_counts"] = {f"A{i:02d}": len(rel.get(zh, ())) for i, (_, zh) in enumerate(A, 1)}

by_q = {q: g for q, g in df.groupby("qid")}


def rank_of(sub, col):
    return sub.sort_values(col, ascending=False, kind="mergesort")["ccid"].tolist()


def ranked_lists(label):
    """Rankings (lists of ccid, best first) for every method for one query label."""
    en = by_q[f"{label}_en"].copy()
    out = {"video_en": rank_of(en, "video_cos"), "chunk_en": rank_of(en, "chunk_max")}
    chunk_rank = {c: r for r, c in enumerate(out["chunk_en"], 1)}
    # v1 hybrid: chunk similarity fused with "closest concept in the video" similarity
    cm = en.dropna(subset=["concept_sim"]).sort_values("concept_sim", ascending=False, kind="mergesort")
    concept_rank = {c: r for r, c in enumerate(cm["ccid"].tolist(), 1)}
    en["rrf1"] = [1 / (RRF_K + chunk_rank[c]) + (1 / (RRF_K + concept_rank[c]) if c in concept_rank else 0.0)
                  for c in en["ccid"]]
    out["hybrid_en"] = rank_of(en, "rrf1")
    # v2 hybrid: chunk similarity fused with keyword (BM25) search on the translated Chinese terms
    tm = en.dropna(subset=["terms_bm25"]).sort_values("terms_bm25", ascending=False, kind="mergesort")
    terms_rank = {c: r for r, c in enumerate(tm["ccid"].tolist(), 1)}
    en["rrf2"] = [1 / (RRF_K + chunk_rank[c]) + (1 / (RRF_K + terms_rank[c]) if c in terms_rank else 0.0)
                  for c in en["ccid"]]
    out["hybrid2_en"] = rank_of(en, "rrf2")
    tl = tm["ccid"].tolist()
    out["terms_en"] = tl + [c for c in out["chunk_en"] if c not in set(tl)]
    if label.startswith("A"):
        zh = by_q[f"{label}_zh"]
        out["chunk_zh"] = rank_of(zh, "chunk_max")
        bm = zh.dropna(subset=["bm25"]).sort_values("bm25", ascending=False, kind="mergesort")
        ranked = bm["ccid"].tolist()
        out["bm25_zh"] = ranked + [c for c in zh["ccid"] if c not in set(ranked)]
    return out


def metrics(ranked, relset):
    top5, top10 = set(ranked[:5]), set(ranked[:10])
    dcg = sum(1 / np.log2(i + 2) for i, c in enumerate(ranked[:10]) if c in relset)
    idcg = sum(1 / np.log2(i + 2) for i in range(min(len(relset), 10)))
    return {"p5": len(top5 & relset) / 5, "p10": len(top10 & relset) / 10,
            "ndcg10": dcg / idcg if idcg else 0.0,
            "r50": len(set(ranked[:50]) & relset) / len(relset) if relset else 0.0}


# ---------- 6. set A: automatic evaluation ----------
methods_A = ["bm25_zh", "video_en", "chunk_en", "chunk_zh", "hybrid_en", "terms_en", "hybrid2_en"]
per_query = {}
for i, (en, zh) in enumerate(A, 1):
    label = f"A{i:02d}"
    lists = ranked_lists(label)
    per_query[label] = {"query_en": en, "concept_zh": zh, "relevant": len(rel.get(zh, ())),
                        "metrics": {m: metrics(lists[m], rel.get(zh, set())) for m in methods_A}}
rng = np.random.default_rng(42)
summary = {}
for m in methods_A:
    row = {}
    for met in ("p5", "p10", "ndcg10", "r50"):
        vals = np.array([per_query[k]["metrics"][m][met] for k in per_query])
        boots = [vals[rng.integers(0, len(vals), len(vals))].mean() for _ in range(2000)]
        row[met] = {"mean": round(float(vals.mean()), 3),
                    "ci95": [round(float(np.percentile(boots, 2.5)), 3), round(float(np.percentile(boots, 97.5)), 3)]}
    summary[m] = row
stats["set_A_summary"] = summary
stats["set_A_per_query"] = per_query
pairs = {}
for a_, b_ in (("chunk_en", "video_en"), ("hybrid_en", "chunk_en"), ("hybrid2_en", "chunk_en"), ("chunk_en", "bm25_zh")):
    d = np.array([per_query[k]["metrics"][a_]["ndcg10"] - per_query[k]["metrics"][b_]["ndcg10"] for k in per_query])
    pairs[f"{a_} vs {b_} (nDCG@10)"] = {"wins": int((d > 0.0001).sum()), "losses": int((d < -0.0001).sum()),
                                        "ties": int((abs(d) <= 0.0001).sum())}
stats["set_A_paired_comparisons"] = pairs

# ---------- 7. sets B and C: rankings, plus candidates that still need a human judgment ----------
names = {r["ccid"]: r["name"] for r in vt.select("ccid", "name").collect()}
cj = spark.read.json(SUB + "/course.json")
courses = (cj.select(F.col("id").alias("course_id"), F.col("name").alias("course_name"), F.explode("resource").alias("res"))
             .select("course_id", "course_name", F.col("res.resource_id").alias("rid"))
             .filter(F.col("rid").startswith("V_")))
bridge = (sc.textFile(SUB + "/video_id-ccid.txt").map(lambda l: l.rstrip("\n").split("\t"))
            .filter(lambda p: len(p) == 2).toDF(["rid", "ccid"]))
cmap = (courses.join(bridge, "rid").groupBy("ccid").agg(F.min(F.struct("course_id", "course_name")).alias("c"))
        .select("ccid", F.col("c.course_name").alias("course_name"))).toPandas().set_index("ccid")["course_name"].to_dict()
ctext = {(r["ccid"], int(r["chunk_idx"])): r["chunk_text"]
         for r in spark.read.parquet(PROC + "/chunk_embeddings").select("ccid", "chunk_idx", "chunk_text").collect()}
best_chunk = df.set_index(["qid", "ccid"])["best_chunk"].to_dict()

judged = json.load(open(JUDGE_PATH, encoding="utf-8"))["queries"] if os.path.exists(JUDGE_PATH) else {}
POOL = {"B": ["video_en", "chunk_en", "hybrid_en", "hybrid2_en"], "C": ["video_en", "chunk_en", "hybrid2_en"]}
ALL_M = ["video_en", "chunk_en", "hybrid_en", "hybrid2_en", "terms_en"]
cands, rankings = {}, {}
for setname, qlist in (("B", B), ("C", C)):
    for i, text in enumerate(qlist, 1):
        label = f"{setname}{i:02d}"
        lists = ranked_lists(label)
        rankings[label] = {"query": text, "set": setname, "translation": translations.get(f"{label}_en", []),
                           **{m: lists[m][:10] for m in ALL_M}}
        already = set(judged.get(label, {}).get("grades", {}))
        pool = {}
        for m in POOL[setname]:
            for r, c in enumerate(lists[m][:5], 1):
                pool.setdefault(c, {})[m] = r
        items = []
        for c, ranks in pool.items():
            if c in already:
                continue
            bc = int(best_chunk[(f"{label}_en", c)])
            items.append({"ccid": c, "video": names.get(c), "course": cmap.get(c),
                          "snippet": ctext.get((c, bc), "")[:170], "ranks": ranks})
        cands[label] = {"query": text, "candidates": items}
json.dump(cands, open(CAND_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1, default=py)
json.dump(rankings, open(RANK_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=1, default=py)
stats["to_judge_new_B"] = int(sum(len(v["candidates"]) for k, v in cands.items() if k.startswith("B")))
stats["to_judge_new_C"] = int(sum(len(v["candidates"]) for k, v in cands.items() if k.startswith("C")))

# ---------- 8. export assets for the standalone search function (serving side) ----------
os.makedirs(ASSETS, exist_ok=True)
ch = spark.read.parquet(PROC + "/chunk_embeddings").orderBy("ccid", "chunk_idx").toPandas()
np.save(ASSETS + "/chunk_emb.npy", np.stack(ch["embedding"].apply(lambda x: np.asarray(x, dtype="float16")).values))
json.dump({"ccid": ch["ccid"].tolist(), "chunk_idx": ch["chunk_idx"].tolist(), "text": ch["chunk_text"].tolist()},
          open(ASSETS + "/chunks.json", "w", encoding="utf-8"), ensure_ascii=False)
np.save(ASSETS + "/concept_emb.npy", Cemb.astype("float16"))
cindex = {n: i for i, n in enumerate(cnames)}
vlist = vc.select("ccid", "concepts").collect()
json.dump({"concept_names": cnames,
           "video_concepts": {r["ccid"]: [cindex[n] for n in r["concepts"] if n in cindex] for r in vlist},
           "video_meta": {c: {"name": names.get(c), "course": cmap.get(c)} for c in names}},
          open(ASSETS + "/meta.json", "w", encoding="utf-8"), ensure_ascii=False)
# document term statistics for BM25 at query time
tok_rows = vt.select("ccid", "n_tokens", "tokens").collect()
tfs = {r["ccid"]: {} for r in tok_rows}
for r in tok_rows:
    d = tfs[r["ccid"]]
    for w in r["tokens"]:
        d[w] = d.get(w, 0) + 1
json.dump({"N": N, "avgdl": avgdl, "dl": {r["ccid"]: r["n_tokens"] for r in tok_rows},
           "df": {r["term"]: int(r["df"]) for r in ts.collect()}, "tf": tfs},
          open(ASSETS + "/bm25.json", "w", encoding="utf-8"), ensure_ascii=False)
stats["exported_chunks"] = int(len(ch))

stats["runtime_sec"] = round(time.time() - t0, 1)
json.dump(stats, open(STATS_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=2, default=py)

print("=" * 60)
print("MODULE 5 SCORING COMPLETE | runtime", stats["runtime_sec"], "s | score rows", stats["score_rows"])
print("translation (English query -> Chinese concept) on set A: hit@1", stats["translation_hit_at_1_setA"],
      "| hit@3", stats["translation_hit_at_3_setA"])
print("queries with no translation above the threshold:", stats["queries_with_no_translation"])
print("SET A (30 queries) mean metrics [95% bootstrap CI]:")
for m in methods_A:
    s = summary[m]
    print(f"  {m:10s} P@5 {s['p5']['mean']:.3f}  P@10 {s['p10']['mean']:.3f} {s['p10']['ci95']}  "
          f"nDCG@10 {s['ndcg10']['mean']:.3f} {s['ndcg10']['ci95']}  R@50 {s['r50']['mean']:.3f}")
print("paired comparisons:", pairs)
print("translations for set B/C queries:")
for lab, d in rankings.items():
    print("  ", lab, d["query"], "->", d["translation"])
print("new candidates to judge | B:", stats["to_judge_new_B"], "| C:", stats["to_judge_new_C"])
print("=" * 60)
spark.stop()

import json
import os
import random
import sys
import time

import numpy as np
import pandas as pd
from pyspark import StorageLevel
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.functions import array_to_vector
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType)
from pyspark.sql.window import Window

sys.stdout.reconfigure(encoding="utf-8")

HDFS = "hdfs://localhost:9000"
# EDUINSIGHT_SCOPE=subset (default, CS development subset) or full (whole MOOCCubeX corpus)
FULL = os.environ.get("EDUINSIGHT_SCOPE", "subset") == "full"
PROC = HDFS + ("/eduinsight/processed_full" if FULL else "/eduinsight/processed")
SUB = HDFS + ("/eduinsight/full" if FULL else "/eduinsight/subset_cs")
SUFFIX = "_full" if FULL else ""
STATS_PATH = f"/home/aryan/module4_stats{SUFFIX}.json"
POINTS_PATH = f"/home/aryan/module4_points{SUFFIX}.json"
K_SWEEP = [10, 15, 20, 30, 40, 60] if FULL else [6, 8, 10, 12, 15, 18, 22, 26, 30]
MIN_K = 10          # rule: best silhouette among k >= MIN_K (k < 10 merges distinct subjects)
TOP_NEIGHBOURS = 10
BLOCK = 500         # rows of the similarity matrix computed at a time
TSNE_N = 8000       # t-SNE map uses at most this many videos (a random sample at full scale)

spark = (SparkSession.builder.appName("EduInsight-Module4-Clustering" + ("-full" if FULL else ""))
         .master(os.environ.get("EDUINSIGHT_MASTER", "local[*]"))
         .config("spark.sql.shuffle.partitions", "64" if FULL else "8")
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("WARN")
t0 = time.time()
stats = {"scope": "full" if FULL else "subset"}


def py(o):
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    raise TypeError(type(o))


# ---------- load embeddings written by Module 3 ----------
emb = spark.read.parquet(PROC + "/video_embeddings").select("ccid", "embedding")
feat = emb.withColumn("features", array_to_vector("embedding")).select("ccid", "features")
feat.persist(StorageLevel.MEMORY_AND_DISK)
N = feat.count()
stats["videos"] = N

# ---------- 1. KMeans (Spark MLlib, cosine distance): sweep k, pick by silhouette ----------
evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction",
                                metricName="silhouette", distanceMeasure="cosine")
models, sweep = {}, []
for k in K_SWEEP:
    ts_ = time.time()
    m = KMeans(k=k, seed=42, maxIter=50, distanceMeasure="cosine", featuresCol="features").fit(feat)
    sil = evaluator.evaluate(m.transform(feat))
    models[k] = m
    sweep.append({"k": k, "silhouette": round(float(sil), 4), "seconds": round(time.time() - ts_, 1)})
    print("k =", k, "silhouette", sweep[-1]["silhouette"], "in", sweep[-1]["seconds"], "s", flush=True)
stats["k_sweep"] = sweep
K = max([s for s in sweep if s["k"] >= MIN_K], key=lambda s: s["silhouette"])["k"]
stats["chosen_k"] = K
stats["chosen_k_rule"] = f"highest silhouette among k >= {MIN_K}"

assign = models[K].transform(feat).select("ccid", F.col("prediction").alias("cluster"))
assign.write.mode("overwrite").parquet(PROC + "/video_clusters")
assign = spark.read.parquet(PROC + "/video_clusters").cache()

# stability: refit with other seeds and compare with the final clustering
from sklearn.metrics import adjusted_rand_score
base = assign.toPandas().set_index("ccid")["cluster"]
aris = []
for seed in (1, 2, 3):
    m2 = KMeans(k=K, seed=seed, maxIter=50, distanceMeasure="cosine", featuresCol="features").fit(feat)
    p2 = m2.transform(feat).select("ccid", F.col("prediction").alias("c2")).toPandas().set_index("ccid")["c2"]
    aris.append(round(float(adjusted_rand_score(base.loc[p2.index], p2)), 3))
stats["stability_ari_other_seeds"] = aris

# ---------- 2. who is in each cluster: course + video names ----------
cj = spark.read.json(SUB + "/course.json")
courses = (cj.select(F.col("id").alias("course_id"), F.col("name").alias("course_name"),
                     F.explode("resource").alias("res"))
             .select("course_id", "course_name", F.col("res.resource_id").alias("rid"))
             .filter(F.col("rid").startswith("V_")))
bridge = (sc.textFile(SUB + "/video_id-ccid.txt").map(lambda l: l.rstrip("\n").split("\t"))
            .filter(lambda p: len(p) == 2).toDF(["rid", "ccid"]))
cmap = (courses.join(bridge, "rid")
        .groupBy("ccid").agg(F.min(F.struct("course_id", "course_name")).alias("c"))
        .select("ccid", F.col("c.course_id").alias("course_id"), F.col("c.course_name").alias("course_name")))
vt = spark.read.parquet(PROC + "/videos_tokenized")
vnames = vt.select("ccid", "name")
info = (assign.join(cmap, "ccid", "left").join(vnames, "ccid", "left")).toPandas()
info["course_id"] = info["course_id"].where(info["course_id"].notna(), "?" + info["ccid"])  # no course -> its own group
info["course_name"] = info["course_name"].fillna("(no course)")
info = info.set_index("ccid")

# ---------- 3. label clusters with distinctive terms (Spark aggregation) ----------
ts = spark.read.parquet(PROC + "/term_stats").select("term", "idf")
n_c = assign.groupBy("cluster").count().withColumnRenamed("count", "n_c")
tdf = (vt.select("ccid", "tokens").join(assign, "ccid")
         .select("cluster", F.explode(F.array_distinct("tokens")).alias("term"))
         .groupBy("cluster", "term").agg(F.count("*").alias("df_c")))
scored = (tdf.join(n_c, "cluster").join(ts, "term")
          .withColumn("share", F.col("df_c") / F.col("n_c"))
          .filter((F.col("share") >= 0.15) & (F.col("df_c") >= 5))
          .withColumn("score", F.col("share") * F.col("idf")))
w = Window.partitionBy("cluster").orderBy(F.desc("score"))
top_terms = (scored.withColumn("rn", F.row_number().over(w)).filter("rn <= 8")
             .groupBy("cluster").agg(F.collect_list(F.struct("rn", "term")).alias("t")).toPandas())
terms_of = {int(r["cluster"]): [x["term"] for x in sorted(r["t"], key=lambda z: z["rn"])]
            for _, r in top_terms.iterrows()}
scored.select("cluster", "term", "df_c", "n_c", "idf", "score").write.mode("overwrite").parquet(PROC + "/cluster_terms")

clusters = []
for c, g in info.groupby("cluster"):
    vc = g["course_id"].value_counts()
    cn = g.drop_duplicates("course_id").set_index("course_id")["course_name"]
    clusters.append({
        "cluster": int(c), "size": int(len(g)), "n_courses": int(len(vc)),
        "largest_course_share": round(float(vc.iloc[0] / len(g)), 3),
        "top_courses": [(cn[i], int(n)) for i, n in vc.head(3).items()],
        "terms": terms_of.get(int(c), []),
    })
clusters.sort(key=lambda d: -d["size"])
stats["clusters"] = clusters
stats["clusters_with_5plus_courses"] = int(sum(1 for d in clusters if d["n_courses"] >= 5))

# ---------- 4. distributed similarity: top-10 neighbours of every video ----------
epd = emb.orderBy("ccid").toPandas()
ids = epd["ccid"].tolist()
E = np.stack(epd["embedding"].apply(lambda x: np.asarray(x, dtype="float32")).values)
del epd
Eb, idsb = sc.broadcast(E), sc.broadcast(ids)


def neighbours_partition(positions):
    E_, ids_ = Eb.value, idsb.value
    pos_all = np.fromiter(positions, dtype=int)
    out = []
    for b in range(0, len(pos_all), BLOCK):
        pos = pos_all[b:b + BLOCK]
        S = E_[pos] @ E_.T
        S[np.arange(len(pos)), pos] = -1.0
        top = np.argpartition(-S, TOP_NEIGHBOURS, axis=1)[:, :TOP_NEIGHBOURS]
        for i, p in enumerate(pos):
            order = top[i][np.argsort(-S[i, top[i]])]
            for r, j in enumerate(order):
                out.append(Row(ccid=ids_[p], rank=r + 1, neighbour=ids_[j], cosine=float(S[i, j])))
    return iter(out)


nb_schema = StructType([StructField("ccid", StringType()), StructField("rank", IntegerType()),
                        StructField("neighbour", StringType()), StructField("cosine", DoubleType())])
nb = spark.createDataFrame(sc.parallelize(range(N), 96 if FULL else 8).mapPartitions(neighbours_partition), nb_schema)
nb.write.mode("overwrite").parquet(PROC + "/video_neighbours")
nbp = spark.read.parquet(PROC + "/video_neighbours").toPandas()
stats["neighbour_rows"] = int(len(nbp))

# ---------- 5. redundancy across courses, validated against real word overlap ----------
nbp["course_a"] = nbp["ccid"].map(info["course_id"])
nbp["course_b"] = nbp["neighbour"].map(info["course_id"])
cross = nbp[nbp["course_a"] != nbp["course_b"]].copy()
cross["lo"] = np.minimum(cross["ccid"].values, cross["neighbour"].values)
cross["hi"] = np.maximum(cross["ccid"].values, cross["neighbour"].values)
cross = cross.sort_values("cosine", ascending=False).drop_duplicates(["lo", "hi"])
stats["cross_course_pairs_in_top10"] = int(len(cross))
stats["same_course_neighbour_share_pct"] = round(100 * float((nbp["course_a"] == nbp["course_b"]).mean()), 1)
stats["redundancy_by_threshold"] = {
    str(t): {"pairs": int((cross["cosine"] >= t).sum()),
             "videos": int(len(set(cross.loc[cross["cosine"] >= t, "ccid"]) | set(cross.loc[cross["cosine"] >= t, "neighbour"])))}
    for t in (0.80, 0.85, 0.90, 0.95)}

random.seed(42)
bins = [(0.0, 0.80), (0.80, 0.85), (0.85, 0.90), (0.90, 0.95), (0.95, 1.01)]
bin_samples = []
for lo, hi in bins:
    sub = cross[(cross["cosine"] >= lo) & (cross["cosine"] < hi)]
    bin_samples.append(sub.sample(min(400, len(sub)), random_state=42) if len(sub) else sub)
all_ids = list(info.index)
rnd_pairs = []
while len(rnd_pairs) < 2000:
    a, b = random.sample(all_ids, 2)
    if info.at[a, "course_id"] != info.at[b, "course_id"]:
        rnd_pairs.append((a, b))
top_pairs_df = cross.head(15)

# word lists are loaded only for the videos these pairs involve
need = set()
for s_ in bin_samples:
    need |= set(s_["ccid"]) | set(s_["neighbour"])
for a, b in rnd_pairs:
    need |= {a, b}
need |= set(top_pairs_df["ccid"]) | set(top_pairs_df["neighbour"])
tok = {r["ccid"]: set(r["tokens"]) for r in
       vt.filter(F.col("ccid").isin(list(need))).select("ccid", "tokens").collect()}


def jaccard(a, b):
    A, B = tok.get(a, set()), tok.get(b, set())
    return len(A & B) / len(A | B) if (A | B) else 0.0


bin_stats = []
for (lo, hi), sample in zip(bins, bin_samples):
    sub_n = int(((cross["cosine"] >= lo) & (cross["cosine"] < hi)).sum())
    js = [jaccard(r["ccid"], r["neighbour"]) for _, r in sample.iterrows()]
    bin_stats.append({"cosine_range": [lo, min(hi, 1.0)], "pairs": sub_n,
                      "mean_token_jaccard": round(float(np.mean(js)), 3) if js else None})
stats["jaccard_by_cosine_bin"] = bin_stats
stats["random_cross_course_mean_jaccard"] = round(float(np.mean([jaccard(a, b) for a, b in rnd_pairs])), 3)

hi_df = cross[cross["cosine"] >= 0.90]
cp = {}
for r in hi_df[["ccid", "neighbour"]].itertuples(index=False):
    key = tuple(sorted((info.at[r.ccid, "course_name"], info.at[r.neighbour, "course_name"])))
    cp[key] = cp.get(key, 0) + 1
stats["course_pair_overlap_ge_0.90"] = [{"courses": list(k), "video_pairs": v}
                                        for k, v in sorted(cp.items(), key=lambda kv: -kv[1])[:10]]

snip = {r["ccid"]: r["s"] for r in vt.filter(F.col("ccid").isin(list(need)))
        .select("ccid", F.substring("text", 1, 70).alias("s")).collect()}
top_pairs = []
for _, r in top_pairs_df.iterrows():
    a, b = r["ccid"], r["neighbour"]
    top_pairs.append({"cosine": round(float(r["cosine"]), 3), "token_jaccard": round(jaccard(a, b), 3),
                      "a": {"video": info.at[a, "name"], "course": info.at[a, "course_name"], "text": snip.get(a)},
                      "b": {"video": info.at[b, "name"], "course": info.at[b, "course_name"], "text": snip.get(b)}})
stats["top_cross_course_pairs"] = top_pairs

# ---------- 6. 2-D map of videos (t-SNE, driver); a random sample at full scale ----------
from sklearn.manifold import TSNE
rs = np.random.default_rng(42)
sel = np.arange(N) if N <= TSNE_N else np.sort(rs.choice(N, TSNE_N, replace=False))
stats["map_points"] = int(len(sel))
xy = TSNE(n_components=2, init="pca", perplexity=30, random_state=42, learning_rate="auto").fit_transform(E[sel])
xy = (xy - xy.min(axis=0)) / (xy.max(axis=0) - xy.min(axis=0))
sel_ids = [ids[i] for i in sel]
coords = pd.DataFrame({"ccid": sel_ids, "x": xy[:, 0].astype(float), "y": xy[:, 1].astype(float)})
spark.createDataFrame(coords).write.mode("overwrite").parquet(PROC + "/video_coords")
points = {
    "x": [int(round(v * 1000)) for v in xy[:, 0]], "y": [int(round(v * 1000)) for v in xy[:, 1]],
    "c": [int(info.at[i, "cluster"]) for i in sel_ids],
    "clusters": [{"cluster": d["cluster"], "size": d["size"], "terms": d["terms"][:5]} for d in clusters],
}
if not FULL:
    course_list = sorted(info["course_name"].unique())
    cidx = {c: i for i, c in enumerate(course_list)}
    points["course"] = [cidx[info.at[i, "course_name"]] for i in sel_ids]
    points["courses"] = course_list
with open(POINTS_PATH, "w", encoding="utf-8") as f:
    json.dump(points, f, ensure_ascii=False)

stats["runtime_sec"] = round(time.time() - t0, 1)
with open(STATS_PATH, "w", encoding="utf-8") as f:
    json.dump(stats, f, ensure_ascii=False, indent=2, default=py)

print("=" * 60)
print("MODULE 4 COMPLETE | scope:", stats["scope"], "| videos:", N)
print("k sweep (silhouette):", [(s["k"], s["silhouette"]) for s in sweep])
print("chosen k:", K, "| seed stability (ARI vs other seeds):", aris)
print("clusters (id, size, #courses, terms):")
for d in clusters:
    print("  ", d["cluster"], d["size"], d["n_courses"], d["terms"][:6])
print("redundancy by threshold:", stats["redundancy_by_threshold"])
print("jaccard by cosine bin:", bin_stats, "| random:", stats["random_cross_course_mean_jaccard"])
print("runtime_sec:", stats["runtime_sec"])
print("=" * 60)
spark.stop()

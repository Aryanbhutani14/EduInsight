import json
import random
import sys
import time

import numpy as np
import pandas as pd
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
PROC = HDFS + "/eduinsight/processed"
SUB = HDFS + "/eduinsight/subset_cs"
STATS_PATH = "/home/aryan/module4_stats.json"
POINTS_PATH = "/home/aryan/module4_points.json"
K_SWEEP = [6, 8, 10, 12, 15, 18, 22, 26, 30]
MIN_K = 10          # rule: best silhouette among k >= MIN_K (k < 10 merges distinct subjects)
TOP_NEIGHBOURS = 10

spark = (SparkSession.builder.appName("EduInsight-Module4-Clustering")
         .master("local[*]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("WARN")
t0 = time.time()
stats = {}


def py(o):
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    raise TypeError(type(o))


# ---------- load embeddings written by Module 3 ----------
emb = spark.read.parquet(PROC + "/video_embeddings").select("ccid", "embedding")
feat = emb.withColumn("features", array_to_vector("embedding")).select("ccid", "features").cache()
N = feat.count()
stats["videos"] = N

# ---------- 1. KMeans (Spark MLlib, cosine distance): sweep k, pick by silhouette ----------
evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="prediction",
                                metricName="silhouette", distanceMeasure="cosine")
models, sweep = {}, []
for k in K_SWEEP:
    m = KMeans(k=k, seed=42, maxIter=50, distanceMeasure="cosine", featuresCol="features").fit(feat)
    sil = evaluator.evaluate(m.transform(feat))
    models[k] = m
    sweep.append({"k": k, "silhouette": round(float(sil), 4)})
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
info["course_id"] = info["course_id"].fillna("?")
info["course_name"] = info["course_name"].fillna("?")
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
rows = emb.orderBy("ccid").collect()
ids = [r["ccid"] for r in rows]
E = np.array([r["embedding"] for r in rows], dtype="float32")
Eb, idsb = sc.broadcast(E), sc.broadcast(ids)


def neighbours_partition(positions):
    E_, ids_ = Eb.value, idsb.value
    pos = np.fromiter(positions, dtype=int)
    if len(pos) == 0:
        return iter([])
    S = E_[pos] @ E_.T
    S[np.arange(len(pos)), pos] = -1.0
    top = np.argpartition(-S, TOP_NEIGHBOURS, axis=1)[:, :TOP_NEIGHBOURS]
    out = []
    for i, p in enumerate(pos):
        order = top[i][np.argsort(-S[i, top[i]])]
        for r, j in enumerate(order):
            out.append(Row(ccid=ids_[p], rank=r + 1, neighbour=ids_[j], cosine=float(S[i, j])))
    return iter(out)


nb_schema = StructType([StructField("ccid", StringType()), StructField("rank", IntegerType()),
                        StructField("neighbour", StringType()), StructField("cosine", DoubleType())])
nb = spark.createDataFrame(sc.parallelize(range(N), 8).mapPartitions(neighbours_partition), nb_schema)
nb.write.mode("overwrite").parquet(PROC + "/video_neighbours")
nbp = spark.read.parquet(PROC + "/video_neighbours").toPandas()
stats["neighbour_rows"] = int(len(nbp))

# ---------- 5. redundancy across courses, validated against real word overlap ----------
nbp["course_a"] = nbp["ccid"].map(info["course_id"])
nbp["course_b"] = nbp["neighbour"].map(info["course_id"])
cross = nbp[nbp["course_a"] != nbp["course_b"]].copy()
cross["key"] = cross.apply(lambda r: tuple(sorted((r["ccid"], r["neighbour"]))), axis=1)
cross = cross.sort_values("cosine", ascending=False).drop_duplicates("key")
stats["cross_course_pairs_in_top10"] = int(len(cross))
stats["same_course_neighbour_share_pct"] = round(100 * float((nbp["course_a"] == nbp["course_b"]).mean()), 1)
stats["redundancy_by_threshold"] = {
    str(t): {"pairs": int((cross["cosine"] >= t).sum()),
             "videos": int(len(set(cross.loc[cross["cosine"] >= t, "ccid"]) | set(cross.loc[cross["cosine"] >= t, "neighbour"])))}
    for t in (0.80, 0.85, 0.90, 0.95)}

tok = {r["ccid"]: set(r["tokens"]) for r in vt.select("ccid", "tokens").collect()}


def jaccard(a, b):
    A, B = tok.get(a, set()), tok.get(b, set())
    return len(A & B) / len(A | B) if (A | B) else 0.0


random.seed(42)
bins = [(0.0, 0.80), (0.80, 0.85), (0.85, 0.90), (0.90, 0.95), (0.95, 1.01)]
bin_stats = []
for lo, hi in bins:
    sub = cross[(cross["cosine"] >= lo) & (cross["cosine"] < hi)]
    sample = sub.sample(min(400, len(sub)), random_state=42) if len(sub) else sub
    js = [jaccard(r["ccid"], r["neighbour"]) for _, r in sample.iterrows()]
    bin_stats.append({"cosine_range": [lo, min(hi, 1.0)], "pairs": int(len(sub)),
                      "mean_token_jaccard": round(float(np.mean(js)), 3) if js else None})
stats["jaccard_by_cosine_bin"] = bin_stats
all_ids = list(info.index)
rnd = []
while len(rnd) < 2000:
    a, b = random.sample(all_ids, 2)
    if info.at[a, "course_id"] != info.at[b, "course_id"]:
        rnd.append(jaccard(a, b))
stats["random_cross_course_mean_jaccard"] = round(float(np.mean(rnd)), 3)

hi = cross[cross["cosine"] >= 0.90]
cp = {}
for _, r in hi.iterrows():
    key = tuple(sorted((info.at[r["ccid"], "course_name"], info.at[r["neighbour"], "course_name"])))
    cp[key] = cp.get(key, 0) + 1
stats["course_pair_overlap_ge_0.90"] = [{"courses": list(k), "video_pairs": v}
                                        for k, v in sorted(cp.items(), key=lambda kv: -kv[1])[:10]]

snip = {r["ccid"]: r["s"] for r in vt.select("ccid", F.substring("text", 1, 70).alias("s")).collect()}
top_pairs = []
for _, r in cross.head(15).iterrows():
    a, b = r["ccid"], r["neighbour"]
    top_pairs.append({"cosine": round(float(r["cosine"]), 3), "token_jaccard": round(jaccard(a, b), 3),
                      "a": {"video": info.at[a, "name"], "course": info.at[a, "course_name"], "text": snip.get(a)},
                      "b": {"video": info.at[b, "name"], "course": info.at[b, "course_name"], "text": snip.get(b)}})
stats["top_cross_course_pairs"] = top_pairs

# ---------- 6. 2-D map of all videos (t-SNE, driver) ----------
from sklearn.manifold import TSNE
xy = TSNE(n_components=2, init="pca", perplexity=30, random_state=42, learning_rate="auto").fit_transform(E)
xy = (xy - xy.min(axis=0)) / (xy.max(axis=0) - xy.min(axis=0))
coords = pd.DataFrame({"ccid": ids, "x": xy[:, 0].astype(float), "y": xy[:, 1].astype(float)})
spark.createDataFrame(coords).write.mode("overwrite").parquet(PROC + "/video_coords")
course_list = sorted(info["course_name"].unique())
cidx = {c: i for i, c in enumerate(course_list)}
points = {
    "x": [int(round(v * 1000)) for v in xy[:, 0]], "y": [int(round(v * 1000)) for v in xy[:, 1]],
    "c": [int(info.at[i, "cluster"]) for i in ids],
    "course": [cidx[info.at[i, "course_name"]] for i in ids],
    "courses": course_list,
    "clusters": [{"cluster": d["cluster"], "size": d["size"], "terms": d["terms"][:5]} for d in clusters],
}
with open(POINTS_PATH, "w", encoding="utf-8") as f:
    json.dump(points, f, ensure_ascii=False)

stats["runtime_sec"] = round(time.time() - t0, 1)
with open(STATS_PATH, "w", encoding="utf-8") as f:
    json.dump(stats, f, ensure_ascii=False, indent=2, default=py)

print("=" * 60)
print("MODULE 4 COMPLETE")
print("k sweep (silhouette):", [(s["k"], s["silhouette"]) for s in sweep])
print("chosen k:", K, "| seed stability (ARI vs other seeds):", aris)
print("clusters (size, #courses, terms):")
for d in clusters:
    print("  ", d["cluster"], d["size"], d["n_courses"], d["terms"][:6])
print("redundancy by threshold:", stats["redundancy_by_threshold"])
print("jaccard by cosine bin:", bin_stats, "| random:", stats["random_cross_course_mean_jaccard"])
print("runtime_sec:", stats["runtime_sec"])
print("=" * 60)
spark.stop()

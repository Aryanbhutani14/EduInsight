import json
import re
import sys
import time

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

sys.stdout.reconfigure(encoding="utf-8")

HDFS = "hdfs://localhost:9000"
IN = HDFS + "/eduinsight/subset_cs"
OUT = HDFS + "/eduinsight/processed"
STATS_PATH = "/home/aryan/module2_stats.json"

spark = (SparkSession.builder.appName("EduInsight-Module2-Preprocessing")
         .master("local[*]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("WARN")

import stopwordsiso

# Spoken-lecture fillers that survive the generic stopword list. Reviewed by hand from the
# top document-frequency terms: deictics, quantifiers, discourse markers, light verbs.
# Domain words (算法, 函数, 数据, 方法 ...) are deliberately NOT here; IDF down-weights them.
LECTURE_FILLERS = set("""
同学们 同学 大家 老师 好的 接下来 下面 首先 其次 然后 所以 因此 其实 比如 例如 那么 这样 这里
那里 现在 一下 一种 也就是 就是说 可能 已经 时候
需要 进行 没有 两个 三个 几个 这种 这是 看到 能够 实际上 不同 最后 来看 非常 之后 之前 以后
前面 后面 上面 里面 当中 应该 简单 也就是说 比如说 知道 刚才 之间 比较 直接 整个 第一个
第二个 很多 一定 同样 起来 一点 一次 看看 今天 继续 希望 通常 叫做 必须 不能 不会 当前 地方
""".split())

STOP = set(stopwordsiso.stopwords("zh")) | set(stopwordsiso.stopwords("en")) | LECTURE_FILLERS
STOP_B = sc.broadcast(STOP)
TOKEN_OK = re.compile(r"^[一-鿿A-Za-z][一-鿿A-Za-z0-9+#._\-]*$")


def process_partition(lines):
    import jieba
    jieba.setLogLevel(60)
    stop = STOP_B.value
    for line in lines:
        o = json.loads(line)
        segs = [s.strip() for s in o["text"] if s and s.strip()]
        text = " ".join(segs)
        raw = jieba.lcut(text)
        kept = []
        for t in raw:
            t = t.strip().lower()
            if len(t) < 2 or t in stop or not TOKEN_OK.match(t):
                continue
            kept.append(t)
        dur = (o["end"][-1] - o["start"][0]) if o["end"] and o["start"] else 0.0
        yield Row(ccid=o["ccid"], name=o["name"], n_segments=len(segs),
                  duration_sec=float(dur), n_chars=len(text),
                  n_tokens_raw=len(raw), n_tokens=len(kept),
                  tokens=kept, text=text, segments=segs)


schema = StructType([
    StructField("ccid", StringType()), StructField("name", StringType()),
    StructField("n_segments", IntegerType()), StructField("duration_sec", DoubleType()),
    StructField("n_chars", IntegerType()), StructField("n_tokens_raw", IntegerType()),
    StructField("n_tokens", IntegerType()), StructField("tokens", ArrayType(StringType())),
    StructField("text", StringType()), StructField("segments", ArrayType(StringType())),
])

t0 = time.time()
stats = {}

# ---------- 1. Tokenize + stopword-filter captions (distributed) ----------
lines = sc.textFile(IN + "/video.json", minPartitions=8)
videos = spark.createDataFrame(lines.mapPartitions(process_partition), schema)
videos.write.mode("overwrite").parquet(OUT + "/videos_tokenized")

vt = spark.read.parquet(OUT + "/videos_tokenized")
vt.cache()
agg = vt.agg(F.count("*").alias("n"), F.sum("n_tokens_raw").alias("raw"),
             F.sum("n_tokens").alias("kept"), F.avg("n_tokens").alias("avg_kept"),
             F.sum("n_chars").alias("chars")).first()
stats["videos"] = agg["n"]
stats["tokens_raw"] = agg["raw"]
stats["tokens_kept"] = agg["kept"]
stats["pct_tokens_removed"] = round(100 * (1 - agg["kept"] / agg["raw"]), 1)
stats["avg_tokens_per_video"] = round(agg["avg_kept"], 1)
stats["total_chars"] = agg["chars"]

# ---------- 2. Corpus term statistics (distributed aggregation) ----------
terms = (vt.select("ccid", F.explode("tokens").alias("term"))
           .groupBy("term")
           .agg(F.count("*").alias("tf"), F.countDistinct("ccid").alias("df")))
N_VIDEOS = stats["videos"]
terms = (terms.withColumn("idf", F.log(F.lit(float(N_VIDEOS)) / F.col("df")))
              .withColumn("tfidf", F.col("tf") * F.col("idf")))
terms.write.mode("overwrite").parquet(OUT + "/term_stats")
terms = spark.read.parquet(OUT + "/term_stats")
stats["vocab_size"] = terms.count()
stats["hapax_terms_df1"] = terms.filter("df = 1").count()
stats["vocab_df_ge_2"] = stats["vocab_size"] - stats["hapax_terms_df1"]
stats["top_terms_by_df"] = [(r["term"], r["df"], r["tf"]) for r in
                            terms.orderBy(F.desc("df")).limit(30).collect()]
stats["top_terms_by_tf"] = [(r["term"], r["tf"], r["df"]) for r in
                            terms.orderBy(F.desc("tf")).limit(30).collect()]
stats["top_terms_by_tfidf"] = [(r["term"], round(r["tfidf"], 1), r["df"]) for r in
                               terms.filter("df >= 5").orderBy(F.desc("tfidf")).limit(30).collect()]

# ---------- 3. Concept-noise filter + distributed concept<->video join ----------
concepts = spark.read.json(IN + "/concept.json").select("id", "name")
concepts = concepts.withColumn("is_noisy", F.length("name") <= 2)
stats["concepts_total"] = concepts.count()
stats["concepts_noisy"] = concepts.filter("is_noisy").count()
clean = concepts.filter(~F.col("is_noisy")).select(F.col("id").alias("concept_id"), "name")
clean.write.mode("overwrite").parquet(OUT + "/concepts_clean")
stats["concepts_clean"] = stats["concepts_total"] - stats["concepts_noisy"]

cv = (sc.textFile(IN + "/concept-video.txt", minPartitions=4)
        .map(lambda l: l.rstrip("\n").split("\t"))
        .filter(lambda p: len(p) == 2)
        .toDF(["concept_id", "ccid"]))
stats["concept_video_links_raw"] = cv.count()
vc = (cv.join(F.broadcast(clean), "concept_id")
        .groupBy("ccid")
        .agg(F.collect_set("name").alias("concepts"), F.count("*").alias("n_concepts")))
vc.write.mode("overwrite").parquet(OUT + "/video_concepts")
vc = spark.read.parquet(OUT + "/video_concepts")
stats["videos_with_clean_concepts"] = vc.count()
stats["avg_clean_concepts_per_video"] = round(vc.agg(F.avg("n_concepts")).first()[0], 1)
stats["concept_video_links_clean"] = vc.agg(F.sum("n_concepts")).first()[0]

# ---------- 4. Before/after example for the demo ----------
ex = vt.filter(F.col("ccid") == "AC1E9080F4785AB99C33DC5901307461").first()
if ex is not None:
    import jieba
    jieba.setLogLevel(60)
    demo = []
    for seg in ex["segments"][:3]:
        raw = jieba.lcut(seg)
        kept = [t.strip().lower() for t in raw
                if len(t.strip()) >= 2 and t.strip().lower() not in STOP and TOKEN_OK.match(t.strip().lower())]
        demo.append({"segment": seg, "raw_tokens": raw, "kept_tokens": kept})
    stats["demo"] = {"video_name": ex["name"], "examples": demo}

stats["runtime_sec"] = round(time.time() - t0, 1)
with open(STATS_PATH, "w", encoding="utf-8") as f:
    json.dump(stats, f, ensure_ascii=False, indent=2)

print("=" * 60)
print("MODULE 2 COMPLETE")
for k in ["videos", "tokens_raw", "tokens_kept", "pct_tokens_removed", "avg_tokens_per_video",
          "vocab_size", "hapax_terms_df1", "vocab_df_ge_2", "concepts_total", "concepts_noisy",
          "concepts_clean", "videos_with_clean_concepts", "avg_clean_concepts_per_video", "runtime_sec"]:
    print(f"{k}: {stats[k]}")
print("top terms by document frequency:", [(t, d) for t, d, _ in stats["top_terms_by_df"][:20]])
print("top terms by tf-idf (df>=5):", [(t, d) for t, _, d in stats["top_terms_by_tfidf"][:30]])
print("=" * 60)
spark.stop()

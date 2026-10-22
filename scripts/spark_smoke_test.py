import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("EduInsight-Module1-SmokeTest").master("local[*]").getOrCreate()
sc = spark.sparkContext

# Read video.json (JSON-lines) straight from HDFS
rdd = sc.textFile("hdfs://localhost:9000/eduinsight/subset_cs/video.json")

def parse(line):
    obj = json.loads(line)
    return (obj["ccid"], len(obj["text"]))

parsed = rdd.map(parse)
parsed.cache()

n_videos = parsed.count()
total_segments = parsed.map(lambda x: x[1]).sum()

print("=" * 50)
print("EDUINSIGHT MODULE 1 SMOKE TEST (Spark reading from HDFS)")
print("=" * 50)
print(f"Videos read from HDFS: {n_videos}")
print(f"Total caption segments: {total_segments}")
print(f"Avg segments/video: {total_segments / n_videos:.1f}")

# Also read course.json and cross check course count
course_rdd = sc.textFile("hdfs://localhost:9000/eduinsight/subset_cs/course.json")
n_courses = course_rdd.count()
print(f"Courses read from HDFS: {n_courses}")

print("=" * 50)
print("SMOKE TEST PASSED" if n_videos == 4071 and n_courses == 59 else "SMOKE TEST: UNEXPECTED COUNTS")
print("=" * 50)

spark.stop()

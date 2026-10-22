import json
import os
import re
import sys
from collections import Counter

sys.stdout.reconfigure(encoding="utf-8")

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(BASE, "eda", "dashboard_stats.json")

stats = {}

# File sizes (full corpus)
files = ["course.json", "video.json", "concept.json", "concept-course.txt",
         "concept-video.txt", "course-field.json", "video_id-ccid.txt"]
stats["file_sizes_mb"] = {}
for fn in files:
    p = os.path.join(BASE, fn)
    stats["file_sizes_mb"][fn] = round(os.path.getsize(p) / (1024 * 1024), 1)

# Course counts + language split
cjk_re = re.compile(r"[一-鿿]")
n_courses = 0
n_chinese_name = 0
n_english_name = 0
with open(os.path.join(BASE, "course.json"), encoding="utf-8") as f:
    for line in f:
        obj = json.loads(line)
        n_courses += 1
        name = obj.get("name", "")
        if cjk_re.search(name):
            n_chinese_name += 1
        else:
            n_english_name += 1
stats["total_courses"] = n_courses
stats["courses_with_chinese_name"] = n_chinese_name
stats["courses_with_non_chinese_name"] = n_english_name

# Video counts + total duration + total segments (full corpus)
n_videos = 0
total_duration_sec = 0.0
total_segments = 0
with open(os.path.join(BASE, "video.json"), encoding="utf-8") as f:
    for line in f:
        obj = json.loads(line)
        n_videos += 1
        if obj["end"]:
            total_duration_sec += (obj["end"][-1] - obj["start"][0])
        total_segments += len(obj["text"])
stats["total_videos"] = n_videos
stats["total_duration_hours"] = round(total_duration_sec / 3600, 1)
stats["total_caption_segments"] = total_segments

# Concept count (full)
n_concepts = 0
with open(os.path.join(BASE, "concept.json"), encoding="utf-8") as f:
    for _ in f:
        n_concepts += 1
stats["total_concepts"] = n_concepts

# Relation counts
def count_lines(fn):
    n = 0
    with open(os.path.join(BASE, fn), encoding="utf-8") as f:
        for _ in f:
            n += 1
    return n

stats["concept_course_links"] = count_lines("concept-course.txt")
stats["concept_video_links"] = count_lines("concept-video.txt")
stats["bridge_rows"] = count_lines("video_id-ccid.txt")

# Field/discipline distribution
field_counter = Counter()
n_field_rows = 0
with open(os.path.join(BASE, "course-field.json"), encoding="utf-8") as f:
    for line in f:
        obj = json.loads(line)
        n_field_rows += 1
        for fld in obj.get("field", []):
            field_counter[fld] += 1
stats["total_field_tagged_courses"] = n_field_rows
stats["distinct_fields"] = len(field_counter)
stats["top_fields"] = field_counter.most_common(15)

with open(OUT, "w", encoding="utf-8") as f:
    json.dump(stats, f, ensure_ascii=False, indent=2)

print(json.dumps(stats, ensure_ascii=False, indent=2))

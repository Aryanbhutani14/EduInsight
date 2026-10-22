import json
import os

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(BASE, "subset_cs")
os.makedirs(OUT, exist_ok=True)

FIELD = "计算机科学与技术"
MIN_CAPTIONED_VIDEOS = 10

print("Loading course-field.json ...")
cs_course_field_rows = []
with open(os.path.join(BASE, "course-field.json"), encoding="utf-8") as f:
    for line in f:
        obj = json.loads(line)
        if FIELD in obj.get("field", []):
            cs_course_field_rows.append(obj)
cs_ids = {f"C_{r['course_id']}" for r in cs_course_field_rows}
print(f"CS-tagged courses: {len(cs_ids)}")

print("Loading video_id-ccid.txt bridge ...")
bridge = {}
with open(os.path.join(BASE, "video_id-ccid.txt"), encoding="utf-8") as f:
    for line in f:
        parts = line.rstrip("\n").split("\t")
        if len(parts) == 2:
            bridge[parts[0]] = parts[1]

print("Loading video.json ccids ...")
video_ccids = set()
with open(os.path.join(BASE, "video.json"), encoding="utf-8") as f:
    for line in f:
        video_ccids.add(json.loads(line)["ccid"])

print("Filtering course.json for CS courses with enough captioned videos ...")
kept_courses = []
target_ccids = set()
with open(os.path.join(BASE, "course.json"), encoding="utf-8") as f:
    for line in f:
        obj = json.loads(line)
        if obj["id"] not in cs_ids:
            continue
        vids = [r["resource_id"] for r in obj.get("resource", []) if r.get("resource_id", "").startswith("V_")]
        captioned = [bridge[v] for v in vids if bridge.get(v) in video_ccids]
        if len(captioned) >= MIN_CAPTIONED_VIDEOS:
            kept_courses.append(obj)
            target_ccids.update(captioned)

kept_ids = {c["id"] for c in kept_courses}
print(f"Courses kept (>= {MIN_CAPTIONED_VIDEOS} captioned videos): {len(kept_courses)}")
print(f"Unique target ccids: {len(target_ccids)}")

with open(os.path.join(OUT, "course.json"), "w", encoding="utf-8") as f:
    for c in kept_courses:
        f.write(json.dumps(c, ensure_ascii=False) + "\n")

print("Writing course-field.json subset ...")
with open(os.path.join(OUT, "course-field.json"), "w", encoding="utf-8") as f:
    for r in cs_course_field_rows:
        if f"C_{r['course_id']}" in kept_ids:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

print("Writing video.json subset ...")
n_videos_written = 0
with open(os.path.join(BASE, "video.json"), encoding="utf-8") as fin, \
     open(os.path.join(OUT, "video.json"), "w", encoding="utf-8") as fout:
    for line in fin:
        obj = json.loads(line)
        if obj["ccid"] in target_ccids:
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
            n_videos_written += 1
print(f"Videos written: {n_videos_written}")

print("Writing video_id-ccid.txt subset (only rows resolving to target ccids) ...")
n_bridge_written = 0
with open(os.path.join(BASE, "video_id-ccid.txt"), encoding="utf-8") as fin, \
     open(os.path.join(OUT, "video_id-ccid.txt"), "w", encoding="utf-8") as fout:
    for line in fin:
        parts = line.rstrip("\n").split("\t")
        if len(parts) == 2 and parts[1] in target_ccids:
            fout.write(line)
            n_bridge_written += 1
print(f"Bridge rows written: {n_bridge_written}")

print("Writing concept-course.txt subset ...")
n_cc = 0
with open(os.path.join(BASE, "concept-course.txt"), encoding="utf-8") as fin, \
     open(os.path.join(OUT, "concept-course.txt"), "w", encoding="utf-8") as fout:
    for line in fin:
        parts = line.rstrip("\n").split("\t")
        if len(parts) == 2 and parts[1] in kept_ids:
            fout.write(line)
            n_cc += 1
print(f"concept-course rows written: {n_cc}")

print("Writing concept-video.txt subset ...")
n_cv = 0
referenced_concepts = set()
with open(os.path.join(BASE, "concept-video.txt"), encoding="utf-8") as fin, \
     open(os.path.join(OUT, "concept-video.txt"), "w", encoding="utf-8") as fout:
    for line in fin:
        parts = line.rstrip("\n").split("\t")
        if len(parts) == 2 and parts[1] in target_ccids:
            fout.write(line)
            referenced_concepts.add(parts[0])
            n_cv += 1
print(f"concept-video rows written: {n_cv}")

print("Collecting referenced concepts from concept-course.txt too ...")
with open(os.path.join(OUT, "concept-course.txt"), encoding="utf-8") as f:
    for line in f:
        parts = line.rstrip("\n").split("\t")
        if len(parts) == 2:
            referenced_concepts.add(parts[0])
print(f"Total unique referenced concepts: {len(referenced_concepts)}")

print("Writing concept.json subset ...")
n_concepts = 0
with open(os.path.join(BASE, "concept.json"), encoding="utf-8") as fin, \
     open(os.path.join(OUT, "concept.json"), "w", encoding="utf-8") as fout:
    for line in fin:
        obj = json.loads(line)
        if obj["id"] in referenced_concepts:
            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
            n_concepts += 1
print(f"Concepts written: {n_concepts}")

print("\n=== DONE ===")
print(f"Output folder: {OUT}")

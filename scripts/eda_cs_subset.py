import json
import os
import sys
from collections import Counter, defaultdict

sys.stdout.reconfigure(encoding="utf-8")

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SUB = os.path.join(PROJ, "subset_cs")
OUT = os.path.join(PROJ, "eda")
os.makedirs(OUT, exist_ok=True)

plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial Unicode MS", "sans-serif"]
plt.rcParams["axes.unicode_minus"] = False

# ---------- Load ----------
courses = []
with open(os.path.join(SUB, "course.json"), encoding="utf-8") as f:
    for line in f:
        courses.append(json.loads(line))

videos = []
with open(os.path.join(SUB, "video.json"), encoding="utf-8") as f:
    for line in f:
        videos.append(json.loads(line))

bridge = {}
with open(os.path.join(SUB, "video_id-ccid.txt"), encoding="utf-8") as f:
    for line in f:
        parts = line.rstrip("\n").split("\t")
        if len(parts) == 2:
            bridge[parts[0]] = parts[1]

concept_video_pairs = []
with open(os.path.join(SUB, "concept-video.txt"), encoding="utf-8") as f:
    for line in f:
        parts = line.rstrip("\n").split("\t")
        if len(parts) == 2:
            concept_video_pairs.append((parts[0], parts[1]))

concept_course_pairs = []
with open(os.path.join(SUB, "concept-course.txt"), encoding="utf-8") as f:
    for line in f:
        parts = line.rstrip("\n").split("\t")
        if len(parts) == 2:
            concept_course_pairs.append((parts[0], parts[1]))

ccid_to_video = {v["ccid"]: v for v in videos}
video_ccids = set(ccid_to_video.keys())

stats_lines = []
def log(s=""):
    stats_lines.append(str(s))
    print(s)

log("=== EDA: CS Subset (subset_cs/) ===\n")
log(f"Courses: {len(courses)}")
log(f"Videos (captioned): {len(videos)}")
log(f"Concept-video links: {len(concept_video_pairs)}")
log(f"Concept-course links: {len(concept_course_pairs)}")

# ---------- 1. Videos per course ----------
videos_per_course = {}
for c in courses:
    vids = [r["resource_id"] for r in c.get("resource", []) if r.get("resource_id", "").startswith("V_")]
    captioned = [v for v in vids if bridge.get(v) in video_ccids]
    videos_per_course[c["id"]] = len(captioned)

vpc_values = sorted(videos_per_course.values(), reverse=True)
log(f"\nVideos per course: min={min(vpc_values)}, max={max(vpc_values)}, "
    f"mean={sum(vpc_values)/len(vpc_values):.1f}, median={vpc_values[len(vpc_values)//2]}")

id_to_name = {c["id"]: c["name"] for c in courses}
top10 = sorted(videos_per_course.items(), key=lambda x: -x[1])[:10]
log("\nTop 10 courses by captioned video count:")
for cid, cnt in top10:
    log(f"  {cnt:4d}  {cid}  {id_to_name[cid]}")

plt.figure(figsize=(10, 5))
plt.bar(range(len(vpc_values)), vpc_values, color="#4C72B0")
plt.xlabel("Course (ranked)")
plt.ylabel("Captioned video count")
plt.title("Videos per Course (CS Subset, 59 courses)")
plt.tight_layout()
plt.savefig(os.path.join(OUT, "01_videos_per_course.png"), dpi=150)
plt.close()

# ---------- 2. Video duration & caption segment stats ----------
durations = []
n_segments = []
for v in videos:
    if v["end"]:
        durations.append(v["end"][-1] - v["start"][0])
    n_segments.append(len(v["text"]))

total_duration_hours = sum(durations) / 3600
log(f"\nTotal lecture duration in subset: {total_duration_hours:.1f} hours")
log(f"Video duration (seconds): min={min(durations):.0f}, max={max(durations):.0f}, "
    f"mean={sum(durations)/len(durations):.0f}, median={sorted(durations)[len(durations)//2]:.0f}")
log(f"Caption segments per video: min={min(n_segments)}, max={max(n_segments)}, "
    f"mean={sum(n_segments)/len(n_segments):.1f}")

plt.figure(figsize=(8, 5))
plt.hist([d/60 for d in durations], bins=40, color="#55A868", edgecolor="white")
plt.xlabel("Video duration (minutes)")
plt.ylabel("Number of videos")
plt.title(f"Video Duration Distribution (n={len(videos)})")
plt.tight_layout()
plt.savefig(os.path.join(OUT, "02_video_duration_hist.png"), dpi=150)
plt.close()

# ---------- 3. Top concepts by frequency (raw vs. noise-filtered) ----------
def cid_name(cid):
    return cid.split("_", 1)[1].rsplit("_", 1)[0] if "_" in cid else cid

concept_video_counter = Counter(cid for cid, ccid in concept_video_pairs)
top_concepts_raw = concept_video_counter.most_common(20)
log("\nTop 20 concepts by raw frequency (includes generic short words):")
for cid, cnt in top_concepts_raw:
    log(f"  {cnt:4d}  {cid_name(cid)}")

concept_name_lengths = {cid: len(cid_name(cid)) for cid in concept_video_counter}
n_short = sum(1 for l in concept_name_lengths.values() if l <= 2)
log(f"\nConcepts with name length <=2 chars (generic/noisy): {n_short} "
    f"({n_short/len(concept_name_lengths)*100:.1f}% of {len(concept_name_lengths)} unique concepts in video links)")

# filtered: drop concept names of length <= 2 (removes generic single/double-char noise)
top_concepts_filtered = [(cid, cnt) for cid, cnt in concept_video_counter.most_common(200)
                          if len(cid_name(cid)) >= 3][:20]
log("\nTop 20 concepts after filtering out length<=2 generic terms (real CS topics):")
for cid, cnt in top_concepts_filtered:
    log(f"  {cnt:4d}  {cid_name(cid)}")

for tag, data, color, title in [
    ("03a_top_concepts_raw", top_concepts_raw, "#C44E52", "Top 20 Concepts - Raw Frequency (unfiltered)"),
    ("03b_top_concepts_filtered", top_concepts_filtered, "#4C956C", "Top 20 Concepts - After Removing Generic Short Terms"),
]:
    names_, counts_ = [], []
    for cid, cnt in reversed(data):
        names_.append(cid_name(cid))
        counts_.append(cnt)
    plt.figure(figsize=(9, 8))
    plt.barh(range(len(names_)), counts_, color=color)
    plt.yticks(range(len(names_)), names_, fontsize=9)
    plt.xlabel("Number of videos")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(os.path.join(OUT, f"{tag}.png"), dpi=150)
    plt.close()

# ---------- 4. Concept coverage per course ----------
concepts_per_course = defaultdict(set)
for cid, course_id in concept_course_pairs:
    concepts_per_course[course_id].add(cid)

cpc_values = sorted((len(v) for v in concepts_per_course.values()), reverse=True)
log(f"\nUnique concepts per course: min={min(cpc_values)}, max={max(cpc_values)}, "
    f"mean={sum(cpc_values)/len(cpc_values):.1f}")

plt.figure(figsize=(10, 5))
plt.bar(range(len(cpc_values)), cpc_values, color="#8172B2")
plt.xlabel("Course (ranked)")
plt.ylabel("Unique concept count")
plt.title("Concept Coverage per Course (CS Subset)")
plt.tight_layout()
plt.savefig(os.path.join(OUT, "04_concepts_per_course.png"), dpi=150)
plt.close()

# ---------- 5. Total unique concepts, overlap ----------
all_concepts_video = set(cid for cid, _ in concept_video_pairs)
all_concepts_course = set(cid for cid, _ in concept_course_pairs)
log(f"\nUnique concepts (video-linked): {len(all_concepts_video)}")
log(f"Unique concepts (course-linked): {len(all_concepts_course)}")
log(f"Overlap: {len(all_concepts_video & all_concepts_course)}")

with open(os.path.join(OUT, "eda_summary.txt"), "w", encoding="utf-8") as f:
    f.write("\n".join(stats_lines))

print("\nSaved charts + summary to:", OUT)

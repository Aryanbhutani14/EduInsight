"""Record manual relevance grades for the Module 5 candidates (round 2) into eda/module5_judgments.json.

Grade 2 = the lecture teaches the query topic; 1 = closely related or the right concept in another
context; 0 = not relevant. Grades are listed in the order the candidates appear in
eda/module5_candidates_BC.json at the time of judging. Judged by reading the video title and the first
~100 characters of the best-matching passage; the rank tags were visible, so judging was not blind.
"""
import json
import os

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CAND = os.path.join(BASE, "eda", "module5_candidates_BC.json")
JUDG = os.path.join(BASE, "eda", "module5_judgments.json")

GRADES = {
    "B04": [0, 0], "B05": [0, 0], "B07": [1], "B08": [1, 2, 0], "B09": [2], "B10": [0, 0, 0, 1, 0],
    "C01": [1, 1, 0, 0, 2, 1, 2, 0, 0, 0],
    "C02": [2, 2, 2, 2, 2, 1, 2, 2],
    "C03": [2] * 12,
    "C04": [2, 2, 2, 2, 2, 1, 1],
    "C05": [0, 1, 2, 0, 0, 1, 0, 2, 1, 1],
    "C06": [2, 2, 2, 2, 2, 1, 2],
    "C07": [0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 0, 0],
    "C08": [2, 2, 2, 2, 1, 1, 2, 2, 1],
    "C09": [2, 0, 2, 1, 2, 0, 1, 1, 1, 1, 1],
    "C10": [2, 2, 2, 2, 2, 1, 0, 2],
    "C11": [2, 1, 2, 0, 2, 2, 2, 1],
    "C12": [2, 2, 1, 1, 0, 0, 1, 1, 2, 2, 0],
}

cands = json.load(open(CAND, encoding="utf-8"))
judg = json.load(open(JUDG, encoding="utf-8"))
added = 0
for label, grades in GRADES.items():
    items = cands[label]["candidates"]
    assert len(items) == len(grades), (label, len(items), len(grades))
    q = judg["queries"].setdefault(label, {"query": cands[label]["query"], "round": 2, "grades": {}})
    for it, g in zip(items, grades):
        if it["ccid"] not in q["grades"]:
            q["grades"][it["ccid"]] = {"grade": g, "video": it["video"], "course": it["course"], "snippet": it["snippet"][:100]}
            added += 1
json.dump(judg, open(JUDG, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print("added", added, "grades; total", sum(len(v["grades"]) for v in judg["queries"].values()),
      "over", len(judg["queries"]), "queries")

"""Score the ranking methods on the manually judged query sets B (development) and C (held-out)."""
import json
import os
import sys

import numpy as np

sys.stdout.reconfigure(encoding="utf-8")
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
judg = json.load(open(os.path.join(BASE, "eda", "module5_judgments.json"), encoding="utf-8"))["queries"]
ranks = json.load(open(os.path.join(BASE, "eda", "module5_rankings_BC.json"), encoding="utf-8"))

METHODS = {"B": ["video_en", "chunk_en", "hybrid_en", "hybrid2_en"], "C": ["video_en", "chunk_en", "hybrid2_en"]}
rng = np.random.default_rng(42)


def score(label, method):
    top5 = ranks[label][method][:5]
    g = judg[label]["grades"]
    missing = [c for c in top5 if c not in g]
    assert not missing, (label, method, "unjudged items in top 5")
    grades = [g[c]["grade"] for c in top5]
    dcg = sum(gr / np.log2(i + 2) for i, gr in enumerate(grades))
    all_g = sorted((v["grade"] for v in g.values()), reverse=True)[:5]
    idcg = sum(gr / np.log2(i + 2) for i, gr in enumerate(all_g))
    return {"strict_p5": sum(x == 2 for x in grades) / 5, "lenient_p5": sum(x >= 1 for x in grades) / 5,
            "ndcg5": dcg / idcg if idcg else 0.0, "grades": grades}


result = {}
for setname in ("B", "C"):
    labels = sorted(k for k in ranks if k.startswith(setname))
    per = {m: {lab: score(lab, m) for lab in labels} for m in METHODS[setname]}
    summ = {}
    for m in METHODS[setname]:
        row = {}
        for met in ("strict_p5", "lenient_p5", "ndcg5"):
            v = np.array([per[m][lab][met] for lab in labels])
            boots = [v[rng.integers(0, len(v), len(v))].mean() for _ in range(2000)]
            row[met] = {"mean": round(float(v.mean()), 3),
                        "ci95": [round(float(np.percentile(boots, 2.5)), 3), round(float(np.percentile(boots, 97.5)), 3)]}
        summ[m] = row
    paired = {}
    for a, b in (("hybrid2_en", "chunk_en"), ("chunk_en", "video_en"), ("hybrid2_en", "video_en")):
        d = np.array([per[a][lab]["strict_p5"] - per[b][lab]["strict_p5"] for lab in labels])
        paired[f"{a} vs {b} (strict P@5)"] = {"wins": int((d > 0).sum()), "losses": int((d < 0).sum()), "ties": int((d == 0).sum())}
    result[setname] = {"queries": len(labels), "summary": summ, "paired": paired,
                       "per_query": {lab: {m: per[m][lab] for m in METHODS[setname]} for lab in labels}}

json.dump(result, open(os.path.join(BASE, "eda", "module5_judged_eval.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=1)
for setname, title in (("B", "SET B (development, 10 queries)"), ("C", "SET C (held-out, 12 queries)")):
    print(title)
    for m in METHODS[setname]:
        s = result[setname]["summary"][m]
        print(f"  {m:11s} strict P@5 {s['strict_p5']['mean']:.3f} {s['strict_p5']['ci95']}  "
              f"lenient P@5 {s['lenient_p5']['mean']:.3f} {s['lenient_p5']['ci95']}  graded nDCG@5 {s['ndcg5']['mean']:.3f}")
    print("  paired:", result[setname]["paired"])
print("per-query strict P@5 (video / chunk / hybrid2), set C:")
for lab, d in result["C"]["per_query"].items():
    print("  ", lab, ranks[lab]["query"][:40].ljust(40), d["video_en"]["strict_p5"], d["chunk_en"]["strict_p5"], d["hybrid2_en"]["strict_p5"])

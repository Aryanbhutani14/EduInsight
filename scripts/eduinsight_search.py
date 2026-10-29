"""EduInsight search engine: English or Chinese query in, ranked lecture videos (with scores and snippets) out.

    python eduinsight_search.py "linked list" --top 5

Method (the "hybrid2" ranking evaluated in Module 5):
  1. chunk similarity: the query is embedded with the same multilingual model as the lectures, and each
     video is scored by its best-matching caption chunk (cosine similarity);
  2. query translation: the query is matched against the ~26,000 Chinese concept names, and the nearest
     (up to 3, cosine >= 0.40) are turned into Chinese search terms;
  3. keyword search: BM25 over the lecture word lists using those terms;
  4. the two rankings are fused with reciprocal rank fusion (k = 60).
It ranks by content match only; nothing here says which teacher or lecture is "best".

Needs the files exported by scripts/spark_module5_rank.py (default folder ~/search_assets) and, for fast
query encoding, a GPU (it falls back to CPU).
"""
import argparse
import json
import os
import sys

import numpy as np

ASSETS = os.environ.get("EDUINSIGHT_ASSETS", os.path.expanduser("~/search_assets"))
MODEL = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"
TRANS_K, TRANS_TAU, RRF_K = 3, 0.40, 60
BM25_K1, BM25_B = 1.5, 0.75


class SearchEngine:
    def __init__(self, assets=ASSETS, device=None):
        import jieba
        import stopwordsiso
        import torch
        from sentence_transformers import SentenceTransformer

        jieba.setLogLevel(60)
        self._jieba = jieba
        self._stop = set(stopwordsiso.stopwords("zh")) | set(stopwordsiso.stopwords("en"))
        self.chunk_emb = np.load(os.path.join(assets, "chunk_emb.npy")).astype(np.float32)
        chunks = json.load(open(os.path.join(assets, "chunks.json"), encoding="utf-8"))
        self.chunk_ccid = np.array(chunks["ccid"])
        self.chunk_text = chunks["text"]
        meta = json.load(open(os.path.join(assets, "meta.json"), encoding="utf-8"))
        self.concept_names = meta["concept_names"]
        self.video_meta = meta["video_meta"]
        self.concept_emb = np.load(os.path.join(assets, "concept_emb.npy")).astype(np.float32)
        bm = json.load(open(os.path.join(assets, "bm25.json"), encoding="utf-8"))
        self.N, self.avgdl, self.dl, self.df, self.tf = bm["N"], bm["avgdl"], bm["dl"], bm["df"], bm["tf"]
        device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.model = SentenceTransformer(MODEL, device=device)
        if device == "cuda":
            self.model.half()
        # chunks are stored sorted by video, so each video is one contiguous block
        change = np.flatnonzero(self.chunk_ccid[1:] != self.chunk_ccid[:-1]) + 1
        self.starts = np.concatenate([[0], change])
        self.ends = np.concatenate([change, [len(self.chunk_ccid)]])
        self.ccids = self.chunk_ccid[self.starts]

    def _terms_of(self, text):
        toks = [w.strip().lower() for w in self._jieba.lcut(text)]
        toks = [w for w in toks if len(w) >= 2 and w not in self._stop]
        return toks or [text.strip().lower()]

    def translate(self, qvec):
        """Nearest Chinese concept names to the query: [(name, cosine)]."""
        sims = self.concept_emb @ qvec
        order = np.argsort(-sims)[:TRANS_K]
        return [(self.concept_names[i], float(sims[i])) for i in order if sims[i] >= TRANS_TAU]

    def _bm25(self, term_weights):
        scores = {}
        for term, w in term_weights.items():
            df = self.df.get(term)
            if not df:
                continue
            idf = np.log(1 + (self.N - df + 0.5) / (df + 0.5))
            for ccid, tfs in self.tf.items():
                f = tfs.get(term)
                if f:
                    s = w * idf * f * (BM25_K1 + 1) / (f + BM25_K1 * (1 - BM25_B + BM25_B * self.dl[ccid] / self.avgdl))
                    scores[ccid] = scores.get(ccid, 0.0) + s
        return scores

    def search(self, query, top=5):
        qvec = self.model.encode([query], normalize_embeddings=True, convert_to_numpy=True)[0].astype(np.float32)
        sims = self.chunk_emb @ qvec
        best_idx = np.array([s + int(np.argmax(sims[s:e])) for s, e in zip(self.starts, self.ends)])
        best_cos = sims[best_idx]
        chunk_order = np.argsort(-best_cos, kind="stable")
        chunk_rank = {self.ccids[i]: r for r, i in enumerate(chunk_order, 1)}

        translation = self.translate(qvec)
        term_w = {}
        for name, sim in translation:
            for t in set(self._terms_of(name)):
                term_w[t] = max(term_w.get(t, 0.0), sim)
        kw = self._bm25(term_w)
        kw_rank = {c: r for r, c in enumerate(sorted(kw, key=lambda c: -kw[c]), 1)}

        fused = np.array([1 / (RRF_K + chunk_rank[c]) + (1 / (RRF_K + kw_rank[c]) if c in kw_rank else 0.0)
                          for c in self.ccids])
        results = []
        for i in np.argsort(-fused, kind="stable")[:top]:
            c = self.ccids[i]
            m = self.video_meta.get(c, {})
            results.append({"rank": len(results) + 1, "ccid": c, "video": (m.get("name") or "").strip(),
                            "course": m.get("course"), "chunk_similarity": round(float(best_cos[i]), 3),
                            "keyword_match": c in kw_rank, "snippet": self.chunk_text[best_idx[i]][:200]})
        return {"query": query, "searched_concepts": [(n, round(s, 3)) for n, s in translation], "results": results}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("query")
    ap.add_argument("--top", type=int, default=5)
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    out = SearchEngine().search(args.query, args.top)
    print(f"Query: {out['query']}")
    print("Also searched for (Chinese concepts):",
          ", ".join(f"{n} ({s})" for n, s in out["searched_concepts"]) or "none above the similarity threshold")
    for r in out["results"]:
        print(f"\n#{r['rank']}  {r['video'] or '(untitled video)'}  |  {r['course']}")
        print(f"    chunk similarity {r['chunk_similarity']}   keyword match: {'yes' if r['keyword_match'] else 'no'}")
        print(f"    {r['snippet']}")


if __name__ == "__main__":
    main()

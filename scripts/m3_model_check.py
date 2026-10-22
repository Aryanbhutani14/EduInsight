import sys
import time

import torch
from sentence_transformers import SentenceTransformer, util

sys.stdout.reconfigure(encoding="utf-8")
NAME = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"

t = time.time()
m = SentenceTransformer(NAME, device="cuda")
print(f"model loaded in {time.time() - t:.1f}s on {torch.cuda.get_device_name(0)}")
print("max_seq_length:", m.max_seq_length, "| embedding dim:", m.get_sentence_embedding_dimension())

sample = "下面就进入这门课程的第一章第一节 我们首先来讨论所谓的计算 计算首先是我们这门课程的直接研究对象和内容"
ids = m.tokenizer(sample, add_special_tokens=False)["input_ids"]
print(f"sample caption: {len(sample)} characters -> {len(ids)} tokens")

pairs = [
    ("dynamic programming", "动态规划"),
    ("linked list", "链表"),
    ("operating system", "操作系统"),
    ("computer network", "计算机网络"),
    ("dynamic programming", "红烧肉的做法"),
    ("linked list", "古代诗词鉴赏"),
]
a = m.encode([p[0] for p in pairs], normalize_embeddings=True, convert_to_tensor=True)
b = m.encode([p[1] for p in pairs], normalize_embeddings=True, convert_to_tensor=True)
print("cross-lingual cosine similarity (English query vs Chinese text):")
for (en, zh), s in zip(pairs, (a * b).sum(dim=1).tolist()):
    print(f"  {s:.3f}  {en!r} vs {zh!r}")

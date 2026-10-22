#!/bin/bash
# Module 3 dependencies: PyTorch (CUDA wheel from PyPI), sentence-transformers, pandas, pyarrow
set -e
python3 -m pip install --user --upgrade pip 2>&1 | tail -1
python3 -m pip install --user torch sentence-transformers pandas pyarrow 2>&1 | tail -5
python3 - << 'EOF'
import torch, sentence_transformers, pandas, pyarrow
print("torch", torch.__version__, "| cuda available:", torch.cuda.is_available())
if torch.cuda.is_available():
    print("GPU:", torch.cuda.get_device_name(0), "| VRAM GB:", round(torch.cuda.get_device_properties(0).total_memory / 1e9, 1))
print("sentence_transformers", sentence_transformers.__version__, "| pandas", pandas.__version__, "| pyarrow", pyarrow.__version__)
EOF

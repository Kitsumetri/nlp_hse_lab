# NLP HSE Lab 2

## Preparations

Requirements:

- `python==3.10`
- `nvcc>=1.17` (for `flash-attn` library)

```bash
git clone https://github.com/Kitsumetri/nlp_hse_lab.git
cd nlp_hse_lab
python3 -m venv .venv
source .venv/bin/activate
pip install -U setuptools wheel pip
pip install -r requirements.txt
pip install flash-attn --no-build-isolation
```

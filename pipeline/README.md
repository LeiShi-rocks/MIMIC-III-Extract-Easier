# MIMIC-III Extraction Pipeline (Per-ICU Batched)

This repository contains a minimal, end-to-end workflow for converting raw MIMIC-III CSV dumps into the standard “MIMIC-Extract” formatted dataset (all_hourly_data.h5 and companion files). It merges two key ideas from upstream projects:

1. **DuckDB-based import** (from [MIMICExtractEasy](https://github.com/SphtKr/MIMICExtractEasy)): build the entire MIMIC-III relational schema + concepts using DuckDB—no PostgreSQL required.
2. **Per-ICU batched extraction** (our modification of [MIMIC-Extract](https://github.com/MLforHealth/MIMIC_Extract)): avoid giant SQL UNION queries by iterating ICU stays one at a time, which makes the pipeline scale to all 25k stays without hanging.

The final output matches the structure expected by tools such as PyHealth’s [`MIMICExtractDataset`](https://pyhealth.readthedocs.io/en/latest/api/datasets/pyhealth.datasets.MIMICExtractDataset.html).

---
## Directory layout

```
MIMICExtractEasy/
├─ data/                # place raw MIMIC-III CSVs here; build artifacts & outputs also land here
├─ pipeline/
│   ├─ mimic_direct_extract.py    # per-ICU extraction script
│   ├─ utils/                     # DuckDB build scripts imported from upstream
│   └─ resources/                 # schemas, item mapping, variable ranges (from upstream)
├─ LICENSE
└─ README.md
```

---
## Prerequisites

* PhysioNet credentialed access to MIMIC-III v1.4.
* Download the CSVs (either full or demo). Suppose they reside under `data/mimiciii/1.4/`.
* Python ≥ 3.10 on macOS/Linux/Windows.

We recommend performing all commands inside a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate   # Linux/macOS
# venv\Scripts\activate   # Windows PowerShell
pip install --upgrade pip
```

---
## Step 1 – Install requirements

```bash
pip install -r pipeline/utils/buildmimic/duckdb/requirements.txt
pip install -r pipeline/requirements.txt
```

Those files pull in DuckDB, pandas, tqdm, etc.

---
## Step 2 – Build the DuckDB database + concepts

```bash
DATA_DIR="$(pwd)/data"
MIMIC_DATA_DIR="$DATA_DIR/mimiciii/1.4"   # adjust to demo path if needed
DB_PATH="$DATA_DIR/mimic3.db"

python pipeline/utils/buildmimic/duckdb/import_duckdb.py \ 
    "$MIMIC_DATA_DIR" "$DB_PATH" --skip-indexes

python pipeline/utils/buildmimic/duckdb/import_duckdb.py \ 
    "$MIMIC_DATA_DIR" "$DB_PATH" --skip-tables --make-concepts
```

This loads all CSVs and creates the concept views (NIV durations, etc.) used by MIMIC-Extract.

---
## Step 3 – Run the per-ICU extraction

```bash
python pipeline/mimic_direct_extract.py \
    --duckdb_database "$DB_PATH" \
    --duckdb_schema main \
    --resource_path pipeline/resources \
    --plot_hist 0 \
    --extract_notes 0 \
    --out_path "$DATA_DIR"
```

Key features:
* queries each ICU stay separately to avoid SQL planner overload;
* normalizes integer columns before writing HDF5;
* writes the usual outputs (`all_hourly_data.h5`, `vitals_hourly_data.h5`, `C.h5`, etc.) under `data/`.

For a quick smoke test you can add `--pop_size 100`. Omitting it processes all 25,756 eligible stays.

Expected runtime on a modern laptop: ~4–6 hours for the full cohort (mostly CPU time spent executing the per-ICU queries).

---
## Step 4 – Analyze the results (optional)

Load the output with PyHealth:

```python
from pyhealth.datasets import MIMICExtractDataset

root = "./data"
dataset = MIMICExtractDataset(
    root=root,
    tables=["C", "vitals_labs", "interventions"],
    dev=True,
    refresh_cache=True,
    itemid_to_variable_map="pipeline/resources/itemid_to_variable_map.csv",
)
dataset.stat()
dataset.info()
```

---
## Credits

* [MIMIC-Extract](https://github.com/MLforHealth/MIMIC_Extract) for the original pipeline ([Wang et al., 2019](https://arxiv.org/abs/1907.08322)).
* [MIMICExtractEasy](https://github.com/SphtKr/MIMICExtractEasy) for DuckDB build scripts and modernized dependencies.
* [PyHealth](https://pyhealth.readthedocs.io/en/latest/api/datasets/pyhealth.datasets.MIMICExtractDataset.html) for convenient downstream analysis APIs.

Please cite the MIMIC-Extract publication if you use this workflow in your research.


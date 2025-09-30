Markdown
# MIMIC-III Extraction (Per-ICU Batched Pipeline)

This repository provides a minimal, end-to-end workflow for turning raw MIMIC-III CSV files into the standard MIMIC-Extract formatted dataset (`all_hourly_data.h5`, `vitals_hourly_data.h5`, `C.h5`, etc.). It combines:

- the DuckDB-based import scripts from **MIMICExtractEasy**, so you can build the database without PostgreSQL;
- our **per-ICU batched** version of `mimic_direct_extract.py`, which avoids the large UNION queries that previously caused the pipeline to hang when extracting the full cohort.

The resulting dataset is compatible with tooling such as PyHealth's [`MIMICExtractDataset`](https://pyhealth.readthedocs.io/en/latest/api/datasets/pyhealth.datasets.MIMICExtractDataset.html).

---
## Repository layout

```
MIMICExtractEasy/
├─ data/                       # raw MIMIC-III CSVs + generated DB + outputs
├─ pipeline/
│  ├─ mimic_direct_extract.py   # per-ICU extraction script
│  ├─ README.md                 # quick reference for pipeline usage
│  ├─ resources/                # schemas, itemID mapping, variable ranges
│  ├─ SQL_Queries/              # cohort + extraction SQL templates
│  └─ utils/
│      └─ buildmimic/duckdb/    # DuckDB database build scripts
├─ LICENSE
└─ README.md                    # (this file)
```

---
## Prerequisites

- PhysioNet credentialed access to MIMIC-III v1.4.
- A downloaded copy of the MIMIC-III CSV files (full or demo). Place them under `data/` (e.g., `data/mimiciii/1.4/`).
- Python ≥ 3.10 on macOS/Linux/Windows. A virtual environment is recommended:

  ```bash
  python3 -m venv venv
  source venv/bin/activate      # Windows: venv\Scripts\activate
  pip install --upgrade pip
  ```

---
## Step 1 – Install requirements

```bash
pip install -r pipeline/utils/buildmimic/duckdb/requirements.txt
pip install -r pipeline/requirements.txt
```

These pull in DuckDB, pandas, tqdm, etc.

---
## Step 2 – Build the DuckDB database

```bash
DATA_DIR="$(pwd)/data"
MIMIC_DATA_DIR="$DATA_DIR/mimiciii/1.4"   # adjust if using demo dataset
DB_PATH="$DATA_DIR/mimic3.db"

python pipeline/utils/buildmimic/duckdb/import_duckdb.py \
    "$MIMIC_DATA_DIR" "$DB_PATH" --skip-indexes

python pipeline/utils/buildmimic/duckdb/import_duckdb.py \
    "$MIMIC_DATA_DIR" "$DB_PATH" --skip-tables --make-concepts
```

This loads all CSVs into DuckDB and builds the concept views (NIV durations, etc.) required for extraction.

---
## Step 3 – Run the per-ICU extraction

```bash
python pipeline/mimic_direct_extract.py \
    --duckdb_database "$DB_PATH" \
    --duckdb_schema main \
    --resource_path pipeline/resources \
    --extract_notes 0 \
    --plot_hist 0 \
    --out_path "$DATA_DIR"
```

Highlights:

- ICU stays are processed one at a time (`per-ICU batching`), so DuckDB never sees huge `IN (...)` clauses and the job scales to all 25 756 stays.
- Integer columns are normalized before writing to HDF5 to avoid `Int32` dtype issues.
- Outputs appear directly under `data/` (e.g., `all_hourly_data.h5`, `static_data.csv`, `vitals_hourly_data.h5`).

Pass `--pop_size 100` for a smoke test; omit it to extract the full cohort (expect several hours on a laptop).

---
## Step 4 – Optional analysis with PyHealth

```python
from pyhealth.datasets import MIMICExtractDataset

dataset = MIMICExtractDataset(
    root="./data",
    tables=["C", "vitals_labs", "interventions"],
    dev=True,
    refresh_cache=True,
    itemid_to_variable_map="pipeline/resources/itemid_to_variable_map.csv",
)
print(dataset.stat())
```

---
## Credits

- [**MIMIC-Extract**](https://github.com/MLforHealth/MIMIC_Extract) for the original pipeline and data schema (Wang et al., 2019).
- [**MIMICExtractEasy**](https://github.com/SphtKr/MIMICExtractEasy) for DuckDB import scripts and modernized dependencies.
- [**PyHealth**](https://pyhealth.readthedocs.io/en/latest/api/datasets/pyhealth.datasets.MIMICExtractDataset.html) for convenient downstream analysis APIs.

Please cite the MIMIC-Extract publication if you use this workflow in your research.

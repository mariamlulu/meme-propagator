# MemeProp / Fingerprint Analysis — Full Analysis Toolkit

> Repository for fingerprint / meme-propagation analysis.  
> Includes SQL schema & analytics, DuckDB sample database, raw compressed dataset, Python analysis scripts, and final visual outputs.

---

## 🔎 Project overview

This repository contains everything needed to reproduce the meme/fingerprint analysis pipeline: schema → staging → analytics (SQL), data + DuckDB snapshot, and Python scripts to run the full analysis and plots.

The analysis was performed with a DuckDB-backed workflow and Python scripts that read the compressed dataset and produce CSVs and figures.

---

## 📁 Files in this repository

- `01_schema.sql`  
  SQL schema / DDL. Use this to create the database tables and initial structure.

- `02_staging.sql`  
  Staging SQL: ingest & transform raw data into canonical staging tables (cleaning, type conversions).

- `03_analytics.sql`  
  Analytics SQL: final aggregations, fingerprint / meme propagation metrics, and the views queried by the reporting scripts.

- `RS_2023-01.zst`  
  Raw dataset compressed with Zstandard (.zst). This is the raw input used by the pipeline (large). Place it in `data/raw/` before running the pipeline.

- `SQL_TECHNIQUES.md`  
  Notes on the SQL techniques used in the project (window functions, joins, CTE patterns, optimization tips).

- `analyze_fingerprints.py`  
  Python script to analyze fingerprint-level statistics and export summary CSVs.

- `create_timeline_plot.py`  
  Script to generate the timeline visualization for top fingerprints (exports PNG/PNG).

- `fingerprint_stats.csv`  
  Exported fingerprint statistics (CSV) — final or intermediate summary file.

- `image_mutations.csv`  
  CSV describing image mutation events detected in the analysis.

- `memeprop.duckdb`  
  DuckDB database snapshot. Contains staged and processed tables so you can run analytics locally without reprocessing the whole raw dataset.

- `run_full_analysis.py`  
  Orchestrator script that runs the entire pipeline end-to-end (decompress → ingest → process → analyze → export). Use this for a single command reproducible run.

- `show_mutations.py`  
  Small utility to inspect mutation records quickly (CLI friendly).

- `show_subreddit_analysis.py`  
  Quick reporting script for per-subreddit adoption / propagation stats.

- `subreddit_adoption_stats.csv`  
  CSV export summarizing adoption statistics by subreddit.

- `top_fingerprint_timeline.csv`  
  CSV used to build the timeline plot.

- `top_fingerprint_timeline.png`  
  Pre-built timeline PNG (visual output).

---

## ⚙️ Requirements & setup

> Recommended: Python 3.8+ and DuckDB

1. Create a virtual environment and install dependencies:

```bash
python -m venv venv
# macOS / Linux
source venv/bin/activate
# Windows
# venv\Scripts\activate

# If you have a requirements.txt:
pip install -r requirements.txt

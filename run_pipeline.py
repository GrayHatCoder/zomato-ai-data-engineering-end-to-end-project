"""
Run the whole pipeline locally - the same four tasks as the Airflow DAG
(airflow/dags/zomato_batch.py), but on a laptop with no cloud accounts:

    reload_raw  ->  dbt_build_core  ->  enrich_reviews  ->  dbt_build_ai
    (CSV lake -> DuckDB RAW)  (dbt build + tests)  (AI labels)  (AI mart)

Cloud version            Local version (this script)
---------------------    ---------------------------------------------
Amazon S3 bucket         data/lake/raw/<table>/  (same folder layout)
Snowflake ZOMATO db      warehouse/zomato.duckdb (RAW/STAGING/MARTS/AI)
COPY INTO                local/load_raw.py
dbt-snowflake            dbt-duckdb, SAME models (see zomato/macros/duckdb_compat.sql)
Airflow DAG              this script (Airflow still works for the cloud run)
OpenAI enrichment        OpenAI if OPENAI_API_KEY is set, else a free offline classifier

Usage:
    python run_pipeline.py              # generate data if missing, then run all 4 tasks
    python run_pipeline.py --fresh      # regenerate the dataset and rebuild everything
"""
import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY = sys.executable
DB_PATH = ROOT / "warehouse" / "zomato.duckdb"
LAKE = ROOT / "data" / "lake" / "raw"
DBT_PROJECT = ROOT / "zomato"
PROFILES = ROOT / "local" / "profiles"


def dbt_cmd():
    exe = Path(PY).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    return [str(exe)] if exe.exists() else [shutil.which("dbt") or "dbt"]


def run(task, cmd, cwd=ROOT):
    print(f"\n=== [{task}] {' '.join(Path(c).name if i == 0 else c for i, c in enumerate(cmd))}")
    t0 = time.time()
    env = {**os.environ, "DUCKDB_PATH": str(DB_PATH), "PYTHONIOENCODING": "utf-8"}
    result = subprocess.run(cmd, cwd=cwd, env=env)
    if result.returncode != 0:
        sys.exit(f"\n[{task}] FAILED (exit {result.returncode}) - pipeline stopped.")
    print(f"=== [{task}] done in {time.time() - t0:.1f}s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fresh", action="store_true", help="regenerate data and rebuild the warehouse")
    ap.add_argument("--orders", default="50000", help="orders to generate (with --fresh or first run)")
    args = ap.parse_args()

    if args.fresh and DB_PATH.exists():
        DB_PATH.unlink()
    if args.fresh or not (LAKE / "orders").exists():
        run("generate_data", [PY, "local/generate_data.py", "--orders", args.orders])

    dbt = dbt_cmd()
    dbt_args = ["--project-dir", str(DBT_PROJECT), "--profiles-dir", str(PROFILES)]
    started = time.time()
    run("reload_raw", [PY, "local/load_raw.py"])
    run("dbt_build_core", dbt + ["build", "--exclude", "tag:ai"] + dbt_args, cwd=DBT_PROJECT)
    run("enrich_reviews", [PY, "local/enrich_reviews.py"])
    run("dbt_build_ai", dbt + ["build", "--select", "tag:ai"] + dbt_args, cwd=DBT_PROJECT)
    print(f"\nPipeline finished in {time.time() - started:.1f}s. Warehouse: {DB_PATH}")
    print("Open the app:  streamlit run app/streamlit_app.py")


if __name__ == "__main__":
    main()

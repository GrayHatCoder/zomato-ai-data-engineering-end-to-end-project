"""
Bronze load — the local twin of snowflake/04_raw_tables.sql + 05_copy_into.sql.

Reads every CSV from the local lake (data/lake/raw/<table>/) into the RAW schema
of a DuckDB warehouse file (warehouse/zomato.duckdb). Column order and types
mirror the Snowflake RAW DDL exactly:
  - dimension files are loaded as text (with the throwaway `_idx` column) so the
    dbt staging layer does the cleaning, same as in Snowflake
  - fact files are loaded typed

Usage:
    python local/load_raw.py
"""
import os
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
LAKE = ROOT / "data" / "lake" / "raw"
DB_PATH = Path(os.getenv("DUCKDB_PATH", ROOT / "warehouse" / "zomato.duckdb"))

# column name -> DuckDB type, in CSV order (matches snowflake/04_raw_tables.sql)
TABLES = {
    "restaurants": {c: "VARCHAR" for c in ["_idx", "id", "name", "city", "rating", "rating_count", "cost",
                                            "cuisine", "lic_no", "link", "address", "menu"]},
    "users": {c: "VARCHAR" for c in ["_idx", "user_id", "name", "email", "password", "age", "gender",
                                      "marital_status", "occupation", "monthly_income", "education",
                                      "family_size"]},
    "food": {c: "VARCHAR" for c in ["_idx", "f_id", "item", "veg_or_non_veg"]},
    "menu": {c: "VARCHAR" for c in ["_idx", "menu_id", "r_id", "f_id", "cuisine", "price"]},
    "orders": {
        "order_id": "BIGINT", "order_timestamp": "TIMESTAMP", "order_date": "DATE", "user_id": "BIGINT",
        "r_id": "BIGINT", "restaurant_city": "VARCHAR", "cuisine": "VARCHAR", "items_count": "BIGINT",
        "sales_qty": "BIGINT", "subtotal": "BIGINT", "discount": "BIGINT", "delivery_fee": "BIGINT",
        "gst": "BIGINT", "sales_amount": "BIGINT", "currency": "VARCHAR", "payment_method": "VARCHAR",
        "order_status": "VARCHAR", "customer_rating": "BIGINT", "delivery_time_min": "BIGINT",
    },
    "order_items": {
        "order_item_id": "BIGINT", "order_id": "BIGINT", "r_id": "BIGINT", "f_id": "VARCHAR",
        "price": "BIGINT", "quantity": "BIGINT", "line_amount": "BIGINT",
    },
    "reviews": {
        "review_id": "BIGINT", "order_id": "BIGINT", "user_id": "BIGINT", "restaurant_id": "BIGINT",
        "rating": "BIGINT", "comment": "VARCHAR", "review_date": "DATE",
    },
}
DIMENSIONS = {"restaurants", "users", "food", "menu"}   # messy source -> skip bad rows (ON_ERROR=CONTINUE)


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    print(f"Loading RAW tables into {DB_PATH}")
    for table, cols in TABLES.items():
        folder = LAKE / table
        files = sorted(folder.glob("*.csv"))
        if not files:
            raise SystemExit(f"No CSV found in {folder} - run `python local/generate_data.py` first.")
        t0 = time.time()
        col_spec = "{" + ", ".join(f"'{c}': '{t}'" for c, t in cols.items()) + "}"
        lenient = ", ignore_errors=true, null_padding=true" if table in DIMENSIONS else ""
        glob = (folder / "*.csv").as_posix()
        con.execute(f"""
            CREATE OR REPLACE TABLE raw.{table} AS
            SELECT * FROM read_csv('{glob}', header=true, delim=',', quote='"',
                                   nullstr=['', '\\N', 'NULL'], columns={col_spec}{lenient})
        """)
        n = con.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
        print(f"  raw.{table:<12} {n:>9,} rows  ({time.time() - t0:.1f}s)")
    con.close()


if __name__ == "__main__":
    main()

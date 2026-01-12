"""
duckdb_loader.py

Purpose:
--------
Creates and manages the DuckDB analytical layer.

Responsibilities:
- initialize DuckDB database
- load enriched parquet tables
- expose safe SQL execution function
- serve as the ONLY data access layer for agents
"""

import os
import pandas as pd
from .connection import get_connection


# -----------------------------
# Configuration
# -----------------------------

DATA_PATH = "data/processed"

TABLES = {
    "fact_sales": "fact_sales_enriched.parquet",
    "dim_product": "dim_product_enriched.parquet",
    "finance_summary": "finance_summary.parquet"
}


# -----------------------------
# Database setup
# -----------------------------

def initialize_database():
    """
    Load parquet tables into DuckDB.
    This can be rerun safely (tables replaced).
    """

    print("[INFO] Initializing DuckDB warehouse...")

    con = get_connection()

    for table, file in TABLES.items():

        file_path = os.path.join(DATA_PATH, file)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Missing parquet file: {file_path}")

        print(f"[INFO] Loading {table} from {file}")

        con.execute(f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT * FROM read_parquet('{file_path}')
        """)

    con.close()

    print("[OK] DuckDB initialized successfully.")


# -----------------------------
# Query execution layer
# -----------------------------

def run_query(sql: str) -> pd.DataFrame:
    """
    Execute SQL safely against DuckDB.

    This is the ONLY function agents should call.
    """

    con = get_connection(read_only=True)

    try:
        df = con.execute(sql).fetch_df()
        return df
    finally:
        con.close()


# -----------------------------
# Quick health check
# -----------------------------

def health_check():
    """Run sanity queries to confirm system readiness."""

    con = get_connection(read_only=True)

    print("\n[HEALTH CHECK] Tables available:")
    print(con.execute("SHOW TABLES").fetch_df())

    print("\n[HEALTH CHECK] fact_sales sample:")
    print(con.execute("SELECT * FROM fact_sales LIMIT 5").fetch_df())

    print("\n[HEALTH CHECK] Revenue check:")
    print(con.execute("SELECT SUM(revenue) AS total_revenue FROM fact_sales").fetch_df())

    con.close()


# -----------------------------
# Entry point
# -----------------------------

if __name__ == "__main__":
    initialize_database()
    health_check()

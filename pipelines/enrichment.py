"""
enrichment.py

Purpose:
--------
Add business-friendly derived features to canonical tables.
These features make downstream analytics and agents more reliable.

Input:
- data/processed/fact_sales.parquet
- data/processed/dim_product.parquet

Output:
- data/processed/fact_sales_enriched.parquet
- data/processed/dim_product_enriched.parquet
"""

import os
import pandas as pd

PROCESSED_PATH = "data/processed"


# -----------------------------
# Helper functions
# -----------------------------

def normalize_region(val):
    if pd.isna(val):
        return "unknown"
    val = str(val).lower()
    if "india" in val or val == "domestic":
        return "domestic"
    return "international"


def normalize_category(val):
    if pd.isna(val):
        return "unknown"
    return str(val).strip().lower()


# -----------------------------
# Enrichment logic
# -----------------------------

def enrich_fact_sales():

    print("[INFO] Enriching fact_sales...")

    fact_sales = pd.read_parquet(os.path.join(PROCESSED_PATH, "fact_sales.parquet"))

    # normalize category
    fact_sales["category_clean"] = fact_sales["category"].apply(normalize_category)

    # normalize region
    fact_sales["region_clean"] = fact_sales["region"].apply(normalize_region)

    # business flags
    fact_sales["is_international"] = fact_sales["region_clean"] == "international"
    fact_sales["is_high_value_order"] = fact_sales["revenue"] > 5000

    # time labels
    fact_sales["year_month"] = fact_sales["order_date"].dt.to_period("M").astype(str)
    fact_sales["year_quarter"] = fact_sales["order_date"].dt.to_period("Q").astype(str)

    output_path = os.path.join(PROCESSED_PATH, "fact_sales_enriched.parquet")
    fact_sales.to_parquet(output_path, index=False)

    print("[OK] fact_sales enriched and saved.")


def enrich_dim_product():

    print("[INFO] Enriching dim_product...")

    dim_product = pd.read_parquet(os.path.join(PROCESSED_PATH, "dim_product.parquet"))

    # normalize category
    if "category" in dim_product.columns:
        dim_product["category_clean"] = dim_product["category"].apply(normalize_category)

    # stock flags
    if "current_stock" in dim_product.columns:
        dim_product["is_low_stock"] = dim_product["current_stock"] < 10

    output_path = os.path.join(PROCESSED_PATH, "dim_product_enriched.parquet")
    dim_product.to_parquet(output_path, index=False)

    print("[OK] dim_product enriched and saved.")


# -----------------------------
# Runner
# -----------------------------

if __name__ == "__main__":
    enrich_fact_sales()
    enrich_dim_product()
    print("\n[INFO] Enrichment completed successfully.")

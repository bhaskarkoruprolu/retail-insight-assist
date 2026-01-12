"""
transformations.py

Purpose:
--------
Create canonical retail tables from standardized datasets.

Outputs:
- fact_sales.parquet
- dim_product.parquet
- finance_summary.parquet

This is the heart of the analytical data foundation.
"""

import os
import pandas as pd

STAGING_PATH = "data/staging"
PROCESSED_PATH = "data/processed"

os.makedirs(PROCESSED_PATH, exist_ok=True)


# -----------------------------
# Utility helpers
# -----------------------------

def add_time_features(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Add year, month, quarter columns from a date field."""
    df["year"] = df[date_col].dt.year
    df["month"] = df[date_col].dt.month
    df["quarter"] = df[date_col].dt.to_period("Q").astype(str)
    return df


def enforce_fact_sales_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Hard enforcement of fact_sales schema.
    This guarantees analytics-safe downstream queries.
    """

    # ---- identifiers / dimensions ----
    dim_cols = [
        "order_id", "sku", "style", "category", "currency",
        "region", "country", "state", "city", "sales_channel",
        "fulfillment_type", "order_status"
    ]

    for col in dim_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # ---- metrics ----
    if "revenue" in df.columns:
        df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")

    if "units" in df.columns:
        df["units"] = pd.to_numeric(df["units"], errors="coerce").astype("Int64")

    # ---- dates ----
    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    # ---- flags ----
    if "is_cancelled" in df.columns:
        df["is_cancelled"] = df["is_cancelled"].astype(bool)

    if "is_b2b" in df.columns:
        df["is_b2b"] = df["is_b2b"].astype("boolean")

    # ---- data quality gate ----
    df = df[df["revenue"].notna()]
    df = df[df["units"].notna()]
    df = df[df["order_date"].notna()]

    return df


# -----------------------------
# FACT SALES
# -----------------------------

def build_fact_sales() -> pd.DataFrame:
    """Build unified fact_sales table from domestic and international sales."""

    print("[INFO] Building fact_sales...")

    # ---- Amazon domestic sales ----
    amazon = pd.read_csv(os.path.join(STAGING_PATH, "Amazon Sale Report.csv"))

    amazon_fact = pd.DataFrame({
        "order_id": amazon.get("order_id"),
        "order_date": pd.to_datetime(amazon.get("date"), errors="coerce"),
        "sku": amazon.get("sku"),
        "style": amazon.get("style"),
        "category": amazon.get("category"),
        "units": 1,
        "revenue": amazon.get("amount"),
        "currency": amazon.get("currency"),
        "region": "domestic",
        "country": amazon.get("ship_country"),
        "state": amazon.get("ship_state"),
        "city": amazon.get("ship_city"),
        "sales_channel": amazon.get("sales_channel"),
        "fulfillment_type": amazon.get("fulfilment"),
        "order_status": amazon.get("status"),
        "is_b2b": amazon.get("b2b")
    })

    amazon_fact["is_cancelled"] = amazon_fact["order_status"].str.contains("cancel", case=False, na=False)
    amazon_fact = add_time_features(amazon_fact, "order_date")

    # ---- International sales ----
    intl = pd.read_csv(os.path.join(STAGING_PATH, "International sale Report.csv"))

    intl_fact = pd.DataFrame({
        "order_id": None,
        "order_date": pd.to_datetime(intl.get("date"), errors="coerce"),
        "sku": intl.get("sku"),
        "style": intl.get("style"),
        "category": None,
        "units": intl.get("pcs"),
        "revenue": intl.get("gross_amt"),
        "currency": None,
        "region": "international",
        "country": None,
        "state": None,
        "city": None,
        "sales_channel": "international",
        "fulfillment_type": None,
        "order_status": "completed",
        "is_b2b": None
    })

    intl_fact["is_cancelled"] = False
    intl_fact = add_time_features(intl_fact, "order_date")

    # ---- Union & schema enforcement ----
    fact_sales = pd.concat([amazon_fact, intl_fact], ignore_index=True)
    fact_sales = enforce_fact_sales_schema(fact_sales)

    return fact_sales


# -----------------------------
# DIM PRODUCT
# -----------------------------

def build_dim_product() -> pd.DataFrame:
    """Build unified product dimension."""

    print("[INFO] Building dim_product...")

    product = pd.read_csv(os.path.join(STAGING_PATH, "Sale Report.csv"))
    pricing_may = pd.read_csv(os.path.join(STAGING_PATH, "May-2022.csv"))
    pricing_pl = pd.read_csv(os.path.join(STAGING_PATH, "P  L March 2021.csv"))

    product_dim = pd.DataFrame({
        "sku": product.get("sku_code").astype(str),
        "style": product.get("design_no").astype(str),
        "category": product.get("category").astype(str),
        "size": product.get("size").astype(str),
        "color": product.get("color").astype(str),
        "current_stock": pd.to_numeric(product.get("stock"), errors="coerce")
    })

    if "sku" in pricing_may.columns:
        product_dim = product_dim.merge(pricing_may, how="left", on="sku")

    if "sku" in pricing_pl.columns:
        product_dim = product_dim.merge(pricing_pl, how="left", on="sku")

    product_dim = product_dim.drop_duplicates(subset=["sku"])

    return product_dim


# -----------------------------
# FINANCE SUMMARY 
# -----------------------------

def build_fact_finance() -> pd.DataFrame:
    """
    Build finance summary table.

    Modeled as a finance snapshot (not transactional fact).
    """

    print("[INFO] Building finance summary table...")

    expense = pd.read_csv(os.path.join(STAGING_PATH, "Expense IIGF.csv"))
    expense.columns = [c.strip().lower() for c in expense.columns]

    if "recived_amount" not in expense.columns or "expance" not in expense.columns:
        raise ValueError("Finance file missing expected columns: recived_amount, expance")

    finance_summary = pd.DataFrame({
        "snapshot_date": pd.Timestamp.today().normalize(),
        "received_amount": pd.to_numeric(expense["recived_amount"], errors="coerce"),
        "expense_amount": pd.to_numeric(expense["expance"], errors="coerce")
    })

    finance_summary = finance_summary.dropna()

    return finance_summary


# -----------------------------
# Runner
# -----------------------------

def run_transformations():
    fact_sales = build_fact_sales()
    dim_product = build_dim_product()
    finance_summary = build_fact_finance()

    fact_sales.to_parquet(os.path.join(PROCESSED_PATH, "fact_sales.parquet"), index=False)
    dim_product.to_parquet(os.path.join(PROCESSED_PATH, "dim_product.parquet"), index=False)
    finance_summary.to_parquet(os.path.join(PROCESSED_PATH, "finance_summary.parquet"), index=False)

    print("\n[INFO] Canonical tables created in data/processed/")


if __name__ == "__main__":
    run_transformations()

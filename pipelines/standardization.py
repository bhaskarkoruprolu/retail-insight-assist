"""
standardization.py

Purpose:
--------
Standardize raw datasets into clean, schema-stable CSVs.

Operations performed:
- normalize column names
- drop unnamed columns
- trim and normalize strings
- parse date columns
- enforce numeric types (CRITICAL)
- remove obvious junk values

This script does NOT create canonical tables.
It only prepares consistent staging datasets.
"""

import os
import re
import pandas as pd


# -----------------------------
# Configuration
# -----------------------------

RAW_PATH = "data/raw"
STAGING_PATH = "data/staging"

os.makedirs(STAGING_PATH, exist_ok=True)


# -----------------------------
# Helper functions
# -----------------------------

def normalize_column_name(col: str) -> str:
    """
    Convert column names into machine-friendly format:
    - lowercase
    - replace spaces and symbols with underscores
    - remove duplicate underscores
    """
    col = col.strip().lower()
    col = re.sub(r"[^\w]+", "_", col)
    col = re.sub(r"_+", "_", col)
    return col.strip("_")


def basic_string_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """Strip spaces from all string columns."""
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()
    return df


def remove_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns like 'Unnamed: 0'."""
    unnamed_cols = [c for c in df.columns if "unnamed" in c.lower()]
    return df.drop(columns=unnamed_cols, errors="ignore")


def try_parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Attempt to parse columns that look like dates."""
    for col in df.columns:
        if any(x in col.lower() for x in ["date", "time", "day"]):
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                pass
    return df


def try_parse_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Generic numeric inference (fallback)."""
    for col in df.columns:
        if any(x in col.lower() for x in ["amt", "amount", "price", "rate", "cost", "qty", "pcs", "stock"]):
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# -----------------------------
# Retail schema enforcement (NEW - CRITICAL)
# -----------------------------

def enforce_retail_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforce canonical retail data types.
    This guarantees analytics-safe downstream SQL.
    """

    # ---- Numeric metrics ----
    for col in ["revenue", "sales", "amount", "total_amount", "price"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["units", "quantity", "qty"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # ---- Identifiers (never numeric) ----
    for col in ["order_id", "product_id", "customer_id", "store_id"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # ---- Dimensions ----
    for col in ["category", "subcategory", "region", "country", "state", "channel"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # ---- Date fields ----
    for col in ["order_date", "purchase_date", "transaction_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # ---- Data quality filters ----
    if "revenue" in df.columns:
        df = df[df["revenue"].notna()]

    if "units" in df.columns:
        df = df[df["units"].notna()]

    return df


# -----------------------------
# Core standardization logic
# -----------------------------

def standardize_file(file_name: str):
    """Standardize a single raw CSV and save to staging."""

    print(f"[INFO] Standardizing: {file_name}")

    input_path = os.path.join(RAW_PATH, file_name)
    output_path = os.path.join(STAGING_PATH, file_name)

    df = pd.read_csv(input_path)

    # Normalize column names
    df.columns = [normalize_column_name(c) for c in df.columns]

    # Drop unnamed columns
    df = remove_unnamed_columns(df)

    # Clean string values
    df = basic_string_cleanup(df)

    # Parse dates (heuristic)
    df = try_parse_dates(df)

    # Parse numeric fields (heuristic)
    df = try_parse_numeric(df)

    # Enforce retail schema (hard rules)
    df = enforce_retail_schema(df)

    # Save standardized dataset
    df.to_csv(output_path, index=False)

    print(f"[OK] Saved standardized file to: {output_path}")


def run_standardization():
    """Run standardization on all raw CSV files."""

    for file_name in os.listdir(RAW_PATH):
        if file_name.lower().endswith(".csv"):
            standardize_file(file_name)

    print("\n[INFO] Standardization completed.")


# -----------------------------
# Entry point
# -----------------------------

if __name__ == "__main__":
    run_standardization()

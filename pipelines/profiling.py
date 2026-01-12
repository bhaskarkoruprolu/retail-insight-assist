"""
profiling.py

Purpose:
--------
Generate data quality and statistics profiles for canonical tables.
These reports will support validation agent and monitoring.

Input:
- data/processed/*.parquet

Output:
- data/profiles/*.json
"""

import os
import json
import pandas as pd

PROCESSED_PATH = "data/processed"
PROFILE_PATH = "data/profiles"

os.makedirs(PROFILE_PATH, exist_ok=True)


# -----------------------------
# Profiling functions
# -----------------------------

def profile_dataframe(df: pd.DataFrame, table_name: str) -> dict:
    profile = {
        "table": table_name,
        "row_count": int(len(df)),
        "column_count": int(df.shape[1]),
        "null_percentage": {},
        "numeric_summary": {},
        "top_values": {}
    }

    # null stats
    for col in df.columns:
        profile["null_percentage"][col] = float(df[col].isna().mean())

    # numeric stats
    num_df = df.select_dtypes(include="number")
    for col in num_df.columns:
        profile["numeric_summary"][col] = {
            "min": float(num_df[col].min()),
            "max": float(num_df[col].max()),
            "mean": float(num_df[col].mean())
        }

    # top categorical values
    cat_df = df.select_dtypes(include="object")
    for col in cat_df.columns:
        profile["top_values"][col] = cat_df[col].value_counts().head(5).to_dict()

    return profile


def run_profiling():

    for file in os.listdir(PROCESSED_PATH):

        if file.endswith(".parquet"):

            table_name = file.replace(".parquet", "")
            print(f"[INFO] Profiling {table_name}")

            df = pd.read_parquet(os.path.join(PROCESSED_PATH, file))
            profile = profile_dataframe(df, table_name)

            output_file = os.path.join(PROFILE_PATH, f"{table_name}_profile.json")
            with open(output_file, "w") as f:
                json.dump(profile, f, indent=4)

            print(f"[OK] Profile saved: {output_file}")


# -----------------------------
# Runner
# -----------------------------

if __name__ == "__main__":
    run_profiling()
    print("\n[INFO] Data profiling completed.")

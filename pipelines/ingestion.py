
#ingestion.py

#Purpose:
#--------
#Raw data ingestion script.
#- Loads original CSV files
#- Saves untouched copies into data/raw/
#- Generates an ingestion audit report (JSON)

#This script does NOT clean or transform data.
#It only establishes a trusted raw layer and metadata audit.


import os
import json
import shutil
from datetime import datetime

import pandas as pd


# -----------------------------
# Configuration
# -----------------------------

RAW_INPUT_PATH = "C:\\Users\\india\\Desktop\\Blend_360_Project_files\\Blend 360 Asmnt_DataSet"   # put your original CSVs here
RAW_OUTPUT_PATH = "data/raw"            # untouched copies stored here
REPORT_PATH = "data/raw/ingestion_report.json"


# -----------------------------
# Utility functions
# -----------------------------

def ensure_directories():
    """Create required folders if they don't exist."""
    os.makedirs(RAW_INPUT_PATH, exist_ok=True)
    os.makedirs(RAW_OUTPUT_PATH, exist_ok=True)


def profile_dataframe(df: pd.DataFrame) -> dict:
    """
    Generate basic profiling info for a dataframe.
    This is lightweight and safe for raw ingestion.
    """
    profile = {
        "rows": int(df.shape[0]),
        "columns": int(df.shape[1]),
        "column_names": list(df.columns),
        "null_percent": {
            col: float(df[col].isna().mean()) for col in df.columns
        }
    }
    return profile


# -----------------------------
# Main ingestion logic
# -----------------------------

def ingest_files():
    """
    Main function:
    - scans RAW_INPUT_PATH
    - copies files to RAW_OUTPUT_PATH
    - loads CSVs and builds ingestion report
    """

    ingestion_report = {
        "ingestion_time": datetime.utcnow().isoformat(),
        "files": {}
    }

    for file_name in os.listdir(RAW_INPUT_PATH):

        if not file_name.lower().endswith(".csv"):
            continue

        source_file = os.path.join(RAW_INPUT_PATH, file_name)
        target_file = os.path.join(RAW_OUTPUT_PATH, file_name)

        print(f"[INFO] Ingesting file: {file_name}")

        # Copy file exactly as-is (no modification)
        shutil.copy2(source_file, target_file)

        # Load CSV for profiling only
        df = pd.read_csv(source_file)

        file_profile = profile_dataframe(df)

        file_profile["file_size_mb"] = round(
            os.path.getsize(source_file) / (1024 * 1024), 2
        )

        ingestion_report["files"][file_name] = file_profile

    # Save ingestion audit report
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(ingestion_report, f, indent=4)

    print("\n[INFO] Ingestion completed successfully.")
    print(f"[INFO] Audit report saved to: {REPORT_PATH}")


# -----------------------------
# Entry point
# -----------------------------

if __name__ == "__main__":
    ensure_directories()
    ingest_files()

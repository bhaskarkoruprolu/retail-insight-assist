"""
run_pipeline.py

Purpose:
--------
Single entry point to run the full retail data pipeline.

Execution order:
1. Raw ingestion
2. Standardization
3. Canonical transformations
4. Enrichment
5. Data profiling

If any step fails, the pipeline stops.
"""

import sys

# Import pipeline steps
from ingestion import ingest_files, ensure_directories
from standardization import run_standardization
from transformations import run_transformations
from enrichment import enrich_fact_sales, enrich_dim_product
from profiling import run_profiling


def run_step(step_name, func):
    print(f"\n========== {step_name} ==========")
    try:
        func()
        print(f"[SUCCESS] {step_name} completed.")
    except Exception as e:
        print(f"[FAILED] {step_name}")
        print("Error:", str(e))
        sys.exit(1)


def main():

    run_step("RAW INGESTION", lambda: (ensure_directories(), ingest_files()))
    run_step("STANDARDIZATION", run_standardization)
    run_step("TRANSFORMATIONS", run_transformations)
    run_step("ENRICHMENT", lambda: (enrich_fact_sales(), enrich_dim_product()))
    run_step("PROFILING", run_profiling)

    print("\n🎯 FULL DATA PIPELINE COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()

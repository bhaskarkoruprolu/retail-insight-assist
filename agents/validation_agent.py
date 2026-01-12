"""
validation_agent.py

Purpose:
--------
Validate queries and query results before they reach any LLM summarization.

This agent:
- blocks unsafe SQL patterns (pre-execution)
- checks emptiness and schema alignment
- enforces business sanity rules
- detects suspicious analytics
- decides whether to continue, warn, or block

It NEVER generates insights.
"""

import yaml
import pandas as pd
import re
from typing import Dict, Any


# -----------------------------
# Load business rules
# -----------------------------

def load_business_rules(path: str = "schema/business_rules.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


BUSINESS_RULES = load_business_rules()


# -----------------------------
# SQL Guardrails (STEP 4 - CRITICAL)
# -----------------------------

FORBIDDEN_SUM_COLUMNS = [
    "order_id", "sku", "style", "category",
    "region", "country", "state", "city",
    "sales_channel", "currency"
]


def validate_sql(sql: str):
    """
    Block analytics-unsafe SQL before execution.
    """

    sql_lower = sql.lower()

    # ---- block SUM on non-numeric columns ----
    for col in FORBIDDEN_SUM_COLUMNS:
        pattern = rf"sum\s*\(\s*{col}\s*\)"
        if re.search(pattern, sql_lower):
            raise ValueError(f"Unsafe aggregation detected: SUM({col}) is not allowed")

    # ---- block SELECT * ----
    if "select *" in sql_lower:
        raise ValueError("Wildcard SELECT is not allowed in analytics queries")

    # ---- block cartesian products ----
    if " join " in sql_lower and " on " not in sql_lower:
        raise ValueError("JOIN without ON clause detected")

    # ---- basic SQL injection / corruption patterns ----
    forbidden_tokens = [";--", "drop table", "delete from", "truncate table"]
    for token in forbidden_tokens:
        if token in sql_lower:
            raise ValueError("Potentially destructive SQL detected")


# -----------------------------
# Core result validation logic
# -----------------------------

def validate_result(routed_intent: Dict[str, Any], data_output: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate query result against business and sanity rules.

    Returns:
    {
        status: pass | warn | block,
        issues: [ ... ],
        row_count: int
    }
    """

    issues = []
    status = "pass"

    df: pd.DataFrame = data_output["result"]
    row_count = data_output["row_count"]

    # -----------------------------
    # 1. Empty or null-heavy result
    # -----------------------------

    if df.empty:
        return {
            "status": "block",
            "issues": ["Query returned no data"],
            "row_count": 0
        }

    null_ratio = df.isna().mean().mean()
    if null_ratio > 0.5:
        issues.append("Result contains more than 50% null values")
        status = "warn"

    # -----------------------------
    # 2. Row explosion protection
    # -----------------------------

    max_rows = BUSINESS_RULES["validation"]["max_rows_returned"]

    if row_count > max_rows:
        issues.append(f"Too many rows returned: {row_count} (limit: {max_rows})")
        status = "warn"

    # -----------------------------
    # 3. Metric sanity checks
    # -----------------------------

    sanity_limits = BUSINESS_RULES["validation"]["sanity_limits"]

    for col in df.select_dtypes(include="number").columns:

        min_val = df[col].min()
        max_val = df[col].max()

        if "growth" in col.lower():
            if min_val < sanity_limits["revenue_growth_min"] or max_val > sanity_limits["revenue_growth_max"]:
                issues.append(f"Growth values out of sane range in {col}")
                status = "warn"

        if "cancellation" in col.lower():
            if max_val > sanity_limits["cancellation_rate_max"]:
                issues.append("Cancellation rate exceeds 100%")
                status = "block"

    # -----------------------------
    # 4. Intent-result consistency
    # -----------------------------

    expected_dims = routed_intent.get("dimensions", [])
    for dim in expected_dims:
        if dim not in df.columns:
            issues.append(f"Expected dimension missing from result: {dim}")
            status = "block"

    expected_metrics = routed_intent.get("metrics", [])

    # Metric outputs are renamed in Data Agent (total_revenue, total_orders, etc.)
    metric_column_map = {
        "revenue": "total_revenue",
        "units": "total_units",
        "orders": "total_orders"
    }

    for metric in expected_metrics:
        expected_col = metric_column_map.get(metric, metric)
        if expected_col not in df.columns:
            issues.append(f"Expected metric missing from result: {expected_col}")
            status = "block"

    # -----------------------------
    # 5. Business risk flags
    # -----------------------------

    if "total_revenue" in df.columns:
        if df["total_revenue"].sum() <= 0:
            issues.append("Total revenue is zero or negative")
            status = "warn"

    # -----------------------------
    # 6. Data quality checks
    # -----------------------------

    if "category" in df.columns:
        unknown_ratio = (
            df["category"].isna().sum() +
            (df["category"].astype(str).str.lower() == "unknown").sum()
        ) / len(df)

        if unknown_ratio > 0.2:
            issues.append(
                f"High proportion of 'unknown' categories detected ({unknown_ratio:.0%})"
            )
            status = "warn"

    # -----------------------------
    # Final verdict
    # -----------------------------

    return {
        "status": status,
        "issues": issues,
        "row_count": row_count
    }


# -----------------------------
# Local test stub
# -----------------------------

if __name__ == "__main__":

    # SQL safety test
    try:
        validate_sql("SELECT SUM(order_id) FROM fact_sales")
    except Exception as e:
        print("[BLOCKED SQL]", e)

    # Result validation test
    fake_output = {
        "result": pd.DataFrame({
            "category": ["A", "B"],
            "total_revenue": [10000, 20000],
            "total_orders": [10, 15]
        }),
        "row_count": 2,
        "columns": ["category", "total_revenue", "total_orders"]
    }

    fake_intent = {
        "metrics": ["revenue", "orders"],
        "dimensions": ["category"]
    }

    verdict = validate_result(fake_intent, fake_output)
    print("\nVALIDATION RESULT:\n", verdict)

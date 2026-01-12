"""
router.py

Purpose:
--------
Route structured intent to an executable query plan.

This agent:
- validates requested tables and fields against schema registry
- determines required datasets and joins
- flags heavy vs light queries
- selects execution strategy (qa vs summarization)
- enforces schema and business constraints

It does NOT generate SQL.
"""

import yaml
from typing import Dict, Any, List


# -----------------------------
# Load schema registry
# -----------------------------

def load_schema_registry(path: str = "schema/registry.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


SCHEMA = load_schema_registry()


# -----------------------------
# Router core
# -----------------------------

def route_intent(intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich intent with routing and execution plan.

    Returns updated intent with:
    - resolved_tables
    - required_joins
    - query_complexity
    - execution_mode
    """

    routed_intent = intent.copy()

    # -----------------------------
    # 1. Validate target tables
    # -----------------------------

    available_tables = SCHEMA["tables"].keys()

    resolved_tables = []
    for table in intent.get("target_tables", []):
        if table not in available_tables:
            raise ValueError(f"Unknown table requested: {table}")
        resolved_tables.append(table)

    if not resolved_tables:
        # default to fact_sales if not specified
        resolved_tables = ["fact_sales"]

    routed_intent["resolved_tables"] = resolved_tables


    # -----------------------------
    # 2. Determine required joins
    # -----------------------------

    required_joins = []

    if "fact_sales" in resolved_tables and "dim_product" in resolved_tables:
        required_joins.append({
            "left": "fact_sales",
            "right": "dim_product",
            "on": ["sku"],
            "type": "left"
        })

    routed_intent["required_joins"] = required_joins


    # -----------------------------
    # 3. Determine execution mode
    # -----------------------------

    if intent["question_type"] == "summary":
        execution_mode = "summarization"
    else:
        execution_mode = "qa"

    routed_intent["execution_mode"] = execution_mode


    # -----------------------------
    # 4. Estimate query complexity
    # -----------------------------

    complexity_score = 0

    complexity_score += len(routed_intent.get("metrics", []))
    complexity_score += len(routed_intent.get("dimensions", []))
    complexity_score += len(required_joins) * 2

    if intent.get("time_range", {}).get("comparison"):
        complexity_score += 2

    if complexity_score <= 3:
        query_complexity = "light"
    elif complexity_score <= 6:
        query_complexity = "medium"
    else:
        query_complexity = "heavy"

    routed_intent["query_complexity"] = query_complexity


    # -----------------------------
    # 5. Enforce safety limits
    # -----------------------------

    max_dims = SCHEMA["business_rules"]["safety_limits"]["max_groupby_columns"]

    if len(intent.get("dimensions", [])) > max_dims:
        raise ValueError(
            f"Too many group by dimensions requested. Max allowed: {max_dims}"
        )

    routed_intent["safety_status"] = "pass"


    return routed_intent


# -----------------------------
# Local test stub
# -----------------------------

if __name__ == "__main__":

    test_intent = {
        "question_type": "ranking",
        "business_question": "Which category has highest revenue?",
        "target_tables": ["fact_sales"],
        "metrics": ["revenue"],
        "dimensions": ["category_clean"],
        "filters": {},
        "time_range": {
            "type": "relative",
            "start": None,
            "end": None,
            "period": "last_3_months",
            "comparison": None
        },
        "grain": "month",
        "expected_output": "ranked_list",
        "ambiguities": [],
        "confidence": 0.84
    }

    routed = route_intent(test_intent)
    print("\nROUTED INTENT:\n")
    for k, v in routed.items():
        print(f"{k}: {v}")

"""
data_agent.py

Purpose:
--------
Deterministic data extraction agent.

This agent:
- validates intent against schema registry
- builds SQL queries from routed intent
- enforces metric semantics
- executes SQL using DuckDB layer
- returns result + metadata

It does NOT summarize or interpret data.
"""

import yaml
from typing import Dict, Any, List
from storage.duckdb_loader import run_query


# -----------------------------
# Load schema registry
# -----------------------------

def load_schema_registry(path: str = "schema/registry.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


SCHEMA = load_schema_registry()


# -----------------------------
# Metric contract (CRITICAL)
# -----------------------------

METRIC_SQL_MAP = {
    "revenue": "SUM(revenue) AS total_revenue",
    "units": "SUM(units) AS total_units",
    "orders": "COUNT(DISTINCT order_id) AS total_orders"
}


# -----------------------------
# Validation helpers
# -----------------------------

def validate_columns(table: str, columns: List[str]):
    available = SCHEMA["tables"][table]["columns"].keys()
    for col in columns:
        if col not in available:
            raise ValueError(f"Column '{col}' not found in table '{table}'")


def validate_metrics(metrics: List[str]):
    for m in metrics:
        if m not in METRIC_SQL_MAP:
            raise ValueError(f"Metric '{m}' is not supported or not analytics-safe")


def resolve_dimension_column(dim: str, tables: list) -> str:
    """
    Resolve which table a dimension belongs to.
    """
    dimension_tables = [t for t in tables if t.startswith("dim_")]
    fact_tables = [t for t in tables if t.startswith("fact_")]

    for table in dimension_tables + fact_tables:
        if dim in SCHEMA["tables"][table]["columns"]:
            return f"{table}.{dim}"

    raise ValueError(f"Unable to resolve dimension column: {dim}")


# -----------------------------
# SQL builders
# -----------------------------

def build_select_clause(intent: Dict[str, Any]) -> List[str]:
    select_parts = []

    # ---- dimensions ----
    for dim in intent.get("dimensions", []):
        qualified_dim = resolve_dimension_column(dim, intent["resolved_tables"])
        select_parts.append(f"{qualified_dim} AS {dim}")

    # ---- metrics (STRICT) ----
    for metric in intent.get("metrics", []):
        metric_sql = METRIC_SQL_MAP.get(metric)
        if not metric_sql:
            raise ValueError(f"Unsupported or unsafe metric: {metric}")
        select_parts.append(metric_sql)

    return select_parts


def build_where_clause(filters: Dict[str, Any]) -> str:
    if not filters:
        return ""

    conditions = []

    for col, values in filters.items():
        if isinstance(values, list):
            vals = ", ".join([f"'{v}'" for v in values])
            conditions.append(f"{col} IN ({vals})")
        else:
            conditions.append(f"{col} = '{values}'")

    return "WHERE " + " AND ".join(conditions)


def build_group_by_clause(dimensions: List[str], tables: list) -> str:
    if not dimensions:
        return ""

    qualified_dims = [
        resolve_dimension_column(dim, tables)
        for dim in dimensions
    ]

    return "GROUP BY " + ", ".join(qualified_dims)


# -----------------------------
# Core execution function
# -----------------------------

def execute_intent(routed_intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build and execute SQL from routed intent.

    Returns:
    {
        result: DataFrame,
        sql: str,
        row_count: int,
        columns: list
    }
    """

    if routed_intent.get("out_of_scope") or routed_intent.get("status") == "blocked":
        raise ValueError("Data agent invoked for blocked or out-of-scope intent")

    tables = routed_intent["resolved_tables"]
    dimensions = routed_intent.get("dimensions", [])
    metrics = routed_intent.get("metrics", [])
    filters = routed_intent.get("filters", {})

    primary_table = tables[0]

    # ---- validations ----
    validate_columns(primary_table, dimensions)
    validate_metrics(metrics)

    # ---- SELECT ----
    select_clause = build_select_clause(routed_intent)
    select_sql = "SELECT " + ", ".join(select_clause)

    # ---- FROM + JOINS ----
    from_sql = f"FROM {primary_table}"

    join_sql_parts = []
    for join in routed_intent.get("required_joins", []):
        join_sql_parts.append(
            f"{join['type'].upper()} JOIN {join['right']} "
            f"ON {join['left']}.sku = {join['right']}.sku"
        )

    join_sql = " ".join(join_sql_parts)

    # ---- WHERE ----
    where_sql = build_where_clause(filters)

    # ---- GROUP BY ----
    group_by_sql = build_group_by_clause(dimensions, tables)

    # ---- Final SQL ----
    sql = f"""
    {select_sql}
    {from_sql}
    {join_sql}
    {where_sql}
    {group_by_sql}
    """

    # ---- execute ----
    df = run_query(sql)

    return {
        "result": df,
        "sql": sql,
        "row_count": len(df),
        "columns": list(df.columns)
    }


# -----------------------------
# Local test stub
# -----------------------------

if __name__ == "__main__":

    test_routed_intent = {
        "question_type": "ranking",
        "business_question": "Which category has highest revenue?",
        "resolved_tables": ["fact_sales"],
        "required_joins": [],
        "metrics": ["revenue", "orders"],
        "dimensions": ["category"],
        "filters": {},
        "execution_mode": "qa",
        "query_complexity": "light"
    }

    output = execute_intent(test_routed_intent)

    print("\nSQL USED:\n", output["sql"])
    print("\nRESULT SAMPLE:\n", output["result"].head())

"""
connection.py

Purpose:
--------
Centralized database connection manager.

All database access must go through this module.
This allows future replacement of DuckDB with
Snowflake / BigQuery / Postgres without changing agents.
"""

import duckdb

DB_PATH = "storage/retail.duckdb"


def get_connection(read_only: bool = False):
    """
    Create and return a DuckDB connection.

    Args:
        read_only (bool): if True, opens DB in read-only mode

    Returns:
        duckdb.DuckDBPyConnection
    """
    return duckdb.connect(database=DB_PATH, read_only=read_only)

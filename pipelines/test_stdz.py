import duckdb

con = duckdb.connect()
con.execute("DESCRIBE SELECT * FROM 'data/processed/fact_sales.parquet'").fetchdf()

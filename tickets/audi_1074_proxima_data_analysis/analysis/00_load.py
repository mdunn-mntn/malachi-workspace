#!/usr/bin/env python3
"""AUDI-1074: one-time load of the parquet drop into outputs/proxima.duckdb.

Usage: python3 00_load.py  (all later analysis scripts read the .duckdb, never
the raw parquet — each raw scan of the 13GB tree costs ~10min/query)
"""
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "outputs" / "proxima_20260717"
DB = ROOT / "outputs" / "proxima.duckdb"

DB.unlink(missing_ok=True)
con = duckdb.connect(str(DB))

for name, sql in [
    ("basket", f"SELECT * FROM read_parquet('{DATA}/basket/*')"),
    ("items", f"SELECT * FROM read_parquet('{DATA}/items/*')"),
    ("ipmap", f"SELECT _COL_0 AS customer_id, _COL_1 AS ip_address "
              f"FROM read_parquet('{DATA}/ip_mapping/*')"),
]:
    print(f"loading {name}...", flush=True)
    con.execute(f"CREATE TABLE {name} AS {sql}")
    print(f"  {name}: {con.execute(f'SELECT COUNT(*) FROM {name}').fetchone()[0]:,} rows", flush=True)

print("done", flush=True)

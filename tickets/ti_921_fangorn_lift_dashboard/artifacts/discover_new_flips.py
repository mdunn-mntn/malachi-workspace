#!/usr/bin/env python3
"""
TI-921 — Run the discovery query for newly-flipped Fangorn AIDs.

Reads `wave_config.csv` (the manual source of truth) at runtime and injects
the known AID list into the discovery SQL. Returns a CSV of any AID with
`vertical_data_source = 46` in BQ that's NOT yet logged in wave_config.

Usage:
    python3 discover_new_flips.py

Output:
    Prints a CSV to stdout. Each row is a newly-flipped AID with a
    `suggested_csv_row` column already formatted to paste into wave_config.csv.

Why this exists:
    The standalone SQL file `queries/ti_921_discover_new_flips.sql` has the
    known-AID list hardcoded inline — it gets stale every time we add a wave
    to wave_config.csv. This script injects from the CSV at runtime so we
    only ever update one file.
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

from google.cloud import bigquery

HERE = Path(__file__).resolve().parent
WAVE_CONFIG = HERE / "wave_config.csv"
BQ_PROJECT = "dw-main-bronze"


def known_aids() -> list[int]:
    with WAVE_CONFIG.open() as f:
        return [int(row["advertiser_id"]) for row in csv.DictReader(f)]


def build_query(aids: list[int]) -> str:
    if not aids:
        # Edge case: empty CSV → all flipped AIDs are "new"
        known_clause = "SELECT CAST(NULL AS INT64) AS advertiser_id WHERE FALSE"
    else:
        rows = " UNION ALL ".join(f"SELECT {a} AS advertiser_id" for a in aids)
        known_clause = rows
    return f"""
WITH known AS ({known_clause}),
current_treated AS (
  SELECT
    c.advertiser_id,
    TIMESTAMP_MILLIS(c.datastream_metadata.source_timestamp) AS flip_ts_utc,
    DATE(TIMESTAMP_MILLIS(c.datastream_metadata.source_timestamp), "America/Los_Angeles") AS flip_date_pt
  FROM `dw-main-bronze.integrationprod.audience_advertiser_configurations` c
  WHERE c.vertical_data_source = 46
)
SELECT
  ct.advertiser_id,
  a.company_name,
  av.vertical_id,
  av.vertical_name,
  ct.flip_ts_utc,
  ct.flip_date_pt,
  CONCAT(
    CAST(ct.advertiser_id AS STRING), ',',
    a.company_name, ',',
    CAST(ct.flip_date_pt AS STRING), ',',
    'TierX-WaveY,',
    COALESCE(av.vertical_name, 'unknown'), ',',
    'true,true,',
    'review pixel + dollar status'
  ) AS suggested_csv_row
FROM current_treated ct
LEFT JOIN `dw-main-bronze.integrationprod.advertisers` a USING(advertiser_id)
LEFT JOIN `dw-main-silver.fpa.advertiser_verticals` av
  ON ct.advertiser_id = av.advertiser_id AND av.type = 1
WHERE ct.advertiser_id NOT IN (SELECT advertiser_id FROM known)
ORDER BY ct.flip_ts_utc;
"""


def main() -> int:
    aids = known_aids()
    print(f"# Known AIDs in wave_config.csv: {len(aids)}", file=sys.stderr)
    sql = build_query(aids)

    client = bigquery.Client(project=BQ_PROJECT)
    df = client.query(sql).to_dataframe()
    if df.empty:
        print("# No new flips — wave_config.csv is up to date.", file=sys.stderr)
        return 0

    print(f"# Found {len(df)} newly-flipped AIDs not yet in wave_config.csv:",
          file=sys.stderr)
    df.to_csv(sys.stdout, index=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())

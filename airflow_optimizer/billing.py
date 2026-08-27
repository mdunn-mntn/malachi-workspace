"""Blended Dataproc rate from the GCP billing export, so savings price from actual spend.

The DCU SKUs bill in MILLI DCU-hours (verified against the export 2026-08-27: $121,731 over
2.44e9 pricing units = $0.0498/DCU-h only after the x1000), hence the 1000 factor in the SQL.

The export lives in `mntn-billing-00.gcp_cloud_billing_standard` (day-partitioned; the last
two days are excluded because billing finalizes late). The rate is post-discount `cost` over
`usage.amount_in_pricing_units` for the Serverless DCU SKUs, converted to dollars per
executor-hour with the measured DCU-per-executor-hour ratio. Access is a dataset-scoped
grant (mntn-devops#5121); until it lands, or on any failure, the caller falls back to the
OPTIMIZER_USD_PER_EXEC_H environment variable and nothing in the sweep breaks.
"""

from __future__ import annotations

import json
import subprocess

BILLING_TABLE = "mntn-billing-00.gcp_cloud_billing_standard.gcp_billing_export_v1_01E62F_CDF2FC_8AC7A4"
DCU_PER_EXEC_H = 5.44  # measured (INC-005 batch); the conservative end of the 5.4-9.9 range

_RATE_SQL = f"""
SELECT 1000 * SUM(cost) / NULLIF(SUM(usage.amount_in_pricing_units), 0) AS usd_per_dcu_h
FROM `{BILLING_TABLE}`
WHERE service.description = 'Dataproc'
  AND sku.description LIKE '%Data Compute Unit%'
  AND _PARTITIONTIME BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 32 DAY)
                         AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
"""


def blended_usd_per_exec_h(timeout_s: int = 120) -> tuple[float | None, str]:
    """(rate, note). None means no access or no data; the note says which."""
    try:
        r = subprocess.run(
            ["bq", "query", "--use_legacy_sql=false", "--format=json",
             "--project_id=mntn-billing-00",
             "--location=US", _RATE_SQL],
            capture_output=True, timeout=timeout_s,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        return None, f"billing query did not run: {str(e)[:80]}"
    if r.returncode != 0:
        return None, f"billing export unreadable: {r.stderr.decode(errors='replace')[:120]}"
    try:
        rows = json.loads(r.stdout.decode() or "[]")
        usd_per_dcu = float(rows[0]["usd_per_dcu_h"])
    except (ValueError, KeyError, IndexError, TypeError, AttributeError):
        return None, "billing export returned no DCU rows"
    rate = round(usd_per_dcu * DCU_PER_EXEC_H, 3)
    return rate, (f"blended from 30d of actual spend: ${usd_per_dcu:.4f}/DCU-h x "
                  f"{DCU_PER_EXEC_H} DCU-h per executor-hour")

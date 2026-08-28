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
import os
import subprocess

BILLING_TABLE = (
    "mntn-billing-00.gcp_cloud_billing_standard.gcp_billing_export_v1_01E62F_CDF2FC_8AC7A4"
)
DCU_PER_EXEC_H = 5.44  # measured (INC-005 batch); the conservative end of the 5.4-9.9 range

_RATE_SQL = f"""
SELECT {{factor}} * SUM(cost) / NULLIF(SUM(usage.amount_in_pricing_units), 0) AS usd_per_unit
FROM `{BILLING_TABLE}`
WHERE service.description = '{{service}}'
  AND sku.description LIKE '{{sku_like}}'
  AND _PARTITIONTIME BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 32 DAY)
                         AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
"""


def _blended_rate(
    service: str, sku_like: str, factor: float = 1.0, timeout_s: int = 120
) -> tuple[float | None, str]:
    """Post-discount dollars per pricing unit for one SKU family, over 30 finalized days."""
    sql = _RATE_SQL.format(service=service, sku_like=sku_like, factor=factor)
    try:
        r = subprocess.run(
            [
                "bq",
                "query",
                "--use_legacy_sql=false",
                "--format=json",
                "--project_id=mntn-billing-00",
                "--location=US",
                sql,
            ],
            capture_output=True,
            timeout=timeout_s,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        return None, f"billing query did not run: {str(e)[:80]}"
    if r.returncode != 0:
        return None, f"billing export unreadable: {r.stderr.decode(errors='replace')[:120]}"
    try:
        rows = json.loads(r.stdout.decode() or "[]")
        usd = float(rows[0]["usd_per_unit"])
    except (ValueError, KeyError, IndexError, TypeError, AttributeError):
        return None, f"billing export returned no rows for {service} / {sku_like}"
    return usd, f"blended from 30d of actual spend on {service} {sku_like}"


def blended_usd_per_exec_h(timeout_s: int = 120) -> tuple[float | None, str]:
    """(rate, note) for Spark executor-hours. None means no access or no data."""
    usd_per_dcu, note = _blended_rate(
        "Dataproc", "%Data Compute Unit%", factor=1000, timeout_s=timeout_s
    )
    if usd_per_dcu is None:
        return None, note
    rate = round(usd_per_dcu * DCU_PER_EXEC_H, 3)
    return rate, (
        f"blended from 30d of actual spend: ${usd_per_dcu:.4f}/DCU-h x "
        f"{DCU_PER_EXEC_H} DCU-h per executor-hour"
    )


def blended_usd_per_slot_h(timeout_s: int = 120) -> tuple[float | None, str]:
    """(rate, note) for BigQuery slot-hours, from the edition-slot SKUs."""
    rate, note = _blended_rate("BigQuery Reservation API", "%Slot%", timeout_s=timeout_s)
    if rate is not None:
        return round(rate, 4), note
    try:
        return float(os.environ["OPTIMIZER_USD_PER_SLOT_H"]), f"configured fallback; {note}"
    except (KeyError, ValueError):
        return None, note


def surface_rates() -> dict[str, tuple[float | None, str]]:
    """Every surface's dollars-per-unit: spark per executor-hour, bq per slot-hour."""
    return {"spark": blended_usd_per_exec_h(), "bq": blended_usd_per_slot_h()}

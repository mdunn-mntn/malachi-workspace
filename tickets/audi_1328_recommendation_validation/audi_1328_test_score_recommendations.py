#!/usr/bin/env python3
"""Regression tests for the two harness defects: the .config() diff parse and the pre_fix_quiet gate.

Usage:
  python3 -m unittest audi_1328_test_score_recommendations -v
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest

import audi_1328_score_recommendations as H

REPO = H.DEFAULT_REPO
HAS_REPO = os.path.exists(os.path.join(REPO, ".git"))

REAL_DIFF = '''diff --git a/models/ipdsc/ipdsc_ds_49.py b/models/ipdsc/ipdsc_ds_49.py
index 70d6d83..55cf9fc 100644
--- a/models/ipdsc/ipdsc_ds_49.py
+++ b/models/ipdsc/ipdsc_ds_49.py
@@ -40,6 +40,7 @@ class DS49(IPDSCDailyDataSourceModel):
         self.__spark = (
             SparkSession.builder.appName(f"Populate {self.model_id()}")
-            .config("spark.sql.shuffle.partitions", "1700")
+            .config("spark.sql.shuffle.partitions", "3400")
             .getOrCreate()
         )
'''


class ConfigCallParse(unittest.TestCase):
    def test_matches_config_lines_below_the_diff_header(self):
        found = H.CONFIG_CALL.findall(REAL_DIFF)
        self.assertEqual(
            found,
            [("-", "spark.sql.shuffle.partitions", "1700"),
             ("+", "spark.sql.shuffle.partitions", "3400")],
        )

    def test_ignores_the_diff_file_header_lines(self):
        keys = {k for _sign, k, _v in H.CONFIG_CALL.findall(REAL_DIFF)}
        self.assertEqual(keys, {"spark.sql.shuffle.partitions"})


@unittest.skipUnless(HAS_REPO, f"needs the airflow-ti checkout at {REPO}")
class ShippedChangesOverRealPullRequests(unittest.TestCase):
    def shipped(self, pr_number: str, dag_id: str) -> dict:
        url = f"https://github.com/SteelHouse/airflow-ti/pull/{pr_number}"
        found = H.merge_commit(REPO, url, "origin/main")
        if not found:
            self.skipTest(f"#{pr_number} is not on origin/main in this checkout")
        return H.shipped_changes(REPO, url, dag_id, "origin/main")

    def test_added_config_line_is_the_only_evidence_of_the_ipdsc_ds_49_fix(self):
        shipped = self.shipped("1272", "ipdsc_ds_49")
        self.assertTrue(shipped["resolvable"])
        self.assertEqual(
            shipped["changed"].get("spark.sql.files.maxPartitionBytes"), (None, "67108864")
        )

    def test_removed_and_added_config_lines_pair_into_one_before_after(self):
        shipped = self.shipped("1231", "fangorn_score_monitor")
        self.assertEqual(
            shipped["changed"].get("spark.sql.shuffle.partitions"), ("256", "2048")
        )

    def test_no_attributed_unit_reads_as_an_empty_shipped_change(self):
        for pr_number, dag_id in (
            ("1272", "conv_log_derived_ip"),
            ("1273", "conversion_log_advertiser_id_dsc_id"),
            ("1270", "guid_conv_log_pivot_ip_vertical_id"),
            ("1273", "guid_log_advertiser_id_dsc_id"),
            ("1270", "guid_log_pivot_ip_vertical_id"),
            ("1272", "ipdsc_ds_49"),
            ("1273", "site_visit_signal_advertiser_id_dsc_id"),
        ):
            with self.subTest(pr=pr_number, dag=dag_id):
                self.assertTrue(self.shipped(pr_number, dag_id)["changed"])


def row(dag_id: str, key: str, date: str, state: str, exec_h: float | None = 10.0, **extra) -> dict:
    base = {"dag_id": dag_id, "key": key, "date": date, "state": state,
            "surface": "spark", "exec_h": exec_h}
    base.update(extra)
    return base


APPLIED = {"fix_pr": "https://github.com/SteelHouse/airflow-ti/pull/1273",
           "applied_date": "2026-09-03",
           "fix": "Raise spark.sql.shuffle.partitions to 3400."}

PRE_FIX_LEDGER = [
    row("measured_dag", "disk_spill:1", "2026-09-01", "new"),
    row("measured_dag", "disk_spill:1", "2026-09-02", "chronic"),
    row("measured_dag", "disk_spill:1", "2026-09-03", "applied", **APPLIED),
    row("measured_dag", "exec_h", "2026-09-04", "observed"),
    row("unmeasured_dag", "disk_spill:1", "2026-09-01", "new"),
    row("unmeasured_dag", "disk_spill:1", "2026-09-02", "chronic"),
    row("unmeasured_dag", "disk_spill:1", "2026-09-03", "applied", **APPLIED),
]
POST_FIX_LEDGER = [
    row(dag, "exec_h", date, "observed")
    for dag in ("measured_dag", "unmeasured_dag", "noisy_dag")
    for date in ("2026-09-05", "2026-09-06", "2026-09-07")
] + [row("noisy_dag", "disk_spill:2", date, "chronic")
     for date in ("2026-09-02", "2026-09-05", "2026-09-06", "2026-09-07")]


def run(rows: list[dict], effective_from: str = "2026-09-05") -> dict:
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
        path = fh.name
    try:
        return H.analyse(H.load_ledger(path), "", 3, "origin/main", effective_from)
    finally:
        os.unlink(path)


def unit_for(result: dict, dag_id: str) -> dict:
    return next(u for u in result["units"] if u["dag_id"] == dag_id)


class PreFixQuietGate(unittest.TestCase):
    def test_silence_on_an_unmeasured_pre_fix_date_is_not_pre_fix_quiet(self):
        result = run(PRE_FIX_LEDGER)
        self.assertEqual(unit_for(result, "unmeasured_dag")["pre_fix_quiet_dates"], "")

    def test_silence_on_a_measured_pre_fix_date_is_pre_fix_quiet(self):
        result = run(PRE_FIX_LEDGER)
        self.assertEqual(unit_for(result, "measured_dag")["pre_fix_quiet_dates"], "2026-09-04")

    def test_the_forecast_writes_off_only_the_measured_one(self):
        result = run(PRE_FIX_LEDGER)
        fc = H.forecast(result)
        self.assertEqual([u["dag_id"] for u in fc["pre_fix_quiet"]], ["measured_dag"])
        self.assertIn("unmeasured_dag", [u["dag_id"] for u in fc["best_case_attributable"]])

    def test_the_gate_decides_the_verdict_once_the_window_is_long_enough(self):
        result = run(PRE_FIX_LEDGER + POST_FIX_LEDGER)
        measured, unmeasured = unit_for(result, "measured_dag"), unit_for(result, "unmeasured_dag")
        self.assertTrue(measured["eligible"] and unmeasured["eligible"])
        self.assertIn("pre_fix_quiet", measured["attribution_failures"])
        self.assertNotIn("pre_fix_quiet", unmeasured["attribution_failures"])

    def test_a_finding_that_never_fired_is_not_written_off_as_pre_fix_quiet(self):
        never_fired = [row("silent_dag", "disk_spill:1", "2026-09-03", "applied", **APPLIED),
                       row("silent_dag", "exec_h", "2026-09-04", "observed")]
        result = run(PRE_FIX_LEDGER + never_fired)
        self.assertEqual(unit_for(result, "silent_dag")["pre_fix_quiet_dates"], "")


if __name__ == "__main__":
    unittest.main()

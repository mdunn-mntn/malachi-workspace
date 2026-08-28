# Demo readiness checklist — the fully automated optimizer + debugger system

Written 2026-08-28 after #1245 merged. Status per item: DONE (shipped and verified), LIVE-TEST
(shipped, first real event proves it), GAP (work remains, owner named). The demo is credible
when every GAP is closed and every LIVE-TEST has fired once.

## 1. Coverage

- DONE - Spark fleet: daily sweep reads every event log in GCS + PHS; adv_score monitor was the
  last readable gap (#1241).
- LIVE-TEST - BigQuery surface: bq_profile ships in #1245; tomorrow's 09:00 UTC sweep must
  write `optimizer_bq_<date>.md` and ledger rows with `surface="bq"`.
- LIVE-TEST - Databricks native jobs: findings_reports ships in #1245; same sweep check for
  `surface="dbx"` rows.
- GAP - The coverage report still counts the 39 non-Spark DAGs as "unprofiled": coverage.py
  judges only Spark event logs. It must count a DAG profiled when bq/dbx saw its cost. One
  small PR (coverage.py + test). Owner: me, next PR.
- GAP - dbt-on-warehouse tasks (create_ip_verticals, 4 tasks) are profiled via warehouse query
  history but not tied back to their DAG in coverage. Fold into the same coverage PR.
- GAP - Pod surface: blocked on the Astro Universal Metrics Exporter setup (steps:
  `audi_1194_astro_metrics_exporter_setup.md`). Owner: Malachi (15 min), then me (pod_profile PR).
- VERIFY - "Full 30-day scan": the ledger holds daily rows since 2026-08-21; event logs older
  than the GCS retention are gone, so 30 days of HISTORY accumulates by mid-September. Nothing
  to build; state it correctly in the demo.

## 2. Cost correctness (the unit-mixing risk you named)

The rule everywhere: units NEVER add across surfaces; only dollars add.
- DONE - Ledger: `surface` field on every row; resolution and before/after math scoped per
  surface (tests in both directions).
- DONE - Digest headline: spark priced at the live Dataproc rate; other surfaces print their
  own unit and only print dollars when their own rate exists.
- DONE - Mode: headline dollars are spark-only; "Savings by surface" table shows each surface
  in its own unit.
- LIVE-TEST - BQ slot-hour rate: SKU family ('BigQuery Reservation API' / '%Slot%') is a
  guess until the first prod sweep prints it; on failure it falls back to
  OPTIMIZER_USD_PER_SLOT_H (set it as a backup, like 0.278 was). Verify the printed rate
  against a hand-check before the demo.
- GAP - Databricks dollars in savings: dbx savings are tracked in DBU; the $/DBU blend from
  system.billing is computed for reports but not fed into the savings math. Small PR: pass a
  dbx rate into usd_rates. Owner: me.
- GAP - One "total dollars saved" card that SUMS the per-surface dollar figures (never units)
  once bq/dbx rates are verified. Mode query + card edit. Owner: me.

## 3. Failure response (debugger)

- DONE - 15-min rapid sweep, terminal failures only, exactly-once via GCS markers.
- DONE - Threads in #alerts-tpa-pipeline AND #monitor-tpa (Astro var updated 2026-08-28);
  unmatched failures go loose ONLY to #airflow-debugger.
- LIVE-TEST - The next terminal failure proves the whole chain: thread reply + marker + Bug
  ticket. fangorn_household_14day_lookback (2026-08-28) is the candidate if try 2 died.
- DONE - New failure kinds: signature-less failures upload raw logs to `unclassified/<ds>/`
  and the digest calls them out; the noon laptop job surfaces them for taxonomy additions.
- GAP - "Automatically updating" the taxonomy is half manual by design: a person adds the
  signature. Say so in the demo, or accept the risk of overpromising.

## 4. Tickets, Confluence, dashboard

- DONE - Auto-filed Bugs per Bryce's spec (type, environment, source, priority + reason line,
  parent epic), dedup against open tickets, proven live (AUDI-1249).
- PENDING (IT) - Swap Astro JIRA_USER_EMAIL/JIRA_API_TOKEN to the service account when Robin's
  team delivers; until then tickets file as Malachi.
- DONE - Playbook 2908061697: merged reference, known-issues rows auto-appended, priority
  rubric, fix log auto-synced from the ledger (noon job).
- DONE - Mode dashboard auto-refreshes from the GCS-backed external table; set the report
  schedule in the Mode UI if not already (API refused; one-time click).

## 5. PR-to-savings provenance

- DONE - Every saving requires fix_pr: ledger mark_applied records PR + date; savings,
  fix log, and the Mode fixes table all carry the link.
- BY DESIGN - mark_applied is a human step (one command per merged fix). It is the moment we
  assert "this PR is the fix"; automating it would guess. Demo it as the one manual step.
- DONE - Annual estimate recalculates every sweep: run-rate spread over calendar days since
  each fix, x365, per surface.

## 6. Hackathon

- DONE - The merge list exists: `outputs/audi_1194_hackathon_optimizations_2026_08_27.md`
  (17 PR-READY Spark fixes) + dbt#174 held for it.
- TASK - After each hackathon merge: one mark_applied per fix so savings start accruing.

## 7. Pre-demo test run (do all of these, in order, the day before)

1. Trigger spark_optimizer_daily manually; confirm all five artifacts land in GCS
   (backlog, coverage, digest, bq, savings) + dbx section.
2. Confirm ledger rows for all three surfaces with correct `surface` values.
3. Confirm the printed bq $/slot-h rate is sane vs a hand calculation.
4. Trigger a harmless test-DAG failure; watch: alert -> threaded reply <=15 min -> marker ->
   Bug ticket with priority reason -> playbook row.
5. Open Mode: headline, by-surface table, fixes table, fangorn savings accruing.
6. Check #airflow-debugger carries ONLY unmatched diagnoses, no duplicates.

## Open GAP summary (the build list)

| # | Item | Size |
|---|---|---|
| 1 | coverage.py counts bq/dbx-profiled DAGs as covered (kills "39 unprofiled") | small PR |
| 2 | dbx $/DBU rate into savings usd_rates | small PR |
| 3 | Combined total-dollars card in Mode (post rate verification) | Mode edit |
| 4 | Pod surface (blocked on Malachi's exporter setup) | medium PR |
| 5 | Set OPTIMIZER_USD_PER_SLOT_H backup var on Astro | 1 min, Malachi |
| 6 | Jira SA swap (blocked on IT) | 1 min, Malachi |

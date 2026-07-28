# On-Call Runbook — Master

**Read this FIRST on any on-call alert. Append an incident entry after every resolution.**
The more incidents we log, the faster the next one closes. If an alert matches a row in
§2 Known-Alert Catalog, jump straight to its protocol.

- **Home:** `on-call/` (raw alert logs live here too, named as downloaded).
- **Update rule:** after resolving ANY alert, add/append to §2 (one-line signature) and §3 (full
  incident). Never delete rows — a "benign, expected" verdict is as valuable as a fix.
- **Prod safety (non-negotiable):** never modify prod DAGs or push to `main` in `airflow-ti` /
  `sqlmesh` to "fix" an alert. Diagnose → clear/re-run or route to the owner. Widening a timeout or
  soft-failing a sensor is a code change owned by the producing team, not an on-call action.

---

## 1. General triage protocol (any Airflow alert)

1. **Identify** DAG + task + logical date from the alert (`[prod] Airflow <Team> FAILURE [dag/task] at <ts>`).
2. **Pull the task log**, find what the task is *actually doing* — not that it failed:
   - **Sensor** → the poke target (`Sensor checks existence of : <bucket>, <object>`).
   - **Producer/Spark/BQ** → the output path / query / the real exception (search the log tail for `ERROR`/`Exception`/`Traceback`, skip the boilerplate).
3. **Check empirical state** — did the thing it waited on / was supposed to write actually land?
   `gcloud storage ls -l "gs://<bucket>/<path>/"` (reauth with `gcloud auth login` if you get
   `Reauthentication required`; do it as ONE call — parallel gsutil calls trip the reauth quota).
4. **Classify** (see verdict taxonomy below).
5. **Act** per class. **Log** the incident in §2 + §3.

**Verdict taxonomy**

| Class | Signature | Action |
|---|---|---|
| **Benign / expected** | Alert is a known side-effect of intended behavior (e.g. optional-partner skip). Main pipeline succeeded. | Ack. Reply in thread "expected, <reason>". No re-run. Log it. |
| **Late data** | The awaited object exists *now*, arrived after the sensor's window. | Clear the failed task → it passes immediately. Not an outage. |
| **Real upstream failure** | Object genuinely absent AND was required; or producer task threw a real error. | Find + re-run the producer task (mind batch-id traps), or route to the feed/vendor owner. |
| **DAG/logic bug** | Wrong path, bad param, code regression. | Route to the owning team with the evidence. Do NOT hot-patch prod. |

---

## 2. Known-Alert Catalog (signature → verdict → protocol)

| Alert signature | Root cause | Verdict | Protocol |
|---|---|---|---|
| `ipdsc_monitor / precondition_<partner>` GCS sensor **18h timeout** (e.g. `precondition_bombora`, DS51) | Optional 3P partner didn't deliver source files that day → producer skips it silently → monitor pages on the absent `ipdsc/dt=.../data_source_id=<id>/` partition | **Benign / expected** on partner-skip days (verify source absence first) | INC-001 |

---

## 3. Incident log

### INC-001 — `ipdsc_monitor` `precondition_bombora` sensor timeout (DS51 Bombora)
**Date:** 2026-07-28 · **Alert:** `🔴 [prod] Airflow Targeting FAILURE [ipdsc_monitor/precondition_bombora] at 2026-07-26 17:05 PT` · `AirflowSensorTimeout: run duration 64836s exceeds timeout 64800.0` (18h).

**Verdict: BENIGN / EXPECTED.** Bombora (DS51) is an `optional: true` partner. It didn't deliver its
source files, so the producer skipped it and no `data_source_id=51` partition was written; the separate
`ipdsc_monitor` DAG doesn't know about the skip and pages on the absent partition. The producer's own
docstring documents this exact alert as expected on partner-skip days. **No action needed** — the main
`tpa_export` pipeline completed fine (exports `{"data_source_id": 51, "cats": []}`).

**The two DAGs (both team TPA_EXPORT):**
- **Producer** `tpa_ipdsc_export` — `dags/tpa_export/tpa_ipdsc_export.py`, schedule `35 2 * * *` (02:35 UTC), severity 0. Writes `gs://mntn-data-archive-prod/ipdsc/dt=<ds>/data_source_id=<id>/`. Bombora build = task **`ipdsc_bombora`** (model `ipdsc_third_party_audience_builder --partner bombora`), gated by source sensor **`wait_bombora_src`** (1h timeout, `mode=reschedule`, `soft_fail=True` because optional → SKIPPED when source absent).
- **Consumer/monitor** `ipdsc_monitor` — `dags/monitoring/ipdsc_monitor.py`, schedule `5 0 * * *` (00:05 UTC), severity 1. Registry-driven loop builds one `precondition_<partner>` `GCSObjectExistenceSensor` per entry in `THIRD_PARTY_AUDIENCE_BUILDERS`, each **18h timeout / 60s poke / `soft_fail=False`** → this is what pages.

**Registry** `dags/ipdsc_third_party_audience_builders.json` — Bombora entry:
`data_source_id: 51`, `optional: true`, `source_date_offset_days: 1`,
`input_glob_template: gs://mntn-data-partners/partners/bombora/segments/{yyyymmdd}/hem_segments_*.csv.gz`.
So `ipdsc/dt=D` sources Bombora files dated **D−1**.

**Diagnosis run (copy-paste for next time):**
```bash
# 1. Confirm the missing ipdsc partition + that ONLY the partner is missing (pipeline else healthy)
gcloud storage ls "gs://mntn-data-archive-prod/ipdsc/dt=2026-07-27/data_source_id=51/"   # -> no objects
gcloud storage ls "gs://mntn-data-archive-prod/ipdsc/dt=2026-07-27/"                       # -> 16 other DS present
# 2. Root cause = check the PARTNER SOURCE for date D-1 (offset 1)
gcloud storage ls "gs://mntn-data-partners/partners/bombora/segments/20260726/"            # -> no objects = Bombora didn't deliver
gcloud storage ls "gs://mntn-data-partners/partners/bombora/segments/20260725/"            # -> delivered fine (contrast)
```
**07-27 evidence:** DS51 partition absent; 16 other sources present (2,4,8,13,14,16,17,18,19,35,42,43,46,47,49,63); Bombora source `20260726/` absent (07-25 present, 07-24/07-27 also absent → Bombora feed is intermittent).

**Decision tree for this alert next time:**
1. Source dir for D−1 **absent** → benign optional-partner skip. Ack, reply, done. (this case)
2. Source dir **present** but ipdsc partition absent → the `ipdsc_bombora` builder failed with a real error. Check the `tpa_ipdsc_export` run's `ipdsc_bombora` task log; re-run it (or mark-success to ship without it — export tolerates `cats: []`). **Batch-id trap:** to re-run `tpa_export`/`ipdsc_geo`, clear the paired `create_batch_id*` task WITH its downstream, else it silently reattaches to the old batch. `ipdsc_<partner>` tasks are immune (try_number in batch id).
3. Partition present *now* → it landed late; clear `precondition_bombora` to pass.

**Late-arriving recovery (partner file shows up after export shipped):** trigger a NEW manual
`tpa_ipdsc_export` run with params `{"dt":"<YYYY-MM-DD>","force_export":true}`. Do NOT task-clear the
original run (params can't change on an existing run; `force_export` stays false → no-op).

**If this pages too often:** the durable fix is to make `ipdsc_monitor`'s DS51 precondition tolerate
skips (e.g. `soft_fail=True` on optional partners' preconditions) so it stops paging on expected skips.
That's a `airflow-ti` code change owned by the TPA_EXPORT / AUDI team — propose it, don't hot-patch.

---

## 4. System reference (producer → consumer maps as we learn them)

**IPDSC / TPA export chain (team TPA_EXPORT, `airflow-ti`)**
```
Bombora vendor drop ──▶ gs://mntn-data-partners/partners/bombora/segments/<D-1>/   (source, optional)
        │  wait_bombora_src (1h, reschedule, soft_fail)  [tpa_ipdsc_export @ 02:35 UTC]
        ▼
ipdsc_bombora builder ──▶ gs://mntn-data-archive-prod/ipdsc/dt=<D>/data_source_id=51/_SUCCESS
        │                                                   ▲
        ▼                                                   │ polls (18h, hard-fail)  [ipdsc_monitor @ 00:05 UTC]
run_geo ──▶ tpa_export ──▶ external table bucket           precondition_bombora  ← ALERTS here
```
- Mandatory data sources (DS4, DS17, …) are never tolerated — a missing mandatory partition
  hard-fails `tpa_export`. Only optional partners (currently just Bombora/DS51) skip silently.
- `ds17` sources ShareThis at `gs://mntn-data-partners/partners/sharethis/segments/date=<D-1>/`.
- Full DS id → vendor map + ipdsc query tips: `knowledge/data_catalog.md` (`bronze.external.ipdsc__v1`, DS-id legend).

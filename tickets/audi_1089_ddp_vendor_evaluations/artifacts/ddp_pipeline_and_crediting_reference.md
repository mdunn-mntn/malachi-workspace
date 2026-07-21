# DDP usage-reporting pipeline & crediting logic — reference (for owning + changing it)

Source: `SteelHouse/bae-sql-utility/ddp/usage reporting` (the 1,008-line main script; saved locally at
`artifacts/ddp_scripts/ddp_usage_reporting_main.sql`), read 2026-07-21. This is what BAE (Sherwin/Maya) run
each month. Runs in `dw-main-gold` project. Dates are **hardcoded and manually edited each month** (a known
fragility — the script even has stray leftover dates like `'2025-06-30'`, `'2020-03-01'`).

## The pipeline in 5 steps

- **Step 0 — reference tables.** From `integrationprod.direct_data_partners`, filtered to `is_current AND
  external_reporting_required` and valid-in-month, split into **TPA/interests**, **CRM**, **MM** tables. Keeps
  `report_data_source_id`, `credit_data_source_id`, `data_source_id`, `fixed_cpm`. Also builds
  `..._mm_credit` = per credit-dsid, the array of report dsids + `credit_divisor` — this is how **DS28 + DS40
  (both 33Across) are treated as ONE credit**.
- **Step 1 — impression↔category matches.** From `enriched_impressions` (the persisted intermediate,
  `mntn-analytics-prod-01`), keeping only **`campaigns.channel_id=8 (CTV) AND funnel_level=1 AND objective_id=1
  (Prospecting F1)`** — the confirmed billing scope. Unnests `category_info` JSON → `data_source_category_id`,
  `and_seq`, `or_seq`. Split TPA / CRM (ds 4) / MM (ds 13,19).
- **Step 2 — CPMs.** TPA: LiveRamp variable CPM per category, ShareThis fixed. CRM & MM: join
  **`external.targeted_signal`** (ip + category, **30-day lookback**: `ts.dt BETWEEN cil.dt-30 AND cil.dt`) to
  get `source_data_source_id` (the originating vendor, **including free logs 23/30**). MM aggregates
  `mm_dsids = array_agg(distinct report_dsid)` and `cpm = max(fixed_cpm)` (= $0.50 if any paid vendor present).
- **Step 3 — WINNERS (the AND/OR logic).** `ddp_winners` picks, per impression:
  - **OR → lowest CPM** wins (`or_cpm_seq = rank() ... order by tv_cpm ASC`, keep rank 1). A $0 provider wins its OR group.
  - **AND → highest CPM** wins (`and_cpm_seq = rank() ... order by tv_cpm DESC`, keep rank 1).
  - `ddp_winners_imp.impression_cnt = 1.0 / count(*) over (ad_served_id)` — the impression split across winning **providers** (MM vs LiveRamp vs ShareThis vs CRM).
  - `ddp_mm_winners_imp` adds `mm_dsids_winner` (all winning source dsids, **incl. free logs**) and `mm_dsid_count`.
- **Step 4 — per-vendor usage.** For each MM vendor (28/40/24/33/36), sum its share of the impression and
  bill `usage = ceil(impressions)/1000 × $0.50` (hardcoded $0.50). Domains with <1000 impressions → NULL
  (privacy threshold). Final rows land in `bronze.coredw.usage_reporting_data` with `status='In Progress'`.

## How free logs are handled TODAY (confirmed in code — reconciles our whole analysis)

The per-vendor MM credit (Step 4) is:
```
impression_share = (w.impression_cnt / w.mm_dsid_count) / <count of this credit's own dsids among winners>
```
- **`mm_dsid_count` = count of distinct credit-vendors among the winners, INCLUDING free logs (guid 23, augmentor 30).**
  So free logs **take unpaid slots in the divisor** — they *dilute* every paid vendor's share (bigger N) but
  the free logs have no usage output table, so they're never paid.
- **Net: a paid vendor STILL earns `share × $0.50` on an impression a free log also covered** — it just earns a
  smaller share. That is exactly why `ddp_mm_winners_imp.tv_cpm=$0.50` on free-covered impressions in our proof:
  the $0.50 is the paid vendor's *residual* rate; the free log's slot is $0.
- So **"we already filter out free logs" (Sherwin) = the free logs take $0 slots — NOT that paid vendors get
  zeroed.** Our ~$275K (same-visit) / $412K (vertical) is the **incremental** saving from FULL preemption.

## Where to change it for FULL free-log preemption (the ownership goal)

The single lever: in `ddp_mm_winners_imp` (or the Step-4 per-vendor usage), when `mm_dsids_winner` contains a
free log (23 or 30), **set the paid vendors' `impression_cnt` for that `ad_served_id` to 0** instead of the
`1/mm_dsid_count` share. Concretely, add to the winners/usage a guard like:
```sql
-- full preemption: if a free log won this impression, paid vendors get nothing
... CASE WHEN EXISTS(SELECT 1 FROM UNNEST(mm_dsids_winner) d WHERE d IN (23,30)) THEN 0
         ELSE (w.impression_cnt / w.mm_dsid_count) END ...
```
That's the whole change — everything upstream (matches, winners, domains) stays. Free logs = guid + augmentor
together (per Malachi; don't split out augmentor). This needs Andy Everson's contract-terms blessing before it ships.

## Companion scripts (saved in `artifacts/ddp_scripts/`)
- `usage_reporting_audits.sql` — the month-over-month variance audit (3 gates: usage-diff %, impression-delta, $ increase) → `coredw.usage_reporting_audits`.
- `ddpmonthlyemail.py` — orchestrator that runs the per-vendor email scripts (`ddpmonthlyusageemail-<Vendor>.py`) → emails partner reports from `partnerbilling@`.

## Gotchas for whoever owns it
- **Dates are hardcoded per run** and manually edited (stray leftover dates present) — a first candidate to parameterize.
- MM is priced at **`max(fixed_cpm)`** and TPA/CRM winners at **min (OR) / max (AND)** — subtle; the $0.50 is also hardcoded in Step 4, so a rate change means editing two places.
- CRM matches with no `targeted_signal` record are inserted at CPM 0 (so they still win OR groups at $0).
- No one owns the *logic* today; BAE executes it. Andy Everson owns vendor contracts + flat-fee $ (not visible to BAE).

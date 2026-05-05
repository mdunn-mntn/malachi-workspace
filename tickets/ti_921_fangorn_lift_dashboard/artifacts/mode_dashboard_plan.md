# Mode Dashboard Plan — Fangorn Lift Monitor

**Status:** Draft. Discuss with Alex at 2026-05-06 handoff meeting; refine before building.

---

## What the dashboard exists to do

1. **Live monitoring** — once a cohort flips, anyone in the org can pull up the dashboard and see its KPI movement vs pre-period without pinging Malachi/Alex.
2. **Cross-cohort comparison** — when Tier-2 starts flipping, see whether Wave 2's lift looks different from Wave 1's at the same days-since-flip.
3. **Durable archive** — past experiments and rolled-back cohorts live here permanently. This is the "shut it off but keep results" pattern. No more results scattered across Slack threads, decks, and notebooks.

The third point is what makes a dashboard worth building (vs just emailing pre/post tables). It's the one thing the team has consistently lacked.

---

## Three views

### View 1: Live cohort overview
**Default landing page.** One row per (cohort, advertiser).

| Column | Source |
|---|---|
| Cohort label | wave_config.csv |
| Advertiser name | wave_config.csv → advertisers.company_name |
| Flip date | wave_config.csv |
| Days since flip | computed |
| Maturity (D+1..D+27 / Mature) | computed: 4-week rule per TI-780 |
| Impressions pre / post / Δ% | pre_post.csv |
| IVR pre / post / Δ% (with conditional formatting) | pre_post.csv |
| CVR pre / post / Δ% (muted if no pixel) | pre_post.csv + wave_config.has_conversion_pixel |
| ROAS pre / post / Δ% (muted if no $) | pre_post.csv + wave_config.has_dollar_value |
| CausalImpact rel_effect (IVR) ± 95% CrI | ci_results.csv |
| CausalImpact p-value (IVR) | ci_results.csv |

Filters: cohort, vertical, maturity. Sort: pct change descending.

### View 2: Advertiser drill-down
Pick an advertiser → see:
- Daily IVR / VVR / CVR / ROAS / CPV / CPA series, pre + post, with flip-date marker.
- The CausalImpact plot per metric (actual vs counterfactual, point effect, cumulative effect).
- Pre/post summary table for that one advertiser.
- Notes (from `wave_config.csv` → `notes` column, e.g., "no conversion pixel").

This is what someone asks for when "the live overview shows a big drop for advertiser X — what happened?"

### View 3: Archive
Frozen rows for closed-out cohorts. Schema mirrors View 1 but adds:
- Status (`Active`, `Mature - Live`, `Mature - Closed Out`, `Rolled Back`).
- Final readout link (Confluence / artifact path).
- Decision (`Continue rollout`, `Stop`, `Iterate on model`).
- Closeout date.

When a cohort matures and we close it out, copy its View-1 row into the archive table with the final values frozen. Don't recompute the archive when daily data refreshes — it's a snapshot.

---

## Data plumbing

```
[Daily run, Databricks notebook]
  ├── outputs/ti_921_panel.csv          → BQ scratch table or direct Mode query
  ├── outputs/ti_921_pre_post.csv       → BQ scratch table or direct Mode query
  └── outputs/ti_921_ci_results.csv     → BQ scratch table or direct Mode query

[Mode]
  ├── live_cohort_overview               (joins above three CSVs / tables)
  ├── advertiser_drilldown               (parameterized on advertiser_id)
  └── archive                            (manual, append-only)
```

Two patterns to choose from:

**A. Mode queries hit BigQuery directly.** Notebook writes daily aggregates to a dedicated BQ scratch dataset (e.g. `dw-main-silver.scratch.ti921_*`); Mode queries those tables. Pro: refresh is automatic. Con: requires write access to a BQ scratch dataset, which we may not have configured.

**B. Notebook produces CSVs; uploaded to Mode datasets.** Pro: zero data-platform dependencies. Con: manual upload step, easy to forget.

Recommendation: pattern A if scratch-write access is available; otherwise pattern B with the notebook scheduled in Databricks (writes CSVs to a known location) and a Mode-side daily-refresh dataset reading from that location.

---

## Build order (3 phases)

1. **Phase 1 — Live overview** (1-2 days). Start with View 1 only, fed by the existing Databricks notebook output. Get one cohort showing real numbers. Validate with the team.
2. **Phase 2 — Drill-down** (1-2 days). Add View 2. Most of the work is parameterizing queries on `advertiser_id` and embedding the CausalImpact PNGs.
3. **Phase 3 — Archive** (1 day). Define the schema; build a Mode form that lets us close out a cohort with one click (writes the frozen row).

Don't try to build all three at once. View 1 is the highest-leverage piece — it answers the "did Fangorn work?" question that everyone is asking. Views 2-3 layer on once the live data is real.

---

## Questions to resolve at handoff

1. Mode workspace: which one (TAR? Data Eng?). Do we have permissions?
2. Pattern A vs B above — does the team have a BQ scratch dataset for derived analytics tables?
3. Embedding CausalImpact plots in Mode — Mode supports image URLs from S3/GCS. Where would the notebook write them?
4. Slack alert hook — do we want Mode to fire a Slack message when a row crosses the 20% threshold? Easy if Mode supports webhooks; not worth building if it doesn't.
5. Archive schema — fields above are a draft. Anything missing that you'd want to look up six months from now?

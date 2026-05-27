# TI-956: Scheduled Interest Segment Scoring Job

**Jira:** https://mntn.atlassian.net/browse/TI-956
**Status:** In Progress
**Date Started:** 2026-05-27
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction
Make LiveRamp interest-segment quality + performance scores available on a
recurring schedule, written to a GCS path. Not a production deployment — a
scheduled job that runs Alex's scoring class on a weekly or monthly cadence so
consumers (Macie, downstream UI/admin tooling) don't have to ask Alex to rerun
the notebook every time.

Source code: [targeting-infra-ml#57](https://github.com/SteelHouse/targeting-infra-ml/pull/57)
— quality scoring utilities + example notebook. Class is PySpark, ~two lines
to invoke the full scoring pipeline given the input dataset.

Origin: ask from Paulo + Allison to "use interest segments more." Allison +
Alex first proposed classifying by keywords/verticals; Paulo asked for
"something more nuanced." This scoring framework is the nuanced answer.

Confluence design docs (saved as offline copies in `artifacts/`):
- [Liveramp Segment Quality Steps - Archive](https://mntn.atlassian.net/wiki/spaces/TAR/pages/3274506251/Liveramp+Segment+Quality+Steps+-+Archive) (page id 3274506251) — initial 7-component design.
- [Liveramp Segment Quality Steps - Update](https://mntn.atlassian.net/wiki/spaces/TAR/pages/3523477544/Liveramp+Segment+Quality+Steps+-+Update) (page id 3523477544) — **current canonical version, supersedes Archive**. Adds targetability + performance components; switches final mapping from percentile rank to sigmoid of standardized z_combo.

## 2. The Problem
- LiveRamp = ~90% of MNTN's interest segments. There are 200k+ interest segments total.
- Search for "households with $100K+ income" returns 90 segments — search is improved (Jeff Capone's AT demo) but there's **no quality signal**.
- Today the UI only surfaces segment size. Users have no way to pick the best one of 90.
- 11Lab is the first concrete use case: Mike asked Alex to score 150 interest segments to drop ~10 garbage ones and add ~20 BUK keywords while maintaining audience size (theory: keyword targeting > interest segments at the margin).

The scoring framework exists in a notebook. It needs to run on a schedule and
write to GCS so the rest of the org can build on top of it. Initial consumer
is admin users in the MNTN UI.

## 3. Plan of Action
1. Read [targeting-infra-ml#57](https://github.com/SteelHouse/targeting-infra-ml/pull/57)
   — `utils/segment` + the example notebook Alex will add.
2. Understand the inputs the scoring class needs (input dataset construction).
3. Decide hosting (per Alex: defer to me, consult Victor / Ryan):
   - Databricks scheduled notebook, OR
   - Vertex AI job triggered by Airflow DAG, OR
   - Regular PySpark on GCP compute in an Airflow DAG.
4. Define the schedule: **weekly or monthly** (LiveRamp segments update on
   LiveRamp's cadence — not daily). Monthly likely sufficient; confirm with Alex.
5. Define the GCS output path + schema. Coordinate with Macie on consumer expectations.
6. Run once end-to-end, verify outputs against Alex's manual notebook run.
7. Stand up the recurring schedule.

## 4. Investigation & Findings

### Scope
- ~252,075 LiveRamp segments total. Goal: produce a 0–100 quality score per `dscid` (data_source_category_id) on a recurring schedule.

### Sampling foundation (Bernoulli + Horvitz–Thompson)
Full IPDSC explode (date × IP × dscid) is prohibitively expensive. The pipeline keeps a Bernoulli-sampled subset of edges at rate **p = 0.0001** and applies HT-style weights so each sampled row stands for many unsampled rows.

- Row kept iff `u01(ip, dscid[, day]) < p`. `hash_scope='edge'` reuses the same draw across days; `edge_day` draws independently per day.
- `build_edges_with_weights_estimator_only` computes:
  - `m_hat = k_samp / p` — estimated total edges per (ip, day)
  - `pi_ipday = 1 − (1−p)^m_hat` — HT inclusion probability
  - `rep_ipday = 1 / max(pi_ipday, floor)` — IP-day weight (with optional cap)
  - `p_edge` carried per edge for segment-reach sums (`1/p_edge`)
- Pair-level intersections weight by `1/p²`.

### Scoring components (canonical Update version, 9 axes)
Each axis is computed per `dscid`, then transformed (log1p / logit / −log1p(CV)) so direction is "higher is better," z-scored across the cohort (winsorized at ±5σ, nulls → z=0), and combined.

| Axis | Default weight | What it measures | Where computed |
|------|---:|---|---|
| activity | 20.0 | Sum of daily HT reach over 30d window. Weak signal if a segment barely appears. | `segment_quality_utils/reach.py` |
| stability | 9.0 | Coefficient of variation (sd/mean) of daily reach over 14d. Low CV = steady. Composite uses `−log1p(cv14)`. | reach.py |
| share | 5.0 | Σ reach_hat / Σ N_ip_hat over the window. Avoids "ubiquitous" segments dominating. Composite uses `logit`. | reach.py |
| uniqueness | 25.0 | Mean Jaccard to top-5 neighbors (intersection-ranked from both sides). `uniqueness_unit = 1 − mean_topk_jaccard` then logit. | `segment_quality_utils/uniqueness.py` |
| sample | 9.0 | Effective sample size proxy: distinct sampled (dscid, ip, event_date) hits. Fidelity check against HT extrapolation. | reach.py |
| staleness | 2.0 | Exponential decay from last `updated_date` (90d half-life), fallback to `created_date`. `deprecated=True` → 0. Missing date → 0. | `segment_quality_utils/attributes.py` |
| specificity | 30.0 | IDF: treat each IP-day as a document, each segment as a term. `idf = log((N+1)/(k+1))`; `idf_norm = idf / log(N+1)`. Rare segments score higher. | attributes.py |
| **targetability** | **20.0** | HT-weighted reach restricted to IPs in `targetable_ips_df` ÷ total HT reach for the segment. Filters "waste" reach outside our addressable graph. (**NEW vs Archive**) | attributes.py |
| **performance** | **12.0** | Optional. Association of segment with campaign delivery outcomes (visit lift, conversion lift, consistency, support, specificity-of-targeting). Skipped if `performance_df` / `campaign_segment_targets` missing → `z_performance = 0`. (**NEW vs Archive**) | `segment_quality_utils/performance.py` |

### Final composite mapping (Update version)
```
z_combo      = Σ weight_i * z_i           # weighted sum of axis z-scores
z_combo_std  = standardize(z_combo)        # within cohort
quality_score = round(100 * sigmoid(z_combo_std / 1.5), 1)
```
Archive version used percentile rank of `z_combo`; the Update switched to sigmoid so extreme cohorts don't pile at 0 or 100.

### Performance layer mechanics (when inputs present)
1. **`build_campaign_segment_targets_panel`** — join active campaigns to their segment target rows; assign `target_weight` by crowdedness (`equal | 1/n | 1/√n`). Derive visit rate, conversion rate, post-visit CVR, ROAS, cost metrics.
2. **`add_campaign_performance_lifts`** — for each campaign, compare rates to **advertiser-level leave-one-out baseline** (other campaigns of the same advertiser, excluding this one; fallback to global). Clipped log lifts to limit outlier leverage.
3. **`summarize_performance_by_segment`** — roll up to `dscid`: weighted-avg clipped log lifts, medians, dispersion, advertiser/campaign counts, "campaign_equiv" mass, spend/impression support, top-advertiser concentration (HHI), targeting specificity.
4. **`performance_score_per_segment`** — map features to unit scores `u_*` (sigmoid of median log lifts, consistency, support, targeting specificity) → logit → cohort z → weighted `z_perf_combo`. Apply `perf_diversity_confidence = 1 − top_advertiser_campaign_equiv_share` to down-weight scores dominated by one advertiser. Final `performance_blend_signal = z_perf_combo_std × perf_diversity_confidence` becomes `x_performance` in the main composite.

Performance sub-weights (inside the performance score, not the main composite):
| Sub-component | Default |
|---|---:|
| visit_lift | 35.0 |
| conversion_lift | 27.0 |
| consistency | 18.0 |
| support | 12.0 |
| specificity (crowded targeting) | 8.0 |

**Caveat (Alex's own callout in the doc):** these are lifts vs LOO/global baselines, not randomized holdouts. Selection effects (budget, creative, geo) can drive segment-vs-baseline differences. Performance is a **supportive** signal, not a causal one.

### Code layout (Update doc)
| Topic | File |
|-------|------|
| Sampling (Bernoulli, HT panel) | `sampling_logic.py` (a.k.a. `sampling_quality_sampling.py`) |
| Shared helpers, default weights | `segment_quality_utils/base.py` |
| Reach / activity / volatility / share / ESS | `segment_quality_utils/reach.py` |
| Top-k Jaccard | `segment_quality_utils/uniqueness.py` |
| Specificity, staleness, targetable share | `segment_quality_utils/attributes.py` |
| Performance panels / scores | `segment_quality_utils/performance.py` |
| End-to-end composite | `segment_quality_utils/composite.py` |
| Notebook façade | `segment_quality_utils/facade.py` |

Entry point: `ThirdPartySegmentQuality(panel).quality_score_per_segment(...)` — accepts `weights`, `targetable_ips_df`, performance inputs.

### Inputs the scheduled job will need
1. **Panel** — output of `build_edges_with_weights_estimator_only` over IPDSC for the chosen window (30d primary, 14d for volatility). Sampling rate p=0.0001.
2. **`seg_meta_df`** — from `bronze.tpa.categories` (or equivalent): `data_source_category_id`, `updated_date`, `created_date`, `deprecated`. Needed for staleness.
3. **`targetable_ips_df`** — set of IPs in MNTN's addressable universe. Needed for targetability axis (default weight 20.0). Source TBD — likely an aggregate from bid/audience tables.
4. **`performance_df` + `campaign_segment_targets`** — optional. Joins active campaigns with their target segments + delivery KPIs (spend, impressions, visits, conversions, revenue). If missing, performance axis is skipped (`z_performance = 0`).
5. **Sample notebook** — Alex will add a usage notebook to the repo demonstrating the end-to-end call.

### Hosting trade-offs
- **Databricks scheduled notebook** — fastest path, easiest re-run. Alex has been working in Databricks; the notebook lives at `https://1262887251702944.4.gcp.databricks.com/editor/notebooks/1904079882625393`. Downside: less standard for our production pipelining.
- **Vertex AI + DAG** / **Airflow + PySpark on GCP compute** — closer to how production pipelines should live. Alex's hint: "good opportunity for you to learn and build some of that pipelining stuff." Talk to **Victor or Ryan** before committing.

### Malachi suggestions for additional metrics (not yet in v1)
- **Visits per user** over 30-day window
- **Impact-weighted ranking** — for campaigns currently using interest segments, what would be the perceived improvement if they switched to the #1-ranked alternative? Helps prioritize which scores to surface where.
- **Filter aggressively in the UI** — don't show users 90 choices. Use the quality score to cut low-quality segments before surfacing.

## 5. Solution
_Pending._

## 6. Questions Answered
- **Q:** Why does this need a schedule rather than ad-hoc reruns?
  **A:** Consumers (Macie + admin UI) need fresh scores without asking Alex. LiveRamp segments update on their cadence — not daily, but they do update.
- **Q:** Daily / weekly / monthly?
  **A:** Weekly or monthly. Not daily — LiveRamp doesn't change that fast.
- **Q:** Where does output go?
  **A:** GCS path. Macie will consume it to build an admin-user-facing surface in the MNTN UI.
- **Q:** Is this production-grade?
  **A:** No. Interim step — get scores accessible to admin users. Productionization is a later ticket.
- **Q:** Where does the scoring code live?
  **A:** `targeting-infra-ml` PR #57, `utils/segment`. PySpark class, two lines to invoke.

## 7. Data Documentation Updates
_Pending._
- Possible: add LiveRamp interest segment scoring inputs/outputs to `knowledge/data_knowledge.md` once schema is settled.

## 8. Open Items / Follow-ups
- Decide host (Databricks vs Vertex+DAG vs Airflow+PySpark) — consult Victor / Ryan.
- Confirm GCS output path with Macie.
- Confirm weekly vs monthly cadence with Alex.
- Should the v1 output include impact-weighted ranking metrics, or hold for v2?
- **Identify `targetable_ips_df` source.** Default composite gives targetability weight 20.0 — second-highest after specificity. Without it, scores will be biased. Likely candidates: aggregated bid_logs / event_log IPs over the same window, or a curated audience-platform list. Confirm with Alex + Zach.
- **Decide whether to run the performance layer in v1.** Needs `performance_df` (advertiser_id, campaign_id, spend, impressions, visits, conversions, revenue) and `campaign_segment_targets` (advertiser_id, campaign_id, dscid). If we skip it, `z_performance = 0` and the composite still works. Including it adds 12.0 weight and the causal caveat Alex flagged in the doc.
- **Pin `tpa.categories` schema** for staleness (need `updated_date`, `created_date`, `deprecated`).
- **Output schema** — at minimum: `dscid`, `quality_score`, all `z_*` axes, raw component values (`reach_hat_30d`, `cv14`, `avg_share_30d`, etc.) for debugging/audits.

## 9. Meeting Notes
- `meetings/ti_956_01_malachi_alex_catchup_2026_05_27.txt` — same Malachi + Alex catchup. Interest-segment scoring is the second half of the meeting.
- **Next step:** Alex to add a usage-notebook example to the repo so Malachi can read through it. Tech deep-dive on hosting + schedule scheduled for early next week.

## Acceptance Criteria (from Jira)
- Data pipeline running which generates LiveRamp interest segment scores.

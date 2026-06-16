# TI-1027: [SPIKE] 5x5 Data Evaluation

**Jira:** https://mntn.atlassian.net/browse/TI-1027
**Status:** In Progress
**Date Started:** 2026-06-16
**Date Completed:** —
**Assignee:** Malachi

---

## 1. Introduction
Our contract with the data provider **5x5 (DS 25)** expires **end of June 2026**. Leadership (Kale McNaney; ticket by
Alyson Lefkowitz) wants a **renew / drop / renegotiate decision** backed by an **estimate of value**. 5x5 is a DDP
(direct data partner) that delivers **IP → URL** site-visit signals feeding **MNTN Matched** (vertical classification
→ feature store → MM scoring).

**Kale's framing (the spine):** *"Quantify/estimate just the measurable bits to start. 5x5 accounts for a certain %
of raw data — is its impact on Fangorn/MNTN Matched **outsized relative to its scale, or in line**? How do we estimate
the value of Fangorn/MNTN Matched? It's an estimation exercise."* → Headline metric = **leverage ratio**
(contribution_share ÷ data_share); dollars come from valuing MM, then attributing 5x5's slice.

**Sean Yang (pipeline owner) decision bar + action loop:** *"5x5 is just one of several sources into
site_visit_signal. If their data is unique with minimal overlap with other vendors we should keep them… let me know
either way so I will adjust the DAG."* → Our rec drives Sean toggling `25` in/out of `ENABLED_DSIDS`.

## 2. The Problem
- Is 5x5's flat-fee data worth renewing? What breaks if we drop it?
- Ticket observations to verify: (a) "only sending domain, not extended URL — can't tell what person is looking at";
  (b) "some data looks like quality websites, rest are total garbage."
- Hard deadline: contract ends end of June 2026.

## 3. Plan of Action
Per approved plan (`~/.claude/plans/read-ti-1027-…md`), Kale's estimation chain:
0. **Phase 0** — clear blockers (cost structure/amount; delivery recency) + confirm lineage.
1. **Phase 1 SCALE** — each vendor's share of site_visit_signal (records / IPs / domains) → 5x5 `data_share`.
2. **Phase 2 QUALITY** — ecommerce/vertical classifier on 5x5 domains → % whitelist vs garbage, % classifiable.
3. **Phase 3 CONTRIBUTION vs SCALE** — overlap DS25 vs internal DS30∪DS23 + other DDPs; net-new; **leverage ratio**.
4. **Phase 4 VALUE OF MM** — MM-touched revenue + Fangorn lift band (the denominator).
5. **Phase 5 SYNTHESIS** — attribute 5x5 slice vs flat fee + break-even; verticals impacted; keep/drop/renegotiate;
   notify Sean.

## 4. Investigation & Findings

### 4.1 Confirmed lineage (from `SteelHouse/airflow-ti`)
**Raw feed → processing → unified signal → MM:**
- **Raw 5x5 feed:** `gs://mntn-data-partners/partners/5x5/ip_to_url/y=YYYY/m=MM/d=DD/h=HH/*.snappy.parquet`.
  Cols `_COL_0`=ip, `_COL_1`=url, `_COL_2`=epoch(sec). Delivered in **~2-hour batches** (not hourly).
- **Processing:** `spark/fpa/dsid25_5x5_processing.py`, DAG `fpa_site_visit_batch_serverless` (`@hourly`, Dataproc
  serverless, **5-hour lag** for DS25). `ENABLED_DSIDS = [23, 25, 26, 28, 30, 36]`.
- **Stage 1 (raw archive):** `gs://mntn-data-archive-prod/fpa_vendor_log/data_source_id=25/` (partitioned dt,hh).
- **Stage 2 (unified):** `gs://mntn-data-archive-prod/signals/site_visit_signal/dt=…/hh=…/data_source_id=25/`.
  Shared schema across all vendors: `uid, advertiser_id, ip, url, query_parameters, user_agent, time, data_source_id,
  dt, hh`. **Separable by `data_source_id` — trivial.**
- **Consumers:** `distinct_site_visit_signal_domains.py` (31-day read; regex-strips url to `protocol+domain`;
  **excludes DS23**, includes DS25) → `ddp_vertical_classification_api` / `update_website_verticals` →
  feature store `site_visit_signal_advertiser_id_dsc_id` → `mntn_match_incrementals_submit` (MNTN Matched).
- **BQ landing caveat:** `…zzz_temp.site_visit_signal` is **manual / not auto-populated** (populate trigger commented
  out). Query GCS parquet directly via temp external-table definitions (read-only; no DDL/DML).

### 4.2 site_visit_signal vendor set (who feeds MM via this table)
- **Internal:** DS23 guid_log (MNTN pixel; *excluded from vertical classification*), DS30 augmentor_log (bidstream).
- **External DDP:** DS24 Justuno, **DS25 5x5**, DS26 Predactiv, DS28 33Across, DS33 Sovrn, DS36 Cybba, DS39 Klickly,
  DS40 33Across API. (Observed in partitions; 24/33/39/40 arrive via pixel_page_view_signal backfill workflows.)

### 4.3 Cost structure (`dw-main-silver.tpa.direct_data_partners`, is_current=true) — Phase 0 blocker #1
- **5x5 (DS25): `billing_type = flat_fee`, `fixed_cpm = null`, `used_in_mntn_match = true`, `used_in_interests =
  false`, enabled.** Confirms Alyson: **flat fee** (fixed cost, marginal cost zero), feeds MNTN Matched only.
  Note field: *"we provide report but only impression counts- unknown if this was shared with the customer."*
  Current contract row `valid_from = 2025-10-17`.
- **Peer-rate benchmark for MM DDP data = $0.50 CPM:** 33Across (28), 33Across API (40), Cybba (36), Sovrn (33),
  Justuno (24), and the **disabled** LaunchLabs (27) all bill `fixed_cpm = $0.50`. Predactiv (26) + Klickly (39) are
  also `flat_fee`. → I can build a **break-even** before Sherwin's number: *what would 5x5 cost at $0.50 CPM for the
  impressions its signal drives?*
- **Cost AMOUNT still needed** — flat fee $ not in the table. **→ Ask Sherwin (billing).** Interest-side providers
  (LiveRamp 11/35 variable_cpm, ShareThis 17 $0.95→ was $1.20, Dstillery 18) are not MM and out of scope.

### 4.4 Delivery is live (answers Sean's "are they still dropping data?")
- Raw feed delivering **through today, 2026-06-16** (daily, ~2-hr batches). DS25 site_visit_signal partitions present
  on complete days (e.g. 2026-06-15 hh=00, hh=10). Gaps in some hours = the 2-hr batch cadence + 5-h processing lag,
  not a failure. **Off-switch if we drop:** remove `25` from `ENABLED_DSIDS` (Sean owns).

### 4.5 Raw-feed content — both ticket claims need nuance (sample, 2026-06-15 h=00)
First 6 rows showed:
- **Full URLs WITH paths exist** — e.g. `https://screenrant.com/walking-dead-streets-survival-new-game-release/`. So
  "they only send domain" is **not strictly true**; quantify % with path. (Even so, the vertical classifier strips to
  domain anyway — so path richness is moot for the MM path unless a URL-level consumer exists.)
- **Garbage is real** — `66.249.77.195` = **Googlebot** crawler IP; `widgets.outbrain.com` = ad widget;
  `analytics.o11.tech` = analytics tracker. Bot/infra/tracker noise present → Phase 2 must quantify the garbage share.

### 4.6 Data volumes (windowing decision)
- Full `site_visit_signal` ≈ **250 GiB/day** (dominated by internal DS23/DS30). Raw 5x5 ≈ **1.48 GiB/day**.
- Approach: BQ temp external tables reading only `ip`/`url`/`data_source_id`, scoped windows (start ~7–14d for
  scale/overlap; extend domain-uniqueness to the classifier's 31-day window for the final number). Databricks fallback
  if a single query gets unwieldy.

## 5. Solution
_TBD — recommendation pending Phases 1–5._

## 6. Questions Answered
- **Q:** Where does 5x5 data land / is it separable? **A:** Raw `partners/5x5/ip_to_url/` → `fpa_vendor_log` +
  `site_visit_signal` (DS-keyed). Separable by `data_source_id=25`. Confirmed live through 2026-06-16.
- **Q:** What's the cost structure? **A:** Flat fee (not CPM). Peer MM-DDP rate is $0.50 CPM. Amount pending Sherwin.
- **Q:** Domain-only? **A:** Partially false — full URLs with paths exist in the raw feed; quantifying share. Moot for
  the vertical-classification consumer (strips to domain).

## 7. Data Documentation Updates
_Pending — will add: 5x5/DDP→site_visit_signal lineage, `direct_data_partners` schema + billing types,
`zzz_temp.site_visit_signal` manual caveat, vertical-classifier domain-strip; fix stale "DS25 = no current use"._

## 8. Open Items / Follow-ups
- **Blocker:** 5x5 flat-fee amount ← Sherwin (billing).
- **Ask:** Ryan Kleck for TI-647 exact match-rate method (mirror for apples-to-apples).
- **Deferred (Kale):** full causal ablation (re-run MM with/without DS25 → ΔIVR → ΔRevenue) — only if 5x5 proves
  outsized/non-redundant.
- **Action loop:** deliver rec → notify Sean to keep/remove `25` from `ENABLED_DSIDS`.

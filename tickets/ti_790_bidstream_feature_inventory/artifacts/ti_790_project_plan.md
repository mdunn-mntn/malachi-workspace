# TI-789: Bidstream Feature Store — Project Plan

**Epic:** [TI-789](https://mntn.atlassian.net/browse/TI-789) — Bidstream Feature Extraction & Audience Augmentation
**Team:** Malachi Dunn, Alex Knorr, Ryan Kleck | **PMO:** Bryce Wagg
**Created:** 2026-03-30

---

## Objective

Extract high-value features from MNTN's bidstream data and test them against IVR (Impression-to-Visit Rate) to identify which signals improve targeting performance. Winning features get integrated into Fangorn's feature store. Secondary goal: use bidstream signals to expand DS13/DS19 audience pools.

## Data Sources

| Table | What It Contains | Scale | TTL | Key Unique Fields |
|-------|-----------------|-------|-----|-------------------|
| `bronze.raw.augmentor_log` | Auctions we participated in | 1.2B rows/hr, ~241 GB/day | 10d BQ, ~30d parquet | `iab_categories` (30%), 40 inventory sources, `mntn_segments` |
| `bronze.raw.bidder_auction_events` | Auctions we saw but didn't bid on | 112M rows/hr, ~17 GB/hr | 90d BQ | `content_genre` (87%), `device_make` (90%), `content_series` (37%), `publisher_name` (100%) |
| Parquet archive | Both tables, longer retention | Same | ~30d+ | Same fields, accessible via `gs://mntn-data-archive-prod/` |

Full field profiling in [ti_790_feature_inventory.md](ti_790_feature_inventory.md).

## Feature Candidates (Tiered)

### Tier 1 — Test First
| Feature | Source | Fill % | Signal |
|---------|--------|--------|--------|
| content_genre | bidder_auction_events | 87% | What content they watch — strongest vertical signal. Not in Fangorn today. |
| device_type / device_type_group | both | 100% | CTV vs mobile vs desktop behavior profiles |
| device_make | bidder_auction_events | 90% | Roku vs Samsung vs LG — demographic proxy |
| inventory_source | augmentor_log | 100% | SSP identity (40 sources) — inventory quality signal |
| iab_categories | augmentor_log (bronze) | 30% | IAB content taxonomy — direct vertical mapping |
| network / publisher_name | both | 71-100% | Premium vs long-tail content consumption |
| placement_type | both | 100% | VIDEO vs BANNER |
| os | both | 97-99% | Platform signal (needs LOWER() normalization) |

### Tier 2 — Test Second
| Feature | Source | Fill % | Signal |
|---------|--------|--------|--------|
| content_series | bidder_auction_events | 37% | Specific show — granular. Needs cleanup. |
| content_channel | bidder_auction_events | 36% | Channel identity |
| content_network | bidder_auction_events | 38% | Network identity (structured) |
| app_name / app_bundle | both | 85-99% | App identity. High cardinality — needs bucketing. |
| pmp_deal_ids | both | 98% | Premium deal signal |
| geo_zip | bidder_auction_events | 95% | Geographic signal |
| domain | augmentor_log | 100% | Site identity. High cardinality. |

### Tier 3 — Skip
device_os_version (0% fill), site_domain/site_page (<1%), ifa (privacy-limited), referrer (4%), isp (10%), is_blocked (0% true).

## Known Data Quality Issues

| Issue | Affected Fields | Fix |
|-------|----------------|-----|
| Case inconsistency | os, content_genre, device_make | LOWER() or UPPER() everything |
| Comma-delimited multi-values | content_genre ("sitcom,comedy") | SPLIT + UNNEST |
| Provider-specific prefixes | content_genre ("GENRE_COMEDY") | Strip prefix, normalize |
| Hashed/template garbage | content_series ("d41d8cd9...", "{{CONTENT_SERIES}}") | Filter known bad patterns |
| Duplicate publisher names | network ("NBC Universal" x3 variants) | Mapping table |
| geo format varies | augmentor_log.geo (raw string) vs bidder_auction_events.geo_country (structured) | Parse or use structured |

## Evaluation Methodology

Per Matt Brorby's guidance — same approach used for Fangorn's existing feature selection:

### Step 1: Build Training Dataset
- Sample random IPs from bidstream (0.1% sample = ~1.2M rows/hr from augmentor_log)
- Aggregate features per IP (e.g., "this IP: 60% entertainment, 30% news, Roku, Magnite")
- Join to visit/conversion outcomes from `clickpass_log` + `impression_log` to compute IVR per IP
- Label: visited (1) or not (0) within attribution window

### Step 2: XGBoost Feature Importance
- Train XGBoost model with all Tier 1 + Tier 2 features predicting IVR
- Extract 3 importance metrics:
  - Information gain (how much each feature reduces loss)
  - Frequency (how often each feature is used in splits)
  - Weighted (gain weighted by coverage)
- Composite rank = average rank across all 3 methods

### Step 3: Iterative Paring
- Start with all features (~20-25)
- Drop lowest-ranked features
- Retrain and verify performance holds (AUC, precision/recall)
- Repeat until performance degrades
- Target: identify top 8-12 features that drive most of the signal

### Step 4: Fine-Tuning
- SHAP values on the trimmed model to understand feature interactions
- BIC (Bayesian Information Criterion) to balance fit vs complexity
- Simple group-by / linear regression for categorical features to validate direction of effect

### Step 5: Validation
- Holdout test set (time-split, not random, to avoid leakage)
- Compare IVR across feature-defined segments (e.g., "entertainment genre watchers" vs baseline)
- Verify lift is meaningful and stable across multiple days

## Workstream 2: DS13/DS19 Audience Augmentation

Runs in parallel once Tier 1 feature quality is understood:

1. **Incrementality check**: For a chosen vertical, pull IPs from bidstream with matching `content_genre` or `iab_categories`. Check how many are already tagged with the corresponding DS13 segment (`mntn_segments`).
2. **Predictiveness check**: Do the new (untagged) IPs visit the advertiser's site in the future? Compare visit rates of bidstream-tagged vs random IPs.
3. **Holdout experiment**: Use existing holdout logic — tag only a subset of IPs with the bidstream-derived segment. Measure IVR lift.
4. **Production integration**: If validated, union bidstream IPs into existing DS13/DS19 staging jobs.
5. **RTC exploration**: Can this logic run in real-time conquest (DS13 only)?

## Execution Plan

### Phase 1: Discovery & Normalization (Week of 3/30)

| Task | Owner | Ticket | Status |
|------|-------|--------|--------|
| Feature inventory & quality assessment | Malachi | [TI-790](https://mntn.atlassian.net/browse/TI-790) | Done |
| Vertical classification signals from bidstream | Alex | [TI-791](https://mntn.atlassian.net/browse/TI-791) | In Progress |
| OpenRTB spec investigation & test vertical selection | Ryan | [TI-792](https://mntn.atlassian.net/browse/TI-792) | In Progress |
| Find exchange reference table (`core.exchanges` or equivalent) | Ryan | TI-792 | Open |
| Build normalization mappings (genre, os, device_make, publisher) | Malachi | TI-790 | Open |

**Wednesday 4/2 deliverables:**
- Malachi: Feature list with quality metrics (done) + normalization mappings
- Alex: Signals valuable for vertical classification
- Ryan: OpenRTB field mapping + recommended test vertical + exchange reference table

### Phase 2: Training Dataset & Modeling (Week of 4/7)

| Task | Owner | Ticket | Dependencies |
|------|-------|--------|-------------|
| Build sampled training dataset (IPs + features + IVR labels) | Malachi | [TI-793](https://mntn.atlassian.net/browse/TI-793) | Phase 1 normalization |
| XGBoost feature importance + iterative paring | Malachi | TI-793 | Training dataset |
| SHAP analysis + final feature ranking | Malachi | TI-793 | XGBoost results |

### Phase 3: Audience Augmentation Validation (Week of 4/14)

| Task | Owner | Ticket | Dependencies |
|------|-------|--------|-------------|
| DS13 incrementality & predictiveness validation | TBD | [TI-794](https://mntn.atlassian.net/browse/TI-794) | Phase 2 feature ranking + Alex's vertical mapping |
| Holdout experiment design & execution | TBD | [TI-795](https://mntn.atlassian.net/browse/TI-795) | TI-794 results |

### Phase 4: Integration (Week of 4/21+)

| Task | Owner | Ticket | Dependencies |
|------|-------|--------|-------------|
| Integrate into DS13/DS19 staging + RTC exploration | TBD | [TI-796](https://mntn.atlassian.net/browse/TI-796) | TI-795 experiment success |

## Open Questions

1. **Exchange reference table**: Where does `inventory_source` map to exchange metadata? Ryan investigating.
2. **Parquet vs BQ for modeling**: augmentor_log's 10-day BQ TTL may be too short for training dataset construction. May need to read from parquet archive. Alex is already using PySpark on parquet — may be the right path for large-scale feature extraction.
3. **IP join key between tables**: Can we join augmentor_log and bidder_auction_events on IP to get the union of all features per IP? Need to verify IP overlap.
4. **Attribution window for IVR labels**: What lookback window should we use when joining to clickpass_log? Standard is 30 days, but should align with Fangorn's existing window.
5. **Segment ID mapping**: `mntn_segments` in augmentor_log contains integer segment IDs. Need a reference table to map these to DS13/DS19/etc. segment definitions for the incrementality check.
6. **content_genre normalization**: Who builds the master genre mapping? ~37K raw values → ~50-100 clean categories. Could be a shared artifact.

## Success Criteria

- Identify 5-10 bidstream features that show statistically significant predictive power for IVR
- At least one feature shows >5% relative IVR lift when used for targeting
- DS13 audience pool expanded by >10% with incremental IPs that show comparable or better visit rates
- Features integrated into Fangorn feature store for production use

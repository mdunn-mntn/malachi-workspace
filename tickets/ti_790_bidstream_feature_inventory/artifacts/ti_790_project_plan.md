# TI-789: Feature Store Expansion — Project Plan

**Epic:** [TI-789](https://mntn.atlassian.net/browse/TI-789) — Bidstream Feature Extraction & Audience Augmentation
**Team:** Malachi Dunn, Alex Knorr, Ryan Kleck | **PMO:** Bryce Wagg
**Created:** 2026-03-30 | **Updated:** 2026-03-31

---

## Objective

Catalog every IP-level feature available across MNTN's log tables, build rolling-window aggregations per IP, and test them against IVR to identify which signals improve targeting performance. Winning features get integrated into Fangorn's feature store. Secondary goal: use bidstream signals to expand DS13/DS19 audience pools.

## Data Sources (Exhaustive)

Full field-level inventory in [ti_790_exhaustive_feature_sources.md](ti_790_exhaustive_feature_sources.md).

### Primary Sources (build first — per Matt + Ryan)
| Table | What It Contains | Scale | TTL | Key Unique Signals |
|-------|-----------------|-------|-----|-------------------|
| **guid_log** | Website pixel fires — visitor behavior | — | VIEW (long) | Browser, OS, device from pixel. Product/cart data. GA params. |
| **augmentor_log** | Bidstream — auctions we participated in | 1.2B rows/hr | 10d BQ, ~30d parquet | `iab_categories` (30%), 40 SSPs, `mntn_segments` (86%) |

### Secondary Sources (build second — rich content + engagement)
| Table | What It Contains | Scale | TTL | Key Unique Signals |
|-------|-----------------|-------|-----|-------------------|
| **bidder_auction_events** | Bidstream — auctions we didn't bid on | 112M rows/hr | 90d BQ | `content_genre` (87%), `device_make` (90%), `content_series` (37%) |
| **win_logs** | Beeswax win events | — | 90d | Device model, video completion/skip rate, viewability time |

### Tertiary Sources (build third — enrichment)
| Table | What It Contains | Key Unique Signals |
|-------|-----------------|-------------------|
| **cost_impression_log** | Enriched impressions served | household_score, recency_elapsed_time, cost breakdown |
| **clickpass_log** | Attributed visits | Attribution timing (click/view elapsed), cross-device flag |

### Skip
| Table | Why |
|-------|-----|
| **bid_logs** | Mostly redundant with win_logs + augmentor_log |
| **spend_log** | Intent scores are Fangorn output (circular). Only bid_floor useful. |
| **conversion_log** | Double-counts with guid_log (Matt + Ryan confirmed) |

## Feature Architecture

Per Matt's prototype, features are **rolling-window aggregations per IP**:

```
IP → {
  // guid_log (demand-side: what they do on advertiser sites)
  has_desktop, has_mobile, has_tablet,
  pct_mobile_events, pct_desktop_events,
  n_distinct_browsers, n_distinct_os_family, n_distinct_device_fingerprints,
  has_chrome, has_safari, has_ios, has_android,

  // augmentor_log (supply-side: what content they consume)
  top_iab_category, n_distinct_iab_categories,
  n_distinct_ssps, pct_ctv_device, pct_video_placements,
  top_network, n_distinct_networks, pct_premium_pmp,

  // bidder_auction_events (broader content signals)
  top_genre, genre_entropy,
  pct_entertainment, pct_news, pct_sports, ...
  device_make, n_distinct_publishers,

  // win_logs (engagement signals)
  avg_video_completion_rate, avg_viewability, avg_in_view_time_ms,
  pct_video_skips, n_wins,

  // cost_impression_log (enrichment)
  avg_household_score, avg_recency, total_media_cost,

  // clickpass_log (attribution features)
  avg_view_elapsed, pct_cross_device,

  // Label
  ivr: visits / impressions
}
```

## Known Data Quality Issues

| Issue | Affected Fields | Fix |
|-------|----------------|-----|
| Case inconsistency | os, content_genre, device_make | LOWER() or UPPER() everything |
| Comma-delimited multi-values | content_genre ("sitcom,comedy") | SPLIT + UNNEST |
| Provider-specific prefixes | content_genre ("GENRE_COMEDY") | Strip prefix, normalize |
| Hashed/template garbage | content_series ("d41d8cd9...", "{{CONTENT_SERIES}}") | Filter known bad patterns |
| Duplicate publisher names | network ("NBC Universal" x3 variants) | Mapping table |
| geo format varies | augmentor_log.geo (raw string) vs bidder_auction_events.geo_country (structured) | Parse or use structured |

## Evaluation Methodology (Matt Brorby)

1. **Sample**: Pull random IPs with rolling-window features attached
2. **Join to IVR outcome**: Match IPs to visit data from clickpass_log + impression_log
3. **XGBoost feature importance**: Train model, extract 3 importance metrics (info gain, frequency, weighted)
4. **Iterative paring**: All features → drop least important → retrain → verify performance holds
5. **SHAP values**: Fine-tune on final feature set
6. **BIC**: Balance fit vs complexity

## Execution Plan

### Phase 1: Discovery (Week of 3/30) — DONE
| Task | Owner | Ticket | Status |
|------|-------|--------|--------|
| Feature inventory & quality assessment (both bidstream tables) | Malachi | [TI-790](https://mntn.atlassian.net/browse/TI-790) | **Done** |
| Exhaustive feature source map (all 8 log tables) | Malachi | TI-790 | **Done** |
| Vertical classification signals from bidstream | Alex | [TI-791](https://mntn.atlassian.net/browse/TI-791) | In Progress |
| OpenRTB spec investigation & test vertical selection | Ryan | [TI-792](https://mntn.atlassian.net/browse/TI-792) | In Progress |

### Phase 2: Feature Extraction & Training Dataset (Week of 4/1) — NEXT
| Task | Owner | Ticket | Status |
|------|-------|--------|--------|
| Build guid_log daily snapshot (Matt's prototype pattern) | Malachi | [TI-790](https://mntn.atlassian.net/browse/TI-790) | **Next** |
| Build augmentor_log rolling-window aggregation per IP | Malachi | TI-790 | Open |
| Build bidder_auction_events rolling-window aggregation per IP | Malachi | TI-790 | Open |
| Build normalization mappings (genre, os, device_make, publisher) | Malachi | TI-790 | Open |
| Find exchange reference table | Ryan | TI-792 | Open |
| Construct training dataset: IP features + IVR labels | Malachi | [TI-793](https://mntn.atlassian.net/browse/TI-793) | Blocked on feature extraction |

**Wednesday 4/2 sync deliverables:**
- Malachi: guid_log snapshot + augmentor_log aggregation + normalization mappings
- Alex: Vertical classification signal assessment
- Ryan: OpenRTB field mapping + exchange reference table + test vertical

### Phase 3: Modeling & Feature Ranking (Week of 4/7)
| Task | Owner | Ticket | Status |
|------|-------|--------|--------|
| XGBoost feature importance + iterative paring | Malachi | [TI-793](https://mntn.atlassian.net/browse/TI-793) | Blocked on Phase 2 |
| SHAP analysis + final feature ranking | Malachi | TI-793 | Blocked on XGBoost |

### Phase 4: Audience Augmentation Validation (Week of 4/14)
| Task | Owner | Ticket | Status |
|------|-------|--------|--------|
| DS13 incrementality & predictiveness validation | TBD | [TI-794](https://mntn.atlassian.net/browse/TI-794) | Blocked on Phase 3 |
| Holdout experiment design & execution | TBD | [TI-795](https://mntn.atlassian.net/browse/TI-795) | Blocked on TI-794 |

### Phase 5: Integration (Week of 4/21+)
| Task | Owner | Ticket | Status |
|------|-------|--------|--------|
| Integrate into DS13/DS19 staging + RTC exploration | TBD | [TI-796](https://mntn.atlassian.net/browse/TI-796) | Blocked on TI-795 |

## Open Questions

1. **Exchange reference table**: Where does `inventory_source` map to exchange metadata? Ryan investigating.
2. **Parquet vs BQ for feature extraction**: augmentor_log's 10-day BQ TTL may be too short. Alex is using PySpark on parquet — may be the right path.
3. **Rolling window duration**: 7 days? 14 days? 30 days? Need to align with Fangorn's existing feature cadence. Matt's prototype uses daily snapshots.
4. **IP overlap between tables**: Can we join augmentor_log and bidder_auction_events on IP? Need to verify overlap.
5. **Attribution window for IVR labels**: What lookback window for clickpass_log join? Align with Fangorn's existing window.
6. **Segment ID mapping**: `mntn_segments` integer IDs → DS13/DS19 segment definitions.
7. **guid_log TTL**: What's the underlying table's retention? Need sufficient history for rolling windows.

## Success Criteria

- Identify 5-10 features across all sources that show statistically significant predictive power for IVR
- At least one feature shows >5% relative IVR lift when used for targeting
- DS13 audience pool expanded by >10% with incremental IPs showing comparable or better visit rates
- Features integrated into Fangorn feature store for production use

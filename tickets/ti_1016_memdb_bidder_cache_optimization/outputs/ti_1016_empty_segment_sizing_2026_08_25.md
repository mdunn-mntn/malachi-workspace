# AUDI-1016 — TI-side sizing of duplicate-empty segment records (BQ copy), 2026-08-25

Source: `bronze.raw.tpa_membership_update_log` (physical `sqlmesh__raw.raw__tpa_membership_update_log__546164626`), the BQ landing of the membership-db GCS dump. Queries in [ti_1016_empty_segment_sizing.sql](../queries/ti_1016_empty_segment_sizing.sql).

## Findings

**1. The BQ copy carries only ONE of the six daily sweeps.** Full-day exact count for 2026-08-23: every one of the 3,001,830,875 rows has `hh='08'`, `source_version='v2'`, `delta=false`. The 08:00 UTC sweep (midnight PT) is the only one imported; the other five sweeps and the Kafka activity feed never land in BQ. Consequence: TI-side BQ can size the daily-roster sweep, but Eric's consumer-side dashboard is the only source of truth for the full 24h load (his 10.60B records/day).

**2. Per-sweep volume: ~3.0B records, ~1.23 TiB/day.** Stable 2026-08-18..24 (2.97-3.04B/day). 90-day partition expiry on `time`.

**3. Empty-record composition of the sweep (0.001% TABLESAMPLE, 2.7M rows, 9 sampled days Jun-Aug 2026, all stable):**

| Class | Share | Meaning |
|---|---|---|
| `in_segments` empty AND `out_segments` empty | **~57%** | conveys nothing — the duplicate-empty class (IP with no segments and no change), ~1.71B records/sweep |
| `in_segments` empty, `out_segments` non-empty | ~17% | just-became-empty transition (segments left) — the meaningful "delete" signal |
| `in_segments` non-empty | ~26% | carries current membership |

`scores.key_value` is populated in 0 of 2.7M sampled rows; `delta` is false on every row.

**4. Reconciliation with Eric's 92.9% — NOT the same measure, both plausibly right.** Eric's dashboard counts "% of incoming messages with empty segment *scores*" over all 6 sweeps + Kafka (10.60B records/day, 92.9% empty, peak 402,907/s). The BQ sweep shows 57% fully-empty / 74% no-current-segments. Hypotheses: (a) his counter keys on empty segment-score payloads, which also counts segmented-but-unscored records; (b) the 5 non-imported sweeps could skew emptier (no roster refresh); (c) grain differences (consumer messages vs BQ rows). Discriminating check: read the counter definition at `gcs.rs:929-936` vs this table's importer filter (`tmul_burnin_08_filter.py`) — needs Atlas Code MCP (auth pending).

**5. Volume reconciliation open:** 6 sweeps x 3.0B = 18B > Eric's 10.60B/day; 10.60B / 3.0B ~ 3.5 sweeps/day. Either not all sweeps are full (duty cycle 43.8% suggests some overlap/truncation), or the 08:00 sweep is larger than the intra-day ones. Same discriminating check as above.

## Implication for the fix

Producer-side suppression of the empty-and-unchanged class kills ~57% of the daily-roster sweep outright (~1.71B records/sweep) while preserving the 17% just-became-empty transitions Eric needs once. Against Eric's consumer-side numbers the same filter removes ~92% of total inbound (~400k tps -> ~40k). Both measures agree the empty-and-unchanged class dominates.

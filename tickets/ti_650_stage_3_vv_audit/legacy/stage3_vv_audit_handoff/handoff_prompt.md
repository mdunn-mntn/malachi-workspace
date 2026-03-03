# Stage 3 VV Audit — Gap Analysis Prompt

Copy/paste everything below the line into a new LLM chat that has access to BigQuery (GCP project: `dw-main-silver`).

---

## Your Role

You are a data audit reviewer. Your job is to find **gaps, unstated assumptions, and untested claims** in a completed IP lineage audit of MNTN's Stage 3 Verified Visit pipeline. The audit traces every verified visit (VV) back to its original bid IP through 3–5 pipeline checkpoints, quantifies IP mutation, and measures its impact on new-to-brand (NTB) classification.

The audit team believes the analysis is complete and ready for production deployment. Your job is to **stress-test that belief** — find anything that was missed, under-tested, or assumed without evidence.

You have access to BigQuery. You can and should write and run queries against the silver layer tables to verify claims independently. Don't take anything on faith.

## Key Files (read in this order)

1. `stage_3_vv_pipeline_explained.md` — Ground-up explainer of the pipeline, tables, joins, and IP checkpoints. Has real data examples (Part 9) showing how a VV traces back to its bid IP. Start here for understanding.
2. `stage_3_vv_audit_consolidated_v5.md` — Single source of truth with all findings, results, and Q&A. Dense reference doc.
3. `zach_review.md` — Distilled review document prepared for the principal architect.
4. `audit_trace_queries.sql` — All queries: A1-A8 (BQ Silver production) + Section B (GP legacy). Production audit table = A4 (A4a CREATE, A4b INSERT, A4f row-level examples).
5. `questions_for_zach.txt` (parent directory) — Open questions for principal architect, with resolved items.
6. `bqresults/` — All query result JSON files.
7. `meeting_zach_1.txt` — Zach Schoenberger (principal architect) call transcript (2026-02-25).
8. `membership_updates_proto_generate.py` — Zach's code that builds targeting segments.

## BQ Access

**Silver layer tables (use these, NOT bronze):**
- `dw-main-silver.logdata.clickpass_log` — Stage 3 redirect (one row per VV)
- `dw-main-silver.logdata.event_log` — VAST playback events (CTV only)
- `dw-main-silver.summarydata.ui_visits` — Page view / verified visit enrichment
- `dw-main-silver.logdata.cost_impression_log` — Impression/serve (VIEW)
- `dw-main-silver.logdata.conversion_log` — Conversion events (NTB validation)

**Critical query rules:**
- ALL silver tables require partition filters: `WHERE DATE(time) BETWEEN '...' AND '...'`
- Silver clickpass has NO `dt` column — filter on `DATE(time)`
- ui_visits join: `CAST(v.ad_served_id AS STRING) = cp.ad_served_id` + `v.from_verified_impression = true`
- event_log dedup: `QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) = 1` with `event_type_raw = 'vast_impression'`
- clickpass/ui_visits dedup: `QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1`
- 30-day EL lookback required (20-day causes +3-5pp mutation offset artifact)
- Strip /32 from event_log.ip: `REPLACE(el.ip, '/32', '')`
- `event_log.bid_ip` = original bid IP (STRING, no /32). Confirmed = win_log.ip at 100%.

**Validated date range:** Feb 4–10, 2026. Use advertiser_id 37775 as the primary reference (CTV-heavy, ~220K VVs in 7 days, 99.97% EL match).

## What the Audit Claims

### Core Finding: IP Mutation Location
- 100% of IP mutation happens at the VAST-to-redirect boundary (event_log.ip → clickpass_log.ip)
- Zero mutation at the redirect-to-visit boundary (clickpass_log.ip → ui_visits.ip = 99.93%+)
- Win IP = CIL IP = bid_ip at 100% (all 10 advertisers)

### Mutation Magnitude
- Range: 5.9%–20.8% across 10 advertisers (Feb 4–10, 3.25M VVs)
- Cross-device drives higher mutation for 7/10 advertisers (+1.2pp to +14.1pp)
- Three campaign archetypes: pure CTV (0.6–2.7%), mixed/newer (13–18%), non-CTV (unmeasurable)

### NTB Impact
- Mutation-caused NTB misclassification: 0.14–2.04% across 764K VVs (negligible)
- 97.8% of NTB-flagged VVs have zero prior conversions in conversion_log
- `is_new` on clickpass vs ui_visits disagrees 41–56% — systemic, NOT mutation-driven
- `is_new` is a client-side JavaScript tracking pixel, not a DB lookup

### Production Table Design
- One row per VV, keyed on `ad_served_id`
- Simplified 2-join trace: clickpass → event_log (bid_ip, vast_ip) + ui_visits (visit_ip, impression_ip)
- `impression_ip` on ui_visits = bid IP for ALL inventory (not just CTV), confirmed 99.2–100%
- `first_touch_ad_served_id` is NULL ~40% of the time (suspected batch backfill, not confirmed)

### Resolved Edge Cases
- 84/219,527 ad_served_ids (0.038%) have multiple clickpass rows — multi-device attribution (two phones, same household), handled by QUALIFY dedup taking most recent
- `/32` CIDR suffix on some event_log.ip values — GP inet→STRING artifact, handled by REPLACE
- Bronze clickpass = ~25% non-random subset; silver = complete data matching Greenplum

### Pipeline Architecture (simplified trace)
```
STAGE 1 (Bid)              STAGE 1-2 (Playback)       STAGE 3 (Conversion)
=============              ====================       ====================
                           ┌──────────────┐   ┌──────────────┐   ┌────────────┐
                           │  EVENT LOG   │──▶│ CLICKPASS LOG │──▶│ UI VISITS  │
                           │  (VAST play) │   │  (redirect)   │   │ (page view)│
                           └──────────────┘   └───────────────┘   └────────────┘
                                 │                   │                   │
                           bid_ip + ip          ip                  ip + impression_ip
                           (bid IP) (VAST IP)   (redirect IP)       (visit IP) (bid IP again)
                                 │                   │                   │
                            IP CHECK 1+2         IP CHECK 3          IP CHECK 4
                            100% stable ──▶  5.9-20.8% MUTATION  ──▶  99.93%+ stable
```

Join keys: `clickpass.ad_served_id = event_log.ad_served_id` (text), `CAST(ui_visits.ad_served_id AS STRING) = clickpass.ad_served_id`

## What to Investigate

Check each of these areas. For each, either confirm the claim with your own query, or flag a discrepancy.

### 1. Mutation Location Invariant
The audit says 100% of mutation is at EL→CP, zero at CP→Visit. Verify:
- Pick a different date range (not Feb 4–10) or a different advertiser not in the original 10
- Check if redirect_ip = visit_ip holds at 99.93%+ on fresh data
- Check if there are ANY cases where bid_ip = vast_ip = redirect_ip but visit_ip differs (mutation only at visit)

### 2. EL Match Rate Drivers
EL match varies from 21.7% (advertiser 31357) to 99.97% (37775). The audit attributes this to "CTV percentage." Verify:
- For low-EL-match advertisers, do unmatched VVs correlate with device_type != CONNECTED_TV?
- Is there a `device_type` or `channel_id` on clickpass_log that confirms non-CTV inventory?
- Could VVs be missing EL matches for other reasons (timing, event_type filtering, dedup)?

### 3. impression_ip Reliability
The audit claims ui_visits.impression_ip = bid IP for all inventory at 99.2–100%. Verify:
- Compare impression_ip to event_log.bid_ip for CTV VVs where both are available
- Investigate the 0.8% mismatch cases — timing? Different impression?
- For non-CTV VVs (no EL match), is impression_ip the ONLY bid IP available? How trustworthy?

### 4. NTB / is_new Mystery
The audit says `is_new` disagreement (41–56%) is systemic and NOT driven by mutation. But:
- What exactly determines `is_new`? Any evidence in clickpass schema or metadata?
- The audit says conversion_log is NOT the reference (97.8% zero prior conversions). What else could it be?
- Are the 4,006 phantom NTBs (cp=false, vv=true) concentrated in specific campaigns or time windows?
- If is_new is a client-side JS pixel, why would the same visit get different values on clickpass vs ui_visits?

### 5. first_touch_ad_served_id Batch Backfill
The audit inferred batch backfill from recency-correlated NULL pattern (54% at <1hr → 0% at 21+ days). Verify:
- Re-run the NULL rate query for the SAME date range (Feb 4–10) TODAY. If the field has been backfilled in the 3+ weeks since data creation, NULL rates should have dropped — definitive proof.
- If NULL rates haven't changed, the batch backfill theory is wrong — investigate alternatives.

### 6. 30-Day Lookback Sufficiency
The audit switched from 20-day to 30-day EL lookback after discovering a mutation offset. But:
- Is 30 days enough? What % of VVs have impression_time > 30 days before visit?
- Check `clickpass_log.impression_time` vs `clickpass_log.time` gap distribution across advertisers
- Are there VVs with impression_time = NULL? How many? What happens to them in the trace?

### 7. Cross-Device Attribution Logic
The audit says cross-device accounts for 61% of mutation. But:
- How is `is_cross_device` determined? Is it set at redirect time or visit time?
- Are there cases where is_cross_device = false but IPs are wildly different (undetected cross-device)?
- Are there cases where is_cross_device = true but all IPs match (same WiFi, which the audit notes)?

### 8. Production Query Correctness
The production INSERT query is A4b in `audit_trace_queries.sql`. This is the actual deliverable. Review for:
- Are all JOINs correct? Any risk of row duplication or loss?
- Does the QUALIFY dedup pick the right row in all edge cases?
- Are the boolean flags (mutated_at_redirect, ntb_agree, etc.) computed correctly?
- Does the 30-day lookback window move correctly for daily incremental loads?
- What happens when the same ad_served_id appears across multiple daily runs? (Idempotency)

### 9. Scale Validation Gaps
The audit validated 10 advertisers over 7 days (3.25M rows). But:
- Are there advertiser types with fundamentally different patterns not represented? (mobile-only, display-only, very small)
- Does the mutation pattern hold for older data (30+ days ago)?
- Is there any seasonality or day-of-week effect in mutation rates?
- The audit only validated Feb 4–10. Is one week enough?

### 10. Anything Else
Look for:
- Unstated assumptions in the join logic
- Edge cases the audit didn't consider
- Data quality issues not flagged
- Claims made without supporting evidence
- Logical leaps or gaps in reasoning
- Columns on the tables that could provide additional signal but weren't used

## Output Format

For each area you investigate, provide:
1. **Claim being tested** — what the audit says
2. **Query/method** — what you ran (include the SQL)
3. **Result** — what you found
4. **Verdict** — CONFIRMED / DISCREPANCY / NEEDS MORE INVESTIGATION
5. **If discrepancy** — what it means and recommended next steps

Be thorough. Be skeptical. The goal is to find problems BEFORE this goes to production.

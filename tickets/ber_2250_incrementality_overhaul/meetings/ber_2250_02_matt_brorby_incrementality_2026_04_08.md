# BER-2250: Matt Brorby — Incrementality Strategy & Technical Context

**Date:** 2026-04-08
**Attendees:** Matt Brorby (Staff DS), Malachi Dunn
**Type:** Working sync
**Transcript:** `../ti_835_control_group_design/meetings/ti_835_02_matt_brorby_incrementality_2026_04_08.txt`

---

## Key Takeaways

### 1. Jira Structure Confirmation
- TI-835, TI-837, TI-839, TI-842 should be under a NEW epic (not TI-831)
- TI-831 (Audience Deciles) is a separate workstream
- Both epics under BER-2250 (initiative)
- **Action taken:** Created TI-855 EPIC, moved tickets

### 2. Decile Work Ownership
- Alex Bloore originated the idea (saw it work at The Trade Desk)
- Sean and Ryan have engineering interest
- Zach Schoenberger and Jordan Piepkow are the key implementers (audience/targeting infra)
- It's a customer-facing experimentation tool: split US population into 10 random buckets, advertisers choose which to target
- Currently users don't see the holdout group in most reporting — this would surface it

### 3. LiftLab and External Measurement
- LiftLab is **paid by the advertiser** → bias toward conservative measurement
- Their incremental reports will be as conservative as possible
- MNTN is "at the mercy of these third parties"
- We won't internalize incrementality measurement — trust the third party

### 4. Matt's Incremental ROAS Experience (Prior Role — Mobile)
- Worked as "the third-party guy" measuring incremental ROAS and lifetime value
- Used deterministic mobile data (app installs — device-level, not IP)
- **Time-delta bucketing method:**
  - Equal-sized user buckets ordered by time from ad impression to conversion event
  - First 5 seconds after ad: ~100% incremental (huge spike)
  - Signal degrades rapidly — max useful window was ~6.5 hours for apps
  - Typical useful range: 30 minutes to 6.5 hours
  - "More art than science" — huge variation by app/advertiser
  - He has a **published article** on this methodology
- **Incremental ROAS benchmarks:**
  - Good advertisers: ~$0.90 incremental per dollar
  - Poor advertisers: ~$0.50 or worse
  - Trade Desk: ~$1.15 (considered good)
  - Over $1.00 incremental ROAS is "awesome" and rare
  - Companies claiming $8 ROAS are measuring attributed, not incremental
- Matt suggested looking at time-of-impression to time-of-conversion-event correlation across intent scores (similar to what he commented on Alex's shuffling doc)

### 5. CTV-Specific Challenges
- **Not deterministic** — IP-based, not device-based like mobile
- **Long conversion windows** — CTV products often have 2-week conversion cycles, not seconds/minutes
- **Signal-to-noise ratio** — at longer time intervals, very hard to tell if extra conversions are signal or noise
- **IP quality** — should filter out cellular IPs (T-Mobile etc.) using identity graph as a filter
- Matt hasn't analyzed CTV specifically yet — "it might be totally different"
- The fundamental problem: "the nature of CTV is people aren't converting right away"

### 6. Ensemble Model Approach
- "No one model to rule them all"
- Will need separate optimization strategies:
  - IVR model for performance-focused advertisers
  - Incremental ROAS model for incrementality-focused advertisers
- Trade-off is inherent and unavoidable
- Only applies to advertisers who opt in — won't tank company-wide performance

### 7. Key Quote: "Everyone Suspects" Our Finding
- "Everyone suspects [intent scoring] is just capturing people who are going to visit anyways. Can we see that? How bad is the issue?"
- This validates our TI-835 guid_log finding (~0% lift on total traffic)
- Matt's internal incrementality dashboard experience: "I've never seen negative incrementality or zero incrementality. It's always positive." → suggesting internal dashboards may be overstating

## Action Items (completed)
- [x] Created new EPIC TI-855 under BER-2250
- [x] Moved TI-835/837/839/842 from TI-831 to TI-855
- [x] Created TI-856 (LiftLab research), TI-857 (5 vendor experiments), TI-858 (audience incrementality), TI-859 (bucketing infra)
- [x] Updated knowledge docs with industry benchmarks and CTV challenges

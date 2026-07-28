# AUDI-1176: Gate scoring input to DS14-addressable — Presentation

## Audience
AUDI grooming / whoever implements the gate.

## Key Message
Intersect the 31-day DS13/DS19 scoring input with the current DS14 (8-day) set before scoring, cutting ~39–69% of daily scoring compute with no biddable-coverage loss.

## Narrative Flow

### 1. Context
AUDI-1175 established that ~69% (DS19 MM Core) / 39% (DS13 verticals) of the scored IP universe is non-addressable within the 8-day DS14 window, and safe to stop scoring. This ticket implements the gate.

### 2. What We'll Do
- Add a DS14-recent intersection at the input of `vertical_high`/`vertical_mid` and, for the main $ lever, `prospecting_keywords` (DS19); mirror for the Fangorn 14-day path.
- Alternative insertion point: intersect `intent_score_map` output before the serving-store load.
- Shadow-run before cutover to confirm delivery parity on an advertiser holdout.

### 3. Expected Outcome
~$1.3k/mo (DS13) to ~$11k/mo (DS19) daily-compute savings, plus IPDSC storage reduction.

### 4. So What?
Cost reduction with no targeting-quality loss; also shrinks IPDSC volume (MembershipDB resilience).

### 5. Next Steps
- Blocked by AUDI-1175 (answered — gate is safe).
- Build-time: delivery-parity shadow run on a holdout; flagged rollout with rollback.
- Note: the HHST recommender is auction-scoped (no coupling to the scored universe), per AUDI-1175 — so gating scoring does not bias production thresholds.

## Charts & Visualizations
- Before/after scored-set size and daily Dataproc cost (from the shadow run).

## Appendix
- Insertion points, constraints, and the HHST write-path analysis in `summary.md`.

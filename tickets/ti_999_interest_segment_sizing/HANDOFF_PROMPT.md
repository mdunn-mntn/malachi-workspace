# TI-999 — New-Chat Handoff Prompt

Paste the block below into a fresh chat to pick up this work cleanly. It's
self-contained, points to the canonical docs, and sets the mental model
correctly so the new chat doesn't have to relearn what we already know.

---

```
I'm continuing TI-999 — interest-segment portfolio sizing for MNTN. Sibling to
TI-956 (Alex Knorr's segment-quality scoring framework deploy). Pick up from where
this left off.

Read these first, in order:
1. tickets/ti_999_interest_segment_sizing/summary.md — start with the "Current
   state" block at the top. Then scan Findings 1-14 for the analytical record.
   Findings 11-14d are the canonical headline reads; Findings 3-10 are pre-
   methodology-correction historical context (don't cite them as headlines).
2. knowledge/data_knowledge.md — sections "1P / 3P / MM definitions" and
   "Bidder Scoring Reality" carry the durable infrastructure knowledge from
   this work. These are the conceptual model.
3. tickets/ti_999_interest_segment_sizing/presentation.md — narrative version
   of the findings, organized around the four user questions.
4. The deck v3 (standalone HTML) lives at
   tickets/ti_999_interest_segment_sizing/artifacts/ti_999_presentation_deck_standalone.html.
   Shareable URL pinned in artifacts/share_link.txt.

Conceptual model that took several iterations to lock in (per Victor 2026-05-28):
- 1P = advertiser-uploaded data (CRM, IP-list). NOT scored by MNTN.
- 3P = bought interest segments (LiveRamp, ShareThis, Dstillery). NOT scored.
- MM (Mountain Match) = MNTN-derived targeting, IS scored. Produces
  `household_score` in cost_impression_log.model_params (graduated 0-10000).
- RTC = Real-Time Conquesting. Binary qualifier flag (10000 / -1) for recent-
  site visitors only. Separate from MM — don't conflate.
- The bidder applies household_score (MM) regardless of whether the campaign's
  expression opts in. RTC qualifies a small slice (~5% of impressions).

Headline numbers (prospecting only, 30d ending 2026-05-28):
- 13,511 prospecting campaigns / $24.86M / 30d / ~$298M/yr.
- 34.6% of prospecting spend uses 3P interest segments → ~$103M/yr.
- 18.3% touches stale 3P (ShareThis or Dstillery, both >2yr stale) → ~$55M/yr.
- 72% of 3P-only campaigns ALSO use MM (the layer that actually does scoring).
  When MM is layered, delivery is heavily scored. When 3P stands alone, ~74%
  of delivery is unscored — same as no-3P prospecting.
- Implication: 3P provides selection, not quality. Without per-segment
  scoring (TI-956), 3P doesn't lift quality over generic prospecting.

Operational methodology rules locked in:
- "Interest segment" = bought 3P with material IPDSC volume. Active set:
  {DS17 ShareThis, DS18 Dstillery, DS35 LiveRamp IP}.
- "Prospecting only" = exclude any campaign whose expression references
  DS4 (CRM), DS8 (IP List), DS47 (CRM Identity Graph) — these are list-style
  retargeting. Memory: feedback_crm_excluded_from_prospecting.
- KPI comparisons are descriptive, not causal. Selection effects confound.

Open items before any wider share (summary.md §8):
- Validate the operational interest set + retargeting exclusion + 1P/3P/MM
  framework with Zach S. (audience platform authority). Specific questions in
  summary.md §8 A1-A4 + the three pending Victor confirmations on MM scoping.
- Resolve borderline DS49 Publisher Network — bought 3P or MNTN-internal
  contextual?
- Alex K. sanity-check on numbers vs his framework — already scheduled for
  next-week tech deep-dive.

Open follow-up analyses we sized but haven't run:
- Re-do "pure-3P delivery" cut with FULL MM exclusion (DS13/19/38/46), not
  just RTC/BUK as Finding 14d did. Will tighten the "pure-3P is 74% unscored"
  number.
- Per-3P-provider top advertisers (LiveRamp vs ShareThis vs Dstillery).
- Positive vs negative clause distinction via expression AST parse.
- Per-campaign IP-set resolution (expensive but possible).
- Rank simulation v2 once TI-956 ships real composite scores.

What I want help with right now: [DESCRIBE THE NEXT TASK HERE]
```

---

## Quick reference for me (the user)

| Thing | Where |
|---|---|
| Ticket card | [summary.md](summary.md) — "Current state" block at top |
| Presentation source | [presentation.md](presentation.md) |
| Deck (HTML) | [artifacts/ti_999_presentation_deck.html](artifacts/ti_999_presentation_deck.html) (dev) + `_standalone.html` |
| Deck share URL | [artifacts/share_link.txt](artifacts/share_link.txt) |
| All queries | [queries/](queries/) — 12 .sql files, each documented |
| All outputs | [outputs/](outputs/) — CSVs (gitignored; regenerate via re-running queries) |
| Charts | [artifacts/](artifacts/) `ti_999_chart_*.png` — gitignored; regenerate via `python3 artifacts/generate_charts.py` |
| Standalone deck builder | [artifacts/build_standalone.py](artifacts/build_standalone.py) |

| Memory | What |
|---|---|
| `project_ti_999_interest_segment_sizing` | High-level project context |
| `feedback_crm_excluded_from_prospecting` | Methodology rule: exclude DS4/8/47 |
| `reference_bidder_scoring_reality` | Three score fields in model_params |
| `reference_mntn_1p_3p_mm_definitions` | The 1P / 3P / MM framework |
| `reference_audience_platform_authority` | Zach S. is the canonical voice |

| Jira | Note |
|---|---|
| TI-999 | This ticket |
| TI-956 | Sibling — Alex's segment-quality scoring deploy |

| Todoist | Parent task `6gjfw4X6GGv2XQhM` with open subtasks for Zach validation, DS49 resolution, Alex sanity-check |

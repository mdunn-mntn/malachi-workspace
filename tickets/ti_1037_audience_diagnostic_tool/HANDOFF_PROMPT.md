# TI-1037 — Handoff / fresh-session priming prompt

Paste the block below into a new chat to continue TI-1037. (Also read this ticket's `summary.md`.)

---

You're continuing **TI-1037: Automate client-performance diagnostics (audience-expression-driven)** in the MNTN
workspace (`/Users/malachi/Developer/work/mntn/workspace`). Follow the global + project `CLAUDE.md` operating rules
(read README + knowledge docs at session start; BQ via `.claude/scripts/bq_run.sh`; commit+push constantly; keep
Jira/Todoist/knowledge/summary current).

**What TI-1037 is:** productize a recurring TI diagnostic into a parameterized tool / query-series so we can answer
stakeholder "why is this client performing this way / why is the audience small / which segments should they use?"
questions **on demand** from an `advertiser_id` (+ `audience_id` / `campaign_id`), instead of bespoke each time.
Jira: TI-1037 (parent TI-602, sprint "TI Sprint 06/15-06/29" id 6926, 5 pts, assignee Malachi). Relates-To TI-1026
(the prototype) and TI-956 (the segment-quality-scoring engine this complements).

**READ FIRST — the spec is already written:** `knowledge/audience_diagnostic_playbook.md`. It lays out the diagnostic
as **steps 0–8**, each = one tool module (question → BQ query → interpretation rule → gotcha). Steps 0–8 are **fully
prototyped** in TI-1026 (`tickets/ti_1026_orange_theory_audience_eval/`): every step has a real query in `queries/`
and a worked example in `summary.md`. **Step 9 (deliverability) is the open design input — BLOCKED on a deep-dive
with Chris Addy (Olympus/media-plan); it's the first Todoist subtask. Build steps 0–8 now; slot in 9 after the meeting.**

**The modules + their prototype queries (in `tickets/ti_1026_orange_theory_audience_eval/queries/`):**
0–1. Pull BOTH expressions + decompose the SEGMENT one — `artifacts/parse_expression.py`
2. 3P interest-segment quality — `ti_1026_per_segment_reach_7d.sql`, `ti_1026_reach_overlap_7d.sql`
3. Keywords — `artifacts/classify_keywords.py` + `ui.audience_keyword_state` (parent vs child, filter `is_magic`)
4. Size funnel + exclusion quality — `ti_1026_geo_funnel.sql`, `ti_1026_exclusion_bite_on_mm.sql`, `ti_1026_full_funnel.sql`, `ti_1026_income_provider_agreement.sql`, `ti_1026_income_distribution.sql`
5. Scoring / HHST — `ti_1026_delivered_score_dist.sql` (+ `dso.household_score_thresholds`)
6. Availability (reach/frequency) — `ti_1026_availability.sql`, `ti_1026_availability_daily.sql`
7. Targeting vs creative — `ti_1026_visitrate_by_score.sql`, `ti_1026_ctv_vr_benchmark.sql`
8. UI size vs deliverable — `perml.flight_cid_day_audience_sizes`; eval_batch variants in `artifacts/build_eval_batch_variants.py`

**Load-bearing facts the tool MUST encode (don't re-derive — all in `data_knowledge.md`/`data_catalog.md`):**
- The **bidder uses the SEGMENT expression** (`audience.audience_segments`), NOT the user `audience.audiences`. The
  segment expr AND-layers automated clauses: **DS14 cat 1 = 7-day augmentor activity filter** (the platform's
  "availability" gate), holdout md5 bucket, RTC score, retargeting (DS21/DS34). Pull both expressions.
- **HHST gate** = `dso.household_score_thresholds.threshold` (0 = no gate, ~64% of campaigns; >0 = only bid IPs scored
  ≥ threshold). Under a gate, unscored 3P-only IPs are filtered out (3P ≈ inert); with no gate they deliver as
  unscored junk. household_score lives on `cost_impression_log` (delivered).
- **ipdsc (`dw-main-bronze.external.ipdsc__v1`):** filter `dt` with a **literal** (subquery → 164B-row scan).
  UNNEST `data_source_category_ids.list`, read `.element`. **3P/DS35 delivery is bursty (~2–4 load days/month) — use a
  ≥30-day window; never judge a segment from one day/week.** IPv4 parse via regex `[.]` (NET funcs error on bad/multi
  IPs and don't support SAFE).
- **Keywords:** UI shows ~20 **PARENT** seed keywords; the DS19 expression = selected **CHILD** keywords (the
  expansion that targets), via `ui.audience_keyword_state`. **`is_magic` keywords are UNTARGETABLE UI artifacts —
  exclude them.** Flow: 20 parents → ~200 products → N DS19 children (embedding match; drift happens here). BUK/DAR
  comparison + the 20 parents: shopper-graph autopilot `https://shopper-graph.in.mountain.com/autopilot?advertiser_id=<id>` (VPN-only).
- **UI audience size** (`perml.flight_cid_day_audience_sizes` stage-1 cid where funnel=total; or `external_ddm.segment_sizes`
  GCS; or `eval_batch` API VPN) **OVERSTATES the deliverable** — ignores geo + DS14 + inflated by the 3P OR.
- **3P demographic data is unreliable:** providers agree only ~0.36% on "low-income"; **Equifax/IXI is asset-based and
  skews affluent (under-labels low-income); Experian HHI is realistic.** Never stack providers for one attribute (OR'd
  → union of every provider's errors). Geo is usually the biggest size filter (MaxMind `ST_DWITHIN`, `geo.maxmind_blocks_ipv4`).
- **Targeting vs creative:** score→visit-rate gradient (cost_impression_log × clickpass_log) + peer benchmark; if the
  score discriminates VR but blended VR is low-percentile vs CTV peers → ceiling is creative/offer (advertiser side).
  Causal proof of "exclusions/3P don't help" needs a holdout.

**Build approach:** parameterize each module by advertiser/audience/campaign id; run query → apply interpretation rule
→ emit a standard report section. Standardize the output (a repeatable report). Validate on a second advertiser.

**Open items (Todoist subtasks under TI-1037):** (1) Chris Addy deliverability deep-dive [UNBLOCKER] → (2) define the
parameterized spec → (3) build the query series/tool → (4) standardize output + validate on a 2nd advertiser.

First: read `knowledge/audience_diagnostic_playbook.md` and `tickets/ti_1026_orange_theory_audience_eval/summary.md`,
read the TI-1037 Jira comments + Todoist task/comments, then propose the build plan for steps 0–8.

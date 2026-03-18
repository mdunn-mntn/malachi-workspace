# 2026 Self-Review — Malachi Dunn

## Q1 2026

### Completed tickets

[2026-02] TI-644 Root Insurance (Unsure # days end-to-end) — Investigated 92% CRM audience match miss rate
for Root Insurance ($10M+ CTV campaign). Traced full CRM pipeline (HEM → ipdsc__v1 → bidder).
Confirmed 23.3M include HEMs, ~18.2M resolved IPs (~15M net after exclusions). Identified dead ends
in tmul_daily (14-day TTL, no DS4) and audience_upload_ips (empty for email uploads). Documented
complete table schemas for ipdsc__v1, audience_upload_hashed_emails, audience_uploads. Produced CSV
exports for stakeholder (Kale) deliverable.

[2026-02/03] TI-650 Stage 3 VV Audit (~3 weeks end-to-end, 2026-02-18 → 2026-03-10) — Led full
end-to-end audit of the IP mutation problem in MNTN's verified visit pipeline. Built BQ silver-based trace covering 3.25M VVs, matched Greenplum
within 0.12pp (validated BQ as GP replacement). Identified 100% of mutation occurs at VAST→redirect
boundary. Quantified mutation range 1.2–33.4% across 15 advertisers. Confirmed 4,006 phantom NTB
events/day for reference advertiser. Fixed A4b dedup bug. Designed production audit table
(audit.vv_ip_lineage). Completed Zach Schoenberger review, incorporated 13 docx corrections.

Second review round (2026-03-05/06): Discovered prior VV impression for Zach's specific S3 VV
was a display impression (impression_log, not event_log). Corrected incorrect claim about NULL cause
(verified empirically — UUID a4074373 had zero event_log rows but IS in impression_log). Extended
production model to cover display inventory via il_all CTE (impression_log fallback, COALESCE pattern
applied to all 3 impression lookups). Added cost analysis: daily $17 → $29 (+impression_log ~1.9TB).
Wrote Part 16 of pipeline_explained.md: comprehensive MES trace permutation reference covering all
9 CTV/display combinations, S1→S2→S3 IP follow-through example, NULL cause table.

Speed: Resolved Zach's NULL independently before escalating — traced root cause to display
impression type via empirical BQ query, not guesswork.

Craft: Extended 7-join model to 10-join with display fallback, covering 100% of inventory types.
Part 16 permutation guide is a permanent reference for the team.

Adaptability: Pivoted immediately when display impression was confirmed — re-architected CTEs same
session, updated cost docs, implementation plan, and column reference.

Third round (2026-03-06): Redesigned S1 resolution from `cp_ft_ad_served_id`-based lookup (40% NULL,
unfixable) to full 4-branch CASE chain traversal via `prior_vv_pool` self-JOINs. Extends to 13 LEFT
JOINs (added s1_pv, s2_pv, s1_lt, s1_lt_d, s2_lt, s2_lt_d). Handles all 10 permutations including
S3→S3→S3→S1. Covers ~99%+ of rows (vs 60% before). Confirmed all branches fire via clickpass-only
proxy query across 10 permutations (44K-211K rows each). Incorporated Zach's all-stage design (no
stage filter in cp_dedup) and Ray's TTL context from architecture diagram (90-day lookback confirmed
sized for ~83-day S1→S3 chains). Updated all 5 artifacts to v4 design standard.

Speed: Independently identified the `cp_ft_ad_served_id` NULL problem as architecturally unfixable
and designed the chain traversal solution without escalation. Added `pv_stage <= vv_stage` semantic
constraint and third-level `s2_pv` JOIN for 3-hop chains in same session.

Craft: 4-branch CASE with monotonic stage constraint (`s1_pv.pv_stage <= pv.pv_stage`) is the
correct generalization — covers all realistic permutations with clean termination conditions.
All 5 ticket artifacts updated to v4 design in one session. Permutation matrix and validation
output documented as permanent reference.

Adaptability: Pivoted from checking `cp_ft_ad_served_id` to chain traversal when confirmed
the system limitation is permanent. Incorporated Zach's "all stages" and Ray's TTL context
from stakeholder Slack without prompting.

Negative case analysis (2026-03-10): Zach directive: "work with the negative case — find the ones
where you can't go the full length." Independently identified that retargeting campaigns exist at
every funnel_level (objective_id=4 at S1/S2/S3) — previous "~20% unresolved" was inflated by
retargeting VVs that lack S1 impressions by design. After scoping to prospecting-only CTV:
- S2 resolution: 98.56% (15,880/16,112) via 5 tiers
- Discovered and implemented household_graph tier using graph_ips_aa_100pct_ip (46 additional VVs)
- Primary VV unresolved: 0.34% (54/16,112) — effectively zero
- Remaining 232 are LiveRamp identity graph entries on CGNAT IPs — can explain all of them
- Documented objective_id reference, VVS attribution model logic, campaign naming patterns
- Extended to display (MOBILE/TABLET/GAMES_CONSOLE): 98.29% resolved (2,298/2,338)
- Combined all devices: 98.53% (18,178/18,450). Primary VV unresolved: 0.34% across all types
- Prepared deliverable Zach summary (ti_650_zach_summary.md)

Speed: Independently discovered the retargeting scoping issue by empirically checking campaign
data — eliminated a false 20% ceiling without escalation. Found household_graph as a resolution
tier nobody had suggested. Completed full display analysis same session — identical 0.34% rate.

Craft: 5-tier resolution cascade (bid_ip → guid_vv → guid_imp → redirect → household_graph)
achieves 98.56% CTV S2 resolution. Attribution model analysis (178/232 are competing VVs) shows
primary VV resolution is 99.66%. VVS Business Logic PDF integrated. Display analysis confirms
consistency: 0.34% primary unresolved across ALL device types — not a CTV-specific finding.

Adaptability: Pivoted entire analysis when Zach clarified retargeting isn't relevant. Re-ran all
queries with correct scope in same session. Updated 5 artifacts + data_knowledge.md + queries.
Extended to display per user directive without needing further guidance.

[2026-02/03] MM-44 IPDSC HH Discrepancy (Unsure # days end-to-end) — Investigated 66.2% household drop
(17,589 → 5,944 HHs) across 2,302 campaign groups. Identified 3 root causes: MES inner join block
list [2,14,42], DS type contamination in campaign_segment_history, 35-day lookback behavior.
Documented IPDSC pipeline architecture in full.

### Infrastructure / knowledge work

[2026-03-03] Workspace audit (1 day) — Completed comprehensive audit of all 22 ticket folders. Enforced
lowercase_underscore naming convention across entire workspace (git mv for all tracked files).
Extracted and documented ~500 lines of new knowledge across data_catalog.md (Phase 3: ipdsc__v1,
bronze.tpa tables, Greenplum tables reference, audit table) and data_knowledge.md (RTC, IPDSC/MES
pipeline, Stage 3 VV, Jaguar/DS13, ecommerce classifier, NTB clarification, tmul_daily unnest
patterns, email analysis, audience system architecture, coreDW deprecation). Fixed all Drive file
references in ticket summaries.

---

## Q4 2025 (carried forward — estimate from ticket dates)

[~2025] TI-501 Jaguar KPI — Deep analysis of Jaguar/DS13 Audience Intent Scoring impact on
campaign KPIs. Causal impact methodology. IP-level aggregation queries against ui_conversions
and ui_visits.

[~2025] TI-541 IP Scoring Pipeline — Documented full DS13 pipeline architecture end-to-end.
Produced pipeline overview, walkthrough, and unscored IP investigation documents. Architecture
diagrams (Audience Intent Graph, DS13 Data Pipeline, Biddable Inventory Funnel).

[~2025] TI-542 Max Reach Causal Impact — Bayesian causal impact analysis of Max Reach feature.

[~2025] TI-253 TPA Monitor — Investigated missing TPA domains, confirmed tpa.membership_updates_logs
and DDP URL verticals.

[~2025] TI-254 Low NTB Percentage — Investigated NTB misclassification. Identified cross-device as
primary driver. Documented is_new = client-side pixel (not SQL-auditable).

[~2025] TI-270 Pre-Post GA Jaguar Release — Pre/post analysis of Jaguar GA launch impact on
campaign performance (IVR, CPV, conversion rate).

[~2025] TI-310 NTB Investigations — NTB deep-dive. Produced canonical NTB documentation
("New-to-Brand (NTB) Documentation.gdoc").

[~2025] TI-221 Pre-Post Vertical Classification — Pre/post impact analysis of vertical
classification change. 3 queries (vertical-level, daily trend, campaign-level). Metrics across
multiple campaign groups.

[~2025] TI-033 Vertical Classification Changes — Domain vertical classification changes and
downstream impact analysis.

[~2025] TI-391 Audience Intent Scoring Changes — Pre/post analysis of Audience Intent Scoring
model update.

[~2025] TI-390 MMv3 Performance — MMv3 (Membership Model v3) performance investigation.

[~2025] TI-200 Whitelist/Blocklist — Ecommerce domain whitelist/blocklist maintenance using
classifier thresholds from TGT-4016.

[~2025] TGT-4016 Ecomm Classifier Thresholds — Threshold analysis for ecommerce domain classifier.
P90 ≈ 0.9181 (whitelist), P10 ≈ 0.0002 (blocklist). 251M website visits analyzed.

[~2025] TI-502 IP Scoring Reference — Documented score types and model_params field structure.

[~2025] DM-3118/3188 RTC Monitor — Built RTC vs non-RTC impression monitoring query. Delivered
comparison data export.

[~2025] TI-34 Identity Sync Freshness — Established freshness monitoring for IP identity graph
and blocklist pipeline.

---

## Areas for Improvement

### Speed

**Estimating work and communicating timelines**
I don't consistently estimate how long work will take before starting, and I rarely communicate
expected timelines to stakeholders upfront. This means others can't plan around my deliverables.

- Before starting a ticket, write an estimate (even rough) in summary.md. Compare actual vs
  estimated at completion to calibrate over time.
- When an estimate is clearly wrong mid-work, flag it early rather than letting it silently slip.

**Tracking start/stop dates on long-running work**
TI-650 ran ~4 weeks across multiple rounds. The dates and scope of each phase blurred together
because I didn't rigorously track when rounds started and ended, or break emerging sub-problems
into their own tickets.

- For any ticket that extends beyond one week, log explicit start/stop dates for each phase in
  summary.md (e.g., "Round 2: 2026-03-05 → 2026-03-06, scope: display fallback").
- When a sub-problem emerges that's distinct from the original scope, create a new ticket for it
  rather than letting the parent ticket balloon. Smaller tickets are easier to estimate, track,
  and close.
- At the end of each week on an active ticket, write a 2-sentence status update in summary.md:
  what got done, what's next.

**Meeting preparation**
I sometimes come into meetings having done the minimum prep — I know the topic but haven't
thought ahead about what questions will come up or what context others need.

- Before every stakeholder meeting, spend 15 min reviewing the ticket state, open questions, and
  likely follow-ups. Write 3 bullet points of what I want to communicate and what I need from others.
- For review sessions (e.g., Zach's VV audit reviews), pre-run the queries I expect to be asked
  about so I have fresh numbers, not stale references.
- Target: zero "let me get back to you on that" responses in meetings by end of Q2.

### Craft

**Organizing what type of work I do and when**
I don't have a deliberate structure for how I allocate time across investigation, documentation,
code/query work, and communication. Days can become reactive — jumping between Slack, queries, and
docs without a clear plan.

- Block time by work type: mornings for deep analysis/queries, afternoons for documentation and
  communication. Protect focus blocks.
- At the start of each day, write down the top 1-2 things that need to get done. At the end,
  check whether they did.
- Track how much time goes to each category over a sprint and adjust if the ratio is off.

**Knowledge sharing and teaching**
I build deep expertise in data pipelines and analysis methodology but don't proactively share it
beyond ticket deliverables. The knowledge lives in my workspace and docs but doesn't reach other
engineers who could benefit. I'm supposed to be a knowledge-oriented person on the team — that
means teaching, not just documenting.

- Present at least once per quarter at Engineering All-Hands or team meetings. Next up: causal
  impact analysis methodology (Alyson's active goal).
- When completing a ticket that reveals non-obvious pipeline behavior (e.g., VV bridge using
  clickpass_log not event_log), write a short Confluence/Slack post for the broader team — not
  just update my local docs.
- Offer to walk other engineers through BQ pipeline tracing when they're working on related
  investigations. Be the person people come to for data pipeline questions.
- Target: 2+ knowledge-sharing presentations and 4+ written shareable artifacts by end of H1.

**Creating leverage from strengths**
My strongest areas (deep pipeline knowledge, empirical analysis methodology, documentation
discipline) create value for individual tickets but don't yet scale beyond my own work. The
question is: how do I turn personal expertise into team capability?

- Formalize the empirical analysis protocol into a shareable team guide — not just my CLAUDE.md
  instructions, but a "how to investigate data questions at MNTN" doc that any analyst or engineer
  can follow.
- Build reusable query templates for common investigation patterns (VV tracing, audience pipeline
  debugging, impression chain traversal) and publish them where the team can find them.
- When onboarding new engineers touches data pipeline topics, offer to be the walkthrough resource.
  Turn tribal knowledge into institutional knowledge.

### Adaptability

**Task selection and OKR alignment**
I tend to pick up whatever's next in the queue or follow what's technically interesting rather than
deliberately choosing work that maps to team/org OKRs. This means some high-impact work gets done
by accident rather than by design.

- At the start of each sprint, review the current team OKRs and explicitly map my ticket queue to
  them. If a ticket doesn't connect, ask Kale whether it should be prioritized over one that does.
- When starting a new ticket, write the OKR connection in summary.md upfront — forces me to think
  about "why this matters" before diving into the technical work.
- Build a habit of asking: "Is this the highest-leverage thing I could be working on right now?"
  before picking up the next task.

**Awareness of teammates' work and company direction**
I can get tunnel-visioned on my own tickets and miss what others on the team are working on, what
the broader org priorities are, and where the company is headed. This limits my ability to
contribute context in discussions and to align my work with what matters most.

- Pay closer attention in standups and team syncs — not just for my own updates, but to understand
  what others are blocked on or working toward. Look for places where my data knowledge could help
  unblock someone else.
- Read company-wide updates (All-Hands notes, OKR updates, product roadmap) regularly, not just
  when referenced in a meeting. Understand where the business is going so that my analysis work
  maps to real priorities.
- When starting a new ticket, check: does anyone else on the team have related or overlapping work?
  Could this be done in a way that helps them too?


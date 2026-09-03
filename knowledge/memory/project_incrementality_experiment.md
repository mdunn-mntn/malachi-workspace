---
name: project_incrementality_experiment
description: BER-2250 incrementality overhaul (ghost-bid lift, persuadables gradient, and the two MDE surfaces)
metadata: 
  node_type: memory
  type: project
  originSessionId: 9c582365-7ebc-49fa-9d1e-6d93ac47841b
doc_type: memory
keywords: [incrementality, ber-2250, ghost-bid lift, persuadables gradient, liftlab, kochava, guid_log, clickpass_log, ti-835, incremental roas, matt brorby, remove-ds14 experiment, ds14 availability gate, kirsa, audi-1176, audi-1117, audi-1215, elevenlabs, mde calculator, in-product testing tab, computeMde, forecasted_mde_percent, IPUserSiteVisitorRate, nick scialli, nick martin, chris franz, audi-1213, audi-1323]
domain: [incrementality, experimentation, project]
lifecycle: active
last_verified: 2026-09-03
---
BER-2250 "Incrementality Overhaul" is the highest-leverage initiative for Q2 2026.

**UPDATE (2026-09-03) — the in-product Testing tab is LIVE, and it is a SEPARATE implementation from the standalone MDE calculator.** Edgar von Trotha asked whether the MDE calculator is the same logic and data powering the new testing tab for ghost-bid incrementality tests. No. Same Lewis-Rao two-proportion formula family, separate implementation, different data. Verified against `SteelHouse/gary-ql@cbae0e94` and `SteelHouse/premier-ui@aaf65d59`.

- **Two live call sites, each importing its OWN copy of `computeMde`:** `gary-ql src/gql/types/IncrementalityExperiment/resolvers.ts` → `forecasted_mde_percent` (saved-experiment forecast), and `premier-ui src/app/scenes/Testing/ExperimentBuilder/useMdeForecast.ts` → ForecastSidebar and HoldoutSection (live wizard).
- **No user control over alpha or power:** `computeMde.ts` hardcodes `Z_ALPHA_2 = 1.96` and `Z_BETA = 0.84`, and `MdeInputs` carries no alpha or power field. `DEFAULT_VAR_REDUCTION = 1` (raw). `DEFAULT_COHORT_IVR = 0.0215` survives as dead code.
- **Inputs went live per advertiser 2026-08-25:** `DEFAULT_CPM = 24.84` and `DEFAULT_IMPRESSIONS_PER_IP = 1.5` were deleted; CPM and imps/IP now come from ChAPI per advertiser and the resolver returns null when either is missing or non-positive.
- **Baseline moved 2026-08-25 (RX-7420 series: gary-ql #4664 `27ffe77c`, plus #4662/#4665/#4666)** off the FPA `totalConversionRate` and `graph.usersreached` onto ChAPI per-IP-user rates: `Graph.IPUserSiteVisitorRate` (visits goal) / `Graph.IPUserConversionRate` (conversions), selected by `goal_metric`. `getAudienceFpaReportTotalsByAdvertiserId` is now dead code. This SUPERSEDES the 2026-06-24 note in `knowledge/experimentation.md` saying the UI uses `graph.usersreached` / the FPA conversion rate.
- **Forecast and measurement never touch.** The tab reads ghost-bid RESULTS separately (`significant_95` off the rollup). No measured lift ever feeds the MDE forecast; the forecast and the ghost-bid lift pipeline share no table.
- **Defect 1, arm split (in the tab AND in the shipped gist):** `computeMde` splits the spend-derived IP pool into treated and control, but the holdout is never served, so that pool IS the treated arm. Forecast MDE runs `1/sqrt(1-h)` too large = 1.0541x at h=0.10, 11.8% at h=0.20. This is PESSIMISTIC, not optimistic: fixing it makes tests look EASIER to power. Full convention and the round-trip check: [[reference_mde_arm_split]]. Filed as **AUDI-1323** (Spike, 0 SP, sprint 8303, Relates To AUDI-1213, assigned Malachi to route), writeup at `tickets/audi_1213_mde_calculator_refresh/artifacts/audi_1213_mde_arm_split_writeup.md`.
- **Defect 2, mislabeled stat (new 2026-09-03):** `premier-ui src/app/scenes/Testing/ExperimentBuilder/ForecastSidebar/index.tsx` renders `{ label: 'Impressions', value: formatCount(result.totalIps) }`. `totalIps` is an IP/household count, not impressions (impressions = totalIps * impressionsPerIp), so anyone backing a CPM out of that stat is wrong by the imps/IP factor. Still needs adding to the AUDI-1323 writeup.
- **Defect 3, gist `setOutcome` (found and fixed 2026-09-03):** in `ti_xxx_mde_calculator_prefill.html`, `setOutcome(o)` used a hardcoded `{ ivr: 2.15, cvr: 0.054 }` and never read `S.advertiser`, so toggling IVR→CVR with an advertiser loaded silently dropped that advertiser's own rate for the cohort default; it also never assigned `S.currentOutcome`, so `clearAdvertiser()`'s `setOutcome(S.currentOutcome)` always restored IVR. CONTRADICTS the TI-1019 summary claim that the IVR/CVR toggle re-pulls the advertiser's actual baseline when one is loaded — that was never true in the shipped file.
- **Both surfaces stay.** The tab forces selection of an already-live campaign group with the budget fixed to it, so only the standalone answers "what budget would this test need?" Tool choice: [[reference_mde_surface_choice]].
- **AUDI-1213 delivering-cohort refresh SHIPPED 2026-09-03:** 1,859 delivering advertisers (trailing 30d ending 2026-09-03, no $1k floor, up from 879), advertiser-facing CPM basis, all three defects fixed, republished to the same gist under the original filename so shared links stay valid. Still open on AUDI-1213: the 2,546 lapsed cohort, the Mode port, the VR_STACK 0.595 re-measurement. Numbers and method: [[reference_test_budget_from_rates]].
- **Nick Scialli is not Nick Martin.** Nick Scialli (eng) owns the in-product MDE view / Testing tab. Nick Martin owns Mode dashboards, is the TI-504 experiment owner, and appears in the ghost-bid lift register. Conflated once this session; do not.

**UPDATE (2026-08-24) — UI MDE view moved to Nick Scialli (eng).** Nick Scialli is implementing the in-product MDE view for incrementality testing (prior UI track: Chris Franz's gary-ql PR #4445, `Advertiser.mdeInputs`). Al Beretta routed him the TI-1019 gist calculator; we handed off `ti_884_mde_calculator.py` as source of truth with a warning not to port the gist JS spend conversion (holdout charged for impressions: required spend 1.1111x high, displayed MDE 1.0541x high, the AUDI-1213 defect). An eng-owned UI view may change who the AUDI-1213 refresh serves. **(Superseded 2026-09-03: it did not. The tab cannot do what-if budgets, so the delivering half stayed in scope and shipped; the tab independently carries the same arm-split defect the port warning named. See the 2026-09-03 update above.)** See `tickets/audi_1213_mde_calculator_refresh/summary.md` §8.

**UPDATE (2026-08-21) — ElevenLabs escalation (AUDI-1215).** Customer paused the $770K CGID 122748 campaign 2026-08-20 citing no lift ($10-12M annual account). Verdict: visit lift real in both periods (+11.1% pre / +16.5% post, change n.s.), but incremental visit volume fell ~4x and the powered fixed-holdout conversion instrument shows lift fell 36% after the 6/30-7/29 change bundle. Frequency finding (lift peaks at 2-10 exposures, -17.7% at 11+) backs the frequency-target recommendation. Gruns frequency spot-check floated by Edgar as a follow-up. **Sanity flag, UNRECONCILED (2026-09-03):** advertiser 51660 reads IVR 0.58% / CPM $31.80 on the trailing-30d window ending 2026-09-03 against 3.07% / $8.58 in the June run; the paused $770K campaign group sits inside that window. Not reconciled. See `tickets/audi_1215_elevenlabs_lift_post_audience_change/summary.md` and [[reference_holdout_lift_lineage]].

**CURRENT STATE (2026-07-24) — read first; the April content below is historical.**
- **Measurement ownership moved to the INCR project / First Ascent team.** Matt Brorby owns the ghost-bid lift pipeline; Ryan Kleck owns the bidder/holdout. We *consume* the measurement, we don't rebuild it. See [[project_bidder_level_ghost_bidding_approved]].
- **Ghost-bid lift is productionized:** gold `dw-main-gold.reporting.lift__ghost_bid_{results,rollup}` (time-boxed AUDI-1148, accumulates no-TTL). Query gated, aggregate per-campaign `abs_itt` with **inverse-variance weights, never a naive count pool** (that gives a Simpson-confounded no_score +29%; IVW → ~0).
- **The persuadables gradient (refreshed 2026-07-24, holds on the wider window):** Mid +9.2% · MaxReach +6.6% · PP +1.8% · High +1.7% · no_score +0.2% (~dead). Mid-intent carries the lift; top-intent + untargeted reach are incrementally dead. Raw-visit rank is ~INVERTED vs incremental-lift rank.
- **AUDI-789 (RTC/Fangorn scoring) is the go-forward targeting vehicle** — a visit/spend-optimized scorer de-optimizes incrementality unless lift is a target/guardrail.
- **User steer 2026-07-24:** treat these as old/reassigned work — don't keep extending BER-2250/AUDI-789 unprompted. See [[feedback_dont_extend_old_tickets]].
- **REMOVE-DS14 EXPERIMENT: RESOLVED DEAD 2026-08-25, provenance found.** The 7/30 source was a Slack thread: Kirsa, planning "the incrementality experiment we discussed in our Tuesday meeting", floated the DS14 recency block as "a useful treatment" and asked for its mechanics. In the SAME thread Malachi answered (1d aug + 4d guid build, 8d TTL, 9-12d max) and assessed "I don't think this is gonna have much impact on incrementality. 9-12 days is a long time to NEVER be seen"; Kirsa agreed "probably not super impactful". It was a floated idea, downgraded within minutes, never a planned experiment — but the 7/30 capture recorded it as a hard plan and parked AUDI-1176 on it for 4 weeks. Kirsa confirmed 2026-08-25 she doesn't recognize it. The AUDI-1176 hold is VOID. (Related learning: [[feedback_state_query_provenance]].)
- **Remove-DS14 experiment treatment (Kirsa, 2026-07-30 — unverified, see above):** Kirsa is designing an incrementality experiment that uses **removing the DS14 availability gate** as a campaign treatment — bidding on IPs NOT recently in the free logs. DS14 = "MNTN Global Data" IP-recency gate (built from augmentor_log 1d + bidder_auction_events 1d + guid_log 4d; ~8-day serving TTL, though conditional — could be 30d; see [[project_audi_1175_ds14_scoring_cost]]). Removing it ~1.6× (verticals) / ~3.2× (MM Core) the biddable pool onto scored-but-not-recently-seen IPs; **negligible for display** (100% same-day augmentor echo), **material for CTV** (empirical DS14-window ticket = AUDI-1117). **Mechanism to remove it:** the DS14/cat1 clause is baked into the STORED audience-segment expression at creation (like DS13's `PEAK_PERFORMANCE_DATA_SOURCE_ID=13` in mntn-go), NOT injected at bid time — so removal = edit the segment/expression builder, not a membership-db config. Sign-off before prod: Sean Yang / Zach Schoenberger / Ryan Kleck. **CONFLICT:** AUDI-1176 (gate scoring input to the DS14-addressable set) would FORECLOSE this experiment (it needs the full scored universe). Incrementality is Q2 #1 → **sequencing decision (endorsed by Malachi 2026-07-30): run this experiment FIRST, then AUDI-1176 after / gate output-only; final go = incrementality team.** Recorded on AUDI-1176 (Jira comment + summary §0/§4).

**Kale's direction (2026-04-08):** "The most valuable thing right now is getting this incrementality thing out. Solving this would be HUGE and would dramatically change growth and retention." Everything regresses to incrementality / incremental ROAS.

**The core problem:** MNTN likely looks bad on third-party incrementality platforms (LiftLab, Kochava) because everything is optimized toward the visit. Internal metrics (clickpass_log) overstate true incrementality. External vendors measure something closer to total business impact (guid_log-like).

**TI-835 observational finding confirms this:** guid_log shows ~0% lift (no net new traffic from CTV ads). clickpass_log shows 2-8x lift (attribution capture). The gap between internal and external measurement is the problem.

**Strategic shift:**
- Shutter internal incrementality dashboards → move to approved third-party vendors
- OKR: Run 5 experiments with external vendors
- Change targeting methodology to optimize for incrementality, not just visits
- Customer-driven: ask advertisers what they want (reach, performance, incrementality) → tailor experience
- Need a dedicated LiftLab liaison/DS
- CPM pricing → incrementality changes don't directly hit profit, but IVR will suffer
- **Incremental ROAS** is the top metric, not incremental visits

**Key external vendors:**
- **LiftLab** — primary, keeps coming up
- **Kochava** — another option
- Possibly more

**Three workstreams (Alex Bloore, 2026-04-08):**
1. Intent Score Shuffling Experiment (product brief — TI-837/839/842)
2. Population Split / Deciles (TI-831) — random A/B for customer testing
3. Observational Analysis (TI-835) — baseline using 10% holdout (DONE)

**Tickets:**
- BER-2250: Parent initiative
- TI-831: Audience Deciles for Advertiser Experimentation
- TI-835: Control group design and measurement methodology (3 SP) — **ANALYSIS COMPLETE**
- TI-837: Implementation plan for intent score shuffling (5 SP)
- TI-839: Measure incrementality results (5 SP)
- TI-842: Present results to broader audience (3 SP)

**Product brief:** https://mntn.atlassian.net/wiki/external/NTM1ZmViMzc1YzczNDQ0YjgzZDVlMjdkNTk2ZGY4NmY

**Why:** Existential for the business model. If we can't show up well on third-party incrementality, advertisers will shift budget away. Solving this = competitive moat vs Meta/Google.

**How to apply:** BER-2250 tickets are Tier 1. When external vendor experiments come up, prioritize immediately. Frame all incrementality work in terms of incremental ROAS, not incremental visits. When presenting findings, explicitly connect internal metrics gap to external vendor measurement gap.

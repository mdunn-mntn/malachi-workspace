# TI-1037: Automate client-performance diagnostics (audience-expression-driven)

**Jira:** https://mntn.atlassian.net/browse/TI-1037
**Status:** In Progress (sprint 06/15–06/29, id 6926) — **UNBLOCKED** 2026-06-18 (Chris Addy deliverability deep-dive done)
**Parent:** TI-602 · **Relates-To:** TI-1026 (prototype), TI-956 (segment-quality engine) · **Assignee:** Malachi

---

## Update 2026-07-08 (night) — Module 13 pixel-monitor AUDIT (22-agent adversarial verification)

Full audit of `13 Pixel Health.sql` + its `_flags` 2c consumer. **The SQL contract is sound** — schema/typing,
partition pruning through the re-versioned UNION-ALL view (~831 GB/refresh, verified), Liquid hygiene,
module-12 param parity, and a month-for-month WGU ground-truth replay all pass; every detection branch fires
where it should (Oct'25 STOP check: cov 0.994→0.032, no threshold slip). Verified findings, worst first:

- **FLAG (SQL, 1-line fix): sentinel exclusion incomplete.** Six bare-negative-integer platform pseudo-types
  exist, not two: `-100/-101/-102/-105/-106/-107`. The unexcluded four produce ~46 fake "+1 new type (-1xx)"
  flags in the default window (verified live: 39667 renders "+1 new type (-102)" Aug'25; 20 of 21 `-102`
  advertisers affected). Fix: `AND NOT REGEXP_CONTAINS(conversion_type, r'^-[0-9]+$')` — regex verified to
  match exactly the six and no genuine/pentest type.
- **FLAG (SQL+JS): zero-fire months emit no row → total pixel death fails OPEN.** No month scaffold; JS
  compares adjacent ARRAY rows, so a boundary-aligned complete stop, a stop in the clamped-out current month,
  or any sub-1000-rows/mo advertiser's death renders "none/ok"; a multi-month outage w/ similar-volume resume
  is invisible at both ends. Platform prevalence: 7.8% of advertisers (1,189/15,306) have ≥1 mid-history
  zero-fire month; 705 miss ≥3. Fix: GENERATE_DATE_ARRAY spine emitting zero rows (also fixes gap-straddling
  ratios), or calendar-adjacency in JS.
- **FLAG (JS): `slice(0,4)` digest truncation buries the flagship hit.** WGU default replay = 7 hits; "Oct'25
  order values STOP" (THE receiving-side explanation of revenue→$0) is item 5, hidden behind "+3 more". No
  severity ranking inside pxHits; only ≥10-type bursts get promoted. (Oct volume step is 2.4x in raw fires —
  the 3.2x figure is attributed convs.)
- **FLAG (JS): Feb'26 fake pentest revenue ($222.9M in px_sum_amt) surfaces NOWHERE** — sumAmt is only read
  by the $1-placeholder check; no sum-spike check, and dataset 13 has no other consumer. A $0-placeholder
  variant (sum=0, n_amt>0) is equally invisible. Both are JS-only fixes on columns already shipped.
- **WATCH (JS): "$1 placeholders (thru Jan '25)" mislabels era START as END** (latch fires on first match;
  WGU's era ran through Oct 2 '25) — internally contradictory line, since the same row correctly shows the
  Oct'25 signals. Track last matching month or say "from".
- **WATCH (JS): `pv.rows > 1000` guard** disables volume+STOP checks entirely for ≤1000-fires/mo advertisers
  and is one-sided (999→50,000 spike silent if sustained/last-month). `cov0>=0.5` floor misses sub-50%-baseline
  coverage collapses (mixed purchase+lead pixels); `cov1<0.1` misses 100%→15% partial collapses.
- **WATCH (SQL): mid-month Period_Start/End fabricate volume steps on BOTH ends** (partial month vs full
  month; start needs day ≥~17th + >1000-row stub to cross 2x; a past mid-month End has NO JS skip — e.g.
  end 06-15 → "fires ÷2.2"). Fix: month-truncate scan bounds.
- **WATCH (JS): bsrc bounds chain omits px** — module 13 emits p1_start..p2_end but nothing reads them; if
  query 11 ever errors (the only realistic bsrc-null path — 11 always returns ≥1 row otherwise), the px row
  lies "run query 13". One-term fix: append px to bsrc.
- **WATCH (substrate): silver NULLs order_amt ≥$100M (rows kept)** — a ≥$100M injection reads as a coverage
  drop, never a sum spike; WGU-class sub-$100M fake sums remain visible. data_catalog/knowledge corrected
  ("strips rows" → "NULLs amounts").
- **Notes:** px_ips/px_n_types are dead columns (px_ips would catch single-IP floods <10 types — Feb'26 was
  1 IP; px_n_types would catch type-consolidation); stale header comment (claims 1900/2099 defaults; real
  Mode defaults 2026/2027-01-01, sentinel branches dead-but-safe — same in module 12); driver text
  overstates ("raw pixel fires" = silver, advertiser-scoped, 2024-01 floor — dead-AID fires like WGU 10942
  invisible by design); types_added 40-char JS cut has no ellipsis; LEFT-JOIN registry drop is empirically
  nil (2/40,437, both source-31 offline batch regs).
- **Refuted by verification (do not act on):** "new advertisers get a guaranteed first-month Purchase false
  flag" — the 2026-03-31 Purchase-registration wave lives in a 50M+ advertiser_id namespace absent from the
  advertisers dim (never selectable in the dashboard); real new advertisers get first-month Purchase regs at
  0.19%. Platform-level registry stats ARE inflated by it (now in data_catalog).
- Registry otherwise validated: 141,431 rows, zero dup triples, zero NULL types, create_time=first-fire holds
  for pixel sources; re-registration under a new source_id negligible (26 pairs); WGU months reproduce
  exactly (1 real type Sep'25, 75 junk Feb'26, migration burst 100% `-101`-absorbed).

## Update 2026-07-08 (later) — WGU-REV spin-out validates the dashboard; module 11 units fix

The WGU revenue-to-zero anomaly (spotted on this dashboard) was investigated end-to-end same-day
("WGU-REV" queries in bq_perf_log; full case study in `knowledge/data_knowledge.md` § "WGU (31357)
revenue" + "Detecting an advertiser pixel/tag change"). Relevant to THIS ticket:

- **Chart scope independently validated:** reproducing `05_monthly_metrics` scope (obj=1 AND fl=1 AND
  deleted=FALSE from `integrationprod.campaigns`) matches the rendered WGU revenue series month-for-month,
  incl. the Sep '25 tooltip **2,488 exact**. No other scope (obj≠4, fl=1-any-obj, obj=4, all) comes close.
- **Module 11 units bug FIXED (needs re-paste into Mode):** live `advertisers` row stores
  `conversion_window` as `'720:00:00'` (**HOURS**; = 30d) while the archive is day-grain — the live-row
  fallback regex read 720 as days and manufactured a phantom "conv 30→720" change stamped at the live
  update_time. Fix in `batch1_queries/11 VV Window...sql`: normalize `^\d+:` strings via DIV(...,24).
- **Pixel-health (query 13) future enhancement candidate:** a rogue/legacy-AID sweep (GROUP BY
  advertiser_id over NET.HOST(referer) LIKE advertiser domain, then anti-join `advertisers`) — WGU's lead
  event fires under **dead AID 10942** (~18K/mo dark), invisible to any per-AID query the dashboard runs.
- CS pixel-QA internal note (Jessica DeLeon) independently confirmed from conversion_log to the day:
  untyped LP-tag bursts 04-30→05-16 and 06-24→present — the "possible tracking changes" flag's WGU
  validation pattern is real and recurring.

## Update 2026-07-08 (evening) — params finalized, flags v3 (15 signals, impact-banded), pixel detection, CIL floor 2023-10

- **Params final design:** Advertiser = query-backed searchable dropdown ("id · name", 4,962 advertisers with
  18-mo spend; `options: labels/values` so consumer queries are untouched). Periods = free DATE PICKERS (the
  dropdown experiment was built and REVERTED — Malachi wants arbitrary dates): start sentinel 1900-01-01 →
  Jan 1 of current year; end EXCLUSIVE, clamped `LEAST(end, first-of-current-month)`, default 2027-01-01 =
  "through the last full month" all year. All queries parse params as `DATE(LEFT(p,10))`. **Mode landmines:**
  select defaults must be QUOTED (unquoted YYYY-MM-DD YAML-datifies → matches no option → param EMPTY); an
  undefined/broken param substitutes as empty string and kills EVERY consumer query at once ("no data
  everywhere" incident); date params take only static defaults.
- **09rt final semantics:** prospecting = obj 1/5/6 (all stages); RTC serves count (still a targeted touch);
  HI = 10000 at bid time; `rt_prosp_first` feeds the score-agnostic **IP Recirculation tab** (full window, no
  score floor); HI tab gained a Re-touch % column. Module 06 tier shares are now of SCORE-LOGGED impressions
  + a coverage row (a P1 straddling the logging onset read "4.5% HI" when ~98% of scored imps were HI), and
  deltas are in percentage POINTS (relative % on near-zero shares printed +2,021,727%).
- **Flags v3 — 15 signals from Malachi's 11-check spec, impact-ordered in three bands:** outcomes (spend,
  metric moves ≥15% direction-aware) → drivers most-impactful-first (avg HI share <90%/−3pp bars → no-gate
  campaign-days → avg HHST → high-spend low-gate w/ holiday attribution → short flights → MM usage →
  geo/3P restriction of MM → DS16/21/34 adds → HI then all-IP re-touch at 50%/80% bars) → **measurement
  confounds quarantined at the bottom** (VV lookback; MoM tracking-change detector: visits/convs steps ≥1.5x
  at ±33% flat spend + order-values-stop; pixel/tag changes). A node smoke harness (scratchpad
  `flags_smoke*.js`) renders the scorecard against fabricated datasets and asserts row presence AND band
  ordering — reused each flags edit.
- **New query "13 Pixel Health"** (conversion_log monthly shape + `core_advertiser_conversion_types`
  registry): advertiser-side tag changes reconstructed from the receiving side — **no MNTN table tracks
  their tag manager (Kevin Cipriani, 2026-07-08)**. WGU validation reads the whole case: Sep'25
  `app_submitted` registered → Oct'25 fires 2.4x + order values collapse → Nov'25 stop → **Feb'26 75-type
  SQL-injection burst (pentest — full forensics in WGU-REV: Burp Suite, IP 136.60.22.42)** → May'26 fires
  2.8x. Injection bursts are promoted ahead of the flag row's 4-item display cap.
- **CIL floor corrected to 2023-10-01 FIXED** (~33 months and growing; partitions + 53–92M rows/day
  verified) — 2-year all-IP cumulative works TODAY; HI cumulative is floored at the Jun'25 score onset
  (2 years of HI history arrives Jun 2027). Cumulative counts are window-relative (left-censored at window
  start). **WGU runs ungated:** 5 core campaigns at HHST≤0 every delivering day both halves; first gated
  campaign ever = 127483 (Apr'26+).
- Layout: 820px table cap scoped to the YoY table; narrow tables capped 900–1000px; flags/summary tables and
  the monthly grid fill the width. Low-gate flag reworded (gate SETTING below 6666, P2-only check).

## Update 2026-07-08 — Retargeting tab + Geo + flags scorecard; HI & frequency semantics settled

Batch-1 completion via the paste-deploy workflow (index = 5 tabs: Overview / Audience & Scores / Gate &
Flights / Delivery & Measurement / Retargeting). Deploy queue at close: 09rt SQL (v5) + index pending paste.

- **New modules:** `_flags` Overview scorecard (client-side cross-dataset P1-vs-P2: spend Δ w/ single-month
  driver, avg HI share of scored imps, VV window, DS16/audience changes, short flights, no-gate days — FLAG/
  WATCH/OK chips); `12 Geo Changes` (changelog-style, REGEXP `location_ids` → "US (national)" vs N/210 DMAs;
  finding: **108055 narrowed US→84/210 DMAs on 2026-01-27**); `09rt Retargeting Reach` (dedicated RT tab, 3
  panels: new-vs-returning HI + new-share line / cumulative distinct HI / reach + median frequency). 00b
  re-scoped to a **whole-window campaign-group summary** (durable summarydata funnel metrics + full-window CIL
  reach & score split; every campaign visible; sums to 100%). Monthly trends: revenue-connected metrics red,
  $0-spend months shaded.
- **HI semantics (consistency directive):** an IP counts as HI only from the month a qualifying score was
  actually observed — no borrowing from the future; same rule as module 06. For 09rt recirculation the bar is
  **full hs=10000 only** (8001–9999 deliberately excluded, per Malachi: retargeting an 8000-scored IP is not
  recirculating a top-scored one). Bouqs is still bucketed (non-Fangorn ⇒ HI is EXACTLY 10000), so the stricter
  bar changes nothing there — validated **cum distinct retargeted HI = 176,274** by May '26 (vs the rejected
  278k future-borrowing figure); it will matter on Fangorn-flipped advertisers.
- **Frequency = MEDIAN imps/IP, never mean (CGNAT skew):** Bouqs RT Feb'25 mean 46.8 vs **median 8**; worst
  single IP 8,020 imps/mo; top-100 IPs = 6.4% of impressions from 0.09% of IPs. Norms (same advertiser,
  Apr–May '26): prospecting median 1–2 / mean 1.6–1.7 (the familiar "1–4"); RT median 8–9 / mean 24–31 /
  p90 66–94 — retargeting is inherently ~5× hotter, that's the product not a bug. `rt_freq_median` added to
  the 09rt SQL (`APPROX_QUANTILES`); table shows `med (avg)`.
- **09rt REDEFINED (2026-07-08, later — supersedes the obj=4 version above):** "retargeting reach" is NOT
  obj=4 campaigns; it's the **re-touch rate inside prospecting** — without DS16 prospecting re-serves
  already-touched IPs (VV/conversion excludes only remove visitors/converters). New semantics: base =
  prospecting (obj=1/funnel=1, RTC-excluded, mirrors module 06); HI = served at a FULL 10000 that month;
  re-touched = also served at 10000 in a prior month ("10000 both times"). Same rt_* column aliases so the
  render plumbing survived; tab renamed Retargeting → HI Recirculation. Bouqs validated: prosp median freq
  1–4 ✓; re-touch share 0% (Jun'25) → 75% (May'26), dips when fresh pools open (Jan/Mar '26 new campaigns);
  cum distinct 10000-IPs 2.54M. Also fixed: zero-row datasets now render the module's empty-state instead of
  "no data (Run the query)" (hit on 40341 — zero RT campaigns); flags No-gate row now shows its pp delta.
- **Params v3 + end-date clamp (2026-07-08, later):** Advertiser_ID = searchable query-backed dropdown
  (A–Z, "id · name", 4,962 spenders in last 18 mo; labels/values so consumers untouched). Every query now
  clamps Period_End to first-of-current-month (`LEAST`), so the new far-future default (2099-01-01) means
  "through the last full month" automatically — Mode date params can't do dynamic defaults. 09rt scope
  widened to ALL prospecting stages (obj 1/5/6) per Malachi; Bouqs re-validated (cum 2.62M, re-touch 75%
  May '26; June '26 HI-served collapses to 31k partly by construction — the mid-June Fangorn flip ends
  exactly-10000 scoring, and the recirculation bar is 10000-only). WGU 09rt dry-run: ~1.7TB.
- **Flags scorecard v2 (2026-07-08, later — Malachi's 11-check spec):** Overview scorecard expanded from 6
  to 13 signals, all client-side from existing datasets (no new queries): spend Δ; ALL-metric moves ≥15%
  (direction-aware, red=adverse); MM usage (DS19/13/46, flag if ≥20% of spend without MM); avg HI share
  (<90% either period OR ≥3pp drop = flag); VV shortened; MM restricted by geo slice (<80% of DMAs,
  count-proxy) or 3P∩MM; gates ADDED (DS16/21/34) in-window; re-touch share all-IP + HI (50% watch / 80%
  flag); short flights (any in P2 = watch, increase = flag); HHST=0 days (any = watch); avg HHST (drop
  >500 = flag); high-spend low-gate campaign (≥15% spend, ≥30% days below HI+PP, holiday-aware). Header
  shows flag/watch counts. Smoke-tested via node harness with fabricated datasets (13 rows, 11 flags).
- **Tooling gotcha:** SQL passed to `bq query` as a positional arg must not START with a `--` comment — bq
  parses it as a CLI flag (`FATAL Flags parsing error`). Strip leading comment-only lines when templating
  staged .sql files into a shell arg.

## Update 2026-07-07 (later) — Mode batch-1 debug: stale datasets + VV chart date-adapter fix

Continuation of the batch-1 Mode deploy (commit 8d13650 / mode-assets PR #9). Two symptoms in the live report, both diagnosed:

1. **"Gate trajectory shows 4 dead zero-spend groups, missing 85384" → STALE DATASETS, not a SQL bug.**
   The synced `03 HHST Gate History` SQL is correct — run in BQ with the report params (32147,
   2026-01-01→2026-06-01) it returns **21 groups, 9 with spend** (85384 $103k, 119362 $199k, 108055 $144k …).
   The rendered dataset (only 64534/64544/80969/80970, all $0, and "20 chg" where the SQL yields 25) can only
   have come from an earlier draft run. **Mode's `window.datasets` = the last report Run; a git push updates
   query definitions but NOT the data → always hit Run in the Mode UI after a deploy.** Ribbon/flights showing
   "only one campaign" = same staleness (fresh: ribbon 21 groups, flights 16 groups / 88 flights for 85384).
2. **"Verified-visit window: render error … complete date adapter" → real HTML bug, fixed.** Module 11 used a
   Chart.js `type:"time"` x-scale; the report loads only `chart.umd.min.js` (no date adapter). Fixed by
   switching to the proven module-03 pattern: `type:"linear"` over epoch-ms + month tick callback. Gotcha
   comment added at the Chart.js include.

**RESOLVED 2026-07-07 (evening):** PR #10 closed unmerged — git→Mode edit-sync doesn't apply outside-Mode
changes, so **deploy = paste into the Mode UI** (workflow settled; see README_MODE.md + memory). Malachi pasted
the fixed `index.html` (VV module renders) and re-Ran the report — datasets refreshed and all modules now show
the full picture (the Mode queries were already current; only the Run was missing). Also added a **tabbed
layout** to `index.html` (Overview / Audience & Scores / Gate & Flights / Delivery & Measurement; lazy-render
per tab so Chart.js sizes against visible containers) — modules declare a `tab` in the registry.

**Spend-% design standard (superseded 2026-07-08 — basis now UNIFIED):** the initial 2026-07-07 standard
gave modules their own bases (prospecting-only obj=1/funnel=1 spend for 03/03b/07/08; a 45d in-TTL window for
00b) — the cross-module %s contradicted each other and Malachi called it ("unify"). Final rule: **every
module's % spend = whole campaign-group spend over the full P1→P2 window — ALL funnel stages, retargeting
(obj=4) excluded** — one denominator everywhere, stated in each subtitle; 00b displays exactly that scope so
its table sums to 100%. RT delivery appears only in the dedicated Retargeting tab (09rt) and the planned
Campaign scope dropdown.

**DS16 / DS46 timeline (from audience_segment_archives, 2026-07-07):** DS16 (net-new gate) was added to
Bouqs' 2026 prospecting campaigns starting **2026-04-14** (v2 campaigns got it same-day as creation) —
changelog shows NET +DS16 for groups 117983/119361/119362/119363; 85384 (old flagship) instead DROPPED
DS16 in-window (net −16, matches audit "—"). Then on **2026-06-15/16 the same campaigns got +DS46 (Fangorn
overlay) and dropped DS13** → Bouqs' Fangorn flip is mid-June 2026 (post-Period_End, so visible in the
changelog only with a later Period_End; the live audit shows it). Changelog = state as of Period_End;
audit (module 00, batch 2) = live state — they answer different questions.

**Real client finding queued behind the fix:** Bouqs (32147) **PRO VV lookback cut 30d→14d on 2025-11-18**
(archives_advertiser_archives; also RT 30→7→14 same day). P1 (Jan-Jun'25) measured at 30d, P2 (Jan-Jun'26) at
14d → module 11 will (correctly) flag a measurement confound in any P1-vs-P2 visits/conversions comparison —
this is Nick's decline-reason #5 live on the pilot account.

---

## Update 2026-07-07 — Nick walkthrough: port the tool to a Mode dashboard

**The tool IS built** (supersedes the "nothing built yet" note below): `perf_report/` is a parameterized
YoY client-performance report — `run_report.py` + `report_spec.py` + `charts/` + `queries_exec/` — ~21
modules + an **overview flag scorecard**, run for Bouqs (32147), Kindred (35094), Bouqs Subs (31906).
Meeting transcript: `meetings/ti_1037_01_nick_mode_dashboard_2026_07_07.txt`.

**Direction (from Allison): deliver this as a Mode dashboard.** Nick — experimentation team, ex-Criteo
(built a campaign-troubleshooter there), owns the causal-impact Mode dashboard — gave the porting
walkthrough. Mechanics captured in memory `[[reference_mode_dashboard_porting]]` /
`[[project_audi_1037_mode_dashboard]]`. **Load-bearing porting fact: Mode can't render matplotlib PNGs —
the charts must be rebuilt as HTML/JS.** Keep the (advertiser + period) parameterization; point Claude at
Nick's causal-impact `index.html` in the `modeassets` repo (AUDI space) for styling.

**The 5 standard reasons a prospecting campaign declines YoY** (Nick independently confirmed the same
4–5 buckets from his own troubleshooting — this is the tool's diagnostic spine, 1:1 with its modules/flags):
1. **Audience-size inclusions/filters** (geo or 3P narrowing) — the "shot-yourself-in-the-foot detector"
   (MM pool 6M filtered to 1M). Not wrong per se, but if new vs before, expect the hit.
2. **High-intent % dropping** — HHST auto-paces up/down on deliverability; **short flights (<3d) crash the
   gate to 0** (fallback so the threshold never blocks spend).
3. **DS16 excludes impressions** (net-new gate) — beyond the standard prospecting exclusion (converters +
   visitors only); DS16 additionally removes anyone impressed.
4. **High-intent recirculation** — even at ~100% HI, removing converters each pass funnels out the good and
   leaves the bad; worse when spend jumps (real client: ~98% HI, perf still fell, spend +150%). Converters
   return after ~30d (stage-1 block ≈ 30d). NB **prospecting is NOT purely new users** — stage-1 only excludes
   converters/visitors, so there's multi-touch within prospecting; retargeting is often ~last 20% of budget.
5. **VV-window change** — a client setting a 14d verified-visit window attributes fewer visits → fewer
   conversions/ROAS → looks worse though nothing "changed"; a YoY comparison needs extra time to normalize.

**First job of the tool = verify the client's numbers** (origin: a Neon Pixel escalation — client claimed
flat spend + bad perf; reality was spend −15%, perf +10%). "The Bouqs" is a Neon Pixel account.

**Feature ideas (Nick):** (a) **scope toggle** advertiser-level vs campaign-group-level (+ dropdown
all / stage-1-only); (b) **overlay campaign-change events on the performance timeline** ("did CPA drop after
this change?"); (c) guided **decision-tree** framing (Criteo troubleshooter — straightforward question +
graph, human answers; the machine makes the visual, the human makes the call); (d) the **flags list is the
headline** (already built = the overview scorecard); (e) shareable with **PECs** to triage/flag.

---

## Current state (read first if you're a new session)
> **New chat?** Paste the block in [`HANDOFF_PROMPT.md`](HANDOFF_PROMPT.md). It primes a fresh session with the spec,
> the prototyped modules, the load-bearing facts, and next actions.

- **Spec is written:** `knowledge/audience_diagnostic_playbook.md` — the diagnostic as steps 0–9 (each = a tool module:
  question → query → interpretation → gotcha). **Step 9 (deliverability) is now scoped** (see §4).
- **Prototype is done:** every step 0–8 is prototyped in `tickets/ti_1026_orange_theory_audience_eval/` (queries/ +
  artifacts/ + a full worked example in its summary.md).
- **Nothing built here yet** — folder is scaffolding + handoff. **Build plan is drafted (§5)**, ready to start on approval.

## 1. Introduction
TI stakeholders repeatedly ask "why is this client performing this way / why is the audience small / which 3P segments
should they use?" The first step is always the audience expression, and the analysis is highly systematic (proven by
TI-1026, Orange Theory). This ticket productizes that into a parameterized tool / query-series.

## 2. The Problem
Today every such question is a bespoke investigation. We want an on-demand diagnostic that takes an
`advertiser_id` (+ `audience_id`/`campaign_id`) and emits a standard report covering: expression decomposition,
3P-segment quality, keyword evaluation, the size funnel (geo + exclusions), scoring/HHST, availability, targeting-vs-
creative, UI-size-vs-deliverable, and (once scoped) deliverability.

## 3. Plan of Action
1. ✅ **[UNBLOCKER]** Deliverability deep-dive with **Chris Addy** (Olympus/media-plan) → step 9 scoped (§4, 2026-06-18).
2. ⏳ Define the parameterized diagnostic spec (inputs + the 10 modules steps 0–9; what each emits).
3. ⏳ Build the query series / tool (productize the TI-1026 queries; parameterize advertiser/audience/campaign).
4. ⏳ Standardize the output report + validate on a second advertiser.

## 4. Step 9 deliverability — scoped (Chris Addy deep-dive, 2026-06-18)
**There is NO predictive targetable-IP model.** The platform does not compute "what % of an audience's IPs will be
biddable in period X." So the tool must never promise a targetable-% number — answer deliverability empirically.
**Deliverability target = peer pacing to 96% of budget:** what did *comparable* campaigns require (last ~60–90 days)
to hit **96% of budget** (the "fully delivered" bar), then judge this campaign's spend vs that peer envelope, scaled by
flight length. → **Step 9 = a peer-pacing benchmark, a sibling of Step 7's peer-VR benchmark** — queryable in-tool from
`spend_log` (nanosecond epoch) + the budget/cap field + delivery logs. No external Olympus dependency. Comparable-cohort
selection is the design-sensitive part. Full detail: playbook Step 9 + `data_knowledge.md` ("How deliverability is
actually set"). Distinct from the Media-Plan `deliverability_classification` guardrail risk bucket.

**Budget field resolved + empirically de-risked on OTF (2026-06-18) — the naive chain was wrong:**
- **`campaigns.dso_manage_budget=TRUE` for all OTF campaigns** (the common prospecting case) → the operative budget is
  **DSO-managed**, NOT `campaign_groups.budget` (static 70000) or `core_flights` via `active_flight_id`.
- **`campaign_groups.active_flight_id` is STALE** (OTF: 344166 → a 2022 flight, budget 2.5). **Never use it.** Get the
  real current flight from the latest `dso_campaign_group_flight_budgets.flight_id` (OTF: 1041900 = 2026-06-03→07-01,
  budget 70000) and join THAT to `core_flights` for the window.
- **Budget resolution order:** `dso_manage_budget` → DSO flight budget (latest by `update_time`) for the cap + its
  `flight_id`→`core_flights` window, DSO daily (`dso_campaign_group_daily_budgets.budget`, OTF ≈ $1,690/day) for the
  per-day pace; else `core_flights.budget` for the flight covering `as_of` (by date range) → fallback `campaign_groups.budget`;
  as-of history = `archives_campaign_group_archives` (MAX(version) ≤ as_of).
- **Spend numerator:** `logdata.spend_log.win_cost_micros_usd / 1e6` (grain `campaign_group_id`/`campaign_id`; date-filter
  the `time` partition). Pacing = `spend / (flight_budget × elapsed_days/flight_days)`; "fully delivered" bar = 96%.
- Full mechanics: `data_catalog.md` ("DSO-managed budget tables" + "logdata.spend_log"). **Step 9 is now fully buildable.**

## 5. Build plan (steps 0–9 productization) — drafted 2026-06-18 via design panel

**Approach (recommended): hybrid — SQL templates + a thin importable Python package + a CLI, emitting ONE standard
markdown report.** Chosen over a full installable package (right end-state but ~8–13 SP — would force step 9 + 2nd-
advertiser validation out of v1) and a papermill notebook (notebook hidden-state + toolchain friction). The hybrid keeps
the lean shippable spine but borrows two things the bare CLI lacked: (a) logic lives in an importable `diag/` package so
the v2 parser + cohort builder are unit-testable, and (b) a **golden-file regression** against the frozen TI-1026 outputs
as the hard correctness gate. Everything lives under `tickets/ti_1037_audience_diagnostic_tool/`.

**Layout**
- `queries/ti_1037_*.sql` — TI-1026 SQL with every OTF literal → `{placeholder}` (lowercase, ticket-prefixed).
- `artifacts/diag/` — `params.py` (DiagContext = input+resolved contract) · `resolver.py` (advertiser_id → all derived
  ids, ONE place) · `expr.py` (**unified v1 audiences-parser + NEW v2 segment op-tree walker** — highest-risk, golden-filed)
  · `render.py` (str.format + guard asserting no `{placeholder}` survives; INT64-only id typing) · `bq.py` (shells out to
  `.claude/scripts/bq_run.sh` → inherits perf logging + literal-dt safety) · `interpret.py` (`interpret_stepN(rows,ctx)→Finding`,
  rules lifted verbatim from the playbook) · `cohort.py` (**shared comparable-campaign builder for steps 7 AND 9**) ·
  `report.py` (fixed-section markdown + machine-readable `findings.json`) · `access.py` (VPN/GCS probe + run-or-defer).
- `artifacts/diagnose.py` — CLI entrypoint: `python diagnose.py --advertiser-id N [--audience-id N] [--campaign-id N…]
  [--window-days 30] [--as-of YYYY-MM-DD] [--steps 0-9] [--skip-vpn] [--out DIR]`. Resolve → render → run → interpret →
  report; steps independent given DiagContext; **fail-soft** (a throwing step → ERROR section, pipeline continues).

**Parameter contract / ID resolution (all empirical, nothing hardcoded; chain confirmed vs data_catalog.md):**
1. `segment_id` + **SEGMENT** expr from `audience.audience_segments` (advertiser, `expression_type_id=2`, `is_targeted`);
   if `audience_id` given narrow to it, else live+most-recently-updated, list alternatives. 2. user `audience.audiences`
   expr (for the UI-vs-deliverable gap). 3. DS19 ids+names from `ui.audience_keyword_state` (`is_magic=FALSE` filtered
   **upstream**). 4. DS35 include + DS1/2/4/35/43 excludes + **the gates** (DS14 cat1 availability, holdout md5 bucket,
   RTC score_type, DS21/DS34 retargeting) + studio geo radii — all parsed from the v2 segment expr (polarity from `op:not`
   nesting, gates labelled as gates, geo US-bounded). 5. campaign list from `audience_segments.campaign_id` ∩ live
   campaigns. 6. stage-1 cid from `perml.flight_cid_day_audience_sizes`. 7. HHST per campaign from
   `dso.household_score_thresholds` (0 = no gate). 8. budget+flight+vertical from `campaign_groups`/`core_flights`/
   `fpa.advertiser_verticals`. **Missing-id policy:** resolved/ambiguous/not-found per field; ambiguous → deterministic
   default + record alternatives; not-found non-spine → step SKIPPED w/ reason; no advertiser/segment → hard fail.
   Reproducibility fix: `--as-of` pins the date so steps 5/6/7/9 are repeatable (prototype was not).

**The 10 modules (parameterize → emit):**
- **0 pull expressions** → both expr JSONs (user + bidder-operative) to `outputs/`.
- **1 decompose** → interest-leaves long table + **a separate gates table** + a geo-radii table (prototype omitted gates+geo).
- **2 3P quality** → per-segment reach + %-redundant-with-MM + incremental reach; one window-param module (collapse
  snapshot/_7d dupes; **≥30d** default for burstiness); `NOT IN`→`NOT EXISTS`; 3P-inert-under-gate caveat from HHST.
- **3 keywords** → 20→200→N funnel + off-target/over-broad shares + prune list. **v1 = curated-fallback + `is_magic`
  filter + a flagged manual-review section**; LLM/embedding classifier is a fast-follow (the descope that holds 5 SP).
- **4 size funnel + exclusions** → MM universe → in-fence → not-excluded yield + geo ceiling + provider agreement (~0.36%)
  + income distribution; one shared studios CTE (kills the 3× duplication), literal MM `dt`, windowed exclusion `dt`.
- **5 scoring/HHST** → delivered `household_score` dist (cost_impression_log) + "gate-on-but-delivering-unscored" flag;
  bands keyed off `ctx.hhst_threshold`.
- **6 availability** → reach/frequency + daily cumulative-reach curve (pool-exhausted vs room-to-scale); one shared window.
- **7 targeting vs creative** → score→VR gradient (monotone?) + peer-VR percentile via `cohort.py`; `--as-of` kills the
  CURRENT_TIMESTAMP non-reproducibility.
- **8 UI size vs deliverable** → BQ `perml` size as primary number + the overstatement multiple vs the step-4 funnel;
  always generates the 5 eval_batch payloads + runner headlessly, **eval_batch POST deferred behind a VPN probe**.
- **9 deliverability (NEW)** → peer-pacing-to-96%-of-budget benchmark via `cohort.py`: cohort p10/p25/median/p75/p90 of
  flight-length-normalized pacing, n_peers, %≥96%, target's percentile, budget-vs-audience verdict. Cohort matched on
  CTV (video≥0.95), vertical, geo footprint (studio-count/in-fence bucket), budget tier (log-budget), audience shape
  (DS19:DS35 ratio + HHST on/off). **Guardrail: n_peers≥20 else tiered loosening (drop shape→geo→vertical), record the
  tier; if still <20 emit LOW-CONFIDENCE, never a false percentile.** `pacing_to_date = spend / (budget × elapsed/flight_days)`.

**Output:** ONE `outputs/<advertiser_id>_<as_of>_diagnostic_report.md` + `<adv>_findings.json` (chart/deck reuse). Fixed
section order = the 10 steps so it reads identically for any advertiser (internal report — facts-not-presentation, no
Power Line / three-act). Exec summary = a 10-row Step | Question | Verdict (🟢🟡🔴) | one-line finding table; then per-step
Finding → Verdict → Evidence (one table) → Caveat; appendix = full tables + exact SQL paths + bq job ids + VPN-deferred
artifacts. Optional `--deck` clones TI-1026 `build_deck.py` for live share-outs.

**Milestones (mapped to Todoist subtasks):**
- **Subtask 2 — define the spec:** freeze DiagContext + the 8 resolver queries + the 10-step module table + report
  template. *Early de-risk task:* confirm `campaign_groups.budget` is populated + dollar-denominated on OTF + a peer
  sample, and the spend→budget pacing join (settles step 9 before any build). Deliverable = this §5 + verified resolver stubs.
- **Subtask 3 — build:** templatize the 16 SQL files; build `diag/` + `diagnose.py`; write the new step-9 template +
  cohort logic. **Build the v2 segment-expression walker FIRST and golden-file it** against `ti_1026_segment_344085_expression.json`
  (assert gates classified as gates, excludes via `op:not`, holdout via `op:bucket`, RTC in `select[]`) — it's the long pole.
- **Subtask 4 — standardize + validate:** finalize `report.py`/`findings.json`; **OTF golden-file reconciliation** (in-fence
  ~45.7%, ~1.9M reach, score→VR ~7–20×, peer ~15th pct, provider agreement ~0.36%); then end-to-end on a **structurally-
  different 2nd advertiser** — national / non-geo-fenced, opposite HHST state, MM-heavy/few-3P, different vertical (candidate:
  iMemories AID 37423). Grep ADV2 outputs for OTF leaks (39718/319137/34668); capture any new gotcha in `data_knowledge.md`.

**Effort: 5 SP** (top of band; parameterization + resolver + one new step + glue, not green-field). resolver+DiagContext
~1.5 · templatize 16 SQL ~1 · runner/render/interpret/report/access ~1 · expr.py v2 walker+gate/geo extractors ~1 ·
step-9 template+cohort ~0.5 · OTF golden-file + ADV2 validation ~0.5. (Story points on the Jira ticket say 3 — flag: the
honest estimate is 5 with the keyword-classifier descoped; without that descope it's ~8.)

**Top risks / open questions:**
- **v2 segment-expression schema variety** — OTF is one op-tree shape; other advertisers may nest `op:not`/`op:bucket`/DS14
  differently → walker could mislabel a gate as interest. Mitigation: special-case DS14/holdout/RTC/DS21/DS34, fail-soft on
  unknown ops, golden-file OTF, validate on ADV2. (The single biggest build risk.)
- **Budget semantics** — campaign vs flight vs campaign_group vs DSO-managed caps may differ; confirm precedence w/ Chris
  Addy and surface which source was used per campaign in the report.
- **Step-9 cohort quality** — thin/mismatched peers → meaningless percentile (mitigated by the n≥20 guardrail).
- **ipdsc literal-dt must survive templating** — a window placeholder rendering as date math triggers the 164B-row scan;
  resolver emits concrete literal date strings (never SQL date math) + render asserts it; bq_run.sh perf log catches runaways.
- **Step-3 classifier descoped** to curated-fallback for v1 (non-fitness verticals get weaker step 3 until the fast-follow);
  flag the limitation in-report. **`is_magic`/keyword-state drift** — pull is dated as-of; report states the snapshot date.

## 6. Key references
- Spec: `knowledge/audience_diagnostic_playbook.md`
- Prototype + worked example: `tickets/ti_1026_orange_theory_audience_eval/` (`summary.md`, `queries/`, `artifacts/`)
- Backing knowledge: `knowledge/data_knowledge.md`, `knowledge/data_catalog.md` (segment-expression/DS14, HHST gate,
  ipdsc hygiene, ui.audience_keyword_state, UI-size source, 3P demo-data quality, MaxMind geo-fence, budget fields)
- Full priming detail: `HANDOFF_PROMPT.md`

## 6b. Build progress
- **2026-06-18 — Step 9 budget de-risk DONE** (§4): operative budget = DSO tables; `active_flight_id` stale; spend = `spend_log.win_cost_micros_usd/1e6`. Captured in `data_catalog.md`.
- **2026-06-18 — v2 segment-expression walker DONE + golden-filed (the long pole).** `artifacts/diag/expr.py` =
  unified v1 (`audience.audiences`) + v2 (`audience.audience_segments`) parser. The TI-1026 prototype only read v1
  (keys on `cats`) and was blind to the v2 op-tree + the automated gates; `expr.py` reads both, classifying every leaf
  into includes / excludes / **DS14 availability gate** / **DS21·DS34 retargeting** / **holdout** / **RTC score** +
  geo radii (polarity from `op:not` nesting). Geo radii live under a `value.geo_radii` wrapper; `op:"false"` constant
  clauses handled. `artifacts/diag/test_expr.py` (golden file) **passes**: matches the frozen OTF decomposition exactly
  (DS19=379/DS35=11, excl DS1/2/4/35/43, DS14[1], DS21/34@120d, holdout 39718:/10%, RTC 113001, 1175/21 radii), 0 warnings.
- **Next:** `resolver.py` (advertiser_id → segment_id/audience_id/campaigns/HHST/budget, all from BQ) → templatize the
  16 SQL files → `interpret.py`/`cohort.py`/`report.py` → step-9 pacing template → CLI → OTF reconciliation + ADV2.

## 6c. Kindred (35094) — findings to flag so far (2026-07-02)
Structural changes P1 (Jan–May'25) → P2 (Jan–May'26), from the perf_report modules:
- **Campaign count: 1 prospecting campaign → 6 running simultaneously** (module 01/07 census).
- **Retargeting** (group 89071) ran alongside only **part** of P1 but the **entire** P2 (module 01/08).
- **More HHST gate changes in P2 than P1**, and the gate **dropped numerous times** (module 03/03b; holiday gate-OFF).
- **More campaigns, changing audiences more frequently** in P2 (module 07/07b change-log).
- **HI% share fell ~100% → 89%.** P1 delivery is "unscored" in the logs (score column pre-2025-06), BUT the **HHST gate
  was set and mostly consistent in P1**, so delivery was gated to HI ⇒ we can infer P1 ≈ ~100% HI. P2 = 89% (module 06/06b).
- **Verdict so far:** the HHST drops + HI-share decline are **real but don't fully explain** the performance drop — no
  single glaring cause. Next: audience **sizes**, **who** we targeted, and **HI coverage / recirculation** (are we
  exhausting the HI pool and re-serving the same households?).

- **09 `prospecting_reach_recirculation`** — reach/frequency + HI recirculation test. Per month (CIL, prospecting,
  RTC-excl): reach + freq; HI new-vs-returning (first-HI-month logic); cumulative HI reach; brand-new share. 3-panel
  render. **Kindred: brand-new HI share 100%(Jun'25)→46%(May'26), returning rose to 54% ⇒ recirculating; BUT cumulative
  HI reach 5.3M and still climbing (~500k new/mo) ⇒ pool NOT exhausted; frequency low ~1.2-1.7 imps/IP.** HI metrics
  scored-era only (Jun'25+). Heavy scan (~85GB). *Awaiting review.*
- **10 `prospecting_audience_size_coverage`** — the supply-side denominator. Monthly addressable prospecting pool =
  MAX(`total_audience_size`)/day across stage-1 campaigns, avg monthly, from `perml.flight_cid_day_audience_sizes`
  (floored 2025-02; ~5× UI overstatement → deliverable ≈ pool/5). 2-panel render overlays module 09's cumulative HI
  reach for a **HI coverage** view. **Kindred: pool exploded when 3P/DS35 added (May'25, ~1.7M→81M), then contracted
  ~35% across 2026 (65M Jan→42M May'26); cumulative HI reached 5.3M ≈ 63% of the ~8.5M deliverable and rising ⇒ shrinking
  pool + high coverage = limited fresh-HI headroom** (supports the recirculation read). Caveat: total (kw+3P) size,
  not HI-only. *Awaiting review.*
- **11 `vv_window_change_log`** — flags **VV (verified-visit) lookback window changes** per advertiser (a MEASUREMENT
  confound). Source = `bronze.integrationprod.archives_advertiser_archives` (PRO=`clickpass_acquisition_ttl`,
  RT=`clickpass_click_ttl`, conv=`conversion_window`; NOT advertiser_configurations). Step-line of PRO/RT window over time,
  change markers, P1/P2 bands + red FLAG when the window differs between periods; committable `.md` change-log. **Kindred:
  45/45 → 30/14 on 2025-08-08 — squarely between P1 (PRO 45d) and P2 (PRO 30d), so P1 visits/conv were measured on a LONGER
  window (measurement confound on the P1-vs-P2 gap); conversion window constant 30d.** Built from the vv-window-cvr-
  investigation workflow (10 agents). **Mechanism proven: a conversion requires a VV within the window** (100% co-occurrence
  on ad_served_id; 7 advertisers) ⇒ shortening the VV window can lower conversions/CVR/ROAS on a ~window-length lag —
  captured in `data_knowledge.md`. Empirical magnitude on Kindred confounded (spend burst); mechanism is the durable result.
  *Awaiting review.*

- **12 `campaign_audience_deep_dive`** — decodes WHAT each prospecting campaign targets + flags audience-narrowing red
  flags (the client's "most important piece"). Parses each expression: **geo tier** (included/excluded DMAs, named markets
  via `geo.location_data` location_type_id=4), **interest logic** (MM DS19 **OR/AND** 3P DS35 — OR=additive, AND=narrowing),
  3P segments (names via `tpa.categories`). Emits a deep-dive `.md` + summary-table PNG + red flags (AND-narrowing, small
  limiting 3P, geo footprint/fragmentation, MM narrowing). queries/12 = geo DMA reference. Built from the
  campaign-audience-deep-dive workflow (4 agents). **Kindred verdict: BROADENING / geo-slicing, NOT narrowing** —
  6 campaigns geo-slice the 210 US DMAs into Top-20 (majors, ~40% of TV-HH) / Mid-38 / Low-152; MM & 3P are OR'd (additive,
  no AND-3P); the story is **geo-mix dilution** (P1 100% top-20 → P2 ~48% spend into smaller Mid/Low markets) + top-market
  flagship wind-down + 3-way HiPop fragmentation. 3P segment SIZES gated (GCS bucket access). Geo-slicing knowledge in
  `data_knowledge.md`. *Awaiting review.*
- **12b `geo_tier_deep_dive`** — the readable, quantified geo/DMA deep-dive (module 12 was too terse/small). Emits **two**
  PNGs + a committed `.md`: (1) **tier reference** — all 210 US DMAs NAMED, grouped High-20 / Mid-38 / Low-152, each annotated
  with recent in-window delivery (High & Mid fully named; Low top-12 + full list in `.md`); (2) **tier performance** — the
  flagship YoY collapse (69884 High Pop, same top-20: ROAS **9.74→2.39x**, VR 11.6→5.1‰, CVR 8.7→5.6%) + the P2 footprint
  fragmentation (1→6 campaigns, top-20→210 DMAs) with the ROAS gradient by tier. **Blended prospecting ROAS 9.74x (P1, top-20
  only) → 1.81x (P2)**; ~90% is the flagship collapse, the rest is fragmentation into Mid (1.73x)/Low (1.31x) tiers + 3 new
  same-geo interest-variants (1.18–1.35x). Confirms **"High Pop" = the top-20 markets, NOT all-of-US.** Sources: geo decode
  (`geo.location_data`), per-tier metrics (`sum_by_campaign_by_day` by `campaign_group_id`), per-DMA delivery (`CIL.metro_id`
  → `summarydata.metros`). **Two-id-system bridge (location_id vs Nielsen metro_id) + DMA-grain source constraints in
  `data_knowledge.md`.** *Awaiting review.*
- **12c `interest_logic_deep_dive`** — the per-campaign audience DNA + empirical narrowing check (answers "does any campaign
  narrow reach, and what's unique about the variants?"). Parses all 6 op-trees + overlays HLL reach. **Finding: NO MM-AND-3P
  narrowing anywhere** — all 6 share `(MM DS19[255 kw] OR 3P DS35[11–14 maternity/baby segs])` = additive; the suspected
  "required-3P narrows MM" is absent. **The only differentiator is a DS16 funnel gate on the 3 Q1 variants**
  (Harter/Motherhood/Mom-Focus): `AND ( NOT DS16[7291 Impressions, 787280 Wins] OR DS16[own campaign-group] )` = a
  **net-new-reach gate** (target iff never-impressed/won by Kindred, or already owned by this variant). **Empirical (BQ-native
  HLL on `sum_by_campaign_by_day.uniques`, Jan–May '26):** each variant reaches ~435K households = **~26% of base's 1.64M**,
  **~72% net-new vs base**, **~90% mutually disjoint** (a 3-way creative split of the residual). **Rotation:** base (ungated,
  2.39x ROAS) wound down Jan→Mar and went dark by Apr; the 3 gated variants (1.18–1.35x) ramped up to replace it on the
  smaller, lower-quality residual → the gate narrows by WHO (net-new households), not by 3P. Two PNGs (DNA table +
  funnel-gate evidence) + committed `.md`. DS16-gate decode + HLL reach/overlap technique in `data_knowledge.md`.
  *Awaiting review.*

## 7. Open items
- Build in progress (§5 milestones; §6b log). Next module = `resolver.py`, then the SQL templatization.
- Confirm budget-source precedence (DSO-managed vs flight) with Chris Addy when step 9 lands.
- Keyword classifier (step 3): full LLM/embedding generalization is IN scope per the 2026-06-18 decision (8 SP).

## 8. `perf_report/` sub-tool — parameterized client-performance report (Kindred build, started 2026-07-02)
A second, complementary deliverable under this ticket, built **interactively one module at a time** (Malachi driving,
reviewing each chart before moving on). Distinct from the steps 0–9 audience diagnostic above: this is the **performance**
view (current-health **snapshot + YoY diagnosis**), growing on top of the AUDI-1070 reusable YoY engine.

**Module pattern** (each "thing" = a self-contained trio): `queries/NN_*.sql` (parameterized `{{AID}} {{WIN_START}}
{{WIN_END}} {{P1_*}} {{P2_*}}`) → verified in BQ → `charts/NN_*.py` (reads `outputs/<adv>/NN.csv` → `NN.png` + prints a
one-line `FINDING:`). **Only source is versioned** — repo `.gitignore` drops `*.csv`/`*.png`/`*.svg`, so CSVs/PNGs
regenerate from committed `.sql`+`.py`. Home: `perf_report/{queries,queries_exec,charts,params,outputs/<adv>}`.

**✅ RUN-IT-ALL ASSEMBLER BUILT + MULTI-ADVERTISER (2026-07-04).** One command regenerates the whole 24-chart report for ANY
advertiser: `python run_report.py --params params/<adv>.env`. `run_report.py` reads the params env, and per module in
`report_spec.py` runs its clean single-query param-driven SQL (`queries_exec/<csv>.sql`, `{{AID}} {{WIN_*}} {{P1_*}} {{P2_*}}
{{DELIV_MONTH_*}}` substituted via `bq_run.sh` → `outputs/<adv>/<csv>.csv`) then its chart cmd (tokens `{OUT}{ADV}{P1S..P2E}
{P1L}{P2L}{WINS}{WINE}{DMS}{DME}{HS}{HE}{P1SM..P2EM}`), then stitches every PNG into `outputs/<adv>/report.html`. All modules
generalized advertiser-agnostic: labels from `group_name`, **prospecting derived dynamically (`objective_id=1`, no hardcoded
ids)**, adaptive layouts, and national-vs-DMA handling (12/12b/12c degrade to a national panel when the advertiser targets
loc 237=US). Params: `params/kindred_35094.env`, `params/bouqs_32147.env`. Ran end-to-end for **Kindred (35094)** and **The
Bouqs eCommerce (32147)** — all 24 modules OK for both (Kindred no regression). Two Bouqs units: **32147 eCommerce** (active,
audited) + **31906 Subscriptions** (dark in 2026, excluded). Bouqs YoY prospecting mirrors Kindred (ROAS 3.18→1.37x, rev −65%,
AOV +12% = conversion-quality not basket) but the audience profile differs (national low-HI scaling — module 00/00b/12c).

**✅ OVERVIEW HEADLINE LAYER + DIAGNOSES + 3rd ADVERTISER (2026-07-05).** Added module **`overview`** (`charts/aa_overview.py`) —
the **flag scorecard + auto-TL;DR** that runs LAST (reads the deep-dive CSVs) but **displays FIRST** in `report.html`
(build_html leads with it). It computes quick "likely-your-issue" flags with pre/post + a signal dot: prospecting
ROAS/spend/visit-rate, **VV-window change**, **avg HHST + thrash count**, **short flights before/after**, **campaign count**,
**geo restriction**, **3P restriction**, HI-share — then auto-writes a plain-language TL;DR that reads like the Slack updates
(Kindred: REAL DECLINE, 6 red — ROAS 11.4→1.87x, VV 45→14d, thrash 44→120, short flights 4→15, geo-restricted, 1→6 campaigns).
Also **collapsed Multi-Touch S2+S3 into one "Multi-Touch" stage** in module 00 (audience read still uses the stage-1 expr).
Reports now = **25 charts**, overview-first, for all advertisers. **Third advertiser wired:** `params/bouqs_subs_31906.env`
(The Bouqs **Subscriptions**, dark 2026) — full report on its Sep–Dec YoY season (fixed a module-05 CPM ÷0 on its
0-impression months). **Two committed, adversarially-verified diagnoses:** `outputs/bouqs_32147/bouqs_diagnosis.md` (eCommerce —
fixable low-HI-scaling decline) + `outputs/bouqs_subs_31906/subscriptions_diagnosis.md` (Subscriptions — persistently
unprofitable ROAS<1, consistent with a wind-down, not an audience problem). Each verification caught real issues (32147: a
fabricated ROAS, wrong denominators, an overclaimed cause; 31906: an overclaimed intent framing) — all corrected. **Knowledge
correction:** CIL is NOT 90d TTL — it retains full history to the 2025-01-01 GCP floor (verified 2026-07-05); the score COLUMN
onset (2025-06) is the real early limit, not row retention (`data_knowledge.md`).

**Modules built (approved unless noted):**
- **00 `audience_audit`** — **the systematic front-matter (runs FIRST).** Inventories ALL active campaigns, classifies stage
  by `objective_id` (1=Prospect/4=Retarget/5=MT-S2/6=MT-S3/7=Ego), decodes every audience expression (DS roles/archetype/flags).
  **Reframing findings:** (1) each *campaign group* is a full funnel → group-level metrics (incl 12b/12c) CONFLATE stages, so
  audit at campaign×objective grain; (2) **Retargeting (89071) is the revenue engine — 26.5x ROAS, 85% of revenue on 28% of
  spend, 15,758 conv**; prospecting = 62% of spend / 13% of revenue / 1.9x (top-funnel reach, not the money); (3) prospecting=CTV,
  Multi-Touch=display; MT-S2 spent ~$18K for ~0 last-touch conv. **ONE coherent front-matter PNG** (folds stage map + the
  prospecting funnel): TOP = stage map (where the money goes); BOTTOM = per-prospecting-campaign audit with targeting DNA
  **+ the funnel folded in as columns** (Reached · HI-share [green ≥80%] · Coverage) and flags (narrow/thin geo · net-new gate ·
  MM-AND-3P · low-HI-share · dark). + md. Structural knowledge in `data_knowledge.md`.
  *Focus stays on prospecting; retargeting kept as headline (per Malachi).*
- **00b `prospecting_funnel`** — the audience funnel per obj=1 campaign: **max addressable** (`flight_cid_day_audience_sizes`
  `total_audience_size`, national UI size ~5x-inflated) → **~deliverable** (÷5) → **reached** (CIL distinct ip) → **HI-reached**
  (`household_score`≥8001) + score-bucket composition. **Refines the decline story: prospecting reaches ~80–88% HI at ~4–12%
  coverage → no hard HI ceiling, NOT scraping low-score users; the variants' worse ROAS is net-new HI converting worse, not
  audience-quality erosion.** Base 261318 dark since ~Mar (F1 prospecting stopped; group's later delivery = retargeting). PNG + md.
- **01 `campaign_group_gantt`** — every campaign_group's delivery running-span as a Gantt (first→last active day, active
  days, spend). Reuses the AUDI-1070 census table choice (`summarydata.sum_by_campaign_by_day` → `campaigns` for the group
  id). Kindred = **7 active groups**; flagship 69884 "CTV Prospecting High Pop" ran the full window ($399k/484d), 4 launched
  in 2026. *Approved (subtitle removed per review).*
- **02 `prospecting_audience_expressions`** — per prospecting group (`funnel_level=1 AND objective_id=1`), the latest v2
  audience expression (`audience.audience_segments`), parsed with `diag/expr.py`, rendered as a **targeting-layer × group
  fingerprint matrix** that red-outlines every cell deviating from the flagship template. Also emits a decomposition `.md`.
  **Kindred anomalies:** (a) **DS16 funnel-tag template drift** — the 3 Q1-2026 HiPop launches (115943/45/46) each add
  DS16 (include own `CampaignGroupID` tag + exclude `Impressions`/`Wins`), which the flagship/LowPop/MidPop lack; (b) **DS35
  LiveRamp breadth** — LowPop 96108 carries 14 segments vs 11 elsewhere. Consistent across all: DS19=255 (account-level MM
  keywords, *not* per-campaign), DS2+DS47 customer suppression, DS14 availability gate, DS21/34 own-site retgt excl @180d,
  10% holdout, RTC id 122000 (in-expression ≠ firing — gated by HHST, cross-ref the gate module). DS16 semantics captured in
  `data_knowledge.md`. *Approved.*
- **03 `hhst_gate_history`** — every HHST gate-change event per prospecting campaign from
  `archives.household_score_threshold_archives` (full history to WIN_END to seed the entering value); rendered as a
  **small-multiple step-line** of threshold over time (green HI-zone ≥8000, red no-gate ≤0 bands). Kindred: **287 changes;
  flagship 261318 thrashed 180×; the gate is graduated/auto-paced (not on/off).** *Approved.*
- **03b `hhst_gate_daily_ribbon`** — companion **gate ribbon** (per-campaign lane, each delivering day colored by gate
  bucket: green ≥6600 / amber 1-6599 / red ≤0), forward-filled + clipped to each campaign's active delivery (fixes 03's
  forward-fill-past-death). Ported from AUDI-1070 `gate_ribbon_chart.py`, parameterized to read one daily gate×delivery CSV.
  Kindred: **holiday gate-OFF (Dec–Feb) on flagship + LowPop** reads as red blocks; 98 no-gate days total. *Approved
  (added P1/P2 comparison bands, dropped bottom caption per review).*
- **04 `prospecting_yoy_metrics`** — P1-vs-P2 aggregated-metrics **table** (Metric | Period 1 | Period 2 | Δ%) for all
  prospecting campaigns (funnel=1/obj=1). Query returns raw period sums; render derives every metric (spend, imps, CPM,
  visits, visit rate, conv, conv-rate, revenue, AOV, ROAS) + %Δ (computed on raw totals), colors %Δ by good/bad direction,
  emits PNG + committable `.md`. **Kindred: spend +70% / imps +63% but visits −51%, VR −70%, conv −72%, revenue −72%,
  ROAS 11.40×→1.87× (−84%); AOV flat (−1.7%) ⇒ conversion-COUNT / audience-quality problem, not basket size.** *Approved
  (fonts enlarged + rows tightened per review).*
- **05 `prospecting_monthly_metrics` (+ two renders)** — monthly time series for all prospecting campaigns (funnel=1/obj=1)
  across the continuous window, to pinpoint drastic MoM moves. **05 `_monthly_lines`** = small-multiple monthly line charts
  (one panel per metric, own scale, P1/P2 bands). **05b `_mom_heatmap`** = **MoM %-change flag map** (metric × month,
  diverging red=drop/blue=rise, cells with |Δ|≥40% outlined = "look here"). Kindred: **30 flagged moves; the cluster is
  Nov'25 visits +262% spike → Dec'25→Jan'26 collapse (visits −77%, VR −66%, ROAS −58%/−58%) → Feb'26 rebound; plus a
  Sep-Oct'25 dip/rebound** — aligns with the holiday gate-OFF (module 03/03b). *Approved; extended with 05c per review.*
- **05c `prospecting_baseline_heatmap`** — spike-robust flag map: each month vs the metric's **all-months average**
  (not the prior month). Fixes MoM's double-count — a one-month spike (Nov'25 VR +115% MoM) manufactures a phantom
  "drop" the next month (Dec −66% MoM) even though Dec just reverts. Vs-baseline: Nov shows once as a genuine spike
  (VR +94%), Dec is only −35% (not flagged). Trade-off: because some metrics trend (ROAS 16×→2×), vs-mean also
  lights up the regime (early-2025 above norm, 2026 below) — that's trend, not a one-month anomaly. Use 05b (local
  change) + 05c (level-vs-norm) together; a trailing-3mo-baseline detrended variant is available on request. *Approved.*
- **06 `prospecting_score_buckets_monthly` (+ two renders)** — `household_score` tier distribution (HI 8001-10000 /
  PP 6666-8000 / MI 3333-6665 / MaxReach 1-3332 / unscored ≤0) of prospecting delivery from `cost_impression_log`
  (RTC-excluded). **06 `_score_yoy`** = two-period grouped-bar comparison; **06b `_score_monthly`** = monthly 100%-stacked.
  **Hard constraint (empirical, this ticket):** `household_score` is 0% populated before **2025-06** (logging onset), so
  a true P1 (Jan-May'25) score distribution is impossible. Handled by splitting **`notlogged` (hs IS NULL, pre-2025-06)**
  from real **`unscored` (hs=-1)** — so 06b shows the FULL Jan'25→May'26 window with Jan-May'25 as a gray "No score data"
  band (not falsely red-unscored). 06 (two-period) uses the **standard Jan-May'25 vs Jan-May'26** (generally correct
  default) — for THIS client P1 reads 100% "No score data" (pre-2025-06 logging), so only P2 is measurable; for advertisers
  scored in both windows it's a true side-by-side. Scores read from `cost_impression_log` (won bids).
  **06c `_score_threshold_table`** = the same P1-vs-P2 comparison rendered as a **module-04-style TABLE** (Score tier |
  Period 1 | Period 2 | Δ%), per client request — rows = the 6 score tiers, %Δ colored by good/bad (more HI/PP green,
  more Unscored/MaxReach red), n/a where P1 share is 0. PNG + committable `.md`. (For Kindred P1=100% "No score data".)
- **07 `prospecting_audience_change_history` (+ timeline render)** — the period-audience-diff capability: every distinct
  audience config a prospecting campaign ran over time, from `silver.archives.audience_segment_archives` (collapsed to
  DS-set / audience_id changes; active-in-window campaigns only). **07 render** = a **DS-presence-over-time Gantt** for the
  campaign with the most changes (auto), with audience_id-swap markers + P1/P2 bands. **KEY finding — a campaign's audience
  mutates under a FIXED campaign_id:** flagship 261318 changed **8×**, audience_id swapped **22666→31114** (Sep'24); **DS19
  (keyword MM) present across both periods**, DS13 (vertical) absent in P1 (only pre-period + brief Oct-Dec'25), DS35 (3P)
  added May'25, DS21/34 retgt-excl added Nov'25. So P1's "no scores" = **CIL logging onset (Jun'25), NOT missing MM** (DS19
  was there). Also confirmed the structural shift: **P1 = 1 prospecting campaign, P2 = 6** (flagship even cut spend
  $93k→$57k). Gotcha captured in `data_knowledge.md` (audience not stable under a campaign_id). Heavy scan (~39GB, archive
  not date-prunable). *Approved.*
- **07b `prospecting_audience_change_matrix`** — the change-log **matrix** view (per client request): columns =
  campaign_group_id, rows = change dates, cells = DS delta that day (**green +added / red −removed**, incl/excl/gate
  tagged, navy audience_id swaps, gray initial "start"). DS-level only; segment/category detail deliberately omitted
  (kept for a separate file). Reuses module-07's history CSV (no re-query; incl/excl from the stable DS-role map).
  Emits PNG + committable `.md`. Reveals **platform-wide vs campaign-specific** changes via shared-date rows (Kindred:
  2025-10-29 both live campaigns +DS13, 11-12 both +DS21/34 excl, 12-11 both −DS13). *Approved.*
- **08 `prospecting_flights`** — **scheduled-flight timeline** from `core_flights` (NOT delivery runs): each flight a bar,
  laid out in **3 fixed length-tiers per campaign** (short ≤3d red on top / 4-7d amber middle / long 8+ navy bottom) so
  short flights line up on their own row; dormant (true gaps) grayed; P1/P2 bands; **short-flight count split P1 vs P2**.
  (Flights are back-to-back sequential — a `>=` touch test keeps them on one tier; earlier version staircased them.) Source confirmed by Tofer/Prod Ops: pull Start/End from the flights table; `core_flights.campaign_group_id`
  gives the full history (`dso_campaign_group_flight_budgets` is current-only; `active_flight_id` stale). **Kindred: delivery
  is CONTINUOUS but built from many short back-to-back manual flights — High Pop 69 in-window flights (17 ≤3d), LowPop 51
  (13), 193 total / 42 short.** Flights set manually per launch (Tofer), so coverage may be partial pre-2025. Knowledge
  captured in `data_knowledge.md` (core_flights = flight history). *Awaiting review.*
  **Kindred: HI ~96-100% most months
  BUT unscored spikes to 49% (Nov'25) and 90% (Dec'25)** — the score-level fingerprint of the holiday gate-OFF (cf. 03/03b);
  two-period HI 98.9%→89.6%, unscored 0.1%→8.4% (both windows gate-ON; the big swing is between them). Re-confirmed CIL
  retains the full window (not 90d-rolling) + score floor 2025-06 (already in data_catalog.md §CIL). *Awaiting review.*

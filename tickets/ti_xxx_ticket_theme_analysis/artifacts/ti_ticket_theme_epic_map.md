# TI Ticket Portfolio — Theme & Epic Analysis

**Purpose:** Represent the *types* of analysis work the team does, surface the recurring **customer questions** and **tooling gaps** behind them, and group the work into candidate **epics** for future planning.
**Scope:** 57 ticket folders, work spanning ~Mar–Jun 2026 (Q2-centered, a few 2025 precursors).
**Method:** Automated read of every ticket's `summary.md` → structured extraction → three independent synthesis lenses (customer pain points / tooling gaps / analysis taxonomy) → reconciled into candidate epics → mapped against the live TI epic backlog (86 epics).
**Team:** Audience Intelligence (AUDI)

---

## The one-line story

> **Customers keep asking one question — "Is this real?" — and almost every answer is a hand-built, one-off analysis.**

The portfolio tells two stories at once:
- **Demand side — the questions customers ask.** Four questions recur across nearly every advertiser escalation.
- **Supply side — the tooling gaps.** The same systematic analysis is rebuilt by hand for one advertiser / experiment / vendor after another. **Every customer theme has a tooling-gap twin.**

---

## The four questions customers keep asking

| # | Question (customer voice) | Example tickets |
|---|---------------------------|-----------------|
| 1 | **"Did your targeting actually *cause* these conversions — or would they have happened anyway? My own data scientists ran a lift test and found nothing."** | ElevenLabs (TI-1044), Root (TI-644), BER-2250 |
| 2 | **"Is the audience / 3P segment / vendor / keyword set I'm paying for actually worth it? Some of it delivers 8–10× worse and the UI only shows me size."** | Orange Theory (TI-1026), 5×5 (TI-1027), TI-999, TI-956 |
| 3 | **"Why did my performance suddenly drop after your rollout? My ROAS fell 8×→2× and the bidder UI shows my score collapsing 10000→0."** | AutoCamp (TI-1017), TI-896, TI-390 |
| 4 | **"Can I trust the numbers you report? Households reached, this Verified Visit, my CRM match, my New-to-Brand rate — they don't reconcile."** | MM-44, TI-650, Root (TI-644), TI-310 |

Two further questions come from **leadership / buyers** rather than a single advertiser:
- *"Can a lift test at my spend even detect an effect, before we commit budget?"* (TI-923, TI-XXX power workshop, TI-1044)
- *"Where can I see, in one durable place, what the team delivered and which signals actually predict performance?"* (TI-1003/1033, TI-789)

---

## The cross-cutting tooling gap

Nearly half the portfolio is bespoke notebook-or-SQL toil whose **own summary notes explicitly ask for "a parameterized pipeline."** The few tickets that *did* productize prove the template already exists — it just hasn't been generalized:

- TI-253 — missing-TPA-domains daily Airflow + dbt anti-join (the monitoring template)
- TI-849 → TI-921 — one-shot 3-advertiser SQL → wave-aware auto-flip-detecting lift pipeline + Mode dashboard
- TI-956 — Alex's segment-quality notebook → scheduled airflow-ti model
- TI-1037 — Orange Theory one-off → parameterized per-advertiser diagnostic

The result is analyst-bottlenecked, error-prone, ephemeral work where a self-serve tool, scheduled pipeline, or monitoring dashboard belongs.

---

## Ticket counts by bucket

Each ticket counted once, in its best-fit bucket. **43 of 57 tickets (75%) are reactively answering one of four customer questions.** The remaining 14 are the foundational enablers (signal + knowledge) that make those answers possible.

| Bucket (customer question) | Tickets | Share |
|----------------------------|:------:|:----:|
| **Q1 · Incrementality — "did you *cause* it?"** | **16** | 28% |
| **Q4 · Trust — "can I *trust* your numbers?"** | **13** | 23% |
| **Q2 · Audience/Vendor — "is it *worth* it?"** | **8** | 14% |
| **Q3 · Performance — "why did it *drop*?"** | **6** | 11% |
| Foundational — targeting signal + durable knowledge | 14 | 25% |
| **Total** | **57** | 100% |

*Q1 = themes 1 (6) + 2 (10). Q2 = theme 3 (8). Q3 = themes 4 (4) + 7 (2). Q4 = theme 6 (13). Foundational = themes 5 (7) + 8 (7).*

---

## From reactive to proactive — the story

> **Each of these four questions is a product we haven't built yet.**

Today every question is answered *after* it's asked, one advertiser at a time, by hand. The proactive move is to turn each recurring question into a **standing capability** that answers it before — or the moment — it's asked. The maturity ladder:

`L0 one-off notebook → L1 rerunnable script → L2 scheduled pipeline → L3 self-serve / in-product / alerting`

Almost everything sits at L0–L1. The proactive target is L3 — and the few L2 tickets (TI-253, TI-849→921, TI-956, TI-1037) are the proof the team can get there.

| Bucket | Tickets | Where we are | Proactive capability (L3 target) | Delivered by | Leverage |
|--------|:------:|--------------|----------------------------------|--------------|----------|
| Q1 Incrementality | 16 | L0–L1 (Fangorn lift → L2) | Always-on holdouts + **pre-flight power/MDE gate** + auto-refresh lift dashboard — we surface incrementality *before* the advertiser's data-science team finds ~0 and churns | New Epic B + standing incrementality home | **Retention** (incrementality is the #1 churn driver) + feeds Fangorn training signal |
| Q2 Audience/Vendor | 8 | L0–L1 (TI-956 → L2) | **Quality scores in the buyer UI at selection time** + vendor renewal scorecard — buyers see quality, not just size, *before* they buy the bad segment | Reuse TI-786/956 + ongoing-eval epic | **Revenue** (~$55M/yr flows to stale segments today) |
| Q3 Performance | 6 | L0 (TI-1037 → L2) | **Flip-readiness pre-check + transition-shock alerts + self-serve advertiser diagnostic** — the alert fires before the advertiser escalates | **New Epic A** | **Retention** + analyst time saved per escalation |
| Q4 Trust | 13 | L0–L1 (TI-253 → L2) | **Standing coverage / freshness / reconciliation monitors with alerting** — numbers reconcile *before* reporting, not after a dispute | **New Epic C** (generalize TI-253) | **Retention** + data-credibility floor under everything else |

**Three moves to tell the story around (rule of three):**
1. **Get ahead of the #1 churn question** — make incrementality always-on and power-gate every test before budget is committed.
2. **Put quality where the decision is made** — move segment/vendor quality out of an analyst's notebook and into the buyer UI.
3. **Watch the pipes** — standing monitors + alerts so we catch drift, drops, and reconciliation gaps before the advertiser does.

The self-serve **Advertiser Diagnostic** (New Epic A) is the connective tissue — the on-demand report that answers per-account questions across all three.

**The payoff:** shift from fire-drill to proactive, free the analyst from rebuilding the same analysis, get ahead of churn, and — for incrementality — generate the training signal that makes the targeting models smarter every cycle.

---

## Candidate epics (ordered by leverage)

Each theme is a candidate epic carrying both a **customer-question** angle and a **tooling-gap** angle. The "Epic home" column maps it against the live TI backlog.

| # | Theme / candidate epic | Customer question it answers | Tooling gap | Tickets | Epic home today |
|---|------------------------|------------------------------|-------------|:------:|-----------------|
| 1 | **Incrementality Measurement & Power Gating** | "Did you cause it?" | No CVR-capable, ghost-win-aware holdout pipeline; MDE/power computed by hand per advertiser | 6 | Released only (TI-855, TI-916, BER-2250) → **no standing home** |
| 2 | **Rollout / Feature-Lift Evaluation Pipeline** | "Did this rollout move my KPI — really?" | Every flip = hand-edited notebook re-introducing the same stats bugs | 10 | **GAP** (feature-rollout epics exist; the *evaluation harness* has none) |
| 3 | **Audience, 3P-Segment & Vendor Quality** | "Is what I'm buying worth it?" | Quality scoring lives in one analyst's notebook; UI shows only size | 8 | Released only (TI-786, TI-500, TI-803) → **no standing home** |
| 4 | **Advertiser Decision Support & Diagnostics** | "Why is my account behaving this way / what should I use?" | Each escalation = bespoke multi-query spike; no self-serve report | 4+ | **GAP — no epic** ← *where TI-1044/1045 belong* |
| 5 | **Feature-from-Analysis Onboarding / Feature Store** | "Is our targeting signal good enough?" | Per-feature hand-written PySpark, manual backfill, no schema-drift detection | 7 | **Covered** (TI-789, TI-718, TI-566) |
| 6 | **Identity, Coverage & Metric-Integrity Monitoring** | "Can I trust your numbers?" | Diagnosis is reactive/forensic, often on TTL-expired data; few standing monitors | 13 | Partial (TI-822 Dev, TI-495 Paused) → **needs standing home** |
| 7 | **RTC & Rollout Performance Monitoring** | "Is this feature delivering per dollar — would we catch a cliff?" | Hand-run SQL, brittle regex flag, no dashboard/alerting | 2 | Partial (TI-16 Closed, TI-495) → fold into #6 |
| 8 | **Durable Knowledge, Reference & Infra Hygiene** | "Where do I see what was delivered / how the system works?" | Findings in ephemeral gists, PDFs, tribal knowledge; manual secret sweeps | 7 | Partial (TI-732, TI-602, TI-702) |

---

## Recommended epic actions

**Reuse existing epics** for themes 5 (Feature Store → TI-789/718/566) and the *build* parts of scoring.

**Stand up three new epics** to give homeless-but-recurring work a permanent home:

### → New Epic A: **Advertiser Decision Support & Diagnostics**
Reactive, per-advertiser analysis that helps CS / Sales / leadership decide what to do with a specific account — validate a vendor's lift claim, recommend a measurement setup, diagnose a performance change, evaluate an audience. **This is the gap Bryce is feeling**, and it's where the ad-hoc tickets land.
- Members: **TI-1044** (ElevenLabs), **TI-1045** (client incrementality direction), TI-1026 (Orange Theory), TI-1027 (5×5 vendor eval), TI-1017 (AutoCamp), TI-644 (Root), TI-501 (Jaguar), TI-896 (revenue war room), **TI-1037** (the diagnostic tool that productizes this class).
- Why an epic: 8+ tickets in one quarter, all the same shape, none currently grouped. Recurring and fundable.

### → New Epic B: **Rollout & Incrementality Evaluation Tooling**
The reusable *measurement harness* — distinct from the feature-rollout epics (TI-457/936) that ship the features. Ingests a cohort/flip-date list, auto-selects the valid method, runs cluster-bootstrap DiD + CausalImpact with simulation inference, persists durable results, and gates tests on power/MDE before budget is committed.
- Members: TI-961, TI-748, TI-542, TI-921, TI-849, TI-923, TI-XXX, TI-504, plus naive-pre/post precursors TI-221/270/390/391.

### → New Epic C: **Data-Quality & Identity Monitoring**
Generalize the one ticket that worked (TI-253) into standing coverage / freshness / reconciliation monitors with alerting across identity, enrichment, classification, and attribution pipelines — so silent drop-outs are caught when they happen, not investigated reactively.
- Members: MM-44, TI-650, TI-684, TI-34, TI-253, TI-254, TI-310, TI-737, TI-931, TI-200, TI-033, TGT-4016, TGT-4103, + RTC monitors DM-3118/3188.

---

## Where the ad-hoc tickets go (TI-1044 / TI-1045)

Both are the **same theme**: *Advertiser Decision Support* — applying incrementality/measurement expertise to one client's decision (validate a lift report; advise how to run an incrementality campaign). They don't fit the *build* epics (BER-2250 builds the platform; these **apply** it), which is exactly why they had no home.

**Recommendation:** file both under **New Epic A — Advertiser Decision Support & Diagnostics**. If a new epic isn't wanted yet, the fallback is nesting under the incrementality epic, accepting that it blurs "build" vs. "apply."
*(Note: TI-1045 has no folder in the GitHub workspace, so it is not in the 57-ticket analysis — worth a stub if you want it captured.)*

---

## Full ticket → theme mapping (all 57)

| Theme | Tickets |
|-------|---------|
| 1 · Incrementality Measurement & Power Gating | BER-2250, TI-1044, TI-923, TI-XXX, TI-504, TI-501 |
| 2 · Rollout / Feature-Lift Evaluation Pipeline | TI-961, TI-748, TI-542, TI-921, TI-849, TI-221, TI-270, TI-390, TI-391, TI-780 |
| 3 · Audience, 3P-Segment & Vendor Quality | TI-1026, TI-999, TI-956, TI-1027, TI-803, TI-804, TI-813, TI-797 |
| 4 · Advertiser Decision Support & Diagnostics | TI-1037, TI-1017, TI-896, TI-adhoc_advertiser_scoring_filter |
| 5 · Feature-from-Analysis / Feature Store | TI-789, TI-790, TI-809, TI-810, TI-811, TI-832, TI-931 |
| 6 · Identity, Coverage & Metric-Integrity Monitoring | MM-44, TI-650, TI-644, TI-684, TI-34, TI-253, TI-254, TI-310, TI-737, TI-200, TI-033, TGT-4016, TGT-4103 |
| 7 · RTC & Rollout Performance Monitoring | DM-3118, DM-3188 |
| 8 · Durable Knowledge, Reference & Infra Hygiene | TI-1003, TI-1033, TI-502, TI-541, TI-1016, ti_kafka_secret_sweep, ti_argocd_secrets_audit |

*Cross-listed tickets are placed in their single best-fit theme to keep epics distinct. TI-504/TI-501 also touch theme 2; TI-803/804/813/797 (BUK) also touch theme 5; TI-896 also touches themes 3 and 6.*

---

## Caveats

- Themes are derived from `summary.md` content, not Jira fields; a few tickets (TI-254, TI-390, TI-391) carry partial dates and were placed by content.
- "Epic home" reflects the TI epic backlog as of 2026-06-24 (saved in `outputs/ti_epics_2026_06_24.tsv`); existing epics for incrementality and segment quality are mostly *Released/Closed* point-deliverables rather than standing homes.
- `ti_argocd_secrets_audit` is an empty placeholder folder (no summary), grouped by name/timing only.
- Raw structured records for all 57 tickets are in `outputs/theme_analysis_raw.json`.

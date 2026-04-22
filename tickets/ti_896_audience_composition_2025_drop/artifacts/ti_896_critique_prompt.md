Read the files listed under **Artifacts to read** below, then produce a four-section red-team critique of the TI-896 analysis. Do not ask clarifying questions — start reading the files, then write the critique. No flattery. No compliments. The deck is going in front of nine execs, engineers, TPMs, and data scientists who will look for reasons to dismiss it. Find those reasons first. Cite specific files and line numbers for every finding.

## Context

- **Ticket:** TI-896 — Audience composition shift analysis for a CTO-level war room investigating conversions / ROAS dropping from Nov 2025. Jira: https://mntn.atlassian.net/browse/TI-896
- **Audience for the deck:** mixed — execs (Richard Girges / Paulo Black / Kale McNaney), engineers (Ryan Kleck, Jordan Piepkow), TPMs (Bryce Wagg, Mike Dolt), data / data science (Ray, Will Cavey, Alex Knorr, Matt Brorby). Codex will also automated-review.
- **Primary deliverable:** a RevealJS deck. Local at `tickets/ti_896_audience_composition_2025_drop/artifacts/ti_896_deck_standalone.html`. Shared URL: https://gist.githack.com/mdunn-mntn/f836ba48d987ead2894535e772c8f451/raw/ti_896_deck_standalone.html

## Artifacts to read

- `tickets/ti_896_audience_composition_2025_drop/summary.md` — ticket card with findings and open follow-ups
- `tickets/ti_896_audience_composition_2025_drop/artifacts/ti_896_presentation.md` — full narrative
- `tickets/ti_896_audience_composition_2025_drop/artifacts/ti_896_verification.md` — V1–V10 independent checks
- `tickets/ti_896_audience_composition_2025_drop/artifacts/ti_896_deck.html` — the deck (CDN version) — easier to read than the standalone
- `tickets/ti_896_audience_composition_2025_drop/queries/*.sql` — all five queries
- `tickets/ti_896_audience_composition_2025_drop/outputs/*.csv` — output data
- `tickets/ti_896_audience_composition_2025_drop/meetings/*.txt` and `*.md` — meeting transcript + Slack war-room context the analysis was scoped from
- `documentation/docs/presentation_playbook.md` — MNTN presentation standards
- `documentation/docs/revealjs_guide.md` — deck layout standards
- `knowledge/data_knowledge.md` and `knowledge/mntn_business.md` — DS-id corrections + PP adoption mix entries the ticket added

## Headline claims the deck makes (stress-test these)

1. **21% of 2025-active advertisers have adopted Peak Performance** — using the detector `score_type="rtc" + data_source_id:13 + data_source_id:19` together at segment-expression level.
2. **Every other audience bucket** (MM / Keywords / 3P / CRM / retargeting) moved within ±1pp in the Sep–Dec 2025 drop window.
3. **Track A — spend-weighted** PP share is ~12–13% of cohort spend (vs 21% by advertiser presence). Adopters skew smaller-spend.
4. **Track B — default vs custom** split among PP adopters is 34% / 58% / 3% / 5% using a structural proxy: template expression has only DS13+DS19 → "default"; layers additional DS clauses → "custom".
5. **Track C — ROAS cross-check** — 1,217 advertisers active in both Aug–Sep 2025 and Dec 2025 (≥1,000 VVs each window). PP adopters saw median ROAS lift +46% vs +124% for non-adopters. AOV flat in both cohorts.

## Score on four standards

### 1. Methodological soundness

For each headline claim, audit the underlying query:
- Is the detector definition defensible? What would a skeptic argue against?
- Hidden selection effects (cohort filter, noise floor, archive coverage gaps)?
- Alternative interpretations of the data that could produce the same numbers?
- Does the `LEAD(update_time)` effective-window approach handle edge cases (gaps in CDC, overlapping versions, campaign pause/resume)?
- Does the JOIN to `sum_by_campaign_by_day` on `(campaign_id, day)` handle DST / UTC / date boundaries correctly?
- What would make the "21%" number 18% or 24% instead? Find it if you can.
- Does Track C's baseline window (Aug–Sep 2025) bias results? Is the Q4 lift signal real or confounded by holiday seasonality?
- Does the default-vs-custom classifier hide a systematic effect (e.g., templates with more DS clauses correlate with advertiser size or vertical)?

### 2. Narrative rigor against the Presentation Playbook

- Power Line: is "21% of 2025-active advertisers have adopted Peak Performance" memorable or flat? Propose three alternatives that would beat it.
- Act 1 (Disruption) — does it open with a stat / question / contrast / story / bold claim, or throat-clear?
- Act 2 (Revelation) — is each chart slide a billboard? Identify the top 3 slides that fail the five-second test.
- Act 3 (Resolution) — does the close tie back to the Power Line or peter out?
- Rule of Three — are there exactly 3 takeaways? 3 next steps?
- One number per slide — which slides violate this?
- Story / Hall framework — is there a character-driven moment, or is it all aggregate stats?
- Cialdini checklist: Social Proof, Authority, Scarcity, Commitment, Reciprocity, Unity — which are present / missing?
- Greene's Laws: is the deck bold or hedging? Where should it say less?

### 3. Exec / engineer / DS red-team

Play three devil's advocates in parallel and produce the three sharpest objections from each, plus the best-case rebuttal the current deck already contains OR admits is open:

- **Skeptical exec** — "is this telling us to do something, or just describing?" What's the call to action? Is it clear?
- **Skeptical engineer** — "your regex will miss campaigns with a space before the colon." Audit every regex / schema assumption against the actual sampled expressions (V1 in verification.md).
- **Skeptical data scientist** — "you ran a two-window comparison with no significance testing; your +46 vs +124 could be noise." Audit Track C for statistical rigor.

### 4. Missing views / blind spots

Rank the top 5 charts or cuts the analysis did NOT produce that it probably should have. Candidates (not exhaustive):
- Spend-weighted Track C (PP delivery-weighted ROAS delta)
- Custom-PP vs default-PP performance split
- Vertical cuts (ecom vs lead-gen)
- Cohort-median time series (week-by-week ROAS median for adopters vs non-adopters)
- Coverage diagnostic (% of cohort advertisers with ≥1 archive row)

For each: "add this because…" with the strongest argument.

## Output format

Produce all five sections in one response:

- **Section 1 — Methodology audit.** Issues in rank-order of severity. Cite specific files/lines. For each: severity (critical / moderate / minor), what a skeptic would say, what the fix or rebuttal is.
- **Section 2 — Narrative critique.** Score 1–5 on Power Line, Opening, Act 2 slides, Act 3 close, Rule of Three, Cialdini, Billboard test. Give concrete rewrites for anything scoring <4.
- **Section 3 — Red-team objections.** Three from each devil's advocate. Nine total.
- **Section 4 — Blind spots.** Ranked top 5 missing views with "add this because…"
- **Section 5 — Top 10 fixes before this ships.** Prioritised by impact. Each item: what, why, estimated time to fix.

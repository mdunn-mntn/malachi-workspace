# TI-896 — meticulous critique prompt for a separate chat

Paste the prompt below into a fresh Claude chat (with MNTN workspace mounted) to get a methodology + narrative + methodology-soundness critique of the TI-896 analysis. The chat should be run with the MNTN workspace present so the agent can read the queries, CSVs, and verification note directly.

---

## Prompt to paste

I need an uncompromising, multi-angle critique of an urgent war-room analysis before it reaches execs + engineers + Codex review. Budget rigor over speed. Cite specific files and line numbers for every finding.

### Context

- **Ticket:** [TI-896](https://mntn.atlassian.net/browse/TI-896) — Audience composition shift analysis for a CTO-level war room investigating conversions / ROAS dropping from Nov 2025.
- **Audience:** mixed — execs (Richard Girges / Paulo Black / Kale McNaney), engineers (Ryan Kleck, Jordan Piepkow), TPMs (Bryce Wagg, Mike Dolt), data/DS (Ray, Will Cavey, Alex Knorr, Matt Brorby). Codex will also automated-review.
- **Primary deliverable:** a RevealJS deck. Standalone at `tickets/ti_896_audience_composition_2025_drop/artifacts/ti_896_deck_standalone.html`. Shared URL: https://gist.githack.com/mdunn-mntn/f836ba48d987ead2894535e772c8f451/raw/ti_896_deck_standalone.html
- **Supporting artifacts** (read these, cite line numbers in your critique):
  - `tickets/ti_896_audience_composition_2025_drop/summary.md` — ticket card
  - `tickets/ti_896_audience_composition_2025_drop/artifacts/ti_896_presentation.md` — full narrative
  - `tickets/ti_896_audience_composition_2025_drop/artifacts/ti_896_verification.md` — V1–V10 checks
  - `tickets/ti_896_audience_composition_2025_drop/queries/*.sql` — all five queries
  - `tickets/ti_896_audience_composition_2025_drop/outputs/*.csv` — output data
  - `tickets/ti_896_audience_composition_2025_drop/meetings/*.txt` and `*.md` — meeting transcript + Slack war-room context the analysis was scoped from
  - `documentation/docs/presentation_playbook.md` — MNTN presentation standards
  - `documentation/docs/revealjs_guide.md` — deck layout standards
  - `knowledge/data_knowledge.md` and `knowledge/mntn_business.md` — corrections + PP adoption mix entries the ticket added

### Headline claims the deck makes (these are what to stress-test)

1. **21% of 2025-active advertisers have adopted Peak Performance** (using the detector `score_type="rtc" + data_source_id:13 + data_source_id:19` together, at segment-expression level).
2. **Every other audience bucket** (MM / Keywords / 3P / CRM / retargeting) moved within ±1pp in the Sep–Dec 2025 drop window.
3. **Track A — spend-weighted** PP share is ~12–13% of cohort spend (vs 21% by advertiser presence). Adopters skew smaller-spend.
4. **Track B — default vs custom** split among PP adopters is 34% / 58% / 3% / 5% (using a *structural* proxy: template expression has only DS13+DS19 → "default"; layers additional DS clauses → "custom").
5. **Track C — ROAS cross-check** — 1,217 advertisers active in both Aug–Sep 2025 and Dec 2025 (≥1,000 VVs each window). PP adopters saw median ROAS lift +46% vs +124% for non-adopters. AOV flat in both cohorts.

### What I want you to do

Score the analysis against FOUR independent standards. Be harsh. Be specific. Cite files / lines.

#### 1. Methodological soundness

For each headline claim, audit the underlying query:
- Is the detector definition defensible? What would a skeptic argue against?
- Are there any hidden selection effects (cohort filter, noise floor, archive coverage gaps)?
- What alternative interpretations of the data could produce the same numbers?
- Does the `LEAD(update_time)` effective-window approach handle edge cases (gaps in CDC, overlapping versions, campaign pause/resume)?
- Does the JOIN to `sum_by_campaign_by_day` on `(campaign_id, day)` handle DST / UTC / date boundaries correctly?
- What would make the "21%" number 18% or 24% instead? Find it if you can.
- Does Track C's baseline window (Aug–Sep 2025) bias results? Is the Q4 lift signal real or confounded by holiday seasonality?
- Does the default-vs-custom classifier hide a systematic effect (e.g., do templates with more DS clauses correlate with advertiser size or vertical)?

#### 2. Narrative rigor against the Presentation Playbook

Score every slide against the playbook (use the framework in `claude-prompts/presentation_critique.md` as a starting point but go deeper):
- Power Line: is "21% of 2025-active advertisers have adopted Peak Performance" memorable or flat? Propose three alternatives that would beat it.
- Act 1 (Disruption) — does it open with a stat / question / contrast, or throat-clear?
- Act 2 (Revelation) — is each chart slide a billboard? Identify the top 3 slides that fail the five-second test.
- Act 3 (Resolution) — does the close tie back to the Power Line or peter out?
- Rule of Three — are there exactly 3 takeaways? 3 next steps?
- One number per slide — which slides violate this?
- Story / Hall framework — is there a character-driven moment, or is it all aggregate stats?
- Cialdini: which of Social Proof, Authority, Scarcity, Commitment, Reciprocity, Unity are present / missing?
- Greene's Laws: is the deck bold or hedging? Where should it say less?

#### 3. Exec / engineer red-team

Play three devil's advocates in parallel:
- **Skeptical exec** ("is this telling us to do something, or just describing?") — what's the call to action; is it clear?
- **Skeptical engineer** ("your regex will miss campaigns with a space before the colon") — audit every regex / schema assumption against the actual sampled expressions (V1 in verification.md).
- **Skeptical DS** ("you ran a two-window comparison with no significance testing; your +46 vs +124 could be noise") — audit Track C for statistical rigor.

For each, produce the three sharpest objections and the best-case rebuttal the current deck already contains OR admits is open.

#### 4. Missing views / blind spots

What chart / cut did the analysis NOT produce that it should have?
- Spend-weighted Track C (PP delivery-weighted ROAS delta)?
- Custom-PP vs default-PP performance split (this is flagged as follow-up but could be partially answered now)?
- Vertical cuts (ecom vs lead-gen)?
- Cohort-median time-series (week-by-week ROAS median for adopters vs non-adopters, instead of the two-window delta)?
- Coverage diagnostic (what % of cohort advertisers have ≥1 archive row; is the archive reconstruction complete)?

Rank the top 5 missing views by marginal value relative to exec scrutiny.

### Output format

- **Section 1 — Methodology audit.** Issues in rank-order of severity. Cite specific files/lines. For each issue: severity (critical / moderate / minor), what a skeptic would say, what the fix or rebuttal is.
- **Section 2 — Narrative critique.** Score 1–5 on Power Line, Opening, Act 2 slides, Act 3 close, Rule of Three, Cialdini, Billboard test. Give concrete rewrites for anything <4.
- **Section 3 — Red-team objections.** Three from each devil's advocate (exec, engineer, DS). Nine total.
- **Section 4 — Blind spots.** Ranked top 5 missing views with "add this because…"
- **Section 5 — Top 10 fixes before this ships** to the war room, prioritised by impact. Each item: what, why, estimated time to fix.

No flattery. No compliments. The deck is going in front of nine people who will look for reasons to dismiss it. Find those reasons first.

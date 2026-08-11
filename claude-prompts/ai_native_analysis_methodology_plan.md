# Plan — The Evidence-Ladder Analysis Methodology (Analytical Reasoning Layer)

> Component plan for the super-structure. Scope: **how the reasoning inside an analysis is built —
> from the simplest verified fact up to a defended conclusion — so a finding is credible by
> construction, never by assertion.** Sibling plans cover the ticket-execution layer
> (`ai_native_work_structure_plan.md`), the knowledge base, tooling, and agents. This one defines the
> *epistemic unit* — the claim — and the ladder every claim must climb before it may be stated at a
> given strength.

---

## 0. Thesis

**No claim may be stated more strongly than the evidence rung it has actually reached. Analysis is a
ladder climbed from the ground up — the simplest true fact first, one layer of complexity at a time,
each rung gated by a machine-checkable artifact — and a surprising number is not shippable until it
has been triangulated, its uncertainty quantified, and every objection to it pre-empted in writing.**

The sibling plan makes *where work lives* deterministic. This plan makes *how strongly you may speak*
deterministic. The two failure modes we are killing are the only two ways an analyst loses credibility:
**(1) a broad assumption made before a cheap empirical check** (wrong join key, wrong source-of-truth
table, mis-read column, an unverified invariant), and **(2) a shocking number reported before it was
stressed** (a data artifact, a self-referencing covariate, an un-triangulated point estimate). Every
cited failure below is one of these two. The ladder makes both structurally impossible to reach the
top of.

An AI is trustworthy on analysis when the *strength of its language is a function of a checklist it
cannot skip*, not a function of how confident the prose sounds. This plan makes credibility a typed,
enforced property of a claim — exactly as the sibling plan made status a typed, enforced property of a
ticket.

---

## 1. Requirements → structure traceability

The user's brief maps 1:1 onto the design. This table is the contract:

| Requirement (user's words) | Where it lives | Why it's AI-native |
|---|---|---|
| "tackle problems from the most simple start and gradually build it up" | **The Evidence Ladder** (§3) — 9 gated rungs, ground → conclusion | Rung *N+1* is unenterable until rung *N*'s artifact exists. Building up is a hard gate, not a habit. |
| "never making broad assumptions" | **Rung 1 (Ground)** + the **assumptions register** (§3, §7) | Every assumption is listed and killed by a query before any analytical query runs. Enforced by linter. |
| "strong, factual evidence… build up credibility" | **The Claim Contract + confidence tiers** (§4) | A claim carries its tier; the tier is a function of rungs completed. You cannot *label* a T2 claim you only earned T1 evidence for. |
| "People get skeptic… especially when the numbers are shocking" | **The Shocking-Number Protocol** (§5) | Magnitude/surprise auto-triggers mandatory extra rungs (triangulation + adversarial Q&A + bias direction). |
| "generate supporting graphics, charts, and other things" | **Evidence-provenance chain** (§6) + dataviz/Tufte standards | Every number → query → data file → chart. A claim without a provenance chain is unshippable. |
| "share charts and keep track of them" | **The Chart & Evidence Registry** (§6) | One machine-readable manifest: every chart's ID, the claim it supports, its source query/data, and where it was shared. |
| "a structure that AI will like to follow" | **Determinism + machine gates** (§8) | Fixed rung vocabulary, a claims ledger, a linter that blocks over-claiming. Same idiom as the knowledge base's front-matter→linter→index. |

---

## 2. Design principles (the "why")

1. **Foundation before complexity — always.** The first artifact of any analysis is the simplest true
   descriptive fact that bears on the question (one number, its denominator, its grain). Models,
   segments, and causal claims are layers *on top of* verified primitives, never substitutes for them.
2. **No assumption survives that a query could kill.** Every join key, source-of-truth table, column
   meaning, filter, grain, and system invariant is *unverified until proven with data*. This is the
   existing Empirical Analysis Protocol, promoted to rung 1 and made a gate.
3. **Strength of language = rung reached.** A claim is tagged with a confidence tier; the tier is
   mechanically derived from which rungs were completed. Over-claiming is a lint error, not a style note.
4. **Try to break your own number before anyone else can.** Every fact passes a default sanity-check
   battery (falsify-the-invariant, negative control, structural-break gate, low-volume filter) *before*
   it is reported. The analyst is the first adversary.
5. **A surprising number must be triangulated.** Two independent methods (or two independent tables)
   must agree before a counterintuitive or large-magnitude finding leaves the building. Methods
   convergence *is* the credibility argument.
6. **Uncertainty is never optional.** Every point estimate ships with SE/CI/p — on *both* methods.
   A bare point estimate is unshippable. Direction-of-bias ("this is a lower bound because…") is stated
   for every headline number.
7. **Provenance is a chain, not a citation.** Number → query file → data file → chart → claim. The
   chain is machine-followable; a broken link fails the build. Nothing is "roughly from that table."
8. **Every artifact has one deterministic home and one stable ID.** Queries, data, charts, and claims
   are addressable objects in a registry — so they can be *tracked*, *re-run*, and *shared* without a
   human remembering where they went.
9. **Structure by machine, not memory.** A scaffolder stamps the ladder; a linter enforces the gates
   and the tier rule; an index surfaces the claims ledger. Symmetric with the knowledge base and the
   ticket layer — learn one idiom, operate all three.
10. **The record is honest about what it does not know.** Dead ends, downgraded claims, and "directional
    only" verdicts are first-class entries. A softened conclusion recorded truthfully is a credibility
    *asset* (TI-896 below); a shocking number that was actually an artifact is the liability.

---

## 3. The Evidence Ladder (the core of the design)

Every analysis climbs the same nine rungs in order. **Each rung emits a named artifact; you cannot
enter rung *N+1* until rung *N*'s artifact exists.** That gate is the entire mechanism for "build up
from the simple, never assume." Trivial reads exit early (a rung's artifact can be a single line), but
the *order* is invariant and no rung is skipped — it is only sometimes cheap.

| # | Rung | What it produces | The gate | Grounded in |
|---|------|------------------|----------|-------------|
| 0 | **Frame** | The question in one sentence; the decision it informs; the **null hypothesis**; and *what the number would be under the null for this specific table*. | No data pulled until the null and the "what would disprove me" are written. | TI-835: state the expected null result *per table* before querying. |
| 1 | **Ground** | An **assumptions register**: every join key, source-of-truth table, column meaning, filter, grain, time-coverage, and system invariant — each marked `assumed → resolved` with the query that resolved it. | No analytical query runs while any assumption is still `assumed`. | Empirical Analysis Protocol; the "assumed X, reality Y" catalog (§9). |
| 2 | **Measure the simplest true thing** | *One* descriptive number that bears on the question, with its numerator, denominator, and grain. No model, no segmentation yet. | The single fact must be reproducible from a query file in the registry. | TI-804 established rank→visit-rate monotonicity *before* any per-advertiser model. |
| 3 | **Stress the fact** | The default sanity-check battery run against the rung-2 number (§7): falsify-the-invariant, negative control, structural-break gate, low-volume filter, direction-of-bias. | Any check that fails sends you back down the ladder, not forward. | TI-837 (0 of 5.4M served IPs in holdout); guid_log vs clickpass_log negative control. |
| 4 | **Add one layer** | Exactly one increment of complexity — a segmentation, a control, a covariate, a model term — with the reason it was added and a re-run of rung 3 on the new result. | One layer per step; each layer re-passes the battery. Never two at once. | AUDI-1070 within-HI VR: established composition facts before attributing ROAS. |
| 5 | **Triangulate** | A *second, independent* estimate of the headline number — a different method (DiD ↔ CausalImpact) or a different table (clickpass ↔ conversion ↔ all_facts) — and the agreement/disagreement between them. | A surprising number may not pass this rung on one method alone (§5). Disagreement is investigated, not averaged away. | TI-961 Tier 2 +27% DiD≈CI; AUDI-1070 signature reproduced across three tables. |
| 6 | **Quantify uncertainty** | SE / 95% CI / p-value on the headline estimate, **on both methods**, plus the explicit **bias direction** ("lower bound because IP rotation attenuates toward zero"). | No point estimate ships without its interval. Ratio metrics use simulation/bootstrap, never a normal-approx SE that can explode. | TI-961 hand-rolled CrI blew up to +681%; fixed with N=2000 simulation. |
| 7 | **Pre-empt objections** | A written **adversarial Q&A**: every way the number could be wrong (leakage, survivorship, unequal buckets, selection, confound, artifact), each answered with evidence. | Required for any T2+ claim; mandatory for shocking numbers. Fresh-context adversarial review encouraged. | TI-804's objection Q&A (temporal leakage, IP rotation, bucket sizes) defended 184×. |
| 8 | **Visualize + register** | Tufte-clean chart(s) — one number per point, direct-labeled, finding-as-title — each entered in the **Chart & Evidence Registry** (§6) with its claim ID and provenance chain. | A chart with no registry entry / broken provenance chain fails the build. | MNTN chart standards (memory `reference_deck_standards`, via `/present`); the dataviz skill; the "keep track of / share" requirement. |
| 9 | **Grade the claim** | A **claim ledger entry**: the statement, the number, the confidence **tier** (§4), the rungs completed, the provenance chain, the bias direction. | The linter blocks a tier higher than the rungs earn (§4, §8). This is the final gate before the number may enter a deliverable. | TI-896 correctly downgraded to "directional" when ROAS CIs overlapped. |

**Why a ladder and not a checklist:** a checklist can be completed in any order and its items rationalized
post-hoc. A ladder with per-rung gates forces the *sequence* — you physically cannot have a segmented
model (rung 4) before a verified single fact (rung 2), and you cannot have a verified fact before the
join key is proven (rung 1). The dependency order is the guarantee.

---

## 4. The Claim Contract — confidence tiers (how strongly you may speak)

Every headline number in every deliverable is a **claim** with a tier. The tier is not chosen — it is
*derived* from the highest ladder rung the claim reached, and the linter enforces the mapping.

| Tier | Name | Earned when | Language permitted | Canonical |
|---|---|---|---|---|
| **T0** | Anecdote | A single number, not yet ground-verified. | **Not shippable.** Internal scratch only. | any un-checked pull |
| **T1** | Directional | Rungs 0–3 complete: grounded, one verified fact, sanity-checked, one method/source. | "suggests," "directionally," "consistent with." No point estimate stated as fact. | TI-896 (~12% PP adoption, ROAS CIs overlap → "directional only") |
| **T2** | Defensible | Rungs 0–7: T1 **plus** triangulated (§5), full uncertainty (rung 6), objections pre-empted (rung 7), bias direction stated. | A point estimate with its CI, stated plainly. | TI-961 Tier 2 **+27%** (DiD≈CI, both with intervals) |
| **T3** | Proven / Causal | T2 **plus** a falsified invariant *or* a verified true control/holdout/randomization (rung 3 done at the invariant level). | Causal language ("caused," "lift of X%"). | TI-837 (holdout enforcement falsified to 0/5.4M); TI-504 RCT z-test |

**The one rule that protects credibility:** *a claim is reported at the tier its evidence earned, never
higher — and a **shocking number may not be reported below T2** (§5).* This is the mechanical answer to
"people get skeptic when the numbers are shocking": the more surprising the number, the more rungs it is
*required* to have climbed before it may be spoken, and the tier tag on the claim makes the earned
strength auditable at a glance.

Downgrading is honorable and logged. TI-896's move from "captured ~half the lift" to "directional only"
when the ROAS confidence intervals overlapped is the model behavior — the honesty *is* the credibility.

---

## 5. The Shocking-Number Protocol (surprise triggers extra rungs)

A number is **shocking** — and auto-escalates its requirements — when any of these fire:

- **Magnitude:** an effect > ~2× or < ~0.5× the prior/baseline, or a multiple (184×, 60×, "10x lift").
- **Counterintuitive sign or direction** versus what the team or a stakeholder expects.
- **Decision leverage:** the number would change spend, headcount, a roadmap, or an external commitment.
- **It contradicts a held belief** ("Matched is degrading," "MM adoption is ~50%," "MaxReach turned off").

When shocking, the claim **cannot** ship below **T2** and additionally requires:

1. **Triangulation is mandatory (rung 5), not optional.** A second independent method or table must
   reproduce the number. *Why:* every large false number we have shipped or nearly shipped came from a
   single un-triangulated estimate — TI-504's −70% (self-referencing covariate), TI-961's +681% upper
   bound (hand-rolled CrI), the +33.8% spurious CUPED lift (formula on a non-random cohort).
2. **Direction-of-bias in writing (rung 6).** State whether the estimate is a floor or a ceiling and
   why. TI-804's "184× is a *lower bound* because IP rotation attenuates the signal toward zero" is what
   made a wild-sounding number *more* credible, not less.
3. **Artifact interrogation (rung 3, hardened).** Explicitly rule out the known artifact generators
   before believing the number: under-spend inflating a rate (shrinking denominator), low-volume weeks
   (<1,000 impressions → absurd rates), a structural population break (pre/post IVR ratio outside
   0.5–2×), a mislabeled source (spend-fingerprint the client chart before trusting its label —
   AUDI-1070 caught a "Avon" chart that was HexClad by matching Nov spend to the dollar), and a
   placeholder/joke source (the TI-542 "mullet PDF" from which an agent fabricated numbers).
4. **Fresh-context adversarial review (rung 7).** A reviewer who starts from "assume this is wrong" and
   sees only the source. TI-961's multi-agent verification caught both a formula bug *and* a deeper
   design mismatch before the results meeting; TI-1037's passes caught a fabricated ROAS and wrong
   denominators. Reuse the existing `reviewer-adversarial` agent, twice, independently.

The protocol is not friction for its own sake — it is precisely the set of checks that, in retrospect,
would have caught every shocking number we had to walk back.

---

## 6. Evidence provenance + the Chart & Evidence Registry (generate · track · share)

The user asked for three things about visuals: **generate** them, **keep track** of them, **share** them.
All three are one mechanism: a machine-readable registry in which every chart is an addressable object
with a full provenance chain back to a claim.

**The provenance chain (unbreakable):**

```
claim_id ──cites──▶ number ──from──▶ queries/ti_xxx_*.sql ──produces──▶ data/final/*.csv
                                                                    │
                                                        scripts/generate_charts.py
                                                                    ▼
                                              deliverables/charts/ti_xxx_<claim>.png
```

A claim whose chain has a missing link (a chart hand-built from a number with no query, a CSV no script
reads) **fails the build**. This is what makes "strong factual evidence" a structural property: you
literally cannot ship a number you cannot trace to a query.

**The registry — `deliverables/charts/_registry.yaml`** (one entry per chart; generated-index-friendly):

```yaml
- id: ti_804_vr_by_rank              # stable, greppable, immutable
  claim_id: ti_804_c1                # the claim this chart is evidence for (§9 ledger)
  title: "Top-ranked keywords drive 184x more visits"   # finding, not metric
  rung: 8
  source_query: queries/ti_804_vr_by_rank.sql
  source_data: data/final/keyword_vr_by_rank.csv
  chart_script: scripts/generate_charts.py
  file: deliverables/charts/ti_804_vr_by_rank.png
  interpretation: "Visit rate declines monotonically across rank buckets; effect is a lower bound."
  standards_ok: true                 # Tufte lint passed (data-ink, direct labels, linear scale)
  created: 2026-04-08
  shared_to:                         # the "keep track of where it went" surface
    - {channel: jira, ref: TI-804,  date: 2026-04-08}
    - {channel: deck, ref: deliverables/ti_804_deck.html, date: 2026-04-09}
    - {channel: slack, ref: "#audience-intel", date: 2026-04-09}
```

**Generate** — charts are produced by a committed `scripts/generate_charts.py` reading `data/final/*.csv`
(never hardcoded numbers), following the MNTN chart standards in memory `reference_deck_standards` (reached via `/present`) (Tufte: maximize data-ink,
color encodes meaning, lie-factor = 1 with linear scales, finding-as-title, direct-labeled points,
one-line interpretation). The `dataviz` skill is invoked before the first line of chart code.

**Track** — the registry *is* the tracker. Every chart has a stable ID, a provenance chain, and a
`shared_to` log. `build_index` rolls all `_registry.yaml` files into a workspace-level
`deliverables/_CHART_INDEX.md` — a single place to answer "what charts exist, what claim does each
support, where has each been shared, is any now stale because its source query changed."

**Share** — sharing appends a `shared_to` entry (Jira/Slack/deck/dashboard + ref + date). Because the
chain records the source query and data, a shared chart is *re-runnable* and *auditable*: when someone
asks "where did this 184× come from," the answer is one `source_query` link away, which is exactly the
credibility posture the brief demands. Static PNG for async (Jira/Slack/email); RevealJS deck for live.

**Staleness:** if a chart's `source_query` or `source_data` changes after the chart was generated, the
linter marks the registry entry `stale` and the chart cannot be re-shared until regenerated — a chart in
a deck never silently drifts from the query that made it.

---

## 7. The default sanity-check battery (rung 3, reusable)

These are the checks that recur across every credible MNTN analysis, promoted from tribal knowledge to a
*default* the AI runs on every headline fact without being asked. Each is a named, greppable move.

| Check | What it does | When it fires | Canonical |
|---|---|---|---|
| **Falsify-the-invariant** | Write the SQL that *would disprove* a randomization/holdout/eligibility invariant; expect exact 0s/1s. | Any result depending on a system invariant. | TI-837: 0 of 5,432,546 served IPs in holdout. |
| **Negative control by mechanism** | Run the same measurement where the mechanism is blocked; it must show ~null. | Incrementality / lift. | guid_log ~0% (all traffic) vs clickpass_log 2–8× (ad-gated). |
| **Structural-break gate** | Reject synthetic control if pre/post level ratio is <0.5× or >2× (different population, not treatment). | Any CausalImpact / pre-post. | TI-504: experiment campaigns at 8–40% of parent IVR = population gap, not effect. |
| **Self-referencing-covariate check** | Confirm every covariate is external to the response; a covariate that *is* the answer gives fake-perfect fit. | Any model with covariates. | TI-504: `control_ivr = response` → −70% collapse when removed. |
| **Low-volume filter** | Drop weeks/cells below a volume floor (e.g. <1,000 impressions) that produce absurd rates. | Any rate metric. | IVR=366× artifact from post-pause VV lag. |
| **Denominator-shift check** | Distinguish a real rate rise from a shrinking denominator (under-spend inflates IVR). | Any rate over a spend/exposure base. | AUDI-1070 spend-incident false signal. |
| **Leave-one-out leverage** | Recompute the pooled rate dropping each unit; flag any single unit moving it > ~0.5pp. | Any non-random control pool. | TI-961: Angi alone moved Tier-5 pool CVR ~0.57pp. |
| **Source-label fingerprint** | Never trust the label on a handed-over chart/table; verify by spend/volume signature. | Any third-party or client artifact. | AUDI-1070: "Avon" chart was HexClad (matched Nov spend $903,423). |
| **Placeholder/joke-source guard** | Confirm a source contains real data before extracting from it. | Any PDF/notebook of unknown provenance. | TI-542 mullet PDF → fabricated numbers. |
| **Uncertainty floor** | No point estimate without SE/CI/p; ratio metrics via bootstrap/simulation, never explodable normal-approx SE. | Every point estimate. | TI-961 +681% CrI blow-up. |

The battery is versioned in `knowledge/` and grows: every new "assumed X, reality Y" lesson (§9) that
produces a repeatable check is added here, so the foundation strengthens over time — the same
learning-loop the knowledge base already runs.

---

## 8. Tooling & enforcement (structure by machine, not memory)

Modeled on the proven `bq_introspect / lint_coverage / build_index` trio and symmetric with the sibling
plan's `new_ticket / lint_tickets / build_index`:

- **`claims.yaml`** (per ticket) — the machine-readable **claim ledger**, the analytical analogue of the
  ticket's front-matter. One entry per headline claim: `id`, `statement`, `number`, `tier`,
  `rungs_completed: [0..9]`, `provenance: {query, data, chart}`, `bias_direction`,
  `objections: [...]`, `triangulation: {method_a, method_b, agree}`. This is the typed API a deliverable
  is assembled from.
- **`lint_claims.py`** — the enforcer. **Fails** on: a claim whose `tier` exceeds its
  `rungs_completed` (over-claiming); a shocking-number claim (magnitude/decision flags set) below T2; a
  point estimate with no CI; a chart with no `claim_id` or a broken provenance link; a `data/final/`
  file no query produces; an assumption still `assumed` at ship time; a stale chart re-shared without
  regeneration. Runs in pre-commit and at session Stop — the same slot `lint_coverage.py` occupies for
  docs.
- **`new_analysis.sh <ticket>`** — stamps the ladder skeleton into a ticket: an empty `claims.yaml`, the
  rung-0 Frame prompt, the assumptions register, `queries/ data/final/ scripts/ deliverables/charts/`,
  and a `_registry.yaml` stub. The only sanctioned way an analysis is born — so every analysis is born
  laddered.
- **`build_index.sh`** (extended) — rolls all `claims.yaml` into a workspace `CLAIMS_INDEX.md`
  (every claim, its tier, its ticket, its provenance) and all `_registry.yaml` into `_CHART_INDEX.md`.
  A master orchestrator reads *these*, not the tree.
- **Hooks** (extend `.claude/settings.json`): SessionStart prints any T0/T1 claims sitting in shipped
  deliverables (credibility debt) and any stale charts; Stop reminds to grade un-tiered claims and
  regenerate stale charts. The existing `reviewer-adversarial` agent is the rung-7 executor; `/capture`
  graduates a new sanity check (§7) or "assumed X, reality Y" lesson into `knowledge/`.

Because the tier is *derived and linted*, an AI cannot talk itself into a strong claim with confident
prose — the machine holds the line the same way it holds ticket status and doc coverage.

---

## 9. Anti-patterns this eliminates (with cited evidence)

Every row is a real failure or near-miss from the corpus, and the exact rung/tier gate that now catches it.

| Failure (real) | Root cause | Caught now by |
|---|---|---|
| "92% S3 VV resolution ceiling" | queried the wrong table (impression, not `clickpass_log.ip`) | Rung 1 Ground — source-of-truth resolved before measuring |
| `objective_id` used for funnel stage; 48,934 mis-tagged S3 campaigns | assumed column semantics | Rung 1 Ground — column meaning verified (`funnel_level` authoritative) |
| Media-plan delivery "wildly incorrect" | scoped to all campaigns, not `media_plan.campaign_group_id` | Rung 1 Ground — grain/scope verified ("painful TI-748 lesson") |
| Avon "pure stable DS13" finding | assumed `archives.version` monotonic; it wraps | Rung 3 Stress + rung 7 adversarial review |
| TI-504 −70% effect | self-referencing covariate (covariate = response) | Rung 3 self-referencing-covariate check |
| TI-504 −37%…−79% "Fangorn effect" | population break (experiment IVR 8–40% of parent) | Rung 3 structural-break gate |
| TI-961 +681% upper bound | hand-rolled normal-approx CrI on a ratio near zero | Rung 6 uncertainty floor (simulation, not normal-approx) |
| +33.8% CUPED "lift" | CUPED bolted onto a non-random cohort | Rung 4 one-layer + rung 5 triangulation flagged the anomaly |
| TI-542 Max Reach numbers | fabricated from a joke placeholder PDF | Rung 3 placeholder/joke-source guard |
| "Avon is degrading" chart | mislabeled — was actually HexClad | Rung 3 source-label fingerprint (spend match) |
| "MM adoption ~50%" | assumed MM excludes DS19 | Rung 1 Ground — definitional check (DS19 *is* MNTN Matched) |
| Bare "DiD lift = +27%" next to "p=0.255" | point estimate without matched uncertainty | Rung 6 — both methods carry CI/p |
| Shocking number shipped on one method | no triangulation | §5 — shocking ⇒ mandatory rung 5, ≥ T2 |
| Chart in a deck no one can trace | no provenance chain | Rung 8 registry — broken chain fails build |

---

## 10. How the AI operates it — the fixed ritual

Every analysis, same climb — mirrors the knowledge-side `START_HERE → index → doc` and the work-side
`START_HERE → _BY_STATUS → README`:

1. **Frame (rung 0).** Write the question, the decision, the null, and "what would disprove me." No data
   yet.
2. **Ground (rung 1).** Fill the assumptions register; kill every assumption with a query. Gate: none
   left `assumed`.
3. **Climb (rungs 2–4).** Simplest true fact → stress it with the battery → add one layer → re-stress.
   Never two layers at once; every layer re-passes rung 3.
4. **Defend (rungs 5–7).** Triangulate; attach uncertainty and bias direction; write the adversarial
   Q&A. If the number is shocking, §5's extra rungs are mandatory.
5. **Show (rung 8).** Generate Tufte-clean charts from `data/final/` via a committed script; register
   each with its `claim_id` and provenance; log where it's shared.
6. **Grade (rung 9).** Write the `claims.yaml` entry; the linter derives and checks the tier. Only then
   may the number enter a deliverable — at exactly the strength it earned.

Because the rungs are gated and the tier is linted, the AI produces the *same* credibility posture on
analysis #1 and analysis #500 — a surprising number is never louder than its evidence, and a broad
assumption never reaches a conclusion.

---

## 11. Why it composes into the super-structure

- **Uniform typed contract.** `claims.yaml` is to this layer what front-matter is to the ticket layer:
  a master orchestrator can assemble a report, an audit, or a self-review from *fields* — every claim's
  tier, provenance, and triangulation state — without reading prose or re-judging credibility.
- **Explicit seams to the siblings.** This layer *consumes* the ticket layer's homes
  (`queries/ data/final/ scripts/ deliverables/`) and *emits* into the knowledge layer (new sanity
  checks and "assumed X, reality Y" lessons graduate via `/capture`). `claims.yaml` ↔ ticket
  `README.result`, and `_CHART_INDEX.md` ↔ `deliverables:` are the declared interfaces the master plan
  wires.
- **Symmetric idiom.** Front-matter + linter + generated index appears three times now — knowledge
  docs, tickets, and claims/charts. One mental model operates all three; the plans merge without seam
  translation.
- **Credibility becomes queryable.** The super-structure can, at any moment, answer "what does the team
  currently claim, at what confidence, backed by which evidence, shared where" — from the indexes alone.
  That is the deliverable the brief is really asking for: not prettier charts, but *a system in which no
  claim can outrun its evidence.*

**One-line summary for the synthesizer:** *the analytical layer is a gated evidence ladder — ground the
primitives, measure the simplest fact, stress it, add complexity one layer at a time, triangulate,
quantify uncertainty, pre-empt objections, register the charts, and grade the claim — with a
machine-enforced rule that no claim (and especially no shocking number) may be stated above the rung its
evidence actually reached.*

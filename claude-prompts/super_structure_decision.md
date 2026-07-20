# Super-Structure Decision — What to Adopt, What Not to Build

**Date:** 2026-07-20
**Author:** AI panel synthesis (6 grounded critiques) — draft for Malachi's review
**Input:** 6 grounded adversarial verdicts on the AI-native super-structure plans
**Decision:** Extend ONE proven pattern to 4 real pains with ONE shared toolchain. Do NOT build 6 parallel machines.

---

## 1. Thesis — one pattern, N surfaces

The workspace's self-documenting knowledge layer works because it is **thin and proportionate**:

> **typed front-matter → one linter (`lint_coverage.py`) → one generated index (`build_index.sh`) → a coverage state.**

That is the whole mechanism. It is already shipped and it already scales — the 263-table BigQuery crawl finished this session through it.

The six plans each re-invent that mechanism for their own surface: a ticket schema, a `claims.yaml`, a per-slide YAML, a provenance ledger, a chart registry, a Learning Ledger event-bus. Adopted literally, that is **6 typed schemas, 6 linters, 3 ledgers, a 9-rung gate, a 5-loop engine, and a cron** — governance heavier than the work, and precisely the failure mode the thin pattern was built to avoid.

**The perfect solution applies the ONE pattern to a few felt pains and reuses the ONE toolchain.** Every keeper below is either (a) a handful of new front-matter fields on a file that already exists, (b) a prose edit to a doc that already exists, or (c) 4 new fields logged into a store that already exists. Two doc surfaces get no new tooling at all.

---

## 2. Verdict table — all 6 plans

| Plan | Verdict | Kernel to adopt (thin) | What to cut (heavy) | Pri |
|---|---|---|---|---|
| **bq_velocity_provenance** | **REDUCE → kernel** | Phase 0: harden `bq_run.sh` (source `config.env`, auto dry-run→`est_gb`, enforce the already-declared `BQ_GB_ABORT/WARN/SKIP` knobs, add `--phase`); log 4 fields into the **existing** perf log (`phase`, `est_gb`, `sql_sha256`, `git_commit`); one read-only `bq_verify` card. | 2nd ledger (`provenance.jsonl`), `[^prov:claim_id]` tags + `check_provenance.py` + Stop-nag, `result_sha256` MATCH/DRIFT, `provenance-auditor` agent, `NN_slug.sql` rename. Accrete cookbook/playbook — don't seed empty. | **1** |
| **work_structure** | **REDUCE → kernel** | ~5 front-matter fields (`title/status/date/summary/result`) on the **existing** `summary.md` as tickets are touched; `data/final/` as a naming **convention** in `folder_definitions.md`; optional thin `lint_tickets.py` mirroring `lint_coverage.py`. | 17-field schema (only 4 read), `_BY_THEME` lineage graph, hard `_v2/_final` filename ban, `summary.md→README.md` rename, `_lib/` graduation, done-gate commit block. | **2** |
| **analysis_methodology** | **REDUCE → kernel** | 3 prose edits to `experimentation.md`: write-the-null-first (per-table), the **Shocking-Number Gate** (triangulate + SE/CI/p on both + written bias direction + 1 adversarial pass), one consolidated sanity checklist. Optional T1/T2/T3 language label. | 9-rung hard-gated ladder, `claims.yaml` + `lint_claims.py`, `_registry.yaml` chart registry, `new_analysis.sh` scaffolder, 2 new hooks, provenance-chain build gate. | **3** |
| **self_improvement** | **REDUCE → kernel** | (a) Health scorecard: extend `build_index.sh`/`perf_digest.py` + SessionStart print with orphans (git last-touched), `_UNDOCUMENTED.queue` size, dup estimate, sessions-without-`/capture`. (b) Request-mining: one `UserPromptSubmit` hook → flat `{verb,nouns,hash}` file → on-demand digest that PROPOSES a skill. | Learning Ledger event-bus + `ledger_append.py` + PostToolUse:Read hook, autonomous Gardener (net-negative-bytes auto-delete), archived→deleted ladder, system-retro auto-editing CLAUDE.md, a headless timer-driven "runs-itself" loop (the Pi cron is only the bounded Slack-extraction job → review queue, not a self-improvement engine). | **4** |
| **deck_structure** | **REDUCE → kernel** | The §6 **de-slop grep list** + "specificity = proof of work" framing, appended as one section to `presentation_playbook.md`; grep runs warn-only inside the **existing** `presentation_critique.md` gate. | `new_deck.sh`, `build_deck.sh`, blocking `lint_deck.py`, commit/Stop hook, per-slide YAML schema, 6-type taxonomy, 11-rule contract, mandated Assertion-Evidence style. | **5** |
| **execution_engine** | **ADOPT as-is (already built) — bless** | The shipped factory: 7-agent roster + `agent_pass_runbook.md` + `INGEST_GUIDE.md` + the linters + manifest-as-queue. Append the 6 live-crawl lessons to the runbook as durable doctrine. | `RESTRUCTURE_GUIDE.md`, corpus-wide manifest, whole-workspace bulk port, Phase-4 deps on non-existent linters, the §13.2 `Workflow` JS skeleton. | **6** |

**Nothing is a full ADOPT of a plan as written; nothing is a full REJECT of a whole plan.** Five reduce to a kernel; one is already built. The heavy 80–90% of each plan is cut or deferred.

---

## 3. The collapsed system — these keepers are ONE thing, not six

The verdicts expose three ideas re-minted six times. Collapse each to a single mechanism:

### One provenance store (not 3 ledgers + a registry)
Provenance ledger (bq) + chart/evidence registry (analysis, deck) + Learning Ledger (self-improvement) are **the same idea**. There is exactly one store:

> **`knowledge/bq_perf_log.jsonl`** — already keyed on `job_id + ticket + label + sql_tables` — extended with `sql_sha256 + git_commit + est_gb + phase`. **`git history` is the event/tombstone log.** `bq_verify` reads them.

That single store absorbs the bq plan's provenance, the analysis plan's chart-provenance intent, and the self-improvement plan's "query events." No `provenance.jsonl`, no `_registry.yaml`, no `_ledger/` event-bus.

### One typed-front-matter model (not 3 schemas)
Ticket cards + `claims.yaml` + per-slide YAML are **the same idea** as the coverage front-matter already shipped:

> **YAML front-matter, read by `build_index.sh`.** `coverage_state` on knowledge docs (shipped); `title/status/date/summary/result` on ticket `summary.md` (the only new schema). Nothing new for claims or slides.

### One toolchain (not 6 `new_X.sh`/`lint_X.py`)
- **Generator:** `build_index.sh` — already emits `tickets/INDEX.md` (lines 223-288) and `_COVERAGE.md`. The "empty INDEX columns" pain is fixed by supplying front-matter it already reads.
- **Linter family:** `lint_coverage.py` (shipped) + at most one thin `lint_tickets.py` that mirrors it. No `lint_claims.py`, `lint_deck.py`, `check_provenance.py`, `ledger_append.py`.
- **Instrumented wrapper + provenance store:** `bq_run.sh` + the perf log.
- **Knowledge graduation + weeding:** the shipped **`/capture`** skill (human-gated, in-context). This is the ONLY path facts move to `knowledge/` or get pruned — it replaces the work-plan's `[learned]→knowledge` pathway, the Gardener, and the ladder's up/down states.
- **The factory:** the 7-agent roster + `agent_pass_runbook.md`. This is what *builds* the keepers below.

**Two of the four kernels (analysis, deck) add ZERO tooling — they are prose edits to `experimentation.md` and `presentation_playbook.md`.** The whole net-new surface is: 4 perf-log fields + `bq_verify`, ~5 ticket front-matter fields + optional `lint_tickets.py`, a scorecard extension + one prompt-log hook. That is a thin extension, not a spine.

---

## 4. Anti-goals — do NOT build these (named)

| Do NOT build | Why |
|---|---|
| **Autonomous Gardener** (headless loop, KPI = net-negative bytes, deleting from `data_knowledge.md`/`data_catalog.md`) | Highest risk in the whole set. Auto-deleting the CLAUDE.md-named source of truth; a bytes-KPI is gamed by deleting un-measured nuance; deleted subtleties are invisible until re-derived. All weeding stays inside human-gated `/capture`. |
| **Any second ledger** — `provenance.jsonl`, Learning Ledger event-bus, `_registry.yaml` chart registry | Redundant with the perf log (`job_id` already logged) + git history + memory files. One provenance store, extended. |
| **9-rung hard-gated ladder** (N+1 unenterable until N's artifact exists) | A 10-minute COUNT query does not need 9 gated artifacts. Contradicts the standing `feedback_facts_not_presentation` rule to keep internal data work low-ceremony; gets routed around. |
| **Parallel deck tooling** — `new_deck.sh`/`build_deck.sh`/`lint_deck.py` + per-slide YAML + mandated Assertion-Evidence style | ~90% duplicates shipped `share_deck.sh` + playbook + `presentation_critique.md`. The mandated hero-number/narrative-title style is the exact style the user **rejected on 2026-07-16** for technical decks. Depends on a `data/final/` convention with 0 dirs today (vs 58 `outputs/`) — would fail 100% of current decks. |
| **`result_sha256` MATCH/DRIFT re-run-and-assert** | BQ tables mutate (TTL, late data, SQLMesh rebuilds); false-DRIFT constantly, eroding trust in the verifier. Pin-and-show, don't re-run-and-assert. |
| **`[^prov:claim_id]` tag-every-number + scanner + Stop-nag** | Highest-friction abandoned-discipline pattern; humans route around per-number tagging. |
| **`summary.md → README.md` rename** | Large blast radius (65 folders + CLAUDE.md + memory + templates) for zero gain — the generator already reads `summary.md`. |
| **Hard `_v2/_final` filename ban** | Fights real iteration (`ti_1053`) and collides with committed names. Make `data/final/` a convention, not an enforced ban. |
| **`claims.yaml` + `lint_claims.py`** | Over-claiming is caught by the Shocking-Number checklist + adversarial reviewer, not YAML field comparison. Duplicates the Standard Analysis Protocol. |
| **New agents** — `provenance-auditor`, Gardener | The shipped roster covers it. Don't grow the roster. |
| **`RESTRUCTURE_GUIDE.md` + corpus-wide manifest + whole-workspace bulk port** | 64/65 tickets already conform — near-zero structural debt, high blast radius, no ratified target. Depends on two linters that don't exist. |
| **Autonomous "runs-itself" cadence driver** | The Pi cron runs exactly one bounded job — the Slack knowledge-extraction bot writing to a human-reviewed queue (per CLAUDE.md session-startup pull); it is NOT a self-improvement engine and must not become one. A headless loop that edits `knowledge/`/CLAUDE.md on a timer has no failure signal and no human gate. Every cadence in this system reduces to "run at a stopping point" = human-invoked `/capture`. |
| **Front-loaded empty template docs** — seeded `optimization_playbook.md`/`query_cookbook.md`, `new_analysis.sh` stubs | The empty-template-rot anti-pattern. Accrete via the existing perf-analyst/curator agents; fix the dangling `START_HERE.md:41` link with a 3-line stub. |
| **Auto-editing CLAUDE.md (system-retro)** | Correctly human-gated in the plan, but lowest-signal/highest-stakes loop — near-zero failure signal today. Defer. |

**Coordination guard:** the work-structure and analysis plans would both extend `build_index.sh` and add Stop hooks. **`build_index.sh` extensions and any new linter live in ONE place.** One `lint_tickets.py`, owned by the work-structure kernel; the analysis kernel adds no tooling.

---

## 5. Build order (dependency-sorted, smallest-highest-pain-first)

The **execution engine already exists to build these** — the 7-agent factory can run the front-matter backfill and any lint-to-green loop with no new machinery. Build the four kernels in this order; bless the engine last.

1. **bq_velocity Phase 0** (pri 1) — ✅ **SHIPPED 2026-07-20** (`72281533` + `06fe7132`). Added provenance-only fields (`phase/sql_sha256/sql_preview/git_commit`) to the existing perf log + read-only `bq_verify.py`. **Deliberately shipped the provenance half ONLY, NOT the dry-run abort gate** — the standing `feedback_bq_workflow` rule forbids cost warnings / preempting long queries. Also fixed a latent bug: an apostrophe in a jq comment had broken `bq_run.sh` on `main` since `bafa6bf`.
2. **work_structure kernel** (pri 2) — ✅ **SHIPPED 2026-07-20** (`5ef73a9f` pilot + `9d0c8201` backfill). ~5 front-matter fields on every `summary.md`; `tickets/INDEX.md` now populated with a blessed `result` column; `data/final/` documented in `folder_definitions.md`; thin `lint_tickets.py` (86 cards, 0 violations, `status:done ⇒ real result`). Backfilled 83 cards via the 8-shard factory.
3. **analysis_methodology kernel** (pri 3) — prose edits to `experimentation.md`: write-null-first, Shocking-Number Gate, consolidated sanity checklist. *Reuses the shipped reviewer-adversarial agent. No new tooling.*
4. **self_improvement kernels** (pri 4) — extend `build_index.sh`/`perf_digest.py` for the health scorecard; one `UserPromptSubmit` prompt-log hook + on-demand request-mining digest. *Read-only / append-only, no deletion authority. Builds on #1–2's outputs.*
5. **deck_structure kernel** (pri 5) — append the de-slop checklist to `presentation_playbook.md`; wire the warn-only grep into `presentation_critique.md`. *Doc edit; smallest artifact surface.*
6. **execution_engine** (pri 6, already-built) — bless `agent_pass_runbook.md` + roster as THE factory; append the 6 live-crawl lessons (session-resume, `sql_tables` `__`-truncation fix, `coverage_state` lint gate, READ-ONLY prose-oracle, false-stale `schema_synced` fallback, cross-project seeding). *This is the tool that executes #2's backfill — not a peer to build.*

**Net new surface across all six:** 4 perf-log fields + one wrapper hardening + `bq_verify`; ~5 ticket front-matter fields + one thin linter; a scorecard extension + one prompt-log hook; two prose sections. One toolchain, one provenance store, one front-matter model. That is the thin extension the thesis demands.

---
doc_type: decision
title: BQ Velocity & Provenance Plan — the self-improving query system
summary: "plan to make every BQ query faster than the last (sample-first ladder + perf flywheel + reusable cookbook) and every reported number provably reproducible (provenance ledger + one-command verify)"
status: proposed
date: 2026-07-19
keywords: [bigquery, sampling, sample-first, query optimization, perf loop, cookbook, provenance, reproducibility, verify, trust, velocity]
---

# BQ Velocity & Provenance Plan

> **For the master-planner:** this is the BigQuery pillar. It plugs into the existing MNTN AI
> Workflow Kit (`workflows/ARCHITECTURE.md`) — it does **not** replace it. Everything here either
> (a) implements a component the Architecture already specified but never built, or (b) adds the
> provenance layer the Architecture stopped short of. Where a sibling plan owns tickets, retrieval,
> or agents generically, this plan owns **query speed** and **number-trust**. Consume it as: *four
> pillars, one flywheel, five landing phases.*

---

## 1. Objective

Two outcomes, both compounding:

1. **Velocity — every query is faster than the last.** We query BQ all day over enormous tables;
   full runs stall the flow. Make *sample-first* a hard, enforced ladder, and make every query
   teach the next one (real cost logged → mined → surfaced pre-query → reused). Speed compounds.
2. **Trust — every number is provably reproducible.** "How did you get this?" must have a
   sub-minute, airtight answer: *here is the exact SQL (hashed), here is the BQ job that ran it,
   re-run it yourself — and it still matches.* Reproducibility is mechanized, not promised.

### The five stated pains → where they're solved
| Pain (user's words) | Pillar |
|---|---|
| "Query times are incredibly long… better to get small samples first to estimate before executing full" | **A. Sampling-First Ladder** |
| "Each query we need to figure out how to make it faster next time… BQ has query info about what was slow/fast" | **B. Self-Improving Perf Loop** |
| "Queries we utilize often… keep track of how/why so we don't reinvent the wheel" | **C. Query Cookbook** |
| "Learn new info about tables/gotchas… document so we never make the same mistake twice" | **B + C** (feeds the per-table catalog, playbook, cookbook) |
| "People don't trust our numbers… always easy to answer 'how did you get these', provably verify" | **D. Provenance & Verification Ledger** |

---

## 2. Current-state baseline — what already exists (do NOT rebuild)

The kit is mature. This plan is additive; the master-planner should treat these as **done**:

- **`bq_run.sh`** — assigns a unique `job_id`, runs the query, pulls `bq show -j` stats, appends a
  rich compact record to `knowledge/bq_perf_log.jsonl`: bytes processed/billed, slot/wall time,
  cache hit, partitions processed, per-stage query plan (shuffle + spill), optimizations, index
  usage, and BOTH `referenced_tables` (physical) and `sql_tables` (clean SQLMesh names). ~2082
  queries logged. Forces `--location=us-central1` for the slot reservation.
- **Per-table catalog** — `knowledge/bq/<dataset>/<table>.md`, front-matter-driven, coverage state
  `skeleton→enriched→verified` (today: 185 / 1 / 7), append-only `## Observed cost/facts` +
  `## Changelog`. Two dates: `schema_synced` (machine) vs `last_verified` (human).
- **Generated indexes** — `_CATALOG_INDEX.md`, `_COVERAGE.md`, `_TOPICS.md`, `_ROUTING.md`,
  `_UNDOCUMENTED.queue`, all byte-stable from front-matter via `build_index.sh`.
- **`perf_digest.py`** — deterministic p50/p90 by table, cost offenders, cache-miss repeats, and a
  `phase-accuracy` mode (already expects a `phase` field the wrapper doesn't yet emit).
- **4 hooks** — block raw `bq query` (force the wrapper), flag net-new tables to the queue, print
  routing/coverage at SessionStart, remind `/capture` at Stop.
- **7 agents** — cataloger, reviewer-adversarial ×2, fixer, synthesizer, **perf-analyst**, curator.
- **`config.env` knobs already declared** — `BQ_GB_ABORT=500`, `BQ_GB_WARN=50`,
  `BQ_SAMPLE_SKIP_GB=5`. **They exist but nothing reads them.**

## 3. Gap analysis — what's designed but not built

| # | Gap | Evidence | Pillar |
|---|-----|----------|--------|
| G1 | `bq_run.sh` has **no dry-run gate, no `--phase`, no `est_gb` logging** — sample-first is unenforced | `grep phase\|dry_run\|est_gb bq_run.sh` → nothing | A |
| G2 | **`optimization_playbook.md` missing** — `START_HERE.md` routes to it; it doesn't exist | `ls knowledge/bq/optimization_playbook.md` → not found | B |
| G3 | **`query_cookbook.md` missing** — same; reusable-pattern store has no home | `ls … query_cookbook.md` → not found | C |
| G4 | **`sample_first_query.md` runbook missing** — Architecture §6/§8 names it | not found | A |
| G5 | **No provenance ledger** — nothing binds a reported number → its SQL + job_id + result hash | no `provenance.jsonl`, no `bq_verify` | D |
| G6 | **Ticket query discipline unbuilt** — `new_ticket.sh` / `check_ticket_layout.sh` absent; queries not flat `NN_slug.sql` with greppable provenance headers | scripts not found; tickets use nested subdirs | D |

**Reading:** the perf *plumbing* is excellent; the **sample-first enforcement**, the **learning
destinations**, and the **entire trust layer** are the work.

---

## 4. Target architecture — one flywheel

```
                    ┌──────────────────────────────────────────────────────────┐
                    │  PRE-QUERY  (retrieve before you type)                    │
                    │  bq_precheck.py ds.table → typical cost · required        │
                    │  partition filter · known gotchas · best prior query      │
                    │  query_cookbook.md → a verified, parameterized pattern     │
                    └───────────────────────────┬──────────────────────────────┘
                                                 ▼
   ┌───────────────────────── SAMPLE-FIRST LADDER (Pillar A) ──────────────────────────┐
   │ 0 dry-run (free) → est_gb   → gate: >ABORT refuse · >WARN require sample           │
   │ 1 shape probe (LIMIT 10, schema, null/cardinality)                                 │
   │ 2 --phase sample  (1 partition / TABLESAMPLE) → validate logic + extrapolate       │
   │ 3 bounded backtest (representative window) → confirm the sample scales             │
   │ 4 --phase full    (only after a matching sample for this --label)                  │
   └───────────────────────────────────────┬───────────────────────────────────────────┘
                                            ▼
   bq_run.sh logs: cost + plan + est_gb + phase + sql_sha256  →  knowledge/bq_perf_log.jsonl
                                            │                                  │
              (--ticket set) ──────────────┤                                  ▼
                                            ▼                    perf_digest.py (p50/p90, offenders,
   tickets/<t>/provenance.jsonl            │                    est↔actual, sample→full accuracy)
   claim → sql_file+sha → job_id →         │                                  │
   rows+result_sha  (Pillar D)             │                    perf-analyst agent (on cadence)
                                            │                                  ▼
                                            │        per-table ## Observed cost · optimization_playbook
                                            │        ## Observed rules · query_cookbook before→after
                                            ▼                                  │
   bq_verify.py --ticket → re-run → MATCH/DRIFT      ◀───── next query is cheaper & reuses a
   check_provenance.py → every reported number tagged        verified pattern; every number re-runnable
```

**One sentence:** *retrieve prior cost → sample before full → log real cost → mine it → curate it
back into the pre-query surface, while binding every number to a re-runnable job.* Velocity and
trust are the two halves of the same loop.

---

## 5. Pillar A — Sampling-First Escalation Ladder

**Principle:** never let a query touch the full table until a sample has (a) proven the logic and
(b) predicted the answer. Enforce it in the wrapper; guide it in a runbook.

### 5.1 The ladder (rungs, each cheaper-to-costlier)
| Rung | Action | Cost | Gate to advance |
|---|---|---|---|
| **0 Dry-run** | `bq query --dry_run` → `est_gb` (free, no bytes billed) | $0 | `est_gb ≤ ABORT` (else `--force`); if `> WARN` you MUST sample |
| **1 Shape probe** | `bq show --schema`, `INFORMATION_SCHEMA`, `SELECT … LIMIT 10` on ONE partition; null-rate + `APPROX_COUNT_DISTINCT` on keys | trivial | logic columns understood |
| **2 Sample** | real analytical query on **1 partition** or `TABLESAMPLE SYSTEM (p PERCENT)`; `--phase sample` | small | result shape valid + extrapolated estimate computed |
| **3 Backtest** | widen to a representative window (e.g. 7d) to confirm the sample scales & isn't seasonal | moderate | estimate stable across the window |
| **4 Full** | `--phase full` — only after a matching `--phase sample` for the same `--label` | full | — |

Rung 0 is **automatic** (wrapper). Rungs 1–3 are **judgement** (the runbook). Skip straight to full
only when `est_gb < BQ_SAMPLE_SKIP_GB` (5 GB) — sampling a tiny scan is pointless.

### 5.2 `bq_run.sh` changes (implements G1)
The knobs already exist in `config.env`; wire them in:
1. `source config.env` at top.
2. **Auto dry-run** before every real run → capture `est_gb`.
3. **Gate:** `est_gb > BQ_GB_ABORT` → refuse unless `--force`. `est_gb > BQ_GB_WARN` **and**
   `--phase != sample` **and** no prior `--phase sample` for this `--label` → hard nudge (config
   flag `BQ_SAMPLE_GATE=warn|block`, default `warn` to start, flip to `block` once adopted).
4. New flags: `--phase sample|full` (default `adhoc`), `--sample-spec "1 partition 2026-07-18"`.
5. **Log new fields:** `phase`, `est_gb`, `est_actual_ratio` (= est_gb / gb_processed), `sql_sha256`,
   `sample_spec`, `git_commit`. `est_actual_ratio` is the estimator's report card;
   `phase` unlocks the `phase-accuracy` mode `perf_digest.py` already has.
6. On a `--phase full` with no prior matching sample → print a one-line warning (keeps the
   sample→full accuracy signal honest).

### 5.3 Extrapolation — the MNTN-specific math (goes in the runbook)
Sampling bias is metric-dependent. The runbook `knowledge/bq/runbooks/sample_first_query.md` codifies:
- **Rates / ratios** (visit rate, CTR, win rate): sample estimate ≈ full estimate **directly** if
  the slice is representative. CI = binomial SE on the sample n. Cheapest to sample — one partition
  usually nails it.
- **Sums / counts**: full ≈ sample × (1 / fraction). Single-partition over D days ⇒ `× D` **only if
  stationary** — use a 7-day sample × (total_days / 7) to survive weekly seasonality.
- **Distinct counts**: do **NOT** scale linearly — cardinality is sublinear under sampling and
  underestimates badly. Run `APPROX_COUNT_DISTINCT` on the full column (cheap) or HLL-merge instead.
- **Joins under `TABLESAMPLE`** (critical trap): `TABLESAMPLE` is applied **per table before the
  join**. Sampling both sides multiplies loss (fraction²) and silently drops matches. **Rule: sample
  the fact table only; keep dimensions full.**
- **Partitioned time-series**: prefer **one partition** over `TABLESAMPLE SYSTEM` — it prunes bytes
  *and* is a natural representative slice; `TABLESAMPLE SYSTEM` still reads whole blocks.
- Fold in the existing MNTN gotchas at sample time: epoch units per table (ns/ms/µs), `ip` vs
  `ip_raw`, 10 % holdout `MD5('{AID}:{IP}') mod 1000`, `funnel_level` not `objective_id`,
  low-impression weeks → extreme rates (filter < 1000 imps).

### 5.4 Optional helper — `bq_estimate.py`
Given a sample result + fraction + metric-type, print the scaled point estimate + rough CI and a
**"is a full run even worth it?"** verdict. Turns rung 2 into a decision, not a guess.

---

## 6. Pillar B — The Self-Improving Perf Loop

The plumbing (log + digest + perf-analyst) exists; close three loops.

### 6.1 Pre-query retrieval — `bq_precheck.py ds.table` (the pre-flight card)
Before writing a query, one command prints, for a table:
- typical cost (`perf_digest.py --table` p50/p90 GB),
- the **required/known-good partition filter** (from the table doc front-matter + observed cost),
- known gotchas (`## Observed facts`),
- the **best prior query** that hit it (lowest-GB job for that `sql_tables` set from the perf log).

This is the "have we done this before, and what did it cost?" answer, delivered *before* you burn a
byte. It reads only indexes + the log — no BQ call.

### 6.2 Estimate → actual → learning
With `est_gb` and `phase` now logged (Pillar A), `perf_digest.py` gains real signal:
- **estimator accuracy** — `est_actual_ratio` distribution (is the dry-run trustworthy?),
- **sample→full accuracy** — did rung-2 predict rung-4? (the `phase-accuracy` mode, now fed),
- **partition-filter wins** — same `sql_tables`, filtered vs not, GB delta,
- **cache-miss repeats** — identical SQL re-run cold = wasted spend, flag it.

### 6.3 Curation cadence — the perf-analyst writes learnings home
On cadence (not per query — per `feedback_bq_workflow`, no polling), the **perf-analyst** agent reads
the digest and appends:
- → per-table **`## Observed cost`** (dated one-liner: "spend_log 90d full = 340 GB; 1-partition
  sample = 3.8 GB; filter on `dt` mandatory"),
- → **`optimization_playbook.md` `## Observed rules`** (a technique + its measured before→after),
- → **`query_cookbook.md`** (promote a repeatedly-run query to a verified pattern — see Pillar C).

### 6.4 `optimization_playbook.md` (creates G2)
Techniques ranked by **observed** GB saved, each `###` greppable, each with a real before→after from
the perf log. Seed set (all MNTN-real): partition-prune every log/event table · pre-aggregate before
join · column-prune wide bidder tables (kill `SELECT *`) · `APPROX_COUNT_DISTINCT` for cardinality ·
temp-table a CTE reused ≥2× · filter on the cluster key in source order · single-partition sample
over `TABLESAMPLE` · `agg__daily_sum_by_campaign` (cheapest daily, from Sep-2025) vs
`sum_by_campaign_by_day` (to 2024 for long pre-periods). Front-matter'd → appears in `_ROUTING.md`.

---

## 7. Pillar C — Query Cookbook / Library

**Creates G3.** `knowledge/bq/query_cookbook.md` — a library of **parameterized, verified, reusable**
query patterns so common analyses are never rebuilt from scratch.

### 7.1 Entry schema (one `###` per pattern, greppable)
```
### advertiser_daily_spend        ← slug
what/why   : advertiser spend by day from the cheapest source
params     : {{advertiser_id}} {{start}} {{end}}
sql        : <canonical, cost-optimized — right partition filter + dedup + correct join keys baked in>
cost       : ~2 GB / 6s (p50 over N runs)
provenance : first written TI-XXX ; canonical job_id perf_2026...
gotchas    : epoch=ns ; funnel_level not objective_id ; exclude WGU(31357) for representativeness
verified   : 2026-07-19 (ran, shape confirmed)
```

### 7.2 Governance (mirrors the coverage-state discipline)
- **Promotion:** a ticket query becomes a cookbook entry when it's run **≥2×** or is broadly useful.
  The **curator** (`/capture`) proposes promotions from the perf log's repeat-detection + ticket
  `queries/`.
- **Verification:** an entry is untrusted until someone (or the reviewer-adversarial agent) confirms
  it runs and returns the right shape — then `verified: <date>`. Same skeleton→verified rigor.
- **Dedup:** the curator greps existing slugs before adding — duplicate patterns are bugs.

### 7.3 Seed set (patterns MNTN demonstrably reuses)
advertiser daily spend (cheapest source) · visit rate with 10 % holdout ITT · dedup `bid_logs`
(ROW_NUMBER + `bid_ip` COALESCE fallback) · cohort + flip-date detection (wave-aware) · CTV vs
display split (`channel_id` 8/1) · MM component taxonomy counts (DS19/DS13/DS46) · prospecting
filter (`obj IN (1,5,6)` + `funnel_level`) · CausalImpact tier×day covariate frame.

### 7.4 Optional tool — `bq_snippet.py <slug> --advertiser_id=… --start=…`
Fills params and prints ready-to-run SQL (or pipes into `bq_run.sh`). Turns the library from a doc
into a tool — copy-paste eliminated, gotchas can't be forgotten because they're baked into the
canonical SQL.

---

## 8. Pillar D — Provenance & Verification Ledger (the trust system)

**Creates G5/G6. This is the crown jewel** — it makes "how did you get these numbers?" instantly and
provably answerable.

**Design principle:** *every number that leaves the workspace* (summary, presentation, Jira, Slack,
deck) traces to the exact query + BQ job that produced it, and re-runs to the same value on demand.

### 8.1 The chain of custody
```
number in a deck  →  [^prov:claim_id] tag  →  ledger entry  →  sql_file (sha256-pinned)
                                                     │                    │
                                              job_id (bq show -j)   re-runnable today
                                                     ▼
                                        bq_verify.py → recompute → MATCH ✓ / DRIFT ✗
```

### 8.2 The per-ticket ledger — `tickets/<t>/provenance.jsonl` (append-only)
`bq_run.sh` already takes `--ticket`; add `--claim <id>`. When both are set, the wrapper appends a
provenance record **in addition to** the global perf log:
```json
{"claim":"wgu_90d_visit_rate","label":"...","ticket":"TI-XXX",
 "sql_file":"queries/14_wgu_visit_rate.sql","sql_sha256":"…",
 "job_id":"dw-main-silver:perf_2026…","phase":"full",
 "gb_processed":4.2,"rows_returned":1,"result_sha256":"…",
 "timestamp":"2026-07-19T…","git_commit":"fc6f05a","verify_status":null,"verified_at":null}
```
- `sql_sha256` proves the file wasn't edited after the fact.
- `job_id` → anyone runs `bq show -j` and re-executes.
- `result_sha256` proves the *number* (hash the result rows; the wrapper tees stdout under a row cap;
  for large pulls the `outputs/NN_slug.csv` is the canonical hashed artifact).
- `git_commit` pins repo state.

### 8.3 The SQL header convention (co-located provenance; builds G6)
Every committed query is flat `queries/NN_slug.sql` with a greppable header — the provenance travels
*with* the SQL:
```sql
-- claim:   wgu_90d_visit_rate
-- ticket:  TI-XXX
-- question: 90-day WGU visit rate, 10% holdout ITT
-- tables:  summarydata.visits, core.campaigns
-- sampled: 2026-07-18 (1 partition) → est 22.0% ;  full 2026-07-19 → 22.1%
-- job_id:  perf_20260719_…   cost: 4.2 GB / 8s
```
`new_ticket.sh` scaffolds the flat layout; `check_ticket_layout.sh` lints it (Architecture §7); a
`sql_header_lint` ensures every query carries `claim/ticket/tables/sampled/job_id`.

### 8.4 `bq_verify.py --ticket TI-XXX [--claim …]` (the one-command answer)
Reads the ledger; for each claim: re-run the pinned SQL (or inspect the stored job), recompute
`result_sha256`, report **MATCH / DRIFT** and stamp `verified_at`. When a manager asks "how did you
get 22.1 %?" the answer is a path, a job_id, and *one command that proves it still holds today*.

### 8.5 `check_provenance.py <doc>` (nothing ships untagged)
Scans a `summary.md` / `presentation.md` for numeric claims lacking a `[^prov:claim_id]` tag and
flags them (advisory). Wired as a **pre-share gate**: before a deck or Jira share-out, the
**provenance-auditor** agent runs `bq_verify` + `check_provenance` and reports any un-backed number.
This is "always easy to answer how," mechanized — you cannot present a number you can't defend.

---

## 9. The AI layer — automatic vs. triggered

Honest scope (per Architecture §1): shell hooks **detect/log/enforce/print**; they **cannot invoke a
model**. Semantic work (writing what a pattern means, promoting a cookbook entry, auditing a deck) is
a **triggered** agent step.

| Surface | Role | Auto or triggered |
|---|---|---|
| `bq_run.sh` gate + dual log (perf + provenance) | deterministic sample-first + cost + chain-of-custody | **auto** (Pillar A/D) |
| PreToolUse hook | block raw `bq query` (existing) + now the dry-run gate lives in the wrapper | **auto** |
| SessionStart hook | print routing/coverage (existing) + "N claims unverified in open tickets" | **auto** |
| Stop hook | `/capture` reminder (existing) + "N reported numbers lack provenance tags" | **auto** (advisory) |
| **perf-analyst** (exists) | mine digest → per-table cost, playbook rules, cookbook promotions | triggered, on cadence |
| **curator** (`/capture`, exists) | route facts home, propose cookbook promotions, correct stale lines | triggered |
| **cataloger / reviewer / fixer** (exist) | skeleton→enriched→verified; verify a cookbook entry | triggered |
| **provenance-auditor** (new, thin) | pre-share: `bq_verify` + `check_provenance`, report un-backed numbers | triggered, pre-share |

New agent to add: **provenance-auditor** (read-only: Bash + Read; no Write). Everything else reuses
the existing roster.

---

## 10. Data contracts (so a sibling plan can integrate)

**Perf-log record — fields to ADD** (existing fields unchanged): `phase` (dryrun|sample|full|adhoc),
`est_gb`, `est_actual_ratio`, `sql_sha256`, `sample_spec`, `git_commit`.

**Provenance record** (`tickets/<t>/provenance.jsonl`): `claim, label, ticket, sql_file, sql_sha256,
job_id, phase, gb_processed, rows_returned, result_sha256, timestamp, git_commit, verify_status,
verified_at`.

**Cookbook entry** (§7.1) and **playbook rule** (technique + before→after GB) — markdown `###`
blocks with front-matter on the file so they appear in `_ROUTING.md`.

**New config knobs** (add to existing `config.env`): `BQ_SAMPLE_GATE=warn|block` (default `warn`),
`PROVENANCE_ROWCAP` (max rows to hash inline, default e.g. 10_000).

---

## 11. Success metrics (how the master-planner measures this pillar)

| Metric | Source | Target trend |
|---|---|---|
| Median GB / query | perf log | ↓ over time |
| % real runs preceded by a `--phase sample` | perf log `phase` | ↑ toward 1.0 for `est_gb>WARN` |
| Estimator accuracy `|est−actual|/actual` | `est_actual_ratio` | tighten toward 0 |
| Sample→full extrapolation error | `phase-accuracy` mode | < ~10 % on rate metrics |
| Cookbook reuse rate (queries citing a slug) | headers / ledger | ↑ |
| Catalog coverage skeleton→enriched→verified | `_COVERAGE.md` | 185/1/7 → verified rising |
| % reported numbers with a provenance tag | `check_provenance.py` | → 100 % on shared docs |
| Ledger re-verify MATCH rate | `bq_verify.py` | → 100 % (DRIFT = a real bug caught) |
| Time-to-answer "how did you get this" | manual | < 1 min (one `bq_verify` call) |

---

## 12. Landing order (dependencies are real)

| Phase | Deliverable | Depends on | Effort | Why this order |
|---|---|---|---|---|
| **0** | Harden `bq_run.sh`: dry-run gate + `--phase` + `est_gb`/`sql_sha256`/`git_commit` logging (knobs already exist) | nothing | **S** | highest leverage, smallest change — hits pain #1 directly, unlocks the `phase-accuracy` digest already written |
| **1** | Create `optimization_playbook.md` + `query_cookbook.md` + `runbooks/sample_first_query.md`; seed from existing MEMORY/`data_knowledge` gotchas; front-matter → rebuild indexes | Phase 0 (real cost to cite) | **M** | gives learnings & reusable queries a home (G2/G3/G4) |
| **2** | Provenance: `--claim` + ledger append in `bq_run.sh`; SQL-header convention; `new_ticket.sh`/`check_ticket_layout.sh`; `bq_verify.py`; `check_provenance.py` | Phase 0 | **M–L** | the trust layer (G5/G6) — the hardest sell, the biggest payoff |
| **3** | Wire the cadence: perf-analyst → playbook/cookbook/observed-cost; curator promotes cookbook entries; **provenance-auditor** as pre-share gate; SessionStart/Stop hook additions | Phases 1–2 | **M** | closes the flywheel — learnings flow back automatically |
| **4** | Compounding tools: `bq_precheck.py` (pre-flight card), `bq_snippet.py` (param-fill), `bq_estimate.py` (extrapolation verdict), phase-accuracy dashboards | Phases 1–3 | **M** | pure velocity multipliers once the loop turns |

**Start at Phase 0.** It is a self-contained `bq_run.sh` change against knobs that already exist, and
it immediately produces the `phase`/`est_gb` signal the rest of the loop feeds on.

---

## 13. Risks, gotchas, non-goals

- **Gate friction.** A hard block on un-sampled big queries will annoy at first — default
  `BQ_SAMPLE_GATE=warn`, flip to `block` once adoption shows in the log. `--force` always escapes.
- **Sampling bias is real** — §5.3 exists precisely so we don't ship wrong extrapolations. Distinct
  counts and joins are the landmines; the runbook calls them out explicitly.
- **Result hashing must normalize** (row order, float formatting) or `bq_verify` false-DRIFTs — hash
  a canonicalized, ORDER-BY'd projection, not raw stdout.
- **Read-only stays read-only** — no `bq_verify` write path; verification re-runs SELECTs only. The
  existing no-DDL/DML boundary holds.
- **Non-goals:** not replacing the ticket model, retrieval indexes, or agent roster (sibling plans /
  existing kit own those); not building a warehouse-agnostic layer (the SQLMesh view→physical
  mechanic stays, guarded by `WAREHOUSE_PROFILE`); not automating semantic writes unattended.
- **Idempotency invariants (Architecture §10) are inviolable** — no timestamps in generated files,
  total-ordered sections, append-only observed regions. New docs obey them.

---

## 14. Handoff summary (one paragraph for the master-planner)

MNTN already logs rich per-query BQ cost and runs a self-documenting per-table catalog with agents.
This pillar adds the **two things it lacks**: (1) an **enforced sample-first ladder** — the wrapper
dry-runs, gates on estimated GB, and labels sample vs full so every query is estimated before it's
executed and every run teaches the next via a perf flywheel that curates learnings into a **query
cookbook** and **optimization playbook** surfaced *before* the next query; and (2) a **provenance
ledger** that binds every reported number to a sha-pinned SQL file + BQ job_id + result hash, with a
one-command `bq_verify` that re-runs and proves the number still holds — so "how did you get this?"
is always answered in under a minute. Build **Phase 0 first** (harden `bq_run.sh`); it's the smallest
change and feeds everything downstream.

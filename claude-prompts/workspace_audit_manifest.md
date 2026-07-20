# Workspace Structure Audit — Manifest (review before execution)

Generated 2026-07-20 by `.claude/scripts/audit_structure.py` (deterministic, read-only) + human judgment.
**Nothing here has been executed.** Approve tiers/items and I run them in safe batches.

Regenerate anytime: `python3 .claude/scripts/audit_structure.py --json <path>`

## Headline

The workspace is **~95% structurally clean**. Root is tidy, the 65-ticket skeleton conforms
(`lint_tickets` 0 violations), git hygiene is good (0 untracked-non-ignored files). The audit found
**114 findings**, but most are trivial (30 empty dirs) or judgment calls (which committed CSVs to keep).
A blind "rename/move everything" pass would touch ~1,160 files to fix ~40 real ones and risks breaking
path references — so this is scoped into **safe** vs **judgment** vs **standard-reconciliation** tiers.

| Category | Count | Tier | Default action |
|---|--:|---|---|
| junk (Spark markers, .pyc) | 7 | **1 — safe** | delete + gitignore |
| empty scaffolded dirs | 30 | **1 — safe** | delete (recreated on demand) |
| queries/ non-.sql files | 10 | **2 — judgment** | move to `artifacts/` (or bless the runbook exception) |
| naming violations | 32 | **2 — judgment** | mixed — see breakdown |
| tracked data (committed CSVs/JSON) | 29 | **2 — judgment** | keep as record OR gitignore |
| root stray (.DS_Store, .vscode, vendored tool) | 3 | **2 — judgment** | gitignore / relocate |
| deep nesting (vendored PR copy) | 1 | **2 — judgment** | slim or keep |
| tracked `.claude/projects/` tree | 1 | **2 — judgment** | confirm intentional |
| ticket missing summary.md | 1 | **2 — judgment** | add card |
| root-spec + naming carve-outs stale | — | **3 — standard** | update `folder_definitions.md` |

---

## TIER 1 — SAFE (mechanical, reversible, recommend auto-run on approval)

### 1a. Junk — delete + gitignore (7 files)
Spark/Databricks write-markers and a compiled Python cache — should never have been tracked:
- `tickets/ber_2250_incrementality_overhaul/ti_933_select_lift_analysis/outputs/databricks_7d/result/{_SUCCESS,_started_*,_committed_*}`
- `…/databricks_14d_v3/result/{_SUCCESS,_started_*,_committed_*}`
- `tickets/ti_896_audience_composition_2025_drop/artifacts/__pycache__/generate_charts.cpython-311.pyc`
- **Also gitignore:** `_SUCCESS`, `_started_*`, `_committed_*`, `__pycache__/`, `*.pyc`, `.DS_Store`, `.vscode/` (20 `.DS_Store` currently sit untracked on disk).

### 1b. Empty scaffolded dirs — delete (30)
Ticket subfolders (`queries/ meetings/ outputs/ artifacts/`) created by the template but never used — pure
noise in a file tree. They're recreated on demand. (git doesn't track empty dirs, so this is disk-only tidy.)
Examples: `ti_1003_experiment_archive/{artifacts,queries,meetings,outputs}/`, `audi_1111_vendor_quality/{queries,meetings}/`, … (full list in the JSON manifest).

---

## TIER 2 — JUDGMENT (your call per group)

### 2a. Non-.sql files inside `queries/` (10) → recommend MOVE to `artifacts/`
The spec says `queries/` is SQL-only. These are runner scripts / indexes / guides:
- `.sh` runners: `ti_809/queries/ti_809_run_all_queries.sh`, `audi_1070/queries/reusable_diagnostic_pack/run_diagnostic.sh`, `documentation/docs/advertiser_yoy_diagnostic/queries/run_diagnostic.sh`, `audi_1089/runbook/queries/q14_gcs_ingest_bytes.sh`
- `.md` indexes/guides: `audi_1070/queries/QUERY_INDEX.md`, `audi_1089/runbook/queries/{MANIFEST.md,VALIDATION_GUIDE.md}`, `ti_837…/queries/ti_837_lift_analysis_plan.md`, `audi_1070/queries/reusable_diagnostic_pack/README.md`, `ti_650/queries/_archive/ti_650_zach_traced_ip_guide`
- **Recommendation:** move the `.sh`/`.md` to the sibling `artifacts/`. **Exception to consider:** the `audi_1089/runbook/queries/` MANIFEST+VALIDATION_GUIDE are a *deliberate self-contained handoff package* — arguably bless "a query pack may carry its own MANIFEST/README" rather than move it.

### 2b. Naming violations (32) — split by risk
- **Leave (machine round-trip):** 12 Mode-exported queries with spaces+hash under `ti_1037/perf_report/mode/batch1_queries/` (e.g. `00b Reach By Score.9b2f59dea917.sql`). These round-trip to/from Mode by that exact name — renaming breaks the sync. **Recommend: leave, document the exception.**
- **Rename with care (referenced in docs):** `ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py` is cited by name in `CLAUDE.md` + `experimentation.md`. **Recommend: keep as-is** (rename would need a coordinated ref update for zero real gain) OR rename + update both refs.
- **Rename (safe, low-stakes working docs):** `HANDOFF_PROMPT.md`, `TOMORROW_PLAN.md`, `PHASE5_PLAN.md`, `QUERY_INDEX.md`, `README_MODE.md` → lowercase. CamelCase scripts `TI_688_IP_Score_Eval.py`, `TI_704_Fangorn_DCG_Eval.py`, `TI_704_Fangorn_IP_DCG_Scoring.py` → snake_case (check for importers first). The `LEAN` in `ti_837_lift_analysis_30adv_7day_v5_xwin_LEAN_2segments.sql`. The colon+spaces file `ti_504/artifacts/BEST: Bayesian… .html`. The dash in `ti_797/meetings/…alex_-_project_discussion.txt`.
- **Borderline (kit convention):** `.claude/agents/{perf-analyst,reviewer-adversarial}.md`, `workflows/prompts/reviewer-adversarial.md`, `slack_bot/RECOVERY.md`, `todoist-mcp-transfer/{QUICK-START.md,mcp-server/SETUP.md}` — dashes/uppercase are the external tool's/kit's own convention. **Recommend: leave, carve out in the standard.**

### 2c. Tracked data — committed CSV/JSON outputs (29) → KEEP or GITIGNORE?
Small result CSVs force-added past `.gitignore` (`ti_790` feature rankings ×7, `ti_896` composition ×12,
`ti_832` importance ×5, `ti_921` lift ×4, `ti_650` ×2). They're the **analytical record** — cheap, and
they make a ticket's numbers reproducible without a rerun. **Recommendation: KEEP** (they're a feature, not
debt) — but if you'd rather the repo hold only code+docs, I can gitignore + `git rm --cached` them per-ticket.

### 2d. Root stray (3)
- `.DS_Store` (root) + `.vscode/` → **gitignore** (Tier-1 covers .DS_Store).
- `todoist-mcp-transfer/` (55 MB vendored MCP tool + a node `mcp-server/`) → **recommend relocate out of the analytics workspace** (it's a tool, not analysis). Options: move to a separate repo/dir, or keep but document it as a blessed exception. Your call — it's the single biggest "why is this here?" item.

### 2e. Deep nesting — vendored PR copy (1)
`ti_956/artifacts/targeting_infra_ml_pyproject_pr/` (a checked-in copy of a PR's package, 6 levels deep).
**Recommend:** keep only if it's an active reference; otherwise link to the PR and delete the copy.

### 2f. Tracked `.claude/projects/` tree (1)
A Claude session/memory tree is tracked **inside this repo** (`.claude/projects/-Users-…/memory/…`).
Memory canonically lives in **global** `~/.claude/`. **Recommend: confirm** — if it's a stray duplicate,
gitignore + `git rm --cached`; if intentional (a committed snapshot), document why.

### 2g. Ticket missing `summary.md` (1)
`tickets/ti_argocd_secrets_audit/` has no `summary.md` (fails the skeleton + won't appear in `INDEX`).
**Recommend:** add a card (it's the sibling of `ti_kafka_secret_sweep`, which does have one).

---

## TIER 3 — STANDARD RECONCILIATION (update the spec to match reality)

The audit proved `folder_definitions.md` is itself stale — its "root holds ONLY these 6" list predates
legitimate additions. **Recommend updating `folder_definitions.md` to:**
- Bless the post-spec root entries: `workflows/`, `self_review/`, `slack_bot/`, `README.md`, `.mcp.json`.
- Document the **naming carve-outs**: README-family + generated `INDEX.md`/`SKILL.md`/`MEMORY.md`; sanctioned
  dashed dirs (`claude-prompts/`, vendored tool trees); machine round-trip exports (Mode). This is what makes
  the audit repeatable without re-triaging the same false positives — and it's the "master standard" doc.

---

## Optional deeper pass (agents) — offer, not yet run
The above is the exhaustive **mechanical** audit. A **semantic** pass (a few agents over ticket clusters)
would add what a script can't see: superseded/duplicate deliverables, redundant output CSVs, and misfiled-
by-content files worth consolidating. Say the word and I'll run it.

## What I'll do on approval
Tell me which tiers/items to execute. Default plan if you just say "go": run **Tier 1** (junk + empty dirs +
gitignore) and **Tier 3** (update the standard) — both safe and reversible — and hold Tier 2 for per-group
decisions. All work on a branch, committed in small labeled batches.

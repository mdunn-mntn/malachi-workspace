# PORTING.md — stand up this kit on a fresh machine, then make it yours

This bundle is a **sanitized, generic copy of the AI Workflow Kit**: the machinery (hooks, scripts,
skills, agents, commit gate, indexing + memory system) with all job-specific content stripped and every
private value replaced by a `<PLACEHOLDER>`. Drop it anywhere, bootstrap it, fill the placeholders, and
fill the knowledge base with *your* use case.

The design is `workflows/ARCHITECTURE.md`; the deterministic-layer operator guide is `.claude/README.md`;
the adopt-this overview is `documentation/ai_workflow_kit/README.md`. Two ideas do all the work:
**(1) load indexes, not the tree** — grep generated indexes, open only the one doc a query needs;
**(2) generate and enforce, don't hand-maintain** — indexes are regenerated from front-matter, a commit
gate blocks malformed files, and the docs describing the system are themselves generated.

---

## 0. The cross-job barrier — why this is safe to carry between jobs

This bundle reveals **nothing about the job it came from**. The packager runs two gates and refuses to
emit unless BOTH are clean:
- **Secrets sweep** — zero literal private values (paths, emails, tracker/warehouse IDs, hosts, names).
- **Domain-blind sweep** — zero job/domain CONTEXT: illustrative table/dataset/pipeline/incident/ticket
  names and the domain taxonomy are scrubbed to neutral generics. A reader learns the machinery, not the business.

What never travels: the knowledge base, real memory facts, real tickets/incidents, the runbook's real
catalog, and (global layer) session history, prior-job transcripts, and any live token. You carry your
framework, not your data. A fresh bundle per job keeps the two jobs isolated.

## 1. Quickstart

```bash
tar xzf ai-workflow-kit.tar.gz && cd ai-workflow-kit   # or: cd into the bundle folder
bash bootstrap.sh                  # repo layer: preflight → chmod → hooks → memory symlink → build → verify
bash bootstrap.sh --with-global    # ALSO install your personal ~/.claude/ framework (backs up any existing)
```
`bootstrap.sh` is idempotent. It ends by listing the placeholder files still to fill and the auth steps.
The `global/` layer (your `~/.claude/CLAUDE.md`, settings, MCP snippet) installs only with `--with-global`,
backs up anything it replaces, and never copies the live task-manager token (fill it yourself).

---

## 2. Fill-in table — every placeholder, where it lives, what to set it to

Find them all: `grep -rIn -- '<[A-Z_]*>' .claude .mcp.json .claude/scripts/config.env`.

| Placeholder | Set to | Lives in |
|---|---|---|
| `<WORKSPACE_PATH>` | your checkout path (or leave the self-resolving `$(git rev-parse …)` already substituted in scripts) | skills, `slack_bot/config.yaml` |
| `<WORK_EMAIL>` | your work email | skills, `lib/`, drive mount |
| `<JIRA_BASE_URL>` / `<JIRA_HOST>` | `https://<you>.atlassian.net` / `<you>.atlassian.net` | `new_ticket.sh`, `frame` skill |
| `<GCP_PROJECT>` / `<GCP_PROJECT_BRONZE>` | your BigQuery project(s) | `config.env`, `.mcp.json` |
| `<BQ_REGION>` | your dataset region, e.g. `us-central1` | `config.env`, `.mcp.json` |
| `<DATASETS>` | comma-separated datasets to introspect | `config.env` |
| `<GH_USER>` | your GitHub user (deck-sharing gists) | `share_deck.sh` |
| `<AUDIT_HOST>` / `<AUDIT_HOST_IP>` / `<AUDIT_SSH_KEY>` / `<AUDIT_REPO>` | your always-on audit host, or delete the Pi cron (see §6) | `pi_run_workflow_audit.sh` |
| `<PYTHON311>` | your python3.11 path (local-whisper transcription) | `transcribe.sh` |
| `<ORG>` / `<org>` / `<ORG_DOMAIN>` | your org name / domain (prose + comments) | throughout |

The bootstrap-critical path (`build_index`, `verify`, `hooks_selftest`, `new_ticket`, the linters, the
commit gate) does **not** depend on any placeholder value — the kit verifies clean before you fill a
single one. Placeholders only matter when you actually run the subsystem that reads them.

---

## 3. Toolchain

- **Required:** `git`, `python3` (stdlib only — no pip install needed for the core kit).
- **Recommended:** `jq` (bq wrapper), `gh` (GitHub MCP + deck sharing), `node`/`npx` (MCP servers).
- **Warehouse module (optional):** `bq` + `gcloud` (Google Cloud SDK).
- **Transcription (optional):** `ffmpeg` + either an OpenAI key or `mlx-whisper` (Apple Silicon).
- **.xlsx builder (optional):** `pip install openpyxl pandas numpy`.
- **Slack bot (optional):** `pip install -r slack_bot/requirements.txt` (`slack-sdk anthropic pyyaml`).

---

## 4. Per-user auth (none of this travels in the bundle — set it up per machine)

- **BigQuery:** `gcloud auth login` and `gcloud auth application-default login`.
- **GitHub:** `gh auth login` (the github MCP uses gh's own auth).
- **Jira:** create an API token, export `JIRA_API_TOKEN` in your shell profile; the `frame`/ticket curls
  use it. Set `<JIRA_BASE_URL>` + `<WORK_EMAIL>`.
- **MCP servers (`.mcp.json`):** jira + github + bigquery are wired; enable per project and fill the BQ
  project/region. Add any others (e.g. a task manager) to your user-level MCP config, not the repo.
- **claude.ai connectors** (Drive / Gmail / Calendar, if used): authorize in claude.ai connector settings.

---

## 5. Re-theme for a new use case

1. **Seed the front door.** Edit `knowledge/START_HERE.md` — replace the example task→doc rows with the
   recurring "I need to…" questions of your domain. This is the map every cold session routes through.
2. **Set the hot tier.** Edit `knowledge/memory/MEMORY.md` — keep the generic working rules, add one dense
   block of your domain's always-true gotchas. Keep it small; everything else is grep-on-demand.
3. **Add knowledge as you go.** Write docs under `knowledge/` with front-matter (`doc_type`, `keywords`,
   `domain`, `last_verified`). Run `.claude/scripts/build_index.sh` — they appear in `_ROUTING.md`
   automatically. Capture cross-session facts with `/capture` (writes a `memory/<slug>.md`, rebuilds the index).
4. **Define your own `doc_type`s** if the built-ins (`bq_table`, `memory`, `runbook`, `decision`, `ticket`)
   don't fit — `build_index.sh` indexes any doc_type; add templates under `knowledge/`.
5. **Run the loop.** New work: `new_ticket.sh <id> "<desc>"` → `/frame <id>` (agree the question, the gate
   blocks `in_progress` until locked) → do the work → `/capture` (route what you learned home). Any alert:
   `/oncall`. Weekly: `/workflow-audit` (propose-only system retro).

---

## 6. Subsystems (all shipped as sanitized skeletons — activate what you need)

- **Warehouse module.** Fill `config.env` (`GCP_PROJECT`, `BQ_REGION`, `DATASETS`). Keep
  `WAREHOUSE_PROFILE=generic` unless your `silver.*` objects are views over versioned physical tables
  (then `sqlmesh`, and the cataloger will resolve view→physical). If you don't query a warehouse, ignore
  it — nothing else depends on it. The `enforce_bq_wrapper` hook forces every query through
  `bq_run.sh` (perf + provenance log); disable it by removing its block from `.claude/settings.json`.
- **Branded .xlsx builder** (`lib/xlsx_builder.py`, class `BrandWorkbook`). Swap the `BRAND` hex dict (or
  drop `lib/assets/brand.json` + `lib/assets/logo.png`) for your palette/logo — no code edit needed.
  Demo: `python3 lib/xlsx_demo.py`.
- **Meeting transcription** (`.claude/scripts/transcribe.sh`, `/transcribe`). Needs `ffmpeg` + an OpenAI
  key or `mlx-whisper`. Set `<PYTHON311>` and your recordings dir.
- **Slack knowledge bot** (`slack_bot/`). Runs on an always-on host. See `slack_bot/RECOVERY.md`: create a
  Slack app, put `SLACK_BOT_TOKEN` + `ANTHROPIC_API_KEY` in an env file, `pip install -r requirements.txt`,
  schedule `run_daily.py`. Optional — the core kit doesn't need it.
- **Weekly audit cron** (`.claude/scripts/pi_run_workflow_audit.sh`). Deploy to any always-on host that
  can pull the repo; it runs ONLY the key-free deterministic aggregator and commits a dated
  `signals_<date>.md` (never put an API key on that host). Fill `<AUDIT_HOST>`/`<AUDIT_REPO>`. Or skip it
  entirely and run `/workflow-audit` manually on your dev machine.

---

## 7. Revert / off-switches

- **Memory symlink:** `rm ~/.claude/projects/<slug>/memory` (restore the `.backup-presymlink` if present).
- **Commit gate:** `git config --unset core.hooksPath`. Bypass one commit: `git commit --no-verify`.
- **Any hook:** delete its block from `.claude/settings.json` (they are advisory except the bq wrapper).

---

## 8. Intentionally dropped in this port (add back if you want them)

The prior job's business content (`knowledge/*` prose docs, real memory facts, real tickets/incidents), the
licensed brand assets (`lib/assets/`), the vendored task-manager MCP build, `settings.local.json`
(per-machine), the self-review, a narrow Databricks smoke-test one-off, and — for domain-blindness — the
two most example-dense design docs (`workflows/INGEST_GUIDE.md`, `workflows/bq_velocity_provenance_plan.md`)
and the adtech-sample `lib/xlsx_demo.py`. The `slack_bot/` recovery note was replaced with a generic one.
The machinery that operated on all of it shipped; only the job-specific payload was left behind. To harden
further, extend `documentation/ai_workflow_kit/domain_scrub_map.txt` and re-run the packager.

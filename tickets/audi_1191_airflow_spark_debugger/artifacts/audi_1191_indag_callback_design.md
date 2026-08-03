# AUDI-1191 Phase 3 — In-DAG auto-fire callback (design spec)

**Status: DESIGN ONLY. No airflow-ti change proposed yet.** This is the reviewable spec for Ryan
before any prod code. Scope = the *trigger* half of Phase 3 (auto-fire on task failure). The Slack
auto-reply and propose-only PR remain separately gated.

## BLUF

Wire the debugger into airflow-ti's **existing** `JobConfig.make_default_args` failure-callback list,
behind an opt-in `Variable` flag, as a **two-tier** design: a fast, key-free, network-free **in-worker
first-look** (identity + engine + Airflow-log signature from `context["exception"]`) that emits a
structured event; and an **off-worker deep RCA** (the existing `orchestrate`, which has the gcloud /
databricks creds) that consumes the event. The callback never calls a cloud CLI, never calls an LLM,
and never raises into the scheduler. Net new prod code is ~40 lines in one file; the heavy logic stays
in our workspace package.

## 1. The attach point (exact, verified)

airflow-ti already centralizes every task/DAG failure callback in one place:

- `include/job_config/job_config.py:124` — `make_default_args(severity, **kwargs)` builds a
  `failure_callbacks` list and sets `args["on_failure_callback"] = failure_callbacks` (line 145).
  **This is the task-level attach point** — one appended callback fires on *every* task failure for
  every DAG that builds args through this JobConfig.
- `include/job_config/job_config.py:161` — `make_dag_args(...)` sets a DAG-level `on_failure_callback`
  (line 199) for DagRun-level failures (the task-level list "often does not run" for those, per the
  module docstring, lines 17-20).
- The callback contract is `Callable[[Context], None]` (see `create_slack_callback`,
  `slack_messages.py:60`) — a closure over the Airflow task `Context`.

We add one callback to the `failure_callbacks` list. No DAG files change; teams already opt into
alerting by constructing their DAG args through `JobConfig`.

## 2. The core constraint → why two tiers

The callback runs **inside the Airflow worker/task pod**, which forces four hard limits:

1. **Key-free / no LLM in prod.** Per MNTN policy there is no `ANTHROPIC_API_KEY` in prod (the pattern
   decommissioned 2026-06-10). So the in-worker tier is **deterministic-only** — the regex classifier,
   which needs no network.
2. **Must never raise.** A raising `on_failure_callback` pollutes the worker/scheduler. Mirror the
   existing guard (`pagerduty_messages.py:209-216`, `try/except Exception: logger.exception(...)`).
3. **Must be fast + non-blocking.** The callback holds the worker slot. A synchronous
   `gcloud dataproc batches describe` or `databricks jobs get-run` (seconds, + auth) is too slow and,
   in prod, has no user creds anyway. So the engine RCA is **off-worker**.
4. **Package not present in the image.** `airflow_debugger/` is a workspace package, not an airflow-ti
   dependency. The in-worker tier must be either vendored as a tiny self-contained module or reduced to
   "emit the event; diagnose elsewhere." We choose **emit-the-event** to keep the prod footprint minimal.

**Therefore:**

| Tier | Runs where | Does | Creds | Latency |
|---|---|---|---|---|
| **First-look** | in-worker (callback) | parse `Context` → identity + operator→engine + `classify(exception)` → emit structured event (+ optional enrich the existing Slack failure msg) | none (key-free) | ms |
| **Deep RCA** | on-call box / Mac (has creds) | consume event → `orchestrate` (engine RCA + incident match + optional LLM) → write `<log>.rca.md` / threaded reply | astro + gcloud + databricks OAuth | seconds |

This is the same split the `--watch --diagnose` loop already uses; the callback is a **lower-latency,
event-driven trigger** that replaces (or complements) the 5-minute poll — not a new RCA engine.

## 3. What the callback extracts from Context (no network)

All available synchronously in an `on_failure_callback` (confirmed against `slack_messages.py` +
`pagerduty_messages.py`):

| Field | Source | Feeds |
|---|---|---|
| dag_id, task_id | `context["task_instance"].dag_id / .task_id` | identity / dedup |
| run_id | `context["dag_run"].run_id` | identity / dedup |
| try_number, max_tries | `context["task_instance"].try_number / .max_tries` | dedup / "final try only" gate |
| map_index | `context["task_instance"].map_index` | dedup (mapped tasks) |
| log_url | `context["task_instance"].log_url` | the deep-RCA fetch pointer |
| operator | `type(context["task"]).__name__` | operator→engine routing |
| exception text | `str(context["exception"])` | `classify()` → Airflow-log signature |

The exception text is exactly what the regex taxonomy consumes, so the **first-look signature is
computable in-worker** (e.g. `sensor_timeout`, `path_not_found_late_data`, `pod_evicted_404`) with zero
network. The engine correlation (batch_id / dbx run_id) is deferred to the deep tier because it needs
the full log body / cloud APIs.

## 4. Feature-flag design (mirror the PagerDuty pattern exactly)

Opt-in, off by default, same shape as `pagerduty_send_enabled()` (`job_env.py:25`):

```python
def debugger_autofire_enabled() -> bool:
    # OFF unless explicitly turned on. Prod rollout = flip one Variable per team, reversible.
    raw = Variable.get("DEBUGGER_AUTOFIRE", default="false")
    return str(raw).strip().lower() in {"true", "1", "yes"}
```

Rollout is **one team at a time** by flipping the Variable, fully reversible with no deploy. Optionally
scope tighter with a `JOB_CONFIG_<dag_id>` override (the existing per-DAG override mechanism,
`job_env.py:62`). Until the flag is on for a DAG, the callback is a no-op even if merged.

## 5. Failure isolation + latency budget

- Wrap the whole body in `try/except Exception: logger.exception(...)` — a debugger failure must never
  change task/DAG outcome (it already failed; we only annotate).
- No blocking I/O in-worker. The event emit is a single append (GCS object write or an Airflow
  asset/Dataset emit) — bounded, retryable, best-effort.
- Guard on `try_number > max_tries` (final attempt only) so retried-then-succeeded tasks don't emit.

## 6. Dedup

Idempotency key = `(dag_id, run_id, task_id, try_number, map_index)`. The off-worker consumer skips a
key it has already diagnosed (the incident-match layer + a small seen-set). This also protects against
the DAG-level + task-level callbacks both firing.

## 7. The sanctioned-Slack unlock (finding)

airflow-ti **already posts to Slack from these callbacks** via
`airflow.providers.slack.notifications.slack.SlackNotifier` over an org-sanctioned connection
(`slack_messages.py:68-74`), lazy-imported to avoid Slack SDK import at DAG-parse. This is the blessed
Slack path the *separate* Phase-3 "sanctioned Slack reply" item was blocked on — it exists here because
the integration lives inside airflow, not in our decommissioned local bot. So the first-look tier can
**append the deterministic RCA line to the existing failure message** (or post a threaded follow-up)
with no new Slack credential. The deep-RCA threaded reply still needs a post-back mechanism from the
off-worker box (open question below).

## 8. Workspace-side companion (ours, key-free, buildable now)

`airflow_debugger/context_parse.py::parse_context(ctx: dict) -> ParsedFailure` — the in-callback
extraction as a pure function over a Context-shaped dict (so it is offline-testable with a fake context
and has **no Airflow import**). This is the contract the prod callback would call (or inline). Built +
unit-tested in this ticket (see `airflow_debugger/tests/test_context_parse.py`) so the design is proven,
not paper. It reuses `classify()` and the same operator→engine map as `parse.py`.

## 9. Reference implementation sketch (PROPOSAL — not committed to airflow-ti)

```python
# include/job_config/debugger_callback.py  (proposed; ~40 lines)
def create_debugger_callback(team: str | None = None) -> Callable[[Context], None]:
    def callback(context: Context) -> None:
        try:
            from include.job_config.job_env import debugger_autofire_enabled
            if not debugger_autofire_enabled():
                return
            ti = context.get("task_instance")
            if ti is None or ti.try_number <= (ti.max_tries or 0):
                return  # not the final attempt
            event = {
                "dag_id": ti.dag_id, "task_id": ti.task_id,
                "run_id": context["dag_run"].run_id,
                "try_number": ti.try_number, "map_index": ti.map_index,
                "operator": type(context["task"]).__name__,
                "log_url": ti.log_url,
                "exception": str(context.get("exception") or "")[:4000],
            }
            _emit(event)          # single best-effort GCS append / asset emit
        except Exception:         # never raise into the scheduler
            logger.exception("debugger_callback failed (non-fatal)")
    return callback
```

Wired by appending `create_debugger_callback(self.team.value)` to `failure_callbacks` in
`make_default_args` (job_config.py:126-145), behind the flag.

## 10. Rollout plan + review gates

1. Ryan reviews this spec (attach point, footprint, flag, isolation). **Gate 1.**
2. Land the workspace-side `parse_context` + the off-worker event consumer first (all in our repo,
   key-free, no prod change). Validate against INC-001..009 replays.
3. Open a feature-branch PR to airflow-ti with `debugger_callback.py` + the flag + the one-line wire,
   default **off**. Ryan's review; never push main. **Gate 2.**
4. Flip `DEBUGGER_AUTOFIRE` for one low-risk team's DAG; watch for a week; confirm zero task-outcome
   impact and correct events. **Gate 3.**
5. Expand team-by-team. Slack threaded-reply + propose-only PR remain separately gated.

## 11. Open questions for Ryan

- **Event sink:** GCS prefix (workers already have SA write) vs an Airflow 3 asset/Dataset the on-call
  box watches vs enrich-Slack-only. Preference?
- **Deep-RCA post-back:** how should the off-worker RCA reply land in the alert thread — via the same
  airflow Slack connection (a tiny airflow-side "post RCA" task) or a sanctioned webhook?
- **Placement:** task-level list only, or also the DAG-level `on_failure_callback` for
  dagrun_timeout-style failures (INC-005's TTL kill surfaces as a task failure, so task-level covers it,
  but confirm)?
- **Footprint tolerance:** OK to add one file + one `job_env` flag + one appended callback, or prefer
  the callback body live entirely in a vendored single module?

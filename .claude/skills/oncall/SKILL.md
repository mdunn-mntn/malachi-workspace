---
name: oncall
description: >-
  Handle an on-call alert end-to-end AND enforce the write-back so the runbook gets smarter every time.
  Classifies whether the trigger is an operational alert (→ this workflow) or a question (→ a ticket via
  /frame), runs the general triage protocol, matches the Known-Alert Catalog for an instant protocol,
  checks empirical GCS/BQ state, classifies the verdict, acts (clear/re-run/route — never hot-patch prod),
  and then writes the incident to all three surfaces (§3 narrative + §2 catalog row + incident_log.jsonl)
  and rebuilds the index. Invoke when the user says "on-call", "triage this alert", "handle this pager",
  "an Airflow DAG failed", "pipeline broke", "what's this alert", or drops an alert log in on-call/.
---

# /oncall — triage an alert and make the runbook smarter

The on-call bookend to `/frame` and `/capture`. `/frame` opens a ticket; `/oncall` opens **and closes** an
incident. It reads `on-call/oncall_runbook.md`, does the triage, and **guarantees the write-back** — the
step humans forget (proof: the fangorn alert sat un-logged in `on-call/` until INC-002). Every resolution
makes the next identical alert instant.

**Usage:** `/oncall` (uses the newest alert log in `on-call/`), or `/oncall <path-to-alert-log>`, or
`/oncall <pasted alert text>`.

**Prerequisites:** `gcloud auth login` (reauth if `Reauthentication required` — do it as ONE call, parallel
gsutil trips the reauth quota). Read access to the alert's Airflow/Astronomer task log.

---

## Step 0 — Classify: on-call alert, or a ticket?

Read `on-call/oncall_runbook.md` §0. Apply the decision rule:

> _Did an alert/pager fire and is a pipeline currently degraded?_
> **yes → on-call:** continue this skill.
> **no → it's a question/change:** STOP. This is ticket work — hand off to `/frame <KEY>`, write to
> `tickets/`, not the runbook. Tell the user you're redirecting and why.

Do not create an INC entry for something that isn't an operational alert. Keeping the incident log pure is
what makes "how often does X page?" answerable.

---

## Step 1 — Match the catalog (fast path)

`grep` the DAG/task key in `on-call/oncall_runbook.md` §2 (and `knowledge/_ROUTING.md` for the symptom —
`sensor timeout`, `dataproc`, the DAG name). **If a catalog row matches, jump straight to its `Protocol`
(the linked INC's decision tree) and skip to Step 4.** That decision tree already encodes the answer.

If no match, this is a NEW alert → continue to Step 2.

---

## Step 2 — Triage (runbook §1)

1. **Identify** DAG + task + logical date from the alert header.
2. **Pull the task log**, find what the task is *actually doing* (not just that it failed):
   - **Sensor** → its poke target (`Sensor checks existence of : <bucket>, <object>`).
   - **Producer / Spark / BQ / Vertex** → the output path / query / the real exception. Search the log
     tail for `ERROR` / `Exception` / `Traceback` / `code:` — skip the boilerplate.
3. **Check empirical state** — did the awaited/produced object actually land?
   `gcloud storage ls -l "gs://<bucket>/<path>/"` (one call). For BQ outputs, a COUNT via `bq_run.sh`.

---

## Step 3 — Classify the verdict (runbook §1 taxonomy)

`benign_expected` · `late_data` · `transient_infra` · `resource_contention` · `real_upstream_failure` · `dag_bug`.
Pick from the evidence, not a guess. If you can't reach ground truth, say so and mark the incident
**OBSERVED** (not RESOLVED) with your working hypothesis; flip to RESOLVED only once the cause is verified.
(INC-002 was OBSERVED as `transient_infra` until the owner root-caused it as `resource_contention` — RESOLVED.)

---

## Step 4 — Act (never hot-patch prod)

| Verdict | Action |
|---|---|
| `benign_expected` | Ack. Reply in the alert thread "expected, <reason>". No re-run. |
| `late_data` | Clear the failed task → it passes on the object that's now present. |
| `transient_infra` | Re-run the task once. Recurs → check quota/region capacity, then route to owner. |
| `resource_contention` | Do NOT blind-re-run. Confirm no concurrent job holds the resource, let it FINISH, then re-trigger. Recurs → durable fix to `improvements_backlog.md`. |
| `real_upstream_failure` | Re-run the producer (mind the **batch-id trap**), or route to the feed/vendor owner. |
| `dag_bug` | Route to the owning team with evidence. **Do NOT edit the DAG.** |

**Prod-safety gate (non-negotiable):** widening a timeout, soft-failing a sensor, or editing a DAG is a
code change owned by the producing team. Diagnose → clear/re-run or route. If a durable code fix is
warranted, **log it to `improvements_backlog.md`** (one row, `Status: idea`, `Ref: on-call INC-NNN`) — do
NOT open a Jira ticket by reflex (keeps the board clean); promote to Jira only when it's prioritized. Don't
do the code fix here.

---

## Step 5 — Write back (the enforced step — do ALL THREE, then rebuild)

This is the point of the skill. An incident isn't done until it's logged on every surface:

1. **§3 Incident log** — append `### INC-NNN — <dag> <task> — <one-line>` with: date, alert line, STATUS
   (RESOLVED / OBSERVED), verdict + why, the diagnosis commands (copy-paste-able), and a **decision tree
   for next time**. This is the durable knowledge — write it so the next on-call needs nothing else.
2. **§2 Known-Alert Catalog** — add one row: `DAG/task key | signature | root cause | verdict | INC-NNN`.
3. **`on-call/incident_log.jsonl`** — append one record (shape in runbook §5):
   `{"inc":"INC-NNN","date":"YYYY-MM-DD","dag":"…","task":"…","team":"…","signature":"…","verdict":"…","action":"…","resolved":true|false,"ticket":null|"TI-XXX","ref":"§3 INC-NNN"}`
4. **Rebuild the index** so the new keywords route: `.claude/scripts/build_index.sh`. Add any new symptom
   words (DAG name, error string) to the runbook front-matter `keywords:` first if they'd help future grep.
5. **Post the thread reply** (terse, BLUF) if the team channel expects an ack. Lint via
   `.claude/scripts/lint_comms.py --kind comment` if it's a Jira/Slack write.
6. **Commit + push** (`on-call: INC-NNN <dag> — <verdict>`).

If the incident surfaced a durable fix, add a row to `improvements_backlog.md` (not Jira, unless
prioritized). If it did become a Jira ticket, note the key in §3 and the JSONL `ticket` field.

---

## Guardrails
- **Prod safety** (`[[feedback_airflow_prod_safety]]`): never modify prod DAGs / push `main` to silence an alert.
- **Never delete** a runbook row — a "benign, expected" verdict is as valuable as a fix.
- **Honesty over closure:** if you didn't verify root cause, STATUS is OBSERVED, not RESOLVED. Leave an
  "Open (for the next on-call to close)" note.
- **One reauth call:** parallel gsutil trips the `gcloud` reauth quota.

#!/usr/bin/env python3
"""airflow_api.py — stdlib-only Astro (Airflow 3.x) REST client for the on-call log puller.

Driven by airflow_pull.sh; can also be run directly. Three subcommands:
  version   auth/connectivity smoke test (GET /api/v2/version)
  list      download every task-instance log that ran on a day → renamed .log files + manifest
  watch     poll task states; on each terminal transition, download that task's log and (on failure)
            drop the renamed log into on-call/ so the existing triage hook + /oncall pick it up

Auth: a bearer token, resolved in this order — --token, $AIRFLOW_BEARER, then the active `astro`
CLI context in ~/.astro/config.yaml. No secret is stored by this tool.

Airflow-3 specifics baked in (see plan / research brief):
  - base path is /api/v2; the "all tasks for a day" query is POST /dags/~/dagRuns/~/taskInstances/list
    windowed on start_date (NOT logical_date, which is nullable for asset/manual runs)
  - logs come back as structured JSON {content, continuation_token} or NDJSON, never plaintext —
    both are flattened to text here
  - terminal states: success, failed, upstream_failed, skipped, removed
  - the SSO token expires ~1h, so a 401 mid-run triggers ONE automatic renewal (any `astro` CLI
    call re-mints it on disk) and a retry; an explicitly supplied token is never replaced
"""

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta

TERMINAL_STATES = {"success", "failed", "upstream_failed", "skipped", "removed"}
FAILURE_STATES = {"failed", "upstream_failed"}
PAGE_LIMIT = 100
HTTP_TIMEOUT = 60
HOME = os.path.expanduser("~")
# repo root = .../workspace (this file is .claude/scripts/airflow_api.py)
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# --------------------------------------------------------------------------- auth
# The astro SSO token expires ~1h, so a long poller 401s mid-run. An explicitly supplied token
# (--token / $AIRFLOW_BEARER, e.g. a Deployment API token) is NOT renewable here and must not be
# replaced by whatever happens to be in the astro context.
_RENEWABLE = False
_TOKEN = None


def resolve_bearer(explicit=None):
    """Return a bearer token (no 'Bearer ' prefix) from --token, env, or the astro context."""
    global _RENEWABLE, _TOKEN
    tok = explicit or os.environ.get("AIRFLOW_BEARER")
    if tok:
        _RENEWABLE, _TOKEN = False, tok.strip().removeprefix("Bearer ").strip()
        return _TOKEN
    tok = _token_from_astro_config()
    if tok:
        _RENEWABLE, _TOKEN = True, tok
        return tok
    sys.exit(
        "airflow_api: no bearer token. Run `astro login`, or export AIRFLOW_BEARER=<token>.\n"
        "  (checked --token, $AIRFLOW_BEARER, and ~/.astro/config.yaml active context)"
    )


def _token_from_astro_config():
    """Parse the active-context token out of ~/.astro/config.yaml (no pyyaml in stdlib).

    Astro writes a top-level `context: <domain>` (the active one, e.g. `astronomer.io`) and a
    `contexts:` map whose KEYS replace dots with underscores (e.g. `astronomer_io`), each block
    carrying a `token:` line. The domain header is the shallowest-indent key inside `contexts:`;
    its fields (token, expiresin, …) are deeper. We match the active domain to its key by
    normalizing dots/underscores, and fall back to the only/first token if matching fails.
    """
    path = os.path.join(HOME, ".astro", "config.yaml")
    try:
        with open(path, encoding="utf-8") as f:
            lines = f.readlines()
    except OSError:
        return None

    active = None
    for ln in lines:
        m = re.match(r"^context:\s*(\S+)\s*$", ln)
        if m:
            active = m.group(1).strip().strip('"')
            break

    tokens = {}  # domain-key -> token
    first_token = None
    in_contexts = False
    cur_domain = None
    domain_indent = None
    for ln in lines:
        if re.match(r"^contexts:\s*$", ln):
            in_contexts = True
            continue
        if not in_contexts:
            continue
        if not ln.strip() or ln.lstrip().startswith("#"):
            continue
        indent = len(ln) - len(ln.lstrip(" "))
        if indent == 0:  # a new top-level key ends the contexts block
            break
        m = re.match(r"^\s+([^:\s]+):\s*(.*)$", ln)
        if not m:
            continue
        key, val = m.group(1), m.group(2).strip().strip('"')
        if domain_indent is None:
            domain_indent = indent
        if indent == domain_indent:  # domain header (shallowest key under contexts:)
            cur_domain = key
            continue
        if key == "token" and val:
            token = val.removeprefix("Bearer ").strip()
            if cur_domain:
                tokens[cur_domain] = token
            if first_token is None:
                first_token = token

    if active:
        for cand in (active, active.replace(".", "_"), active.replace("_", ".")):
            if cand in tokens:
                return tokens[cand]
    return first_token


# --------------------------------------------------------------------------- http
def renew_astro_token():
    """Refresh the on-disk astro token and return it if it actually changed.

    Any `astro` CLI call re-mints the SSO token into ~/.astro/config.yaml, so the cheapest
    read doubles as the refresh. Returns None when the token is not renewable (explicitly
    supplied), the CLI is missing, or the value did not move — so a genuinely revoked
    credential fails once instead of looping.
    """
    global _TOKEN
    if not _RENEWABLE:
        return None
    try:
        subprocess.run(["astro", "deployment", "list"], capture_output=True, text=True, timeout=60)
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return None
    fresh = _token_from_astro_config()
    if fresh and fresh != _TOKEN:
        _TOKEN = fresh
        return fresh
    return None


def _request(method, url, token, body=None, accept="application/json", retries=3):
    data = json.dumps(body).encode() if body is not None else None
    # A renewal updates the module token; callers still hold the stale one they were given.
    token = _TOKEN or token
    headers = {"Authorization": f"Bearer {token}", "Accept": accept}
    if data is not None:
        headers["Content-Type"] = "application/json"
    last = None
    renewed = False
    for attempt in range(retries):
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            payload = e.read()
            if e.code == 401 and not renewed:
                renewed = True  # one renewal per request, whatever the retry budget
                fresh = renew_astro_token()
                if fresh:
                    headers["Authorization"] = f"Bearer {fresh}"
                    continue
                return e.code, payload
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                time.sleep(2**attempt)
                last = (e.code, payload)
                continue
            return e.code, payload
        except urllib.error.URLError as e:
            last = (0, str(e).encode())
            if attempt < retries - 1:
                time.sleep(2**attempt)
                continue
    return last if last else (0, b"request failed")


def _get_json(base, token, path, params=None):
    url = base + path
    if params:
        url += "?" + urllib.parse.urlencode(params, doseq=True)
    status, payload = _request("GET", url, token)
    return status, _loads(payload)


def _post_json(base, token, path, body):
    status, payload = _request("POST", base + path, token, body=body)
    return status, _loads(payload)


def _loads(payload):
    try:
        return json.loads(payload)
    except (ValueError, TypeError):
        return {"_raw": payload.decode("utf-8", "replace")}


def _die_on_status(status, obj, what):
    if status == 200:
        return
    if status in (401, 403):
        sys.exit(
            f"airflow_api: auth failed (HTTP {status}) on {what} after an automatic token renewal."
            " Run `astro login` (SSO), or export AIRFLOW_BEARER=<deployment api token>."
        )
    detail = obj.get("detail") or obj.get("_raw") or obj if isinstance(obj, dict) else obj
    sys.exit(f"airflow_api: {what} failed (HTTP {status}): {str(detail)[:300]}")


# --------------------------------------------------------------------------- api
def get_version(base, token):
    status, obj = _get_json(base, token, "/version")
    return status, obj


def resolve_tag_dags(base, token, tag):
    dag_ids, offset = [], 0
    while True:
        status, obj = _get_json(
            base,
            token,
            "/dags",
            {"tags": tag, "tags_match_mode": "any", "limit": PAGE_LIMIT, "offset": offset},
        )
        _die_on_status(status, obj, f"list dags by tag '{tag}'")
        dags = obj.get("dags", [])
        dag_ids += [d["dag_id"] for d in dags]
        total = obj.get("total_entries", len(dag_ids))
        offset += len(dags)
        if not dags or offset >= total:
            break
    return dag_ids


def list_task_instances_for_day(base, token, start_iso, end_iso, dag_ids=None, states=None):
    """POST /dags/~/dagRuns/~/taskInstances/list windowed on start_date. Returns all pages."""
    out, offset = [], 0
    while True:
        body = {
            "start_date_gte": start_iso,
            "start_date_lte": end_iso,
            "page_limit": PAGE_LIMIT,
            "page_offset": offset,
            "order_by": "start_date",
        }
        if dag_ids:
            body["dag_ids"] = dag_ids
        if states:
            body["state"] = states
        status, obj = _post_json(base, token, "/dags/~/dagRuns/~/taskInstances/list", body)
        _die_on_status(status, obj, "list task instances for day")
        tis = obj.get("task_instances", [])
        out += tis
        total = obj.get("total_entries", len(out))
        offset += len(tis)
        if not tis or offset >= total:
            break
    return out


def list_runs_for_day(base, token, dag_id, start_iso, end_iso):
    out, offset = [], 0
    while True:
        status, obj = _get_json(
            base,
            token,
            f"/dags/{urllib.parse.quote(dag_id)}/dagRuns",
            {
                "run_after_gte": start_iso,
                "run_after_lte": end_iso,
                "limit": PAGE_LIMIT,
                "offset": offset,
                "order_by": "run_after",
            },
        )
        _die_on_status(status, obj, f"list dag runs for {dag_id}")
        runs = obj.get("dag_runs", [])
        out += runs
        total = obj.get("total_entries", len(out))
        offset += len(runs)
        if not runs or offset >= total:
            break
    return out


def list_task_instances_in_run(base, token, dag_id, run_id):
    out, offset = [], 0
    dp, rp = urllib.parse.quote(dag_id), urllib.parse.quote(run_id)
    while True:
        status, obj = _get_json(
            base,
            token,
            f"/dags/{dp}/dagRuns/{rp}/taskInstances",
            {"limit": PAGE_LIMIT, "offset": offset},
        )
        _die_on_status(status, obj, f"list task instances in {dag_id}/{run_id}")
        tis = obj.get("task_instances", [])
        out += tis
        total = obj.get("total_entries", len(out))
        offset += len(tis)
        if not tis or offset >= total:
            break
    return out


def list_task_instances_for_task(base, token, dag_id, task_id, limit=100):
    """Recent instances of ONE task across runs, newest first, for a runtime trend."""
    dp = urllib.parse.quote(dag_id)
    status, obj = _get_json(
        base,
        token,
        f"/dags/{dp}/dagRuns/~/taskInstances",
        {"limit": limit, "task_id": task_id, "order_by": "-start_date"},
    )
    if status != 200:
        return []
    rows = obj.get("task_instances", [])
    return [t for t in rows if t.get("task_id") == task_id]


def dag_task_graph(base, token, dag_id):
    """task_id -> its downstream task ids, so an upstream claim can be proved instead of guessed."""
    dp = urllib.parse.quote(dag_id)
    status, obj = _get_json(base, token, f"/dags/{dp}/tasks")
    if status != 200:
        return {}
    return {
        t["task_id"]: list(t.get("downstream_task_ids") or [])
        for t in obj.get("tasks", [])
        if t.get("task_id")
    }


def _ti_key(ti):
    """Identity of one task instance across queries (a mapped task needs map_index)."""
    return (ti.get("dag_id"), ti.get("dag_run_id"), ti.get("task_id"), ti.get("map_index", -1))


def expand_tries(base, token, ti):
    """Return one ti-like dict per try (accurate try_number/state/start_date), newest last.

    Uses GET .../taskInstances/{task}/tries (or /{map_index}/tries for mapped tasks). Each try dict is
    merged over the parent ti so dag_id/dag_run_id/task_id/map_index are always present. Falls back to
    the single current ti if the endpoint is unavailable.
    """
    dp = urllib.parse.quote(ti["dag_id"])
    rp = urllib.parse.quote(ti["dag_run_id"])
    tp = urllib.parse.quote(ti["task_id"])
    mi = ti.get("map_index", -1)
    base_path = f"/dags/{dp}/dagRuns/{rp}/taskInstances/{tp}"
    path = f"{base_path}/{mi}/tries" if mi is not None and mi >= 0 else f"{base_path}/tries"
    status, obj = _get_json(base, token, path)
    if status != 200:
        return [ti]
    tries = obj.get("task_instances", [])
    return [{**ti, **t} for t in tries] or [ti]


def fetch_log(base, token, ti):
    """Fetch and flatten the log for a task instance's current try. Handles NDJSON + JSON+token."""
    dag_id, run_id, task_id = ti["dag_id"], ti["dag_run_id"], ti["task_id"]
    try_number = ti.get("try_number") or 1
    map_index = ti.get("map_index", -1)
    dp, rp, tp = urllib.parse.quote(dag_id), urllib.parse.quote(run_id), urllib.parse.quote(task_id)
    path = f"/dags/{dp}/dagRuns/{rp}/taskInstances/{tp}/logs/{try_number}"

    # Prefer the NDJSON stream (whole log in one call). Fall back to JSON+continuation_token.
    params = {"full_content": "true"}
    if map_index is not None and map_index >= 0:
        params["map_index"] = map_index
    url = base + path + "?" + urllib.parse.urlencode(params)
    status, payload = _request("GET", url, token, accept="application/x-ndjson")
    if status != 200:
        return f"[airflow_api: log fetch HTTP {status}]\n{payload.decode('utf-8', 'replace')[:500]}"

    text = payload.decode("utf-8", "replace")
    # NDJSON path: many JSON objects, one per line (and not a single JSON doc).
    stripped = text.lstrip()
    if stripped.startswith("{") and '"content"' in text[:200]:
        return _flatten_json_log(base, token, url, token_seed=None, first=text)
    return _flatten_ndjson(text)


def _flatten_ndjson(text):
    lines = []
    for raw in text.splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except ValueError:
            lines.append(raw)
            continue
        lines.append(_render_event(obj))
    return "\n".join(lines) + "\n"


def _flatten_json_log(base, token, url, token_seed, first):
    """JSON shape: {content: [...], continuation_token}. Page until the token stops advancing."""
    out, seen_tokens = [], set()
    body_text = first
    while True:
        try:
            obj = json.loads(body_text)
        except ValueError:
            out.append(body_text)
            break
        out.append(_render_content(obj.get("content")))
        cont = obj.get("continuation_token")
        if not cont or cont in seen_tokens:
            break
        seen_tokens.add(cont)
        nxt = url + ("&" if "?" in url else "?") + urllib.parse.urlencode({"token": cont})
        status, payload = _request("GET", nxt, token, accept="application/json")
        if status != 200:
            break
        body_text = payload.decode("utf-8", "replace")
    return "\n".join(x for x in out if x) + "\n"


def _render_content(content):
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    parts = []
    for item in content:
        if isinstance(item, (list, tuple)):
            # older [source, message] pairs
            parts.append(" ".join(str(x) for x in item))
        elif isinstance(item, dict):
            parts.append(_render_event(item))
        else:
            parts.append(str(item))
    return "\n".join(parts)


def _render_event(obj):
    if not isinstance(obj, dict):
        return str(obj)
    ts = obj.get("timestamp") or obj.get("asctime") or ""
    level = obj.get("level") or obj.get("levelname") or ""
    logger = obj.get("logger") or obj.get("name") or ""
    msg = obj.get("event") or obj.get("message") or obj.get("msg")
    if msg is None:
        return json.dumps(obj, ensure_ascii=False)
    head = " ".join(x for x in (str(ts), f"[{level}]" if level else "", logger) if x).strip()
    return f"{head} {msg}".strip() if head else str(msg)


# --------------------------------------------------------------------------- files
def _safe(s):
    return re.sub(r"[^A-Za-z0-9._-]+", "-", str(s)).strip("-") or "na"


def _hhmmss(ti):
    sd = ti.get("start_date")
    if not sd:
        return "000000"
    try:
        dt = datetime.fromisoformat(sd.replace("Z", "+00:00"))
        return dt.strftime("%H%M%S")
    except ValueError:
        return "000000"


def log_filename(ti):
    parts = [_hhmmss(ti), _safe(ti.get("dag_id")), _safe(ti.get("task_id"))]
    if ti.get("map_index", -1) is not None and ti.get("map_index", -1) >= 0:
        parts.append(f"map{ti['map_index']}")
    parts.append(f"try{ti.get('try_number') or 1}")
    parts.append(_safe(ti.get("state") or "none"))
    return "__".join(parts) + ".log"


def manifest_record(ti, log_path):
    def dur(ti):
        d = ti.get("duration")
        return round(d, 1) if isinstance(d, (int, float)) else None

    return {
        "dag": ti.get("dag_id"),
        "task": ti.get("task_id"),
        "run_id": ti.get("dag_run_id"),
        "map_index": ti.get("map_index", -1),
        "state": ti.get("state"),
        "try": ti.get("try_number"),
        "start": ti.get("start_date"),
        "end": ti.get("end_date"),
        "duration_s": dur(ti),
        "operator": ti.get("operator"),
        "log_path": log_path,
    }


# --------------------------------------------------------------------------- day window
def day_window(date_str):
    # Format literal UTC-day boundary strings; no tzinfo object needed (keeps this 3.9-compatible
    # for the system python3 a bash subprocess resolves, and gives ruff nothing to "upgrade").
    d = datetime.strptime(date_str, "%Y-%m-%d")
    start = d.strftime("%Y-%m-%dT00:00:00Z")
    end = (d + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    return start, end


# --------------------------------------------------------------------------- commands
def cmd_version(args):
    token = resolve_bearer(args.token)
    status, obj = get_version(args.base, token)
    if status != 200:
        print(f"AUTH FAIL (HTTP {status}): {str(obj)[:300]}", file=sys.stderr)
        return 1
    print(json.dumps(obj))
    return 0


def cmd_list(args):
    token = resolve_bearer(args.token)
    start_iso, end_iso = day_window(args.date)
    dag_ids = None
    if args.tag:
        dag_ids = resolve_tag_dags(args.base, token, args.tag)
        if not dag_ids:
            print(f"[airflow_api] no DAGs carry tag '{args.tag}'", file=sys.stderr)
            return 0
    elif args.dag:
        dag_ids = [args.dag]
    states = args.state or None

    tis = list_task_instances_for_day(args.base, token, start_iso, end_iso, dag_ids, states)
    recovered = []
    if states and getattr(args, "include_recovered", False):
        # `state=failed` is the state AT PULL TIME, so a task that failed and then retried or was
        # cleared to success is invisible to it - which is exactly the incidents that got resolved.
        # try_number > 1 means it failed at least once, whatever it ended as.
        every = list_task_instances_for_day(args.base, token, start_iso, end_iso, dag_ids, None)
        seen = {_ti_key(t) for t in tis}
        recovered = [
            t
            for t in every
            if (t.get("try_number") or 0) > 1
            and t.get("state") not in FAILURE_STATES
            and _ti_key(t) not in seen
        ]
        tis = tis + recovered
    outdir = os.path.join(args.outdir, args.date)
    os.makedirs(outdir, exist_ok=True)
    manifest_path = os.path.join(outdir, "_manifest.jsonl")

    counts = {
        "total": len(tis),
        "failed": 0,
        "running": 0,
        "logs": 0,
        "recovered": len(recovered),
    }
    recovered_keys = {_ti_key(t) for t in recovered}
    failed_rows = []
    with open(manifest_path, "w", encoding="utf-8") as mf:
        for ti in tis:
            state = ti.get("state")
            if state in FAILURE_STATES:
                counts["failed"] += 1
                failed_rows.append(
                    f"  FAIL {ti.get('dag_id')}.{ti.get('task_id')} (try {ti.get('try_number')})"
                )
            elif state in (None, "running", "queued", "scheduled", "deferred", "up_for_retry"):
                counts["running"] += 1
            # one log per try with --all-tries (failed retries hold the cause), else the current try
            # only. A recovered task is always expanded: its final try is green and says nothing.
            is_recovered = _ti_key(ti) in recovered_keys
            work = expand_tries(args.base, token, ti) if (args.all_tries or is_recovered) else [ti]
            for wti in work:
                text = fetch_log(args.base, token, wti)
                fname = log_filename(wti)
                with open(os.path.join(outdir, fname), "w", encoding="utf-8") as lf:
                    lf.write(text)
                rel = os.path.join(args.outdir, args.date, fname)
                mf.write(json.dumps(manifest_record(wti, rel)) + "\n")
                counts["logs"] += 1
            if getattr(args, "diagnose", False):
                target = None
                if state in FAILURE_STATES:
                    target = ti
                elif is_recovered:
                    # diagnose the try that actually failed, not the green one that followed
                    failed_tries = [w for w in work if w.get("state") in FAILURE_STATES]
                    target = failed_tries[-1] if failed_tries else None
                if target is not None:
                    _run_diagnosis(os.path.join(outdir, log_filename(target)), args.diagnose_cmd)

    extra = f" · {counts['logs']} logs (all tries)" if args.all_tries else ""
    if counts["recovered"]:
        extra += f" · {counts['recovered']} failed-then-recovered"
    print(
        f"{counts['total']} tasks · {counts['failed']} failed · {counts['running']} in-flight{extra}",
        file=sys.stderr,
    )
    print(f"manifest: {manifest_path}", file=sys.stderr)
    for row in failed_rows:
        print(row, file=sys.stderr)
    return 0


def cmd_watch(args):
    token = resolve_bearer(args.token)
    date = args.date
    start_iso, end_iso = day_window(date)
    outdir = os.path.join(args.outdir, date)
    os.makedirs(outdir, exist_ok=True)
    manifest_path = os.path.join(outdir, "_manifest.jsonl")
    oncall_dir = args.oncall_dir

    if args.tag:
        dag_ids = resolve_tag_dags(args.base, token, args.tag)
    elif args.dag:
        dag_ids = [args.dag]
    else:
        print("[airflow_api] watch requires --tag or --dag", file=sys.stderr)
        return 2
    if not dag_ids:
        print(f"[airflow_api] no DAGs to watch (tag '{args.tag}')", file=sys.stderr)
        return 0

    seen_terminal = set()  # (dag, run, task, map_index, try_number)
    idle_rounds = 0
    while True:
        any_run = False
        all_terminal = True
        for dag_id in dag_ids:
            for run in list_runs_for_day(args.base, token, dag_id, start_iso, end_iso):
                any_run = True
                for ti in list_task_instances_in_run(args.base, token, dag_id, run["dag_run_id"]):
                    state = ti.get("state")
                    if state not in TERMINAL_STATES:
                        all_terminal = False
                        continue
                    key = (
                        dag_id,
                        ti.get("dag_run_id"),
                        ti.get("task_id"),
                        ti.get("map_index", -1),
                        ti.get("try_number"),
                    )
                    if key in seen_terminal:
                        continue
                    seen_terminal.add(key)
                    _emit_completion(
                        args.base,
                        token,
                        ti,
                        outdir,
                        manifest_path,
                        oncall_dir,
                        args.all_tries,
                        args.diagnose,
                        args.diagnose_cmd,
                    )
        sys.stderr.flush()
        sys.stdout.flush()
        if any_run and all_terminal and not args.persistent:
            print("WATCH DONE: all watched tasks terminal", flush=True)
            return 0
        idle_rounds = idle_rounds + 1 if not any_run else 0
        if idle_rounds > (args.max_idle_rounds or 0) > 0:
            print("WATCH DONE: no runs found (idle)", flush=True)
            return 0
        time.sleep(args.interval)


def _run_diagnosis(drop_path, diagnose_cmd):
    """Shell out to the RCA orchestrator on a dropped failure log; write <drop>.rca.md.

    Loose coupling by design: this fetcher does not import the debugger, it only
    optionally runs a configured command (default: airflow_debugger.orchestrate).
    """
    cmd = shlex.split(diagnose_cmd) + [drop_path]
    try:
        out = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=300)
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        print(f"  RCA skipped ({type(e).__name__})", flush=True)
        return
    report = out.stdout.strip() or (out.stderr or "").strip()[:500]
    if not report:
        return
    rca_path = drop_path + ".rca.md"
    with open(rca_path, "w", encoding="utf-8") as f:
        f.write(report + "\n")
    print(f"  RCA -> {rca_path}\n  {report.splitlines()[0]}", flush=True)


def _emit_completion(
    base,
    token,
    ti,
    outdir,
    manifest_path,
    oncall_dir,
    all_tries=False,
    diagnose=False,
    diagnose_cmd=None,
):
    state = ti.get("state")
    text = fetch_log(base, token, ti)
    fname = log_filename(ti)
    # the terminal try, plus every prior try when --all-tries (failed retries hold the cause)
    written = [(ti, fname, text)]
    if all_tries:
        cur = ti.get("try_number")
        for wti in expand_tries(base, token, ti):
            if wti.get("try_number") == cur:
                continue
            written.append((wti, log_filename(wti), fetch_log(base, token, wti)))
    with open(manifest_path, "a", encoding="utf-8") as mf:
        for wti, wf, wtext in written:
            with open(os.path.join(outdir, wf), "w", encoding="utf-8") as lf:
                lf.write(wtext)
            mf.write(json.dumps(manifest_record(wti, os.path.join(outdir, wf))) + "\n")
    dur = ti.get("duration")
    dur_s = f"{int(dur)}s" if isinstance(dur, (int, float)) else "?"
    tag = f"{ti.get('dag_id')}.{ti.get('task_id')}"
    if state in FAILURE_STATES and oncall_dir:
        os.makedirs(oncall_dir, exist_ok=True)
        drop = os.path.join(oncall_dir, fname)
        with open(drop, "w", encoding="utf-8") as df:
            df.write(text)
        print(f"FAIL {tag} {state} {dur_s} -> {drop}", flush=True)
        if diagnose:
            _run_diagnosis(drop, diagnose_cmd)
    else:
        marker = "OK" if state == "success" else state.upper()
        print(f"{marker} {tag} {state} {dur_s}", flush=True)


# --------------------------------------------------------------------------- cli
def build_parser():
    p = argparse.ArgumentParser(description="Astro/Airflow-3 task-log puller + completion sensor")
    p.add_argument("--base", required=True, help="Airflow API base URL ending in /api/v2")
    p.add_argument("--token", help="bearer token (else $AIRFLOW_BEARER or ~/.astro context)")
    sub = p.add_subparsers(dest="cmd", required=True)

    v = sub.add_parser("version", help="auth/connectivity smoke test")
    v.set_defaults(func=cmd_version)

    ls = sub.add_parser("list", help="download all task logs for a day")
    ls.add_argument("--date", required=True, help="UTC day YYYY-MM-DD")
    ls.add_argument("--dag", help="restrict to one dag_id")
    ls.add_argument("--tag", help="restrict to DAGs carrying this Airflow tag")
    ls.add_argument("--state", action="append", help="restrict to these states (repeatable)")
    ls.add_argument("--outdir", required=True, help="output dir root (date subdir is added)")
    ls.add_argument(
        "--include-recovered",
        action="store_true",
        help="with --state: also pull tasks that failed then retried/were cleared to success "
        "(try_number > 1). --state is state AT PULL TIME, so these are otherwise invisible.",
    )
    ls.add_argument(
        "--all-tries",
        dest="all_tries",
        action="store_true",
        help="download every try (1..N), not just the latest — failed retries hold the cause",
    )
    ls.add_argument(
        "--diagnose",
        action="store_true",
        help="run the RCA orchestrator on each failed task's log and write <log>.rca.md",
    )
    ls.add_argument(
        "--diagnose-cmd",
        dest="diagnose_cmd",
        default="python3 -m airflow_debugger.orchestrate --no-llm",
        help="command the failure log path is appended to (default: deterministic RCA, no LLM cost)",
    )
    ls.set_defaults(func=cmd_list)

    w = sub.add_parser("watch", help="poll states; emit + download on each terminal transition")
    w.add_argument("--date", required=True, help="UTC day YYYY-MM-DD to watch")
    w.add_argument("--dag", help="watch one dag_id")
    w.add_argument("--tag", help="watch all DAGs carrying this Airflow tag")
    w.add_argument("--outdir", required=True, help="output dir root (date subdir is added)")
    w.add_argument("--oncall-dir", dest="oncall_dir", help="drop failed logs here for /oncall")
    w.add_argument("--interval", type=int, default=30, help="poll seconds")
    w.add_argument("--persistent", action="store_true", help="never self-exit (run under Monitor)")
    w.add_argument(
        "--max-idle-rounds",
        dest="max_idle_rounds",
        type=int,
        default=0,
        help="exit after N polls with no runs found (0 = never)",
    )
    w.add_argument(
        "--all-tries",
        dest="all_tries",
        action="store_true",
        help="on each completion, download every try (1..N), not just the terminal one",
    )
    w.add_argument(
        "--diagnose",
        action="store_true",
        help="on each failure drop, run the RCA orchestrator and write <log>.rca.md for /oncall",
    )
    w.add_argument(
        "--diagnose-cmd",
        dest="diagnose_cmd",
        default="python3 -m airflow_debugger.orchestrate --no-llm",
        help="command the failure log path is appended to (default: deterministic RCA, no LLM cost)",
    )
    w.set_defaults(func=cmd_watch)
    return p


def main():
    args = build_parser().parse_args()
    args.base = args.base.rstrip("/")
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()

"""Append-only ledger of optimizer findings, so a sweep can say what CHANGED.

A backlog regenerated from scratch each day cannot answer the three questions that
decide whether anyone acts: how long has this been true, is it new, and did the fix
work. The ledger keys every finding by (dag_id, detector, stage) and replays the
history to derive a state:

    new             first sweep this key appears
    chronic         same key on RESOLVE_SWEEPS+ consecutive sweeps
    owner_notified  a person sent the ask (set by hand, survives replay)
    wont_fix        owner declined, with a reason (set by hand, survives replay)
    resolved        the key stopped firing for RESOLVE_SWEEPS sweeps
    applied         a fix shipped for this key (records the PR); the ledger then WATCHES
    fix_not_working the key is still firing RESOLVE_SWEEPS sweeps after the fix shipped

`owner_notified` and `wont_fix` are sticky because they record a human decision;
everything else is recomputed from what the detectors saw.

`applied` is deliberately NOT sticky. Recording that a fix shipped is the point of the
register, but a merged fix is not a verified fix: the ledger keeps carrying `fix_pr` and
`applied_date` forward and lets the detectors decide what happened next. The key going
quiet is the win; the key still firing after the grace window is `fix_not_working`.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass, field

LEDGER = os.environ.get("OPTIMIZER_LEDGER",
                        os.path.join("optimizer_out", "optimization_ledger.jsonl"))
RESOLVE_SWEEPS = 3  # consecutive sweeps without a key before it counts as resolved
STICKY = ("owner_notified", "wont_fix")
HUMAN = (*STICKY, "applied")  # states a person sets by hand


@dataclass
class Entry:
    """One finding, on one sweep. The unit the ledger appends."""

    date: str
    dag_id: str
    app_id: str
    key: str  # "<detector>:<stage>" - stable across runs of the same job
    impact: str
    title: str
    fix: str = ""
    owner: str = ""
    dcu_h: float | None = None
    exec_h: float | None = None
    state: str = "new"
    streak: int = 1
    note: str = ""
    fix_pr: str = ""  # set when a fix ships; carried forward so the outcome is attributable
    applied_date: str = ""


def finding_key(finding: object) -> str:
    """Stable identity across runs: the detector, plus the stage when it is stage-scoped.

    Stage numbers are stable for a recurring job; task counts and byte totals are not, so
    the title's other digits must not leak into the key or every sweep looks new.
    """
    detector = getattr(finding, "key", "unknown")
    words = (getattr(finding, "title", "") or "").split()
    stage = ""
    for i, w in enumerate(words[:-1]):
        if w.lower() == "stage" and words[i + 1].isdigit():
            stage = words[i + 1]
            break
    return f"{detector}:{stage}" if stage else detector


def read(path: str = LEDGER) -> list[dict]:
    """Every entry ever written, oldest first. Missing file is an empty ledger."""
    if not os.path.exists(path):
        return []
    out = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue  # a torn last line must not sink the sweep
    return out


def _history(entries: list[dict]) -> dict[tuple[str, str], list[dict]]:
    """Group prior entries by (dag_id, key), preserving order."""
    hist: dict[tuple[str, str], list[dict]] = {}
    for e in entries:
        hist.setdefault((e.get("dag_id", ""), e.get("key", "")), []).append(e)
    return hist


def classify(new: list[Entry], prior: list[dict], date: str, complete: bool = True,
             exec_h_by_dag: dict | None = None) -> list[Entry]:
    """Set state and streak on this sweep's findings from what the ledger already holds.

    `complete=False` means this sweep did not see the whole fleet, so absence proves nothing
    and no key may be resolved from it. Resolution is the only inference drawn from a finding
    NOT appearing, which makes it the only thing a partial sweep can get catastrophically
    wrong: it would announce untouched jobs as fixed.
    """
    hist = _history(prior)
    seen_dates = sorted({e.get("date", "") for e in prior if e.get("date")})
    for entry in new:
        past = [e for e in hist.get((entry.dag_id, entry.key), []) if e.get("date") != date]
        if not past:
            entry.state, entry.streak = "new", 1
            continue
        last = past[-1]
        entry.streak = int(last.get("streak", 0)) + 1
        fix = next((e for e in reversed(past) if e.get("fix_pr")), None)
        if fix:  # attribution survives every later sweep, whatever the outcome
            entry.fix_pr = fix.get("fix_pr", "")
            entry.applied_date = fix.get("applied_date", "")
        sticky = next((e["state"] for e in reversed(past) if e.get("state") in STICKY), None)
        after_fix = [e for e in past if entry.applied_date and e.get("date", "") > entry.applied_date]
        if sticky:
            entry.state, entry.note = sticky, last.get("note", "")
        elif entry.applied_date and len(after_fix) + 1 >= RESOLVE_SWEEPS:
            # still firing well after the fix shipped: the fix did not do what it claimed
            entry.state = "fix_not_working"
            entry.note = f"still firing {len(after_fix) + 1} sweeps after {entry.fix_pr}"
        elif entry.streak >= RESOLVE_SWEEPS:
            entry.state = "chronic"
        else:
            entry.state = "recurring"
    if complete:
        _mark_resolved(new, hist, seen_dates, date, exec_h_by_dag or {})
    return new


def _mark_resolved(new: list[Entry], hist: dict, seen_dates: list[str], date: str,
                   exec_h_by_dag: dict | None = None) -> None:
    """Append a resolved entry, with the sweep's observed exec-hours, for each quiet key."""
    live = {(e.dag_id, e.key) for e in new}
    recent = set(seen_dates[-(RESOLVE_SWEEPS - 1):]) if seen_dates else set()
    for (dag_id, key), past in hist.items():
        if (dag_id, key) in live or not past:
            continue
        last = past[-1]
        if last.get("state") in ("resolved", "wont_fix"):
            continue
        if any(e.get("date") in recent for e in past):
            continue  # still inside the grace window
        pr = last.get("fix_pr", "")
        note = f"stopped firing after {last.get('date', 'an earlier sweep')}"
        if pr:
            note = f"cleared by {pr}; {note}"
        new.append(Entry(
            date=date, dag_id=dag_id, app_id="", key=key, impact=last.get("impact", ""),
            title=last.get("title", ""), owner=last.get("owner", ""),
            exec_h=(exec_h_by_dag or {}).get(dag_id),
            state="resolved", streak=0, note=note,
            fix_pr=pr, applied_date=last.get("applied_date", ""),
        ))


def append(entries: list[Entry], path: str = LEDGER) -> None:
    """Append this sweep's entries, replacing any rows already written for the same date.

    A task retry re-runs the whole sweep. Without this the same (date, dag, key) is written
    twice and every later streak counts the duplicate, so a retried day permanently inflates
    "how long has this been true" - the one number the ledger exists to answer.
    """
    same_day = {(e.date, e.dag_id, e.key) for e in entries}
    prior = [r for r in read(path)
             if (r.get("date"), r.get("dag_id"), r.get("key")) not in same_day]
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as fh:
        for r in prior:
            fh.write(json.dumps(r) + "\n")
        for e in entries:
            fh.write(json.dumps(asdict(e)) + "\n")


def record(reports: list, date: str, owners: dict | None = None, dcu: dict | None = None,
           path: str = LEDGER, known: set | None = None, complete: bool = True) -> list[Entry]:
    """Turn a crawl's JobReports into classified ledger entries and append them.

    `known` is the active-DAG set from the coverage pass; it disambiguates a trailing
    numeric suffix (run index vs data-source id) instead of guessing.

    `complete` says whether this sweep saw the whole fleet. A partial sweep still records what
    it found, but may not resolve anything.

    Every entry's `exec_h` is its dag's total for the sweep-day, summed across the dag's runs,
    so the savings before/after series compare the same measure.
    """
    owners, dcu = owners or {}, dcu or {}
    reports = [r for r in reports if not getattr(r, "error", None)]
    exec_h_by_dag: dict[str, float] = {}
    for r in reports:
        hours = round(getattr(r, "exec_h", 0.0), 1)
        if hours:
            dag = _dag_id(r, known)
            exec_h_by_dag[dag] = round(exec_h_by_dag.get(dag, 0.0) + hours, 1)
    entries = []
    for r in reports:
        if not getattr(r, "findings", None):
            continue
        dag_id = _dag_id(r, known)
        for f in r.findings:
            entries.append(Entry(
                date=date, dag_id=dag_id, app_id=getattr(r, "source", ""),
                key=finding_key(f), impact=getattr(f, "impact", ""),
                title=getattr(f, "title", ""), fix=getattr(f, "fix", ""),
                owner=owners.get(dag_id, ""),
                dcu_h=dcu.get(dag_id),
                exec_h=exec_h_by_dag.get(dag_id),
            ))
    entries = _dedup(entries)
    classify(entries, read(path), date, complete=complete, exec_h_by_dag=exec_h_by_dag)
    append(entries, path)
    return entries


def _dedup(entries: list[Entry]) -> list[Entry]:
    """One entry per (dag_id, key) per sweep - an hourly job contributes many logs."""
    best: dict[tuple[str, str], Entry] = {}
    rank = {"high": 3, "medium": 2, "low": 1}
    for e in entries:
        cur = best.get((e.dag_id, e.key))
        if cur is None or rank.get(e.impact, 0) > rank.get(cur.impact, 0):
            best[(e.dag_id, e.key)] = e
    return list(best.values())


# Per-run decorations Spark bakes into spark.app.name; left in, nothing is ever chronic.
_RUN_STAMP = re.compile(
    r"(?:"
    r"[-_]\d{4}-\d{2}-\d{2}(?:-\d+)*"    # -2026-08-20, -2026-08-20-1787259024
    r"|[-_]\d{8}-\d{6}(?:-\d+)*"          # -20260820-171500-1
    r"|[-_]?\s*\[\d+\]"                   # [19]
    r"|[-_]+$"
    r")+$"
)
# A trailing _<n> is a run index or a data-source id, so it is stripped only to find a match.
_TRAILING_INDEX = re.compile(r"_\d{1,3}$")


def _dag_id(report: object, known: set | None = None) -> str:
    """`Populate site_network_hourly.SiteNetworkHourly` -> `site_network_hourly`.

    Strips per-run stamps so an hourly job keeps ONE identity across sweeps. Derived from the
    report alone: a key the ledger replays cannot depend on a live enumeration. The result is a
    normalised job name, not a guaranteed Airflow dag_id - the digest only links it when it
    matches a DAG coverage saw.
    """
    name = getattr(report, "app_name", None) or getattr(report, "source", "")
    name = name.removeprefix("Populate ").strip()
    if "." in name:
        name = name.split(".")[0]
    name = _RUN_STAMP.sub("", name).rstrip("-_") or name
    if known and name not in known:
        stripped = _TRAILING_INDEX.sub("", name)
        if stripped != name and stripped in known:
            return stripped
    return name


def set_state(dag_id: str, key: str, state: str, note: str = "",
              date: str = "", path: str = LEDGER) -> Entry:
    """Record a human decision (owner_notified / wont_fix). Sticky across later sweeps."""
    if state not in STICKY:
        raise ValueError(f"set_state is for human decisions {STICKY}, not {state!r}")
    past = [e for e in read(path) if e.get("dag_id") == dag_id and e.get("key") == key]
    last = past[-1] if past else {}
    entry = Entry(
        date=date or last.get("date", ""), dag_id=dag_id, app_id=last.get("app_id", ""),
        key=key, impact=last.get("impact", ""), title=last.get("title", ""),
        owner=last.get("owner", ""), state=state, streak=int(last.get("streak", 1)), note=note,
    )
    append([entry], path)
    return entry


def mark_applied(dag_id: str, key: str, fix_pr: str, date: str, note: str = "",
                 path: str = LEDGER) -> Entry:
    """Record that a fix SHIPPED for this finding. The ledger then watches whether it worked."""
    if not fix_pr:
        raise ValueError("mark_applied needs the PR or commit that shipped the fix")
    past = [e for e in read(path) if e.get("dag_id") == dag_id and e.get("key") == key]
    if not past:
        raise ValueError(f"no ledger history for {dag_id}/{key}; nothing to mark applied")
    last = past[-1]
    entry = Entry(
        date=date, dag_id=dag_id, app_id=last.get("app_id", ""), key=key,
        impact=last.get("impact", ""), title=last.get("title", ""),
        owner=last.get("owner", ""), dcu_h=last.get("dcu_h"),
        exec_h=last.get("exec_h"),
        state="applied", streak=int(last.get("streak", 1)), note=note,
        fix_pr=fix_pr, applied_date=date,
    )
    append([entry], path)
    return entry


def shipped(path: str = LEDGER) -> list[dict]:
    """The register: one row per optimization that actually shipped, newest first.

    `outcome` is what the detectors saw AFTER the fix, not what the PR claimed:
    resolved = the finding stopped firing, fix_not_working = it did not, watching = too
    early to say.
    """
    rows: dict[tuple[str, str], dict] = {}
    for e in read(path):
        if not e.get("fix_pr"):
            continue
        k = (e.get("dag_id", ""), e.get("key", ""))
        row = rows.setdefault(k, {
            "dag_id": k[0], "key": k[1], "title": e.get("title", ""),
            "impact": e.get("impact", ""), "owner": e.get("owner", ""),
            "fix_pr": e.get("fix_pr", ""), "applied_date": e.get("applied_date", ""),
            "dcu_h_before": None, "dcu_h_after": None, "outcome": "watching",
        })
        row["fix_pr"] = e.get("fix_pr") or row["fix_pr"]
        row["applied_date"] = e.get("applied_date") or row["applied_date"]
        if e.get("state") in ("resolved", "fix_not_working"):
            row["outcome"] = e["state"]
    # Any entry for the dag_id: this key stops firing once the fix lands, by design.
    for e in read(path):
        dag = e.get("dag_id", "")
        if e.get("dcu_h") is None:
            continue
        for row in rows.values():
            if row["dag_id"] != dag or not row["applied_date"]:
                continue
            if e.get("date", "") < row["applied_date"]:
                row["dcu_h_before"] = e["dcu_h"]
            elif e.get("date", "") > row["applied_date"]:
                row["dcu_h_after"] = e["dcu_h"]
    return sorted(rows.values(), key=lambda r: r["applied_date"], reverse=True)


def render_shipped(rows: list[dict]) -> str:
    """Markdown register. Ranked newest first; the outcome column is the honest bit."""
    if not rows:
        return "No optimizations recorded as shipped yet.\n"
    out = [
        "| Applied | DAG | Finding | Impact | PR | Outcome | DCU/h before | after |",
        "|---|---|---|---|---|---|---:|---:|",
    ]
    for r in rows:
        pr = r["fix_pr"]
        cell = f"[{pr.rsplit('/', 1)[-1]}]({pr})" if pr.startswith("http") else pr
        before = "-" if r["dcu_h_before"] is None else f"{r['dcu_h_before']:.1f}"
        after = "-" if r["dcu_h_after"] is None else f"{r['dcu_h_after']:.1f}"
        out.append(
            f"| {r['applied_date']} | `{r['dag_id']}` | {r['title']} | {r['impact']} | "
            f"{cell} | {r['outcome']} | {before} | {after} |"
        )
    return "\n".join(out) + "\n"


def latest(path: str = LEDGER) -> dict[tuple[str, str], dict]:
    """Current state of every key the ledger has ever seen."""
    out: dict[tuple[str, str], dict] = {}
    for e in read(path):
        out[(e.get("dag_id", ""), e.get("key", ""))] = e
    return out


def savings(path: str = LEDGER) -> dict:
    """Cumulative measured savings since the first shipped fix.

    Only fixes whose finding went quiet (`resolved`) count, and only in the units the ledger
    actually measured: mean executor-hours per sweep-day before the applied date vs after,
    times the days observed since. The series is one value per dag per sweep-day, and a dag
    enters the total once however many resolved findings its fix cleared - the reduction is
    job-level, not per finding. No unit is converted to dollars here; DCU deltas carry the
    committed-use caveat and Databricks money lives in `databricks.job_costs`.
    """
    daily_h: dict[str, dict[str, float]] = {}
    for e in read(path):
        if e.get("exec_h") is not None:
            daily_h.setdefault(e.get("dag_id", ""), {})[e.get("date", "")] = e["exec_h"]
    rows, total_exec_h, counted = [], 0.0, set()
    for r in shipped(path):
        if r["outcome"] != "resolved":
            rows.append({**r, "days_observed": 0, "exec_h_saved": None})
            continue
        series = daily_h.get(r["dag_id"], {})
        before = [h for d, h in series.items() if d < r["applied_date"]]
        after_days = sorted(d for d in series if d > r["applied_date"])
        if not before or not after_days:
            rows.append({**r, "days_observed": len(after_days), "exec_h_saved": None})
            continue
        after = [series[d] for d in after_days]
        daily = sum(before) / len(before) - sum(after) / len(after)
        saved = daily * len(after_days)
        if r["dag_id"] not in counted:
            counted.add(r["dag_id"])
            total_exec_h += max(saved, 0.0)
        rows.append({**r, "days_observed": len(after_days), "exec_h_saved": saved})
    since = min((r["applied_date"] for r in rows if r["applied_date"]), default="")
    return {"since": since, "total_exec_h_saved": total_exec_h, "rows": rows}


def render_savings(s: dict) -> str:
    """The running savings log, one line of total first."""
    if not s["rows"]:
        return "No shipped optimization has a measured outcome yet.\n"
    out = [
        f"**Saved since {s['since']}: {s['total_exec_h_saved']:,.0f} executor-hours** "
        "(measured per-DAG, before-rate minus after-rate times days observed; only fixes whose "
        "finding stopped firing count, and each DAG enters the total once).",
        "",
        "| Applied | DAG | Finding | PR | Outcome | Days observed | Executor-hours saved |",
        "|---|---|---|---|---|---:|---:|",
    ]
    for r in s["rows"]:
        pr = r["fix_pr"]
        cell = f"[{pr.rsplit('/', 1)[-1]}]({pr})" if pr.startswith("http") else pr
        saved = "-" if r["exec_h_saved"] is None else f"{r['exec_h_saved']:,.1f}"
        out.append(f"| {r['applied_date']} | `{r['dag_id']}` | {r['title']} | {cell} | "
                   f"{r['outcome']} | {r['days_observed']} | {saved} |")
    return "\n".join(out) + "\n"


@dataclass
class Delta:
    """What changed between this sweep and the ledger before it."""

    new: list = field(default_factory=list)
    chronic: list = field(default_factory=list)
    resolved: list = field(default_factory=list)
    notified: list = field(default_factory=list)
    fix_not_working: list = field(default_factory=list)


def delta(entries: list[Entry]) -> Delta:
    """Split this sweep's entries into the buckets a digest reads out."""
    d = Delta()
    for e in entries:
        if e.state == "new":
            d.new.append(e)
        elif e.state == "chronic":
            d.chronic.append(e)
        elif e.state == "resolved":
            d.resolved.append(e)
        elif e.state == "fix_not_working":
            d.fix_not_working.append(e)
        elif e.state in STICKY:
            d.notified.append(e)
    return d


if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 5 and sys.argv[1] == "set":
        _, _, dag, key, state, *rest = sys.argv
        print(set_state(dag, key, state, note=" ".join(rest)))
        raise SystemExit(0)
    if len(sys.argv) >= 5 and sys.argv[1] == "applied":
        # applied <dag_id> <key> <fix_pr> <YYYY-MM-DD> [note...]
        _, _, dag, key, pr, date, *rest = sys.argv
        print(mark_applied(dag, key, pr, date, note=" ".join(rest)))
        raise SystemExit(0)
    if len(sys.argv) >= 2 and sys.argv[1] == "shipped":
        print(render_shipped(shipped()))
        raise SystemExit(0)
    if len(sys.argv) >= 2 and sys.argv[1] == "savings":
        print(render_savings(savings()))
        raise SystemExit(0)
    rows = read()
    print(f"{len(rows)} ledger entries, {len({(r['dag_id'], r['key']) for r in rows})} distinct keys")
    for (dag, key), e in sorted(latest().items()):
        print(f"  {e.get('state', ''):<15} {e.get('streak', 0):>3}x  {dag}  {key}")

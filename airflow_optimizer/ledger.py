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

`owner_notified` and `wont_fix` are sticky because they record a human decision;
everything else is recomputed from what the detectors saw.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass, field

LEDGER = "tickets/audi_1194_optimizer_efficiency_crawler/outputs/optimization_ledger.jsonl"
RESOLVE_SWEEPS = 3  # consecutive sweeps without a key before it counts as resolved
STICKY = ("owner_notified", "wont_fix")


@dataclass
class Entry:
    """One finding, on one sweep. The unit the ledger appends."""

    date: str
    dag_id: str
    app_id: str
    key: str  # "<detector>:<stage>" - stable across runs of the same job
    impact: str
    title: str
    owner: str = ""
    dcu_h: float | None = None
    state: str = "new"
    streak: int = 1
    note: str = ""


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


def classify(new: list[Entry], prior: list[dict], date: str) -> list[Entry]:
    """Set state and streak on this sweep's findings from what the ledger already holds."""
    hist = _history(prior)
    seen_dates = sorted({e.get("date", "") for e in prior if e.get("date")})
    for entry in new:
        past = [e for e in hist.get((entry.dag_id, entry.key), []) if e.get("date") != date]
        if not past:
            entry.state, entry.streak = "new", 1
            continue
        last = past[-1]
        entry.streak = int(last.get("streak", 0)) + 1
        sticky = next((e["state"] for e in reversed(past) if e.get("state") in STICKY), None)
        if sticky:
            entry.state, entry.note = sticky, last.get("note", "")
        elif entry.streak >= RESOLVE_SWEEPS:
            entry.state = "chronic"
        else:
            entry.state = "recurring"
    _mark_resolved(new, hist, seen_dates, date)
    return new


def _mark_resolved(new: list[Entry], hist: dict, seen_dates: list[str], date: str) -> None:
    """Append a resolved entry for any key absent from the last RESOLVE_SWEEPS sweeps."""
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
        new.append(Entry(
            date=date, dag_id=dag_id, app_id="", key=key, impact=last.get("impact", ""),
            title=last.get("title", ""), owner=last.get("owner", ""),
            state="resolved", streak=0,
            note=f"stopped firing after {last.get('date', 'an earlier sweep')}",
        ))


def append(entries: list[Entry], path: str = LEDGER) -> int:
    """Write this sweep's entries. Returns how many lines were added."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a") as fh:
        for e in entries:
            fh.write(json.dumps(asdict(e), sort_keys=True) + "\n")
    return len(entries)


def record(reports: list, date: str, owners: dict | None = None, dcu: dict | None = None,
           path: str = LEDGER, known: set | None = None) -> list[Entry]:
    """Turn a crawl's JobReports into classified ledger entries and append them.

    `known` is the active-DAG set from the coverage pass; it disambiguates a trailing
    numeric suffix (run index vs data-source id) instead of guessing.
    """
    owners, dcu = owners or {}, dcu or {}
    entries = []
    for r in reports:
        if getattr(r, "error", None) or not getattr(r, "findings", None):
            continue
        dag_id = _dag_id(r, known)
        for f in r.findings:
            entries.append(Entry(
                date=date, dag_id=dag_id, app_id=getattr(r, "source", ""),
                key=finding_key(f), impact=getattr(f, "impact", ""),
                title=getattr(f, "title", ""), owner=owners.get(dag_id, ""),
                dcu_h=dcu.get(dag_id),
            ))
    entries = _dedup(entries)
    classify(entries, read(path), date)
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


# Unambiguous per-RUN decorations Spark bakes into spark.app.name. Left in, every run
# mints a new ledger key and nothing is ever chronic.
_RUN_STAMP = re.compile(
    r"(?:"
    r"[-_]\d{4}-\d{2}-\d{2}(?:-\d+)*"    # -2026-08-20, -2026-08-20-1787259024
    r"|[-_]\d{8}-\d{6}(?:-\d+)*"          # -20260820-171500-1
    r"|[-_]?\s*\[\d+\]"                   # [19]
    r"|[-_]+$"
    r")+$"
)
# A trailing _<n> is AMBIGUOUS: a run index in `materialize_mntn_select_16`, a data-source
# id in `ipdsc_ds_67`. Stripping it blindly merges ds_13/ds_14/ds_67 into one key, so it is
# only removed when the stripped form is a DAG the coverage pass actually saw.
_TRAILING_INDEX = re.compile(r"_\d{1,3}$")


def _dag_id(report: object, known: set | None = None) -> str:
    """`Populate site_network_hourly.SiteNetworkHourly` -> `site_network_hourly`.

    Strips per-run stamps so an hourly job keeps ONE identity across sweeps. The result is
    a normalised job name, not a guaranteed Airflow dag_id - the digest only links it when
    it matches a DAG coverage saw.
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


def latest(path: str = LEDGER) -> dict[tuple[str, str], dict]:
    """Current state of every key the ledger has ever seen."""
    out: dict[tuple[str, str], dict] = {}
    for e in read(path):
        out[(e.get("dag_id", ""), e.get("key", ""))] = e
    return out


@dataclass
class Delta:
    """What changed between this sweep and the ledger before it."""

    new: list = field(default_factory=list)
    chronic: list = field(default_factory=list)
    resolved: list = field(default_factory=list)
    notified: list = field(default_factory=list)


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
        elif e.state in STICKY:
            d.notified.append(e)
    return d


if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 5 and sys.argv[1] == "set":
        _, _, dag, key, state, *rest = sys.argv
        print(set_state(dag, key, state, note=" ".join(rest)))
        raise SystemExit(0)
    rows = read()
    print(f"{len(rows)} ledger entries, {len({(r['dag_id'], r['key']) for r in rows})} distinct keys")
    for (dag, key), e in sorted(latest().items()):
        print(f"  {e.get('state', ''):<15} {e.get('streak', 0):>3}x  {dag}  {key}")

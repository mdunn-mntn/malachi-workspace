#!/usr/bin/env python3
"""health_scorecard.py — read-only workspace-health signals for the SessionStart print.

The self-improvement kernel's "am I keeping the system tended?" glance. Three signals the coverage
rollup and doc-debt queue don't already show:
  · days since the last /capture  (git commits whose message contains "capture")
  · orphan docs                   (knowledge/*.md untouched in git > STALE_DAYS — drift candidates)
  · duplicate H1 titles           (two knowledge docs with the same `# Title` — a merge smell)

READ-ONLY. No writes, no deletes (deletion authority is a named anti-goal — weeding stays in /capture).
One-line by default (for the hook); `--verbose` names the offenders. Never fails the caller — any error
prints nothing and exits 0, so a bad git state can't break SessionStart.

Usage: health_scorecard.py [--verbose]
"""

import datetime
import os
import re
import subprocess
import sys
import time
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", ".."))
KDIR = os.path.join(ROOT, "knowledge")
MEM_DIR = os.path.join(KDIR, "memory")
STALE_DAYS = 120
EVAL_STALE_DAYS = 14  # retrieval regression suite should run at least biweekly
MEM_STALE_DAYS = 90  # an active memory unverified this long is a refresh candidate
MEM_TOKEN_CAP = (
    1500  # MEMORY.md hot-tier ceiling (~6,000 bytes) — flags REGROWTH, not a 20-token overage
)
DAY = 86400


def _git(*args, timeout=15):
    return subprocess.run(
        ["git", "-C", ROOT, *args], capture_output=True, text=True, timeout=timeout
    ).stdout


def days_since_capture():
    """Days since the last /capture ritual commit.

    Matches the full ritual signature `<TICKET>: capture — …` (colon-space-capture-space-em-dash), not a
    bare mention of the word. This rejects three kinds of false positive seen in the wild: `CDC-capture` in
    a schema note, `capture reminder` in the hook-install commit, and — the subtle one — a commit *about*
    this detector whose body quotes the string `: capture` while describing the mechanism. Requiring the
    trailing ` — ` pins it to the actual consolidation commits. If the convention ever changes, this
    degrades safely to "no /capture commits" (which nudges a capture) rather than a false "0d ago".
    """
    ts = _git("log", "-1", "--format=%ct", "-i", "-E", "--grep=: capture —").strip()
    if not ts.isdigit():
        return None
    return int((time.time() - int(ts)) // DAY)


def days_since_eval():
    """Days since the last retrieval-eval run (commit message signature `retrieval-eval: run —`).

    The eval workflow (claude-prompts/retrieval_eval.js) can't write files, so each run is recorded by
    appending to knowledge/eval_runs.log and committing with this signature. Degrades safely to None
    (never run) → nudges a run, rather than a false "0d ago"."""
    ts = _git("log", "-1", "--format=%ct", "-E", "--grep=retrieval-eval: run —").strip()
    if not ts.isdigit():
        return None
    return int((time.time() - int(ts)) // DAY)


def _is_curated(rel):
    """A human-curated knowledge doc — not a generated index and not an auto-schema stub."""
    base = os.path.basename(rel)
    if base == "INDEX.md" or base.startswith("_"):
        return False
    if rel.startswith(
        "knowledge/memory/"
    ):  # memory has its own signals (memory_*), not the generic orphan/dup ones
        return False
    return rel.endswith(".md") and rel.startswith("knowledge/")


def last_touched():
    """{repo-relative path -> last-commit epoch} for knowledge/ docs, from ONE git-log pass."""
    out = _git("log", "--format=%ct", "--name-only", "--", "knowledge")
    seen, cur = {}, None
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.isdigit():
            cur = int(line)
        elif cur is not None and line not in seen:
            seen[line] = cur  # log is newest-first → first sighting is the last-touched time
    return seen


def orphans():
    """Curated knowledge docs untouched in git for > STALE_DAYS and still present on disk."""
    now, cutoff = time.time(), STALE_DAYS * DAY
    out = []
    for rel, ts in last_touched().items():
        if _is_curated(rel) and os.path.exists(os.path.join(ROOT, rel)) and (now - ts) > cutoff:
            out.append((rel, int((now - ts) // DAY)))
    return sorted(out, key=lambda x: -x[1])


def dup_titles():
    """H1 titles shared by 2+ curated knowledge docs (a merge/duplication smell)."""
    titles = {}
    for dp, dirs, files in os.walk(KDIR):
        dirs[:] = [d for d in dirs if d != "memory"]  # memory files handled by the memory_* signals
        for f in files:
            if not f.endswith(".md") or f == "INDEX.md" or f.startswith("_"):
                continue
            p = os.path.join(dp, f)
            try:
                with open(p, encoding="utf-8") as _fh:
                    for ln in _fh:
                        m = re.match(r"^#\s+(.+?)\s*$", ln)
                        if m:
                            titles.setdefault(m.group(1).lower(), []).append(
                                os.path.relpath(p, ROOT)
                            )
                            break
            except Exception:
                pass
    return {t: ps for t, ps in titles.items() if len(ps) > 1}


# ── memory signals (knowledge/memory/*.md — the unified auto-memory layer) ─────────────────────────
# READ-ONLY, same as the rest of this file. These are the memory analogs of orphans/dup-titles/coverage:
# lifecycle rollup, refresh queue, near-duplicate merge candidates, unresolved wikilinks, hot-tier budget.
def _norm(s):
    return s.strip().lower().replace("-", "_")


def _date_epoch(iso):
    y, m, d = map(int, iso[:10].split("-"))
    return time.mktime(datetime.date(y, m, d).timetuple())


def _mem_files():
    if not os.path.isdir(MEM_DIR):
        return []
    return [
        os.path.join(MEM_DIR, f)
        for f in sorted(os.listdir(MEM_DIR))
        if f.endswith(".md") and f != "MEMORY.md" and not f.startswith("_")
    ]


def _mem_fm(path):
    """Top-level memory front-matter (+ nested metadata.type fallback): type/lifecycle/last_verified/name/keywords/domain."""
    fm = {"keywords": [], "domain": []}
    try:
        lines = Path(path).read_text(encoding="utf-8").split("\n")
    except Exception:
        return fm
    if not lines or lines[0].strip() != "---":
        return fm
    for line in lines[1:]:
        if line.strip() == "---":
            break
        if line[:1] in (" ", "\t") or ":" not in line:
            m = re.match(r"^\s+type:\s*(.+)$", line)  # nested metadata.type fallback
            if m and "type" not in fm:
                fm["type"] = m.group(1).strip().strip('"').strip("'")
            continue
        k, v = line.split(":", 1)
        k, v = k.strip(), v.strip()
        if v.startswith("[") and v.endswith("]"):
            fm[k] = [x.strip().strip('"').strip("'") for x in v[1:-1].split(",") if x.strip()]
        else:
            fm[k] = v.strip('"').strip("'")
    return fm


def memory_lifecycle():
    """(counts by lifecycle, [(file, age_days), …] active memories past MEM_STALE_DAYS since last_verified)."""
    counts, stale, now = {"active": 0, "superseded": 0, "archived": 0}, [], time.time()
    for p in _mem_files():
        fm = _mem_fm(p)
        lc = fm.get("lifecycle", "active")
        counts[lc] = counts.get(lc, 0) + 1
        if lc == "active":
            lv = fm.get("last_verified", "") or ""
            if re.match(r"\d{4}-\d{2}-\d{2}", lv):
                age = (now - _date_epoch(lv)) / DAY
                if age > MEM_STALE_DAYS:
                    stale.append((os.path.basename(p), int(age)))
    stale.sort(key=lambda x: -x[1])
    return counts, stale


def memory_overlap_clusters(min_files=3):
    """Active memory files sharing a significant filename-stem token — near-duplicate merge candidates."""
    generic = {
        "feedback",
        "reference",
        "project",
        "user",
        "audi",
        "ti",
        "ber",
        "dm",
        "not",
        "mntn",
        "workflow",
        "audience",
    }  # type prefixes, ticket prefixes, cross-cutting stopwords
    tok = {}
    for p in _mem_files():
        if _mem_fm(p).get("lifecycle", "active") != "active":
            continue
        stem = os.path.splitext(os.path.basename(p))[0]
        for t in {
            x for x in stem.split("_") if len(x) > 2 and x not in generic and not x.isdigit()
        }:
            tok.setdefault(t, []).append(stem)
    return {t: sorted(v) for t, v in sorted(tok.items()) if len(v) >= min_files}


def memory_wikilinks():
    """[(file, unresolved_target), …] — [[links]] that resolve to no name/stem (kebab↔underscore-normalized)."""
    files = _mem_files()
    names = set()
    for p in files:
        names.add(_norm(os.path.splitext(os.path.basename(p))[0]))
        nm = _mem_fm(p).get("name")
        if nm:
            names.add(_norm(nm))
    out = []
    for p in files:
        try:
            txt = Path(p).read_text(encoding="utf-8")
        except Exception:
            continue
        for m in re.findall(r"\[\[([^\]]+)\]\]", txt):
            if _norm(m) not in names:
                out.append((os.path.basename(p), m))
    return out


def memory_budget():
    """(bytes, approx_tokens) of the always-loaded MEMORY.md hot tier."""
    mm = os.path.join(MEM_DIR, "MEMORY.md")
    if not os.path.exists(mm):
        return 0, 0
    b = os.path.getsize(mm)
    return b, b // 4


def memory_unindexed():
    """Native-tool-written memory files not yet normalized (have name+description but no doc_type:memory)
    → silently absent from _ROUTING.md until `lint_memory.py --fix` (or /capture) runs. This is the
    steady-state gap: the native memory tool writes its own raw schema, unaware of the unified one."""
    out = []
    for p in _mem_files():
        fm = _mem_fm(p)
        if fm.get("name") and fm.get("description") and fm.get("doc_type") != "memory":
            out.append(os.path.basename(p))
    return out


def main():
    verbose = "--verbose" in sys.argv
    mem_only = "--memory" in sys.argv
    try:
        dc = days_since_capture()
        de = days_since_eval()
        orph = orphans()
        dups = dup_titles()
        mcounts, mstale = memory_lifecycle()
        mclusters = memory_overlap_clusters()
        munres = memory_wikilinks()
        munidx = memory_unindexed()
        _mbytes, mtok = memory_budget()
    except Exception:
        return 0  # never break the caller

    over = " OVER" if mtok > MEM_TOKEN_CAP else ""
    uix = (
        f" · {len(munidx)} UNINDEXED" if munidx else ""
    )  # native-written raw files not in _ROUTING yet
    mem_line = (
        f"Memory  : {sum(mcounts.values())} files{uix} · {len(mstale)} stale(>{MEM_STALE_DAYS}d) · "
        f"{len(mclusters)} overlap-cluster(s) · {len(munres)} unresolved link(s) · "
        f"MEMORY.md ~{mtok / 1000:.1f}k/{MEM_TOKEN_CAP / 1000:.1f}k{over}"
    )

    def _mem_detail():
        print(
            f"\nMemory lifecycle: active {mcounts['active']} · superseded {mcounts['superseded']} "
            f"· archived {mcounts['archived']}"
        )
        if munidx:
            print("UNINDEXED (native-written; run `lint_memory.py --fix` to fold into _ROUTING):")
            for n in munidx:
                print(f"  {n}")
        if mstale:
            print(f"Stale active memories (>{MEM_STALE_DAYS}d since last_verified):")
            for n, d in mstale[:15]:
                print(f"  {d:>4}d  {n}")
        if mclusters:
            print(
                "Overlap clusters (shared stem token, ≥3 active — merge candidates, propose-only):"
            )
            for t, fs in mclusters.items():
                print(f"  {t}: {', '.join(fs)}")
        if munres:
            print("Unresolved [[wikilinks]] (repair or drop):")
            for fn, tgt in munres[:20]:
                print(f"  {fn}: [[{tgt}]]")
        if not (mstale or mclusters or munres):
            print("(no stale memories, overlap clusters, or broken links)")

    if mem_only:  # `--memory`: memory section only (for the audit's §10)
        print(mem_line)
        _mem_detail()
        return 0

    cap = "no /capture commits" if dc is None else f"last /capture {dc}d ago"
    if de is None:
        ev = "retrieval-eval never run (Workflow scriptPath: claude-prompts/retrieval_eval.js)"
    elif de > EVAL_STALE_DAYS:
        ev = f"retrieval-eval {de}d ago — STALE, run claude-prompts/retrieval_eval.js"
    else:
        ev = f"retrieval-eval {de}d ago"
    print(
        f"Health  : {cap} · {len(orph)} stale doc(s) (>{STALE_DAYS}d) · {len(dups)} dup-title · {ev}"
    )
    print(mem_line)

    if verbose:
        if orph:
            print(f"\nOrphans (untouched > {STALE_DAYS}d):")
            for rel, d in orph[:20]:
                print(f"  {d:>4}d  {rel}")
        if dups:
            print("\nDuplicate H1 titles:")
            for t, ps in dups.items():
                print(f"  '{t}': {', '.join(ps)}")
        if not orph and not dups:
            print("(no orphans or duplicate titles — knowledge base is tidy)")
        _mem_detail()
    return 0


if __name__ == "__main__":
    sys.exit(main())

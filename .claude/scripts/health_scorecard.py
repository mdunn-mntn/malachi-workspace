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
import os, re, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", ".."))
KDIR = os.path.join(ROOT, "knowledge")
STALE_DAYS = 120
DAY = 86400


def _git(*args, timeout=15):
    return subprocess.run(["git", "-C", ROOT, *args], capture_output=True, text=True, timeout=timeout).stdout


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


def _is_curated(rel):
    """A human-curated knowledge doc — not a generated index and not an auto-schema stub."""
    base = os.path.basename(rel)
    if base == "INDEX.md" or base.startswith("_"):
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
    for dp, _, files in os.walk(KDIR):
        for f in files:
            if not f.endswith(".md") or f == "INDEX.md" or f.startswith("_"):
                continue
            p = os.path.join(dp, f)
            try:
                for ln in open(p, encoding="utf-8"):
                    m = re.match(r"^#\s+(.+?)\s*$", ln)
                    if m:
                        titles.setdefault(m.group(1).lower(), []).append(os.path.relpath(p, ROOT))
                        break
            except Exception:
                pass
    return {t: ps for t, ps in titles.items() if len(ps) > 1}


def main():
    verbose = "--verbose" in sys.argv
    try:
        dc = days_since_capture()
        orph = orphans()
        dups = dup_titles()
    except Exception:
        return 0  # never break the caller

    cap = "no /capture commits" if dc is None else f"last /capture {dc}d ago"
    print(f"Health  : {cap} · {len(orph)} stale doc(s) (>{STALE_DAYS}d) · {len(dups)} dup-title")

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
    return 0


if __name__ == "__main__":
    sys.exit(main())

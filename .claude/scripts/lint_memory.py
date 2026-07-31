#!/usr/bin/env python3
"""lint_memory.py — linter + one-shot migrator for auto-memory files (knowledge/memory/*.md).

Brings the memory layer under the same front-matter discipline as the knowledge layer, so
build_index.sh folds memory into the ONE grep surface (_ROUTING.md) and memory stops being a
whole-loaded, unbounded MEMORY.md.

The unified additive schema (added at the TOP LEVEL, alongside the untouched native keys
`name`/`description`/`metadata`/`type` — never restructures or strips what the native memory tool
wrote):
  doc_type: memory        # build_index inclusion key — only files with doc_type are crawled
  keywords: [...]          # the ONLY field that feeds _ROUTING.md (seeded here, human/Workflow-sharpened)
  domain: [...]            # groups _MEMORY_INDEX.md
  lifecycle: active        # active | superseded | archived  (the coverage_state analog)
  last_verified: <date>    # staleness analog (seeded from git/mtime)

Why additive, not a rewrite: build_index's parse_front_matter flattens the nested `metadata:` block,
so `type` is already readable for BOTH the 31 flat and 113 nested files. Appending 5 top-level keys is
transparent to build_index AND to the native tool (which ignores unknown top-level keys), and cannot
corrupt a file the native tool depends on.

Modes:
  --check (default) : report files missing doc_type/keywords, empty keyword seeds, unresolved
                      wikilinks. Non-zero exit on any structural violation. Hook/CI friendly.
  --fix             : idempotently add the unified keys. Preserves already-good keywords/domain/
                      lifecycle/last_verified on re-run (never clobbers a human/Workflow improvement).

Usage: lint_memory.py [--check | --fix] [--dir knowledge/memory]
"""

import argparse
import datetime
import os
import re
import subprocess
import sys
from pathlib import Path

# Stop-words dropped from auto-seeded keywords (seeds are a floor; a Workflow/human sharpens them).
STOP = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "not",
    "no",
    "is",
    "are",
    "was",
    "were",
    "be",
    "to",
    "of",
    "in",
    "on",
    "for",
    "with",
    "by",
    "as",
    "at",
    "it",
    "its",
    "this",
    "that",
    "these",
    "those",
    "from",
    "use",
    "used",
    "using",
    "via",
    "per",
    "vs",
    "than",
    "then",
    "when",
    "if",
    "but",
    "so",
    "do",
    "does",
    "never",
    "always",
    "only",
    "one",
    "two",
    "new",
    "old",
    "how",
    "what",
    "why",
    "which",
}
TYPE_PREFIXES = ("feedback", "reference", "project", "user")


def _git_iso_date(path, root):
    """Last-commit ISO date for a file; '' if not in git yet."""
    try:
        out = subprocess.run(
            ["git", "-C", root, "log", "-1", "--format=%cs", "--", path],
            capture_output=True,
            text=True,
            timeout=15,
        ).stdout.strip()
        return out if re.match(r"\d{4}-\d{2}-\d{2}", out) else ""
    except Exception:
        return ""


def _mtime_iso(path):
    return datetime.date.fromtimestamp(os.path.getmtime(path)).isoformat()


def split_front_matter(text):
    """Return (fm_lines, body) or (None, text) if no leading --- block."""
    lines = text.split("\n")
    if not lines or lines[0].strip() != "---":
        return None, text
    end = next((i for i in range(1, len(lines)) if lines[i].strip() == "---"), None)
    if end is None:
        return None, text
    return lines[1:end], "\n".join(lines[end + 1 :])


def top_level_key(line):
    """The key of a top-level (unindented) `key: val` front-matter line, else None."""
    if line[:1] in (" ", "\t"):  # indented → a nested metadata child, not top-level
        return None
    m = re.match(r"^([A-Za-z0-9_]+):", line)
    return m.group(1) if m else None


def fm_scalar(fm_lines, key):
    """Value of a top-level scalar key (strips quotes + trailing comment), else None."""
    for line in fm_lines:
        if top_level_key(line) == key:
            v = line.split(":", 1)[1].strip()
            if v[:1] not in ('"', "'", "["):
                h = v.find(" #")
                if h != -1:
                    v = v[:h].strip()
            return v.strip('"').strip("'")
    return None


def fm_list_nonempty(fm_lines, key):
    """True if a top-level list key exists and is a non-empty [...] literal."""
    for line in fm_lines:
        if top_level_key(line) == key:
            v = line.split(":", 1)[1].strip()
            return v.startswith("[") and v.endswith("]") and v[1:-1].strip() != ""
    return False


def effective_type(fm_lines):
    """`type` from a flat top-level `type:` OR a nested `metadata:` child. Default 'reference'."""
    t = fm_scalar(fm_lines, "type")
    if t:
        return t
    for line in fm_lines:
        m = re.match(r"^\s+type:\s*(.+)$", line)  # indented → nested metadata.type
        if m:
            return m.group(1).strip().strip('"').strip("'")
    return "reference"


def seed_keywords(stem, description):
    """A floor set of keywords from the filename stem + salient description tokens.
    Deterministic. Intended to be replaced by a Workflow/human pass, but makes the file
    grep-reachable in _ROUTING.md immediately."""
    parts = stem.split("_")
    if parts and parts[0] in TYPE_PREFIXES:
        parts = parts[1:]
    kws = []
    stem_phrase = "_".join(parts)
    if stem_phrase:
        kws.append(stem_phrase)  # the topic name itself (e.g. hhst_pacing_lever)
    for p in parts:  # + individual significant tokens
        if len(p) > 2 and p not in STOP and p not in kws:
            kws.append(p)
    for w in re.findall(r"[A-Za-z0-9_.]+", (description or "").lower()):
        w = w.strip("._")
        if len(w) > 3 and w not in STOP and w not in kws:
            kws.append(w)
        if len(kws) >= 8:
            break
    return kws or [stem]


def seed_domain(mtype):
    """A minimal domain seed by type. Sharpened later; keeps _MEMORY_INDEX grouping non-empty."""
    return {
        "feedback": ["workflow"],
        "project": ["project"],
        "reference": ["reference"],
        "user": ["user"],
    }.get(mtype, ["reference"])


def _fmt_list(items):
    return "[" + ", ".join(items) + "]"


def is_memory_file(fm_lines):
    """A real memory file carries both name and description (excludes MEMORY.md / stray files)."""
    return (
        fm_lines is not None
        and fm_scalar(fm_lines, "name") is not None
        and fm_scalar(fm_lines, "description") is not None
    )


def fix_file(path, root):
    text = Path(path).read_text(encoding="utf-8")
    fm_lines, body = split_front_matter(text)
    if not is_memory_file(fm_lines):
        return False

    stem = os.path.splitext(os.path.basename(path))[0]
    mtype = effective_type(fm_lines)
    additions = []

    if fm_scalar(fm_lines, "doc_type") is None:
        additions.append("doc_type: memory")
    # only (re)seed when absent/empty — never clobber a sharpened list
    if not fm_list_nonempty(fm_lines, "keywords") and fm_scalar(fm_lines, "keywords") is None:
        additions.append(
            "keywords: " + _fmt_list(seed_keywords(stem, fm_scalar(fm_lines, "description")))
        )
    if fm_scalar(fm_lines, "domain") is None:
        additions.append("domain: " + _fmt_list(seed_domain(mtype)))
    if fm_scalar(fm_lines, "lifecycle") is None:
        additions.append("lifecycle: active")
    if fm_scalar(fm_lines, "last_verified") is None:
        lv = _git_iso_date(path, root) or _mtime_iso(path)
        additions.append(f"last_verified: {lv}")

    if not additions:
        return False  # idempotent — already migrated

    new_fm = fm_lines + additions
    new_text = "---\n" + "\n".join(new_fm) + "\n---\n" + body.lstrip("\n")
    if not new_text.endswith("\n"):
        new_text += "\n"
    if new_text != text:
        Path(path).write_text(new_text, encoding="utf-8")
        return True
    return False


def _all_names(files):
    """Set of resolvable link targets: every file's `name:` + its filename stem, normalized."""
    names = set()
    for p in files:
        fm_lines, _ = split_front_matter(Path(p).read_text(encoding="utf-8"))
        if not is_memory_file(fm_lines):
            continue
        names.add(_norm(os.path.splitext(os.path.basename(p))[0]))
        nm = fm_scalar(fm_lines, "name")
        if nm:
            names.add(_norm(nm))
    return names


def _norm(s):
    return s.strip().lower().replace("-", "_")


def wikilink_report(files):
    """[(file, unresolved_target), ...] — [[links]] that resolve to no name/stem (kebab-normalized)."""
    names, out = _all_names(files), []
    for p in files:
        text = Path(p).read_text(encoding="utf-8")
        for m in re.findall(r"\[\[([^\]]+)\]\]", text):
            if _norm(m) not in names:
                out.append((os.path.basename(p), m))
    return out


def check_file(path):
    text = Path(path).read_text(encoding="utf-8")
    fm_lines, _ = split_front_matter(text)
    if not is_memory_file(fm_lines):
        return []
    v = []
    if fm_scalar(fm_lines, "doc_type") != "memory":
        v.append("missing doc_type: memory (not folded into _ROUTING.md)")
    if not fm_list_nonempty(fm_lines, "keywords"):
        v.append("empty/missing keywords (not grep-reachable in _ROUTING.md)")
    lc = fm_scalar(fm_lines, "lifecycle")
    if lc not in ("active", "superseded", "archived"):
        v.append(f"lifecycle={lc!r} not in active|superseded|archived")
    return v


def collect(dirp):
    files = []
    for dp, _dirs, fns in os.walk(dirp):
        for fn in sorted(fns):
            if fn.endswith(".md") and not fn.startswith("_") and fn != "MEMORY.md":
                files.append(os.path.join(dp, fn))
    return files


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.normpath(os.path.join(here, "..", ".."))
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=os.path.join(root, "knowledge", "memory"))
    ap.add_argument("--fix", action="store_true")
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    if not os.path.isdir(a.dir):
        print(f"lint_memory: no dir {a.dir}", file=sys.stderr)
        return 0
    files = collect(a.dir)

    if a.fix and not a.check:
        n = sum(1 for p in files if fix_file(p, root))
        print(f"lint_memory --fix: migrated {n}/{len(files)} memory files.")
        return 0

    violations = 0
    for p in files:
        for v in check_file(p):
            print(f"VIOLATION {os.path.relpath(p, root)}: {v}", file=sys.stderr)
            violations += 1
    unresolved = wikilink_report(files)
    for fn, tgt in unresolved:
        print(f"WIKILINK   {fn}: [[{tgt}]] resolves to nothing")
    print(
        f"lint_memory --check: {len(files)} files, {violations} violation(s), "
        f"{len(unresolved)} unresolved wikilink(s)."
    )
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())

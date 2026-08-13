#!/usr/bin/env python3
"""audit_structure.py — meticulous, deterministic structure audit against folder_definitions.md.

Walks EVERY tracked file + every on-disk dir and classifies mechanical conformance issues. Emits a JSON
manifest of findings (path, category, severity, action, reason) + a printed summary. READ-ONLY — proposes
actions, executes nothing. The semantic/judgment layer (superseded decks, redundant CSVs, misfiled-by-
content) is added on top by agents; this script is the exhaustive mechanical floor.

Categories → default proposed action:
  junk             → DELETE+GITIGNORE   (spark markers, .DS_Store, __pycache__, *.pyc, .ipynb_checkpoints)
  naming           → RENAME             (uppercase / space / dash in a hand-authored path segment)
  queries_non_sql  → MOVE               (non-.sql file under a queries/ dir)
  root_stray       → FLAG               (top-level entry not in the blessed root set — reconcile standard)
  empty_dir        → DELETE             (empty scaffolded dir; recreated on demand)
  deep_nesting     → REVIEW             (tracked file > DEPTH_FLAG segments deep)
  tracked_data     → REVIEW             (data-looking file tracked in git despite .gitignore patterns)
  ticket_skeleton  → REVIEW             (ticket folder missing summary.md)

Usage: audit_structure.py [--json PATH] [--top-level DIR]   # default prints summary to stdout
"""

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", ".."))
DEPTH_FLAG = 5  # tracked path segments (ROOT-relative) beyond this → flag for review

# Root entries the standard blesses. folder_definitions.md lists the first 6; the rest are legitimate
# additions that post-date it (and are referenced in CLAUDE.md) — the audit surfaces the gap so the
# standard doc can be reconciled, but does not flag these as junk.
BLESSED_ROOT = {
    ".claude",
    ".gitignore",
    "AGENTS.md",  # cross-vendor rules file; CLAUDE.md symlinks to it
    "CLAUDE.md",
    ".agents",  # symlink -> .claude/skills, read by Codex/Cursor/Gemini/Copilot
    ".githooks",
    "pyproject.toml",
    "knowledge",
    "tickets",
    "documentation",
    "claude-prompts",  # spec
    "workflows",
    "self_review",
    "slack_bot",
    "README.md",
    ".mcp.json",
    ".git",  # legit, post-spec
}
ROOT_RECONCILE = {"todoist-mcp-transfer"}  # present but of debatable fate — flag explicitly

# Filenames whose case is conventional (not a violation): README-family + generated/kit conventions.
NAME_OK = re.compile(
    r"^(README|MEMORY|CLAUDE|ARCHITECTURE|INGEST_GUIDE|START_HERE|LICENSE|Makefile|INDEX|SKILL)"
    r"(\.[a-z]+)?$|^_[A-Z]"  # generated _TOPICS/_ROUTING/_CATALOG_INDEX/_COVERAGE/_TABLE_TEMPLATE...
)
# Directory segments whose non-conforming spelling is sanctioned/external (do NOT flag these dir names).
SANCTIONED_DIR = {
    "claude-prompts",  # blessed root folder (folder_definitions.md) — the dash is the standard name
    "todoist-mcp-transfer",
    "mcp-server",
    "claude-memory",  # vendored MCP tool tree (external naming)
    "node_modules",
    ".ipynb_checkpoints",
    "batch1_queries",
}
AUTO_DIR = re.compile(r"^-Users-|^-[A-Za-z]")  # Claude Code auto-generated project-dir slug
CONFIG_JSON = {
    "settings.json",
    "settings.local.json",
    ".mcp.json",
    "package.json",
    "package-lock.json",
    "tsconfig.json",
}
JUNK = re.compile(
    r"(^\.DS_Store$|^_SUCCESS$|^_started_|^_committed_|\.pyc$|^__pycache__$|^\.ipynb_checkpoints$)"
)
DATA_EXT = re.compile(r"\.(csv|tsv|parquet|xlsx?|json|jsonl)(\.gz)?$", re.IGNORECASE)
BAD_SEG = re.compile(r"[A-Z ]|[a-z0-9]-[a-z0-9]")  # uppercase, space, or dash-between-words


def git_files():
    out = subprocess.run(["git", "-C", ROOT, "ls-files"], capture_output=True, text=True).stdout
    return [line for line in out.splitlines() if line]


def add(findings, category, severity, action, path, reason):
    findings.append(
        {
            "category": category,
            "severity": severity,
            "action": action,
            "path": path,
            "reason": reason,
        }
    )


def audit(scope=None):
    findings = []
    files = git_files()
    if scope:
        files = [f for f in files if f.startswith(scope)]

    # --- root-level entries ---
    if not scope:
        for name in sorted(os.listdir(ROOT)):
            if name in BLESSED_ROOT:
                continue
            if name in ROOT_RECONCILE:
                add(
                    findings,
                    "root_stray",
                    "medium",
                    "FLAG",
                    name + "/",
                    "top-level vendored tool not in the folder_definitions root set — decide: relocate, gitignore, or bless",
                )
            else:
                add(
                    findings,
                    "root_stray",
                    "high",
                    "FLAG",
                    name,
                    "not in the blessed root set — folder_definitions says root holds only .claude/.gitignore/knowledge/tickets/documentation/claude-prompts (+ post-spec workflows/self_review/slack_bot/README/.mcp.json)",
                )

    # --- per-file mechanical checks ---
    bad_dirs = set()  # violating directory names, flagged once each
    claude_projects_tracked = False
    for f in files:
        segs = f.split("/")
        base = segs[-1]

        if f.startswith(".claude/projects/"):
            claude_projects_tracked = (
                True  # Claude-managed session/memory tree tracked in-repo — flag once
            )
            continue

        if JUNK.search(base):
            add(
                findings,
                "junk",
                "high",
                "DELETE+GITIGNORE",
                f,
                "build/OS artifact — should never be tracked",
            )
            continue

        # naming — check ONLY the leaf filename (parent dirs handled separately, once each)
        if BAD_SEG.search(base) and not NAME_OK.match(base):
            add(
                findings,
                "naming",
                "medium",
                "RENAME",
                f,
                f"filename '{base}' breaks lowercase/underscore rule (uppercase, space, or dash)",
            )
        # collect violating directory segments (dedup, skip sanctioned + Claude auto-dirs)
        for i, s in enumerate(segs[:-1]):
            if (
                s in SANCTIONED_DIR
                or AUTO_DIR.match(s)
                or not (BAD_SEG.search(s) and not NAME_OK.match(s))
            ):
                continue
            bad_dirs.add("/".join(segs[: i + 1]) + "/")

        # queries/ must hold only .sql
        if "/queries/" in ("/" + f) and not base.endswith(".sql") and base != ".gitkeep":
            add(
                findings,
                "queries_non_sql",
                "medium",
                "MOVE",
                f,
                "non-.sql file under queries/ — belongs in outputs/ or artifacts/",
            )

        # deep nesting
        if len(segs) - 1 > DEPTH_FLAG:
            add(
                findings,
                "deep_nesting",
                "low",
                "REVIEW",
                f,
                f"{len(segs) - 1} levels deep — usually machine-generated; confirm it should be tracked",
            )

        # data tracked despite .gitignore (force-added) — exclude config json + the perf log
        if (
            DATA_EXT.search(base)
            and base not in CONFIG_JSON
            and not f.startswith("knowledge/")
            and base != "bq_perf_log.jsonl"
        ):
            add(
                findings,
                "tracked_data",
                "low",
                "REVIEW",
                f,
                "data-typed file tracked in git — confirm it belongs in the repo vs. gitignored",
            )

    for d in sorted(bad_dirs):
        add(
            findings,
            "naming",
            "medium",
            "RENAME",
            d,
            "directory name breaks lowercase/underscore rule",
        )
    if claude_projects_tracked:
        add(
            findings,
            "tracked_tree",
            "medium",
            "REVIEW",
            ".claude/projects/",
            "Claude-managed session/memory tree is tracked in this repo — confirm intentional vs. should be gitignored (memory canonically lives in global ~/.claude)",
        )

    # --- ticket skeleton: summary.md present? ---
    if not scope or scope.startswith("tickets"):
        tdir = os.path.join(ROOT, "tickets")
        for name in sorted(os.listdir(tdir)):
            d = os.path.join(tdir, name)
            if not os.path.isdir(d) or name.startswith(("_", ".")):
                continue
            if not os.path.exists(os.path.join(d, "summary.md")):
                add(
                    findings,
                    "ticket_skeleton",
                    "medium",
                    "REVIEW",
                    f"tickets/{name}/",
                    "ticket folder has no summary.md",
                )

    # --- empty dirs on disk ---
    for dp, dns, fns in os.walk(ROOT):
        if "/.git" in dp or "node_modules" in dp:
            dns[:] = [d for d in dns if d not in (".git", "node_modules")]
            continue
        rel = os.path.relpath(dp, ROOT)
        if scope and not rel.startswith(scope):
            continue
        if not dns and not fns and rel != ".":
            add(
                findings,
                "empty_dir",
                "low",
                "DELETE",
                rel + "/",
                "empty directory — scaffold remnant; recreated on demand",
            )

    return findings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", help="write full findings manifest to this path")
    ap.add_argument("--top-level", help="scope the audit to one top-level dir")
    args = ap.parse_args()

    findings = audit(args.top_level)
    by_cat = defaultdict(list)
    for x in findings:
        by_cat[x["category"]].append(x)

    print(
        f"═══ audit_structure: {len(findings)} finding(s) across {len(by_cat)} categor(ies) ═══\n"
    )
    order = [
        "root_stray",
        "tracked_tree",
        "junk",
        "naming",
        "queries_non_sql",
        "ticket_skeleton",
        "tracked_data",
        "deep_nesting",
        "empty_dir",
    ]
    for cat in order:
        items = by_cat.get(cat, [])
        if not items:
            continue
        act = items[0]["action"]
        print(f"## {cat}  ({len(items)}) → default {act}")
        for x in items[:60]:
            print(f"  [{x['severity']:>6}] {x['path']}")
            print(f"           ↳ {x['reason']}")
        if len(items) > 60:
            print(f"  … +{len(items) - 60} more")
        print()

    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(findings, fh, indent=2)
        print(f"→ wrote manifest: {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

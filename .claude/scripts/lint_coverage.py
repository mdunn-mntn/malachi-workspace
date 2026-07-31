#!/usr/bin/env python3
"""Coverage-state linter + one-shot migrator for bq_table docs.

The two-date model's guard rail:
  - a doc whose body still has `<Fill:` / `<fill me>` stubs MUST be coverage_state: skeleton
    with an empty last_verified. A stub can never masquerade as enriched/verified.

Modes:
  --check  (default) : non-zero exit on any violation; warns when a skeleton has no stubs left
                       (it's ready to advance). Hook/CI friendly.
  --fix              : one-shot migration of already-seeded docs — normalize front-matter to the
                       unified schema (coverage_state, schema_synced, last_verified:null-if-stub,
                       physical_table, partition_by:unknown-for-views, require_partition_filter,
                       time_unit, ttl_days, domain, keywords) and inject the 3 append regions.

Usage: lint_coverage.py [--check | --fix] [--dir knowledge/bq]
"""

import argparse
import datetime
import os
import re
import sys
from pathlib import Path

STUB_RE = re.compile(r"<Fill:|<fill me>", re.IGNORECASE)
# View definitions quote each component in backticks: `sqlmesh__ds`.`table` — span the backticks/dots.
PHYS_RE = re.compile(r"sqlmesh__[A-Za-z0-9_]+`?\s*\.\s*`?[A-Za-z0-9_]+")


def _phys(text):
    m = PHYS_RE.search(text or "")
    return m.group(0).replace("`", "").replace(" ", "") if m else None


APPEND_REGIONS = """
## Observed cost
<!-- OBSERVED:COST START -->
<!-- perf-analyst appends dated one-liners here: `- YYYY-MM-DD: <slice> scanned <N> GB (est <M>), slot <S>s — <note>` -->
<!-- OBSERVED:COST END -->

## Observed facts
<!-- OBSERVED:FACTS START -->
<!-- capture/curator appends tribal findings here: `- YYYY-MM-DD: <fact verified against source>` -->
<!-- OBSERVED:FACTS END -->

## Changelog
<!-- CHANGELOG START -->
<!-- coverage transitions + schema changes: `- YYYY-MM-DD: skeleton→enriched` / `- YYYY-MM-DD: column X added` -->
<!-- CHANGELOG END -->
"""


def split_front_matter(text):
    lines = text.split("\n")
    if not lines or lines[0].strip() != "---":
        return None, None, text
    end = next((i for i in range(1, len(lines)) if lines[i].strip() == "---"), None)
    if end is None:
        return None, None, text
    return lines[1:end], end, "\n".join(lines)


def fm_get(fm_lines, key):
    for line in fm_lines:
        m = re.match(rf"^{re.escape(key)}:\s*(.*)$", line)
        if m:
            v = m.group(1).strip()
            # strip trailing comment on scalar values
            if v[:1] not in ('"', "'", "["):
                h = v.find(" #")
                if h != -1:
                    v = v[:h].strip()
            return v.strip('"').strip("'")
    return None


def fm_set(fm_lines, updates):
    """Replace existing keys; append missing ones at the end of the block. Returns new fm_lines."""
    seen = set()
    out = []
    for line in fm_lines:
        m = re.match(r"^(\w+):", line)
        if m and m.group(1) in updates:
            out.append(f"{m.group(1)}: {updates[m.group(1)]}")
            seen.add(m.group(1))
        else:
            out.append(line)
    for k, v in updates.items():
        if k not in seen:
            out.append(f"{k}: {v}")
    return out


def check_file(path):
    """Return list of violation strings (empty = clean); also a 'ready' note if applicable."""
    text = Path(path).read_text(encoding="utf-8")
    fm_lines, _, _ = split_front_matter(text)
    if fm_lines is None or fm_get(fm_lines, "doc_type") != "bq_table":
        return [], None
    has_stub = bool(STUB_RE.search(text))
    cov = fm_get(fm_lines, "coverage_state")
    lv = fm_get(fm_lines, "last_verified")
    lv_empty = lv in (None, "", "null", "none")
    v = []
    if has_stub:
        if cov != "skeleton":
            v.append(f"has <Fill:> stubs but coverage_state={cov!r} (must be 'skeleton')")
        if not lv_empty:
            v.append(f"has <Fill:> stubs but last_verified={lv!r} (must be empty/null)")
    # coverage_state is a required, valid field on every bq_table doc (a dropped field silently
    # defaults to 'skeleton' in build_index and corrupts the rollup — see the 20-doc enrich regression)
    if cov in (None, ""):
        v.append("missing coverage_state (required on every bq_table doc)")
    elif cov not in ("skeleton", "enriched", "verified"):
        v.append(f"coverage_state={cov!r} not in skeleton|enriched|verified")
    elif cov == "verified" and lv_empty:
        v.append(
            "coverage_state=verified but last_verified is empty (verified means confirmed vs source on a date)"
        )
    ready = not has_stub and cov == "skeleton"
    return v, (
        "stubs gone but still coverage_state:skeleton — ready to advance to enriched"
        if ready
        else None
    )


def fix_file(path, today):
    text = Path(path).read_text(encoding="utf-8")
    fm_lines, _, _ = split_front_matter(text)
    if fm_lines is None or fm_get(fm_lines, "doc_type") != "bq_table":
        return False
    obj = (fm_get(fm_lines, "object_type") or "BASE TABLE").upper()
    is_view = obj in ("VIEW", "MATERIALIZED VIEW")
    has_stub = bool(STUB_RE.search(text))

    # schema_synced: keep an existing one, else adopt the (misleading) old last_verified date, else today
    schema_synced = fm_get(fm_lines, "schema_synced")
    if not schema_synced or schema_synced in ("null", "none"):
        old_lv = fm_get(fm_lines, "last_verified")
        schema_synced = old_lv if (old_lv and re.match(r"\d{4}-\d{2}-\d{2}", old_lv)) else today

    phys = _phys(text)
    physical_table = phys if phys else ("self" if not is_view else "unknown")
    existing_phys = fm_get(fm_lines, "physical_table")

    updates = {
        "schema_synced": schema_synced,
        "coverage_state": (
            "skeleton" if has_stub else (fm_get(fm_lines, "coverage_state") or "skeleton")
        ),
        "physical_table": existing_phys
        if (existing_phys and existing_phys != "unknown")
        else physical_table,
        "require_partition_filter": fm_get(fm_lines, "require_partition_filter") or "unknown",
        "time_unit": fm_get(fm_lines, "time_unit") or "unknown",
        "ttl_days": fm_get(fm_lines, "ttl_days") or "null",
        "domain": fm_get(fm_lines, "domain") if fm_get(fm_lines, "domain") is not None else "[]",
        "keywords": fm_get(fm_lines, "keywords")
        if fm_get(fm_lines, "keywords") is not None
        else "[]",
    }
    # last_verified: null whenever stubs remain (the core migration)
    if has_stub:
        updates["last_verified"] = "null"
    # views: a false 'none' partition becomes an honest 'unknown' gap (cataloger fills the real one)
    if is_view and (fm_get(fm_lines, "partition_by") in (None, "none", "None", "")):
        updates["partition_by"] = "unknown"

    new_fm = fm_set(fm_lines, updates)
    lines = text.split("\n")
    end = next((i for i in range(1, len(lines)) if lines[i].strip() == "---"), None)
    new_text = "\n".join(["---"] + new_fm + ["---"] + lines[end + 1 :])

    if "OBSERVED:COST START" not in new_text:
        new_text = new_text.rstrip() + "\n" + APPEND_REGIONS
    if not new_text.endswith("\n"):
        new_text += "\n"
    if new_text != text:
        Path(path).write_text(new_text, encoding="utf-8")
        return True
    return False


def main():
    ap = argparse.ArgumentParser()
    here = os.path.dirname(os.path.abspath(__file__))
    ap.add_argument(
        "--dir", default=os.path.normpath(os.path.join(here, "..", "..", "knowledge", "bq"))
    )
    ap.add_argument("--fix", action="store_true")
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    do_fix = a.fix and not a.check
    today = datetime.date.today().isoformat()

    files = []
    for dp, _dirs, fns in os.walk(a.dir):
        for fn in sorted(fns):
            if fn.endswith(".md") and not fn.startswith("_"):
                files.append(os.path.join(dp, fn))

    if do_fix:
        n = sum(1 for p in files if fix_file(p, today))
        print(f"lint_coverage --fix: migrated {n}/{len(files)} table docs.")
        return 0

    violations, ready = 0, 0
    for p in files:
        vs, note = check_file(p)
        rel = os.path.relpath(p, os.path.dirname(a.dir))
        for v in vs:
            print(f"VIOLATION {rel}: {v}", file=sys.stderr)
            violations += 1
        if note:
            print(f"ready     {rel}: {note}")
            ready += 1
    print(
        f"lint_coverage --check: {len(files)} docs, {violations} violation(s), {ready} ready-to-advance."
    )
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Render/refresh BQ table catalog docs from INFORMATION_SCHEMA JSON.

Usage: _render_table_doc.py <dataset> <out_dir> <columns.json> <storage.json> <tables.json>

Each JSON file is the `bq query --format=prettyjson` output (a list of row objects).
Preserves human-written sections on re-runs; only regenerates the AUTO:SCHEMA block and the
derived front-matter fields (partition_by, cluster_by, approx_rows, approx_logical_bytes,
last_verified). This is what makes the catalog safe to refresh without losing enrichment.
"""

import json, os, re, sys, datetime

AUTO_RE = re.compile(r"(<!-- AUTO:SCHEMA START.*?-->\n).*?(\n<!-- AUTO:SCHEMA END -->)", re.DOTALL)
# View definitions quote each component in backticks: `sqlmesh__ds`.`table` — span the backticks/dots.
PHYS_RE = re.compile(r"sqlmesh__[A-Za-z0-9_]+`?\s*\.\s*`?[A-Za-z0-9_]+")


def physical_of(is_view, definition):
    """The physical object a doc describes: 'self' for base tables; the sqlmesh__* table parsed
    from the view definition (SQLMesh profile); 'unknown' if a view whose physical can't be parsed."""
    if not is_view:
        return "self"
    m = PHYS_RE.search(definition or "")
    return m.group(0).replace("`", "").replace(" ", "") if m else "unknown"


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def schema_block(tcols):
    rows = [
        "| column | type | nullable | partition | cluster# |",
        "|--------|------|----------|-----------|----------|",
    ]
    for c in tcols:
        part = "YES" if str(c.get("is_partitioning_column", "")).upper() == "YES" else ""
        clus = c.get("clustering_ordinal_position")
        clus = str(clus) if clus not in (None, "", "null") else ""
        rows.append(
            f"| {c['column_name']} | {c['data_type']} | "
            f"{c.get('is_nullable', '')} | {part} | {clus} |"
        )
    return "\n".join(rows)


def derive(tcols):
    part = next(
        (
            c["column_name"]
            for c in tcols
            if str(c.get("is_partitioning_column", "")).upper() == "YES"
        ),
        "none",
    )
    clus = [
        c["column_name"]
        for c in sorted(
            (c for c in tcols if c.get("clustering_ordinal_position") not in (None, "", "null")),
            key=lambda c: int(c["clustering_ordinal_position"]),
        )
    ]
    return part, clus


def set_fm(content, updates):
    """Replace/insert given keys inside the leading YAML front-matter, leaving the body untouched."""
    lines = content.splitlines()
    if not lines or lines[0].strip() != "---":
        return content
    end = next((i for i in range(1, len(lines)) if lines[i].strip() == "---"), None)
    if end is None:
        return content
    block, seen = lines[1:end], set()
    for i, l in enumerate(block):
        m = re.match(r"^(\w+):", l)
        if m and m.group(1) in updates:
            block[i] = f"{m.group(1)}: {updates[m.group(1)]}"
            seen.add(m.group(1))
    for k, v in updates.items():
        if k not in seen:
            block.append(f"{k}: {v}")
    return "\n".join([lines[0]] + block + lines[end:])


def fm_updates(part, clus, storage, today, ttype):
    """Fields to (re)stamp on a REFRESH of an existing doc. Two invariants:

    1. NEVER emit ``last_verified`` — it is a human field. set_fm() only replaces/inserts, so by
       not emitting it we leave any existing value untouched; refresh bumps only the machine field
       ``schema_synced``. (New docs get ``last_verified: null`` from NEW_DOC.)
    2. For VIEW / MATERIALIZED VIEW, do NOT overwrite partition_by / cluster_by / approx_* —
       INFORMATION_SCHEMA reports a view as unpartitioned/empty, which would silently erase the
       partition/cluster/size the cataloger resolved from the physical sqlmesh__* table. Those
       values are authoritative only for BASE TABLEs, so we re-stamp them only there.
    """
    upd = {"object_type": ttype, "schema_synced": today}
    if ttype not in ("VIEW", "MATERIALIZED VIEW"):
        rows, lbytes = _int(storage.get("total_rows")), _int(storage.get("total_logical_bytes"))
        upd.update(
            {
                "partition_by": part,
                "cluster_by": "[" + ", ".join(clus) + "]",
                "approx_rows": rows if rows is not None else "null",
                "approx_logical_bytes": lbytes if lbytes is not None else "null",
            }
        )
    return upd


NEW_DOC = """---
doc_type: bq_table
title: {ds}.{tbl}
summary: "{summary_default}"
dataset: {ds}
table: {tbl}
object_type: {ttype}
physical_table: {physical}
grain: "{grain_default}"
partition_by: {part_display}
require_partition_filter: unknown
cluster_by: [{clus}]
time_unit: unknown
ttl_days: null
approx_rows: {rows}
approx_logical_bytes: {lbytes}
schema_synced: {today}
last_verified: null
coverage_state: skeleton
domain: []
keywords: []
source: INFORMATION_SCHEMA+human
tags: []
---

# {ds}.{tbl}

## Purpose
<Fill: why this table exists and when to reach for it.>

## Grain & keys
- **Grain:** one row per <fill me>.
- **Key(s) / join columns:** <fill me>.

<!-- AUTO:SCHEMA START — regenerated by scripts/bq_introspect.sh; do NOT hand-edit inside markers -->
{schema}
<!-- AUTO:SCHEMA END -->

## Column meanings (only the non-obvious ones)
<Fill: what columns MEAN — units, encodings, NULL semantics. Not their types.>

## Joins & relationships
<Fill: how it connects to other tables; fan-out warnings.>

## Gotchas
<Fill: late-arriving data, duplicates, partition-column timezone, soft-deletes.>

## Cost & partitioning notes
- Partition `{part_display}`, cluster [{clus}]. Always filter the partition column; avoid SELECT *.
- If this is a VIEW, resolve the physical `{physical}` table to recover the real partition/cluster/TTL (cataloger's job); `partition_by: unknown` is an actionable gap, not a claim of "no partition."

## Example queries
```sql
SELECT <cols> FROM `{ds}.{tbl}` WHERE <partition_col> BETWEEN '<start>' AND '<end>'
```

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


def write_doc(dataset, out_dir, name, tcols, storage, today, ttype="BASE TABLE", definition=None):
    path = os.path.join(out_dir, f"{name}.md")
    part, clus = derive(tcols)
    schema = schema_block(tcols)
    is_view = ttype in ("VIEW", "MATERIALIZED VIEW")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            content = f.read()
        m = AUTO_RE.search(content)
        schema_changed = True
        if m:
            schema_changed = (
                content[m.end(1) : m.start(2)] != schema
            )  # compare the OLD schema block to the new
            content = AUTO_RE.sub(lambda mm: mm.group(1) + schema + mm.group(2), content)
        else:
            content = (
                content.rstrip()
                + "\n\n<!-- AUTO:SCHEMA START — regenerated by scripts/bq_introspect.sh -->\n"
                + schema
                + "\n<!-- AUTO:SCHEMA END -->\n"
            )
        upd = fm_updates(part, clus, storage, today, ttype)
        if not schema_changed:
            upd.pop(
                "schema_synced", None
            )  # no-op reintrospect: don't re-stamp the machine date (would falsely flag verified docs stale)
        content = set_fm(content, upd)
        action = "updated"
    else:
        rows, lbytes = _int(storage.get("total_rows")), _int(storage.get("total_logical_bytes"))
        grain_default = "N/A — derived view" if is_view else "one row per <fill me>"
        summary_default = (
            f"<what this {ttype.lower()} provides>"
            if is_view
            else "one row per <FILL grain> — <what it's for>"
        )
        # Views report as unpartitioned in INFORMATION_SCHEMA — record 'unknown' (an honest gap the
        # cataloger fills from the physical table), never a false 'none'.
        part_display = "unknown" if is_view else part
        physical = physical_of(is_view, definition)
        content = NEW_DOC.format(
            ds=dataset,
            tbl=name,
            ttype=ttype,
            part_display=part_display,
            physical=physical,
            clus=", ".join(clus),
            rows=rows if rows is not None else "null",
            lbytes=lbytes if lbytes is not None else "null",
            today=today,
            schema=schema,
            grain_default=grain_default,
            summary_default=summary_default,
        )
        if is_view and definition:
            content = (
                content.rstrip()
                + "\n\n## View definition\n```sql\n"
                + definition.strip()
                + "\n```\n"
            )
        action = "created"
    if not content.endswith("\n"):
        content += "\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"{action}: {path}")


def main():
    if len(sys.argv) not in (6, 7):
        sys.exit(
            "usage: _render_table_doc.py <dataset> <out_dir> <columns.json> "
            "<storage.json> <tables.json> [views.json]"
        )
    dataset, out_dir, cols_p, storage_p, tables_p = sys.argv[1:6]
    defs = {}
    if len(sys.argv) == 7:
        for r in load(sys.argv[6]):
            defs[r["table_name"]] = r.get("view_definition") or ""
    os.makedirs(out_dir, exist_ok=True)
    cols = load(cols_p)
    storage = {r["table_name"]: r for r in load(storage_p)}
    tables = load(tables_p)
    by_table = {}
    for c in cols:
        by_table.setdefault(c["table_name"], []).append(c)
    today = datetime.date.today().isoformat()
    for t in tables:
        name = t["table_name"]
        ttype = t.get("table_type", "BASE TABLE")
        tcols = sorted(by_table.get(name, []), key=lambda c: int(c.get("ordinal_position") or 0))
        write_doc(
            dataset, out_dir, name, tcols, storage.get(name, {}), today, ttype, defs.get(name)
        )


if __name__ == "__main__":
    main()

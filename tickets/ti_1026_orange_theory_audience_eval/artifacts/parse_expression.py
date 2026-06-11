#!/usr/bin/env python3
"""Parse the audience.audiences expression for OTF audience 34668 (TI-1026).

Schema: {"interest": {"include": [ {"or":[{"data_source_id":N,"cats":[...]}, ...]}, ... ],
                       "exclude": [ ... same shape ... ]}}
Walks both include and exclude, tallies category ids per data_source_id.
Emits a long CSV: polarity,data_source_id,category_id for downstream BQ joins.
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "outputs"
EXPR = OUT / "ti_1026_audience_34668_expression.json"


def collect_clauses(node, polarity, rows, ds_cat):
    """Recursively find {data_source_id, cats} leaves under a subtree."""
    if isinstance(node, dict):
        if "data_source_id" in node and "cats" in node:
            ds = node["data_source_id"]
            for c in node["cats"]:
                rows.append((polarity, ds, c))
                ds_cat[(polarity, ds)].add(c)
        for v in node.values():
            collect_clauses(v, polarity, rows, ds_cat)
    elif isinstance(node, list):
        for v in node:
            collect_clauses(v, polarity, rows, ds_cat)


def main():
    expr = json.loads(EXPR.read_text())
    rows = []
    ds_cat = defaultdict(set)

    interest = expr.get("interest", {})
    collect_clauses(interest.get("include", []), "include", rows, ds_cat)
    collect_clauses(interest.get("exclude", []), "exclude", rows, ds_cat)

    # Also surface any top-level keys other than interest (geo, device, etc.)
    other_keys = [k for k in expr.keys() if k != "interest"]

    # Long CSV for BQ
    long_csv = OUT / "ti_1026_expression_categories_long.csv"
    with long_csv.open("w") as f:
        f.write("polarity,data_source_id,category_id\n")
        for pol, ds, c in rows:
            f.write(f"{pol},{ds},{c}\n")

    # Summary by (polarity, ds)
    print("=== Top-level expression keys ===")
    print("  interest +", other_keys if other_keys else "(no other top-level keys)")
    print()
    print("=== Categories per data_source_id, by polarity ===")
    print(f"{'polarity':<9} {'ds':>4} {'n_distinct_cats':>16}")
    grand = 0
    for (pol, ds), cats in sorted(ds_cat.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        print(f"{pol:<9} {ds:>4} {len(cats):>16}")
        grand += len(cats)
    print(f"{'TOTAL':<9} {'':>4} {grand:>16}")
    print()
    print(f"Total category references (with dupes): {len(rows)}")
    print(f"Distinct data_source_ids: {sorted({ds for _, ds, _ in rows})}")
    # dump the full interest structure shape (top of include/exclude)
    print()
    print("=== include[] block count:", len(interest.get('include', [])),
          "| exclude[] block count:", len(interest.get('exclude', [])), "===")


if __name__ == "__main__":
    main()

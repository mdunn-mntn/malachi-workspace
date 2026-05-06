#!/usr/bin/env python3
"""
Convert databricks_fangorn_lift.ipynb → databricks_fangorn_lift_dbx.py
(Databricks-source format with `# COMMAND ----------` cell separators).

Why: some Databricks workspaces silently drop .ipynb files from Repos
listings (Jupyter support not enabled). This .py format is universally
recognized as a Databricks notebook regardless of workspace config.

Output filename intentionally differs from the .ipynb's base name to
avoid the Databricks Repos same-base-name collision.

Usage:
    python3 _ipynb_to_databricks_py.py
"""
from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE / "databricks_fangorn_lift.ipynb"
DST = HERE / "databricks_fangorn_lift_dbx.py"


def main():
    nb = json.loads(SRC.read_text())
    lines = ["# Databricks notebook source"]

    for i, cell in enumerate(nb["cells"]):
        if i > 0:
            lines.append("")
            lines.append("# COMMAND ----------")
            lines.append("")

        ctype = cell["cell_type"]
        src = cell.get("source", [])
        if isinstance(src, list):
            src = "".join(src)

        if ctype == "markdown":
            lines.append("# MAGIC %md")
            for line in src.rstrip("\n").split("\n"):
                lines.append(f"# MAGIC {line}" if line else "# MAGIC")
        else:
            for line in src.rstrip("\n").split("\n"):
                lines.append(line)

    DST.write_text("\n".join(lines) + "\n")
    print(f"Wrote {DST} ({len(lines)} lines)")


if __name__ == "__main__":
    main()

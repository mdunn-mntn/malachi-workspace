"""TI-1037 audience / client-performance diagnostic tool.

Productizes the TI-1026 diagnostic (steps 0-9) into a parameterized package. Logic lives here
(importable + unit-testable); `diagnose.py` is the thin CLI that orchestrates it. See the build
plan in ../../summary.md (§5) and the spec in knowledge/audience_diagnostic_playbook.md.
"""

from .expr import (
    Leaf,
    Radius,
    Holdout,
    Score,
    ParsedExpression,
    parse_expression,
)

__all__ = [
    "Leaf",
    "Radius",
    "Holdout",
    "Score",
    "ParsedExpression",
    "parse_expression",
]

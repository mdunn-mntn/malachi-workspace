"""Golden-file regression for the expression parser, locked to the TI-1026 Orange Theory audience.

The v2 segment walker is the highest-risk, most-reused module in the tool, so it gets the hardest
correctness gate: parse the two frozen TI-1026 expression JSONs and assert the exact decomposition
(includes/excludes/gates/holdout/score/geo). Any drift here is a parameterization bug.

Runs standalone (``python test_expr.py``) or under pytest. Fixtures = the committed TI-1026 outputs.
"""

from __future__ import annotations

import os

from expr import parse_file

# Resolve the repo root by walking up until we find the knowledge/ dir, then the TI-1026 fixtures.
_HERE = os.path.dirname(os.path.abspath(__file__))


def _repo_root() -> str:
    d = _HERE
    while d != os.path.dirname(d):
        if os.path.isdir(os.path.join(d, "knowledge")) and os.path.isdir(os.path.join(d, "tickets")):
            return d
        d = os.path.dirname(d)
    raise RuntimeError("could not locate repo root from " + _HERE)


_FIX = os.path.join(
    _repo_root(), "tickets", "ti_1026_orange_theory_audience_eval", "outputs"
)
SEGMENT_JSON = os.path.join(_FIX, "ti_1026_segment_344085_expression.json")  # v2, bidder-operative
AUDIENCE_JSON = os.path.join(_FIX, "ti_1026_audience_34668_expression.json")  # v1, user-facing


def test_v2_segment_decomposition():
    p = parse_file(SEGMENT_JSON)

    assert p.version == "2"
    # interest include = MM keywords (DS19) OR 3P interest (DS35)
    assert p.leaves_by_ds(p.includes) == {19: 379, 35: 11}, p.leaves_by_ds(p.includes)
    # suppression excludes (demo/income/CRM/ISP) — NOT retargeting, NOT the availability gate
    assert p.leaves_by_ds(p.excludes) == {1: 13, 2: 3, 4: 2, 35: 7, 43: 1}, p.leaves_by_ds(p.excludes)

    # the automated clauses the user never sees (segment-only):
    assert p.leaves_by_ds(p.availability_gate) == {14: 1}, "DS14 availability gate"
    rt = {l.data_source_id: l for l in p.retargeting}
    assert set(rt) == {21, 34}, "DS21 conversion + DS34 pageview retargeting"
    assert rt[21].lookback_window == 10368000 and rt[34].lookback_window == 10368000  # 120 days

    assert p.holdout is not None
    assert p.holdout.prefix == "39718:"
    assert p.holdout.num_buckets == 1000
    assert (p.holdout.bucket_beg, p.holdout.bucket_end) == (0, 99)
    assert abs(p.holdout.pct - 10.0) < 1e-9, p.holdout.pct  # buckets 0-99 of 1000 = 10%

    assert p.score is not None and p.score.score_type == "rtc" and p.score.id == 113001

    assert len(p.geo_includes) == 1175, len(p.geo_includes)
    assert len(p.geo_excludes) == 21, len(p.geo_excludes)

    assert p.all_data_sources == [1, 2, 4, 14, 19, 21, 34, 35, 43], p.all_data_sources
    assert p.warnings == [], p.warnings  # clean parse, no unknown ops


def test_v1_audience_decomposition():
    p = parse_file(AUDIENCE_JSON)

    assert p.version == "1"
    assert p.leaves_by_ds(p.includes) == {19: 379, 35: 11}, p.leaves_by_ds(p.includes)
    assert p.leaves_by_ds(p.excludes) == {1: 13, 2: 3, 4: 2, 35: 7, 43: 1}, p.leaves_by_ds(p.excludes)

    # v1 is the USER view: NONE of the segment-only automated clauses are present.
    assert p.availability_gate == []
    assert p.retargeting == []
    assert p.holdout is None
    assert p.score is None

    assert len(p.geo_includes) == 946, len(p.geo_includes)
    assert len(p.geo_excludes) == 21, len(p.geo_excludes)


def test_v1_v2_diff_is_the_automation_layer():
    """The v2-minus-v1 delta is exactly the platform automation (the whole reason to pull both)."""
    v1 = parse_file(AUDIENCE_JSON)
    v2 = parse_file(SEGMENT_JSON)
    # same user-chosen interest + suppression...
    assert v1.leaves_by_ds(v1.includes) == v2.leaves_by_ds(v2.includes)
    assert v1.leaves_by_ds(v1.excludes) == v2.leaves_by_ds(v2.excludes)
    # ...but v2 adds availability gate + retargeting + holdout + score that v1 lacks.
    assert v2.availability_gate and not v1.availability_gate
    assert v2.retargeting and not v1.retargeting
    assert v2.holdout and not v1.holdout
    assert v2.score and not v1.score


if __name__ == "__main__":
    test_v2_segment_decomposition()
    test_v1_audience_decomposition()
    test_v1_v2_diff_is_the_automation_layer()
    print("PASS — expr.py golden file (v1 + v2 + diff) matches frozen TI-1026 decomposition")

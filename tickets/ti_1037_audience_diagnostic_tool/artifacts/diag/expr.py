"""Audience-expression parser (diagnostic steps 0-1) — the unified v1 + v2 walker.

MNTN stores an audience two ways:

  * **v1** `audience.audiences`          — the USER's selections. Top keys ``interest`` / ``geo``;
    interest is ``include`` / ``exclude`` lists of ``{or:[{data_source_id, cats}]}`` (leaf key ``cats``).
  * **v2** `audience.audience_segments`  — what the BIDDER actually evaluates. ``version:"2"`` with a
    ``categories.where`` op-tree (ops and/or/not/any/all; leaf key ``category_ids``), a separate
    ``geos.where`` op-tree of radius leaves, and a ``select[]`` carrying the automated clauses the
    user never sees: the **DS14 availability gate**, the **holdout md5 bucket**, the **RTC score
    directive**, and **DS21/DS34 retargeting** excludes.

The whole tool hinges on reading **v2** — the TI-1026 prototype (`parse_expression.py`) only read v1
(it keys on ``cats``/``data_source_id`` and cannot descend the v2 op-tree), so the deliverable size,
the availability gate, holdout, and retargeting were all invisible to it. Always parse both and diff.

Pure stdlib (json + dataclasses) so it runs anywhere. Fail-soft: unknown ops are logged to
``ParsedExpression.warnings`` and the walk continues rather than raising.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional

# --- semantic data-source classification (see knowledge/data_knowledge.md) -------------------
AVAILABILITY_DS = frozenset({14})        # DS14 cat 1 = the 7-day augmentor-activity availability gate
RETARGETING_DS = frozenset({21, 34})     # DS21 Conversion + DS34 Pageview = advertiser-pixel retargeting
# Boolean-set ops + the "true"/"false" constant clauses (a disabled clause renders as {op:"false"}).
_KNOWN_OPS = frozenset({"and", "or", "not", "any", "all", "true", "false"})


@dataclass
class Leaf:
    """A targeting leaf: one data source + the category ids selected under it."""

    data_source_id: int
    category_ids: list[int] = field(default_factory=list)
    lookback_window: Optional[int] = None  # seconds (retargeting leaves carry this)

    @property
    def n(self) -> int:
        return len(self.category_ids)


@dataclass
class Radius:
    """A geo-fence: a point + radius (studio/location fence)."""

    lat: Optional[float]
    long: Optional[float]
    radius: Optional[float]
    unit: Optional[str]


@dataclass
class Holdout:
    """md5-bucket holdout assignment from ``select[].count``."""

    prefix: Optional[str]
    num_buckets: Optional[int]
    bucket_beg: Optional[int]
    bucket_end: Optional[int]

    @property
    def pct(self) -> Optional[float]:
        """Holdout fraction as a percent, or None if undefined."""
        if self.num_buckets and self.bucket_beg is not None and self.bucket_end is not None:
            return (self.bucket_end - self.bucket_beg + 1) / self.num_buckets * 100.0
        return None


@dataclass
class Score:
    """Score directive from ``select[].score`` (e.g. RTC + vertical id)."""

    score_type: Optional[str]
    id: Optional[int]


@dataclass
class ParsedExpression:
    """Decomposed audience expression — the semantic buckets every downstream step consumes."""

    version: str
    includes: list[Leaf] = field(default_factory=list)        # OR'd interest the audience targets (DS19/DS35/...)
    excludes: list[Leaf] = field(default_factory=list)        # suppression (demo/income/CRM/ISP: DS1/2/4/35/43)
    availability_gate: list[Leaf] = field(default_factory=list)  # DS14 (v2 only) — the platform availability filter
    retargeting: list[Leaf] = field(default_factory=list)    # DS21/DS34 past-visitor/converter excludes (v2 only)
    holdout: Optional[Holdout] = None                        # v2 only
    score: Optional[Score] = None                            # v2 only (RTC directive)
    geo_includes: list[Radius] = field(default_factory=list)
    geo_excludes: list[Radius] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def all_data_sources(self) -> list[int]:
        seen: set[int] = set()
        for grp in (self.includes, self.excludes, self.availability_gate, self.retargeting):
            seen.update(l.data_source_id for l in grp)
        return sorted(seen)

    def leaves_by_ds(self, leaves: list[Leaf]) -> dict[int, int]:
        """{data_source_id: total category_ids} for a leaf list (handy for the report)."""
        out: dict[int, int] = {}
        for l in leaves:
            out[l.data_source_id] = out.get(l.data_source_id, 0) + l.n
        return out

    def category_ids_for(self, data_source_id: int, polarity: str = "include") -> list[int]:
        """Flat list of category ids for one DS on the include or exclude side."""
        src = self.includes if polarity == "include" else self.excludes
        ids: list[int] = []
        for l in src:
            if l.data_source_id == data_source_id:
                ids.extend(l.category_ids)
        return ids

    def summary(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "includes": self.leaves_by_ds(self.includes),
            "excludes": self.leaves_by_ds(self.excludes),
            "availability_gate": self.leaves_by_ds(self.availability_gate),
            "retargeting": {
                l.data_source_id: {"n": l.n, "lookback_window": l.lookback_window}
                for l in self.retargeting
            },
            "holdout": (
                None
                if self.holdout is None
                else {
                    "prefix": self.holdout.prefix,
                    "num_buckets": self.holdout.num_buckets,
                    "bucket_beg": self.holdout.bucket_beg,
                    "bucket_end": self.holdout.bucket_end,
                    "pct": self.holdout.pct,
                }
            ),
            "score": None if self.score is None else {"score_type": self.score.score_type, "id": self.score.id},
            "geo_includes": len(self.geo_includes),
            "geo_excludes": len(self.geo_excludes),
            "data_sources": self.all_data_sources,
            "warnings": self.warnings,
        }


# --- walkers --------------------------------------------------------------------------------

def _walk_categories(node: Any, negate: bool, warnings: list[str]) -> Iterator[tuple[bool, dict]]:
    """Yield ``(negate, leaf_dict)`` for every ``{data_source_id, category_ids}`` leaf.

    ``negate`` tracks polarity: it flips under every ``op:"not"``, so an exclude is identified by
    its enclosing ``not`` nesting — NOT by which top-level list it sits in (the v1 mistake).
    """
    if isinstance(node, list):
        for child in node:
            yield from _walk_categories(child, negate, warnings)
        return
    if not isinstance(node, dict):
        return

    op = node.get("op")
    val = node.get("value")

    # Leaf: value is the {data_source_id, category_ids[, lookback_window]} record.
    if isinstance(val, dict) and "data_source_id" in val:
        yield (negate, val)
        return

    if op is not None and op not in _KNOWN_OPS:
        warnings.append(f"categories: unknown op {op!r} (descending anyway)")

    new_negate = negate != (op == "not")  # XOR
    if val is not None:
        yield from _walk_categories(val, new_negate, warnings)


def _walk_geos(node: Any, negate: bool, warnings: list[str]) -> Iterator[tuple[bool, dict]]:
    """Yield ``(negate, radius_dict)`` for every ``{lat, long, radius, unit}`` leaf."""
    if isinstance(node, list):
        for child in node:
            yield from _walk_geos(child, negate, warnings)
        return
    if not isinstance(node, dict):
        return

    if "radius" in node and "lat" in node:
        yield (negate, node)
        return

    op = node.get("op")
    val = node.get("value")
    if op is not None and op not in _KNOWN_OPS:
        warnings.append(f"geos: unknown op {op!r} (descending anyway)")

    # Radius leaves are carried in a {op:"any", value:{geo_radii:[{lat,long,radius,unit}, ...]}} wrapper.
    if isinstance(val, dict) and "geo_radii" in val:
        for rad in val.get("geo_radii") or []:
            yield (negate, rad)
        return

    new_negate = negate != (op == "not")
    if val is not None:
        yield from _walk_geos(val, new_negate, warnings)


# --- v2: audience.audience_segments (the bidder-operative expression) ------------------------

def _parse_v2(obj: dict) -> ParsedExpression:
    warnings: list[str] = []
    out = ParsedExpression(version="2", warnings=warnings)

    cats_where = (obj.get("categories") or {}).get("where")
    if cats_where is None:
        warnings.append("v2: no categories.where found")
    for negate, leafdict in _walk_categories(cats_where, False, warnings):
        leaf = Leaf(
            data_source_id=leafdict.get("data_source_id"),
            category_ids=list(leafdict.get("category_ids") or []),
            lookback_window=leafdict.get("lookback_window"),
        )
        ds = leaf.data_source_id
        if ds in AVAILABILITY_DS:
            out.availability_gate.append(leaf)        # gate regardless of polarity
        elif negate and ds in RETARGETING_DS:
            out.retargeting.append(leaf)
        elif negate:
            out.excludes.append(leaf)
        else:
            out.includes.append(leaf)

    # select[]: holdout bucket + score directive
    for item in obj.get("select") or []:
        if not isinstance(item, dict):
            continue
        count = item.get("count")
        if isinstance(count, dict) and count.get("name") == "holdout":
            where = count.get("where") or {}
            md5 = ((where.get("value") or {}).get("md5")) or {}
            out.holdout = Holdout(
                prefix=md5.get("prefix"),
                num_buckets=md5.get("num_buckets"),
                bucket_beg=md5.get("bucket_beg"),
                bucket_end=md5.get("bucket_end"),
            )
        score = item.get("score")
        if isinstance(score, dict):
            types = score.get("types") or []
            if types and isinstance(types[0], dict):
                out.score = Score(score_type=types[0].get("score_type"), id=types[0].get("id"))

    # geos.where op-tree
    geos = obj.get("geos") or {}
    geo_node = geos.get("where", geos) if isinstance(geos, dict) else geos
    for negate, rad in _walk_geos(geo_node, False, warnings):
        radius = Radius(lat=rad.get("lat"), long=rad.get("long"), radius=rad.get("radius"), unit=rad.get("unit"))
        (out.geo_excludes if negate else out.geo_includes).append(radius)

    return out


# --- v1: audience.audiences (the user-facing selections) ------------------------------------

def _v1_leaves(blocks: Any) -> Iterator[Leaf]:
    """Walk a v1 interest include/exclude list of ``{or:[{data_source_id, cats}]}``."""
    for block in blocks or []:
        if not isinstance(block, dict):
            continue
        for leafdict in block.get("or") or []:
            if not isinstance(leafdict, dict) or "data_source_id" not in leafdict:
                continue
            yield Leaf(
                data_source_id=leafdict.get("data_source_id"),
                # v1 leaf key is "cats"; tolerate "category_ids" too
                category_ids=list(leafdict.get("cats") or leafdict.get("category_ids") or []),
                lookback_window=leafdict.get("lookback_window"),
            )


def _parse_v1(obj: dict) -> ParsedExpression:
    warnings: list[str] = []
    out = ParsedExpression(version="1", warnings=warnings)
    interest = obj.get("interest") or {}
    out.includes.extend(_v1_leaves(interest.get("include")))
    out.excludes.extend(_v1_leaves(interest.get("exclude")))

    geo = obj.get("geo") or {}
    for rad in geo.get("radii_include") or []:
        out.geo_includes.append(Radius(rad.get("lat"), rad.get("long"), rad.get("radius"), rad.get("unit")))
    for rad in geo.get("radii_exclude") or []:
        out.geo_excludes.append(Radius(rad.get("lat"), rad.get("long"), rad.get("radius"), rad.get("unit")))

    # v1 has no availability gate / holdout / score / retargeting — those are segment-only.
    return out


# --- public entry point ---------------------------------------------------------------------

def parse_expression(obj: dict) -> ParsedExpression:
    """Parse a v1 or v2 audience expression (auto-detected) into a :class:`ParsedExpression`."""
    if not isinstance(obj, dict):
        raise ValueError(f"expression must be a JSON object, got {type(obj).__name__}")
    if str(obj.get("version")) == "2" or "categories" in obj:
        return _parse_v2(obj)
    if "interest" in obj:
        return _parse_v1(obj)
    raise ValueError("unrecognized expression schema (no 'version', 'categories', or 'interest' key)")


def parse_file(path: str) -> ParsedExpression:
    with open(path) as fh:
        return parse_expression(json.load(fh))


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("usage: python expr.py <expression.json>", file=sys.stderr)
        raise SystemExit(2)
    parsed = parse_file(sys.argv[1])
    print(json.dumps(parsed.summary(), indent=2))

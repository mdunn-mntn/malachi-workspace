---
name: geo-axes
description: Geo axes: MNTN US-only so GEO-BROAD-incl is default not an axis; collapse GEO-NARROW-excl into parent, only -incl survives
metadata:
  type: feedback
doc_type: memory
keywords: [geo_axes, geo, axes, mntn, broad, incl, default, axis]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-09
---
## from feedback_us_only_no_geo_broad_axis.md

**Rule:** in any campaign-permutation, bucket, or audience-mix analysis, do NOT treat
GEO-BROAD-incl (positive geo clause referencing a country-level location_id, e.g. US = 237)
as a separate column or label. It's the default — collapse it into the baseline.

**Why:** MNTN only targets US right now (per user 2026-06-01). Every prospecting campaign
either has US as the geo include or has no geo at all (effectively the same thing from
MNTN's perspective). Adding GEO-BROAD-incl as an axis just creates noise — it'd appear on
nearly every row and obscure the signal from buyer-meaningful axes (MM / 3P / CRM /
GEO-NARROW-incl / GEO-NARROW-excl).

**How to apply:**

- Keep `GEO-NARROW-incl` (sub-country: state/DMA/city/ZIP) as a real axis — buyer
  deliberately narrowed.
- Keep `GEO-NARROW-excl` (any geo exclusion clause) as a real axis — buyer carved a
  region out.
- DROP `GEO-BROAD-incl` from columns, labels, and GROUP BY clauses. The dropped axis
  effectively says "always TRUE — assume US."
- `MM + GEO-BROAD-incl` and `MM + (no geo)` collapse into the same row: `MM`.
- Reference: Pass 30 SQL at
  `tickets/ti_999_interest_segment_sizing/queries/ti_999_pass30_perm_matrix_geo_narrow_only.sql`
  is the canonical implementation. Pass 28 / 29 (which kept GEO-BROAD as an axis) are
  deprecated for this purpose.

**Location type reference** (`geo.location_data.location_type_id`):
2 = Country (broad / default — drop); 3 = DMA code, 4 = DMA name, 5 = State/Region,
6 = City, 7 = Sub-city/ZIP (all narrow — keep).

**If MNTN starts targeting non-US** (e.g. Canada, UK), revisit: broad/narrow may then
need to be split by country, or "broad" may need to mean "country-level only" rather
than "US specifically."

**See also:** [[reference-mm-3p-intersection-mechanics]] for the targeting-mechanics
context behind these permutations; [[project-ti-999-strategic-goal]] for the TI-999
deck argument that uses these buckets.

## from feedback_geo_narrow_excl_not_meaningful_axis.md

**Rule:** in campaign-permutation / bucket / audience-mix analyses, do NOT treat
`GEO-NARROW-excl` (any negative geo clause — excluding a state, DMA, city, or ZIP
from a broader target) as a separate axis worth splitting permutation rows on.
Collapse it into the parent pattern.

**Why:** geo exclusions only remove a small slice of the addressable audience
(e.g. excluding California from US-wide is a few % of impressions). It doesn't
substantively change which audience the bidder is targeting — unlike adding 3P,
suppressing CRM, or narrowing the include set. Splitting on it inflates the
permutation count without adding signal (per user, 2026-06-01).

**How to apply:**

- DROP `GEO-NARROW-excl` from columns, labels, and GROUP BY clauses in permutation
  matrices. Treat its presence as a property of the parent pattern, not a separate
  bucket.
- `MM + GEO-NARROW-excl` and `MM` collapse into the same row: `MM`.
- `MM + CRM-AND-excl + GEO-NARROW-excl` and `MM + CRM-AND-excl` collapse together.
- Only `GEO-NARROW-incl` (positive sub-country geo — buyer deliberately narrowed
  to a state/DMA/city/ZIP) survives as a geo axis. That one DOES change who gets
  bid on.
- This is the same logic that drops `GEO-BROAD-incl` per
  [[feedback-us-only-no-geo-broad-axis]] — both are "small-effect" axes that
  fragment the matrix without informing the buyer story.
- Pass 32 (in TI-999) is the canonical implementation. Pass 28-30 are deprecated
  for this reason.

**Caveat:** if a future analysis is specifically about geo coverage / exclusion
behavior (e.g. "do advertisers carving out specific states perform better?"),
then GEO-NARROW-excl becomes the headline axis and should NOT be collapsed.
This rule is for general buyer-permutation views.

**See also:** [[feedback-us-only-no-geo-broad-axis]] (sibling rule for GEO-BROAD);
[[reference-mm-3p-intersection-mechanics]] (load-bearing context for what
"meaningful audience change" means in MM scoring).

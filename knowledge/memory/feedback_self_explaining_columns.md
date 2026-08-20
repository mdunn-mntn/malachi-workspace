---
name: feedback_self_explaining_columns
description: "Deliverable column headers must read without the Read me, and a derived group is labelled with its actual range, never its ordinal position"
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [column header naming, self-explaining columns, xlsx deliverable, rank vs peers, site size group, fourth fifth, quintile label, derived group label, percentile direction, AUDI-1210, readability review]
domain: [workflow, business]
lifecycle: active
last_verified: 2026-08-19
---
**A column header that needs the Read me to decode is a defect, even when the Read me defines it correctly.** Caught on AUDI-1210 (2026-08-19): `Rank vs peers` and `Reading` both had accurate glossary entries and still stopped the reader cold.

**How to apply:**
- **Name the column as the sentence the number answers.** `Rank vs peers` → **Similar sites we beat** (percentile now reads forward: 23% = better than 23 of every 100). `Reading` → **Tracking history**. `Site size group` → **Compared against**.
- **Never label a derived group by ordinal position.** "Smallest fifth / Fourth fifth / Largest fifth" carries no information and "fourth fifth" is hard to even parse. Use the actual range: *Under 25K · 25K to 120K · 120K to 350K · 350K to 1.4M · Over 1.4M*. Applies to score bands, spend tiers, date buckets, any quantile split.
- **A range carries its unit** — header or value, never neither. `Under 25K · Over 1.4M` drew an immediate "over 1.4M *what*?"; landed as header **Compared to sites with** + values **`Under 25K visits` … `Over 1.4M visits`**.
- **Three tests before shipping:** (1) read each header aloud with no context — if "what would this cell contain?" is a guess, rename it; (2) read one value aloud — if it prompts "…of what?", the unit is missing; (3) for a bucket label, can the reader tell *why this row landed there*? Ordinals always fail, ranges always pass.

**Now partly enforced in `lib/mntn_xlsx.py` (`_check_labels`, 2026-08-19):** `table()` raises on placeholder headers (`Reading`, `Value`, `Status`, `Category`, `Group`, `Type`, `Rank`, `Score`, `Band`, `Tier`, `Metric`, `Result`, `Flag`, `Notes`, `Label`, `Class`, `Level`) and on ordinal group labels (`fourth fifth`, `third quintile`, `2nd decile`). It immediately caught `Tier` and `Score` in the INCR-75 workbook, which became **Test readiness** and **Candidate score**. The unit test cannot be automated — no check sees that `Over 1.4M` is missing a noun.

**Why:** the Terse Comms rule bans internal vocabulary the artifact does not define, and these passed that bar by being defined — but a definition the reader must go fetch is still friction. The header itself has to carry the meaning. Full text: `documentation/docs/xlsx_deliverable_standard.md` §"Column headers must read without the Read me". See [[feedback_minimize_complexity]], [[reference_xlsx_master_format]].

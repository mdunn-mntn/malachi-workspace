---
name: feedback_contradictions_are_appended
description: "A new claim that conflicts with a source-verified fact is appended with both sources and the settling check, never overwritten — evidence class decides who wins"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 1960e11a-2e08-4ac0-ae04-7354db2ce0d3
doc_type: memory
keywords: [contradiction handling, append not overwrite, source verified fact, evidence class, verbal claim vs code, stale doc correction, is_shared bidder parity, graph interface, capture protocol, reconciling hypothesis, settling check]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-12
---
When a new claim conflicts with a fact already recorded **from source**, do NOT replace the old line.
**Append: keep both, label each one's evidence (who said it, or which file verified it), state the
hypothesis that reconciles them, and name the specific check that settles it.**

**Evidence class decides who may overwrite whom.** A claim replaces a recorded fact only when it is of the
same or better class — code/data verification replaces code/data verification. **A person's word, however
senior or well-placed, does not silently delete a line verified in code**, and recency is not a class: the
newer statement is not automatically the truer one. This is the write-side companion to
[[feedback_hold_evidenced_verdict]], which governs the same asymmetry in conversation.

**Why:** the /capture reflex is "correct anything now outdated," and the owning team's answer *feels* like
the authoritative update that makes the old line stale. It usually isn't. **The disagreement is normally the
finding** — two credible sources conflicting locates a boundary (a layer, a stage, an environment) that
neither source states on its own. Overwriting destroys exactly that, and destroys it quietly: the next
session inherits a confident, unattributed line with no way to know a conflict ever existed.

**Worked example (AUDI-1049, 2026-08-12).** Recorded from source on 08-11: `SteelHouse/id-service/src/
bigtable.rs` contains **zero** `is_shared` references, so the bidder applies no shared-IP filter. Next day
Jack Barbey (ID team) said the ID Service does not match shared IDs to households. Both hold only if shared
edges are dropped **upstream, when the Bigtable serving copy is loaded**, rather than filtered on read — the
code finding was that the *read path* has no `is_shared` logic, not that shared edges reach it. Written up as
an open item in `knowledge/data_knowledge.md`, epic §7j and [[project_fangorn_on_mntn_id]] with the check
named (read the id-service Bigtable loader). Overwriting would have deleted a verified fact **and** hidden a
real architectural question, and it flipped a downstream decision: whether a shared-IP filter is bidder
parity or a deliberate break from it.

**How to apply:** on any conflict — (1) do not delete; (2) mark the newer claim with its source and date next
to the old one; (3) write the reconciling hypothesis; (4) name the check, concretely enough to execute (a
file to read, a query to run), not "confirm with the team"; (5) until it is run, cite the old fact with its
scope narrowed to what was actually verified ("read-path verified", not "the bidder ignores it"). Rule lives
in global CLAUDE.md §3. Related: [[feedback_hold_evidenced_verdict]], [[reference_ticket_framing_gate]].

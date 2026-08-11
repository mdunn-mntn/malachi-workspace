---
name: reference_build_index_frontmatter_parsing
description: "A ' #' inside a front-matter keywords list silently truncated it, dropping the doc from _ROUTING.md and _MEMORY_RECALL.tsv; fixed in build_index.sh 2026-08-11"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [build_index, front-matter parsing, keywords list, inline yaml comment, doc missing from routing, memory not indexed, _MEMORY_RECALL, _ROUTING gap, parse_front_matter, silent truncation, hash in keywords]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-08-11
---
`build_index.sh`'s `parse_front_matter` stripped inline YAML comments by cutting at the first `" #"` **before** testing whether the value was a bracketed list. Any `keywords:` list containing a literal ` #` — e.g. `[..., START_HERE.md, PR #1, build_index.sh]` — was truncated mid-list, then failed the `endswith(']')` test, was mis-parsed as a scalar string, and the doc was **silently skipped**: absent from `knowledge/_ROUTING.md` and `knowledge/_MEMORY_RECALL.tsv`.

**Impact when it bit:** `project_structured_bq_catalog` was invisible to every grep-the-index retrieval path for an unknown period, and `bq_introspect` / `coverage_state` / `lint_coverage` returned zero hits in `_ROUTING.md`. Found 2026-08-11 by the CLAUDE.md slimdown audit, not by any linter — nothing warns on a skipped doc.

**Fixed:** the comment strip now runs only on the tail after the closing `]` for bracketed values. Both the parser and the offending file were corrected.

**How to apply:**
- After adding or editing any `keywords:` list, run `bash .claude/scripts/build_index.sh` and confirm the doc actually landed: `grep -c <slug> knowledge/_ROUTING.md` must be non-zero.
- Quote any keyword containing `#`, `:`, or `[`/`]`.
- A doc missing from `_ROUTING.md` is a **parse failure, not a keyword-choice problem** — check the front-matter shape first. There is no linter for this class of drop.

Related: [[project_memory_layer_unification]], [[project_structured_bq_catalog]], [[project_hot_path_budget]].

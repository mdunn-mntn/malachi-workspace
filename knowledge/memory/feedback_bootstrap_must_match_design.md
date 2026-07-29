---
name: bootstrap-must-match-design
description: "The bootstrap is a family of procedures, not one algorithm — the variant must match the sampling design (i.i.d. → classical, stratified → stratified bootstrap, cluster → cluster bootstrap, time series → block bootstrap)"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: e28957cb-95c7-4eeb-a32b-fd1c38ef16fb
doc_type: memory
keywords: [bootstrap_must_match_design, bootstrap, must, match, design, family, procedures, algorithm]
domain: [workflow]
lifecycle: active
last_verified: 2026-06-03
---
The bootstrap is not a single algorithm. The classical i.i.d. consistency theorem (Bickel-Freedman 1981, Singh 1981) assumes i.i.d. sampling. Stratified, clustered, or time-series designs technically violate that assumption — each has its own consistency theorem and its own matching bootstrap variant.

**The map:**

| Design | Right bootstrap | Theory |
|---|---|---|
| Pure random sample | Classical (resample N w/ replacement from everyone) | Bickel-Freedman 1981, Singh 1981 |
| Stratified random sample | Stratified bootstrap (resample within each stratum, preserve sizes) | Bickel-Freedman 1984, Rao-Wu 1988 |
| Cluster sampling | Cluster bootstrap (resample clusters as units, not items) | Field & Welsh 2007 |
| Time series | Block bootstrap (resample contiguous blocks) | Künsch 1989 |

**Why it matters:**
- Wrong-variant bootstrap on stratified data → conservative CrIs (wider than they should be). Not invalid, just inefficient — leaving statistical power on the table.
- Right-variant bootstrap → tighter, more accurate CrIs that respect the design.
- Design and analysis must match. If you designed the rollout stratified, analyze it with the stratified bootstrap.

**How to apply:**
- Current TI-961 Tier 2 = pure random sample → classical i.i.d. bootstrap is correct, no change needed
- For future MNTN rollouts designed under the experimental design framework (`documentation/docs/feature_rollout_experimental_design.md`) with stratified randomization → use the stratified bootstrap variant sketched in that doc
- For any new analysis: ALWAYS ask "how was the original sample drawn?" before picking the bootstrap variant. The default `np.random.choice` with replacement is the classical i.i.d. version and only applies when the design is i.i.d. random.

Discovered 2026-06-03 during TI-961 closeout discussion. See [[reference-causal-impact-pattern]] and the experimental design doc.

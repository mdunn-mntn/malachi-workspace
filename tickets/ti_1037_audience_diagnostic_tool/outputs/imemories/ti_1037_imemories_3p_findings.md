# iMemories (aid 37423) — best 3P interest segments? (one-off run of the TI-1037 diagnostic, 2026-06-23)

**Ask:** which 3P interest segments are best for iMemories. **Answer in one line:** on the available data, none of
iMemories' 3P segments are distinguishable by intent — they all overlap iMemories' (near-universal) MNTN-Matched layer
at the **base rate**, so 3P here is broad, largely-redundant reach, not targeted intent. The real lever is the over-broad
MM keyword layer, not the 3P choice.

## Context (must read first)
- **iMemories is dormant.** No live campaign groups (4 ENDED, 4 PAUSED, rest deleted/archived); last activity ~2025-09-11.
  This is an advisory / relaunch analysis, not a live-account tune-up.
- **Their last prospecting ran an HHST gate (6666)** and their **newest audiences (Aug–Sep 2025) are MNTN-Matched-only —
  they had already dropped 3P.** Under an HHST gate, unscored 3P-only IPs are filtered (same mechanism as TI-1026/OTF), so
  3P contributes little to *delivery* regardless of which segments are chosen.
- The 3P segments below come from **older** audiences (May–Jun 2025): "photo interest", "AD-Sample", "Wide Net
  Prospecting w/ Cities", "50-54 Females $150k+", "Mountain Match Narrowed".

## Method
- Decomposed all 25 iMemories audiences (`audience.audiences`) with the TI-1037 parser (`diag/expr.py`) → 15 distinct
  DS35 (3P) segment ids + 211 DS19 (MM) keyword ids.
- For each 3P segment: distinct-IP **reach** over a 30-day ipdsc window (2026-05-24→06-22; ≥30d because 3P delivery is
  bursty) + **overlap** with iMemories' 211-keyword MM universe.
- **Base-rate control:** measured the overlap of three *unrelated* segments (OTF fitness/yoga — irrelevant to a
  photo/memories brand) with iMemories' MM. Queries: `queries/ti_1037_imemories_3p_reach_overlap_30d.sql`,
  `queries/ti_1037_imemories_overlap_baserate_control.sql`. Data: `outputs/imemories/ti_1037_imemories_3p_segment_eval.csv`.

## The decisive finding — overlap is base-rate noise here
| | overlap with iMemories' MM |
|---|---|
| iMemories' MM universe (211 DS19 keywords, 30d) | **174.5M IPs** (≈ most of the US ipdsc population) |
| iMemories' 15 own 3P segments | 67.3% – 72.6% |
| **3 unrelated control segments (OTF fitness)** | **67.1% – 67.5%** |

Unrelated segments overlap iMemories' MM at the **same ~67%** as their own segments → the overlap is driven by MM being
near-universal, **not** by intent alignment. (Contrast OTF, whose MM was small/targeted ~4.6M, so 12% overlap was a real
low-intent signal.) **So the overlap metric cannot rank iMemories' 3P segments** — and a second finding falls out: a
174.5M-IP MM layer is barely targeting at all.

## The 15 segments (ranked by 30-day reach; overlap shown vs the 67.4% base rate)
| role (inferred) | segment_id | reach 30d | overlap | vs base | incremental (non-MM) |
|---|---|--:|--:|--:|--:|
| demo 50-54F $150k+ | 1012012851 | 58.2M | 67.3% | −0.1 | 19.0M |
| prospecting-interest | 1009010221 | 51.4M | 68.8% | +1.4 | 16.0M |
| demo 50-54F $150k+ | 1012160271 | 46.5M | 68.3% | +0.9 | 14.7M |
| prospecting-interest | 1004770179 | 37.1M | 68.3% | +0.9 | 11.8M |
| prospecting-interest | 1004788889 | 30.7M | 68.4% | +1.0 | 9.7M |
| prospecting-interest | 1014997731 | 22.9M | 68.1% | +0.7 | 7.3M |
| MM-narrowing | 1013279451 | 15.6M | 68.6% | +1.2 | 4.9M |
| prospecting-interest | 1009006961 | 14.3M | 68.6% | +1.2 | 4.5M |
| MM-narrowing | 1011030701 | 13.9M | 71.3% | +3.9 | 4.0M |
| **photo-interest** | **1003536679** | 11.7M | **72.6%** | **+5.2** | 3.2M |
| prospecting-interest | 1008179391 | 8.9M | 69.0% | +1.6 | 2.8M |
| prospecting-interest | 1018194591 | 2.8M | 70.7% | +3.3 | 0.83M |
| demo 50-54F $150k+ | 1008486591 | 2.3M | 68.8% | +1.4 | 0.71M |
| demo 50-54F $150k+ | 1012917741 | 1.1M | 68.9% | +1.4 | 0.34M |
| demo 50-54F $150k+ | 1013246001 | 0.10M | 69.6% | +2.2 | 0.03M |

Every segment is within ~5 pts of base rate. The most broadly broad ones (58M/51M/46M) are effectively untargeted national
reach. Exact LiveRamp provider/segment names are **not in BigQuery** (they live in the VPN audience-service catalog —
resolve from the UI if needed); roles above are inferred from the audiences each id sat in.

## What "best 3P" means here, honestly
1. **No 3P segment is data-identifiable as high-intent** — overlap (the signal that worked for OTF) is uninformative
   because iMemories' MM is near-universal, and ipdsc gives membership, not delivered performance.
2. **If iMemories re-adds 3P, the only segments with an a-priori rationale are:** the **photo-interest** segment
   (`1003536679` — the single on-modality one, and marginally the most MM-aligned at +5.2), and the **50-54F / $150k+
   demo** segments (iMemories' actual memory-digitizing buyer profile). The seven generic "prospecting-interest" segments
   are unlabeled broad reach with no rationale beyond size.
3. **The bigger lever is the MM keyword layer, not 3P.** 211 keywords reaching ~174.5M IPs means MNTN Matched is barely
   filtering — tightening/curating those keywords would sharpen targeting far more than any 3P segment choice. (This is
   the same conclusion shape as OTF: grow/clean MM, don't lean on bought 3P.)
4. **Under their HHST gate, 3P barely delivers anyway** — so segment choice is moot for delivery until the gate is lowered.

## Recommendation
- Don't pick "best 3P" by intent — the data can't support a ranking, and all 15 are largely redundant with MM.
- If a 3P sharpener is wanted on relaunch: **photo-interest (1003536679)** + the **demo (50-54F/$150k+)** cluster, used as
  a narrowing overlay, **not** the generic prospecting-interest segments.
- Prioritize **curating the MM keyword layer** (174.5M is not targeting) over any 3P decision.
- Confirm with the account team whether iMemories is relaunching; resolve exact segment names from the audience-service
  UI/catalog if a named recommendation is required.

---
name: feedback_report_both_lift_scales
description: "Relative lift is absolute lift divided by the baseline rate, so a relative-only table hides a baseline that varies across rows — report the absolute difference and the baseline beside it, and cluster by advertiser, or a stakeholder will read four takeaways that do not survive"
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [relative lift, absolute lift, risk difference, log risk ratio, rel_itt, abs_itt, rate_holdout, baseline visit rate, incremental visits per household, advertiser clustering, cluster bootstrap, ghost bid, TI-1313, intent band, lift scale]
domain: [experimentation, incrementality]
lifecycle: active
last_verified: 2026-09-03
---
**`rel_itt = abs_itt / rate_holdout`. A relative-lift-only deliverable is an absolute-lift table silently
divided by a number that varies across its own rows.** When the baseline varies two-fold, the two scales
disagree in sign, and the reader cannot tell which question they were answered.

TI-1313 shipped a 22-sheet attribute workbook on relative lift alone. Matt Brorby read four takeaways off it.
Three did not survive re-testing and one was contradicted, because **every cut he named was a baseline-rate
cut wearing an attribute label** (audience >1M baseline 0.897% vs 1.776%; frequency >2.5 1.289% vs 0.770%).
Audience >1M: relative +0.0318 p=0.0196, absolute **-0.0090 pp** p=0.655. Frequency >2.5: relative p=0.252,
absolute p=0.038. Both flip. In an advertiser-clustered meta-regression only `log(rate_holdout)` survived
(b=-0.0335, p=0.0067), carrying ~2x the weighted R² of all three attributes combined.

The worst case is a **flat ratio over a collapsing yield**: the intent bands looked equal on relative lift
(non-High vs High p=0.79) only because the low bands have a ~4x lower baseline. On incremental visits per
household the same comparison is -0.041 pp (p=0.0013), and within the 37 same-campaign pairs -0.217 pp with
33 of 37 negative.

**Why:** relative lift answers "how much did this lift its own baseline"; absolute answers "where should the
next impression go". Only the second is the media decision, and it is the one a lift table almost never shows.

**How to apply:** on every attribute cut report (1) the absolute risk difference, (2) the relative lift,
(3) the baseline rate per level, and (4) median per-campaign cost per incremental outcome as the tiebreak
(never the pooled spend-weighted version, which inverts — see [[feedback_ivw_vs_median_advertiser]]).
**Cluster by the advertiser, not the campaign**: 190 campaign groups were 130 advertisers, and every contrast
that cleared 0.05 unclustered failed clustered ([[bootstrap-must-match-design]]). Full worked detail in
`knowledge/experimentation.md` §"Relative lift hides the baseline" and §"Cluster by the decision unit".
Related: [[feedback_no_naive_pre_post]], [[feedback_hold_evidenced_verdict]].

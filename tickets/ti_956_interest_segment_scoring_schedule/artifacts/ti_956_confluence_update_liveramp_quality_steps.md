none

Supersedes the prior “Liveramp Segment Quality Steps - Archive”. Each
major check below states why it matters, how it is computed in
principle, then the corresponding repository excerpt.

# 1. What we are trying to measure

Third-party segments are used at massive scale; we cannot trust every ID
equally. The quality stack asks: (a) Is the segment statistically well
supported in the sampled behavioral panel? (b) Is it distinct from near
duplicates? (c) Is it specific rather than ubiquitous? (d) Is metadata
fresh? (e) What fraction of its reach sits on IPs we can actually
target? (f) When campaign delivery data exist, does the segment tend to
appear in contexts where outcomes are strong versus a fair baseline?
None of these alone proves “causal” value, but together they separate
obviously weak or redundant segments from those worth human review or
modeling.

All behavioral metrics are estimated from a deliberately sparse sample.
Horvitz–Thompson-style weights reconstruct totals that would be
prohibitive to compute on full IP×day×segment explode. The composite
score is cohort-relative: z-scores compare segments to each other in the
same run, then a sigmoid maps combined strength to a 0–100 display
scale.

# 2. Summary vs. prior PDF

Same core ideas as the original write-up: Bernoulli edge sampling, HT
weights, 30-day activity, 14-day volatility, average share, ESS, top-k
Jaccard, IDF specificity, staleness.

Additions in code: targetable share of weighted reach; optional
performance layer from campaign×segment association panels; default
weights include targetability and performance; final score uses
standardized z_combo + sigmoid rather than percentile rank of z_combo
alone.

# 3. Code layout

|  |  |
|----|----|
| Topic | Location |
| Sampling (Bernoulli, HT panel) | sampling_quality_sampling.py |
| Shared helpers, default weights | sampling_quality/base.py |
| Reach / activity / volatility / share / ESS | segment_quality_utils/reach.py |
| Top-k Jaccard | segment_quality_utils/uniqueness.py |
| Specificity, staleness, targetable share | segment_quality_utils/attributes.py |
| Performance panels / scores | segment_quality_utils/performance.py |
| End-to-end composite | segment_quality_utils/composite.py |
| Notebook façade | segment_quality_utils/facade.py |

# 4. Sampling methodology

**Intuition.** Full IPDSC explode is too expensive. We retain a random
subset of edges so each kept row stands for many unsampled rows via
weights. Edge-level totals use 1/p; IP-day-level analytics need the
probability that the (ip, day) would appear at all, which depends on how
many distinct segments fired that day—estimated from sampled degree.

**Logic in code.** Rows are kept when a deterministic hash u01(ip,
dscid\[, day\]) \< p. \`hash_scope='edge'\` reuses the same draw across
days; \`edge_day\` draws independently per day.
\`build_edges_with_weights_estimator_only\` estimates m_hat = k_samp/p,
then pi_ipday = 1 − (1−p)^m_hat, rep_ipday = 1/max(pi_ipday, floor),
optional cap. Each sampled edge carries p_edge for segment reach sums.

*Source: sampling_logic.py*

# 5. Shared helpers and default composite weights

**Intuition.** Bounded inputs (probabilities, shares) are stretched
through the logit so a one-point improvement near 0 or 1 is not treated
like one near 0.5. The final quality score squeezes a standardized
combined z through a sigmoid so extreme cohorts do not all pile at 0 or
100.

**Logic in code.** DEFAULT_COMPOSITE_WEIGHTS names the relative
importance of each z-column in z_combo. logit_col and sigmoid_expr are
the shared building blocks for transforms and the final mapping.

*Source:* segment_quality_utils*/base.py*

DEFAULT_COMPOSITE_WEIGHTS:

{  
"activity": 20.0,  
"stability": 9.0,  
"share": 5.0,  
"uniqueness": 25.0,  
"sample": 9.0,  
"staleness": 2.0,  
"specificity": 30.0,  
"targetability": 20.0,  
"performance": 12.0,  
}

# 6. Reach-based checks (activity, volatility, share, ESS)

## 6.1 Population and daily reach

**Intuition.** We need a daily denominator: how many IP-days the panel
represents, and how many of those each segment touches. Without a
defensible denominator, “share” is meaningless.

**Logic in code.** N_ip_hat(d) sums rep_ipday over sampled IP-days on d.
reach_hat(d, s) sums 1/p_edge over sampled edges for segment s.
Variances are carried for diagnostics; core metrics use the point
estimates.

## 6.2 Activity (30-day reach)

**Intuition.** A segment that barely appears in the window is a weak
signal for modeling or targeting, regardless of how “pure” it is.

**Logic in code.** Sum of daily reach_hat over \[as_of − 29d, as_of\].
Larger sums imply more estimated distinct IP×day exposure mass.

## 6.3 Volatility / stability (14-day CV)

**Intuition.** Some segments are steady; others spike on a few days
(bugs, batch uploads, short campaigns). Spiky series are harder to rely
on for always-on use and often indicate data artifacts.

**Logic in code.** Build a complete calendar for the 14-day window,
left-join daily reach, fill missing with 0, then CV = sd/mean. The
composite rewards low CV via −log1p(cv).

## 6.4 Average share (30-day)

**Intuition.** Share answers: of the IP-day universe we estimate in the
window, what fraction does this segment touch? Ubiquitous segments can
be less informative for niche targeting; very tiny share can mean
noise—the score balances this with other axes.

**Logic in code.** Numerator: sum of reach_hat over the window per
dscid. Denominator: single scalar sum of N_ip_hat over the same window.
Ratio is avg_share_30d; composite uses logit(avg_share_30d) so the
direction of “good” matches the cohort z design.

## 6.5 Sampling reliability (ESS proxy)

**Intuition.** HT extrapolation from very few physical samples is
unstable. Counting distinct sampled (ip, day) hits is a simple fidelity
check: did we actually see the segment often enough to trust the
estimates?

**Logic in code.** Distinct count of (dscid, ip, event_date) in the
window after dedupe. Log1p in the composite favors more support without
dominating.

*Source:* segment_quality_utils*/reach.py (lines 1–138)*

# 7. Uniqueness (top-k Jaccard vs. neighbors)

**Intuition.** Vendor taxonomies contain near-duplicate segments. If
five neighbors are almost the same audience, buying one buys them all.
Low mean Jaccard to the closest neighbors suggests incremental coverage.

**Logic in code.** Within each (ip, day), segments co-occurring on that
IP-day imply intersection; HT scales pair counts by 1/p². Marginals use
1/p. J = \|A∩B\|/\|A∪B\| with clipping. Top-k neighbors are chosen by
estimated intersection size; the metric averages mean Jaccard from both
ends of each pair. Uniqueness unit = 1 − mean_topk_jaccard, then logit.

*Source:* segment_quality_utils*/uniqueness.py*

# 8. Specificity, targetable share, staleness

## 8.1 Specificity (IDF-style)

**Intuition.** Treat each IP-day as a document and each segment as a
term. Segments that appear on almost every IP-day are “stopwords”—little
discriminative power. Rare terms get higher IDF.

**Logic in code.** N_hat is total HT IP-day mass in the window (sum
rep_ipday). k_hat per segment is HT count of segment IP-days (sum
1/p_edge). idf = log((N+1)/(k+1)); idf_norm divides by log(N+1) for
scale.

## 8.2 Targetable share

**Intuition.** Reach on IPs outside the targetable universe cannot be
monetized the same way. A segment can be large but mostly “waste”
relative to your addressable graph.

**Logic in code.** HT-weighted reach restricted to IPs appearing in
targetable_ips_df, divided by total HT reach for the segment in the
window. Exposed as pct_targetable_30d; composite uses logit.

## 8.3 Staleness

**Intuition.** Stale or deprecated metadata may describe an audience
that no longer exists or has been renamed. Freshness is a weak but cheap
prior when other data are equal.

**Logic in code.** Exponential decay from last update (optional
created_date fallback), half-life 90d by default. Deprecated flag forces
0; missing dates force 0. Output staleness_unit_score in \[0,1\] before
logit in the composite.

*Source:* segment_quality_utils*/attributes.py (lines 1–149)*

# 9. Optional performance (campaign delivery association)

This block is optional: if \`performance_df\` or
\`campaign_segment_targets\` is missing, the composite skips performance
(z_performance = 0). When present, we measure statistical association
between “campaigns that target segment X” and reported delivery
outcomes—not causal incrementality.

## 9.1 End-to-end pipeline

- build_campaign_segment_targets_panel — Join active campaigns from
  performance to segment target rows. Each campaign×segment row gets
  target_weight from how crowded the campaign is (equal, 1/n_targets, or
  1/√n_targets). Derive rates (visit, conversion, post-visit CVR, ROAS)
  and basic cost metrics.

- add_campaign_performance_lifts — For each campaign, compare its rates
  to an advertiser-level leave-one-out baseline: the advertiser’s other
  campaigns’ totals, excluding this campaign (fallback to global rates
  if LOO is undefined). Log lifts are clipped to limit outlier leverage;
  binary “above baseline” flags summarize direction.

- summarize_performance_by_segment — Roll up to dscid: weighted averages
  of clipped log lifts, medians, dispersion, counts of advertisers and
  campaigns, “campaign_equiv” mass from target weights, spend/impression
  support, concentration (top advertiser share, HHI-style summaries),
  and how often campaigns use few targets (specificity of targeting).

- performance_score_per_segment — Map key segment-level features to unit
  scores u\_\* (sigmoid of median log lifts, consistency from fraction
  above baseline and low dispersion, support from n_advertisers /
  campaign_equiv / spend_equiv, targeting specificity from
  avg_n_segment_targets). Logit → cohort z → weighted z_perf_combo →
  standardized z_perf_combo_std → performance_score_raw on 0–100 via
  sigmoid. advertiser_diversity_proxy = 1 −
  top_advertiser_campaign_equiv_share feeds perf_diversity_confidence
  (down-weights scores dominated by one advertiser). performance_score
  applies that confidence; performance_blend_signal = z_perf_combo_std ×
  perf_diversity_confidence is what the main composite z-scores as
  x_performance.

- evaluate_segment_performance_quality — Runs the four steps above and
  returns campaign_panel, segment_performance_features,
  segment_performance_scores. segment_performance_scores(...) is the
  thin entry used from composite (or pass perf_seg_score_df to skip
  recomputation).

## 9.2 Inputs expected

performance_df: advertiser_id, campaign_id, total_spend, impressions,
visits, conversions, revenue. campaign_segment_targets: advertiser_id,
campaign_id, data_source_category_id (joined as dscid). weight_mode:
'equal' \| 'inv' \| 'inv_sqrt' (default spreads credit when campaigns
target many segments).

## 9.3 Caveats

Lifts are vs. advertiser LOO / global fallbacks, not randomized
holdouts. Segments used on high-performing campaigns can reflect
selection (budget, creative, geography) as much as segment quality. Use
performance as a supportive signal combined with panel-based behavior
and targetability.

## 9.4 Sub-weights inside performance_score_per_segment

These apply to the z-scores of the performance-specific x\_\* columns
(logits of u\_\*), not to the main composite directly:

|                                 |                |
|---------------------------------|----------------|
| Sub-component                   | Default weight |
| conversion_lift                 | 27.0           |
| visit_lift                      | 35.0           |
| consistency                     | 18.0           |
| support                         | 12.0           |
| specificity (crowded targeting) | 8.0            |

*Source:* segment_quality_utils*/performance.py*

# 10. Combined quality score (composite)

**Intuition.** We want one ranking-friendly number while preserving
audit columns. Each axis is transformed to be roughly comparable, then
z-scored within the cohort so a segment is “good” relative to peers in
the same batch. Optional performance feeds the same machinery as other
axes via performance_blend_signal.

**Logic in code.** Join all per-dscid features. Build x\_\* columns
(log1p, logit, −log1p(CV)). Cohort mean/sd per x\_\*; nulls → z=0;
winsor ±5σ. z_combo = Σ weight_i z_i. Re-standardize z_combo to
z_combo_std, then quality_score = round(100×sigmoid(z_combo_std/1.5),
1). Weights default includes targetability and performance keys.

Default composite weights (relative):

|               |        |
|---------------|--------|
| Component     | Weight |
| activity      | 20.0   |
| stability     | 9.0    |
| share         | 5.0    |
| uniqueness    | 25.0   |
| sample        | 9.0    |
| staleness     | 2.0    |
| specificity   | 30.0   |
| targetability | 20.0   |
| performance   | 12.0   |

*Source:* segment_quality_utils*/composite.py*

# 11. Facade entry point

**Intuition.** Notebooks hold a panel on the class; methods delegate to
pure functions so the same logic is testable and importable.

**Logic in code.**
ThirdPartySegmentQuality(panel).quality_score_per_segment(...) passes
through weights, targetable_ips_df, performance inputs, and legacy alias
campaign_ds35_targets.

*Source:* segment_quality_utils*/facade.py*

# 12. Usage and deployment

Build the panel with build_edges_with_weights_estimator_only; deploy all
of segment_quality_utils/ together; add repo root to sys.path for from
segment_quality_utilsimport ThirdPartySegmentQuality.

# 13. Conclusion

Behavioral checks answer “what does the segment look like in the panel?”
Performance answers “where does it show up in delivery data, vs.
reasonable baselines?” Targetability answers “how much of that reach is
on IPs we care about?” Together they form a transparent, auditable
stack; tune weights for product priorities, not for hidden overrides.

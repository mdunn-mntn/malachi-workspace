none

### Overview

Part of integrating third party segments into our systems is evaluate
their quality. There are currently 252,075 different Liveramp segments
and identifying which of these are considered “good” is important to
ensure only quality signals are leveraged in our targeting. This
document will outline what checks were created to consider quality and
how those checks were combined into a unified quality score.

### Sampling Methodology

Data related to Liveramp segments (and most of our data sources) are
stored in IPDSC as dt/IP/array of ID combinations. Pulling and exploding
this data into dt/ip/dscid rows is computationally expensive and
unwieldy for any amount of days given the scale of the data. We want to
be able to use a smaller set of rows but still generate representative
insights. We do that be random sampling and then applying weights which
show how many unsampled rows each sampled row “represents”.

We use Bernoulli sampling where each edge (dt/ip/dscid) has the same
probability p of being selected. This p is specified by the user, and in
our case `p = 0.0001`. Individual ip/day/dscid have a uniform
probability and thus a uniform weight of: 1/p. This can be aggregated
for edge level analytics (segment reach).

For ip/day weights there are a few extra steps.

Each ip/day combination has a different number of dscids associated with
it and thus the chance to appear at all needs to be accounted for. We
count the distinct number of rows for each sampled ip/day and divide
this by p. This gives us an estimated number of total edges per ip/day.
Then we calculate the Horvitz–Thompson (HT) weight as:

\hat{\pi} = 1 - (1 - p)^{\hat{m}}center

Then we can use this estimate to get the weight:

\frac{1}{\hat{\pi}}center

These ip/day weights can be used for analytics that are across ip/days
(sums/averages across days).

For overlaps, a co-occurring pair is sampled with probability p^2center,
so its weight is 1/p^2center. Floors and caps on \hat{\pi}center and
weights control variance at extreme sparsity.

Notebook
[here](https://1262887251702944.4.gcp.databricks.com/editor/notebooks/1904079882625393)
provides more details into this sampling methodology and the validation
steps taken.

### Segment 30day Activity

Total Reach of the segment across the evaluation window.

Scope the segments, are they niche vs. covering many IPs?

pywide760def segment_activity_30d(panel, as_of=None, window_days=30):
"""Sum of daily reach over window (robust recent activity size)."""
reach = daily_reach_by_segment(panel) as_of = as_of or
\_infer_as_of(panel) lo = F.date_sub(F.lit(as_of), window_days-1) r =
reach.filter((F.col("event_date") \>= lo) & (F.col("event_date") \<=
F.lit(as_of))) out =
r.groupBy("dscid").agg(F.sum("reach_hat").alias("reach_hat_30d")) return
out

### Segment Volatility

Across a 14 day window, we calculate the mean and standard deviation of
daily reach for each segment. We then calculate the coefficient of
variability (CV) for each segment as sd/mean.

A CV = 0 is perfectly steady daily IP counts.

A CV around 1 has some variation that will be noticeable.

A CV \> 1 means spikey daily IP counts.

pywide760def segment_volatility_14d(panel, as_of=None, window_days=14):
"""Coefficient of variation of daily reach over a dense window (zeros
included).""" reach = daily_reach_by_segment(panel) as_of = as_of or
\_infer_as_of(panel) lo = F.date_sub(F.lit(as_of), window_days-1) cal =
build_window_calendar(panel) segs = (reach.filter(
(F.col("event_date")\>=lo) & (F.col("event_date")\<=F.lit(as_of)))
.select("dscid").distinct()) grid = segs.crossJoin(cal) dense =
grid.join(reach, \["dscid","event_date"\],
"left").fillna({"reach_hat":0.0}) agg =
dense.groupBy("dscid").agg(F.avg("reach_hat").alias("mean"),
F.stddev_samp("reach_hat").alias("sd")) return agg.select("dscid",
F.when(F.col("mean")\>0,
F.col("sd")/F.col("mean")).otherwise(F.lit(None)).alias("cv14"))

### Segment Average Share

Across the evaluation period, how many of the IPs did each segment
touch? Smaller average share is a more unique segment, higher share
means a broad and probably less accurate segment.

pywide760def segment_avg_share_30d(panel, as_of=None, window_days=30):
"""Window-average share = sum reach_hat / sum N_hat.""" reach =
daily_reach_by_segment(panel) pop = build_daily_ip_population(panel)
as_of = as_of or \_infer_as_of(panel) lo = F.date_sub(F.lit(as_of),
window_days-1) r =
reach.filter((F.col("event_date")\>=lo)&(F.col("event_date")\<=F.lit(as_of)))
n =
pop.filter((F.col("event_date")\>=lo)&(F.col("event_date")\<=F.lit(as_of)))
num = r.groupBy("dscid").agg(F.sum("reach_hat").alias("num")) den =
n.agg(F.sum("N_ip_hat").alias("den")).first()\["den"\] return
num.select("dscid", (F.col("num") / F.lit(den)).alias("avg_share_30d"))

### Segment Sampling Reliability

The distinct number of sampled rows. Less samples means the estimates
may not be as reliable.

pywide760def segment_ess_30d(panel, as_of=None, window_days=30): """ESS
proxy = \# of sampled (ip, day) hits for the segment in the window."""
as_of = as_of or \_infer_as_of(panel) lo = F.date_sub(F.lit(as_of),
window_days-1) hits = (panel.filter((F.col("event_date")\>=lo) &
(F.col("event_date")\<=F.lit(as_of))) .select("dscid","ip","event_date")
.dropDuplicates(\["dscid","ip","event_date"\])) return
hits.groupBy("dscid").agg(F.count(F.lit(1)).alias("ess_30d"))

### Segment Jaccard Similarity to Neighbors

Finds the five most similar segments for each segment and then takes the
mean Jaccard similarity across those five segments to say “compared to
my closest neighbors, how similar am I?”. A smaller mean Jaccard
similarity means more unique segment.

pywide760def segment_uniqueness_topk_jaccard_30d( panel, as_of=None,
window_days=30, topk=5, min_intersection_ht=5.0, deg_min=2,
clip_ab_to_min_marginals=True, \# enforce AB_hat \<= min(A_hat_a,
A_hat_b) eps_union=1e-9, \# keep union positive ): """ Returns per
dscid: - mean_topk_jaccard_30d (avg of J over top-K neighbors by AB_hat
from both sides) - neighbors_topk: array\<struct\<neighbor, j, ab_hat,
self_hat, neighbor_hat, rank_by_ab\>\> - neighbors_all:
array\<struct\<neighbor, j, ab_hat, self_hat, neighbor_hat\>\> Notes: \*
Edge sampling at global rate p -\> HT weights: \|A\| with 1/p, \|A∩B\|
with 1/p^2. \* Marginals are computed from edges (so deg=1 days count
properly). \* Intersections are built within each (ip, day) over
distinct dscids. """ \# ---- window bounds ---- as_of = as_of or
\_infer_as_of(panel) lo = F.date_sub(F.lit(as_of), window_days - 1) \#
---- global p (constant for edges) ---- p =
panel.select(F.max(F.col("p_edge").cast("double"))).first()\[0\] if p is
None or p \<= 0.0 or p \> 1.0: raise ValueError("p_edge must be in
(0,1\] and present in panel.") inv_p = 1.0 / p inv_p2 = inv_p \* inv_p
\# ---- windowed distinct edges (ip, day, dscid) ---- e = ( panel
.select("ip", F.to_date("event_date").alias("event_date"), "dscid")
.filter((F.col("event_date") \>= lo) & (F.col("event_date") \<=
F.lit(as_of))) .dropDuplicates(\["ip", "event_date", "dscid"\]) .cache()
) \# ---- marginals \|A\| (HT via count \* 1/p) ---- hits = (
e.groupBy("dscid") .agg((F.count(F.lit(1)) \*
F.lit(inv_p)).alias("A_hat")) .cache() ) \# ---- per-(ip, day) set of
segments ---- ipday = e.groupBy("ip",
"event_date").agg(F.collect_set("dscid").alias("S")) if deg_min is not
None and deg_min \> 1: ipday = ipday.filter(F.size("S") \>=
F.lit(deg_min)) \# ---- unordered pairs within each ip-day ---- L =
ipday.select("ip", "event_date", F.posexplode("S").alias("i",
"a")).alias("L") R = ipday.select("ip", "event_date",
F.posexplode("S").alias("j", "b")).alias("R") pairs = ( L.join(R,
on=\["ip", "event_date"\]) .filter(F.col("R.j") \> F.col("L.i"))
.select( F.least(F.col("L.a"), F.col("R.b")).alias("a"),
F.greatest(F.col("L.a"), F.col("R.b")).alias("b") ) ) \# ----
intersections \|A∩B\| (HT via count \* 1/p^2) ---- inter =
pairs.groupBy("a", "b").agg((F.count(F.lit(1)) \*
F.lit(inv_p2)).alias("AB_hat")) if min_intersection_ht is not None:
inter = inter.filter(F.col("AB_hat") \>= F.lit(min_intersection_ht))
inter = inter.cache() \# Early empty -\> return typed empty if
inter.rdd.isEmpty(): spark = panel.sql_ctx.sparkSession schema =
StructType(\[ StructField("dscid", LongType(), True),
StructField("mean_topk_jaccard_30d", DoubleType(), True),
StructField("neighbors_topk", ArrayType( StructType(\[
StructField("neighbor", LongType(), True), StructField("j",
DoubleType(), True), StructField("ab_hat", DoubleType(), True),
StructField("self_hat", DoubleType(), True), StructField("neighbor_hat",
DoubleType(), True), StructField("rank_by_ab", IntegerType(), True), \])
), True), StructField("neighbors_all", ArrayType( StructType(\[
StructField("neighbor", LongType(), True), StructField("j",
DoubleType(), True), StructField("ab_hat", DoubleType(), True),
StructField("self_hat", DoubleType(), True), StructField("neighbor_hat",
DoubleType(), True), \]) ), True), \]) return
spark.createDataFrame(\[\], schema) \# ---- join marginals, compute J
---- ab = ( inter.join(hits.withColumnRenamed("dscid", "a")
.withColumnRenamed("A_hat", "A_hat_a"), on="a")
.join(hits.withColumnRenamed("dscid", "b") .withColumnRenamed("A_hat",
"A_hat_b"), on="b") ) if clip_ab_to_min_marginals: ab =
ab.withColumn("AB_hat", F.least(F.col("AB_hat"), F.col("A_hat_a"),
F.col("A_hat_b"))) ab = ( ab.withColumn("AUB_hat_raw",
F.col("A_hat_a") + F.col("A_hat_b") - F.col("AB_hat"))
.withColumn("AUB_hat", F.greatest(F.col("AUB_hat_raw"),
F.lit(eps_union))) .withColumn("J", F.col("AB_hat") / F.col("AUB_hat"))
.cache() ) \# ---- ALL evaluated neighbors (post-threshold, post-union
clamp) ---- all_a = ( ab.groupBy("a") .agg(F.collect_list(F.struct(
F.col("b").alias("neighbor"), F.col("J").alias("j"),
F.col("AB_hat").alias("ab_hat"), F.col("A_hat_a").alias("self_hat"),
F.col("A_hat_b").alias("neighbor_hat") )).alias("neighbors_all"))
.withColumnRenamed("a", "dscid") ) all_b = ( ab.groupBy("b")
.agg(F.collect_list(F.struct( F.col("a").alias("neighbor"),
F.col("J").alias("j"), F.col("AB_hat").alias("ab_hat"),
F.col("A_hat_b").alias("self_hat"),
F.col("A_hat_a").alias("neighbor_hat") )).alias("neighbors_all"))
.withColumnRenamed("b", "dscid") ) all_union = (
all_a.unionByName(all_b, allowMissingColumns=True) .groupBy("dscid")
.agg(F.flatten(F.collect_list("neighbors_all")).alias("neighbors_all"))
) \# ---- TOP-K neighbors by intersection per side, with rank ---- w_a =
Window.partitionBy("a").orderBy(F.desc("AB_hat")) top_a = (
ab.withColumn("rank_by_ab", F.row_number().over(w_a))
.filter(F.col("rank_by_ab") \<= F.lit(topk)) .groupBy("a") .agg(
F.avg("J").alias("mean_topk_J"), F.collect_list(F.struct(
F.col("b").alias("neighbor"), F.col("J").alias("j"),
F.col("AB_hat").alias("ab_hat"), F.col("A_hat_a").alias("self_hat"),
F.col("A_hat_b").alias("neighbor_hat"), F.col("rank_by_ab")
)).alias("neighbors_topk") ) .withColumnRenamed("a", "dscid") ) w_b =
Window.partitionBy("b").orderBy(F.desc("AB_hat")) top_b = (
ab.withColumn("rank_by_ab", F.row_number().over(w_b))
.filter(F.col("rank_by_ab") \<= F.lit(topk)) .groupBy("b") .agg(
F.avg("J").alias("mean_topk_J"), F.collect_list(F.struct(
F.col("a").alias("neighbor"), F.col("J").alias("j"),
F.col("AB_hat").alias("ab_hat"), F.col("A_hat_b").alias("self_hat"),
F.col("A_hat_a").alias("neighbor_hat"), F.col("rank_by_ab")
)).alias("neighbors_topk") ) .withColumnRenamed("b", "dscid") ) \# ----
merge sides: average the means, concatenate arrays ---- top_union = (
top_a.select("dscid", "mean_topk_J", "neighbors_topk")
.unionByName(top_b.select("dscid", "mean_topk_J", "neighbors_topk"),
allowMissingColumns=True) .groupBy("dscid") .agg(
F.avg("mean_topk_J").alias("mean_topk_jaccard_30d"),
F.flatten(F.collect_list("neighbors_topk")).alias("neighbors_topk") ) )
\# ---- final join: add neighbors_all ---- out = (
top_union.join(all_union, on="dscid", how="left") .select("dscid",
"mean_topk_jaccard_30d", "neighbors_topk", "neighbors_all") ) return out

### Segment Specificity

This is calculating IDF for each segment. The idea being each IP-day is
a “document” and the segment is the “term” so we evaluate how rare it is
to see a segment across documents (IP-days). Close to 1 is an extremely
rare segment. Close to zero is a totally ubiquitous segment which is
associated with all IPs.

pywide760def segment_specificity_30d(panel, as_of=None, window_days=30,
eps=1e-12): """Per-segment specificity over a window using HT estimates
for N and k. Returns: dscid, N_hat_30d, k_hat_30d, p_hat, idf,
entropy_bits, ubiq_flag, specificity_unit """ if as_of is None: as_of =
\_infer_as_of(panel) lo = F.date_sub(F.lit(as_of), window_days-1) N_hat
= (
panel.filter((F.col("event_date")\>=lo)&(F.col("event_date")\<=F.lit(as_of)))
.agg(F.sum("rep_ipday").alias("N_hat")).first()\["N_hat"\] or 0.0 )
edges = (panel
.filter((F.col("event_date")\>=lo)&(F.col("event_date")\<=F.lit(as_of))))
k_by_seg =
edges.groupBy("dscid").agg(F.sum(1.0/F.col("p_edge")).alias("k_hat_30d"))
Np1 = float(N_hat) + 1.0 out = (k_by_seg .withColumn("N_hat_30d",
F.lit(float(N_hat))) .withColumn("p_hat", F.when(F.col("N_hat_30d")\>0,
F.col("k_hat_30d")/F.col("N_hat_30d")).otherwise(F.lit(0.0)))
.withColumn("idf", F.log((F.col("N_hat_30d")+F.lit(1.0)) /
(F.col("k_hat_30d")+F.lit(1.0)))) .withColumn("p_safe",
F.when(F.col("p_hat")\<=0, F.lit(eps)).when(F.col("p_hat")\>=1,
F.lit(1.0-eps)).otherwise(F.col("p_hat"))) .withColumn("idf_norm",
F.col("idf") / F.lit(F.log(F.lit(Np1))))
.select("dscid","N_hat_30d","k_hat_30d","p_hat","idf", "idf_norm") )
return out

### Segment Staleness

A score from 0 to 1 based on days since last updated in the
`tpa.categories` metadata table. This value is also set to zero if the
`deprecated` flag is set to True. Scores near 1 mean the segment was
recently updated. Scores near 0 mean the segment is deprecated, missing
dates, or hasn’t been updated in awhile.

pywide760def segment_staleness(panel, seg_meta_df, as_of=None,
half_life_days: int = 90, fallback_to_created: bool = True,
deprecated_zero: bool = True): """Days since last update + exponential
half-life unit score in \[0,1\].""" if as_of is None: as_of =
\_infer_as_of(panel) upd = F.col("updated_date") if fallback_to_created:
upd = F.coalesce(F.col("updated_date"), F.col("created_date")) days =
F.greatest(F.datediff(F.lit(as_of), upd), F.lit(0)) ln2 =
0.6931471805599453 score = F.exp(-F.lit(ln2) \* (days.cast("double") /
F.lit(float(half_life_days)))) if "deprecated" in seg_meta_df.columns
and deprecated_zero: score = F.when(F.col("deprecated") == True,
F.lit(0.0)).otherwise(score) score = F.when(upd.isNull(),
F.lit(0.0)).otherwise(score) out = (seg_meta_df .select(
F.col("data_source_category_id").cast("long").alias("dscid"),
F.col("updated_date"), F.col("created_date"), F.col("deprecated") if
"deprecated" in seg_meta_df.columns else
F.lit(False).alias("deprecated") ) .withColumn("updated_effective",
F.when(upd.isNull(), F.lit(None)).otherwise(upd))
.withColumn("days_since_update", days.cast("int"))
.withColumn("staleness_unit_score", score.cast("double"))
.select("dscid", "updated_effective", "days_since_update",
"staleness_unit_score") ) return out

### Combined Quality Score

This function takes all the above quality scores, weights and combines
them into a single value. The weights are how much importance we put on
each check. The steps for generating quality score:

- We join all the quality scores into a single dataframe.

- Then do monotone transformations to stabilize scale of results and
  ensure they all fit the “higher is better” structure.

- We then use z-scores to standardize each quality score (and cap them
  to 5).

- Next we take the weighted sum of scores sum(quality metric \* weight).

- Map these combined scores to percentiles so score 100 == best and
  score 0 == worst.

pywide760def quality_score_per_segment( panel, seg_meta_df, as_of=None,
window_days=30, topk_for_uniqueness=5, weights=None, eps=1e-6,
winsor_z=5.0, ): """ Combine components into a transparent 0–100 segment
quality score. """ if weights is None: weights = { "activity": 20.0,
"stability": 9.0, "share": 5.0, "uniqueness": 25.0, "sample": 9.0,
"staleness": 2.0, "specificity": 30.0 } as_of = as_of or
\_infer_as_of(panel) act = segment_activity_30d(panel, as_of=as_of,
window_days=window_days) cv14 = segment_volatility_14d(panel,
as_of=as_of, window_days=14) shr = segment_avg_share_30d(panel,
as_of=as_of, window_days=window_days) ess = segment_ess_30d(panel,
as_of=as_of, window_days=window_days) uniq =
segment_uniqueness_topk_jaccard_30d(panel, as_of=as_of,
window_days=window_days, topk=topk_for_uniqueness) stale =
segment_staleness(panel, seg_meta_df, as_of=as_of, half_life_days=90)
spec = segment_specificity_30d(panel, as_of=as_of,
window_days=window_days) comp = (act.join(cv14, "dscid", "left")
.join(shr, "dscid", "left") .join(ess, "dscid", "left") .join(uniq,
"dscid", "left") .join(stale, "dscid", "left") .join(spec, "dscid",
"left") ) def logit(col): c = F.least(F.greatest(F.col(col),
F.lit(eps)), F.lit(1.0 - eps)) return F.log(c / (1.0 - c)) comp = (comp
.withColumn("uniqueness_unit",F.least(F.greatest(F.lit(1.0) -
F.col("mean_topk_jaccard_30d"), F.lit(eps)), F.lit(1.0 - eps)))
.withColumn("x_activity", F.log1p(F.col("reach_hat_30d")))
.withColumn("x_stability", -F.log1p(F.col("cv14"))) \# low cv -\> larger
value .withColumn("x_share", logit("avg_share_30d"))
.withColumn("x_uniqueness", logit("uniqueness_unit")) \# low jaccard -\>
larger value .withColumn("x_sample", F.log1p(F.col("ess_30d")))
.withColumn("x_staleness", logit("staleness_unit_score"))
.withColumn("x_specificity", logit("idf_norm")) ) \# ---- compute
mean/std for each transformed metric stats = comp.agg(
\*\[F.avg(c).alias(f"mu\_{c}") for c in \[
"x_activity","x_stability","x_share","x_uniqueness","x_sample","x_staleness","x_specificity"
\]\], \*\[F.stddev_samp(c).alias(f"sd\_{c}") for c in \[
"x_activity","x_stability","x_share","x_uniqueness","x_sample","x_staleness","x_specificity"
\]\] ).first() \# z-score with: NULL -\> z=0 (neutral), sd=0 -\> z=0,
then winsorize def z(colname): mu = float(stats\[f"mu\_{colname}"\]) if
stats\[f"mu\_{colname}"\] is not None else 0.0 sd =
float(stats\[f"sd\_{colname}"\] or 0.0) zraw = (F.col(colname) -
F.lit(mu)) / F.lit(sd) if sd \> 0 else F.lit(0.0) zraw =
F.when(F.col(colname).isNull(), F.lit(0.0)).otherwise(zraw) return
F.least(F.greatest(zraw, F.lit(-winsor_z)), F.lit(winsor_z)) comp =
(comp .withColumn("z_activity", z("x_activity"))
.withColumn("z_stability", z("x_stability")) .withColumn("z_share",
z("x_share")) .withColumn("z_uniqueness", z("x_uniqueness"))
.withColumn("z_sample", z("x_sample")) .withColumn("z_staleness",
z("x_staleness")) .withColumn("z_specificity", z("x_specificity")) )
comp = (comp .withColumn("z_combo", F.lit(weights\["activity"\]) \*
F.col("z_activity") + F.lit(weights\["stability"\]) \*
F.col("z_stability") + F.lit(weights\["share"\]) \* F.col("z_share") +
F.lit(weights\["uniqueness"\]) \* F.col("z_uniqueness")+
F.lit(weights\["sample"\]) \* F.col("z_sample") +
F.lit(weights\["staleness"\]) \* F.col("z_staleness") +
F.lit(weights\["specificity"\]) \* F.col("z_specificity") ) ) \# ----
map to 0–100 via percentile rank (relative to cohort) out = (comp
.withColumn("pct", F.cume_dist().over(Window.orderBy("z_combo")))
.withColumn("quality_score", F.round(100 \* F.col("pct"), 1)) .select(
"dscid","quality_score", \# optional: expose z's and components for
debugging/audits
"z_activity","z_stability","z_share","z_uniqueness","z_sample","z_staleness","z_specificity",
"z_combo",
"reach_hat_30d","cv14","avg_share_30d","ess_30d","mean_topk_jaccard_30d",
"staleness_unit_score","idf_norm") ).distinct() return out

### Conclusion

We are able to evaluate ~250,000 Liveramp segments and assign them a
quality score and ranking for use in our targeting systems.

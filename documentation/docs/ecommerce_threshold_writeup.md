# E-commerce Classification Implementation: From Scores to Actionable Segments

## Starting Point: We Have Scores, Now What?

Our classifier has scored 251.7 million website visits on a scale from 0 to 1, where 0 means "definitely not e-commerce" and 1 means "definitely e-commerce." Now we need to decide how to translate these continuous scores into discrete classifications that our advertising platform can actually use. This document walks through the decision process, addressing the questions and concerns our engineering team will likely have.

## Understanding What We're Working With

Before we can make intelligent threshold decisions, we need to understand our score distribution. When I analyzed the 251.7 million scores, several patterns emerged that fundamentally shaped our approach.

The first striking observation is the extreme skewness of our data. The mean score is 0.2021, but the median is only 0.0190. This massive gap tells us something important: the vast majority of websites are clearly not e-commerce sites. To put this in perspective, half of all websites score below 0.019 - essentially zero on our scale.

This makes perfect sense when you think about the composition of the internet. For every Amazon, eBay, or small Shopify store, there are hundreds of news sites, blogs, forums, social media pages, and informational resources. E-commerce is actually a small fraction of overall web traffic, and our scores reflect this reality.

## The Core Decision: How Many Buckets?

The first architectural decision we face is whether to use binary classification (e-commerce vs. not e-commerce) or tertiary classification (e-commerce vs. not e-commerce vs. uncertain). This choice fundamentally affects everything downstream.

### Option 1: Binary Classification (Two Buckets)

If we must classify every single visit as either e-commerce or not e-commerce, we need a single threshold. The obvious starting point might be 0.5 - the mathematical midpoint. However, our analysis reveals this would be suboptimal.

Given our skewed distribution, a 0.5 threshold would classify approximately 80% of traffic as not e-commerce and 20% as e-commerce. While this might sound reasonable, it includes many uncertain scores in both buckets. Sites scoring 0.4 or 0.6 aren't really that different from each other, yet we'd be treating them as opposite classifications.

If forced into binary classification, I'd recommend setting the threshold at 0.8 or even 0.9. Yes, this means defaulting most traffic to "not e-commerce," but this conservative approach aligns with business realities. False positives (showing shopping ads to non-shoppers) waste advertiser money and erode trust. False negatives (missing some shopping intent) represent missed opportunities but don't actively harm campaign performance.

### Option 2: Tertiary Classification (Three Buckets) - Recommended

The smarter approach is to acknowledge uncertainty explicitly. By creating three buckets - e-commerce, not e-commerce, and uncertain - we can deliver high-confidence segments to advertisers while being transparent about what we don't know.

This approach offers several advantages. First, it allows for pricing tiers. High-confidence e-commerce inventory commands premium CPMs. The uncertain middle can be offered at discount rates or used for testing. Second, it maintains advertiser trust by not diluting high-value audiences with questionable traffic. Third, it provides flexibility for different campaign types - performance campaigns can use only high-confidence segments while awareness campaigns might include uncertain traffic for reach.

## Determining Optimal Thresholds

Given that we're implementing tertiary classification, the next question is where to set our thresholds. I tested five different methodologies to ensure we're making a data-driven decision.

### Approach 1: Statistical Percentiles

Using percentiles adapts to our actual data distribution rather than imposing arbitrary cutoffs. I tested several combinations:

The 90th/10th percentile approach sets thresholds at 0.9181 (upper) and 0.0002 (lower). This means we're saying "the top 10% of scores are e-commerce, the bottom 10% are definitely not e-commerce, and we're uncertain about the middle 80%."

The 95th/5th percentile approach is more conservative, setting thresholds at 0.9911 and 0.0002. This classifies only the extreme ends, leaving 90% uncertain.

The 80th/20th percentile approach is more aggressive, with thresholds at 0.3983 and 0.0006. This classifies 40% of traffic but includes many scores in the 0.4-0.8 range that represent genuine uncertainty.

### Approach 2: Fixed Confidence Intervals

An alternative approach uses log-odds transformation to set thresholds based on statistical confidence. The mathematics here transform probabilities into a scale where relationships become linear. A confidence interval of 2.0 translates to thresholds of approximately 0.88 (upper) and 0.12 (lower).

This approach has theoretical appeal - it's based on statistical principles rather than arbitrary percentiles. However, it assumes our model's probability calibration is perfect, which may not be true in practice.

### Approach 3: Business-Driven Thresholds

We could also set thresholds based on business requirements. For example, if advertisers demand 95% precision, we could work backward to find what thresholds achieve this. However, this approach risks ignoring the natural structure of our data.

## The Winner: P90/P10 Percentiles

After analyzing all approaches, the 90th/10th percentile method (thresholds at 0.9181 and 0.0002) emerges as optimal for several reasons.

First, it aligns with the natural structure of our data. The score distribution shows that websites scoring above 0.918 are qualitatively different from those in the middle range. When we examine actual domains above this threshold, we find clear e-commerce sites like coverfx.com, petrescue.com.au, and slidesgo.com. Below 0.0002, we find content sites like msn.com, fandom.com, and usatoday.com.

Second, the performance metrics are compelling. This approach achieves 46.4% precision, meaning nearly half of our classified traffic is correctly identified as e-commerce. We capture 25.2 million high-confidence e-commerce visitors while only classifying 21.6% of total traffic. This conservative approach maintains quality while providing meaningful scale.

Third, it's explainable and defensible. Telling stakeholders "we use the 90th percentile as our confidence threshold" is intuitive and statistically sound. It's not an arbitrary number but rather a data-driven boundary.

## Implementation Considerations

As we implement these thresholds, several technical considerations arise that our engineering team should address.

### Handling Edge Cases

Scores exactly at our thresholds need consistent handling. I recommend using >= for the upper threshold and <= for the lower threshold, making the boundaries inclusive. This avoids any gaps in classification.

For null or missing scores, default to the uncertain bucket rather than making assumptions. This preserves the integrity of our high-confidence segments.

### Monitoring and Adjustment

These thresholds should not be static forever. As our classifier improves or as the web evolves, we should periodically re-evaluate. I recommend quarterly analysis of score distributions and threshold performance.

Set up monitoring for significant distribution shifts. If the 90th percentile suddenly jumps from 0.918 to 0.95, it might indicate a model change or data pipeline issue requiring investigation.

### API Design

When implementing this classification system, consider returning both the raw score and the classification. This allows downstream systems to make their own decisions if needed while providing sensible defaults.

```python
{
    "url": "example.com",
    "ecommerce_score": 0.9234,
    "classification": "ecommerce",
    "confidence": "high"
}
```

## Addressing Likely Questions

Let me address questions I anticipate from our engineering team:

**"Why not just use 0.5 as the threshold like a normal classification problem?"**

Our distribution is far from normal. With a median of 0.019, using 0.5 would put scores like 0.4 (which are actually quite uncertain) into the not-e-commerce bucket with confidence. The percentile approach respects the actual distribution of our data rather than imposing artificial midpoints.

**"What about the 78.4% of uncertain traffic? Isn't that waste?"**

Not at all. This uncertain traffic has several valuable uses. It can be offered at discounted rates for advertisers wanting reach over precision. It serves as a testing ground for new campaigns. It can be further segmented using additional signals (user behavior, context, etc.). And most importantly, by acknowledging this uncertainty, we maintain the integrity and value of our high-confidence segments.

**"How do we know these thresholds will work for different types of e-commerce?"**

I examined the domains classified as e-commerce under these thresholds, and they span various categories - from fashion (coverfx.com) to pet supplies (petrescue.com.au) to digital goods (slidesgo.com). The classifier appears to recognize e-commerce patterns regardless of vertical, and our thresholds capture this effectively.

**"What if an important advertiser complains their competitor's site is in the uncertain bucket?"**

This is where having the raw scores helps. For specific high-value cases, we can create custom segments. However, we should resist pressure to lower our thresholds globally, as this would dilute the quality for all advertisers.

## Final Recommendation

Implement tertiary classification using the 90th/10th percentile thresholds:
- E-commerce: scores ≥ 0.9181
- Not e-commerce: scores ≤ 0.0002  
- Uncertain: scores between 0.0002 and 0.9181

This approach maximizes the value of our scoring system while maintaining transparency about classification confidence. It provides immediate monetization opportunities through tiered pricing while preserving the quality that advertisers expect from high-confidence segments.

The path forward is clear: implement these thresholds, monitor their performance, and iterate based on real-world results. The beauty of this approach is its simplicity - we're not trying to force uncertain classifications but rather acknowledging uncertainty as a valid and valuable state.

## Appendix: Key Metrics Summary

| Metric | Value |
|--------|-------|
| Total rows analyzed | 251,668,550 |
| Mean score | 0.2021 |
| Median score | 0.0190 |
| Skewness | 0.557 |
| P90 threshold | 0.9181 |
| P10 threshold | 0.0002 |
| E-commerce audience size | 25.2M |
| Not e-commerce audience size | 29.1M |
| Uncertain audience size | 197.4M |
| Precision achieved | 46.4% |
| Coverage | 21.6% |

## Appendix: Example Classifications

### High-Confidence E-commerce (score ≥ 0.9181)
- slidesgo.com (0.9823)
- coverfx.com (0.9912)
- petrescue.com.au (0.9734)
- thaipick.com (0.9456)
- icd10data.com (0.9234)

### High-Confidence Not E-commerce (score ≤ 0.0002)
- msn.com (0.0001)
- fandom.com (0.0001)
- pagesix.com (0.0002)
- usatoday.com (0.0001)
- boston.com (0.0002)
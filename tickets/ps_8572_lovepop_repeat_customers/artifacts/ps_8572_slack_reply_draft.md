# Slack reply draft (PS-8572 thread, for Malachi to paste)

Double-checked everything in BQ. Bottom line, the blocks are set up correctly and always were, but the CRM exclusion itself wasn't. Their main customer list (the 3.6M April upload) was only attached to the campaign's exclusion on 7/16. Before 6/30 there was no CRM exclusion on the campaign at all, and from 6/30 to 7/16 only the small 341K list from the 6/29 upload was excluded. So anyone on the April list was targetable in prospecting until 7/16. That's the one real gap. Everything else in their report is working as designed.

On my 5 points:

1. Blocks and config. Verified on continuously since April 14. Current exclusion expression is correct too (both lists, right polarity). The gap above is a wiring timeline issue, not a config issue.
2. Definition of repeat. Their own export proves the 30d conversion window is doing this. Zero of their 2,290 rows violate our windows, and the max visit to conversion gap in their own data is exactly 30.0 days. The "1 impression producing 5 orders over 17 days" pattern is 5 purchases inside 30 days of one verified visit, attributed as designed. Also their samples are heavy repeat buyers, 3 to 88 lifetime orders.
3. S2/S3 spend. Confirmed live. In the sample, every single post-conversion impression came from stage 2/3 or their separate retargeting campaign group, zero from stage 1. If they don't want existing customers seeing ads, zero out S2/S3 spend, and note their standalone retargeting group serves converters by design and carries no CRM exclusion.
4. S1 until conversion. Confirmed. All stage 1 impressions in the sample were pre-conversion. Stage 1 stops once they convert.
5. IP drift and match gap. Real and visible. Match rates are 63% and 67%, so roughly a third of their list never resolves to an IP we can block. Visit IP differs from conversion IP in 3 of 10 samples, and 29 of the 100 multi-order clusters in their export span multiple household IPs.

So the answer to "are we excluding the CRM list correctly" is yes as of 7/16, no before that. The repeat customers they're seeing are some mix of the pre-7/16 gap, the unmatched third of their list, and attribution doing exactly what a 30 day conversion window does.

*(pending: exact impression count served to April-list members during the 6/29 to 7/16 gap, adjudication query still running; point 3's "no CRM exclusion on the RT group" is being confirmed by the sibling-expression audit in the same run)*

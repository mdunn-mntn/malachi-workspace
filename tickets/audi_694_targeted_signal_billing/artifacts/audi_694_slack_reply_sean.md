# Draft Slack reply to Sean (2026-08-17, AUDI-694 thread)

> Sean: yeah i think it involves DS63 too since it's now replacing DS4 for our crm matching.
> Can you check with Jack and Wei on that?

---

Yeah, DS63 is the whole thing actually. I checked and DS47 has literally zero impressions in
enriched_impressions on every day this month, so it never reaches the meter at all. Makes sense since
it's exclusion only and an excluded household never gets served. So the ticket as written is a no-op
and all the exposure is DS63, which went live on the 6th.

Wei already has a PR up to add the DS63 leg to the crediting script (bae-sql-utility#24), it's been
blocked on BAE review since the 6th. Two things I want to get in front of him and Jack:

The PR as committed can't run, it reads translation_date and the column is translation_timestamp on
both signal tables. But the output tables in gold are populated and there's a newer version from the
13th that looks better than the PR, so I think the diff BAE is sitting on just isn't the current
design. Worth pointing them at the right one.

The bigger one is how we split the impression. On real DS63 output last week deepsync is the only
per-impression billable source and it's on 96.6% of impressions, but 99.6% of those also have guid or
augmentor on them. Depending on whether free logs count in the divisor, deepsync gets paid 4.7x more
or less, and under the preemption rule from AUDI-1113 it's 259x. Nobody's actually made that call and
it lands differently than the DS4 leg does today.

There's also a coverage gap. About 39% of billable DS63 impressions aren't getting a crediting row at
all right now, and on the DS4 side those get inserted at zero cpm specifically so they don't hand
their share to the TPA and MM vendors. So it cuts both ways and I want to net it out before I put a
number on anything.

I've got an agenda written up. Want me to set something up with Wei and Jack this week, and do you
want Alyson on it?

# Draft Slack reply to Sean (2026-08-17, AUDI-694 thread)

> Sean: yeah i think it involves DS63 too since it's now replacing DS4 for our crm matching.
> Can you check with Jack and Wei on that?

---

Yeah, DS63 is the whole thing actually. I dug into it this morning and DS47 turns out to be a
non-issue for billing, it's exclusion only, and an excluded household never gets served so there's no
impression to credit. DS63 is the inclusion replacement and that's the one that flows into the meter.

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

I've got an agenda written up. Want me to set something up with Wei and Jack this week, and do you
want Alyson on it?

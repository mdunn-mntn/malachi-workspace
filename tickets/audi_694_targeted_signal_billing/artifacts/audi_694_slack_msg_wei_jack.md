# Draft Slack message to Wei + Jack (AUDI-694)

Hey Wei, Jack. Sean pointed me at AUDI-694 and I've been digging into how DDP crediting behaves once
CRM inclusions move onto DS63. Found a couple of things I'd rather talk through than trade in a thread.

First one is small but it's blocking you. bae-sql-utility#24 reads translation_date on both signal
tables and the column is translation_timestamp, so that diff can't actually run. I checked the older
dev tables from the 2nd and 3rd too and it was never called translation_date, so it's not fallout from
the ID-421 refactor. Meanwhile the gold tables are populated and there's a newer build from the 13th
with leg1 and leg2 broken out separately, which honestly looks better than what's in the PR. My guess
is BAE is sitting on a diff that isn't your current work. If you point them at the right one that
should unblock ID-407.

Second one is the real reason I want to meet. On last week's DS63 output deepsync is the only source
that bills per impression and it's on 96.6% of them, but 99.6% of those impressions also have guid or
augmentor on them. So the divisor matters a lot. If we only count billable partners, deepsync is the
sole source on basically every impression and takes 100% of it. If we count all the graph sources the
way the MNTN Matched leg does, it's 4.7x less. Under the preemption rule from AUDI-1113 it's 259x
less. Nobody's actually picked one, and whatever we pick will differ from how the DS4 leg prices the
same thing today.

Dollars are small right now since DS63 is only on four uploads, but no DS63 credit has ever been
billed, so whatever ships first becomes the precedent for MNTN ID crediting too.

There's also a 33Across thing (DS40 credits to 28 but the graph leg counts them as two vendors, same
issue BAE hit on BAE-4923) and a couple of others I can walk through.

Got an agenda written up, should be about 45 minutes. Any chance this week?

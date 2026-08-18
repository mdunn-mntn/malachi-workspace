# Draft Slack message to Wei + Jack (AUDI-694)

Hey Wei, Jack. Two things on DS63 crediting, both need you.

bae-sql-utility#24 can't run. It reads translation_date, the column is translation_timestamp. BAE is
reviewing a diff that isn't your current work, and it's what's blocking ID-407. Point them at the
build from the 13th.

Bigger one: nobody's picked the divisor rule. Deepsync is the only per-impression billable source on
DS63 and it's on 96.6% of impressions, but 99.6% of those also have guid or augmentor. Billable-only
divisor pays it 4.7x what MM parity does, 259x what AUDI-1113 preemption does. Separately 39% of
in-scope DS63 impressions get no crediting row at all, which pushes the other way.

No DS63 credit has ever been billed, so whatever ships first sets the precedent for MNTN ID too.

45 min this week? Agenda's written.

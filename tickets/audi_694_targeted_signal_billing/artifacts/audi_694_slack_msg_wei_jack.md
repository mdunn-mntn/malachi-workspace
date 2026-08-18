# Draft Slack message to Wei + Jack (AUDI-694)

Hey Wei, Jack. Looking at DS63 vendor crediting for AUDI-694. Two asks.

1. Can you repoint BAE at the right SQL? bae-sql-utility#24 reads translation_date but the column is
translation_timestamp, so that diff won't run. The build from the 13th looks like your real work.
That review is what's blocking ID-407.

2. Can we get 45 min this week to agree how a DS63 impression gets split across the sources that
enabled it? Right now deepsync is the only vendor that bills per impression, it's on 96.6% of DS63
impressions, and 99.6% of those also have guid or augmentor on them. So whether our free logs count
in the split decides whether deepsync gets 100% of the impression or a fifth of it. The graph leg and
the MNTN Matched leg currently answer that differently, and I don't think anyone's deliberately
picked. Same question then applies to MNTN ID.

I've got numbers and an agenda, happy to send ahead.

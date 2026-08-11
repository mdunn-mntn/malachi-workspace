# Slack handoff draft — to Ryan Kleck (AUDI-431)

Hey Ryan, I finished the blocklist/whitelist re-assessment (AUDI-431). The lists hadn't been touched since we shipped them last September, so I re-ran the whole thing off the missing_domains output and the prod model's own scores.

What I've got:

3,014 adds that resolve 94% of the current uncategorized visit volume: 2,988 blocklist and 26 whitelist. The blocklist ones are news, adtech, content farms and parse-garbage strings where the model already scores them as non-ecommerce, so the list just stops us re-scoring them daily.

Two things worth your time. First, I ended up fetching every ambiguous domain rather than trusting the classifier, and share-of-urls-over-0.4 turned out to be worthless as a shop signal. Of 41 domains where nearly 100% of urls cleared the cutoff, only 2 were real stores. The rest are content farms, video players, scraper tools and classifieds whose templated urls all score just above the line.

Second, we are scoring domains that do not exist. cootlogix.com is rank 19 by volume, 88M rows in 28 days, and every path returns a Wix "no site connected" error. o11.tech has no DNS record at all. Nine of those 41 were dead.

Separate finding you'll want to see: of the top 500 highest-traffic domains that ARE in website_crawl_verticals, 76 carry a vertical two independent review passes agree is wrong. yahoo.com is mapped to Dating & Relationships, google.com to Security Software, facebook.com to B2B Sales & Marketing, myshopify.com to Family Planning. I checked ip_vertical_associations before claiming impact, and 40 of the 76 are blocklisted so they never reach IPs. The other 36 are live and worth fixing, about 413M URLs a week, led by facebook.com, nextmillmedia.com and smilewanted.com. The suggested replacements are constrained to the actual 152-vertical list, not free text.

One more thing I tripped over: 362 domains are in both the whitelist and the blocklist right now, including google.com, yahoo.com and myshopify.com. That predates this work (it's in the files you shipped last September). Blocklist is checked first so it wins and the whitelist entry does nothing, but you may want to reconcile them while we're in there. List is in the workbook.

Everything is in the workbook in Drive (Tickets/AUDI-431): decision sheet with per-row reasons and evidence, the two additions files in the exact shipped format, and the corrections list. Only 10 domains are still undecided and they are the ones nothing could fetch, so this is ready whenever you are.

Two questions:
1. Same deploy as last time, drop the updated CSVs into vertical_categorizations/ecommerce_domain_whitelist/? The blocklist file I built is a full merged replacement (your 1,464 + the adds).
2. For the 76 wrong verticals, what's the right mechanism, vertical_manual_overrides/ or is_manual_override in the wcv rebuild? Happy to prep whichever format you need.

One heads-up: the whitelist adds still won't have a vertical until the next crawl refresh, so that list doubles as a seed for the wcv backfill you mentioned.

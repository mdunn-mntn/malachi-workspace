# Slack handoff draft — to Ryan Kleck (AUDI-431)

Hey Ryan, I finished the blocklist/whitelist re-assessment (AUDI-431). The lists hadn't been touched since we shipped them last September, so I re-ran the whole thing off the missing_domains output and the prod model's own scores.

What I've got:

1651 auto-adds that resolve 54% of the current uncategorized visit volume. 1641 blocklist (news/adtech/content farms, median score under 0.05 and under 5% of URLs clearing your 0.4 gate, so the blocklist just codifies what the model already decides, plus 24 stable parse-garbage strings like comhttps. following the localhost. precedent) and 10 whitelist (long-tail shops the model scores over 0.92). I ran an adversarial review pass over the auto-decisions, the blocklist sample came back 0 disputes out of 100 and I demoted 5 borderline whitelist rows to manual.

Separate finding you'll want to see: of the top 500 highest-traffic domains that ARE in website_crawl_verticals, 76 carry a vertical two independent review passes agree is wrong. yahoo.com is mapped to Dating & Relationships, google.com to Security Software, facebook.com to B2B Sales & Marketing, myshopify.com to Family Planning. I checked ip_vertical_associations before claiming impact, and 40 of the 76 are blocklisted so they never reach IPs. The other 36 are live and worth fixing, about 413M URLs a week, led by facebook.com, nextmillmedia.com and smilewanted.com. The suggested replacements are constrained to the actual 152-vertical list, not free text.

One more thing I tripped over: 362 domains are in both the whitelist and the blocklist right now, including google.com, yahoo.com and myshopify.com. That predates this work (it's in the files you shipped last September). Blocklist is checked first so it wins and the whitelist entry does nothing, but you may want to reconcile them while we're in there. List is in the workbook.

Everything is in the workbook in Drive (Tickets/AUDI-431), decision sheet with per-row reasons, the two additions files in the exact shipped format, and the corrections list. About 1,370 genuinely ambiguous domains are in a manual tab sorted by volume, I'll work through the head of that before we ship.

Two questions:
1. Same deploy as last time, drop the updated CSVs into vertical_categorizations/ecommerce_domain_whitelist/? The blocklist file I built is a full merged replacement (your 1,464 + the adds).
2. For the 76 wrong verticals, what's the right mechanism, vertical_manual_overrides/ or is_manual_override in the wcv rebuild? Happy to prep whichever format you need.

One heads-up: the whitelist adds still won't have a vertical until the next crawl refresh, so that list doubles as a seed for the wcv backfill you mentioned.

# Slack handoff draft — to Ryan Kleck (AUDI-431)

Hey Ryan, I finished the blocklist/whitelist re-assessment (AUDI-431). The lists hadn't been touched since we shipped them last September, so I re-ran the whole thing off the missing_domains output and the prod model's own scores.

What I've got:

3,024 adds that resolve 94% of the current uncategorized visit volume: 2,922 blocklist and 102 whitelist. Nothing is left undecided.

The part worth your attention: I ended up fetching every proposed blocklist domain rather than trusting the classifier or my own read of the domain name, and that caught 76 real stores we would have permanently blocked. A 3% error rate on 2,484 checked. They are almost all creator blogs running their own shop on a subdomain or /shop, so anything judging by the homepage sees recipes and moves on. keviniscooking.com sells spice rubs, hearthookhome.com has 224 crochet patterns on WooCommerce, pantrymama.com sells cookbooks. Also some mainstream ones with first-party Shopify stores: bonappetit.com, newyorker.com, sfchronicle.com, lemonde.fr, goheels.com. Every one of those is now on the whitelist instead, and each was confirmed twice by separate fetches.

Two other things fell out of it. Share-of-urls-over-0.4 turned out to be useless as a shop signal, of 41 domains where nearly 100% of urls cleared the cutoff only 2 were real stores. And we are scoring a lot of dead domains, roughly 6-7% of what I checked, including cootlogix.com which is rank 19 by volume at 88M rows a month and has no site connected at all.

Separate finding you'll want to see: of the top 500 highest-traffic domains that ARE in website_crawl_verticals, 76 carry a vertical two independent review passes agree is wrong. yahoo.com is mapped to Dating & Relationships, google.com to Security Software, facebook.com to B2B Sales & Marketing, myshopify.com to Family Planning. I checked ip_vertical_associations before claiming impact, and 40 of the 76 are blocklisted so they never reach IPs. The other 36 are live and worth fixing, about 413M URLs a week, led by facebook.com, nextmillmedia.com and smilewanted.com. The suggested replacements are constrained to the actual 152-vertical list, not free text.

One more thing I tripped over: 362 domains are in both the whitelist and the blocklist right now, including google.com, yahoo.com and myshopify.com. That predates this work (it's in the files you shipped last September). Blocklist is checked first so it wins and the whitelist entry does nothing, but you may want to reconcile them while we're in there. List is in the workbook.

Everything is in the workbook in Drive (Tickets/AUDI-431): decision sheet with per-row reasons and evidence, the two additions files in the exact shipped format, and the corrections list. Only 10 domains are still undecided and they are the ones nothing could fetch, so this is ready whenever you are.

Two questions:
1. Same deploy as last time, drop the updated CSVs into vertical_categorizations/ecommerce_domain_whitelist/? The blocklist file I built is a full merged replacement (your 1,464 + the adds).
2. For the 76 wrong verticals, what's the right mechanism, vertical_manual_overrides/ or is_manual_override in the wcv rebuild? Happy to prep whichever format you need.

One heads-up: the whitelist adds still won't have a vertical until the next crawl refresh, so that list doubles as a seed for the wcv backfill you mentioned.

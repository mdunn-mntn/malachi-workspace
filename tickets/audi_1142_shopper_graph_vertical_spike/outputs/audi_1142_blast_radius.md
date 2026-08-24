# AUDI-1142 blast radius: /vertical shared-domain miss population

Run 2026-08-24 against `dw-main-bronze.integrationprod` via `bq_run.sh`. Queries with per-run result comments: `queries/audi_1142_blast_radius.sql`. Active advertiser = `deleted=FALSE AND is_test=FALSE`. Domain = `company_url` lowercased, trimmed, protocol and leading `www.` stripped, cut at first `/?#:` or space, and required to contain a dot.

## Results

| # | Metric | Value |
|---|--------|-------|
| D1 | Active advertisers (all) | 37,802 |
| D1 | Active advertisers with non-empty company_url | 37,801 |
| D1 | Active advertisers with a valid normalized domain | 37,696 |
| D1 | Distinct normalized domains | 30,503 |
| D1 | **Domains shared by >1 advertiser_id** | **2,018** |
| D1 | **Advertiser_ids sitting on shared domains** | **9,211** (24.4% of valid-domain actives) |
| D1 | Largest single domain | youtube.com, 955 AIDs |
| D2 | **Shared-domain AIDs with NO fpa_advertiser_verticals row** (true /vertical miss population) | **2,740** (29.7% of shared-domain AIDs) |
| D2 | Same, requiring no type=1 row | 2,740 (identical) |
| D2 | Context: ALL active AIDs with no vertical row | 8,025 (21.2% of 37,802) |
| D3 | mm_domain_map rows with domain not in mapped advertiser's company_url | **BLOCKED — table not in BQ** (historically ~561, 2026-04-20, Postgres) |
| D4 | mm_domain_map total rows / distinct root advertisers | **BLOCKED — table not in BQ** |
| xref | `dw-main-gold.bae.v_aid_flagged_dup_domain` (BAE's curated dup-domain flag list) | 823 AIDs on 312 domains |

## Composition of shared domains (top 15 by AID count)

youtube.com 955 · google.com 326 · mountain.com 294 · instagram.com 259 · facebook.com 251 · gmail.com 238 · tiktok.com 229 · auth.mountain.com 203 · orangetheory.com 149 · youtu.be 104 · metalsupermarkets.com 103 · amazon.com 79 · linkedin.com 75 · example.com 70 · linktr.ee 51

Two distinct populations: placeholder/social URLs from self-serve signups (youtube, google, gmail, facebook, instagram, tiktok, mountain.com self-references, example.com), and genuine franchise hoteling (orangetheory.com, metalsupermarkets.com). The placeholder population inflates the raw 9,211; the franchise population is the one the mm_domain_map hoteling flow and the /vertical cache-miss bug actually serve. BAE's curated `v_aid_flagged_dup_domain` (823 AIDs / 312 domains) is a closer proxy for the genuine-hoteling subset than the raw shared-domain count.

## D3/D4: mm_domain_map is not in BigQuery

Verified 2026-08-24: region-wide `INFORMATION_SCHEMA.TABLES` searches (`%domain_map%`, `%domain%`, `%mm_%`) across dw-main-bronze, dw-main-silver, and dw-main-gold, in both us-central1 and the US multi-region, return no mm_domain_map. The only fpa-schema tables replicated to bronze are `integrationprod.fpa_advertiser_verticals` and `integrationprod.fpa_categories`; `silver.fpa` holds only the matching two views. The ~561-row domain-mismatch reconfirm and the total-row/root-advertiser counts need a read of Postgres `fpa.mm_domain_map` (diagnostic query at knowledge/data_catalog.md:2944, via Ryan Kleck 2026-04-20); owner is DS/targeting.

## Caveats

- Advertiser count grew since the table doc's 2026-07-19 snapshot (35,558 live then, 37,802 now); live CDC dim.
- Only 1 active AID has an empty company_url; 105 more have URLs that do not normalize to a dotted domain (malformed, e.g. `https//x.com`).
- Normalization strips leading `www.` only; other subdomains stay distinct (auth.mountain.com ≠ mountain.com). youtube.com and youtu.be count separately.
- An earlier regex draft (double-backslash escaping in a raw string) silently excluded the letter "s" from the domain character class and skipped www-stripping, producing 2,243 shared domains / 15,336 AIDs. Discarded; the fixed version was validated against 10 literal URL cases before the final runs.
- D2 counts any missing `fpa_advertiser_verticals` row; the type=1-only count is identical, consistent with the table's strict 2-rows-per-advertiser grain.

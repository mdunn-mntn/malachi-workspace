# BQ Results Index

All results from BQ Silver queries for advertiser 37775 (Feb 4-10, 2026).

## Attribution Model

| File | What It Shows |
|------|---------------|
| `attribution_breakdown.json` | 219,613 VVs: 42.48% same ID, 17.47% different ID, 40.05% first_touch NULL |
| `last_vs_first_touch_timing.json` | 1,000 rows showing which impression is more recent (ad_served_id always newer) |
| `timing_pattern_summary.json` | 91.76% ad_served_id more recent, 8.21% first-touch VAST NULL, 0% exceptions |
| `single_attribution_impression_density.json` | Even when ad_served_id = first_touch, IPs have 6-30 distinct impressions in 30-day window |
| `inter_impression_bid_ip_mutation.json` | 14.28% of multi-impression VVs have different bid IPs between first and last touch |
| `el_match_rate_by_first_touch_group.json` | All three groups (same_id, different_id, ft_null) have identical ~99.97% EL match |

## Schema & First-Touch Investigation

| File | What It Shows |
|------|---------------|
| `clickpass_schema.json` | 33 columns including click_elapsed, click_url, destination_click_url (click data embedded in visit row, no click-type discriminator) |
| `first_touch_null_rate_by_recency.json` | NULL rate inversely correlates with recency: 54% at <1hr, 18% at 14-21 days (disproves lookback hypothesis, confirms batch processing) |

## VV Examples by Impression Count

| File | What It Shows |
|------|---------------|
| `example_vvs_by_impression_count.json` | Types A-E with IP comparison, cross-device flag, NTB status |
| `timeline_type_a_1_impression.json` | bid_ip=173.184.150.62, all IPs identical, same device, NTB |
| `timeline_type_b_2_impressions.json` | bid_ip=16.98.111.49, all IPs identical over 20 days, cross-device, returning, first_touch NULL |
| `timeline_type_c_5_impressions.json` | bid_ip=71.206.63.109, all IPs identical over 21 days, same device, NTB |
| `timeline_type_e_369_impressions_outlier.json` | bid_ip=104.171.65.16, 98 distinct vast_ips, bid NEVER equals vast (0/369) -- datacenter/proxy pattern |

## Key Patterns

- **Types A-C (1, 2, 5 impressions):** bid_ip = vast_ip = redirect_ip. Zero mutation. These represent the majority of VVs.
- **Type E (369 impressions):** bid_ip constant but never equals any vast_ip. 98 rotating vast_ips over 8 days. Anomalous -- likely datacenter/proxy/ad-tech infrastructure, not a normal household.
- **Missing: Type D (10 impressions)** was not run; example_vvs_by_impression_count.json shows bid_ip=vast_ip=38.62.141.230 (all identical, same device, NTB).

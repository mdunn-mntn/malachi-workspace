# TI-650: Unresolved S3 VV IP Origin Analysis

Top 20 advertisers, Feb 4–11, 90-day lookback. For each unresolved S3 VV bid_ip,
reverse-lookup ALL impressions served to that IP across ALL campaigns.

## Key Finding

**Every single origin campaign is either Retargeting (objective_id=4) or same-stage S3 (objective_id=6).**

Not a single unresolved IP traces back to a prospecting S1 campaign. These IPs entered
the S3 funnel through the identity graph / retargeting pipeline, bypassing S1 entirely.

## Origin Campaign Breakdown (top 50)

### By Objective

| Objective | Campaign Count | Typical VVs Linked |
|-----------|---------------|-------------------|
| **Retargeting (obj=4)** | ~35 campaigns | 19K–167K each |
| **Multi-Touch Full Funnel (obj=6, S3)** | ~5 campaigns | 62K–118K each |
| **Multi-Touch (obj=5, S2)** | ~3 campaigns | 6K–8K each |
| **Prospecting (obj=1, S1)** | **ZERO** | — |

### By Funnel Level

| Origin Funnel Level | Campaign Count |
|--------------------|---------------|
| funnel_level=1 (but obj=4 Retargeting) | ~18 campaigns |
| funnel_level=2 (obj=4 Retargeting or obj=5 S2) | ~18 campaigns |
| funnel_level=3 (obj=6 S3) | ~5 campaigns |

Note: funnel_level=1 campaigns with objective_id=4 are **retargeting campaigns at funnel_level=1**,
NOT prospecting. This confirms the known gotcha: retargeting campaigns exist at every funnel_level.

### Cross-Device Rate

50-53% cross-device across all origin campaigns. These users are viewing ads on different
devices than their S3 VV — consistent with identity graph entry rather than IP-based targeting.

## Interpretation

The unresolved S3 VVs are **structurally unresolvable via IP matching** because:

1. **These IPs never had a prospecting S1 impression.** They entered the funnel through
   the identity graph (LiveRamp, CRM audience upload, retargeting segment) — their first
   ad was a retargeting campaign, not a prospecting campaign.

2. **The S1 pool only contains prospecting (obj 1,5,6) campaigns.** Since these IPs were
   only ever served retargeting ads, they have no S1 pool entry to match against.

3. **~50% are cross-device.** Even if they had an S1 impression, the IP would differ.

## Implications for the Audit Table

The unresolved rate (~3-5% for most advertisers, higher for heavy retargeting advertisers
like 31357/42097) represents the **identity graph entry rate** — users who entered the
funnel via CRM/retargeting without an IP-traceable prospecting impression.

This is **expected behavior, not a gap in the audit.** The s1_resolution_method for these
VVs should be NULL or a dedicated value like 'identity_graph_entry' to distinguish them
from actual resolution failures.

## Query

`queries/ti_650_unresolved_ip_origin.sql`

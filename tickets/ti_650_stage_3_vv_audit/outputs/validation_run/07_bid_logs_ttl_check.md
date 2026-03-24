# bid_logs TTL Confirmation

**Date:** 2026-03-23
**Question:** Do bid_logs records actually get purged, or is the NO_BID_IP classification wrong?

## Test

Took 10 NO_BID_IP ad_served_ids and searched impression_log and bid_logs with **no time filter**.

## Result

All 10: `has_impression_log = true`, `has_bid_logs = false`.

| Field | Result |
|-------|--------|
| impression_log record exists | 10/10 (100%) |
| bid_logs record exists | 0/10 (0%) |
| impression_log IPs | All `10.105.x.x` (internal NAT) |

## Conclusion

**bid_logs TTL is real.** Records are purged. impression_log persists but for these VVs only has internal proxy IPs (`10.105.x.x`). Without the bid_logs record, we cannot extract the external bid_ip, and without bid_ip, we cannot search for the prior VV.

The 60 NO_BID_IP VVs in the validation run are genuinely untraceable — not a query bug, not a time-window issue.

**Note for data_knowledge.md:** bid_logs TTL confirmed empirically. impression_log has longer retention but impression_log.ip can be internal NAT (10.105.x.x) for display impressions, making it useless without the bid_logs join.

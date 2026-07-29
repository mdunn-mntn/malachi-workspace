---
name: Use source tables for IP tracing
description: When tracing IPs through the pipeline, always get each IP from its source table (bid_logs for bid_ip, win_logs for win_ip, etc.) — never use proxy tables like CIL as shortcuts
type: feedback
doc_type: memory
keywords: [source_table_ips, source, table, ips, tracing, through, pipeline, each]
domain: [workflow]
lifecycle: active
last_verified: 2026-03-13
---
When tracing an IP through the MES pipeline, each step's IP must come from the actual source table for that step. Do NOT use CIL or any other proxy table as a shortcut to get bid_ip — use bid_logs.ip directly.

Source table mapping:
- bid_ip → bid_logs.ip (joined via auction_id)
- win_ip → win_logs.ip (joined via auction_id)
- serve_ip → impression_log.ip (joined via ad_served_id)
- vast_start_ip → event_log.ip where event_type_raw = 'vast_start'
- vast_impression_ip → event_log.ip where event_type_raw = 'vast_impression'
- redirect_ip → clickpass_log.ip (joined via ad_served_id)

Reason: We can't trust any table except each step's source table to have the correct IP. Even though CIL.ip = bid_ip at 100% in validation, the principle is to trace from the authoritative source.

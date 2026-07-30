---
name: Unresolved VVs are NOT CRM/LiveRamp
description: Unresolved S3/S2 VVs cannot be explained by identity graph entry — they MUST follow the IP path. Unresolved = lookback too short, table TTL truncated, or a bug.
type: feedback
doc_type: memory
keywords: [unresolved vv, crm, liveramp, identity graph, ip path, lookback window, ttl truncation, clickpass_ip, bid_ip, stage 3]
domain: [identity, incrementality]
lifecycle: active
last_verified: 2026-07-29
---
It is IMPOSSIBLE for LiveRamp/CRM identity graph entries to cause unresolved VVs in the audit. Every VV MUST follow the IP-based system (S3 bid_ip → prior S2/S1 VV clickpass_ip, S2 bid_ip → S1 event_log.ip). If resolution fails, the causes are:
1. Lookback window not long enough
2. Historical tables have been truncated (TTL expiration)
3. An error/bug in the logs

**Why:** The user (Malachi) explicitly corrected the assumption that LiveRamp/CRM bridging could explain unresolved VVs. The targeting system requires IP-based entry into each stage — there is no alternative path.

**How to apply:** Never attribute unresolved VVs to "identity graph entry" or "CRM bridging." Always frame unresolved as a lookback/TTL/data issue. When investigating unresolved VVs, extend the lookback window and check for table truncation before concluding they are structural.

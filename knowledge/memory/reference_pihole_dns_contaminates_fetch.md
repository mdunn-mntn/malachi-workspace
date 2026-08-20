---
name: reference_pihole_dns_contaminates_fetch
description: Pi-hole on this Mac sinkholes domains to 0.0.0.0, so any fetch-based agent workflow will wrongly report live sites as dead unless it re-resolves against a public DNS server.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [pihole, pi-hole, DNS sinkhole, 0.0.0.0, unreachable domain, false dead domain, curl --resolve, dig @8.8.8.8, WebFetch, domain classification, AUDI-431, pihole allowlist, IP blocking mode, NULL blocking, NXDOMAIN blocking, LAN sinkhole address, is_global, transparent DNS interception, port 53 redirect, pinned curl dead code, AUDI-1191]
domain: [infra, workflow]
lifecycle: active
last_verified: 2026-08-20
---

Malachi's Pi-hole answers blocked domains with **0.0.0.0** on this Mac's default resolver. Any agent workflow that fetches URLs will therefore report those domains as dead/unreachable when they are actually live, and the failure is silent: the agent sees a connection error and concludes the domain is gone.

**Why:** in AUDI-431 (2026-08-11) 27 of 186 domains marked "unreachable" by fetch agents were Pi-hole false negatives, caught by Malachi asking whether his own blocklist was interfering. Same root cause as the INC-012/013 misdiagnosis where Cloud Logging looked broken but the Pi-hole was blackholing `logging.googleapis.com`.

**How to apply:** never accept "unreachable" from a fetch agent at face value on this machine. Diff local vs public DNS per domain (`dig +short <d>` vs `dig +short @8.8.8.8 <d>`) — local `0.0.0.0` or empty with a real public A record means Pi-hole, not a dead domain. Re-fetch those with `curl --resolve <domain>:443:<public_ip>` to bypass it; do NOT ask the user to disable Pi-hole, the per-domain diff is faster and more precise. Put the re-resolve instruction in the agent prompt when a workflow's whole job is fetching arbitrary domains. In AUDI-431 all 27 were adult/piracy/adtech and stayed blocklisted either way, so the contamination changed no outcome, but it would in any task where the domain set is not junk. [[feedback_background_work_liveness]] [[reference_pi5_server]]

**Status 2026-08-20: Pi-hole is ON and `logging.googleapis.com` is now ALLOWLISTED.** Verify state before assuming a block: `dig +short @192.168.10.177 doubleclick.net` should return `0.0.0.0` (Pi-hole alive) while `logging.googleapis.com` returns real Google IPs. Live Cloud Logging returns HTTP 200 again, so the INC-013 sinkhole no longer self-reproduces — a fallback that only fires on a DNS-block marker will not be exercised naturally, and must be forced to prove it is not dead code.

**Blocking MODE matters, and the naive guard misses one of them.** Rejecting `0.0.0.0` covers NULL mode and an empty `dig` answer covers NXDOMAIN mode — but in **IP-blocking mode** Pi-hole answers with its own LAN address (here `192.168.10.177`), which starts with a digit, is not `0.0.0.0`, and sails through. Pinning `curl --resolve` to it then aims the request at the blocker on :443, surfacing one layer later as a confusing parse error instead of an honest resolution failure. **Require a globally routable address** (`ipaddress.IPv4Address.is_global`), not merely "not 0.0.0.0" — fixed in `airflow_debugger/dataproc_rca.py::_public_ip` (AUDI-1191).

**`dig +short @8.8.8.8` bypasses Pi-hole only when it is a plain DNS server.** If the router transparently NAT-redirects outbound port 53, `@8.8.8.8` is answered by Pi-hole anyway — which is exactly when the IP-mode guard above earns its keep. Related caveat: a pinned fetch is only as good as its auth; `gcloud auth print-access-token` still resolves through the system resolver, so a token refresh can die in the same sinkhole the pinning routes around (IMP-051). See [[project_airflow_debugger]].

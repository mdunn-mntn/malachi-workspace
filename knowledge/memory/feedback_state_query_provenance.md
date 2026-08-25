---
name: feedback_state_query_provenance
description: "Distinguish what you QUERIED from what you INFERRED or were told; never imply you pulled data from a system you can't reach; keep the exact backing query ready."
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [query provenance, queried vs inferred, show the backing query, memdb not in bigquery, serving store not queryable, dont overstate data source, back it up with a query, state what you actually ran, aud22]
domain: [workflow, bigquery]
lifecycle: active
last_verified: 2026-07-31
---
When reporting a data finding, be explicit about **where each value came from** — what you actually queried vs what you inferred or took from someone else's artifact. Never phrase a result so it implies you pulled data from a system you cannot reach.

**Why:** aud22/memdb, 2026-07-31. My reply said memdb "showed 535 Columbus" and framed the whole finding around memdb, which made Malachi ask "how are you querying memdb?" — I wasn't. memdb is a serving store, not in BigQuery. The 535 came from Benny's drift image; I only queried the geo tables (`network_locations`, `raw.geo_maxmind_versions`) and inferred memdb's build by matching its value to the version history. Separately he asked "do you have the query to back this up?" — a finding should ship with the exact re-runnable query, not just the conclusion.

**How to apply:**
- Say what you ran and what you didn't. "I queried network_locations and the version calendar; the memdb value is from your image, so I'm inferring its build" beats "memdb showed X".
- If a store isn't in BigQuery (memdb / Aerospike household-profile / any serving store), say it's not queryable from BQ and name the real check (aql on the household-profile `geo_version` bin, or the owning team). See [[reference_bidder_serving_stores]].
- Keep the backing query ready and hand it over on request, in a clean copy-paste re-runnable form. The user pastes these into the thread as evidence.
- Inference is fine, but label it as inference, not a queried fact.

Related: [[feedback_hold_evidenced_verdict]], [[feedback_read_full_source_before_verdict]], [[feedback_facts_not_presentation]], [[feedback_slack_reply_voice]].

**Extension (2026-08-25, AUDI-1176):** the same rule applies to GATING claims. A second-hand claim ("Kirsa's remove-DS14 experiment needs ungated scoring") parked a sprint-ready $1.3-11k/mo ticket for 4 weeks; when finally checked, the named owner had never heard of it, and the 7/30 capture recorded the conclusion with no source (who said it, where, when). **Any claim that blocks or sequences work must carry its provenance at capture time** — if you cannot name the source, record it as unverified and set a check-by date instead of parking indefinitely.

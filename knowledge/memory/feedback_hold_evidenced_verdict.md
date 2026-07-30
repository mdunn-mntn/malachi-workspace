---
name: feedback_hold_evidenced_verdict
description: "Don't fold to a domain owner's plausible-but-hedged pushback; hold the evidenced verdict and settle it with a test"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 443e7168-7c62-47df-afdd-8d8cbe74c71d
doc_type: memory
keywords: [hold the evidenced verdict, dont fold to pushback, domain owner objection, hypothesis not refutation, discriminating test, read the deployed source, github org code-search, correct answer vs known mechanism, DS51 enriched_impressions, INC-001]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-29
---
When I've reached a conclusion backed by direct evidence and someone (even the domain owner / a senior
engineer) pushes back with a plausible architectural objection — especially one hedged with "who knows" /
"I'm hoping that's true" — that is a **hypothesis to test, not a refutation to fold to**. Do NOT abandon a
well-evidenced verdict and flip to the opposite conclusion just because the pushback sounds authoritative.

**Why:** In the DS51 / `enriched_impressions` case (2026-07-29, INC-001), the original GCS-evidenced call —
DS51=0 for 07-27 is correct, caused by the Bombora same-day skip — was right. Jordan raised a smart
objection ("IPDSC = targetable IPs; a 35-day lookback should preserve DS51 from prior drops") hedged with
"who knows." I fully flipped to a "suspected build bug / not benign" reframe, even calling my original
answer wrong. Jordan then confirmed the **original** answer was correct — he just hadn't known the
mechanism. I'd conceded a correct conclusion and flip-flopped the runbook three times.

**How to apply:**
1. Acknowledge the objection as valid to check — don't dismiss it, don't fold to it.
2. Keep the evidenced verdict as the working answer until the objection is actually confirmed.
3. Design/run the **discriminating test** that separates the hypotheses (here: overlay DS51=0 against the
   skip-day calendar, then check the actual serving side — the real answer was serving-side, NOT the
   "same-day-membership" guess I floated; the test + the source beat every hypothesis).
4. Distinguish "correct answer" from "known mechanism" — an answer can be right while the *why* is still
   open; say exactly that instead of reopening the verdict.
5. **When the mechanism is a code fact, close it by reading the DEPLOYED SOURCE, not by hypothesizing.** If
   the repo isn't cloned locally, GitHub org code-search finds it (`enriched_impressions org:SteelHouse` found
   `SteelHouse/data-pipeline/pyspark_pipelines/impression_enrichment.py`). Reading that builder + a 1:1
   source replication proved the DS51 zero was **serving-side** (a single-source campaign served 0 impressions
   on the skip day), which RETRACTED my own interim "same-day-keyed" guess. A guessed mechanism (even a
   plausible one) is worth exactly nothing next to the actual code — read it before asserting.

6. **One confirming case is NOT proof of a mechanism — actively seek the counter-case.** Same INC-001, second
   miss (2026-07-29): after verifying enriched DS51 ≈ served impressions 1:1, I asserted "the ipdsc skip zeroed
   serving on 07-27" from that single day. Jordan flagged DS51 should have a ~90d membership TTL (so it should
   persist). Testing it surfaced the counter-case: **07-25 was ALSO a skip day and served 104K**, and on 07-27
   the advertiser served 1.47M via other campaigns. The skip-correlation was a red herring. Before asserting
   "X causes Y," find the case where X holds but Y doesn't — if it exists, X isn't the cause. Verify the
   mechanism across the negative cases, not just the one that fits.

Related: [[feedback_no_unsolicited_suggestions]], [[feedback_facts_not_presentation]], [[reference_oncall_runbook]], [[reference_data_pipeline_repo]].

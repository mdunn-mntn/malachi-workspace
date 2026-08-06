---
name: feedback_hold_evidenced_verdict
description: "Don't fold to a domain owner's plausible-but-hedged pushback; hold the evidenced verdict and settle it with a test"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 443e7168-7c62-47df-afdd-8d8cbe74c71d
doc_type: memory
keywords: [hold the evidenced verdict, dont fold to pushback, domain owner objection, hypothesis not refutation, discriminating test, read the deployed source, github org code-search, correct answer vs known mechanism, DS51 enriched_impressions, INC-001, INC-012, lost executors red herring]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-06
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

7. **Two-step verification for a derived-table anomaly: (a) is the number TRUE? (b) WHERE did the rows go?**
   Same INC-001, final resolution (2026-07-29): the thread spent FOUR rounds theorizing why
   `enriched`/`cost_impression_log` DS51=0 for 07-27 was *correct* (same-day-keyed, serving-dark, campaign
   pause). Step (a) — one `spend_log` query — ended that: the campaigns won **110,792** auctions, **$904
   billed, 100% rendered**, so the impressions were REAL and 0 was wrong. But then I over-claimed a FIFTH
   framing ("CIL dropped/lost the rows"). Step (b) — `GROUP BY campaign_id` + physical-table time-travel —
   corrected THAT too: the rows were in CIL all along under **`campaign_id = -3`** (unresolved sentinel);
   a reprocess had re-stamped resolved→`-3` (109,530 attributed 47h ago → 0 now). So it was a
   campaign-RESOLUTION regression, not data-loss. Lesson: reconcile an anomalous 0 against the spend/event
   source of truth (`spend_log`/`win_logs`) FIRST, THEN find where the rows actually are (group by the id,
   time-travel the partition) BEFORE naming the mechanism. Don't say "dropped" until you've looked for the
   rows under other keys. (Even the owner's mid-thread "0 is correct" was wrong.)

8. **The mirror failure — don't NAME a root cause from one surface, then thrash the verdict.** INC-008
   (2026-07-30, Fangorn inference Dataproc create failing): I named the cause **three times** from partial
   evidence — champion/challenger contention (from a grid screenshot), then external zonal stockout (from the
   Dataproc op error), then "quota ceiling" (from the owner's worker-pool log) — before the owner + the re-run
   self-recovering settled it as a **transient external stockout**. Each call was premature: I published a
   verdict before pulling ALL the evidence surfaces and before ground truth. For a multi-surface infra failure,
   the discipline is symmetric with the hold rule: **gather every surface and run the discriminating test BEFORE
   naming a cause**, THEN hold it against hedged pushback but update on genuine new ground-truth. Concretely for
   a Dataproc `create-dataproc-cluster` code-9: pull the failed-op error (zonal), the Vertex worker-pool/`service`
   log (quota), AND `gcloud compute regions describe <region>` quota-vs-usage — and reconcile — before you call
   it; and run the actual discriminating test (here: challenger cluster DELETE-time vs champion CREATE-time, which
   refuted the contention theory outright — 62-min gap, plus the champion is UPSTREAM of the challenger so they're
   never concurrent). **On a live/evolving incident, prefer waiting for self-recovery or owner confirmation over
   publishing a verdict you'll rewrite in 20 minutes.** Under-updating (point 1, folding) and over-updating
   (this, thrashing) are the two failure modes of the same skill — the fix for both is: test first, name once,
   then hold-with-updating. See [[reference_fangorn_inference_dataproc]].

9. **The pattern working as written — INC-012 (2026-08-06, `materialize_mntn_select`).** The owner's first read
   was "lost executors"; the alert thread said "preemption". The `driveroutput` evidence showed a GCS list
   timeout (`Error listing gs://.../augmentor_log/region=` → `SocketTimeoutException`), and the
   executor-lost lines were benign idle scale-downs logged at ERROR. Held the evidenced verdict against
   both plausible reads, shipped the fix same day (airflow-ti#1176: literal region paths + a `globStatus`
   null-guard in `get_paths`), and the owner merged it. Acknowledge the owner's read, test it against the
   evidence, hold, close with the discriminating artifact.

Related: [[feedback_no_unsolicited_suggestions]], [[feedback_facts_not_presentation]], [[feedback_source_table_ips]], [[reference_oncall_runbook]], [[reference_data_pipeline_repo]], [[reference_fangorn_inference_dataproc]], [[feedback_read_full_source_before_verdict]].

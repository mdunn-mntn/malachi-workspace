---
name: reference-mm-3p-intersection-mechanics
description: "When 3P-include is added to MM campaign with HHST>0, 3P narrows MM to (MM ∩ 3P), it does NOT expand audience. The strongest argument for TI-956 segment curation."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 2a20d28f-2a8c-4757-a5e4-36e63bd41f18
doc_type: memory
keywords: [mm 3p intersection, 3p include narrows mm, hhst, AND OR semantics, DS13, DS19, TI-956, TI-999, segment curation, ryan kleck]
domain: [audience-scoring, bidding]
lifecycle: active
last_verified: 2026-06-01
---
**Locked logic (Ryan Kleck, TI team, 2026-06-01)** — the critical structural insight that drives the TI-999 / TI-956 deck argument.

**When a buyer adds a 3P-include layer to an MM campaign with HHST > 0:**

1. Scoring pipeline scores IPs in DS13/19 (the MM layer).
2. MemDB translates the expression — with AND semantics, campaign membership = MM IPs ∩ 3P IPs.
3. Bidder receives membership + scores. With HHST > 0, only IPs with score ≥ HHST get bid on.
4. **IPs in 3P-only (not in DS13/19) have no score → fail HHST → not bid on.**
5. Net result: bidder bids on **(MM ∩ 3P) only** — 3P narrows MM.

**Plain-English consequence:** adding 3P-include to MM does NOT expand the addressable audience. It NARROWS MM scoring to the 3P-intersected subset. The 3P segment's quality determines **which slice of MM-scored IPs the bidder sees**.

**Why this is load-bearing for TI-999:**

- MM+3P combinations are the **majority of prospecting spend** (Pass 21: MM+3P = 22.6%, MM+3P+CRM = 10.3%, MM+CRM = 15.8%; sum ~50%).
- **Within MM+3P-include specifically (Pass 26): 80% is OR semantics; 5% is AND semantics; 8% mixed.** OR-include is bidder-inert under HHST > 0; AND-include genuinely narrows MM.
- Buyers believe they're combining MM with interest segments for audience expansion / diversification.
- For AND-include: 3P is a real narrowing filter on MM. 3P quality determines which MM-scored IPs end up in the bid pool.
- For OR-include: bidder ignores the 3P clause (3P-only IPs unscored, fail HHST). 3P clause doesn't change delivery.

**Spend semantics clarification (Malachi, 2026-06-01):**

- **Audience size does NOT determine spend.** Advertisers are charged only when MNTN bids on AND wins an impression. So adding an ineffective 3P clause doesn't "cost more" — same spend, same delivery, just a UI label that misrepresents what was targeted.
- The "audience-size theater" framing is about decoupled targeting intent (buyer believes they're targeting MM+3P, but mechanically gets MM-only delivery), NOT about wasted dollars.

**TI-956 curation argument (refined):**

- For AND-include (5% of MM+3P-incl spend): segment quality determines delivery quality → curation has real lift on KPIs.
- For OR-include (80% of MM+3P-incl spend): segment quality doesn't affect delivery, but curation prevents buyers from believing they're targeting low-quality segments when they're actually getting MM-only delivery → curation has attribution / UI honesty value.
- Both warrant the TI-956 build, but the framing is different per cohort.

**Exception cases:**

- HHST not set (or = 0): bidder ignores scores → 3P expands MM by bringing unscored 3P-only IPs into bidding. This is the bad pattern — buyers broadening to unscored audiences without realizing.
- HHST distribution across MM+3P campaigns is currently unknown. Pending investigation (search `bid_events` for the threshold per Ryan).

**See also:** [[reference_rtc_hhst_gating]] (HHST gates whether ANY score is used by the bidder), [[reference_prospecting_scores_gcs_monitor]] (Fangorn-on vs Non-Fangorn distribution), [[project_ti_999_strategic_goal]] (overall TI-999 framing), `knowledge/data_knowledge.md` § "MM + 3P intersection mechanics — LOCKED LOGIC" for the full canonical write-up with Venn diagram.

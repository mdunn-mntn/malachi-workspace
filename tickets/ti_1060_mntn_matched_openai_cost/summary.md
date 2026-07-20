---
doc_type: ticket
title: "TI-1060: Reduce OpenAI Cost in the MNTN Matched (DS19) Keyword Pipeline"
status: backlog
date: 2026-06-26
summary: "Find levers to cut OpenAI spend on the DS19 daily keyword classification pipeline"
result: "in progress — ranked 7 levers; biggest bet is BGE-large embedding to replace gpt-4o-mini"
---

# TI-1060 — Reduce OpenAI Cost in the MNTN Matched (DS19) Keyword Pipeline

**Jira:** [TI-1060](https://mntn.atlassian.net/browse/TI-1060) · **Blocked by:** [TI-1058](https://mntn.atlassian.net/browse/TI-1058) (pipeline map)
**Status:** Backlog (investigation seeded) · **Source:** Ryan Kleck walkthrough 2026-06-26
**Primary code:** `SteelHouse/shopper_graph` @ `4f0fc3746f7fd0869cb50e006321023536e23062`

---

## 1. Introduction / goal

Reduce OpenAI spend on the **DS19 keyword** classification (daily `gpt-4o-mini` Batch API). Ryan: *"very
expensive"* … *"if we could tell leadership we cut our OpenAI costs by half, that would make them pretty happy."*
Cost reduction is a Kale focus area. Full pipeline map is in [TI-1058](https://mntn.atlassian.net/browse/TI-1058);
this ticket is the optimization investigation that depends on it.

**Scope note:** DS13 (vertical) is already cheap — its OpenAI step runs only every few months and is cached. **All
cost levers here are about the DS19 daily keyword flow.**

## 2. What the cost actually is (so we target the right thing)

Per [TI-1058 §5], the pipeline already dedups correctly:
- Anti-join on `composite_key` (URL minus query string) so **already-classified URLs are never re-sent**.
- One OpenAI request per unique URL regardless of how many vendors (`data_source_id`) reported it.

⇒ **The cost driver is the raw count of distinct path-level URLs sent each day** ("very unique URLs… we send a lot
of them" — Ryan), **not** `data_source_id` multiplication (that hypothesis is disproven — see §4). So the levers are:
shrink the URL set, shrink the prompt, or change the classification method.

## 3. Ranked candidates

### A. Replace gpt-4o-mini with the in-pipeline local embedding (biggest lever)
BGE-large (`system.ai.bge_large_en_v1_5/3`) is **already in the pipeline and free** (Databricks Unity Catalog) — it's
used post-batch to snap OpenAI's free-text category to a taxonomy keyword (`product_categorization_temp.py`,
threshold 0.6). Hypothesis: embed the **product/URL text directly** and nearest-neighbor into the taxonomy, skipping
gpt-4o-mini for many/most URLs. If even a large fraction of URLs can be classified by embedding alone, this is the
path to "cut costs by half."
- **Next check:** prototype embed(URL/product_name) → top-k taxonomy match; measure agreement vs current gpt-4o-mini
  output on a sample; quantify the share of URLs that clear a confidence bar without the LLM.
- **Risk:** accuracy on ambiguous URLs; needs the eval harness from candidate F. Work with **Alex Knorr** ("you might
  have a better model").

### B. Prompt token reduction (cheap, immediate)
- **`product_sku` is hardcoded to literal `1`** (`product_uniques.py`) → every request sends `" Product SKU:1"` —
  pure dead tokens. Remove it. *(Ryan noticed this live.)*
- **Missing spaces** in `" Product SKU:"` / `" Product URL:"` — fix.
- **`max_tokens=1000`** for a tiny 4-field JSON is generous; lower it.
- The full instruction repeats per request (Batch API has no prompt caching) → keep the instruction minimal.
- **Next check:** trim prompt, re-run a sample batch, diff outputs for regressions.

### C. Low-frequency URL/keyword filtering (volume cut)
Rare URLs whose resulting keyword is seen too few times can't yield good targeting signal (Ryan: "red bottom shoes…
we won't even be able to get good quality singles… might not be beneficial to even send keywords over that aren't
seen all the time"). Filter pre-batch by recurrence (e.g. composite_key or domain frequency over a window) before
sending to OpenAI.
- **Next check:** distribution of per-URL / per-keyword frequency; model the volume saved vs targeting coverage lost
  at several thresholds.

### D. Re-enable / fix homepage-description enrichment
`openai_batch_input_raw.py` joins homepage descriptions **only for `apollaperformance.com`** (hardcoded `.isin([...])`).
For every other domain the "Website description" is omitted. This is dead code or a regression; it both wastes the
join and removes context that could improve classification accuracy (reducing bad keywords → less wasted spend).
- **Next check:** confirm intent with Ryan/Victor; either remove or broaden the filter and measure accuracy lift.

### E. Model choice
Re-evaluate `gpt-4o-mini` vs newer/cheaper models. Caveat (Ryan): changing prompt or model shifts outputs, so this
requires a before/after eval.
- **Next check:** candidate models × the eval harness (F); compare cost/accuracy.

### F. Build an accuracy eval harness + QA the current output
Everything above needs a way to say "did quality hold?" Start with Ryan's QA idea: pull ~100 random rows from
`prod.mntn_matched.product_categorization` and judge accuracy. Ryan's live example: a soccer-news URL → "online
content/publications" (vector-map degraded it). Poor baseline accuracy strengthens the case to change the approach
and gives a yardstick for A/B/E.
- **Next check:** sample query + manual/LLM-judge accuracy scoring; becomes the regression gate for all changes.

### G. Taxonomy auto-add is currently disabled (coverage, not direct cost)
Step 3 of `product_categorization_temp.py` (add new keywords to the index when below threshold) is **commented out**
("post migration" TODO). While disabled, sub-threshold categories are dropped. Re-enabling affects coverage/quality
and interacts with A/C — track it but it's not a direct cost cut. Owner: **Victor**.

## 4. Disproven hypothesis (record so we don't chase it)

**`data_source_id` in the group-by does NOT multiply OpenAI requests.** Malachi's original concern and Ryan's on-call
uncertainty are answered by the code: `collect_set(data_source_id)` → one `custom_id` per URL, and
`openai_batch_input_formatted` keeps `rn=1` per `custom_id` before the batch file is written. `data_source_id` is
retained for **billing attribution** only. Full trace in [TI-1058 §5]. (augmentor_log DS30 duplicate URLs are already
absorbed by the composite_key anti-join.)

## 5. Collaborators
- **Victor** — Common Crawl homepage refresh + taxonomy auto-add rules.
- **Alex Knorr** — advertiser keywords / DAR overlap ("you might have a better model").

## 6. Open items
- Get actual OpenAI spend numbers (per day / per month) to size the prize and prioritize A vs B/C.
- Decide sequencing: B (quick token wins) now; A (embedding substitution) is the big bet but needs F (eval harness).

# TI-829: Loom Script — "Keyword Ranking Matters" (~5 min)

**Audience:** Paulo Black (VP Eng), Richard Girges (CTO)
**Presenters:** Malachi Dunn + Alex Knorr
**Visual:** RevealJS deck screen-shared, advance with arrow keys
**Tone:** Lead with WHY and impact. Bold statements backed by numbers. No hedging.

---

## Slide 1: Title (0:00–0:15) — Malachi

> "We found a 72x signal in our keyword targeting that we're not using. Today, every keyword-matched IP gets the same score — 10,000 — regardless of how relevant that keyword actually is. Here's what that's costing us."

---

## Slide 2: MNTN Match V2 (0:15–0:40) — Malachi

> "Here's how keywords work today. An LLM scrapes the homepage, guesses keywords, and maps them to targetable categories. The problem: every IP that matches any keyword — whether it's the best keyword or the worst — gets scored identically. 10,000. No differentiation. We're treating a 72x signal as binary."

---

## Slide 3: Bottoms-Up Keywords (0:40–1:15) — Alex

> "BUK solves this. Instead of guessing from a homepage, it learns from 30 days of real behavioral data — which IPs actually visited and converted on which advertisers — across all 6,000 of our advertisers. It produces a ranked list of keywords per advertiser. Not just 'what keywords matter' but 'which keywords matter most for this specific advertiser.' That ranking is where the 72x signal lives."

---

## Slide 4: Framing (1:15–1:30) — Malachi

> "We already know keywords are important. The question is: are we capturing all the value in them? The answer is no. Scoring everything uniformly is leaving massive signal on the table."

---

## Slide 5: 72x (1:30–2:00) — Malachi

> "30 billion IPs. 500 advertisers. IPs matched to the top-ranked keywords visit at 72 times the rate of bottom-ranked. Today, all of those IPs — rank 1 through rank 200 — are scored 10,000. Same score. That's the gap."

---

## Slide 6: The Cliff (2:00–2:25) — Malachi

> "And it's not gradual — the signal drops off a cliff. 72x at the top, then 14, 5.6, 2.9, 1.9, down to 1x. This is a clean, monotonic decline across 500 advertisers. It's a signal. And today the pacing system has no way to act on it."

---

## Slide 7: Advertiser-Specific (2:25–2:50) — Alex

> "This signal is advertiser-specific. If you rank keywords globally — same ranking for everyone — you only get 3x differentiation. Rank them per-advertiser and it's 72x. The value is in knowing which keywords matter most for each advertiser. That's what the model captures and what continuous scoring would unlock."

---

## Slide 8: 85% Consistency (2:50–3:15) — Malachi

> "This isn't a few outliers. 85% of advertisers show greater than 10x lift. Median is 82x. 67 verticals — travel, apparel, B2B, non-profits, fitness. It's consistent across the board."

---

## Slide 9: So What (3:15–3:50) — Malachi

> "Two things. One — keyword ranking is the highest-leverage signal we're not using. 72x aggregate, 82x median. That's not incremental, it's an order of magnitude. Two — continuous scoring fixes this. Instead of flat 10,000 for everyone, we score on a gradient. Highest-ranked keyword IPs get priority, then we expand as budget allows. Bigger audience. Better signal. Pacing gets finer control."

---

## Slide 10: Next Steps (3:50–4:30) — Alex

> "We've already started validating this against live experiments. In the Fangorn experiment, IPs with higher BUK scores showed higher visit rates in actual campaign delivery — same pattern, different data source. Next, we implement continuous scoring — pass the keyword rankings through to the bidder so pacing can act on this 72x signal instead of treating it as flat."

---

## Slide 11: Close (4:30–4:50) — Malachi

> "Not all keywords are equal. Ranking them matters. 72x across 500 advertisers. We have the signal. We just need to use it."

---

## Recording Tips

- Fullscreen the deck before starting Loom
- Use arrow keys to advance (spacebar works for fragments)
- Lead every slide with the impact/result, then explain if needed — never the other way around
- Don't read verbatim — these are talking points, not a teleprompter
- If you finish under 5 min, that's good. Under is always better than over.
- Deck link: https://gist.githack.com/mdunn-mntn/be58ab51d75c3b284988fe61932bbd2f/raw/ti_813_presentation_deck_standalone.html

# Planning Prompt — BUK Exec Loom (~5 min)

Paste this into a fresh Claude session to plan the script.

---

I'm preparing a **~5 minute Loom** for MNTN execs — Paulo Black (VP Eng), Richard Girges (CTO), and likely Kale (Director, Targeting Infrastructure). Kale explicitly requested this. Co-recording with Alex Knorr.

I work on MNTN's **Targeting Infrastructure** team — we build the performance targeting engine that decides who sees which ad. This Loom is about our **Bottoms-Up Keywords (BUK)** work and how it reframes CTV in the performance marketing mix.

Use the Presentation Playbook at `documentation/docs/presentation_playbook.md` as the authoritative guide for every craft choice (Power Line, openers, three-act, Rule of Three, close).

---

## The argument I want to land

**Working Power Line:** "CTV isn't a replacement for paid search — it *is* paid search, upgraded."

Sharpen this. I want the version an exec repeats in their next meeting without looking at notes.

---

## Context for the audience

Three performance channels in play:
1. **Paid search** (Google) — mature, trusted
2. **Paid social** (Meta) — mature, trusted
3. **CTV / performance streaming** (MNTN's wheelhouse) — new, scrutinized

**The objections we keep hearing from buyers:**
- Hesitant to shift budget out of search/social into CTV
- Worried CTV won't deliver the same ROI
- CTV feels like an unknown channel with unfamiliar signal

**The root misconception:** buyers think CTV is a *different, riskier thing* than search. It isn't.

---

## The insight that changes the frame

- Performance on CTV is measured on the **site visit**. Third-party pixels and our internal pixel tell us who visited after the ad served.
- **How does someone arrive at that site in the first place?** Almost always a search.
- So CTV performance is already riding on search intent — buyers just don't see it that way.
- **BUK goes further:** we scrape the advertiser's website and tie on-site behavior back to keyword intent. We're not just capturing the query that got the user there — we're capturing what they actually *wanted* once they arrived.

**The Nike example (use this as the story beat):**
- A user searches Google for "athletic shoes."
- They land on Nike and end up on **colorful running shoes**.
- Paid search only sees "athletic shoes."
- BUK captures "colorful athletic running shoes" — the richer intent revealed by behavior, not just the query.

**Net:** CTV targeting doesn't just run on search signal. It runs on **enriched** search signal — the same intent paid search uses, plus the context only on-site behavior reveals.

---

## The exec takeaway

Shifting budget from paid search to CTV is not a trade-off. It's an **upgrade**: same intent signal, richer context, TV reach. The buyer isn't switching channels — they're getting more value from the same intent dollar.

---

## What I need from you

Build a 5-minute Loom script that:

1. **Power Line.** Finalize one (≤10 words) plus 2 alternatives. The one execs will repeat.
2. **Opener.** Pick one of the 5 proven openers from the playbook (Startling Stat, Question, Story, Bold Claim, Contrast). Recommend one and draft the first 20 seconds verbatim. **No "today I'm going to talk about."**
3. **Three-act structure:**
   - **Disruption** — name the objection (execs' hesitation about CTV)
   - **Revelation** — CTV is enriched paid search, not a new channel
   - **Resolution** — the budget decision is an upgrade, not a trade
4. **Story beat.** Use the Nike "athletic shoes → colorful running shoes" example. Hall framework: character, emotion, moment, specific detail. Tight — 30 seconds max.
5. **Rule of Three.** Exactly 3 takeaways. Name them, ordered for memory.
6. **Close.** End on the Power Line or a clear call to action. **Never** "any questions?" or "that's all I have."
7. **Word budget.** ~750 spoken words total (5 min at 150 wpm). Give me word counts per section.

---

## Deliverable format

- **Final Power Line** + 2 alternatives
- **Script** with explicit section timing: Opener :00–:30, Act 1 :30–1:30, Act 2 1:30–3:00, Story 3:00–3:30, Act 3 3:30–4:30, Close 4:30–5:00
- **Speaker notes** inline — where to slow down, where to emphasize, where to pause
- **Optional screen share cues** — if I should show a slide or visual, one-line description of what's on screen
- A **Cialdini pass** noting where I'm leveraging social proof, authority, scarcity, commitment, reciprocity, or unity

---

## Constraints

- **Marketing language, not engineering language.** Execs want budget / ROI / confidence, not "targeting infrastructure" or "intent enrichment pipeline."
- **Define jargon in one phrase or drop it.** "BUK" becomes "we scrape the website to capture what the searcher actually wanted."
- **Bold and persuasive.** Assume the audience is skeptical about CTV.
- **Concrete over abstract.** Nike example in, hand-wave out.
- **No hedging.** "This is why CTV wins" not "this might suggest CTV could potentially..."

---

## Self-critique before delivering

After drafting, run the script through `claude-prompts/presentation_critique.md`. Score it on all 10 dimensions. Apply fixes for anything scoring ≤3. Then hand me the final script.

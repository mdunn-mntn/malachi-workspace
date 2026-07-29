---
name: deck-standards
description: Decks: Tufte+RevealJS standards, share via share_deck.sh githack (auto after deck), HTMLPeek/PageDrop alt, no matplotlib titles on chart slides, author name on title slide, no named attributions
metadata:
  type: reference
doc_type: memory
keywords: [deck standards, tufte, revealjs, share_deck.sh, githack, htmlpeek, pagedrop, no matplotlib titles, author name malachi dunn, no named attributions, chart color meaning]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-10
---
**Scope note (2026-07-10, AUDI-1089):** auto-share applies to finished DECKS. When Malachi is assembling the
doc himself and asks for "the graphs," deliver the PNG set locally (artifacts/ folder) — NO gist/share link
unless he asks for one.

**Chart style for Malachi's doc-assembly PNGs (2026-07-10):** plain DESCRIPTIVE titles (what it is, not the
finding), one-line caption BELOW the chart (small gray), no editorial annotations or inline comments, no
em-dashes anywhere, uniform colors (navy bars, gray for internal), tables welcome (e.g. ranked daily-delivery
table). The "title states the finding" playbook rule does NOT apply to these.

## from reference_visualization_standards.md

## Team Visualization Preferences (2026-04-02 Slack thread)

**Edward Tufte** is the team-endorsed visualization authority. Alex Knorr, Andrew Samaha both recommend him. Alex specifically recommended the Tufte online course as a starting point.

**RevealJS** is the team's preferred interactive presentation format:
- Mike Dolt: "building out good looking interactive presentations using RevealJS under the hood"
- Jason Mills: "develop the content in markdown then ask to convert to template pulling in company brand"
- Both use Claude to generate these

**People doing presentations well at MNTN:**
- Jason Mills — Claude-generated, markdown → branded template pipeline
- Mike Dolt — RevealJS interactive presentations via Claude
- Alex Knorr — Tufte-informed, Malachi's direct audience for TI-804

**Additional resources recommended by team:**
- bermudez — Andy Kirk's "Visualising Data" (visualisingdata.com/resources). Chart categories: Categorical, Hierarchical, Relational, Temporal, Spatial.
- Alex Bloore (VP TPMs) — "Good Charts" by Scott Berinato (Harvard). Focused on "what makes a good chart?"
- Tim Kachler — NotebookLM for visualization materials

**Malachi's stated goal (in Slack):** "create a systematic approach to visualization" — gather good info, create a repeatable presentation prompt. Shared step-by-step workflow with team (well-received by Rogus, Kaitlin Dickinson, others).

## How to Apply
- When creating presentation visualizations, follow Tufte principles (high data-ink ratio, no chartjunk, small multiples)
- Generate interactive RevealJS HTML alongside static PNGs when presenting to the team
- The presentation playbook (`documentation/docs/presentation_playbook.md`) includes Tufte visualization section
- The RevealJS guide (`documentation/docs/revealjs_guide.md`) has layout rules, color meaning rules, size guidelines, and slide templates — read before building any deck
- **Color meaning:** Red = hero numbers only, Navy = emphasis text, Gray = context. Red reads as "bad" to execs — never use it for status text or emphasis lines.

## from reference_html_sharing.md

## HTML Sharing Tools (Kale recommended, 2026-04-03)

For sharing RevealJS standalone decks without requiring file downloads:

**HTMLPeek** (htmlpeek.com) — preferred
- Paste HTML into editor, click "Publish & Share", get a permanent URL
- Anonymous shares expire after 30 days; logged-in shares are permanent
- Password protection available
- Workflow: `cat deck_standalone.html | pbcopy` → paste → publish → share URL in Slack

**PageDrop** (pagedrop.io) — alternative
- Drag-and-drop HTML file hosting

## How to Apply
When sharing RevealJS decks with the team, use HTMLPeek to generate a URL instead of attaching the .html file in Slack. Cleaner experience for the viewer — they just click a link.

**Sensitivity check:** Only use for non-sensitive content. The presentation data (advertiser names, lift numbers, methodology) is internal analysis but not PII/credentials. Kale confirmed this is fine.

## from feedback_deck_share_link.md
After building any RevealJS deck, **immediately run `bash .claude/scripts/share_deck.sh <standalone_path>`** and deliver the rendered githack URL in the response. Do not wait to be asked.

**Why:** The user can't click on a local HTML file from the conversation. A githack URL is the only form they can actually open and review. Asking them to run `share_deck.sh` themselves adds friction and breaks the "here's the deck" handoff.

**How to apply:**
- Flow is: build `*_deck.html` → build `*_deck_standalone.html` → run `share_deck.sh` on the standalone → paste the rendered URL in the response.
- Applies to every deck build: drafts, revisions, iterations. Each revision gets a new gist (githack caches aggressively).
- The script copies the URL to clipboard automatically; still paste it in the response so it's retrievable from the conversation.
- If `gh` auth is missing or the script fails, fall back to HTMLPeek/PageDrop and flag the failure to the user.

## from feedback_no_double_titles_on_chart_slides.md
When building a RevealJS deck where a slide is `class="img-slide"` with a chart PNG plus an `<h2>` slide title, **do NOT also include a matplotlib title or subtitle inside the chart**. The slide H2 is bigger, more prominent, and stacks visually with the chart's internal title — they overlap when rendered.

**Why:** TI-933 deck v1 had `set_title()` + `text(0, 1.04, ...)` subtitle on every chart, plus an `<h2>` on the slide. The two stacked and overlapped on every chart slide. Malachi flagged this with screenshots.

**How to apply:**
- On chart-only slides: keep the slide H2, drop `ax.set_title(...)` and `ax.text(0, 1.04, ...)` from the matplotlib chart
- On standalone PNGs (not in a deck): keep the matplotlib title (chart needs to stand alone)
- A footer caption on the slide (e.g., `<p class="footer-note">`) is fine — it's small enough not to compete

If a chart needs both a slide and standalone use, generate two versions of the PNG (with and without title) or render the title as a separate slide H2 element only.

## from feedback_author_name_on_first_slide.md
Every deck must have **"Malachi Dunn"** on its first/title slide. This is a positive rule — author attribution on the slide where the deck identity lives.

**Why:** The user wants visible attribution as the author of decks they create. Established 2026-05-08 during the TI-917 Loom build.

**How to apply:**
- For RevealJS decks: put name in the title slide (the slide with the deck title + agenda), styled as a small line below the title — typically navy, ~0.8em, centered. Not in a footer (footers are for runtime / audience metadata).
- If the deck has a "cold open" slide before the title, the name still goes on the title slide (slide 2 by convention), not the cold open.
- Apply automatically when building or editing decks. Don't ask.

**Carve-out from `feedback_no_names_in_decks.md`:** the existing rule against named attributions in shared artifacts still applies to *other people's* names (Lauren, Al, Zach, etc. — never name them in shared decks). Malachi's own name on the first slide is the explicit, durable exception.

## from feedback_no_names_in_decks.md
When building or editing any shared / external-facing artifact, never include named attributions for quotes, framings, caveats, preferences, or insights unless the user explicitly says to.

**Scope (broadened 2026-05-04 from decks-only to all shared artifacts):**
- Presentation decks (RevealJS, slides, PDF) — original scope
- Jira ticket summaries, descriptions, comments
- Todoist task titles, descriptions, comments
- Slack message drafts the user will paste
- Any document that gets shared, forwarded, or read by people other than the user

**Forbidden patterns:**
- "Per Zach Schoenberger..." / "Per Alex K..."
- "Channel context (Alex Bloore):"
- "Caveat (Alex K):"
- "[Name] explicitly does not want to own X"
- "[Name] said..."
- Parenthetical name credits at the end of a paragraph
- Personal preferences attributed by name ("Matt's preference is...")
- Any attribution that names an MNTN employee or contact

**Why:** the user explicitly asked 2026-05-01 (decks) and reaffirmed 2026-05-04 (tickets — TI-886 had "Matt explicitly does not want to own implementation" which had to go). Reasons not stated but inferable:
- Artifacts get forwarded outside the original audience; named attributions become tribal-knowledge gotchas
- Attributing preferences/opinions to one person can imply they own a position they didn't formally take
- Personal preferences shift; statements about who wants what become stale faster than the technical content
- Strips noise; readers focus on the work, not the politics

**How to apply:**
- When writing new content, never add a name. Frame the insight as the team's / the data's / the methodology's / the work's.
- When editing existing content, strip name attributions on first pass. Replace with neutral framings: "Channel context: a CTV ad's call-to-action..." instead of "Channel context (Alex Bloore):..."
- Code paths and branch names (e.g., `mbrorby/workspace/impression-uplift`) are technical identifiers, not opinion attributions — keep them unless explicitly asked to remove.

**Where attributions ARE allowed (working docs, not shared artifacts):**
- Workspace `summary.md` files
- `knowledge/*.md` (cross-session continuity needs source-of-truth attributions)
- `methodology_defense.md` and similar Q&A backup docs
- Memory files
- Meeting transcripts and per-meeting actions docs (records, not deliverables)

**If the user asks for a specific attribution to be added in a shared artifact, do so — but never add one proactively.**

**Exception:** speaker self-attributions ("I measured...", "we found...") and direct quotes the user explicitly drafted stay as-is.

**Standing carve-out (2026-05-08):** the user's own name — **Malachi Dunn** — must always appear on the first/title slide of any deck. See `feedback_author_name_on_first_slide.md`. This is an explicit, durable exception. Other people's names still don't appear in shared artifacts.

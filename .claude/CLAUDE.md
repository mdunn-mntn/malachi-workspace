# MNTN Workspace — Project Instructions

See global `~/.claude/CLAUDE.md` for the full operating rules (always-on behaviors, naming conventions, commit protocol, empirical analysis protocol, BQ safety rules). This file adds project-specific paths and structure.

## Workspace Structure

```
workspace/
├── knowledge/            ← shared data docs — source of truth, in git, org-accessible
│   ├── data_catalog.md   ← table schemas, partitions, join keys, query tips
│   ├── data_knowledge.md ← business logic, gotchas, tribal knowledge
│   ├── mntn_business.md  ← general MNTN business knowledge, products, org, industry
│   ├── experimentation.md ← experiment methodology, covariate selection, test design lessons
│   └── README.md
├── tickets/
│   ├── _template/        ← copy summary_template.md when starting a new ticket
│   └── ti_xxx_name/      ← one folder per ticket (lowercase, underscores)
│       ├── summary.md    ← required
│       ├── queries/      ← .sql files
│       ├── outputs/      ← csvs, jsons, query results
│       ├── meetings/     ← meeting transcripts, notes
│       └── artifacts/    ← notebooks, pdfs, scripts, deliverables
├── slack_bot/            ← Slack knowledge extraction bot (runs on Pi 5 at midnight PST)
├── documentation/        ← reference docs, architecture diagrams, code snippets
├── self_review/          ← performance self-assessment (gitignored, never committed)
├── claude-prompts/       ← planning files and prompt templates
└── .claude/scripts/      ← Claude tooling scripts (bq_run.sh, etc.)
```

## Key Paths

| Path | Purpose |
|------|---------|
| `README.md` | Workspace structure, philosophy, and how-to — read at session start, update when workspace conventions change |
| `knowledge/strategic_north_star.md` | **Q2 OKR leverage filter** — read at session start, evaluate every task against it, flag low-leverage work |
| `knowledge/data_catalog.md` | Table schemas and join keys — read at session start, update immediately when new schema learned |
| `knowledge/data_knowledge.md` | Business logic and gotchas — read at session start, update immediately when new knowledge found |
| `knowledge/mntn_business.md` | General MNTN business knowledge — products, strategy, org, industry, terminology. Update when learning business context from docs, meetings, or conversations |
| `knowledge/experimentation.md` | Experiment methodology, covariate selection, test design lessons — update when working on any experiment/analysis ticket |
| `knowledge/folder_definitions.md` | **Exact definition of what goes in every folder** — check here before placing any file |
| `tickets/_template/summary_template.md` | Copy this when starting a new ticket — internal working doc |
| `tickets/_template/presentation_template.md` | Copy this when starting a new ticket — external-facing narrative for sharing |
| `.claude/scripts/bq_run.sh` | BQ query wrapper — logs performance metrics to `knowledge/bq_perf_log.jsonl` |
| `.claude/scripts/transcribe.sh` | Meeting transcription — runs both OpenAI (whisper-1) and local mlx-whisper, merges best of both (OpenAI accuracy backbone + local coverage patches). Use `--provider openai` or `--provider local` to force one. `--keep-both` saves individual provider files. |
| `knowledge/bq_perf_log.jsonl` | Append-only log of BQ query performance (bytes, slots, wall time, cache hits) |
| `knowledge/slack_review_queue.md` | Medium-confidence Slack extractions needing manual review |
| `slack_bot/` | Slack knowledge extraction bot — scraper, extractor, updater. Runs on Pi 5 at midnight PST. Add bot to channels via `/invite @Knowledge Extractor` |
| `self_review/summary.md` | Self-review guide — workflow, rubric, leadership direction (Paulo/Kale/Alyson), how to write rationales |
| `self_review/self_review_2.md` | **Active self-review** — update after every ticket (gitignored) |
| `self_review/self_review_1.html` | Submitted review #1 (archived, do not modify) |
| `documentation/docs/presentation_playbook.md` | **Presentation standards** — read before creating any presentation. Power Line, structure, storytelling, persuasion, delivery, checklists |
| `documentation/docs/revealjs_guide.md` | **RevealJS layout guide** — config, font sizes, cutoff prevention rules, standalone build process. Read before building any RevealJS deck. |
| `documentation/docs/revealjs_guide.md` | **RevealJS layout guide** — config, font sizes, cutoff prevention rules, standalone build process. Read before building any RevealJS deck. |

## Self-Review Entry Guide

When adding entries to the active self-review, consider these for every piece of work:
- **Rubric criteria for a 4**: Speed (no oversight, independent blocker resolution, balancing tasks), Craft (quality, standards, technology understanding, credibility), Adaptability (adapting to change, ambiguous problems, supporting peers)
- **Business impact**: tie work to Kale's focus areas — revenue growth, revenue retention, cost reduction
- **Paulo's framing**: frame work as "explaining why the system behaves this way" and "being the go-to person for ecosystem questions." Use verbs like "explained why," "gave the team a clear picture of," "go-to reference"
- **At review time**: argue the rubric not a ticket list, 3-5 high-impact tasks per section, ~9 different tasks across all three sections, format for scannability, one improvement per section

Full guide in `self_review/summary.md`.

## Ticket Work Protocol

**When working on any ticket**, always read `tickets/ti_xxx_name/summary.md` first to orient to the current state, open items, and file structure. This is the ticket card — it tells you what's been done, what's pending, and where everything lives.

## File Naming Convention

**Folder names** carry the descriptive label: `ti_650_stage_3_vv_audit/`

**File names** inside a ticket use the ticket prefix + short descriptor — NOT the full folder description:
- `ti_650_summary.md` (not `stage_3_vv_audit_summary.md`)
- `ti_650_audit_trace_queries.sql` (not `stage_3_vv_audit_trace_queries.sql`)
- `ti_650_column_reference.md` (not `vv_ip_lineage_column_reference.md`)

Pattern: `ti_xxx_short_name.ext` — the ticket number is the anchor, the filename is descriptive of the file's purpose.

Exception: `summary.md` at the ticket root can remain just `summary.md` (it's the standard template file).

## Ticket Deliverables: summary.md vs presentation.md

Every ticket has a `summary.md`. Some tickets also get a `*_presentation.md` in `artifacts/`. These are fundamentally different documents with different audiences, standards, and purposes.

### summary.md — The Analytical Record

- **Audience:** You (future you), collaborators who need full context
- **Purpose:** Complete, honest, evolving record of the work — findings, dead ends, open questions, methodology, gotchas
- **Tone:** Precise, thorough, technical. Include everything someone would need to pick up where you left off.
- **Structure:** Follows the summary template. Sections filled as work progresses. Updated continuously.
- **Data:** Full tables, exact numbers, all caveats, all limitations. Nothing rounded or simplified.
- **What belongs here:** Every finding, every failed approach, every assumption, every open question. SQL column names are fine. Technical jargon is fine. Length is fine.
- **Standards:** Accuracy and completeness. No playbook rules apply.

### *_presentation.md — The Persuasion Artifact

- **Audience:** The room — leadership, cross-functional stakeholders, the team. People who need to decide or act.
- **Purpose:** Move the audience to a specific belief or action. Not to document — to persuade.
- **Tone:** Bold, concise, narrative. Says less than the summary, but says it better.
- **Structure:** Three-act (Disruption → Revelation → Resolution). NOT the summary reordered — a different document built from scratch using the summary as raw material.
- **Data:** One number per point. Rounded for business audiences. Anchored with context. Contrast over absolutes. Full tables in appendix only.
- **What belongs here:** Only what serves the Power Line. Kill everything else. If it doesn't help the audience believe your one thing, it goes in the appendix or stays in the summary.
- **Standards:** Full Presentation Playbook applies (see below).

### The Workflow

1. **Do the work** → update `summary.md` continuously (findings, queries, iterations)
2. **When it's time to present** → create `artifacts/ti_xxx_presentation.md` as a NEW document
3. **Mine the summary** for insights, but rewrite them as narrative — don't copy-paste sections
4. **Build visualizations** → generate exec-quality charts following Tufte principles (see Visualization Standards below). Every presentation with quantitative findings must have accompanying charts.
5. **The summary is the source of truth.** The presentation is the highlight reel. They should never contradict each other, but the presentation will intentionally omit most of what's in the summary.

### When to Create a Presentation

Not every ticket needs one. Create `*_presentation.md` when:
- You're presenting findings to a group (team meeting, stakeholder review, cross-functional share-out)
- Leadership needs a digestible version of complex analysis
- The work produces a recommendation that requires buy-in
- Someone asks "can you walk us through what you found?"

If the ticket is internal housekeeping, a quick investigation, or a simple bug fix — `summary.md` is sufficient.

## Presentation Standards

When creating or editing any presentation file (slides, decks, `*_presentation.md`, or any artifact intended for an audience):

1. **Read `documentation/docs/presentation_playbook.md` first** — it is the authoritative guide for all presentation work.
2. **Every presentation must have a Power Line** — one sentence (10 words or fewer) the audience will remember. Write it before building anything else.
3. **Structure:** Three-act (Disruption → Revelation → Resolution). Never present findings in discovery order — lead with the insight.
4. **Opening:** Use one of the five proven openers (Startling Stat, Question, Story, Bold Claim, Contrast). Never start with "So today I'm going to talk about..."
5. **Data slides:** One number per slide. Anchor before reveal. Use contrast over absolutes. Round for business audiences.
6. **Rule of Three:** Three takeaways, three categories, three next steps. Not four.
7. **Story requirement:** At least one story per presentation using the Hall framework (character + emotion + moment + specific detail).
8. **Close:** End on the Power Line or a clear call to action. Never end with "that's all I have" or "any questions?"
9. **Audience adaptation:** Technical = show rigor + methodology. Business = lead with implication + round numbers. Mixed = headline up front, detail in appendix.
10. **Billboard Test:** Every slide must be graspable at a glance. One idea per slide. Kill bullet points where possible.

**Cialdini checklist for persuasive presentations:**
- Social proof (who else validates this?)
- Authority (methodology rigor, scale)
- Scarcity (why now?)
- Commitment ladder (small yes before big ask)
- Reciprocity (give insight freely)
- Unity ("we" not "I")

**Default critique process:** After finishing or substantially revising any `*_presentation.md`, run the critique prompt at `claude-prompts/presentation_critique.md` against it. This is the default — do not skip it. The critique scores 10 areas (Power Line, Opening, Narrative, Story, Data Persuasion, Cialdini, Billboard Test, Close, Audience Adaptation, Boldness) on a 1-5 scale and produces a prioritized fix list. Apply the fixes before considering the presentation done.

## Visualization Standards

Every presentation with quantitative findings must include accompanying data visualizations. Follow these standards (full details in Part 8 of `documentation/docs/presentation_playbook.md`).

### Tufte Principles (Non-Negotiable)

1. **Maximize data-ink ratio.** Remove gridlines, borders, background fills, legends (use direct labels), 3D effects, shadows. Every pixel should encode data.
2. **Color encodes meaning, never decoration.** One accent color for the key insight (red), supporting data (navy), context (gray). Never decorative gradients.
3. **Lie factor = 1.** Linear scales for exec audiences. If the effect is 184x, show 184x visually. No log scales that compress dramatic differences.
4. **Annotate, don't decorate.** Every chart gets a one-line interpretation stating the business implication. The audience should never have to decode what the chart means.
5. **Small multiples > complex single charts.** When comparing across 5+ categories, use a grid of simple charts rather than one overloaded chart.
6. **Direct label data points.** Put the number on or next to the bar/dot. Don't make the audience cross-reference to an axis.

### Chart Generation Standards

- **Font:** Helvetica Neue (or system equivalent). Never matplotlib defaults.
- **Background:** Light off-white (#FAFAFA), not pure white.
- **Resolution:** 200 DPI minimum for PNGs.
- **Script:** Every chart set must have a `generate_charts.py` script in `artifacts/` for reproducibility. Data comes from CSVs in `outputs/`, not hardcoded.
- **Titles:** State the finding, not the metric. "Top-Ranked Keywords Drive 184x More Visits" not "Visit Rate by Keyword Rank Bucket."
- **Subtitles:** One line of context/methodology in gray below the title.

### Dual Output: Static + Interactive

- **Static PNGs** (`artifacts/ti_xxx_chart_*.png`): For Jira, Slack, email, documentation, async review. Always generated.
- **Interactive RevealJS HTML** (`artifacts/ti_xxx_presentation_deck.html`): For live team presentations. Progressive reveal, hover tooltips, animated transitions. Generated when presenting to a live audience.

RevealJS approach: write content in markdown, convert to a self-contained HTML file using RevealJS CDN. Charts embedded as inline SVG or base64 PNG. The team (Jason Mills, Mike Dolt) uses this format.

### Chart Workflow

1. **Run analysis** → save results to `outputs/*.csv`
2. **Write `generate_charts.py`** in `artifacts/` — reads CSVs, produces PNGs following Tufte principles
3. **Review charts** against the Chartjunk Checklist (playbook Part 8): Can I remove this element? Does this color encode data? Could a table replace this chart?
4. **If presenting live** → also generate RevealJS HTML deck with progressive reveal
5. **Reference charts** in both `presentation.md` and `summary.md`

## Codex Review
Codex will review your code after you're done. Write with that in mind — keep code clean, well-structured, and ready for automated review.

## Google Drive

Mounted at `~/Library/CloudStorage/GoogleDrive-malachi@mountain.com/My Drive/`.
Ticket-specific Drive files are listed in each `tickets/ti_xxx/summary.md` under "Drive Files".
Drive files cannot be committed to git — reference their paths in summaries only.

## Git
- Remote: `git@github.com:mdunn-mntn/malachi-workspace.git`
- Root: `/Users/malachi/Developer/work/mntn/workspace/`
- Commit and push after every meaningful change — no batching
- No `Co-Authored-By` lines

# Plan 4: Self-Review Feedback Loop System

## Goal
Build a continuously-updated self-review system that captures evidence of Speed, Craft, and Adaptability from every task — so that when performance review time comes, the rationale writes itself.

---

## The Rubric (from MNTN)

| Category | 4 - Excellent | 3 - Meets Expectations |
|----------|--------------|----------------------|
| **Speed** | Meets deadlines with no oversight. Resolves blockers independently. Exceeds expectations balancing multiple tasks. | Meets deadlines with minimal oversight. Communicates issues promptly. Manages workload for consistent progress. |
| **Craft** | High-quality, maintainable, scalable work. Proactively sets standards. Exceptional tech stack understanding. Highly credible. | Reliable, maintainable work. Participates in reviews. Solid tech understanding. Trusted by peers. |
| **Adaptability** | Quickly adapts to changing priorities. Consistently solves ambiguous problems. Supports peers through change. | Adjusts effectively with minimal disruption. Handles uncertainty professionally. Seeks clarity proactively. |

### Boss' Boss Priorities
The three most important things at a company:
1. **Revenue growth**
2. **Revenue retention**
3. **Cost reduction**

### Boss' Priorities
- Sharing what you're doing and knowledge with others
- Presentations to the overall engineering team

---

## Deliverables

### File 1: `self_review_2026.md` — Running Evidence Log
**Location:** `/Users/malachi/Developer/work/mntn/workspace/self_review/self_review_2026.md`

Structure:
```markdown
# Self-Review 2026 — Malachi

## Speed

### Evidence Log
| Date | Ticket/Task | Evidence | Score Indicator |
|------|------------|---------|-----------------|
| 2026-02-24 | TI-650 | Built v2→v3 trace pipeline in 2 weeks, independently resolved IP type mismatch, win_log ID mismatch, and NULL device_ip blockers without escalation | 4 |
| ... | ... | ... | ... |

### Current Rationale (Draft)
{Polished paragraph for the review form, updated periodically}

---

## Craft

### Evidence Log
| Date | Ticket/Task | Evidence | Score Indicator |
|------|------------|---------|-----------------|
| 2026-02-24 | TI-650 | Designed 5-checkpoint IP trace methodology that decomposed 21.2% aggregate mutation into precise hop-level measurements. Discovered ALL mutation occurs at redirect hop, not visit — correcting a prior assumption. | 4 |
| ... | ... | ... | ... |

### Current Rationale (Draft)
{Polished paragraph for the review form, updated periodically}

---

## Adaptability

### Evidence Log
| Date | Ticket/Task | Evidence | Score Indicator |
|------|------------|---------|-----------------|
| 2026-02-24 | TI-650 | When BQ pipeline gap blocked the original plan (raw.visits/CIL stopped ingesting Jan 31), pivoted to Greenplum-first approach and documented BQ port requirements for when data resumes. | 4 |
| ... | ... | ... | ... |

### Current Rationale (Draft)
{Polished paragraph for the review form, updated periodically}

---

## Revenue Impact & Knowledge Sharing

### Revenue Growth / Retention / Cost Reduction
| Date | Ticket/Task | Impact | Category |
|------|------------|--------|----------|
| 2026-02-24 | TI-650 | Identified 4,006 phantom NTB events/day for one advertiser — returning visitors misclassified as new-to-brand. Fixing this improves targeting accuracy and advertiser trust (revenue retention). | Revenue Retention |
| ... | ... | ... | ... |

### Knowledge Sharing & Presentations
| Date | What | Audience | Notes |
|------|------|----------|-------|
| ... | ... | ... | ... |
```

### File 2: `review_rubric.md` — Reference Copy
**Location:** `/Users/malachi/Developer/work/mntn/workspace/self_review/review_rubric.md`

A clean markdown version of the CSV rubric for easy reference.

---

## The Feedback Loop

### How it works — Passive Collection

1. **After every ticket or significant task**, Claude reviews the work and proposes entries:
   - Which category does this demonstrate? (Speed / Craft / Adaptability)
   - What specific evidence supports it?
   - What score level does it indicate? (use rubric language)
   - Does it tie to revenue growth/retention/cost reduction?

2. **The user approves/edits** the proposed entries

3. **Entries accumulate** in the evidence log throughout the year

### Periodic Consolidation (Monthly or On-Demand)

When asked (or monthly), Claude:
1. Reviews all evidence entries since last consolidation
2. Drafts updated "Current Rationale" paragraphs for each category
3. Identifies gaps — categories with thin evidence that need attention
4. Suggests actions to strengthen weak areas

### Review Time (Semi-Annual / Annual)

When the actual review form needs to be filled:
1. Evidence log has 6-12 months of entries
2. Claude drafts the final rationale for each category
3. Rationale is backed by specific, quantifiable examples
4. Revenue impact section ties work to business outcomes
5. Knowledge sharing section documents presentations and mentoring

---

## Seeding with Existing Work

### From the VV Audit (TI-650) — Already Known

**Speed:**
- Built the initial trace query (v1), iterated to v2 (4-checkpoint), then v3 (5-checkpoint with clickpass starting point) within weeks
- Independently resolved multiple blockers: IP type mismatch (/32 inet), win_log Beeswax ID confusion, NULL device_ip, BQ data gaps
- Managed parallel investigation across BigQuery and Greenplum environments

**Craft:**
- Designed a novel 5-checkpoint IP trace methodology
- Decomposed aggregate 21.2% mutation into hop-level precision — discovered 100% occurs at redirect
- Built NTB disagreement × mutation cross-tab revealing 4,006 phantom NTB events
- Documented 18 discrete accomplishments with quantified results
- Thorough edge case documentation (10 implementation notes)

**Adaptability:**
- Pivoted from BQ-first to Greenplum-first when BQ pipeline gap was discovered
- Reframed v2 findings when v3 revealed "mutation at visit" was actually "mutation at redirect"
- Adapted methodology when clickpass_log proved to be a better starting point than ui_visits

**Revenue Impact:**
- Phantom NTB finding directly impacts revenue retention (advertiser trust in targeting accuracy)
- Per-campaign mutation variance (0.6% to 20.6%) enables targeted fixes for worst-offending campaigns
- Cross-device mutation insight (61.2%) informs infrastructure investment decisions

### From Other Tickets
We should review the existing ticket folders and extract any additional evidence. Many of the `ti_*` folders likely have work worth documenting.

---

## Integration Points

### With Ticket System (Plan 2)
Every ticket summary has a "Performance Review Tags" section at the bottom. These tags are the raw material that feeds into the self-review evidence log.

### With CLAUDE.md (Plan 3)
The global instructions include a directive to propose self-review entries after completing significant work.

### With Data Documentation (Plan 1)
Contributions to data documentation demonstrate Craft (setting standards, maintaining quality) and can be cited as evidence.

---

## Maintaining the "Excellent" Bar

To consistently score 4s, the evidence should demonstrate:

**Speed (score 4):**
- "no oversight" — resolved blockers independently
- "exceeding expectations" — delivered more than asked

**Craft (score 4):**
- "sets standards" — created reusable methodologies, documentation
- "exceptional understanding" — deep knowledge of the tech stack
- "highly credible" — trusted as an authority

**Adaptability (score 4):**
- "embraces new challenges proactively" — didn't wait to be told
- "solves ambiguous problems" — turned unclear requirements into concrete results
- "supports peers" — shared knowledge, helped others

---

## Estimated Effort
- **Initial setup (files, seed from VV audit):** ~45 min
- **Rubric markdown conversion:** ~10 min
- **CLAUDE.md integration:** Part of Plan 3
- **Ongoing:** 2-3 min per ticket (approve/edit proposed entries)
- **Monthly consolidation:** ~15 min

---

## Success Criteria
- By review time, each category has 10+ specific, quantified evidence entries
- Rationale paragraphs practically write themselves from the evidence
- No last-minute scrambling to remember what you did 6 months ago
- Revenue impact is clearly articulated with specific numbers
- Knowledge sharing is documented with dates and audiences
- The review is genuinely impressive — backed by data, not vague claims

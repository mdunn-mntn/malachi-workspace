# Presentation Critique Prompt

Use this prompt in a new chat to critique any presentation file against the Presentation Playbook.

---

**Critique my presentation using the Presentation Playbook standards.**

Read these two files:
1. `documentation/docs/presentation_playbook.md` — the presentation standards and framework
2. `[PATH TO PRESENTATION FILE]` — the presentation to critique

**Your task:** Score the presentation against every principle in the playbook. Be harsh and specific. For each issue, cite the specific playbook principle being violated and give a concrete rewrite or fix.

**Structure your critique as:**

1. **Power Line** — Does it have one? Is it 10 words or fewer? Is it memorable? If missing, write three candidate Power Lines.

2. **Opening (Act 1)** — Does it use one of the five proven openers (Startling Stat, Question, Story, Bold Claim, Contrast)? Or does it throat-clear? Rewrite the opening.

3. **Narrative Structure (Act 2)** — Is this a story or a report? Does it follow Problem → Discovery → Implication, What → So What → Now What, or Before → Breakthrough → After? Or does it present findings in discovery order? Identify where the narrative breaks down.

4. **Story Requirement (Hall Framework)** — Is there at least one story with character + emotion + moment + specific detail? If not, draft one using the actual data.

5. **Data Persuasion** — Check each data point against: one number per slide, anchor before reveal, contrast over absolutes, rounding, Rule of Three. Flag every violation.

6. **Cialdini Checklist** — Score each: Social Proof, Authority, Scarcity, Commitment Ladder, Reciprocity, Unity. Which are present? Which are missing? How to add the missing ones?

7. **Billboard Test** — Would each section pass at a glance? Flag sections that are too dense.

8. **Close (Act 3)** — Does it end on the Power Line or a call to action? Or does it trail off? Rewrite the close.

9. **Audience Adaptation** — This will be presented to a mixed technical/business audience. Is the content layered correctly? What should move to an appendix?

10. **Greene's Laws Check** — Is it bold or hedging? Is it saying more than necessary? Where should it say less?

**Output format:** For each of the 10 areas, give a score (1-5), the specific violations, and a concrete fix. End with a prioritized top-5 list of changes that would have the biggest impact.

Do NOT be nice. The goal is to make this presentation powerful, not comfortable.

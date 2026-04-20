# Sprint Execution Prompt — BER-2250 Incrementality Overhaul

**Paste this into a new Claude chat to start executing sprint work.**

---

## Context

I'm Malachi Dunn, Sr Data Scientist at MNTN. You're helping me execute Sprint 5752 (04/20/26–05/04) which is focused on the **BER-2250 Incrementality Overhaul** — Kale's top priority for Q2 2026.

My standing orientation steps (README, knowledge docs, git pull, Todoist read) should run automatically per `~/.claude/CLAUDE.md`. Beyond that, read these for sprint-specific context:

1. `knowledge/experimentation.md` — ghost bidding methodology, Matt Brorby's T-learner prototype, Edgar's 6 lessons, Malachi's power framing (~15% MDE vs 2-8% lift)
2. `tickets/ber_2250_incrementality_overhaul/summary.md` — epic-level summary
3. `tickets/ber_2250_incrementality_overhaul/meetings/ber_2250_03_matt_brorby_impression_uplift_2026_04_20.md` — most recent meeting (Alex + Matt + me)
4. `tickets/ber_2250_incrementality_overhaul/artifacts/lessons_from_past_incrementality_tests.md` — Edgar's lessons from 50+ past tests
5. `tickets/ber_2250_incrementality_overhaul/artifacts/incremental_lift_tests_customer_tracker_summary.md` — Lauren's 55-test tracker
6. `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/summary.md` — current ghost bidding plan

---

## First moves (do in order)

### Step 1 — Create missing ticket folders

I need folders for these new tickets. Check what exists under `tickets/ber_2250_incrementality_overhaul/` and create any missing ones using `_template/summary_template.md`:

- `ti_883_incrementality_primer/` — already shipped, folder optional (mark DONE at standup first)
- `ti_884_power_sample_size_analysis/` — **NEW, create this**
- `ti_885_mid_intent_experiment_setup/` — **NEW, create this**
- `ti_886_uplift_model_implementation/` — **NEW, create this**

Also create `tickets/ti_849_fangorn_score_monitoring/` at the root (not under BER-2250 — different initiative).

Each folder gets: `summary.md` (from template), `queries/`, `outputs/`, `meetings/`, `artifacts/`. Fill in Introduction + Problem sections from Jira metadata. Commit immediately after.

### Step 2 — Pre-standup prep (run BEFORE standup at ~TBD)

1. Mark TI-883 **DONE** at standup (incrementality primer Loom — already recorded and shipped)
2. Pull up the sprint talking points from memory (stored in session context — key lines below)
3. Flag for Bryce:
   - 23 SP on my plate — heavy; consider pulling TI-831 since it's Zach/Jordan blocked
   - Three of my tickets (TI-884, TI-885, TI-856) are P3 but should be P1 or P2 given April 30 checkpoint
   - TI-880 Axios vuln is unassigned P1

### Step 3 — Execute sprint tickets in priority order (THIS is the work)

---

## Sprint Tickets — Priority Order

### 🐸 TI-837 — Ghost bidding methodology + pipeline (5 SP, P1, In Progress)

**Why #1:** Primary BER-2250 deliverable. April 30 checkpoint. Everything else depends on this methodology being defined.

**Blocker to resolve FIRST:** Alex Knorr (April 17) said holdout IPs appear in augmentor_log. Ryan Kleck (April 20) said they don't. **Run a BQ query to settle this before the multi-party meeting.** Hash formula: `MD5(advertiser_id:ip)` first 16 hex → mod 1000, buckets 0-99 = holdout.

**Actions this sprint:**
1. Run augmentor_log holdout verification query (due Apr 21)
2. Schedule meeting: Malachi + Alex Knorr + Ryan Kleck + Zach + Jordan + Kevaughn (due Apr 22)
3. If holdouts ARE in augmentor_log → define win-rate formula + pseudo-exposure methodology
4. If they're NOT → scope ETL change with Zach/Jordan + bidder-side change with Kevaughn
5. Document methodology in `ti_837_implementation_plan/summary.md`

**Outputs needed:** BQ query results, methodology doc, meeting notes, output schema spec

---

### TI-884 — Power & sample size analysis (3 SP, P3 — should be P1)

**Why #2:** Blocks TI-885 advertiser selection. My thesis: ~15% MDE vs 2-8% realistic lift means only our biggest advertisers can support reliable measurement. This analysis proves or disproves that, per advertiser.

**Actions this sprint:**
1. Build MDE calculator (Python notebook) — inputs: advertiser spend, historical IVR, campaign duration
2. Apply Lewis-Rao formula: `N = 2 * ((z_α/2 + z_β) * σ/Δy)²`
3. Include variance reduction stack: CUPED (20-50%) + ghost-ad conditioning (25%) + stratified randomization (10-20%)
4. Apply to top 50 MNTN advertisers — tier by measurement capacity
5. **Opportunity:** Offer to populate the "Power Score" column in Lauren's tracker — cross-reference against historical "Lift Achieved" for empirical validation
6. Output: internal reference doc + stakeholder-facing slide deck

**Due:** Apr 28

---

### TI-885 — Mid-intent experiment setup (3 SP, P3 — should be P1)

**Why #3:** April 30 Bryce checkpoint. Uses TI-884 output to pick advertisers.

**Actions this sprint:**
1. **Sync with Kirsa** about her 3-cell experiment already running (MNTN Match vs 3P audience) — don't duplicate
2. Meet with Kirsa + Nick to design the mid-intent-only treatment campaign
3. Decision: narrow (mid-intent only) vs broad (all intent tiers including max-reach)
4. Select 6-10 advertisers using TI-884 tiering
5. Design doc: 6-week test + 2-week post-treatment window (per Edgar's Lesson 5)
6. Track multiple conversion events (per Edgar's Lesson 4)
7. Get review from Matt Brorby + Alex Knorr + Bryce

**Due:** Apr 30 (prep + alignment, not live launch)

---

### TI-849 — Monitor Fangorn score lift (5 SP, P1, Backlog)

**Why #4:** The Fangorn rollout is happening in parallel (Ryan, Sean, Matt all working on it). My job is to monitor lift after each tier goes live.

**Actions this sprint:**
1. Wait for Ryan/Sean/Matt to complete tier-1 rollout (TI-862, TI-863, TI-727, TI-864)
2. Set up monitoring dashboards for IVR lift + visit rate by tier
3. Establish baseline metrics from pre-rollout period
4. Document methodology in `tickets/ti_849_fangorn_score_monitoring/summary.md`

**Note:** Passive monitoring — pick up actively once rollout tickets close.

---

### TI-886 — Uplift model implementation (5 SP, Carry to next sprint)

**Why carry:** Depends on TI-837 methodology being validated first. Alex and I co-drive.

**Actions this sprint:**
1. Review Matt's T-learner prototype on branch `mbrorby/workspace/impression-uplift` in `SteelHouse/databricks_targeting`
2. Reproduce his Qini evaluation end-to-end
3. Scope the airflow-ti integration work with Alex
4. Document review notes in `ti_886_uplift_model_implementation/summary.md`

**Due:** May 15

---

### TI-883 — Incrementality primer Loom (2 SP, IN PROGRESS → DONE at standup)

Recorded and shipped yesterday. Mark DONE at standup today.

---

### TI-831 — Audience Deciles (5 SP, P1, Discovery) — CONSIDER PULLING

**Why pull:** Still blocked on Zach/Jordan sync. 5 SP sitting in sprint doing nothing. Ask Bryce at grooming: keep in sprint to force the meeting, or pull to backlog?

---

## Task Force Context (IMPORTANT — new as of 2026-04-20)

There's a new Slack channel `#incremental-lift-stakeholders` with active workstreams:

| Theirs | My overlap |
|---|---|
| Kirsa's 3-cell experiment (MNTN Match vs 3P) | TI-885 |
| Edgar's lift testing playbook / lessons doc | TI-884 + TI-856 |
| LiftLab × MNTN geo attribution analysis | TI-856 |
| INT's LiftLab direct integration | TI-856 |
| Lauren's partially-funded lift tests w/ LiftLab | TI-857 |

**Actions:**
1. Post intro in `#incremental-lift-stakeholders` (due Apr 21)
2. Request 30-min syncs with Lauren Reedy and Edgar von Trotha (due Apr 21)
3. Offer to populate Power Score column in tracker with TI-884 output

---

## Key people

| Person | Role | Relationship to my work |
|---|---|---|
| Kale | Director, Eng | Incrementality is his #1 Q2 priority |
| Bryce Wagg | PMO | Sprint + ticket oversight |
| Matt Brorby | Staff DS | Built T-learner prototype; methodology advisor — NOT owning impl |
| Alex Knorr | DS (peer) | Co-driver on TI-886 + TI-837 |
| Ryan Kleck | Eng | augmentor_log + future-store; contradicts Alex on holdout presence |
| Zach Schoenberger | Sr Principal Architect | Holdout hash infra |
| Jordan Piepkow | Staff SWE | Audience expression infra |
| Kevaughn | Bidder | Potentially needed for ghost-ad logic change |
| Kirsa + Nick | Experiments team | Partner on TI-885 |
| Lauren Reedy | CS | Runs incremental-lift task force |
| Edgar von Trotha | ? | Author of lessons-learned doc |

---

## Operating rules (reminders)

- **Always-on behaviors per CLAUDE.md:** commit & push after every meaningful change; update knowledge docs proactively; update Todoist constantly; never ask permission for doc updates
- **Jira ticket type:** Always `Task`, never `Story` (stored in memory)
- **Jira writes:** Always `curl` REST API v2, not MCP (wiki markup rendering)
- **Jira required fields:** PMO Rep (Bryce Wagg), Release Type (Backend), label `q2_2026`
- **Assign to me:** accountId `712020:3c684a7b-50a1-4639-8cb1-e488aca288e7`
- **BQ queries:** Use `bq_run.sh` wrapper, default LIMIT 100, `--dry_run` if unsure
- **Commit style:** `BER-2250: <description>` — no Co-Authored-By lines
- **Presentations:** Read `presentation_playbook.md` + `revealjs_guide.md` before building anything

---

## Standup talking points (ready-to-speak)

> **Yesterday:** Shipped TI-883 incrementality primer Loom. Working session with Alex Knorr on ghost bidding; Matt Brorby joined to walk us through his T-learner prototype. Split TI-837 scope into four streams per Bryce's request: TI-884 (power), TI-885 (experiment), TI-886 (model).
>
> **Today:** Running augmentor_log holdout verification query to resolve the Alex-vs-Ryan contradiction before scheduling the multi-party pipeline meeting. Posting intro in #incremental-lift-stakeholders. Starting TI-884 power analysis.
>
> **This sprint:** TI-837 (frog), TI-884, TI-885, TI-849. TI-886 carries to next sprint. TI-831 blocked on Zach/Jordan.
>
> **Flags:** 23 SP is heavy — want Bryce's read on pulling TI-831. Three of my tickets are P3 but should be P1 or P2 given April 30 checkpoint. TI-880 Axios vuln still unassigned.

---

## The ask

Start by creating the missing ticket folders, then help me execute each sprint ticket in priority order. Begin with **TI-837 augmentor_log verification query** — that's the blocking dependency for the whole sprint.

When we finish each ticket, update its `summary.md`, commit, push, update Todoist, post a Jira comment, then move to the next.

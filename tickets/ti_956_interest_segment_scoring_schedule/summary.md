# TI-956: Scheduled Interest Segment Scoring Job

**Jira:** https://mntn.atlassian.net/browse/TI-956
**Status:** In Progress
**Date Started:** 2026-05-27
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction
Make LiveRamp interest-segment quality + performance scores available on a
recurring schedule, written to a GCS path. Not a production deployment — a
scheduled job that runs Alex's scoring class on a weekly or monthly cadence so
consumers (Macie, downstream UI/admin tooling) don't have to ask Alex to rerun
the notebook every time.

Source code: [targeting-infra-ml#57](https://github.com/SteelHouse/targeting-infra-ml/pull/57)
— quality scoring utilities + example notebook. Class is PySpark, ~two lines
to invoke the full scoring pipeline given the input dataset.

Origin: ask from Paulo + Allison to "use interest segments more." Allison +
Alex first proposed classifying by keywords/verticals; Paulo asked for
"something more nuanced." This scoring framework is the nuanced answer.

## 2. The Problem
- LiveRamp = ~90% of MNTN's interest segments. There are 200k+ interest segments total.
- Search for "households with $100K+ income" returns 90 segments — search is improved (Jeff Capone's AT demo) but there's **no quality signal**.
- Today the UI only surfaces segment size. Users have no way to pick the best one of 90.
- 11Lab is the first concrete use case: Mike asked Alex to score 150 interest segments to drop ~10 garbage ones and add ~20 BUK keywords while maintaining audience size (theory: keyword targeting > interest segments at the margin).

The scoring framework exists in a notebook. It needs to run on a schedule and
write to GCS so the rest of the org can build on top of it. Initial consumer
is admin users in the MNTN UI.

## 3. Plan of Action
1. Read [targeting-infra-ml#57](https://github.com/SteelHouse/targeting-infra-ml/pull/57)
   — `utils/segment` + the example notebook Alex will add.
2. Understand the inputs the scoring class needs (input dataset construction).
3. Decide hosting (per Alex: defer to me, consult Victor / Ryan):
   - Databricks scheduled notebook, OR
   - Vertex AI job triggered by Airflow DAG, OR
   - Regular PySpark on GCP compute in an Airflow DAG.
4. Define the schedule: **weekly or monthly** (LiveRamp segments update on
   LiveRamp's cadence — not daily). Monthly likely sufficient; confirm with Alex.
5. Define the GCS output path + schema. Coordinate with Macie on consumer expectations.
6. Run once end-to-end, verify outputs against Alex's manual notebook run.
7. Stand up the recurring schedule.

## 4. Investigation & Findings
### What the scoring computes (two layers)
**Quality of segment** (independent of campaign performance):
- IP activity in last 30 days
- Coverage vs the rest of our universe (a segment containing 90% of all observed IPs = generic blanket → not useful)
- Similarity to neighboring segments
- Update recency (some segments last updated in 2024 → low value)

**Performance of segment** (based on campaigns that historically used it):
- Aggregate IVR, conversion rate, etc. for campaigns targeting this segment

### Hosting trade-offs
- **Databricks scheduled notebook** — fastest path, easiest re-run. Alex has been working in Databricks. Downside: less standard for our production pipelining.
- **Vertex AI + DAG** / **Airflow + PySpark on GCP compute** — closer to how production pipelines should live. Alex's hint: "good opportunity for you to learn and build some of that pipelining stuff." Talk to **Victor or Ryan** before committing.

### Malachi suggestions for additional metrics (not yet in v1)
- **Visits per user** over 30-day window
- **Impact-weighted ranking** — for campaigns currently using interest segments, what would be the perceived improvement if they switched to the #1-ranked alternative? Helps prioritize which scores to surface where.
- **Filter aggressively in the UI** — don't show users 90 choices. Use the quality score to cut low-quality segments before surfacing.

## 5. Solution
_Pending._

## 6. Questions Answered
- **Q:** Why does this need a schedule rather than ad-hoc reruns?
  **A:** Consumers (Macie + admin UI) need fresh scores without asking Alex. LiveRamp segments update on their cadence — not daily, but they do update.
- **Q:** Daily / weekly / monthly?
  **A:** Weekly or monthly. Not daily — LiveRamp doesn't change that fast.
- **Q:** Where does output go?
  **A:** GCS path. Macie will consume it to build an admin-user-facing surface in the MNTN UI.
- **Q:** Is this production-grade?
  **A:** No. Interim step — get scores accessible to admin users. Productionization is a later ticket.
- **Q:** Where does the scoring code live?
  **A:** `targeting-infra-ml` PR #57, `utils/segment`. PySpark class, two lines to invoke.

## 7. Data Documentation Updates
_Pending._
- Possible: add LiveRamp interest segment scoring inputs/outputs to `knowledge/data_knowledge.md` once schema is settled.

## 8. Open Items / Follow-ups
- Decide host (Databricks vs Vertex+DAG vs Airflow+PySpark) — consult Victor / Ryan.
- Confirm GCS output path with Macie.
- Confirm weekly vs monthly cadence with Alex.
- Should the v1 output include impact-weighted ranking metrics, or hold for v2?

## 9. Meeting Notes
- `meetings/ti_956_01_malachi_alex_catchup_2026_05_27.txt` — same Malachi + Alex catchup. Interest-segment scoring is the second half of the meeting.
- **Next step:** Alex to add a usage-notebook example to the repo so Malachi can read through it. Tech deep-dive on hosting + schedule scheduled for early next week.

## Acceptance Criteria (from Jira)
- Data pipeline running which generates LiveRamp interest segment scores.

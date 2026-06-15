# TI-1033: TI Experiment Archive — Host, Ship, Polish

**Jira:** https://mntn.atlassian.net/browse/TI-1033
**Status:** In Progress (this sprint, 06/15–06/29) · 3 SP
**Parent work:** TI-1003 (closed/done — Phase 1 archive built)
**Assignee:** Malachi

---

## 1. Introduction
Follow-up to TI-1003. The TI experiment archive (manifest-driven static site, 8 experiments,
KPI-centric landing) is **built and committed** — it just isn't hosted yet. This ticket gets it live
at an internal bookmarkable URL and finishes the polish.

**Source repo:** `/Users/malachi/Developer/work/mntn/ti-experiment-archive/` (its own git repo, builds with
`python build.py` → `dist/`). Full Phase-1 record: `tickets/ti_1003_experiment_archive/summary.md`.
Plan: `/Users/malachi/.claude/plans/so-i-have-ticket-witty-kay.md`.

## 2. What's left (the task)
1. **Host** — confirm SteelHouse GitHub Enterprise has access-controlled internal Pages; fallback is a
   GCS bucket + IAP gated to `@mountain.com`. Decide + stand up.
2. **Deploy** — create the repo, wire the GitHub Action (`.github/workflows/deploy.yml`, already drafted), deploy.
3. **Verify access** — org members can load the URL, logged-out cannot. Legal/IT sign-off on showing
   revenue $ + advertiser names behind org SSO.
4. **Ship** — deliver the bookmarkable URL; post in the team channel.
5. **Polish** — clarity pass on the remaining experiment pages (TI-835/884/999/542); make the in-chart
   accent color follow result tone (red = win only); copy cleanup.

**Done when:** bookmarkable internal URL is live + access-controlled, all 8 experiments render cleanly.

## 8. Deferred to Phase 2 (separate follow-ups, NOT this ticket)
- Weekly auto-refresh for live experiments (Fangorn) from `RolloutTierEvaluations.py`.
- Confidence-discounted $ value per KPI + one caveated portfolio range (no false-precision grand total).

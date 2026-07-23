---
doc_type: ticket
title: "TI-1033: TI Experiment Archive — Host, Ship, Polish"
status: in_progress
date: 2026-06-15
summary: "Host and ship the built TI experiment archive at an access-controlled internal URL"
result: "in progress — archive built; hosting, deploy, access-control and polish still pending"
keywords: [ti-1033, ti-1003, experiment archive, static site, github pages, iap, gcs, fangorn, rollouttierevaluations]
---

## TL;DR

**Q:** What is TI-1033 and its current status?

**A:** TI-1033 hosts, ships, and polishes the TI experiment archive built under TI-1003. The archive is a manifest-driven static site (8 experiments, KPI-centric landing) that is built and committed but not yet hosted. Status: in progress — archive built; hosting, deploy, access-control, and polish still pending. Remaining task: (1) host at an access-controlled internal URL — confirm SteelHouse GitHub Enterprise has access-controlled internal Pages, fallback is a GCS bucket + IAP gated to @mountain.com; (2) deploy via the drafted GitHub Action (.github/workflows/deploy.yml); (3) verify org members can load / logged-out cannot, plus Legal/IT sign-off on showing revenue $ + advertiser names behind org SSO; (4) ship the bookmarkable URL and post in the team channel; (5) polish the remaining experiment pages (TI-835/884/999/542), make in-chart accent color follow result tone (red = win only), copy cleanup. Done when a bookmarkable internal URL is live and access-controlled and all 8 experiments render cleanly. Source repo: /Users/malachi/Developer/work/mntn/ti-experiment-archive/ (own git repo, builds with python build.py → dist/). Phase 2 deferred: weekly auto-refresh for live experiments (Fangorn) from RolloutTierEvaluations.py, and confidence-discounted $ value per KPI.

**How:** Read the full summary.md (front matter + sections 1, 2, 8 present). No queries/ or outputs/ directories exist. No Findings/Results section beyond the front-matter result line; status reported as front matter and section 2 state it.

**Learned:**
- TI experiment archive is a manifest-driven static site of 8 experiments with a KPI-centric landing, built under TI-1003 and hosted/shipped under TI-1033
- Source repo /Users/malachi/Developer/work/mntn/ti-experiment-archive/ builds with python build.py to dist/
- Hosting plan: SteelHouse GitHub Enterprise access-controlled internal Pages, fallback GCS bucket + IAP gated to @mountain.com
- Legal/IT sign-off needed to show revenue $ + advertiser names behind org SSO
- Phase 2 defers weekly auto-refresh for Fangorn from RolloutTierEvaluations.py

**Reuse when:**
- Deploying or locating the TI experiment archive site
- Questions about where TI experiment results are hosted internally
- Access-control / SSO gating for internal dashboards showing revenue or advertiser names

---

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

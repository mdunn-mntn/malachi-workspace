---
doc_type: ticket
title: "TI-1003: TI Experiment Archive"
status: done
date: 2026-06-15
summary: "Manifest-driven internal static site: portfolio scorecard of TI's measured impact"
result: "Phase 1 built + verified — 8 experiments seeded; repo/Pages deploy split to TI-1033"
---

# TI-1003: TI Experiment Archive

**Jira:** https://mntn.atlassian.net/browse/TI-1003
**Status:** Done (Phase 1 archive built + committed). Remaining host/deploy + polish split into **TI-1033** (this sprint, 3 SP).
**Date Started:** 2026-06-09
**Date Completed:** 2026-06-15
**Assignee:** Malachi
**Parent:** TI-602

---

## 1. Introduction
TI has run ~12 experiments measuring business impact (IVR/visit rate, CVR, CPA, revenue), but results
are scattered across ticket folders, Databricks notebooks, and ephemeral personal-account public gists
(`share_deck.sh` → a new githack URL per revision). There is no single, durable, bookmarkable place where
anyone at MNTN can see "what has TI done for the business."

TI-1003 builds that: an internal, manifest-driven static site — a master scorecard index + one scannable
page per experiment (intention + big bold impact number + chart + Jira link).

## 2. The Problem
- No portfolio view of TI's measured impact for leadership / stakeholders.
- Per-experiment results live only with their owner; no self-serve.
- Current sharing (gist + githack) is ephemeral, public, and personal-account-scoped — wrong trust boundary
  for revenue $ and advertiser names.

## 3. Plan of Action
Full plan: `/Users/malachi/.claude/plans/so-i-have-ticket-witty-kay.md`.

**Decisions (with user):**
- Host: internal GitHub Pages (private SteelHouse org repo, Pages access-controlled to org members).
- Scope: phased — ship the static archive first; auto-refresh + portfolio-$ methodology = Phase 2.
- Impact framing (Phase 2): ranked per-experiment $ scorecard with confidence discounts + one caveated
  portfolio range (no false-precision grand total); de-dup overlapping prospecting populations.

**Architecture:** manifest-driven multi-page static site (Python + Jinja2 → static HTML), one YAML per
experiment, RevealJS deck CSS/palette reused, per-ticket chart PNGs reused. Mirrors the internal Data
Documentation App (manifest → index + detail pages).

**Site source repo:** built standalone at `/Users/malachi/Developer/work/mntn/ti-experiment-archive/`
(separate from this personal workspace, which git-ignores `*.html/*.png/*.json`). Becomes
`SteelHouse/ti-experiment-archive` once created.

### Phase 1 steps
1. ✅ Workspace ticket folder
2. ⬜ Create SteelHouse repo + internal Pages *(gated on confirmations below — only remaining blocker)*
3. ✅ Scaffold build.py + Jinja2 templates + CSS (+ deploy.yml)
4. ✅ Seed Fangorn (TI-961) + BUK (TI-804) manifests + portfolio.yaml
5. ✅ Author remaining 6 manifests (504, 748, 835, 884, 999, 542) — drafted from each summary.md + adversarially verified; chart PNGs + decks copied in
6. ✅ deploy.yml (internal Pages, Phase-2 cron stubbed)
7. ✅ Verify: all 8 build, every chart/deck ref resolves, screenshots reviewed
8. ⬜ Update Jira with bookmarkable URL *(after repo + Pages exist)*

## 4. Investigation & Findings
- Experiments to seed (8): TI-961/921 Fangorn rollout (live), TI-504 Fangorn RCT, TI-748 Media Plan,
  TI-804/813 BUK keyword value, TI-835/BER-2250 incrementality (live/two-story), TI-884 power analysis,
  TI-999 interest-segment sizing, TI-542 Max Reach.
- Reusable: `ti_813_presentation_deck.html` CSS palette; per-ticket `generate_charts.py` PNGs;
  `RolloutTierEvaluations.py` (Phase-2 analyzer); `data_documentation_app.md` (precedent).

## 5. Solution
**Phase 1 built and verified locally** — the full static archive is done; only repo creation + Pages
turn-on remain (gated below). Site source: `/Users/malachi/Developer/work/mntn/ti-experiment-archive/`
(its own git repo, 2 commits). `python3 build.py` → `dist/` renders 8 experiments.

**8 experiments seeded** (scorecard order by sort_rank):
| id | Title | Headline | Status |
|----|-------|----------|--------|
| ti-961 | Fangorn Rollout | +27% IVR (DiD) | live |
| ti-835 | CTV Incrementality Holdout | ~0% net-new · 2–8x attributed | ongoing (two-story) |
| ti-804 | BUK Keyword Value | 184x visit-rate lift | concluded |
| ti-884 | Incrementality Power Limits | $200k/mo spend threshold | concluded |
| ti-748 | Media Plan Config | +10–17% (new cfg) · −26 to −31% (old) | concluded |
| ti-999 | 3P Segment Sizing | $103M/yr on 3P; $55M stale | analysis |
| ti-504 | Fangorn Intent RCT | +41% IVR (2 of 5 advertisers) | concluded |
| ti-542 | Max Reach Lift | Mixed (heterogeneous by segment) | concluded |

All numbers drafted from each ticket's `summary.md` and adversarially re-verified against the source
(workflow `ti1003-author-manifests`; verify stage caught + fixed real overstatements, e.g. TI-748 config
attribution, TI-748 publisher count, TI-504 "2 of 5 significant" not generalized).

**Architecture delivered:** manifest-driven (one YAML/experiment) → `build.py` (Python+Jinja2) → static
multi-page site. Reused the RevealJS deck palette (`assets/ti-archive.css`). Two-story experiments use
`headline_pair`; live ones get a "live data" note.

**Redesign (post-review feedback "overlapping graphs, takeaways not landing"):**
- Replaced the reused technical matplotlib PNGs (overlapping CI bands, dense grids) with **built-in inline
  bar/diverging charts** — `build.py` computes the geometry; one clean chart per experiment that directly
  carries the headline; dropped where no clean comparison fits. No more image clutter.
- Each page now shows **every KPI the experiment moved**, with the top 1–2 highlighted (per user: "show all
  impacted KPIs, highlight the top ones").
- Landing is **KPI-centric** ("What TI has moved"): experiments grouped by a canonical `metric` (ivr,
  incrementality, spend_3p, measurement) — KPI as the card title, each contributing experiment's movement
  below. Scales as we add experiments: a new ticket just declares `metric: <key>` to join a KPI group.
- Headline color encodes result type via `tone` (win=red, opportunity=blue, neutral=navy); per-experiment
  KPI cards are KPI-name-as-title with the movement below; scorecard rows realigned (badge centered).
- Fixed a real CSS bug: `.big-number` letter-spacing inherited as an absolute length into the small two-story
  pair labels and jammed the letters — reset to normal.
- Adding an experiment = drop one YAML (`tone`, `metric`, inline `chart:`, `kpis:`), rebuild. No image assets.

**Data-integrity catch:** the chart/KPI extraction agents **fabricated** per-cluster numbers for TI-542
(Max Reach) — its `summary.md` has no numbers, the notebook outputs were stripped, and the only artifact is a
**joke placeholder PDF** (`ti_542_mullet_performance_report.pdf` — literal mullet haircuts). Caught by grepping
the source, reverted TI-542 to an honest "Mixed / no aggregate distilled." The other 7 experiments' numbers
were spot-checked against their real summaries and all confirmed present.

## 6. Questions Answered
- **Q:** Host? **A:** Internal GitHub Pages (private SteelHouse repo, Pages access-controlled).
- **Q:** Scope? **A:** Phased — static archive first; auto-refresh + portfolio-$ = Phase 2.
- **Q:** Impact framing? **A:** Scorecard + caveated range (Phase 2); Phase 1 shows per-experiment headlines.

## 7. Data Documentation Updates
None (no schema/business-logic discoveries — this is a presentation/tooling ticket).

## 8. Open Items / Follow-ups
**MUST CONFIRM before deploy (only remaining blocker):**
1. Is SteelHouse on GitHub Enterprise Cloud with **Pages access-control** (private-repo Pages restricted to
   org members)? This is what makes "internal GitHub Pages" actually internal.
2. Can we create `SteelHouse/ti-experiment-archive` (or who creates it)? Owning team / CODEOWNERS?
3. Are revenue-$ figures + advertiser names OK behind org-SSO Pages, or need masking? (Legal/IT.)

**Content follow-ups:**
- **TI-542:** shown as "Mixed — heterogeneous by segment"; no aggregate exists (results in the gitignored
  `ti_542_mullet_performance_report.pdf`). Distill a per-cluster aggregate → real headline number later.
- **TI-961:** headline is a frozen 2026-06-03 snapshot. Phase 2 wires the weekly auto-refresh.

**Phase 2 (deferred):** weekly live-refresh GitHub Action (headless `RolloutTierEvaluations.py` → live
manifest); confidence-discounted $ scorecard + caveated portfolio range. GCP WIF/SA + GCS results path TBD.

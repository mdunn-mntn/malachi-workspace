# TI-1003: TI Experiment Archive

**Jira:** https://mntn.atlassian.net/browse/TI-1003
**Status:** In Progress (Phase 1 build)
**Date Started:** 2026-06-09
**Date Completed:** —
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
2. ⬜ Create SteelHouse repo + internal Pages *(gated on confirmations below)*
3. 🔄 Scaffold build.py + templates + CSS
4. 🔄 Seed Fangorn (TI-961) + BUK (TI-804) manifests
5. ⬜ Author remaining manifests (504, 748, 835, 884, 999, 542) + copy chart PNGs
6. ⬜ deploy.yml (internal Pages)
7. ⬜ Verify + access-control check
8. ⬜ Update Jira with bookmarkable URL

## 4. Investigation & Findings
- Experiments to seed (8): TI-961/921 Fangorn rollout (live), TI-504 Fangorn RCT, TI-748 Media Plan,
  TI-804/813 BUK keyword value, TI-835/BER-2250 incrementality (live/two-story), TI-884 power analysis,
  TI-999 interest-segment sizing, TI-542 Max Reach.
- Reusable: `ti_813_presentation_deck.html` CSS palette; per-ticket `generate_charts.py` PNGs;
  `RolloutTierEvaluations.py` (Phase-2 analyzer); `data_documentation_app.md` (precedent).

## 5. Solution
*(in progress)*

## 6. Questions Answered
*(in progress)*

## 7. Data Documentation Updates
*(pending)*

## 8. Open Items / Follow-ups — MUST CONFIRM before deploy
1. Is SteelHouse on GitHub Enterprise Cloud with **Pages access-control** (private-repo Pages restricted to
   org members)? This is what makes "internal GitHub Pages" actually internal.
2. Can we create `SteelHouse/ti-experiment-archive` (or who creates it)? Owning team / CODEOWNERS?
3. Are revenue-$ figures + advertiser names OK behind org-SSO Pages, or need masking? (Legal/IT.)
4. Phase 2 only: GCP service account / WIF provider for BQ read; exact GCS/BQ path Fangorn results land in.

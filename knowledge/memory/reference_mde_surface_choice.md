---
name: reference_mde_surface_choice
description: Two MDE surfaces exist and answer different questions — the in-product Testing tab forecasts an already-live campaign group at its fixed budget, the standalone calculator (gist plus Mode report) is the only one that does what-if budgets
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [MDE calculator, in-product Testing tab, ExperimentBuilder, ForecastSidebar, HoldoutSection, forecasted_mde_percent, computeMde, what-if budget, lift test budget recommendation, ghost bid incrementality test, standalone gist prefill, Mode MDE calculator report, Edgar von Trotha, Nick Scialli, AUDI-1213 closed, AUDI-1324 duplicate, TI-1019, no variance reduction toggle]
domain: [incrementality, experimentation, repos]
lifecycle: active
last_verified: 2026-09-03
---
**Pick the surface by whether the budget is an input or an output.** Verified live 2026-09-03 against `SteelHouse/gary-ql@cbae0e94` and `SteelHouse/premier-ui@aaf65d59`.

- **In-product Testing tab** (`premier-ui src/app/scenes/Testing/ExperimentBuilder/useMdeForecast.ts` → ForecastSidebar + HoldoutSection; `gary-ql src/gql/types/IncrementalityExperiment/resolvers.ts` → `forecasted_mde_percent`): forecasts the MDE of an **already-live campaign group**. It forces you to select one and fixes the budget to that selection. Use it for "what can this live campaign detect?"
- **Standalone calculator** (`ti_xxx_mde_calculator_prefill.html`, TI-1019, refreshed and extended under AUDI-1213 2026-09-03): the ONLY surface that does **what-if budget exploration**. Use it for "what budget would this test need?" and for any advertiser not currently live, delivering or lapsed. It now ships in two places: the githack gist and a **Mode report** in the "Audience Intelligence" space (port mechanics: [[reference_mode_dashboard_porting]]). **The Mode report IS the calculator, not a second deliverable.** Treating it as one is why AUDI-1324 was filed to split it out and then closed as a duplicate on 2026-09-03.

**Why it matters:** the tab's fixed-budget constraint is exactly why the standalone still exists. Edgar von Trotha is fielding a rising number of customers asking for lift-test budget recommendations, which the tab structurally cannot answer. This BREAKS the 2026-08-25 premise (recorded on AUDI-1213 and in [[project_backlog_gate_pings]]) that the UI owns delivering advertisers and the standalone only needs the lapsed cohort — the delivering half came back into scope and shipped 2026-09-03: 1,859 delivering advertisers in the morning build, then 4,387 (1,857 delivering + 2,530 lapsed) once the lapsed cohort landed the same day on a 365-day lookback. AUDI-1213 CLOSED Done 2026-09-03, retitled "One weekly-refreshed Mode MDE calculator for every advertiser".

**They are separate implementations, not one shared library.** Same Lewis-Rao two-proportion formula family, different code and different data: each tab call site imports its own copy of `computeMde`, alpha/power are hardcoded there with no user control, and the tab's baseline rates come from ChAPI per-IP-user rates while the standalone embeds a snapshot array. A number from one will not always match the other, and the tab still carries the arm-split defect ([[reference_mde_arm_split]], AUDI-1323, open and Nick Scialli's as of 2026-09-03). Nick Scialli (eng) owns the tab; the standalone is ours. **Neither surface exposes a variance-reduction toggle:** the tab hardcodes `DEFAULT_VAR_REDUCTION = 1` (raw), and the toggle, its post-stack hero number and its chart series were REMOVED from the standalone and the Mode build on 2026-09-03 after the 0.595 stacked-variance constant was refuted (measurement: `knowledge/experimentation.md`). Every MDE either surface reports is a raw-variance MDE.

Detail and defect list: [[project_incrementality_experiment]], [[reference_test_budget_from_rates]], `tickets/audi_1213_mde_calculator_refresh/summary.md`.

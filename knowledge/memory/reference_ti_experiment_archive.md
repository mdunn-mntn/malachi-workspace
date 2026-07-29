---
name: reference_ti_experiment_archive
description: TI experiment archive — manifest-driven internal site cataloging every TI experiment by KPI; add each new experiment as one YAML.
metadata: 
  node_type: memory
  type: reference
  originSessionId: 18b76576-d68f-42c8-949a-d2e5e56d1262
doc_type: memory
keywords: [ti experiment archive, manifest yaml, experiment site, TI-1003, TI-1033, TI-542 max reach, KPI catalog, IVR CVR]
domain: [experimentation, repos, project]
lifecycle: active
last_verified: 2026-06-17
---
The **TI experiment archive** is an internal, bookmarkable static site cataloging every Targeting Infra experiment: a "what TI has moved" landing grouped by KPI (IVR, CVR, incrementality, 3P-spend, measurement) + one page per experiment (intention → big bold movement → every KPI moved → method → inline bar chart).

- **Source repo:** `~/Developer/work/mntn/ti-experiment-archive/` (its own git repo, NOT the workspace — the workspace gitignores html/png). Build: `python build.py` → `dist/`. Python + Jinja2, no framework.
- **Built in TI-1003** (closed/done). **Hosting + deploy + polish = TI-1033** (host on SteelHouse GHE internal Pages or GCS+IAP; not yet live as of 2026-06-17).
- **Add a new experiment = drop one `manifests/<id>.yaml`** — fields: `id`, `title`, `subtitle`, `tickets`, `status` (live/ongoing/concluded/analysis), `tone` (win=red / opportunity=blue / neutral=navy), `metric` (KPI group key; add `kpi_groups: [{key,value,num_class,note}]` if it moved >1 KPI), inline `chart` (kind bars|diverging, no image files), `kpis` (every KPI moved, top 1–2 `highlight: true`). KPI groups defined in `manifests/portfolio.yaml`. No template edits. **Do this whenever an experiment wraps.**
- **Framing rules:** IVR = proven headline KPI; CVR = second target, noisier. Color encodes the result (red only for genuine wins), not decoration. Drop corny taglines — plain "we improve targeting → do IVR/CVR go up?".

**Trap — TI-542 (Max Reach):** results are NOT recoverable. Notebook outputs stripped; the only artifact `ti_542_mullet_performance_report.pdf` is a **joke placeholder** (mullet haircuts, "party in the back, data in the stack") with zero Max Reach data. Don't cite Max Reach numbers or extract "results" from that PDF — an agent fabricated plausible per-cluster numbers from it during TI-1003. Archive shows it honestly as "Mixed — no aggregate distilled."

See [[reference_causal_impact_pattern]] and `knowledge/experimentation.md` §"Experiment results archive".

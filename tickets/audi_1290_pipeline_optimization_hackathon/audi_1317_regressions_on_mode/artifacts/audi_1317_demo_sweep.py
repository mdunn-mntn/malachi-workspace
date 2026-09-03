"""Drive four real sweeps over two real event-log dirs to show a regression appear and resolve.

    python3 artifacts/audi_1317_demo_sweep.py <worktree> <logs-dir> <corpus.jsonl> <outdir>

The Spark path is real: crawl, stage metrics, guard, ledger, digest. The BigQuery, pod,
Databricks, billing and Slack surfaces are stubbed so the demo touches no prod credential.
The seed is the baseline window scaled to SCALE, which is the same arithmetic as doubling the
run under test and leaves the run itself untouched.
"""

import json
import os
import sys

SCALE = 0.4
SWEEPS = ["2026-09-02", "2026-09-03", "2026-09-04", "2026-09-05"]


def load(path):
    return [json.loads(line) for line in open(path) if line.strip()]


def write_window(rows, parsed, path, scale):
    """The metrics file the sweep restores: prior runs only, disk spill scaled by `scale`."""
    out = []
    for r in rows:
        if r["app_id"] in parsed:
            continue
        out.append({**r, "disk_spill": int(r["disk_spill"] * scale)})
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        for r in out:
            fh.write(json.dumps(r) + "\n")
    return len(out)


def main():
    worktree, logs, corpus, outdir = sys.argv[1:5]
    sys.path.insert(0, worktree)
    from include.spark_optimizer import billing as billing_mod
    from include.spark_optimizer import ledger as ledger_mod
    from include.spark_optimizer import notify as notify_mod
    from include.spark_optimizer import pod_profile as pod_mod
    from include.spark_optimizer import sweep as sweep_mod
    from include.spark_optimizer.bq_profile import profile as _bq

    sweep_mod.bq_mod.profile = lambda *a, **k: []
    pod_mod.profile = lambda *a, **k: []
    billing_mod.surface_rates = lambda: {"spark": (None, "hermetic"), "bq": (None, "hermetic")}
    sweep_mod.billing_mod.surface_rates = billing_mod.surface_rates
    notify_mod.deliver_thread = lambda *a, **k: {}
    sweep_mod.notify_mod.deliver_thread = notify_mod.deliver_thread
    assert _bq

    rows = load(corpus)
    parsed = {"eventlog_v2_batch-42e88a22-6f13-4282-9910-34d2e097ea4e",
              "eventlog_v2_batch-8f1a450a-2ebc-44de-a375-ef5408d27b2f"}
    ledger_path = os.path.join(outdir, "optimization_ledger.jsonl")
    metrics = os.path.join(outdir, "optimizer_stage_metrics.jsonl")

    for i, date in enumerate(SWEEPS):
        scale = SCALE if i == 0 else 1.0
        n = write_window(rows, parsed, metrics, scale)
        out = sweep_mod.run([logs], date, outdir=outdir, ledger_path=ledger_path)
        firing = [e for e in ledger_mod.read(ledger_path)
                  if e["date"] == date and e["key"].startswith("regression_")]
        print(f"\n=== sweep {date} (window {n} rows, disk spill x{scale}) ===")
        print(f"scanned {out['scanned']}  findings {out['findings']}  "
              f"regressions {out['regressions']}  ledger rows {out['ledger_entries']}")
        for e in sorted(firing, key=lambda e: e["key"]):
            print(f"  {e['state']:<9} streak {e['streak']}  {e['dag_id']}  {e['key']}")
            print(f"            {e['title']}")
        for line in out["slack"].splitlines():
            if "Regressed" in line or line.startswith("- ") and "GiB" in line:
                print(f"  digest| {line}")


if __name__ == "__main__":
    main()

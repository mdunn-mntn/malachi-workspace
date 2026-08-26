"""Turn crawl.jsonl + coverage.pkl into the AUDI-1194 validation tables."""
import collections
import json
import pickle
import sys

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
from airflow_optimizer import coverage as cov_mod  # noqa: E402
from airflow_optimizer import ledger as ledger_mod  # noqa: E402

SW = "/private/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/1e1ddf5c-5fa7-4d0c-b846-f111b4c43a5b/scratchpad/fullsweep"
ALL_KEYS = ["missing_statistics", "broadcast_candidate", "shuffle_partition_sizing",
            "window_full_sort", "repeated_scan", "skew", "straggler", "disk_spill",
            "shuffle_fetch_wait", "gc_pressure", "spot_preemption_cost",
            "idle_reserved_executors", "cache_ineffective", "shuffle_fetch_instability"]


class _R:
    def __init__(self, d):
        self.app_name = d.get("app_name")
        self.source = d["source"]


def main():
    rows = [json.loads(line) for line in open(f"{SW}/crawl.jsonl")]
    cov = pickle.load(open(f"{SW}/coverage.pkl", "rb"))
    cov._owner_index = None
    known = cov.dag_ids_including_paused
    ok = [r for r in rows if not r.get("error")]
    bad = [r for r in rows if r.get("error")]

    per = collections.defaultdict(lambda: {
        "runs": 0, "exec_h": 0.0, "worst_h": 0.0, "findings": 0, "high": 0,
        "keys": collections.Counter(), "example": None, "app_name": "", "errors": 0})
    for r in rows:
        job = ledger_mod._dag_id(_R(r), known)
        t = per[job]
        t["runs"] += 1
        t["app_name"] = t["app_name"] or (r.get("app_name") or "")
        if r.get("error"):
            t["errors"] += 1
            continue
        t["exec_h"] += r["exec_h"]
        t["worst_h"] = max(t["worst_h"], r["exec_h"])
        fs = r.get("findings", [])
        t["findings"] += len(fs)
        t["high"] += sum(1 for f in fs if f["impact"] == "high")
        for f in fs:
            t["keys"][f["key"]] += 1
        if fs and (t["example"] is None or r["exec_h"] > t["example"][0]):
            t["example"] = (r["exec_h"], r["source"], fs[0])

    task_rows = []
    for job, t in per.items():
        ex = t["example"]
        task_rows.append({
            "job": job,
            "dag_id": cov.resolve(t["app_name"] or job),
            "runs": t["runs"],
            "unreadable": t["errors"],
            "exec_h_total": round(t["exec_h"], 1),
            "exec_h_worst": round(t["worst_h"], 1),
            "findings": t["findings"],
            "high": t["high"],
            "detectors": ", ".join(f"{k} x{n}" for k, n in t["keys"].most_common()),
            "top_finding": ex[2]["title"] if ex else "",
            "top_fix": ex[2]["fix"] if ex else "",
        })
    task_rows.sort(key=lambda r: r["exec_h_total"], reverse=True)

    names = {r.get("app_name") or r["source"] for r in rows}
    unresolved = cov.unresolved(names)
    ur = collections.Counter()
    for _, why in unresolved:
        if "nothing to match" in why:
            ur["Spark set no app name"] += 1
        elif "named by" in why:
            ur["two or more DAGs claim the name"] += 1
        else:
            ur["no DAG defines a task with this name"] += 1

    det = collections.Counter()
    example = {}
    for r in ok:
        for f in r.get("findings", []):
            det[f["key"]] += 1
            if f["key"] not in example:
                example[f["key"]] = dict(f, source=r["source"],
                                         app_name=r.get("app_name") or "", exec_h=r["exec_h"])

    out = {
        "logs_total": len(rows), "logs_ok": len(ok), "logs_error": len(bad),
        "errors": collections.Counter(r["error"].split(":")[0] for r in bad).most_common(),
        "distinct_jobs": len(per),
        "resolved_jobs": sum(1 for r in task_rows if r["dag_id"]),
        "dags_covered": len({r["dag_id"] for r in task_rows if r["dag_id"]}),
        "findings_total": sum(len(r.get("findings", [])) for r in ok),
        "exec_h_total": round(sum(r["exec_h"] for r in ok), 1),
        "task_rows": task_rows,
        "unresolved_counts": dict(ur),
        "unresolved_detail": unresolved,
        "detector_counts": {k: det.get(k, 0) for k in ALL_KEYS},
        "detector_examples": example,
        "logs_with_plan": sum(1 for r in ok if r.get("n_plan", 0)),
        "dags_total": len(cov.dag_ids_including_paused),
        "dags_active": len(cov.dags),
        "dags_profilable": len(cov.profilable),
    }
    json.dump(out, open(f"{SW}/analysis.json", "w"), indent=1, default=str)
    print(f"logs {out['logs_ok']}/{out['logs_total']} parsed, {out['logs_error']} unreadable")
    print(f"jobs {out['distinct_jobs']}, resolved to a DAG {out['resolved_jobs']}, "
          f"distinct DAGs {out['dags_covered']} of {out['dags_profilable']} with a Spark task")
    print(f"findings {out['findings_total']}, executor-hours {out['exec_h_total']:,.0f}")
    print("fired:", {k: v for k, v in out["detector_counts"].items() if v})
    print("SILENT:", [k for k, v in out["detector_counts"].items() if not v])
    print("unresolved:", out["unresolved_counts"])


if __name__ == "__main__":
    main()

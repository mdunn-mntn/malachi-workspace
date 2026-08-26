"""Are the silent detectors correctly quiet, or is their input never populated?"""
import glob, json, os, random, sys
from concurrent.futures import ProcessPoolExecutor
sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
SW = "/private/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/1e1ddf5c-5fa7-4d0c-b846-f111b4c43a5b/scratchpad/fullsweep"


def probe(log):
    from airflow_optimizer.eventlog import parse_eventlog
    from airflow_optimizer.optimizations import parse_plan_text
    try:
        run = parse_eventlog(log)
    except Exception:
        return None
    stages = run.stages or []
    plans = [s.plan_text for s in (run.sql or []) if getattr(s, "plan_text", "")]
    scans = stats = 0
    for p in plans:
        parsed = parse_plan_text(p)
        scans += len(parsed.scans)
        stats += sum(1 for sc in parsed.scans if getattr(sc, "size_bytes", 0))
    return {
        "gc_ms": sum(s.gc_time_ms for s in stages),
        "run_ms": sum(s.run_time_ms for s in stages),
        "fetch_failed": sum(s.fetch_failed for s in stages),
        "rdd_evictions": getattr(run, "rdd_evictions", 0),
        "removed_reasons": sorted({(e.removed_reason or "")[:60] for e in run.executors
                                   if e.removed_reason}),
        "failed_tasks": sum(e.failed_tasks for e in run.executors),
        "n_plans": len(plans),
        "plan_scans": scans,
        "plan_scans_with_size": stats,
        "plan_chars": sum(len(p) for p in plans),
    }


if __name__ == "__main__":
    logs = sorted(glob.glob(f"{SW}/logs/*.zstd"))
    random.seed(7)
    sample = random.sample(logs, min(300, len(logs)))
    with ProcessPoolExecutor(max_workers=8) as ex:
        res = [r for r in ex.map(probe, sample) if r]
    n = len(res)
    print(f"{n} logs probed\n")
    print(f"stages carry gc_time_ms > 0 : {sum(1 for r in res if r['gc_ms'] > 0)}/{n}")
    ratios = [r['gc_ms'] / r['run_ms'] for r in res if r['run_ms']]
    ratios.sort()
    if ratios:
        print(f"  GC share of task time: max {ratios[-1]:.3%}, p99 {ratios[int(.99*len(ratios))-1]:.3%}"
              f"  (detector fires at >= 10%)")
    print(f"fetch_failed > 0           : {sum(1 for r in res if r['fetch_failed'])}/{n}")
    print(f"rdd_evictions > 0          : {sum(1 for r in res if r['rdd_evictions'])}/{n}")
    print(f"any failed tasks           : {sum(1 for r in res if r['failed_tasks'])}/{n}")
    reasons = {}
    for r in res:
        for x in r["removed_reasons"]:
            reasons[x] = reasons.get(x, 0) + 1
    print("executor removed_reason values seen:")
    for k, v in sorted(reasons.items(), key=lambda kv: -kv[1])[:8]:
        print(f"   {v:>4}  {k}")
    print()
    print(f"logs with a SQL plan       : {sum(1 for r in res if r['n_plans'])}/{n}")
    print(f"plan leaf scans parsed     : {sum(r['plan_scans'] for r in res)}")
    print(f"  of those, carrying a size estimate: {sum(r['plan_scans_with_size'] for r in res)}")
    print(f"total plan text            : {sum(r['plan_chars'] for r in res):,} chars")

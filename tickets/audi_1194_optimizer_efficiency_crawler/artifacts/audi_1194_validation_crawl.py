"""Crawl the 30-day corpus in parallel and emit one JSON row per event log."""
import json, os, sys, traceback
from concurrent.futures import ProcessPoolExecutor, as_completed

sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")
SW = "/private/tmp/claude-501/-Users-malachi-Developer-work-mntn-workspace/1e1ddf5c-5fa7-4d0c-b846-f111b4c43a5b/scratchpad/fullsweep"


def one(log):
    from airflow_optimizer.crawl import executor_hours
    from airflow_optimizer.optimize import analyze_eventlog
    base = os.path.basename(log.rstrip("/"))
    try:
        run, findings = analyze_eventlog(log)
        empty = not getattr(run, "jobs", None) and not getattr(run, "stages", None)
        if empty and not getattr(run, "app_end_ts", None):
            return {"source": base, "error": "truncated: no jobs, no stages, no ApplicationEnd"}
        return {
            "source": base,
            "app_name": run.app_name,
            "app_id": getattr(run, "app_id", ""),
            "exec_h": executor_hours(run),
            "n_sql": len(getattr(run, "sql", []) or []),
            "n_plan": sum(1 for s in (getattr(run, "sql", []) or []) if getattr(s, "plan_text", "")),
            "findings": [{"key": f.key, "impact": f.impact, "title": f.title,
                          "evidence": f.evidence, "fix": f.fix, "rec_type": f.rec_type}
                         for f in findings],
        }
    except Exception as e:
        return {"source": base, "error": f"{type(e).__name__}: {str(e)[:160]}"}


def main():
    from airflow_optimizer.crawl import _event_logs
    root = sys.argv[1] if len(sys.argv) > 1 else f"{SW}/logs"
    out_path = sys.argv[2] if len(sys.argv) > 2 else f"{SW}/crawl.jsonl"
    logs = _event_logs([root])
    lim = int(os.environ.get("LIMIT", "0"))
    if lim:
        logs = logs[:lim]
    print(f"{len(logs)} event logs to crawl", flush=True)
    out = open(out_path, "w")
    done = 0
    with ProcessPoolExecutor(max_workers=int(os.environ.get("WORKERS", "8"))) as ex:
        futs = {ex.submit(one, lg): lg for lg in logs}
        for fut in as_completed(futs):
            try:
                row = fut.result()
            except Exception as e:
                row = {"source": os.path.basename(futs[fut]), "error": f"worker died: {e}"}
            out.write(json.dumps(row) + "\n")
            done += 1
            if done % 100 == 0:
                out.flush()
                print(f"{done}/{len(logs)}", flush=True)
    out.close()
    print(f"CRAWL_COMPLETE {done}", flush=True)


if __name__ == "__main__":
    main()

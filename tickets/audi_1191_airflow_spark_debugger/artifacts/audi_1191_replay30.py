"""Replay every distinct failure in the corpus through the full debugger chain.

The diagnosis dict is kept alongside the rendered output so a renderer change can be checked
without a second live pass over GCP.
"""
import glob
import json
import os
import sys
import time
import traceback

os.environ.setdefault(
    "AIRFLOW_API_BASE",
    "https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/api/v2",
)
sys.path.insert(0, "/Users/malachi/Developer/work/mntn/workspace")

from airflow_debugger import slack_block  # noqa: E402
from airflow_debugger.orchestrate import investigate  # noqa: E402
from airflow_debugger.parse import diagnose, parse_log_file  # noqa: E402
from airflow_debugger.report import _repo_paths  # noqa: E402
from airflow_debugger.signatures import classify  # noqa: E402

OUT = sys.argv[1]
paths = sorted(glob.glob("on-call/airflow_logs/*/*.log"))
paths = [p for p in paths if p.endswith(("__failed.log", "__upstream_failed.log"))]
print(f"failed-state logs: {len(paths)}", flush=True)

# Group offline first: the full chain hits live GCP, so run it once per DISTINCT failure.
groups: dict[tuple, list[str]] = {}
for p in paths:
    try:
        parsed = parse_log_file(p)
        d = diagnose(parsed)
    except Exception:
        groups.setdefault(("PARSE_ERROR", p, ""), []).append(p)
        continue
    ident = d.get("identity", {})
    sig = d.get("root_signature") or d.get("airflow_signature") or {}
    if not sig:
        text = " ".join(
            str(x) for x in (d.get("root_error"), (d.get("spark") or {}).get("error_text")) if x
        )
        m = classify(text) if text else None
        sig = {"key": m.key} if m else {}
    key = (ident.get("dag_id") or "?", ident.get("task_id") or "?", sig.get("key") or "UNCLASSIFIED")
    groups.setdefault(key, []).append(p)

print(f"distinct failures: {len(groups)}", flush=True)
repo = _repo_paths()
rows = []
for i, (key, members) in enumerate(sorted(groups.items()), 1):
    # Prefer a member carrying error text: the newest log is often an empty stub that hides the gap.
    def _texted(path: str) -> int:
        try:
            dd = diagnose(parse_log_file(path))
        except Exception:
            return 0
        return len((dd.get("root_error") or (dd.get("spark") or {}).get("error_text") or "").strip())

    rep = max(sorted(members), key=_texted)
    t0 = time.time()
    try:
        res = investigate(rep, use_llm=False, profile_perf=False)
        diag = res["diagnosis"]
        block = slack_block.render(diag, repo_paths=repo)
        rows.append(
            {
                "dag_id": key[0],
                "task_id": key[1],
                "signature": key[2],
                "occurrences": len(members),
                "days": sorted({m.split("/")[2] for m in members}),
                "representative": rep,
                "confidence": res["confidence"],
                "report": res["report"],
                "slack": block,
                "diagnosis": diag,
                "similar": [m.get("inc") for m in res.get("similar_incidents", [])[:3]],
                "elapsed": round(time.time() - t0, 1),
            }
        )
    except Exception as e:
        rows.append(
            {
                "dag_id": key[0],
                "task_id": key[1],
                "signature": key[2],
                "occurrences": len(members),
                "days": sorted({m.split("/")[2] for m in members}),
                "representative": rep,
                "confidence": "ERROR",
                "report": f"{type(e).__name__}: {e}",
                "slack": "",
                "similar": [],
                "traceback": traceback.format_exc()[-1500:],
                "elapsed": round(time.time() - t0, 1),
            }
        )
    print(f"[{i}/{len(groups)}] {key[0]}/{key[1]} {key[2]} -> {rows[-1]['confidence']} ({rows[-1]['elapsed']}s)", flush=True)

with open(OUT, "w") as f:
    json.dump(rows, f, indent=2, default=str)
print("WROTE", OUT, flush=True)

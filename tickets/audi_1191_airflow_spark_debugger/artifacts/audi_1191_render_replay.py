"""Render a replay JSON into the per-failure markdown record."""
import json
import sys
from collections import Counter, defaultdict

src, out = sys.argv[1], sys.argv[2]
with open(src) as fh:
    rows = json.load(fh)

WEATHER = {"cluster_create_stockout", "quota_exhaustion", "spot_preemption", "ttl_exceeded",
           "task_externally_terminated", "batch_cancelled", "pod_evicted_404"}

hi = [r for r in rows if r["confidence"] == "high"]
lo = [r for r in rows if r["confidence"] not in ("high", "ERROR")]
err = [r for r in rows if r["confidence"] == "ERROR"]
logs = sum(r["occurrences"] for r in rows)
days = sorted({d for r in rows for d in r["days"]})

L = []
w = L.append
w("# AUDI-1191 — every failed task in the window, and what the debugger says about it\n")
ondisk = len([d for d in __import__("os").listdir("on-call/airflow_logs") if d.startswith("2026")])
w(f"Generated 2026-08-26 from `on-call/airflow_logs/` — {ondisk} days pulled, failures on {len(days)} of them, {days[0]} to {days[-1]}.")
w("Harness: `artifacts/audi_1191_replay30.py`. Every distinct failure ran the full chain "
  "(`orchestrate.investigate`) — the same code path the prod DAG runs — and was rendered through "
  "`slack_block.render`, so each block below is what Slack would carry.\n")
w(f"**{logs} failed-state logs collapse to {len(rows)} distinct failures**, keyed by "
  "`(dag_id, task_id, signature)`.\n")
w("| | Distinct | Logs |")
w("|---|---:|---:|")
w(f"| Root-caused, high confidence | {len(hi)} | {sum(r['occurrences'] for r in hi)} |")
w(f"| Named condition, low confidence | {len(lo)} | {sum(r['occurrences'] for r in lo)} |")
w(f"| Chain errors / crashes | {len(err)} | {sum(r['occurrences'] for r in err)} |")
w("")
if not err:
    w(f"**Nothing crashed.** All {len(rows)} ran parse, route, engine RCA, signature, incident match "
      "and render without an exception.\n")

w("---\n\n## 1. Signatures that fired\n")
sig = Counter()
sigl = Counter()
for r in hi:
    sig[r["signature"]] += 1
    sigl[r["signature"]] += r["occurrences"]
w("| Signature | Distinct | Logs | Class |")
w("|---|---:|---:|---|")
for k, n in sorted(sig.items(), key=lambda kv: (-sigl[kv[0]], kv[0])):
    w(f"| `{k}` | {n} | {sigl[k]} | {'weather' if k in WEATHER else 'actionable'} |")
w("")

w("---\n\n## 2. Low-confidence results, by the condition each one names\n")
w("Every one of these carries a stated reason there is no root cause, not a bare `unclassified`.\n")
w("| DAG / task | Logs | What the debugger says |")
w("|---|---:|---|")
for r in sorted(lo, key=lambda r: -r["occurrences"]):
    first = next((ln.strip() for ln in r["report"].splitlines()[1:]
                  if ln.strip() and not ln.strip().startswith("http")), "").strip()
    w(f"| `{r['dag_id']}/{r['task_id']}` | {r['occurrences']} | {first[:200]} |")
w("")

w("---\n\n## 3. What to fix, ranked by how much of a DAG's noise is actionable\n")
w("A DAG that fails 25 times on a GCP stockout needs capacity work, not debugging. `days` is how "
  "many distinct days it failed on: a high count over few days is one bad episode, a low count "
  "across many days is a persistent defect.\n")
per = defaultdict(lambda: [0, 0, 0, 0, set()])
for r in rows:
    o, k = r["occurrences"], r["dag_id"]
    per[k][0] += o
    per[k][4].update(r["days"])
    if r["confidence"] != "high":
        per[k][3] += o
    elif r["signature"] in WEATHER:
        per[k][2] += o
    else:
        per[k][1] += o
w("| Rank | DAG | Logs | Days | Actionable | Weather | No cause in log |")
w("|---:|---|---:|---:|---:|---:|---:|")
for i, (k, v) in enumerate(sorted(per.items(), key=lambda kv: (-kv[1][1], -kv[1][0])), 1):
    w(f"| {i} | `{k}` | {v[0]} | {len(v[4])} | **{v[1]}** | {v[2]} | {v[3]} |")
act = sum(v[1] for v in per.values())
wea = sum(v[2] for v in per.values())
non = sum(v[3] for v in per.values())
w("")
w("| | Logs | Share |")
w("|---|---:|---:|")
w(f"| Actionable, someone can fix this | {act} | {act / logs:.0%} |")
w(f"| Weather: capacity, quota, preemption | {wea} | {wea / logs:.0%} |")
w(f"| No cause in the log, next hop named | {non} | {non / logs:.0%} |")
w("")
w("**Most on-call pages are weather or a pointer elsewhere.** That is the argument for AUDI-1217: "
  "quota and stockout work removes more alert volume than any amount of DAG debugging.\n")

w("---\n\n## 4. Every distinct failure, with its output\n")
w("Ordered by how many logs it accounts for.\n")
for r in sorted(rows, key=lambda r: (-r["occurrences"], r["dag_id"], r["task_id"])):
    w(f"### `{r['dag_id']}` / `{r['task_id']}` — {r['signature']}\n")
    w(f"**{r['occurrences']} log(s)** on {', '.join(r['days'])} · confidence **{r['confidence']}** · "
      f"representative `{r['representative']}`\n")
    if r.get("similar"):
        w(f"Similar incidents: {', '.join(x for x in r['similar'] if x)}\n")
    w("```")
    w(r["report"].strip())
    w("```\n")
    if r.get("slack"):
        w("Slack block:\n")
        w("```")
        w(r["slack"].strip())
        w("```\n")

with open(out, "w") as fh:
    fh.write("\n".join(L) + "\n")
print("WROTE", out, len(rows), "failures")

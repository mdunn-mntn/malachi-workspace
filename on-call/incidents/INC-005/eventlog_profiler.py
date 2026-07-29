#!/usr/bin/env python3
"""Profile a Spark event log: per-stage timing, shuffle, GC, spill, skew, FetchFailed, executor churn."""
import json, sys, collections, statistics

PATH = sys.argv[1]

def ms(x):  # to ms
    return (x or 0) / 1e6

app_start = None
last_ts = 0
# per stage (keyed by stageId; aggregate across attempts but track attempts)
stages = collections.defaultdict(lambda: {
    "name": "", "attempts": set(), "numTasks": 0,
    "tsucc": 0, "tfail": 0, "tfetchfail": 0,
    "submit_min": None, "complete_max": None,
    "runTime": 0.0, "cpuTime": 0.0, "gcTime": 0.0, "fetchWait": 0.0,
    "shReadBytes": 0, "shWriteBytes": 0, "shReadRecs": 0,
    "inputBytes": 0, "memSpill": 0, "diskSpill": 0,
    "peakExecMem": 0, "maxTaskRun": 0.0, "durs": [],
    "failReason": None,
})
sql_exec = {}           # executionId -> {desc, time, plan}
jobs = {}               # jobId -> {stages, submit, complete, result}
fetchfail = collections.Counter()     # (stageId, shuffleId) -> count
fetchfail_times = []
exec_added = 0; exec_removed = 0
exec_remove_reasons = collections.Counter()
exec_live = 0; exec_max = 0
stage_active_last = {}  # stageId -> last task finish time (to find where it ended)

with open(PATH) as f:
    for line in f:
        try:
            e = json.loads(line)
        except Exception:
            continue
        ev = e.get("Event", "")
        if ev == "SparkListenerApplicationStart":
            app_start = e.get("Timestamp")
        elif ev == "SparkListenerExecutorAdded":
            exec_added += 1; exec_live += 1; exec_max = max(exec_max, exec_live)
            last_ts = max(last_ts, e.get("Timestamp", 0))
        elif ev == "SparkListenerExecutorRemoved":
            exec_removed += 1; exec_live -= 1
            exec_remove_reasons[(e.get("Removed Reason") or "")[:60]] += 1
            last_ts = max(last_ts, e.get("Timestamp", 0))
        elif ev == "SparkListenerSQLExecutionStart":
            sql_exec[e.get("executionId")] = {
                "desc": e.get("description", ""),
                "time": e.get("time"),
                "plan": (e.get("physicalPlanDescription") or "")[:4000],
            }
        elif ev == "SparkListenerJobStart":
            jobs[e.get("Job ID")] = {"stages": e.get("Stage IDs", []),
                                     "submit": e.get("Submission Time"),
                                     "complete": None, "result": None}
        elif ev == "SparkListenerJobEnd":
            j = jobs.get(e.get("Job ID"))
            if j:
                j["complete"] = e.get("Completion Time")
                j["result"] = (e.get("Job Result") or {}).get("Result")
        elif ev == "SparkListenerStageSubmitted":
            si = e.get("Stage Info", {})
            sid = si.get("Stage ID"); st = stages[sid]
            st["name"] = si.get("Stage Name", st["name"])
            st["numTasks"] = max(st["numTasks"], si.get("Number of Tasks", 0))
            st["attempts"].add(si.get("Stage Attempt ID", 0))
            sub = si.get("Submission Time")
            if sub: st["submit_min"] = sub if st["submit_min"] is None else min(st["submit_min"], sub)
        elif ev == "SparkListenerStageCompleted":
            si = e.get("Stage Info", {})
            sid = si.get("Stage ID"); st = stages[sid]
            st["name"] = si.get("Stage Name", st["name"])
            st["numTasks"] = max(st["numTasks"], si.get("Number of Tasks", 0))
            st["attempts"].add(si.get("Stage Attempt ID", 0))
            comp = si.get("Completion Time")
            if comp:
                st["complete_max"] = comp if st["complete_max"] is None else max(st["complete_max"], comp)
                last_ts = max(last_ts, comp)
            if si.get("Failure Reason"):
                st["failReason"] = si["Failure Reason"][:200]
        elif ev == "SparkListenerTaskEnd":
            sid = e.get("Stage ID"); st = stages[sid]
            ti = e.get("Task Info", {}) or {}
            reason = e.get("Task End Reason", {}) or {}
            r = reason.get("Reason", "")
            fin = ti.get("Finish Time", 0); lau = ti.get("Launch Time", 0)
            if fin: last_ts = max(last_ts, fin); stage_active_last[sid] = max(stage_active_last.get(sid,0), fin)
            if r == "Success":
                st["tsucc"] += 1
            elif r == "FetchFailed":
                st["tfetchfail"] += 1
                fetchfail[(sid, reason.get("Shuffle ID"))] += 1
                if fin: fetchfail_times.append(fin)
            else:
                st["tfail"] += 1
            tm = e.get("Task Metrics") or {}
            if tm:
                st["runTime"] += tm.get("Executor Run Time", 0)
                st["cpuTime"] += ms(tm.get("Executor CPU Time", 0))
                st["gcTime"]  += tm.get("JVM GC Time", 0)
                st["memSpill"]  += tm.get("Memory Bytes Spilled", 0)
                st["diskSpill"] += tm.get("Disk Bytes Spilled", 0)
                st["peakExecMem"] = max(st["peakExecMem"], tm.get("Peak Execution Memory", 0))
                srm = tm.get("Shuffle Read Metrics") or {}
                st["fetchWait"]  += srm.get("Fetch Wait Time", 0)
                st["shReadBytes"] += (srm.get("Remote Bytes Read",0)+srm.get("Local Bytes Read",0))
                st["shReadRecs"]  += srm.get("Total Records Read",0)
                swm = tm.get("Shuffle Write Metrics") or {}
                st["shWriteBytes"] += swm.get("Shuffle Bytes Written",0)
                im = tm.get("Input Metrics") or {}
                st["inputBytes"] += im.get("Bytes Read",0)
                dur = (fin-lau) if (fin and lau) else tm.get("Executor Run Time",0)
                st["maxTaskRun"] = max(st["maxTaskRun"], dur)
                if len(st["durs"]) < 4000: st["durs"].append(dur)

def gb(b): return b/1e9
def hh(m):
    m=int(m/1000); return f"{m//3600}h{(m%3600)//60:02d}m{m%60:02d}s"

span = (last_ts - app_start)/1000 if app_start else 0
print(f"# APP span (start->last event): {span/3600:.2f}h  ({hh(span*1000)})")
print(f"# Executors: added={exec_added} removed={exec_removed} max_concurrent={exec_max}")
if exec_remove_reasons:
    print("# Executor removal reasons:")
    for reason,c in exec_remove_reasons.most_common(8):
        print(f"    {c:5d}  {reason}")
print(f"# FetchFailed tasks total: {sum(fetchfail.values())}  across {len(fetchfail)} (stage,shuffle) pairs")
for (sid,shid),c in fetchfail.most_common(10):
    print(f"    stage {sid} shuffleId {shid}: {c}")
if fetchfail_times:
    print(f"# FetchFailed time span: {(max(fetchfail_times)-min(fetchfail_times))/60000:.1f} min "
          f"(first {(min(fetchfail_times)-app_start)/60000:.0f}min into run, last {(max(fetchfail_times)-app_start)/60000:.0f}min)")

# Job results
print("\n# JOBS:")
for jid,j in sorted(jobs.items()):
    dur = (j["complete"]-j["submit"])/1000 if j["complete"] and j["submit"] else None
    print(f"    job {jid}: result={j['result']} stages={len(j['stages'])} dur={hh(dur*1000) if dur else 'INCOMPLETE'}")

# rank stages by wall-clock span
rows=[]
for sid,st in stages.items():
    wc = (st["complete_max"]-st["submit_min"])/1000 if st["complete_max"] and st["submit_min"] else None
    rows.append((sid,st,wc))
# also compute how "late" a stage was active (max task finish - app_start)
print("\n# TOP STAGES by cumulative executor Run Time (where wall-clock went):")
print(f"{'stg':>4} {'att':>3} {'nTsk':>6} {'ok':>6} {'fail':>5} {'FF':>5} {'wall':>9} {'runT_sum':>10} {'cpu%':>5} {'gc%':>5} {'fetchWt%':>8} {'shRead':>9} {'shWrite':>9} {'spill(m/d)GB':>13} {'maxTask':>8}")
for sid,st,wc in sorted(rows, key=lambda x:-x[1]["runTime"])[:16]:
    run=st["runTime"];
    cpu_pct = 100*st["cpuTime"]/run if run else 0
    gc_pct  = 100*st["gcTime"]/run if run else 0
    fw_pct  = 100*st["fetchWait"]/run if run else 0
    print(f"{sid:>4} {len(st['attempts']):>3} {st['numTasks']:>6} {st['tsucc']:>6} {st['tfail']:>5} {st['tfetchfail']:>5} "
          f"{hh(wc*1000) if wc else '   n/a':>9} {hh(st['runTime']):>10} {cpu_pct:>4.0f} {gc_pct:>4.0f} {fw_pct:>7.0f} "
          f"{gb(st['shReadBytes']):>8.1f}G {gb(st['shWriteBytes']):>8.1f}G {gb(st['memSpill']):>5.1f}/{gb(st['diskSpill']):>5.1f} {hh(st['maxTaskRun']):>8}")

# skew + memory detail for the dominant stage
dom = max(rows, key=lambda x:x[1]["runTime"])[1]
print(f"\n# DOMINANT stage name: {dom['name'][:160]}")
if dom["durs"]:
    d=sorted(dom["durs"]); n=len(d)
    print(f"# task dur (sampled n={n}): p50={hh(d[n//2])} p95={hh(d[int(n*0.95)])} p99={hh(d[int(n*0.99)])} max={hh(d[-1])}  -> skew ratio max/p50 = {d[-1]/max(d[n//2],1):.1f}x")
print(f"# dominant peakExecMem/task max = {gb(dom['peakExecMem']):.2f}GB ; spill mem={gb(dom['memSpill']):.1f}GB disk={gb(dom['diskSpill']):.1f}GB")

# which SQL execution / plan
print("\n# SQL executions:")
for eid,s in sorted(sql_exec.items()):
    print(f"    exec {eid}: {s['desc'][:120]}")

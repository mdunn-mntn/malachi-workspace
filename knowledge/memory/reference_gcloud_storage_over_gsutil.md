---
name: reference_gcloud_storage_over_gsutil
description: gsutil is BANNED on Astro task pods (2026-09-02) — bulk copies land ~2 of ~194 objects in EVERY mode (forked -m, threads-only -m, plain sequential cp -I) while the identical command moves all 194 from a Mac; the pod fix is gcloud-token + GCS JSON API (objects.list + alt=media). On the Mac forked -m still dies; prefer gcloud storage cp.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [gsutil, gcloud storage, GCS download, multi-file copy, hang, stall, LibreSSL, parallel copy, multiprocessing, python 3.9, parallel_process_count, threads-only gsutil, forked workers die, constrained pod cpu, fetch.py GSUTIL_OPTS, PR 1260, downloader freeze partial sweep, spark-events composite, GHFS composite no hashes, found no hashes to validate, gsutil banned astro pods, json api download, objects.list alt=media, cp -I sequential, PR 1263, PR 1264, zstd -t verify, stored bytes untranscoded, gsutil rm -r hang, gcloud storage rm -r, delete prefix, AUDI-1279, 791 files 17 minutes zero copied, gcloud storage 6 seconds, stale gsutil processes, other claude sessions, ps etime gsutil, ReauthUnattendedError, gsutil ls empty listing, false zero listing, AUDI-1321]
domain: [infra, workflow]
lifecycle: active
last_verified: 2026-09-05
---

`gsutil -m cp` (and `-m cp -r`) stalls indefinitely on multi-file transfers on this Mac (single-file and sequential copies work). `gcloud -q storage cp` moves the same file sets at ~10-30 MiB/s without issue.

Re-confirmed 2026-08-24 (AUDI-1142 shopper-graph pod-log pulls): `-m` hung again; a sequential `gsutil cp` loop worked. The two early hypotheses (macOS LibreSSL, AUDI-431 2026-08-10; gsutil's Python 3.9 `multiprocessing` on macOS, 2026-08-24) were SETTLED 2026-09-01: the discriminating variable is **process forking**, and it is not Mac-only — see below.

**Why:** cost 2 stalled background downloads + a 15-min stall-detector round-trip in AUDI-431 (2026-08-10), and another hang in AUDI-1142 (2026-08-24).

**How to apply:** default to `gcloud -q storage cp [-r]` for every GCS transfer involving wildcards or multiple objects. **The `ls`/`du` carve-out is NOT safe either (2026-09-05):** on an expired reauth `gsutil ls` prints the error to stderr and an EMPTY listing to stdout, so a piped count reads a full prefix as ZERO — [[reference_gsutil_reauth_false_zero]]. Where gsutil is required (e.g. `check_hashes=never` for zstd event logs), use plain `cp` or threads-only `-m` (`-o "GSUtil:parallel_process_count=1"`). [[feedback_background_work_liveness]]

## Root cause settled 2026-09-01 — forked `-m` workers die quietly in constrained pods too (AUDI-1194)

**gsutil `-m`'s process-FORKED workers die silently under CPU constraint, and the parent still exits "Done".** Every optimizer sweep since 2026-08-28 landed ~2/192 spark-events logs (194/200 counted failed) on the 0.25-CPU Astro pod while exiting cleanly, which froze finding resolution for 6 consecutive sweeps — the sweep never errored, it just went partial forever (full impact: `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_diagnosis_2026_09_01.md` §1).

Proven by isolation, not inference: forked `-m` hangs or silently loses files on the Mac AND the pod; plain `cp` and `-m` with `-o "GSUtil:parallel_process_count=1"` (threads-only parallelism) copy everything on both.

- **Fix: airflow-ti PR #1260** — `GSUTIL_OPTS` in `airflow_optimizer/fetch.py` forces threads-only `-m`.
- **Benign warning, do not chase:** spark-events objects are GHFS-synced COMPOSITE objects with NO stored hashes — gsutil's "Found no hashes to validate" warning under `check_hashes=never` is expected, not a failure.

## Superseding correction 2026-09-02 — gsutil itself is broken on Astro task pods; JSON API is the path (AUDI-1194)

The 2026-09-01 root cause was too narrow and the threads-only fix was FALSIFIED in prod the next
day (evidence then: threads-only `-m` and plain `cp` copied everything in the Mac/pod isolation
matrix). On the Astro task pod, bulk copies land ~2 of ~194 objects in EVERY gsutil mode — forked
`-m`, threaded `-m` with `parallel_process_count=1` (PR #1260's fix), AND plain sequential
`cp -I`. The identical sequential command moves all 194 objects (1.8 GiB) from a Mac. Not source
deletion: fresh listings immediately re-stat clean. The note above stays as the evidence trail;
its Mac-side findings still hold (forked `-m` dies on the Mac too).

Same pod-only failure class the debugger's marker writes hit 2026-08-28 (PR #1243, gsutil
unauthenticated in Astro pods) — which is exactly the day optimizer finding resolution froze.

**THE fix everywhere on pods: gcloud-token + GCS JSON API** — `objects.list` + `alt=media`
downloads (stored bytes come back untranscoded; verify zstd payloads with `zstd -t`). PR #1263
(drop `-m`) was insufficient; **PR #1264 (JSON API downloader rewrite) MERGED, live on image
`deploy-2026-09-02T19-27-09`** — the 19:35 UTC sweep ran complete=True on the full corpus.

Rule of thumb: pods get the JSON API; the Mac gets `gcloud -q storage cp`; `gsutil` survives only
for `ls`/`du` listings — and even there it can return a silent empty listing on an expired reauth
(2026-09-05, [[reference_gsutil_reauth_false_zero]]).

## 2026-09-03 — `rm -r` too (AUDI-1279)
`gsutil -m rm -r gs://mntn-data-archive-dev/shopper_graph/audi_1279_staging/` hung for 2 min on this Mac and was killed;
`gcloud -q storage rm -r` on the same prefix finished in seconds. Same rule as `cp`: `gcloud storage` for any multi-object delete.

[[project_airflow_optimizer]]

## 2026-09-05 — the Mac gap measured, and 34 stale gsutil processes from other sessions (AUDI-1321)
**`gsutil -q -m cp -r` copied ZERO of 791 small parquet files in 17 minutes; `gcloud storage cp -r` copied all
791 in 6 seconds** — same source, same destination, same credential. That is the cleanest measurement of the
gap so far, and it is the one to quote: this is not "gsutil is slower", it is "gsutil does not finish".

**Contributing cause worth checking before blaming the tool: 34 stale `gsutil` processes left behind by two
OTHER Claude sessions sharing this machine** — 32 of them running **3 days 20 hours** (session `590a4308`,
copying into `scratchpad/iso_m_I/`) and one **2 days 2 hours** (spark-events into session `67074af2`). A
forked-worker copy that hangs never exits, so they accumulate silently and compete for the same network and
CPU. Check with `ps -eo pid,etime,command | grep gsutil` and clear the dead ones before diagnosing a slow
transfer; a hung copy belongs to whichever session started it, so kill by PID, not with a blanket `pkill -f
python` that could take a live job down. [[feedback_background_work_liveness]] [[feedback_shared_worktree_commits]]

**Separately: an expired gcloud reauth makes `gsutil ls` return an EMPTY listing on stdout** while
`ReauthUnattendedError` goes to stderr, so `gsutil ls … 2>/dev/null | grep -c` reads a full prefix as **0
objects**. Full write-up and the guard: [[reference_gsutil_reauth_false_zero]].

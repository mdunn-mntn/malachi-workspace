# Compass RCA answer — INC-006 batch_fetch (2026-07-29)

Root cause (moderate confidence): batch_fetcher.py::download_file() crashes on unhandled OpenAI SDK exception — no null-check on output_file_id before files.content(). If the batch completed with all rows errored (output_file_id=None, only error_file_id set), files.content(None) throws and kills the pod deterministically on all 3 retries. Secondary: possible OOM at the pod's 1Gi limit (whole result buffered in-process).

Could NOT confirm (access gaps): live pod logs/traceback (Airflow deploy not in Compass's monitored GKE/Loki fleet — likely Composer or a different cluster/project); OpenAI batch id/status + GCS artifacts (PERMISSION_DENIED on mntn-data-archive-prod, different project).

Confirmed via static code: DAG def (batch_fetch, image OPEN_AI_BATCH, 1Gi limit, memory bumped before in 9b18155/9bf82ac); fetch_results.py entrypoint; the bug in batch_fetcher.py download_file L27-45 (no null check, try/except only wraps upload not fetch); batch_transition flips was_submitted only once the batch is in_progress/completed.

Fix: SteelHouse/shopper_graph/openai/openai_wrapper/batch_fetcher.py download_file — null-check output_file_id + terminal-status guards (failed/expired/cancelled -> skip) + try/except around fetch. Also gate fetch_results.py update_source_file_s3() on real download success. Secondary infra fix: bump batch_fetch memory >1Gi if OOM confirmed.

(Full answer pasted by Malachi from Compass Basecamp chat.)

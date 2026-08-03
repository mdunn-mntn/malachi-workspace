"""Automated Airflow/Spark failure-triage agent (AUDI-1191).

Key-free, deterministic-first debugger for failed Airflow tasks. Deterministic
code (this package) fetches logs, correlates the downstream Spark job, and
matches failure signatures; an LLM orchestrator (added later) only synthesizes
the BLUF/STAR report. Runs on interactive `astro` / `gcloud` / Databricks-OAuth
auth — no stored tokens, no Slack bot.

Modules:
- signatures     : deterministic Spark/Airflow failure taxonomy (regex fingerprints)
- databricks_rca : net-new Databricks job-run analyzer (CLI, key-free)
- dataproc_rca   : Dataproc batch analyzer (harvested from data-eng-assistant)
- parse          : Airflow-log parser + operator->engine router + cross-layer synthesis
- report         : BLUF/STAR <=500-char report generator
- incident_match : lightweight local matcher over on-call/incident_log.jsonl
- synth          : LLM synthesis fallback for unknown signatures (Anthropic Messages API)
- orchestrate    : top-level entrypoint (log -> diagnosis -> report)
"""

__all__ = [
    "databricks_rca",
    "dataproc_rca",
    "incident_match",
    "orchestrate",
    "parse",
    "report",
    "signatures",
    "synth",
]

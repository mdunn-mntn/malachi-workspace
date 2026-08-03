"""LLM synthesis fallback for the airflow debugger (deterministic-first).

Used ONLY when the deterministic signature classifier finds no match (confidence
"low"). Reasons over the few-KB evidence bundle + matched past incidents and
returns a <=500-char BLUF/STAR RCA via the Anthropic Messages API
(claude-opus-4-8). The LLM-provider auth (ANTHROPIC_API_KEY, or an `ant auth
login` profile) is the orchestration credential and is separate from the
key-free data-access layer (astro / gcloud / Databricks OAuth).
"""

from __future__ import annotations

import json

MODEL = "claude-opus-4-8"
_MAX_INPUT = 12000  # chars of evidence JSON sent to the model

_SYSTEM = (
    "You are an on-call Spark/Airflow root-cause assistant. Given a deterministic evidence "
    "bundle from a failed Airflow task and its downstream Spark job (Dataproc or Databricks), "
    "write ONE BLUF/STAR root-cause report. Rules: 500 characters or fewer total; the first "
    "line is the answer (what failed, the most likely root cause, and a confidence word: "
    "high, medium, or low); then one line on the recommended action and whether it is a code "
    "fix or a compute/infra change. No em-dashes. No preamble. No speculation beyond the "
    "evidence. If the evidence is insufficient, say so and name the single thing to check next."
)


def synthesize(evidence: dict, matches: list[dict] | None = None, model: str = MODEL) -> str | None:
    """Return a <=500-char RCA, or a short note if the LLM is unavailable."""
    try:
        import anthropic
    except ImportError:
        return None
    payload = json.dumps(
        {"evidence": evidence, "similar_past_incidents": matches or []}, default=str
    )[:_MAX_INPUT]
    try:
        client = anthropic.Anthropic()  # resolves ANTHROPIC_API_KEY / ant profile from env
        resp = client.messages.create(
            model=model,
            max_tokens=1024,
            thinking={"type": "adaptive"},  # bounded reasoning; quality-leaning, rarely invoked
            system=_SYSTEM,
            messages=[{"role": "user", "content": payload}],
        )
    except Exception as e:  # auth / network / API — never crash the debugger
        return f"(LLM synthesis unavailable: {type(e).__name__})"
    text = next((b.text for b in resp.content if b.type == "text"), "")
    return text.strip()[:600] or None

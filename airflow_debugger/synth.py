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
_MAX_OUTPUT = 500  # chars of RCA returned; matches report.py's contract
_TRIM_STEPS = (2000, 500, 100)  # per-string caps tried until the payload fits

_SYSTEM = (
    "You are an on-call Spark/Airflow root-cause assistant. Given a deterministic evidence "
    "bundle from a failed Airflow task and its downstream Spark job (Dataproc or Databricks), "
    "write ONE BLUF/STAR root-cause report. Rules: 500 characters or fewer total; the first "
    "line is the answer (what failed, the most likely root cause, and a confidence word: "
    "high, medium, or low); then one line on the recommended action and whether it is a code "
    "fix or a compute/infra change. No em-dashes. No preamble. No speculation beyond the "
    "evidence. If the evidence is insufficient, say so and name the single thing to check next."
)


def _trim(obj: object, max_str: int) -> object:
    """Truncate long string VALUES in place of slicing, so the JSON stays valid."""
    if isinstance(obj, str) and len(obj) > max_str:
        return obj[:max_str] + "...[truncated]"
    if isinstance(obj, dict):
        return {k: _trim(v, max_str) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_trim(v, max_str) for v in obj]
    return obj


def _build_payload(evidence: dict, matches: list[dict] | None) -> str:
    """Serialize evidence + matches as valid JSON within _MAX_INPUT chars."""
    doc = {"evidence": evidence, "similar_past_incidents": matches or []}
    payload = json.dumps(doc, default=str)
    for max_str in _TRIM_STEPS:
        if len(payload) <= _MAX_INPUT:
            break
        payload = json.dumps(_trim(json.loads(payload), max_str), default=str)
    return payload


def synthesize(
    evidence: dict, matches: list[dict] | None = None, model: str = MODEL
) -> tuple[str | None, str | None]:
    """Return (rca, note): a <=500-char RCA or None, plus a note when the LLM was unavailable.

    A None rca means the deterministic report must stand; the note (if any) explains why the
    LLM produced nothing and belongs in the diagnosis, never in the report itself.
    """
    try:
        import anthropic
    except ImportError:
        return None, "LLM synthesis unavailable: anthropic package not installed"
    payload = _build_payload(evidence, matches)
    try:
        client = anthropic.Anthropic()  # resolves ANTHROPIC_API_KEY / ant profile from env
        resp = client.messages.create(
            model=model,
            max_tokens=4096,  # hard cap on adaptive thinking + visible text; headroom for both
            thinking={"type": "adaptive"},  # bounded reasoning; quality-leaning, rarely invoked
            system=_SYSTEM,
            messages=[{"role": "user", "content": payload}],
        )
    except Exception as e:  # auth / network / API: never crash the debugger
        return None, f"LLM synthesis unavailable: {type(e).__name__}: {str(e)[:200]}"
    text = next((b.text for b in resp.content if b.type == "text"), "").strip()
    if not text:
        note = "LLM returned no text"
        if getattr(resp, "stop_reason", None) == "max_tokens":
            note += " (max_tokens cap hit during thinking)"
        return None, note
    if getattr(resp, "stop_reason", None) == "max_tokens":
        return text[:_MAX_OUTPUT], "LLM output truncated at max_tokens cap"
    return text[:_MAX_OUTPUT], None

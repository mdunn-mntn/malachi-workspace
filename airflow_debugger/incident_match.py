"""Local incident matcher (lightweight, no heavy ML dependency).

Matches a new failure against the local corpus (`on-call/incident_log.jsonl`) by
token overlap + dag/task boosts, surfacing the top-k most similar past
incidents. For a small corpus (tens of incidents) lexical overlap is sufficient
and avoids a torch/sentence-transformers dependency; upgrade to embeddings only
if the corpus grows large.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

_CORPUS = Path(__file__).resolve().parents[1] / "on-call" / "incident_log.jsonl"
_STOP = {
    "the", "a", "an", "to", "of", "in", "on", "for", "and", "or", "is", "was",
    "at", "by", "with", "from", "this", "that", "it", "as", "not", "job", "task",
}  # fmt: skip
_TOKEN = re.compile(r"[a-z0-9_]+")


def _tokens(text: str | None) -> set[str]:
    return {t for t in _TOKEN.findall((text or "").lower()) if len(t) > 2 and t not in _STOP}


def _load() -> list[dict]:
    if not _CORPUS.exists():
        return []
    return [json.loads(line) for line in _CORPUS.read_text().splitlines() if line.strip()]


def match(
    dag: str | None,
    task: str | None,
    query_text: str,
    top_k: int = 3,
    min_score: float = 0.08,
) -> list[dict]:
    """Return the top-k most similar past incidents (empty if the corpus is absent)."""
    q = _tokens(" ".join(filter(None, [dag, task, query_text])))
    if not q:
        return []
    scored = []
    for r in _load():
        doc = _tokens(
            " ".join(
                filter(None, [r.get("dag"), r.get("task"), r.get("signature"), r.get("verdict")])
            )
        )
        if not doc:
            continue
        score = len(q & doc) / len(q | doc)  # Jaccard
        if dag and r.get("dag") == dag:
            score += 0.30
        if task and r.get("task") == task:
            score += 0.20
        if score >= min_score:
            scored.append(
                {
                    "inc": r.get("inc"),
                    "verdict": r.get("verdict"),
                    "ticket": r.get("ticket"),
                    "score": round(score, 3),
                    "signature": (r.get("signature") or "")[:160],
                }
            )
    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[:top_k]

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
import sys
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
    records: list[dict] = []
    for lineno, line in enumerate(_CORPUS.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            rec = None
        if isinstance(rec, dict):
            records.append(rec)
        else:
            # one bad append (e.g. interrupted write-back) must not crash match()
            print(f"incident_match: skipped malformed corpus line {lineno}", file=sys.stderr)
    return records


def match(
    dag: str | None,
    task: str | None,
    query_text: str,
    top_k: int = 3,
    min_score: float = 0.08,
) -> list[dict]:
    """Return the top-k most similar past incidents (empty if the corpus is absent)."""
    q_text = _tokens(query_text)
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
        # overlap coefficient over the query, so long corpus signatures don't dilute the score
        score = len(q & doc) / len(q)
        # identity boosts need some query-text agreement, else same-dag noise clears min_score
        boostable = not q_text or bool(q_text & doc)
        if boostable and dag and r.get("dag") == dag:
            score += 0.30
        if boostable and task and r.get("task") == task:
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

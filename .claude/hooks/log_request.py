#!/usr/bin/env python3
"""log_request.py — UserPromptSubmit hook. Append ONE keyword-only record per user prompt.

Feeds the request-mining digest (`.claude/scripts/request_digest.py`), which surfaces recurring request
shapes so a repeated task can be PROPOSED as a /skill (human decides — nothing is auto-created).

Privacy / safety:
  · LOCAL ONLY — writes to knowledge/.request_log.jsonl, which is gitignored. Never pushed.
  · KEYWORD ONLY — stores a coarse verb + up to 10 content tokens + a one-way hash. The raw prompt text
    is NEVER stored (the hash can't be reversed). Deleting the file at any time loses nothing but history.
  · NON-BLOCKING — any error prints nothing and exits 0, so it can never interfere with a prompt.
"""
import hashlib, json, os, re, sys
from datetime import datetime

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
LOG = os.path.join(ROOT, "knowledge", ".request_log.jsonl")

# First-token imperatives worth tracking as the request's "verb".
VERBS = {
    "add", "analyze", "audit", "build", "chart", "check", "compare", "compute", "create", "debug",
    "deploy", "diagnose", "document", "draft", "explain", "find", "fix", "generate", "investigate",
    "list", "make", "measure", "plot", "port", "pull", "query", "rebuild", "refactor", "review", "run",
    "show", "summarize", "test", "update", "validate", "verify", "write",
}
STOP = {
    "the", "a", "an", "and", "or", "but", "to", "of", "in", "on", "for", "with", "this", "that", "these",
    "those", "is", "are", "was", "were", "be", "been", "do", "does", "did", "please", "can", "could",
    "you", "your", "our", "we", "it", "its", "my", "me", "i", "what", "how", "why", "when", "where",
    "which", "who", "should", "would", "will", "need", "want", "get", "got", "from", "at", "as", "by",
    "so", "if", "then", "them", "they", "there", "here", "into", "out", "up", "down", "not", "no", "yes",
    "all", "any", "some", "more", "most", "just", "now", "one", "two", "let", "lets", "keep", "going",
    "like", "about", "over", "than", "also", "have", "has", "had", "was",
}
TOKEN = re.compile(r"[a-z0-9_]{3,}")


def main():
    try:
        raw = sys.stdin.read()
        prompt = (json.loads(raw).get("prompt") or "").strip() if raw.strip() else ""
        if not prompt:
            return 0
        low = prompt.lower()
        toks = TOKEN.findall(low)
        verb = toks[0] if toks and toks[0] in VERBS else ""
        nouns, seen = [], set()
        for t in toks:
            if t in STOP or t == verb or t in seen:
                continue
            seen.add(t)
            nouns.append(t)
            if len(nouns) >= 10:
                break
        rec = {
            "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "verb": verb,
            "nouns": nouns,
            "hash": hashlib.sha256(low.encode("utf-8")).hexdigest()[:12],
        }
        os.makedirs(os.path.dirname(LOG), exist_ok=True)
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
    except Exception:
        pass  # never interfere with the prompt
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Stop hook — measure the reply that just shipped and record the verdict.

The chat cap (RULE 0) was the only cap in the standard with no gate: lint_comms.py sees Jira/PR/commit
payloads via PreToolUse, but a chat reply passes through nothing. This closes the loop. It measures the
final assistant text, exempts genuinely un-compressible payload (fenced code, tables), and writes the
verdict to .claude/state/chat_brevity.json. brevity_pointer.py reads that file on the NEXT prompt and
feeds a breach back into context, so a long reply tightens the following one instead of vanishing.
Advisory only (always exit 0) — blocking a Stop hook makes the model write MORE, which is the wrong way.
"""
import json, os, re, sys

CHAR_CAP, WORD_CAP = 500, 75
ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
STATE = os.path.join(ROOT, ".claude", "state", "chat_brevity.json")
LOG = os.path.join(ROOT, ".claude", "state", "chat_brevity_log.jsonl")


def prose_only(text):
    text = re.sub(r"```.*?```", "", text, flags=re.S)          # fenced code
    text = re.sub(r"^\s*\|.*\|\s*$", "", text, flags=re.M)     # table rows
    return text.strip()


def last_reply(path):
    try:
        rows = [json.loads(l) for l in open(path, encoding="utf-8") if l.strip()]
    except Exception:
        return ""
    for row in reversed(rows):
        msg = row.get("message") or {}
        if row.get("type") != "assistant" or msg.get("role") != "assistant":
            continue
        content = msg.get("content")
        if not isinstance(content, list):
            continue
        text = "\n".join(b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text")
        if text.strip():
            return text
    return ""


def main():
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    reply = last_reply(payload.get("transcript_path") or "")
    if not reply.strip():
        return 0
    body = prose_only(reply)
    chars, words = len(body), len(body.split())
    over = chars > CHAR_CAP or words > WORD_CAP
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    verdict = {"chars": chars, "words": words, "over": over,
               "session": payload.get("session_id", ""), "cap": [CHAR_CAP, WORD_CAP]}
    json.dump(verdict, open(STATE, "w"))
    with open(LOG, "a") as fh:
        fh.write(json.dumps(verdict) + "\n")
    if over:
        print(f"[brevity] BREACH: last reply {chars}ch/{words}w vs cap {CHAR_CAP}ch/{WORD_CAP}w "
              f"(prose only, code+tables exempt). RULE 0.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

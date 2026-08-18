#!/usr/bin/env python3
"""UserPromptSubmit hook — put the cap in the last position before generation.

RULE 0 sits at the top of a 14k-char CLAUDE.md loaded thousands of tokens ago; everything nearer the
generation point (memory recall, routing, tool results, ultracode's "be exhaustive") argues for more
words. This spends ~20 tokens re-pointing at the rule from the recency slot, and escalates when
chat_brevity_meter.py recorded a breach on the previous turn. Pointer, not a restatement.
Silent on harness re-invocations. Any error prints nothing and exits 0.
"""
import json, os, sys

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
STATE = os.path.join(ROOT, ".claude", "state", "chat_brevity.json")
SKIP = ("task-notification", "toolu_", "<system-reminder", "<local-command", "<command-name",
        "has completed", "background task", "stdout of the background")


def main():
    try:
        raw = sys.stdin.read()
        prompt = (json.loads(raw).get("prompt") or "").strip() if raw.strip() else ""
    except Exception:
        return 0
    if not prompt or any(m in prompt.lower() for m in SKIP):
        return 0

    line = "[brevity] RULE 0: answer in line 1, then stop. Cap 500ch/75w prose. No preamble, no tool-call narration, no closing summary. Depth goes in files, not in the reply."
    try:
        last = json.load(open(STATE))
        if last.get("over"):
            line += (f" LAST REPLY BREACHED at {last['chars']}ch/{last['words']}w — this one is a hard "
                     f"rewrite: ≤3 fragments, no exceptions.")
    except Exception:
        pass
    print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())

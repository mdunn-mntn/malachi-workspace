"""Slack delivery for one sweep's digest, off until a token and a channel both exist.

The gate is the credential, not a flag: a flag can be flipped by someone who has not thought
about which channel the bot may write to, and a missing token cannot. With neither set the sweep
renders the digest and returns it unsent, so its shape is reviewable in a log and in tests long
before anything reaches a channel.
"""

from __future__ import annotations

import json
import os
import subprocess

SLACK_API = "https://slack.com/api"
TOKEN_ENV = "SLACK_BOT_TOKEN"
CHANNEL_ENV = "OPTIMIZER_SLACK_CHANNEL"
_TIMEOUT = 20
_MAX_CHARS = 39_000  # chat.postMessage rejects a text block over 40k


def enabled() -> bool:
    """True only when a token and a channel are both configured."""
    return bool(os.environ.get(TOKEN_ENV) and os.environ.get(CHANNEL_ENV))


def _post(method: str, payload: dict) -> dict:
    """One Slack Web API call. Returns the parsed body, or an error dict; never raises."""
    token = os.environ.get(TOKEN_ENV) or ""
    try:
        res = subprocess.run(
            ["curl", "-sS", "--fail-with-body", "-X", "POST", f"{SLACK_API}/{method}",
             "-H", "Content-Type: application/json; charset=utf-8",
             "--data", json.dumps(payload), "-K", "-"],
            # -K - reads the auth header from stdin: argv is world-readable and lands in tracebacks
            input=f'header = "Authorization: Bearer {token}"\n',
            capture_output=True, text=True, timeout=_TIMEOUT,
        )  # fmt: skip
    except (subprocess.TimeoutExpired, OSError) as e:
        return {"ok": False, "error": f"curl {type(e).__name__}"}
    if res.returncode != 0:
        return {"ok": False, "error": (res.stderr or res.stdout).strip()[:200]}
    try:
        return json.loads(res.stdout or "{}")
    except ValueError:
        return {"ok": False, "error": "non-json response from Slack"}


def deliver(text: str, channel: str = "") -> dict:
    """Post one digest. Returns what happened; never raises into the sweep."""
    if not text.strip():
        return {"sent": False, "reason": "empty digest"}
    if not (channel or enabled()):
        return {"sent": False, "reason": f"no {TOKEN_ENV} or {CHANNEL_ENV}"}
    body = text if len(text) <= _MAX_CHARS else text[:_MAX_CHARS] + "\n… truncated for Slack."
    res = _post("chat.postMessage",
                {"channel": channel or os.environ[CHANNEL_ENV], "text": body,
                 "unfurl_links": False})
    return {"sent": bool(res.get("ok")), "error": None if res.get("ok") else res.get("error")}


def deliver_thread(parent: list, replies: list, channel: str = "") -> dict:
    """Post the summary, then each DAG as its own reply under it. Never raises."""
    if not parent:
        return {"sent": False, "reason": "nothing to post"}
    if not (channel or enabled()):
        return {"sent": False, "reason": f"no {TOKEN_ENV} or {CHANNEL_ENV}"}
    target = channel or os.environ[CHANNEL_ENV]
    res = _post("chat.postMessage",
                {"channel": target, "blocks": parent, "text": "Spark optimizer digest",
                 "unfurl_links": False})
    if not res.get("ok"):
        return {"sent": False, "error": res.get("error"), "replies": 0}
    ts, posted, failed = res.get("ts"), 0, []
    for blocks in replies:
        r = _post("chat.postMessage",
                  {"channel": target, "thread_ts": ts, "blocks": blocks,
                   "text": "finding", "unfurl_links": False})
        if r.get("ok"):
            posted += 1
        else:
            failed.append(r.get("error"))
    return {"sent": True, "ts": ts, "replies": posted,
            "error": "; ".join(f for f in failed if f) or None}

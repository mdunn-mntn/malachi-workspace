"""Settle the question the signature leaves open, instead of handing it to the reader.

A signature says what class of failure this is. For most classes that still leaves a fork the
on-call has to resolve by hand: a task killed at its timeout either outgrew a limit it has been
creeping toward, or hung; a missing partition either landed late or never landed. "Read the
runtime trend" and "check whether the object exists" are the two halves of that work, and the
debugger can do both. A verdict that ends in an instruction to go look is not a verdict.

Each resolver returns the branch it settled, the evidence that settled it, and solutions ranked
by what the evidence supports. When the evidence is not reachable the resolver returns nothing
and the signature's own remedy stands, which is the honest fallback: a guessed branch is worse
than an unresolved one.
"""

from __future__ import annotations

import json
import re
import statistics
from dataclasses import dataclass, field

_TRACEBACK_TAIL = re.compile(
    r"^(?P<exc>[A-Za-z_][\w.]*(?:Error|Exception|Failure|Timeout)):\s*(?P<msg>.+)$", re.MULTILINE
)
# A qualified name arrives as `catalog`.`schema`.`table`, so the chain has to be captured whole:
_QUALIFIED = r"(`?[\w-]+`?(?:\.`?[\w-]+`?)*)"
_MISSING_REL = re.compile(
    rf"TABLE_OR_VIEW_NOT_FOUND\].{{0,60}}?{_QUALIFIED} cannot be found|"
    rf"UNRESOLVED_COLUMN.{{0,60}}?[`'\"]{_QUALIFIED}[`'\"]|"
    rf"cannot resolve [`'\"]{_QUALIFIED}[`'\"]",
    re.IGNORECASE | re.DOTALL,
)
_PRINCIPAL = re.compile(r"([\w.+-]+@[\w-]+\.iam\.gserviceaccount\.com|[\w.+-]+@[\w.-]+\.\w+)")
_DENY_DETAIL = re.compile(r'details\s*=\s*"([^"]{10,300})"')
_PERMISSION = re.compile(r"\b((?:storage|bigquery|dataproc|aiplatform|compute)\.[\w.]+)\b")
_QUOTA = re.compile(
    r"Insufficient '?(?P<metric>[A-Z_0-9]+)'? quota.{0,40}?Requested (?P<req>[\d.]+),"
    r" available (?P<avail>\d+(?:\.\d+)?)",
    re.IGNORECASE | re.DOTALL,
)
_ZONE = re.compile(r"zones?/([a-z]+-[a-z]+\d+-[a-z])")
_DB_USER = re.compile(
    r'password authentication failed for user "?([\w.-]+)"?|'
    r"Access denied for user '([\w.-]+)'@",
    re.IGNORECASE,
)
_GS_PATH = re.compile(r"(gs://[\w.\-/=]+)")
_JDBC = re.compile(r"jdbc:(\w+)://([\w.\-]+(?::\d+)?(?:/[\w-]+)?)")


@dataclass
class Resolution:
    """One settled fork: what it was, what proved it, and what to do about it."""

    verdict: str
    evidence: str
    solutions: list[str] = field(default_factory=list)


def _last_exception(text: str) -> tuple[str, str] | None:
    """The deepest `SomeError: message` line, which is the one that actually raised."""
    hits = _TRACEBACK_TAIL.findall(text or "")
    if not hits:
        return None
    exc, msg = hits[-1]
    return exc, msg.strip()[:220]


def _timeout_budget(text: str) -> float | None:
    """The execution_timeout Airflow announced, in seconds."""
    m = re.search(r"execution_timeout.{0,40}?([\d.]+)\s*(second|s\b)", text or "", re.IGNORECASE)
    if m:
        return float(m.group(1))
    m = re.search(r"timeout of ([\d.]+) seconds", text or "", re.IGNORECASE)
    return float(m.group(1)) if m else None


def _execution_timeout(diag: dict, text: str, client: object | None) -> Resolution | None:
    """Outgrew its limit, or hung? The run history answers it; the log alone does not."""
    if client is None:
        return None
    ident = diag.get("identity") or {}
    dag_id, task_id = ident.get("dag_id"), ident.get("task_id")
    if not (dag_id and task_id):
        return None
    try:
        history = client.task_history(dag_id, task_id)
    except Exception:
        return None
    ok = [t["duration"] for t in history if t.get("state") == "success" and t.get("duration")]
    bad = [t["duration"] for t in history if t.get("state") == "failed" and t.get("duration")]
    if len(ok) < 3 or not bad:
        return None

    median = statistics.median(ok)
    killed = max(bad)
    budget = _timeout_budget(text) or killed
    recent = statistics.median(ok[: max(len(ok) // 3, 3)])
    older = statistics.median(ok[-max(len(ok) // 3, 3) :])
    growth = (recent - older) / older if older else 0.0

    ev = (
        f"killed at {killed / 60:.0f}m against a {budget / 60:.0f}m limit; the last "
        f"{len(ok)} successful runs took {older / 60:.0f}m rising to {recent / 60:.0f}m "
        f"({growth:+.0%})"
    )
    if median >= 0.6 * budget:
        headroom = _runs_until_breach(recent, growth, budget)
        when = (
            f"about {headroom} more runs at the current growth rate"
            if headroom
            else "indefinitely, since runtime is not growing"
        )
        new_limit = max(budget * 1.5, recent * 1.5)
        return Resolution(
            verdict=(
                "The task outgrew its time limit. Successful runs already use most of the time "
                "allowed, so it ran out of time doing real work rather than hanging."
            ),
            evidence=ev,
            solutions=[
                f"Now: raise execution_timeout from {budget / 60:.0f}m to {new_limit / 60:.0f}m. "
                f"That holds for {when}.",
                f"Then find out why it got slower: runtime rose {growth:+.0%} across these runs. "
                "Compare the input row count or file count for the same period.",
                "If the input did not grow, the task itself got slower: profile the longest stage "
                "of a recent successful run against an older one.",
            ],
        )
    return Resolution(
        verdict=(
            "The task hung. Successful runs finish in about "
            f"{median / 60:.0f}m against a {budget / 60:.0f}m limit, so the killed run was not "
            "doing more work, it stopped making progress."
        ),
        evidence=ev,
        solutions=[
            "Now: re-run it once. A hang that does not reproduce was a stuck dependency.",
            "Read the killed run's last log line and compare it with a successful run's line at "
            "the same point; the gap is where it stopped.",
            "Do not raise the time limit. It would only make the next hang take longer to page.",
        ],
    )


def _runs_until_breach(current: float, growth: float, budget: float) -> int | None:
    """How many more runs the new limit survives at the observed growth rate."""
    if growth <= 0 or current <= 0:
        return None
    limit, runs, dur = budget * 1.5, 0, current
    while dur < limit and runs < 200:
        dur *= 1 + growth
        runs += 1
    return runs


def _dbt_runtime(diag: dict, text: str, client: object | None) -> Resolution | None:
    """dbt prints its own summary above the real Python exception. Report the exception."""
    exc = _last_exception(text)
    if not exc:
        return None
    model = re.search(r"(?:Runtime|Database) Error in model ([\w.]+)", text or "")
    who = f" in model {model.group(1)}" if model else ""
    return Resolution(
        verdict=f"The model raised {exc[0]}{who}: {exc[1]}",
        evidence="deepest exception in the traceback under dbt's Runtime Error line",
        solutions=[
            f"Fix {exc[0]} at its source; dbt's line numbers are templated and point elsewhere.",
            "Re-run the single model before the full selector to confirm the fix.",
        ],
    )


def _analysis(diag: dict, text: str, client: object | None) -> Resolution | None:
    """Name the relation or column that did not resolve, rather than the class of error."""
    m = _MISSING_REL.search(text or "")
    name = next((g for g in (m.groups() if m else ()) if g), None)
    if not name:
        return None
    name = name.replace("`", "")
    return Resolution(
        verdict=f"The query references `{name}`, which does not resolve.",
        evidence=f"matched on the unresolved identifier `{name}`",
        solutions=[
            f"Confirm `{name}` exists and the job's role can see it; a rename upstream is the usual cause.",
            f"If it was renamed, update the reference; if it was dropped, restore it or drop the read of `{name}`.",
        ],
    )


def _auth(diag: dict, text: str, client: object | None) -> Resolution | None:
    """A grant problem and an expired token need opposite actions, and the error says which."""
    principal = _PRINCIPAL.search(text or "")
    perm = _PERMISSION.search(text or "")
    detail = _DENY_DETAIL.search(text or "")
    expired = re.search(r"token.{0,20}expired|invalid[_ ]token|401", text or "", re.IGNORECASE)
    if detail and not perm:
        # The service explained the denial in its own words; that beats a guessed permission name.
        return Resolution(
            verdict=f"The call was refused: {detail.group(1)}",
            evidence="the refusal message the service returned",
            solutions=[
                "Grant the identity the access that message names, on the resource it names.",
                "Retries cannot clear a refusal; confirm the grant landed before re-running.",
            ],
        )
    if not (principal or perm):
        return None
    who = principal.group(1) if principal else "the job's identity"
    what = perm.group(1) if perm else "the resource in the error"
    if expired and not perm:
        return Resolution(
            verdict=f"The credential for {who} expired; nothing is missing from the grant.",
            evidence="the error is a token expiry, not a permission denial",
            solutions=["Refresh the credential and re-run; a retry alone clears an expiry."],
        )
    return Resolution(
        verdict=f"{who} is missing `{what}`.",
        evidence=f"principal and permission read off the denial: {who} / {what}",
        solutions=[
            f"Grant `{what}` to {who} on the named resource; retries cannot clear a missing grant.",
            "At MNTN the grant is a Crossplane change, not a console edit.",
        ],
    )


def _db_credential(diag: dict, text: str, client: object | None) -> Resolution | None:
    """Name the rejected user so the right secret gets checked."""
    m = _DB_USER.search(text or "")
    user = next((g for g in (m.groups() if m else ()) if g), None)
    target = _JDBC.search(text or "")
    if not user:
        if not target:
            return None
        where = f"{target.group(1)} at {target.group(2)}"
        return Resolution(
            verdict=f"The credential for {where} was rejected. The server named no user, so the "
            "secret the job reads is stale or points at the wrong role.",
            evidence=f"authentication failure against {where}, with no user in the message",
            solutions=[
                f"Compare the secret this job reads for {where} against the current password.",
                "Check when the secret last rotated against the last green run of this task.",
                "Re-running with the same credential fails identically; fix the secret first.",
            ],
        )
    return Resolution(
        verdict=f"The database rejected the password for `{user}`.",
        evidence=f"the server returned an authentication failure for user `{user}`",
        solutions=[
            f"Compare the secret the job reads for `{user}` against the database's current password.",
            "If it rotated, repoint the job at the current secret; re-running with the old one fails identically.",
        ],
    )


def _quota(diag: dict, text: str, client: object | None) -> Resolution | None:
    """Quota errors carry the exact numbers. Report the shortfall, not the category."""
    m = _QUOTA.search(text or "")
    if not m:
        return None
    metric, req, avail = m.group("metric"), float(m.group("req")), float(m.group("avail"))
    short = req - avail
    return Resolution(
        verdict=f"The request needed {req:.0f} {metric} and {avail:.0f} were free, short by {short:.0f}.",
        evidence=f"{metric}: requested {req:.0f}, available {avail:.0f}",
        solutions=[
            f"Find what holds the rest of {metric} in this region before raising anything; "
            "a single idle cluster taking the headroom looks identical to a ceiling that is too low (INC-025).",
            f"If nothing is holding it, raise {metric} for the region (AUDI-1217).",
            f"To unblock this run now, shrink the request below {avail:.0f} {metric}.",
        ],
    )


def _stockout(diag: dict, text: str, client: object | None) -> Resolution | None:
    """A stockout is a zone fact. Name the zones so the retry does not land in the same one."""
    zones = sorted(set(_ZONE.findall(text or "")))
    if not zones:
        return None
    plural = "zones" if len(zones) > 1 else "zone"
    return Resolution(
        verdict=f"GCE had no capacity in {plural} {', '.join(zones)} for the requested machine type.",
        evidence=f"the refusal names {plural} {', '.join(zones)}",
        solutions=[
            "Delete any cluster left in ERROR first; it holds quota and blocks the retry.",
            f"Re-run in 1-2h. Autozone usually picks outside {zones[0]} on the next attempt.",
            "Recurring in the same zone means pinning another zone or widening the machine family.",
        ],
    )


def _late_data(diag: dict, text: str, client: object | None) -> Resolution | None:
    """Landed late or never landed? The path is in the error and the object either exists or not."""
    path = _GS_PATH.search(text or "")
    if not path:
        return None
    target = path.group(1)
    return Resolution(
        verdict=f"The job read {target} before it existed.",
        evidence=f"the read that failed names {target}",
        solutions=[
            f"Check {target} and its _SUCCESS marker now. Present means the producer was late: re-run this task.",
            "Absent means the producer failed or was skipped: fix or re-run the producer, and do not widen the sensor.",
        ],
    )


RESOLVERS = {
    "task_execution_timeout": _execution_timeout,
    "dbt_model_runtime_error": _dbt_runtime,
    "analysis_exception": _analysis,
    "auth_error": _auth,
    "db_credential_rejected": _db_credential,
    "quota_exhaustion": _quota,
    "cluster_create_stockout": _stockout,
    "path_not_found_late_data": _late_data,
}


def resolve(diag: dict, log_text: str = "", client: object | None = None) -> Resolution | None:
    """Settle the signature's open fork, or None when the evidence does not reach a branch."""
    root = diag.get("root_signature") or {}
    fn = RESOLVERS.get(root.get("key") or "")
    if not fn:
        return None
    # Whole engine bundle: the detail that settles a fork sits in a different key per engine.
    text = "\n".join(
        str(x)
        for x in (
            diag.get("root_error"),
            json.dumps(diag.get("spark") or {}, default=str),
            log_text,
        )
        if x
    )
    try:
        return fn(diag, text, client)
    except Exception:
        return None


def as_lines(res: Resolution) -> tuple[str, str]:
    """(why, how) for a settled fork. The solutions are numbered because they are ordered."""
    why = f"{res.verdict} ({res.evidence})"
    how = " ".join(f"{i}. {s}" for i, s in enumerate(res.solutions, 1))
    return why, how

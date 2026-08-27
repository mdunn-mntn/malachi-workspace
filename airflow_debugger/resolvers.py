"""Settle the fork a signature leaves open, or return nothing."""

from __future__ import annotations

import json
import math
import re
import statistics
from dataclasses import dataclass, field

_FLAT = 0.02  # runtime noise below this reads as flat, not a trend
_HORIZON_CAP = 200  # past this the horizon is noise, so say the limit holds instead of a number

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
_EXPIRED = re.compile(
    r"token.{0,20}expired|invalid[_ ]token|\b401\b[^\n]{0,24}unauthorized"
    r"|unauthorized[^\n]{0,24}\b401\b",
    re.IGNORECASE,
)
_DENY_DETAIL = re.compile(r'details\s*=\s*"([^"]{10,300})"')
# A grant is `service.resource.verb`; `<service>.googleapis.com` and a Java frame are neither.
_PERMISSION = re.compile(
    r"(?<![\w.])((?:storage|bigquery|dataproc|aiplatform|compute|serviceusage|secretmanager|logging|pubsub|monitoring|artifactregistry|cloudresourcemanager)\.(?!googleapis\b)"
    r"[a-z][a-zA-Z]*\.[a-z][a-zA-Z]*)\b"
)
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
# The path must sit next to the failure phrase: a log mentions config paths that always exist.
_GS_PATH = re.compile(
    r"(?:PATH_NOT_FOUND|Path does not exist|path does not exist|Missing[^\n]{0,40}partition|"
    r"does not exist|not found)[^\n]{0,120}?(gs://[\w.\-/=]*[\w=/-])",
    re.IGNORECASE,
)
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
    bad = [t for t in history if t.get("state") == "failed" and t.get("duration")]
    if len(ok) < 3 or not bad:
        return None

    # The limit is declared on the task; inferring it from past failures reads a stale config.
    budget = None
    try:
        budget = client.task_timeout(dag_id, task_id)
    except Exception:
        budget = None
    budget = budget or _timeout_budget(text)
    if not budget:
        return None

    run_id = ident.get("run_id")
    this_run = next((t for t in bad if t.get("dag_run_id") == run_id), None)
    # A kill well under the declared limit means the limit changed after it, so it proves nothing.
    if this_run and this_run["duration"] < 0.95 * budget:
        return None
    third = len(ok) // 3
    trended = third >= 2
    recent = statistics.median(ok[:third]) if trended else ok[0]
    older = statistics.median(ok[-third:]) if trended else recent
    growth = (recent - older) / older if trended and older else 0.0
    span = max(len(ok) - third, 1)

    # Name a kill duration only for THIS run; another failure's duration reads as measured.
    hit = (
        f"killed at {this_run['duration'] / 60:.0f}m against a {budget / 60:.0f}m limit"
        if this_run
        else f"hit its {budget / 60:.0f}m limit"
    )
    ev = f"{hit}; {_trend(older, recent, growth, len(ok) if trended else 0)}"
    if recent >= 0.6 * budget:
        new_limit = max(budget * 1.5, recent * 1.5)
        headroom = _runs_until_breach(recent, growth, new_limit, span)
        raise_it = f"Now: raise execution_timeout from {budget / 60:.0f}m to {new_limit / 60:.0f}m."
        if not trended:
            raise_it += " Too few successful runs to say whether it will need raising again."
        elif growth <= _FLAT:
            raise_it += " Runtime is not growing, so it should not need raising again."
        elif headroom is None or headroom > _HORIZON_CAP:
            raise_it += f" At the current {growth:+.0%} drift that holds for hundreds of runs."
        else:
            raise_it += f" That holds for about {max(headroom, 1)} more runs at the current rate."
        if growth > _FLAT:
            why = [
                f"Then find out why it got slower: runtime rose {growth:+.0%} across these runs. "
                "Compare the input row count or file count for the same period.",
                "If the input did not grow, the task itself got slower: profile the longest "
                "stage of a recent successful run against an older one.",
            ]
        else:
            why = [
                "Then find out why it needs the whole window: runtime is not growing, so it has "
                "been running this close to the limit since the limit was set. Compare the input "
                "row count or file count with a run from before it was set.",
                "If the input is unchanged, the limit was set too tight: profile the longest "
                "stage of a successful run to see where the time goes.",
            ]
        return Resolution(
            verdict=(
                "The task outgrew its time limit. Successful runs already use most of the time "
                "allowed, so it ran out of time doing real work rather than hanging."
            ),
            evidence=ev,
            solutions=[raise_it, *why],
        )
    return Resolution(
        verdict=(
            "The task hung. Successful runs finish in about "
            f"{recent / 60:.0f}m against a {budget / 60:.0f}m limit, so the killed run was not "
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


def _trend(older: float, recent: float, growth: float, runs: int) -> str:
    """The runtime history in words that match the sign of the change, or no claim at all.

    Fewer than six successes cannot be split into two disjoint windows, and comparing a slice
    with itself reported a steady runtime for a task that had plainly doubled."""
    if not runs:
        return f"the last successful run took {recent / 60:.0f}m, too few to read a trend from"
    head = f"the last {runs} successful runs took "
    if growth > _FLAT:
        return head + f"{older / 60:.0f}m rising to {recent / 60:.0f}m ({growth:+.0%})"
    if growth < -_FLAT:
        return head + f"{older / 60:.0f}m falling to {recent / 60:.0f}m ({growth:+.0%})"
    return head + f"a steady {recent / 60:.0f}m"


def _runs_until_breach(current: float, growth: float, limit: float, span: int) -> int | None:
    """How many more runs the raised limit survives, at the per-run share of the observed drift."""
    if growth <= _FLAT or current <= 0 or current >= limit:
        return None
    per_run = (1 + growth) ** (1 / span) - 1
    if per_run <= 0:
        return None
    return int(math.log(limit / current) / math.log(1 + per_run))


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
            f"Now: open the model's source and fix the {exc[0]}. dbt's line numbers are "
            "templated, so they point at the wrong line; search for the call in the message.",
            "Then re-run this model alone before the full selector, so a second failure is not "
            "confused with the first.",
            "If the message names a value rather than a bug, the input data changed: check the "
            "upstream table for the same period.",
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
            f"Now: check whether `{name}` exists. If it does, the job's role cannot see it and "
            "the fix is a grant.",
            "If it does not exist, it was renamed or dropped upstream. Find the new name and "
            "update the reference to it.",
            "If nothing replaced it, the read itself is stale: remove it, or restore the object "
            "with its owner.",
        ],
    )


def _auth(diag: dict, text: str, client: object | None) -> Resolution | None:
    """A grant problem and an expired token need opposite actions, and the error says which."""
    # Read identity and permission out of the DENIAL; the INFO preamble names another account.
    deny = re.search(
        r"(AccessDenied|Access Denied|PERMISSION_DENIED|Forbidden|does not have)", text or "", re.I
    )
    scope = (text or "")[deny.start() :] if deny else (text or "")
    principal = _PRINCIPAL.search(scope)
    perm = _PERMISSION.search(scope)
    detail = _DENY_DETAIL.search(text or "")
    expired = _EXPIRED.search(text or "")
    if detail and not perm:
        # The service explained the denial in its own words; that beats a guessed permission name.
        return Resolution(
            verdict=f"The call was refused: {detail.group(1)}",
            evidence="the refusal message the service returned",
            solutions=[
                "Now: grant the identity the access that message names, on the resource it "
                "names. At MNTN that is a Crossplane change, not a console edit.",
                "Then confirm the grant landed before re-running; a retry cannot clear a refusal.",
                "If the grant is already in place, the job is running as a different identity "
                "than you think: check which service account it actually uses.",
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
            f"Now: grant `{what}` to {who} on the resource in the error. At MNTN that is a "
            "Crossplane change, not a console edit.",
            "Then re-run; a retry before the grant lands fails identically.",
            f"If {who} already has it, the binding is on the wrong resource or the wrong project: "
            "check the scope, not the role.",
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
                f"Now: compare the secret this job reads for {where} against that database's "
                "current password.",
                "Then check when the secret last rotated against this task's last successful run. "
                "A rotation between the two is the cause.",
                "If the secret matches, the job is reading a different secret than you think: "
                "check which one the connection actually resolves.",
            ],
        )
    return Resolution(
        verdict=f"The database rejected the password for `{user}`.",
        evidence=f"the server returned an authentication failure for user `{user}`",
        solutions=[
            f"Now: compare the secret the job reads for `{user}` against that database's current "
            "password.",
            "Then, if it rotated, repoint the job at the current secret and re-run. The old one "
            "fails identically every time.",
            f"If the password is right, `{user}` may have been dropped or locked on the server "
            "side: check the account itself.",
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
            f"Now: list what is consuming {metric} in this region. A single idle cluster holding "
            "the headroom looks exactly like a ceiling that is too low (INC-025), and deleting it "
            "is faster than a quota request.",
            f"If nothing is holding it, the ceiling really is too low: raise {metric} for the "
            "region. That is the AUDI-1217 work.",
            f"To unblock this one run without waiting for either, shrink the request below "
            f"{avail:.0f} {metric}.",
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
            "Now: delete any cluster left in ERROR. It still holds quota, so the retry fails on "
            "quota rather than capacity and the real cause gets hidden.",
            f"Then re-run in 1-2 hours. Autozone usually lands outside {zones[0]} on the next "
            "attempt and the job goes green with no change.",
            "Durable fix: configure backup compute clusters in another zone and machine family "
            "so the pipeline fails over instead of retrying into the stockout "
            "(targeting-infra-ml#95 is the pattern). Pinning one other zone only moves it.",
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
            f"Now: check whether {target} exists. If it does, the producer was simply late and "
            "re-running this task is the whole fix.",
            "If they do not, the producer failed or was skipped. Diagnose that task; this one is "
            "correct to have stopped.",
            "Do not widen the sensor window to make this pass. That hides a late producer until "
            "it is late enough to matter.",
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


_ERROR_MARKERS = (
    "Traceback (most recent call last)",
    "PERMISSION_DENIED",
    "AccessDenied",
    "AnalysisException",
    "[error]",
    "ERROR - ",
    "Exception:",
    "Error:",
)
_WINDOW_BEFORE = 4000
_WINDOW_AFTER = 20_000


def error_window(log_text: str, anchor: str = "") -> str:
    """The part of the log around the failure, never the whole file."""
    if not log_text:
        return ""
    at = log_text.rfind(anchor) if anchor else -1
    if at < 0:
        at = max((log_text.rfind(m) for m in _ERROR_MARKERS), default=-1)
    if at < 0:
        return log_text[-_WINDOW_AFTER:]
    return log_text[max(at - _WINDOW_BEFORE, 0) : at + _WINDOW_AFTER]


def resolve(diag: dict, log_text: str = "", client: object | None = None) -> Resolution | None:
    """Settle the signature's open fork, or None when the evidence does not reach a branch."""
    root = diag.get("root_signature") or {}
    fn = RESOLVERS.get(root.get("key") or "")
    if not fn:
        return None
    # Narrow sources first, then only the log around the failure, never the whole file.
    text = "\n".join(
        str(x)
        for x in (
            diag.get("root_error"),
            json.dumps(diag.get("spark") or {}, default=str),
            error_window(log_text, root.get("matched_on") or ""),
        )
        if x
    )
    try:
        return fn(diag, text, client)
    except Exception:
        return None

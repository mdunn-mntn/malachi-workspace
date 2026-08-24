"""Errors that can never be the root cause, only the thing standing in front of it.

INC-025 cost a day because the deepest exception in a replica log was a cleanup handler's own
404, not the quota refusal that triggered the cleanup. A classifier that always trusts the
deepest error will report that mask as the verdict, confidently and wrongly.

Each mask names what it hides and where the real cause lives. A mask must never end a chain
silently: either a resolver reaches the next hop, or the report says out loud that it stopped
on a mask, so "one hop short" surfaces as a known gap instead of a plausible answer.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Mask:
    """A pattern that stands in front of the real error."""

    key: str
    pattern: str
    hides: str
    next_hop: str
    resolver: str | None


MASKS: list[Mask] = [
    Mask(
        "dataproc_cleanup_delete_404",
        r"NotFound: 404 Not found: Cluster projects/[^/]+/regions/[^/]+/clusters/(\S+)",
        "the CreateCluster refusal that left nothing to delete",
        "the ClusterController admin audit log for that cluster name",
        "vertex_rca._cluster_create_error",
    ),
    Mask(
        "slack_notifier_failed",
        r"channel_not_found|SlackApiError|slack_sdk\.errors",
        "the task failure the on-failure callback was trying to announce",
        "the task's own error, above the callback frames",
        None,
    ),
    Mask(
        "dataproc_batch_reattach",
        r"Batch with given id already exists|Attaching to the job",
        "the earlier attempt's failure, which this retry inherited rather than caused",
        "the first attempt's batch, whose driver output holds the original error",
        None,
    ),
]


def detect(text: str | None) -> Mask | None:
    """The mask standing in front of this error, if it is one."""
    if not text:
        return None
    for m in MASKS:
        if re.search(m.pattern, text, re.IGNORECASE):
            return m
    return None


def note(mask: Mask) -> str:
    """The line a report prints when a chain terminates on a mask."""
    return f"This is not the cause: it hides {mask.hides}. Read {mask.next_hop}."

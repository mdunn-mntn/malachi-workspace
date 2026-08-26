"""The slice of the log the resolvers actually read.

An exception is the last thing a log writes. The cap was applied from the front, so on any log
over the limit the resolver read heartbeat lines and never saw the failure. That shipped green:
the suite fed short strings, and a short string has no front to cut.
"""

from __future__ import annotations

import os
import tempfile

from airflow_debugger import orchestrate


def test_a_log_longer_than_the_cap_is_still_resolved() -> None:
    """The exception sits at the end of the log. Capping from the front discards exactly it."""
    log = ("2026-08-24T03:00:00Z [info] airflow.task heartbeat\n" * 200) + (
        "2026-08-24T03:10:00Z [error] AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] "
        "The table or view `prod`.`ml`.`live` cannot be found.\n"
    )
    with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as f:
        f.write(log)
    cap = orchestrate._RESOLVER_MAX_CHARS
    orchestrate._RESOLVER_MAX_CHARS = 2000
    try:
        res = orchestrate.investigate(f.name, use_llm=False, profile_perf=False)
    finally:
        orchestrate._RESOLVER_MAX_CHARS = cap
        os.unlink(f.name)
    assert res["diagnosis"]["resolution"]["verdict"].startswith(
        "The query references `prod.ml.live`"
    )


if __name__ == "__main__":
    for name, fn in sorted(dict(globals()).items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("OK - orchestrator log-slice tests passed")

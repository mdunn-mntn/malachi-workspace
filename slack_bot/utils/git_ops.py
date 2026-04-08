"""Git auto-commit and push for knowledge doc updates."""

import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

WORKSPACE = Path(__file__).resolve().parent.parent.parent


def commit_and_push(date_label: str, items_count: int) -> bool:
    """Stage knowledge/ changes, commit, and push.

    Only touches knowledge/ files — never modifies code or other workspace files.
    Returns True if commit succeeded.
    """
    try:
        # Stage only knowledge/ files
        subprocess.run(
            ["git", "add", "knowledge/"],
            cwd=WORKSPACE,
            check=True,
            capture_output=True,
        )

        # Check if there's anything to commit
        result = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=WORKSPACE,
            capture_output=True,
        )
        if result.returncode == 0:
            logger.info("No knowledge/ changes to commit")
            return False

        msg = f"knowledge: slack extraction {date_label} — {items_count} items added"
        subprocess.run(
            ["git", "commit", "-m", msg],
            cwd=WORKSPACE,
            check=True,
            capture_output=True,
        )
        logger.info("Committed: %s", msg)

        subprocess.run(
            ["git", "push", "origin", "main"],
            cwd=WORKSPACE,
            check=True,
            capture_output=True,
        )
        logger.info("Pushed to origin/main")
        return True

    except subprocess.CalledProcessError as e:
        logger.error("Git operation failed: %s\n%s", e, e.stderr.decode() if e.stderr else "")
        return False

"""Orchestrator — runs the full scrape → extract → update → commit pipeline."""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from scraper import scrape
from extractor import extract
from updater import update_docs
from utils.git_ops import commit_and_push

logger = logging.getLogger(__name__)


def run_pipeline(
    hours_back: int = 24,
    channel_ids: list[str] | None = None,
    test_mode: bool = False,
    skip_commit: bool = False,
):
    """Run the full daily pipeline.

    Args:
        hours_back: Hours of Slack history to fetch.
        channel_ids: Optional channel IDs to restrict to.
        test_mode: If True, scrape 1 hour from first channel only, no commit.
        skip_commit: If True, skip git commit/push step.
    """
    date_label = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if test_mode:
        hours_back = 1
        skip_commit = True
        date_label = f"{date_label}-test"
        logger.info("=== TEST MODE: 1 hour, no commit ===")

    # Step 1: Scrape
    logger.info("Step 1/4: Scraping Slack messages...")
    try:
        raw_path = scrape(
            hours_back=hours_back,
            channel_ids=channel_ids,
            date_override=date_label,
        )
    except Exception as e:
        logger.error("Scrape failed: %s", e)
        sys.exit(1)

    # Step 2: Extract
    logger.info("Step 2/4: Extracting knowledge via LLM...")
    try:
        items = extract(raw_path)
    except Exception as e:
        logger.error("Extraction failed (raw data saved at %s): %s", raw_path, e)
        sys.exit(1)

    if not items:
        logger.info("No knowledge items extracted — done")
        return

    # Step 3: Update docs
    logger.info("Step 3/4: Updating knowledge docs...")
    try:
        result = update_docs(items, date_label=date_label)
    except Exception as e:
        logger.error("Doc update failed: %s", e)
        sys.exit(1)

    logger.info(
        "Updated: %s | Review queue: %d | Skipped: %d | Applied: %d",
        result["updated_files"],
        result["review_queue_count"],
        result["skipped"],
        result["items_applied"],
    )

    # Step 4: Commit and push
    if skip_commit:
        logger.info("Step 4/4: Skipping commit (test mode or --no-commit)")
    else:
        logger.info("Step 4/4: Committing and pushing...")
        commit_and_push(date_label, result["items_applied"])

    logger.info("=== Pipeline complete ===")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Run Slack knowledge extraction pipeline")
    parser.add_argument("--test", action="store_true", help="Test mode: 1 hour, no commit")
    parser.add_argument("--hours", type=int, default=24, help="Hours of history to fetch")
    parser.add_argument("--channels", nargs="*", help="Specific channel IDs")
    parser.add_argument("--no-commit", action="store_true", help="Skip git commit/push")
    args = parser.parse_args()

    run_pipeline(
        hours_back=args.hours,
        channel_ids=args.channels,
        test_mode=args.test,
        skip_commit=args.no_commit,
    )

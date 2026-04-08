"""Daily Slack message scraper.

Fetches messages from all channels the bot belongs to (or a configured subset),
resolves user names, fetches thread replies, and saves structured JSON.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from utils.slack_client import SlackClient

logger = logging.getLogger(__name__)

WORKSPACE = Path(__file__).resolve().parent.parent
CONFIG_PATH = WORKSPACE / "slack_bot" / "config.yaml"
RAW_DIR = WORKSPACE / "knowledge" / "slack_raw"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def scrape(
    hours_back: int = 24,
    channel_ids: list[str] | None = None,
    date_override: str | None = None,
) -> Path:
    """Scrape messages and return path to the saved JSON file.

    Args:
        hours_back: How many hours of history to fetch (default 24).
        channel_ids: Optional list of channel IDs to restrict to.
        date_override: Force a specific date label (YYYY-MM-DD) for the output file.
    """
    config = load_config()
    client = SlackClient()

    now = datetime.now(timezone.utc)
    oldest = (now - timedelta(hours=hours_back)).timestamp()
    latest = now.timestamp()

    date_label = date_override or now.strftime("%Y-%m-%d")
    out_path = RAW_DIR / f"{date_label}.json"

    # Idempotency: skip if already scraped today
    if out_path.exists():
        logger.info("Already scraped for %s — skipping", date_label)
        return out_path

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Determine channels to scrape
    if channel_ids:
        channels = [{"id": cid, "name": cid} for cid in channel_ids]
    else:
        configured = config.get("channels")
        if configured:
            channels = [{"id": ch["id"], "name": ch["name"]} for ch in configured]
        else:
            # Auto-discover: all channels the bot is in
            channels = client.list_bot_channels()

    logger.info(
        "Scraping %d channels for %s (last %dh)", len(channels), date_label, hours_back
    )

    all_channel_data = []

    for ch in channels:
        ch_id, ch_name = ch["id"], ch["name"]
        logger.info("  → %s (%s)", ch_name, ch_id)

        messages = client.fetch_history(ch_id, oldest, latest)
        if not messages:
            continue

        processed = []
        for msg in messages:
            entry = {
                "channel": ch_name,
                "channel_id": ch_id,
                "ts": msg.get("ts"),
                "user": client.resolve_user(msg.get("user", "unknown")),
                "text": msg.get("text", ""),
                "thread_ts": msg.get("thread_ts"),
                "reply_count": msg.get("reply_count", 0),
                "replies": [],
            }

            # Fetch thread replies if any
            if msg.get("reply_count", 0) > 0 and msg.get("thread_ts") == msg.get("ts"):
                raw_replies = client.fetch_replies(ch_id, msg["ts"])
                entry["replies"] = [
                    {
                        "user": client.resolve_user(r.get("user", "unknown")),
                        "text": r.get("text", ""),
                        "ts": r.get("ts"),
                    }
                    for r in raw_replies
                ]

            processed.append(entry)

        all_channel_data.append(
            {"channel": ch_name, "channel_id": ch_id, "messages": processed}
        )

    output = {
        "date": date_label,
        "scraped_at": now.isoformat(),
        "hours_back": hours_back,
        "channels_scraped": len(all_channel_data),
        "channels": all_channel_data,
    }

    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    total_msgs = sum(len(ch["messages"]) for ch in all_channel_data)
    logger.info("Saved %d messages across %d channels → %s", total_msgs, len(all_channel_data), out_path)
    return out_path


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="Scrape Slack messages")
    parser.add_argument("--hours", type=int, default=24, help="Hours of history to fetch")
    parser.add_argument("--channels", nargs="*", help="Specific channel IDs to scrape")
    parser.add_argument("--date", help="Override date label (YYYY-MM-DD)")
    args = parser.parse_args()

    path = scrape(hours_back=args.hours, channel_ids=args.channels, date_override=args.date)
    print(f"Output: {path}")

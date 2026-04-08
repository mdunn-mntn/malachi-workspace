"""Slack API wrapper with rate limiting, pagination, and user resolution."""

import os
import time
import logging

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logger = logging.getLogger(__name__)


class SlackClient:
    """Thin wrapper around slack_sdk.WebClient with rate-limit handling."""

    def __init__(self, token: str | None = None):
        self.client = WebClient(token=token or os.environ["SLACK_BOT_TOKEN"])
        self._user_cache: dict[str, str] = {}

    # -- rate-limit helper --------------------------------------------------

    def _call(self, method: str, **kwargs):
        """Call a Slack API method with automatic retry on rate-limit."""
        func = getattr(self.client, method)
        while True:
            try:
                return func(**kwargs)
            except SlackApiError as e:
                if e.response.status_code == 429:
                    retry_after = int(e.response.headers.get("Retry-After", 5))
                    logger.warning("Rate limited — sleeping %ds", retry_after)
                    time.sleep(retry_after)
                else:
                    raise

    # -- channels -----------------------------------------------------------

    def list_bot_channels(self) -> list[dict]:
        """Return channels the bot has been invited to."""
        channels = []
        cursor = None
        while True:
            kwargs = {"types": "public_channel,private_channel", "limit": 200}
            if cursor:
                kwargs["cursor"] = cursor
            resp = self._call("conversations_list", **kwargs)
            for ch in resp["channels"]:
                if ch.get("is_member"):
                    channels.append({"id": ch["id"], "name": ch["name"]})
            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
        return channels

    # -- history ------------------------------------------------------------

    def fetch_history(
        self, channel_id: str, oldest: float, latest: float
    ) -> list[dict]:
        """Fetch all messages in a channel between oldest and latest (unix ts)."""
        messages = []
        cursor = None
        while True:
            kwargs = {
                "channel": channel_id,
                "oldest": str(oldest),
                "latest": str(latest),
                "limit": 200,
            }
            if cursor:
                kwargs["cursor"] = cursor
            resp = self._call("conversations_history", **kwargs)
            messages.extend(resp.get("messages", []))
            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor or not resp.get("has_more"):
                break
        return messages

    def fetch_replies(self, channel_id: str, thread_ts: str) -> list[dict]:
        """Fetch all replies in a thread (excludes the parent message)."""
        replies = []
        cursor = None
        while True:
            kwargs = {"channel": channel_id, "ts": thread_ts, "limit": 200}
            if cursor:
                kwargs["cursor"] = cursor
            resp = self._call("conversations_replies", **kwargs)
            # First message is the parent — skip it
            batch = resp.get("messages", [])
            if not cursor and batch:
                batch = batch[1:]
            replies.extend(batch)
            cursor = resp.get("response_metadata", {}).get("next_cursor")
            if not cursor or not resp.get("has_more"):
                break
        return replies

    # -- users --------------------------------------------------------------

    def resolve_user(self, user_id: str) -> str:
        """Resolve a user ID to display name (lazy, per-user lookup with cache)."""
        if user_id in self._user_cache:
            return self._user_cache[user_id]
        try:
            resp = self._call("users_info", user=user_id)
            profile = resp["user"].get("profile", {})
            name = (
                profile.get("display_name")
                or profile.get("real_name")
                or resp["user"].get("name", user_id)
            )
            self._user_cache[user_id] = name
            return name
        except SlackApiError:
            self._user_cache[user_id] = user_id
            return user_id

"""Post OLC webhook activity to Slack.

Two transports, whichever is configured — an incoming webhook wins because it
needs no scopes:

    OLC_SLACK_WEBHOOK_URL                  https://hooks.slack.com/services/...
    SLACK_BOT_TOKEN + OLC_SLACK_CHANNEL    chat.postMessage (channel id, e.g. C0AJ...)

Silent no-op when neither is set. Never raises: OLC retries any non-2xx
response, so a Slack outage must not turn an already-processed webhook into a
redelivery.
"""
from __future__ import annotations

import logging
import os

import requests

log = logging.getLogger(__name__)

TIMEOUT = 5          # OLC is waiting on our response; do not dawdle
MAX_CHARS = 3500     # Slack hard-caps a text block at 3000-4000


def configured() -> str | None:
    """Which transport is live — for the health probe."""
    if os.getenv("OLC_SLACK_WEBHOOK_URL"):
        return "incoming-webhook"
    if os.getenv("SLACK_BOT_TOKEN") and os.getenv("OLC_SLACK_CHANNEL"):
        return "bot-token"
    return None


def post(text: str) -> bool:
    """Best-effort Slack post. Returns True only on a confirmed success."""
    text = text[:MAX_CHARS]
    try:
        url = os.getenv("OLC_SLACK_WEBHOOK_URL")
        if url:
            r = requests.post(url, json={"text": text}, timeout=TIMEOUT)
            return r.status_code == 200

        token = os.getenv("SLACK_BOT_TOKEN")
        channel = os.getenv("OLC_SLACK_CHANNEL")
        if not (token and channel):
            return False

        r = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}"},
            json={"channel": channel, "text": text, "unfurl_links": False},
            timeout=TIMEOUT,
        )
        body = r.json() if r.content else {}
        if not body.get("ok"):
            # `channel_not_found` here almost always means the bot was never
            # invited to the channel, not that the id is wrong.
            log.warning("slack chat.postMessage failed: %s", body.get("error"))
            return False
        return True
    except Exception as e:                      # noqa: BLE001 - never propagate
        log.warning("slack post failed: %s", e)
        return False

"""
RetailFlow Pipeline — Alerting & Notification Utility
======================================================

Sends pipeline state alerts to a Slack or Discord webhook.
Reads ``PIPELINE_WEBHOOK_URL`` from the environment.

Supports both Discord embed format and Slack-compatible JSON payloads
(webhook URL is auto-detected by pattern).

Usage:
    from scripts.alerts import send_pipeline_alert

    send_pipeline_alert(
        status="warning",
        stage="ingestion",
        details={"Loaded Rows": 1000, "Rejected Rows": 5},
    )
"""

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# ── Colour palette ──────────────────────────────────────────────────────
# Discord embed colours (hex → decimal). Slack uses hex strings via
# ``attachments[].color`` or the ``"#xxxxxx"`` string in text fallback.
_COLORS: Dict[str, int] = {
    "success": 0x36A64F,
    "warning": 0xFFA500,
    "critical": 0xFF0000,
}

_STATUS_EMOJI: Dict[str, str] = {
    "success": "\u2705",
    "warning": "\u26A0\uFE0F",
    "critical": "\uD83D\uDCA5",
}

_STATUS_LABEL: Dict[str, str] = {
    "success": "Success",
    "warning": "Warning",
    "critical": "Critical",
}


# ── Helpers ─────────────────────────────────────────────────────────────

def _get_webhook_url() -> str:
    """Return the configured webhook URL, or empty string if not set."""
    url = os.getenv("PIPELINE_WEBHOOK_URL", "").strip()
    if not url:
        logger.warning("Webhook not configured, skipping live alert.")
    return url


def _is_discord_webhook(url: str) -> bool:
    """Detect Discord webhook URLs by domain."""
    return "discord.com" in url or "discordapp.com" in url


def _field_block(name: str, value: str, inline: bool = True) -> Dict[str, Any]:
    """Build a single embed field (Discord) or Slack attachment field."""
    return {"name": name, "value": value, "inline": inline}


# ── Payload builders ────────────────────────────────────────────────────

def _build_slack_payload(
    status: str, stage: str, fields: list
) -> Dict[str, Any]:
    """Slack-compatible payload using the attachment pattern.

    Falls back to a simple ``text`` field so the message is always visible
    even if the blocks aren't rendered.
    """
    emoji = _STATUS_EMOJI.get(status, "")
    label = _STATUS_LABEL.get(status, "Unknown")
    color_hex = "#{:06x}".format(_COLORS.get(status, 0x808080))

    text = f"{emoji}  RetailFlow Pipeline — {label}  |  Stage: {stage}"

    attachment = {
        "color": color_hex,
        "title": text,
        "fields": fields,
        "footer": "RetailFlow Pipeline",
        "ts": int(datetime.now(timezone.utc).timestamp()),
    }

    return {"text": text, "attachments": [attachment]}


def _build_discord_payload(
    status: str, stage: str, fields: list
) -> Dict[str, Any]:
    """Discord embed payload."""
    emoji = _STATUS_EMOJI.get(status, "")
    label = _STATUS_LABEL.get(status, "Unknown")
    color = _COLORS.get(status, 0x808080)

    embed = {
        "title": f"{emoji}  RetailFlow Pipeline — {label}",
        "color": color,
        "fields": fields,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return {"embeds": [embed]}


# ── Public API ──────────────────────────────────────────────────────────

def send_pipeline_alert(
    status: str,
    stage: str,
    details: Optional[Dict[str, Any]] = None,
) -> bool:
    """Send a pipeline state alert to the configured webhook.

    Args:
        status: ``"success"``, ``"warning"``, or ``"critical"``.
        stage:  Pipeline stage name (e.g. ``"ingestion"``, ``"dbt-test"``).
        details:  Optional dict of extra key-value pairs for embed fields.

    Returns:
        ``True`` if the alert was sent successfully, ``False`` if the webhook
        is not configured or the request failed.
    """
    url = _get_webhook_url()
    if not url:
        return False

    fields = [_field_block("Stage", stage)]

    if details:
        for key, value in details.items():
            fields.append(_field_block(key, str(value)))

    payload = (
        _build_discord_payload(status, stage, fields)
        if _is_discord_webhook(url)
        else _build_slack_payload(status, stage, fields)
    )

    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    }

    try:
        req = urllib.request.Request(
            url, data=data, headers=headers, method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info(
                "Alert sent (status=%s, stage=%s) — HTTP %d",
                status,
                stage,
                resp.status,
            )
        return True
    except urllib.error.URLError as exc:
        logger.error("Failed to send alert: %s", exc)
        return False

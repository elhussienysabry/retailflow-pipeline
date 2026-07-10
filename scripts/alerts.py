"""
RetailFlow Pipeline — Alerting & Notification Utility
======================================================

Sends pipeline state alerts to a Slack or Discord webhook.
Reads ``PIPELINE_WEBHOOK_URL`` from the environment.

Supports both Discord embed format and Slack Block Kit payloads
(webhook URL is auto-detected by pattern).  Uses the ``requests``
library for robust HTTP transport with proper timeouts and error
handling.

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
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

# ── Colour palette ──────────────────────────────────────────────────────
# Discord embed colours (hex → decimal).  Slack uses hex strings in
# the Block Kit ``"color"`` field of section blocks.
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

_REQUEST_TIMEOUT = 15  # seconds


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


def _build_fields_list(
    status: str, stage: str, details: Optional[Dict[str, Any]] = None
) -> list:
    """Build a shared list of field dicts used by both Slack and Discord."""
    fields: list = []
    if stage:
        fields.append({"name": "Stage", "value": stage, "inline": True})
    if details:
        for key, value in details.items():
            fields.append({"name": key, "value": str(value), "inline": True})
    return fields


# ── Slack Block Kit payload builder ─────────────────────────────────────


def _build_slack_payload(
    status: str, stage: str, details: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Modern Slack Block Kit payload.

    Uses a header block, context block, and section blocks with
    an optional side ``color`` indicator via ``"type": "section"``
    accessory fields.
    """
    emoji = _STATUS_EMOJI.get(status, "")
    label = _STATUS_LABEL.get(status, "Unknown")
    color_hex = "#{:06x}".format(_COLORS.get(status, 0x808080))

    blocks: list[Dict[str, Any]] = []

    # ── Header ──────────────────────────────────────────────────────
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"{emoji}  RetailFlow Pipeline — {label}",
            "emoji": True,
        },
    })

    # ── Context (stage & timestamp) ──────────────────────────────────
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": (
                    f"*Stage:* {stage}   |   *Timestamp:* "
                    f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
                ),
            },
        ],
    })

    # ── Divider ─────────────────────────────────────────────────────
    blocks.append({"type": "divider"})

    # ── Detail fields ────────────────────────────────────────────────
    if details:
        field_chunks: list[Dict[str, str]] = []
        for key, value in details.items():
            field_chunks.append({
                "type": "mrkdwn",
                "text": f"*{key}:*\n{value}",
            })
        # Slack blocks support up to 10 fields per section.
        for i in range(0, len(field_chunks), 10):
            chunk = field_chunks[i:i + 10]
            blocks.append({
                "type": "section",
                "fields": chunk,
            })

    # ── Footer ──────────────────────────────────────────────────────
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": (
                    "RetailFlow Pipeline  •  "
                    "<https://github.com/elhussienysabry/retailflow-pipeline|GitHub>"
                ),
            },
        ],
    })

    return {
        "text": f"{emoji} RetailFlow Pipeline — {label} — Stage: {stage}",
        "attachments": [
            {
                "color": color_hex,
                "blocks": blocks,
            }
        ],
    }


# ── Discord embed payload builder ───────────────────────────────────────


def _build_discord_payload(
    status: str, stage: str, details: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Discord embed payload with modern markdown styling."""
    emoji = _STATUS_EMOJI.get(status, "")
    label = _STATUS_LABEL.get(status, "Unknown")
    color = _COLORS.get(status, 0x808080)

    fields = _build_fields_list(status, stage, details)

    embed = {
        "title": f"{emoji}  RetailFlow Pipeline — {label}",
        "color": color,
        "fields": fields,
        "footer": {"text": "RetailFlow Pipeline"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return {"embeds": [embed]}


# ── Core HTTP sender ────────────────────────────────────────────────────


def _post_webhook(
    payload: Dict[str, Any],
    url: str,
    label: str = "alert",
) -> bool:
    """POST a JSON payload to a webhook URL with proper error handling.

    Args:
        payload:   The JSON-serialisable dict to send.
        url:       The webhook endpoint.
        label:     Human-readable label for log messages.

    Returns:
        ``True`` if the POST succeeded (HTTP 2xx), ``False`` otherwise.
    """
    logger.debug("Sending %s to %s", label, url)

    try:
        resp = requests.post(
            url,
            json=payload,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            },
            timeout=_REQUEST_TIMEOUT,
        )
    except requests.ConnectionError as exc:
        logger.error(
            "Failed to send %s — connection error: %s", label, exc
        )
        return False
    except requests.Timeout as exc:
        logger.error(
            "Failed to send %s — request timed out (%ss): %s",
            label,
            _REQUEST_TIMEOUT,
            exc,
        )
        return False
    except requests.RequestException as exc:
        logger.error(
            "Failed to send %s — unexpected request error: %s", label, exc
        )
        return False

    # Log the response body for debugging webhook failures.
    if not resp.ok:
        logger.error(
            "%s rejected — HTTP %s: %s",
            label,
            resp.status_code,
            resp.text[:500],
        )
        return False

    logger.info(
        "%s delivered — HTTP %s",
        label,
        resp.status_code,
    )
    return True


# ── dbt test result alert ───────────────────────────────────────────────


def parse_dbt_test_results(
    run_results_path: str,
) -> Dict[str, Any]:
    """Read ``run_results.json`` and return a summary of failed/errored tests.

    Args:
        run_results_path: Absolute path to ``dbt/target/run_results.json``.

    Returns:
        Dict with keys:
            - ``total`` — total test count.
            - ``failed`` — list of dicts (unique_id, status, execution_time, message).
            - ``errored`` — list of dicts (unique_id, status, execution_time, message).
    """
    try:
        with open(run_results_path, encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.warning("Could not read run_results.json: %s", exc)
        return {"total": 0, "failed": [], "errored": []}

    results = data.get("results", [])
    failed: list = []
    errored: list = []

    for r in results:
        uid = r.get("unique_id", "unknown")
        status = r.get("status", "")
        if status in ("fail", "error"):
            entry = {
                "unique_id": uid,
                "status": status,
                "execution_time": round(r.get("execution_time", 0), 3),
                "message": r.get("message", ""),
            }
            if status == "fail":
                failed.append(entry)
            else:
                errored.append(entry)

    summary = {
        "total": len(results),
        "failed": failed,
        "errored": errored,
    }
    logger.info(
        "dbt test results: %d total, %d failed, %d errored",
        summary["total"],
        len(failed),
        len(errored),
    )
    return summary


def send_dbt_test_alert(
    run_results_path: str,
    exit_code: int,
) -> bool:
    """Send a rich alert for dbt data quality SLA breaches.

    Parses ``run_results.json`` and builds a Discord embed / Slack Block Kit
    listing every failed or errored test with its unique ID, status, and
    execution time.

    Args:
        run_results_path: Absolute path to ``dbt/target/run_results.json``.
        exit_code: The dbt process exit code (included in the alert).

    Returns:
        ``True`` if the alert was sent, ``False`` otherwise.
    """
    url = _get_webhook_url()
    if not url:
        return False

    summary = parse_dbt_test_results(run_results_path)
    total = summary["total"]
    all_bad = summary["failed"] + summary["errored"]

    if not all_bad:
        logger.info("No dbt test failures to alert on.")
        return False

    stage_label = "dbt-test"
    details: Dict[str, Any] = {
        "Stage": stage_label,
        "Exit Code": str(exit_code),
        "Tests Run": str(total),
        "Failed": str(len(summary["failed"])),
        "Errored": str(len(summary["errored"])),
    }

    # List up to 5 failed tests inline.
    for i, t in enumerate(all_bad[:5]):
        uid_short = t["unique_id"].split(".")[-1]
        details[f"\u274C {t['status'].upper()} \u2014 {uid_short}"] = (
            t["message"] or f"execution_time={t['execution_time']}s"
        )

    if len(all_bad) > 5:
        details["... and more"] = (
            f"{len(all_bad) - 5} additional failure(s) \u2014 "
            "check run_results.json for full details."
        )

    payload = (
        _build_discord_payload("critical", stage_label, details)
        if _is_discord_webhook(url)
        else _build_slack_payload("critical", stage_label, details)
    )

    return _post_webhook(payload, url, label="dbt-test-alert")


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

    payload = (
        _build_discord_payload(status, stage, details)
        if _is_discord_webhook(url)
        else _build_slack_payload(status, stage, details)
    )

    return _post_webhook(payload, url, label=f"pipeline-alert/{stage}")

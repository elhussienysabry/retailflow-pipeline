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

CLI test mode:
    python scripts/alerts.py --test-webhook
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# ── Colour palette ──────────────────────────────────────────────────────
_COLORS: Dict[str, int] = {
    "success": 0x36A64F,
    "warning": 0xFFA500,
    "critical": 0xFF0000,
}

_STATUS_EMOJI: Dict[str, str] = {
    "success": "\u2705",
    "warning": "\u26a0\ufe0f",
    "critical": "\ud83d\udca5",
}

_STATUS_LABEL: Dict[str, str] = {
    "success": "Success",
    "warning": "Warning",
    "critical": "Critical",
}

_REQUEST_TIMEOUT = 15  # seconds


# ── Helpers ─────────────────────────────────────────────────────────────


_ENV_LOADED = False


def _ensure_env() -> None:
    """Load ``.env`` once so ``PIPELINE_WEBHOOK_URL`` is picked up."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
    _ENV_LOADED = True


def _get_webhook_url() -> str:
    _ensure_env()
    url = os.getenv("PIPELINE_WEBHOOK_URL", "").strip()
    if not url:
        logger.warning("Webhook not configured, skipping live alert.")
    return url


def _is_discord_webhook(url: str) -> bool:
    return "discord.com" in url or "discordapp.com" in url


# ── Payload builders ────────────────────────────────────────────────────


def _build_discord_payload(
    status: str,
    stage: str,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a Discord embed payload.

    Falls back to a plain ``content`` message if ``details`` is empty
    so there is always visible text in the channel.
    """
    emoji = _STATUS_EMOJI.get(status, "")
    label = _STATUS_LABEL.get(status, "Unknown")
    color = _COLORS.get(status, 0x808080)

    fields: list = []
    if details:
        for key, value in details.items():
            fields.append({"name": str(key), "value": str(value), "inline": True})

    embed: Dict[str, Any] = {
        "title": f"{emoji}  RetailFlow Pipeline — {label}",
        "color": color,
        "footer": {"text": "RetailFlow Pipeline"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if fields:
        embed["fields"] = fields
    if stage:
        embed["description"] = f"**Stage:** {stage}"

    return {"embeds": [embed]}


def _build_discord_fallback_text(status: str, stage: str) -> Dict[str, Any]:
    """Send a simple text message if the embed format is rejected."""
    emoji = _STATUS_EMOJI.get(status, "")
    label = _STATUS_LABEL.get(status, "Unknown")
    text = f"{emoji} **RetailFlow Pipeline — {label}** | Stage: `{stage}`"
    return {"content": text}


def _build_slack_payload(
    status: str,
    stage: str,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Modern Slack Block Kit payload."""
    emoji = _STATUS_EMOJI.get(status, "")
    label = _STATUS_LABEL.get(status, "Unknown")
    color_hex = "#{:06x}".format(_COLORS.get(status, 0x808080))

    blocks: list[Dict[str, Any]] = []

    blocks.append(
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji}  RetailFlow Pipeline — {label}",
                "emoji": True,
            },
        }
    )

    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"*Stage:* {stage}   |   *Timestamp:* "
                        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    ),
                }
            ],
        }
    )

    blocks.append({"type": "divider"})

    if details:
        field_chunks: list[Dict[str, str]] = []
        for key, value in details.items():
            field_chunks.append(
                {
                    "type": "mrkdwn",
                    "text": f"*{key}:*\n{value}",
                }
            )
        for i in range(0, len(field_chunks), 10):
            chunk = field_chunks[i : i + 10]
            blocks.append({"type": "section", "fields": chunk})

    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        "RetailFlow Pipeline  •  "
                        "<https://github.com/elhussienysabry/retailflow-pipeline|GitHub>"
                    ),
                }
            ],
        }
    )

    return {
        "text": f"{emoji} RetailFlow Pipeline — {label} — Stage: {stage}",
        "attachments": [{"color": color_hex, "blocks": blocks}],
    }


# ── Core HTTP sender with embed → plain-text fallback ───────────────────


def _post_webhook(
    payload: Dict[str, Any],
    url: str,
    label: str = "alert",
) -> bool:
    """POST a JSON payload to a webhook.

    If the rich payload is rejected with HTTP 4xx, tries a fallback
    plain-text message so something always appears in the channel.

    Returns:
        ``True`` on success (HTTP 2xx), ``False`` otherwise.
    """
    logger.debug("Sending %s to %s", label, url)

    def _do_post(body: Dict[str, Any]) -> requests.Response:
        session = requests.Session()
        # Respect system proxy env vars (HTTP_PROXY, HTTPS_PROXY, NO_PROXY)
        session.trust_env = True
        return session.post(
            url,
            json=body,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            },
            timeout=_REQUEST_TIMEOUT,
        )

    try:
        resp = _do_post(payload)
    except requests.ConnectionError as exc:
        logger.error("Failed to send %s — connection error: %s", label, exc)
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
            "Failed to send %s — unexpected request error: %s",
            label,
            exc,
        )
        return False

    # ── Success ─────────────────────────────────────────────────────
    if resp.ok:
        logger.info("%s delivered — HTTP %s", label, resp.status_code)
        return True

    # ── HTTP error: log response body for debugging ──────────────────
    logger.error(
        "%s rejected — HTTP %s — response: %s",
        label,
        resp.status_code,
        resp.text[:500],
    )

    # ── Fallback: try a plain text message for Discord ───────────────
    if _is_discord_webhook(url) and "embeds" in payload:
        logger.info("Embed rejected — retrying with plain-text fallback ...")
        fallback = _build_discord_fallback_text("critical", "webhook-test")
        try:
            resp2 = _do_post(fallback)
        except requests.RequestException:
            return False
        if resp2.ok:
            logger.info(
                "Fallback plain-text message delivered — HTTP %s", resp2.status_code
            )
            return True
        logger.error(
            "Fallback also rejected — HTTP %s: %s",
            resp2.status_code,
            resp2.text[:500],
        )

    return False


# ── dbt test result alert ───────────────────────────────────────────────


def parse_dbt_test_results(run_results_path: str) -> Dict[str, Any]:
    """Read ``run_results.json`` and return a summary of failed/errored tests."""
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

    summary = {"total": len(results), "failed": failed, "errored": errored}
    logger.info(
        "dbt test results: %d total, %d failed, %d errored",
        summary["total"],
        len(failed),
        len(errored),
    )
    return summary


def send_dbt_test_alert(run_results_path: str, exit_code: int) -> bool:
    """Send a rich alert for dbt data quality SLA breaches."""
    url = _get_webhook_url()
    if not url:
        return False

    summary = parse_dbt_test_results(run_results_path)
    all_bad = summary["failed"] + summary["errored"]

    if not all_bad:
        logger.info("No dbt test failures to alert on.")
        return False

    details: Dict[str, Any] = {
        "Stage": "dbt-test",
        "Exit Code": str(exit_code),
        "Tests Run": str(summary["total"]),
        "Failed": str(len(summary["failed"])),
        "Errored": str(len(summary["errored"])),
    }

    for t in all_bad[:5]:
        uid_short = t["unique_id"].split(".")[-1]
        details[f"\u274c {t['status'].upper()} \u2014 {uid_short}"] = (
            t["message"] or f"execution_time={t['execution_time']}s"
        )

    if len(all_bad) > 5:
        details["... and more"] = (
            f"{len(all_bad) - 5} additional failure(s) \u2014 "
            "check run_results.json for full details."
        )

    payload = (
        _build_discord_payload("critical", "dbt-test", details)
        if _is_discord_webhook(url)
        else _build_slack_payload("critical", "dbt-test", details)
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
        status:  ``"success"``, ``"warning"``, or ``"critical"``.
        stage:   Pipeline stage name.
        details: Optional dict of extra key-value pairs.

    Returns:
        ``True`` if sent, ``False`` if webhook not configured or failed.
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


# ── CLI test mode ───────────────────────────────────────────────────────


def _test_webhook() -> int:
    """Send a test payload to the configured webhook and print the result.

    Usage:
        python scripts/alerts.py --test-webhook
    """
    url = _get_webhook_url()
    if not url:
        print("\n  [ERROR] PIPELINE_WEBHOOK_URL is not set in .env or environment.\n")
        return 1

    print(f"\n  Webhook URL: {url}")
    print(f"  Destination: {'Discord' if _is_discord_webhook(url) else 'Slack'}")
    print("  Testing embed payload ...")

    payload = _build_discord_payload("success", "webhook-test", {"Test": "OK"})
    ok = _post_webhook(payload, url, label="test")
    if ok:
        print("  [OK]  Test payload delivered successfully!\n")
        return 0

    print(
        "  [FAIL]  Rich payload failed. Fallback already attempted by _post_webhook.\n"
    )
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="RetailFlow Alerting Utility")
    parser.add_argument(
        "--test-webhook",
        action="store_true",
        help="Send a test message to the configured webhook URL",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if args.test_webhook:
        return _test_webhook()

    print("No action specified. Use --test-webhook to verify your webhook setup.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

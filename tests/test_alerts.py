"""
Tests for the alerts/notifications module.

Verifies payload builders, webhook detection, dbt test result parsing,
and core dispatch logic using mocked HTTP sessions.
"""

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.alerts import (  # noqa: E402
    _build_discord_payload,
    _build_discord_fallback_text,
    _build_slack_payload,
    _is_discord_webhook,
    _post_webhook,
    _get_webhook_url,
    parse_dbt_test_results,
    send_pipeline_alert,
    _COLORS,
    _STATUS_EMOJI,
    _STATUS_LABEL,
)


class TestConstants:
    def test_colors_have_expected_keys(self) -> None:
        assert "success" in _COLORS
        assert "warning" in _COLORS
        assert "critical" in _COLORS

    def test_status_emoji_have_expected_keys(self) -> None:
        assert "success" in _STATUS_EMOJI
        assert "warning" in _STATUS_EMOJI
        assert "critical" in _STATUS_EMOJI

    def test_status_label_have_expected_keys(self) -> None:
        assert "success" in _STATUS_LABEL
        assert "warning" in _STATUS_LABEL
        assert "critical" in _STATUS_LABEL


class TestIsDiscordWebhook:
    def test_discord_com(self) -> None:
        assert _is_discord_webhook("https://discord.com/api/webhooks/xxx")

    def test_discordapp_com(self) -> None:
        assert _is_discord_webhook("https://discordapp.com/api/webhooks/xxx")

    def test_slack_webhook(self) -> None:
        assert not _is_discord_webhook("https://hooks.slack.com/services/xxx")

    def test_empty_url(self) -> None:
        assert not _is_discord_webhook("")


class TestBuildDiscordPayload:
    def test_creates_embed_with_title_and_color(self) -> None:
        payload = _build_discord_payload("success", "ingestion")
        assert "embeds" in payload
        embed = payload["embeds"][0]
        assert "RetailFlow Pipeline" in embed["title"]
        assert embed["color"] == _COLORS["success"]

    def test_includes_stage_in_description(self) -> None:
        payload = _build_discord_payload("warning", "dbt-test")
        embed = payload["embeds"][0]
        assert "dbt-test" in embed["description"]

    def test_includes_details_as_fields(self) -> None:
        payload = _build_discord_payload(
            "critical", "schema-drift",
            {"Entity": "customers", "Severity": "CRITICAL"},
        )
        embed = payload["embeds"][0]
        assert "fields" in embed
        field_names = [f["name"] for f in embed["fields"]]
        assert "Entity" in field_names
        assert "Severity" in field_names

    def test_no_details_omits_fields(self) -> None:
        payload = _build_discord_payload("success", "test")
        embed = payload["embeds"][0]
        assert "fields" not in embed

    def test_unknown_status_uses_grey(self) -> None:
        payload = _build_discord_payload("unknown", "test")
        embed = payload["embeds"][0]
        assert embed["color"] == 0x808080


class TestBuildDiscordFallbackText:
    def test_creates_content_string(self) -> None:
        payload = _build_discord_fallback_text("critical", "test-stage")
        assert "content" in payload
        assert "test-stage" in payload["content"]


class TestBuildSlackPayload:
    def test_creates_header_with_emoji(self) -> None:
        payload = _build_slack_payload("success", "export")
        assert "attachments" in payload
        attachment = payload["attachments"][0]
        blocks = attachment["blocks"]
        assert any("header" in str(b.get("type", "")) for b in blocks)

    def test_slack_color_is_hex_string(self) -> None:
        payload = _build_slack_payload("warning", "test")
        attachment = payload["attachments"][0]
        assert attachment["color"].startswith("#")

    def test_includes_details_in_fields(self) -> None:
        payload = _build_slack_payload(
            "critical", "schema-drift",
            {"Rows": "1000", "Rejected": "5"},
        )
        attachment = payload["attachments"][0]
        blocks = attachment["blocks"]
        combined = json.dumps(blocks)
        assert "Rows" in combined
        assert "Rejected" in combined

    def test_fallback_text_present(self) -> None:
        payload = _build_slack_payload("success", "test")
        assert "text" in payload


class TestGetWebhookUrl:
    @patch("scripts.alerts.os.getenv", return_value="https://hook.example.com")
    def test_returns_url_when_set(self, mock_getenv: MagicMock) -> None:
        url = _get_webhook_url()
        assert url == "https://hook.example.com"

    @patch("scripts.alerts.os.getenv", return_value="")
    def test_returns_empty_when_not_set(self, mock_getenv: MagicMock) -> None:
        url = _get_webhook_url()
        assert url == ""


class TestPostWebhook:
    @patch("scripts.alerts.requests.Session.post")
    def test_successful_post_returns_true(self, mock_post: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        result = _post_webhook(
            {"content": "test"}, "https://hook.example.com", label="test"
        )
        assert result is True

    @patch("scripts.alerts.requests.Session.post")
    def test_failed_post_returns_false(self, mock_post: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.ok = False
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        mock_post.return_value = mock_response

        result = _post_webhook(
            {"content": "test"}, "https://hook.example.com", label="test"
        )
        assert result is False

    @patch("scripts.alerts.requests.Session.post", side_effect=requests.exceptions.ConnectionError)
    def test_connection_error_returns_false(
        self, mock_post: MagicMock
    ) -> None:
        result = _post_webhook(
            {"content": "test"}, "https://hook.example.com", label="test"
        )
        assert result is False

    @patch("scripts.alerts.requests.Session.post", side_effect=requests.exceptions.Timeout)
    def test_timeout_returns_false(self, mock_post: MagicMock) -> None:
        result = _post_webhook(
            {"content": "test"}, "https://hook.example.com", label="test"
        )
        assert result is False

    @patch("scripts.alerts._build_discord_fallback_text")
    @patch("scripts.alerts.requests.Session.post")
    def test_discord_fallback_on_4xx(
        self, mock_post: MagicMock, mock_fallback: MagicMock
    ) -> None:
        first_response = MagicMock()
        first_response.ok = False
        first_response.status_code = 400
        first_response.text = "Bad Request"
        fallback_response = MagicMock()
        fallback_response.ok = True
        fallback_response.status_code = 200
        mock_post.side_effect = [first_response, fallback_response]
        mock_fallback.return_value = {"content": "fallback"}

        result = _post_webhook(
            {"embeds": [{"title": "test"}]},
            "https://discord.com/api/webhooks/xxx",
            label="test",
        )
        assert result is True
        assert mock_post.call_count == 2


class TestSendPipelineAlert:
    @patch("scripts.alerts._get_webhook_url", return_value="")
    def test_no_webhook_returns_false(self, mock_url: MagicMock) -> None:
        result = send_pipeline_alert("success", "test")
        assert result is False

    @patch("scripts.alerts._get_webhook_url", return_value="https://discord.com/api/webhooks/xxx")
    @patch("scripts.alerts._post_webhook", return_value=True)
    def test_discord_payload_built_and_sent(
        self, mock_post: MagicMock, mock_url: MagicMock
    ) -> None:
        result = send_pipeline_alert(
            "success", "ingestion", {"Rows": "1000"}
        )
        assert result is True
        assert mock_post.called

    @patch("scripts.alerts._get_webhook_url", return_value="https://hooks.slack.com/services/xxx")
    @patch("scripts.alerts._post_webhook", return_value=True)
    def test_slack_payload_built_and_sent(
        self, mock_post: MagicMock, mock_url: MagicMock
    ) -> None:
        result = send_pipeline_alert(
            "warning", "dbt-test", {"Failed": "3"}
        )
        assert result is True
        assert mock_post.called


class TestParseDbtTestResults:
    def test_empty_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            results_file = Path(tmpdir) / "run_results.json"
            results_file.write_text('{"results": []}')
            summary = parse_dbt_test_results(str(results_file))
            assert summary["total"] == 0
            assert summary["failed"] == []
            assert summary["errored"] == []

    def test_missing_file_returns_default(self) -> None:
        summary = parse_dbt_test_results("/nonexistent/run_results.json")
        assert summary["total"] == 0

    def test_parses_failed_and_errored(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            results_file = Path(tmpdir) / "run_results.json"
            data = {
                "results": [
                    {
                        "unique_id": "test.fail.1",
                        "status": "fail",
                        "execution_time": 1.5,
                        "message": "assert failure",
                    },
                    {
                        "unique_id": "test.error.1",
                        "status": "error",
                        "execution_time": 0.5,
                        "message": "syntax error",
                    },
                    {
                        "unique_id": "test.pass.1",
                        "status": "pass",
                        "execution_time": 0.2,
                    },
                ]
            }
            results_file.write_text(json.dumps(data))
            summary = parse_dbt_test_results(str(results_file))
            assert summary["total"] == 3
            assert len(summary["failed"]) == 1
            assert len(summary["errored"]) == 1
            assert summary["failed"][0]["unique_id"] == "test.fail.1"
            assert summary["errored"][0]["unique_id"] == "test.error.1"

    def test_invalid_json_returns_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            results_file = Path(tmpdir) / "run_results.json"
            results_file.write_text("not json")
            summary = parse_dbt_test_results(str(results_file))
            assert summary["total"] == 0

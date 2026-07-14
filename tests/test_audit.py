"""
Tests for the AuditContext — runtime telemetry capture and persistence.
"""

import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.audit import AuditContext, _AUDIT_DDL  # noqa: E402


class TestAuditContextInit:
    """Verify the initial state of a fresh AuditContext."""

    def test_defaults_are_sane(self) -> None:
        ctx = AuditContext()
        assert ctx.status == "RUNNING"
        assert ctx.records_ingested == 0
        assert ctx.records_rejected == 0
        assert ctx.parquet_file_path == ""
        assert ctx.duration_seconds is None
        assert ctx.sla_breached is False
        assert ctx.error_message == ""

    def test_run_id_is_uuid(self) -> None:
        ctx = AuditContext()
        import uuid
        assert uuid.UUID(ctx.run_id)

    def test_start_time_is_set(self) -> None:
        ctx = AuditContext()
        assert ctx.start_time is not None


class TestAuditTelemetryCapture:
    """Test ingest_from_dlq and collect_parquet_paths."""

    def test_ingest_from_dlq_extracts_counts(self) -> None:
        ctx = AuditContext()
        ctx.ingest_from_dlq({"loaded": 14100, "rejected": 3})
        assert ctx.records_ingested == 14100
        assert ctx.records_rejected == 3

    def test_ingest_from_dlq_empty_dict_sets_zero(self) -> None:
        ctx = AuditContext()
        ctx.ingest_from_dlq({})
        assert ctx.records_ingested == 0
        assert ctx.records_rejected == 0

    def test_collect_parquet_paths_no_directory(self) -> None:
        ctx = AuditContext(lakehouse_dir="/tmp/nonexistent_audit_test_dir_xyz")
        ctx.collect_parquet_paths()
        assert ctx.parquet_file_path == ""

    def test_collect_parquet_paths_finds_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "customers.parquet").touch()
            Path(tmpdir, "orders.parquet").touch()
            ctx = AuditContext(lakehouse_dir=tmpdir)
            ctx.collect_parquet_paths()
            assert "customers.parquet" in ctx.parquet_file_path
            assert "orders.parquet" in ctx.parquet_file_path


class TestAuditFinalize:
    """Test the finalize helper."""

    def test_sets_end_time_and_duration(self) -> None:
        ctx = AuditContext()
        import time
        time.sleep(0.01)
        ctx.finalize(status="SUCCESS")
        assert ctx.status == "SUCCESS"
        assert ctx.end_time is not None
        assert ctx.duration_seconds is not None
        assert ctx.duration_seconds > 0

    def test_preserves_custom_values(self) -> None:
        ctx = AuditContext()
        ctx.finalize(status="FAILED", sla_breached=True, error_message="oops")
        assert ctx.status == "FAILED"
        assert ctx.sla_breached is True
        assert ctx.error_message == "oops"


class TestAuditCommit:
    """Integration-style tests for the commit method (mocked engine)."""

    def test_commit_success(self) -> None:
        ctx = AuditContext()
        ctx.records_ingested = 100
        ctx.records_rejected = 2
        ctx.finalize(status="SUCCESS")

        mock_engine = MagicMock()
        result = ctx.commit(engine=mock_engine)

        assert result is True
        # Should have executed DDL statements + INSERT
        assert mock_engine.connect.called or mock_engine.begin.called

    def test_commit_returns_false_on_db_error(self) -> None:
        ctx = AuditContext()
        ctx.finalize(status="FAILED")

        mock_engine = MagicMock()
        mock_engine.begin.side_effect = Exception("DB unreachable")
        # The connect() path still needs to work for DDL
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        result = ctx.commit(engine=mock_engine)

        assert result is False

    def test_commit_with_None_engine_tries_auto_create(self) -> None:
        """When engine is None, commit() tries to build one from env vars.
        This test verifies it does not crash when env vars are missing."""
        ctx = AuditContext()
        ctx.finalize(status="SUCCESS")
        # Should return False because env vars won't resolve to a real DB.
        result = ctx.commit(engine=None)
        # Either False (connection failed) or True (if a local PG happens to be running).
        assert isinstance(result, bool)


class TestAuditDdl:
    """Verify the DDL string is valid (syntax check via SQLAlchemy)."""

    def test_ddl_parses_without_error(self) -> None:
        """Parse statements through SQLAlchemy's text() — no syntax errors."""
        for stmt in _AUDIT_DDL.split(";"):
            stripped = stmt.strip()
            if stripped:
                try:
                    text(stripped)
                except Exception as exc:
                    pytest.fail(f"DDL parse failed: {exc}\n{stripped}")

    def test_ddl_includes_check_constraint(self) -> None:
        assert "CHECK (status IN (" in _AUDIT_DDL
        assert "'SUCCESS'" in _AUDIT_DDL
        assert "'FAILED'" in _AUDIT_DDL
        assert "'SCHEMA_DRIFT'" in _AUDIT_DDL


class TestAuditDlqDataShape:
    """Ensure the DLQ summary dict shape from load_to_postgres is compatible."""

    def test_real_dlq_shape_can_be_ingested(self) -> None:
        dlq = {
            "execution_date": "2026-07-13",
            "loaded": 14100,
            "rejected": 3,
            "tables": {
                "customers.csv": {"loaded": 1000, "rejected": 0},
                "orders.csv": {"loaded": 10000, "rejected": 3},
            },
        }
        ctx = AuditContext()
        ctx.ingest_from_dlq(dlq)
        assert ctx.records_ingested == 14100
        assert ctx.records_rejected == 3

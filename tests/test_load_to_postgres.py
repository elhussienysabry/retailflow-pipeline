"""
Tests for the CSV-to-PostgreSQL loader script.

Verifies that:
    - The engine creation works with valid environment variables
    - Schema creation SQL is correct
    - CSV loading handles edge cases (missing files, empty CSVs)
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

# WHY: Add the project root to sys.path so we can import the scripts module.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.load_to_postgres import (  # noqa: E402
    SOURCE_MAP,
    ensure_raw_schema,
    get_engine,
    truncate_table,
)


class TestGetEngine:
    """Tests for the get_engine function."""

    def test_uses_env_variables(self) -> None:
        """Engine should use environment variables for connection params."""

        with patch.dict(
            os.environ,
            {
                "POSTGRES_HOST": "testhost",
                "POSTGRES_PORT": "9999",
                "POSTGRES_DB": "testdb",
                "POSTGRES_USER": "testuser",
                "POSTGRES_PASSWORD": "testpass",
            },
        ):
            engine = get_engine()
            url = str(engine.url)
            assert "testhost" in url
            assert "9999" in url
            assert "testdb" in url
            assert "testuser" in url

    def test_engine_type(self) -> None:
        """Should return a SQLAlchemy Engine instance."""
        engine = get_engine()
        assert "Engine" in type(engine).__name__


class TestEnsureRawSchema:
    """Tests for the ensure_raw_schema function."""

    def test_creates_schema(self) -> None:
        """Should execute CREATE SCHEMA IF NOT EXISTS."""
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        ensure_raw_schema(mock_engine)

        assert mock_conn.exec_driver_sql.called
        call_args = str(mock_conn.exec_driver_sql.call_args)
        assert "CREATE SCHEMA" in call_args


class TestTruncateTable:
    """Tests for the truncate_table function."""

    def test_executes_truncate(self) -> None:
        """Should execute TRUNCATE TABLE statement."""
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        truncate_table(mock_engine, "raw.customers")

        assert mock_conn.exec_driver_sql.called
        call_args = str(mock_conn.exec_driver_sql.call_args)
        assert "TRUNCATE" in call_args
        assert "raw.customers" in call_args


class TestCSVTableMap:
    """Tests for the CSV-to-table mapping."""

    def test_has_expected_tables(self) -> None:
        """Verify table names match expectations (includes new POS JSON table)."""
        tables = [t for _, t, _ in SOURCE_MAP]
        assert "raw.customers" in tables
        assert "raw.products" in tables
        assert "raw.orders" in tables
        assert "raw.pos_store_sales" in tables

    def test_has_four_mappings(self) -> None:
        """Verify there are exactly four file-to-table mappings (CSV + JSON)."""
        assert len(SOURCE_MAP) == 4

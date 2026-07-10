"""
Tests for the project status script.

Verifies that each check function returns the expected (status, message) tuples
under various conditions using mocked external dependencies.
"""

import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.project_status import (  # noqa: E402
    check_docker,
    check_postgres,
    check_env_file,
    check_raw_csvs,
    check_database_rows,
    _determine_overall,
    main,
)


class TestCheckDocker:
    """Tests for the check_docker function."""

    @patch("scripts.project_status._run_cmd")
    def test_docker_running(self, mock_run_cmd: MagicMock) -> None:
        mock_run_cmd.return_value = MagicMock(returncode=0)
        status, msg = check_docker()
        assert status == "OK"
        assert "running" in msg.lower()

    @patch("scripts.project_status._run_cmd")
    def test_docker_not_running(self, mock_run_cmd: MagicMock) -> None:
        mock_run_cmd.return_value = MagicMock(returncode=1)
        status, msg = check_docker()
        assert status == "FAIL"
        assert "not running" in msg.lower()

    @patch("scripts.project_status._run_cmd", side_effect=FileNotFoundError)
    def test_docker_not_installed(self, mock_run_cmd: MagicMock) -> None:
        status, msg = check_docker()
        assert status == "FAIL"
        assert "not found" in msg.lower()

    @patch("scripts.project_status._run_cmd", side_effect=TimeoutError)
    def test_docker_timeout(self, mock_run_cmd: MagicMock) -> None:
        status, msg = check_docker()
        assert status == "FAIL"


class TestCheckPostgres:
    """Tests for the check_postgres function."""

    @patch("scripts.project_status._run_cmd")
    @patch("scripts.project_status.create_engine")
    def test_container_running_and_reachable(
        self, mock_create_engine: MagicMock, mock_run_cmd: MagicMock
    ) -> None:
        mock_run_cmd.return_value = MagicMock(
            stdout="retailflow-db\n", returncode=0
        )
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine

        status, msg = check_postgres()
        assert status == "OK"
        assert "reachable" in msg.lower()

    @patch("scripts.project_status._run_cmd")
    def test_container_not_running(self, mock_run_cmd: MagicMock) -> None:
        mock_run_cmd.return_value = MagicMock(stdout="", returncode=0)
        status, msg = check_postgres()
        assert status == "FAIL"
        assert "not running" in msg.lower()

    @patch("scripts.project_status._run_cmd")
    @patch("scripts.project_status.create_engine")
    def test_container_running_but_not_reachable(
        self, mock_create_engine: MagicMock, mock_run_cmd: MagicMock
    ) -> None:
        mock_run_cmd.return_value = MagicMock(
            stdout="retailflow-db\n", returncode=0
        )
        mock_create_engine.side_effect = Exception("Connection refused")

        status, msg = check_postgres()
        assert status == "WARNING"
        assert "not reachable" in msg.lower()


class TestCheckEnvFile:
    """Tests for the check_env_file function."""

    def test_env_file_exists_and_loads(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_file = Path(tmpdir) / ".env"
            env_file.write_text("TEST_VAR=hello\n")
            with patch(
                "scripts.project_status.ENV_FILE", env_file
            ), patch("scripts.project_status.load_dotenv", return_value=True):
                status, msg = check_env_file()
                assert status == "OK"
                assert "loaded" in msg.lower()

    def test_env_file_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            missing = Path(tmpdir) / ".env"
            with patch("scripts.project_status.ENV_FILE", missing):
                status, msg = check_env_file()
                assert status == "FAIL"
                assert "not found" in msg.lower()


class TestCheckRawCsvs:
    """Tests for the check_raw_csvs function."""

    def test_all_csvs_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            for f in ["customers.csv", "products.csv", "orders.csv"]:
                (Path(tmpdir) / f).write_text("col1,col2\n")
            with patch("scripts.project_status.DATA_RAW", Path(tmpdir)):
                status, msg = check_raw_csvs()
                assert status == "OK"
                assert "present" in msg.lower()

    def test_missing_csvs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "customers.csv").write_text("col1,col2\n")
            with patch("scripts.project_status.DATA_RAW", Path(tmpdir)):
                status, msg = check_raw_csvs()
                assert status == "FAIL"
                assert "missing" in msg.lower()
                assert "products.csv" in msg
                assert "orders.csv" in msg

    def test_all_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.project_status.DATA_RAW", Path(tmpdir)):
                status, msg = check_raw_csvs()
                assert status == "FAIL"
                assert "customers.csv" in msg


class TestCheckDatabaseRows:
    """Tests for the check_database_rows function."""

    @patch("scripts.project_status.create_engine")
    def test_all_tables_have_rows(self, mock_create_engine: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        mock_result = MagicMock()
        mock_result.scalar.return_value = 100
        mock_conn.execute.return_value = mock_result

        mock_create_engine.return_value = mock_engine

        status, msg = check_database_rows()
        assert status == "OK"
        assert "data" in msg.lower()

    @patch("scripts.project_status.create_engine")
    def test_empty_tables(self, mock_create_engine: MagicMock) -> None:
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        def side_effect(*args, **kwargs):
            mock_result = MagicMock()
            mock_result.scalar.return_value = 0
            return mock_result

        mock_conn.execute.side_effect = side_effect
        mock_create_engine.return_value = mock_engine

        status, msg = check_database_rows()
        assert status == "WARNING"
        assert "empty" in msg.lower()

    @patch("scripts.project_status.create_engine")
    def test_connection_error(self, mock_create_engine: MagicMock) -> None:
        mock_create_engine.side_effect = Exception("connection error")
        status, msg = check_database_rows()
        assert status == "WARNING"
        assert "could not check" in msg.lower()


class TestDetermineOverall:
    """Tests for the _determine_overall function."""

    def test_all_ok(self) -> None:
        statuses = ["OK", "OK", "OK"]
        assert _determine_overall(statuses) == "OK"

    def test_any_fail(self) -> None:
        statuses = ["OK", "FAIL", "OK"]
        assert _determine_overall(statuses) == "FAIL"

    def test_warning_only(self) -> None:
        statuses = ["OK", "WARNING", "OK"]
        assert _determine_overall(statuses) == "WARNING"

    def test_warning_and_fail(self) -> None:
        statuses = ["WARNING", "FAIL"]
        assert _determine_overall(statuses) == "FAIL"


class TestMain:
    """Integration tests for the main function."""

    @patch("scripts.project_status.check_docker")
    @patch("scripts.project_status.check_postgres")
    @patch("scripts.project_status.check_env_file")
    @patch("scripts.project_status.check_raw_csvs")
    @patch("scripts.project_status.check_database_rows")
    def test_healthy_path(
        self,
        mock_rows: MagicMock,
        mock_csvs: MagicMock,
        mock_env: MagicMock,
        mock_pg: MagicMock,
        mock_docker: MagicMock,
    ) -> None:
        mock_docker.return_value = ("OK", "Docker running")
        mock_pg.return_value = ("OK", "PostgreSQL reachable")
        mock_env.return_value = ("OK", ".env loaded")
        mock_csvs.return_value = ("OK", "CSVs present")
        mock_rows.return_value = ("OK", "All tables have data")

        with patch("scripts.project_status.sys.exit") as mock_exit:
            main()
            mock_exit.assert_called_once_with(0)

    @patch("scripts.project_status.check_docker")
    @patch("scripts.project_status.check_postgres")
    @patch("scripts.project_status.check_env_file")
    @patch("scripts.project_status.check_raw_csvs")
    def test_docker_failure_skips_pg(
        self,
        mock_csvs: MagicMock,
        mock_env: MagicMock,
        mock_pg: MagicMock,
        mock_docker: MagicMock,
    ) -> None:
        mock_docker.return_value = ("FAIL", "Docker not running")
        mock_env.return_value = ("OK", ".env loaded")
        mock_csvs.return_value = ("OK", "CSVs present")

        with patch("scripts.project_status.sys.exit") as mock_exit:
            main()
            mock_exit.assert_called_once_with(2)
            mock_pg.assert_not_called()

    @patch("scripts.project_status.check_docker")
    @patch("scripts.project_status.check_postgres")
    @patch("scripts.project_status.check_env_file")
    @patch("scripts.project_status.check_raw_csvs")
    @patch("scripts.project_status.check_database_rows")
    def test_degraded_path(
        self,
        mock_rows: MagicMock,
        mock_csvs: MagicMock,
        mock_env: MagicMock,
        mock_pg: MagicMock,
        mock_docker: MagicMock,
    ) -> None:
        mock_docker.return_value = ("OK", "Docker running")
        mock_pg.return_value = ("OK", "PostgreSQL reachable")
        mock_env.return_value = ("OK", ".env loaded")
        mock_csvs.return_value = ("WARNING", "Some CSVs missing")
        mock_rows.return_value = ("OK", "All tables have data")

        with patch("scripts.project_status.sys.exit") as mock_exit:
            main()
            mock_exit.assert_called_once_with(1)

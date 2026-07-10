"""
RetailFlow Pipeline — End-to-End Status Check
==============================================

Performs a quick health check of the entire RetailFlow Pipeline:

    - Is Docker Desktop running?
    - Is the PostgreSQL container running and reachable?
    - Does the .env file exist and load correctly?
    - Are all raw CSV files present?
    - Does each raw PostgreSQL table have data?

Usage:
    python scripts/project_status.py

Exit codes:
    0 — OK (healthy)
    1 — WARNING (degraded)
    2 — FAIL (unhealthy)
"""

import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
ENV_FILE = PROJECT_ROOT / ".env"

REQUIRED_CSVS: List[str] = ["customers.csv", "products.csv", "orders.csv"]
REQUIRED_TABLES: List[str] = ["raw.customers", "raw.products", "raw.orders"]

FIX_HINTS: dict = {}


def _run_cmd(cmd: List[str], timeout: int = 10) -> subprocess.CompletedProcess:
    """Run a shell command and return the result."""
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def check_docker() -> Tuple[str, str]:
    """Check whether Docker Desktop is running.

    Returns:
        (status, message) where status is "OK", "WARNING", or "FAIL".
    """
    try:
        result = _run_cmd(["docker", "info"])
        if result.returncode == 0:
            return "OK", "Docker Desktop is running"
        return "FAIL", "Docker Desktop is not running"
    except FileNotFoundError:
        return "FAIL", "Docker command not found — is Docker installed?"
    except subprocess.TimeoutExpired:
        return "FAIL", "Docker info timed out — Docker may be starting"
    except Exception as exc:
        return "FAIL", f"Docker check error: {exc}"


def check_postgres() -> Tuple[str, str]:
    """Check whether the PostgreSQL container is running and reachable.

    Returns:
        (status, message).
    """
    try:
        result = _run_cmd(
            [
                "docker",
                "ps",
                "--filter",
                "name=retailflow-db",
                "--format",
                "{{.Names}}",
            ]
        )
        if "retailflow-db" not in result.stdout:
            return (
                "FAIL",
                "PostgreSQL container (retailflow-db) is not running",
            )
    except FileNotFoundError:
        return "FAIL", "Docker command not found"
    except subprocess.TimeoutExpired:
        return "FAIL", "Docker ps timed out"
    except Exception as exc:
        return "FAIL", f"Container check error: {exc}"

    try:
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "retailflow")
        user = os.getenv("POSTGRES_USER", "retailflow_user")
        password = os.getenv("POSTGRES_PASSWORD", "retailflow_pass")

        conn_str = (
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        )
        engine = create_engine(conn_str, connect_args={"connect_timeout": 5})
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        return "OK", "PostgreSQL container is running and reachable"

    except Exception as exc:
        return (
            "WARNING",
            f"Container is running but database is not reachable: {exc}",
        )


def check_env_file() -> Tuple[str, str]:
    """Check whether .env exists and can be loaded.

    Returns:
        (status, message).
    """
    if not ENV_FILE.exists():
        return (
            "FAIL",
            ".env file not found at project root. Copy .env.example to .env",
        )

    try:
        loaded = load_dotenv(ENV_FILE, override=True)
        if loaded:
            return "OK", ".env file exists and loaded successfully"
        return "WARNING", ".env file found but no variables could be loaded"
    except Exception as exc:
        return "FAIL", f"Failed to load .env file: {exc}"


def check_raw_csvs() -> Tuple[str, str]:
    """Check whether all required raw CSV files exist in data/raw/.

    Returns:
        (status, message).
    """
    missing = [f for f in REQUIRED_CSVS if not (DATA_RAW / f).exists()]
    if not missing:
        return "OK", "All required CSV files present in data/raw/"

    files = ", ".join(missing)
    return (
        "FAIL",
        f"Missing CSV files in data/raw/: {files}. "
        f"Run 'python scripts/generate_fake_data.py'",
    )


def check_database_rows() -> Tuple[str, str]:
    """Check whether each raw PostgreSQL table contains rows.

    Returns:
        (status, message).
    """
    try:
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "retailflow")
        user = os.getenv("POSTGRES_USER", "retailflow_user")
        password = os.getenv("POSTGRES_PASSWORD", "retailflow_pass")

        conn_str = (
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        )
        engine = create_engine(conn_str, connect_args={"connect_timeout": 5})

        empty_tables: List[str] = []
        with engine.connect() as conn:
            for table in REQUIRED_TABLES:
                try:
                    count = conn.execute(
                        text(f"SELECT COUNT(*) FROM {table}")
                    ).scalar()
                    if count == 0:
                        empty_tables.append(table)
                except Exception:
                    empty_tables.append(f"{table} (table not found)")

        if not empty_tables:
            return "OK", "All raw tables have data"

        tables = ", ".join(empty_tables)
        return (
            "WARNING",
            f"Tables missing or empty: {tables}. "
            f"Run 'python scripts/load_to_postgres.py'",
        )

    except Exception as exc:
        return "WARNING", f"Could not check row counts: {exc}"


def _determine_overall(statuses: List[str]) -> str:
    """Aggregate check statuses into a single overall status.

    Args:
        statuses: List of status strings ("OK", "WARNING", "FAIL").

    Returns:
        "OK", "WARNING", or "FAIL".
    """
    if any(s == "FAIL" for s in statuses):
        return "FAIL"
    if any(s == "WARNING" for s in statuses):
        return "WARNING"
    return "OK"


def _print_report(checks: List[Tuple[str, Tuple[str, str]]]) -> None:
    """Print the formatted status report to stdout."""
    overall = _determine_overall([s for _, (s, _) in checks])

    print("\nRetailFlow Pipeline Status Report")
    print("-" * 33)

    for name, (status, msg) in checks:
        print(f"{name}: {status} - {msg}")

    print("-" * 33)

    if overall == "OK":
        print("Overall Status: Healthy")
        logger.info("All checks passed — pipeline is healthy.")
    elif overall == "WARNING":
        print("Overall Status: Degraded")
        logger.warning("Some checks have warnings — pipeline may be partially operational.")
    else:
        print("Overall Status: Unhealthy")
        logger.error("One or more critical checks failed — pipeline is not operational.")

    failed = [(name, status) for name, (status, _) in checks if status in ("FAIL", "WARNING")]
    if failed:
        _print_fix_hints(failed)

    print()


def _print_fix_hints(failed: List[Tuple[str, str]]) -> None:
    """Print beginner-friendly fix hints for failed or warning checks.

    Args:
        failed: List of (check_name, status) tuples for failing checks.
    """
    hints: dict = {
        "Docker": (
            "Start Docker Desktop from the Start Menu or system tray, "
            "then wait a moment and re-run this script."
        ),
        "PostgreSQL": (
            "Start the container: 'docker compose up -d' from the project root."
        ),
        ".env File": (
            "Copy the template:\n"
            "  Windows:  copy .env.example .env\n"
            "  Git Bash: cp .env.example .env"
        ),
        "Raw CSV Files": (
            "Run: .venv\\Scripts\\python scripts\\generate_fake_data.py"
        ),
        "Database Row Counts": (
            "Run: .venv\\Scripts\\python scripts\\load_to_postgres.py"
        ),
    }

    print("\nFix Hints:")
    for name, _ in failed:
        hint = hints.get(name)
        if hint:
            print(f"  - {hint}")


def main() -> None:
    """Run all health checks and print a status report."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("Starting RetailFlow Pipeline status check...")

    docker_status = check_docker()
    env_status = check_env_file()
    csv_status = check_raw_csvs()

    if docker_status[0] == "FAIL":
        pg_status = ("FAIL", "Skipped — Docker not available")
        row_status = ("WARNING", "Skipped — PostgreSQL not reachable")
    else:
        pg_status = check_postgres()
        row_status = (
            ("WARNING", "Skipped — PostgreSQL not reachable")
            if pg_status[0] != "OK"
            else check_database_rows()
        )

    checks: List[Tuple[str, Tuple[str, str]]] = [
        ("Docker", docker_status),
        ("PostgreSQL", pg_status),
        (".env File", env_status),
        ("Raw CSV Files", csv_status),
        ("Database Row Counts", row_status),
    ]

    overall = _determine_overall([s for _, (s, _) in checks])
    _print_report(checks)

    sys.exit(0 if overall == "OK" else 2 if overall == "FAIL" else 1)


if __name__ == "__main__":
    main()

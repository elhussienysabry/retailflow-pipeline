"""
RetailFlow Pipeline — Orchestrator
===================================

Executes the end-to-end data pipeline as a sequential DAG:

    Generate Data ──> Load to Postgres ──> dbt Run
    ──> dbt Test ──> Excel Export

Each step runs in its correct virtual environment (.venv or .venv-dbt).
If any step fails, the pipeline halts immediately (circuit breaker).
"""

import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

logger = logging.getLogger("orchestrate")

PROJECT_ROOT = Path(__file__).resolve().parent.parent

STEP_HEADER = """
{sep}
  {emoji}  STEP {num}/{total} — {name}
{sep}"""

STEP_OK = "  OK  {name} completed in {elapsed:.2f}s"
STEP_FAIL = "  FAIL  {name} FAILED (exit code {code})"


def _py_exe() -> str:
    """Return the path to the main .venv Python executable."""
    return str(PROJECT_ROOT / ".venv" / "Scripts" / "python.exe")


def _dbt_exe() -> str:
    """Return the path to the isolated dbt executable."""
    return str(PROJECT_ROOT / ".venv-dbt" / "Scripts" / "dbt.exe")


def _step_box(num: int, total: int, name: str) -> str:
    width = 68
    sep = "-" * width
    icons = ["[DATA]", "[LOAD]", "[DBT]", "[TEST]", "[EXCEL]"]
    icon = icons[num - 1] if num <= len(icons) else "[...]"
    return STEP_HEADER.format(
        sep=sep, emoji=icon, num=num, total=total, name=name
    )


def _run_command(cmd: List[str], cwd: Path = None, label: str = "") -> int:
    """Execute a subprocess and stream output in real time.

    Args:
        cmd: Command list (e.g. [python.exe, "script.py", "--flag"]).
        cwd: Working directory (None = PROJECT_ROOT).
        label: Human-readable label for log messages.

    Returns:
        subprocess return code (0 = success).
    """
    cwd = cwd or PROJECT_ROOT
    logger.info("Running: %s", " ".join(str(c) for c in cmd))
    logger.info("  Working directory: %s", cwd)

    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
    )

    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()

    proc.wait()
    return proc.returncode


def step_generate_data(profile: str = "medium") -> int:
    """Step 1: Generate synthetic CSV data via Faker."""
    total = _run_command(
        [_py_exe(), str(PROJECT_ROOT / "scripts" / "generate_fake_data.py"),
         "--profile", profile],
        label="generate-fake-data",
    )
    return total


def step_load_to_postgres() -> int:
    """Step 2: Load CSV files into PostgreSQL raw schema."""
    return _run_command(
        [_py_exe(), str(PROJECT_ROOT / "scripts" / "load_to_postgres.py")],
        label="load-to-postgres",
    )


def step_dbt_run() -> int:
    """Step 3: Execute dbt models (staging -> intermediate -> marts)."""
    dbt = _dbt_exe()
    dbt_dir = PROJECT_ROOT / "dbt"

    for model_group in ("staging", "intermediate", "marts"):
        logger.info("--- dbt run --select %s ---", model_group)
        rc = _run_command(
            [dbt, "run", "--select", model_group],
            cwd=dbt_dir,
            label=f"dbt-run-{model_group}",
        )
        if rc != 0:
            return rc
    return 0


def step_dbt_test() -> int:
    """Step 4: Run dbt data quality tests."""
    return _run_command(
        [_dbt_exe(), "test"],
        cwd=PROJECT_ROOT / "dbt",
        label="dbt-test",
    )


def step_excel_export() -> int:
    """Step 5: Export analytics to a styled Excel workbook."""
    return _run_command(
        [_py_exe(), "-m", "src.exports.excel_exporter"],
        label="excel-export",
    )


PIPELINE_STEPS: List[Tuple[str, callable]] = [
    ("Generate Data", step_generate_data),
    ("Load to PostgreSQL", step_load_to_postgres),
    ("dbt Run", step_dbt_run),
    ("dbt Test", step_dbt_test),
    ("Excel Export", step_excel_export),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="RetailFlow Pipeline — Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/orchestrate.py\n"
            "  python scripts/orchestrate.py --profile small\n"
            "  python scripts/orchestrate.py --profile large\n"
        ),
    )
    parser.add_argument(
        "--profile",
        type=str,
        default="medium",
        choices=["small", "medium", "large"],
        help="Scale profile passed to the data generator (default: medium)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    total_steps = len(PIPELINE_STEPS)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    pipeline_start = time.monotonic()

    print()
    print("+" + "=" * 70 + "+")
    print("|  RETAILFLOW PIPELINE ORCHESTRATOR")
    print("|  Profile: {:<51s}|".format(args.profile))
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print("|  Started: {:<49s}|".format(now_utc))
    print("+" + "=" * 70 + "+")
    print()

    for step_num, (step_name, step_fn) in enumerate(PIPELINE_STEPS, start=1):
        print(_step_box(step_num, total_steps, step_name))
        step_start = time.monotonic()

        try:
            if step_name == "Generate Data":
                rc = step_fn(profile=args.profile)
            else:
                rc = step_fn()
        except Exception as exc:
            logger.critical(
                "Unhandled exception in step '%s': %s",
                step_name,
                exc,
                exc_info=True,
            )
            rc = -1

        elapsed = time.monotonic() - step_start

        if rc == 0:
            logger.info(STEP_OK.format(name=step_name, elapsed=elapsed))
        else:
            logger.critical(STEP_FAIL.format(name=step_name, code=rc))
            print()
            print("+" + "=" * 70 + "+")
            print("|  FAIL  PIPELINE HALTED")
            print("|  Step '%s' failed with exit code %d." % (step_name, rc))
            print("|  Subsequent steps were skipped.")
            print("+" + "=" * 70 + "+")
            sys.exit(1)

        print()

    total_elapsed = time.monotonic() - pipeline_start
    print()
    print("+" + "=" * 70 + "+")
    print("|  OK  PIPELINE COMPLETE")
    msg = "|  All %d steps succeeded in %.2f seconds." % (
        total_steps, total_elapsed
    )
    print(msg)
    print("+" + "=" * 70 + "+")
    print()


if __name__ == "__main__":
    main()

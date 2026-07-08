"""
RetailFlow Pipeline — Orchestrator
===================================

Executes the end-to-end data pipeline as a sequential DAG:

    Generate Data ──> Load to Postgres ──> dbt Run
    ──> dbt Test ──> Excel Export ──> dbt Docs Generate

Each step runs in its correct virtual environment (.venv or .venv-dbt).
If any step fails, the pipeline halts immediately (circuit breaker).

Alerts are dispatched to a Slack/Discord webhook at key states:
    - Ingestion phase: warning alert when rows are sent to the DLQ
    - dbt test failure: critical alert before circuit breaker exit
    - Pipeline completion: success summary alert
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Tuple

logger = logging.getLogger("orchestrate")

PROJECT_ROOT = Path(__file__).resolve().parent.parent

STEP_HEADER = """
{sep}
  {emoji}  STEP {num}/{total} — {name}
{sep}"""

STEP_OK = "  OK  {name} completed in {elapsed:.2f}s"
STEP_FAIL = "  FAIL  {name} FAILED (exit code {code})"

# ── Alerting ────────────────────────────────────────────────────────────
# Graceful import: try the package path first, then the sibling path.
_ALERTS_AVAILABLE = False
send_pipeline_alert = None  # type: ignore
try:
    from scripts.alerts import send_pipeline_alert  # noqa: E402
    _ALERTS_AVAILABLE = True
except ImportError:
    try:
        from alerts import send_pipeline_alert  # noqa: E402, F811
        _ALERTS_AVAILABLE = True
    except ImportError:
        pass

if not _ALERTS_AVAILABLE:
    logger.info("scripts.alerts not available — alerts disabled.")

# Store the last parsed DLQ data so alert functions can reference it.
_last_dlq_data: Dict[str, Any] = {}


class CommandResult(NamedTuple):
    """Result of a subprocess execution with captured output lines."""

    returncode: int
    output: List[str]


def _py_exe() -> str:
    """Return the path to the Python executable.

    On Windows: use the project-local .venv.
    On Linux (container): use the system Python (``sys.executable``).
    """
    if sys.platform == "win32":
        return str(PROJECT_ROOT / ".venv" / "Scripts" / "python.exe")
    return sys.executable


def _dbt_exe() -> str:
    """Return the path to the dbt executable.

    Resolution order:
        1. ``DBT_EXECUTABLE`` env var (for container override).
        2. Platform-relative path under ``.venv-dbt/``.
    """
    env_dbt = os.getenv("DBT_EXECUTABLE")
    if env_dbt:
        return env_dbt
    if sys.platform == "win32":
        return str(PROJECT_ROOT / ".venv-dbt" / "Scripts" / "dbt.exe")
    return "dbt"


def _step_box(num: int, total: int, name: str) -> str:
    width = 68
    sep = "-" * width
    icons = ["[DATA]", "[LOAD]", "[DBT]", "[TEST]", "[EXCEL]", "[DOCS]"]
    icon = icons[num - 1] if num <= len(icons) else "[...]"
    return STEP_HEADER.format(
        sep=sep, emoji=icon, num=num, total=total, name=name
    )


def _run_command(
    cmd: List[str], cwd: Path = None, label: str = ""
) -> CommandResult:
    """Execute a subprocess, stream output in real time, and capture lines.

    Args:
        cmd: Command list (e.g. [python.exe, "script.py", "--flag"]).
        cwd: Working directory (None = PROJECT_ROOT).
        label: Human-readable label for log messages.

    Returns:
        CommandResult with return code and captured output lines.
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

    output_lines: List[str] = []
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        output_lines.append(line.rstrip("\n"))

    proc.wait()
    return CommandResult(proc.returncode, output_lines)


# ---------------------------------------------------------------------------
# DLQ summary parsing & alerting
# ---------------------------------------------------------------------------


def _parse_dlq_data(output_lines: List[str]) -> Dict[str, Any]:
    """Extract DLQ summary dict from captured subprocess output."""
    for line in output_lines:
        if line.startswith("DLQ_SUMMARY:"):
            try:
                return json.loads(line[len("DLQ_SUMMARY:"):])
            except (json.JSONDecodeError, KeyError):
                logger.warning("Could not parse DLQ summary: %s", line)
            break
    return {}


def _log_dlq_summary(output_lines: List[str]) -> None:
    """Search captured output for a ``DLQ_SUMMARY:`` JSON line, log it,
    and stash the parsed data for the alerting hook."""
    global _last_dlq_data
    data = _parse_dlq_data(output_lines)
    if not data:
        return

    _last_dlq_data = data
    loaded = data.get("loaded", 0)
    rejected = data.get("rejected", 0)

    logger.info(
        "DLQ Ingestion Summary — loaded=%d clean, "
        "rejected=%d to Dead Letter Queue",
        loaded,
        rejected,
    )
    tables = data.get("tables", {})
    for csv_name, counts in tables.items():
        logger.info(
            "  %s → loaded=%d  rejected=%d",
            csv_name,
            counts.get("loaded", 0),
            counts.get("rejected", 0),
        )


def _send_ingestion_alert() -> None:
    """Dispatch a warning-level alert if any rows were sent to the DLQ
    during the ingestion phase."""
    if not _ALERTS_AVAILABLE:
        return
    rejected = _last_dlq_data.get("rejected", 0)
    if rejected <= 0:
        return

    loaded = _last_dlq_data.get("loaded", 0)
    total = loaded + rejected
    pct = (rejected / total * 100) if total > 0 else 0.0

    send_pipeline_alert(
        status="warning",
        stage="ingestion",
        details={
            "Loaded Rows": loaded,
            "Rejected Rows": rejected,
            "Rejection Rate": f"{pct:.1f}%",
        },
    )


def _send_step_failure_alert(step_name: str, rc: int) -> None:
    """Dispatch a critical alert when a step fails and the circuit breaker
    halts the pipeline.  dbt test failures get a specific message."""
    if not _ALERTS_AVAILABLE:
        return

    if step_name == "dbt Test":
        send_pipeline_alert(
            status="critical",
            stage="dbt-test",
            details={
                "Error": (
                    "\U0001F4A5 Pipeline Broken: "
                    "dbt Data Quality Tests Failed at Stage: dbt Test"
                ),
                "Exit Code": rc,
            },
        )
    else:
        send_pipeline_alert(
            status="critical",
            stage=step_name.lower().replace(" ", "-"),
            details={
                "Error": "Pipeline halted by circuit breaker",
                "Exit Code": rc,
            },
        )


def _send_success_alert(total_elapsed: float) -> None:
    """Dispatch a success alert at the end of the pipeline."""
    if not _ALERTS_AVAILABLE:
        return

    details: Dict[str, Any] = {
        "Total Steps": len(PIPELINE_STEPS),
        "Duration": f"{total_elapsed:.2f}s",
    }

    rejected = _last_dlq_data.get("rejected", 0)
    if rejected is not None and rejected > 0:
        details["DLQ Rejected"] = rejected

    send_pipeline_alert(
        status="success",
        stage="pipeline-complete",
        details=details,
    )


# ---------------------------------------------------------------------------
# Pipeline step implementations
# ---------------------------------------------------------------------------


def step_generate_data(profile: str = "medium") -> int:
    """Step 1: Generate synthetic CSV data via Faker."""
    result = _run_command(
        [
            _py_exe(),
            str(PROJECT_ROOT / "scripts" / "generate_fake_data.py"),
            "--profile",
            profile,
        ],
        label="generate-fake-data",
    )
    return result.returncode


def step_load_to_postgres() -> int:
    """Step 2: Load CSV files into PostgreSQL raw schema with DLQ guardrail."""
    result = _run_command(
        [_py_exe(), str(PROJECT_ROOT / "scripts" / "load_to_postgres.py")],
        label="load-to-postgres",
    )
    _log_dlq_summary(result.output)
    return result.returncode


def step_dbt_run() -> int:
    """Step 3: Execute dbt models (staging -> intermediate -> marts)."""
    dbt = _dbt_exe()
    dbt_dir = PROJECT_ROOT / "dbt"

    for model_group in ("staging", "intermediate", "marts"):
        logger.info("--- dbt run --select %s ---", model_group)
        result = _run_command(
            [dbt, "run", "--select", model_group],
            cwd=dbt_dir,
            label=f"dbt-run-{model_group}",
        )
        if result.returncode != 0:
            return result.returncode
    return 0


def step_dbt_test() -> int:
    """Step 4: Run dbt data quality tests."""
    result = _run_command(
        [_dbt_exe(), "test"],
        cwd=PROJECT_ROOT / "dbt",
        label="dbt-test",
    )
    return result.returncode


def step_excel_export() -> int:
    """Step 5: Export analytics to a styled Excel workbook."""
    result = _run_command(
        [_py_exe(), "-m", "src.exports.excel_exporter"],
        label="excel-export",
    )
    return result.returncode


def step_dbt_docs_generate() -> int:
    """Step 6: Compile dbt project and generate documentation artifacts."""

    # Fresh manifest.json ensures catalog.json is in sync.
    result = _run_command(
        [_dbt_exe(), "compile"],
        cwd=PROJECT_ROOT / "dbt",
        label="dbt-compile",
    )
    if result.returncode != 0:
        return result.returncode

    result = _run_command(
        [_dbt_exe(), "docs", "generate"],
        cwd=PROJECT_ROOT / "dbt",
        label="dbt-docs-generate",
    )
    return result.returncode


PIPELINE_STEPS: List[Tuple[str, callable]] = [
    ("Generate Data", step_generate_data),
    ("Load to PostgreSQL", step_load_to_postgres),
    ("dbt Run", step_dbt_run),
    ("dbt Test", step_dbt_test),
    ("Excel Export", step_excel_export),
    ("dbt Docs Generate", step_dbt_docs_generate),
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
            # Send ingestion-phase warning alert if rows were rejected.
            if step_name == "Load to PostgreSQL":
                _send_ingestion_alert()
        else:
            logger.critical(STEP_FAIL.format(name=step_name, code=rc))
            # Circuit breaker: dispatch critical alert before exiting.
            _send_step_failure_alert(step_name, rc)
            print()
            print("+" + "=" * 70 + "+")
            print("|  FAIL  PIPELINE HALTED")
            print("|  Step '%s' failed with exit code %d." % (step_name, rc))
            print("|  Subsequent steps were skipped.")
            print("+" + "=" * 70 + "+")
            sys.exit(1)

        print()

    total_elapsed = time.monotonic() - pipeline_start
    _send_success_alert(total_elapsed)

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

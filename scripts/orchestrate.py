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
import locale
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

# ── Pre-flight health check ────────────────────────────────────────────

_REQUIRED_DIRS = [
    "data/raw",
    "dbt",
    "dbt/models",
    "dbt/target",
    "scripts",
    "src",
]

_DBT_VENV_DIR = PROJECT_ROOT / ".venv-dbt"


def _check_dbt_venv() -> None:
    """Verify the isolated dbt virtual environment is accessible."""
    marker = (
        _DBT_VENV_DIR / "Scripts" / "dbt.exe"
        if sys.platform == "win32"
        else _DBT_VENV_DIR / "bin" / "dbt"
    )
    if not marker.exists():
        msg = (
            f"dbt virtual environment not found at {_DBT_VENV_DIR}.\n"
            "  Run `make setup-dbt` to create it."
        )
        logger.critical(msg)
        print("\n  [PRE-FLIGHT FAIL] " + msg + "\n")
        sys.exit(1)
    logger.info("Pre-flight OK — dbt venv found at %s", _DBT_VENV_DIR)


def _run_preflight_checks() -> None:
    """Validate structural readiness before executing any pipeline step.

    Exits with a descriptive message if any core component is missing.
    """
    missing = []
    for rel in _REQUIRED_DIRS:
        if not (PROJECT_ROOT / rel).is_dir():
            missing.append(rel)

    if missing:
        msg = (
            "The following required directories are missing:\n"
        )
        for d in missing:
            msg += f"    - {d}/\n"
        msg += (
            "  Ensure the project structure is intact.\n"
            "  Run `git checkout -- .` or re-clone the repository."
        )
        logger.critical("Pre-flight check failed — %d missing dir(s)", len(missing))
        print("\n  [PRE-FLIGHT FAIL] " + msg + "\n")
        sys.exit(1)

    logger.info(
        "Pre-flight OK — %d/%d directories present",
        len(_REQUIRED_DIRS) - len(missing),
        len(_REQUIRED_DIRS),
    )

    _check_dbt_venv()


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
send_dbt_test_alert = None  # type: ignore
try:
    from scripts.alerts import send_pipeline_alert, send_dbt_test_alert  # noqa: E402
    _ALERTS_AVAILABLE = True
except ImportError:
    try:
        from alerts import send_pipeline_alert, send_dbt_test_alert  # noqa: E402, F811
        _ALERTS_AVAILABLE = True
    except ImportError:
        pass

if not _ALERTS_AVAILABLE:
    logger.info("scripts.alerts not available — alerts disabled.")

# Store the last parsed DLQ data so alert functions can reference it.
_last_dlq_data: Dict[str, Any] = {}
# Store schema drift data captured during Step 2.
_last_schema_drift_critical: List[Dict[str, Any]] = []
_last_schema_drift_warning: List[Dict[str, Any]] = []


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
    icons = ["[DATA]", "[LOAD]", "[DBT]", "[TEST]", "[EXCEL]", "[DOCS]", "[LINEAGE]", "[PROFILE]"]
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

    # Use the locale's preferred encoding (cp1252 on US Windows, utf-8 on
    # Linux) so that non-ASCII characters (e.g. em dashes in log
    # messages) do not crash the pipe with a UnicodeDecodeError.
    sys_enc = locale.getpreferredencoding() or "utf-8"
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding=sys_enc,
        errors="replace",
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


# ---------------------------------------------------------------------------
# Schema Drift marker parsing & alerting
# ---------------------------------------------------------------------------


def _parse_schema_drift_data(
    output_lines: List[str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Extract schema drift markers from captured subprocess output.

    Returns:
        (critical_list, warning_list) — each list contains the parsed
        JSON details dicts from any SCHEMA_DRIFT_CRITICAL: and
        SCHEMA_DRIFT_WARNING: lines found in the output.
    """
    critical: List[Dict[str, Any]] = []
    warning: List[Dict[str, Any]] = []

    for line in output_lines:
        if line.startswith("SCHEMA_DRIFT_CRITICAL:"):
            try:
                critical.append(
                    json.loads(line[len("SCHEMA_DRIFT_CRITICAL:"):])
                )
            except (json.JSONDecodeError, KeyError):
                logger.warning(
                    "Could not parse SCHEMA_DRIFT_CRITICAL: %s", line
                )
        elif line.startswith("SCHEMA_DRIFT_WARNING:"):
            try:
                warning.append(
                    json.loads(line[len("SCHEMA_DRIFT_WARNING:"):])
                )
            except (json.JSONDecodeError, KeyError):
                logger.warning(
                    "Could not parse SCHEMA_DRIFT_WARNING: %s", line
                )

    return critical, warning


def _log_schema_drift(
    critical_list: List[Dict[str, Any]],
    warning_list: List[Dict[str, Any]],
) -> None:
    """Log parsed schema drift events."""
    for item in critical_list:
        entity = item.get("entity", "unknown")
        missing = item.get("missing_columns", [])
        mismatches = item.get("type_mismatches", {})
        logger.critical(
            "Schema Drift [CRITICAL] — %s quarantined. "
            "Missing: %s  Type mismatches: %s",
            entity,
            ", ".join(missing) if missing else "none",
            mismatches,
        )
    for item in warning_list:
        entity = item.get("entity", "unknown")
        extra = item.get("extra_columns", [])
        logger.warning(
            "Schema Drift [WARNING] — %s has extra columns: %s",
            entity,
            ", ".join(extra),
        )


def _send_schema_drift_alerts(
    critical_list: List[Dict[str, Any]],
    warning_list: List[Dict[str, Any]],
) -> None:
    """Dispatch alerts for schema drift events.

    Critical drift → red alert with missing/type details.
    Warning drift  → amber alert with extra column details.
    """
    if not _ALERTS_AVAILABLE:
        return

    for item in critical_list:
        entity = item.get("entity", "unknown")
        missing = item.get("missing_columns", [])
        mismatches = item.get("type_mismatches", {})
        details: Dict[str, Any] = {
            "Entity": entity,
            "Severity": "CRITICAL — Pipeline Halted",
        }
        if missing:
            details["Missing Columns"] = ", ".join(missing)
        if mismatches:
            details["Type Mismatches"] = str(mismatches)
        send_pipeline_alert(
            status="critical",
            stage="schema-drift",
            details=details,
        )

    for item in warning_list:
        entity = item.get("entity", "unknown")
        extra = item.get("extra_columns", [])
        send_pipeline_alert(
            status="warning",
            stage="schema-drift",
            details={
                "Entity": entity,
                "Severity": "WARNING — Pipeline Continuing",
                "Extra Columns": ", ".join(extra),
            },
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
        # Send a rich alert that lists every failed/errored test from
        # run_results.json, then fall back to the generic alert.
        run_results = PROJECT_ROOT / "dbt" / "target" / "run_results.json"
        if send_dbt_test_alert and run_results.exists():
            send_dbt_test_alert(str(run_results), rc)
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
    """Step 2: Load CSV files into PostgreSQL raw schema with DLQ guardrail.

    Stores parsed schema drift data in module globals for alert
    dispatch after the step completes.
    """
    global _last_schema_drift_critical, _last_schema_drift_warning
    result = _run_command(
        [_py_exe(), str(PROJECT_ROOT / "scripts" / "load_to_postgres.py")],
        label="load-to-postgres",
    )
    _log_dlq_summary(result.output)
    drift_critical, drift_warning = _parse_schema_drift_data(result.output)
    _log_schema_drift(drift_critical, drift_warning)
    _last_schema_drift_critical = drift_critical
    _last_schema_drift_warning = drift_warning
    return result.returncode


def step_dbt_run() -> int:
    """Step 3: Execute dbt models (staging -> intermediate -> marts)."""
    dbt = _dbt_exe()
    dbt_dir = PROJECT_ROOT / "dbt"

    for model_group in ("staging", "intermediate", "marts"):
        cmd = [dbt, "run", "--select", model_group]
        # Full-refresh marts so the incremental fact table is rebuilt from
        # scratch alongside the dimension tables — otherwise stale FK refs
        # in the incremental backlog will fail relationship tests.
        if model_group == "marts":
            cmd.append("--full-refresh")
        logger.info(
            "--- dbt run --select %s%s ---",
            model_group,
            " --full-refresh" if model_group == "marts" else "",
        )
        result = _run_command(cmd, cwd=dbt_dir, label=f"dbt-run-{model_group}")
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


def step_generate_lineage() -> int:
    """Step 7: Render a dynamic lineage blueprint from manifest.json."""
    result = _run_command(
        [_py_exe(), str(PROJECT_ROOT / "scripts" / "generate_lineage.py")],
        label="generate-lineage",
    )
    return result.returncode


def step_generate_profiling() -> int:
    """Step 8: Generate HTML data profile report from mart tables."""
    result = _run_command(
        [_py_exe(), str(PROJECT_ROOT / "scripts" / "generate_profiling.py")],
        label="generate-profiling",
    )
    return result.returncode


PIPELINE_STEPS: List[Tuple[str, callable]] = [
    ("Generate Data", step_generate_data),
    ("Load to PostgreSQL", step_load_to_postgres),
    ("dbt Run", step_dbt_run),
    ("dbt Test", step_dbt_test),
    ("Excel Export", step_excel_export),
    ("dbt Docs Generate", step_dbt_docs_generate),
    ("Lineage Graph Export", step_generate_lineage),
    ("Data Profile Report", step_generate_profiling),
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

    _run_preflight_checks()

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
            # Ingestion-phase alerts: DLQ, schema drift.
            if step_name == "Load to PostgreSQL":
                _send_ingestion_alert()
                _send_schema_drift_alerts(
                    _last_schema_drift_critical,
                    _last_schema_drift_warning,
                )
        else:
            logger.critical(STEP_FAIL.format(name=step_name, code=rc))
            # Circuit breaker: dispatch critical alert before exiting.
            _send_step_failure_alert(step_name, rc)
            print()
            print("+" + "=" * 70 + "+")
            halt_reason = (
                "SCHEMA DRIFT — Critical schema change detected. "
                "Source file quarantined."
                if rc == 2
                else "Pipeline halted by circuit breaker."
            )
            print("|  FAIL  %s" % halt_reason)
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

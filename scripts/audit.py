"""
RetailFlow Pipeline — Audit Context
====================================

Tracks execution metadata for a single pipeline run and persists it to
the ``audit.pipeline_runs`` table in PostgreSQL.

Usage::

    from scripts.audit import AuditContext

    audit = AuditContext()
    # ... run pipeline steps ...
    audit.records_ingested = 14100
    audit.records_rejected = 0
    audit.collect_parquet_paths()
    audit.commit(status="SUCCESS", sla_breached=False)
"""

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_AUDIT_DDL = """
CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    start_time        TIMESTAMPTZ      NOT NULL,
    end_time          TIMESTAMPTZ,
    status            VARCHAR(20)      NOT NULL DEFAULT 'RUNNING'
                      CHECK (status IN ('RUNNING','SUCCESS','FAILED','SCHEMA_DRIFT')),
    records_ingested  INTEGER          NOT NULL DEFAULT 0,
    records_rejected  INTEGER          NOT NULL DEFAULT 0,
    parquet_file_path TEXT,
    duration_seconds  DOUBLE PRECISION,
    sla_breached      BOOLEAN          NOT NULL DEFAULT FALSE,
    error_message     TEXT,
    created_at        TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);
"""


def _get_engine() -> Engine:
    """Create a SQLAlchemy engine from environment variables."""
    import os
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "retailflow")
    user = os.getenv("POSTGRES_USER", "retailflow_user")
    password = os.getenv("POSTGRES_PASSWORD", "retailflow_pass")
    connection_string = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )
    return create_engine(connection_string, echo=False)


class AuditContext:
    """Collects runtime telemetry for a pipeline execution.

    The context is initialized at the start of ``orchestrate.main()`` and
    its ``commit()`` method is called from the ``finally`` block so that
    *every* invocation writes a row, even when the pipeline fails.
    """

    def __init__(self, lakehouse_dir: Optional[str] = None) -> None:
        self.run_id: str = str(uuid.uuid4())
        self.start_time: datetime = datetime.now(timezone.utc)
        self.end_time: Optional[datetime] = None
        self.status: str = "RUNNING"
        self.records_ingested: int = 0
        self.records_rejected: int = 0
        self.parquet_file_path: str = ""
        self.duration_seconds: Optional[float] = None
        self.sla_breached: bool = False
        self.error_message: str = ""
        self._lakehouse_dir: str = lakehouse_dir or ""

    # ── Telemetry capture helpers ──────────────────────────────────────

    def ingest_from_dlq(self, dlq_data: Dict[str, Any]) -> None:
        """Capture records_ingested and records_rejected from DLQ summary.

        Called after the ingestion step (Step 2) parses the
        ``DLQ_SUMMARY:`` JSON line.
        """
        self.records_ingested = dlq_data.get("loaded", 0)
        self.records_rejected = dlq_data.get("rejected", 0)

    def collect_parquet_paths(self) -> None:
        """Scan ``data/lakehouse/`` for ``*.parquet`` files and store them
        as a comma-separated list of absolute paths."""
        if not self._lakehouse_dir:
            return
        lakehouse = Path(self._lakehouse_dir)
        if not lakehouse.is_dir():
            return
        paths: List[str] = []
        for f in sorted(lakehouse.glob("*.parquet")):
            paths.append(str(f.resolve()))
        self.parquet_file_path = ", ".join(paths)

    def finalize(
        self,
        status: str,
        sla_breached: bool = False,
        error_message: str = "",
    ) -> None:
        """Set end-of-run metadata before commit.

        Args:
            status: ``"SUCCESS"``, ``"FAILED"``, or ``"SCHEMA_DRIFT"``.
            sla_breached: Whether the total runtime exceeded the SLA.
            error_message: Optional human-readable failure reason.
        """
        now = datetime.now(timezone.utc)
        self.end_time = now
        self.duration_seconds = (now - self.start_time).total_seconds()
        self.status = status
        self.sla_breached = sla_breached
        self.error_message = error_message

    # ── Persistence ────────────────────────────────────────────────────

    def commit(self, engine: Optional[Engine] = None) -> bool:
        """Insert the audit record into ``audit.pipeline_runs``.

        Creates the ``audit`` schema and table if they do not exist
        (self-healing migration).

        Args:
            engine: SQLAlchemy Engine.  If ``None``, one is created from
                environment variables.

        Returns:
            ``True`` if the insert succeeded, ``False`` if it failed
            (logged but never raised — audit failures must not crash
            the pipeline).
        """
        close_engine = False
        if engine is None:
            try:
                engine = _get_engine()
                close_engine = True
            except Exception as exc:
                logger.error("Audit: cannot create engine — %s", exc)
                return False

        try:
            # Self-healing: ensure schema and table exist.
            with engine.connect() as conn:
                for stmt in _AUDIT_DDL.split(";"):
                    stripped = stmt.strip()
                    if stripped:
                        conn.execute(text(stripped))
                conn.commit()

            insert_sql = text("""
                INSERT INTO audit.pipeline_runs (
                    run_id, start_time, end_time, status,
                    records_ingested, records_rejected,
                    parquet_file_path, duration_seconds,
                    sla_breached, error_message
                ) VALUES (
                    :run_id, :start_time, :end_time, :status,
                    :records_ingested, :records_rejected,
                    :parquet_file_path, :duration_seconds,
                    :sla_breached, :error_message
                )
            """)
            with engine.begin() as conn:
                conn.execute(insert_sql, {
                    "run_id": self.run_id,
                    "start_time": self.start_time,
                    "end_time": self.end_time,
                    "status": self.status,
                    "records_ingested": self.records_ingested,
                    "records_rejected": self.records_rejected,
                    "parquet_file_path": self.parquet_file_path or None,
                    "duration_seconds": self.duration_seconds,
                    "sla_breached": self.sla_breached,
                    "error_message": self.error_message or None,
                })

            logger.info(
                "Audit record written — run_id=%s status=%s "
                "ingested=%d rejected=%d duration=%.2fs",
                self.run_id,
                self.status,
                self.records_ingested,
                self.records_rejected,
                self.duration_seconds or 0.0,
            )
            return True

        except Exception as exc:
            logger.error(
                "Audit: failed to write pipeline_runs record — %s",
                exc,
                exc_info=True,
            )
            return False
        finally:
            if close_engine and engine is not None:
                engine.dispose()

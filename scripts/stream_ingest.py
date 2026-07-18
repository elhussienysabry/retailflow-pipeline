"""
RetailFlow Pipeline — CDC / Operational Data Store (ODS) Ingestion
===================================================================

Reads source CSV/JSON files and applies dynamic upserts into
``ods.live_transactions``, simulating a Change Data Capture flow.

Modes:
    **micro-batch** (default) — reads all source files in one pass,
    chunks them, and upserts every chunk into the ODS table.

    **poll** (``--poll``) — watches ``data/raw/`` for file modifications
    every N seconds and ingests only changed files.

Usage:
    python scripts/stream_ingest.py                     # micro-batch
    python scripts/stream_ingest.py --poll               # continuous poll
    python scripts/stream_ingest.py --poll --interval 5  # poll every 5s

Environment variables (from .env):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

ODS_TABLE = "ods.live_transactions"
ODS_SCHEMA = "ods"

SOURCE_FILES: List[Tuple[str, str, str]] = [
    ("customers.csv", "customers", "csv"),
    ("products.csv", "products", "csv"),
    ("orders.csv", "orders", "csv"),
    ("pos_store_sales.json", "pos", "json"),
]

CHUNK_SIZE = 5000
WATERMARK_FILE = os.path.join(BASE_DIR, "..", "data", ".ods_watermark.json")

CREATE_ODS_SQL = """
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {table} (
        record_id       TEXT        NOT NULL,
        source_system   TEXT        NOT NULL,
        source_key      TEXT        NOT NULL,
        payload         JSONB       NOT NULL,
        upsert_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        batch_id        TEXT        NOT NULL,
        PRIMARY KEY (record_id)
    );

    CREATE INDEX IF NOT EXISTS idx_ods_upsert_at
        ON {schema}.live_transactions (upsert_at DESC);

    CREATE INDEX IF NOT EXISTS idx_ods_source_system
        ON {schema}.live_transactions (source_system);
"""

UPSERT_SQL = """
    INSERT INTO {table} (record_id, source_system, source_key, payload, upsert_at, batch_id)
    VALUES (:record_id, :source_system, :source_key, :payload, :upsert_at, :batch_id)
    ON CONFLICT (record_id) DO UPDATE SET
        payload  = EXCLUDED.payload,
        upsert_at = EXCLUDED.upsert_at,
        batch_id = EXCLUDED.batch_id
"""

# ── Helpers ────────────────────────────────────────────────────────────────


def get_engine() -> Engine:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "retailflow")
    user = os.getenv("POSTGRES_USER", "retailflow_user")
    password = os.getenv("POSTGRES_PASSWORD", "retailflow_pass")
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(conn_str, echo=False)


def ensure_ods_schema(engine: Engine) -> None:
    ddl = CREATE_ODS_SQL.format(schema=ODS_SCHEMA, table=ODS_TABLE)
    with engine.connect() as conn:
        for statement in ddl.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.exec_driver_sql(stmt + ";")
        conn.commit()
    logger.info("ODS schema and table %s ready.", ODS_TABLE)


def _generate_record_id(source_key: str, source_system: str) -> str:
    namespace = uuid.uuid5(uuid.NAMESPACE_DNS, "retailflow.ods")
    return str(uuid.uuid5(namespace, f"{source_system}:{source_key}"))


def _read_source(filepath: str, file_type: str) -> pd.DataFrame:
    if file_type == "csv":
        return pd.read_csv(filepath)
    if file_type == "json":
        with open(filepath, encoding="utf-8") as f:
            records = json.load(f)
        return pd.DataFrame(records) if records else pd.DataFrame()
    raise ValueError(f"Unsupported file type: {file_type}")


def _chunk_dataframe(
    df: pd.DataFrame, size: int
) -> Generator[pd.DataFrame, None, None]:
    for start in range(0, len(df), size):
        yield df.iloc[start : start + size].copy()


def _to_ods_rows(
    df: pd.DataFrame,
    source_system: str,
    key_column: str,
    batch_id: str,
    now: datetime,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        source_key = str(row.get(key_column, ""))
        payload = json.dumps(row.to_dict(), default=str)
        record_id = _generate_record_id(source_key, source_system)
        rows.append(
            {
                "record_id": record_id,
                "source_system": source_system,
                "source_key": source_key,
                "payload": payload,
                "upsert_at": now,
                "batch_id": batch_id,
            }
        )
    return rows


# ── Watermark tracking (for poll mode) ─────────────────────────────────────


def _load_watermark() -> Dict[str, float]:
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE) as f:
            return json.load(f)
    return {}


def _save_watermark(watermark: Dict[str, float]) -> None:
    os.makedirs(os.path.dirname(WATERMARK_FILE), exist_ok=True)
    with open(WATERMARK_FILE, "w") as f:
        json.dump(watermark, f, indent=2)


def _get_file_mtime(filepath: str) -> float:
    return os.path.getmtime(filepath)


# ── Core ingestion ─────────────────────────────────────────────────────────


def ingest_file(
    engine: Engine,
    filename: str,
    source_system: str,
    file_type: str,
    batch_id: str,
) -> Tuple[int, int]:
    filepath = os.path.join(os.path.dirname(BASE_DIR), "data", "raw", filename)
    if not os.path.exists(filepath):
        logger.warning("Source file not found: %s", filepath)
        return 0, 0

    df = _read_source(filepath, file_type)
    if df.empty:
        logger.info("File %s is empty — skipped.", filename)
        return 0, 0

    total_rows = 0

    key_map = {
        "customers": "customer_id",
        "products": "product_id",
        "orders": "order_id",
        "pos": "sale_id",
    }
    key_column = key_map.get(source_system, "id")

    now = datetime.now(timezone.utc)

    with engine.connect() as conn:
        for chunk in _chunk_dataframe(df, CHUNK_SIZE):
            rows = _to_ods_rows(chunk, source_system, key_column, batch_id, now)
            conn.execute(
                text(UPSERT_SQL.format(table=ODS_TABLE)),
                rows,
            )
            total_rows += len(rows)
        conn.commit()

    logger.info(
        "ODS ingest: %s → %s (%d rows, batch=%s)",
        filename,
        ODS_TABLE,
        total_rows,
        batch_id,
    )
    return total_rows, len(df)


def run_micro_batch(engine: Engine) -> int:
    batch_id = datetime.now(timezone.utc).strftime("micro_%Y%m%d_%H%M%S")
    grand_total = 0

    for filename, source_system, file_type in SOURCE_FILES:
        loaded, _ = ingest_file(engine, filename, source_system, file_type, batch_id)
        grand_total += loaded

    logger.info(
        "ODS micro-batch complete — %d total rows upserted (batch=%s).",
        grand_total,
        batch_id,
    )
    return grand_total


def run_poll_loop(engine: Engine, interval: int) -> None:
    logger.info("ODS poll mode started — watching data/raw/ every %ds.", interval)
    watermark = _load_watermark()

    while True:
        batch_id = datetime.now(timezone.utc).strftime("poll_%Y%m%d_%H%M%S")
        any_ingested = False

        for filename, source_system, file_type in SOURCE_FILES:
            filepath = os.path.join(os.path.dirname(BASE_DIR), "data", "raw", filename)
            if not os.path.exists(filepath):
                continue

            current_mtime = _get_file_mtime(filepath)
            last_mtime = watermark.get(filename, 0.0)

            if current_mtime > last_mtime:
                loaded, _ = ingest_file(
                    engine, filename, source_system, file_type, batch_id
                )
                if loaded > 0:
                    watermark[filename] = current_mtime
                    any_ingested = True

        if any_ingested:
            _save_watermark(watermark)

        time.sleep(interval)


# ── CLI entry point ────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="RetailFlow Pipeline — CDC / ODS Streaming Ingest",
    )
    parser.add_argument(
        "--poll",
        action="store_true",
        help="Enable continuous polling mode (monitors data/raw/ for changes)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Poll interval in seconds (default: 10, used with --poll)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    engine = get_engine()
    ensure_ods_schema(engine)

    if args.poll:
        run_poll_loop(engine, args.interval)
    else:
        count = run_micro_batch(engine)
        print(f"ODS_RESULT:{json.dumps({'batch_loaded': count})}")


if __name__ == "__main__":
    main()

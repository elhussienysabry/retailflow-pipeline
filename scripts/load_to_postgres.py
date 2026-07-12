"""
RetailFlow Pipeline — Hybrid Ingestion Engine
===============================================

Reads CSV (e-commerce) and JSON (POS store) source files from ``data/raw/``,
validates rows, loads clean data into the PostgreSQL ``raw`` schema, and
runs schema harmonisation to create a unified ``raw.unified_transactions``
table with upsert semantics.

Supports multi-source hybrid ingestion:
    - ``customers.csv``   — online customer records
    - ``products.csv``    — product catalogue
    - ``orders.csv``      — online e-commerce orders
    - ``pos_store_sales.json`` — physical store POS sales

Guardrail checks per entity:
    Customers:  customer_id not null, email contains '@'
    Products:   product_id not null, price_cents >= 0
    Orders:     order_id / customer_id / product_id not null,
                quantity >= 0, discount_pct in [0, 100]

Usage:
    python scripts/load_to_postgres.py

Environment variables (from .env):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import hashlib
import json
import logging
import os
import shutil
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# ── Alerting (self-contained dispatch for standalone runs) ────────────────
# Graceful import: if called from the orchestrator, the alerts module
# may be importable via the package path or the sibling path.
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

SOURCE_MAP: List[Tuple[str, str, str]] = [
    ("customers.csv", "raw.customers", "csv"),
    ("products.csv", "raw.products", "csv"),
    ("orders.csv", "raw.orders", "csv"),
    ("pos_store_sales.json", "raw.pos_store_sales", "json"),
]

# Base dtype map for source columns (execution tracking added at load time).
DTYPE_MAP: Dict[str, object] = {
    "customer_id": "string",
    "product_id": "string",
    "order_id": "string",
    "first_name": "string",
    "last_name": "string",
    "email": "string",
    "country": "string",
    "city": "string",
    "signup_date": "string",
    "gender": "string",
    "name": "string",
    "category": "string",
    "supplier_country": "string",
    "status": "string",
}


# ---------------------------------------------------------------------------
# Schema Blueprint — Drift Detector
# ---------------------------------------------------------------------------
# Defines the expected schema for every source file. The drift detector
# compares actual file columns (and their pandas dtypes) against this
# blueprint before any data is loaded.
#
# Severity levels:
#   CRITICAL — Missing required column or type mismatch → file moved to
#              data/rejected_schemas/, pipeline halts, red alert fired.
#   WARNING  — Extra unknown column present → pipeline continues, amber
#              alert fired.
# ---------------------------------------------------------------------------

SCHEMA_BLUEPRINT: Dict[str, Dict[str, Any]] = {
    "customers": {
        "required_columns": {
            "customer_id": "string",
            "first_name": "string",
            "last_name": "string",
            "email": "string",
            "country": "string",
            "city": "string",
            "signup_date": "string",
            "age": "int64",
            "gender": "string",
        },
    },
    "products": {
        "required_columns": {
            "product_id": "string",
            "name": "string",
            "category": "string",
            "price_cents": "int64",
            "stock_quantity": "int64",
            "supplier_country": "string",
        },
    },
    "orders": {
        "required_columns": {
            "order_id": "string",
            "customer_id": "string",
            "product_id": "string",
            "quantity": "int64",
            "order_date": "string",
            "status": "string",
            "discount_pct": "int64",
            "shipping_days": "int64",
        },
    },
    "pos_store_sales": {
        "required_columns": {
            "sale_id": "string",
            "store_id": "string",
            "product_id": "string",
            "quantity": "int64",
            "unit_price_cents": "int64",
            "total_amount": "int64",
            "transaction_timestamp": "string",
            "payment_method": "string",
        },
    },
}

_REJECTED_SCHEMAS_DIR = os.path.join(
    os.path.dirname(__file__), "..", "data", "rejected_schemas"
)


def _normalize_dtype(dtype_str: str) -> str:
    """Normalise pandas dtype strings for cross-version comparison.

    - ``"string"``, ``"str"``, ``"object"`` → ``"string"``
    - ``"int64"``, ``"Int64"``, ``"int32"`` → ``"int64"``
    - ``"float64"``, ``"Float64"``, ``"float32"`` → ``"float64"``
    - ``"bool"``, ``"boolean"`` → ``"bool"``
    """
    base = dtype_str.lower().strip()
    if base in ("string", "str", "object"):
        return "string"
    if base.startswith("int"):
        return "int64"
    if base.startswith("float"):
        return "float64"
    if base in ("bool", "boolean"):
        return "bool"
    return base


def _detect_schema_drift(
    filepath: str, entity_name: str, file_type: str
) -> Tuple[str, Dict[str, Any]]:
    """Compare actual file columns against the schema blueprint.

    Args:
        filepath: Absolute path to the source file.
        entity_name: Blueprint key (e.g. ``"customers"``).
        file_type: ``"csv"`` or ``"json"``.

    Returns:
        ``("none", {})`` if the schema matches the blueprint.
        ``("warning", details)`` if extra unknown columns are found
        (pipeline continues, non-blocking).
        ``("critical", details)`` if required columns are missing or
        types mismatch (caller must halt the pipeline).

    The caller is responsible for moving the file to
    ``data/rejected_schemas/`` on critical drift.
    """
    blueprint = SCHEMA_BLUEPRINT.get(entity_name)
    if blueprint is None:
        return "none", {}

    required = set(blueprint["required_columns"].keys())

    # ── Read one row to inspect column names & types ────────────
    try:
        if file_type == "csv":
            sample = pd.read_csv(filepath, nrows=1)
        elif file_type == "json":
            with open(filepath, encoding="utf-8") as f:
                records = json.load(f)
            sample = pd.DataFrame(records[:1]) if records else pd.DataFrame()
        else:
            return "none", {}
    except Exception:
        logger.warning("Could not read %s for schema drift check.", filepath)
        return "none", {}

    actual_cols = set(sample.columns)
    actual_dtypes: Dict[str, str] = {
        col: str(dtype) for col, dtype in sample.dtypes.items()
    }

    # ── Critical: missing required columns ──────────────────────
    missing = required - actual_cols
    if missing:
        details: Dict[str, Any] = {
            "entity": entity_name,
            "severity": "critical",
            "filepath": filepath,
            "missing_columns": sorted(missing),
        }
        logger.critical(
            "SCHEMA DRIFT [CRITICAL] — %s missing required column(s): %s",
            entity_name,
            ", ".join(sorted(missing)),
        )
        return "critical", details

    # ── Critical: column type mismatches ────────────────────────
    type_mismatches: Dict[str, Dict[str, str]] = {}
    for col, expected_type in blueprint["required_columns"].items():
        if col in actual_dtypes:
            actual_norm = _normalize_dtype(actual_dtypes[col])
            expected_norm = _normalize_dtype(expected_type)
            if actual_norm != expected_norm:
                type_mismatches[col] = {
                    "expected": expected_type,
                    "actual": actual_dtypes[col],
                }
    if type_mismatches:
        details = {
            "entity": entity_name,
            "severity": "critical",
            "filepath": filepath,
            "type_mismatches": type_mismatches,
        }
        logger.critical(
            "SCHEMA DRIFT [CRITICAL] — %s has type mismatch(es): %s",
            entity_name,
            type_mismatches,
        )
        return "critical", details

    # ── Warning: extra columns beyond the blueprint ─────────────
    extra = actual_cols - required
    if extra:
        details = {
            "entity": entity_name,
            "severity": "warning",
            "filepath": filepath,
            "extra_columns": sorted(extra),
        }
        logger.warning(
            "SCHEMA DRIFT [WARNING] — %s has extra unknown column(s): %s",
            entity_name,
            ", ".join(sorted(extra)),
        )
        return "warning", details

    return "none", {}


def _move_to_rejected_schemas(filename: str) -> str:
    """Move a source file to the ``data/rejected_schemas/`` quarantine.

    Args:
        filename: Base name of the file (e.g. ``"orders.csv"``).

    Returns:
        Destination path of the moved file.
    """
    src = os.path.join(
        os.path.dirname(__file__), "..", "data", "raw", filename
    )
    os.makedirs(_REJECTED_SCHEMAS_DIR, exist_ok=True)
    dest = os.path.join(_REJECTED_SCHEMAS_DIR, filename)
    shutil.move(src, dest)
    logger.warning("File moved to schema quarantine: %s → %s", src, dest)
    return dest


# ---------------------------------------------------------------------------
# Validation guardrails
# ---------------------------------------------------------------------------

def _validate_customers(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Check customer rows for null keys and malformed emails."""
    mask = (
        df["customer_id"].isna()
        | df["email"].isna()
        | ~df["email"].astype(str).str.contains("@", na=False)
    )
    clean = df[~mask].copy()
    rejected = df[mask].copy()

    def _reason(row):
        reasons = []
        if pd.isna(row.get("customer_id")):
            reasons.append("missing customer_id")
        if pd.isna(row.get("email")):
            reasons.append("missing email")
        elif "@" not in str(row.get("email", "")):
            reasons.append("malformed email (missing @)")
        return "; ".join(reasons)

    if not rejected.empty:
        rejected["rejection_reason"] = rejected.apply(_reason, axis=1)
        logger.warning(
            "Customers guardrail: %d row(s) rejected", len(rejected)
        )

    return clean, rejected


def _validate_products(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Check product rows for null keys and negative prices."""
    mask = (
        df["product_id"].isna()
        | df["price_cents"].isna()
        | (df["price_cents"] < 0)
    )
    clean = df[~mask].copy()
    rejected = df[mask].copy()

    def _reason(row):
        reasons = []
        if pd.isna(row.get("product_id")):
            reasons.append("missing product_id")
        if pd.isna(row.get("price_cents")):
            reasons.append("missing price_cents")
        elif row.get("price_cents", 0) < 0:
            reasons.append("negative price_cents")
        return "; ".join(reasons)

    if not rejected.empty:
        rejected["rejection_reason"] = rejected.apply(_reason, axis=1)
        logger.warning(
            "Products guardrail: %d row(s) rejected", len(rejected)
        )

    return clean, rejected


def _validate_orders(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Check order rows for null FKs, negative quantity, invalid discount."""
    mask = (
        df["order_id"].isna()
        | df["customer_id"].isna()
        | df["product_id"].isna()
        | (df["quantity"] < 0)
        | (df["discount_pct"] < 0)
        | (df["discount_pct"] > 100)
    )
    clean = df[~mask].copy()
    rejected = df[mask].copy()

    def _reason(row):
        reasons = []
        if pd.isna(row.get("order_id")):
            reasons.append("missing order_id")
        if pd.isna(row.get("customer_id")):
            reasons.append("missing customer_id")
        if pd.isna(row.get("product_id")):
            reasons.append("missing product_id")
        if row.get("quantity", 0) < 0:
            reasons.append("negative quantity")
        d = row.get("discount_pct", 0)
        if d < 0 or d > 100:
            reasons.append(f"discount_pct out of range [0,100] ({d})")
        return "; ".join(reasons)

    if not rejected.empty:
        rejected["rejection_reason"] = rejected.apply(_reason, axis=1)
        logger.warning(
            "Orders guardrail: %d row(s) rejected", len(rejected)
        )

    return clean, rejected


# ---------------------------------------------------------------------------
# PII Anonymization (GDPR / CCPA compliance)
# ---------------------------------------------------------------------------

_PII_COLUMNS = ("first_name", "last_name", "email")


def _anonymize_pii(df: pd.DataFrame) -> pd.DataFrame:
    """SHA-256 hash PII columns in-place for GDPR/CCPA compliance.

    For each PII column, nulls are preserved as nulls; non-null values are
    stripped, lowercased, and replaced with their hex digest.

    Args:
        df: Customer DataFrame with PII columns.

    Returns:
        The same DataFrame with PII columns hashed.
    """
    for col in _PII_COLUMNS:
        if col not in df.columns:
            continue
        mask = df[col].notna()
        df.loc[mask, col] = df.loc[mask, col].apply(
            lambda v: hashlib.sha256(
                v.strip().lower().encode("utf-8")
            ).hexdigest()
        )
    logger.info("PII columns anonymised: %s", ", ".join(_PII_COLUMNS))
    return df


# ---------------------------------------------------------------------------
# Dead Letter Queue
# ---------------------------------------------------------------------------

def _write_dlq(entity_name: str, rejected_df: pd.DataFrame) -> None:
    """Append rejected rows to a timestamped DLQ CSV file.

    Args:
        entity_name: Entity name (e.g. 'customer', 'product', 'order').
        rejected_df: DataFrame with rejected rows + ``rejection_reason``.
    """
    if rejected_df.empty:
        return

    rejected_dir = os.path.join(
        os.path.dirname(__file__), "..", "data", "rejected"
    )
    os.makedirs(rejected_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(
        rejected_dir, f"rejected_{entity_name}s_{timestamp}.csv"
    )

    rejected_df.to_csv(out_path, index=False)
    logger.info("DLQ written: %s (%d row(s))", out_path, len(rejected_df))


# ---------------------------------------------------------------------------
# JSON ingestion
# ---------------------------------------------------------------------------


def _load_json_to_table(
    engine: Engine,
    json_filename: str,
    table_name: str,
    execution_date: str,
) -> int:
    """Load a JSON file of records into a PostgreSQL table (idempotent).

    Appends an ``_execution_date`` column, deletes any prior rows with the
    same execution date, then inserts.  Re-running on the same date is
    idempotent — zero duplicates.
    """
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    filepath = os.path.join(data_dir, json_filename)

    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"JSON file not found: {filepath}. Run generate_fake_data.py first."
        )

    logger.info("Loading %s → %s ...", filepath, table_name)

    with open(filepath, encoding="utf-8") as f:
        records: list = json.load(f)

    if not records:
        logger.warning("JSON file %s is empty.", filepath)
        return 0

    df = pd.DataFrame(records)
    df["_execution_date"] = execution_date

    # Idempotent: remove any rows from a prior run on the same date.
    delete_by_execution_date(engine, table_name, execution_date)

    df.to_sql(
        table_name.split(".")[1],
        engine,
        schema="raw",
        if_exists="append",
        index=False,
        method="multi",
    )
    logger.info(
        "Loaded %d rows into %s (execution_date=%s)",
        len(df), table_name, execution_date,
    )
    return len(df)


# ---------------------------------------------------------------------------
# Schema harmonisation & unified upsert
# ---------------------------------------------------------------------------

_UNIFIED_TABLE = "raw.unified_transactions"

_UNIFIED_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    transaction_id   TEXT NOT NULL,
    source_system    TEXT NOT NULL,
    product_id       TEXT,
    quantity         INTEGER,
    transaction_date DATE,
    total_amount     NUMERIC(12,2),
    store_id         TEXT,
    customer_id      TEXT,
    status           TEXT,
    payment_method   TEXT,
    discount_pct     INTEGER,
    shipping_days    INTEGER,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_id, source_system)
);
"""

_UPSERT_SQL = """
INSERT INTO {table} (
    transaction_id, source_system, product_id, quantity,
    transaction_date, total_amount, store_id, customer_id,
    status, payment_method, discount_pct, shipping_days, ingested_at
)
SELECT
    order_id               AS transaction_id,
    'online'               AS source_system,
    product_id,
    quantity,
    order_date::date       AS transaction_date,
    NULL::numeric          AS total_amount,   -- revenue calc happens in dbt
    NULL::text             AS store_id,
    customer_id,
    status,
    NULL::text             AS payment_method,
    discount_pct,
    shipping_days,
    NOW()                  AS ingested_at
FROM raw.orders

UNION ALL

SELECT
    sale_id                AS transaction_id,
    'pos'                  AS source_system,
    product_id,
    quantity,
    transaction_timestamp::date AS transaction_date,
    total_amount           AS total_amount,
    store_id,
    NULL::text             AS customer_id,
    'completed'            AS status,
    payment_method,
    NULL::integer          AS discount_pct,
    NULL::integer          AS shipping_days,
    NOW()                  AS ingested_at
FROM raw.pos_store_sales

ON CONFLICT (transaction_id, source_system) DO UPDATE SET
    product_id       = EXCLUDED.product_id,
    quantity         = EXCLUDED.quantity,
    transaction_date = EXCLUDED.transaction_date,
    total_amount     = EXCLUDED.total_amount,
    store_id         = EXCLUDED.store_id,
    status           = EXCLUDED.status,
    payment_method   = EXCLUDED.payment_method,
    ingested_at      = NOW();
"""


def _harmonize_and_upsert_unified(engine: Engine) -> int:
    """Create (if missing) and upsert into ``raw.unified_transactions``.

    Harmonises columns from ``raw.orders`` (online) and
    ``raw.pos_store_sales`` (POS) into a single unified schema.

    Args:
        engine: SQLAlchemy Engine instance.

    Returns:
        Number of rows upserted.
    """
    logger.info("Creating unified transactions table if missing ...")
    ddl = _UNIFIED_DDL.format(table=_UNIFIED_TABLE)
    with engine.connect() as conn:
        conn.exec_driver_sql(ddl)
        conn.commit()

    logger.info("Upserting harmonised transactions ...")
    upsert = _UPSERT_SQL.format(table=_UNIFIED_TABLE)
    with engine.connect() as conn:
        result = conn.execute(text(upsert))
        conn.commit()

    count = result.rowcount
    logger.info(
        "Unified transactions upserted: %d rows into %s",
        count,
        _UNIFIED_TABLE,
    )
    return count


# ---------------------------------------------------------------------------
# Validation dispatch
# ---------------------------------------------------------------------------

_ENTITY_VALIDATORS = {
    "customers": _validate_customers,
    "products": _validate_products,
    "orders": _validate_orders,
}


def _validate_and_split(
    csv_filename: str, df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Route a DataFrame to the correct validator based on its CSV name.

    Args:
        csv_filename: e.g. 'customers.csv'.
        df: Raw DataFrame to validate.

    Returns:
        (clean_df, rejected_df).
    """
    entity = csv_filename.replace(".csv", "")
    validator = _ENTITY_VALIDATORS.get(entity)
    if validator is None:
        return df, pd.DataFrame()
    return validator(df)


# ---------------------------------------------------------------------------
# Database helpers (unchanged)
# ---------------------------------------------------------------------------

def get_engine() -> Engine:
    """Create a SQLAlchemy engine from environment variables."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "retailflow")
    user = os.getenv("POSTGRES_USER", "retailflow_user")
    password = os.getenv("POSTGRES_PASSWORD", "retailflow_pass")

    connection_string = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    )
    engine = create_engine(connection_string, echo=False)
    logger.info("Connected to PostgreSQL at %s:%s/%s", host, port, database)
    return engine


def ensure_raw_schema(engine: Engine) -> None:
    """Create the ``raw`` schema in PostgreSQL if it does not exist."""
    with engine.connect() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS raw;")
        conn.commit()
    logger.info("Ensured raw schema exists.")


def delete_by_execution_date(
    engine: Engine, table_name: str, execution_date: str
) -> None:
    """Idempotent clean-up: delete rows matching the current execution date.

    Removes all records from *table_name* whose ``_execution_date`` matches
    the current run's date.  This ensures that re-running the pipeline on
    the same calendar day produces exactly the same warehouse state with
    zero duplicate rows, while preserving data from prior runs.

    The ``_execution_date`` column is added via ALTER TABLE if not present,
    so the migration is self-healing across schema versions.
    """
    with engine.connect() as conn:
        # Self-healing migration: add the column if the table already exists
        # without it (e.g. after a schema upgrade).
        col_check = conn.execute(
            text(
                f"SELECT EXISTS ("
                f"  SELECT 1 FROM information_schema.columns "
                f"  WHERE table_schema = '{table_name.split('.')[0]}' "
                f"    AND table_name = '{table_name.split('.')[1]}' "
                f"    AND column_name = '_execution_date'"
                f")"
            )
        ).scalar()
        if not col_check:
            logger.info(
                "Adding _execution_date column to %s for idempotent load.",
                table_name,
            )
            conn.exec_driver_sql(
                f"ALTER TABLE {table_name} "
                f"ADD COLUMN _execution_date DATE;"
            )
        conn.commit()

        deleted = conn.execute(
            text(
                f"DELETE FROM {table_name} "
                f"WHERE _execution_date = :exec_date"
            ),
            {"exec_date": execution_date},
        ).rowcount
        conn.commit()
    logger.info(
        "Idempotent cleanup: deleted %d row(s) from %s "
        "for execution_date=%s",
        deleted, table_name, execution_date,
    )


def load_csv_to_table(
    engine: Engine,
    csv_filename: str,
    table_name: str,
    execution_date: str,
) -> Tuple[int, int]:
    """Read, validate, and load clean rows (idempotent).

    Appends an ``_execution_date`` column, deletes any prior rows with the
    same execution date before inserting, then streams chunks.  Re-running
    on the same date is idempotent.

    Args:
        engine: SQLAlchemy Engine instance.
        csv_filename: e.g. 'orders.csv'.
        table_name: e.g. 'raw.orders'.
        execution_date: ISO date string for the current run.

    Returns:
        (loaded_count, rejected_count).
    """
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    filepath = os.path.join(data_dir, csv_filename)

    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"CSV file not found: {filepath}. Run generate_fake_data.py first."
        )

    logger.info("Loading %s → %s ...", filepath, table_name)

    # Idempotent: remove any rows from a prior run on the same date.
    delete_by_execution_date(engine, table_name, execution_date)

    chunk_size = 10_000
    total_loaded = 0
    total_rejected = 0
    entity_name = csv_filename.replace(".csv", "").rstrip("s")

    for chunk in pd.read_csv(filepath, dtype=DTYPE_MAP, chunksize=chunk_size):
        clean_chunk, rejected_chunk = _validate_and_split(
            csv_filename, chunk
        )

        if not clean_chunk.empty:
            # Tag every row with the current execution date for idempotency.
            clean_chunk["_execution_date"] = execution_date

            # Anonymise PII for customer data before persisting.
            if entity_name == "customer":
                _anonymize_pii(clean_chunk)

            clean_chunk.to_sql(
                table_name.split(".")[1],
                engine,
                schema="raw",
                if_exists="append",
                index=False,
                method="multi",
            )
            total_loaded += len(clean_chunk)

        if not rejected_chunk.empty:
            _write_dlq(entity_name, rejected_chunk)
            total_rejected += len(rejected_chunk)

    logger.info(
        "Loaded %d rows into %s, rejected %d to DLQ "
        "(execution_date=%s)",
        total_loaded,
        table_name,
        total_rejected,
        execution_date,
    )
    return total_loaded, total_rejected


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Connect to PostgreSQL, validate, load clean rows, write DLQ.

    Every row loaded is tagged with ``_execution_date`` set to today's UTC
    date.  Before inserting, any rows from a prior run on the same date are
    deleted — guaranteeing idempotent re-runs (zero duplicates).
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # ── Generate the idempotency execution date once per run ──────────
    execution_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info(
        "Idempotent run — execution_date=%s", execution_date
    )

    start_time = datetime.now()

    try:
        engine = get_engine()
        ensure_raw_schema(engine)

        grand_loaded = 0
        grand_rejected = 0
        per_table: Dict[str, Dict[str, int]] = {}

        for filename, table_name, file_type in SOURCE_MAP:
            # ── Schema Drift Detection ──────────────────────────
            entity_name = (
                filename.replace(".csv", "").replace(".json", "")
            )
            filepath = os.path.join(
                os.path.dirname(__file__), "..", "data", "raw", filename
            )
            drift_type, drift_details = _detect_schema_drift(
                filepath, entity_name, file_type
            )

            if drift_type == "critical":
                _move_to_rejected_schemas(filename)
                print(f"SCHEMA_DRIFT_CRITICAL:{json.dumps(drift_details)}")
                logger.critical(
                    "Schema drift — critical. Pipeline halted "
                    "for %s.", filename
                )
                if _ALERTS_AVAILABLE and send_pipeline_alert:
                    alert_details: Dict[str, Any] = {
                        "Entity": drift_details.get("entity", filename),
                        "Severity": "CRITICAL — Pipeline Halted",
                        "File": drift_details.get("filepath", ""),
                    }
                    missing = drift_details.get("missing_columns", [])
                    if missing:
                        alert_details["Missing Columns"] = ", ".join(missing)
                    mismatches = drift_details.get("type_mismatches", {})
                    if mismatches:
                        alert_details["Type Mismatches"] = str(mismatches)
                    send_pipeline_alert(
                        status="critical",
                        stage="schema-drift",
                        details=alert_details,
                    )
                sys.exit(2)

            if drift_type == "warning":
                print(f"SCHEMA_DRIFT_WARNING:{json.dumps(drift_details)}")
                if _ALERTS_AVAILABLE and send_pipeline_alert:
                    extra = drift_details.get("extra_columns", [])
                    send_pipeline_alert(
                        status="warning",
                        stage="schema-drift",
                        details={
                            "Entity": drift_details.get("entity", filename),
                            "Severity": "WARNING — Pipeline Continuing",
                            "Extra Columns": ", ".join(extra),
                        },
                    )

            # ── Continue with idempotent load ─────────────────────
            if file_type == "json":
                loaded = _load_json_to_table(
                    engine, filename, table_name, execution_date
                )
                grand_loaded += loaded
                per_table[filename] = {"loaded": loaded, "rejected": 0}
            else:
                loaded, rejected = load_csv_to_table(
                    engine, filename, table_name, execution_date
                )
                grand_loaded += loaded
                grand_rejected += rejected
                per_table[filename] = {"loaded": loaded, "rejected": rejected}

        # Schema harmonisation: merge online + POS into unified table.
        unified_count = _harmonize_and_upsert_unified(engine)
        per_table["_unified_transactions"] = {
            "loaded": unified_count,
            "rejected": 0,
        }

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(
            "Hybrid ingestion complete! Loaded %d clean rows, "
            "rejected %d bad rows to DLQ, "
            "harmonised %d unified transactions in %.2f seconds.",
            grand_loaded,
            grand_rejected,
            unified_count,
            elapsed,
        )

        summary = {
            "execution_date": execution_date,
            "loaded": grand_loaded,
            "rejected": grand_rejected,
            "tables": per_table,
        }
        print(f"DLQ_SUMMARY:{json.dumps(summary)}")

    except Exception as exc:
        logger.error("Data loading failed: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

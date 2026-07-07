"""
RetailFlow Pipeline — CSV to PostgreSQL Loader with Data Quality Guardrails
===========================================================================

Reads raw CSV files from data/raw/, validates rows before insertion,
loads clean rows into PostgreSQL `raw` schema, and writes rejected rows
to a Dead Letter Queue (data/rejected/).

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

import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

CSV_TABLE_MAP: List[Tuple[str, str]] = [
    ("customers.csv", "raw.customers"),
    ("products.csv", "raw.products"),
    ("orders.csv", "raw.orders"),
]

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


def truncate_table(engine: Engine, table_name: str) -> None:
    """Remove all rows from a table (idempotent load)."""
    with engine.connect() as conn:
        conn.exec_driver_sql(
            f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;"
        )
        conn.commit()
    logger.info("Truncated table: %s", table_name)


def load_csv_to_table(
    engine: Engine, csv_filename: str, table_name: str
) -> Tuple[int, int]:
    """Read, validate, load clean rows and DLQ rejected rows.

    Args:
        engine: SQLAlchemy Engine instance.
        csv_filename: e.g. 'orders.csv'.
        table_name: e.g. 'raw.orders'.

    Returns:
        (loaded_count, rejected_count).

    Raises:
        FileNotFoundError: If the CSV file does not exist.
    """
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
    filepath = os.path.join(data_dir, csv_filename)

    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"CSV file not found: {filepath}. Run generate_fake_data.py first."
        )

    logger.info("Loading %s → %s ...", filepath, table_name)

    chunk_size = 10_000
    total_loaded = 0
    total_rejected = 0
    entity_name = csv_filename.replace(".csv", "").rstrip("s")

    for chunk in pd.read_csv(filepath, dtype=DTYPE_MAP, chunksize=chunk_size):
        clean_chunk, rejected_chunk = _validate_and_split(
            csv_filename, chunk
        )

        if not clean_chunk.empty:
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
        "Loaded %d rows into %s, rejected %d to DLQ",
        total_loaded,
        table_name,
        total_rejected,
    )
    return total_loaded, total_rejected


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Connect to PostgreSQL, validate, load clean rows, write DLQ."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    start_time = datetime.now()

    try:
        engine = get_engine()
        ensure_raw_schema(engine)

        grand_loaded = 0
        grand_rejected = 0
        per_table: Dict[str, Dict[str, int]] = {}

        for csv_file, table_name in CSV_TABLE_MAP:
            truncate_table(engine, table_name)
            loaded, rejected = load_csv_to_table(engine, csv_file, table_name)
            grand_loaded += loaded
            grand_rejected += rejected
            per_table[csv_file] = {"loaded": loaded, "rejected": rejected}

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(
            "Data loading complete! Loaded %d clean rows, "
            "rejected %d bad rows to DLQ in %.2f seconds.",
            grand_loaded,
            grand_rejected,
            elapsed,
        )

        summary = {
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

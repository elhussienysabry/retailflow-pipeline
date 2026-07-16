"""
RetailFlow Pipeline — Upstream Data Contracts (Pandera)
=======================================================

Defines formal Pandera ``SchemaModel`` classes that act as strict
**Data Contracts** at the ingestion boundary.  Every incoming batch
must satisfy these contracts before rows are allowed into the main
pipeline; violating rows are quarantined without blocking clean data.

Contracts enforced:
    - ``CustomerContract`` — email regex, age range, gender enum, nullability
    - ``ProductContract``  — price > 0, category presence, nullability

Usage::

    from scripts.schemas import validate_with_contract

    clean_df, quarantine_df = validate_with_contract(raw_df, "customers")
"""

import logging
from typing import List, Optional, Tuple

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors
from pandera.typing import Series

logger = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════════════════
# Customer Contract
# ═════════════════════════════════════════════════════════════════════════════


class CustomerContract(pa.DataFrameModel):
    """Data contract enforced on every ``customers`` batch.

    Columns not listed here (e.g. ``signup_date``) are permitted but
    not validated — the contract focuses on business-critical fields.
    """

    customer_id: Series[str] = pa.Field(
        unique=True,
        nullable=False,
        description="Unique customer identifier (UUID). Must be present and unique.",
    )
    first_name: Series[str] = pa.Field(
        nullable=False,
        description="Customer given name. Must not be null.",
    )
    last_name: Series[str] = pa.Field(
        nullable=False,
        description="Customer surname. Must not be null.",
    )
    email: Series[str] = pa.Field(
        nullable=False,
        regex=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        description="Email address. Must match a valid email pattern.",
    )
    country: Series[str] = pa.Field(
        nullable=False,
        description="Country of residence. Must not be null.",
    )
    city: Series[str] = pa.Field(
        nullable=False,
        description="City of residence. Must not be null.",
    )
    age: Series[int] = pa.Field(
        ge=18,
        le=100,
        nullable=False,
        description="Age in years. Must be between 18 and 100 inclusive.",
    )
    gender: Series[str] = pa.Field(
        isin=["Male", "Female", "Other"],
        nullable=False,
        description="Gender identity. Must be one of: Male, Female, Other.",
    )


# ═════════════════════════════════════════════════════════════════════════════
# Product Contract
# ═════════════════════════════════════════════════════════════════════════════


class ProductContract(pa.DataFrameModel):
    """Data contract enforced on every ``products`` batch."""

    product_id: Series[str] = pa.Field(
        unique=True,
        nullable=False,
        description="Unique product identifier (UUID). Must be present and unique.",
    )
    name: Series[str] = pa.Field(
        nullable=False,
        description="Product display name. Must not be null.",
    )
    category: Series[str] = pa.Field(
        nullable=False,
        description="Product category. Must not be null.",
    )
    price_cents: Series[int] = pa.Field(
        gt=0,
        nullable=False,
        description="Price in cents. Must be a positive integer (> 0).",
    )


# ═════════════════════════════════════════════════════════════════════════════
# Contract registry
# ═════════════════════════════════════════════════════════════════════════════

# Maps a CSV / JSON filename (as it appears in ``SOURCE_MAP``) to its
# corresponding Pandera schema class.  Only entities listed here are
# subject to contract validation.
CONTRACT_MAP: dict = {
    "customers.csv": CustomerContract,
    "products.csv": ProductContract,
}


# ═════════════════════════════════════════════════════════════════════════════
# Validation dispatcher
# ═════════════════════════════════════════════════════════════════════════════


def _extract_failure_indices(err: SchemaErrors) -> set:
    """Collect all row-level failure indices from a ``SchemaErrors`` exception.

    Iterates over every ``SchemaError`` in the exception, extracts the
    ``failure_cases`` DataFrame, and returns a deduplicated set of
    integer row indices whose values violated the contract.

    Column-level or schema-level errors that do not carry row indices
    are ignored — they do not produce quarantine rows.
    """
    indices: set = set()
    for schema_error in err.schema_errors:
        failure_cases = schema_error.failure_cases
        if failure_cases is not None and not failure_cases.empty:
            idx_col = failure_cases.get("index")
            if idx_col is not None:
                valid_indices = idx_col.dropna().unique()
                if len(valid_indices) > 0:
                    indices.update(int(i) for i in valid_indices)
    return indices


def validate_with_contract(
    df: pd.DataFrame,
    source_filename: str,
    columns: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Validate a DataFrame against the contract for *source_filename*.

    This is the primary entry point for Data Contract enforcement.
    The function:

    1. Looks up the Pandera schema for the given source file.
    2. If no contract exists, returns ``(df, empty DataFrame)`` — data
       passes through unchecked.
    3. Runs Pandera validation in **lazy mode** so that ALL violations
       are collected in a single pass.
    4. On **no violations**: returns ``(df, empty DataFrame)``.
    5. On **violations**: splits the DataFrame into rows that satisfy
       the contract (clean) and rows that do not (quarantine).

    The quarantine DataFrame includes extra columns:

    - ``quarantine_reason`` — human-readable description of the violation

    Args:
        df: The raw DataFrame to validate.
        source_filename: The source file name (e.g. ``"customers.csv"``)
            used as the key into ``CONTRACT_MAP``.
        columns: Optional subset of columns to validate.  If ``None``,
            all contract columns are checked.

    Returns:
        ``(clean_df, quarantine_df)``.  One of the two may be empty.
    """
    schema_cls = CONTRACT_MAP.get(source_filename)
    if schema_cls is None:
        return df, pd.DataFrame()

    # Build the schema — optionally restrict to a subset of columns.
    schema = schema_cls.to_schema()
    if columns is not None:
        subset = {c: schema.columns[c] for c in columns if c in schema.columns}
        schema = schema.__class__(
            {"col": c for c in columns}, columns=subset  # type: ignore[arg-type]
        )

    try:
        schema.validate(df, lazy=True)
        return df, pd.DataFrame()
    except SchemaErrors as err:
        failure_indices = _extract_failure_indices(err)

        if not failure_indices:
            # No row-level failures — could be a schema-level issue
            # (e.g. missing column).  Quarantine the whole batch.
            logger.critical(
                "Contract violation — no row-level indices found. "
                "Quarantining entire %s batch (%d rows).",
                source_filename,
                len(df),
            )
            quarantine = df.copy()
            quarantine["quarantine_reason"] = f"Contract failure — {err.args[0]}"
            return pd.DataFrame(), quarantine

        quarantine_mask = df.index.isin(failure_indices)
        clean = df[~quarantine_mask].copy()
        quarantine = df[quarantine_mask].copy()

        # Build a human-readable reason per row.
        reason_map: dict = {}
        for schema_error in err.schema_errors:
            col = schema_error.column_name or "?"
            check = str(schema_error.check or "?")
            fc = schema_error.failure_cases
            if fc is not None and not fc.empty and "index" in fc.columns:
                for _, row in fc.iterrows():
                    idx = row.get("index")
                    if pd.notna(idx):
                        reason_map.setdefault(int(idx), []).append(f"{col}: {check}")

        quarantine["quarantine_reason"] = quarantine.index.map(
            lambda i: "; ".join(reason_map.get(i, ["unknown contract violation"]))
        )

        entity = source_filename.replace(".csv", "").replace(".json", "")
        logger.warning(
            "Data Contract [%s] — %d row(s) quarantined, "
            "%d row(s) passed validation.",
            entity,
            len(quarantine),
            len(clean),
        )
        return clean, quarantine

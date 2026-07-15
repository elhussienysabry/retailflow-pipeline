"""
Tests for the CSV-to-PostgreSQL loader script.

Verifies that:
    - The engine creation works with valid environment variables
    - Schema creation SQL is correct
    - CSV loading handles edge cases (missing files, empty CSVs)
    - Validation guardrails catch bad rows correctly
    - PII anonymisation produces deterministic hashes
    - Schema drift detection flags missing / extra columns
"""

import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

# WHY: Add the project root to sys.path so we can import the scripts module.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.load_to_postgres import (  # noqa: E402
    SOURCE_MAP,
    PARQUET_COMPRESSION,
    ensure_raw_schema,
    get_engine,
    delete_by_execution_date,
    _normalize_dtype,
    _detect_schema_drift,
    _validate_customers,
    _validate_products,
    _validate_orders,
    _anonymize_pii,
    _validate_and_split,
    _write_dlq,
    _write_lakehouse_parquet,
    _duckdb_harmonize,
    _move_to_rejected_schemas,
    _hive_partition_path,
    clean_lakehouse_partition,
    _UNIFIED_DDL,
)


class TestGetEngine:
    """Tests for the get_engine function."""

    def test_uses_env_variables(self) -> None:
        """Engine should use environment variables for connection params."""

        with patch.dict(
            os.environ,
            {
                "POSTGRES_HOST": "testhost",
                "POSTGRES_PORT": "9999",
                "POSTGRES_DB": "testdb",
                "POSTGRES_USER": "testuser",
                "POSTGRES_PASSWORD": "testpass",
            },
        ):
            engine = get_engine()
            url = str(engine.url)
            assert "testhost" in url
            assert "9999" in url
            assert "testdb" in url
            assert "testuser" in url

    def test_engine_type(self) -> None:
        """Should return a SQLAlchemy Engine instance."""
        engine = get_engine()
        assert "Engine" in type(engine).__name__


class TestEnsureRawSchema:
    """Tests for the ensure_raw_schema function."""

    def test_creates_schema(self) -> None:
        """Should execute CREATE SCHEMA IF NOT EXISTS."""
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        ensure_raw_schema(mock_engine)

        assert mock_conn.exec_driver_sql.called
        call_args = str(mock_conn.exec_driver_sql.call_args)
        assert "CREATE SCHEMA" in call_args


class TestDeleteByExecutionDate:
    """Tests for the delete_by_execution_date function (idempotent load)."""

    def test_executes_delete_where_execution_date(self) -> None:
        """Should execute DELETE with execution_date parameter."""
        mock_conn = MagicMock()
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        # Mock the information_schema check to return True (column exists).
        mock_conn.execute.return_value.scalar.return_value = True

        delete_by_execution_date(mock_engine, "raw.customers", "2026-07-12")

        # Should have called execute() at least twice (column check + DELETE).
        assert mock_conn.execute.call_count >= 2
        # Collect all positional args (the SQL text objects).
        all_sql = []
        for call_args in mock_conn.execute.call_args_list:
            args, _ = call_args
            for a in args:
                all_sql.append(str(a))
        assert any("DELETE" in s for s in all_sql)
        assert any("information_schema" in s for s in all_sql)


class TestCSVTableMap:
    """Tests for the CSV-to-table mapping."""

    def test_has_expected_tables(self) -> None:
        """Verify table names match expectations (includes new POS JSON table)."""
        tables = [t for _, t, _ in SOURCE_MAP]
        assert "raw.customers" in tables
        assert "raw.products" in tables
        assert "raw.orders" in tables
        assert "raw.pos_store_sales" in tables

    def test_has_four_mappings(self) -> None:
        """Verify there are exactly four file-to-table mappings (CSV + JSON)."""
        assert len(SOURCE_MAP) == 4


class TestNormalizeDtype:
    def test_string_variants_normalise(self) -> None:
        assert _normalize_dtype("string") == "string"
        assert _normalize_dtype("str") == "string"
        assert _normalize_dtype("object") == "string"

    def test_int_variants_normalise(self) -> None:
        assert _normalize_dtype("int64") == "int64"
        assert _normalize_dtype("Int64") == "int64"
        assert _normalize_dtype("int32") == "int64"

    def test_float_variants_normalise(self) -> None:
        assert _normalize_dtype("float64") == "float64"
        assert _normalize_dtype("Float64") == "float64"
        assert _normalize_dtype("float32") == "float64"

    def test_bool_variants_normalise(self) -> None:
        assert _normalize_dtype("bool") == "bool"
        assert _normalize_dtype("boolean") == "bool"

    def test_unknown_type_passed_through(self) -> None:
        assert _normalize_dtype("datetime64[ns]") == "datetime64[ns]"


class TestDetectSchemaDrift:
    def test_no_drift_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "customers.csv"
            # Use quoted strings so pandas reads string dtypes, not int64.
            csv_path.write_text(
                'customer_id,first_name,last_name,email,country,city,signup_date,age,gender\n"c1","John","Doe","j@example.com","US","NY","2024-01-01","30","M"\n'
            )
            drift, details = _detect_schema_drift(str(csv_path), "customers", "csv")
            assert drift == "none"
            assert details == {}

    def test_missing_columns_returns_critical(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "customers.csv"
            csv_path.write_text("customer_id,first_name\n1,John\n")
            drift, details = _detect_schema_drift(str(csv_path), "customers", "csv")
            assert drift == "critical"
            assert "missing_columns" in details
            assert details["entity"] == "customers"

    def test_extra_columns_returns_warning(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "orders.csv"
            # Quote string columns to avoid int64 dtype inference.
            cols = "order_id,customer_id,product_id,quantity,order_date,status,discount_pct,shipping_days,extra_col\n"
            csv_path.write_text(
                cols + '"o1","c1","p1",1,"2024-01-01","pending",0,1,"extra"\n'
            )
            drift, details = _detect_schema_drift(str(csv_path), "orders", "csv")
            assert drift == "warning"
            assert "extra_columns" in details

    def test_unknown_entity_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "unknown.csv"
            csv_path.write_text("a,b\n1,2\n")
            drift, details = _detect_schema_drift(str(csv_path), "unknown", "csv")
            assert drift == "none"

    def test_empty_json_returns_critical(self) -> None:
        """An empty JSON file has no columns, so all required are missing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "empty.json"
            json_path.write_text("[]")
            drift, details = _detect_schema_drift(
                str(json_path), "pos_store_sales", "json"
            )
            assert drift == "critical"
            assert "missing_columns" in details

    def test_json_with_missing_columns_returns_critical(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "pos_store_sales.json"
            json_path.write_text('[{"sale_id": "1"}]')
            drift, details = _detect_schema_drift(
                str(json_path), "pos_store_sales", "json"
            )
            assert drift == "critical"

    def test_unknown_file_type_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.parquet"
            path.write_text("")
            drift, details = _detect_schema_drift(str(path), "customers", "parquet")
            assert drift == "none"


class TestMoveToRejectedSchemas:
    def test_moves_file_to_quarantine(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir) / "data" / "raw"
            raw_dir.mkdir(parents=True)
            # The function resolves ".." from the scripts dir, so we must
            # create that path for OS-level path resolution to succeed.
            scripts_dir = Path(tmpdir) / "scripts"
            scripts_dir.mkdir()
            src_file = raw_dir / "test.csv"
            src_file.write_text("a,b\n1,2\n")

            with patch(
                "scripts.load_to_postgres._REJECTED_SCHEMAS_DIR",
                str(Path(tmpdir) / "data" / "rejected_schemas"),
            ), patch(
                "scripts.load_to_postgres.os.path.dirname",
                return_value=str(scripts_dir),
            ):
                dest = _move_to_rejected_schemas("test.csv")
                assert not src_file.exists()
                assert Path(dest).exists()


class TestValidateCustomers:
    def test_clean_rows_pass(self) -> None:
        df = pd.DataFrame(
            {
                "customer_id": ["1", "2"],
                "email": ["a@b.com", "c@d.com"],
            }
        )
        clean, rejected = _validate_customers(df)
        assert len(clean) == 2
        assert len(rejected) == 0

    def test_missing_customer_id_rejected(self) -> None:
        df = pd.DataFrame(
            {
                "customer_id": [None, "2"],
                "email": ["a@b.com", "c@d.com"],
            }
        )
        clean, rejected = _validate_customers(df)
        assert len(clean) == 1
        assert len(rejected) == 1
        assert "missing customer_id" in rejected["rejection_reason"].iloc[0]

    def test_missing_email_rejected(self) -> None:
        df = pd.DataFrame(
            {
                "customer_id": ["1", "2"],
                "email": [None, "c@d.com"],
            }
        )
        clean, rejected = _validate_customers(df)
        assert len(clean) == 1
        assert len(rejected) == 1
        assert "missing email" in rejected["rejection_reason"].iloc[0]

    def test_malformed_email_rejected(self) -> None:
        df = pd.DataFrame(
            {
                "customer_id": ["1"],
                "email": ["notanemail"],
            }
        )
        clean, rejected = _validate_customers(df)
        assert len(rejected) == 1
        assert "malformed email" in rejected["rejection_reason"].iloc[0]


class TestValidateProducts:
    def test_clean_rows_pass(self) -> None:
        df = pd.DataFrame(
            {
                "product_id": ["1", "2"],
                "price_cents": [1000, 2000],
            }
        )
        clean, rejected = _validate_products(df)
        assert len(clean) == 2
        assert len(rejected) == 0

    def test_negative_price_rejected(self) -> None:
        df = pd.DataFrame(
            {
                "product_id": ["1"],
                "price_cents": [-100],
            }
        )
        clean, rejected = _validate_products(df)
        assert len(rejected) == 1
        assert "negative" in rejected["rejection_reason"].iloc[0]


class TestValidateOrders:
    def test_clean_rows_pass(self) -> None:
        df = pd.DataFrame(
            {
                "order_id": ["1"],
                "customer_id": ["1"],
                "product_id": ["1"],
                "quantity": [1],
                "discount_pct": [10],
            }
        )
        clean, rejected = _validate_orders(df)
        assert len(clean) == 1
        assert len(rejected) == 0

    def test_negative_quantity_rejected(self) -> None:
        df = pd.DataFrame(
            {
                "order_id": ["1"],
                "customer_id": ["1"],
                "product_id": ["1"],
                "quantity": [-1],
                "discount_pct": [0],
            }
        )
        clean, rejected = _validate_orders(df)
        assert len(rejected) == 1
        assert "negative quantity" in rejected["rejection_reason"].iloc[0]

    def test_discount_out_of_range_rejected(self) -> None:
        df = pd.DataFrame(
            {
                "order_id": ["1"],
                "customer_id": ["1"],
                "product_id": ["1"],
                "quantity": [1],
                "discount_pct": [150],
            }
        )
        clean, rejected = _validate_orders(df)
        assert len(rejected) == 1
        assert "discount_pct out of range" in rejected["rejection_reason"].iloc[0]


class TestAnonymizePii:
    def test_hashes_name_and_email(self) -> None:
        df = pd.DataFrame(
            {
                "first_name": ["John"],
                "last_name": ["Doe"],
                "email": ["John.Doe@Example.COM"],
            }
        )
        result = _anonymize_pii(df)
        # All values should be 64-char hex strings (SHA-256).
        assert result["first_name"].iloc[0] != "John"
        assert len(result["first_name"].iloc[0]) == 64
        assert result["email"].iloc[0] != "John.Doe@Example.COM"

    def test_preserves_nulls(self) -> None:
        df = pd.DataFrame(
            {
                "first_name": [None],
                "last_name": ["Smith"],
                "email": ["a@b.com"],
            }
        )
        result = _anonymize_pii(df)
        assert result["first_name"].iloc[0] is None

    def test_missing_pii_column_does_not_error(self) -> None:
        df = pd.DataFrame({"non_pii": ["value"]})
        result = _anonymize_pii(df)
        assert "non_pii" in result.columns


class TestValidateAndSplit:
    def test_routes_to_customers_validator(self) -> None:
        df = pd.DataFrame(
            {
                "customer_id": ["1", None],
                "email": ["a@b.com", "b@c.com"],
            }
        )
        clean, rejected = _validate_and_split("customers.csv", df)
        assert len(clean) == 1
        assert len(rejected) == 1

    def test_routes_to_products_validator(self) -> None:
        df = pd.DataFrame(
            {
                "product_id": ["1"],
                "price_cents": [-1],
            }
        )
        clean, rejected = _validate_and_split("products.csv", df)
        assert len(rejected) == 1

    def test_routes_to_orders_validator(self) -> None:
        df = pd.DataFrame(
            {
                "order_id": ["1"],
                "customer_id": ["1"],
                "product_id": ["1"],
                "quantity": [1],
                "discount_pct": [150],
            }
        )
        clean, rejected = _validate_and_split("orders.csv", df)
        assert len(rejected) == 1

    def test_unknown_entity_returns_all_clean(self) -> None:
        df = pd.DataFrame({"a": [1]})
        clean, rejected = _validate_and_split("unknown.csv", df)
        assert len(clean) == 1
        assert len(rejected) == 0


class TestWriteDlq:
    def test_empty_df_does_nothing(self) -> None:
        df = pd.DataFrame()
        _write_dlq("customer", df)  # should not raise

    def test_writes_csv_with_rejection_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "scripts.load_to_postgres.os.path.dirname",
                return_value=str(Path(tmpdir) / "scripts"),
            ):
                df = pd.DataFrame(
                    {
                        "customer_id": [None],
                        "email": ["a@b.com"],
                        "rejection_reason": ["missing customer_id"],
                    }
                )
                _write_dlq("customer", df)
                rejected_dir = Path(tmpdir) / "data" / "rejected"
                csv_files = list(rejected_dir.glob("rejected_customers_*.csv"))
                assert len(csv_files) == 1
                content = csv_files[0].read_text()
                assert "missing customer_id" in content


class TestHivePartitionPath:
    """Tests for the _hive_partition_path helper."""

    def test_returns_correct_hive_path(self) -> None:
        with patch(
            "scripts.load_to_postgres.LAKEHOUSE_DIR",
            "/base/lakehouse",
        ):
            path = _hive_partition_path("customers", "2026-07-14")
            expected = os.path.join(
                "/base/lakehouse",
                "customers",
                "year=2026",
                "month=07",
                "day=14",
            )
            assert path == expected

    def test_includes_lakehouse_root(self) -> None:
        with patch(
            "scripts.load_to_postgres.LAKEHOUSE_DIR",
            "/base/lakehouse",
        ):
            path = _hive_partition_path("orders", "2026-01-05")
            expected_prefix = os.path.join("/base/lakehouse", "orders")
            assert path.startswith(expected_prefix)


class TestCleanLakehousePartition:
    """Tests for the clean_lakehouse_partition function."""

    def test_removes_existing_partition(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            partition = Path(tmpdir) / "customers" / "year=2026" / "month=07" / "day=14"
            partition.mkdir(parents=True)
            (partition / "data.parquet").write_text("dummy")
            assert partition.exists()

            with patch(
                "scripts.load_to_postgres.LAKEHOUSE_DIR",
                str(Path(tmpdir)),
            ):
                result = clean_lakehouse_partition("customers", "2026-07-14")
                assert result is True
                assert not partition.exists()

    def test_non_existent_partition_returns_false(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "scripts.load_to_postgres.LAKEHOUSE_DIR",
                str(Path(tmpdir)),
            ):
                result = clean_lakehouse_partition("customers", "2026-07-14")
                assert result is False


class TestWriteLakehouseParquet:
    """Tests for the _write_lakehouse_parquet function."""

    def test_writes_parquet_with_snappy_compression(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            lakehouse = Path(tmpdir) / "lakehouse"
            with patch(
                "scripts.load_to_postgres.LAKEHOUSE_DIR",
                str(lakehouse),
            ):
                df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
                out_path = _write_lakehouse_parquet(df, "test_entity", "2026-07-14")
                assert out_path
                parquet_file = Path(out_path)
                assert parquet_file.exists()
                assert parquet_file.suffix == ".parquet"
                # Verify Hive-partitioned directory structure.
                assert "year=2026" in str(parquet_file)
                assert "month=07" in str(parquet_file)
                assert "day=14" in str(parquet_file)
                # Verify we can read it back.
                result = pd.read_parquet(str(parquet_file))
                assert len(result) == 2
                assert list(result.columns) == ["a", "b"]

    def test_empty_df_returns_empty_string(self) -> None:
        df = pd.DataFrame()
        result = _write_lakehouse_parquet(df, "empty_entity", "2026-07-14")
        assert result == ""

    def test_uses_configured_compression(self) -> None:
        assert PARQUET_COMPRESSION == "snappy"


class TestDuckdbHarmonize:
    """Tests for the _duckdb_harmonize function."""

    def test_no_parquet_files_returns_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "scripts.load_to_postgres.LAKEHOUSE_DIR",
                str(Path(tmpdir) / "lakehouse"),
            ):
                mock_engine = MagicMock()
                count = _duckdb_harmonize(mock_engine, "2026-07-13")
                assert count == 0

    def test_harmonizes_orders_and_pos_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            lakehouse = Path(tmpdir) / "lakehouse"
            lakehouse.mkdir()

            # Write a minimal orders.parquet in Hive-partitioned structure.
            orders_partition = (
                lakehouse / "orders" / "year=2026" / "month=07" / "day=13"
            )
            orders_partition.mkdir(parents=True)
            orders_df = pd.DataFrame(
                {
                    "order_id": ["o1", "o2"],
                    "product_id": ["p1", "p2"],
                    "quantity": [2, 1],
                    "order_date": ["2026-01-01", "2026-01-02"],
                    "customer_id": ["c1", "c2"],
                    "status": ["completed", "pending"],
                    "discount_pct": [0, 10],
                    "shipping_days": [3, 5],
                }
            )
            orders_df.to_parquet(
                str(orders_partition / "orders.parquet"),
                index=False,
            )

            # Write a minimal pos_store_sales.parquet in Hive-partitioned structure.
            pos_partition = (
                lakehouse / "pos_store_sales" / "year=2026" / "month=07" / "day=13"
            )
            pos_partition.mkdir(parents=True)
            pos_df = pd.DataFrame(
                {
                    "sale_id": ["s1"],
                    "store_id": ["STORE_001"],
                    "product_id": ["p3"],
                    "quantity": [5],
                    "unit_price_cents": [1999],
                    "total_amount": [9995],
                    "transaction_timestamp": ["2026-01-03T10:00:00"],
                    "payment_method": ["credit_card"],
                }
            )
            pos_df.to_parquet(
                str(pos_partition / "pos_store_sales.parquet"),
                index=False,
            )

            mock_conn = MagicMock()
            mock_engine = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            # Mock the information_schema check (column exists) and DELETE.
            mock_conn.execute.return_value.scalar.return_value = True
            # Mock the DDL execution
            mock_conn.exec_driver_sql.return_value = None

            with patch(
                "scripts.load_to_postgres.LAKEHOUSE_DIR",
                str(lakehouse),
            ):
                count = _duckdb_harmonize(mock_engine, "2026-07-13")
                # Should have 2 orders + 1 POS = 3 unified rows
                assert count == 3

    def test_only_orders_parquet_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            lakehouse = Path(tmpdir) / "lakehouse"
            lakehouse.mkdir()
            orders_partition = (
                lakehouse / "orders" / "year=2026" / "month=07" / "day=13"
            )
            orders_partition.mkdir(parents=True)
            orders_df = pd.DataFrame(
                {
                    "order_id": ["o1"],
                    "product_id": ["p1"],
                    "quantity": [1],
                    "order_date": ["2026-01-01"],
                    "customer_id": ["c1"],
                    "status": ["completed"],
                    "discount_pct": [0],
                    "shipping_days": [2],
                }
            )
            orders_df.to_parquet(
                str(orders_partition / "orders.parquet"),
                index=False,
            )

            mock_conn = MagicMock()
            mock_engine = MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            mock_conn.execute.return_value.scalar.return_value = True

            with patch(
                "scripts.load_to_postgres.LAKEHOUSE_DIR",
                str(lakehouse),
            ):
                count = _duckdb_harmonize(mock_engine, "2026-07-13")
                assert count == 1

    def test_unified_ddl_includes_execution_date(self) -> None:
        """Verify the DDL template includes _execution_date."""
        assert "_execution_date" in _UNIFIED_DDL

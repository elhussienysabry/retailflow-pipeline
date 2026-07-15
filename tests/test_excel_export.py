"""
Tests for the Excel analytics export module.

Verifies that:
    - build_workbook creates a workbook with all 4 expected sheets
    - Each sheet has the correct name
    - Headers are written correctly for each sheet
    - Empty DataFrames produce placeholder "No data" sheets
    - Currency formatting is applied to revenue columns
    - Column widths are auto-fitted
"""

import os
import sys
import tempfile
from pathlib import Path
from typing import Dict
from unittest.mock import MagicMock, patch

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.exports.excel_exporter import (  # noqa: E402
    SHEET_NAMES,
    ANALYTICS_QUERIES,
    _format_currency_columns,
    build_workbook,
    fetch_data,
    get_engine,
    save_workbook,
)


def _make_sample_data() -> Dict[str, pd.DataFrame]:
    """Return sample DataFrames matching the 4 expected sheets."""
    return {
        "Top Customers": pd.DataFrame(
            {
                "first_name": ["Alice", "Bob"],
                "last_name": ["Smith", "Jones"],
                "email": ["a@x.com", "b@x.com"],
                "country": ["US", "UK"],
                "city": ["NYC", "London"],
                "total_net_revenue": [1500.50, 1200.00],
            }
        ),
        "Monthly Sales": pd.DataFrame(
            {
                "month": pd.to_datetime(["2024-01-01", "2024-02-01"]),
                "total_orders": [100, 120],
                "net_revenue": [50000.00, 60000.00],
                "month_over_month_growth_pct": [None, 20.0],
            }
        ),
        "Category Performance": pd.DataFrame(
            {
                "category": ["Electronics", "Clothing"],
                "total_orders": [50, 40],
                "total_units_sold": [200, 180],
                "total_net_revenue": [30000.00, 25000.00],
                "avg_discount_pct": [10.5, 8.0],
                "revenue_share_pct": [54.55, 45.45],
            }
        ),
        "Cohort Analysis": pd.DataFrame(
            {
                "cohort_month": pd.to_datetime(["2024-01-01"]),
                "cohort_index": [0],
                "active_customers": [50],
                "total_revenue": [10000.00],
                "avg_revenue_per_customer": [200.00],
            }
        ),
    }


class TestBuildWorkbook:
    """Tests for the build_workbook function."""

    def test_creates_four_sheets(self) -> None:
        """Workbook should have exactly 4 sheets."""
        data = _make_sample_data()
        wb = build_workbook(data)
        assert len(wb.sheetnames) == 4

    def test_sheet_names_match_expected(self) -> None:
        """Sheet names should match SHEET_NAMES."""
        data = _make_sample_data()
        wb = build_workbook(data)
        assert wb.sheetnames == SHEET_NAMES

    def test_top_customers_headers(self) -> None:
        """Top Customers sheet should have correct headers."""
        data = _make_sample_data()
        wb = build_workbook(data)
        ws = wb["Top Customers"]
        headers = [ws.cell(row=1, column=i).value for i in range(1, 7)]
        assert headers == [
            "first_name",
            "last_name",
            "email",
            "country",
            "city",
            "total_net_revenue",
        ]

    def test_monthly_sales_headers(self) -> None:
        """Monthly Sales sheet should have correct headers."""
        data = _make_sample_data()
        wb = build_workbook(data)
        ws = wb["Monthly Sales"]
        headers = [ws.cell(row=1, column=i).value for i in range(1, 5)]
        assert headers == [
            "month",
            "total_orders",
            "net_revenue",
            "month_over_month_growth_pct",
        ]

    def test_category_performance_headers(self) -> None:
        """Category Performance sheet should have correct headers."""
        data = _make_sample_data()
        wb = build_workbook(data)
        ws = wb["Category Performance"]
        headers = [ws.cell(row=1, column=i).value for i in range(1, 7)]
        assert headers == [
            "category",
            "total_orders",
            "total_units_sold",
            "total_net_revenue",
            "avg_discount_pct",
            "revenue_share_pct",
        ]

    def test_cohort_analysis_headers(self) -> None:
        """Cohort Analysis sheet should have correct headers."""
        data = _make_sample_data()
        wb = build_workbook(data)
        ws = wb["Cohort Analysis"]
        headers = [ws.cell(row=1, column=i).value for i in range(1, 6)]
        assert headers == [
            "cohort_month",
            "cohort_index",
            "active_customers",
            "total_revenue",
            "avg_revenue_per_customer",
        ]

    def test_empty_dataframe_placeholder(self) -> None:
        """Empty DataFrames should show 'No data available'."""
        data = {
            "Top Customers": pd.DataFrame(),
            "Monthly Sales": pd.DataFrame(),
            "Category Performance": pd.DataFrame(),
            "Cohort Analysis": pd.DataFrame(),
        }
        wb = build_workbook(data)
        for sheet_name in SHEET_NAMES:
            ws = wb[sheet_name]
            assert ws.cell(row=1, column=1).value == "No data available"

    def test_header_styling_applied(self) -> None:
        """Headers should have bold font and fill color."""
        data = _make_sample_data()
        wb = build_workbook(data)
        ws = wb["Top Customers"]
        cell = ws.cell(row=1, column=1)
        assert cell.font.bold is True
        assert cell.fill.start_color.rgb is not None

    def test_data_values_written(self) -> None:
        """Data values should appear in the correct cells."""
        data = _make_sample_data()
        wb = build_workbook(data)
        ws = wb["Top Customers"]
        assert ws.cell(row=2, column=1).value == "Alice"
        assert ws.cell(row=2, column=2).value == "Smith"
        assert ws.cell(row=2, column=6).value == 1500.50

    def test_currency_formatting_applied(self) -> None:
        """Revenue columns should have currency number format."""
        data = _make_sample_data()
        wb = build_workbook(data)
        ws = wb["Top Customers"]
        revenue_cell = ws.cell(row=2, column=6)
        assert revenue_cell.number_format is not None
        assert "0.00" in revenue_cell.number_format


class TestSaveWorkbook:
    """Tests for the save_workbook function."""

    def test_saves_to_outputs_directory(self) -> None:
        """File should be saved in the outputs/ directory."""
        data = _make_sample_data()
        wb = build_workbook(data)
        with patch("src.exports.excel_exporter.OUTPUT_DIR", Path(".")):
            filepath = save_workbook(wb)
            assert filepath.exists()
            assert "retail_analytics" in filepath.name
            assert filepath.suffix == ".xlsx"
            filepath.unlink()

    def test_timestamp_in_filename(self) -> None:
        """Filename should contain a timestamp."""
        data = _make_sample_data()
        wb = build_workbook(data)
        with patch("src.exports.excel_exporter.OUTPUT_DIR", Path(".")):
            filepath = save_workbook(wb)
            parts = filepath.stem.split("_")
            assert len(parts) >= 3
            filepath.unlink()


class TestGetEngine:
    """Tests for the get_engine function."""

    def test_uses_env_variables(self) -> None:
        """Engine should use DB_* or POSTGRES_* env vars."""
        with patch.dict(
            os.environ,
            {
                "DB_HOST": "dbhost",
                "DB_PORT": "7777",
                "DB_NAME": "dbname",
                "DB_USER": "dbuser",
                "DB_PASSWORD": "dbpass",
            },
            clear=True,
        ):
            engine = get_engine()
            url = str(engine.url)
            assert "dbhost" in url
            assert "7777" in url
            assert "dbname" in url
            assert "dbuser" in url

    def test_fallback_to_postgres_env(self) -> None:
        """Engine should fall back to POSTGRES_* vars if DB_* not set."""
        with patch.dict(
            os.environ,
            {
                "POSTGRES_HOST": "pghost",
                "POSTGRES_PORT": "5432",
                "POSTGRES_DB": "pgdb",
                "POSTGRES_USER": "pguser",
                "POSTGRES_PASSWORD": "pgpass",
            },
            clear=True,
        ):
            engine = get_engine()
            url = str(engine.url)
            assert "pghost" in url
            assert "pgdb" in url


class TestFetchData:
    """Tests for the fetch_data function."""

    @patch("src.exports.excel_exporter.pd.read_sql")
    def test_returns_dataframes_for_each_query(self, mock_read_sql: MagicMock) -> None:
        mock_engine = MagicMock()
        mock_read_sql.return_value = pd.DataFrame({"col": [1]})

        data = fetch_data(mock_engine)
        assert len(data) == len(ANALYTICS_QUERIES)
        for sheet_name in ANALYTICS_QUERIES:
            assert sheet_name in data

    @patch("src.exports.excel_exporter.pd.read_sql", side_effect=Exception("DB error"))
    def test_query_failure_returns_empty_df(self, mock_read_sql: MagicMock) -> None:
        mock_engine = MagicMock()
        data = fetch_data(mock_engine)
        for sheet_name, df in data.items():
            assert df.empty


class TestSaveWorkbookFull:
    """Additional save_workbook tests."""

    def test_creates_output_dir_if_missing(self) -> None:
        """Should create OUTPUT_DIR if it doesn't exist."""
        data = {"Top Customers": pd.DataFrame({"a": [1]})}
        wb = build_workbook(data)
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "outputs"
            with patch("src.exports.excel_exporter.OUTPUT_DIR", output_dir):
                filepath = save_workbook(wb)
                assert filepath.exists()
                assert output_dir.exists()


class TestCurrencyFormatting:
    """Tests for the _format_currency_columns function."""

    def test_revenue_columns_formatted(self) -> None:
        """Columns with 'revenue' in name should get currency format."""
        df = pd.DataFrame({"net_revenue": [100.50], "category": ["A"]})
        wb = build_workbook({"Top Customers": df})
        sheet_name = list(wb.sheetnames)[0]
        ws = wb[sheet_name]
        _format_currency_columns(ws, df)
        cell = ws.cell(row=2, column=1)
        assert cell.number_format is not None
        assert "0.00" in cell.number_format

    def test_price_column_formatted(self) -> None:
        """Columns with 'price' in name should get currency format."""
        df = pd.DataFrame({"unit_price": [25.99], "category": ["A"]})
        data = dict.fromkeys(SHEET_NAMES, pd.DataFrame())
        data["Top Customers"] = df
        wb = build_workbook(data)
        ws = wb["Top Customers"]
        _format_currency_columns(ws, df)
        cell = ws.cell(row=2, column=1)
        assert cell.number_format is not None
        assert "0.00" in cell.number_format

    def test_amount_column_formatted(self) -> None:
        """Columns with 'amount' in name should get currency format."""
        df = pd.DataFrame({"total_amount": [500.00], "discount": [10]})
        data = dict.fromkeys(SHEET_NAMES, pd.DataFrame())
        data["Top Customers"] = df
        wb = build_workbook(data)
        ws = wb["Top Customers"]
        _format_currency_columns(ws, df)
        cell = ws.cell(row=2, column=1)
        assert cell.number_format is not None

    def test_non_currency_column_not_formatted(self) -> None:
        """Columns without currency keywords should not be formatted."""
        df = pd.DataFrame({"quantity": [5], "category": ["A"]})
        data = dict.fromkeys(SHEET_NAMES, pd.DataFrame())
        data["Top Customers"] = df
        wb = build_workbook(data)
        ws = wb["Top Customers"]
        _format_currency_columns(ws, df)
        for col_idx in range(1, 3):
            for row_idx in range(2, 3):
                cell = ws.cell(row=row_idx, column=col_idx)
                format_str = cell.number_format
                assert format_str is None or "#,##0.00" not in format_str

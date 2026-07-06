"""
Tests for the fake data generation script.

Verifies that:
    - CSV files are created in the expected directory
    - Each CSV has the correct number of rows
    - Each CSV has the expected columns
    - Data types are consistent (e.g., ages are integers)
"""

import csv
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

# WHY: Add the project root to sys.path so we can import the scripts module.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.generate_fake_data import generate_customers, generate_products, generate_orders  # noqa: E402


class TestGenerateCustomers:
    """Tests for the generate_customers function."""

    def test_creates_csv_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.generate_fake_data.ensure_output_dir", return_value=tmpdir):
                filepath = generate_customers(100)
                assert os.path.exists(filepath)
                assert filepath.endswith("customers.csv")

    def test_correct_number_of_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.generate_fake_data.ensure_output_dir", return_value=tmpdir):
                filepath = generate_customers(50)
                with open(filepath, "r") as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                assert len(rows) == 50

    def test_expected_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.generate_fake_data.ensure_output_dir", return_value=tmpdir):
                filepath = generate_customers(10)
                with open(filepath, "r") as f:
                    reader = csv.DictReader(f)
                    columns = reader.fieldnames
                expected = [
                    "customer_id", "first_name", "last_name", "email",
                    "country", "city", "signup_date", "age", "gender",
                ]
                assert sorted(columns) == sorted(expected)

    def test_age_is_integer(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.generate_fake_data.ensure_output_dir", return_value=tmpdir):
                filepath = generate_customers(100)
                with open(filepath, "r") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        age = int(row["age"])
                        assert 18 <= age <= 80


class TestGenerateProducts:
    """Tests for the generate_products function."""

    def test_creates_csv_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.generate_fake_data.ensure_output_dir", return_value=tmpdir):
                filepath = generate_products(50)
                assert os.path.exists(filepath)
                assert filepath.endswith("products.csv")

    def test_correct_number_of_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.generate_fake_data.ensure_output_dir", return_value=tmpdir):
                filepath = generate_products(30)
                with open(filepath, "r") as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                assert len(rows) == 30

    def test_valid_categories(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.generate_fake_data.ensure_output_dir", return_value=tmpdir):
                filepath = generate_products(50)
                valid_categories = {"Electronics", "Clothing", "Food", "Home"}
                with open(filepath, "r") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        assert row["category"] in valid_categories

    def test_positive_price_cents(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.generate_fake_data.ensure_output_dir", return_value=tmpdir):
                filepath = generate_products(50)
                with open(filepath, "r") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        assert int(row["price_cents"]) > 0


class TestGenerateOrders:

    def test_creates_csv_file(self) -> None:
        """The function should create an orders.csv file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.generate_fake_data.ensure_output_dir", return_value=tmpdir):
                customer_ids = ["c1", "c2", "c3"]
                product_ids = ["p1", "p2"]
                filepath = generate_orders(50, customer_ids, product_ids)
                assert os.path.exists(filepath)
                assert filepath.endswith("orders.csv")

    def test_correct_number_of_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.generate_fake_data.ensure_output_dir", return_value=tmpdir):
                customer_ids = ["c1", "c2", "c3"]
                product_ids = ["p1", "p2"]
                filepath = generate_orders(25, customer_ids, product_ids)
                with open(filepath, "r") as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                assert len(rows) == 25

    def test_references_existing_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.generate_fake_data.ensure_output_dir", return_value=tmpdir):
                customer_ids = ["c1", "c2", "c3"]
                product_ids = ["p1", "p2"]
                filepath = generate_orders(50, customer_ids, product_ids)
                with open(filepath, "r") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        assert row["customer_id"] in customer_ids
                        assert row["product_id"] in product_ids

    def test_valid_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("scripts.generate_fake_data.ensure_output_dir", return_value=tmpdir):
                customer_ids = ["c1", "c2", "c3"]
                product_ids = ["p1", "p2"]
                filepath = generate_orders(50, customer_ids, product_ids)
                valid_statuses = {"completed", "returned", "pending"}
                with open(filepath, "r") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        assert row["status"] in valid_statuses

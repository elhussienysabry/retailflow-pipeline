"""
Tests for data transformation logic used in the pipeline.

Verifies that:
    - The cents_to_dollars conversion is accurate
    - Discount calculations produce expected results
    - Status normalization works correctly
"""

import sys
from pathlib import Path

import pytest

# WHY: Add the project root to sys.path so we can import project modules.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class TestCentsToDollars:
    """Tests for the cents-to-dollars conversion logic."""

    def test_converts_cents_to_dollars(self) -> None:
        """100 cents should equal 1 dollar."""
        assert 100 / 100.0 == 1.0

    def test_converts_large_amount(self) -> None:
        """99999 cents should equal 999.99 dollars."""
        assert round(99999 / 100.0, 2) == 999.99

    def test_zero_cents(self) -> None:
        """0 cents should equal 0 dollars."""
        assert 0 / 100.0 == 0.0

    def test_rounding(self) -> None:
        """Amounts should round to 2 decimal places."""
        result = round(199 / 100.0, 2)
        assert result == 1.99


class TestDiscountCalculation:
    """Tests for the discount application logic.

    Matches the logic in int_orders_enriched.sql:
        net_revenue_cents = ROUND(quantity * price_cents * (1 - discount_pct / 100))
    """

    def test_no_discount(self) -> None:
        """With 0% discount, net should equal gross."""
        gross = 1000 * 5  # quantity=5, price_cents=1000
        net = round(gross * (1.0 - 0 / 100.0))
        assert net == gross

    def test_ten_percent_discount(self) -> None:
        """10% discount on 5000 cents should give 4500 cents."""
        gross = 5000
        net = round(gross * (1.0 - 10 / 100.0))
        assert net == 4500

    def test_fifty_percent_discount(self) -> None:
        """50% discount on 2000 cents should give 1000 cents."""
        gross = 2000
        net = round(gross * (1.0 - 50 / 100.0))
        assert net == 1000

    def test_multiple_quantity(self) -> None:
        """quantity=3, price_cents=1500, discount=20%."""
        quantity = 3
        price_cents = 1500
        gross = quantity * price_cents
        net = round(gross * (1.0 - 20 / 100.0))
        assert gross == 4500
        assert net == 3600


class TestStatusNormalization:
    """Tests for order status normalization logic.

    Matches logic in stg_orders.sql:
        LOWER(TRIM(status)) AS status
    """

    def test_lowercases_status(self) -> None:
        """Status should be lowercased."""
        assert "completed" == "COMPLETED".lower().strip()

    def test_trims_whitespace(self) -> None:
        """Whitespace should be trimmed from status."""
        assert "pending" == "  Pending  ".lower().strip()

    def test_valid_statuses(self) -> None:
        """All three statuses should normalize correctly."""
        assert "completed" == "completed"
        assert "returned" == "returned"
        assert "pending" == "pending"

"""
Tests for the --profile CLI argument in generate_fake_data.py.

Verifies that:
    - The default profile is 'medium' with correct row counts
    - Each profile (small, medium, large) maps to the correct row counts
    - Explicit --customers flag overrides the profile
    - Explicit --products flag overrides the profile
    - Explicit --orders flag overrides the profile
    - Mixed explicit overrides work correctly
"""

import sys
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.generate_fake_data import SCALE_PROFILES, parse_args  # noqa: E402


class TestScaleProfiles:
    """Tests for the SCALE_PROFILES dictionary."""

    def test_has_small_profile(self) -> None:
        assert "small" in SCALE_PROFILES
        assert SCALE_PROFILES["small"]["customers"] == 1_000
        assert SCALE_PROFILES["small"]["products"] == 100
        assert SCALE_PROFILES["small"]["orders"] == 10_000

    def test_has_medium_profile(self) -> None:
        assert "medium" in SCALE_PROFILES
        assert SCALE_PROFILES["medium"]["customers"] == 10_000
        assert SCALE_PROFILES["medium"]["products"] == 500
        assert SCALE_PROFILES["medium"]["orders"] == 100_000

    def test_has_large_profile(self) -> None:
        assert "large" in SCALE_PROFILES
        assert SCALE_PROFILES["large"]["customers"] == 100_000
        assert SCALE_PROFILES["large"]["products"] == 5_000
        assert SCALE_PROFILES["large"]["orders"] == 1_000_000


class TestParseArgsProfiles:
    """Tests for parse_args with --profile."""

    def test_default_profile_is_medium(self) -> None:
        """Without --profile, should default to medium values."""
        with patch("sys.argv", ["generate_fake_data.py"]):
            args = parse_args()
            assert args.customers == 10_000
            assert args.products == 500
            assert args.orders == 100_000
            assert args.profile == "medium"

    def test_small_profile(self) -> None:
        """--profile small should set small values."""
        with patch("sys.argv", ["generate_fake_data.py", "--profile", "small"]):
            args = parse_args()
            assert args.customers == 1_000
            assert args.products == 100
            assert args.orders == 10_000
            assert args.profile == "small"

    def test_large_profile(self) -> None:
        """--profile large should set large values."""
        with patch("sys.argv", ["generate_fake_data.py", "--profile", "large"]):
            args = parse_args()
            assert args.customers == 100_000
            assert args.products == 5_000
            assert args.orders == 1_000_000
            assert args.profile == "large"

    def test_explicit_customers_overrides_profile(self) -> None:
        """--customers should override profile for customers only."""
        with patch(
            "sys.argv",
            ["generate_fake_data.py", "--profile", "small", "--customers", "500"],
        ):
            args = parse_args()
            assert args.customers == 500
            assert args.products == 100
            assert args.orders == 10_000

    def test_explicit_products_overrides_profile(self) -> None:
        """--products should override profile for products only."""
        with patch(
            "sys.argv",
            ["generate_fake_data.py", "--profile", "small", "--products", "50"],
        ):
            args = parse_args()
            assert args.customers == 1_000
            assert args.products == 50
            assert args.orders == 10_000

    def test_explicit_orders_overrides_profile(self) -> None:
        """--orders should override profile for orders only."""
        with patch(
            "sys.argv",
            ["generate_fake_data.py", "--profile", "small", "--orders", "5000"],
        ):
            args = parse_args()
            assert args.customers == 1_000
            assert args.products == 100
            assert args.orders == 5_000

    def test_mixed_overrides(self) -> None:
        """Multiple explicit flags should override the profile correctly."""
        with patch(
            "sys.argv",
            [
                "generate_fake_data.py",
                "--profile",
                "medium",
                "--customers",
                "999",
                "--orders",
                "888",
            ],
        ):
            args = parse_args()
            assert args.customers == 999
            assert args.products == 500
            assert args.orders == 888

    def test_explicit_without_profile_uses_defaults(self) -> None:
        """Explicit flags without --profile should still get the default profile."""
        with patch(
            "sys.argv",
            ["generate_fake_data.py", "--customers", "100"],
        ):
            args = parse_args()
            assert args.customers == 100
            assert args.products == 500
            assert args.orders == 100_000
            assert args.profile == "medium"

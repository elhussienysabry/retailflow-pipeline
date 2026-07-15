"""
RetailFlow Pipeline — Data Generator Package

Re-exports the scale profiles and generation functions from the canonical
implementation at ``scripts.generate_fake_data`` so they are importable
from ``src.data_generator`` as well.

Usage:
    from src.data_generator import generate_customers, SCALE_PROFILES
"""

from scripts.generate_fake_data import (  # noqa: F401
    SCALE_PROFILES,
    generate_customers,
    generate_orders,
    generate_products,
)

__all__ = [
    "SCALE_PROFILES",
    "generate_customers",
    "generate_orders",  
    "generate_products",
]

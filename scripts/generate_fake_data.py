"""
RetailFlow Pipeline — Fake Data Generator
===========================================

Generates realistic fake retail data for the RetailFlow Pipeline:

    - customers.csv  (10,000 rows) — customer demographics
    - products.csv   (500 rows)    — product catalog
    - orders.csv     (100,000 rows) — transaction history

Usage:
    python scripts/generate_fake_data.py
    python scripts/generate_fake_data.py --customers 5000 --products 200 --orders 50000

The output files are written to data/raw/.
"""

import argparse
import csv
import logging
import os
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Tuple

from faker import Faker

# WHY: Use a module-level logger so log messages show the script name.
logger = logging.getLogger(__name__)

# Constants for data generation
DEFAULT_NUM_CUSTOMERS = 10_000
DEFAULT_NUM_PRODUCTS = 500
DEFAULT_NUM_ORDERS = 100_000
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

# WHY: Seed the Faker generator for reproducible results across runs.
fake = Faker()
Faker.seed(42)

# Product categories and their weightings for realistic distribution
PRODUCT_CATEGORIES: List[Tuple[str, float]] = [
    ("Electronics", 0.25),
    ("Clothing", 0.35),
    ("Food", 0.15),
    ("Home", 0.25),
]

# Order statuses with realistic probability distribution
ORDER_STATUSES: List[Tuple[str, float]] = [
    ("completed", 0.80),
    ("returned", 0.10),
    ("pending", 0.10),
]

SUPPLIER_COUNTRIES: List[str] = [
    "China", "USA", "Germany", "India", "Vietnam",
    "Mexico", "Japan", "South Korea", "Italy", "Brazil",
]


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments to override default row counts."""
    parser = argparse.ArgumentParser(
        description="Generate fake retail data for the RetailFlow Pipeline."
    )
    parser.add_argument(
        "--customers",
        type=int,
        default=DEFAULT_NUM_CUSTOMERS,
        help=f"Number of customer records (default: {DEFAULT_NUM_CUSTOMERS})",
    )
    parser.add_argument(
        "--products",
        type=int,
        default=DEFAULT_NUM_PRODUCTS,
        help=f"Number of product records (default: {DEFAULT_NUM_PRODUCTS})",
    )
    parser.add_argument(
        "--orders",
        type=int,
        default=DEFAULT_NUM_ORDERS,
        help=f"Number of order records (default: {DEFAULT_NUM_ORDERS})",
    )
    return parser.parse_args()


def ensure_output_dir() -> str:
    """Create the output directory if it doesn't exist.

    Returns:
        The absolute path to the output directory.
    """
    abs_path = os.path.abspath(OUTPUT_DIR)
    os.makedirs(abs_path, exist_ok=True)
    logger.info("Output directory: %s", abs_path)
    return abs_path


def generate_customers(num_customers: int) -> str:
    """Generate a CSV of customer records.

    Args:
        num_customers: Number of customer rows to generate.

    Returns:
        The absolute file path of the generated CSV.
    """
    output_dir = ensure_output_dir()
    filepath = os.path.join(output_dir, "customers.csv")

    fieldnames = [
        "customer_id", "first_name", "last_name", "email",
        "country", "city", "signup_date", "age", "gender",
    ]

    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for _ in range(num_customers):
            writer.writerow(
                {
                    "customer_id": str(uuid.uuid4()),
                    "first_name": fake.first_name(),
                    "last_name": fake.last_name(),
                    "email": fake.email(),
                    "country": fake.country(),
                    "city": fake.city(),
                    "signup_date": fake.date_between(
                        start_date="-3y", end_date="today"
                    ).isoformat(),
                    "age": fake.random_int(min=18, max=80),
                    "gender": fake.random_element(elements=("Male", "Female", "Non-binary")),
                }
            )

    logger.info("Generated %d customers → %s", num_customers, filepath)
    return filepath


def generate_products(num_products: int) -> str:
    """Generate a CSV of product records.

    Args:
        num_products: Number of product rows to generate.

    Returns:
        The absolute file path of the generated CSV.
    """
    output_dir = ensure_output_dir()
    filepath = os.path.join(output_dir, "products.csv")

    fieldnames = [
        "product_id", "name", "category",
        "price_cents", "stock_quantity", "supplier_country",
    ]

    # WHY: Pre-compute categories map so each product gets a weighted random category.
    categories = [cat for cat, _ in PRODUCT_CATEGORIES]
    weights = [w for _, w in PRODUCT_CATEGORIES]

    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for _ in range(num_products):
            writer.writerow(
                {
                    "product_id": str(uuid.uuid4()),
                    "name": fake.word().capitalize() + " " + fake.word().capitalize(),
                    "category": random.choices(categories, weights=weights, k=1)[0],
                    "price_cents": fake.random_int(min=99, max=99999),
                    "stock_quantity": fake.random_int(min=0, max=1000),
                    "supplier_country": fake.random_element(SUPPLIER_COUNTRIES),
                }
            )

    logger.info("Generated %d products → %s", num_products, filepath)
    return filepath


def generate_orders(
    num_orders: int,
    customer_ids: List[str],
    product_ids: List[str],
) -> str:
    """Generate a CSV of order records linked to existing customers and products.

    Args:
        num_orders: Number of order rows to generate.
        customer_ids: List of valid customer UUIDs to reference.
        product_ids: List of valid product UUIDs to reference.

    Returns:
        The absolute file path of the generated CSV.
    """
    output_dir = ensure_output_dir()
    filepath = os.path.join(output_dir, "orders.csv")

    fieldnames = [
        "order_id", "customer_id", "product_id", "quantity",
        "order_date", "status", "discount_pct", "shipping_days",
    ]

    statuses = [s for s, _ in ORDER_STATUSES]
    status_weights = [w for _, w in ORDER_STATUSES]

    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for _ in range(num_orders):
            writer.writerow(
                {
                    "order_id": str(uuid.uuid4()),
                    "customer_id": fake.random_element(customer_ids),
                    "product_id": fake.random_element(product_ids),
                    "quantity": fake.random_int(min=1, max=10),
                    "order_date": fake.date_between(
                        start_date="-2y", end_date="today"
                    ).isoformat(),
                    "status": random.choices(statuses, weights=status_weights, k=1)[0],
                    "discount_pct": fake.random_int(min=0, max=50),
                    "shipping_days": fake.random_int(min=1, max=14),
                }
            )

    logger.info("Generated %d orders → %s", num_orders, filepath)
    return filepath


def main() -> None:
    """Main entry point: parse args, generate all datasets."""
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info(
        "Starting data generation: %d customers, %d products, %d orders",
        args.customers,
        args.products,
        args.orders,
    )

    start_time = datetime.now()

    try:
        customers_path = generate_customers(args.customers)
        products_path = generate_products(args.products)

        # WHY: Read back generated IDs to ensure referential integrity in orders.
        customer_ids = _read_ids(customers_path, "customer_id")
        product_ids = _read_ids(products_path, "product_id")

        orders_path = generate_orders(args.orders, customer_ids, product_ids)

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(
            "Data generation complete! (%d customers, %d products, %d orders) in %.2f seconds",
            args.customers,
            args.products,
            args.orders,
            elapsed,
        )
        logger.info("Files created:")
        logger.info("  %s", customers_path)
        logger.info("  %s", products_path)
        logger.info("  %s", orders_path)

    except Exception as exc:
        logger.error("Data generation failed: %s", exc, exc_info=True)
        raise


def _read_ids(filepath: str, column: str) -> List[str]:
    """Read a single column of IDs from a CSV file.

    Args:
        filepath: Path to the CSV file.
        column: Name of the column to extract.

    Returns:
        A list of ID strings.
    """
    ids: List[str] = []
    with open(filepath, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ids.append(row[column])
    return ids


if __name__ == "__main__":
    main()

"""
RetailFlow Pipeline — Streamlit Dashboard
==========================================

A clean, lightweight dashboard that shows real-time analytics from the
RetailFlow data warehouse:

    - KPI cards: Total Orders, Total Net Revenue, Active Customers
    - Line chart: Monthly sales trend
    - Bar chart: Category performance
    - Data table: Top 10 customers

Usage:
    streamlit run src/dashboard/app.py

Environment variables:
    DB_HOST / POSTGRES_HOST
    DB_PORT / POSTGRES_PORT
    DB_NAME / POSTGRES_DB
    DB_USER / POSTGRES_USER
    DB_PASSWORD / POSTGRES_PASSWORD
"""

import logging
import os
from typing import Any, Tuple

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

PAGE_TITLE = "RetailFlow Pipeline Dashboard"
PAGE_ICON = ":bar_chart:"

st.set_page_config(page_title=PAGE_TITLE, page_icon=PAGE_ICON, layout="wide")

QUERY_KPIS = """
    SELECT
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(net_revenue_dollars), 2) AS total_net_revenue,
        COUNT(DISTINCT customer_id) AS active_customers
    FROM marts.fct_orders
    WHERE status = 'completed'
"""

QUERY_MONTHLY_SALES = """
    SELECT
        DATE_TRUNC('month', order_date)::DATE AS month,
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(net_revenue_dollars), 2) AS net_revenue
    FROM marts.fct_orders
    WHERE status = 'completed'
    GROUP BY DATE_TRUNC('month', order_date)
    ORDER BY month
"""

QUERY_CATEGORY_PERF = """
    SELECT
        p.category,
        ROUND(SUM(f.net_revenue_dollars), 2) AS total_net_revenue,
        SUM(f.quantity) AS total_units_sold
    FROM marts.dim_products AS p
    INNER JOIN marts.fct_orders AS f
        ON p.product_id = f.product_id
    WHERE f.status = 'completed'
    GROUP BY p.category
    ORDER BY total_net_revenue DESC
"""

QUERY_TOP_CUSTOMERS = """
    SELECT
        c.first_name,
        c.last_name,
        c.email,
        c.country,
        c.city,
        ROUND(SUM(f.net_revenue_dollars), 2) AS total_net_revenue
    FROM marts.dim_customers AS c
    INNER JOIN marts.fct_orders AS f
        ON c.customer_id = f.customer_id
    WHERE f.status = 'completed'
    GROUP BY c.customer_id, c.first_name, c.last_name,
             c.email, c.country, c.city
    ORDER BY total_net_revenue DESC
    LIMIT 10
"""


def _get_env(key: str, fallback: str) -> str:
    return os.getenv(key) or os.getenv(fallback) or ""


@st.cache_resource(show_spinner="Connecting to database...")
def get_engine() -> Engine:
    """Create (and cache) the SQLAlchemy engine.

    Returns:
        A SQLAlchemy Engine connected to PostgreSQL.
    """
    host = _get_env("DB_HOST", "POSTGRES_HOST") or "localhost"
    port = _get_env("DB_PORT", "POSTGRES_PORT") or "5432"
    database = _get_env("DB_NAME", "POSTGRES_DB") or "retailflow"
    user = _get_env("DB_USER", "POSTGRES_USER") or "retailflow_user"
    password = _get_env("DB_PASSWORD", "POSTGRES_PASSWORD") or "retailflow_pass"

    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(conn_str, echo=False)


@st.cache_data(ttl=300, show_spinner="Fetching KPI data...")
def fetch_kpis(_engine: Engine) -> Tuple[int, float, int]:
    """Fetch total orders, total net revenue, and active customers.

    Args:
        _engine: SQLAlchemy Engine (prefixed with _ for Streamlit hashing).

    Returns:
        (total_orders, total_net_revenue, active_customers).
    """
    with _engine.connect() as conn:
        row = conn.execute(text(QUERY_KPIS)).one()
    return row.total_orders, row.total_net_revenue, row.active_customers


@st.cache_data(ttl=300, show_spinner="Fetching monthly sales...")
def fetch_monthly_sales(_engine: Engine) -> pd.DataFrame:
    """Fetch monthly sales trend data.

    Args:
        _engine: SQLAlchemy Engine.

    Returns:
        DataFrame with month, total_orders, net_revenue.
    """
    return pd.read_sql(text(QUERY_MONTHLY_SALES), _engine)


@st.cache_data(ttl=300, show_spinner="Fetching category performance...")
def fetch_category_perf(_engine: Engine) -> pd.DataFrame:
    """Fetch category performance data.

    Args:
        _engine: SQLAlchemy Engine.

    Returns:
        DataFrame with category, total_net_revenue, total_units_sold.
    """
    return pd.read_sql(text(QUERY_CATEGORY_PERF), _engine)


@st.cache_data(ttl=300, show_spinner="Fetching top customers...")
def fetch_top_customers(_engine: Engine) -> pd.DataFrame:
    """Fetch top 10 customers by revenue.

    Args:
        _engine: SQLAlchemy Engine.

    Returns:
        DataFrame with customer details and total_net_revenue.
    """
    return pd.read_sql(text(QUERY_TOP_CUSTOMERS), _engine)


def render_kpi_cards(total_orders: int, total_revenue: float, active_customers: int) -> None:
    """Render three KPI metric cards in a row.

    Args:
        total_orders: Total completed order count.
        total_revenue: Total net revenue in dollars.
        active_customers: Distinct customer count.
    """
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Total Orders", value=f"{total_orders:,}")
    with col2:
        st.metric(label="Total Net Revenue", value=f"${total_revenue:,.2f}")
    with col3:
        st.metric(label="Active Customers", value=f"{active_customers:,}")


def render_monthly_chart(df: pd.DataFrame) -> None:
    """Render a line chart of monthly sales.

    Args:
        df: DataFrame with month, total_orders, net_revenue.
    """
    if df.empty:
        st.info("No monthly sales data available.")
        return
    st.subheader("Monthly Sales Trend")
    chart_df = df.set_index("month")
    st.line_chart(chart_df[["net_revenue", "total_orders"]], height=350)


def render_category_chart(df: pd.DataFrame) -> None:
    """Render a bar chart of category performance.

    Args:
        df: DataFrame with category, total_net_revenue, total_units_sold.
    """
    if df.empty:
        st.info("No category performance data available.")
        return
    st.subheader("Category Performance")
    chart_df = df.set_index("category")
    st.bar_chart(chart_df[["total_net_revenue", "total_units_sold"]], height=350)


def render_top_customers(df: pd.DataFrame) -> None:
    """Render a data table of top customers.

    Args:
        df: DataFrame with customer details and total_net_revenue.
    """
    if df.empty:
        st.info("No top customer data available.")
        return
    st.subheader("Top Customers by Revenue")
    display_df = df.copy()
    display_df["total_net_revenue"] = display_df["total_net_revenue"].apply(
        lambda x: f"${x:,.2f}"
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def main() -> None:
    """Main entry point: build and render the Streamlit dashboard."""
    st.title(PAGE_TITLE)
    st.markdown("Real-time analytics from the RetailFlow data warehouse.")
    st.divider()

    if st.button(":arrows_counterclockwise: Refresh Data", type="primary"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()

    try:
        engine = get_engine()
    except Exception as exc:
        st.error(f"Could not connect to the database: {exc}")
        st.info(
            "Make sure PostgreSQL is running and your .env file is configured. "
            "Run 'make run && make load-data && make dbt-run' first."
        )
        return

    try:
        total_orders, total_revenue, active_customers = fetch_kpis(engine)
        render_kpi_cards(total_orders, total_revenue, active_customers)
    except Exception as exc:
        st.error(f"Failed to fetch KPIs: {exc}")
        return

    col_left, col_right = st.columns(2)

    with col_left:
        try:
            monthly_df = fetch_monthly_sales(engine)
            render_monthly_chart(monthly_df)
        except Exception as exc:
            st.error(f"Failed to load monthly sales: {exc}")

    with col_right:
        try:
            category_df = fetch_category_perf(engine)
            render_category_chart(category_df)
        except Exception as exc:
            st.error(f"Failed to load category data: {exc}")

    try:
        top_customers_df = fetch_top_customers(engine)
        render_top_customers(top_customers_df)
    except Exception as exc:
        st.error(f"Failed to load top customers: {exc}")


if __name__ == "__main__":
    main()

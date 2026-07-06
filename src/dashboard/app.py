"""
RetailFlow Pipeline — Streamlit Dashboard
==========================================

A clean, lightweight dashboard that shows real-time analytics from the
RetailFlow data warehouse.

Usage:
    streamlit run src/dashboard/app.py          # using .venv
    .venv/Scripts/streamlit run src/dashboard/app.py

Environment variables:
    DB_HOST / POSTGRES_HOST
    DB_PORT / POSTGRES_PORT
    DB_NAME / POSTGRES_DB
    DB_USER / POSTGRES_USER
    DB_PASSWORD / POSTGRES_PASSWORD
"""

import logging
import os
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

PAGE_TITLE = "RetailFlow Pipeline Dashboard"
PAGE_ICON = ":bar_chart:"

st.set_page_config(page_title=PAGE_TITLE, page_icon=PAGE_ICON, layout="wide")
st.session_state.setdefault("last_refresh", datetime.now(timezone.utc))

DB_MISSING_HINT = (
    "The dbt models (`marts.fct_orders`, `marts.dim_customers`, etc.) "
    "have not been materialized yet. "
    "Please run the following commands in your terminal:\n\n"
    "```bash\n"
    ".venv-dbt\\Scripts\\dbt run --project-dir dbt\n"
    "```\n\n"
    "Then re-launch this dashboard."
)

QUERY_KPIS = """
    SELECT
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(net_revenue_dollars), 2) AS total_net_revenue,
        COUNT(DISTINCT customer_id) AS active_customers
    FROM marts.fct_orders
    WHERE status = 'completed'
"""

QUERY_EXTRA_KPIS = """
    SELECT
        ROUND(
            SUM(net_revenue_dollars) / NULLIF(COUNT(DISTINCT order_id), 0), 2
        ) AS avg_order_value,
        COUNT(DISTINCT CASE WHEN status != 'completed' THEN order_id END) AS non_completed,
        COUNT(DISTINCT order_id) AS all_orders
    FROM marts.fct_orders
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

QUERY_CUSTOMER_GEO = """
    SELECT
        c.country,
        COUNT(DISTINCT c.customer_id) AS customer_count,
        ROUND(SUM(f.net_revenue_dollars), 2) AS total_revenue
    FROM marts.dim_customers AS c
    INNER JOIN marts.fct_orders AS f
        ON c.customer_id = f.customer_id
    WHERE f.status = 'completed'
    GROUP BY c.country
    ORDER BY total_revenue DESC
    LIMIT 15
"""

QUERY_CATEGORIES = """
    SELECT DISTINCT category FROM marts.dim_products ORDER BY category
"""

QUERY_FRESHNESS = """
    SELECT MAX(order_date) AS last_order_date FROM marts.fct_orders
"""


def _get_env(key: str, fallback: str) -> str:
    return os.getenv(key) or os.getenv(fallback) or ""


@st.cache_resource(show_spinner="Connecting to database...")
def get_engine() -> Engine:
    host = _get_env("DB_HOST", "POSTGRES_HOST") or "localhost"
    port = _get_env("DB_PORT", "POSTGRES_PORT") or "5432"
    database = _get_env("DB_NAME", "POSTGRES_DB") or "retailflow"
    user = _get_env("DB_USER", "POSTGRES_USER") or "retailflow_user"
    password = _get_env("DB_PASSWORD", "POSTGRES_PASSWORD") or "retailflow_pass"
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(conn_str, echo=False)


def _is_undefined_table(exc: Exception) -> bool:
    msg = str(exc).lower()
    keywords = (
        "does not exist",
        "doesn't exist",
        "relation",
        "undefined table",
        "undefined_table",
        "42p01",
    )
    return any(kw in msg for kw in keywords)


def _safe_query(
    engine: Engine,
    query: str,
    label: str,
    warn_in_expander: bool = False,
) -> Optional[pd.DataFrame]:
    try:
        return pd.read_sql(text(query), engine)
    except Exception as exc:
        if _is_undefined_table(exc):
            msg = f"**{label}** — dbt models not found."
            if warn_in_expander:
                st.warning(msg, icon=":material/warning:")
                with st.expander("Show technical details"):
                    st.code(traceback.format_exc(), language="text")
                    st.markdown(DB_MISSING_HINT)
            return None
        raise


@st.cache_data(ttl=300, show_spinner="Fetching KPI data...")
def fetch_kpis(_engine: Engine) -> Tuple[int, float, int]:
    try:
        row = _engine.connect().execute(text(QUERY_KPIS)).one()
        return row.total_orders, row.total_net_revenue, row.active_customers
    except Exception as exc:
        if _is_undefined_table(exc):
            with st.expander("Show technical details"):
                st.code(traceback.format_exc(), language="text")
                st.markdown(DB_MISSING_HINT)
            return 0, 0.0, 0
        raise


@st.cache_data(ttl=300, show_spinner="Fetching extra KPIs...")
def fetch_extra_kpis(_engine: Engine) -> Tuple[float, int, int]:
    try:
        row = _engine.connect().execute(text(QUERY_EXTRA_KPIS)).one()
        return row.avg_order_value, row.non_completed, row.all_orders
    except Exception as exc:
        if _is_undefined_table(exc):
            return 0.0, 0, 0
        raise


@st.cache_data(ttl=300, show_spinner="Fetching monthly sales...")
def fetch_monthly_sales(_engine: Engine) -> pd.DataFrame:
    r = _safe_query(_engine, QUERY_MONTHLY_SALES, "Monthly Sales")
    return r if r is not None else pd.DataFrame()


@st.cache_data(ttl=300, show_spinner="Fetching category performance...")
def fetch_category_perf(_engine: Engine) -> pd.DataFrame:
    r = _safe_query(_engine, QUERY_CATEGORY_PERF, "Category Performance")
    return r if r is not None else pd.DataFrame()


@st.cache_data(ttl=300, show_spinner="Fetching top customers...")
def fetch_top_customers(_engine: Engine) -> pd.DataFrame:
    r = _safe_query(_engine, QUERY_TOP_CUSTOMERS, "Top Customers")
    return r if r is not None else pd.DataFrame()


@st.cache_data(ttl=300, show_spinner="Fetching customer geography...")
def fetch_customer_geo(_engine: Engine) -> pd.DataFrame:
    r = _safe_query(_engine, QUERY_CUSTOMER_GEO, "Customer Geography")
    return r if r is not None else pd.DataFrame()


@st.cache_data(ttl=300, show_spinner="Fetching categories...")
def fetch_categories(_engine: Engine) -> List[str]:
    r = _safe_query(_engine, QUERY_CATEGORIES, "Categories")
    return r["category"].tolist() if r is not None else []


@st.cache_data(ttl=300, show_spinner="Checking data freshness...")
def fetch_freshness(_engine: Engine) -> Optional[str]:
    r = _safe_query(_engine, QUERY_FRESHNESS, "Freshness")
    if r is not None and not r.empty:
        return str(r["last_order_date"].iloc[0])
    return None


def render_refresh_timestamp() -> None:
    now = datetime.now(timezone.utc)
    st.session_state.last_refresh = now
    st.caption(f":material/schedule: Last refreshed: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")


def render_kpi_cards(
    total_orders: int,
    total_revenue: float,
    active_customers: int,
    avg_order_value: float,
    return_rate: float,
    all_orders: int,
    non_completed: int,
) -> None:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Total Orders", value=f"{total_orders:,}", help="Completed orders")
    with col2:
        st.metric(label="Total Net Revenue", value=f"${total_revenue:,.2f}", help="Revenue from completed orders")
    with col3:
        st.metric(label="Active Customers", value=f"{active_customers:,}", help="Distinct customers who ordered")

    col4, col5, col6 = st.columns(3)
    with col4:
        st.metric(label="Avg Order Value", value=f"${avg_order_value:,.2f}", help="Revenue per completed order")
    with col5:
        returned = non_completed if all_orders > 0 else 0
        st.metric(label="Returned / Pending", value=f"{returned:,}", help="Non-completed orders")
    with col6:
        pct = return_rate
        st.metric(label="Return Rate", value=f"{pct:.1f}%", help="% of orders not completed")


def render_monthly_chart(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("No monthly sales data available.")
        return
    fig = px.line(
        df,
        x="month",
        y=["net_revenue", "total_orders"],
        title="Monthly Sales Trend",
        labels={"value": "Amount", "month": "", "variable": "Metric"},
        color_discrete_map={"net_revenue": "#00c853", "total_orders": "#2979ff"},
        height=350,
    )
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=10, r=10, t=40, b=10),
        hovermode="x unified",
    )
    fig.update_yaxes(tickprefix="$", row=1, col=1)
    st.plotly_chart(fig, use_container_width=True)


def render_category_chart(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("No category performance data available.")
        return
    fig = px.bar(
        df,
        x="category",
        y=["total_net_revenue", "total_units_sold"],
        title="Category Performance",
        barmode="group",
        labels={"value": "Amount", "category": "", "variable": "Metric"},
        color_discrete_map={"total_net_revenue": "#00c853", "total_units_sold": "#ff6d00"},
        height=350,
    )
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=10, r=10, t=40, b=10),
        hovermode="x unified",
    )
    fig.update_yaxes(tickprefix="$", row=1, col=1)
    st.plotly_chart(fig, use_container_width=True)


def render_geo_chart(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("No geography data available.")
        return
    fig = px.bar(
        df,
        x="total_revenue",
        y="country",
        title="Revenue by Country (Top 15)",
        orientation="h",
        labels={"total_revenue": "Total Revenue", "country": ""},
        color="customer_count",
        color_continuous_scale="Blues",
        height=400,
    )
    fig.update_layout(margin=dict(l=10, r=10, t=40, b=10))
    fig.update_xaxes(tickprefix="$")
    st.plotly_chart(fig, use_container_width=True)


def render_top_customers(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("No top customer data available.")
        return
    display_df = df.copy()
    display_df["total_net_revenue"] = display_df["total_net_revenue"].apply(
        lambda x: f"${x:,.2f}"
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def render_sidebar() -> Dict[str, Any]:
    with st.sidebar:
        st.markdown(f"### {PAGE_TITLE}")
        st.markdown("Real-time analytics from the RetailFlow data warehouse.")
        st.divider()

        refresh_col, auto_col = st.columns([1, 2])
        with refresh_col:
            if st.button(":arrows_counterclockwise: Refresh", type="primary", use_container_width=True):
                st.cache_data.clear()
                st.cache_resource.clear()
                st.rerun()
        with auto_col:
            auto = st.checkbox("Auto-refresh", value=st.session_state.get("auto_refresh", False), key="auto_refresh")
            if auto:
                interval = st.selectbox(
                    "Interval",
                    options=[30, 60, 120, 300],
                    format_func=lambda s: f"{s // 60}m {s % 60}s" if s >= 60 else f"{s}s",
                    index=1,
                    key="refresh_interval",
                )
            else:
                interval = 0

        st.divider()
        st.markdown("**Filters**")

        categories = st.session_state.get("categories", [])
        selected_categories = st.multiselect(
            "Category",
            options=categories,
            default=categories,
            placeholder="All categories",
        )

        st.divider()
        st.markdown("**Export**")
        if st.button(":material/download: Export to Excel", use_container_width=True):
            _run_export()
        st.markdown(
            ":material/info: Exports styled analytics workbook to `outputs/`"
        )

        st.divider()
        freshness = st.session_state.get("freshness", None)
        if freshness:
            st.caption(f":material/calendar_month: Data up to: **{freshness}**")

        now = st.session_state.get("last_refresh", datetime.now(timezone.utc))
        st.caption(f":material/schedule: Dashboard refreshed: {now.strftime('%H:%M:%S')} UTC")

        st.divider()
        with st.expander(":material/description: About"):
            st.markdown(
                "**RetailFlow Pipeline**\n\n"
                "End-to-end retail analytics pipeline:\n"
                "- Faker data generation\n"
                "- PostgreSQL warehouse\n"
                "- dbt transformations\n"
                "- Streamlit dashboard\n"
                "- Excel export"
            )

    return {"categories": selected_categories}


def _run_export() -> None:
    with st.spinner("Generating Excel export..."):
        try:
            result = subprocess.run(
                [sys.executable, "-m", "src.exports.excel_exporter"],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode == 0:
                for line in result.stdout.splitlines():
                    if "saved" in line.lower() or "complete" in line.lower():
                        st.success(f":material/check_circle: Export complete — {line.strip()}")
                        return
                st.success(":material/check_circle: Export completed successfully.")
            else:
                st.error(f"Export failed:\n```\n{result.stderr[:500]}\n```")
        except subprocess.TimeoutExpired:
            st.error("Export timed out after 60 seconds.")
        except Exception as e:
            st.error(f"Export error: {e}")


def main() -> None:
    filters = render_sidebar()

    total_orders, total_revenue, active_customers = fetch_kpis(get_engine())
    avg_order_value, non_completed, all_orders = fetch_extra_kpis(get_engine())
    return_rate = (non_completed / all_orders * 100) if all_orders > 0 else 0.0

    render_kpi_cards(
        total_orders, total_revenue, active_customers,
        avg_order_value, return_rate, all_orders, non_completed,
    )

    st.divider()

    col_left, col_right = st.columns(2)
    with col_left:
        monthly_df = fetch_monthly_sales(get_engine())
        render_monthly_chart(monthly_df)
    with col_right:
        category_df = fetch_category_perf(get_engine())
        render_category_chart(category_df)

    col_geo, col_table = st.columns(2)
    with col_geo:
        geo_df = fetch_customer_geo(get_engine())
        render_geo_chart(geo_df)
    with col_table:
        top_customers_df = fetch_top_customers(get_engine())
        st.subheader("Top Customers by Revenue")
        render_top_customers(top_customers_df)

    freshness = fetch_freshness(get_engine())
    st.session_state.freshness = freshness

    render_refresh_timestamp()

    if st.session_state.get("auto_refresh", False):
        interval = st.session_state.get("refresh_interval", 60)
        st.rerun(ttl=interval * 1000)


if __name__ == "__main__":
    main()

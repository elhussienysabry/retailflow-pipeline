"""
RetailFlow Pipeline — ML Demand Forecasting Module
====================================================

Pulls daily historical revenue from ``marts.fct_orders``, fits an ARIMA
model per product category, forecasts the next 30 days, and writes
predictions to ``marts.fct_demand_forecast``.

Usage:
    python scripts/generate_forecast.py

Environment variables (from .env):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import logging
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

FORECAST_TABLE = "marts.fct_demand_forecast"
FORECAST_DAYS = 30
MIN_HISTORY_DAYS = 10

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

HISTORY_SQL = """
    SELECT
        f.order_date,
        p.category AS product_category,
        SUM(f.net_revenue_dollars) AS daily_net_revenue
    FROM marts.fct_orders AS f
    INNER JOIN marts.dim_products AS p
        ON f.product_id = p.product_id
    WHERE f.status = 'completed'
    GROUP BY f.order_date, p.category
    ORDER BY f.order_date, p.category
"""

CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS {table} (
        forecast_date         DATE NOT NULL,
        product_category      TEXT NOT NULL,
        forecasted_net_revenue NUMERIC(12,2) NOT NULL,
        model_generated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (forecast_date, product_category)
    )
"""

CLEAR_SQL = "DELETE FROM {table}"


def get_engine() -> Engine:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "retailflow")
    user = os.getenv("POSTGRES_USER", "retailflow_user")
    password = os.getenv("POSTGRES_PASSWORD", "retailflow_pass")
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(conn_str, echo=False)


def fetch_history(engine: Engine) -> pd.DataFrame:
    df = pd.read_sql(text(HISTORY_SQL), engine)
    if df.empty:
        logger.warning("No historical data found in marts.fct_orders.")
        return df
    df["order_date"] = pd.to_datetime(df["order_date"])
    logger.info(
        "Fetched %d historical rows across %d categories.",
        len(df),
        df["product_category"].nunique(),
    )
    return df


def _arima_forecast(series: pd.Series, steps: int) -> Optional[pd.Series]:
    try:
        from statsmodels.tsa.arima.model import ARIMA
    except ImportError:
        logger.error("statsmodels is not installed. Run: pip install statsmodels")
        return None

    ts = series.dropna().sort_index()
    if len(ts) < MIN_HISTORY_DAYS:
        logger.warning(
            "Insufficient history (%d days, need %d) — skipping ARIMA.",
            len(ts),
            MIN_HISTORY_DAYS,
        )
        return None

    try:
        model = ARIMA(ts, order=(1, 1, 1))
        fitted = model.fit()
        forecast = fitted.forecast(steps=steps)
        return forecast
    except Exception as exc:
        logger.warning("ARIMA fit failed for series (len=%d): %s", len(ts), exc)
        return None


def _fallback_ma_forecast(series: pd.Series, steps: int) -> Optional[pd.Series]:
    ts = series.dropna()
    if len(ts) < 3:
        return None
    window = min(7, len(ts))
    ma = ts.rolling(window=window).mean().iloc[-1]
    if pd.isna(ma) or ma <= 0:
        ma = ts.iloc[-1]
    return pd.Series([ma] * steps)


def generate_forecasts(history: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict] = []
    now = datetime.now(timezone.utc)
    model_generated_at = now.isoformat()

    for category, grp in history.groupby("product_category"):
        grp = grp.sort_values("order_date").set_index("order_date")
        daily = grp["daily_net_revenue"].asfreq("D")
        daily = daily.fillna(0)

        forecast = _arima_forecast(daily, FORECAST_DAYS)
        if forecast is None:
            forecast = _fallback_ma_forecast(daily, FORECAST_DAYS)
        if forecast is None:
            logger.warning(
                "No forecast possible for category '%s' — skipping.", category
            )
            continue

        last_date = daily.index[-1]
        future_dates = pd.date_range(
            start=last_date + pd.Timedelta(days=1),
            periods=len(forecast),
            freq="D",
        )
        for date_val, pred_val in zip(future_dates, forecast):
            rows.append(
                {
                    "forecast_date": date_val.date(),
                    "product_category": category,
                    "forecasted_net_revenue": round(max(float(pred_val), 0.0), 2),
                    "model_generated_at": model_generated_at,
                }
            )

        logger.info(
            "Category '%s': forecast %d days (ARIMA=%s, history=%d days)",
            category,
            len(forecast),
            forecast is not None and len(forecast) > 0,
            len(daily),
        )
    return pd.DataFrame(rows)


def ensure_table(engine: Engine) -> None:
    ddl = CREATE_TABLE_SQL.format(table=FORECAST_TABLE)
    with engine.connect() as conn:
        conn.exec_driver_sql(ddl)
        conn.commit()
    logger.info("Ensured table %s exists.", FORECAST_TABLE)


def write_forecasts(engine: Engine, forecasts: pd.DataFrame) -> int:
    if forecasts.empty:
        logger.info("No forecasts to write.")
        return 0

    with engine.connect() as conn:
        conn.exec_driver_sql(CLEAR_SQL.format(table=FORECAST_TABLE))
        conn.commit()

    forecasts.to_sql(
        FORECAST_TABLE.split(".")[1],
        engine,
        schema=FORECAST_TABLE.split(".")[0],
        if_exists="append",
        index=False,
        method="multi",
    )
    count = len(forecasts)
    logger.info("Wrote %d forecast rows to %s.", count, FORECAST_TABLE)
    return count


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    engine = get_engine()
    ensure_table(engine)

    history = fetch_history(engine)
    if history.empty:
        logger.warning("No historical data — forecast table will be empty.")
        return

    forecasts = generate_forecasts(history)
    write_forecasts(engine, forecasts)

    logger.info("Demand forecasting complete.")


if __name__ == "__main__":
    main()

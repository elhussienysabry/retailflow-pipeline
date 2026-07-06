from src.dashboard.app import get_engine, _safe_query, QUERY_KPIS, QUERY_MONTHLY_SALES, QUERY_CATEGORY_PERF, QUERY_TOP_CUSTOMERS
import logging
logging.basicConfig(level=logging.INFO)
engine = get_engine()
for name, sql in [("KPIs", QUERY_KPIS), ("Monthly Sales", QUERY_MONTHLY_SALES), ("Category", QUERY_CATEGORY_PERF), ("Top Customers", QUERY_TOP_CUSTOMERS)]:
    df = _safe_query(engine, name, sql)
    print(f"{name}: {len(df)} rows, cols={list(df.columns)}")
import os; os.remove(__file__)

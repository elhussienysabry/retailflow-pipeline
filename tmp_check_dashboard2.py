from src.dashboard.app import get_engine, _safe_query, QUERY_KPIS, QUERY_MONTHLY_SALES, QUERY_CATEGORY_PERF, QUERY_TOP_CUSTOMERS
print("QUERY_KPIS:", repr(QUERY_KPIS[:50]))
print("QUERY_MONTHLY_SALES:", repr(QUERY_MONTHLY_SALES[:50]))
engine = get_engine()
df = _safe_query(engine, "test", QUERY_KPIS)
print("KPIs result:", len(df), "rows")
import os; os.remove(__file__)

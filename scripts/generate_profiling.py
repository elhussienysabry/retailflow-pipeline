"""
RetailFlow Pipeline — Data Profiling & HTML Report Generator
=============================================================

Queries the production ``marts`` schema tables (``fct_orders``,
``dim_customers``, ``dim_products``) from PostgreSQL, computes
column-level descriptive statistics (missing %, data types,
numerical summaries, cardinality flags), and renders a
self-contained interactive HTML report.

Output: ``docs/profiling/retailflow_data_profile.html``

Usage:
    python scripts/generate_profiling.py
"""

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "docs" / "profiling"
OUTPUT_PATH = OUTPUT_DIR / "retailflow_data_profile.html"

# ── Connection ──────────────────────────────────────────────────────────


def _get_engine():
    load_dotenv(PROJECT_ROOT / ".env", override=True)
    user = os.getenv("POSTGRES_USER", "retailflow_user")
    pwd = os.getenv("POSTGRES_PASSWORD", "retailflow_pass")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "retailflow")
    return create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}")


# ── Schema ──────────────────────────────────────────────────────────────

_MART_TABLES: List[str] = [
    "dim_customers",
    "dim_products",
    "fct_orders",
]

_TABLE_LABELS: Dict[str, str] = {
    "dim_customers": "Customers Dimension",
    "dim_products": "Products Dimension",
    "fct_orders": "Orders Fact",
}

# Columns we treat as numeric for stat computation
_NUMERIC_TYPES = {
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "NUMERIC",
    "DECIMAL",
    "FLOAT",
    "DOUBLE PRECISION",
    "REAL",
}

# ── Column profile ──────────────────────────────────────────────────────


def _profile_column(col: str, dtype: str, series: pd.Series) -> Dict[str, Any]:
    """Compute a comprehensive profile for a single column."""
    total = len(series)
    nulls = int(series.isna().sum())
    null_pct = round(nulls / total * 100, 2) if total else 0.0
    uniques = int(series.nunique())
    cardinality_pct = round(uniques / total * 100, 2) if total else 0.0

    prof: Dict[str, Any] = {
        "column": col,
        "dtype": dtype,
        "total": total,
        "nulls": nulls,
        "null_pct": null_pct,
        "uniques": uniques,
        "cardinality_pct": cardinality_pct,
        "high_cardinality": cardinality_pct > 90.0,
        "all_null": nulls == total,
    }

    clean = series.dropna()

    if dtype.upper() in _NUMERIC_TYPES and len(clean) > 0:
        prof["is_numeric"] = True
        prof["min"] = _fmt_num(clean.min())
        prof["max"] = _fmt_num(clean.max())
        prof["mean"] = _fmt_num(clean.mean())
        prof["median"] = _fmt_num(clean.median())
        prof["std"] = _fmt_num(clean.std())
        prof["p25"] = _fmt_num(clean.quantile(0.25))
        prof["p75"] = _fmt_num(clean.quantile(0.75))
        prof["skew"] = round(float(clean.skew()), 4)
        # Flag if std is 0 (constant column)
        prof["constant"] = float(clean.std()) == 0.0
    else:
        prof["is_numeric"] = False

    # Top value for categorical / low-cardinality text
    if dtype.upper() not in _NUMERIC_TYPES and len(clean) > 0:
        vc = clean.value_counts()
        prof["top_value"] = str(vc.index[0])
        prof["top_freq"] = int(vc.iloc[0])
        prof["top_pct"] = round(int(vc.iloc[0]) / len(clean) * 100, 2)

    return prof


def _fmt_num(val) -> str:
    """Format a numeric value nicely."""
    try:
        fval = float(val)
        if abs(fval) >= 1_000_000:
            return f"{fval:,.2f}"
        if abs(fval) >= 1_000:
            return f"{fval:,.2f}"
        if fval == round(fval):
            return str(int(fval))
        return f"{fval:.4f}"
    except (TypeError, ValueError):
        return str(val)


# ── Table profile ───────────────────────────────────────────────────────


def profile_table(
    engine, schema: str, table: str
) -> Tuple[str, List[Dict[str, Any]], int]:
    """Query a table and return profiling stats for every column."""
    logger.info("Profiling %s.%s ...", schema, table)
    df = pd.read_sql_table(table, engine, schema=schema)
    insp = inspect(engine)
    col_types = {
        c["name"]: str(c["type"]) for c in insp.get_columns(table, schema=schema)
    }
    row_count = len(df)
    profiles = []
    for col in df.columns:
        dtype = col_types.get(col, "TEXT")
        prof = _profile_column(col, dtype, df[col])
        profiles.append(prof)
    logger.info(
        "  %s.%s — %d rows, %d columns profiled",
        schema,
        table,
        row_count,
        len(profiles),
    )
    return table, profiles, row_count


# ── Report rendering ────────────────────────────────────────────────────


def _render_html(all_profiles: List[Tuple[str, List[Dict[str, Any]], int]]) -> str:
    """Build a self-contained interactive HTML report."""
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    total_tables = len(all_profiles)
    total_rows = sum(rc for _, _, rc in all_profiles)
    total_cols = sum(len(p) for _, p, _ in all_profiles)

    # ── Build tab navigation ──────────────────────────────────────────
    tabs_nav = ""
    tabs_content = ""

    for idx, (table_name, profiles, row_count) in enumerate(all_profiles):
        active = "active" if idx == 0 else ""
        checked = "checked" if idx == 0 else ""
        label = _TABLE_LABELS.get(table_name, table_name)
        tab_id = f"tab-{table_name}"

        tabs_nav += (
            f'<input type="radio" name="tabs" id="{tab_id}" {checked}>\n'
            f'<label for="{tab_id}" class="tab-label {active}">{label}</label>\n'
        )

        # ── Summary cards ─────────────────────────────────────────
        total_null = sum(p["nulls"] for p in profiles)
        high_card = sum(1 for p in profiles if p["high_cardinality"])
        num_cols = sum(1 for p in profiles if p.get("is_numeric"))
        cat_cols = len(profiles) - num_cols

        cards = []
        cards.append(
            f'<div class="card"><span class="card-val">'
            f'{row_count:,}</span><span class="card-label">Total Rows</span></div>'
        )
        cards.append(
            f'<div class="card"><span class="card-val">'
            f'{len(profiles)}</span><span class="card-label">Columns</span></div>'
        )
        cards.append(
            f'<div class="card"><span class="card-val">'
            f'{num_cols}</span><span class="card-label">Numeric</span></div>'
        )
        cards.append(
            f'<div class="card"><span class="card-val">'
            f'{cat_cols}</span><span class="card-label">Categorical</span></div>'
        )
        cards.append(
            f'<div class="card"><span class="card-val">'
            f'{total_null:,}</span><span class="card-label">Null Cells</span></div>'
        )
        cards.append(
            f'<div class="card warn"><span class="card-val">'
            f"{high_card}</span>"
            f'<span class="card-label">High-Cardinality</span></div>'
        )
        summary_cards = (
            '<div class="summary-row">\n'
            + "\n".join("          " + c for c in cards)
            + "\n        </div>"
        )

        # ── Per-column rows ───────────────────────────────────────
        col_rows = ""
        for p in profiles:
            missing_bar_pct = p["null_pct"]
            if missing_bar_pct > 20:
                missing_bar_color = "#dc3545"
            elif missing_bar_pct > 5:
                missing_bar_color = "#ffc107"
            else:
                missing_bar_color = "#28a745"

            badge = ""
            if p["all_null"]:
                badge = '<span class="badge badge-danger">ALL NULL</span>'
            elif p["high_cardinality"]:
                badge = '<span class="badge badge-warning">High-Cardinality</span>'
            elif p.get("constant"):
                badge = '<span class="badge badge-info">Constant</span>'

            stats_html = ""
            if p.get("is_numeric"):
                rows = []
                for lbl, key in [
                    ("Min", "min"),
                    ("Max", "max"),
                    ("Mean", "mean"),
                    ("Median", "median"),
                    ("Std Dev", "std"),
                    ("P25", "p25"),
                    ("P75", "p75"),
                    ("Skew", "skew"),
                ]:
                    rows.append(
                        f'<div class="stat"><span class="stat-label">'
                        f"{lbl}</span>"
                        f'<span class="stat-val">{p[key]}</span></div>'
                    )
                stats_html = (
                    '<div class="stats-grid">\n'
                    + "\n".join("                  " + r for r in rows)
                    + "\n                </div>"
                )
            elif "top_value" in p:
                stats_html = (
                    '<div class="stats-grid compact">\n'
                    f'  <div class="stat"><span class="stat-label">'
                    f"Top Value</span>"
                    f'<span class="stat-val top-val">'
                    f'{p["top_value"]}</span></div>\n'
                    f'  <div class="stat"><span class="stat-label">'
                    f"Top Freq</span>"
                    f'<span class="stat-val">'
                    f'{p["top_freq"]:,} ({p["top_pct"]}%)'
                    f"</span></div>\n"
                    "</div>"
                )

            col_rows += f"""
            <tr>
              <td class="col-name">{p["column"]} {badge}</td>
              <td><code>{p["dtype"]}</code></td>
              <td class="num">{p["uniques"]:,}</td>
              <td class="num">{p["nulls"]:,}</td>
              <td class="bar-cell">
                <div class="bar-container">
                  <div class="bar-fill"
                  style="width:{missing_bar_pct}%;background:{missing_bar_color};"
                  ></div>
                </div>
                <span class="bar-label">{missing_bar_pct}%</span>
              </td>
              <td class="stats-cell">
                <details>
                  <summary>Stats</summary>
                  {stats_html}
                </details>
              </td>
            </tr>"""

        tabs_content += f"""
        <div class="tab-content {active}" id="content-{table_name}">
          {summary_cards}
          <table class="profile-table">
            <thead>
              <tr>
                <th>Column</th>
                <th>Type</th>
                <th>Unique</th>
                <th>Missing</th>
                <th>Missing %</th>
                <th>Details</th>
              </tr>
            </thead>
            <tbody>
              {col_rows}
            </tbody>
          </table>
        </div>"""

    # ── Assemble full HTML ─────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>RetailFlow Pipeline — Data Profile Report</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen,
                 Ubuntu, Cantarell, sans-serif;
    background: #f4f6f9; color: #1a1a2e; line-height: 1.6; padding: 20px;
  }}
  .container {{ max-width: 1200px; margin: 0 auto; }}

  header {{
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    color: #fff; padding: 28px 32px; border-radius: 12px; margin-bottom: 24px;
    display: flex; justify-content: space-between; align-items: center;
    flex-wrap: wrap; gap: 12px;
  }}
  header h1 {{ font-size: 22px; font-weight: 700; }}
  header .meta {{ font-size: 13px; color: #a0aec0; }}

  .summary-bar {{
    display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px;
  }}
  .summary-bar .pill {{
    background: #fff; border-radius: 8px; padding: 12px 20px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    font-size: 14px;
  }}
  .summary-bar .pill strong {{ font-size: 20px; color: #1a1a2e; }}

  /* ── Tabs ── */
  .tabs {{ display: flex; flex-wrap: wrap; margin-bottom: 0; }}
  .tabs input[type="radio"] {{ display: none; }}
  .tab-label {{
    padding: 12px 24px; background: #e2e8f0; cursor: pointer;
    border-radius: 8px 8px 0 0; font-weight: 600; font-size: 14px;
    color: #4a5568; transition: all 0.2s; margin-right: 4px;
  }}
  .tab-label:hover {{ background: #cbd5e1; }}
  .tabs input:checked + .tab-label {{
    background: #fff; color: #1a1a2e; box-shadow: 0 -2px 4px rgba(0,0,0,0.06);
  }}
  .tab-content {{ display: none; background: #fff; border-radius: 0 8px 8px 8px;
    padding: 24px; box-shadow: 0 2px 6px rgba(0,0,0,0.06); }}
  .tab-content.active {{ display: block; }}

  /* ── Summary cards ── */
  .summary-row {{
    display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px;
  }}
  .card {{
    background: #f8fafc; border-radius: 8px; padding: 14px 18px;
    flex: 1 0 120px; text-align: center; border: 1px solid #e2e8f0;
  }}
  .card.warn {{ border-color: #f6ad55; background: #fffaf0; }}
  .card-val {{ display: block; font-size: 24px; font-weight: 700; color: #1a1a2e; }}
  .card-label {{ font-size: 11px; text-transform: uppercase; color: #718096;
    letter-spacing: 0.5px; }}

  /* ── Table ── */
  .profile-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  .profile-table thead {{ background: #edf2f7; }}
  .profile-table th {{
    padding: 10px 12px; text-align: left; font-weight: 600; color: #2d3748;
    border-bottom: 2px solid #cbd5e1; white-space: nowrap;
  }}
  .profile-table td {{
    padding: 10px 12px; border-bottom: 1px solid #e2e8f0; vertical-align: middle;
  }}
  .profile-table tr:hover {{ background: #f7fafc; }}
  .col-name {{ font-weight: 600; white-space: nowrap; }}
  .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  code {{ background: #edf2f7; padding: 2px 6px; border-radius: 4px;
    font-size: 12px; color: #2b6cb0; }}
  .bar-cell {{ display: flex; align-items: center; gap: 8px; }}
  .bar-container {{ flex: 1; height: 8px; background: #edf2f7; border-radius: 4px;
    overflow: hidden; min-width: 80px; }}
  .bar-fill {{ height: 100%; border-radius: 4px; transition: width 0.3s; }}
  .bar-label {{ font-size: 12px; color: #4a5568; min-width: 44px; text-align: right; }}

  /* ── Badges ── */
  .badge {{
    display: inline-block; font-size: 10px; font-weight: 700; padding: 2px 8px;
    border-radius: 10px; margin-left: 6px; text-transform: uppercase;
    vertical-align: middle;
  }}
  .badge-danger {{ background: #fed7d7; color: #c53030; }}
  .badge-warning {{ background: #fefcbf; color: #975a16; }}
  .badge-info {{ background: #bee3f8; color: #2a4365; }}

  /* ── Details / Stats ── */
  .stats-cell details summary {{
    cursor: pointer; color: #4a5568; font-size: 12px; font-weight: 600;
    padding: 4px 8px; border-radius: 4px; user-select: none;
  }}
  .stats-cell details summary:hover {{ background: #edf2f7; }}
  .stats-grid {{
    display: grid; grid-template-columns: 1fr 1fr 1fr 1fr;
    gap: 4px 12px; padding: 10px 0 4px;
  }}
  .stats-grid.compact {{ grid-template-columns: 1fr 1fr; }}
  .stat {{ display: flex; flex-direction: column; }}
  .stat-label {{ font-size: 10px; text-transform: uppercase; color: #718096;
    letter-spacing: 0.3px; }}
  .stat-val {{ font-size: 13px; font-weight: 600; color: #2d3748; }}
  .top-val {{
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 160px;
    display: inline-block;
  }}

  @media (max-width: 768px) {{
    .stats-grid {{ grid-template-columns: 1fr 1fr; }}
    .summary-row {{ gap: 8px; }}
    .card {{ flex: 1 0 80px; padding: 10px; }}
  }}
</style>
</head>
<body>
<div class="container">
  <header>
    <div>
      <h1>RetailFlow Pipeline — Data Profile Report</h1>
      <div class="meta">Generated: {generated_at} &middot; {total_tables}
        tables &middot; {total_rows:,} rows &middot; {total_cols} columns</div>
    </div>
  </header>

  <div class="summary-bar">
    <div class="pill"><strong>{total_rows:,}</strong> Rows</div>
    <div class="pill"><strong>{total_cols}</strong> Columns</div>
    <div class="pill"><strong>{total_tables}</strong> Tables</div>
  </div>

  <div class="tabs">
    {tabs_nav}
  </div>

  {tabs_content}

  <footer style="margin-top: 32px; text-align: center; font-size: 12px; color: #a0aec0;">
    RetailFlow Pipeline &middot; Automated Data Profiling Report
  </footer>
</div>
</body>
</html>"""
    return html


# ── Main entry point ────────────────────────────────────────────────────


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    engine = _get_engine()

    logger.info(
        "Connected to PostgreSQL — profiling %d mart tables",
        len(_MART_TABLES),
    )

    all_profiles: List[Tuple[str, List[Dict[str, Any]], int]] = []
    for table in _MART_TABLES:
        name, profiles, row_count = profile_table(engine, "marts", table)
        all_profiles.append((name, profiles, row_count))

    if not all_profiles:
        logger.error("No tables profiled — nothing to render.")
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    html = _render_html(all_profiles)
    OUTPUT_PATH.write_text(html, encoding="utf-8")

    total_null = sum(p["nulls"] for _, profs, _ in all_profiles for p in profs)
    high_card = sum(
        1 for _, profs, _ in all_profiles for p in profs if p["high_cardinality"]
    )

    logger.info(
        "Profile report written to %s "
        "(%d tables, %d columns, %d null cells, %d high-cardinality flags)",
        OUTPUT_PATH,
        len(all_profiles),
        sum(len(p) for _, p, _ in all_profiles),
        total_null,
        high_card,
    )
    print(f"\n  Data profile saved to: {OUTPUT_PATH}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())

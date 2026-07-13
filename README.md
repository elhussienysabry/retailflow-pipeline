# RetailFlow Pipeline — v1.2.0

[![CI/CD Build Status](https://github.com/elhussienysabry/retailflow-pipeline/actions/workflows/ci_cd.yml/badge.svg)](https://github.com/elhussienysabry/retailflow-pipeline/actions/workflows/ci_cd.yml)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![PostgreSQL 15](https://img.shields.io/badge/postgres-15-316192.svg)](https://www.postgresql.org/)
[![dbt 1.7](https://img.shields.io/badge/dbt-1.7-E34F26.svg)](https://github.com/dbt-labs/dbt-core)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

> **A learning-first, production-grade data engineering pipeline** — simulating a real-world ELT platform for a fictional e-commerce company. Built to demonstrate mastery of modern data stack engineering: schema governance, idempotent warehousing, CI/CD automation, data quality circuit breakers, and observability-driven alerting.

---

## Five Engineering Achievements

| # | Achievement | Implementation | Production Relevance |
|---|-------------|----------------|----------------------|
| 1 | **Data Schema Evolution & Drift Detection** | `SCHEMA_BLUEPRINT` dictionary in `load_to_postgres.py` enforces per-entity column schemas. Missing columns / type mismatches → file quarantined to `data/rejected_schemas/`, pipeline halts with exit code 2, red alert dispatched. Extra unknown columns → amber warning, pipeline continues. | Prevents silent corruption when upstream source schemas evolve without notice. Graduated severity (Critical / Warning) mirrors real-world SLAs. |
| 2 | **Warehouse Idempotency & Fault Tolerance** | `DELETE FROM target WHERE _execution_date = :today` before every batch insert. Each row tagged with the run's UTC date. Unified transactions use `INSERT ... ON CONFLICT DO UPDATE`. Re-running the pipeline N times on the same calendar day produces identical warehouse state with zero duplicates. | Guarantees deterministic re-runs — essential for backfills, retries, and incremental refresh cycles without data corruption. |
| 3 | **Dual Virtual Environment Strategy** | `.venv/` (pandas, streamlit, airflow) and `.venv-dbt/` (dbt-core 1.7 + dbt-postgres) resolve the `mashumaro<4` vs `mashumaro>=4` dependency conflict between dbt-core and Airflow/Great Expectations. Orchestrator resolves the correct executable per step via `_py_exe()` / `_dbt_exe()`. | Mirrors production runtime isolation patterns — no single `pip install` can satisfy conflicting transitive dependencies. Common in teams running dbt alongside orchestration tools. |
| 4 | **CI/CD & DevOps Automation** | GitHub Actions workflow (`.github/workflows/ci_cd.yml`) runs `flake8` linting, `black --check` formatting, full 155+ test `pytest` suite, and `dbt debug` + `dbt parse` SQL compilation against a live PostgreSQL 15 service container on every push/PR. | Catches Python regressions, SQL compilation errors, and broken `ref()` chains before merge. Service container pattern eliminates the need for external test databases. |
| 5 | **Data Quality Circuit Breaker & Graph Lineage** | 48 automated dbt tests across all models. On failure, orchestrator reads `run_results.json`, dispatches per-test metadata (unique ID, status, execution time, database message) to Discord/Slack. After successful run, `generate_lineage.py` builds a `networkx.DiGraph` from `manifest.json` and renders a colour-coded 200 DPI PNG lineage map. | Goes beyond pass/fail — the alert tells the team *exactly* which SLA was breached. The commitable lineage artifact documents data flow without external tooling. |

---

## Technology Stack

| Layer | Tools | Purpose |
|-------|-------|---------|
| **Language** | Python 3.12 | Core pipeline, ingestion, automation |
| **Warehouse** | PostgreSQL 15 (Docker) | Relational data warehouse |
| **Lakehouse** | DuckDB 1.5, Apache Parquet, PyArrow | Columnar serialisation, OLAP harmonisation via embedded DuckDB |
| **Transformation** | dbt-core 1.7, dbt-postgres 1.7 | SQL model compilation, incremental materialisation, data quality tests |
| **Orchestration** | Python CLI orchestrator, Airflow DAG alternative | Sequential DAG execution, circuit breaker, environment switching |
| **Ingestion** | pandas, SQLAlchemy, Faker | CSV + JSON hybrid ingestion, PII anonymisation, schema drift detection |
| **Dashboard** | Streamlit, Plotly | Live KPI monitoring with 5-min cached refresh |
| **CI/CD** | GitHub Actions (flake8, black, pytest, dbt parse) | Automated quality gates on every commit |
| **Columnar Storage** | Apache Parquet (Snappy), PyArrow | Compressed columnar serialisation — the "L" in the Lakehouse |
| **Columnar Storage** | Apache Parquet (Snappy), PyArrow | Compressed columnar serialisation — the "L" in the Lakehouse |
| **Containerisation** | Docker Compose (PostgreSQL + app + pgAdmin) | Reproducible local development and deployment |
| **Testing** | pytest 7, pytest-cov | 155+ unit and integration tests |
| **Observability** | Discord / Slack webhooks, colour-coded embeds | Real-time pipeline alerting with rich failure metadata |
| **Data Profiling** | pandas, self-contained HTML report | Per-column statistical analysis with interactive visualisation |
| **Lineage** | networkx, matplotlib | Automated DAG rendering at 200 DPI |

---

## Quick Start

```bash
git clone https://github.com/elhussienysabry/retailflow-pipeline.git
cd retailflow-pipeline
cp .env.example .env

# Set up both isolated environments
make setup            # .venv (core dependencies)
make setup-dbt        # .venv-dbt (dbt-core + dbt-postgres only)

# Start PostgreSQL
make run

# Execute the full 8-step pipeline
make pipeline
# OR
.venv\Scripts\python scripts\orchestrate.py --profile small
```

### Pipeline Execution Map

| Step | Component | Environment | What Happens | Duration (small) |
|------|-----------|-------------|-------------|-------------------|
| 1 | Generate Data | `.venv` | Faker creates 4 source files (CSV + JSON) in `data/raw/` | ~1.5s |
| 2 | Load to PostgreSQL | `.venv` | Schema drift check → per-entity validation → PII SHA-256 hash → DLQ isolation → Parquet serialisation (data/lakehouse/) → DELETE + INSERT by execution_date → DuckDB harmonise Parquet → unified insert | ~4s |
| 3 | dbt Run | `.venv-dbt` | Staging (views) → Intermediate (view) → Marts (tables, full-refresh) | ~20s |
| 4 | dbt Test | `.venv-dbt` | 48 data quality tests executed; circuit breaker on failure | ~6s |
| 5 | Excel Export | `.venv` | 4 analytics sheets → styled `.xlsx` in `outputs/` | ~1.5s |
| 6 | dbt Docs Generate | `.venv-dbt` | `dbt compile` + `dbt docs generate` → `manifest.json` + `catalog.json` | ~12s |
| 7 | Lineage Graph Export | `.venv` | NetworkX parses manifest → colour-coded PNG at `docs/lineage/` | ~2.5s |
| 8 | Data Profile Report | `.venv` | Pandas profiles mart tables → interactive HTML at `docs/profiling/` | ~1.5s |

---

## Architecture Overview

```
                    ┌─────────────────────────────────────────────────────┐
                    │                  DATA SOURCES                       │
                    │  Python Faker → customers.csv  products.csv         │
                    │                  orders.csv    pos_store_sales.json  │
                    └──────────────────────┬──────────────────────────┬───┘
                                           │                          │
                                  ┌────────▼────────┐         ┌──────▼──────┐
                                  │  SCHEMA DRIFT   │         │  JSON       │
                                  │  DETECTOR       │         │  INGESTION  │
                                  │  (Layer 0)      │         │             │
                                  │                 │         │             │
                                  │  ┌─ Blueprint   │         │             │
                                  │  │  compare      │         │             │
                                  │  │               │         │             │
                                  │  │ Pass → load   │         │             │
                                  │  │ WARN → alert  │         │             │
                                  │  │ CRITICAL →    │         │             │
                                  │  │  quarantine   │         │             │
                                  │  │  + exit(2)    │         │             │
                                  │  └───────────────┘         │             │
                                  └──────┬─────────────────────┘─────────────┘
                                         │
                            ┌────────────┴────────────┐
                            │                         │
                     ┌──────▼──────┐          ┌───────▼──────────┐
                     │  VALIDATE    │          │  DEAD LETTER     │
                     │  + PII HASH  │          │  QUEUE (DLQ)     │
                     │  + DLQ       │          │  data/rejected/  │
                     │  isolation   │          │                  │
                     │              │          │  Bad rows with   │
                     │              │          │  rejection_reason│
                     └──────┬──────┘          └───────────────────┘
                            │
                     ┌──────▼───────────────────────────────────────┐
                     │  L  LOCAL DATA LAKEHOUSE                      │
                     │  A  data/lakehouse/*.parquet (Snappy)         │
                     │  K  ┌─────────────────────────────────────┐   │
                     │  E  │  _write_lakehouse_parquet():        │   │
                     │  H  │  Clean chunks → PyArrow + Snappy    │   │
                     │  O  │  → customers.parquet, orders.parquet│   │
                     │  U  │  products.parquet, pos_store_sales. │   │
                     │  S  │  parquet (JSON path)                │   │
                     │  E  └─────────────────────────────────────┘   │
                     └──────────────────────┬────────────────────────┘
                                            │
                     ┌──────────────────────▼────────────────────────┐
                     │  DUCKDB OLAP HARMONISATION                     │
                     │  _duckdb_harmonize(): reads *.parquet via      │
                     │  DuckDB, UNION ALL orders + pos_store_sales,   │
                     │  writes result → raw.unified_transactions      │
                     └──────────────────────┬────────────────────────┘
                                            │
                     ┌──────▼───────────────────────────────────────┐
                     │        PostgreSQL 15 WAREHOUSE                │
                     │  raw.customers  raw.products  raw.orders      │
                     │  raw.pos_store_sales  raw.unified_transactions│
                     └──────────────────────┬────────────────────────┘
                                            │
                     ┌──────────────────────▼────────────────────────┐
                     │              dbt TRANSFORMATION                │
                     │  staging ──► intermediate ──► marts           │
                     │  (views)       (views)        (tables)        │
                     │  stg_orders    int_orders_    dim_customers    │
                     │  stg_cust..    enriched       dim_products     │
                     │  stg_prod..                   fct_orders       │
                     │                                               │
                     │  ┌── 48 automated data quality tests ────┐    │
                     │  │  not_null, unique, accepted_values,   │    │
                     │  │  relationships, custom singular       │    │
                     │  └───────────────────────────────────────┘    │
                     └──────────────┬─────────────────────┬──────────┘
                                    │                     │
                    ┌───────────────┼─────────────────┐   │
                    ▼               ▼                 ▼   ▼
              ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐
              │STREAMLIT │  │EXCEL     │  │dbt DOCS  │  │LINEAGE GRAPH │
              │DASHBOARD │  │EXPORT    │  │CATALOG   │  │(NetworkX PNG)│
              │:8501     │  │outputs/  │  │make docs │  │docs/lineage/ │
              └──────────┘  └──────────┘  └──────────┘  └──────────────┘

              ┌─────────────────────────────────────────────────────────┐
              │              CI/CD (GitHub Actions)                     │
              │  Push/PR ──► flake8 ──► black ──► pytest (77+) ──►     │
              │               dbt debug ──► dbt parse (SQL compile)    │
              │               PostgreSQL 15 service container (shared) │
              └─────────────────────────────────────────────────────────┘
```

---

## Key Production Patterns

### Schema Drift Detection

Every source file is checked against `SCHEMA_BLUEPRINT` before any data touches PostgreSQL:

```
                    ┌──────────────────────┐
                    │   SCHEMA_BLUEPRINT   │
                    │  customers: 9 cols   │
                    │  products:  6 cols   │
                    │  orders:    8 cols   │
                    │  pos:       8 cols   │
                    └───────┬──────────────┘
                            │
                ┌───────────┴───────────┐
                ▼                       ▼
       ┌──────────────────┐   ┌──────────────────────┐
       │  Missing column  │   │  Extra column        │
       │  Type mismatch   │   │  (not in blueprint)  │
       │                  │   │                      │
       │  CRITICAL        │   │  WARNING             │
       │  → file moved to │   │  → pipeline continues│
       │    rejected_schemas │  → amber alert fired  │
       │  → red alert     │   │                      │
       │  → pipeline halts│   │                      │
       │  → exit code 2   │   │                      │
       └──────────────────┘   └──────────────────────┘
```

### Local Data Lakehouse

After validation and before PostgreSQL insert, clean data is serialised as **Snappy-compressed Apache Parquet** files in `data/lakehouse/`. This serves as:
- **Columnar intermediate storage** — enables schema-on-read without schema-on-write
- **Source for DuckDB OLAP** — `_duckdb_harmonize()` reads `.parquet` via embedded DuckDB, unions orders + POS, and inserts into `raw.unified_transactions`
- **Reproducible audit trail** — every pipeline run leaves a self-describing Parquet artifact

### Idempotent Loading Strategy

| Source Type | Strategy | Mechanism |
|-------------|----------|-----------|
| CSV (customers, products, orders) | **Parquet + Delete + Insert** | Clean chunks accumulate → `_write_lakehouse_parquet()` → `DELETE FROM target WHERE _execution_date = :today` → add `_execution_date` → `to_sql(if_exists="append")` |
| JSON (pos_store_sales) | **Parquet + Delete + Insert** | `_write_lakehouse_parquet()` → `DELETE FROM target WHERE _execution_date = :today` → add `_execution_date` → `to_sql(if_exists="append")` |
| Unified transactions | **DuckDB harmonise + Insert** | `_duckdb_harmonize()` reads `orders.parquet` + `pos_store_sales.parquet` via embedded DuckDB `UNION ALL`, maps columns, writes to `raw.unified_transactions` with `_execution_date` |

This guarantees that running the pipeline N times on the same calendar day produces exactly the same warehouse state with zero duplicate rows across all tables.

### Alerting Engine

`scripts/alerts.py` dispatches colour-coded messages to Discord or Slack webhooks:

| Event | Colour | Payload Contents |
|-------|--------|------------------|
| **Schema Drift Critical** | Red | Entity name, missing columns, type mismatches, quarantined file path |
| **Schema Drift Warning** | Amber | Entity name, extra column names |
| **DLQ Rejected Rows** | Amber | Loaded count, rejected count, rejection rate % |
| **dbt Test Failure** | Red | Per-test unique ID, status, execution time, database message (from `run_results.json`) |
| **Pipeline Complete** | Green | Total steps, duration, DLQ summary |

Webhook failures are gracefully handled with Discord embed → plain-text fallback. Alerts never crash the pipeline.

### dbt Incremental Materialisation

`fct_orders` uses dbt's incremental model with `unique_key='order_id'`. The orchestrator passes `--full-refresh` on `marts` each run so dimension tables (FK references) stay in sync:

```sql
{{ config(materialized='incremental', unique_key='order_id') }}
SELECT ...
{% if is_incremental() %}
  WHERE order_date >= (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

---

## Dual Virtual Environment Strategy

`dbt-core` 1.7 pins `mashumaro<4`. Airflow and Great Expectations require `mashumaro>=4`. These cannot coexist in a single `pip install`:

| Environment | Location | Contents | Used By |
|-------------|----------|----------|---------|
| **Main** | `.venv/` | pandas, SQLAlchemy, streamlit, airflow, great_expectations, openpyxl, pytest, flake8, black, Faker, requests, networkx, matplotlib, python-dotenv | Steps 1, 2, 5, 7, 8; dashboard; tests; health check |
| **dbt** | `.venv-dbt/` | `dbt-core==1.7.14`, `dbt-postgres==1.7.14` | Steps 3, 4, 6; dbt debug, dbt parse |

The orchestrator resolves the correct executable per step:

```python
def _py_exe():   # → .venv/Scripts/python.exe (Win) or sys.executable (Linux)
def _dbt_exe():  # → .venv-dbt/Scripts/dbt.exe (Win) or $DBT_EXECUTABLE (Linux/container)
```

---

## Data Scale Profiles

| Profile | `--profile` | Customers | Products | Orders | POS Sales | Runtime |
|---------|-------------|-----------|----------|--------|-----------|---------|
| Small | `small` | 1,000 | 100 | 10,000 | 3,000 | ~45s |
| Medium | `medium` | 10,000 | 500 | 100,000 | 30,000 | ~3m |
| Large | `large` | 100,000 | 5,000 | 1,000,000 | 300,000 | ~30m |

---

## Testing

```bash
# Full suite (155+ tests)
.venv\Scripts\pytest tests/ -v --tb=short

# With coverage
.venv\Scripts\pytest tests/ --cov=scripts/ --cov-report=term-missing

# Single test file
.venv\Scripts\pytest tests/test_transformations.py -v
```

| Test File | Coverage | Validates |
|-----------|----------|-----------|
| `test_generate_data.py` | Row counts, columns, valid ranges | Faker output correctness |
| `test_transformations.py` | Cents→dollars, discount calc, status normalisation | Business logic |
| `test_load_to_postgres.py` | Engine creation, schema, truncation, Lakehouse, DuckDB, Parquet | DB connectivity & lakehouse |
| `test_project_status.py` | Docker, PostgreSQL, env, Lakehouse, CSVs, status aggregation | Pipeline health logic (8 dims) |
| `test_excel_export.py` | Workbook, sheets, headers, styling, currency format | Export formatting |
| `test_generate_data_profiles.py` | CLI defaults, overrides, parsing | Argument resolution |

---

## CI/CD Pipeline

On every push/PR to `main`/`master`, GitHub Actions executes two parallel jobs against a shared PostgreSQL 15 service container:

| Job | Steps | Purpose |
|-----|-------|---------|
| **Core Python** | `flake8` lint → `black --check` → `pytest` (155+ tests) | Code quality + regression detection |
| **dbt Validation** | `pip install dbt-core dbt-postgres` → `CREATE SCHEMA raw` → `dbt debug` → `dbt parse` | SQL compilation check — validates all models, refs, sources, macros |

---

## Generated Artifacts

| Artifact | Tool | Location | Description |
|----------|------|----------|-------------|
| **Lineage Graph** | `generate_lineage.py` (NetworkX + matplotlib) | `docs/lineage/current_data_lineage.png` | Colour-coded 200 DPI PNG — staging (green) → intermediate (blue) → marts (gold) |
| **Data Profile** | `generate_profiling.py` (pandas) | `docs/profiling/retailflow_data_profile.html` | Interactive HTML — per-column missing %, cardinality flags, numeric summary, top values |
| **dbt Docs** | `dbt docs generate` | `dbt/target/` (via `make docs` at localhost:8080) | Browsable catalog, column metadata, interactive DAG, test dashboard |
| **Excel Export** | `src/exports/excel_exporter.py` (openpyxl) | `outputs/retail_analytics_*.xlsx` | Styled workbook — Top Customers, Monthly Sales, Category Performance, Cohort Analysis |

---

## Health Check

```bash
.venv\Scripts\python scripts/project_status.py
```

Validates 8 dimensions: Docker Desktop → PostgreSQL container → `.env` file → dbt environment → Lakehouse Parquet files → raw CSV files → database row counts → schema drift quarantine. Exits with code 0 (healthy), 1 (degraded), or 2 (unhealthy) with actionable fix hints.

---

## Learning Path

This project teaches, in progression order:

1. **Python Data Engineering** — pandas, SQLAlchemy, Faker, logging, type hints, argparse
2. **SQL Transformations** — dbt models, Jinja macros, star schema design, incremental materialisation
3. **Data Quality** — dbt tests (not_null, unique, relationships, accepted_values), run_results.json parsing
4. **Orchestration** — circuit breaker pattern, virtual environment switching, streaming subprocess output
5. **Observability** — Discord/Slack webhooks, colour-coded embeds, rich failure metadata
6. **Schema Governance** — drift detection, blueprint enforcement, graduated severity, file quarantine
7. **Warehouse Idempotency** — DELETE + INSERT by execution_date, upsert MERGE, deterministic re-runs
8. **CI/CD** — GitHub Actions, service containers, linting, format checking, SQL compilation
9. **Containerisation** — Docker Compose, multi-stage builds, dual-venv runtime environment
10. **Visualisation** — Streamlit dashboards, Plotly charts, NetworkX lineage graphs
11. **Documentation** — dbt docs, auto-generated lineage blueprints, architecture docs, data profiles

---

## Project Structure

```
retailflow-pipeline/
│
├── .venv/                  # Core Python env — data gen, loading, dashboard, tests
├── .venv-dbt/              # Isolated dbt env — dbt-core, dbt-postgres only
│
├── scripts/                # Core pipeline scripts
│   ├── orchestrate.py      # 8-step orchestrator + circuit breaker + alerting
│   ├── generate_fake_data.py   # Faker synthetic data (CSV + JSON)
│   ├── load_to_postgres.py     # Hybrid ingestion + schema drift + PII hash + Parquet + DuckDB
│   ├── generate_lineage.py     # NetworkX lineage graph renderer
│   ├── generate_profiling.py   # Pandas data profiling → HTML report
│   ├── alerts.py               # Discord/Slack webhook dispatcher
│   └── project_status.py       # End-to-end pipeline health check (8 dimensions)
│
├── src/
│   ├── dashboard/app.py        # Streamlit KPI dashboard
│   └── exports/excel_exporter.py   # Styled Excel workbook export
│
├── dbt/                     # dbt project
│   ├── models/              # 7 SQL models (staging/intermediate/marts)
│   ├── tests/               # 2 custom singular tests
│   ├── macros/              # Jinja SQL macros
│   ├── profiles.yml         # DB connection (env-var driven)
│   └── dbt_project.yml      # dbt project config
│
├── .github/workflows/       # CI/CD — lint, test, dbt parse on every push
├── docker-compose.yml       # PostgreSQL 15 + app + pgAdmin
├── Dockerfile               # Multi-stage, dual-venv image
├── Makefile                 # Dev workflow commands
├── sql/                     # Schema DDL + analytics queries
├── tests/                   # 155+ pytest tests
├── data/
│   ├── raw/                 # Generated source files (gitignored)
│   ├── lakehouse/           # Parquet columnar lake (gitignored)
│   ├── rejected/            # Dead Letter Queue (gitignored)
│   └── rejected_schemas/    # Schema drift quarantine (gitignored)
├── docs/
│   ├── lineage/             # Auto-generated lineage PNG
│   └── profiling/           # Interactive HTML profile reports
└── outputs/                 # Excel exports (gitignored)
```

---

## Licence

Educational use. Free to modify and share. Built as a learning portfolio project.

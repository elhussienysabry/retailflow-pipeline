# RetailFlow Pipeline

**A foundational data engineering project — simulating a production-grade ELT pipeline for a fictional e-commerce company.**

[![CI/CD Build Status](https://github.com/elhussienysabry/retailflow-pipeline/actions/workflows/ci_cd.yml/badge.svg)](https://github.com/elhussienysabry/retailflow-pipeline/actions/workflows/ci_cd.yml)

---

## Why This Project Exists

This repository is a **learning-first data engineering project**. It intentionally mirrors the tools, patterns, and operational concerns you encounter daily in a real data team — but keeps the scope small enough to build in a weekend and reason about end-to-end.

The core narrative:

> Your company sells products online and in physical stores. Thousands of orders arrive every day. The CEO wants dashboards, the data team wants quality guarantees, and the platform needs to survive schema changes without waking anyone at 3 AM.

This project shows how those concerns translate into code.

---

## Five Engineering Achievements

| # | Capability | What It Does | Why It Matters |
|---|-----------|-------------|----------------|
| 1 | **Dual Virtual Environment Strategy** | `.venv` for core data processing (pandas, streamlit, airflow) and `.venv-dbt` for isolated dbt-core (avoids `mashumaro` dependency conflict) | Real-world dependency management — Airflow + dbt cannot coexist in the same `pip install`. This mirrors how teams isolate production runtimes. |
| 2 | **CI/CD & DevOps Automation** | GitHub Actions workflow on every push: `flake8` linting, `black --check` formatting, 77+ `pytest` tests, and `dbt debug` + `dbt parse` against a live PostgreSQL service container | Ensures every commit is validated before merge. Catches SQL compilation errors, broken refs, and Python regressions automatically. |
| 3 | **Schema Drift Detector** | Each source file is checked against a `SCHEMA_BLUEPRINT` before ingestion. Missing columns / type mismatches halt the pipeline (critical — file quarantined to `data/rejected_schemas/`, red alert fired). Extra unknown columns pass with an amber warning | Production pipelines break silently when source schemas evolve. This detector makes drift visible immediately, with graduated severity and automatic alerting. |
| 4 | **Data Quality & Observability** | 48 automated dbt tests across all models. A circuit breaker reads `run_results.json` on failure and dispatches a metadata-rich alert listing every breached test (unique ID, status, database message) to Discord or Slack | Goes beyond pass/fail — the alert tells you *exactly* which data quality SLA was violated, with per-test execution details. |
| 5 | **Automated Graph Lineage** | `generate_lineage.py` parses `dbt/target/manifest.json` via `networkx.DiGraph`, colour-codes nodes by layer (staging green / intermediate blue / marts gold), and renders a 200 DPI PNG at `docs/lineage/current_data_lineage.png` | Produces a version-control-friendly, commitable asset that documents how data flows through the transformation layers — no external tooling required. |

---

## Tech Stack

| Layer | Tools |
|-------|-------|
| **Language** | Python 3.12 |
| **Warehouse** | PostgreSQL 15 (Docker) |
| **Transformation** | dbt-core 1.7, dbt-postgres |
| **Orchestration** | Python CLI orchestrator + Airflow DAG |
| **Ingestion** | pandas, SQLAlchemy, Faker |
| **Dashboard** | Streamlit, Plotly |
| **CI/CD** | GitHub Actions (flake8, black, pytest, dbt parse) |
| **Container** | Docker Compose (3 services) |
| **Testing** | pytest 7, pytest-cov |
| **Alerting** | Discord / Slack webhooks (requests) |

---

## Quick Start

```bash
git clone https://github.com/elhussienysabry/retailflow-pipeline.git
cd retailflow-pipeline
cp .env.example .env

# Set up both virtual environments
make setup            # .venv — pandas, streamlit, airflow, etc.
make setup-dbt        # .venv-dbt — dbt-core + dbt-postgres only

# Start PostgreSQL
make run

# Run the full pipeline (8 steps, ~45 seconds on small profile)
make pipeline
# OR:
.venv\Scripts\python scripts\orchestrate.py --profile small
```

That single command executes:

| Step | Component | Environment | Duration (small) |
|------|-----------|-------------|-------------------|
| 1 | Generate synthetic data (CSV + JSON) | `.venv` | ~1.5s |
| 2 | Load + schema drift check + PII hash + upsert unified | `.venv` | ~4s |
| 3 | dbt run (staging → intermediate → marts) | `.venv-dbt` | ~20s |
| 4 | dbt test (48 quality checks) | `.venv-dbt` | ~6s |
| 5 | Excel analytics export | `.venv` | ~1.5s |
| 6 | dbt docs generate (catalog + lineage JSON) | `.venv-dbt` | ~12s |
| 7 | NetworkX lineage graph render | `.venv` | ~2.5s |
| 8 | HTML data profile report | `.venv` | ~1.5s |

---

## Architecture Overview

```
                    ┌──────────────────────────────────────────────┐
                    │              DATA SOURCES                   │
                    │  (Python Faker → CSVs + JSON in data/raw/)  │
                    └──────────┬───────────────────┬──────────────┘
                               │                   │
                     ┌─────────▼─────────┐  ┌──────▼──────┐
                     │  Schema Drift      │  │  Schema     │
                     │  Detector          │  │  Harmoniser │
                     │  (blueprint check) │  │  (unified   │
                     │  ↳ rejected_schemas│  │   upsert)   │
                     └─────────┬─────────┘  └──────┬──────┘
                               │                   │
                     ┌─────────▼───────────────────▼──────────┐
                     │         PostgreSQL 15 (Docker)          │
                     │  raw.customers  raw.orders              │
                     │  raw.products   raw.pos_store_sales     │
                     │  raw.unified_transactions               │
                     └───────────────────┬─────────────────────┘
                                         │
                    ┌────────────────────▼─────────────────────┐
                    │         dbt Transformation Layer          │
                    │                                           │
                    │  staging ─► intermediate ─► marts         │
                    │  (views)      (view)      (tables)       │
                    │  stg_orders   int_orders_   dim_customers │
                    │  stg_cust..   enriched      dim_products  │
                    │  stg_prod..                 fct_orders    │
                    │                                           │
                    │  48 automated data quality tests          │
                    └──────┬──────────────────┬────────────────┘
                           │                  │
              ┌────────────┼──────────────┐   │
              ▼            ▼              ▼   ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐
        │Streamlit │ │Excel     │ │dbt Docs  │ │Lineage Graph │
        │Dashboard │ │Export    │ │Catalog   │ │(NetworkX PNG)│
        │:8501     │ │outputs/  │ │make docs │ │docs/lineage/ │
        └──────────┘ └──────────┘ └──────────┘ └──────────────┘
```

---

## Project Structure

```
retailflow-pipeline/
│
├── .venv/                  # Main Python env — data gen, load, dashboard, tests
├── .venv-dbt/              # Isolated dbt env — dbt-core, dbt-postgres only
│
├── scripts/
│   ├── orchestrate.py       # 8-step pipeline orchestrator + circuit breaker
│   ├── generate_fake_data.py# Faker-based synthetic data (CSV + JSON)
│   ├── load_to_postgres.py # Hybrid ingestion, schema drift detector, PII hash, unified upsert
│   ├── generate_lineage.py # NetworkX lineage graph renderer
│   ├── generate_profiling.py# Pandas data profiling → HTML report
│   ├── alerts.py            # Discord/Slack webhook dispatch
│   └── project_status.py   # End-to-end health check
│
├── dbt/                     # dbt project (staging → intermediate → marts)
│   ├── models/              # 7 SQL models with YAML test definitions
│   ├── tests/               # 2 custom singular tests
│   └── macros/              # Jinja SQL macros
│
├── src/
│   ├── dashboard/app.py     # Streamlit KPI dashboard
│   └── exports/excel_exporter.py # Styled Excel workbook export
│
├── airflow/dags/            # Airflow DAG definition (alternative orchestrator)
├── sql/                     # Reference SQL (schema DDL + analytics queries)
├── tests/                   # 77+ pytest tests
├── docs/                    # Generated artifacts (lineage PNG, profiling HTML)
│
├── .github/workflows/       # CI/CD — lint, test, dbt-parse on every push
├── docker-compose.yml       # PostgreSQL 15 + Streamlit app + pgAdmin
├── Dockerfile               # Multi-stage, dual-venv image
└── Makefile                 # Dev workflow commands
```

---

## Key Design Decisions

### Dual Virtual Environments

`dbt-core` 1.7 pins `mashumaro<4`. Airflow and Great Expectations require `mashumaro>=4`. These cannot coexist in one `pip install`. The project solves this with two independent venvs:

| Environment | Location | Contents |
|-------------|----------|----------|
| **Main** | `.venv/` | pandas, SQLAlchemy, streamlit, airflow, GE, openpyxl, pytest, flake8, Faker |
| **dbt** | `.venv-dbt/` | `dbt-core==1.7.14`, `dbt-postgres==1.7.14` |

The orchestrator (`orchestrate.py`) resolves the correct executable per step using `_py_exe()` and `_dbt_exe()`, with `DBT_EXECUTABLE` env-var override for containerised runs.

### Schema Drift Detection

Every source file is checked against `SCHEMA_BLUEPRINT` before any data is loaded:

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
    │    rejected_schemas│  │  → amber alert fired │
    │  → red alert     │   │                      │
    │  → pipeline halts│   │                      │
    └──────────────────┘   └──────────────────────┘
```

### Alerting Pipeline

The alerting engine (`scripts/alerts.py`) dispatches colour-coded messages to a Discord (embed) or Slack (Block Kit) webhook at three pipeline states:

| Event | Colour | Payload |
|-------|--------|---------|
| Ingestion DLQ has rejected rows | Amber | Loaded / Rejected / Rejection Rate % |
| dbt test failure | Red | Per-test unique_id, status, execution_time, database message (from `run_results.json`) |
| Pipeline complete | Green | Total steps, duration, DLQ count |

Graceful fallback: if `PIPELINE_WEBHOOK_URL` is not set, alerts are silently skipped. A webhook failure never crashes the pipeline.

### Incremental Loading

`fct_orders` uses dbt's incremental materialisation with `unique_key='order_id'`:

- First run: loads full history (empty target table)
- Subsequent runs: `WHERE order_date >= (SELECT MAX(order_date) FROM {{ this }})` — processes only new/changed data
- The orchestrator passes `--full-refresh` on `marts` each run so FK relationships stay in sync with fully-rebuilt dimension tables

---

## Data Profiles

| Profile | Customers | Products | Orders | POS Sales | Runtime |
|---------|-----------|----------|--------|-----------|---------|
| small | 1,000 | 100 | 10,000 | 3,000 | ~45s |
| medium | 10,000 | 500 | 100,000 | 30,000 | ~3m |
| large | 100,000 | 5,000 | 1,000,000 | 300,000 | ~30m |

```bash
.venv\Scripts\python scripts\orchestrate.py --profile small
.venv\Scripts\python scripts\orchestrate.py --profile large
```

---

## Testing

```bash
# Full suite (77 tests)
.venv\Scripts\pytest tests/ -v --tb=short

# With coverage
.venv\Scripts\pytest tests/ --cov=scripts/ --cov-report=term-missing

# Run a single test file
.venv\Scripts\pytest tests/test_transformations.py -v
```

| Test File | Coverage | What It Validates |
|-----------|----------|-------------------|
| `test_generate_data.py` | Row counts, columns, valid ranges | Faker output correctness |
| `test_transformations.py` | Cents→dollars, discount calc, status normalisation | Business logic |
| `test_load_to_postgres.py` | Engine creation, schema, truncation | DB connectivity |
| `test_project_status.py` | Docker, Postgres, env, CSVs, overall status | Pipeline health logic |
| `test_excel_export.py` | Workbook, sheets, headers, styling, currency | Export formatting |
| `test_generate_data_profiles.py` | CLI defaults, overrides, parsing | Argument resolution |

---

## Health Check

```bash
.venv\Scripts\python scripts/project_status.py
```

Checks Docker, PostgreSQL, `.env` file, raw CSVs, database row counts, and schema drift quarantine. Exits with code 0 (healthy), 1 (degraded), or 2 (unhealthy) with fix hints.

---

## CI/CD Pipeline

On every push/PR to `main`/`master`, GitHub Actions runs two jobs against a live PostgreSQL service container:

| Job | Steps |
|-----|-------|
| **Core Python** | `flake8` lint → `black --check` → `pytest` (77 tests) |
| **dbt Validation** | `pip install dbt-core` → `dbt debug` → `dbt parse` |

---

## Learning Path

This project teaches, in order:

1. **Python data engineering** — pandas, SQLAlchemy, Faker, logging, type hints
2. **SQL transformations** — dbt models, Jinja macros, star schema design
3. **Data quality** — dbt tests (not_null, unique, relationships, accepted_values), run_results.json parsing
4. **Orchestration** — circuit breaker pattern, virtual environment switching, streaming subprocess output
5. **Observability** — Discord/Slack webhooks, colour-coded embeds, rich failure metadata
6. **Schema governance** — drift detection, blueprint enforcement, quarantine
7. **CI/CD** — GitHub Actions, service containers, linting, format checking
8. **Containerization** — Docker Compose, multi-stage builds, dual-venv runtime
9. **Visualisation** — Streamlit dashboards, Plotly charts, NetworkX lineage graphs
10. **Documentation** — dbt docs, auto-generated lineage blueprints, architecture docs

---

## License

Educational use. Free to modify and share.

# RetailFlow Pipeline — Architecture & Data Lineage

> **Audience:** Data engineers, solutions architects, hiring managers.
> **Purpose:** Document the end-to-end data lifecycle, system boundaries, and operational runbook.

---

## Table of Contents

1. [Project Directory Blueprint](#1-project-directory-blueprint)
2. [ASCII Data Flow Diagram](#2-ascii-data-flow-diagram)
3. [Layer 1 — Data Generation (Ingestion)](#3-layer-1--data-generation-ingestion)
4. [Layer 2 — PostgreSQL Warehouse (Storage)](#4-layer-2--postgresql-warehouse-storage)
5. [Layer 3 — dbt Transformation (Processing)](#5-layer-3--dbt-transformation-processing)
6. [Layer 4 — Orchestration Layer](#6-layer-4--orchestration-layer)
7. [Layer 5 — Consumption Layer (Business-Facing)](#7-layer-5--consumption-layer-business-facing)
8. [Quality Guardrails — CI/CD Pipeline](#8-quality-guardrails--cicd-pipeline)
9. [Containerization & Deployment](#9-containerization--deployment)
10. [Virtual Environment Strategy](#10-virtual-environment-strategy)
11. [Step-by-Step Execution Sequence](#11-step-by-step-execution-sequence)

---

## 1. Project Directory Blueprint

```
retailflow-pipeline/
│
├── .github/workflows/         # CI/CD: GitHub Actions (lint, test, dbt-parse)
├── dbt/                       # dbt transformation layer (isolated)
│   ├── models/
│   │   ├── staging/           # Mirror raw tables, clean & type
│   │   ├── intermediate/      # Join staging tables, enrich
│   │   └── marts/             # Business-ready dims + fact
│   ├── macros/                # Jinja SQL macros (cents_to_dollars, schema override)
│   ├── tests/                 # Custom dbt data tests
│   ├── profiles.yml           # DB connection (env-var driven)
│   └── dbt_project.yml        # dbt project config
│
├── scripts/                   # Core Python ETL scripts
│   ├── orchestrate.py         # Centralised pipeline orchestrator (new)
│   ├── generate_fake_data.py  # Faker-based synthetic data generator
│   ├── load_to_postgres.py    # CSV PostgreSQL (raw schema)
│   └── project_status.py      # Health check for all pipeline components
│
├── src/                       # Python package — dashboard & export
│   ├── dashboard/app.py       # Streamlit interactive dashboard
│   ├── exports/excel_exporter.py  # Styled Excel analytics export
│   └── data_generator/__init__.py # Re-exports from scripts/
│
├── sql/                       # Raw SQL for reference
│   ├── schema/                # DDL (CREATE SCHEMA / TABLE)
│   └── analytics/             # Business analysis queries
│
├── tests/                     # pytest suite (77+ tests)
├── data/raw/                  # Generated CSVs (gitignored)
│   └── .gitkeep
├── data/rejected/             # Dead Letter Queue — rejected rows (gitignored)
│   └── .gitkeep
├── outputs/                   # Excel exports (gitignored)
│   └── .gitkeep
├── images/                    # Screenshots for README
│   └── dashboard.png
│
├── .github/workflows/ci_cd.yml  # CI/CD pipeline definition
├── ARCHITECTURE.md            # This file
├── README.md                  # Project overview & usage
├── Makefile                   # Dev workflow commands
├── requirements.txt           # Python dependencies
├── docker-compose.yml         # PostgreSQL + pgAdmin
└── .env.example               # Environment template
```

### What was removed

| Artifact | Reason |
|----------|--------|
| `dbt/.user.yml` | Auto-generated dbt metadata; not for version control |
| `tmp_*.py` | One-off diagnostic scripts |
| `images/dashbboard.png` | Typo; duplicate of `dashboard.png` |
| All `__pycache__/` | Python bytecode; already gitignored, cleaned manually |

---

## 2. ASCII Data Flow Diagram

```
 RETAILFLOW PIPELINE — END-TO-END DATA FLOW
 ============================================


  .________________________.
  |   FAKER SEED PROFILES  |       Scale profiles: small / medium / large
  |  (generate_fake_data)  |       ~10K customers, 500 products, 100K orders
  |________________________|
              |
              | CSV files (data/raw/)
              v
  .________________________.
  |  INGESTION (Python)    |       load_to_postgres.py
  |  CSV  ──>  PostgreSQL  |       Truncate + insert (idempotent)
  |         raw schema     |       Guardrails validate rows before insert
  |________________________|
         |            |
         | clean      | bad rows
         | rows       v
         |         .________________.
         |         |  DEAD LETTER   |
         |         |  QUEUE (DLQ)   |
         |         |  data/rejected/|
         |         |________________|
         |
         | PostgreSQL (port 5432)
         v
  .________________________.
  |  dbt STAGING LAYER     |       stg_customers, stg_orders, stg_products
  |  (staging schema)      |       Clean: trim, cast, deduplicate
  |________________________|
              |
              | {{ ref('stg_*') }}
              v
  .________________________.
  |  dbt INTERMEDIATE      |       int_orders_enriched
  |  (intermediate schema) |       Join orders + customers + products
  |________________________|       Compute gross/net revenue in cents
              |
              | {{ ref('int_orders_enriched') }}
              v
  .________________________.
  |  dbt MARTS LAYER       |       dim_customers, dim_products, fct_orders
  |  (marts schema)        |       Star schema: dims + fact
  |________________________|       Revenue converted to dollars
              |
              |                  .________________________.
              |                  |  ORCHESTRATION LAYER    |
              |                  |  (scripts/orchestrate)  |
              +----------------> |  Sequential DAG         |
              |                  |  Automatic venv switch  |
              |                  |  Circuit breaker        |
              |                  |________________________|
              |                             |
              |                  ┌──────────┴──────────┐
              |                  │                     │
              v                  v                     v
  .________________________.    ._____________________________.
  |  STREAMLIT DASHBOARD   |    |  EXCEL EXPORTER             |
  |  (src/dashboard/app.py) |    |  (src/exports/             |
  |  Live KPI monitoring   |    |   excel_exporter.py)        |
  |  Plotly charts         |    |  4 analytics sheets         |
  |  5-min cache + refresh |    |  Styled .xlsx output        |
  |________________________|    |_____________________________|
```

---

## 3. Layer 1 — Data Generation (Ingestion)

### Synthetic Data Engine — `scripts/generate_fake_data.py`

**Purpose:** Seed realistic transactional data for development and testing.

**Scale Profiles (CLI-driven):**

| Profile  | `--profile` | Customers | Products | Orders   | Runtime (approx) |
|----------|-------------|-----------|----------|----------|------------------|
| Small    | `small`     | 1,000     | 100      | 10,000   | ~2 seconds       |
| Medium   | `medium`    | 10,000    | 500      | 100,000  | ~15 seconds      |
| Large    | `large`     | 100,000   | 5,000    | 1,000,000| ~3 minutes       |

**Resolution order:** `--profile` sets defaults → explicit `--customers` / `--products` / `--orders` flags override individual dimensions.

**Output:** 3 CSV files written to `data/raw/`:
- `customers.csv` — UUID, name, email (guaranteed unique), country, city, signup_date, age, gender
- `products.csv` — UUID, name, category (weighted: Clothing 35%, Electronics 25%, Home 25%, Food 15%), price_cents, stock, supplier_country
- `orders.csv` — UUID, FK→customers, FK→products, quantity, order_date, status (completed 80%, returned 10%, pending 10%), discount_pct, shipping_days

### CSV Loader — `scripts/load_to_postgres.py`

**Purpose:** Stream CSV contents into the PostgreSQL `raw` schema with data quality guardrails.

**Behavior:**
- Creates `raw` schema if missing (`CREATE SCHEMA IF NOT EXISTS`)
- Truncates each target table before loading (idempotent)
- Uses `pandas.read_csv(chunksize=10_000)` + `to_sql(method="multi")` for memory-efficient bulk inserts
- Column types: UUIDs as `string`, dates as `string` (cast later in dbt)

**Data Quality Guardrails (per entity):**

| Entity | Check | Rejection reason |
|--------|-------|------------------|
| Customers | `customer_id` not null, email contains `@` | `missing customer_id`, `missing email`, `malformed email (missing @)` |
| Products | `product_id` not null, `price_cents >= 0` | `missing product_id`, `missing price_cents`, `negative price_cents` |
| Orders | `order_id`/`customer_id`/`product_id` not null, `quantity >= 0`, `discount_pct` in `[0, 100]` | `missing order_id`, `missing customer_id`, `missing product_id`, `negative quantity`, `discount_pct out of range` |

**Dead Letter Queue (DLQ):**
- Bad rows are **not** silently dropped — they are isolated to `data/rejected/`
- Each rejected row gets a `rejection_reason` column describing the violation
- DLQ files are timestamped (`rejected_orders_20260707_033908.csv`) to prevent collisions across runs
- The pipeline continues normally; only clean rows reach PostgreSQL
- The orchestrator captures and logs the loaded vs. rejected count at the end of the ingestion step

---

## 4. Layer 2 — PostgreSQL Warehouse (Storage)

**Infrastructure:** PostgreSQL 15 running in Docker via `docker-compose.yml`.

| Property | Value |
|----------|-------|
| Host | `localhost:5432` |
| Database | `retailflow` |
| User | `retailflow_user` |
| Password | `retailflow_pass` (from `.env`) |
| Port | 5432 (mapped to host) |

**Schema layout after full pipeline run:**

```
raw          (schema)  — 3 tables, loaded by load_to_postgres.py
├── customers
├── products
└── orders

staging      (schema)  — 3 views, created by dbt
├── stg_customers
├── stg_orders
└── stg_products

intermediate (schema)  — 1 view, created by dbt
└── int_orders_enriched

marts        (schema)  — 3 tables, created by dbt
├── dim_customers
├── dim_products
└── fct_orders
```

---

## 5. Layer 3 — dbt Transformation (Processing)

**Environment:** Isolated `.venv-dbt` — only `dbt-core==1.7.14` + `dbt-postgres==1.7.14` installed. This avoids the `mashumaro` version conflict with other packages (Airflow, Great Expectations).

### Staging Models (`models/staging/`)

| Model | Source | Transformations |
|-------|--------|----------------|
| `stg_customers` | `raw.customers` | Trim whitespace, lowercase email, cast signup_date to DATE, deduplicate |
| `stg_products` | `raw.products` | Trim name/category, filter `price_cents <= 0`, deduplicate |
| `stg_orders` | `raw.orders` | Cast order_date to DATE, lowercase/trim status, filter null FKs, deduplicate |

**Materialization:** Views (lightweight, no storage).

### Intermediate Model (`models/intermediate/`)

| Model | Source | Transformations |
|-------|--------|----------------|
| `int_orders_enriched` | All 3 staging models | LEFT JOIN orders customers products, compute `gross_revenue_cents` and `net_revenue_cents` |

**Materialization:** View.

### Mart Models (`models/marts/`)

| Model | Type | Grain | Key Measures |
|-------|------|-------|-------------|
| `dim_customers` | Dimension | 1 row per customer | `total_orders`, `lifetime_value_cents` |
| `dim_products` | Dimension | 1 row per product | `total_orders`, `total_units_sold`, `total_revenue_cents` |
| `fct_orders` | Fact | 1 row per order line | `gross_revenue_dollars`, `net_revenue_dollars` (via `cents_to_dollars()` macro) |

**Materialization:** Tables (snapshot for analytics performance).

### dbt Tests

48 automated data quality tests across all models:
- `not_null` — critical columns never null
- `unique` — primary keys and email are unique
- `accepted_values` — category and status values are valid
- `relationships` — foreign keys reference valid primary keys
- 2 custom singular tests: `assert_positive_revenue`, `assert_no_null_customer_id`

---

## 6. Layer 4 — Orchestration Layer

**Script:** `scripts/orchestrate.py`

The orchestrator manages the end-to-end pipeline lifecycle as a sequential DAG:

```
[1] Generate Data ──> [2] Load PostgreSQL ──> [3] dbt Run ──> [4] dbt Test ──> [5] Excel Export

  (.venv)                 (.venv)              (.venv-dbt)      (.venv-dbt)        (.venv)
```

### Key Design Decisions

| Concern | Implementation |
|---------|---------------|
| **Environment switching** | Each step resolves the correct executable: `.venv\Scripts\python.exe` for steps 1, 2, 5; `.venv-dbt\Scripts\dbt.exe` for steps 3, 4 |
| **Circuit breaker** | `subprocess.Popen` runs each step; non-zero `returncode` triggers `sys.exit(1)` before proceeding to the next step |
| **Streaming output** | `stdout=subprocess.PIPE` with real-time line-by-line printing so the user sees progress as it happens |
| **Run duration** | Each step is timed with `time.monotonic()`; total pipeline time is printed on completion |
| **Profile propagation** | `--profile` is parsed at the orchestrator level and forwarded to `generate_fake_data.py` |
| **DLQ summary capture** | Step 2 output is scanned for a `DLQ_SUMMARY:` JSON line; orchestrator logs loaded vs. rejected counts per table |
| **dbt step splitting** | `dbt run` is split into 3 sub-steps (`staging`, `intermediate`, `marts`) with individual failure handling |

### Usage

```bash
# Default medium profile
python scripts/orchestrate.py

# Small profile (fast for testing)
python scripts/orchestrate.py --profile small

# Via Make shortcut
make pipeline
```

### CLI Reference

```
usage: orchestrate.py [-h] [--profile {small,medium,large}]

options:
  -h, --help            Show help message
  --profile {small,medium,large}
                        Scale profile passed to the data generator (default: medium)
```

---

## 7. Layer 5 — Consumption Layer (Business-Facing)

### 7.1 Streamlit Dashboard — `src/dashboard/app.py`

**Purpose:** Real-time KPI monitoring for business stakeholders.

**Architecture:**
```
Browser ← HTTP/WS ← Streamlit Server ← SQLAlchemy ← PostgreSQL
                          │
                    ┌─────┴─────┐
                    │ Cache     │
                    │ (5 min    │
                    │  TTL)     │
                    └───────────┘
```

**Key design decisions:**

| Concern | Implementation |
|---------|---------------|
| Data freshness | `@st.cache_data(ttl=300)` — auto-refreshes every 5 minutes |
| Connectivity | `@st.cache_resource` for the SQLAlchemy engine (singleton) |
| dbt-missing resilience | `_safe_query()` catches `UndefinedTable` → shows `st.warning()` + expandable traceback |
| Charts | Plotly Express (`px.line`, `px.bar`) — interactive hover, zoom, unified tooltips |
| Auto-refresh | Sidebar checkbox + `st.rerun(ttl=interval_ms)` |
| Export trigger | Sidebar button → `subprocess.run([sys.executable, "-m", "src.exports.excel_exporter"])` |

**Dashboard sections:**
1. **Sidebar** — Refresh button, auto-refresh toggle, category filter, Export to Excel, data freshness timestamp
2. **KPI row** — Total Orders, Total Net Revenue, Active Customers, Avg Order Value, Returned/Pending, Return Rate %
3. **Monthly Sales Trend** — Plotly line chart (revenue + orders over time)
4. **Category Performance** — Grouped bar chart (revenue + units by category)
5. **Revenue by Country** — Horizontal bar chart, colored by customer count (top 15)
6. **Top 10 Customers** — Data table with formatted revenue

### 7.2 Excel Analytics Exporter — `src/exports/excel_exporter.py`

**Purpose:** Generate executive-ready `.xlsx` files for offline analysis.

**Queries executed against `marts` schema:**

| Sheet | SQL Query |
|-------|-----------|
| Top Customers | Top 10 customers by `SUM(net_revenue_dollars)` |
| Monthly Sales | Monthly `COUNT(orders)`, `SUM(revenue)`, MoM growth % |
| Category Performance | Revenue, units sold, revenue share % per category |
| Cohort Analysis | Customer retention and spending by first-purchase month |

**Styling:**
- Dark blue header fill (`1F4E79`) with white bold font
- Auto-fitted column widths
- Currency format (`$#,##0.00`) on revenue columns
- Timestamped filename: `retail_analytics_YYYYMMDD_HHMMSS.xlsx`

**Output:** Written to `outputs/` directory (gitignored).

---

## 8. Quality Guardrails — CI/CD Pipeline

**File:** `.github/workflows/ci_cd.yml`

```
                    ┌─────────────────────────────────────┐
                    │         GitHub Actions               │
                    │                                      │
                    │  Push / PR to main/master            │
                    │         │                            │
                    │    ┌────┴────┐                       │
                    │    │         │                       │
                    │    ▼         ▼                       │
                    │  ┌────┐  ┌────┐                      │
                    │  │Core│  │dbt │                      │
                    │  │Py  │  │Val │                      │
                    │  └─┬──┘  └─┬──┘                      │
                    │    │       │                         │
                    │    ▼       ▼                         │
                    │  ┌────┐  ┌────┐                      │
                    │  │flake8│  │dbt │                     │
                    │  │py test│ │debug│                    │
                    │  │77 tests│ │parse│                   │
                    │  └────┘  └────┘                      │
                    │                                      │
                    │  PostgreSQL 15 (service)             │
                    └─────────────────────────────────────┘
```

### Job 1 — Core Python (Lint & Test)

| Step | Tool | What it validates |
|------|------|-------------------|
| 1 | `actions/checkout@v4` | Pulls the repo |
| 2 | `actions/setup-python@v5` | Python 3.12, pip cache |
| 3 | `pip install -r requirements.txt` | Installs pandas, pytest, openpyxl, streamlit, flake8, black |
| 4 | `flake8` | Code style (unused imports, undefined variables) — exits non-zero on violations |
| 5 | `black --check` | Formatting consistency — continues on warning (non-blocking) |
| 6 | `pytest` | 77+ unit tests — mocks external DB, validates business logic |

### Job 2 — dbt Validation

| Step | Tool | What it validates |
|------|------|-------------------|
| 1 | `pip install dbt-core==1.7.14 dbt-postgres==1.7.14` | Simulates `.venv-dbt` isolation |
| 2 | `CREATE SCHEMA raw` | Creates source tables for dbt to reference |
| 3 | `dbt debug` | Connection test — verifies `profiles.yml` + pg reachable |
| 4 | `dbt parse` | SQL compilation — validates all models, refs, sources, and macros |

**PostgreSQL service container:** Both jobs share a temporary Postgres 15 container (`postgres:15`) with health checks, so dbt commands have a live database to connect to.

---

## 9. Containerization & Deployment

The project is fully containerized via Docker Compose with two core services:

| Service | Container | Base Image | Purpose |
|---------|-----------|------------|---------|
| `db` | `retailflow-db` | `postgres:15-alpine` | PostgreSQL warehouse with `pg_isready` healthcheck |
| `app` | `retailflow-app` | `python:3.12-slim` (via `Dockerfile`) | Streamlit dashboard + ETL orchestration |
| `pgadmin` | `retailflow-pgadmin` | `dpage/pgadmin4:latest` | Web-based PostgreSQL admin (optional) |

### Dockerfile Architecture

The `Dockerfile` uses four logical layers for optimal caching:

```
Layer 1: System deps (gcc, libpq-dev, curl)
         └── RUN apt-get install ...
Layer 2: Core Python deps (pandas, streamlit, airflow, GE, ...)
         └── RUN pip install ...
Layer 3: Isolated dbt venv (/opt/dbt-venv)
         └── dbt-core==1.7.14 + dbt-postgres==1.7.14
Layer 4: Application code (scripts/, src/, dbt/, tests/)
```

**Why an isolated dbt venv?** `dbt-core` 1.7.x pins `mashumaro<4` while Airflow and Great Expectations require `mashumaro>=4`. Installing dbt in a separate venv with a symlink into `PATH` avoids the conflict while keeping both runtimes accessible.

### Cross-Platform Orchestrator

The `_dbt_exe()` and `_py_exe()` functions in `scripts/orchestrate.py` now detect the platform:

| Platform | `_py_exe()` | `_dbt_exe()` |
|----------|-------------|--------------|
| Windows (local) | `.venv\Scripts\python.exe` | `.venv-dbt\Scripts\dbt.exe` |
| Linux (container) | `sys.executable` | `$DBT_EXECUTABLE` env var (→ `/opt/dbt-venv/bin/dbt`) |

### Network Configuration

Inside the Docker network, the database is reachable as `db` (the Compose service name). The `app` service receives `POSTGRES_HOST: db` via its `environment` block, overriding the `.env` default of `localhost`. All Python connection helpers (`get_engine()`) read from env vars at runtime, so they adapt automatically — no code changes needed.

### Deployment Runbook

**Prerequisites:**
- Docker Engine 24+ and Docker Compose v2 installed
- Ports 5432, 8501, 5050 free on the host

**Step 1 — Build & Launch (single command):**

```bash
docker compose up --build
```

This single command:
1. Builds the `app` image from the Dockerfile
2. Pulls `postgres:15-alpine` and `dpage/pgadmin4:latest`
3. Starts `db` first (with 15s grace period healthcheck)
4. Starts `app` only after `db` reports healthy
5. Starts `pgadmin` after `db` is healthy
6. Mounts volumes for persistent data
7. Maps ports: 5432 (Postgres), 8501 (Streamlit), 5050 (pgAdmin)

**Step 2 — Access the Dashboard:**

Open [http://localhost:8501](http://localhost:8501) in your browser. The Streamlit dashboard connects to the containerized PostgreSQL via the `db` hostname automatically.

**Step 3 — Run the Full Pipeline inside the Container:**

```bash
# Exec into the running app container
docker exec -it retailflow-app python scripts/orchestrate.py --profile small

# Or override the default CMD at launch:
docker compose run --rm app python scripts/orchestrate.py --profile small
```

**Step 4 — Tear Down:**

```bash
docker compose down          # Stop containers
docker compose down -v       # Stop + delete volumes (⚠️ destroys data)
```

### Service Dependency Graph

```
docker compose up --build
        │
        ▼
    ┌──────┐
    │  db  │  (postgres:15-alpine, port 5432)
    │      │  healthcheck: pg_isready
    └──┬───┘
       │ condition: service_healthy
       ├──────────────────┐
       ▼                  ▼
   ┌──────┐         ┌──────────┐
   │ app  │         │ pgadmin  │
   │port  │         │ port     │
   │ 8501 │         │ 5050     │
   └──────┘         └──────────┘
```

---

## 10. Virtual Environment Strategy

The project uses **two independent Python virtual environments** to isolate conflicting dependency chains.

| Environment | Location | Contents | When to use |
|-------------|----------|----------|-------------|
| **Main** | `.venv/` | pandas, SQLAlchemy, streamlit, plotly, openpyxl, pytest, flake8, black, Faker, Airflow, Great Expectations | Data generation, loading, dashboard, Excel export, testing |
| **dbt** | `.venv-dbt/` | `dbt-core==1.7.14`, `dbt-postgres==1.7.14` | All `dbt` commands (run, test, debug, parse) |

**Why two envs?** `dbt-core` pins `mashumaro<4` while Airflow and Great Expectations require `mashumaro>=4`. A single environment cannot satisfy both. The Makefile, orchestrator, and CI/CD workflow all respect this split.

**Makefile reference:**
```bash
make setup            # Creates .venv + installs all deps
make setup-dbt        # Creates .venv-dbt + installs dbt
make dbt-run          # Uses .venv-dbt\Scripts\dbt
make dbt-test         # Uses .venv-dbt\Scripts\dbt
make dashboard        # Uses .venv\Scripts\streamlit
make export           # Uses .venv\Scripts\python -m src.exports.excel_exporter
make pipeline         # Uses .venv\Scripts\python scripts\orchestrate.py
```

---

## 11. Step-by-Step Execution Sequence

> Use this checklist when cloning the repo onto a **fresh machine**. Run commands in order.

### Prerequisites

- [ ] Python 3.12+ installed
- [ ] Docker Desktop installed and running
- [ ] Git installed

### Setup

```bash
# 1. Clone
git clone https://github.com/elhussienysabry/retailflow-pipeline.git
cd retailflow-pipeline

# 2. Environment file
cp .env.example .env
# (edit .env if needed — defaults work for local dev)

# 3. Main virtual environment + deps
make setup

# 4. Isolated dbt environment
make setup-dbt

# 5. Start PostgreSQL
make run
```

### Full Pipeline (Single Command)

```bash
# 6. Run everything — generate, load, transform, test, export
make pipeline
# OR: .venv\Scripts\python scripts\orchestrate.py

# With a specific profile:
.venv\Scripts\python scripts\orchestrate.py --profile small
```

### Step-by-Step (Manual)

```bash
# 6. Generate synthetic data
make generate-data
# Options: --profile small | medium | large

# 7. Load CSVs into PostgreSQL raw schema
make load-data

# 8. Run dbt transformations (staging  intermediate  marts)
make dbt-run

# 9. Run dbt data quality tests
make dbt-test

# 10. Export styled Excel workbook
make export
# Saves to: outputs/retail_analytics_*.xlsx

# 11. Launch interactive dashboard
make dashboard
# Opens at: http://localhost:8501
```

### Validation

```bash
# 12. Run project health check
make status

# 13. Run full Python test suite
make test
# Or: .venv\Scripts\python -m pytest tests/ -v --tb=short
```

### CI/CD (GitHub)

Once pushed to `main` / `master`, the workflow at `.github/workflows/ci_cd.yml` automatically:
1. Spins up PostgreSQL
2. Installs all Python deps
3. Runs `flake8` + `black --check`
4. Executes 77+ pytest tests
5. Installs dbt in isolation
6. Runs `dbt debug` + `dbt parse`

---

*Architecture document v1.1 — Generated for the RetailFlow Pipeline project.*

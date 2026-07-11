# RetailFlow Pipeline — Architecture & Data Lineage

> **Audience:** Data engineers, solutions architects, hiring managers.
> **Purpose:** Document the end-to-end data lifecycle, system boundaries, and operational runbook.

---

## Table of Contents

1. [Project Directory Blueprint](#1-project-directory-blueprint)
2. [ASCII Data Flow Diagram](#2-ascii-data-flow-diagram)
3. [Layer 0 — Schema Drift Detection](#3-layer-0--schema-drift-detection)
4. [Layer 1 — Data Generation (Ingestion)](#4-layer-1--data-generation-ingestion)
5. [Layer 2 — PostgreSQL Warehouse (Storage)](#5-layer-2--postgresql-warehouse-storage)
6. [Layer 3 — dbt Transformation (Processing)](#6-layer-3--dbt-transformation-processing)
7. [Layer 4 — Orchestration Layer](#7-layer-4--orchestration-layer)
8. [Layer 5 — Consumption Layer (Business-Facing)](#8-layer-5--consumption-layer-business-facing)
9. [Quality Guardrails — CI/CD Pipeline](#9-quality-guardrails--cicd-pipeline)
10. [Containerization & Deployment](#10-containerization--deployment)
11. [Virtual Environment Strategy](#11-virtual-environment-strategy)
12. [Layer 6 — Observability & Alerting](#12-layer-6--observability--alerting)
13. [Layer 7 — Data Governance & Lineage](#13-layer-7--data-governance--lineage)
14. [Layer 8 — Data Profiling](#14-layer-8--data-profiling)
15. [Step-by-Step Execution Sequence](#15-step-by-step-execution-sequence)

---

## 1. Project Directory Blueprint

```
retailflow-pipeline/
│
├── .venv/                        # Main Python env (pandas, streamlit, etc.)
├── .venv-dbt/                    # Isolated dbt env (dbt-core, dbt-postgres)
│
├── scripts/                      # Core Python ETL scripts
│   ├── orchestrate.py            # 8-step pipeline orchestrator + circuit breaker
│   ├── generate_fake_data.py     # Faker synthetic data (CSV + JSON)
│   ├── load_to_postgres.py       # Hybrid ingestion, schema drift detector, PII hash
│   ├── generate_lineage.py       # NetworkX lineage graph renderer
│   ├── generate_profiling.py     # Pandas data profiling → HTML report
│   ├── alerts.py                 # Discord/Slack webhook dispatcher
│   └── project_status.py         # E2E health check
│
├── dbt/                          # dbt transformation project
│   ├── models/
│   │   ├── staging/              # Clean & type (3 views)
│   │   ├── intermediate/         # Join & enrich (1 view)
│   │   └── marts/                # Business-ready dims + fact (3 tables)
│   ├── tests/                    # Custom singular tests (2)
│   ├── macros/                   # Jinja SQL macros
│   ├── profiles.yml              # DB connection (env-var driven)
│   └── dbt_project.yml           # dbt config
│
├── src/                          # Python package
│   ├── dashboard/app.py          # Streamlit KPI dashboard
│   ├── exports/excel_exporter.py # Styled Excel workbook
│   └── data_generator/           # Re-exports from scripts/
│
├── sql/
│   ├── schema/                   # DDL (CREATE SCHEMA / TABLE)
│   └── analytics/                # Business analysis queries
│
├── tests/                        # 77+ pytest tests
├── data/
│   ├── raw/                      # Generated CSVs + JSON (gitignored)
│   ├── rejected/                 # Dead Letter Queue — rejected rows (gitignored)
│   └── rejected_schemas/         # Schema drift quarantine (gitignored)
├── outputs/                      # Excel exports (gitignored)
├── docs/
│   ├── lineage/                  # current_data_lineage.png
│   └── profiling/                # retailflow_data_profile.html
│
├── .github/workflows/ci_cd.yml   # CI/CD: lint, test, dbt-parse
├── docker-compose.yml            # PostgreSQL 15 + app + pgAdmin
├── Dockerfile                    # Multi-stage, dual-venv image
├── Makefile                      # Dev workflow commands
├── ARCHITECTURE.md               # This file
└── README.md                     # Project overview
```

---

## 2. ASCII Data Flow Diagram

```
  RETAILFLOW PIPELINE — END-TO-END DATA FLOW
  ============================================


  .________________________.
  |  FAKER DATA GENERATOR  |       Scale profiles: small / medium / large
  |  (generate_fake_data)  |       4 output files in data/raw/
  |________________________|
         |                   \
         |                    \  JSON files
         | CSV files           \   pos_store_sales.json
         | (data/raw/)          \
         | customers.csv         \
         | products.csv           \
         | orders.csv              \
         |                          |
         v                          v
  ._______________________.  ._____________________________.
  |  SCHEMA DRIFT CHECK   |  |  For each source file, the  |
  |  (Layer 0)            |  |  drift detector compares    |
  |                       |  |  actual columns + dtypes    |
  |  ┌─ Compare against   |  |  against SCHEMA_BLUEPRINT. |
  |  │  SCHEMA_BLUEPRINT  |  |                             |
  |  │                     |  |  CRITICAL: missing column  |
  |  │  Pass → continue   |  |  or type mismatch. File    |
  |  │  WARN → extra col  |  |  moved to data/rejected_   |
  |  │  CRITICAL → halt,  |  |  schemas/. Pipeline halts. |
  |  │  quarantine file   |  |  Red alert fired.           |
  |  │____________________|  |_____________________________|
         |                          |
         | (pass)                   | (critical)
         v                          v
  ._________________________.  .__________________________.
  |  CSV INGESTION (Python) |  |  SCHEMA QUARANTINE       |
  |  ┌─ pandas chunksize    |  |  data/rejected_schemas/  |
  |  │ Truncate + insert    |  |  (file isolated, never   |
  |  │ Validation guard-    |  |  reaches PostgreSQL)     |
  |  │ rails per entity     |  |__________________________|
  |  │ PII anonymisation    |
  |  │ (customers only)     |
  |  │ DLQ bad rows         |
  |  │______________________|
         |            |              |
         | clean      | bad rows     |
         | rows       v              |
         |         .________________. |
         |         │  DEAD LETTER   │ |
         |         │  QUEUE (DLQ)   │ |
         |         │  data/rejected/│ |
         |         │________________│ |
         |                           |
         |            _______________/
         |           /
         v           v
  .________________________________________.
  │   PostgreSQL raw schema (port 5432)     │
  │   raw.customers    raw.orders           │
  │   raw.products     raw.pos_store_sales  │
  |_________________________________________|
         |
         |  Schema Harmonisation
         |  _harmonize_and_upsert_unified()
         |  Maps: transaction_timestamp → transaction_date
         |        sale_id → transaction_id
         |  Adds: source_system ('online' / 'pos')
         v
  .________________________________________.
  │   raw.unified_transactions              │
  │   Primary Key: (transaction_id,         │
  │                 source_system)          │
  │   Upsert: INSERT ... ON CONFLICT        │
  │          DO UPDATE                      │
  |_________________________________________|
         |
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
              |        ┌──────────────────────────────────────────────┐
              |        │         ORCHESTRATION LAYER (Layer 4)        │
              |        │         scripts/orchestrate.py               │
              +------->│   1. Generate Data        (.venv)            │
              |        │   2. Load + Drift Check   (.venv)            │
              |        │   3. dbt Run              (.venv-dbt)        │
              |        │   4. dbt Test             (.venv-dbt)        │
              |        │   5. Excel Export         (.venv)            │
              |        │   6. dbt Docs Generate    (.venv-dbt)        │
              |        │   7. Lineage Graph        (.venv)            │
              |        │   8. Data Profile Report  (.venv)            │
              |        │                                               │
              |        │   Circuit breaker: halt on non-zero exit      │
              |        │   Alerts: DLQ warning, dbt critical, success  │
              |        │   Schema drift: CRITICAL → halt, WARN → pass  │
              |        └──────────────────────────────────────────────┘
              |                    |           |              |
              |         ┌──────────┴──────────┐ │              |
              |         │                     │ │              |
              v         v                     v v              v
  .________________________.   ._________________________.   .___________________________.
  |  STREAMLIT DASHBOARD   |   |  EXCEL EXPORTER         |   |  ALERTING ENGINE          |
  |  (src/dashboard/app.py) |   |  (src/exports/          |   |  scripts/alerts.py         |
  |  Live KPI monitoring   |   |   excel_exporter.py)     |   │  Discord / Slack webhook   │
  |  Plotly charts         |   |  4 analytics sheets      |   │  Colour-coded embeds       │
  |  5-min cache + refresh |   |  Styled .xlsx output     |   │  (green/amber/red)         │
  |________________________|   |__________________________|   │  Graceful skip if unset    │
                                                              │  Rich dbt failure metadata │
                                                              │  Schema drift alerts       │
                                                              |____________________________|
                                                                         |
                                                              .___________________________.
                                                              |  dbt DOCS CATALOG         │
                                                              │  dbt docs generate         │
                                                              │  Metadata catalog +        │
                                                              │  interactive lineage       │
                                                              │  graph (make docs to view) │
                                                              |____________________________|
                                                                         |
                                                              .___________________________.
                                                              |  LINEAGE BLUEPRINT        │
                                                              │  generate_lineage.py       │
                                                              │  NetworkX → 200 DPI PNG    │
                                                              │  docs/lineage/             │
                                                              |____________________________|
                                                                         |
                                                              .___________________________.
                                                              |  DATA PROFILE REPORT      │
                                                              │  generate_profiling.py     │
                                                              │  pandas → interactive HTML │
                                                              │  docs/profiling/           │
                                                              |____________________________|
```

---

## 3. Layer 0 — Schema Drift Detection

**Module:** `scripts/load_to_postgres.py` (built into `main()`)

The schema drift detector runs **before** any data is loaded into PostgreSQL. Each source file is inspected (header row for CSV, first record for JSON) and compared against a `SCHEMA_BLUEPRINT` dictionary that defines expected column names and pandas dtypes for every entity.

### Blueprint Definition

```python
SCHEMA_BLUEPRINT = {
    "customers": {
        "required_columns": {
            "customer_id": "string", "first_name": "string",
            "last_name": "string", "email": "string",
            "country": "string", "city": "string",
            "signup_date": "string", "age": "int64", "gender": "string",
        },
    },
    "products": {
        "required_columns": {
            "product_id": "string", "name": "string",
            "category": "string", "price_cents": "int64",
            "stock_quantity": "int64", "supplier_country": "string",
        },
    },
    "orders": {
        "required_columns": {
            "order_id": "string", "customer_id": "string",
            "product_id": "string", "quantity": "int64",
            "order_date": "string", "status": "string",
            "discount_pct": "int64", "shipping_days": "int64",
        },
    },
    "pos_store_sales": {
        "required_columns": {
            "sale_id": "string", "store_id": "string",
            "product_id": "string", "quantity": "int64",
            "unit_price_cents": "int64", "total_amount": "int64",
            "transaction_timestamp": "string", "payment_method": "string",
        },
    },
}
```

### Drift Severity Levels

| Severity | Condition | Action | Alert |
|----------|-----------|--------|-------|
| **CRITICAL** | Required column missing, or pandas dtype does not match blueprint (after normalisation) | File moved to `data/rejected_schemas/`. Pipeline halts with exit code 2. | Red embed sent to webhook listing the entity, missing columns, and type mismatches. |
| **WARNING** | Extra columns present that are not in the blueprint | Pipeline continues. Extra columns logged. | Amber embed sent with the entity and extra column names. |
| **NONE** | Schema matches blueprint exactly | Normal processing proceeds. | — |

### Drift Decision Tree

```
                     Read first row of source file
                               │
                    ┌──────────┴──────────┐
                    ▼                     ▼
           ┌────────────────┐   ┌─────────────────┐
           │ Missing        │   │ All required     │
           │ required cols  │   │ columns present  │
           └───────┬────────┘   └─────────┬────────┘
                   │                      │
                   │              ┌───────┴───────┐
                   │              ▼               ▼
                   │     ┌──────────────┐  ┌──────────────┐
                   │     │ Type         │  │ Types match  │
                   │     │ mismatch     │  │              │
                   │     └──────┬───────┘  └──────┬───────┘
                   │            │                  │
                   │            │          ┌───────┴───────┐
                   │            │          ▼               ▼
                   │            │  ┌──────────────┐  ┌──────────────┐
                   │            │  │ Extra cols   │  │ Exact match  │
                   │            │  │ present      │  │              │
                   │            │  └──────┬───────┘  └──────┬───────┘
                   │            │         │                  │
                   ▼            ▼         ▼                  ▼
             ┌──────────┐ ┌──────────┐ ┌──────────┐  ┌──────────┐
             │Critical  │ │Critical  │ │Warning   │  │Pass      │
             │Halt      │ │Halt      │ │Continue  │  │Continue  │
             │Quarantine│ │Quarantine│ │Amber     │  │No alert  │
             │Red alert │ │Red alert │ │alert     │  │          │
             └──────────┘ └──────────┘ └──────────┘  └──────────┘
```

### Dtype Normalisation

pandas dtype strings vary across versions and platforms (e.g. `"string"` vs `"str"` vs `"object"`). The drift detector normalizes all dtypes before comparison:

| Raw dtype | Normalised |
|-----------|------------|
| `"string"`, `"str"`, `"object"` | `"string"` |
| `"int64"`, `"Int64"`, `"int32"` | `"int64"` |
| `"float64"`, `"Float64"`, `"float32"` | `"float64"` |
| `"bool"`, `"boolean"` | `"bool"` |

### Orchestrator Integration

When `load_to_postgres.py` detects critical drift, it prints a `SCHEMA_DRIFT_CRITICAL:` JSON line and exits with code 2. The orchestrator parses these markers from the captured subprocess output:

```
SCHEMA_DRIFT_CRITICAL:{"entity": "customers", "severity": "critical",
  "filepath": "...", "missing_columns": ["age"], "type_mismatches": {}}
```

The orchestrator logs the event, dispatches a red alert, and halts subsequent steps. Warning drift is logged and alerted but does not halt the pipeline.

### Health Check Integration

`scripts/project_status.py` checks `data/rejected_schemas/` for quarantined files. If any exist, the status report shows a `FAIL` status with the file names and a fix hint pointing to `SCHEMA_BLUEPRINT`.

---

## 4. Layer 1 — Data Generation (Ingestion)

### Synthetic Data Engine — `scripts/generate_fake_data.py`

**Purpose:** Seed realistic transactional data for development and testing.

**Scale Profiles (CLI-driven):**

| Profile | `--profile` | Customers | Products | Orders | POS Sales | Runtime (approx) |
|---------|-------------|-----------|----------|--------|-----------|------------------|
| Small | `small` | 1,000 | 100 | 10,000 | 3,000 | ~2 seconds |
| Medium | `medium` | 10,000 | 500 | 100,000 | 30,000 | ~15 seconds |
| Large | `large` | 100,000 | 5,000 | 1,000,000 | 300,000 | ~3 minutes |

**Resolution order:** `--profile` sets defaults → explicit `--customers` / `--products` / `--orders` / `--pos-sales` flags override individual dimensions.

**Output:** 4 files written to `data/raw/`:
- `customers.csv` — UUID, name, email (guaranteed unique), country, city, signup_date, age, gender
- `products.csv` — UUID, name, category (weighted), price_cents, stock_quantity, supplier_country
- `orders.csv` — UUID, FK→customers, FK→products, quantity, order_date, status (completed 80%, returned 10%, pending 10%), discount_pct, shipping_days
- `pos_store_sales.json` — sale_id, store_id, product_id, quantity, unit_price_cents, total_amount, transaction_timestamp, payment_method

### Hybrid Ingestion Engine — `scripts/load_to_postgres.py`

**Purpose:** Ingest both CSV (e-commerce) and JSON (POS store) source files into the PostgreSQL `raw` schema, then run schema harmonisation to produce a unified transactions table.

**Behavior:**
- Validates schema drift against `SCHEMA_BLUEPRINT` **before** loading each file (see Layer 0)
- Creates `raw` schema if missing (`CREATE SCHEMA IF NOT EXISTS`)
- Truncates each target table before loading (idempotent)
- CSV files use `pandas.read_csv(chunksize=10_000)` + validation guardrails + `to_sql(method="multi")`
- JSON files use `json.load()` → `pd.DataFrame()` → `to_sql(if_exists="replace")`
- Column types: UUIDs as `string`, dates as `string` (cast later in dbt)

**Data Quality Guardrails (per entity):**

| Entity | Check | Rejection reason |
|--------|-------|------------------|
| Customers | `customer_id` not null, email contains `@` | `missing customer_id`, `missing email`, `malformed email (missing @)` |
| Products | `product_id` not null, `price_cents >= 0` | `missing product_id`, `missing price_cents`, `negative price_cents` |
| Orders | `order_id`/`customer_id`/`product_id` not null, `quantity >= 0`, `discount_pct` in `[0, 100]` | `missing order_id`, `missing customer_id`, `missing product_id`, `negative quantity`, `discount_pct out of range` |

**Dead Letter Queue (DLQ):**
- Bad rows are isolated to `data/rejected/` with a `rejection_reason` column
- DLQ files are timestamped (`rejected_orders_20260707_033908.csv`)
- The pipeline continues normally; only clean rows reach PostgreSQL
- The orchestrator captures and logs the loaded vs. rejected count

**PII Anonymization (GDPR / CCPA Compliance):**
- `first_name`, `last_name`, and `email` are SHA-256 hashed before storage
- Hashing is deterministic: whitespace-stripped, lowercased, then hex-digested
- Null values preserved as-is
- Raw CSV files on disk remain unmodified

**Schema Harmonisation & Unified Upsert:**
- After all raw source tables are loaded, `_harmonize_and_upsert_unified()` creates (if missing) and populates `raw.unified_transactions`
- Column mapping:

| Unified Column | Online Source | POS Source |
|---|---|---|
| `transaction_id` | `order_id` | `sale_id` |
| `source_system` | `'online'` | `'pos'` |
| `transaction_date` | `order_date` | `transaction_timestamp::date` |
| `total_amount` | `NULL` (computed in dbt) | `total_amount` |
| `store_id` | `NULL` | `store_id` |
| `customer_id` | `customer_id` | `NULL` |
| `status` | `status` | `'completed'` |
| `payment_method` | `NULL` | `payment_method` |

- Uses `INSERT ... ON CONFLICT (transaction_id, source_system) DO UPDATE` for idempotent re-runs

---

## 5. Layer 2 — PostgreSQL Warehouse (Storage)

**Infrastructure:** PostgreSQL 15 running in Docker via `docker-compose.yml`.

| Property | Value |
|----------|-------|
| Host | `localhost:5432` |
| Database | `retailflow` |
| User | `retailflow_user` |
| Password | `retailflow_pass` (from `.env`) |

**Schema layout after full pipeline run:**

```
raw          (schema)  — 5 tables, loaded by load_to_postgres.py
├── customers
├── products
├── orders
├── pos_store_sales
└── unified_transactions

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

## 6. Layer 3 — dbt Transformation (Processing)

**Environment:** Isolated `.venv-dbt` — only `dbt-core==1.7.14` + `dbt-postgres==1.7.14`.

### Staging Models (`models/staging/`)

| Model | Source | Transformations |
|-------|--------|----------------|
| `stg_customers` | `raw.customers` | Trim whitespace, lowercase email, cast signup_date to DATE, deduplicate |
| `stg_products` | `raw.products` | Trim name/category, filter `price_cents <= 0`, deduplicate |
| `stg_orders` | `raw.orders` | Cast order_date to DATE, lowercase/trim status, filter null FKs, deduplicate |

**Materialization:** Views.

### Intermediate Model (`models/intermediate/`)

| Model | Source | Transformations |
|-------|--------|----------------|
| `int_orders_enriched` | All 3 staging models | LEFT JOIN orders + customers + products, compute `gross_revenue_cents` and `net_revenue_cents` |

**Materialization:** View.

### Mart Models (`models/marts/`)

| Model | Type | Grain | Key Measures |
|-------|------|-------|-------------|
| `dim_customers` | Dimension | 1 row per customer | `total_orders`, `lifetime_value_cents` |
| `dim_products` | Dimension | 1 row per product | `total_orders`, `total_units_sold`, `total_revenue_cents` |
| `fct_orders` | Fact | 1 row per order line | `gross_revenue_dollars`, `net_revenue_dollars` |

**Materialization:**
- `dim_customers`, `dim_products` — **table** (full refresh each run)
- `fct_orders` — **incremental** with `unique_key='order_id'`

**`fct_orders` Incremental Strategy:**
- `unique_key: order_id` enables UPSERT semantics (`MERGE` on PostgreSQL)
- `{% if is_incremental() %}` filters to `WHERE order_date >= (SELECT MAX(order_date) FROM {{ this }})`
- First run (empty target) loads all historical data
- Subsequent runs are lightweight single-day scans

### dbt Tests

48 automated data quality tests across all models:
- `not_null` — critical columns never null
- `unique` — primary keys and email are unique
- `accepted_values` — category and status values are valid
- `relationships` — foreign keys reference valid primary keys
- 2 custom singular tests: `assert_positive_revenue`, `assert_no_null_customer_id`

---

## 7. Layer 4 — Orchestration Layer

**Script:** `scripts/orchestrate.py`

The orchestrator manages the end-to-end pipeline lifecycle as a sequential DAG with 8 steps:

```
[1] Generate Data   ──> [2] Load PostgreSQL  ──> [3] dbt Run  ──> [4] dbt Test
       (.venv)              (.venv)               (.venv-dbt)      (.venv-dbt)
                                                       │
                                                       v
[5] Excel Export   ──> [6] dbt Docs Gen   ──> [7] Lineage Graph ──> [8] Profile Report
      (.venv)              (.venv-dbt)            (.venv)              (.venv)
```

### Key Design Decisions

| Concern | Implementation |
|---------|---------------|
| **Environment switching** | Each step resolves the correct executable: `_py_exe()` for `.venv`, `_dbt_exe()` for `.venv-dbt` |
| **Circuit breaker** | Non-zero `returncode` → `sys.exit(1)` before proceeding. Exit code 2 = schema drift halt |
| **Streaming output** | `subprocess.PIPE` with real-time line-by-line printing |
| **Step timing** | `time.monotonic()` per step; total pipeline time on completion |
| **Profile propagation** | `--profile` forwarded to `generate_fake_data.py` |
| **DLQ summary capture** | Step 2 output scanned for `DLQ_SUMMARY:` JSON line |
| **dbt step splitting** | `dbt run` split into 3 sub-steps (`staging`, `intermediate`, `marts`) with individual failure handling |
| **Schema drift markers** | Step 2 output scanned for `SCHEMA_DRIFT_CRITICAL:` and `SCHEMA_DRIFT_WARNING:` JSON lines |
| **Alerting hooks** | DLQ warning, schema drift critical/warning, dbt test critical, success recap — all dispatched via `scripts/alerts.py` |

### Pre-Flight Health Checks

Before executing any step, `_run_preflight_checks()` validates:

| Check | What it validates |
|-------|-------------------|
| Critical directories | `data/raw/`, `dbt/`, `dbt/models/`, `dbt/target/`, `scripts/`, `src/` |
| dbt virtual environment | `.venv-dbt/Scripts/dbt.exe` (Win) or `.venv-dbt/bin/dbt` (Linux) |

### CLI Reference

```bash
python scripts/orchestrate.py [--profile {small,medium,large}]
make pipeline
make pipeline profile=small
```

---

## 8. Layer 5 — Consumption Layer (Business-Facing)

### 8.1 Streamlit Dashboard — `src/dashboard/app.py`

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

**Sections:**
1. **Sidebar** — Refresh button, auto-refresh toggle, category filter, Export to Excel
2. **KPI row** — Total Orders, Total Net Revenue, Active Customers, Avg Order Value, Returned/Pending, Return Rate %
3. **Monthly Sales Trend** — Plotly line chart
4. **Category Performance** — Grouped bar chart
5. **Revenue by Country** — Horizontal bar chart (top 15)
6. **Top 10 Customers** — Data table

### 8.2 Excel Analytics Exporter — `src/exports/excel_exporter.py`

| Sheet | SQL Query Source |
|-------|-----------------|
| Top Customers | `marts.dim_customers` — top 10 by `SUM(net_revenue_dollars)` |
| Monthly Sales | MoM revenue and order counts |
| Category Performance | Revenue share % per category |
| Cohort Analysis | Customer retention by first-purchase month |

**Styling:** Dark blue headers (`1F4E79`), auto-fitted columns, currency format (`$#,##0.00`), timestamped filename.

---

## 9. Quality Guardrails — CI/CD Pipeline

**File:** `.github/workflows/ci_cd.yml`

```
                    ┌──────────────────────────────────────────────────┐
                    │              GitHub Actions                      │
                    │                                                  │
                    │         Push / PR to main/master                 │
                    │                    │                             │
                    │              ┌─────┴─────┐                       │
                    │              │           │                       │
                    │              ▼           ▼                       │
                    │         ┌────────┐ ┌────────┐                    │
                    │         │ Core   │ │ dbt    │                    │
                    │         │ Python │ │ Val.   │                    │
                    │         └───┬────┘ └───┬────┘                    │
                    │             │          │                         │
                    │             ▼          ▼                         │
                    │       ┌─────────┐ ┌─────────┐                    │
                    │       │ flake8  │ │dbt debug│                    │
                    │       │ black   │ │dbt parse│                    │
                    │       │ pytest  │ │         │                    │
                    │       │ 77 tests│ │         │                    │
                    │       └─────────┘ └─────────┘                    │
                    │                                                  │
                    │   PostgreSQL 15 service container (shared)       │
                    └──────────────────────────────────────────────────┘
```

### Job 1 — Core Python (Lint & Test)

| Step | Tool | What it validates |
|------|------|-------------------|
| 1 | `actions/checkout@v4` | Pulls the repo |
| 2 | `actions/setup-python@v5` | Python 3.12, pip cache |
| 3 | `pip install -r requirements.txt` | Installs all main deps |
| 4 | `flake8` | Code style (ignores E501, W503) |
| 5 | `black --check` | Formatting consistency |
| 6 | `pytest` | 77+ unit tests |

### Job 2 — dbt Validation

| Step | Tool | What it validates |
|------|------|-------------------|
| 1 | `pip install dbt-core dbt-postgres` | Simulates `.venv-dbt` isolation |
| 2 | `CREATE SCHEMA raw` | Creates source tables |
| 3 | `dbt debug` | Connection test |
| 4 | `dbt parse` | SQL compilation — validates all models, refs, sources, macros |

---

## 10. Containerization & Deployment

| Service | Container | Base Image | Purpose |
|---------|-----------|------------|---------|
| `db` | `retailflow-db` | `postgres:15-alpine` | PostgreSQL warehouse with `pg_isready` healthcheck |
| `app` | `retailflow-app` | `python:3.12-slim` (via `Dockerfile`) | Streamlit dashboard + ETL pipeline |
| `pgadmin` | `retailflow-pgadmin` | `dpage/pgadmin4:latest` | Web-based PostgreSQL admin (optional) |

### Dockerfile Architecture

```
Layer 1: System deps (gcc, libpq-dev, curl)
Layer 2: Core Python deps (pandas, streamlit, airflow, GE, networkx, requests, ...)
Layer 3: Isolated dbt venv (/opt/dbt-venv) — dbt-core + dbt-postgres
Layer 4: Application code (scripts/, src/, dbt/, tests/)
```

### Deployment Runbook

```bash
# Build & launch
docker compose up --build

# Access dashboard: http://localhost:8501
# Access pgAdmin:  http://localhost:5050

# Run pipeline inside container
docker exec -it retailflow-app python scripts/orchestrate.py --profile small

# Tear down
docker compose down
docker compose down -v  # ⚠️ destroys data volumes
```

---

## 11. Virtual Environment Strategy

| Environment | Location | Contents | When to use |
|-------------|----------|----------|-------------|
| **Main** | `.venv/` | pandas, SQLAlchemy, streamlit, plotly, openpyxl, pytest, flake8, black, Faker, Airflow, GE, requests, networkx, matplotlib | Data generation, loading, dashboard, Excel export, testing, lineage, profiling |
| **dbt** | `.venv-dbt/` | `dbt-core==1.7.14`, `dbt-postgres==1.7.14` | All `dbt` commands (run, test, debug, parse, docs generate) |

**Why two envs?** `dbt-core` pins `mashumaro<4` while Airflow and Great Expectations require `mashumaro>=4`. A single environment cannot satisfy both.

Cross-platform resolution in the orchestrator:

| Platform | `_py_exe()` | `_dbt_exe()` |
|----------|-------------|--------------|
| Windows (local) | `.venv\Scripts\python.exe` | `.venv-dbt\Scripts\dbt.exe` |
| Linux (container) | `sys.executable` | `$DBT_EXECUTABLE` env var |

---

## 12. Layer 6 — Observability & Alerting

**Script:** `scripts/alerts.py`

### Architecture

```
Pipeline Event ──> orchestrator ──> send_pipeline_alert() / send_dbt_test_alert()
                                          │
                                    ┌─────┴──────┐
                                    │            │
                                    ▼            ▼
                               Discord       Slack
                             (embeds)    (Block Kit)
                                    │            │
                                    └────┬───────┘
                                         │
                                    requests.post()
                                    (timeout=15s,
                                     trust_env=True
                                     for proxy support)
                                         │
                                    ┌────┴────┐
                                    │         │
                                    ▼         ▼
                               HTTP 2xx   HTTP 4xx/5xx
                               (logged)   (logged + fallback
                                           plain-text attempt)
```

### Alert Triggers

| Status | Colour | When | Payload |
|--------|--------|------|---------|
| `success` | Green | All 8 steps complete | Total steps, duration, DLQ count |
| `warning` | Amber | DLQ rejected rows > 0 | Loaded / Rejected / Rejection Rate % |
| `warning` | Amber | Schema drift: extra columns | Entity name, extra column list |
| `critical` | Red | Any step fails (circuit breaker) | Stage name, exit code, error message |
| `critical` | Red | Schema drift: missing/type mismatch | Entity name, missing columns, type mismatches |
| `critical` | Red | dbt test failure | Per-test unique_id, status, execution_time, DB message |

### dbt Test Metadata Alerting

When dbt tests fail, the orchestrator:

1. Calls `parse_dbt_test_results()` which reads `dbt/target/run_results.json`
2. Extracts every result with `status == "fail"` or `status == "error"`
3. Lists up to 5 individual failures inline with short test name, status, and database message
4. Shows remainder count if more than 5 tests fail
5. Dispatches with title: `�� DATA QUALITY SLA BREACH: dbt Test Failed!`

### Key Design Decisions

| Concern | Implementation |
|---------|---------------|
| **Transport** | `requests.post()` with 15-second timeout |
| **Auto-detection** | URL domain pattern determines Discord vs Slack format |
| **Graceful skip** | Logs "Webhook not configured" if `PIPELINE_WEBHOOK_URL` unset |
| **Error resilience** | All HTTP exceptions caught — webhook failure never crashes pipeline |
| **Response logging** | First 500 chars of response body logged on 4xx/5xx |
| **Proxy support** | `session.trust_env = True` for corporate proxy environments |
| **Plain-text fallback** | If Discord embed is rejected (4xx), retries with plain `content` message |

---

## 13. Layer 7 — Data Governance & Lineage

The pipeline produces two complementary artifacts:

| Artifact | Tool | Location | Purpose |
|----------|------|----------|---------|
| **Interactive catalog** | `dbt docs generate` | `dbt/target/` (via `make docs` at localhost:8080) | Browsable model catalogue, column metadata, interactive DAG, test dashboard |
| **Static lineage blueprint** | `generate_lineage.py` | `docs/lineage/current_data_lineage.png` | Version-control-friendly, colour-coded PNG showing model dependency graph |

### Dynamic Lineage Blueprint (Step 7)

**Script:** `scripts/generate_lineage.py`

#### How It Works

1. **Parses** `dbt/target/manifest.json` — the JSON artifact from `dbt docs generate`
2. **Filters** to `resource_type == "model"` nodes only
3. **Detects layer** from model name prefix:
   - `stg_*` → **Staging** (green `#2E8B57`)
   - `int_*` → **Intermediate** (blue `#4169E1`)
   - `dim_*` / `fct_*` → **Marts** (gold `#DAA520`)
4. **Builds a `networkx.DiGraph`** — edges for every `ref()` dependency
5. **Computes a layered layout** — staging left, intermediate centre, marts right
6. **Renders** with `matplotlib` at 200 DPI

#### Example Graph Structure

```
  stg_customers ──┐
                  ├──> int_orders_enriched ──> fct_orders
  stg_orders ─────┤
                  │
  stg_products ───┤
                  ├──> dim_customers
                  │
                  └──> dim_products
```

#### Lifecycle

```
Pipeline ──> dbt docs generate ──> manifest.json ──> generate_lineage.py ──> current_data_lineage.png
(steps 1-5)  (Step 6, .venv-dbt)                    (Step 7, .venv)          (docs/lineage/)
```

---

## 14. Layer 8 — Data Profiling

**Script:** `scripts/generate_profiling.py` (Step 8)

Generates an interactive HTML data profile report for all three mart tables:

| Table | Rows | Columns Profiled |
|-------|------|------------------|
| `marts.dim_customers` | 1,000 | 11 |
| `marts.dim_products` | 100 | 9 |
| `marts.fct_orders` | 10,000 | 10 |

**Per-column statistics:**
- Missing count and percentage (with progress bar)
- Cardinality flags (high/medium/low)
- Numeric columns: min, max, mean, median, std, skew, P25, P75
- Categorical columns: top value, top frequency

**Output:** `docs/profiling/retailflow_data_profile.html` — self-contained, interactive HTML with CSS tab navigation, summary cards, and expandable stats sections.

---

## 15. Step-by-Step Execution Sequence

### Prerequisites

- [ ] Python 3.12+ installed
- [ ] Docker Desktop installed and running
- [ ] Git installed

### Setup

```bash
git clone https://github.com/elhussienysabry/retailflow-pipeline.git
cd retailflow-pipeline
cp .env.example .env

make setup            # .venv + pip install
make setup-dbt        # .venv-dbt + dbt-core install
make run              # docker compose up -d
```

### Full Pipeline (Single Command)

```bash
make pipeline
# OR
.venv\Scripts\python scripts/orchestrate.py --profile small
```

The orchestrator runs all 8 steps:

| Step | Component | Environment | What Happens |
|------|-----------|-------------|-------------|
| 1 | Generate Data | `.venv` | Faker creates CSVs + JSON in `data/raw/` |
| 2 | Load to PostgreSQL | `.venv` | Schema drift check → validation → PII hash → load → unified upsert |
| 3 | dbt Run | `.venv-dbt` | staging (views) → intermediate (view) → marts (tables, full-refresh) |
| 4 | dbt Test | `.venv-dbt` | 48 data quality tests executed |
| 5 | Excel Export | `.venv` | 4 analytics sheets → styled `.xlsx` in `outputs/` |
| 6 | dbt Docs Generate | `.venv-dbt` | `dbt compile` + `dbt docs generate` → `manifest.json` |
| 7 | Lineage Graph Export | `.venv` | NetworkX parses manifest → PNG at `docs/lineage/` |
| 8 | Data Profile Report | `.venv` | Pandas profiles mart tables → HTML at `docs/profiling/` |

### Validation

```bash
# Health check
make status

# Python tests
make test

# dbt tests (separate)
cd dbt && ..\.venv-dbt\Scripts\dbt test

# View artifacts
make docs             # dbt docs portal at localhost:8080
make dashboard        # Streamlit at localhost:8501
```

### CI/CD (GitHub)

Once pushed to `main`/`master`, the workflow at `.github/workflows/ci_cd.yml` automatically:
1. Spins up PostgreSQL service container
2. Installs all Python deps
3. Runs `flake8` + `black --check`
4. Executes 77+ pytest tests
5. Installs dbt in isolation
6. Runs `dbt debug` + `dbt parse`

---

*Architecture document v1.1.0 — Generated for the RetailFlow Pipeline project.*

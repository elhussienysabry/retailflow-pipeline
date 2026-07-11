# RetailFlow Pipeline — Architecture & Data Lineage

> **Audience:** Data engineers, solutions architects, engineering managers.
> **Version:** v1.2.0
> **Purpose:** Document the end-to-end data lifecycle, system boundaries, operational runbook, and production engineering patterns (schema drift detection, idempotent warehousing, dual-venv isolation, CI/CD automation, circuit-breaker alerting).

---

## Table of Contents

1. [Project Directory Blueprint](#1-project-directory-blueprint)
2. [ASCII Data Flow Diagram](#2-ascii-data-flow-diagram)
3. [Layer 0 — Schema Drift Detection](#3-layer-0--schema-drift-detection)
4. [Layer 1 — Data Generation & Ingestion](#4-layer-1--data-generation--ingestion)
5. [Layer 2 — PostgreSQL Warehouse & Idempotency](#5-layer-2--postgresql-warehouse--idempotency)
6. [Layer 3 — dbt Transformation (Processing)](#6-layer-3--dbt-transformation-processing)
7. [Layer 4 — Orchestration Layer](#7-layer-4--orchestration-layer)
8. [Layer 5 — Consumption Layer (Business-Facing)](#8-layer-5--consumption-layer-business-facing)
9. [Layer 6 — Observability & Alerting](#9-layer-6--observability--alerting)
10. [Layer 7 — Data Governance & Lineage](#10-layer-7--data-governance--lineage)
11. [Layer 8 — Data Profiling](#11-layer-8--data-profiling)
12. [Virtual Environment Strategy](#12-virtual-environment-strategy)
13. [Quality Guardrails — CI/CD Pipeline](#13-quality-guardrails--cicd-pipeline)
14. [Containerisation & Deployment](#14-containerisation--deployment)
15. [Step-by-Step Execution Sequence](#15-step-by-step-execution-sequence)

---

## 1. Project Directory Blueprint

```
retailflow-pipeline/
│
├── .venv/                        # Main Python env (pandas, streamlit, airflow, etc.)
├── .venv-dbt/                    # Isolated dbt env (dbt-core 1.7, dbt-postgres 1.7)
│
├── scripts/                      # Core Python ETL scripts (7 modules)
│   ├── __init__.py
│   ├── orchestrate.py            # 8-step pipeline orchestrator + circuit breaker + alert dispatch
│   ├── generate_fake_data.py     # Faker synthetic data (CSV + JSON) with scale profiles
│   ├── load_to_postgres.py       # Hybrid ingestion, schema drift detector, PII anonymisation, unified upsert
│   ├── generate_lineage.py       # NetworkX lineage graph renderer (200 DPI PNG)
│   ├── generate_profiling.py     # Pandas data profiling → interactive HTML report
│   ├── alerts.py                 # Discord/Slack webhook dispatcher with embed fallback
│   └── project_status.py         # E2E health check (Docker → PG → env → CSVs → drift quarantine)
│
├── src/                          # Python application package
│   ├── __init__.py
│   ├── dashboard/app.py          # Streamlit KPI dashboard (5-min cache, Plotly charts)
│   └── exports/excel_exporter.py # Styled Excel workbook (4 analytics sheets)
│
├── dbt/                          # dbt transformation project
│   ├── models/
│   │   ├── staging/              # Clean, cast, deduplicate (3 views)
│   │   ├── intermediate/         # Join + enrich (1 view)
│   │   └── marts/                # Business-ready star schema (2 dims + 1 fact table)
│   ├── tests/                    # Custom singular tests (2)
│   ├── macros/                   # Jinja SQL macros
│   ├── profiles.yml              # DB connection (env-var driven)
│   └── dbt_project.yml           # dbt config
│
├── sql/                          # Reference SQL
│   ├── schema/                   # DDL (CREATE SCHEMA / TABLE)
│   └── analytics/                # Business analysis queries
│
├── tests/                        # 77+ pytest tests (6 test modules)
├── data/
│   ├── raw/                      # Generated CSVs + JSON (gitignored)
│   ├── rejected/                 # Dead Letter Queue — rejected rows with rejection_reason (gitignored)
│   └── rejected_schemas/         # Schema drift quarantine (gitignored)
├── outputs/                      # Excel exports (gitignored)
├── docs/
│   ├── lineage/                  # current_data_lineage.png (auto-generated)
│   └── profiling/                # retailflow_data_profile.html (auto-generated)
│
├── .github/workflows/ci_cd.yml   # CI/CD: flake8 → black → pytest → dbt debug → dbt parse
├── docker-compose.yml            # PostgreSQL 15 + app + pgAdmin (3 services)
├── Dockerfile                    # Multi-stage, dual-venv image
├── Makefile                      # Dev workflow commands (18 targets)
├── ARCHITECTURE.md               # This file
└── README.md                     # Project overview
```

---

## 2. ASCII Data Flow Diagram

```
  RETAILFLOW PIPELINE — END-TO-END DATA FLOW v1.2.0
  =====================================================
  LEGEND: [D] = Drift Detector  [I] = Idempotent Load  [Q] = Quarantine


  .__________________________________.
  |  GITHUB ACTIONS CI/CD            |      Push/PR triggers:
  |  ┌─ core-python ─────────────────|───   flake8 + black + pytest (77+ tests)
  |  │  flake8 lint → black check    |       + dbt debug + dbt parse
  |  │  → pytest (77+ tests)         |       Shared PostgreSQL 15 container
  |  │                                |
  |  └─ dbt-validation ──────────────|
  |     dbt debug → dbt parse (SQL)  |
  |__________________________________|


  .__________________________________.
  |  FAKER DATA GENERATOR            |  Scale profiles: small / medium / large
  |  (generate_fake_data)            |  4 output files → data/raw/
  |__________________________________|
         |                   \
         |                    \  JSON files
         | CSV files           \   pos_store_sales.json
         | (data/raw/)          \
         | customers.csv         \
         | products.csv           \
         | orders.csv              \
         |                          |
         v                          v
  ._____________________________________.  .________________________________.
  |  LAYER 0 — SCHEMA DRIFT DETECTOR [D] |  |  _detect_schema_drift()       |
  |  For each source file:                |  |  Reads 1st row (CSV header /  |
  |                                       |  |  JSON first record), compares |
  |  ┌─ Compare columns + dtypes          |  |  against SCHEMA_BLUEPRINT.    |
  |  │  against SCHEMA_BLUEPRINT          |  |                               |
  |  │                                    |  |  CRITICAL: missing column or  |
  |  │  PASS → continue to ingest         |  |  type mismatch. File moved to |
  |  │  WARN → extra columns, continue    |  |  data/rejected_schemas/.      |
  |  │  CRITICAL → quarantine + exit(2)   |  |  Pipeline halts. Red alert.   |
  |  │____________________________________|  | WARN: extra cols → amber alert|
  |         |                          |     |_______________________________|
  |         |                          |
  |    ┌────┴────┐              ┌──────┴──────────┐
  |    | PASS    |              | CRITICAL         |
  |    v         v              v                  v
  |  .___________________.  .____________________________.
  |  |  IDEMPOTENT LOAD  |  |  SCHEMA QUARANTINE [Q]     |
  |  |  _load_csv_to_    |  |  data/rejected_schemas/     |
  |  |  table() [I]      |  |  File isolated. Never       |
  |  |                   |  |  reaches PostgreSQL.        |
  |  |  ┌─ TRUNCATE each |  |  Must be reconciled before  |
  |  |  │  target table  |  |  re-run.                    |
  |  |  │ Pandas chunks  |  |_____________________________|
  |  |  │ Validation     |
  |  |  │ guardrails per |
  |  |  │ entity         |
  |  |  │ PII SHA-256    |
  |  |  │ hash (customers)|
  |  |  │ DLQ bad rows   |
  |  |  │________________|
  |         |            |              |
  |    ┌────┘            |              |
  |    |                 |              |
  |    | clean rows      | bad rows     |
  |    |                 v              |
  |    |           .________________.   |
  |    |           |  DLQ [Q]       |   |
  |    |           | data/rejected/ |   |
  |    |           | (timestamped   |   |
  |    |           |  CSVs with     |   |
  |    |           |  rejection_reason)| |
  |    |           |________________|   |
  |    |                               |
  |    |              _________________/
  |    |             /
  |    v             v
  |  .____________________________________.
  |  |  PostgreSQL WAREHOUSE (Layer 2)    |
  |  |  raw schema (5 tables):            |
  |  |  ┌──────────────────────────────┐  |
  |  |  │ IDEMPOTENT [I]               │  |
  |  |  │ TRUNCATE + INSERT (CSV)      │  |
  |  |  │ to_sql(if_exists="replace")  │  |
  |  |  │  (JSON)                      │  |
  |  |  └──────────────────────────────┘  |
  |  |  raw.customers  raw.orders         |
  |  |  raw.products   raw.pos_store_sales|
  |  |____________________________________|
  |         |
  |         | Schema Harmonisation
  |         | _harmonize_and_upsert_unified()
  |         | Maps: transaction_timestamp → transaction_date
  |         |       sale_id → transaction_id
  |         | Adds: source_system ('online' / 'pos')
  |         v
  |  .____________________________________.
  |  |  raw.unified_transactions          |
  |  |  ┌──────────────────────────────┐  |
  |  |  │ UPSERT MERGE [I]             │  |
  |  |  │ INSERT ... ON CONFLICT       │  |
  |  |  │ (transaction_id, source_     │  |
  |  |  │  system) DO UPDATE SET ...   │  |
  |  |  │ → zero duplicates on re-run  │  |
  |  |  └──────────────────────────────┘  |
  |  |  PK: (transaction_id, source_system)|
  |  |____________________________________|
  |         |
  |         v
  |  .____________________________________.
  |  |  dbt TRANSFORMATION (Layer 3)      |
  |  |  staging ──► intermediate ──► marts|
  |  |  (views)      (view)       (tables)|
  |  |  stg_orders   int_orders_  dim_    |
  |  |  stg_cust..   enriched     customers|
  |  |  stg_prod..                dim_    |
  |  |                            products|
  |  |                            fct_    |
  |  |                            orders  |
  |  |  ┌──────────────────────────────┐  |
  |  |  │ 48 automated dbt tests       │  |
  |  |  │ not_null  unique  accepted_  │  |
  |  |  │ values  relationships  custom│  |
  |  |  │ Circuit breaker on FAILURE   │  |
  |  |  └──────────────────────────────┘  |
  |  |____________________________________|
  |         |                    |
  |    ┌────┴────────────────────┴────┐
  |    |            |                 |
  |    v            v                 v
  |  ._________.  ._________.  .________________.
  |  | STREAMLIT|  | EXCEL   |  | ALERT ENGINE   |
  |  | :8501    |  | export  |  | (Layer 6)      |
  |  | Plotly   |  | .xlsx   |  | Discord/Slack  |
  |  | KPI dash |  | outputs/|  | webhooks       |
  |  |__________|  |_________|  |________________|
  |                                    |
  |         ┌──────────────────────────┘
  |         v
  |  ._________________________.   .___________________________.
  |  | dbt DOCS CATALOG        |   | LINEAGE BLUEPRINT         |
  |  | (Layer 7)                |   | (Layer 7)                 |
  |  | dbt docs generate        |   | generate_lineage.py       |
  |  | Browsable catalog +      |   | NetworkX → matplotlib     |
  |  | interactive DAG          |   | 200 DPI PNG               |
  |  | make docs → :8080        |   | docs/lineage/             |
  |  |__________________________|   |___________________________|
  |                                             |
  |                                    .___________________________.
  |                                    | DATA PROFILE REPORT       |
  |                                    | (Layer 8)                 |
  |                                    | generate_profiling.py     |
  |                                    | pandas → interactive HTML |
  |                                    | docs/profiling/           |
  |                                    |___________________________|
```

---

## 3. Layer 0 — Schema Drift Detection

**Module:** `scripts/load_to_postgres.py` (integrated into `main()`)

The schema drift detector runs **before any data is loaded** into PostgreSQL. Each source file is inspected (header row for CSV, first record for JSON) and compared against `SCHEMA_BLUEPRINT`, a dictionary defining expected column names and pandas dtypes for every entity.

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

### Drift Detection Algorithm

```
Read first record of source file
  │
  ├── Extract column names → set(actual_cols)
  ├── Extract pandas dtypes → dict(actual_dtypes)
  │
  ├── Compute missing = required_cols - actual_cols
  │   └── If non-empty → CRITICAL (missing columns)
  │
  ├── For each required column present:
  │   ├── Normalise actual dtype via _normalize_dtype()
  │   ├── Normalise expected dtype from blueprint
  │   └── If mismatch → CRITICAL (type mismatch)
  │
  ├── Compute extra = actual_cols - required_cols
  │   └── If non-empty (and no critical) → WARNING (extra columns)
  │
  └── Otherwise → PASS
```

### Dtype Normalisation

pandas dtype strings vary across versions and platforms. The detector normalises before comparison:

| Raw dtype | Normalised |
|-----------|------------|
| `"string"`, `"str"`, `"object"` | `"string"` |
| `"int64"`, `"Int64"`, `"int32"` | `"int64"` |
| `"float64"`, `"Float64"`, `"float32"` | `"float64"` |
| `"bool"`, `"boolean"` | `"bool"` |

### Severity Levels & Actions

| Severity | Condition | Action | Exit Code | Alert |
|----------|-----------|--------|-----------|-------|
| **CRITICAL** | Required column missing, or dtype mismatch after normalisation | File moved to `data/rejected_schemas/`. Pipeline halts immediately. | `2` (distinct from general failure code `1`) | Red embed: entity name, missing columns list, type mismatch details (expected vs actual) |
| **WARNING** | Extra columns present not in blueprint | Pipeline continues. Extra columns logged and tracked. | `0` | Amber embed: entity name, extra column names |
| **NONE** | Schema matches blueprint exactly | Normal processing. | `0` | — |

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
            │exit code 2│ │exit code 2│ │          │  │          │
            └──────────┘ └──────────┘ └──────────┘  └──────────┘
```

### Orchestrator Integration

When `load_to_postgres.py` detects critical drift, it:
1. Moves the file to `data/rejected_schemas/`
2. Prints a `SCHEMA_DRIFT_CRITICAL:` JSON marker to stdout
3. Exits with code `2`

The orchestrator parses these markers from the captured subprocess output:

```
SCHEMA_DRIFT_CRITICAL:{"entity": "customers", "severity": "critical",
  "filepath": "...", "missing_columns": ["age"], "type_mismatches": {}}
```

Warning drift outputs a similar `SCHEMA_DRIFT_WARNING:` marker but exits with code `0` (pipeline continues). The orchestrator logs both and dispatches corresponding alerts after the step completes.

### Health Check Integration

`scripts/project_status.py` checks `data/rejected_schemas/` for quarantined files. If any exist, the status report shows a `FAIL` status with the file names and a fix hint pointing to `SCHEMA_BLUEPRINT`.

---

## 4. Layer 1 — Data Generation & Ingestion

### 4.1 Synthetic Data Engine — `scripts/generate_fake_data.py`

**Purpose:** Seed realistic transactional data for development and testing using Faker with deterministic seed (`Faker.seed(42)`).

**Scale Profiles (CLI-driven):**

| Profile | `--profile` | Customers | Products | Orders | POS Sales | Runtime (approx) |
|---------|-------------|-----------|----------|--------|-----------|------------------|
| Small | `small` | 1,000 | 100 | 10,000 | 3,000 | ~2 seconds |
| Medium | `medium` | 10,000 | 500 | 100,000 | 30,000 | ~15 seconds |
| Large | `large` | 100,000 | 5,000 | 1,000,000 | 300,000 | ~3 minutes |

**Resolution order:** `--profile` sets defaults → explicit `--customers` / `--products` / `--orders` / `--pos-sales` flags override individual dimensions.

**Output:** 4 files written to `data/raw/`:
- `customers.csv` — UUID, first_name, last_name, email (guaranteed unique), country, city, signup_date, age, gender
- `products.csv` — UUID, name, category (weighted: Electronics 25%, Clothing 35%, Food 15%, Home 25%), price_cents, stock_quantity, supplier_country
- `orders.csv` — UUID, FK→customers, FK→products, quantity, order_date, status (completed 80%, returned 10%, pending 10%), discount_pct, shipping_days
- `pos_store_sales.json` — sale_id, store_id (10 locations), product_id, quantity, unit_price_cents, total_amount, transaction_timestamp, payment_method (credit/debit/cash/mobile_wallet)

**Referential integrity:** ID lists are extracted from generated CSVs via `_read_ids()` and passed into order generation to guarantee valid FK references.

### 4.2 Hybrid Ingestion Engine — `scripts/load_to_postgres.py`

**Purpose:** Ingest CSV (e-commerce) and JSON (POS store) source files into the PostgreSQL `raw` schema, validate against quality guardrails, anonymise PII, then run schema harmonisation to produce a unified transactions table with upsert semantics.

**Execution flow per file:**

```
for each (filename, table_name, file_type) in SOURCE_MAP:
    1. _detect_schema_drift(filepath, entity, file_type)
       → CRITICAL: _move_to_rejected_schemas() + sys.exit(2)
       → WARNING:  print marker, continue
       → NONE:     continue

    2. truncate_table(engine, table_name)         [CSV only]
    3. load_csv_to_table() or _load_json_to_table()

    4. _harmonize_and_upsert_unified(engine)      [after all 4 sources]
```

#### Data Quality Guardrails (per entity)

| Entity | Validations | Rejection Reason |
|--------|------------|------------------|
| **Customers** | `customer_id` not null, email contains `@` | `missing customer_id`, `missing email`, `malformed email (missing @)` |
| **Products** | `product_id` not null, `price_cents` not null, `price_cents >= 0` | `missing product_id`, `missing price_cents`, `negative price_cents` |
| **Orders** | `order_id`/`customer_id`/`product_id` not null, `quantity >= 0`, `discount_pct` in `[0, 100]` | `missing order_id`, `missing customer_id`, `missing product_id`, `negative quantity`, `discount_pct out of range` |

#### Dead Letter Queue (DLQ)

Bad rows are isolated to `data/rejected/` with a `rejection_reason` column. DLQ files are timestamped (`rejected_orders_20260707_033908.csv`). The pipeline continues normally — only clean rows reach PostgreSQL. The orchestrator captures the loaded vs rejected count for alerting.

#### PII Anonymisation (GDPR / CCPA Compliance)

`first_name`, `last_name`, and `email` are SHA-256 hashed before storage:
- Deterministic: whitespace-stripped, lowercased, then hex-digested
- Null values preserved as-is
- Raw CSV files on disk remain unmodified

#### Schema Harmonisation & Unified Upsert

After all raw source tables are loaded, `_harmonize_and_upsert_unified()` creates (if missing) and populates `raw.unified_transactions`:

| Unified Column | Online Source (raw.orders) | POS Source (raw.pos_store_sales) |
|---|---|---|
| `transaction_id` | `order_id` | `sale_id` |
| `source_system` | `'online'` | `'pos'` |
| `transaction_date` | `order_date` | `transaction_timestamp::date` |
| `total_amount` | `NULL` (computed in dbt) | `total_amount` |
| `store_id` | `NULL` | `store_id` |
| `customer_id` | `customer_id` | `NULL` |
| `status` | `status` | `'completed'` |
| `payment_method` | `NULL` | `payment_method` |

**Idempotent upsert** uses `INSERT ... ON CONFLICT (transaction_id, source_system) DO UPDATE` — guaranteed zero duplicates on re-run.

---

## 5. Layer 2 — PostgreSQL Warehouse & Idempotency

**Infrastructure:** PostgreSQL 15 running in Docker via `docker-compose.yml`.

| Property | Value |
|----------|-------|
| Host | `localhost:5432` (host) / `db:5432` (Docker network) |
| Database | `retailflow` |
| User | `retailflow_user` |
| Password | `retailflow_pass` (from `.env`) |

### Idempotent Loading Strategy

| Source Type | Strategy | Code Path | Schema |
|-------------|----------|-----------|--------|
| **CSV** (customers, products, orders) | **Clean Slate** — `TRUNCATE TABLE ... RESTART IDENTITY CASCADE` → `pandas.to_sql(if_exists="append")` | `truncate_table()` + `load_csv_to_table()` via `chunksize=10_000` + `method="multi"` | `raw.*` |
| **JSON** (pos_store_sales) | **Replace** — `pandas.to_sql(if_exists="replace")` | `_load_json_to_table()` | `raw.pos_store_sales` |
| **Unified** (online + POS) | **Upsert MERGE** — `INSERT ... ON CONFLICT DO UPDATE` | `_harmonize_and_upsert_unified()` | `raw.unified_transactions` |

This design guarantees that re-running the pipeline N times on the same data batch produces exactly the same warehouse state with zero duplicate rows across all tables.

### Schema Layout (post-pipeline)

```
raw          (schema)  — 5 tables, managed by load_to_postgres.py
├── customers              ← TRUNCATE + INSERT (idempotent)
├── products               ← TRUNCATE + INSERT (idempotent)
├── orders                 ← TRUNCATE + INSERT (idempotent)
├── pos_store_sales        ← to_sql(if_exists="replace") (idempotent)
└── unified_transactions   ← UPSERT MERGE (idempotent)

staging      (schema)  — 3 views, created by dbt run --select staging
├── stg_customers
├── stg_orders
└── stg_products

intermediate (schema)  — 1 view, created by dbt run --select intermediate
└── int_orders_enriched

marts        (schema)  — 3 tables, created by dbt run --select marts
├── dim_customers          ← full refresh each run
├── dim_products           ← full refresh each run
└── fct_orders             ← incremental (unique_key='order_id')
```

### Unified Transactions DDL

```sql
CREATE TABLE IF NOT EXISTS raw.unified_transactions (
    transaction_id   TEXT NOT NULL,
    source_system    TEXT NOT NULL,
    product_id       TEXT,
    quantity         INTEGER,
    transaction_date DATE,
    total_amount     NUMERIC(12,2),
    store_id         TEXT,
    customer_id      TEXT,
    status           TEXT,
    payment_method   TEXT,
    discount_pct     INTEGER,
    shipping_days    INTEGER,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_id, source_system)
);
```

---

## 6. Layer 3 — dbt Transformation (Processing)

**Environment:** Isolated `.venv-dbt` — only `dbt-core==1.7.14` + `dbt-postgres==1.7.14`.

### Model Architecture

```
raw.customers ──> stg_customers ──┐
                                  ├──> int_orders_enriched ──> fct_orders
raw.orders ────> stg_orders ──────┤           │
                                  │           ├──> dim_customers
raw.products ──> stg_products ────┘           │
                                              └──> dim_products
```

### Staging Models (`models/staging/`)

| Model | Source | Transformations | Materialisation |
|-------|--------|----------------|-----------------|
| `stg_customers` | `raw.customers` | Trim whitespace, lowercase email, cast signup_date to DATE, deduplicate by customer_id | View |
| `stg_products` | `raw.products` | Trim name/category, filter `price_cents <= 0` (keep > 0), deduplicate | View |
| `stg_orders` | `raw.orders` | Cast order_date to DATE, lowercase/trim status, filter null FKs, deduplicate | View |

### Intermediate Model (`models/intermediate/`)

| Model | Source | Transformations | Materialisation |
|-------|--------|----------------|-----------------|
| `int_orders_enriched` | All 3 staging models | LEFT JOIN orders + customers + products, `gross_revenue_cents = quantity * price_cents`, `net_revenue_cents = gross * (1 - discount_pct/100)` | View |

### Mart Models (`models/marts/`)

| Model | Type | Grain | Key Measures | Materialisation |
|-------|------|-------|-------------|-----------------|
| `dim_customers` | Dimension | 1 row per customer | `total_orders`, `lifetime_value_cents` | Table (full refresh) |
| `dim_products` | Dimension | 1 row per product | `total_orders`, `total_units_sold`, `total_revenue_cents` | Table (full refresh) |
| `fct_orders` | Fact | 1 row per order line | `gross_revenue_dollars`, `net_revenue_dollars` | **Incremental** (`unique_key='order_id'`) |

### fct_orders Incremental Strategy

```sql
{{ config(materialized='incremental', unique_key='order_id') }}

SELECT ... FROM {{ ref('int_orders_enriched') }}

{% if is_incremental() %}
  WHERE order_date >= (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

- First run (empty target table): loads full history
- Subsequent runs: `WHERE order_date >= (SELECT MAX(order_date) FROM {{ this }})` — processes only new/changed data
- The orchestrator passes `--full-refresh` on `marts` so FK references in dimension tables stay in sync

### dbt Tests — 48 Automated Quality Checks

| Test Type | Count | What It Validates |
|-----------|-------|-------------------|
| `not_null` | 15 | Critical columns never null (PKs, FKs, business keys) |
| `unique` | 5 | Primary keys (customer_id, product_id, order_id) and email |
| `accepted_values` | 6 | Category values, order status values, payment methods |
| `relationships` | 4 | FK references exist in parent tables |
| Custom singular | 2 | `assert_positive_revenue` (no negative net revenue), `assert_no_null_customer_id` in fct_orders |
| Standard (generic) | 16 | Additional column-level constraints |

---

## 7. Layer 4 — Orchestration Layer

**Script:** `scripts/orchestrate.py`

The orchestrator manages the end-to-end pipeline lifecycle as a sequential DAG with 8 steps, circuit breaker pattern, and cross-environment executable resolution.

### Step Sequence

```
[1] Generate Data     .venv        Faker → 4 source files in data/raw/
[2] Load PostgreSQL   .venv        Drift check → validate → PII hash → load → unified upsert
[3] dbt Run           .venv-dbt    staging (views) → intermediate (view) → marts (tables, full-refresh)
[4] dbt Test          .venv-dbt    48 data quality tests; circuit breaker on failure
[5] Excel Export      .venv        4 analytics sheets → styled .xlsx in outputs/
[6] dbt Docs Gen      .venv-dbt    dbt compile + dbt docs generate → manifest.json + catalog.json
[7] Lineage Graph     .venv        NetworkX parses manifest → 200 DPI PNG at docs/lineage/
[8] Profile Report    .venv        Pandas profiles mart tables → interactive HTML at docs/profiling/
```

### Key Design Decisions

| Concern | Implementation |
|---------|---------------|
| **Environment switching** | `_py_exe()` resolves `.venv/Scripts/python.exe` (Win) or `sys.executable` (Linux); `_dbt_exe()` resolves `.venv-dbt/Scripts/dbt.exe` (Win) or `$DBT_EXECUTABLE` env var (Linux/container) |
| **Circuit breaker** | Non-zero `returncode` → `sys.exit(1)` before next step; exit code 2 = distinct schema drift halt message |
| **Streaming output** | `subprocess.PIPE` with `locale.getpreferredencoding()` + `errors="replace"` (fixes `UnicodeDecodeError` on Windows with cp1252-encoded output) |
| **Step timing** | `time.monotonic()` per step and total pipeline time |
| **Profile propagation** | `--profile` forwarded to `generate_fake_data.py` |
| **dbt step splitting** | `dbt run` split into 3 sub-steps (`staging`, `intermediate`, `marts`) with individual failure handling |
| **Schema drift markers** | Step 2 output scanned for `SCHEMA_DRIFT_CRITICAL:` and `SCHEMA_DRIFT_WARNING:` JSON lines |
| **Alerting hooks** | DLQ warning, schema drift critical/warning, dbt test critical, success recap — all via `scripts/alerts.py` |
| **.env auto-loading** | Each subprocess script calls `load_dotenv()` independently; the orchestrator relies on child processes to load their own env |

### Pre-Flight Health Checks

Before executing any step, `_run_preflight_checks()` validates:

| Check | What It Validates |
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
                    │ @cache    │
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

| Sheet | SQL Source Query |
|-------|-----------------|
| Top Customers | `marts.dim_customers` + `marts.fct_orders` — top 10 by `SUM(net_revenue_dollars)` |
| Monthly Sales | `marts.fct_orders` — MoM revenue, order counts, growth % |
| Category Performance | `marts.dim_products` + `marts.fct_orders` — revenue share %, avg discount |
| Cohort Analysis | `marts.fct_orders` — customer retention by first-purchase month cohort |

**Styling:** Dark blue headers (`1F4E79`), auto-fitted columns, currency format (`$#,##0.00`), timestamped filename.

---

## 9. Layer 6 — Observability & Alerting

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
| `warning` | Amber | Schema drift: extra columns detected | Entity name, extra column list |
| `critical` | Red | Any step fails (circuit breaker) | Stage name, exit code, error message |
| `critical` | Red | Schema drift: missing columns / type mismatch | Entity name, missing columns, type mismatches |
| `critical` | Red | dbt test failure | Per-test unique_id, status, execution_time, database message (from `run_results.json`) |

### dbt Test Metadata Alerting

When dbt tests fail, the orchestrator:
1. Calls `parse_dbt_test_results()` which reads `dbt/target/run_results.json`
2. Extracts every result with `status == "fail"` or `status == "error"`
3. Lists up to 5 individual failures inline with short test name, status, and database message
4. Shows remainder count if more than 5 tests fail
5. Dispatches with title: `DATA QUALITY SLA BREACH: dbt Test Failed!`

### Key Design Decisions

| Concern | Implementation |
|---------|---------------|
| **Transport** | `requests.Session.post()` with 15-second timeout |
| **Auto-detection** | URL domain pattern (`discord.com` / `discordapp.com`) determines Discord vs Slack format |
| **Graceful skip** | Logs "Webhook not configured" if `PIPELINE_WEBHOOK_URL` unset; never crashes pipeline |
| **Error resilience** | All HTTP exceptions caught — webhook failure never crashes pipeline |
| **Response logging** | First 500 chars of response body logged on 4xx/5xx |
| **Proxy support** | `session.trust_env = True` for corporate proxy environments |
| **Plain-text fallback** | If Discord embed is rejected (4xx), retries with plain `content` message |

---

## 10. Layer 7 — Data Governance & Lineage

The pipeline produces two complementary artifacts:

| Artifact | Tool | Location | Purpose |
|----------|------|----------|---------|
| **Interactive catalog** | `dbt docs generate` | `dbt/target/` (via `make docs` at localhost:8080) | Browsable model catalog, column metadata, interactive DAG, test dashboard |
| **Static lineage blueprint** | `generate_lineage.py` | `docs/lineage/current_data_lineage.png` | Version-control-friendly, colour-coded PNG showing model dependency graph |

### Dynamic Lineage Blueprint (Step 7)

**Script:** `scripts/generate_lineage.py`

#### How It Works

1. **Parses** `dbt/target/manifest.json` — JSON artifact from `dbt docs generate`
2. **Filters** to `resource_type == "model"` nodes only
3. **Detects layer** from model name prefix:
   - `stg_*` → **Staging** (green `#2E8B57`)
   - `int_*` → **Intermediate** (blue `#4169E1`)
   - `dim_*` / `fct_*` → **Marts** (gold `#DAA520`)
4. **Builds a `networkx.DiGraph`** — edges for every `ref()` dependency, skipping source references
5. **Computes a layered layout** — staging left, intermediate centre, marts right
6. **Renders** with `matplotlib` at 200 DPI with white-on-dark labels, arrow edges, and layer annotation boxes

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

## 11. Layer 8 — Data Profiling

**Script:** `scripts/generate_profiling.py` (Step 8)

Generates an interactive HTML data profile report for all three mart tables:

| Table | Rows (small) | Columns Profiled |
|-------|-------------|------------------|
| `marts.dim_customers` | 1,000 | 11 |
| `marts.dim_products` | 100 | 9 |
| `marts.fct_orders` | 10,000 | 10 |

### Per-Column Statistics

- Missing count and percentage (with colour-coded progress bar: green < 5%, amber < 20%, red ≥ 20%)
- Cardinality flags: high-cardinality (> 90% unique), constant (std = 0), all-null
- Numeric columns: min, max, mean, median, std, skew, P25, P75
- Categorical columns: top value, top frequency, top percentage

### Output

`docs/profiling/retailflow_data_profile.html` — self-contained, interactive HTML with:
- CSS tab navigation (one tab per table)
- Summary cards (total rows, columns, numeric/categorical count, null cells, high-cardinality count)
- Expandable stats sections via `<details>` elements
- Responsive design for mobile
- Dark gradient header with metadata (generation timestamp, totals)

---

## 12. Virtual Environment Strategy

### Isolation Map

| Environment | Location | Dependencies | Used By Pipeline Steps | Also Used By |
|-------------|----------|-------------|----------------------|-------------|
| **Main** | `.venv/` | pandas, numpy, SQLAlchemy, psycopg2-binary, Faker, apache-airflow, great_expectations, pytest, pytest-cov, black, flake8, pre-commit, python-dotenv, tqdm, openpyxl, streamlit, plotly, requests, networkx, matplotlib | 1 (data gen), 2 (load), 5 (excel), 7 (lineage), 8 (profiling) | Dashboard, tests, health check, linting, formatting |
| **dbt** | `.venv-dbt/` | `dbt-core==1.7.14`, `dbt-postgres==1.7.14` | 3 (dbt run), 4 (dbt test), 6 (dbt docs) | `dbt debug`, `dbt parse` (CI/CD) |

### Root Cause: mashumaro Dependency Conflict

`dbt-core` 1.7.x pins `mashumaro<4`. Apache Airflow (≥ 2.7) and Great Expectations (≥ 0.18) both require `mashumaro>=4`. A single `pip install` cannot satisfy both constraints simultaneously:

```
# This WILL fail:
pip install dbt-core==1.7.14 apache-airflow>=2.7
# → ERROR: mashumaro 3.x incompatible with mashumaro 4.x
```

The dual-venv strategy mirrors how production teams isolate dbt runtimes from orchestration runtimes.

### Cross-Platform Executable Resolution

| Platform | `_py_exe()` | `_dbt_exe()` |
|----------|-------------|--------------|
| **Windows (local dev)** | `.venv\Scripts\python.exe` | `.venv-dbt\Scripts\dbt.exe` |
| **Linux (Docker container)** | `sys.executable` | `$DBT_EXECUTABLE` env var → `/opt/dbt-venv/bin/dbt` |

### Container Build (Dockerfile)

```
Layer 1: System deps        gcc, libpq-dev, curl
Layer 2: Core Python deps   pandas, streamlit, airflow, GE, networkx, requests, ...
Layer 3: Isolated dbt env   /opt/dbt-venv — dbt-core + dbt-postgres only
Layer 4: Application code   scripts/, src/, dbt/, tests/
```

---

## 13. Quality Guardrails — CI/CD Pipeline

**File:** `.github/workflows/ci_cd.yml`

### Workflow Diagram

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
                    │       │ 77 tests│ │ SQL     │                    │
                    │       └─────────┘ │ compiles│                    │
                    │                   └─────────┘                    │
                    │                                                  │
                    │   PostgreSQL 15 service container (shared)       │
                    └──────────────────────────────────────────────────┘
```

### Job 1 — Core Python (Lint & Test)

| Step | Tool | What It Validates |
|------|------|-------------------|
| 1 | `actions/checkout@v4` | Pulls the repository |
| 2 | `actions/setup-python@v5` | Python 3.12 with pip cache |
| 3 | `pip install -r requirements.txt` | Installs all main dependencies |
| 4 | `flake8` | Code style (ignores E501, W503) |
| 5 | `black --check` | Formatting consistency |
| 6 | `pytest` on tests/ | 77+ unit and integration tests |

### Job 2 — dbt Validation

| Step | Tool | What It Validates |
|------|------|-------------------|
| 1 | `pip install dbt-core dbt-postgres` | Simulates `.venv-dbt` isolation |
| 2 | `CREATE SCHEMA raw` | Creates source tables for dbt source resolution |
| 3 | `dbt debug` | Connection test to live PostgreSQL service container |
| 4 | `dbt parse` | SQL compilation — validates all models, refs, sources, macros, tests |

---

## 14. Containerisation & Deployment

| Service | Container | Base Image | Purpose |
|---------|-----------|------------|---------|
| `db` | `retailflow-db` | `postgres:15-alpine` | PostgreSQL warehouse with `pg_isready` healthcheck |
| `app` | `retailflow-app` | `python:3.12-slim` (via `Dockerfile`) | Streamlit dashboard + full ETL pipeline |
| `pgadmin` | `retailflow-pgadmin` | `dpage/pgadmin4:latest` | Web-based PostgreSQL admin (optional) |

### Docker Compose Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  app        │────▶│  db         │     │  pgadmin    │
│ :8501       │     │ :5432       │     │ :5050       │
│             │     │             │     │             │
│ Streamlit   │     │ PostgreSQL  │     │ Web Admin   │
│ ETL scripts │     │ 15 alpine   │     │ (optional)  │
└─────────────┘     └─────────────┘     └─────────────┘
```

### Deployment Runbook

```bash
# Build & launch all services
docker compose up --build

# Access dashboard: http://localhost:8501
# Access pgAdmin:   http://localhost:5050

# Run pipeline inside container
docker exec -it retailflow-app python scripts/orchestrate.py --profile small

# Run health check inside container
docker exec -it retailflow-app python scripts/project_status.py

# Tear down
docker compose down
docker compose down -v  # ⚠️ destroys data volumes
```

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

make setup            # .venv + pip install -r requirements.txt + docker compose pull
make setup-dbt        # .venv-dbt + pip install dbt-core dbt-postgres
make run              # docker compose up -d (PostgreSQL + pgAdmin)
```

### Full Pipeline (Single Command)

```bash
make pipeline
# OR
.venv\Scripts\python scripts/orchestrate.py --profile small
```

### Detailed Step Execution

| Step | Component | Environment | What Happens | On Failure |
|------|-----------|-------------|-------------|------------|
| 1 | Generate Data | `.venv` | Faker creates 4 source files in `data/raw/` with referential integrity | Exit code 1 → pipeline halts |
| 2 | Load to PostgreSQL | `.venv` | Schema drift check → TRUNCATE tables → validation guardrails → PII hash → load → DLQ isolation → unified upsert | Exit code 1 (general) or 2 (schema drift); DLQ warning sent |
| 3 | dbt Run | `.venv-dbt` | 3 sub-steps: staging (views) → intermediate (view) → marts (tables, full-refresh) | Exit code 1 → pipeline halts mid-substep |
| 4 | dbt Test | `.venv-dbt` | 48 data quality tests; `run_results.json` parsed for failure metadata | Critical alert with per-test details → pipeline halts |
| 5 | Excel Export | `.venv` | 4 analytics sheets → styled `.xlsx` in `outputs/` | Exit code 1 → pipeline halts |
| 6 | dbt Docs Generate | `.venv-dbt` | `dbt compile` + `dbt docs generate` → `manifest.json` + `catalog.json` | Exit code 1 → pipeline halts |
| 7 | Lineage Graph | `.venv` | NetworkX parses `manifest.json` → 200 DPI PNG at `docs/lineage/` | Exit code 1 → pipeline halts |
| 8 | Data Profile Report | `.venv` | Pandas profiles mart tables → interactive HTML at `docs/profiling/` | Exit code 1 → pipeline halts |

### Validation

```bash
# Health check (6 dimensions)
make status
# → Checks: Docker → PostgreSQL → .env → CSVs → DB rows → schema drift quarantine

# Python test suite (77+ tests)
make test

# dbt tests (separate, 48 tests)
cd dbt && ..\.venv-dbt\Scripts\dbt test

# View generated artifacts
make docs             # dbt docs portal at localhost:8080
make dashboard        # Streamlit at localhost:8501
```

### CI/CD (GitHub)

Once pushed to `main`/`master`, the workflow at `.github/workflows/ci_cd.yml` automatically:
1. Spins up PostgreSQL 15 service container with health checks
2. Installs all Python dependencies from `requirements.txt`
3. Runs `flake8` linting + `black --check` formatting
4. Executes 77+ pytest tests against live PostgreSQL
5. Installs dbt-core + dbt-postgres in isolation
6. Creates `raw` schema for source resolution
7. Runs `dbt debug` (connection test) + `dbt parse` (SQL compilation)

---

*Architecture document v1.2.0 — Generated for the RetailFlow Pipeline project.*

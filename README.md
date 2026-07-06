# RetailFlow Pipeline

**An end-to-end data pipeline for a fictional e-commerce retail company — built by and for junior data engineers.**

---

## 1. Project Overview

RetailFlow Pipeline simulates a real-world task you would face as a junior data engineer:

> Your company sells products online. Every day, thousands of orders come in. The CEO wants to know: *Who are our best customers? Which products sell best? How is revenue trending?*

To answer these questions, you need to:

1. **Ingest** raw data from multiple sources (CSV files simulating an e-commerce platform)
2. **Clean and validate** the data (remove duplicates, fix types, check quality)
3. **Load** it into a data warehouse (PostgreSQL)
4. **Transform** it into analysis-ready tables using dbt (staging → intermediate → marts)
5. **Run analytics queries** to answer business questions

This project teaches the **modern data stack** — tools and patterns used at real companies like Airbnb, Spotify, and GitLab.

---

## 2. Architecture Diagram

```
                         RETAILFLOW PIPELINE ARCHITECTURE
    =====================================================================

    DATA SOURCES                    INGESTION                   RAW LAYER
    =============                   =========                   =========

    +-------------------+     +--------------------+     +------------------+
    |  Fake Data Gen    | --> |  Load CSVs to      | --> |  raw.customers   |
    |  (Python/Faker)   |     |  PostgreSQL         |     |  raw.products    |
    +-------------------+     |  (SQLAlchemy)       |     |  raw.orders      |
            |                 +--------------------+     +------------------+
            |                                                        |
            v                                                        v
    +-------------------+                                 +------------------+
    |  Raw CSVs in      |                                 |  Data Quality    |
    |  data/raw/        |                                 |  (Great Expect.) |
    +-------------------+                                 +------------------+
                                                                 |
                                                                 v
    =====================================================================
    STAGING ────> INTERMEDIATE ────> MARTS ────> ANALYTICS
    =====================================================================
         |                |                |                |
         v                v                v                v
    +-----------+   +--------------+   +------------+   +------------------+
    | stg_orders|   |int_orders_   |   |dim_customers|   |Top Customers by  |
    | stg_cust..|-->|enriched      |-->|dim_products |-->|Revenue           |
    | stg_prod..|   |(joined view) |   |fct_orders   |   |Monthly Sales     |
    +-----------+   +--------------+   +------------+   |Cohort Analysis   |
                                                        +------------------+
                                                                 |
                                                                 v
                                                     +------------------+
                                                     |  dbt Tests       |
                                                     |  (data quality)  |
                                                     +------------------+
```

**Data flow direction:** Left to right. Raw data comes in from the left, gets processed through each layer, and comes out the right as business-ready tables.

---

## 3. Tech Stack

| Tool | Purpose | Why We Use It | Junior Tip |
|------|---------|---------------|------------|
| **Python 3.10+** | Main programming language | Universal in data engineering; easy to learn | Focus on type hints and logging early |
| **Apache Airflow 2.8+** | Pipeline orchestration (DAGs) | Industry standard for scheduling & monitoring | Think of it as a "to-do list" for your pipeline tasks |
| **dbt 1.7+** | Data transformation SQL | Writes SQL for you; handles dependencies | dbt turns SQL into a modular, testable language |
| **PostgreSQL 15** | Data warehouse | Free, powerful, runs locally via Docker | Practice writing CTEs and window functions |
| **Docker & Compose** | Local infrastructure | No "works on my machine" problems | Docker = virtual Lego for services |
| **Pandas 2.x** | Data cleaning & ETL | Swiss Army knife for tabular data in Python | Vectorized ops are faster than for-loops |
| **Great Expectations 0.18+** | Data quality checks | Automated validation before data hits warehouse | Like unit tests but for your data |
| **SQLAlchemy 2.x** | Database connection | Python ↔ PostgreSQL bridge | Use context managers (`with`) for connections |
| **Faker 22+** | Fake data generation | Realistic synthetic data for dev/testing | Seed it (`Faker.seed(42)`) for reproducible results |
| **pytest 7+** | Unit testing | Ensure code works before deployment | One test file per source file |
| **Black + Flake8** | Code formatting/linting | Enforces consistent style like a senior code reviewer | Run `make format` before every commit |

---

## 4. Prerequisites

Install these tools **before** setting up the project:

| Tool | Version | Download |
|------|---------|----------|
| Python | 3.10+ | [python.org](https://www.python.org/downloads/) |
| Docker Desktop | Latest | [docker.com](https://www.docker.com/products/docker-desktop/) |
| Git | Latest | [git-scm.com](https://git-scm.com/downloads) |
| PostgreSQL client (psql) | 15+ | Included with PostgreSQL or `apt install postgresql-client` |

Verify installations:

```bash
python --version    # Should be 3.10+
docker --version    # Should be 24+
docker compose version  # Should be 2.x+
git --version       # Should be 2.x+
psql --version      # Should be 15+
```

---

## 5. Setup Instructions

### Step 1: Clone the repo

```bash
git clone https://github.com/elhussienysabry/retailflow-pipeline.git
cd retailflow-pipeline
```

### Step 2: Copy environment file

```bash
cp .env.example .env
```

Open `.env` in a text editor. All variables have sensible defaults — you typically only need to change passwords.

### Step 3: Run setup

```bash
make setup
```

**What `make setup` does internally:**

1. Copies `.env.example` to `.env` if not present
2. Creates a Python virtual environment (`.venv/`)
3. Installs all Python dependencies (pandas, SQLAlchemy, etc.) into `.venv`
4. Pulls the PostgreSQL and pgAdmin Docker images

This typically takes 2-5 minutes depending on your internet speed.

### Step 3b: Set up the isolated dbt environment (recommended)

To avoid known library conflicts between `dbt-core` and other packages (e.g., `mashumaro`), the project uses **two isolated virtual environments**:

- `.venv` — Main environment for data generation, loading, export, and the dashboard.
- `.venv-dbt` — Separate environment exclusively for `dbt-core` and `dbt-postgres`.

Set up the dbt environment once:

```bash
make setup-dbt
```

This creates `.venv-dbt/` and installs `dbt-core==1.7.14` / `dbt-postgres==1.7.14` inside it.

### Step 4: Start services

```bash
make run
```

**What `make run` does internally:**

1. Starts PostgreSQL on `localhost:5432` in a Docker container
2. Starts pgAdmin on `http://localhost:5050` (optional web admin tool)
3. Runs the SQL schema scripts automatically (creates `raw`, `staging`, `marts` schemas)
4. Waits 10 seconds for PostgreSQL to be healthy

### Step 5: Run the full pipeline manually

```bash
make generate-data   # Creates fake CSVs in data/raw/
make load-data      # Loads CSVs into PostgreSQL raw schema
make dbt-run        # Runs dbt (uses .venv-dbt) → staging → intermediate → marts
make dbt-test       # Runs dbt data quality tests (uses .venv-dbt)
```

> **Note:** The `dbt-run` and `dbt-test` commands use the isolated `.venv-dbt` environment
> to avoid conflicts with other Python packages. If you prefer to run dbt manually:
> ```bash
> cd dbt
> ..\.venv-dbt\Scripts\dbt run
> ..\.venv-dbt\Scripts\dbt test
> ```

### Step 6: Run analytics queries

Connect to PostgreSQL directly:

```bash
psql -h localhost -U retailflow_user -d retailflow
```

Then run any analytics query from `sql/analytics/`, for example:

```sql
\i sql/analytics/top_customers_by_revenue.sql
```

### Step 7 (Alternative): Trigger via Airflow

1. Open Airflow UI at **http://localhost:8080**
2. Log in with credentials from your `.env` file (default: `admin` / `admin`)
3. Find the DAG named `retailflow_pipeline`
4. Click the ▶ Play button → **Trigger DAG**
5. Watch each task execute in order

---

## 6. Project Structure Explained

| Folder / File | What It Is | Why It Exists |
|---------------|-----------|---------------|
| `data/raw/` | Raw CSV files (orders, customers, products) | Simulates data ingestion from source systems |
| `data/processed/` | Cleaned output files | Reserved for future processed exports |
| `scripts/` | Python data scripts | Core pipeline logic (generate, load, and status check) |
| `src/exports/` | Analytics Excel export | Runs 4 warehouse queries and produces a styled `.xlsx` workbook |
| `src/dashboard/` | Streamlit dashboard | Interactive KPI dashboard with charts and data tables |
| `outputs/` | Export output directory | Timestamped Excel analytics exports land here |
| `.venv/` | Main Python virtual environment | Data gen, loading, export, dashboard, tests |
| `.venv-dbt/` | Isolated dbt virtual environment | `dbt-core` + `dbt-postgres` only (avoids `mashumaro` conflicts) |
| `airflow/dags/` | Airflow DAG definition | Orchestrates the 8-step pipeline |
| `airflow/plugins/` | Custom Airflow hooks | Reusable database connection code |
| `dbt/models/staging/` | dbt staging models | Raw → clean (rename, cast, deduplicate) |
| `dbt/models/intermediate/` | dbt join models | Combine staging tables (enrich orders) |
| `dbt/models/marts/` | dbt mart models | Business-ready dims + fact table |
| `dbt/tests/` | dbt data tests | Custom SQL assertions for data quality |
| `dbt/macros/` | dbt Jinja macros | Reusable SQL snippets (e.g., cents → dollars) |
| `sql/schema/` | PostgreSQL DDL scripts | Create schemas and tables in the warehouse |
| `sql/analytics/` | Business analysis queries | Answer real business questions |
| `tests/` | pytest unit tests | Verify Python code correctness |
| `great_expectations/` | GE expectation suites | Data quality validation config |
| `.env.example` | Environment template | Centralized configuration |

---

## 7. Data Flow Walkthrough

Follow a single order record through the entire pipeline:

### Step 1: Fake data generation

`scripts/generate_fake_data.py` creates:
- `data/raw/customers.csv` (10,000 rows)
- `data/raw/products.csv` (500 rows)
- `data/raw/orders.csv` (100,000 rows)

**Example record in `orders.csv`:**
```
order_id                               | customer_id                          | product_id                          | quantity | order_date | status    | discount_pct | shipping_days
a1b2c3d4-...                           | e5f6g7h8-...                         | i9j0k1l2-...                         | 3        | 2024-06-15 | completed | 10           | 5
```

### Step 2: Load to PostgreSQL raw schema

`scripts/load_to_postgres.py` reads the CSVs and inserts rows into:

```
raw.orders   →  PostgreSQL table (raw schema)
raw.customers →  PostgreSQL table (raw schema)
raw.products  →  PostgreSQL table (raw schema)
```

### Step 3: dbt staging

`dbt/models/staging/stg_orders.sql` transforms the raw data:
- Casts `order_date` from TEXT to DATE
- Normalizes `status` to lowercase
- Removes rows with null foreign keys
- Deduplicates

```sql
-- Simplified example of what happens:
SELECT
    order_id,
    CAST(order_date AS DATE) AS order_date,
    LOWER(TRIM(status)) AS status
FROM raw.orders
WHERE order_id IS NOT NULL
```

### Step 4: dbt intermediate

`dbt/models/intermediate/int_orders_enriched.sql` joins all staging tables:

```sql
SELECT
    o.*,
    c.first_name, c.last_name, c.email,
    p.product_name, p.category, p.price_cents,
    o.quantity * p.price_cents AS gross_revenue_cents,
    ROUND(o.quantity * p.price_cents * (1 - o.discount_pct/100)) AS net_revenue_cents
FROM stg_orders o
LEFT JOIN stg_customers c ON o.customer_id = c.customer_id
LEFT JOIN stg_products p ON o.product_id = p.product_id
```

### Step 5: dbt marts (business-ready)

The intermediate data feeds three mart tables:

- **`dim_customers`** — one row per customer with lifetime value
- **`dim_products`** — one row per product with total sales
- **`fct_orders`** — one row per order with revenue in dollars

### The original order record now appears in:

```
raw.orders               → Raw copy
staging.stg_orders        → Cleaned copy
intermediate.int_orders... → Enriched with customer + product data
marts.fct_orders          → Final fact row with revenue in dollars
marts.dim_customers       → Customer total updated with this order
marts.dim_products        → Product totals updated with this order
```

---

## 8. dbt Models Explained

dbt organizes SQL transformations into three layers. Each layer has a specific purpose.

### Staging Layer (`dbt/models/staging/`)

**Purpose:** Mirror raw tables but cleaned. No joins. No business logic.

| Model | Source | What Changes |
|-------|--------|-------------|
| `stg_orders` | `raw.orders` | Cast dates, lowercase status, remove duplicates |
| `stg_customers` | `raw.customers` | Trim whitespace, lowercase email, deduplicate |
| `stg_products` | `raw.products` | Filter invalid prices, trim names, deduplicate |

**Rule of thumb:** One staging model per source table. Staging should be boring — just clean and type.

### Intermediate Layer (`dbt/models/intermediate/`)

**Purpose:** Join staging tables together. Add business calculations.

| Model | Source | What Changes |
|-------|--------|-------------|
| `int_orders_enriched` | stg_orders + stg_customers + stg_products | Join all 3, compute gross and net revenue |

**Rule of thumb:** Intermediate models are the "assembly line" — you combine parts to make something more valuable.

### Marts Layer (`dbt/models/marts/`)

**Purpose:** Business-ready tables organized as a star schema. Analysts query these directly.

| Model | Type | Contains |
|-------|------|----------|
| `dim_customers` | Dimension | Customer attributes + lifetime value |
| `dim_products` | Dimension | Product attributes + total sales |
| `fct_orders` | Fact | Order measures in dollars |

**Rule of thumb:** Marts should answer business questions directly. If an analyst can't use these tables, they aren't done right.

---

## 9. Data Quality

### Great Expectations

Great Expectations (GE) is a tool that validates your data before it enters the warehouse. Think of it as **unit tests for your data**.

The expectation suite at `great_expectations/expectations/orders_suite.json` checks these rules on the orders CSV:

| Expectation | Plain English |
|-------------|---------------|
| `expect_table_columns_to_match_set` | The orders CSV must have exactly 8 columns with these names |
| `expect_column_values_to_not_be_null` on `order_id` | Every order must have an ID (no nulls) |
| `expect_column_values_to_not_be_null` on `customer_id` | Every order must reference a customer |
| `expect_column_values_to_be_in_set` on `status` | Status must be "completed", "returned", or "pending" — nothing else |
| `expect_column_values_to_be_between` on `quantity` | Quantity must be between 1 and 10 |
| `expect_column_values_to_be_between` on `discount_pct` | Discount must be between 0% and 50% |
| `expect_column_values_to_be_between` on `shipping_days` | Shipping days must be between 1 and 14 |

### dbt Tests

Every dbt model has a matching `.yml` file with at least 2 tests per model:

- **`not_null`** — Column has no null values
- **`unique`** — Every value in the column is different
- **`accepted_values`** — Column only contains allowed values
- **`relationships`** — Foreign key points to a valid primary key

Plus 2 custom singular tests:

- `assert_positive_revenue.sql` — No order has negative revenue
- `assert_no_null_customer_id.sql` — All orders reference valid customers

---

## 10. Analytics Queries

These 4 SQL queries in `sql/analytics/` answer real business questions.

### Query 1: Top Customers by Revenue

**File:** `sql/analytics/top_customers_by_revenue.sql`

**Business question:** Who are our top 10 highest-value customers?

```sql
SELECT first_name, last_name, email, country, city,
       ROUND(total_net_revenue, 2) AS total_net_revenue
FROM customer_revenue
ORDER BY total_net_revenue DESC
LIMIT 10;
```

### Query 2: Monthly Sales Trend

**File:** `sql/analytics/monthly_sales_trend.sql`

**Business question:** How is revenue trending month over month?

```sql
SELECT month, total_orders, net_revenue,
       ROUND((net_revenue - LAG(net_revenue) OVER (ORDER BY month))
             / NULLIF(LAG(net_revenue) OVER (ORDER BY month), 0) * 100, 2)
       AS month_over_month_growth_pct
FROM monthly_revenue
ORDER BY month;
```

### Query 3: Product Category Performance

**File:** `sql/analytics/product_category_performance.sql`

**Business question:** Which product categories drive the most revenue?

```sql
SELECT category, total_orders, total_units_sold, total_net_revenue,
       ROUND(total_net_revenue / SUM(total_net_revenue) OVER () * 100, 2)
       AS revenue_share_pct
FROM category_stats
ORDER BY total_net_revenue DESC;
```

### Query 4: Customer Cohort Analysis

**File:** `sql/analytics/customer_cohort_analysis.sql`

**Business question:** How does customer spending change over time by signup cohort?

```sql
SELECT cohort_month, cohort_index, active_customers,
       total_revenue, avg_revenue_per_customer
FROM cohort_aggregated
ORDER BY cohort_month, cohort_index;
```

---

## 11. How to Run Tests

```bash
# Run all tests
pytest tests/ -v --tb=short

# Run a specific test file
pytest tests/test_generate_data.py -v

# Run tests with coverage report
pytest tests/ --cov=scripts/ --cov-report=term-missing
```

### What each test file checks

| Test File | What It Tests | Key Checks |
|-----------|--------------|------------|
| `test_generate_data.py` | Fake data generator | Correct row counts, expected columns, valid data ranges |
| `test_transformations.py` | Business logic | Cents-to-dollars conversion, discount calculation, status normalization |
| `test_load_to_postgres.py` | DB loader | Engine creation, schema creation, truncation logic |
| `test_project_status.py` | Status checker | Docker, PostgreSQL, env, CSV checks, overall status logic |
| `test_excel_export.py` | Excel exporter | Workbook creation, sheet names, headers, styling, currency format |
| `test_generate_data_profiles.py` | Scale profiles | Profile defaults, explicit overrides, CLI argument parsing |

---

## 12. How to Run the Status Check

The `project_status.py` script performs a quick end-to-end health check of your pipeline. It verifies Docker, PostgreSQL, the `.env` file, raw CSV files, and database row counts — and prints beginner-friendly fix hints if something is wrong.

```bash
# Run the status check
python scripts/project_status.py
```

**Exit codes:**
- `0` — Everything is healthy
- `1` — Warnings (pipeline is degraded but usable)
- `2` — Failures (pipeline is not operational)

Example output when everything is working:

```text
RetailFlow Pipeline Status Report
---------------------------------
Docker: OK - Docker Desktop is running
PostgreSQL: OK - PostgreSQL container is running and reachable
.env File: OK - .env file exists and loaded successfully
Raw CSV Files: OK - All required CSV files present in data/raw/
Database Row Counts: OK - All raw tables have data
Overall Status: Healthy
```

Example output when something is broken:

```text
RetailFlow Pipeline Status Report
---------------------------------
Docker: OK - Docker Desktop is running
PostgreSQL: WARNING - Container is running but database is not reachable
.env File: OK - .env file exists and loaded successfully
Raw CSV Files: FAIL - Missing csv files in data/raw/
Database Row Counts: WARNING - Skipped — PostgreSQL not reachable
Overall Status: Unhealthy

Fix Hints:
  - Start the container: 'docker compose up -d' from the project root.
  - Run: .venv\Scripts\python scripts\generate_fake_data.py
```

## 13. How to Use the New Features

### 13.1 Data Generation with Scale Profiles

The data generator supports predefined scale profiles for quick dataset sizing (uses `.venv`):

```bash
# Small dataset (fast, good for testing)
.venv\Scripts\python scripts\generate_fake_data.py --profile small

# Medium dataset (default)
.venv\Scripts\python scripts\generate_fake_data.py --profile medium

# Large dataset (100k customers, 5k products, 1M orders)
.venv\Scripts\python scripts\generate_fake_data.py --profile large
```

**Override individual counts:**

Any explicit `--customers`, `--products`, or `--orders` flag overrides the profile for that specific count:

```bash
# Start from 'small' but generate 50,000 orders
.venv\Scripts\python scripts\generate_fake_data.py --profile small --orders 50000
```

**Profile reference:**

| Profile  | Customers | Products | Orders    |
|----------|-----------|----------|-----------|
| small    | 1,000     | 100      | 10,000    |
| medium   | 10,000    | 500      | 100,000   |
| large    | 100,000   | 5,000    | 1,000,000 |

### 13.2 Analytics Excel Export

After running dbt (`make dbt-run`), export analytics to a professionally styled Excel workbook. This script runs in `.venv`:

```bash
# Via Make
make export

# Or directly
.venv\Scripts\python -m src.exports.excel_exporter
```

The workbook is saved to `outputs/retail_analytics_YYYYMMDD_HHMMSS.xlsx` and contains 4 sheets:

| Sheet               | Contents                                    |
|---------------------|---------------------------------------------|
| Top Customers       | Top 10 customers by total net revenue       |
| Monthly Sales       | Month-over-month revenue and order counts   |
| Category Performance| Revenue and units sold per product category  |
| Cohort Analysis     | Customer retention by first-purchase month   |

The Excel file uses professional formatting: bold headers with a dark blue fill, auto-fitted column widths, and currency formatting on revenue columns.

### 13.3 Streamlit Analytics Dashboard

Launch an interactive dashboard to explore pipeline analytics in real time (uses `.venv`):

```bash
# Via Make
make dashboard

# Or directly
.venv\Scripts\streamlit run src\dashboard\app.py
```

The dashboard opens in your browser at `http://localhost:8501`:

![RetailFlow Pipeline Dashboard](Images/dashboard.png)

**Dashboard features:**

| Feature | Description |
|---------|-------------|
| **6 KPI cards** | Total Orders, Total Net Revenue, Active Customers, Avg Order Value, Returned / Pending, Return Rate |
| **Monthly Sales Trend** | Interactive Plotly line chart with hover, zoom, and unified tooltip |
| **Category Performance** | Grouped bar chart comparing revenue and units sold per category |
| **Revenue by Country** | Top 15 countries by revenue, colored by customer count |
| **Top 10 Customers** | Data table with customer details and formatted revenue |
| **Sidebar filters** | Auto-refresh toggle with configurable interval, category multi-select |
| **Export to Excel** | One-click export button that generates the styled analytics workbook |
| **Data freshness** | Shows latest order date from the warehouse |

The dashboard uses Streamlit's built-in caching (`@st.cache_data`) so it only queries the database when data expires (5 minutes) or when you click Refresh.

> **Note:** All three features require PostgreSQL to be running with dbt models materialized. Run `make run && make generate-data && make load-data && make dbt-run` first.
>
> **Virtual environment reference:**
> | Environment | Location | Purpose |
> |-------------|----------|---------|
> | `.venv` | Project root | Data generation, loading, export, dashboard, status checks |
> | `.venv-dbt` | Project root | `dbt-core` + `dbt-postgres` only (avoids `mashumaro` conflicts) |

---

## 14. Common Errors & Fixes

| Error Message | Cause | Fix |
|---------------|-------|-----|
| `psycopg2.OperationalError: connection refused` | PostgreSQL Docker container not running | Run `make run` and wait 10 seconds |
| `Faker` module not found | Virtual environment not activated or dependencies not installed | Run `make setup` to install all dependencies |
| `dbt: command not found` | dbt not installed or not in PATH | Activate venv: `.venv\Scripts\activate` or run `make setup` |
| `Permission denied` on Docker socket | Docker Desktop not running or user not in docker group | Start Docker Desktop, restart terminal |
| `relation "raw.orders" does not exist` | Schema SQL not executed | Run `docker compose down -v` then `make run` again |
| `Port 5432 already in use` | Another PostgreSQL instance running locally | Stop local PostgreSQL: `sudo service postgresql stop` |
| `Great Expectations checkpoint not found` | GE not initialized for this project | Run `great_expectations init` in the project root |
| `dbt test` fails with `value "COMPLETED" not in accepted values` | Raw data has uppercase status | The staging model should lowercase status — check `stg_orders.sql` |
| `KeyError: 'POSTGRES_HOST'` | `.env` file missing or not loaded | Copy `.env.example` to `.env` and fill in values |
| `make` not recognized | Make is not installed on Windows | Install via Chocolatey: `choco install make`, or run commands manually |
| `csv.Error: field larger than field limit` | Very large CSV field | Rare with this dataset; increase `csv.field_size_limit()` |
| `sqlalchemy.exc.ProgrammingError: schema "raw" already exists` | Schema exists but is empty | This is harmless — the script uses `CREATE SCHEMA IF NOT EXISTS` |
| `airflow.exceptions.AirflowException: DAG not found` | DAG file not in correct folder | Ensure `retailflow_dag.py` is in `airflow/dags/` |

---

## 15. What You Learned

Congratulations! By completing this project, you have practiced these real-world data engineering skills:

- **Building an end-to-end data pipeline** from ingestion to analytics
- **Working with Apache Airflow** — defining DAGs, tasks, dependencies, and execution order
- **Writing dbt models** — staging, intermediate, and mart layers with proper testing
- **Modeling data as a star schema** — dimension tables and fact tables for analytics
- **Containerizing services with Docker** — PostgreSQL and pgAdmin for local development
- **Using Great Expectations** for automated data quality validation
- **Writing clean, production-ready Python** — type hints, logging, docstrings, error handling
- **Exporting analytics to Excel** — styled workbooks with openpyxl, auto-fitted columns, currency formatting
- **Building Streamlit dashboards** — KPI cards, charts, caching, and interactive refresh for real-time analytics
- **Designing CLI scale profiles** — predefined dataset sizes with backward-compatible override flags
- **Structuring SQL with CTEs** — no nested subqueries, readable and maintainable
- **Performing cohort analysis** — a classic analytics technique used at every tech company
- **Testing data code with pytest** — mock database connections, verify transformations
- **Using environment variables for configuration** — 12-factor app methodology
- **Following software engineering best practices** — pre-commit hooks, linting, formatting
- **Understanding the modern data stack** — how Airflow, dbt, PostgreSQL, and Python work together
- **Debugging pipeline failures** — reading logs, checking task dependencies, fixing schema issues
- **Documenting technical work** — writing READMEs, model descriptions, and inline comments

---

## 16. Next Steps

This project is a foundation. Here's how to extend it for more advanced learning:

### Add Spark (Distributed Processing)

Replace `pandas` with **PySpark** for the data generation step. This prepares you for big data scenarios where data doesn't fit in memory.

```python
# Instead of pandas
df = spark.read.csv("data/raw/orders.csv", header=True)
df.write.mode("append").saveAsTable("raw.orders")
```

### Move to AWS S3 (Cloud Storage)

Instead of reading CSVs from disk, upload them to **Amazon S3** and use **boto3** to read them. This simulates a real cloud-native architecture.

### Add Kafka Streaming (Real-Time Data)

Set up **Apache Kafka** + **Kafka Connect** to stream orders in real time instead of batch processing. This teaches stream processing concepts (event time, watermarks, exactly-once semantics).

### Deploy the Dashboard to the Cloud

Deploy the Streamlit dashboard to **Streamlit Community Cloud**, **Hugging Face Spaces**, or **Railway** so stakeholders can access it without running Python. Streamlit Cloud is free for public apps.

### Add Incremental Loading

Instead of full refreshes, implement **incremental dbt models** that only process new data since the last run. This is critical for production pipelines.

### Add CI/CD with GitHub Actions

Create a `.github/workflows/pipeline.yml` that runs tests, linting, and even deploys dbt models on every pull request.

### Add Data Lineage with dbt docs

```bash
cd dbt
dbt docs generate
dbt docs serve
```

This creates a web UI showing how data flows through your models — who depends on what.

---

## 17. Glossary

| Term | Simple Definition |
|------|-------------------|
| **ETL** | Extract, Transform, Load — old-school pipeline: transform before loading |
| **ELT** | Extract, Load, Transform — modern pipeline: load raw, transform in warehouse (what dbt does) |
| **DAG** | Directed Acyclic Graph — a set of tasks with dependencies, no cycles (like Airflow DAGs) |
| **Schema** | A namespace in a database that groups tables together (e.g., `raw`, `staging`, `marts`) |
| **Idempotent** | An operation that produces the same result no matter how many times you run it (safe to re-run) |
| **Fact Table** | The "metric" table in a star schema — contains measures (revenue, quantity) and foreign keys |
| **Dimension Table** | The "context" table in a star schema — contains attributes (customer name, product category) |
| **Star Schema** | A database design with one central fact table surrounded by dimension tables (looks like a star) |
| **Data Warehouse** | A database optimized for analytics queries — stores historical data from many sources |
| **Data Lake** | A storage system for raw data in any format (CSV, JSON, Parquet, images, videos) |
| **Lakehouse** | A hybrid that combines data lake flexibility with warehouse performance (e.g., Databricks) |
| **Pipeline** | A series of steps that move and transform data from source to destination (the whole project!) |
| **Orchestration** | Automating, scheduling, and monitoring pipeline tasks (what Airflow does) |
| **Transformation** | Changing data from one format/structure to another (cleaning, joining, aggregating) |
| **Staging** | The first transformation layer — raw data gets cleaned and typed (no joins yet) |
| **Partition** | Splitting a table into smaller pieces by a column (e.g., by date) for faster queries |
| **Index** | A data structure that speeds up lookups on specific columns (like a book's table of contents) |
| **Data Quality** | How trustworthy your data is — completeness, accuracy, consistency, validity |
| **CDC** | Change Data Capture — tracking changes in a source database to sync them to the warehouse |
| **Data Lineage** | A map showing where data came from, how it was transformed, and where it goes |

---

## License

This project is for educational purposes. Free to use, modify, and share.

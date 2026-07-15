# =============================================================================
# RetailFlow Pipeline — Makefile
# =============================================================================
# Common commands for setup, running, and maintaining the pipeline.
# Type `make help` to see all available commands.
# =============================================================================

.PHONY: help setup setup-dbt run stop clean clean-rejected test coverage lint format status export dashboard pipeline docs dbt-run dbt-test dbt-snapshot dbt-deps sql-analyze generate-data load-data lakehouse

help:  # Print available commands with descriptions
	@echo "RetailFlow Pipeline — Available Commands"
	@echo "=========================================="
	@echo ""
	@echo "  make setup          Install dependencies, create venv, pull Docker images"
	@echo "  make run            Start all Docker services (PostgreSQL + pgAdmin)"
	@echo "  make stop           Stop all Docker services"
	@echo "  make generate-data  Run the fake data generation script"
	@echo "  make load-data      Load generated CSV data into PostgreSQL raw schema"
	@echo "  make setup-dbt      Create isolated venv for dbt (avoids mashumaro conflicts)"
	@echo "  make dbt-deps       Install dbt packages (dbt_utils for SCD Type 2)"
	@echo "  make dbt-snapshot   Run dbt snapshots (SCD Type 2 history tracking)"
	@echo "  make dbt-run        Execute all dbt models (staging -> intermediate -> marts)"
	@echo "  make dbt-test       Run dbt data tests on all models"
	@echo "  make pipeline       Run full 9-step pipeline end-to-end via orchestrator"
	@echo "  make test           Run pytest unit tests (174+ tests)"
	@echo "  make coverage       Run pytest with coverage report"
	@echo "  make status         Run end-to-end pipeline health check (8 dimensions)"
	@echo "  make lakehouse      Verify Lakehouse Parquet files in data/lakehouse/"
	@echo "  make export         Export analytics to styled Excel workbook"
	@echo "  make dashboard      Launch the Streamlit analytics dashboard"
	@echo "  make docs           Launch the dbt docs metadata portal in browser"
	@echo "  make lint           Run flake8 linting on all Python files"
	@echo "  make format         Auto-format Python code with black"
	@echo "  make sql-analyze    Run all 4 analytics queries against the warehouse"
	@echo "  make clean          Remove both venvs, generated data, Docker volumes"
	@echo "  make clean-rejected Remove DLQ rejected files and schema drift quarantine"
	@echo ""

setup:  # Install dependencies, create .env if missing, pull Docker images
	@echo ">> Setting up RetailFlow Pipeline..."
	@if not exist ".env" copy .env.example .env
	@echo ">> Creating Python virtual environment..."
	@if not exist ".venv" python -m venv .venv
	@echo ">> Installing Python dependencies..."
	@.venv\Scripts\pip install -r requirements.txt
	@echo ">> Pulling Docker images..."
	@docker compose pull
	@echo ">> Setup complete! Run 'make run' to start services."

run:  # Start all Docker containers (PostgreSQL + pgAdmin) in detached mode
	@echo ">> Starting Docker services..."
	@docker compose up -d
	@echo ">> Waiting for PostgreSQL to be healthy..."
	@timeout /t 10 /nobreak >nul
	@echo ">> Services are running:"
	@echo "   PostgreSQL : localhost:5432"
	@echo "   pgAdmin    : http://localhost:5050"
	@echo ">> To generate and load data: make generate-data && make load-data"

stop:  # Stop all Docker containers
	@echo ">> Stopping Docker services..."
	@docker compose down
	@echo ">> All services stopped."

generate-data:  # Run the fake data generation script
	@echo ">> Generating fake retail data..."
	@.venv\Scripts\python scripts\generate_fake_data.py
	@echo ">> Data generated in data/raw/"

load-data:  # Load raw CSVs into PostgreSQL raw schema
	@echo ">> Loading raw data into PostgreSQL..."
	@.venv\Scripts\python scripts\load_to_postgres.py
	@echo ">> Data loaded into PostgreSQL raw schema."

dbt-deps:  # Install dbt packages (dbt_utils for SCD Type 2)
	@echo ">> Installing dbt packages..."
	@cd dbt && ..\.venv-dbt\Scripts\dbt deps
	@echo ">> dbt packages installed."

dbt-snapshot:  # Run dbt snapshots (SCD Type 2 history tracking)
	@echo ">> Running dbt snapshots..."
	@cd dbt && ..\.venv-dbt\Scripts\dbt snapshot
	@echo ">> dbt snapshots complete."

setup-dbt:  # Create and install the isolated dbt virtual environment
	@echo ">> Setting up isolated dbt virtual environment..."
	@if not exist ".venv-dbt" python -m venv .venv-dbt
	@echo ">> Installing dbt-core and dbt-postgres..."
	@.venv-dbt\Scripts\pip install dbt-core==1.7.14 dbt-postgres==1.7.14
	@echo ">> dbt environment ready. Use '.venv-dbt\\Scripts\\dbt' for dbt commands."

dbt-run:  # Execute dbt models (staging -> intermediate -> marts; marts full-refresh)
	@echo ">> Running dbt staging models..."
	@cd dbt && ..\.venv-dbt\Scripts\dbt run --select staging
	@echo ">> Running dbt intermediate models..."
	@cd dbt && ..\.venv-dbt\Scripts\dbt run --select intermediate
	@echo ">> Running dbt mart models (full-refresh)..."
	@cd dbt && ..\.venv-dbt\Scripts\dbt run --select marts --full-refresh
	@echo ">> All dbt models executed."

dbt-test:  # Run dbt data tests
	@echo ">> Running dbt data tests..."
	@cd dbt && ..\.venv-dbt\Scripts\dbt test
	@echo ">> dbt tests complete."

sql-analyze:  # Execute all 4 analytics queries against the warehouse
	@echo ">> Running analytics queries..."
	@echo "=== Top Customers by Revenue ==="
	@.venv\Scripts\python -c "from sqlalchemy import create_engine, text; import os; e = create_engine(f'postgresql://{os.getenv(\"POSTGRES_USER\",\"retailflow_user\")}:{os.getenv(\"POSTGRES_PASSWORD\",\"retailflow_pass\")}@{os.getenv(\"POSTGRES_HOST\",\"localhost\")}:{os.getenv(\"POSTGRES_PORT\",\"5432\")}/{os.getenv(\"POSTGRES_DB\",\"retailflow\")}'); import pathlib; q = pathlib.Path('sql/analytics/top_customers_by_revenue.sql').read_text(); r = e.execute(text(q)); [print(row) for row in r]"
	@echo "=== Monthly Sales Trend ==="
	@.venv\Scripts\python -c "from sqlalchemy import create_engine, text; import os; e = create_engine(f'postgresql://{os.getenv(\"POSTGRES_USER\",\"retailflow_user\")}:{os.getenv(\"POSTGRES_PASSWORD\",\"retailflow_pass\")}@{os.getenv(\"POSTGRES_HOST\",\"localhost\")}:{os.getenv(\"POSTGRES_PORT\",\"5432\")}/{os.getenv(\"POSTGRES_DB\",\"retailflow\")}'); import pathlib; q = pathlib.Path('sql/analytics/monthly_sales_trend.sql').read_text(); r = e.execute(text(q)); [print(row) for row in r]"
	@echo "=== Product Category Performance ==="
	@.venv\Scripts\python -c "from sqlalchemy import create_engine, text; import os; e = create_engine(f'postgresql://{os.getenv(\"POSTGRES_USER\",\"retailflow_user\")}:{os.getenv(\"POSTGRES_PASSWORD\",\"retailflow_pass\")}@{os.getenv(\"POSTGRES_HOST\",\"localhost\")}:{os.getenv(\"POSTGRES_PORT\",\"5432\")}/{os.getenv(\"POSTGRES_DB\",\"retailflow\")}'); import pathlib; q = pathlib.Path('sql/analytics/product_category_performance.sql').read_text(); r = e.execute(text(q)); [print(row) for row in r]"
	@echo "=== Customer Cohort Analysis ==="
	@.venv\Scripts\python -c "from sqlalchemy import create_engine, text; import os; e = create_engine(f'postgresql://{os.getenv(\"POSTGRES_USER\",\"retailflow_user\")}:{os.getenv(\"POSTGRES_PASSWORD\",\"retailflow_pass\")}@{os.getenv(\"POSTGRES_HOST\",\"localhost\")}:{os.getenv(\"POSTGRES_PORT\",\"5432\")}/{os.getenv(\"POSTGRES_DB\",\"retailflow\")}'); import pathlib; q = pathlib.Path('sql/analytics/customer_cohort_analysis.sql').read_text(); r = e.execute(text(q)); [print(row) for row in r]"

status:  # Run the end-to-end pipeline health check
	@echo ">> Running pipeline health check..."
	@.venv\Scripts\python scripts\project_status.py

export:  # Export analytics to a styled Excel workbook (requires dbt-run first)
	@echo ">> Exporting analytics to Excel..."
	@.venv\Scripts\python -m src.exports.excel_exporter

pipeline:  # Run the full 9-step pipeline end-to-end via the orchestrator
	@echo ">> Running full pipeline via orchestrator..."
	@.venv\Scripts\python scripts\orchestrate.py

dashboard:  # Launch the Streamlit analytics dashboard
	@echo ">> Starting Streamlit dashboard..."
	@.venv\Scripts\streamlit run src\dashboard\app.py

docs:  # Generate dbt docs artifacts and launch the metadata portal in a browser
	@echo ">> Compiling dbt project..."
	@cd dbt && ..\.venv-dbt\Scripts\dbt compile
	@echo ">> Generating documentation catalog..."
	@cd dbt && ..\.venv-dbt\Scripts\dbt docs generate
	@echo ">> Starting dbt docs server at http://localhost:8080..."
	@cd dbt && ..\.venv-dbt\Scripts\dbt docs serve

test:  # Run pytest unit tests (155+ tests)
	@echo ">> Running unit tests..."
	@.venv\Scripts\pytest tests\ -v --tb=short
	@echo ">> Tests complete."

coverage:  # Run pytest with coverage report
	@echo ">> Running unit tests with coverage..."
	@.venv\Scripts\pytest tests\ --cov=scripts/ --cov=src/ --cov-report=term-missing
	@echo ">> Coverage complete."

lint:  # Run flake8 linting on all Python files
	@echo ">> Running flake8 linter..."
	@.venv\Scripts\flake8 scripts\ airflow\ tests\ --max-line-length=100 --ignore=E203,E501,W503
	@echo ">> Linting complete."

format:  # Auto-format all Python code with black
	@echo ">> Formatting Python code with black..."
	@.venv\Scripts\black scripts\ airflow\ tests\
	@echo ">> Formatting complete."

lakehouse:  # Verify Lakehouse Parquet files in data/lakehouse/
	@echo ">> Checking Lakehouse Parquet files..."
	@if exist "data\lakehouse\*.parquet" (for %%f in (data\lakehouse\*.parquet) do @echo "  [OK]  %%~nxf") else (echo "  [WARN]  No Parquet files found in data/lakehouse/. Run 'make pipeline' first.")
	@echo ">> Lakehouse check complete."

clean-rejected:  # Remove DLQ files, schema drift quarantine, and outputs
	@echo ">> Cleaning rejected and quarantine directories..."
	@if exist "data\rejected\*.csv" del /q data\rejected\*.csv 2>nul
	@if exist "data\rejected_schemas\*.*" del /q data\rejected_schemas\*.* 2>nul
	@if exist "outputs\*.xlsx" del /q outputs\*.xlsx 2>nul
	@echo ">> Clean rejected complete."

clean:  # Remove generated data, Python cache, Docker volumes, and both virtual environments
	@echo ">> Cleaning generated data..."
	@if exist "data\raw\*.csv" del /q data\raw\*.csv 2>nul
	@if exist "data\processed\*.csv" del /q data\processed\*.csv 2>nul
	@echo ">> Removing Python virtual environments..."
	@if exist ".venv" rmdir /s /q .venv 2>nul
	@if exist ".venv-dbt" rmdir /s /q .venv-dbt 2>nul
	@echo ">> Removing Docker volumes..."
	@docker compose down -v 2>nul
	@echo ">> Clean complete."

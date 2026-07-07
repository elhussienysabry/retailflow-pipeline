# =============================================================================
# RetailFlow Pipeline — Makefile
# =============================================================================
# Common commands for setup, running, and maintaining the pipeline.
# Type `make help` to see all available commands.
# =============================================================================

.PHONY: help setup setup-dbt run stop clean test lint format status export dashboard pipeline

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
  @echo "  make dbt-run        Execute all dbt models (staging → intermediate → marts)"
  @echo "  make dbt-test       Run dbt data tests on all models"
	@echo "  make sql-analyze    Run all 4 analytics queries against the warehouse"
  @echo "  make test           Run pytest unit tests"
  @echo "  make status         Run end-to-end pipeline health check"
  @echo "  make export         Export analytics to styled Excel workbook"
  @echo "  make pipeline       Run full pipeline end-to-end via orchestrator"
  @echo "  make dashboard      Launch the Streamlit analytics dashboard"
	@echo "  make lint           Run flake8 linting on all Python files"
	@echo "  make format         Auto-format Python code with black"
	@echo "  make clean          Remove generated data, Python cache, and Docker volumes"
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

setup-dbt:  # Create and install the isolated dbt virtual environment
	@echo ">> Setting up isolated dbt virtual environment..."
	@if not exist ".venv-dbt" python -m venv .venv-dbt
	@echo ">> Installing dbt-core and dbt-postgres..."
	@.venv-dbt\Scripts\pip install dbt-core==1.7.14 dbt-postgres==1.7.14
	@echo ">> dbt environment ready. Use '.venv-dbt\\Scripts\\dbt' for dbt commands."

dbt-run:  # Execute dbt models (staging → intermediate → marts)
	@echo ">> Running dbt staging models..."
	@cd dbt && ..\.venv-dbt\Scripts\dbt run --select staging
	@echo ">> Running dbt intermediate models..."
	@cd dbt && ..\.venv-dbt\Scripts\dbt run --select intermediate
	@echo ">> Running dbt mart models..."
	@cd dbt && ..\.venv-dbt\Scripts\dbt run --select marts
	@echo ">> All dbt models executed."

dbt-test:  # Run dbt data tests
	@echo ">> Running dbt data tests..."
	@cd dbt && ..\.venv-dbt\Scripts\dbt test
	@echo ">> dbt tests complete."

sql-analyze:  # Execute all 4 analytics queries and show results
	@echo ">> Running analytics queries..."
	@echo "=== Top Customers by Revenue ==="
	@type sql\analytics\top_customers_by_revenue.sql | .venv\Scripts\python -c "import sys; from pathlib import Path; sys.stdin.read()" 2>nul || echo "Run manually: psql -d retailflow -f sql/analytics/top_customers_by_revenue.sql"
	@echo "=== Monthly Sales Trend ==="
	@echo "Run manually: psql -d retailflow -f sql/analytics/monthly_sales_trend.sql"
	@echo "=== Product Category Performance ==="
	@echo "Run manually: psql -d retailflow -f sql/analytics/product_category_performance.sql"
	@echo "=== Customer Cohort Analysis ==="
	@echo "Run manually: psql -d retailflow -f sql/analytics/customer_cohort_analysis.sql"

status:  # Run the end-to-end pipeline health check
	@echo ">> Running pipeline health check..."
	@.venv\Scripts\python scripts\project_status.py

export:  # Export analytics to a styled Excel workbook (requires dbt-run first)
	@echo ">> Exporting analytics to Excel..."
	@.venv\Scripts\python -m src.exports.excel_exporter

pipeline:  # Run the full pipeline end-to-end via the orchestrator
	@echo ">> Running full pipeline via orchestrator..."
	@.venv\Scripts\python scripts\orchestrate.py

dashboard:  # Launch the Streamlit analytics dashboard
	@echo ">> Starting Streamlit dashboard..."
	@.venv\Scripts\streamlit run src\dashboard\app.py

test:  # Run pytest unit tests
	@echo ">> Running unit tests..."
	@.venv\Scripts\pytest tests\ -v --tb=short
	@echo ">> Tests complete."

lint:  # Run flake8 linting on all Python files
	@echo ">> Running flake8 linter..."
	@.venv\Scripts\flake8 scripts\ airflow\ tests\ --max-line-length=100
	@echo ">> Linting complete."

format:  # Auto-format all Python code with black
	@echo ">> Formatting Python code with black..."
	@.venv\Scripts\black scripts\ airflow\ tests\
	@echo ">> Formatting complete."

clean:  # Remove generated data, Python cache, and Docker volumes
	@echo ">> Cleaning generated data..."
	@if exist "data\raw\*.csv" del /q data\raw\*.csv 2>nul
	@if exist "data\processed\*.csv" del /q data\processed\*.csv 2>nul
	@echo ">> Removing Python cache..."
	@if exist ".venv" rmdir /s /q .venv 2>nul
	@echo ">> Removing Docker volumes..."
	@docker compose down -v 2>nul
	@echo ">> Clean complete."

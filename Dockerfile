# =============================================================================
# RetailFlow Pipeline — Production-Grade Docker Image
# =============================================================================
# Multi-layer image for the `app` service that runs both the Streamlit
# dashboard and the full ETL orchestration pipeline.
#
# Two Python environments are used to isolate the dbt dependency chain
# (mashumaro conflict with Airflow / Great Expectations):
#   1. System Python — all main deps (pandas, streamlit, airflow, GE, …)
#   2. /opt/dbt-venv  — only dbt-core + dbt-postgres
#
# Usage:
#   docker build -t retailflow-app .
#   docker run --rm -p 8501:8501 retailflow-app
# =============================================================================

FROM python:3.12-slim AS builder

# ------------------------------------------------------------------
# Layer 1: System dependencies
# ------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------------------
# Layer 2: Core Python dependencies (cached via Docker layer)
# ------------------------------------------------------------------
WORKDIR /app
COPY requirements.txt .

# Install all dependencies EXCEPT dbt (handled separately below).
# Filtering avoids the mashumaro version conflict between dbt-core<4
# and apache-airflow / great_expectations which require mashumaro>=4.
RUN pip install --no-cache-dir \
    pandas \
    numpy \
    SQLAlchemy \
    psycopg2-binary \
    Faker \
    apache-airflow \
    great_expectations \
    pytest \
    pytest-cov \
    black \
    flake8 \
    pre-commit \
    python-dotenv \
    tqdm \
    openpyxl \
    streamlit \
    plotly \
    requests \
    networkx \
    matplotlib

# ------------------------------------------------------------------
# Layer 3: Isolated dbt environment
# ------------------------------------------------------------------
# dbt-core 1.7.x pins mashumaro<4.  Installing it in a separate venv
# prevents runtime breakage when Airflow / GE upgrade mashumaro in the
# global site-packages.  The binary is symlinked into PATH.
RUN python -m venv /opt/dbt-venv && \
    /opt/dbt-venv/bin/pip install --no-cache-dir \
        dbt-core==1.7.14 \
        dbt-postgres==1.7.14 && \
    ln -s /opt/dbt-venv/bin/dbt /usr/local/bin/dbt

# ------------------------------------------------------------------
# Layer 4: Application source code
# ------------------------------------------------------------------
COPY scripts/ scripts/
COPY src/ src/
COPY dbt/ dbt/
COPY tests/ tests/
COPY Makefile .

# Ensure data directories exist (content is gitignored)
RUN mkdir -p data/raw data/rejected outputs

# ------------------------------------------------------------------
# Runtime configuration
# ------------------------------------------------------------------

# dbt executable override for the orchestrator (see _dbt_exe())
ENV DBT_EXECUTABLE=/opt/dbt-venv/bin/dbt

# Streamlit config
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_HEADLESS=true

EXPOSE 8501

# Default: launch the Streamlit analytics dashboard
CMD ["streamlit", "run", "src/dashboard/app.py", \
     "--server.port=8501", "--server.address=0.0.0.0"]

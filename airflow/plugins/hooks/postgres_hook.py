"""
RetailFlow Pipeline — Custom PostgreSQL Hook for Airflow
=========================================================

A thin wrapper around SQLAlchemy for use in Airflow DAG tasks.

This hook provides:
    - Connection management using environment variables
    - A context manager for safe connection lifecycle
    - Helper methods for common database operations

Usage:
    from airflow.plugins.hooks.postgres_hook import RetailFlowPostgresHook

    hook = RetailFlowPostgresHook()
    with hook.get_conn() as conn:
        result = conn.execute("SELECT count(*) FROM raw.orders")
"""

import logging
import os
from contextlib import contextmanager
from typing import Any, Generator, List, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection

logger = logging.getLogger(__name__)


class RetailFlowPostgresHook:
    """Custom PostgreSQL hook for the RetailFlow Pipeline.

    Reads database connection parameters from environment variables
    (which Airflow can source from .env or Docker secrets).
    """

    def __init__(self) -> None:
        self._engine: Engine | None = None

    def _get_engine(self) -> Engine:
        """Lazily create and cache a SQLAlchemy Engine.

        Returns:
            A SQLAlchemy Engine instance.
        """
        if self._engine is None:
            host = os.getenv("POSTGRES_HOST", "postgres")
            port = os.getenv("POSTGRES_PORT", "5432")
            database = os.getenv("POSTGRES_DB", "retailflow")
            user = os.getenv("POSTGRES_USER", "retailflow_user")
            password = os.getenv("POSTGRES_PASSWORD", "retailflow_pass")

            connection_string = (
                f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
            )
            self._engine = create_engine(connection_string, echo=False)
            logger.debug("Created PostgreSQL engine for %s@%s:%s", user, host, port)

        return self._engine

    @contextmanager
    def get_conn(self) -> Generator[Connection, None, None]:
        """Provide a transactional database connection as a context manager.

        Yields:
            A SQLAlchemy Connection that auto-commits on exit.

        Example:
            with hook.get_conn() as conn:
                conn.execute("SELECT 1")
        """
        engine = self._get_engine()
        with engine.connect() as conn:
            yield conn
            conn.commit()

    def fetch_all(self, query: str, params: dict[str, Any] | None = None) -> List[Tuple[Any, ...]]:
        """Execute a query and return all result rows.

        Args:
            query: SQL query string (may contain :param style placeholders).
            params: Optional dictionary of query parameters.

        Returns:
            A list of row tuples.
        """
        with self.get_conn() as conn:
            result = conn.execute(text(query), parameters=params or {})
            return result.fetchall()

    def execute(self, query: str, params: dict[str, Any] | None = None) -> None:
        """Execute a SQL statement (DDL or DML).

        Args:
            query: SQL statement string.
            params: Optional dictionary of query parameters.
        """
        with self.get_conn() as conn:
            conn.execute(text(query), parameters=params or {})

    def table_exists(self, schema: str, table: str) -> bool:
        """Check if a table exists in the database.

        Args:
            schema: Schema name.
            table: Table name.

        Returns:
            True if the table exists, False otherwise.
        """
        query = """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = :schema AND table_name = :table
            )
        """
        result = self.fetch_all(query, {"schema": schema, "table": table})
        return bool(result[0][0]) if result else False

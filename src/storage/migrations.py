"""Simple migration support for the AG Enforcement Tracker.

For this project, we use SQLAlchemy's create_all() for initial setup.
This module provides a lightweight migration mechanism for schema changes
after the initial deployment, without requiring Alembic.
"""

from __future__ import annotations

import logging

from sqlalchemy import inspect, text

from src.storage.models import Base

logger = logging.getLogger(__name__)


def check_schema(engine) -> list[str]:
    """Compare existing database schema against the ORM models.

    Returns a list of issues found (empty list means schema is up to date).
    """
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    expected_tables = set(Base.metadata.tables.keys())

    issues = []
    missing = expected_tables - existing_tables
    for table in sorted(missing):
        issues.append(f"Missing table: {table}")

    for table in sorted(expected_tables & existing_tables):
        existing_cols = {c["name"] for c in inspector.get_columns(table)}
        expected_cols = {c.name for c in Base.metadata.tables[table].columns}
        for col in sorted(expected_cols - existing_cols):
            issues.append(f"Missing column: {table}.{col}")

    return issues


def migrate(engine) -> None:
    """Apply any missing tables or columns.

    This is a simple additive migration — it can add tables and columns
    but does not handle column type changes or deletions.
    """
    issues = check_schema(engine)
    if not issues:
        logger.info("Schema is up to date.")
        return

    logger.info("Found %d schema issues, applying migrations...", len(issues))

    # Create any missing tables
    Base.metadata.create_all(engine)

    # For missing columns, add them with ALTER TABLE
    inspector = inspect(engine)
    for issue in issues:
        if issue.startswith("Missing column:"):
            table_col = issue.replace("Missing column: ", "")
            table, col = table_col.split(".")
            sa_col = Base.metadata.tables[table].columns[col]
            col_type = sa_col.type.compile(engine.dialect)
            nullable = "NULL" if sa_col.nullable else "NOT NULL"
            default = ""
            if sa_col.default is not None:
                default = f" DEFAULT {sa_col.default.arg!r}"
            sql = f"ALTER TABLE {table} ADD COLUMN {col} {col_type} {nullable}{default}"
            with engine.begin() as conn:
                conn.execute(text(sql))
            logger.info("Added column: %s.%s", table, col)

    logger.info("Migration complete.")

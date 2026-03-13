"""
Database Layer
SQLAlchemy-based connection supporting SQLite (default) and PostgreSQL.
Provides bulk insert methods with idempotent (ON CONFLICT skip) behavior.
"""

import os
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker


# Database URL — defaults to SQLite; set DATABASE_URL env var for PostgreSQL
# PostgreSQL example: postgresql://user:password@localhost:5432/billing_db
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///billing_migration.db")


engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)


def init_db():
    """Initialize database schema from schema.sql file."""
    schema_path = os.path.join(os.path.dirname(__file__), "..", "database", "schema.sql")
    
    with open(schema_path, "r") as f:
        schema_sql = f.read()

    with engine.begin() as conn:
        # SQLite doesn't support multiple statements in execute — split them
        statements = [s.strip() for s in schema_sql.split(";") if s.strip()]
        for stmt in statements:
            try:
                conn.execute(text(stmt))
            except Exception as e:
                print(f"  Warning: {e}")


def get_db():
    """Dependency for FastAPI — yields a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_table_counts() -> dict:
    """Get row counts for all tables."""
    tables = ["accounts", "subscriptions", "churn_events", "feature_usage", "support_tickets"]
    counts = {}
    with engine.connect() as conn:
        for table in tables:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                counts[table] = result.scalar()
            except Exception:
                counts[table] = 0
    return counts


def bulk_insert(table_name: str, records: list[dict]) -> dict:
    """
    Insert records into a table, skipping duplicates.
    
    Returns:
        {"inserted": int, "skipped": int, "errors": list[str]}
    """
    if not records:
        return {"inserted": 0, "skipped": 0, "errors": []}

    inserted = 0
    skipped = 0
    errors = []

    # Get the primary key column name
    pk_map = {
        "accounts": "account_id",
        "subscriptions": "subscription_id",
        "churn_events": "churn_event_id",
        "feature_usage": "usage_id",
        "support_tickets": "ticket_id",
    }
    pk_col = pk_map.get(table_name)

    with engine.begin() as conn:
        # First, get existing PKs to skip duplicates
        existing_pks = set()
        if pk_col:
            try:
                result = conn.execute(text(f"SELECT {pk_col} FROM {table_name}"))
                existing_pks = {row[0] for row in result}
            except Exception:
                pass

        columns = list(records[0].keys())
        placeholders = ", ".join([f":{col}" for col in columns])
        col_names = ", ".join(columns)
        insert_sql = text(f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})")

        for record in records:
            try:
                # Skip if PK already exists (idempotent)
                if pk_col and record.get(pk_col) in existing_pks:
                    skipped += 1
                    continue

                conn.execute(insert_sql, record)
                inserted += 1
            except Exception as e:
                skipped += 1
                error_msg = str(e).split("\n")[0][:200]
                if len(errors) < 10:  # Cap error messages
                    errors.append(f"Row {record.get(pk_col, '?')}: {error_msg}")

    return {"inserted": inserted, "skipped": skipped, "errors": errors}


def clear_all_tables():
    """Clear all data from all tables (for re-migration)."""
    tables = ["feature_usage", "support_tickets", "churn_events", "subscriptions", "accounts"]
    with engine.begin() as conn:
        for table in tables:
            try:
                conn.execute(text(f"DELETE FROM {table}"))
            except Exception:
                pass

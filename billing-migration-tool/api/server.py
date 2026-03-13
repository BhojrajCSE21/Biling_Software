"""
FastAPI Server — Billing Data Migration API
Provides REST endpoints for importing billing data and checking migration stats.
"""

from fastapi import FastAPI, HTTPException
from api.models import (
    AccountIn, SubscriptionIn, ChurnEventIn,
    FeatureUsageIn, SupportTicketIn,
    ImportResult, MigrationStats, HealthResponse,
)
from api.database import init_db, bulk_insert, get_table_counts

app = FastAPI(
    title="Billing Data Migration API",
    description="REST API for migrating SaaS billing data into a structured database.",
    version="1.0.0",
)


@app.on_event("startup")
def startup():
    """Initialize the database on server start."""
    init_db()
    print("✅ Database initialized")


# ============================================================
# Health Check
# ============================================================

@app.get("/api/health", response_model=HealthResponse)
def health_check():
    try:
        counts = get_table_counts()
        return HealthResponse(status="healthy", database="connected", version="1.0.0")
    except Exception as e:
        return HealthResponse(status="unhealthy", database=str(e), version="1.0.0")


# ============================================================
# Import Endpoints
# ============================================================

@app.post("/api/import/accounts", response_model=ImportResult)
def import_accounts(records: list[AccountIn]):
    """Import account records into the database."""
    data = [r.model_dump() for r in records]
    # Convert date objects to strings for SQLite compatibility
    for d in data:
        d["signup_date"] = str(d["signup_date"])
    
    result = bulk_insert("accounts", data)
    total = len(records)
    status = "success" if result["inserted"] == total else ("partial" if result["inserted"] > 0 else "failed")
    
    return ImportResult(
        table="accounts",
        total_records=total,
        imported=result["inserted"],
        skipped=result["skipped"],
        errors=result["errors"],
        status=status,
    )


@app.post("/api/import/subscriptions", response_model=ImportResult)
def import_subscriptions(records: list[SubscriptionIn]):
    """Import subscription records into the database."""
    data = [r.model_dump() for r in records]
    for d in data:
        d["start_date"] = str(d["start_date"])
        d["end_date"] = str(d["end_date"]) if d["end_date"] else None
    
    result = bulk_insert("subscriptions", data)
    total = len(records)
    status = "success" if result["inserted"] == total else ("partial" if result["inserted"] > 0 else "failed")
    
    return ImportResult(
        table="subscriptions",
        total_records=total,
        imported=result["inserted"],
        skipped=result["skipped"],
        errors=result["errors"],
        status=status,
    )


@app.post("/api/import/churn-events", response_model=ImportResult)
def import_churn_events(records: list[ChurnEventIn]):
    """Import churn event records into the database."""
    data = [r.model_dump() for r in records]
    for d in data:
        d["churn_date"] = str(d["churn_date"])
    
    result = bulk_insert("churn_events", data)
    total = len(records)
    status = "success" if result["inserted"] == total else ("partial" if result["inserted"] > 0 else "failed")
    
    return ImportResult(
        table="churn_events",
        total_records=total,
        imported=result["inserted"],
        skipped=result["skipped"],
        errors=result["errors"],
        status=status,
    )


@app.post("/api/import/feature-usage", response_model=ImportResult)
def import_feature_usage(records: list[FeatureUsageIn]):
    """Import feature usage records into the database."""
    data = [r.model_dump() for r in records]
    for d in data:
        d["usage_date"] = str(d["usage_date"])
    
    result = bulk_insert("feature_usage", data)
    total = len(records)
    status = "success" if result["inserted"] == total else ("partial" if result["inserted"] > 0 else "failed")
    
    return ImportResult(
        table="feature_usage",
        total_records=total,
        imported=result["inserted"],
        skipped=result["skipped"],
        errors=result["errors"],
        status=status,
    )


@app.post("/api/import/support-tickets", response_model=ImportResult)
def import_support_tickets(records: list[SupportTicketIn]):
    """Import support ticket records into the database."""
    data = [r.model_dump() for r in records]
    for d in data:
        d["submitted_at"] = str(d["submitted_at"])
        d["closed_at"] = str(d["closed_at"]) if d["closed_at"] else None
    
    result = bulk_insert("support_tickets", data)
    total = len(records)
    status = "success" if result["inserted"] == total else ("partial" if result["inserted"] > 0 else "failed")
    
    return ImportResult(
        table="support_tickets",
        total_records=total,
        imported=result["inserted"],
        skipped=result["skipped"],
        errors=result["errors"],
        status=status,
    )


# ============================================================
# Stats
# ============================================================

@app.get("/api/stats", response_model=MigrationStats)
def get_stats():
    """Get current record counts for all tables."""
    counts = get_table_counts()
    return MigrationStats(**counts)

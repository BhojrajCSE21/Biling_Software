"""
Pydantic models for billing data entities.
Used for API request/response validation.
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import date, datetime


# ============================================================
# Request Models (for API import endpoints)
# ============================================================

class AccountIn(BaseModel):
    account_id: str
    account_name: str
    industry: str
    country: str
    signup_date: date
    referral_source: Optional[str] = None
    plan_tier: str
    seats: int
    is_trial: bool = False
    churn_flag: bool = False


class SubscriptionIn(BaseModel):
    subscription_id: str
    account_id: str
    start_date: date
    end_date: Optional[date] = None
    plan_tier: str
    seats: int
    mrr_amount: float = 0
    arr_amount: float = 0
    is_trial: bool = False
    upgrade_flag: bool = False
    downgrade_flag: bool = False
    churn_flag: bool = False
    billing_frequency: str
    auto_renew_flag: bool = True


class ChurnEventIn(BaseModel):
    churn_event_id: str
    account_id: str
    churn_date: date
    reason_code: str
    refund_amount_usd: float = 0
    preceding_upgrade_flag: bool = False
    preceding_downgrade_flag: bool = False
    is_reactivation: bool = False
    feedback_text: Optional[str] = None


class FeatureUsageIn(BaseModel):
    usage_id: str
    subscription_id: str
    usage_date: date
    feature_name: str
    usage_count: int
    usage_duration_secs: int
    error_count: int = 0
    is_beta_feature: bool = False


class SupportTicketIn(BaseModel):
    ticket_id: str
    account_id: str
    submitted_at: datetime
    closed_at: Optional[datetime] = None
    resolution_time_hours: Optional[float] = None
    priority: str
    first_response_time_minutes: Optional[int] = None
    satisfaction_score: Optional[float] = None
    escalation_flag: bool = False


# ============================================================
# Response Models
# ============================================================

class ImportResult(BaseModel):
    """Response returned after an import operation."""
    table: str
    total_records: int
    imported: int
    skipped: int
    errors: list[str] = Field(default_factory=list)
    status: str  # "success", "partial", "failed"


class MigrationStats(BaseModel):
    """Overall migration statistics."""
    accounts: int = 0
    subscriptions: int = 0
    churn_events: int = 0
    feature_usage: int = 0
    support_tickets: int = 0


class HealthResponse(BaseModel):
    status: str
    database: str
    version: str = "1.0.0"

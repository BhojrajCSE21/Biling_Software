-- Billing Data Migration Schema
-- Supports both PostgreSQL and SQLite

-- ============================================================
-- ACCOUNTS — Core customer/company records
-- ============================================================
CREATE TABLE IF NOT EXISTS accounts (
    account_id      VARCHAR(20) PRIMARY KEY,
    account_name    VARCHAR(100) NOT NULL,
    industry        VARCHAR(50) NOT NULL,
    country         VARCHAR(10) NOT NULL,
    signup_date     DATE NOT NULL,
    referral_source VARCHAR(50),
    plan_tier       VARCHAR(20) NOT NULL CHECK (plan_tier IN ('Basic', 'Pro', 'Enterprise')),
    seats           INTEGER NOT NULL CHECK (seats > 0),
    is_trial        BOOLEAN NOT NULL DEFAULT FALSE,
    churn_flag      BOOLEAN NOT NULL DEFAULT FALSE
);

-- ============================================================
-- SUBSCRIPTIONS — Billing subscriptions per account
-- ============================================================
CREATE TABLE IF NOT EXISTS subscriptions (
    subscription_id   VARCHAR(20) PRIMARY KEY,
    account_id        VARCHAR(20) NOT NULL REFERENCES accounts(account_id),
    start_date        DATE NOT NULL,
    end_date          DATE,  -- NULL means active subscription
    plan_tier         VARCHAR(20) NOT NULL CHECK (plan_tier IN ('Basic', 'Pro', 'Enterprise')),
    seats             INTEGER NOT NULL CHECK (seats > 0),
    mrr_amount        DECIMAL(12,2) NOT NULL DEFAULT 0,
    arr_amount        DECIMAL(12,2) NOT NULL DEFAULT 0,
    is_trial          BOOLEAN NOT NULL DEFAULT FALSE,
    upgrade_flag      BOOLEAN NOT NULL DEFAULT FALSE,
    downgrade_flag    BOOLEAN NOT NULL DEFAULT FALSE,
    churn_flag        BOOLEAN NOT NULL DEFAULT FALSE,
    billing_frequency VARCHAR(20) NOT NULL CHECK (billing_frequency IN ('monthly', 'annual')),
    auto_renew_flag   BOOLEAN NOT NULL DEFAULT TRUE
);

-- ============================================================
-- CHURN EVENTS — Cancellation/churn records
-- ============================================================
CREATE TABLE IF NOT EXISTS churn_events (
    churn_event_id          VARCHAR(20) PRIMARY KEY,
    account_id              VARCHAR(20) NOT NULL REFERENCES accounts(account_id),
    churn_date              DATE NOT NULL,
    reason_code             VARCHAR(50) NOT NULL,
    refund_amount_usd       DECIMAL(10,2) NOT NULL DEFAULT 0,
    preceding_upgrade_flag  BOOLEAN NOT NULL DEFAULT FALSE,
    preceding_downgrade_flag BOOLEAN NOT NULL DEFAULT FALSE,
    is_reactivation         BOOLEAN NOT NULL DEFAULT FALSE,
    feedback_text           TEXT
);

-- ============================================================
-- FEATURE USAGE — Per-subscription feature usage logs
-- ============================================================
CREATE TABLE IF NOT EXISTS feature_usage (
    usage_id            VARCHAR(20) PRIMARY KEY,
    subscription_id     VARCHAR(20) NOT NULL REFERENCES subscriptions(subscription_id),
    usage_date          DATE NOT NULL,
    feature_name        VARCHAR(50) NOT NULL,
    usage_count         INTEGER NOT NULL CHECK (usage_count >= 0),
    usage_duration_secs INTEGER NOT NULL CHECK (usage_duration_secs >= 0),
    error_count         INTEGER NOT NULL DEFAULT 0,
    is_beta_feature     BOOLEAN NOT NULL DEFAULT FALSE
);

-- ============================================================
-- SUPPORT TICKETS — Customer support interactions
-- ============================================================
CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id                   VARCHAR(20) PRIMARY KEY,
    account_id                  VARCHAR(20) NOT NULL REFERENCES accounts(account_id),
    submitted_at                TIMESTAMP NOT NULL,
    closed_at                   TIMESTAMP,
    resolution_time_hours       DECIMAL(8,2),
    priority                    VARCHAR(20) NOT NULL CHECK (priority IN ('low', 'medium', 'high', 'urgent')),
    first_response_time_minutes INTEGER,
    satisfaction_score          DECIMAL(3,1),
    escalation_flag             BOOLEAN NOT NULL DEFAULT FALSE
);

-- ============================================================
-- INDEXES for query performance
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_subscriptions_account ON subscriptions(account_id);
CREATE INDEX IF NOT EXISTS idx_churn_events_account ON churn_events(account_id);
CREATE INDEX IF NOT EXISTS idx_feature_usage_subscription ON feature_usage(subscription_id);
CREATE INDEX IF NOT EXISTS idx_support_tickets_account ON support_tickets(account_id);
CREATE INDEX IF NOT EXISTS idx_accounts_plan_tier ON accounts(plan_tier);
CREATE INDEX IF NOT EXISTS idx_subscriptions_plan_tier ON subscriptions(plan_tier);

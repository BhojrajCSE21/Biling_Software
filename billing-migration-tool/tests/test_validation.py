"""
Unit tests for data validation and cleaning logic.
"""

import sys
import os
import pytest
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.clean_data import DataCleaner
from scripts.validate_data import DataValidator


# ============================================================
# Test Data Fixtures
# ============================================================

@pytest.fixture
def cleaner():
    return DataCleaner()


@pytest.fixture
def validator():
    return DataValidator()


@pytest.fixture
def sample_accounts():
    return pd.DataFrame({
        "account_id": ["A-aaa111", "A-bbb222", "A-ccc333"],
        "account_name": ["Company A", "Company B ", "Company C"],  # B has trailing space
        "industry": ["FinTech", "EdTech", "DevTools"],
        "country": ["us", "IN", "uk"],  # us and uk are lowercase
        "signup_date": ["2024-01-15", "2024-02-20", "invalid-date"],
        "referral_source": ["organic", "ads", "partner"],
        "plan_tier": ["basic", "Pro", "ENTERPRISE"],  # Mixed casing
        "seats": [5, 10, 20],
        "is_trial": [False, True, False],
        "churn_flag": [False, False, True],
    })


@pytest.fixture
def sample_subscriptions():
    return pd.DataFrame({
        "subscription_id": ["S-aaa111", "S-bbb222", "S-ccc333"],
        "account_id": ["A-aaa111", "A-bbb222", "A-ccc333"],
        "start_date": ["2024-01-15", "2024-02-20", "2024-03-10"],
        "end_date": [None, "2024-06-20", None],
        "plan_tier": ["Basic", "Pro", "Enterprise"],
        "seats": [5, 10, 20],
        "mrr_amount": [100, 200, 0],
        "arr_amount": [1200, 2400, 0],
        "is_trial": [False, False, True],
        "upgrade_flag": [False, True, False],
        "downgrade_flag": [False, False, False],
        "churn_flag": [False, True, False],
        "billing_frequency": ["monthly", "annual", "monthly"],
        "auto_renew_flag": [True, True, False],
    })


@pytest.fixture
def sample_feature_usage_with_dupes():
    return pd.DataFrame({
        "usage_id": ["U-aaa111", "U-bbb222", "U-aaa111"],  # Duplicate!
        "subscription_id": ["S-aaa111", "S-bbb222", "S-aaa111"],
        "usage_date": ["2024-01-15", "2024-02-20", "2024-01-16"],
        "feature_name": ["feature_1", "feature_2", "feature_1"],
        "usage_count": [10, 5, 8],
        "usage_duration_secs": [300, 150, 200],
        "error_count": [0, 1, 0],
        "is_beta_feature": [False, True, False],
    })


@pytest.fixture
def sample_churn_events():
    return pd.DataFrame({
        "churn_event_id": ["C-aaa111", "C-bbb222"],
        "account_id": ["A-ccc333", "A-ddd444"],  # ddd444 doesn't exist in accounts
        "churn_date": ["2024-06-01", "2024-07-15"],
        "reason_code": ["pricing", "features"],
        "refund_amount_usd": [50.0, 0.0],
        "preceding_upgrade_flag": [False, True],
        "preceding_downgrade_flag": [False, False],
        "is_reactivation": [False, False],
        "feedback_text": ["too expensive", None],
    })


# ============================================================
# Cleaning Tests
# ============================================================

class TestDataCleaner:

    def test_clean_accounts_normalizes_country(self, cleaner, sample_accounts):
        cleaned, report = cleaner.clean_accounts(sample_accounts)
        assert all(c.isupper() for c in cleaned["country"])

    def test_clean_accounts_normalizes_plan_tier(self, cleaner, sample_accounts):
        cleaned, report = cleaner.clean_accounts(sample_accounts)
        assert set(cleaned["plan_tier"]).issubset({"Basic", "Pro", "Enterprise"})

    def test_clean_accounts_trims_whitespace(self, cleaner, sample_accounts):
        cleaned, report = cleaner.clean_accounts(sample_accounts)
        assert cleaned["account_name"].iloc[1] == "Company B"  # Trimmed

    def test_clean_feature_usage_removes_duplicates(self, cleaner, sample_feature_usage_with_dupes):
        cleaned, report = cleaner.clean_feature_usage(sample_feature_usage_with_dupes)
        assert len(cleaned) == 2  # 3 rows → 2 (1 duplicate removed)
        assert cleaned["usage_id"].duplicated().sum() == 0

    def test_clean_churn_fills_missing_feedback(self, cleaner, sample_churn_events):
        cleaned, report = cleaner.clean_churn_events(sample_churn_events)
        assert cleaned["feedback_text"].isna().sum() == 0
        assert cleaned["feedback_text"].iloc[1] == "No feedback provided"


# ============================================================
# Validation Tests
# ============================================================

class TestDataValidator:

    def test_schema_validation_accounts_valid(self, validator, sample_accounts):
        is_valid, errors, warnings = validator.validate_schema_accounts(sample_accounts)
        assert is_valid is True
        assert len(errors) == 0

    def test_schema_validation_detects_duplicates(self, validator, sample_feature_usage_with_dupes):
        is_valid, errors, warnings = validator.validate_schema_feature_usage(
            sample_feature_usage_with_dupes
        )
        assert is_valid is False
        assert any("duplicate" in e.lower() for e in errors)

    def test_business_rules_invalid_plan_tier(self, validator):
        df = pd.DataFrame({
            "account_id": ["A-aaa111"],
            "account_name": ["Test"],
            "industry": ["FinTech"],
            "country": ["US"],
            "signup_date": ["2024-01-01"],
            "referral_source": ["organic"],
            "plan_tier": ["SuperPlan"],  # Invalid!
            "seats": [5],
            "is_trial": [False],
            "churn_flag": [False],
        })
        is_valid, errors, warnings = validator.validate_business_rules_accounts(df)
        assert is_valid is False
        assert any("plan_tier" in e.lower() for e in errors)

    def test_cross_table_detects_orphan_churn(self, validator, sample_accounts, sample_churn_events):
        dataframes = {
            "accounts": sample_accounts,
            "churn_events": sample_churn_events,
        }
        is_valid, errors, warnings = validator.validate_cross_table(dataframes)
        # A-ddd444 in churn doesn't exist in accounts
        assert is_valid is False
        assert any("churn event account_ids not found" in e.lower() for e in errors)

    def test_cross_table_detects_churn_flag_inconsistency(self, validator):
        accounts = pd.DataFrame({
            "account_id": ["A-aaa111", "A-bbb222"],
            "account_name": ["Co A", "Co B"],
            "industry": ["FinTech", "EdTech"],
            "country": ["US", "IN"],
            "signup_date": ["2024-01-01", "2024-02-01"],
            "referral_source": ["organic", "ads"],
            "plan_tier": ["Basic", "Pro"],
            "seats": [5, 10],
            "is_trial": [False, False],
            "churn_flag": [True, False],  # A-aaa111 flagged as churned
        })
        churn = pd.DataFrame({
            "churn_event_id": ["C-xxx111"],
            "account_id": ["A-bbb222"],  # Event for B, but B not flagged
            "churn_date": ["2024-06-01"],
            "reason_code": ["pricing"],
            "refund_amount_usd": [0],
            "preceding_upgrade_flag": [False],
            "preceding_downgrade_flag": [False],
            "is_reactivation": [False],
            "feedback_text": ["too expensive"],
        })
        
        dataframes = {"accounts": accounts, "churn_events": churn}
        _, errors, warnings = validator.validate_cross_table(dataframes)
        
        # Should warn about A-aaa111 (flagged but no event) and A-bbb222 (event but not flagged)
        assert len(warnings) >= 2
        assert any("churn_flag=True but no churn event" in w for w in warnings)
        assert any("churn events but churn_flag=False" in w for w in warnings)

    def test_validate_all_returns_structured_result(self, validator, sample_accounts, sample_subscriptions):
        dataframes = {
            "accounts": sample_accounts,
            "subscriptions": sample_subscriptions,
        }
        result = validator.validate_all(dataframes)
        
        assert "is_valid" in result
        assert "total_errors" in result
        assert "total_warnings" in result
        assert "details" in result
        assert isinstance(result["details"], list)

"""
Data Validation Module
Three-level validation: Schema, Business Rules, Cross-Table Integrity.
Each validation returns (is_valid, errors, warnings).
"""

import pandas as pd
import re
from datetime import datetime


class DataValidator:
    """Validates billing data for schema correctness, business rules, and cross-table integrity."""

    VALID_PLAN_TIERS = {"Basic", "Pro", "Enterprise"}
    VALID_BILLING_FREQ = {"monthly", "annual"}
    VALID_PRIORITIES = {"low", "medium", "high", "urgent"}
    VALID_REASON_CODES = {"budget", "competitor", "features", "pricing", "support", "unknown"}
    VALID_COUNTRIES = {"US", "UK", "IN", "CA", "DE", "FR", "AU"}
    VALID_INDUSTRIES = {"EdTech", "FinTech", "DevTools", "HealthTech", "Cybersecurity"}

    ID_PATTERNS = {
        "account_id": r"^A-[a-f0-9]{6}$",
        "subscription_id": r"^S-[a-f0-9]{6}$",
        "churn_event_id": r"^C-[a-f0-9]{6}$",
        "usage_id": r"^U-[a-f0-9]{6}$",
        "ticket_id": r"^T-[a-f0-9]{6}$",
    }

    # ============================================================
    # LEVEL 1 — Schema Validation
    # ============================================================

    def validate_schema_accounts(self, df: pd.DataFrame) -> tuple[bool, list, list]:
        errors, warnings = [], []
        required = ["account_id", "account_name", "industry", "country", "signup_date", "plan_tier", "seats"]

        for col in required:
            nulls = df[col].isna().sum()
            if nulls > 0:
                errors.append(f"[ACCOUNTS] Required field '{col}' has {nulls} null values")

        # ID format check
        bad_ids = df[~df["account_id"].astype(str).str.match(self.ID_PATTERNS["account_id"])].shape[0]
        if bad_ids > 0:
            warnings.append(f"[ACCOUNTS] {bad_ids} account_ids don't match expected format A-xxxxxx")

        # Seats must be positive
        neg_seats = (df["seats"] <= 0).sum()
        if neg_seats > 0:
            errors.append(f"[ACCOUNTS] {neg_seats} rows have seats <= 0")

        return len(errors) == 0, errors, warnings

    def validate_schema_subscriptions(self, df: pd.DataFrame) -> tuple[bool, list, list]:
        errors, warnings = [], []
        required = ["subscription_id", "account_id", "start_date", "plan_tier", "seats",
                     "mrr_amount", "arr_amount", "billing_frequency"]

        for col in required:
            nulls = df[col].isna().sum()
            if nulls > 0:
                errors.append(f"[SUBSCRIPTIONS] Required field '{col}' has {nulls} null values")

        bad_ids = df[~df["subscription_id"].astype(str).str.match(self.ID_PATTERNS["subscription_id"])].shape[0]
        if bad_ids > 0:
            warnings.append(f"[SUBSCRIPTIONS] {bad_ids} subscription_ids don't match expected format S-xxxxxx")

        return len(errors) == 0, errors, warnings

    def validate_schema_churn_events(self, df: pd.DataFrame) -> tuple[bool, list, list]:
        errors, warnings = [], []
        required = ["churn_event_id", "account_id", "churn_date", "reason_code", "refund_amount_usd"]

        for col in required:
            nulls = df[col].isna().sum()
            if nulls > 0:
                errors.append(f"[CHURN_EVENTS] Required field '{col}' has {nulls} null values")

        return len(errors) == 0, errors, warnings

    def validate_schema_feature_usage(self, df: pd.DataFrame) -> tuple[bool, list, list]:
        errors, warnings = [], []
        required = ["usage_id", "subscription_id", "usage_date", "feature_name", "usage_count", "usage_duration_secs"]

        for col in required:
            nulls = df[col].isna().sum()
            if nulls > 0:
                errors.append(f"[FEATURE_USAGE] Required field '{col}' has {nulls} null values")

        # Check for duplicates
        dupes = df.duplicated(subset=["usage_id"]).sum()
        if dupes > 0:
            errors.append(f"[FEATURE_USAGE] {dupes} duplicate usage_id values found")

        return len(errors) == 0, errors, warnings

    def validate_schema_support_tickets(self, df: pd.DataFrame) -> tuple[bool, list, list]:
        errors, warnings = [], []
        required = ["ticket_id", "account_id", "submitted_at", "priority"]

        for col in required:
            nulls = df[col].isna().sum()
            if nulls > 0:
                errors.append(f"[SUPPORT_TICKETS] Required field '{col}' has {nulls} null values")

        return len(errors) == 0, errors, warnings

    # ============================================================
    # LEVEL 2 — Business Rule Validation
    # ============================================================

    def validate_business_rules_accounts(self, df: pd.DataFrame) -> tuple[bool, list, list]:
        errors, warnings = [], []

        # Plan tier must be valid
        invalid_tiers = df[~df["plan_tier"].isin(self.VALID_PLAN_TIERS)]["plan_tier"].unique()
        if len(invalid_tiers) > 0:
            errors.append(f"[ACCOUNTS] Invalid plan_tiers: {list(invalid_tiers)}")

        # Country must be valid
        invalid_countries = df[~df["country"].isin(self.VALID_COUNTRIES)]["country"].unique()
        if len(invalid_countries) > 0:
            warnings.append(f"[ACCOUNTS] Unexpected countries: {list(invalid_countries)}")

        # Industry check
        invalid_industries = df[~df["industry"].isin(self.VALID_INDUSTRIES)]["industry"].unique()
        if len(invalid_industries) > 0:
            warnings.append(f"[ACCOUNTS] Unexpected industries: {list(invalid_industries)}")

        return len(errors) == 0, errors, warnings

    def validate_business_rules_subscriptions(self, df: pd.DataFrame) -> tuple[bool, list, list]:
        errors, warnings = [], []

        # Plan tier
        invalid_tiers = df[~df["plan_tier"].isin(self.VALID_PLAN_TIERS)]["plan_tier"].unique()
        if len(invalid_tiers) > 0:
            errors.append(f"[SUBSCRIPTIONS] Invalid plan_tiers: {list(invalid_tiers)}")

        # Billing frequency
        invalid_freq = df[~df["billing_frequency"].isin(self.VALID_BILLING_FREQ)]["billing_frequency"].unique()
        if len(invalid_freq) > 0:
            errors.append(f"[SUBSCRIPTIONS] Invalid billing_frequency: {list(invalid_freq)}")

        # MRR/ARR relationship: ARR should be approximately MRR * 12
        non_trial = df[df["is_trial"] == False].copy()
        if len(non_trial) > 0:
            non_trial["expected_arr"] = non_trial["mrr_amount"] * 12
            mismatch = non_trial[abs(non_trial["arr_amount"] - non_trial["expected_arr"]) > 1]
            if len(mismatch) > 0:
                warnings.append(f"[SUBSCRIPTIONS] {len(mismatch)} rows where ARR != MRR * 12")

        # Trials should have MRR = 0
        trial_with_mrr = df[(df["is_trial"] == True) & (df["mrr_amount"] > 0)]
        if len(trial_with_mrr) > 0:
            errors.append(f"[SUBSCRIPTIONS] {len(trial_with_mrr)} trial subscriptions with MRR > 0")

        # MRR/ARR non-negative
        neg_mrr = (df["mrr_amount"] < 0).sum()
        neg_arr = (df["arr_amount"] < 0).sum()
        if neg_mrr > 0:
            errors.append(f"[SUBSCRIPTIONS] {neg_mrr} rows with negative MRR")
        if neg_arr > 0:
            errors.append(f"[SUBSCRIPTIONS] {neg_arr} rows with negative ARR")

        return len(errors) == 0, errors, warnings

    def validate_business_rules_churn_events(self, df: pd.DataFrame) -> tuple[bool, list, list]:
        errors, warnings = [], []

        # Reason code check
        invalid_codes = df[~df["reason_code"].isin(self.VALID_REASON_CODES)]["reason_code"].unique()
        if len(invalid_codes) > 0:
            warnings.append(f"[CHURN_EVENTS] Unexpected reason_codes: {list(invalid_codes)}")

        # Refund non-negative
        neg_refund = (df["refund_amount_usd"] < 0).sum()
        if neg_refund > 0:
            errors.append(f"[CHURN_EVENTS] {neg_refund} rows with negative refund_amount_usd")

        return len(errors) == 0, errors, warnings

    def validate_business_rules_feature_usage(self, df: pd.DataFrame) -> tuple[bool, list, list]:
        errors, warnings = [], []

        # Usage count > 0
        zero_usage = (df["usage_count"] <= 0).sum()
        if zero_usage > 0:
            warnings.append(f"[FEATURE_USAGE] {zero_usage} rows with usage_count <= 0")

        # Duration should be positive
        zero_dur = (df["usage_duration_secs"] <= 0).sum()
        if zero_dur > 0:
            warnings.append(f"[FEATURE_USAGE] {zero_dur} rows with usage_duration_secs <= 0")

        return len(errors) == 0, errors, warnings

    def validate_business_rules_tickets(self, df: pd.DataFrame) -> tuple[bool, list, list]:
        errors, warnings = [], []

        # Priority check
        invalid_pri = df[~df["priority"].isin(self.VALID_PRIORITIES)]["priority"].unique()
        if len(invalid_pri) > 0:
            errors.append(f"[SUPPORT_TICKETS] Invalid priorities: {list(invalid_pri)}")

        # Resolution time should be positive
        if "resolution_time_hours" in df.columns:
            neg_res = (df["resolution_time_hours"].dropna() < 0).sum()
            if neg_res > 0:
                errors.append(f"[SUPPORT_TICKETS] {neg_res} rows with negative resolution_time_hours")

        return len(errors) == 0, errors, warnings

    # ============================================================
    # LEVEL 3 — Cross-Table Integrity
    # ============================================================

    def validate_cross_table(self, dataframes: dict[str, pd.DataFrame]) -> tuple[bool, list, list]:
        """Validate referential integrity and consistency across all tables."""
        errors, warnings = [], []

        accounts = dataframes.get("accounts")
        subs = dataframes.get("subscriptions")
        churn = dataframes.get("churn_events")
        usage = dataframes.get("feature_usage")
        tickets = dataframes.get("support_tickets")

        if accounts is not None:
            account_ids = set(accounts["account_id"].unique())

            # Check subscription → account FK
            if subs is not None:
                orphan_subs = set(subs["account_id"].unique()) - account_ids
                if orphan_subs:
                    errors.append(
                        f"[CROSS-TABLE] {len(orphan_subs)} subscription account_ids not found in accounts table"
                    )

            # Check churn → account FK
            if churn is not None:
                orphan_churn = set(churn["account_id"].unique()) - account_ids
                if orphan_churn:
                    errors.append(
                        f"[CROSS-TABLE] {len(orphan_churn)} churn event account_ids not found in accounts table"
                    )

            # Check tickets → account FK
            if tickets is not None:
                orphan_tickets = set(tickets["account_id"].unique()) - account_ids
                if orphan_tickets:
                    errors.append(
                        f"[CROSS-TABLE] {len(orphan_tickets)} ticket account_ids not found in accounts table"
                    )

            # Check subscription → usage FK
            if subs is not None and usage is not None:
                sub_ids = set(subs["subscription_id"].unique())
                orphan_usage = set(usage["subscription_id"].unique()) - sub_ids
                if orphan_usage:
                    errors.append(
                        f"[CROSS-TABLE] {len(orphan_usage)} feature_usage subscription_ids not found in subscriptions table"
                    )

            # Churn flag consistency check
            if churn is not None:
                churned_accounts_flag = set(accounts[accounts["churn_flag"] == True]["account_id"])
                churned_accounts_events = set(churn["account_id"].unique())

                # Flagged but no event
                flag_no_event = churned_accounts_flag - churned_accounts_events
                if flag_no_event:
                    warnings.append(
                        f"[CROSS-TABLE] {len(flag_no_event)} accounts have churn_flag=True but no churn event record"
                    )

                # Event but not flagged
                event_no_flag = churned_accounts_events - churned_accounts_flag
                if event_no_flag:
                    warnings.append(
                        f"[CROSS-TABLE] {len(event_no_flag)} accounts have churn events but churn_flag=False"
                    )

        return len(errors) == 0, errors, warnings

    # ============================================================
    # Main Validation Entry Point
    # ============================================================

    def validate_all(self, dataframes: dict[str, pd.DataFrame]) -> dict:
        """
        Run all 3 levels of validation on all tables.
        
        Returns:
            {
                "is_valid": bool,
                "total_errors": int,
                "total_warnings": int,
                "details": [
                    {"level": "schema", "table": "...", "errors": [...], "warnings": [...]},
                    ...
                ]
            }
        """
        all_errors = []
        all_warnings = []
        details = []

        # Level 1 + Level 2 — Per-table validation
        validators = {
            "accounts": (self.validate_schema_accounts, self.validate_business_rules_accounts),
            "subscriptions": (self.validate_schema_subscriptions, self.validate_business_rules_subscriptions),
            "churn_events": (self.validate_schema_churn_events, self.validate_business_rules_churn_events),
            "feature_usage": (self.validate_schema_feature_usage, self.validate_business_rules_feature_usage),
            "support_tickets": (self.validate_schema_support_tickets, self.validate_business_rules_tickets),
        }

        for table_name, (schema_fn, rules_fn) in validators.items():
            if table_name in dataframes:
                df = dataframes[table_name]

                # Schema validation
                _, s_errors, s_warnings = schema_fn(df)
                all_errors.extend(s_errors)
                all_warnings.extend(s_warnings)
                details.append({
                    "level": "schema",
                    "table": table_name,
                    "errors": s_errors,
                    "warnings": s_warnings,
                })

                # Business rules
                _, b_errors, b_warnings = rules_fn(df)
                all_errors.extend(b_errors)
                all_warnings.extend(b_warnings)
                details.append({
                    "level": "business_rules",
                    "table": table_name,
                    "errors": b_errors,
                    "warnings": b_warnings,
                })

        # Level 3 — Cross-table integrity
        _, c_errors, c_warnings = self.validate_cross_table(dataframes)
        all_errors.extend(c_errors)
        all_warnings.extend(c_warnings)
        details.append({
            "level": "cross_table",
            "table": "all",
            "errors": c_errors,
            "warnings": c_warnings,
        })

        return {
            "is_valid": len(all_errors) == 0,
            "total_errors": len(all_errors),
            "total_warnings": len(all_warnings),
            "all_errors": all_errors,
            "all_warnings": all_warnings,
            "details": details,
        }

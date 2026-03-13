"""
Data Cleaning Module
Handles: null values, whitespace, duplicates, type coercion, and normalization
for all 5 billing tables. Each method returns (cleaned_df, report_dict).
"""

import pandas as pd
from datetime import datetime


class DataCleaner:
    """Cleans and normalizes billing data from CSV sources."""

    def __init__(self):
        self.cleaning_log = []

    def _log(self, table: str, action: str, affected: int):
        entry = {"table": table, "action": action, "affected_rows": affected}
        self.cleaning_log.append(entry)
        return entry

    def clean_accounts(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        """Clean the accounts table."""
        report = {"table": "accounts", "original_rows": len(df), "actions": []}
        df = df.copy()

        # 1. Trim whitespace from all string columns
        str_cols = df.select_dtypes(include="object").columns
        for col in str_cols:
            trimmed = df[col].str.strip()
            changed = (df[col] != trimmed).sum()
            if changed > 0:
                df[col] = trimmed
                report["actions"].append(self._log("accounts", f"Trimmed whitespace in '{col}'", int(changed)))

        # 2. Normalize country codes to uppercase
        before = df["country"].copy()
        df["country"] = df["country"].str.upper()
        changed = (before != df["country"]).sum()
        if changed > 0:
            report["actions"].append(self._log("accounts", "Normalized country codes to uppercase", int(changed)))

        # 3. Normalize plan_tier casing (Title Case)
        before = df["plan_tier"].copy()
        df["plan_tier"] = df["plan_tier"].str.strip().str.title()
        changed = (before != df["plan_tier"]).sum()
        if changed > 0:
            report["actions"].append(self._log("accounts", "Normalized plan_tier casing", int(changed)))

        # 4. Parse signup_date to proper date format
        df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        null_dates = df["signup_date"].isna().sum()
        if null_dates > 0:
            report["actions"].append(self._log("accounts", "Found unparseable signup dates", int(null_dates)))

        # 5. Ensure boolean columns are actual booleans
        for col in ["is_trial", "churn_flag"]:
            df[col] = df[col].astype(bool)

        # 6. Remove exact duplicate rows
        dupes = df.duplicated(subset=["account_id"]).sum()
        if dupes > 0:
            df = df.drop_duplicates(subset=["account_id"], keep="first")
            report["actions"].append(self._log("accounts", "Removed duplicate account_id rows", int(dupes)))

        report["cleaned_rows"] = len(df)
        return df, report

    def clean_subscriptions(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        """Clean the subscriptions table."""
        report = {"table": "subscriptions", "original_rows": len(df), "actions": []}
        df = df.copy()

        # 1. Trim whitespace
        str_cols = df.select_dtypes(include="object").columns
        for col in str_cols:
            original = df[col].copy()
            df[col] = df[col].str.strip()
            changed = (original.fillna("") != df[col].fillna("")).sum()
            if changed > 0:
                report["actions"].append(self._log("subscriptions", f"Trimmed whitespace in '{col}'", int(changed)))

        # 2. Normalize plan_tier
        df["plan_tier"] = df["plan_tier"].str.strip().str.title()

        # 3. Normalize billing_frequency to lowercase
        df["billing_frequency"] = df["billing_frequency"].str.strip().str.lower()

        # 4. Parse dates
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        # end_date NaT is expected (active subscriptions) — leave as None
        df["end_date"] = df["end_date"].where(df["end_date"].notna(), None)

        # 5. Ensure MRR/ARR are non-negative
        neg_mrr = (df["mrr_amount"] < 0).sum()
        neg_arr = (df["arr_amount"] < 0).sum()
        if neg_mrr > 0:
            df.loc[df["mrr_amount"] < 0, "mrr_amount"] = 0
            report["actions"].append(self._log("subscriptions", "Fixed negative MRR values to 0", int(neg_mrr)))
        if neg_arr > 0:
            df.loc[df["arr_amount"] < 0, "arr_amount"] = 0
            report["actions"].append(self._log("subscriptions", "Fixed negative ARR values to 0", int(neg_arr)))

        # 6. Ensure trials have MRR=0
        trial_with_mrr = ((df["is_trial"] == True) & (df["mrr_amount"] > 0)).sum()
        if trial_with_mrr > 0:
            df.loc[(df["is_trial"] == True) & (df["mrr_amount"] > 0), "mrr_amount"] = 0
            df.loc[(df["is_trial"] == True) & (df["arr_amount"] > 0), "arr_amount"] = 0
            report["actions"].append(self._log("subscriptions", "Zeroed MRR/ARR for trial subscriptions", int(trial_with_mrr)))

        # 7. Ensure boolean columns
        for col in ["is_trial", "upgrade_flag", "downgrade_flag", "churn_flag", "auto_renew_flag"]:
            df[col] = df[col].astype(bool)

        # 8. Remove duplicate subscription_ids
        dupes = df.duplicated(subset=["subscription_id"]).sum()
        if dupes > 0:
            df = df.drop_duplicates(subset=["subscription_id"], keep="first")
            report["actions"].append(self._log("subscriptions", "Removed duplicate subscription_id rows", int(dupes)))

        report["cleaned_rows"] = len(df)
        return df, report

    def clean_churn_events(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        """Clean the churn events table."""
        report = {"table": "churn_events", "original_rows": len(df), "actions": []}
        df = df.copy()

        # 1. Trim whitespace
        str_cols = df.select_dtypes(include="object").columns
        for col in str_cols:
            original = df[col].copy()
            df[col] = df[col].str.strip()

        # 2. Fill missing feedback_text
        null_feedback = df["feedback_text"].isna().sum()
        if null_feedback > 0:
            df["feedback_text"] = df["feedback_text"].fillna("No feedback provided")
            report["actions"].append(self._log("churn_events", "Filled missing feedback_text", int(null_feedback)))

        # 3. Normalize reason_code to lowercase
        df["reason_code"] = df["reason_code"].str.lower().str.strip()

        # 4. Ensure refund >= 0
        neg_refund = (df["refund_amount_usd"] < 0).sum()
        if neg_refund > 0:
            df.loc[df["refund_amount_usd"] < 0, "refund_amount_usd"] = 0
            report["actions"].append(self._log("churn_events", "Fixed negative refund amounts", int(neg_refund)))

        # 5. Parse churn_date
        df["churn_date"] = pd.to_datetime(df["churn_date"], errors="coerce").dt.strftime("%Y-%m-%d")

        # 6. Boolean columns
        for col in ["preceding_upgrade_flag", "preceding_downgrade_flag", "is_reactivation"]:
            df[col] = df[col].astype(bool)

        # 7. Remove duplicates
        dupes = df.duplicated(subset=["churn_event_id"]).sum()
        if dupes > 0:
            df = df.drop_duplicates(subset=["churn_event_id"], keep="first")
            report["actions"].append(self._log("churn_events", "Removed duplicate churn_event_id rows", int(dupes)))

        report["cleaned_rows"] = len(df)
        return df, report

    def clean_feature_usage(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        """Clean the feature usage table."""
        report = {"table": "feature_usage", "original_rows": len(df), "actions": []}
        df = df.copy()

        # 1. Remove duplicate usage_ids (known issue: 21 duplicates)
        dupes = df.duplicated(subset=["usage_id"]).sum()
        if dupes > 0:
            df = df.drop_duplicates(subset=["usage_id"], keep="first")
            report["actions"].append(self._log("feature_usage", "Removed duplicate usage_id rows", int(dupes)))

        # 2. Trim feature_name
        df["feature_name"] = df["feature_name"].str.strip().str.lower()

        # 3. Parse usage_date
        df["usage_date"] = pd.to_datetime(df["usage_date"], errors="coerce").dt.strftime("%Y-%m-%d")

        # 4. Ensure counts are non-negative
        neg_usage = (df["usage_count"] < 0).sum()
        if neg_usage > 0:
            df.loc[df["usage_count"] < 0, "usage_count"] = 0
            report["actions"].append(self._log("feature_usage", "Fixed negative usage_count", int(neg_usage)))

        neg_duration = (df["usage_duration_secs"] < 0).sum()
        if neg_duration > 0:
            df.loc[df["usage_duration_secs"] < 0, "usage_duration_secs"] = 0
            report["actions"].append(self._log("feature_usage", "Fixed negative duration", int(neg_duration)))

        # 5. Boolean
        df["is_beta_feature"] = df["is_beta_feature"].astype(bool)

        report["cleaned_rows"] = len(df)
        return df, report

    def clean_support_tickets(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        """Clean the support tickets table."""
        report = {"table": "support_tickets", "original_rows": len(df), "actions": []}
        df = df.copy()

        # 1. Normalize priority to lowercase
        df["priority"] = df["priority"].str.strip().str.lower()

        # 2. Handle null satisfaction_score — leave as NaN (will become None in JSON)
        null_score = df["satisfaction_score"].isna().sum()
        if null_score > 0:
            report["actions"].append(self._log("support_tickets", f"Found null satisfaction_score (will import as null)", int(null_score)))

        # 3. Parse timestamps
        df["submitted_at"] = pd.to_datetime(df["submitted_at"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")
        df["closed_at"] = pd.to_datetime(df["closed_at"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")
        df["closed_at"] = df["closed_at"].where(df["closed_at"].notna(), None)

        # 4. Ensure resolution_time > 0
        neg_res = (df["resolution_time_hours"] < 0).sum() if df["resolution_time_hours"].notna().any() else 0
        if neg_res > 0:
            df.loc[df["resolution_time_hours"] < 0, "resolution_time_hours"] = 0
            report["actions"].append(self._log("support_tickets", "Fixed negative resolution_time", int(neg_res)))

        # 5. Remove duplicates
        dupes = df.duplicated(subset=["ticket_id"]).sum()
        if dupes > 0:
            df = df.drop_duplicates(subset=["ticket_id"], keep="first")
            report["actions"].append(self._log("support_tickets", "Removed duplicate ticket_id rows", int(dupes)))

        # 6. Boolean
        df["escalation_flag"] = df["escalation_flag"].astype(bool)

        report["cleaned_rows"] = len(df)
        return df, report

    def clean_all(self, dataframes: dict[str, pd.DataFrame]) -> tuple[dict[str, pd.DataFrame], list[dict]]:
        """
        Clean all tables at once.
        
        Args:
            dataframes: dict mapping table name to DataFrame
            
        Returns:
            (cleaned_dataframes, list_of_reports)
        """
        cleaners = {
            "accounts": self.clean_accounts,
            "subscriptions": self.clean_subscriptions,
            "churn_events": self.clean_churn_events,
            "feature_usage": self.clean_feature_usage,
            "support_tickets": self.clean_support_tickets,
        }

        cleaned = {}
        reports = []

        for table_name, cleaner_fn in cleaners.items():
            if table_name in dataframes:
                cleaned_df, report = cleaner_fn(dataframes[table_name])
                cleaned[table_name] = cleaned_df
                reports.append(report)

        return cleaned, reports

"""
Migration Orchestrator
End-to-end pipeline: Load CSVs → Clean → Validate → Push via API → Generate Report

Usage:
    python scripts/migrate_data.py                    # Full migration
    python scripts/migrate_data.py --dry-run           # Validate only, no push
    python scripts/migrate_data.py --data-dir ./data   # Custom data directory
    python scripts/migrate_data.py --api-url http://localhost:8000  # Custom API URL
"""

import os
import sys
import json
import argparse
import time
from datetime import datetime

import pandas as pd
import requests

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.clean_data import DataCleaner
from scripts.validate_data import DataValidator


# ============================================================
# Configuration
# ============================================================

TABLE_FILES = {
    "accounts": "ravenstack_accounts.csv",
    "subscriptions": "ravenstack_subscriptions.csv",
    "churn_events": "ravenstack_churn_events.csv",
    "feature_usage": "ravenstack_feature_usage.csv",
    "support_tickets": "ravenstack_support_tickets.csv",
}

# Migration order — respects foreign key dependencies
MIGRATION_ORDER = ["accounts", "subscriptions", "feature_usage", "churn_events", "support_tickets"]

API_ENDPOINTS = {
    "accounts": "/api/import/accounts",
    "subscriptions": "/api/import/subscriptions",
    "churn_events": "/api/import/churn-events",
    "feature_usage": "/api/import/feature-usage",
    "support_tickets": "/api/import/support-tickets",
}

BATCH_SIZE = 500  # Records per API call


# ============================================================
# Helper Functions
# ============================================================

def print_header(text: str):
    width = 60
    print(f"\n{'='*width}")
    print(f"  {text}")
    print(f"{'='*width}")


def print_step(step: int, text: str):
    print(f"\n  [{step}/6] {text}")
    print(f"  {'-'*50}")


def load_csvs(data_dir: str) -> dict[str, pd.DataFrame]:
    """Load all CSV files from the data directory."""
    dataframes = {}
    for table_name, filename in TABLE_FILES.items():
        filepath = os.path.join(data_dir, filename)
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            dataframes[table_name] = df
            print(f"    ✅ {table_name}: {len(df)} rows loaded")
        else:
            print(f"    ❌ {table_name}: File not found — {filepath}")
    return dataframes


def df_to_records(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to list of dicts, handling NaN → None."""
    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    # Convert any remaining NaN/NaT values
    for record in records:
        for key, value in record.items():
            if pd.isna(value) if not isinstance(value, str) else False:
                record[key] = None
    return records


def push_to_api(api_url: str, table_name: str, records: list[dict]) -> dict:
    """Push records to the API in batches."""
    endpoint = api_url.rstrip("/") + API_ENDPOINTS[table_name]
    total_imported = 0
    total_skipped = 0
    all_errors = []

    # Split into batches
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE

        try:
            response = requests.post(endpoint, json=batch, timeout=60)
            if response.status_code == 200:
                result = response.json()
                total_imported += result.get("imported", 0)
                total_skipped += result.get("skipped", 0)
                all_errors.extend(result.get("errors", []))
                print(f"    📦 Batch {batch_num}/{total_batches}: "
                      f"{result.get('imported', 0)} imported, {result.get('skipped', 0)} skipped")
            else:
                error_msg = f"Batch {batch_num}: HTTP {response.status_code} — {response.text[:200]}"
                all_errors.append(error_msg)
                print(f"    ❌ Batch {batch_num}/{total_batches}: {error_msg}")
        except requests.exceptions.ConnectionError:
            all_errors.append(f"Batch {batch_num}: Connection refused — is the API server running?")
            print(f"    ❌ Cannot connect to API at {endpoint}")
            break
        except Exception as e:
            all_errors.append(f"Batch {batch_num}: {str(e)[:200]}")
            print(f"    ❌ Batch {batch_num}: {e}")

    return {
        "imported": total_imported,
        "skipped": total_skipped,
        "errors": all_errors,
    }


# ============================================================
# Report Generation
# ============================================================

def generate_report(
    cleaning_reports: list[dict],
    validation_result: dict,
    migration_results: dict,
    dry_run: bool,
    elapsed_time: float,
) -> dict:
    """Generate a comprehensive migration report."""
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "mode": "DRY RUN" if dry_run else "FULL MIGRATION",
        "elapsed_seconds": round(elapsed_time, 2),
        "cleaning": cleaning_reports,
        "validation": {
            "is_valid": validation_result["is_valid"],
            "total_errors": validation_result["total_errors"],
            "total_warnings": validation_result["total_warnings"],
            "errors": validation_result["all_errors"],
            "warnings": validation_result["all_warnings"],
        },
        "migration": migration_results if not dry_run else "Skipped (dry run)",
    }

    return report


def print_report(report: dict):
    """Print a formatted summary of the migration report."""
    
    print_header(f"MIGRATION REPORT — {report['mode']}")
    print(f"  Timestamp: {report['timestamp']}")
    print(f"  Duration:  {report['elapsed_seconds']}s")

    # Cleaning summary
    print(f"\n  📋 CLEANING SUMMARY")
    for cr in report["cleaning"]:
        actions = len(cr.get("actions", []))
        original = cr.get("original_rows", "?")
        cleaned = cr.get("cleaned_rows", "?")
        removed = int(original) - int(cleaned) if isinstance(original, int) and isinstance(cleaned, int) else 0
        print(f"    {cr['table']:20s} | {original} → {cleaned} rows | {actions} actions | {removed} removed")
        for action in cr.get("actions", []):
            print(f"      • {action['action']} ({action['affected_rows']} rows)")

    # Validation summary
    val = report["validation"]
    status = "✅ PASSED" if val["is_valid"] else "❌ FAILED"
    print(f"\n  🔍 VALIDATION: {status}")
    print(f"    Errors:   {val['total_errors']}")
    print(f"    Warnings: {val['total_warnings']}")

    if val["errors"]:
        print(f"\n    Errors:")
        for err in val["errors"]:
            print(f"      ❌ {err}")
    if val["warnings"]:
        print(f"\n    Warnings:")
        for warn in val["warnings"]:
            print(f"      ⚠️  {warn}")

    # Migration summary
    if isinstance(report["migration"], dict):
        print(f"\n  🚀 MIGRATION RESULTS")
        total_imported = 0
        total_skipped = 0
        for table, result in report["migration"].items():
            imported = result.get("imported", 0)
            skipped = result.get("skipped", 0)
            total_imported += imported
            total_skipped += skipped
            status_icon = "✅" if not result.get("errors") else "⚠️"
            print(f"    {status_icon} {table:20s} | {imported} imported | {skipped} skipped")
        print(f"\n    TOTAL: {total_imported} imported, {total_skipped} skipped")
    else:
        print(f"\n  🚀 MIGRATION: {report['migration']}")

    print(f"\n{'='*60}\n")


def save_report(report: dict, reports_dir: str):
    """Save report to JSON file."""
    os.makedirs(reports_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    mode = "dryrun" if "DRY" in report["mode"] else "migration"
    filename = f"report_{mode}_{timestamp}.json"
    filepath = os.path.join(reports_dir, filename)

    with open(filepath, "w") as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"  📄 Report saved: {filepath}")
    return filepath


# ============================================================
# Main Migration Pipeline
# ============================================================

def run_migration(data_dir: str, api_url: str, dry_run: bool = False):
    """Execute the full migration pipeline."""
    start_time = time.time()

    print_header("BILLING DATA MIGRATION TOOL")
    print(f"  Mode:     {'🔍 DRY RUN (no data will be pushed)' if dry_run else '🚀 FULL MIGRATION'}")
    print(f"  Data Dir: {data_dir}")
    print(f"  API URL:  {api_url}")

    # Step 1: Load CSVs
    print_step(1, "Loading CSV files")
    dataframes = load_csvs(data_dir)
    if not dataframes:
        print("  ❌ No data files found. Aborting.")
        return

    # Step 2: Clean data
    print_step(2, "Cleaning data")
    cleaner = DataCleaner()
    cleaned_dfs, cleaning_reports = cleaner.clean_all(dataframes)
    for cr in cleaning_reports:
        actions = len(cr.get("actions", []))
        print(f"    ✅ {cr['table']}: {cr.get('original_rows', '?')} → {cr.get('cleaned_rows', '?')} rows ({actions} actions)")

    # Step 3: Validate data
    print_step(3, "Validating data (3 levels)")
    validator = DataValidator()
    validation_result = validator.validate_all(cleaned_dfs)
    
    if validation_result["is_valid"]:
        print(f"    ✅ All validations passed!")
    else:
        print(f"    ⚠️  Validation completed with {validation_result['total_errors']} errors")
    
    if validation_result["total_warnings"] > 0:
        print(f"    ℹ️  {validation_result['total_warnings']} warnings found")

    # Step 4: Convert to JSON
    print_step(4, "Converting to JSON payloads")
    json_payloads = {}
    for table_name in MIGRATION_ORDER:
        if table_name in cleaned_dfs:
            records = df_to_records(cleaned_dfs[table_name])
            json_payloads[table_name] = records
            print(f"    ✅ {table_name}: {len(records)} records prepared")

    # Step 5: Push to API (or skip in dry-run)
    migration_results = {}
    if dry_run:
        print_step(5, "Push to API — SKIPPED (dry run mode)")
        print(f"    ℹ️  No data was pushed. Run without --dry-run to migrate.")
    else:
        print_step(5, "Pushing data to API")
        
        # Check API health first
        try:
            health = requests.get(f"{api_url.rstrip('/')}/api/health", timeout=5)
            if health.status_code == 200:
                print(f"    ✅ API is healthy")
            else:
                print(f"    ❌ API health check failed. Start the server first:")
                print(f"       uvicorn api.server:app --reload")
                return
        except requests.exceptions.ConnectionError:
            print(f"    ❌ Cannot connect to API at {api_url}")
            print(f"       Start the server first: uvicorn api.server:app --reload")
            return

        for table_name in MIGRATION_ORDER:
            if table_name in json_payloads:
                print(f"\n    📤 Migrating: {table_name}")
                result = push_to_api(api_url, table_name, json_payloads[table_name])
                migration_results[table_name] = result

    # Step 6: Generate report
    elapsed = time.time() - start_time
    print_step(6, "Generating migration report")

    report = generate_report(
        cleaning_reports=cleaning_reports,
        validation_result=validation_result,
        migration_results=migration_results,
        dry_run=dry_run,
        elapsed_time=elapsed,
    )

    # Save report
    reports_dir = os.path.join(os.path.dirname(__file__), "..", "reports")
    save_report(report, reports_dir)

    # Print summary
    print_report(report)

    return report


# ============================================================
# CLI Entry Point
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Billing Data Migration Tool - Migrate SaaS billing data from CSV to database via API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/migrate_data.py --dry-run              # Validate only
  python scripts/migrate_data.py                         # Full migration
  python scripts/migrate_data.py --data-dir ./custom_data
  python scripts/migrate_data.py --api-url http://localhost:9000
        """
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and report without pushing data to the API",
    )
    parser.add_argument(
        "--data-dir",
        default=os.path.join(os.path.dirname(__file__), "..", "data"),
        help="Path to directory containing CSV files (default: ./data)",
    )
    parser.add_argument(
        "--api-url",
        default="http://localhost:8000",
        help="Base URL of the migration API (default: http://localhost:8000)",
    )

    args = parser.parse_args()
    
    # Resolve absolute path
    data_dir = os.path.abspath(args.data_dir)

    run_migration(
        data_dir=data_dir,
        api_url=args.api_url,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()

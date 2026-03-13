# Billing Data Migration & Validation Tool

A Python-based tool that migrates SaaS billing data from CSV files into a structured database through a REST API, with built-in data cleaning, multi-level validation, and detailed reporting.

Built to simulate a real-world customer onboarding workflow — the kind of work done at companies like [Zenskar](https://zenskar.com) during data migration from platforms like Stripe or Chargebee.

---

## 🏗️ Architecture

```
CSV Files → [Data Cleaner] → [Data Validator] → [JSON] → [FastAPI REST API] → [SQLite/PostgreSQL]
                                                   ↓
                                          [Migration Report]
```

**Data flow follows strict dependency order:**

```
accounts → subscriptions → feature_usage
       ↘ churn_events
       ↘ support_tickets
```

---

## 🗂️ Project Structure

```
billing-migration-tool/
├── data/                          # Source CSV files (5 tables, 33K+ records)
├── scripts/
│   ├── clean_data.py              # Data cleaning (nulls, types, duplicates)
│   ├── validate_data.py           # 3-level validation engine
│   └── migrate_data.py            # Migration orchestrator (CLI entry point)
├── api/
│   ├── server.py                  # FastAPI REST server
│   ├── models.py                  # Pydantic schemas for all entities
│   └── database.py                # SQLAlchemy DB layer (SQLite/PostgreSQL)
├── database/
│   └── schema.sql                 # SQL schema with constraints & indexes
├── reports/                       # Generated migration reports (JSON)
├── tests/
│   └── test_validation.py         # Unit tests for cleaning & validation
├── requirements.txt
└── README.md
```

---

## 📊 Dataset

Migrates **5 related billing tables** from the Ravenstack SaaS platform:

| Table | Records | Description |
|---|---|---|
| **Accounts** | 500 | Customer companies |
| **Subscriptions** | 5,000 | Billing subscriptions per account |
| **Feature Usage** | 25,000 | Per-subscription feature usage logs |
| **Churn Events** | 600 | Cancellation/churn records |
| **Support Tickets** | 2,000 | Customer support interactions |

---

## ✨ Key Features

### 🧹 Data Cleaning
- Removes duplicate records (21 duplicate `usage_id`s detected)
- Normalizes inconsistent casing, whitespace, and formats
- Handles null values with sensible defaults
- Ensures numeric fields are non-negative
- Validates and coerces date formats

### 🔍 Three-Level Validation
1. **Schema Validation** — Required fields, data types, ID format patterns
2. **Business Rules** — Plan tier validity, MRR/ARR relationship, billing frequency
3. **Cross-Table Integrity** — Foreign key consistency, churn flag alignment across tables

### 🚀 REST API Migration
- FastAPI server with endpoints for each table
- Batch processing (500 records/batch)
- Idempotent inserts (no duplicate data on re-run)
- Health check and statistics endpoints

### 📋 Migration Reports
- Detailed JSON reports saved to `reports/`
- Cleaning actions audit trail
- Validation results (errors + warnings)
- Import statistics per table

### 🔒 Dry-Run Mode
Validate and preview results **without** pushing any data.

---

## 🛠️ Tech Stack

| Technology | Purpose |
|---|---|
| **Python 3.10+** | Core language |
| **Pandas** | Data loading, cleaning, transformation |
| **FastAPI** | REST API framework |
| **SQLAlchemy** | Database ORM |
| **Pydantic** | Data validation & serialization |
| **SQLite** | Default database (swappable to PostgreSQL) |
| **Pytest** | Unit testing |

---

## 🚀 Quick Start

### 1. Install dependencies

```bash
cd billing-migration-tool
pip install -r requirements.txt
```

### 2. Run a dry-run (validate without migrating)

```bash
python scripts/migrate_data.py --dry-run
```

This will clean, validate, and generate a report **without** touching the database.

### 3. Start the API server

```bash
uvicorn api.server:app --reload
```

The API will be available at `http://localhost:8000`.  
Interactive docs at `http://localhost:8000/docs`.

### 4. Run the full migration

```bash
python scripts/migrate_data.py
```

### 5. Check migration stats

```bash
curl http://localhost:8000/api/stats
```

---

## 📡 API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/health` | GET | Health check |
| `/api/stats` | GET | Record counts per table |
| `/api/import/accounts` | POST | Import accounts |
| `/api/import/subscriptions` | POST | Import subscriptions |
| `/api/import/churn-events` | POST | Import churn events |
| `/api/import/feature-usage` | POST | Import feature usage |
| `/api/import/support-tickets` | POST | Import support tickets |

---

## 🧪 Testing

```bash
python -m pytest tests/ -v
```

---

## 🔍 Data Quality Issues Detected

The tool automatically detects and handles these real-world data issues:

| Issue | Table | Details |
|---|---|---|
| **Duplicate IDs** | Feature Usage | 21 duplicate `usage_id` records |
| **Churn flag mismatch** | Accounts ↔ Churn Events | 35 accounts flagged churned with no event; 277 events for non-churned accounts |
| **Missing satisfaction scores** | Support Tickets | 41.2% null values |
| **Missing feedback text** | Churn Events | 24.7% null values |

---

## 🔄 Using with PostgreSQL

Set the `DATABASE_URL` environment variable:

```bash
export DATABASE_URL=postgresql://user:password@localhost:5432/billing_db
uvicorn api.server:app --reload
python scripts/migrate_data.py
```

---

## 📄 CLI Usage

```bash
# Full migration
python scripts/migrate_data.py

# Dry run (validate only)
python scripts/migrate_data.py --dry-run

# Custom data directory
python scripts/migrate_data.py --data-dir /path/to/csvs

# Custom API URL
python scripts/migrate_data.py --api-url http://localhost:9000
```

---


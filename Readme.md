# WaterWise: Operational and Analytical Batch Data Pipeline

## Stage 1: Data Extraction Documentation

### Objective

The objective of the WaterWise Data Extraction Stage is to extract the required operational data from the existing WaterWise Supabase/PostgreSQL database and prepare a documented, traceable, and validated dataset for the succeeding Data Validation and Transformation stages.

> The goal of this stage is to extract data from an identified source system and prepare a documented, traceable, and validated dataset for the next stage of the data pipeline.

---

## 1. Data Source and Extraction Specification

### 1.1 Source System

| Item | Description |
| --- | --- |
| **Source System** | WaterWise Sucol Water System |
| **Purpose** | The WaterWise system manages consumer information, water meter readings, consumption records, billing information, payments, and account balances. These operational records serve as the source data for the WaterWise analytical and decision-support functions. |
| **Source Database** | Supabase PostgreSQL |
| **Database management system** | PostgreSQL (via Supabase) |
| **File format** | Not applicable — direct database extraction (no CSV/JSON/Excel source files) |

### 1.2 Extraction Method

| Item | Description |
| --- | --- |
| **Extraction tool** | Python (`supabase-py` / `psycopg2`) |
| **Orchestration tool** | Apache Airflow |
| **Extraction type** | Scheduled batch extraction |
| **Extraction mode** | Incremental extraction (with initial full load) |

The extraction process follows this sequence:

1. Apache Airflow starts the scheduled extraction job.
2. Python makes the connection to the WaterWise Supabase/PostgreSQL database.
3. Python fetches the desired records from the chosen tables.
4. The records extracted are placed in the rawdata staging.
5. Validation of the data conducted at the extraction level.
6. Data from the pipeline stage is validated and passed to the next pipeline stage.

### 1.3 Incremental Extraction Window

For incremental extraction, the extraction window is determined per table:

| Table | Window field | Logic |
| --- | --- | --- |
| `consumers` | `updated_at` | Extract rows where `updated_at >= last_successful_run AND updated_at < current_run` |
| `consumption` | `reading_date`, `updated_at` | Extract rows where `reading_date` falls in the target billing month, or `updated_at` is within the window |
| `billing` | `billing_date`, `updated_at` | Extract rows where `billing_date >= window_start` |
| `payments` | `payment_date`, `created_at` | Extract rows where `payment_date >= window_start` |

Initial load is a full extraction. Succeeding runs are incremental using the stored `last_successful_run` timestamp from the extraction log.

### 1.4 Extraction Schedule

| Item | Value |
| --- | --- |
| Frequency | Daily scheduled batch |
| Airflow DAG | `waterwise_daily_extraction` |
| Scheduled time | Once daily (off-peak, e.g. 01:00 Asia/Manila) |
| Retry | Up to 3 retries with backoff on connection/query failure |

### 1.5 Extraction Scope

The extraction will focus on the operational records necessary for WaterWise's analytical pipeline.

| Table | Data to extract | Purpose |
| --- | --- | --- |
| Consumers | `id, username, first_name, middle_name, last_name, email, contact_number, purok_no, status, timestamps` | Identify consumers and their locations |
| Consumption | `id, consumer_id, reading_date, previous_reading, present_reading, consumption, timestamps` | Analyze water consumption |
| Billing | `id, consumption_id, user_id, billing_date, due_date, total_bill, timestamps` | Monitor billing and account balances |
| Payments | `id, billing_id, total_paid, amount_tendered, remaining_balance, payment_date, payment_method, timestamps` | Monitor collections and payment transactions |

> **Out of scope for Stage 1 (per Lab 2 schema, deferred to later stages):** `admins`, `meter_readers`, `notifications`, `notification_reads`, `consumer_push_subscriptions`, `notification_push_jobs`, `events`, `generated_reports`, `ai_consumption_cache`, `admin_trusted_devices`, `password_reset_request_limits`, `system_settings`, and session/audit tables. Stage 1 extracts only the 4 operational tables above.

### 1.6 Source Limitations and Assumptions

Limitations:

- The extraction depends on the availability of the Supabase/PostgreSQL database.
- Database credentials and network connectivity must be available.
- Changes to the database schema may affect the extraction process.
- Missing or incomplete historical records may affect analytical results.
- Temporary database or network failures may interrupt extraction.

Assumptions:

- Service-role credentials used by the extraction job are kept server-side and never exposed to the frontend.
- Schema reference: `backend/db/migration.sql` and `backend/db/full_setup_migration.sql`.
- One accepted consumption reading per consumer per month (`UNIQUE(consumer_id, YEAR(reading_date), MONTH(reading_date))`).
- Row Level Security blocks direct browser access; extraction uses the server-side service-role key.

---

## 2. Source Tables and Column Specification

> Requirement: Include only the columns and details relevant to the data pipeline. Do not copy every row of the source table.

### 2.1 Extraction Tables Overview

| Table | Data to extract | Purpose |
| --- | --- | --- |
| Consumers | `id, username, first_name, middle_name, last_name, email, contact_number, purok_no, status, timestamps` | Identify consumers and their locations |
| Consumption | `id, consumer_id, reading_date, previous_reading, present_reading, consumption, timestamps` | Analyze water consumption |
| Billing | `id, consumption_id, user_id, billing_date, due_date, total_bill, timestamps` | Monitor billing and account balances |
| Payments | `id, billing_id, total_paid, amount_tendered, remaining_balance, payment_date, payment_method, timestamps` | Monitor collections and payment transactions |

### 2.2 Consumers Table

**Purpose:** Stores WaterWise consumer account and identification information.

| Column | Data Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `INTEGER` | PK | Unique consumer identifier |
| `username` | `VARCHAR(50)` | UNIQUE | Consumer login username |
| `first_name` | `VARCHAR(50)` | — | Consumer first name |
| `middle_name` | `VARCHAR(50)` | — | Consumer middle name (nullable) |
| `last_name` | `VARCHAR(50)` | — | Consumer last name |
| `email` | `VARCHAR(255)` | UNIQUE | Consumer email |
| `contact_number` | `VARCHAR(20)` | — | Consumer contact number |
| `purok_no` | `INTEGER` | — | Consumer's purok / location zone |
| `status` | `VARCHAR(20)` | — | Consumer account status (`active` / `inactive`) |
| `created_at` | `TIMESTAMPTZ` | — | Record creation timestamp |
| `updated_at` | `TIMESTAMPTZ` | — | Last update timestamp |

**Relationships:** Referenced by `consumption.consumer_id`, `billing.user_id`.
**Reason for inclusion:** Consumer identity and location (`purok_no`, `status`) are required dimensions for per-purok and per-resident analytics.

### 2.3 Consumption Table

**Purpose:** Stores monthly water meter readings and computed consumption.

| Column | Data Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `INTEGER` | PK | Unique reading identifier |
| `consumer_id` | `INTEGER` | FK → `consumers.id` | Owner of the reading |
| `reading_date` | `DATE` | — | Date the meter was read; defines the billing month |
| `previous_reading` | `NUMERIC(10,2)` | — | Previous meter value |
| `present_reading` | `NUMERIC(10,2)` | — | Current meter value |
| `consumption` | `NUMERIC(10,2)` | GENERATED (`present - previous`) | Water consumed in cubic meters |
| `idempotency_key` | `TEXT` | UNIQUE (nullable) | Deduplication key for offline retries |
| `created_at` | `TIMESTAMPTZ` | — | Record creation timestamp |
| `updated_at` | `TIMESTAMPTZ` | — | Last update timestamp |

**Relationships:** Child of `consumers`; parent of `billing` via `billing.consumption_id`. One row per consumer per month enforced by unique index.
**Reason for inclusion:** Core fact for consumption prediction, anomaly detection, and billing computation.

### 2.4 Billing Table

**Purpose:** Stores billing records derived from consumption.

| Column | Data Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `INTEGER` | PK | Unique billing identifier |
| `consumption_id` | `INTEGER` | FK → `consumption.id` | Source reading |
| `user_id` | `INTEGER` | FK → `consumers.id` | Billed consumer |
| `billing_date` | `DATE` | — | Billing period date |
| `due_date` | `DATE` | — | Payment due date |
| `total_bill` | `NUMERIC(10,2)` | — | Total amount billed (`consumption × rate_per_cubic_meter`) |
| `remaining_balance` | `NUMERIC(10,2)` | — | Outstanding balance |
| `status` | `VARCHAR(30)` | — | Billing status (`Unpaid` / partially paid / paid) |
| `created_at` | `TIMESTAMPTZ` | — | Record creation timestamp |
| `updated_at` | `TIMESTAMPTZ` | — | Last update timestamp |

**Relationships:** Child of `consumption` and `consumers`; parent of `payments` via `payments.billing_id`.
**Reason for inclusion:** Required for billing history, balances, flagged-account (≥3 outstanding bills), and collection analytics.

### 2.5 Payments Table

**Purpose:** Stores payment transactions against billing records.

| Column | Data Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `INTEGER` | PK | Unique payment identifier |
| `billing_id` | `INTEGER` | FK → `billing.id` | Bill being paid |
| `total_paid` | `NUMERIC(10,2)` | — | Amount applied to the bill |
| `amount_tendered` | `NUMERIC(10,2)` | — | Cash/payment received |
| `change_given` | `NUMERIC(10,2)` | — | Change returned (cash overpayment) |
| `remaining_balance` | `NUMERIC(10,2)` | — | Balance left after payment |
| `payment_date` | `DATE` | — | Date of payment (Asia/Manila) |
| `payment_method` | `VARCHAR` | — | Method (`Cash` / `GCash` / bank transfer) + reference number |
| `idempotency_key` | `TEXT` | UNIQUE (nullable) | Protects retries from duplicate financial records |
| `created_at` | `TIMESTAMPTZ` | — | Record creation timestamp |

**Relationships:** Child of `billing`.
**Reason for inclusion:** Required for collection monitoring, oldest-first payment order validation, and revenue reporting.

---

## 3. Extraction Validation and Data Quality Checks

For each validation check: check name, target, purpose, and measurable pass/fail criteria. Scope is extraction-level validation only. Data cleaning, standardization, and business transformations are documented in the Transformation Stage.

| Check Name | Target | Purpose | Validation Criteria (Pass / Fail) |
| --- | --- | --- | --- |
| Source connection and accessibility | Supabase/PostgreSQL connection | Confirm the source is reachable before extraction | PASS: connection succeeds within timeout; FAIL: connection error → stop run, log error |
| Required tables and columns exist | `consumers`, `consumption`, `billing`, `payments` + columns in §2 | Detect schema drift early | PASS: all required tables/columns present; FAIL: any missing → stop run |
| Required identifiers present | `consumers.id`, `consumption.id + consumer_id + reading_date`, `billing.id + consumption_id + user_id`, `payments.id + billing_id` | Ensure join keys exist | PASS: 0 rows with NULL identifiers; FAIL: >0 → quarantine affected rows |
| Primary key uniqueness | PK of each extracted table | Detect duplicates in extract | PASS: 0 duplicate PKs; FAIL: >0 → fail table validation |
| Record count and extraction completeness | Row counts per table vs. source `COUNT(*)` in window | Verify no rows lost | PASS: `extracted == source count` in window; WARNING if count differs by >0 but <1%; FAIL if mismatch beyond tolerance or 0 rows when source >0 |
| Referential completeness | `consumption.consumer_id`, `billing.consumption_id/user_id`, `payments.billing_id` | Ensure no orphaned facts | PASS: 0 orphans; FAIL: >0 orphans → quarantine |
| Range sanity (extraction-level only) | `present_reading >= previous_reading`, `consumption >= 0`, `total_bill >= 0` | Catch obviously corrupt extracts | PASS: 0 violations; WARNING: violations present but passed through with flag for Transformation |
| Data extraction errors or interruptions | Airflow task / Python job | Trace partial extracts | PASS: task success, no exception; FAIL: exception/timeout → retry per §7 |

---

## 4. Extraction Metadata and Log Specification

Each extraction run records the following fields for monitoring, troubleshooting, auditing, and recovery:

| Field | Purpose | Example value / format |
| --- | --- | --- |
| `pipeline_run_id` | Unique ID of the extraction run | `ww-extract-2026-03-10-0100` / UUID |
| `table_name` | Table or file name extracted | `consumption` |
| `extraction_started_at` | Extraction start timestamp | `2026-03-10T01:00:00+08:00` (ISO 8601, Asia/Manila) |
| `extraction_ended_at` | Extraction end timestamp | `2026-03-10T01:04:32+08:00` |
| `extraction_status` | Final status of the table/run | `SUCCESS` / `WARNING` / `FAILED` |
| `records_read` | Number of records read from source | `1250` |
| `records_extracted` | Number of records written to staging | `1250` |
| `records_rejected` | Number of records rejected/quarantined | `3` |
| `extraction_window_start` | Start of incremental window | `2026-03-09T01:00:00+08:00` |
| `extraction_window_end` | End of incremental window | `2026-03-10T01:00:00+08:00` |
| `validation_result` | Summary of §3 checks | `PASS` / `WARNING: range-check 2 rows flagged` / `FAIL: PK duplicates` |
| `error_message` / `error_code` | Error detail if applicable | `None` / `CONN_TIMEOUT: Supabase unreachable after 3 retries` |
| `extraction_duration_ms` | Wall-clock duration | `272000` |

Example log record (JSON):

```json
{
  "pipeline_run_id": "ww-extract-2026-03-10-0100",
  "table_name": "consumption",
  "extraction_started_at": "2026-03-10T01:00:00+08:00",
  "extraction_ended_at": "2026-03-10T01:04:32+08:00",
  "extraction_status": "SUCCESS",
  "records_read": 1250,
  "records_extracted": 1250,
  "records_rejected": 0,
  "extraction_window_start": "2026-03-09T01:00:00+08:00",
  "extraction_window_end": "2026-03-10T01:00:00+08:00",
  "validation_result": "PASS",
  "error_message": null,
  "extraction_duration_ms": 272000
}
```

Usage:

- **Monitoring:** Airflow dashboard + run status per table.
- **Troubleshooting:** window + counts + error code pinpoint missing/duplicate data.
- **Auditing:** run ID + validation result prove what was extracted when.
- **Recovery:** window + `records_read` vs. `records_extracted` determine safe re-run range without double-loading (aided by `idempotency_key`).

---

## 5. Log Retention and Access

| Item | Policy |
| --- | --- |
| **Retention period** | Operational extraction logs: minimum **90 days**. Audit-relevant run summaries (run ID, window, counts, validation result): minimum **1 year** for auditing and recovery. Justification: covers monthly billing cycles (3+ outstanding bills → flagged accounts) plus troubleshooting window. |
| **Storage location** | Airflow task logs + centralized append-only log store (e.g. Postgres log table / cloud object storage path `logs/waterwise-extraction/YYYY-MM-DD/`). Staging data itself lives in `rawdata/<table>/YYYY-MM-DD/`. |
| **Access permissions** | Data engineers and Super Admin only (read). Write restricted to the extraction service account / Airflow worker. No consumer or meter-reader access. Credentials never stored in logs. |
| **Archiving** | Logs older than 90 days are compressed and archived to cold storage; audit summaries retained online for 1 year. Archive preserves `pipeline_run_id` indexing for lookup. |
| **Deletion rules** | Logs must not be deleted before the defined retention period or while they are needed for troubleshooting, auditing, or recovery. Deletion after retention requires data-engineer approval and is logged. Operational logs may be purged after 90 days; audit records only after 1 year. |

---

## 6. Extraction Data Contract and Naming Convention

### 6.1 Required Schema

Expected table and column names, data types, required identifiers and constraints:

- Tables: `consumers`, `consumption`, `billing`, `payments` as specified in §2.
- Required identifiers: `consumers.id`, `consumption.(id, consumer_id, reading_date)`, `billing.(id, consumption_id, user_id)`, `payments.(id, billing_id)` — all NOT NULL in the extract.
- Constraints preserved in contract: PK uniqueness per table; FK references listed in §2; `consumption` one-row-per-consumer-per-month.

### 6.2 Naming Convention

- Column naming format: `snake_case` (e.g. `consumer_id`, `reading_date`, `total_bill`).
- Rules for abbreviations: use full descriptive names (`total_paid`, `amount_tendered`, `remaining_balance`); avoid ad-hoc abbreviations; document any abbreviation once in the contract.
- Primary keys: `<table>_id` conceptually, implemented as `id` per WaterWise schema; foreign keys: `<referenced_table_singular>_id` (e.g. `consumer_id`, `consumption_id`, `billing_id`, `user_id` → `consumers.id`).

### 6.3 Schema Consistency

Unexpected changes to column names, data types, or required fields are handled as follows:

1. The §3 `required tables and columns exist` check fails the run before staging write.
2. The pipeline stops/quarantines per §7; no silent coercion.
3. A schema-change ticket is raised; the contract is versioned and the extractor mapping updated.
4. Backfill/re-run uses the stored extraction window after the fix.

### 6.4 Source-to-Standardized Field Mapping

Document source-to-standardized field mappings when applicable. If the standardized field name is `customer_id`, it must be used consistently in the documented pipeline unless a justified mapping is defined.

WaterWise uses its native names as the standard (no rename except noted type normalizations):

| Source Column | Standardized Column | Source Type | Target Type | Mapping Type |
| --- | --- | --- | --- | --- |
| `consumers.purok_no` | `purok_no` | `INTEGER` | `INTEGER` | Direct (standard name is `purok_no`, not `purok`) |
| `consumers.contact_number` | `contact_number` | `VARCHAR(20)` | `VARCHAR` | Direct (stored as text, not integer) |
| `consumption.reading_date` | `reading_date` | `DATE` | `DATE` | Direct |
| `consumption.consumption` | `consumption` | `NUMERIC(10,2)` GENERATED | `DECIMAL(10,2)` | Type mapping (computed `present - previous`) |
| `billing.user_id` | `user_id` | `INTEGER` | `INTEGER` | Direct (FK to `consumers.id`) |
| `payments.payment_date` | `payment_date` | `DATE` | `DATE` | Direct |
| `*.created_at` / `*.updated_at` | `created_at` / `updated_at` | `TIMESTAMPTZ` | `TIMESTAMP WITH TIME ZONE` | Type mapping (ISO 8601 with timezone) |

---

## 7. Extraction Acceptance and Handover Rules

Each rule defines: validation condition, acceptance criteria, pipeline action, automatic vs. manual decision, and responsible role for manual approval.

### 7.1 Decision Rules

| Overall Condition | Acceptance Decision | Pipeline Action |
| --- | --- | --- |
| All required tables PASS | APPROVED | Proceed to Transformation |
| One or more non-critical WARNING | MANUAL REVIEW | Hold pipeline |
| One or more critical FAIL | REJECTED | Stop / retry / quarantine |

### 7.2 Required Decisions

- **Automatic Approval:** All critical validation checks pass (connection OK, schema OK, 0 NULL identifiers, 0 PK duplicates, counts match, 0 orphans). Pipeline promotes staged `rawdata` + metadata + validation results automatically.
- **Manual Review:** Non-critical warnings (e.g. minor count variance within tolerance, range-sanity flags for Transformation to clean). Responsible: **data engineer / Super Admin**. Action: hold promotion until reviewed and explicitly released.
- **Rejection or Failure:** Any critical FAIL (connection loss after retries, missing table/column, PK duplicates, orphan FKs, zero-extract when source non-zero). Pipeline retries transient failures (up to 3×), otherwise quarantines the affected table extract and stops promotion. Responsible: **data engineer** investigates via §4 logs and re-triggers the window.

### 7.3 Handover

What is passed to the next stage — Transformation (`PostgreSQL SQL` per Lab 2) → Storage (`Existing Supabase/PostgreSQL`) → consumption via dashboard, analytics, PDF reports, demand forecasting, and administrative recommendations:

- Staged raw datasets: `rawdata/consumers|consumption|billing|payments/<run_date>/` with agreed file format (e.g. Parquet/CSV + schema file).
- Extraction metadata per §4 (run ID, window, counts, duration).
- Validation results per §3 (per-table PASS/WARNING/FAIL + rejected-row quarantine location).
- Processing-period and grouping config carried forward per Lab 2: selected monthly or yearly period, overall or purok-level grouping.
- Example handover summary:

| Validation Summary | Result |
| --- | --- |
| consumers table validation | PASS |
| consumption table validation | PASS |
| billing table validation | WARNING |
| payments table validation | PASS |
| Overall Extraction | MANUAL REVIEW |

> Handover contains validated raw data only. Data cleaning, standardization, and business transformations are performed in the Transformation stage, not during extraction.

---

## 8. Common Extraction Problems and Mitigation

| # | Problem description | Possible cause | Impact on extraction | Proposed solution / mitigation |
| --- | --- | --- | --- | --- |
| 1 | Supabase/PostgreSQL unreachable or connection timeout | Network outage, wrong credentials, Supabase maintenance | Run fails; no records extracted; downstream stages starved | Retry with exponential backoff (3×) via Airflow; alert on final failure; store last window for safe re-run; rotate/verify service-role credentials out-of-band |
| 2 | Schema change breaks extractor (renamed column, new NOT NULL, changed type) | Application migration (e.g. new `system_settings` or consumer columns) without pipeline update | `Required tables/columns` check FAILs; run stopped | Fail fast + quarantine; version the data contract (§6); update extractor mapping; re-run the same window after fix |
| 3 | Partial extract / Airflow task killed mid-run | Worker OOM, timeout on large `consumption`/`billing` history | Staging has incomplete month; counts mismatch | Idempotent re-run of the same window using `idempotency_key`; validate `records_read == records_extracted` before promotion |
| 4 | Missing or late monthly readings (consumer has no reading for the month) | Meter reader has not yet recorded; offline outbox not yet synced | Analytical gaps for that purok/resident; completeness WARNING | Flag as WARNING for manual review; do not fabricate readings; Transformation handles imputation policy; re-extract the window after readings sync |
| 5 | Duplicate or out-of-order payment/billing rows from retries | Client retry without idempotency key, double-submit | PK/idempotency conflicts; inflated collections | Enforce `idempotency_key` uniqueness; deduplicate on `(billing_id, payment_date, total_paid)`; quarantine duplicates for review |

---

## Expected Output

The documentation must be consistent with the implemented extraction process and provide sufficient information for team member to understand, validate, monitor, and maintain the extraction stage.

The extracted data must be validated and packaged for handover to the next stage: Transformation. Data cleaning, standardization, and business transformations must not be performed unless explicitly required by the extraction process.

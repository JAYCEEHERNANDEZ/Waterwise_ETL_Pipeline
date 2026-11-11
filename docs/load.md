# WaterWise: Operational and Analytical Batch Data Pipeline

## Stage 3: Data Load Documentation

### Objective

The objective of the WaterWise Data Load Stage is to load the validated, transformed dataset handed over from the Transformation Stage (`docs/transformation.md`) into the WaterWise warehouse database, and to confirm that the loaded data is complete, accurate, traceable, and ready for dashboards, reports, and analysis.

This pipeline is **batch only**. Data is loaded in scheduled, bounded batches. Streaming, micro-batch, and real-time loading are out of scope.

```
Main DB (Supabase)  →  Stage 1 Extraction  →  Stage 2 Transformation  →  Stage 3 Load  →  Warehouse DB (second Supabase project)
     read only             rawdata/               transformed/            this stage          written only by this stage
```

---

## 1. Load Source and Target Specification

### 1.1 Load Input

| Item | Description |
| --- | --- |
| **Input origin** | Stage 2 handover only. This stage does **not** connect to the main WaterWise database, does not re-extract, and does not re-transform. |
| **Transformed datasets** | `transformed/<table>/<run_date>/<table>_<run_date>.csv` (UTF-8 CSV with header) for `dim_purok`, `dim_consumer`, `fact_consumption`, `fact_billing`, `fact_payment` |
| **Schema sidecar** | `transformed/<table>/<run_date>/<table>_<run_date>.schema.json` — column list, NOT NULL columns, `rules_version`, `run_id`, `parent_run_id` |
| **Transformation run logs** | `logs/waterwise-transformation/<run_date>/<run_id>_<table>.json` — `transformation_status`, `records_output`, `output_checksums`, batch window |
| **Transformation run ID** | `run_id` of the Stage 2 run, e.g. `ww-transform-2026-03-10-0200-7b3e42`. Stored in the warehouse as `transformation_run_id`. |
| **Batch ID** | One batch = one transformation run date: `ww-batch-<run_date>`, e.g. `ww-batch-2026-03-10` |
| **Batch window** | `batch_window_start` to `batch_window_end` from the transformation log, e.g. `2026-03-10T00:00:00+08:00` to `2026-03-10T23:59:59+08:00` |

Each daily batch is a **complete snapshot**: the scheduled extraction runs in full mode (`docs/extraction.md` §1.4), so every handover contains all current consumers, readings, bills, and payments.

### 1.2 Target System

| Item | Description |
| --- | --- |
| **Target name** | WaterWise Warehouse |
| **Purpose** | Analytical database for dashboards, PDF reports, demand forecasting, and administrative recommendations. Kept separate from the main application database so analytical queries and loads can never affect billing and payment operations. |
| **Hosting** | A second Supabase project, separate from the main WaterWise project |
| **Database management system** | PostgreSQL (Supabase) |
| **Target schema** | `warehouse` (a dedicated schema, not `public`, so the tables are not exposed through the Supabase auto-generated API) |
| **Schema definition** | `db/warehouse_schema.sql` |

### 1.3 Load Method

| Item | Description |
| --- | --- |
| **Load tool** | Python 3.10+ with `psycopg2` — `pipeline/load.py` |
| **Orchestration** | Apache Airflow — DAG `waterwise_daily_load` |
| **Frequency / schedule** | Daily at 03:00 Asia/Manila, after the 02:00 transformation completes |
| **Connection** | `WAREHOUSE_DB_URL` environment variable (Supabase session pooler). Stored only in `pipeline/.env`, which is not committed. |
| **How data is written** | **Bulk load + merge.** Each CSV is bulk-inserted into a temporary staging table, validated there, then merged into the target table with one `INSERT ... ON CONFLICT ... DO UPDATE` (upsert) statement per table. No row-by-row inserts. |

### 1.4 Load Scope

Tables are loaded in dependency order — dimensions before facts, parents before children:

```
dim_purok → dim_consumer → fact_consumption → fact_billing → fact_payment
```

| Item | Value |
| --- | --- |
| **Tables loaded** | The five tables above, plus one `load_log` row per table per run |
| **Expected records per batch** | Up to about 50,000 rows in total across all tables (a full snapshot of one barangay water system). `dim_purok` is under 20 rows. |
| **Time window** | The load must finish within **15 minutes** (by 03:15), well before office hours when dashboards are used. A run longer than 15 minutes is reported as a warning. |

### 1.5 Limitations and Assumptions

**Limitations:**

- The load depends on the availability of the warehouse Supabase project and on network access to it.
- The Supabase free plan limits the database to **500 MB** and pauses a project after about 7 days without activity. The daily load keeps the project active.
- The free plan has no automatic backups (see Section 7.4).
- The connection goes through the Supabase pooler, which limits the number of connections. The load uses one connection for data and one for logging.

**Assumptions:**

- The load runs off-peak (03:00). Dashboards only read, and PostgreSQL readers are not blocked by the load, so no locking window is needed.
- Deterministic surrogate keys from Stage 2 (`docs/transformation.md` §1.4) are stable, so the same record always has the same key in every batch.
- The handover is trusted for content but verified for integrity: this stage checks counts, checksums, and constraints, and reports problems — it never repairs data.

**Load account:**

| Role | Used by | Privileges |
| --- | --- | --- |
| `etl_loader` | `pipeline/load.py` | `USAGE` on schema `warehouse`; `SELECT`, `INSERT`, `UPDATE` on the dimension and fact tables; `SELECT`, `INSERT` on `load_log` (append-only: no `UPDATE`, no `DELETE`); temporary tables. No `DELETE`, no DDL, no access to any other schema, and no access to the main database. |
| `warehouse_reader` | Dashboards and reports | `SELECT` only on schema `warehouse` |
| Project owner (`postgres`) | Data engineer, manually | Runs `db/warehouse_schema.sql` and maintenance. Not used by the pipeline. |

Passwords and connection strings are never written in this document, in code, or in logs.

> **Requirement:** the load uses only the data handed over from the Transformation Stage. Nothing is re-extracted or re-transformed in this stage.

---

## 2. Target Tables and Column Specification

All tables live in the `warehouse` schema. Column names and types follow the transformed data contract (`docs/transformation.md` §6.1); only the columns important to loading are listed here.

### 2.1 Overview

| Table | Type | Primary Key | Foreign Keys | Key Constraints | Purpose |
| --- | --- | --- | --- | --- | --- |
| `dim_purok` | Dimension | `purok_key` | None | `purok_no` UNIQUE, NOT NULL | Zones (puroks) for per-area reporting |
| `dim_consumer` | Dimension | `consumer_key` | `purok_key` → `dim_purok` | `consumer_id` UNIQUE, NOT NULL; `status` in code set | Consumer attributes for reporting |
| `fact_consumption` | Fact | `consumption_key` | `consumer_key` → `dim_consumer` | `consumption_id` UNIQUE; `consumption >= 0`; `present_reading >= previous_reading` | Monthly meter readings |
| `fact_billing` | Fact | `billing_key` | `consumption_key` → `fact_consumption`; `consumer_key` → `dim_consumer` | `billing_id` UNIQUE; `total_bill >= 0`; `remaining_balance >= 0` | Bills, balances, and aging |
| `fact_payment` | Fact | `payment_key` | `billing_key` → `fact_billing` | `payment_id` UNIQUE; `total_paid >= 0` | Payment transactions |
| `load_log` | Log / audit | `load_log_id` | None | `run_id`, `batch_id`, `table_name`, `status` NOT NULL | One row per table per load run (Section 6) |

### 2.2 Columns Added by the Load Stage

Every dimension and fact table has two extra columns that do not exist in the handover:

| Column | Type | Constraint | Purpose |
| --- | --- | --- | --- |
| `load_batch_id` | `VARCHAR(64)` | NOT NULL | The batch that last wrote this row (**batch ID column**). Used by post-load checks and for tracing. |
| `loaded_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | When the row was last written |

Every row also keeps `transformation_run_id` from the handover, so a warehouse row can be traced back to its transformation run and, through `parent_run_id`, to its extraction run.

### 2.3 `dim_purok`

| Column | Type | Constraint | Notes |
| --- | --- | --- | --- |
| `purok_key` | `CHAR(16)` | PK | Surrogate key from Stage 2 |
| `purok_no` | `INTEGER` | UNIQUE, NOT NULL | **Match key.** `0` = UNKNOWN zone |
| `purok_label` | `VARCHAR(50)` | NOT NULL | Display name |
| `is_unknown` | `BOOLEAN` | NOT NULL | `TRUE` for the UNKNOWN row |

**Reason for inclusion:** every per-purok total in dashboards groups by this table.

### 2.4 `dim_consumer`

| Column | Type | Constraint | Notes |
| --- | --- | --- | --- |
| `consumer_key` | `CHAR(16)` | PK | Surrogate key |
| `consumer_id` | `INTEGER` | UNIQUE, NOT NULL | **Match key** (natural key from the source) |
| `full_name`, `username`, `email` | `VARCHAR` | NOT NULL | Identity |
| `middle_name`, `contact_number` | `VARCHAR` | NULL allowed | Optional |
| `purok_key` | `CHAR(16)` | FK → `dim_purok.purok_key`, NOT NULL | Zone of the consumer |
| `status` | `VARCHAR(10)` | NOT NULL, CHECK in (`ACTIVE`, `INACTIVE`, `UNKNOWN`) | Overwritten when it changes (SCD Type 1) |
| `active_flag` | `BOOLEAN` | NOT NULL | Soft flag for inactive consumers |
| `valid_from`, `valid_to`, `is_current` | `TIMESTAMPTZ`, `TIMESTAMPTZ`, `BOOLEAN` | `valid_from`, `is_current` NOT NULL | History columns delivered by Stage 2. Under SCD Type 1 there is one row per consumer and `is_current` is always `TRUE`. |

**Index:** `purok_key`. **Reason for inclusion:** conformed dimension shared by all three fact tables.

### 2.5 `fact_consumption`

| Column | Type | Constraint | Notes |
| --- | --- | --- | --- |
| `consumption_key` | `CHAR(16)` | PK | Surrogate key |
| `consumption_id` | `INTEGER` | UNIQUE, NOT NULL | **Match key** |
| `consumer_key` | `CHAR(16)` | FK → `dim_consumer`, NOT NULL | Owner of the reading |
| `reading_date`, `billing_period` | `DATE`, `CHAR(7)` | NOT NULL | Reading date and `YYYY-MM` period |
| `previous_reading`, `present_reading` | `NUMERIC(12,2)` | NOT NULL, CHECK `present_reading >= previous_reading` | Meter values |
| `consumption` | `NUMERIC(12,2)` | NOT NULL, CHECK `>= 0` | Cubic meters; used in the aggregate check |

**Indexes:** `consumer_key`, `billing_period`, `load_batch_id`. **Reason for inclusion:** core fact for consumption analysis and forecasting.

### 2.6 `fact_billing`

| Column | Type | Constraint | Notes |
| --- | --- | --- | --- |
| `billing_key` | `CHAR(16)` | PK | Surrogate key |
| `billing_id` | `INTEGER` | UNIQUE, NOT NULL | **Match key** |
| `consumption_key` | `CHAR(16)` | FK → `fact_consumption`, NOT NULL | Reading that was billed |
| `consumer_key` | `CHAR(16)` | FK → `dim_consumer`, NOT NULL | Billed consumer |
| `billing_date`, `due_date`, `billing_period` | `DATE`, `DATE`, `CHAR(7)` | NOT NULL | Dates |
| `total_bill` | `NUMERIC(12,2)` | NOT NULL, CHECK `>= 0` | Used in the aggregate check |
| `remaining_balance` | `NUMERIC(12,2)` | NOT NULL, CHECK `>= 0` | Outstanding amount |
| `status` | `VARCHAR(10)` | NOT NULL, CHECK in (`PAID`, `PARTIAL`, `UNPAID`, `UNKNOWN`) | Bill status |
| `days_overdue`, `overdue_bucket`, `outstanding_bill_count`, `is_flagged_account` | — | NOT NULL | Aging values computed by Stage 2 for the batch reference date; overwritten by each batch |

**Indexes:** `(consumer_key, status, due_date)`, `billing_period`, `load_batch_id`. **Reason for inclusion:** billing history, balances, and flagged-account reporting.

### 2.7 `fact_payment`

| Column | Type | Constraint | Notes |
| --- | --- | --- | --- |
| `payment_key` | `CHAR(16)` | PK | Surrogate key |
| `payment_id` | `INTEGER` | UNIQUE, NOT NULL | **Match key** |
| `billing_key` | `CHAR(16)` | FK → `fact_billing`, NOT NULL | Bill that was paid |
| `payment_date` | `DATE` | NOT NULL | Date of payment |
| `payment_method` | `VARCHAR(20)` | NOT NULL, CHECK in (`CASH`, `GCASH`, `BANK_TRANSFER`, `OTHER`) | Method code |
| `total_paid` | `NUMERIC(12,2)` | NOT NULL, CHECK `>= 0` | Used in the aggregate check |
| `reference_number`, `collection_rate` | `VARCHAR(100)`, `NUMERIC(9,4)` | NULL allowed | Optional |

**Indexes:** `(billing_key, payment_date)`, `load_batch_id`. **Reason for inclusion:** collection monitoring and revenue reporting.

### 2.8 `load_log`

Append-only audit table with one row per target table per load run. Fields are defined in Section 6. **Indexes:** `(batch_id, status)` for the duplicate-batch check, `run_id`.

A view, `warehouse.latest_successful_load`, shows the most recent approved batch per table so dashboards can display "data as of".

---

## 3. Load Strategy and Rules

### 3.1 Strategy per Table

| Table | Load Type | Write Logic | Match Key | Changing Data | On Failure |
| --- | --- | --- | --- | --- | --- |
| `dim_purok` | Incremental | Upsert | `purok_no` | Overwrite (SCD Type 1) | Roll back whole batch, stop |
| `dim_consumer` | Incremental | Upsert | `consumer_id` | Overwrite (SCD Type 1) | Roll back whole batch, stop |
| `fact_consumption` | Incremental | Upsert | `consumption_id` | Overwrite with latest values | Roll back whole batch, stop |
| `fact_billing` | Incremental | Upsert | `billing_id` | Overwrite with latest values | Roll back whole batch, stop |
| `fact_payment` | Incremental | Upsert | `payment_id` | Overwrite with latest values | Roll back whole batch, stop |
| `load_log` | Append | Insert only | — | Never changed | Written in its own transaction |

**Why incremental upsert and not truncate-and-reload:** the handover is a full snapshot, but a truncate would leave the tables empty for dashboards if the load failed midway, and it would delete warehouse rows whose source record was later quarantined. Upsert inserts new records, updates existing ones, and never removes history.

### 3.2 Write Logic

For each table, in load order:

1. Create a temporary staging table and bulk-insert the CSV rows into it.
2. Validate the staged rows against the target constraints (types, NOT NULL, CHECK, foreign key exists in the target). Rows that fail are **rejected**: they are not written, they are saved to the load quarantine (Section 7.2), and they are counted in `records_rejected`.
3. Merge the remaining rows into the target with `INSERT ... ON CONFLICT (<match key>) DO UPDATE`, setting `load_batch_id` and `loaded_at`.
4. Count the rows inserted and the rows updated.

### 3.3 Handling of Changing Data

| Case | Handling |
| --- | --- |
| **Changed attributes** (consumer status, bill status, balances, aging values) | **SCD Type 1 — overwrite.** The source keeps no status history, so there is nothing to build Type 2 history from (`docs/transformation.md` §5.4, D-06). |
| **Deleted or inactive records** | Inactive consumers arrive with `status = INACTIVE` and `active_flag = FALSE` and stay in the warehouse (soft delete). A record that is missing from a later batch is **kept**, never hard-deleted; it is recognizable by an older `load_batch_id`. The count of such rows is written to the load log. |
| **Late-arriving records** | Because every batch is a full snapshot, a late reading or payment simply appears in a later batch and is inserted by the upsert. No special path is needed. |
| **Handover schema differs from the target** | **Reject the batch.** The pre-load schema check (Section 4) compares the handover columns with the target tables; any difference is a critical failure and nothing is written. The data contract is then updated in both stages and the batch is rerun. |

### 3.4 Load Order and Dependencies

- Dimensions are loaded before facts, and each fact after the fact it references (order in Section 1.4).
- **Facts with no matching parent record are rejected**, not attached to an "Unknown" member. Stage 2 already guarantees referential integrity (its check V-06), so an orphan at load time means the handover is damaged. Rejected rows are counted and make the batch fail or go to review (Section 8).
- The only "Unknown" member is the UNKNOWN purok (`purok_no = 0`), which Stage 2 delivers as a normal `dim_purok` row.

### 3.5 Transaction and Rollback Handling

- **The whole batch — all five tables and the post-load checks — runs in one database transaction.**
- The transaction is committed only when the acceptance decision is APPROVED (Section 8). In every other case it is rolled back, so the warehouse is left exactly as it was before the run. No partial data can remain.
- If the process or the connection dies midway, PostgreSQL discards the open transaction automatically.
- `load_log` rows are written on a **separate connection** after the decision, so failed and rolled-back runs are still recorded. If the warehouse cannot be reached at all, the log is still written to the JSON log files (Section 7.1).

### 3.6 Rerun and Idempotency

- Running the same batch twice cannot create duplicates or inflate totals: the upsert matches on the natural key, so the second run updates the same rows with the same values.
- The pre-load duplicate-batch check stops a batch that was already loaded successfully. An intentional reload uses `--force-reload`, which is safe for the reason above.
- **Recovery of a failed batch:** fix the cause and run the same batch again (`python -m pipeline.load --run-date <date>`). A failed run left nothing behind, so the rerun starts from a clean state.

---

## 4. Pre-Load Validation Checks

These checks run before any data is written. A critical failure stops the load; nothing is written to the dimension or fact tables.

| ID | Check Name | Target | Purpose | Validation Criteria | Severity |
| --- | --- | --- | --- | --- | --- |
| PRE-01 | Target connection | Warehouse database | Ensure the load can start | PASS: connection succeeds within 10 seconds (after up to 3 attempts). FAIL: otherwise | Critical |
| PRE-02 | Write permission | `warehouse` schema, role `etl_loader` | Avoid failing halfway for missing privileges | PASS: role has `INSERT` and `UPDATE` on all five tables and `INSERT` on `load_log`. FAIL: any privilege missing | Critical |
| PRE-03 | Handover completeness | `transformed/<table>/<run_date>/` | Make sure the whole batch arrived | PASS: CSV, `.schema.json`, and transformation log exist for all five tables. FAIL: any file missing | Critical |
| PRE-04 | Handover status | Transformation logs | Load approved data only | PASS: `transformation_status = APPROVED` for all tables, or the batch was released by a named reviewer (`--handover-released-by`). FAIL: `REJECTED`, or `MANUAL REVIEW` without a release | Critical |
| PRE-05 | Schema match | All target tables vs. `.schema.json` | Prevent type and column errors | PASS: every handover column exists in the target table and every target NOT NULL column is in the handover. FAIL: any missing or extra column | Critical |
| PRE-06 | Record count | CSV row count vs. `records_output` | Detect a truncated or changed file | PASS: counts are equal for every table. FAIL: any difference | Critical |
| PRE-07 | File integrity | CSV SHA-256 vs. `output_checksums` | Prove the files are the ones Stage 2 validated | PASS: checksums are equal for every table. FAIL: any difference | Critical |
| PRE-08 | Duplicate batch | `load_log (batch_id, transformation_run_id)` | Avoid loading the same batch twice | PASS: no earlier `SUCCESS` for this batch and transformation run, or `--force-reload` is given. FAIL: already loaded | Critical |
| PRE-09 | Storage space | `pg_database_size()` vs. the 500 MB plan limit | Avoid running out of space mid-load | PASS: below 80% used. WARNING: 80% to 95%. FAIL: above 95% | Critical above 95%; Non-critical from 80% |
| PRE-10 | Batch not empty | CSV row counts | Avoid "successfully" loading nothing | PASS: `dim_consumer` and `dim_purok` have at least 1 row. FAIL: 0 rows | Critical |

Note on PRE-04: the transformation writes its CSV files even when it rejects a batch, so the existence of the files proves nothing. The status in the log is what counts.

---

## 5. Post-Load Validation and Reconciliation

These checks run **inside the load transaction**, after all five tables are written and before the commit, so a critical failure can still roll everything back.

| ID | Check Name | Target | Purpose | Validation Criteria | Severity |
| --- | --- | --- | --- | --- | --- |
| POST-01 | Record reconciliation | All target tables | Detect lost or extra records | PASS: `records_received = records_inserted + records_updated + records_rejected` for every table. FAIL: any difference | Critical |
| POST-02 | Batch row count | Rows with this `load_batch_id` | Confirm the rows are really in the target | PASS: count in target = `records_received - records_rejected` for every table. FAIL: any difference | Critical |
| POST-03 | Key uniqueness | Primary key and natural key of every table | Prevent duplicates | PASS: duplicate count = 0. FAIL: more than 0 | Critical |
| POST-04 | Referential integrity | `dim_consumer.purok_key`, `fact_consumption.consumer_key`, `fact_billing.consumption_key`, `fact_billing.consumer_key`, `fact_payment.billing_key` | Prevent orphan rows | PASS: 100% of keys match the parent table. FAIL: any orphan | Critical |
| POST-05 | Required columns | NOT NULL columns of the contract | Data completeness | PASS: 0 NULLs. FAIL: more than 0 | Critical |
| POST-06 | Amount reconciliation | `SUM(consumption)`, `SUM(total_bill)`, `SUM(total_paid)` | Confirm no value was lost or changed | PASS: sum in the CSV (minus rejected rows) = sum in the target for this batch, within 0.01. FAIL: otherwise | Critical |
| POST-07 | Rejected records | `records_rejected` per table | Decide acceptance | PASS: 0 rejected. WARNING: more than 0 and at most 1.0% of the table. FAIL: more than 1.0% | Critical above 1.0%; Non-critical up to 1.0% |
| POST-08 | Optional column null rate | `dim_consumer.middle_name`, `dim_consumer.contact_number`, `fact_payment.reference_number` | Watch data completeness over time | PASS: null rate at most 90%. WARNING: above 90% | Non-critical |
| POST-09 | Load duration | Whole run | Keep the load inside its time window | PASS: at most 15 minutes. WARNING: longer | Non-critical |
| POST-10 | Load errors or interruptions | Load process | Catch database errors and crashes | PASS: no exception. FAIL: any database error, timeout, or interruption (transaction is rolled back) | Critical |

In POST-01, *rejected* means rows the load did not write (Section 3.2, step 2). Rows removed earlier by the Transformation Stage are not counted here; they belong to the Stage 2 reconciliation.

> **Scope limitation:** this stage performs no cleaning and no business transformation. A problem found here is reported and handled by the rules in Section 8; the fix is made in the stage that caused it.

---

## 6. Load Metadata and Log Specification

One record is written per target table per load run, to `warehouse.load_log` and to a JSON file (Section 7.1).

| Field | Purpose | Example |
| --- | --- | --- |
| `run_id` | Unique ID of the load run | `ww-load-2026-03-10-0300-a1b2c3` |
| `transformation_run_id` | Link to the transformation run | `ww-transform-2026-03-10-0200-7b3e42` |
| `batch_id` | Batch being loaded | `ww-batch-2026-03-10` |
| `batch_window_start`, `batch_window_end` | Date range covered by the batch | `2026-03-10T00:00:00+08:00` to `2026-03-10T23:59:59+08:00` |
| `table_name` | Target table | `fact_billing` |
| `load_type` | Full or incremental load | `INCREMENTAL` |
| `load_start_ts` | When the table load started | `2026-03-10T03:00:04+08:00` |
| `load_end_ts` | When the table load ended | `2026-03-10T03:00:19+08:00` |
| `load_duration_sec` | Time taken to load the table | `15` |
| `records_received` | Rows received from transformation | `12395` |
| `records_inserted` | New rows written | `210` |
| `records_updated` | Existing rows overwritten | `12185` |
| `records_rejected` | Rows the load did not write | `0` |
| `status` | Result for this table | `SUCCESS` / `WARNING` / `FAILED` / `ROLLED_BACK` |
| `decision` | Overall batch decision (Section 8) | `APPROVED` / `MANUAL REVIEW` / `REJECTED` |
| `pre_load_result` | Outcome of the pre-load checks | `PASS` or `FAIL: PRE-07` |
| `post_load_result` | Outcome of the post-load checks | `PASS` or `WARNING: POST-08` |
| `error_code` | Failure or rejection reason, if any | `NULL` / `PRE04_HANDOVER_NOT_APPROVED` / `DB_CONNECTION` |
| `error_message` | Error detail, without credentials | `NULL` |
| `reviewed_by` | Reviewer who released a held batch, if any | `NULL` |
| `source_file`, `source_checksum` | Handover file that was loaded | `transformed/fact_billing/2026-03-10/fact_billing_2026-03-10.csv`, `9f2c…` |
| `load_code_version` | Version of the load code | `1.0` |

`status = ROLLED_BACK` marks a table that loaded correctly but was undone because another table or check failed.

**Example log record (JSON):**

```json
{
  "run_id": "ww-load-2026-03-10-0300-a1b2c3",
  "transformation_run_id": "ww-transform-2026-03-10-0200-7b3e42",
  "batch_id": "ww-batch-2026-03-10",
  "batch_window_start": "2026-03-10T00:00:00+08:00",
  "batch_window_end": "2026-03-10T23:59:59+08:00",
  "table_name": "fact_billing",
  "load_type": "INCREMENTAL",
  "load_start_ts": "2026-03-10T03:00:04+08:00",
  "load_end_ts": "2026-03-10T03:00:19+08:00",
  "load_duration_sec": 15,
  "records_received": 12395,
  "records_inserted": 210,
  "records_updated": 12185,
  "records_rejected": 0,
  "status": "SUCCESS",
  "decision": "APPROVED",
  "pre_load_result": "PASS",
  "post_load_result": "PASS",
  "error_code": null,
  "error_message": null,
  "reviewed_by": null,
  "source_file": "transformed/fact_billing/2026-03-10/fact_billing_2026-03-10.csv",
  "source_checksum": "9f2c4a1d8b7e6f5a3c2d1b0e9f8a7b6c",
  "load_code_version": "1.0"
}
```

**How the metadata and logs are used:**

- **Monitoring:** the Airflow UI shows each run as success or failure; `load_log` shows status, counts, and duration per table, and `load_duration_sec` shows when loads are getting slower.
- **Troubleshooting:** `pre_load_result`, `post_load_result`, and `error_code` name the exact check that failed; the counts show which table lost or rejected rows.
- **Auditing:** `run_id`, `batch_id`, `source_checksum`, and `load_code_version` prove which file was loaded, when, and by which code version. `reviewed_by` records who released a held batch.
- **Data lineage:** warehouse row → `load_batch_id` → `load_log` → `transformation_run_id` → Stage 2 log → `parent_run_id` → Stage 1 log → source table.
- **Recovery of a failed batch:** a `FAILED` or `ROLLED_BACK` record means nothing was committed, so the same `batch_id` can be rerun after the fix. The last `SUCCESS` record shows which batch the warehouse currently holds.

---

## 7. Log Retention, Rejected Data, and Access

### 7.1 Load Logs

| Item | Policy |
| --- | --- |
| **Operational logs** | JSON files in `logs/waterwise-load/<YYYY-MM-DD>/<run_id>_<table>.json`, plus the Airflow task log. Retained for a minimum of **90 days**. |
| **Audit records** | Rows in `warehouse.load_log`. Retained for a minimum of **1 year**. |
| **Justification** | Same periods as Stages 1 and 2, so a batch can be traced through all three stages for as long as any one of them keeps its record. One year covers a full cycle of monthly billing and the flagged-account rule (3 or more outstanding bills). |
| **Archiving** | JSON logs older than 90 days are compressed and moved to cold storage. `load_log` rows stay online for 1 year. |
| **Deletion rules** | Nothing is deleted before its retention period ends, or while it is needed for troubleshooting, auditing, or recovery. Deletion after that is done by the data engineer, with approval, and is itself recorded. |

### 7.2 Rejected Records

| Item | Policy |
| --- | --- |
| **Location** | `quarantine/load/<run_date>/rejected.csv` |
| **Content** | `run_id`, `batch_id`, `table_name`, `reject_reason` (for example `FK_NOT_FOUND: billing_key`), the key of the row, and the values needed to diagnose it. Email and contact number are masked. |
| **Retention** | Minimum **1 year**, because a rejected bill or payment is a financial record whose rejection may need to be explained later. |
| **Reprocessing** | Rejected rows are never loaded by hand. The cause is fixed in the stage that produced it and the batch is rerun. |

### 7.3 Access Permissions

| Who | Read | Write | Delete |
| --- | --- | --- | --- |
| Data engineer | Logs, `load_log`, quarantine | — | After retention, with approval |
| Super Admin | Logs, `load_log`, quarantine | — | Approves deletion |
| Load process (`etl_loader`, Airflow worker) | `load_log` | Appends logs and quarantine files | Never |
| Dashboards (`warehouse_reader`) | `load_log` and the `latest_successful_load` view | — | Never |
| Consumers, meter readers, billing staff | No access | — | — |

### 7.4 Backup Before Major Loads

- **Daily loads** need no snapshot: the single transaction protects the warehouse, and the warehouse can be rebuilt at any time by reloading a transformation handover.
- **Major loads** — a schema change, a `--force-reload`, or a backfill of old batches — require the data engineer to take a `pg_dump` of the `warehouse` schema first, because the Supabase free plan has no automatic backups. The dump is kept until the load has been verified.

---

## 8. Load Acceptance, Rollback, and Completion Rules

### 8.1 Decision Rules

| Overall Condition | Acceptance Decision | Pipeline Action | Decision | Responsible |
| --- | --- | --- | --- | --- |
| All pre-load and post-load checks PASS | **APPROVED** | Commit the transaction, write `load_log` as `SUCCESS`, report completion | Automatic | — |
| No critical FAIL, one or more non-critical WARNING | **MANUAL REVIEW** | Roll back (nothing is published), write `load_log`, notify the owner. After review the batch is rerun with `--accept-warnings "<reviewer>"` and then committed | Manual | Data engineer / Super Admin |
| One or more critical FAIL | **REJECTED** | Roll back, write `load_log` as `FAILED`, stop | Automatic | Data engineer investigates |

### 8.2 Rules in Detail

| Rule | Validation condition | Acceptance criteria | Pipeline action |
| --- | --- | --- | --- |
| **Automatic approval** | PRE-01 to PRE-10 and POST-01 to POST-10 | Every check PASS: handover approved, counts and sums reconcile, 0 rejected rows | Commit and mark the batch complete |
| **Manual review** | PRE-09, POST-07, POST-08, POST-09 | A warning only: storage at 80–95%, rejected rows at most 1.0% of a table, optional-column null rate above 90%, or run longer than 15 minutes | Hold: roll back and wait for the reviewer. The reviewer checks the log and the quarantine file, then either reruns with `--accept-warnings` (recorded in `reviewed_by`) or leaves the batch rejected |
| **Rejection or failure** | Any critical check | Handover not approved, schema mismatch, count or checksum mismatch, duplicate batch, reconciliation or sum mismatch, duplicate keys, orphans, NULL in a required column, more than 1.0% rejected, or a database error | Roll back and stop |

**Retries:**

- **Connection or network errors** (warehouse unreachable, timeout, dropped connection): the task is retried automatically, **up to 3 retries, 5 minutes apart**. A retry is safe because the failed attempt was rolled back.
- **Data or contract failures** (any other critical FAIL): **not retried**, because running again would fail the same way. The pipeline stops until the cause is fixed and the batch is rerun.

**Notification on failure or rejection:** the Airflow task is marked failed, which alerts the **data engineer** (Airflow UI, and email when SMTP is configured). The run summary and the `load_log` record name the failed check. The Super Admin is informed when published data will be more than one day late.

### 8.3 Completion and Handover

When a batch completes, the load reports:

- the final decision and `run_id`, `batch_id`, `transformation_run_id`;
- per table: records received, inserted, updated, rejected, and status;
- the data now available: the five `warehouse` tables, current as of the batch window.

**Example completion summary:**

| Validation Summary | Result |
| --- | --- |
| `dim_purok` load | PASS |
| `dim_consumer` load | PASS |
| `fact_consumption` load | PASS |
| `fact_billing` load | PASS |
| `fact_payment` load | WARNING |
| **Overall Load** | **MANUAL REVIEW** |

**How downstream users know new data is available:** dashboards and reports read `warehouse.latest_successful_load`, which shows the latest approved batch and its load time, and display it as "data as of". A batch that is held or rejected never appears there, so users keep seeing the previous complete batch.

---

## 9. Common Load Problems and Mitigation

| # | Problem | Possible Cause | Impact | Mitigation |
| --- | --- | --- | --- | --- |
| 1 | Duplicate rows or inflated totals after a rerun | Airflow retry, or a batch started twice by hand | Double-counted consumption, billing, and collections | Upsert on the natural key makes reruns harmless; PRE-08 stops an already-loaded batch; POST-03 and POST-06 confirm no duplicates and equal sums |
| 2 | Foreign key violation | A fact loaded before its parent, or a parent row missing from the handover | Load error or orphan facts | Fixed load order (Section 1.4); orphans are rejected and counted before the merge; POST-04 verifies 100% match |
| 3 | Partial load after a crash | Worker killed or connection dropped in the middle of the batch | Dimensions updated but facts not: inconsistent dashboards | The whole batch is one transaction; PostgreSQL discards it if the connection dies; the rerun starts clean |
| 4 | Warehouse unreachable or project paused | Supabase pauses free projects after about 7 days without activity; network outage; maintenance | Load cannot start; dashboards also down | PRE-01 with 3 connection attempts, then Airflow retries (3 × 5 minutes); the daily load keeps the project active; if paused, the data engineer resumes it in the Supabase dashboard and reruns the batch |
| 5 | Connection refused or dropped by the pooler | Wrong pooler mode or port, direct (IPv6-only) host used from Docker, or special characters in the password not URL-encoded | Load fails at PRE-01, or drops mid-transaction | Use the session pooler connection string; URL-encode the password (`DOCKER.md`); the load uses only two connections |
| 6 | Database reaches the 500 MB plan limit | Growth of fact tables and `load_log` over time | Supabase puts the database in read-only mode; loads fail | PRE-09 warns at 80% and stops at 95%; `load_log` rows are archived after 1 year; upgrade the plan or archive old periods before the limit is reached |
| 7 | Handover schema does not match the warehouse | Stage 2 added, renamed, or retyped a column without updating `db/warehouse_schema.sql` | Insert errors, or values in the wrong column | PRE-05 rejects the batch before anything is written; the contract and the schema script are updated together, then the batch is rerun |
| 8 | Handover file changed or incomplete after validation | Transformation rerun while the load was starting, interrupted copy, or manual edit | Unvalidated data could reach the warehouse | PRE-06 (row count) and PRE-07 (SHA-256 checksum) compare the files with the transformation log |
| 9 | Loading a batch that the transformation rejected | The CSV files exist even when Stage 2 rejects or holds a batch | Wrong figures published | PRE-04 reads the status from the transformation log, never from the presence of files |
| 10 | Load runs too long or times out | Row-by-row inserts, or growth in data volume | Dashboards show old data in the morning; pooler may cut the connection | Bulk insert into staging plus one merge statement per table; indexes on match keys; POST-09 warns when a run exceeds 15 minutes |
| 11 | Missed day | Machine or Airflow was off at 03:00 | The warehouse stays one or more batches behind | `latest_successful_load` shows the data date; because each handover is a full snapshot, loading the newest batch brings the warehouse fully up to date |

---

## Expected Output

The documentation must be consistent with the implemented load process and provide sufficient information for a team member to understand, validate, monitor, and maintain the load stage.

The loaded data is validated, reconciled with the transformed input, and logged so that every batch can be traced, audited, and recovered. No extraction or transformation logic is added in this stage.

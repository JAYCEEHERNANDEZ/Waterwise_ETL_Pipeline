# WaterWise: Operational and Analytical Batch Data Pipeline

## Stage 3: Data Load Documentation

### Objective

The goal of this stage is to load the validated, transformed dataset handed over from the Transformation Stage (`docs/transformation.md`) into the WaterWise warehouse database, and to confirm that the loaded data is complete, accurate, traceable, and ready for use by reports, dashboards, or analysis.

This pipeline is **batch only**. Data is loaded in scheduled, bounded batches. Streaming, micro-batch, and real-time loading are out of scope.

---

## 1. Load Source and Target Specification

- **Load Input**
  - **Name and location of the transformed output:** CSV files (UTF-8, with header) received from the Transformation Stage at `transformed/<table>/<run_date>/<table>_<run_date>.csv`, for `dim_purok`, `dim_consumer`, `fact_consumption`, `fact_billing`, and `fact_payment`. Each file comes with a `.schema.json` file (column list and NOT NULL columns) and a transformation run log at `logs/waterwise-transformation/<run_date>/<run_id>_<table>.json` (status, record counts, file checksums).
  - **Transformation run ID:** the `run_id` of the Stage 2 run, e.g. `ww-transform-2026-03-10-0200-7b3e42`.
  - **Batch ID:** one batch per transformation run date, `ww-batch-<run_date>`, e.g. `ww-batch-2026-03-10`.
  - **Batch window:** `batch_window_start` to `batch_window_end` from the transformation log, e.g. `2026-03-10T00:00:00+08:00` to `2026-03-10T23:59:59+08:00`. Each daily batch is a complete snapshot, because the scheduled extraction runs in full mode (`docs/extraction.md` §1.4).
- **Target System**
  - **Name and purpose of the target:** WaterWise Warehouse — an analytical database for dashboards, PDF reports, demand forecasting, and administrative recommendations. It is a second Supabase project, separate from the main WaterWise application database, so loads and analytical queries can never affect billing and payment operations.
  - **Database management system:** PostgreSQL (Supabase).
  - **Target schema:** `warehouse`, created by `db/warehouse_schema.sql`.
- **Load Method**
  - **Load tool:** Python 3.10+ with `psycopg2` (`pipeline/load.py`), scheduled by Apache Airflow (DAG `waterwise_daily_load`).
  - **Batch frequency and schedule:** daily at 03:00 Asia/Manila, after the 02:00 transformation completes.
  - **How data is written:** bulk insert followed by merge/upsert. Each CSV is bulk-inserted into a temporary staging table, then merged into the target table with one `INSERT ... ON CONFLICT ... DO UPDATE` statement per table. No row-by-row inserts are used.
- **Load Scope**
  - **Tables loaded and order:** dimensions before facts, parents before children: `dim_purok` → `dim_consumer` → `fact_consumption` → `fact_billing` → `fact_payment`. One `load_log` row is written per table per run.
  - **Expected number of records and time window:** up to about 50,000 rows in total per batch (a full snapshot of one barangay water system). The load must finish within 15 minutes (by 03:15), before office hours when dashboards are used.
- **Limitations and Assumptions**
  - **Limitations:**
    - The load depends on the availability of the warehouse Supabase project and on network access to it.
    - The Supabase free plan limits the database to 500 MB of storage, has no automatic backups, and pauses a project after about 7 days without activity.
    - The connection goes through the Supabase pooler, which limits the number of connections; the load uses two.
    - The load runs off-peak (03:00). Dashboards only read data and are not blocked by the load, so no locking window during business hours is needed.
  - **Account or role used for loading:** role `etl_loader`. Privileges: `USAGE` on schema `warehouse`; `SELECT`, `INSERT`, `UPDATE` on the dimension and fact tables; `SELECT`, `INSERT` on `load_log`; temporary tables. It has no `DELETE`, no DDL, and no access to the main database. Dashboards use a separate read-only role, `warehouse_reader`. The connection string is kept in the `WAREHOUSE_DB_URL` environment variable and is never written in documentation, code, or logs.

**Requirement:** The load uses only the data handed over from the Transformation Stage. Nothing is re-extracted or re-transformed in this stage.

---

## 2. Target Tables and Column Specification

| Table | Type | Primary Key | Foreign Keys | Key Constraints | Purpose |
| --- | --- | --- | --- | --- | --- |
| `dim_purok` | Dimension | `purok_key` | None | `purok_no` UNIQUE, NOT NULL | Zone (purok) attributes for per-area reporting |
| `dim_consumer` | Dimension | `consumer_key` | `purok_key` | `consumer_id` UNIQUE, NOT NULL | Consumer attributes for reporting |
| `fact_consumption` | Fact | `consumption_key` | `consumer_key` | `consumption_id` UNIQUE; `consumption >= 0` | Monthly meter readings for analysis |
| `fact_billing` | Fact | `billing_key` | `consumption_key`, `consumer_key` | `billing_id` UNIQUE; `total_bill >= 0` | Bills and balances for analysis |
| `fact_payment` | Fact | `payment_key` | `billing_key` | `payment_id` UNIQUE; `total_paid >= 0` | Payment transactions for analysis |
| `load_log` | Log | `load_log_id` | None | `run_id`, `batch_id`, `table_name`, `status` NOT NULL | Record of every load run |

All tables are in the `warehouse` schema. Column names and types follow the transformed data contract (`docs/transformation.md` §6.1). Every dimension and fact table also has two columns added by the load: `load_batch_id` (the batch ID column) and `loaded_at`.

### `dim_purok`

- **Table name and type:** `dim_purok` — dimension
- **Purpose:** zones (puroks) used to group consumers and totals by area
- **Important columns and data types:** `purok_key` `CHAR(16)`, `purok_no` `INTEGER`, `purok_label` `VARCHAR(50)`, `is_unknown` `BOOLEAN`
- **Primary key or surrogate key:** `purok_key` (surrogate key from the Transformation Stage)
- **Foreign keys and relationships:** none; referenced by `dim_consumer.purok_key`
- **Constraints:** `purok_no` UNIQUE and NOT NULL; `purok_label` and `is_unknown` NOT NULL
- **Indexes or partitions:** unique index on `purok_no`; no partitions
- **Columns the load depends on:** match key `purok_no`; batch ID column `load_batch_id`; no history columns
- **Reason for inclusion:** every per-purok report groups by this table

### `dim_consumer`

- **Table name and type:** `dim_consumer` — dimension
- **Purpose:** consumer identity, zone, and status
- **Important columns and data types:** `consumer_key` `CHAR(16)`, `consumer_id` `INTEGER`, `full_name` `VARCHAR(152)`, `email` `VARCHAR(255)`, `purok_key` `CHAR(16)`, `status` `VARCHAR(10)`, `active_flag` `BOOLEAN`
- **Primary key or surrogate key:** `consumer_key`
- **Foreign keys and relationships:** `purok_key` → `dim_purok.purok_key`; referenced by all three fact tables
- **Constraints:** `consumer_id` UNIQUE and NOT NULL; `status` CHECK in (`ACTIVE`, `INACTIVE`, `UNKNOWN`); `middle_name` and `contact_number` may be NULL
- **Indexes or partitions:** unique index on `consumer_id`; index on `purok_key`
- **Columns the load depends on:** match key `consumer_id`; batch ID column `load_batch_id`; history columns `valid_from`, `valid_to`, `is_current` are delivered by the Transformation Stage and loaded as received (SCD Type 1, so there is one row per consumer and `is_current` is always `TRUE`)
- **Reason for inclusion:** shared dimension for consumption, billing, and payment analysis

### `fact_consumption`

- **Table name and type:** `fact_consumption` — fact
- **Purpose:** one row per monthly meter reading per consumer
- **Important columns and data types:** `consumption_key` `CHAR(16)`, `consumption_id` `INTEGER`, `consumer_key` `CHAR(16)`, `reading_date` `DATE`, `billing_period` `CHAR(7)`, `previous_reading`, `present_reading`, `consumption` `NUMERIC(12,2)`
- **Primary key or surrogate key:** `consumption_key`
- **Foreign keys and relationships:** `consumer_key` → `dim_consumer.consumer_key`; referenced by `fact_billing.consumption_key`
- **Constraints:** `consumption_id` UNIQUE and NOT NULL; CHECK `consumption >= 0`; CHECK `present_reading >= previous_reading`
- **Indexes or partitions:** indexes on `consumer_key`, `billing_period`, `load_batch_id`; no partitions at this volume
- **Columns the load depends on:** match key `consumption_id`; batch ID column `load_batch_id`
- **Reason for inclusion:** core fact for consumption analysis and demand forecasting

### `fact_billing`

- **Table name and type:** `fact_billing` — fact
- **Purpose:** one row per bill, with balance and aging values
- **Important columns and data types:** `billing_key` `CHAR(16)`, `billing_id` `INTEGER`, `consumption_key` `CHAR(16)`, `consumer_key` `CHAR(16)`, `billing_date`, `due_date` `DATE`, `total_bill`, `remaining_balance` `NUMERIC(12,2)`, `status` `VARCHAR(10)`, `days_overdue` `INTEGER`, `is_flagged_account` `BOOLEAN`
- **Primary key or surrogate key:** `billing_key`
- **Foreign keys and relationships:** `consumption_key` → `fact_consumption.consumption_key`; `consumer_key` → `dim_consumer.consumer_key`; referenced by `fact_payment.billing_key`
- **Constraints:** `billing_id` UNIQUE and NOT NULL; CHECK `total_bill >= 0`; CHECK `remaining_balance >= 0`; `status` CHECK in (`PAID`, `PARTIAL`, `UNPAID`, `UNKNOWN`)
- **Indexes or partitions:** indexes on `(consumer_key, status, due_date)`, `billing_period`, `load_batch_id`
- **Columns the load depends on:** match key `billing_id`; batch ID column `load_batch_id`
- **Reason for inclusion:** billing history, outstanding balances, and flagged-account reporting

### `fact_payment`

- **Table name and type:** `fact_payment` — fact
- **Purpose:** one row per payment transaction
- **Important columns and data types:** `payment_key` `CHAR(16)`, `payment_id` `INTEGER`, `billing_key` `CHAR(16)`, `payment_date` `DATE`, `payment_method` `VARCHAR(20)`, `total_paid`, `amount_tendered` `NUMERIC(12,2)`
- **Primary key or surrogate key:** `payment_key`
- **Foreign keys and relationships:** `billing_key` → `fact_billing.billing_key`
- **Constraints:** `payment_id` UNIQUE and NOT NULL; CHECK `total_paid >= 0`; `payment_method` CHECK in (`CASH`, `GCASH`, `BANK_TRANSFER`, `OTHER`); `reference_number` and `collection_rate` may be NULL
- **Indexes or partitions:** indexes on `(billing_key, payment_date)`, `load_batch_id`
- **Columns the load depends on:** match key `payment_id`; batch ID column `load_batch_id`
- **Reason for inclusion:** collection monitoring and revenue reporting

### `load_log`

- **Table name and type:** `load_log` — log
- **Purpose:** one row per target table per load run (fields in Section 6)
- **Important columns and data types:** `run_id`, `transformation_run_id`, `batch_id` `VARCHAR(64)`, `table_name` `VARCHAR(40)`, `status` `VARCHAR(20)`, record counts `INTEGER`, timestamps `TIMESTAMPTZ`
- **Primary key or surrogate key:** `load_log_id` (generated)
- **Foreign keys and relationships:** none
- **Constraints:** `run_id`, `batch_id`, `table_name`, `status` NOT NULL; rows are only inserted, never updated or deleted by the load role
- **Indexes or partitions:** indexes on `(batch_id, status)` and `run_id`
- **Columns the load depends on:** `batch_id` and `status`, used by the duplicate batch check
- **Reason for inclusion:** monitoring, auditing, lineage, and recovery of every batch

---

## 3. Load Strategy and Rules

- **Load Type**
  - All five dimension and fact tables use an **incremental load**: records are matched on a defined key, new records are inserted and changed records are updated. No table is truncated, so dashboards never see an empty table and a failed load cannot remove existing data.
  - `load_log` is append-only.
- **Write Logic**
  - **Upsert** (insert new, update existing) for all five tables.
  - Match key used to detect existing records: `purok_no`, `consumer_id`, `consumption_id`, `billing_id`, `payment_id` (the natural key of each table).
  - Rows that would violate a target constraint (wrong type, NULL in a required column, failed CHECK, or missing parent record) are not written. They are counted as rejected and saved to the rejected-records file (Section 7).
- **Handling of Changing Data**
  - **Overwrite (SCD Type 1)** for both dimensions and for changed fact values such as bill status, balances, and aging values. History (SCD Type 2) is not kept because the source system stores no status history (`docs/transformation.md` §5.4).
  - **Deleted or inactive records:** inactive consumers arrive with `status = INACTIVE` and `active_flag = FALSE` and stay in the warehouse (soft delete flag). A record missing from a later batch is kept, never hard-deleted.
  - **Late-arriving records:** every batch is a full snapshot, so a late reading or payment appears in a later batch and is inserted by the upsert.
  - **Handover whose schema differs from the target:** the batch is rejected by the pre-load schema check (Section 4) and nothing is written.
- **Load Order and Dependencies**
  - Parent tables are loaded before child tables: `dim_purok` → `dim_consumer` → `fact_consumption` → `fact_billing` → `fact_payment`.
  - **Facts with no matching dimension record are rejected.** The Transformation Stage already guarantees matching keys, so an orphan at load time means the handover is damaged. The only "Unknown" member is the UNKNOWN purok (`purok_no = 0`), which arrives as a normal `dim_purok` row.
- **Transaction and Rollback Handling**
  - The **whole batch** (all five tables and the post-load checks) is loaded in **one transaction**.
  - The transaction is committed only when the batch is accepted (Section 8). Otherwise it is rolled back, so the warehouse is left exactly as it was before the run and no partial data remains. If the process or connection dies, PostgreSQL discards the open transaction.
  - `load_log` rows are written on a separate connection, so failed runs are still recorded.
- **Rerun and Idempotency**
  - Running the same batch twice does not create duplicates or inflate totals, because the upsert matches on the natural key and writes the same values to the same rows.
  - Reruns and recovery: a failed batch left nothing behind, so it is fixed and run again (merge on key). The duplicate batch check stops a batch that already succeeded; an intentional reload uses `--force-reload`.

| Table | Load Type | Write Logic | Match Key | On Failure |
| --- | --- | --- | --- | --- |
| `dim_purok` | Incremental | Upsert (SCD Type 1) | `purok_no` | Roll back batch, stop |
| `dim_consumer` | Incremental | Upsert (SCD Type 1) | `consumer_id` | Roll back batch, stop |
| `fact_consumption` | Incremental | Upsert | `consumption_id` | Roll back batch, stop |
| `fact_billing` | Incremental | Upsert | `billing_id` | Roll back batch, stop |
| `fact_payment` | Incremental | Upsert | `payment_id` | Roll back batch, stop |

---

## 4. Pre-Load Validation Checks

These checks are performed before data is written to the target. A critical failure stops the load.

| Check Name | Target | Purpose | Validation Criteria | Severity |
| --- | --- | --- | --- | --- |
| Target connection | Warehouse database | Ensure the load can start | Connection succeeds within 10 seconds (up to 3 attempts) | Critical |
| Write permission | `warehouse` schema, role `etl_loader` | Avoid failing midway for missing privileges | Role can `INSERT` and `UPDATE` all five tables and `INSERT` into `load_log` | Critical |
| Handover completeness | `transformed/<table>/<run_date>/` | Make sure the whole batch arrived | CSV, `.schema.json`, and transformation log exist for all five tables | Critical |
| Handover status | Transformation metadata | Load approved data only | Status = APPROVED for all tables (a batch on MANUAL REVIEW must first be released by a named reviewer) | Critical |
| Schema match | All target tables | Prevent type or column errors | All handover columns exist in the target with expected types; no missing or extra columns | Critical |
| Record count | Handover CSV files | Detect a truncated or changed file | Row count of each CSV = `records_output` in the transformation log | Critical |
| File integrity | Handover CSV files | Prove the files are the ones the transformation validated | SHA-256 of each CSV = checksum in the transformation log | Critical |
| Duplicate batch | `load_log.batch_id` | Avoid loading twice | Batch ID not previously loaded with SUCCESS | Critical |
| Storage space | Warehouse database size | Avoid running out of space | Used space is below 95% of the 500 MB limit | Critical |
| Storage warning | Warehouse database size | Give early notice before space runs out | Used space is below 80% of the 500 MB limit | Non-critical |

---

## 5. Post-Load Validation and Reconciliation

These checks are performed after loading, inside the same transaction, so a critical failure can still be rolled back.

| Check Name | Target | Purpose | Validation Criteria | Severity |
| --- | --- | --- | --- | --- |
| Record reconciliation | All target tables | Detect lost or extra records | received = loaded + rejected, where loaded = inserted + updated | Critical |
| Batch row count | Rows with this `load_batch_id` | Confirm the rows are in the target | Row count in target = received − rejected | Critical |
| Key uniqueness | Primary key and match key of each table | Prevent duplicates | Duplicate count = 0 | Critical |
| Referential integrity | `dim_consumer.purok_key`, `fact_consumption.consumer_key`, `fact_billing.consumption_key`, `fact_billing.consumer_key`, `fact_payment.billing_key` | Prevent orphan facts | 100% match in the parent table | Critical |
| Required columns | NOT NULL columns of each table | Data completeness | Null count = 0 | Critical |
| Amount reconciliation | `fact_consumption.consumption`, `fact_billing.total_bill`, `fact_payment.total_paid` | Confirm no value loss | Source sum = target sum for the batch (within 0.01) | Critical |
| Rejected records | All target tables | Limit records not loaded | Rejected records are 1% or less of each table | Critical |
| Rejected records warning | All target tables | Bring any rejected record to review | Rejected count = 0 | Non-critical |
| Load errors | Load process | Detect errors and interruptions | No database error, constraint violation, timeout, or interruption | Critical |
| Null check | `dim_consumer.contact_number`, `dim_consumer.middle_name`, `fact_payment.reference_number` | Data completeness | Null rate is 90% or less | Non-critical |
| Load duration | Whole load run | Keep the load inside its time window | Load finishes within 15 minutes | Non-critical |

*Rejected* means records the load did not write, such as constraint violations. Records already removed during transformation are not counted.

**Scope limitation:** No new cleaning or business transformation is performed here. Issues found are reported and handled using the rules in Section 8.

---

## 6. Load Metadata and Log Specification

One record is written per target table per load run.

| Field | Purpose | Example |
| --- | --- | --- |
| `run_id` | Unique ID of the pipeline run | `ww-load-2026-03-10-0300-a1b2c3` |
| `transformation_run_id` | Link to the transformation run | `ww-transform-2026-03-10-0200-7b3e42` |
| `batch_id` | Batch being loaded | `ww-batch-2026-03-10` |
| `batch_window` | Date range covered by the batch | `2026-03-10 to 2026-03-10` |
| `table_name` | Target table | `fact_billing` |
| `load_type` | Full or incremental load | `INCREMENTAL` |
| `load_start_ts` | When the table load started | `2026-03-10 03:00:04` |
| `load_end_ts` | When the table load ended | `2026-03-10 03:00:19` |
| `load_duration_sec` | Time taken to load the table | `15` |
| `records_received` | Rows received from transformation | `12,395` |
| `records_inserted` | New rows written | `210` |
| `records_updated` | Existing rows changed | `12,185` |
| `records_rejected` | Rows that failed to load | `0` |
| `status` | Result of the load | `SUCCESS` |
| `pre_load_result` | Outcome of the pre-load checks | `PASS` |
| `post_load_result` | Outcome of the post-load checks | `PASS` |
| `error_code` | Failure or rejection reason, if any | `NULL` |
| `error_message` | Error detail, if any | `NULL` |
| `reviewed_by` | Reviewer who released a held batch, if any | `NULL` |
| `load_code_version` | Version of the load code | `v1.0` |

How the metadata and logs are used:

- **Monitoring:** the Airflow UI shows each run as success or failure; `status`, the record counts, and `load_duration_sec` show the result and speed of each table load.
- **Troubleshooting:** `pre_load_result`, `post_load_result`, and `error_code` name the check that failed; the record counts show which table lost or rejected rows.
- **Auditing:** `run_id`, `batch_id`, `load_start_ts`, and `load_code_version` prove what was loaded, when, and by which version of the code; `reviewed_by` records who released a held batch.
- **Data lineage:** each warehouse row carries `load_batch_id` and `transformation_run_id`, which link it to its load record, then to the transformation run, and from there to the extraction run.
- **Recovery of a failed batch:** a failed record means nothing was committed, so the same `batch_id` is run again after the fix. The latest `SUCCESS` record shows which batch the warehouse currently holds.

---

## 7. Log Retention, Rejected Data, and Access

- **Log retention period and justification:** operational logs are kept for a minimum of 90 days; audit records are kept for a minimum of 1 year. These are the same periods as the Extraction and Transformation stages, so a batch can be traced through all three stages. One year covers a full cycle of monthly billing and the flagged-account rule (3 or more outstanding bills).
- **Storage location of logs:** operational logs are JSON files in `logs/waterwise-load/<YYYY-MM-DD>/<run_id>_<table>.json`, together with the Airflow task log. Audit records are the rows of `warehouse.load_log`.
- **Location and retention of records rejected during loading:** `quarantine/load/<run_date>/rejected.csv`, with the run ID, batch ID, table, reject reason, and the values needed to diagnose the row (email and contact number masked). Retained for a minimum of 1 year, because a rejected bill or payment is a financial record.
- **Access permissions:**
  - Read: data engineer and Super Admin (logs, `load_log`, rejected records); dashboards read `load_log` only.
  - Write: the load process only (`etl_loader` role and the Airflow worker), and only by appending.
  - Delete: data engineer, after the retention period and with Super Admin approval. The load role cannot delete.
  - Consumers, meter readers, and billing staff have no access.
- **Archiving requirements:** JSON logs older than 90 days are compressed and moved to cold storage. `load_log` rows stay online for 1 year.
- **Deletion rules and the responsible process:** nothing is deleted before its retention period ends. After that, the data engineer deletes expired logs and rejected records with approval, and the deletion is recorded.
- **Backup or snapshot of the target before major loads:** daily loads need no snapshot, because the single transaction protects the warehouse and it can be rebuilt by reloading a handover. Before a major load (schema change, forced reload, or backfill of old batches), the data engineer takes a `pg_dump` of the `warehouse` schema, because the Supabase free plan has no automatic backups.

**Requirement:** Logs and rejected records are not deleted before the defined retention period or while they are needed for troubleshooting, auditing, or recovery.

Operational logs (JSON files, 90 days) are kept separately from audit records (`load_log` rows and rejected records, 1 year).

---

## 8. Load Acceptance, Rollback, and Completion Rules

- **Automatic Approval**
  - **Validation condition:** all pre-load checks (Section 4) and post-load checks (Section 5).
  - **Acceptance criteria:** all critical checks pass, reconciliation matches, and there are no warnings.
  - **Pipeline action:** commit the transaction, mark the batch complete, publish the data.
  - **Decision:** automatic.
- **Manual Review**
  - **Validation condition:** the non-critical checks — storage warning, rejected records warning, null check, load duration.
  - **Acceptance criteria:** no critical check fails, but one or more non-critical checks give a warning, for example a small number of rejected records within the 1% tolerance.
  - **Pipeline action:** hold publication — the transaction is rolled back so nothing is published — and notify the owner. After review, the batch is run again with `--accept-warnings "<reviewer>"`, which commits it and records the reviewer.
  - **Decision:** manual. **Responsible:** data engineer / Super Admin.
- **Rejection or Failure**
  - **Validation condition:** any critical check.
  - **Acceptance criteria:** the batch fails when the handover is not approved, the schema does not match, counts or checksums differ, the batch was already loaded, reconciliation or amounts do not match, duplicate or orphan keys exist, a required column is NULL, more than 1% of records are rejected, or a database error occurs.
  - **Pipeline action:** roll back, then:
    - connection or network errors: **retry** automatically, up to **3 retries** with a **5-minute wait** between retries;
    - data or contract failures: **stop** without retrying, because a retry would fail the same way. The batch is run again after the cause is fixed.
  - **Decision:** automatic.
  - **Who is notified:** the data engineer, through the failed Airflow task (Airflow UI, and email when configured). The Super Admin is informed when published data will be more than one day late.
- **Completion and Handover**
  - **What is reported:** final status, run ID, batch ID, and transformation run ID; records received, inserted, updated, and rejected per table; and the data now available to users (the five `warehouse` tables, current as of the batch window).
  - **How downstream users are notified:** the view `warehouse.latest_successful_load` shows the latest approved batch and its load time, and dashboards display it as "data as of". A held or rejected batch never appears there, so users keep seeing the previous complete batch.

**Decision Rules**

| Overall Condition | Acceptance Decision | Pipeline Action |
| --- | --- | --- |
| All tables PASS | APPROVED | Mark batch complete, publish data |
| One or more non-critical WARNING | MANUAL REVIEW | Hold publication, notify owner |
| One or more critical FAIL | REJECTED | Roll back, retry, or stop |

| Validation Summary | Result |
| --- | --- |
| `dim_purok` load | PASS |
| `dim_consumer` load | PASS |
| `fact_consumption` load | PASS |
| `fact_billing` load | PASS |
| `fact_payment` load | WARNING |
| **Overall Load** | **MANUAL REVIEW** |

---

## 9. Common Load Problems and Mitigation

| Problem | Possible Cause | Impact | Mitigation |
| --- | --- | --- | --- |
| Duplicate rows after a rerun | Airflow retry, or the batch started twice by hand | Inflated consumption, billing, and collection totals | Upsert on the match key; duplicate batch check before load; key uniqueness and amount checks after load |
| Foreign key violation | Fact loaded before its parent, or parent row missing from the handover | Load fails or orphan facts | Enforce load order; reject and count facts with no matching parent; referential integrity check |
| Partial load after a crash | Worker killed or connection dropped mid-batch | Inconsistent target data (dimensions updated, facts not) | Load the whole batch in one transaction and roll back on failure |
| Warehouse unreachable | Supabase pauses free projects after about 7 days without activity; network outage | Load cannot start; dashboards unavailable | Connection check with 3 attempts, then 3 Airflow retries; daily load keeps the project active; resume the project and run the batch again |
| Connection refused or dropped | Wrong pooler mode or port; IPv6-only direct host used from Docker; password not URL-encoded | Load fails at the connection check or mid-transaction | Use the session pooler connection string; URL-encode the password (`DOCKER.md`) |
| Database reaches the storage limit | Fact tables and `load_log` grow past the 500 MB free-plan limit | Database becomes read-only; loads fail | Storage check warns at 80% and stops at 95%; archive old `load_log` rows; upgrade the plan |
| Handover schema does not match the target | Transformation added or renamed a column without updating `db/warehouse_schema.sql` | Insert errors or values in the wrong column | Schema match check rejects the batch; update the contract and schema script together, then rerun |
| Handover file changed after validation | Transformation rerun during the load, interrupted copy, or manual edit | Unvalidated data reaches the warehouse | Record count and file integrity (checksum) checks against the transformation log |
| Rejected batch gets loaded | Transformation writes its CSV files even when it rejects a batch | Wrong figures published | Handover status check reads the status from the transformation log, not from the presence of files |
| Load runs too long | Row-by-row inserts, or data volume growth | Delays reporting; pooler may time out | Bulk insert into a staging table and one merge per table; indexes on match keys; load duration warning |

---

## Expected Output

The documentation is consistent with the implemented load process and provides sufficient information for a team member to understand, validate, monitor, and maintain the load stage.

The loaded data is validated, reconciled with the transformed input, and logged so that every batch can be traced, audited, and recovered. No new extraction or transformation logic is added in this stage.

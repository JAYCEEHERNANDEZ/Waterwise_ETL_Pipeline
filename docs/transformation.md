# WaterWise: Operational and Analytical Batch Data Pipeline

## Stage 2: Data Transformation Documentation

### Objective

The objective of the WaterWise Data Transformation Stage is to take the validated dataset handed over from the Extraction Stage (`docs/extraction.md`) and transform it into a cleaned, standardized, and business-ready dataset that is documented, traceable, and validated for the succeeding Load Stage.

---

## 1. Transformation Input and Processing Specification

### 1.1 Input Source

| Item | Description |
| --- | --- |
| **Input origin** | Stage 1 handover only. This stage does **not** connect to the Supabase/PostgreSQL source system and does not re-extract any record. |
| **Staging area** | `rawdata/<table>/<run_date>/` written by Stage 1 |
| **File format** | CSV with UTF-8 encoding, header row present, plus a `.schema.json` column sidecar per table |
| **Extraction logs** | `logs/waterwise-extraction/<YYYY-MM-DD>/<pipeline_run_id>_<table>.json` |
| **Input tables** | `consumers`, `consumption`, `billing`, `payments` |
| **Parent run ID** | Stage 1 `pipeline_run_id` for the batch (e.g. `ww-extract-2026-03-10-0100-4f2a91`) |

Concrete handover artifacts consumed by this stage:

| Artifact | Path pattern | Use in Transformation |
| --- | --- | --- |
| Staged consumers | `rawdata/consumers/<run_date>/consumers_<run_date>.csv` | Build `dim_purok`, `dim_consumer` |
| Staged consumption | `rawdata/consumption/<run_date>/consumption_<run_date>.csv` | Build `fact_consumption` |
| Staged billing | `rawdata/billing/<run_date>/billing_<run_date>.csv` | Build `fact_billing` |
| Staged payments | `rawdata/payments/<run_date>/payments_<run_date>.csv` | Build `fact_payment` |
| Column sidecar | `<table>_<run_date>.schema.json` | Schema-drift check (Section 6.4) |
| Extraction run log | `logs/waterwise-extraction/.../<pipeline_run_id>_<table>.json` | `records_read` reference for reconciliation; `parent_run_id` lineage |

**Example:** Stage 1 extracts `consumers`, `consumption`, `billing`, and `payments` for run `ww-extract-2026-03-10-0100-4f2a91`. Stage 2 receives the four staged CSV sets plus their run logs, profiles them, applies the cleaning rules in Section 3, the standardization rules in Section 4, and the business rules in Section 5, and emits the star-schema output defined in Section 6.

> Requirement (Lab 4 §1): the input must match the handover from the Extraction Stage. Do not re-extract from the source system, and do not copy the source database description from Stage 1 — refer to the extraction handover instead. This document describes only how the staged output is received and processed.

### 1.2 Batch Processing Method

| Item | Description |
| --- | --- |
| **Transformation tool** | Python 3.10+ (stdlib `csv`, `decimal`, `hashlib`, `datetime`) |
| **Orchestration tool** | Apache Airflow — DAG `waterwise_daily_transformation` |
| **Frequency / schedule** | Daily at 02:00 Asia/Manila (30 minutes after the Stage 1 01:00 extraction) |
| **Batch size** | All records staged by the parent extraction run (one extraction run = one transformation batch) |
| **Retry** | Up to 3 retries, 5-minute backoff, on file-read or parse failure |
| **Rerun model** | Re-read the same staged batch and overwrite the output for that run date |
| **Idempotency** | **Yes.** Running the same batch twice produces byte-identical output and never duplicates rows. See Section 1.4. |

**Processing order** (dimensions before facts, parents before children):

```
dim_purok → dim_consumer → fact_consumption → fact_billing → fact_payment
```

`fact_billing` depends on `fact_consumption` (billing is derived from a reading) and `dim_consumer`. `fact_payment` depends on `fact_billing` (a payment is applied to a bill). Each step reads the previous step's output, so a failure at step *n* leaves steps 1..*n-1* already written and is recovered by rerunning the whole batch — which is safe because reruns are idempotent.

### 1.3 Transformation Scope

| Table | Transformed to | Operation |
| --- | --- | --- |
| `consumers` | `dim_purok`, `dim_consumer` | Cleaned, standardized, SCD Type 1 on `status`, surrogate keys |
| `consumption` | `fact_consumption` | Cleaned, date standardization, billing-period attributes derived |
| `billing` | `fact_billing` | Cleaned, standardized, aging/overdue attributes derived |
| `payments` | `fact_payment` | Cleaned, standardized, payment-order rank and allocation derived |

**Fields cleaned:** `first_name`, `last_name`, `middle_name`, `username`, `email`, `contact_number`, `status`, `payment_method`, `reference_number`, `purok_no`, all `*_reading`/`*_bill`/`*_paid` numerics.

**Fields standardized:** all `DATE`/`TIMESTAMPTZ` columns, all money and meter columns, all categorical code columns.

**Fields derived:** `*_key` surrogate keys, `reading_year`, `reading_month`, `billing_year`, `billing_month`, `days_overdue`, `is_overdue`, `overdue_bucket`, `days_to_due`, `outstanding_bill_count`, `is_flagged_account`, `payment_allocation_rank`, `is_oldest_unpaid_payment`, `collection_rate`.

**Fields aggregated:** none at this stage. Monthly and per-purok aggregates are a Load/consumption concern; this stage emits grain-level facts only (Section 5, "Aggregations").

### 1.4 Idempotency and Failed-Batch Recovery

Idempotency is achieved by three properties:

1. **Deterministic surrogate keys.** Keys are `MD5(<namespace>:<source_id>)` truncated to 16 hex characters (`stage.py` contract is stable, source IDs are stable). The same consumer always receives the same `consumer_key` on every rerun, so no new dimension rows appear.
2. **Atomic output write.** Each output file is written to `<name>.tmp` and moved into place with `os.replace()` only after the batch completes successfully. A crash mid-write leaves the previous complete file intact; it never leaves a truncated file.
3. **Run-date-scoped paths.** Output lives under `transformed/<table>/<run_date>/`, so rerunning a batch overwrites exactly one directory instead of appending.

**Failed-batch rerun:** fix the cause, then re-run the same `run_date` with the same `parent_run_id`. Because of the three properties above, the rerun reproduces the previous output exactly. If the cause was a Stage 1 rejection, the fix belongs in Stage 1 and the extraction run must be re-triggered for that window first (`docs/extraction.md` §8, Problem 3).

### 1.5 Dependencies and Assumptions

**Dependencies:**

| Dependency | Type | Behavior if unavailable |
| --- | --- | --- |
| Stage 1 staged CSV + run logs | Hard | Batch FAILs immediately; no output written |
| Stage 1 `_schema` sidecars | Hard | Batch FAILs on schema-drift check |
| Prior stage output within the same batch (e.g. `dim_consumer` before `fact_billing`) | Hard | Batch FAILs at that step; upstream steps already written and safe to overwrite on rerun |

**Assumptions:**

- One accepted consumption reading per consumer per billing month is guaranteed by the source unique index `idx_consumption_consumer_month`, carried forward from Stage 1. This stage does not re-derive or re-enforce it; it validates it.
- `consumption.consumption` arrives already computed by the source as a `GENERATED ALWAYS ... STORED` column. This stage treats it as authoritative and does not recompute it from `present_reading - previous_reading`; a disagreement is surfaced as a validation check, not silently corrected.
- `purok_no` is a small, stable set of integers representing Sucol zones. A NULL or unrecognized `purok_no` maps to the sentinel `UNKNOWN` purok rather than rejecting the consumer, because a missing zone must not remove a consumer's billing history from analysis.
- Stage 1 quarantine is not an input. Records Stage 1 quarantined (NULL identifiers) are absent from the staged CSV and are counted in Stage 1's `records_rejected`, not here.
- All timestamps are interpreted in **Asia/Manila**. Stage 1 extracts `TIMESTAMPTZ` values which are UTC-aware; this stage renders them as ISO 8601 with an explicit `+08:00` offset in the output.
- Late-arriving readings do occur (meter readers syncing offline outboxes). This stage reprocesses a configurable lookback window rather than assuming the current window is complete (Section 11, Problem 2).

---

## 2. Data Profiling and Issue Identification

The issues below are **representative estimates derived from the WaterWise source schema** (`db/full_setup_migration.sql`) and the failure modes already documented in `docs/extraction.md` §8. They are the expected profile for the first production run. Percentages are **estimates to be confirmed against the first real staged batch** and must be corrected in this table at that time.

| # | Table | Column | Issue Type | Example of the issue | Affected Records (est.) | Planned Treatment |
| --- | --- | --- | --- | --- | --- | --- |
| P-01 | `consumers` | `first_name`, `last_name` | Leading/trailing whitespace | `" Dela Cruz "` | ~4% | Trim — Rule C-01 |
| P-02 | `consumers` | `middle_name` | Missing value | `NULL` | ~60% | Keep NULL, no fill — Rule C-03 |
| P-03 | `consumers` | `email` | Mixed upper/lower case | `Juan.DelaCruz@Mail.COM` | ~3% | Lowercase — Rule C-02 |
| P-04 | `consumers` | `username` | Mixed case + whitespace | `" JDelacruz "` | ~2% | Trim + lowercase — Rule C-01, C-02 |
| P-05 | `consumers` | `contact_number` | Missing value | `NULL` | ~25% | Keep NULL — Rule C-03 |
| P-06 | `consumers` | `purok_no` | Missing value / out of range | `NULL`, `0`, `9999` | ~5% | Map to `UNKNOWN` sentinel — Rule C-04 |
| P-07 | `consumers` | `status` | Inconsistent case | `"Active"`, `"ACTIVE"` | ~2% | Uppercase + map — Rule S-02 |
| P-08 | `consumption` | `present_reading` vs `previous_reading` | Meter rollover / corrupt reading, produces negative consumption | `prev=150.00, pres=12.00` | ~0.5% | Reject to quarantine — Rule C-05 |
| P-09 | `consumption` | `reading_date` | Reading date not within the billed month | `reading_date=2026-02-28`, `billing_date=2026-02-15` | ~3% | Keep, flag for Transformation — Rule B-05 |
| P-10 | `consumption` | Duplicate `(consumer_id, reading_month)` | Duplicate reading in same month | `id=881, id=902` same consumer+month | ~0.2% | Keep latest by `updated_at` — Rule C-06 |
| P-11 | `billing` | `total_bill` | Negative value | `-50.00` | ~0.1% | Reject to quarantine — Rule C-05 |
| P-12 | `billing` | `status` | Free-text status variants | `"unpaid"`, `"Unpaid"`, `"PARTIALLY PAID"` | ~5% | Map to code set — Rule S-03 |
| P-13 | `billing` | `remaining_balance` | Sign / rounding drift vs. `total_bill` | `total_bill=250.00, remaining_balance=-0.01` | ~2% | Clamp and flag — Rule C-07 |
| P-14 | `billing` | `billing_date` / `due_date` | `due_date` earlier than `billing_date` | `billing_date=2026-03-01, due_date=2026-02-20` | ~1% | Correct to billing_date + 15 days — Rule C-08 |
| P-15 | `payments` | `payment_method` | Free-text method variants + NULL | `"cash"`, `"GCASH"`, `NULL` | ~4% | Map to code set, NULL → `OTHER` — Rule S-04, C-03 |
| P-16 | `payments` | Duplicate `(billing_id, payment_date, total_paid)` | Duplicate payment from client retry | two rows, same bill/date/amount | ~0.5% | Keep earliest `created_at` — Rule C-06 |
| P-17 | `payments` | `amount_tendered` / `change_given` | Inconsistent with `total_paid` | `tendered=200, paid=150, change=0` | ~1% | Recompute `change_given` — Rule C-09 |
| P-18 | `billing` / `payments` | FK columns | Orphan record (FK with no matching parent) | `payments.billing_id=4401`, no such bill | ~0.3% | Reject child to quarantine — Rule C-10 |
| P-19 | all | Text columns | Leading/trailing whitespace on staged CSV values | `"Cash "`, `"active "` | ~2% | Trim — Rule C-01 |

**Profiling requirements:**

- Profiling runs on the staged CSV **before** any cleaning rule is applied.
- Every profile row must be reproducible: re-running the profile on the same staged batch yields identical counts.
- Counts are recorded in the run log as `profile_<table>_<issue>_count` so a later run can be compared against this baseline.
- **Requirement:** every cleaning rule in Section 3 traces back to at least one row in this table (P-01…P-19). A rule with no profiled issue behind it must be removed or justified.

> The percentages above are estimates, not measurements. On the first production run, replace them with the actual counts produced by the profiling step before submitting this document.

---

## 3. Data Cleaning Rules

Rules are applied in ascending Rule ID order. Every rule increments a `rules_applied` counter for the records it modifies or rejects; counters appear in the run log (Section 8).

### 3.1 Cleaning Rule Specification

| Rule ID and name | Target | Condition | Action | Justification |
| --- | --- | --- | --- | --- |
| **C-01** Trim text | All VARCHAR/TEXT columns: `first_name`, `middle_name`, `last_name`, `username`, `status`, `payment_method`, `reference_number` | Value has leading or trailing whitespace | `value.strip()`; collapse internal repeated spaces to one | Prevents false mismatches on joins and on `status`/`payment_method` code mapping, and prevents `"Cash "` failing to map to `CASH` |
| **C-02** Normalize identifier case | `consumers.email`, `consumers.username` | Value contains uppercase characters | `value.lower()` | Source unique indexes are case-insensitive (`lower(btrim(username))`), so mixed case in the extract breaks equality joins and equality filters |
| **C-03** Handle missing nullable values | `middle_name`, `contact_number`, `reference_number`, `idempotency_key` | Value is NULL or empty after trim | Retain as NULL; never impute a placeholder | These are genuinely optional fields; a fabricated placeholder would be indistinguishable from real data and would corrupt contactability and payment-reference reporting |
| **C-04** Normalize missing zone | `consumers.purok_no` | Value is NULL, `<= 0`, or outside the known zone range | Set to the `UNKNOWN` sentinel zone and set `purok_is_unknown = TRUE` | A missing location zone must not remove a consumer's billing history from per-purok analysis; the flag keeps the gap reportable |
| **C-05** Reject impossible values | `consumption` (`present_reading < previous_reading`), `billing.total_bill < 0`, any `total_paid < 0` | Numeric value violates a physically or commercially impossible constraint | Reject the row to quarantine with `rule_id=C-05`; exclude from output | A negative consumption or negative bill is not a cleaning problem, it is a corrupt record. Publishing it would corrupt every downstream aggregate |
| **C-06** Deduplicate | `consumption` on `(consumer_id, reading_month)`; `payments` on `(billing_id, payment_date, total_paid)` | Two or more rows share the deduplication key | Keep exactly one: for `consumption` the latest `updated_at` (tie-break on highest `id`); for `payments` the earliest `created_at` (tie-break on lowest `id`). Log the discarded rows to the duplicate log with `rule_id=C-06` | Matches the source's offline-retry semantics. Readings are corrected in place (latest wins); payments are immutable financial events (earliest wins) |
| **C-07** Clamp balance drift | `billing.remaining_balance`, `payments.remaining_balance` | Balance is negative while the bill is fully paid, or `abs(balance) < 0.005` | Clamp `-0.01 → 0.00`; set `balance_corrected = TRUE` | Sub-cent rounding drift is a formatting artifact, not a financial event; rejecting it would reject valid bills. The flag keeps the correction auditable |
| **C-08** Repair due date | `billing.due_date` | `due_date < billing_date` | Set `due_date = billing_date + 15 days`; set `due_date_corrected = TRUE` | A due date before the billing date makes `days_overdue` meaningless and would misclassify every account in aging |
| **C-09** Recompute change | `payments.change_given` | `change_given != amount_tendered - total_paid` | Set `change_given = amount_tendered - total_paid` (rounded half-up to 2dp) | Change is arithmetic, not an independent business value; recomputing it guarantees the cash reconciliation identity holds |
| **C-10** Reject orphans | FK columns: `consumption.consumer_id`, `billing.consumption_id`, `billing.user_id`, `payments.billing_id` | FK value has no matching parent in the staged set | Reject the **child** row to quarantine with `rule_id=C-10`; the parent is retained | An orphan fact cannot be attributed to a consumer or a bill, so it is unusable for any per-consumer or per-purok measure |
| **C-11** Enforce code domain | `consumers.status`, `billing.status`, `payments.payment_method` | Value not in the mapped code set after C-01 | Map known synonyms (Section 4.3); if still unmapped, set `UNKNOWN` and set `code_unmapped = TRUE` | An unmapped code must be visible as a data-quality signal, not silently coerced into a valid-looking code |

### 3.2 Required Considerations

| Consideration | How it is handled |
| --- | --- |
| **Missing / null values** | Optional fields are retained as NULL (C-03) — never imputed. Required identifiers were already guaranteed NOT NULL by Stage 1, so a NULL here is a contract violation and FAILs validation rather than entering a cleaning rule. `purok_no` NULL maps to a sentinel (C-04) because zone is required for dimensional grouping but not for record validity. |
| **Duplicate detection and which record is kept** | C-06. Readings: latest `updated_at` (correction semantics). Payments: earliest `created_at` (immutable financial event). The discarded row is logged with its full source values and the surviving key, so the decision is reversible. |
| **Leading/trailing whitespace and invalid characters** | C-01 trims all text columns and collapses internal runs of whitespace. Invalid characters (control characters, non-printable bytes) are stripped by the same rule; a value that becomes empty after stripping is set to NULL (not to an empty string) so it is handled by the NULL path. |
| **Invalid or out-of-range values** | C-05 (impossible numerics → reject), C-04 (out-of-range zone → sentinel), C-07 (balance drift → clamp), C-08 (impossible date ordering → repair). Rejection is reserved for values that cannot be made usable without inventing data. |
| **Orphan records** | C-10. Children are rejected, parents are kept. This choice is deliberate: rejecting the parent would cascade-delete valid history for every other child pointing at it, whereas rejecting the orphan child removes exactly the unusable records. |

> **Requirement:** no record is ever dropped silently. Every rejected record is written to quarantine (Section 9) with its `run_id`, `rule_id`, `rejection_reason`, and source column values, and is counted in `records_rejected` (Section 8).

---

## 4. Standardization and Data Type Rules

### 4.1 Data Types — Target Type per Column

| Output table | Column | Source type | Target type | Conversion |
| --- | --- | --- | --- | --- |
| `dim_consumer` | `consumer_key` | — | `CHAR(16)` | Deterministic MD5 surrogate |
| `dim_consumer` | `consumer_id` | `INTEGER` | `INTEGER` | Direct (natural key retained) |
| `dim_consumer` | `username` | `VARCHAR(50)` | `VARCHAR(50)` | Trim + lowercase (C-01, C-02) |
| `dim_consumer` | `first_name`, `last_name` | `VARCHAR(50)` | `VARCHAR(50)` | Trim (C-01) |
| `dim_consumer` | `middle_name` | `VARCHAR(50)` | `VARCHAR(50)` NULL | Trim; NULL preserved (C-03) |
| `dim_consumer` | `full_name` | — | `VARCHAR(152)` | Derived: `first_name` + `' '` + `last_name` (B-01) |
| `dim_consumer` | `email` | `VARCHAR(255)` | `VARCHAR(255)` | Trim + lowercase (C-01, C-02) |
| `dim_consumer` | `contact_number` | `VARCHAR(20)` | `VARCHAR(20)` NULL | Trim; NULL preserved; **not** cast to integer |
| `dim_consumer` | `purok_key` | — | `CHAR(16)` | Derived surrogate of resolved zone (B-02) |
| `dim_consumer` | `purok_no` | `INTEGER` | `INTEGER` NULL | Sentinel `0` = UNKNOWN (C-04) |
| `dim_consumer` | `status` | `VARCHAR(20)` | `VARCHAR(10)` | Map to `ACTIVE` / `INACTIVE` (S-02) |
| `dim_consumer` | `created_at`, `updated_at` | `TIMESTAMPTZ` | `TIMESTAMPTZ` | Render ISO 8601 `+08:00` (S-01) |
| `fact_consumption` | `consumption_key` | — | `CHAR(16)` | Deterministic MD5 surrogate |
| `fact_consumption` | `consumption_id` | `INTEGER` | `INTEGER` | Direct (natural key retained) |
| `fact_consumption` | `consumer_key` | `INTEGER` (FK) | `CHAR(16)` | Join to `dim_consumer.consumer_key` |
| `fact_consumption` | `reading_date` | `DATE` | `DATE` | ISO 8601 `YYYY-MM-DD` (S-01) |
| `fact_consumption` | `reading_year`, `reading_month` | — | `SMALLINT` | Derived (B-03) |
| `fact_consumption` | `previous_reading`, `present_reading` | `NUMERIC(10,2)` | `NUMERIC(12,2)` | Direct, rounded half-up 2dp |
| `fact_consumption` | `consumption` | `NUMERIC(10,2)` GENERATED | `NUMERIC(12,2)` | Direct (source-computed; **not** recomputed) |
| `fact_consumption` | `consumption_cbm` | — | `NUMERIC(12,2)` | Business alias of `consumption` (B-04) |
| `fact_consumption` | `days_in_period` | — | `INTEGER` | Derived calendar days of billing month (B-05) |
| `fact_consumption` | `consumption_per_day` | — | `NUMERIC(12,4)` | `consumption / days_in_period` (B-06) |
| `fact_billing` | `billing_key` | — | `CHAR(16)` | Deterministic MD5 surrogate |
| `fact_billing` | `billing_id` | `INTEGER` | `INTEGER` | Direct |
| `fact_billing` | `consumption_key` | `INTEGER` (FK) | `CHAR(16)` | Join to `fact_consumption.consumption_key` |
| `fact_billing` | `consumer_key` | `INTEGER` (FK) | `CHAR(16)` | Join to `dim_consumer.consumer_key` |
| `fact_billing` | `billing_date`, `due_date` | `DATE` | `DATE` | ISO 8601 (S-01) |
| `fact_billing` | `billing_year`, `billing_month` | — | `SMALLINT` | Derived (B-03) |
| `fact_consumption` / `fact_billing` | `billing_period` | — | `CHAR(7)` | `'YYYY-MM'` derived (B-03) |
| `fact_billing` | `total_bill` | `NUMERIC(10,2)` | `NUMERIC(12,2)` | Direct, rounded half-up |
| `fact_billing` | `remaining_balance` | `NUMERIC(10,2)` | `NUMERIC(12,2)` | Clamped (C-07) |
| `fact_billing` | `status` | `VARCHAR(30)` | `VARCHAR(10)` | Map to `PAID` / `PARTIAL` / `UNPAID` (S-03) |
| `fact_billing` | `days_to_due` | — | `INTEGER` | `due_date - billing_date` (B-07) |
| `fact_billing` | `days_overdue` | — | `INTEGER` | `GREATEST(today - due_date, 0)` (B-08) |
| `fact_billing` | `is_overdue` | — | `BOOLEAN` | `days_overdue > 0` (B-08) |
| `fact_billing` | `overdue_bucket` | — | `VARCHAR(10)` | Aging band (B-09) |
| `fact_billing` | `outstanding_bill_count` | — | `INTEGER` | Rolling count of unpaid bills per consumer (B-10) |
| `fact_billing` | `is_flagged_account` | — | `BOOLEAN` | `outstanding_bill_count >= 3` (B-11) |
| `fact_payment` | `payment_key` | — | `CHAR(16)` | Deterministic MD5 surrogate |
| `fact_payment` | `payment_id` | `INTEGER` | `INTEGER` | Direct |
| `fact_payment` | `billing_key` | `INTEGER` (FK) | `CHAR(16)` | Join to `fact_billing.billing_key` |
| `fact_payment` | `total_paid`, `amount_tendered`, `change_given` | `NUMERIC(10,2)` | `NUMERIC(12,2)` | Direct; `change_given` recomputed (C-09) |
| `fact_payment` | `remaining_balance` | `NUMERIC(10,2)` | `NUMERIC(12,2)` | Clamped (C-07) |
| `fact_payment` | `payment_date` | `DATE` | `DATE` | ISO 8601 (S-01) |
| `fact_payment` | `payment_year`, `payment_month` | — | `SMALLINT` | Derived (B-03) |
| `fact_payment` | `payment_method` | `VARCHAR(40)` | `VARCHAR(20)` | Map to code set (S-04) |
| `fact_payment` | `payment_allocation_rank` | — | `INTEGER` | Oldest-first rank per billing (B-12) |
| `fact_payment` | `is_oldest_unpaid_payment` | — | `BOOLEAN` | Rank 1 and bill still unpaid (B-12) |
| `fact_payment` | `collection_rate` | — | `NUMERIC(9,4)` | `SUM(total_paid) / total_bill` per billing (B-13) |
| `fact_payment` | `reference_number` | `VARCHAR(100)` | `VARCHAR(100)` NULL | Trim; NULL preserved (C-03) |

### 4.2 Formats and Precision

| Aspect | Rule | Rationale |
| --- | --- | --- |
| Date format | `YYYY-MM-DD` (ISO 8601) | Matches Stage 1 output; sorts lexicographically |
| Timestamp format | ISO 8601 with explicit offset, e.g. `2026-03-10T01:00:00+08:00` | Stage 1 emits UTC-aware values; rendering a fixed `+08:00` offset keeps Asia/Manila unambiguous without a separate timezone column |
| Time zone | Asia/Manila for all business dates and all `today`-relative derivations | Sucol operates in PHT; `payment_date` is documented as Asia/Manila in `docs/extraction.md` §2.5 |
| Numeric precision | All money and meter columns `NUMERIC(12,2)`; rates `NUMERIC(9,4)`; per-day measures `NUMERIC(12,4)` | Widened from the source `NUMERIC(10,2)` so future cumulative measures cannot overflow |
| Rounding | `ROUND(x, 2)` half-up, applied once at conversion; never re-rounded downstream | Prevents cumulative drift; banker's rounding would bias totals toward even values |
| Money unit | Philippine pesos (PHP), integer minor units are **not** used | `NUMERIC(12,2)` avoids the float error that integer-minor-unit conversion would introduce from a text source |
| Text case | Identifiers (`email`, `username`) lowercase; codes (`status`, `payment_method`) uppercase; names preserve source case after trim | Lowercase identifiers make equality matching reliable; uppercase codes make the code sets self-evident in the output |
| Booleans | `TRUE` / `FALSE` | Unambiguous in CSV |

### 4.3 Reference Values and Codes

| Field | Source variants observed | Standard code set | Unmapped handling |
| --- | --- | --- | --- |
| `consumers.status` | `active`, `Active`, `ACTIVE`, `inactive`, `Inactive`, `INACTIVE` | `ACTIVE`, `INACTIVE` | `UNKNOWN` + `code_unmapped = TRUE` |
| `billing.status` | `Unpaid`, `unpaid`, `UNPAID`, `Paid`, `paid`, `Partially Paid`, `partially_paid`, `PARTIALLY PAID` | `PAID`, `PARTIAL`, `UNPAID` | `UNKNOWN` + `code_unmapped = TRUE` |
| `payments.payment_method` | `Cash`, `cash`, `CASH`, `GCash`, `GCASH`, `gcash`, `Bank Transfer`, `bank_transfer`, `Maya`, `Online` | `CASH`, `GCASH`, `BANK_TRANSFER`, `OTHER` | `OTHER` + `code_unmapped = TRUE` |
| `consumers.purok_no` | Integer zone, `NULL`, `0`, out-of-range values | `1..N` known zones, `0` = `UNKNOWN` sentinel | `UNKNOWN` sentinel + `purok_is_unknown = TRUE` |
| Currency | — | `PHP` (single currency; no conversion applied) | n/a |
| Unit of measure | — | `cubic_meter` (CBM) for all consumption measures | n/a |
| Region | — | `PH` / `Asia/Manila` | n/a |

### 4.4 Standardization Rule Specification

| Rule ID | Column | Source format | Standard format | On failure |
| --- | --- | --- | --- | --- |
| **S-01** | all `DATE` / `TIMESTAMPTZ` | `YYYY-MM-DD`, ISO 8601 UTC | `YYYY-MM-DD`, ISO 8601 `+08:00` | Reject row, quarantine `rule_id=S-01` |
| **S-02** | `consumers.status` | Mixed-case free text | `ACTIVE` / `INACTIVE` | Set `UNKNOWN`, flag |
| **S-03** | `billing.status` | Mixed-case free text | `PAID` / `PARTIAL` / `UNPAID` | Set `UNKNOWN`, flag |
| **S-04** | `payments.payment_method` | Mixed-case free text | `CASH` / `GCASH` / `BANK_TRANSFER` / `OTHER` | Set `OTHER`, flag |
| **S-05** | all money columns | `NUMERIC` text from CSV | `NUMERIC(12,2)` | Reject row, quarantine `rule_id=S-05` |
| **S-06** | all meter reading columns | `NUMERIC` text from CSV | `NUMERIC(12,2)` | Reject row, quarantine `rule_id=S-06` |
| **S-07** | `billing.billing_month` derived from `billing_date` | — | `YYYY-MM` | n/a (derived; cannot fail) |
| **S-08** | `dim_*.*_key`, `fact_*.*_key` | — | `CHAR(16)` lowercase hex | n/a (deterministic; cannot fail) |

**Conversion-failure principle:** a conversion failure means the source value cannot be interpreted at all (unparseable date text, non-numeric money). Such a record is rejected to quarantine with the failing `rule_id`. A value that is parseable but out of a business range is handled by a cleaning rule in Section 3, never by a conversion rule — the two failure classes are kept separate so the log distinguishes "unreadable" from "unusable".

---

## 5. Business Transformation Rules

### 5.1 Business Rule Specification

| Rule ID and name | Input columns | Logic / formula | Output column(s) | Business reason |
| --- | --- | --- | --- | --- |
| **B-01** Full name | `first_name`, `last_name` | `first_name \|\| ' ' \|\| last_name` (after C-01) | `full_name` | Analytics and reports label consumers by full name; deriving it once avoids every consumer of the warehouse re-implementing the concatenation |
| **B-02** Zone surrogate | `purok_no` (after C-04) | `MD5('purok:' \|\| resolved_purok_no)` | `purok_key` | Stable zone surrogate independent of the natural integer key, so zone attributes can be extended later |
| **B-03** Period attributes | `reading_date`, `billing_date`, `payment_date` | Extract year, month; `billing_period = to_char(date,'YYYY-MM')` | `reading_year`, `reading_month`, `billing_year`, `billing_month`, `billing_period`, `payment_year`, `payment_month` | Monthly reporting and demand forecasting group by month; precomputing it avoids repeated date arithmetic in the dashboard and keeps month boundaries consistent across all three facts |
| **B-04** Consumption alias | `consumption` | Rename to the business-facing measure name; value unchanged | `consumption_cbm` | Makes the unit explicit at the column name, so a consumer of the model cannot mistake CBM for pesos or gallons |
| **B-05** Period length | `reading_date` | `days_in_period = EXTRACT(DAY FROM date_trunc('month', d) + INTERVAL '1 month' - date_trunc('month', d))` | `days_in_period` | Normalizing monthly consumption to a daily rate requires knowing whether the billing month has 28, 29, 30, or 31 days |
| **B-06** Daily consumption | `consumption`, `days_in_period` | `consumption / days_in_period`, rounded to 4dp | `consumption_per_day` | Makes February readings comparable to January readings; required for anomaly detection and forecasting inputs |
| **B-07** Days to due | `billing_date`, `due_date` (after C-08) | `days_to_due = due_date - billing_date` | `days_to_due` | Distinguishes short-term from long-term credit; supports collection-priority ranking |
| **B-08** Overdue measures | `due_date`, batch reference date | `days_overdue = GREATEST(reference_date - due_date, 0)`; `is_overdue = days_overdue > 0` | `days_overdue`, `is_overdue` | The operational question is "who is late and by how much", which a raw date difference answers badly for bills not yet due |
| **B-09** Aging bucket | `days_overdue` | `CURRENT` (not overdue), `D1_30`, `D31_60`, `D61_90`, `D90_PLUS` | `overdue_bucket` | Standard aging bands let collection staff prioritize without reading individual dates, and map directly to report filters |
| **B-10** Outstanding bill count | `billing_key`, `consumer_key`, `status` | For each consumer, count bills with `status IN ('UNPAID','PARTIAL')` and `due_date <= reference_date` | `outstanding_bill_count` | The WaterWise flagged-account rule is "≥3 outstanding bills"; this precomputes the exact quantity the rule tests |
| **B-11** Flagged account | `outstanding_bill_count` | `is_flagged_account = outstanding_bill_count >= 3` | `is_flagged_account` | This is the documented administrative recommendation trigger; materializing it lets the dashboard filter flagged accounts without recomputing a window function per request |
| **B-12** Payment allocation order | `payment_key`, `billing_key`, `payment_date`, `created_at`, `status` | `payment_allocation_rank = ROW_NUMBER() OVER (PARTITION BY billing_key ORDER BY payment_date, created_at, payment_id)`; `is_oldest_unpaid_payment = (rank = 1 AND billing.status IN ('UNPAID','PARTIAL'))` | `payment_allocation_rank`, `is_oldest_unpaid_payment` | Implements the oldest-first payment-order validation promised in `docs/extraction.md` §2.5, and identifies the payment a consumer still needs to make |
| **B-13** Collection rate | `SUM(total_paid)` per `billing_key`, `total_bill` | `collection_rate = SUM(total_paid) / total_bill`, NULL when `total_bill = 0` | `collection_rate` | Drives collection-efficiency reporting; the NULL-on-zero case prevents a division error and a meaningless infinity |
| **B-14** Balance correctness | `total_bill`, `remaining_balance`, `status` | Recompute expected status from amounts; set `status` where it disagrees | `status`, `status_recomputed = TRUE` | Free-text status from the application can lag the amounts; the amounts are the source of truth for a financial model. The flag preserves the discrepancy for review |

### 5.2 Joins and Lookups

| Join | Left | Right | Key | Type | Unmatched handling |
| --- | --- | --- | --- | --- | --- |
| J-01 | `fact_consumption` | `dim_consumer` | `consumer_id = dim_consumer.consumer_id` | INNER (1:1) | Child rejected by C-10 before this join; 0 unmatched by construction |
| J-02 | `fact_billing` | `fact_consumption` | `consumption_id = fact_consumption.consumption_id` | INNER (1:1) | Child rejected by C-10 |
| J-03 | `fact_billing` | `dim_consumer` | `user_id = dim_consumer.consumer_id` | INNER (1:1) | Child rejected by C-10 |
| J-04 | `fact_payment` | `fact_billing` | `billing_id = fact_billing.billing_id` | INNER (many:1) | Child rejected by C-10 |
| J-05 | `dim_consumer` | `dim_purok` | `purok_no = dim_purok.purok_no` | LEFT (many:1) | None — every `purok_no` including the `0` sentinel has a `dim_purok` row |

**Join cardinality guarantee:** every right-hand key is unique — `dim_consumer.consumer_id` is unique after C-06, `fact_consumption.consumption_id` is unique after C-06, `fact_billing.billing_id` is unique after C-06, and `dim_purok.purok_no` is unique by construction. Therefore no join can multiply rows. This is asserted by validation check V-08 (Section 7), which compares pre-join and post-join row counts; a mismatch is a **critical** failure, not a warning.

### 5.3 Filtering and Business Exclusions

| Exclusion ID | Table | Condition | Reason | Recorded as |
| --- | --- | --- | --- | --- |
| **X-01** | `dim_consumer` | `status = 'INACTIVE'` | Inactive consumers are excluded from **active-consumer counts and forecasts**, but **retained** in the dimension and in all fact tables | `active_flag` boolean; no rows removed |
| **X-02** | `fact_billing` (revenue views only) | `status = 'UNPAID'` | Outstanding balances are not recognized revenue | `is_revenue_recognized` boolean computed at load/query time; **no rows removed from the fact** |
| **X-03** | `payments.change_given` | Always | Change returned to a consumer is cash out, not collections | `change_given` excluded from `collection_rate` and from every revenue measure (B-13 uses `total_paid` only) |
| **X-04** | `fact_payment` | `payment_date > batch reference date` | A payment dated in the future is a data-entry error; including it inflates collections | Rejected to quarantine, `rule_id=X-04` |

> **Requirement (Lab 4 §5):** extraction may apply only technical filters such as the batch date window; business exclusions belong to this stage. Records are extracted first, then excluded here with the count and reason recorded, so excluded records remain reportable. X-01, X-02, and X-03 therefore remove **zero** rows — they set flags. Only X-04 removes rows, and every removed row is counted in `records_rejected` and logged to quarantine with its reason.

### 5.4 Dimensional Modeling Steps

| Step | Action | Detail |
| --- | --- | --- |
| D-01 | Generate surrogate keys | `consumer_key`, `purok_key`, `consumption_key`, `billing_key`, `payment_key` as `MD5('<entity>:<source_id>')` truncated to 16 hex chars. Deterministic, so reruns are idempotent (Section 1.4). |
| D-02 | Retain natural keys | `consumer_id`, `consumption_id`, `billing_id`, `payment_id` are kept alongside every surrogate key so any fact row can be traced back to the exact source record. |
| D-03 | Build dimension `dim_purok` | Grain: one row per known zone plus the `UNKNOWN` sentinel. Attributes: `purok_key`, `purok_no`, `purok_label`, `is_unknown`. |
| D-04 | Build dimension `dim_consumer` | Grain: one row per consumer. Attributes: identity, contact, zone FK, `status`, SCD metadata. |
| D-05 | Build facts `fact_consumption`, `fact_billing`, `fact_payment` | Grain: one row per source reading / bill / payment. Degenerate dimensions `consumption_id`, `billing_id`, `payment_id` retained; conformed dimension `consumer_key` shared across all three facts. |
| D-06 | SCD handling — Type 1 on `dim_consumer.status` | `status` is overwritten in place, with `valid_from`, `valid_to`, and `is_current` maintained. **Justification:** WaterWise has no consumer status history table (`db/full_setup_migration.sql` stores only the current `status`), and no requirement in the dashboard or forecasting scope asks for the date an account became inactive. Type 2 would create rows that cannot be sourced. If status history becomes a requirement, a history table must first be added in the source application. |
| D-07 | No SCD on `dim_purok` | Zone attributes are static reference data. |

---

## 6. Transformed Data Contract and Naming Convention

> This is **not** a repeat of the extraction contract. The extraction contract (`docs/extraction.md` §6) describes what this stage receives from the source. This contract describes what the Load Stage will receive, and it introduces columns that do not exist in the source: surrogate keys, derived measures, and dimensional structures.

### 6.1 Required Output Schema

#### `dim_purok`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `purok_key` | `CHAR(16)` | NOT NULL | PK | Surrogate, deterministic |
| `purok_no` | `INTEGER` | NOT NULL | UNIQUE | `0` = UNKNOWN sentinel |
| `purok_label` | `VARCHAR(50)` | NOT NULL | — | Human-readable zone name |
| `is_unknown` | `BOOLEAN` | NOT NULL | — | `TRUE` for the sentinel row |

#### `dim_consumer`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `consumer_key` | `CHAR(16)` | NOT NULL | PK | Surrogate, deterministic |
| `consumer_id` | `INTEGER` | NOT NULL | UNIQUE | Natural key |
| `username` | `VARCHAR(50)` | NOT NULL | — | Trimmed, lowercase |
| `first_name` | `VARCHAR(50)` | NOT NULL | — | Trimmed |
| `middle_name` | `VARCHAR(50)` | NULL | — | Optional |
| `last_name` | `VARCHAR(50)` | NOT NULL | — | Trimmed |
| `full_name` | `VARCHAR(152)` | NOT NULL | — | Derived (B-01) |
| `email` | `VARCHAR(255)` | NOT NULL | — | Trimmed, lowercase |
| `contact_number` | `VARCHAR(20)` | NULL | — | Text, never cast to integer |
| `purok_key` | `CHAR(16)` | NOT NULL | FK → `dim_purok.purok_key` | Conformed dimension |
| `purok_no` | `INTEGER` | NOT NULL | — | `0` = UNKNOWN |
| `purok_is_unknown` | `BOOLEAN` | NOT NULL | — | Gap flag (C-04) |
| `status` | `VARCHAR(10)` | NOT NULL | — | `ACTIVE` / `INACTIVE` / `UNKNOWN` |
| `code_unmapped` | `BOOLEAN` | NOT NULL | — | `TRUE` if a status variant was unmapped |
| `active_flag` | `BOOLEAN` | NOT NULL | — | Derived; drives exclusion X-01 |
| `valid_from` | `TIMESTAMPTZ` | NOT NULL | — | SCD Type 1 metadata |
| `valid_to` | `TIMESTAMPTZ` | NULL | — | NULL while current |
| `is_current` | `BOOLEAN` | NOT NULL | — | SCD Type 1 metadata |
| `created_at` | `TIMESTAMPTZ` | NOT NULL | — | Source, ISO 8601 `+08:00` |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL | — | Source |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

#### `fact_consumption`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `consumption_key` | `CHAR(16)` | NOT NULL | PK | Surrogate |
| `consumption_id` | `INTEGER` | NOT NULL | UNIQUE | Natural key (degenerate dim) |
| `consumer_key` | `CHAR(16)` | NOT NULL | FK → `dim_consumer.consumer_key` | Conformed dimension |
| `reading_date` | `DATE` | NOT NULL | — | `YYYY-MM-DD` |
| `reading_year` | `SMALLINT` | NOT NULL | — | Derived (B-03) |
| `reading_month` | `SMALLINT` | NOT NULL | — | Derived (B-03) |
| `billing_period` | `CHAR(7)` | NOT NULL | — | `YYYY-MM` (B-03) |
| `previous_reading` | `NUMERIC(12,2)` | NOT NULL | — | Meter value |
| `present_reading` | `NUMERIC(12,2)` | NOT NULL | — | Meter value |
| `consumption` | `NUMERIC(12,2)` | NOT NULL | — | Source-computed; **CHECK ≥ 0** |
| `consumption_cbm` | `NUMERIC(12,2)` | NOT NULL | — | Business alias (B-04) |
| `days_in_period` | `INTEGER` | NOT NULL | — | Derived (B-05) |
| `consumption_per_day` | `NUMERIC(12,4)` | NOT NULL | — | Derived (B-06) |
| `reading_date_adjusted` | `BOOLEAN` | NOT NULL | — | `TRUE` if reading date ≠ billing month end |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

**Grain:** one row per accepted monthly reading per consumer.

#### `fact_billing`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `billing_key` | `CHAR(16)` | NOT NULL | PK | Surrogate |
| `billing_id` | `INTEGER` | NOT NULL | UNIQUE | Natural key |
| `consumption_key` | `CHAR(16)` | NOT NULL | FK → `fact_consumption.consumption_key` | Source reading |
| `consumer_key` | `CHAR(16)` | NOT NULL | FK → `dim_consumer.consumer_key` | Conformed dimension |
| `billing_date` | `DATE` | NOT NULL | — | `YYYY-MM-DD` |
| `due_date` | `DATE` | NOT NULL | — | Repaired by C-08 |
| `billing_year` | `SMALLINT` | NOT NULL | — | Derived (B-03) |
| `billing_month` | `SMALLINT` | NOT NULL | — | Derived (B-03) |
| `billing_period` | `CHAR(7)` | NOT NULL | — | `YYYY-MM` (B-03) |
| `total_bill` | `NUMERIC(12,2)` | NOT NULL | — | **CHECK ≥ 0** |
| `remaining_balance` | `NUMERIC(12,2)` | NOT NULL | — | **CHECK ≥ 0**, clamped (C-07) |
| `status` | `VARCHAR(10)` | NOT NULL | — | `PAID` / `PARTIAL` / `UNPAID` / `UNKNOWN` |
| `status_recomputed` | `BOOLEAN` | NOT NULL | — | `TRUE` if status corrected from amounts (B-14) |
| `balance_corrected` | `BOOLEAN` | NOT NULL | — | `TRUE` if clamped (C-07) |
| `due_date_corrected` | `BOOLEAN` | NOT NULL | — | `TRUE` if repaired (C-08) |
| `is_revenue_recognized` | `BOOLEAN` | NOT NULL | — | Exclusion X-02 flag |
| `days_to_due` | `INTEGER` | NOT NULL | — | Derived (B-07) |
| `days_overdue` | `INTEGER` | NOT NULL | — | Derived (B-08) |
| `is_overdue` | `BOOLEAN` | NOT NULL | — | Derived (B-08) |
| `overdue_bucket` | `VARCHAR(10)` | NOT NULL | — | Derived (B-09) |
| `outstanding_bill_count` | `INTEGER` | NOT NULL | — | Derived (B-10) |
| `is_flagged_account` | `BOOLEAN` | NOT NULL | — | Derived (B-11), threshold ≥ 3 |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

**Grain:** one row per bill.

#### `fact_payment`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `payment_key` | `CHAR(16)` | NOT NULL | PK | Surrogate |
| `payment_id` | `INTEGER` | NOT NULL | UNIQUE | Natural key |
| `billing_key` | `CHAR(16)` | NOT NULL | FK → `fact_billing.billing_key` | Billed item |
| `payment_date` | `DATE` | NOT NULL | — | `YYYY-MM-DD` |
| `payment_year` | `SMALLINT` | NOT NULL | — | Derived (B-03) |
| `payment_month` | `SMALLINT` | NOT NULL | — | Derived (B-03) |
| `payment_method` | `VARCHAR(20)` | NOT NULL | — | `CASH` / `GCASH` / `BANK_TRANSFER` / `OTHER` |
| `code_unmapped` | `BOOLEAN` | NOT NULL | — | `TRUE` if variant was unmapped |
| `total_paid` | `NUMERIC(12,2)` | NOT NULL | — | **CHECK ≥ 0** |
| `amount_tendered` | `NUMERIC(12,2)` | NOT NULL | — | Cash received |
| `change_given` | `NUMERIC(12,2)` | NOT NULL | — | Recomputed (C-09) |
| `remaining_balance` | `NUMERIC(12,2)` | NOT NULL | — | Clamped (C-07) |
| `reference_number` | `VARCHAR(100)` | NULL | — | Optional |
| `payment_allocation_rank` | `INTEGER` | NOT NULL | — | Derived (B-12) |
| `is_oldest_unpaid_payment` | `BOOLEAN` | NOT NULL | — | Derived (B-12) |
| `collection_rate` | `NUMERIC(9,4)` | NULL | — | Derived (B-13); NULL when `total_bill = 0` |
| `change_corrected` | `BOOLEAN` | NOT NULL | — | `TRUE` if `change_given` was recomputed |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

**Grain:** one row per payment transaction.

### 6.2 Naming Convention

| Element | Rule | Example |
| --- | --- | --- |
| Table names | `snake_case` with a type prefix | `dim_consumer`, `fact_billing` |
| Column names | `snake_case` | `billing_period`, `total_bill` |
| Surrogate keys | `<entity>_key`, type `CHAR(16)` | `consumer_key`, `purok_key` |
| Natural keys | `<entity>_id` | `consumer_id`, `billing_id` |
| Foreign keys | Reference the target's surrogate key | `fact_billing.consumer_key` → `dim_consumer.consumer_key` |
| Measures | Noun form ending in the unit or metric | `consumption_cbm`, `collection_rate` |
| Booleans | Predicate form, `is_` / `has_` prefix | `is_overdue`, `is_flagged_account` |
| Code columns | Uppercase value set, `snake_case` column | `status` ∈ {`PAID`, `UNPAID`} |
| Timestamps | `*_at` | `created_at`, `transformation_started_at` |
| Lineage | `<entity>_run_id` | `transformation_run_id` |
| Abbreviations | Only `cbm` (cubic meter) and `purok` (the WaterWise zone term, retained from the source schema); both defined here once and used nowhere else | — |

### 6.3 Source-to-Target Mapping

| Source column | Target column | Source type | Target type | Transformation applied |
| --- | --- | --- | --- | --- |
| `consumers.id` | `dim_consumer.consumer_id` | `INTEGER` | `INTEGER` | Direct |
| `consumers.id` | `dim_consumer.consumer_key` | `INTEGER` | `CHAR(16)` | Derive surrogate — D-01 |
| `consumers.username` | `dim_consumer.username` | `VARCHAR(50)` | `VARCHAR(50)` | Trim C-01, lowercase C-02 |
| `consumers.first_name` | `dim_consumer.first_name` | `VARCHAR(50)` | `VARCHAR(50)` | Trim C-01 |
| `consumers.middle_name` | `dim_consumer.middle_name` | `VARCHAR(50)` | `VARCHAR(50)` NULL | Trim C-01, NULL preserved C-03 |
| `consumers.last_name` | `dim_consumer.last_name` | `VARCHAR(50)` | `VARCHAR(50)` | Trim C-01 |
| `consumers.first_name` + `last_name` | `dim_consumer.full_name` | — | `VARCHAR(152)` | Derive B-01 |
| `consumers.email` | `dim_consumer.email` | `VARCHAR(255)` | `VARCHAR(255)` | Trim C-01, lowercase C-02 |
| `consumers.contact_number` | `dim_consumer.contact_number` | `VARCHAR(20)` | `VARCHAR(20)` NULL | Trim C-01, keep as text C-03 |
| `consumers.purok_no` | `dim_consumer.purok_no` | `INTEGER` | `INTEGER` | Sentinel for missing/out-of-range C-04 |
| `consumers.purok_no` | `dim_consumer.purok_key` | `INTEGER` | `CHAR(16)` | Derive surrogate B-02 |
| `consumers.status` | `dim_consumer.status` | `VARCHAR(20)` | `VARCHAR(10)` | Map to code set S-02 |
| `consumers.status` | `dim_consumer.active_flag` | `VARCHAR(20)` | `BOOLEAN` | Derive X-01 |
| `consumers.created_at` / `updated_at` | `dim_consumer.created_at` / `updated_at` | `TIMESTAMPTZ` | `TIMESTAMPTZ` | Render ISO 8601 `+08:00` S-01 |
| `consumption.id` | `fact_consumption.consumption_id` | `INTEGER` | `INTEGER` | Direct |
| `consumption.id` | `fact_consumption.consumption_key` | `INTEGER` | `CHAR(16)` | Derive surrogate D-01 |
| `consumption.consumer_id` | `fact_consumption.consumer_key` | `INTEGER` | `CHAR(16)` | Join J-01 to `dim_consumer` |
| `consumption.reading_date` | `fact_consumption.reading_date` | `DATE` | `DATE` | Standardize S-01 |
| `consumption.reading_date` | `fact_consumption.reading_year`, `reading_month`, `billing_period` | `DATE` | `SMALLINT`, `SMALLINT`, `CHAR(7)` | Derive B-03 |
| `consumption.previous_reading` | `fact_consumption.previous_reading` | `NUMERIC(10,2)` | `NUMERIC(12,2)` | Widen, round S-06 |
| `consumption.present_reading` | `fact_consumption.present_reading` | `NUMERIC(10,2)` | `NUMERIC(12,2)` | Widen, round S-06 |
| `consumption.consumption` | `fact_consumption.consumption` | `NUMERIC(10,2)` GENERATED | `NUMERIC(12,2)` | Direct; negative rejected C-05 |
| `consumption.consumption` | `fact_consumption.consumption_cbm` | `NUMERIC(10,2)` | `NUMERIC(12,2)` | Rename alias B-04 |
| `consumption.consumption` | `fact_consumption.consumption_per_day` | `NUMERIC(10,2)` | `NUMERIC(12,4)` | Derive B-06 |
| `consumption.idempotency_key` | — | `TEXT` | — | **Not carried forward.** Extraction-level deduplication artifact; retains no analytical value once the fact is keyed by `consumption_id` |
| `billing.id` | `fact_billing.billing_id` | `INTEGER` | `INTEGER` | Direct |
| `billing.id` | `fact_billing.billing_key` | `INTEGER` | `CHAR(16)` | Derive surrogate D-01 |
| `billing.consumption_id` | `fact_billing.consumption_key` | `INTEGER` | `CHAR(16)` | Join J-02 to `fact_consumption` |
| `billing.user_id` | `fact_billing.consumer_key` | `INTEGER` | `CHAR(16)` | Join J-03 to `dim_consumer` |
| `billing.billing_date` / `due_date` | `fact_billing.billing_date` / `due_date` | `DATE` | `DATE` | Standardize S-01; repair C-08 |
| `billing.total_bill` | `fact_billing.total_bill` | `NUMERIC(10,2)` | `NUMERIC(12,2)` | Widen, round S-05 |
| `billing.remaining_balance` | `fact_billing.remaining_balance` | `NUMERIC(10,2)` | `NUMERIC(12,2)` | Clamp C-07 |
| `billing.status` | `fact_billing.status` | `VARCHAR(30)` | `VARCHAR(10)` | Map S-03, recompute B-14 |
| `billing.*` | `fact_billing.days_to_due`, `days_overdue`, `is_overdue`, `overdue_bucket` | — | `INTEGER`, `INTEGER`, `BOOLEAN`, `VARCHAR(10)` | Derive B-07, B-08, B-09 |
| `billing.*` | `fact_billing.outstanding_bill_count`, `is_flagged_account` | — | `INTEGER`, `BOOLEAN` | Derive B-10, B-11 |
| `payments.id` | `fact_payment.payment_id` | `INTEGER` | `INTEGER` | Direct |
| `payments.id` | `fact_payment.payment_key` | `INTEGER` | `CHAR(16)` | Derive surrogate D-01 |
| `payments.billing_id` | `fact_payment.billing_key` | `INTEGER` | `CHAR(16)` | Join J-04 to `fact_billing` |
| `payments.total_paid` | `fact_payment.total_paid` | `NUMERIC(10,2)` | `NUMERIC(12,2)` | Widen, round S-05 |
| `payments.amount_tendered` | `fact_payment.amount_tendered` | `NUMERIC(10,2)` | `NUMERIC(12,2)` | Widen, round S-05 |
| `payments.change_given` | `fact_payment.change_given` | `NUMERIC(10,2)` | `NUMERIC(12,2)` | Recompute C-09 |
| `payments.remaining_balance` | `fact_payment.remaining_balance` | `NUMERIC(10,2)` | `NUMERIC(12,2)` | Clamp C-07 |
| `payments.payment_date` | `fact_payment.payment_date` | `DATE` | `DATE` | Standardize S-01; future-date reject X-04 |
| `payments.payment_method` | `fact_payment.payment_method` | `VARCHAR(40)` | `VARCHAR(20)` | Map S-04 |
| `payments.reference_number` | `fact_payment.reference_number` | `VARCHAR(100)` | `VARCHAR(100)` NULL | Trim C-01, NULL preserved C-03 |
| `payments.*` | `fact_payment.payment_allocation_rank`, `is_oldest_unpaid_payment`, `collection_rate` | — | `INTEGER`, `BOOLEAN`, `NUMERIC(9,4)` | Derive B-12, B-13 |
| `payments.idempotency_key` | — | `TEXT` | — | **Not carried forward.** See `consumption.idempotency_key` |

### 6.4 Schema Consistency

Unexpected changes in the **staged input** are handled as follows:

1. **Input schema check.** Before any cleaning rule runs, the staged CSV header and its `.schema.json` sidecar are compared against the expected Stage 1 contract (`docs/extraction.md` §6.1). A missing required column is a **critical** failure.
2. **Fail fast, no silent coercion.** The batch stops before writing any output. Missing columns are never defaulted to NULL and extra columns are never silently dropped — both are logged.
3. **Versioned contract.** This document is versioned (`rules_version` in Section 8). A change to the source-to-target mapping requires a new contract version and a documented ticket.
4. **Added columns are tolerated.** A new *optional* column in the staged input is logged as `input_column_unmapped` and ignored; it does not fail the batch, because Stage 1 is allowed to extend its own contract independently.
5. **Recovery.** After the contract or the Stage 1 extractor is fixed, reprocess the same `run_date` with the same `parent_run_id`. Because reruns are idempotent (Section 1.4), the reprocessed output replaces the prior output cleanly.

**Contract change log:**

| Contract version | Date | Change | Author |
| --- | --- | --- | --- |
| 1.0 | 2026-03-10 | Initial Stage 2 contract: star schema `dim_purok` / `dim_consumer` / `fact_consumption` / `fact_billing` / `fact_payment` | data engineering |

---

## 7. Transformation Validation and Data Quality Checks

These checks verify the **transformed output**. Stage 1 validation (connectivity, extraction execution, source-to-staging completeness) is **not** repeated here, per Lab 4 §7 scope limitation. Stage 1's `records_read` is used only as a **reference value** for reconciliation.

| Check Name | Target | Purpose | Validation Criteria (Pass / Fail) | Severity |
| --- | --- | --- | --- | --- |
| **V-01** Record reconciliation | All output tables | Detect lost records; prove nothing vanished silently | PASS: `records_read = records_output + records_rejected` for every table, where `records_read` is the staged CSV row count and `records_output + records_rejected` are the output file row count and the quarantine row count for that table. FAIL: any difference | **Critical** |
| **V-02** Duplicate accounting | All output tables | Prove removed duplicates are accounted for, not lost | PASS: `records_read = records_output + records_rejected + duplicates_discarded` where `duplicates_discarded` equals the `C-06` log count for that table. FAIL: unexplained residual | **Critical** |
| **V-03** Surrogate key uniqueness | `*.*_key` in all 5 tables | Prevent dimension/fact row multiplication | PASS: 0 duplicate values per key column. FAIL: >0 | **Critical** |
| **V-04** Natural key uniqueness | `dim_consumer.consumer_id`, `fact_consumption.consumption_id`, `fact_billing.billing_id`, `fact_payment.payment_id` | Preserve source grain | PASS: 0 duplicates per column. FAIL: >0 | **Critical** |
| **V-05** Required columns NOT NULL | All NOT NULL columns per Section 6.1 | Catch rows that entered output with a missing key or measure | PASS: 0 NULLs in any NOT NULL column. FAIL: >0 | **Critical** |
| **V-06** Referential integrity (output) | `fact_consumption.consumer_key`, `fact_billing.consumption_key`, `fact_billing.consumer_key`, `fact_payment.billing_key`, `dim_consumer.purok_key` | Prevent orphans in the transformed model | PASS: 100% match rate against the parent output table. FAIL: any orphan | **Critical** |
| **V-07** Join cardinality / row multiplication | Row counts before and after joins J-01…J-05 | Catch a non-unique lookup key inflating totals | PASS: post-join row count equals pre-join row count for every INNER join; LEFT join J-05 may only reduce by NULL matches (which are zero by construction). FAIL: any increase | **Critical** |
| **V-08** Business rule — non-negative measures | `consumption`, `total_bill`, `total_paid`, `remaining_balance`, `change_given`, `previous_reading`, `present_reading` | Enforce commercial invariants | PASS: 0 negative values. FAIL: >0 | **Critical** |
| **V-09** Business rule — reading monotonicity | `fact_consumption.present_reading >= previous_reading` | Enforce physical meter ordering | PASS: 0 violations. FAIL: >0 | **Critical** |
| **V-10** Business rule — cash reconciliation | `fact_payment.change_given = amount_tendered - total_paid` | Guarantee the arithmetic identity after C-09 | PASS: 0 mismatches within `0.01` tolerance. FAIL: >0 | **Critical** |
| **V-11** Business rule — derived balance consistency | `remaining_balance <= total_bill` on `fact_billing`; `total_bill > 0` where `status = 'PAID'` | Catch impossible financial states | PASS: 0 violations. FAIL: >0 | **Critical** |
| **V-12** Business rule — flagged account threshold | `is_flagged_account = (outstanding_bill_count >= 3)` | Verify the documented administrative rule | PASS: 0 disagreements between the flag and the count. FAIL: >0 | **Critical** |
| **V-13** Aggregate reconciliation — consumption | `SUM(fact_consumption.consumption)` vs. `SUM` of the same field in the staged `consumption` CSV, net of `C-05`/`C-06` | Prove cleaning did not silently alter a financial measure | PASS: sums agree to within `0.01`. FAIL: otherwise | **Critical** |
| **V-14** Aggregate reconciliation — billed amounts | `SUM(fact_billing.total_bill)` vs. staged `billing` CSV, net of `C-05`/`C-06` | Same, for revenue | PASS: sums agree to within `0.01`. FAIL: otherwise | **Critical** |
| **V-15** Aggregate reconciliation — collections | `SUM(fact_payment.total_paid)` vs. staged `payments` CSV, net of `C-06` | Same, for collections; confirms `change_given` (X-03) is excluded from collections | PASS: sums agree to within `0.01`. FAIL: otherwise | **Critical** |
| **V-16** Exclusion accounting | `X-01`, `X-02`, `X-03`, `X-04` counters | Prove business exclusions are counted and reported, never silent | PASS: every exclusion's count is present in the run log and equals the number of rows it affected. FAIL: missing counter | **Non-critical** |
| **V-17** Data type / format conformance | Every output column vs. Section 6.1 | Confirm CSV values parse as the contracted type and match the contracted format | PASS: 0 values failing type parse or format check per column. FAIL: >0 | **Non-critical** |
| **V-18** Code domain conformance | `status`, `payment_method`, `overdue_bucket`, `billing_period` | Confirm code sets are respected | PASS: 100% of values within the Section 4.3 code sets. FAIL: any out-of-domain value | **Non-critical** |
| **V-19** Null rate on optional columns | `middle_name`, `contact_number`, `reference_number` | Track degradation of optional data over time | PASS: null rate within the baseline recorded in Section 2 for that column. WARNING: null rate increased vs. the previous run. FAIL: null rate > 90% (column is effectively useless) | **Non-critical** |
| **V-20** Unmapped code rate | `consumers.code_unmapped`, `payments.code_unmapped` | Detect new code variants appearing in the source | PASS: unmapped rate ≤ 1% of rows. WARNING: 1–5%. FAIL: > 5% | **Non-critical** |
| **V-21** Rejection rate | Per table and overall | Decide acceptance against the pre-declared threshold | PASS: rejection rate ≤ 1.0% (Section 10). WARNING: > 1.0% and ≤ 5.0%. FAIL: > 5.0% | **Critical** |
| **V-22** Idempotency assertion | Output of a repeat run | Prove Section 1.4's claim | PASS: reprocessing the same `run_date` with the same `parent_run_id` produces an identical output file checksum. FAIL: any difference | **Non-critical** |
| **V-23** Lineage completeness | `transformation_run_id` on every output row | Make every output row traceable | PASS: 0 output rows with a NULL or mismatched `transformation_run_id`. FAIL: >0 | **Critical** |
| **V-24** Sensitive data absence | `dim_consumer`, all output files | Ensure no credentials or secrets leak into the transformed layer | PASS: 0 rows containing `consumers.password` or any DSN/credential fragment. FAIL: >0 | **Critical** |

**Severity semantics:**

- **Critical** — stops the pipeline. The batch is rejected and quarantined; it does not proceed to Load.
- **Non-critical** — produces a WARNING. The batch is held for manual review but is not auto-rejected.

---

## 8. Transformation Metadata and Log Specification

Each transformation batch run records the following fields for monitoring, troubleshooting, auditing, lineage, and recovery:

| Field | Purpose | Example value / format |
| --- | --- | --- |
| `run_id` | Unique ID of the transformation run | `ww-transform-2026-03-10-0200-7b3e42` |
| `parent_run_id` | Stage 1 `pipeline_run_id` this batch came from (referenced, not duplicated) | `ww-extract-2026-03-10-0100-4f2a91` |
| `batch_id` | Unique ID of the batch within the run | `ww-transform-2026-03-10-0200-7b3e42-b1` |
| `batch_window_start` | Start of the processed data range | `2026-03-09T01:00:00+08:00` |
| `batch_window_end` | End of the processed data range | `2026-03-10T01:00:00+08:00` |
| `reference_date` | "Today" used by all date-relative derivations (B-08, B-09, B-10) | `2026-03-10` |
| `table_name` | Output table produced | `fact_billing` |
| `transformation_started_at` | Stage start timestamp | `2026-03-10T02:00:00+08:00` |
| `transformation_ended_at` | Stage end timestamp | `2026-03-10T02:03:41+08:00` |
| `transformation_status` | Final status | `SUCCESS` / `WARNING` / `FAILED` |
| `records_read` | Rows received from the staged CSV | `1250` |
| `records_output` | Rows written to the output table | `1239` |
| `records_rejected` | Rows written to quarantine | `8` |
| `records_modified` | Rows changed but retained (informational; **not** part of the V-01 equation) | `417` |
| `duplicates_discarded` | Rows removed by C-06 | `3` |
| `rules_applied` | Records affected per rule | `C-01: 54; C-02: 38; C-06: 3; S-04: 51; B-12: 1250` |
| `exclusions_applied` | Records affected per business exclusion | `X-04: 1` |
| `validation_results` | Summary of Section 7 checks | `PASS` / `WARNING: V-19 null rate up on contact_number` / `FAIL: V-01 reconciliation 1250 != 1239+8` |
| `output_file` | Path to the produced file | `transformed/fact_billing/2026-03-10/fact_billing_2026-03-10.csv` |
| `output_checksum` | SHA-256 of the output file; the idempotency assertion (V-22) | `9f2c...` |
| `quarantine_file` | Path to the rejected-record file, if any | `quarantine/transformation/2026-03-10/fact_billing_rejected.csv` |
| `error_message` / `error_code` | Error detail if applicable | `None` / `S01_FAIL: staged consumption missing column 'present_reading'` |
| `transformation_duration_ms` | Wall-clock duration | `221000` |
| `rules_version` | Version of the transformation code and rules applied | `1.0` |

**Example log record (JSON):**

```json
{
  "run_id": "ww-transform-2026-03-10-0200-7b3e42",
  "parent_run_id": "ww-extract-2026-03-10-0100-4f2a91",
  "batch_id": "ww-transform-2026-03-10-0200-7b3e42-b1",
  "batch_window_start": "2026-03-09T01:00:00+08:00",
  "batch_window_end": "2026-03-10T01:00:00+08:00",
  "reference_date": "2026-03-10",
  "table_name": "fact_billing",
  "transformation_started_at": "2026-03-10T02:01:12+08:00",
  "transformation_ended_at": "2026-03-10T02:01:39+08:00",
  "transformation_status": "WARNING",
  "records_read": 1250,
  "records_output": 1239,
  "records_rejected": 8,
  "records_modified": 417,
  "duplicates_discarded": 3,
  "rules_applied": {
    "C-01": 54,
    "C-07": 22,
    "C-08": 6,
    "S-03": 47,
    "B-10": 1239
  },
  "exclusions_applied": {
    "X-04": 1
  },
  "validation_results": "WARNING: V-19 null rate on dim_consumer.contact_number increased vs. baseline; V-01 and V-14 PASS",
  "output_file": "transformed/fact_billing/2026-03-10/fact_billing_2026-03-10.csv",
  "output_checksum": "9f2c4a1d8b7e6f5a3c2d1b0e9f8a7b6c",
  "quarantine_file": "quarantine/transformation/2026-03-10/fact_billing_rejected.csv",
  "error_message": null,
  "transformation_duration_ms": 27000,
  "rules_version": "1.0"
}
```

**Usage:**

- **Monitoring:** Airflow dashboard plus per-table status; V-21 rejection rate is the headline metric.
- **Troubleshooting:** `rules_applied` identifies which rule caused a volume shift; `validation_results` names the failing check; `quarantine_file` locates the offending rows.
- **Auditing:** `run_id` + `parent_run_id` + `rules_version` prove exactly which code transformed which extraction output, and when.
- **Data lineage:** `parent_run_id` links back to the Stage 1 log; `transformation_run_id` on every output row links forward to the Load Stage.
- **Recovery:** `batch_window_*` + `output_checksum` determine the safe reprocess range. Reprocessing the same `run_date` is always safe because reruns are idempotent (Section 1.4); a checksum mismatch against a previous run of the same `run_date` signals a rules-version change and requires review before promotion.

**Usage of Stage 1 metadata:** this stage reads `records_read` and `validation_result` from the Stage 1 run log for reconciliation (V-01) and for the "Extraction Validation Summary" line of the handover (Section 10). It never modifies or re-derives Stage 1 metadata.

---

## 9. Log Retention, Rejected Data, and Access

Stage 2 records, ownership, and retention are defined **separately** from Stage 1, even though both stages use the same log tree.

### 9.1 Transformation Log Retention

| Item | Policy |
| --- | --- |
| **Retention period** | Operational transformation logs: minimum **90 days**. Audit-relevant run summaries (`run_id`, `parent_run_id`, window, counts, `rules_applied`, `validation_results`, `rules_version`): minimum **1 year**. Justification: the flagged-account rule (B-11) and the monthly billing cycle mean a single consumer decision can require reading back up to a year of transformation history to explain an `outstanding_bill_count`. |
| **Storage location** | `logs/waterwise-transformation/YYYY-MM-DD/<run_id>_<table>.json`, alongside the Airflow task log for the same `run_id`. Output data lives in `transformed/<table>/<run_date>/`. |
| **Access permissions** | Data engineers and Super Admin (read). Write restricted to the transformation service account / Airflow worker. No consumer, meter-reader, or billing-clerk access. Credentials are never written to logs. |
| **Archiving** | Logs older than 90 days are compressed and moved to cold storage. Audit summaries stay online for 1 year. The archive preserves `run_id` and `parent_run_id` indexing so any lineage lookup still resolves. |
| **Deletion rules** | Logs must not be deleted before the retention period, or while needed for troubleshooting, auditing, or recovery. Deletion after retention requires data-engineer approval and is itself logged. Operational logs may be purged after 90 days; audit records only after 1 year. |

### 9.2 Quarantine (Rejected Records)

| Item | Policy |
| --- | --- |
| **Location** | `quarantine/transformation/<run_date>/<table>_rejected.csv` |
| **Retention** | Minimum **1 year**, matching the audit-summary period, because a rejected bill or payment is a financial record whose rejection a data engineer may need to explain long after the fact. Retained separately from operational logs. |
| **Content** | One row per rejected record: `run_id`, `parent_run_id`, `table_name`, `rule_id` (the C-/S-/X- rule that rejected it), `rejection_reason` (free text), and the source column values needed to diagnose the issue. |
| **Data minimization** | Only columns necessary to diagnose the rejection are stored. `consumers.email` and `consumers.contact_number` are **masked** (e.g. `j***@mail.com`, `09***1234`) in quarantine, since a rejected consumer row has no analytical need for readable PII. `consumers.password` is never written anywhere in this stage — Stage 1 does not extract it (Section 6.1 of `docs/extraction.md`), and V-24 asserts its absence. |
| **Access permissions** | Data engineers and Super Admin only (read). Write restricted to the transformation service account. |
| **Deletion rules** | Never deleted before 1 year. Deletion requires data-engineer approval and is logged. Rejected financial records additionally require Super Admin approval. |
| **Reprocessing** | A quarantined record may be re-admitted by fixing the source and re-running Stage 1 for that window, then reprocessing the transformation batch. Records are **not** manually promoted from quarantine, because that would bypass the reconciliation count in V-01. |

### 9.3 Ownership Summary

| Concern | Stage 1 (Extraction) | Stage 2 (Transformation) |
| --- | --- | --- |
| Logs record | Retrieval errors, extraction counts, source-to-staging reconciliation, range-sanity flags | Cleaning actions, conversion errors, validation failures, rule application counts |
| Log retention | 90 days operational / 1 year audit | 90 days operational / 1 year audit |
| Quarantine content | Rows with NULL identifiers, rejected at source-of-record level | Rows that could not be safely transformed, with `rule_id` + reason + batch ID |
| Quarantine retention | Per `docs/extraction.md` §5 | 1 year, with PII masked |
| Owner | Data engineer | Data engineer (Super Admin approves deletions) |

---

## 10. Transformation Acceptance and Handover Rules

### 10.1 Pre-Declared Rejection-Rate Threshold

> **Threshold: 1.0% per table and overall.**

**Justification (defined before any results are observed):** WaterWise billing is legally and operationally complete per consumer-month — a bill that is silently missing is a collections and an accountability problem, not a rounding artifact. A rejection rate above 1.0% cannot be explained by isolated corrupt records; it indicates either a systematic source defect (for example a code-set change in the billing application) or a cleaning rule that is too aggressive. Either cause warrants human review before the batch is allowed to influence collections reporting or flagged-account recommendations. Below 1.0%, the rejected records are individually traceable in quarantine and the retained population is materially complete.

The 5.0% escalation to automatic **FAIL** (V-21) exists because above that level the output is no longer a faithful representation of the input and must not be promoted under any review outcome.

### 10.2 Decision Rules

| Overall Condition | Acceptance Decision | Pipeline Action |
| --- | --- | --- |
| All tables PASS | **APPROVED** | Proceed to Load |
| One or more non-critical WARNING | **MANUAL REVIEW** | Hold batch |
| One or more critical FAIL | **REJECTED** | Stop, quarantine, or retry |

### 10.3 Required Decisions

- **Automatic Approval:** All critical checks in Section 7 pass, no non-critical WARNING is raised, and the rejection rate is ≤ 1.0%. The transformed output is packaged for the Load Stage automatically, including metadata and validation results.
- **Manual Review:** Any non-critical WARNING is raised (for example V-19 optional-field null rate degradation, V-20 unmapped-code rate above 1%, or V-22 idempotency checksum drift after a rules-version change), **or** the rejection rate is between 1.0% and 5.0%. The batch is held and not promoted. **Responsible: data engineer / Super Admin.** The reviewer inspects `quarantine_file` and `rules_applied`, then explicitly releases or rejects the batch. The hold state and the release decision are logged.
- **Rejection or Failure:** Any critical check fails, or the rejection rate exceeds 5.0%. The batch is **stopped**: no output is promoted to Load, all rejected records are quarantined with their `rule_id`, and the run is logged as `REJECTED`. **Responsible: data engineer** investigates via the Section 8 logs. If the cause is a transient read/parse error, the batch is **retried** up to 3 times with backoff; if the cause is data or contract related, the batch is **not** retried automatically — it is fixed and then reprocessed idempotently for the same `run_date`. If Stage 1 was the cause, Stage 1 is re-triggered for that window first.

### 10.4 Handover

The following is passed to the **Load Stage** (loading itself is not performed in this stage):

| Handover artifact | Contents |
| --- | --- |
| Transformed datasets | `transformed/<table>/<run_date>/<table>_<run_date>.csv` for `dim_purok`, `dim_consumer`, `fact_consumption`, `fact_billing`, `fact_payment`, each with a `.schema.json` sidecar describing the Section 6.1 contract |
| Metadata | Per-table JSON run log with every Section 8 field, including `parent_run_id` for lineage and `rules_version` for reproducibility |
| Validation results | Per-table PASS / WARNING / FAIL from Section 7, plus the overall decision and the acceptance basis |
| Quarantine | `quarantine/transformation/<run_date>/<table>_rejected.csv` with `rule_id` and reason, available to the Load Stage so it can reconcile rejected counts |
| Aggregation config | `reference_date` and `batch_window_*` used for all date-relative derivations, so the Load Stage can reproduce the same `days_overdue` and `outstanding_bill_count` values |
| Grouping config | `billing_period` (`YYYY-MM`) and `purok_key` are pre-computed and standardized, so downstream consumers group monthly and per-purok without re-deriving either |

**Example handover summary:**

| Validation Summary | Result |
| --- | --- |
| `dim_purok` transformation | PASS |
| `dim_consumer` transformation | PASS |
| `fact_consumption` transformation | PASS |
| `fact_billing` transformation | WARNING |
| `fact_payment` transformation | PASS |
| Extraction Validation Summary (from `parent_run_id`) | APPROVED |
| Rejection rate (overall) | 0.64% (≤ 1.0% threshold) |
| **Overall Transformation** | **MANUAL REVIEW** |

> **Requirement:** the handover contains transformed data only. Loading into the target database or warehouse must not be performed in this stage unless explicitly required. The transformed output is documented, validated, traceable, and packaged; the Load Stage owns persistence.

---

## 11. Common Transformation Problems and Mitigation

| # | Problem description | Possible cause | Impact on the transformation process | Proposed solution / mitigation |
| --- | --- | --- | --- | --- |
| 1 | Duplicate rows after a batch rerun | Batch rerun without idempotent output handling | Every count, sum, and derived attribute is inflated; `outstanding_bill_count` and `collection_rate` are wrong, and flagged-account recommendations become unreliable | Deterministic surrogate keys (D-01) plus atomic `os.replace()` writes into run-date-scoped directories (Section 1.4); V-22 asserts checksum stability across reruns |
| 2 | Late-arriving readings missing from the batch | Meter reader syncs an offline outbox after the batch window closed; `docs/extraction.md` §8 Problem 4 | A consumer-month is absent from `fact_consumption`, so their bill and any downstream demand forecast show a gap that looks like zero consumption rather than missing data | Reprocess a configurable lookback window (default 3 billing months) rather than only the current window; record `batch_window_start` so the widened scope is auditable; `fact_consumption.reading_date_adjusted` flags readings outside their billing month |
| 3 | Unexpected schema change in the staged input | Stage 1 contract extended or a source migration renamed a column | Rule application fails mid-batch; naive handling either crashes or silently writes NULLs into NOT NULL output columns | Input schema check against the Stage 1 contract before any rule runs (Section 6.4 step 1); fail fast without writing output; versioned contract and a schema-change ticket; no silent coercion |
| 4 | Row multiplication after a join | A lookup key is not unique — for example two `dim_consumer` rows share a `consumer_id` because the deduplication step was skipped | Output row counts exceed input row counts; `SUM(total_bill)` and `SUM(total_paid)` inflate, and the aggregate reconciliation checks (V-13/V-14/V-15) fail | Uniqueness guaranteed by C-06 and by natural-key checks V-04; V-07 compares pre-join and post-join row counts on every join and treats any increase as **critical**; add a critical uniqueness check whenever a new join is introduced |
| 5 | Cash reconciliation mismatch on payments | `amount_tendered`, `total_paid`, and `change_given` disagree — typically because the application stored a rounded or partial value | `collection_rate` and any cash reconciliation report disagree with the physical cash drawer, undermining trust in the collections analytics | C-09 recomputes `change_given` as `amount_tendered - total_paid` and sets `change_corrected`; V-10 asserts the identity within `0.01`; residual mismatches are logged for manual review |
| 6 | A new `status` or `payment_method` value appears after an application release | The billing or collection app adds a code the pipeline has never seen | Affected rows map to `UNKNOWN` / `OTHER`, so aging buckets and collection-method breakdowns silently lose detail | C-11 maps known synonyms and flags unmapped values; V-20 monitors the unmapped rate and raises a **warning** above 1%; `code_unmapped` flags the affected rows so a new code is noticed rather than absorbed |
| 7 | Batch exceeds its time window | Row-count growth in `consumption`/`billing` history, or the per-consumer window functions in B-10/B-12 becoming expensive at scale | The transformation finishes after the Load Stage's slot, delaying or overlapping downstream loads | Process tables in dependency order so the largest tables are not blocked behind the others; partition by `billing_period` in the Load Stage; index `fact_billing(consumer_key, status, due_date)` and `fact_payment(billing_key, payment_date)`; monitor `transformation_duration_ms` against a threshold and alert on growth before it breaches the window |
| 8 | Rounding drift between source sums and transformed sums | Independent rounding of each value versus rounding only at the aggregate | V-13/V-14/V-15 fail on a difference of a few centavos, producing a false critical rejection | All money columns are rounded half-up to 2dp exactly once at conversion (Section 4.2) and never re-rounded downstream; aggregate checks compare with a `0.01` tolerance; a larger drift indicates a genuine cleaning-rule defect and is investigated rather than absorbed |
| 9 | `purok_no` NULLs fragment the zone dimension | Consumers registered without a zone, or a zone assignment added after registration | Per-purok totals omit those consumers entirely, so zone revenue is understated and the zones are not comparable | C-04 maps missing zones to the single `UNKNOWN` sentinel with `purok_is_unknown = TRUE`; the `UNKNOWN` row is visible in every per-purok report so the gap is countable rather than invisible; V-19 tracks the null rate over time |
| 10 | Orphaned child records arriving late | A payment or bill was staged in a batch whose parent was rejected in an earlier batch, or a parent row is missing from the staged set for that window | Orphan facts would either break the output foreign keys or be dropped, and dropping them silently breaks the V-01 reconciliation | C-10 rejects orphans to quarantine with `rule_id=C-10` and counts them in `records_rejected`, so V-01 still balances; the parent-backfill behavior in Stage 1 (`docs/extraction.md` §1.3) means genuine orphans are rare, so a spike indicates a source integrity problem worth investigating |

---

## Expected Output

The documentation must be consistent with the implemented transformation process and provide sufficient information for a team member to understand, validate, monitor, and maintain the transformation stage.

The transformed data must be validated and packaged for handover to the next stage: **Load**. Loading into the target database or data warehouse must not be performed in this stage unless explicitly required.

> **Note on Section 2:** the profiling percentages in Section 2 are representative estimates based on the WaterWise source schema and the failure modes documented in `docs/extraction.md` §8. They are not measurements. On the first production run of this stage, they must be replaced with the actual counts produced by the profiling step, and `rules_version` must be incremented if any cleaning rule changes as a result.
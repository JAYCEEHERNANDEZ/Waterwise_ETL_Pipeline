"""WaterWise Stage 3: Data Load.

Implements docs/load.md:
- Input: Stage 2 handover ONLY (transformed/<table>/<run_date>/*.csv, the
  .schema.json sidecars, and the transformation run logs). This stage never
  connects to the main WaterWise database, never re-extracts and never
  re-transforms.
- Target: the WaterWise warehouse (second Supabase project, schema
  `warehouse`, created by db/warehouse_schema.sql).

Steps of one run:
1. Read the transformation handover (Section 1, Load Input).
2. Pre-load validation checks (Section 4). They only read.
3. Load all five tables in one transaction (Section 3), dimensions before
   facts: bulk copy into a staging table, reject rows that break a target
   constraint or have no parent row, then upsert on the match key (SCD Type 1).
4. Post-load validation and reconciliation (Section 5), inside the same
   transaction before the commit.
5. Decide (Section 8): APPROVED commits; MANUAL REVIEW and REJECTED roll back.
6. Record the run in warehouse.load_log and logs/waterwise-load/, and rejected
   rows in quarantine/load/ (Sections 6 and 7).

Connection: WAREHOUSE_DB_URL (session pooler, role etl_loader), read from
pipeline/.env or the repo-root .env. Never committed, never logged.

Usage:
    # Load the newest handover
    python -m pipeline.load

    # Load the handover of a specific transformation run date
    python -m pipeline.load --run-date 2026-03-10

    # Run every check and the full load, then roll back; writes nothing
    python -m pipeline.load --run-date 2026-03-10 --dry-run

    # Publish a batch held for warnings, after reviewing it
    python -m pipeline.load --run-date 2026-03-10 --accept-warnings "Juan Dela Cruz"

    # A transformation batch on MANUAL REVIEW, released by a named reviewer
    python -m pipeline.load --handover-released-by "Juan Dela Cruz"

    # Load a batch that was already loaded with SUCCESS (intentional reload)
    python -m pipeline.load --run-date 2026-03-10 --force-reload

Exit codes:
    0 = APPROVED (committed; with --dry-run: would be committed)
    3 = MANUAL REVIEW (rolled back, waiting for --accept-warnings)
    1 = REJECTED (rolled back; a data or contract problem, do not retry)
    2 = usage / input error (no handover to read)
    4 = REJECTED because the warehouse could not be reached (safe to retry)

Tests: python -m unittest discover -s tests -v
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

try:
    from dotenv import load_dotenv
    _HERE = Path(__file__).resolve().parent
    # Same order as stage.py: pipeline/.env first, then the repo-root .env.
    load_dotenv(_HERE / ".env")
    load_dotenv(_HERE.parent / ".env")
except ImportError:  # pragma: no cover - environment variables still work
    pass

try:
    import psycopg2
    from psycopg2 import sql as psycopg2_sql
except ImportError:  # pragma: no cover - surfaced by the connection check
    psycopg2 = None
    psycopg2_sql = None

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent  # repo root (Waterwise_ETL_Pipeline/)
TRANSFORMED_ROOT = BASE_DIR / "transformed"
TRANSFORMATION_LOG_ROOT = BASE_DIR / "logs" / "waterwise-transformation"
LOAD_LOG_ROOT = BASE_DIR / "logs" / "waterwise-load"
QUARANTINE_ROOT = BASE_DIR / "quarantine" / "load"

LOAD_CODE_VERSION = "v1.0"
WAREHOUSE_SCHEMA = "warehouse"

# Target connection check (docs/load.md Section 4).
CONNECT_TIMEOUT_S = 10
CONNECT_ATTEMPTS = 3
CONNECT_BACKOFF_S = (2, 4)

# Storage checks (docs/load.md Section 4). The Supabase free plan allows
# 500 MB; override with WAREHOUSE_STORAGE_LIMIT_MB on a paid plan.
STORAGE_LIMIT_MB_DEFAULT = 500
STORAGE_FAIL_PCT = 95
STORAGE_WARN_PCT = 80

# Columns the load stage adds to every dimension and fact table; they are not
# part of the transformation handover.
LOAD_COLUMNS = ("load_batch_id", "loaded_at")

# Expected target column types (docs/transformation.md Section 6.1, as created
# by db/warehouse_schema.sql), in information_schema.columns.data_type terms.
_CHAR, _VARCHAR = "character", "character varying"
_INT, _SMALLINT, _NUM = "integer", "smallint", "numeric"
_DATE, _TS, _BOOL = "date", "timestamp with time zone", "boolean"
EXPECTED_TYPES: dict[str, dict[str, str]] = {
    "dim_purok": {
        "purok_key": _CHAR, "purok_no": _INT, "purok_label": _VARCHAR,
        "is_unknown": _BOOL,
    },
    "dim_consumer": {
        "consumer_key": _CHAR, "consumer_id": _INT, "username": _VARCHAR,
        "first_name": _VARCHAR, "middle_name": _VARCHAR, "last_name": _VARCHAR,
        "full_name": _VARCHAR, "email": _VARCHAR, "contact_number": _VARCHAR,
        "purok_key": _CHAR, "purok_no": _INT, "purok_is_unknown": _BOOL,
        "status": _VARCHAR, "code_unmapped": _BOOL, "active_flag": _BOOL,
        "valid_from": _TS, "valid_to": _TS, "is_current": _BOOL,
        "created_at": _TS, "updated_at": _TS, "transformation_run_id": _VARCHAR,
    },
    "fact_consumption": {
        "consumption_key": _CHAR, "consumption_id": _INT, "consumer_key": _CHAR,
        "reading_date": _DATE, "reading_year": _SMALLINT, "reading_month": _SMALLINT,
        "billing_period": _CHAR, "previous_reading": _NUM, "present_reading": _NUM,
        "consumption": _NUM, "consumption_cbm": _NUM, "days_in_period": _INT,
        "consumption_per_day": _NUM, "reading_date_adjusted": _BOOL,
        "transformation_run_id": _VARCHAR,
    },
    "fact_billing": {
        "billing_key": _CHAR, "billing_id": _INT, "consumption_key": _CHAR,
        "consumer_key": _CHAR, "billing_date": _DATE, "due_date": _DATE,
        "billing_year": _SMALLINT, "billing_month": _SMALLINT, "billing_period": _CHAR,
        "total_bill": _NUM, "remaining_balance": _NUM, "status": _VARCHAR,
        "status_recomputed": _BOOL, "balance_corrected": _BOOL,
        "due_date_corrected": _BOOL, "is_revenue_recognized": _BOOL,
        "days_to_due": _INT, "days_overdue": _INT, "is_overdue": _BOOL,
        "overdue_bucket": _VARCHAR, "outstanding_bill_count": _INT,
        "is_flagged_account": _BOOL, "transformation_run_id": _VARCHAR,
    },
    "fact_payment": {
        "payment_key": _CHAR, "payment_id": _INT, "billing_key": _CHAR,
        "payment_date": _DATE, "payment_year": _SMALLINT, "payment_month": _SMALLINT,
        "payment_method": _VARCHAR, "code_unmapped": _BOOL, "total_paid": _NUM,
        "amount_tendered": _NUM, "change_given": _NUM, "remaining_balance": _NUM,
        "reference_number": _VARCHAR, "payment_allocation_rank": _INT,
        "is_oldest_unpaid_payment": _BOOL, "collection_rate": _NUM,
        "change_corrected": _BOOL, "transformation_run_id": _VARCHAR,
    },
}
for _columns in EXPECTED_TYPES.values():
    _columns.update({"load_batch_id": _VARCHAR, "loaded_at": _TS})

# Asia/Manila has no DST; a fixed +08:00 offset is exact.
PHT = timezone(timedelta(hours=8))

# Load order (docs/load.md Section 1, Load Scope): dimensions before facts,
# parents before children.
LOAD_ORDER = (
    "dim_purok",
    "dim_consumer",
    "fact_consumption",
    "fact_billing",
    "fact_payment",
)

# docs/load.md Section 2 and 3: surrogate primary key, and the match key the
# upsert uses to find an existing row (the natural key from the source).
PRIMARY_KEYS = {
    "dim_purok": "purok_key",
    "dim_consumer": "consumer_key",
    "fact_consumption": "consumption_key",
    "fact_billing": "billing_key",
    "fact_payment": "payment_key",
}
MATCH_KEYS = {
    "dim_purok": "purok_no",
    "dim_consumer": "consumer_id",
    "fact_consumption": "consumption_id",
    "fact_billing": "billing_id",
    "fact_payment": "payment_id",
}
# (column, parent table, parent column) for every foreign key in the warehouse.
FOREIGN_KEYS = {
    "dim_purok": [],
    "dim_consumer": [("purok_key", "dim_purok", "purok_key")],
    "fact_consumption": [("consumer_key", "dim_consumer", "consumer_key")],
    "fact_billing": [("consumption_key", "fact_consumption", "consumption_key"),
                     ("consumer_key", "dim_consumer", "consumer_key")],
    "fact_payment": [("billing_key", "fact_billing", "billing_key")],
}


def log(msg: str) -> None:
    ts = datetime.now(PHT).isoformat()
    print(f"[{ts}] {msg}", flush=True)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# Reading the Stage 2 handover (docs/load.md Section 1, Load Input)
# ---------------------------------------------------------------------------

class HandoverError(Exception):
    """Raised when there is no handover to read at all (exit code 2)."""


@dataclass
class TableHandover:
    """Everything Stage 2 handed over for one table."""

    table: str
    csv_path: Path
    schema_path: Path
    log_path: Path | None = None
    # From the CSV file itself
    header: list[str] = field(default_factory=list)
    rows: list[dict[str, str]] = field(default_factory=list)
    checksum: str | None = None
    # From the .schema.json sidecar
    columns: list[str] = field(default_factory=list)
    required_not_null: list[str] = field(default_factory=list)
    transformation_run_id: str | None = None
    # From the transformation run log
    transformation_status: str | None = None
    records_output: int | None = None
    expected_checksum: str | None = None

    @property
    def row_count(self) -> int:
        return len(self.rows)


@dataclass
class Handover:
    """One batch = the five tables of one transformation run date."""

    run_date: str
    tables: dict[str, TableHandover] = field(default_factory=dict)
    transformation_run_id: str | None = None
    parent_run_id: str | None = None
    rules_version: str | None = None
    batch_window_start: str | None = None
    batch_window_end: str | None = None
    # Missing or inconsistent artifacts. The pre-load checks turn these into
    # a critical "handover completeness" failure.
    problems: list[str] = field(default_factory=list)

    @property
    def batch_id(self) -> str:
        return f"ww-batch-{self.run_date}"

    @property
    def batch_window(self) -> str:
        start = (self.batch_window_start or self.run_date)[:10]
        end = (self.batch_window_end or self.run_date)[:10]
        return f"{start} to {end}"

    @property
    def total_rows(self) -> int:
        return sum(t.row_count for t in self.tables.values())


def _is_iso_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def resolve_run_date(run_date: str | None, transformed_root: Path) -> str:
    """Return the batch date to load.

    With no date given, the newest date that has a `dim_consumer` handover is
    used. A date that was asked for but does not exist is an error: the load
    never silently switches to a different batch.
    """
    if run_date is not None and not _is_iso_date(run_date):
        raise HandoverError(f"invalid --run-date {run_date!r} (expected YYYY-MM-DD)")

    available: set[str] = set()
    for table in LOAD_ORDER:
        table_dir = transformed_root / table
        if table_dir.is_dir():
            available.update(d.name for d in table_dir.iterdir()
                             if d.is_dir() and _is_iso_date(d.name))
    if not available:
        raise HandoverError(
            f"no transformation handover found under {transformed_root}. "
            "Run Stage 2 first: python -m pipeline.transform")

    if run_date is None:
        return max(available)
    if run_date not in available:
        raise HandoverError(
            f"no transformation handover for {run_date} under {transformed_root} "
            f"(newest available: {max(available)})")
    return run_date


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def read_table_handover(table: str, run_date: str, transformed_root: Path,
                        log_root: Path, problems: list[str]) -> tuple[TableHandover, dict[str, Any]]:
    """Read the CSV, the sidecar, and the run log of one table.

    Anything missing or unreadable is appended to `problems` instead of
    raising, so one run reports every gap in the handover at once.
    Returns the table handover and its transformation log (empty if absent).
    """
    batch_dir = transformed_root / table / run_date
    item = TableHandover(
        table=table,
        csv_path=batch_dir / f"{table}_{run_date}.csv",
        schema_path=batch_dir / f"{table}_{run_date}.schema.json",
    )

    # --- CSV: rows are kept as text; an empty string means NULL -------------
    if item.csv_path.is_file():
        item.checksum = sha256_file(item.csv_path)
        with open(item.csv_path, newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            item.header = list(reader.fieldnames or [])
            item.rows = [dict(row) for row in reader]
    else:
        problems.append(f"{table}: missing CSV {item.csv_path}")

    # --- Schema sidecar: the column contract and the producing run ----------
    sidecar = _read_json(item.schema_path) if item.schema_path.is_file() else None
    if sidecar is None:
        problems.append(f"{table}: missing or unreadable schema file {item.schema_path}")
        sidecar = {}
    item.columns = list(sidecar.get("columns") or [])
    item.required_not_null = list(sidecar.get("required_not_null") or [])
    item.transformation_run_id = sidecar.get("run_id")

    # --- Transformation run log of the run that wrote this CSV --------------
    # Several transformation runs can exist for one run date; the sidecar names
    # the one that produced the file on disk.
    log_payload: dict[str, Any] = {}
    if item.transformation_run_id:
        item.log_path = log_root / run_date / f"{item.transformation_run_id}_{table}.json"
        loaded = _read_json(item.log_path) if item.log_path.is_file() else None
        if loaded is None:
            problems.append(f"{table}: missing or unreadable transformation log {item.log_path}")
        else:
            log_payload = loaded
    elif sidecar:
        problems.append(f"{table}: schema file has no run_id, cannot locate the transformation log")

    item.transformation_status = log_payload.get("transformation_status")
    records_output = log_payload.get("records_output")
    item.records_output = records_output if isinstance(records_output, int) else None
    item.expected_checksum = (log_payload.get("output_checksums") or {}).get(table)

    return item, {**sidecar, **log_payload}


def read_handover(run_date: str | None = None,
                  transformed_root: Path | None = None,
                  log_root: Path | None = None) -> Handover:
    """Read the whole Stage 2 handover for one batch. Touches no database."""
    transformed_root = transformed_root or TRANSFORMED_ROOT
    log_root = log_root or TRANSFORMATION_LOG_ROOT

    handover = Handover(run_date=resolve_run_date(run_date, transformed_root))
    run_ids: dict[str, str] = {}

    for table in LOAD_ORDER:
        item, meta = read_table_handover(
            table, handover.run_date, transformed_root, log_root, handover.problems)
        handover.tables[table] = item
        if item.transformation_run_id:
            run_ids[table] = item.transformation_run_id
        # Batch-level metadata: the first table that provides a value wins.
        handover.parent_run_id = handover.parent_run_id or meta.get("parent_run_id")
        handover.rules_version = handover.rules_version or meta.get("rules_version")
        handover.batch_window_start = handover.batch_window_start or meta.get("batch_window_start")
        handover.batch_window_end = handover.batch_window_end or meta.get("batch_window_end")

    # All five files must come from the same transformation run; a mix means a
    # transformation rerun was interrupted or files were copied by hand.
    distinct = sorted(set(run_ids.values()))
    if len(distinct) == 1:
        handover.transformation_run_id = distinct[0]
    elif len(distinct) > 1:
        handover.problems.append(
            "tables come from different transformation runs: "
            + ", ".join(f"{t}={r}" for t, r in run_ids.items()))

    return handover


def describe_handover(handover: Handover) -> None:
    log(f"Handover {handover.batch_id} | transformation_run_id={handover.transformation_run_id} "
        f"| parent_run_id={handover.parent_run_id} | window={handover.batch_window}")
    for table in LOAD_ORDER:
        item = handover.tables[table]
        log(f"  {table}: rows={item.row_count} columns={len(item.header)} "
            f"status={item.transformation_status} records_output={item.records_output}")
    for problem in handover.problems:
        log(f"  PROBLEM: {problem}")


# ---------------------------------------------------------------------------
# Warehouse connection
# ---------------------------------------------------------------------------

def get_warehouse_dsn() -> str | None:
    value = os.environ.get("WAREHOUSE_DB_URL", "").strip().strip('"').strip("'")
    return value or None


def mask(text: str, dsn: str | None) -> str:
    """Remove the password (raw and URL-decoded) from a message before printing it."""
    if not dsn:
        return text
    try:
        raw = urlsplit(dsn).password
    except ValueError:
        raw = None
    for secret in {raw, unquote(raw) if raw else None}:
        if secret:
            text = text.replace(secret, "***")
    return text


def project_ref(dsn: str | None) -> str | None:
    """Supabase project ref from a DSN: pooler user `role.<ref>` or host `db.<ref>.supabase.co`."""
    if not dsn:
        return None
    try:
        parts = urlsplit(dsn)
        user, host = parts.username or "", parts.hostname or ""
    except ValueError:
        return None
    if "." in user:
        return user.split(".", 1)[1]
    if host.startswith("db.") and host.endswith(".supabase.co"):
        return host.split(".")[1]
    return None


def connect_warehouse(dsn: str):
    """Connect with CONNECT_ATTEMPTS attempts. Raises the last error."""
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is not installed. Run: pip install -r pipeline/requirements.txt")
    last_exc: Exception | None = None
    for attempt in range(1, CONNECT_ATTEMPTS + 1):
        try:
            conn = psycopg2.connect(dsn, connect_timeout=CONNECT_TIMEOUT_S,
                                    application_name="waterwise-load")
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError as exc:
            last_exc = exc
            log(f"warehouse connection attempt {attempt}/{CONNECT_ATTEMPTS} failed: "
                f"{mask(str(exc).strip(), dsn)}")
            # A wrong password does not fix itself, and repeated failed logins
            # make the Supabase pooler block new connections for a while
            # (ECIRCUITBREAKER). Retrying either one only extends the block.
            message = str(exc)
            if "authentication failed" in message or "ECIRCUITBREAKER" in message:
                break
            if attempt < CONNECT_ATTEMPTS:
                time.sleep(CONNECT_BACKOFF_S[min(attempt - 1, len(CONNECT_BACKOFF_S) - 1)])
    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Pre-load validation checks (docs/load.md Section 4)
# ---------------------------------------------------------------------------

CRITICAL, NON_CRITICAL = "Critical", "Non-critical"


@dataclass
class Check:
    name: str
    target: str
    criteria: str
    severity: str
    status: str = "PASS"          # PASS / WARNING / FAIL / NOT RUN
    detail: str = ""

    def as_dict(self) -> dict[str, str]:
        return {"name": self.name, "target": self.target, "criteria": self.criteria,
                "severity": self.severity, "status": self.status, "detail": self.detail}


def check_target_connection(dsn: str | None) -> tuple[Check, Any]:
    check = Check("Target connection", "Warehouse database",
                  f"Connection succeeds within {CONNECT_TIMEOUT_S} seconds "
                  f"(up to {CONNECT_ATTEMPTS} attempts)", CRITICAL)
    if not dsn:
        check.status, check.detail = "FAIL", "WAREHOUSE_DB_URL is not set (pipeline/.env or .env)"
        return check, None
    # Safety: the load must never write to the main application database.
    main_ref = project_ref(os.environ.get("WATERWISE_DB_URL"))
    target_ref = project_ref(dsn)
    if main_ref and target_ref and main_ref == target_ref:
        check.status = "FAIL"
        check.detail = ("WAREHOUSE_DB_URL points at the main WaterWise project; "
                        "the load only writes to the warehouse project")
        return check, None
    try:
        conn = connect_warehouse(dsn)
    except Exception as exc:  # noqa: BLE001 - reported as a failed check
        check.status = "FAIL"
        check.detail = mask(f"{type(exc).__name__}: {str(exc).strip()}", dsn)
        return check, None
    with conn.cursor() as cur:
        cur.execute("SELECT current_user, current_database()")
        user, database = cur.fetchone()
    conn.rollback()
    check.detail = f"connected as {user} to {database} (project {target_ref or 'unknown'})"
    return check, conn


def check_handover_completeness(handover: Handover) -> Check:
    check = Check("Handover completeness", f"transformed/<table>/{handover.run_date}/",
                  "CSV, .schema.json, and transformation log exist for all five tables",
                  CRITICAL)
    if handover.problems:
        check.status = "FAIL"
        check.detail = "; ".join(handover.problems)
    else:
        check.detail = f"{len(handover.tables)} tables complete"
    return check


def check_handover_status(handover: Handover, released_by: str | None) -> Check:
    check = Check("Handover status", "Transformation metadata",
                  "Status = APPROVED for all tables (MANUAL REVIEW only when released "
                  "by a named reviewer)", CRITICAL)
    statuses = {t: item.transformation_status for t, item in handover.tables.items()}
    not_approved = {t: s for t, s in statuses.items() if s != "APPROVED"}
    if not not_approved:
        check.detail = "all tables APPROVED"
    elif released_by and all(s == "MANUAL REVIEW" for s in not_approved.values()):
        check.detail = f"MANUAL REVIEW batch released by {released_by}"
    else:
        check.status = "FAIL"
        check.detail = ", ".join(f"{t}={s}" for t, s in not_approved.items())
    return check


def check_record_count(handover: Handover) -> Check:
    check = Check("Record count", "Handover CSV files",
                  "Row count of each CSV = records_output in the transformation log",
                  CRITICAL)
    bad = [f"{t}: csv={item.row_count} log={item.records_output}"
           for t, item in handover.tables.items()
           if item.records_output is None or item.row_count != item.records_output]
    if bad:
        check.status, check.detail = "FAIL", "; ".join(bad)
    else:
        check.detail = f"{handover.total_rows} rows match the transformation log"
    return check


def check_file_integrity(handover: Handover) -> Check:
    check = Check("File integrity", "Handover CSV files",
                  "SHA-256 of each CSV = checksum in the transformation log", CRITICAL)
    bad = [t for t, item in handover.tables.items()
           if not item.checksum or item.checksum != item.expected_checksum]
    if bad:
        check.status = "FAIL"
        check.detail = "checksum missing or different for: " + ", ".join(bad)
    else:
        check.detail = "all checksums match"
    return check


def _target_columns(cur) -> dict[str, dict[str, str]]:
    cur.execute(
        """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = ANY(%s)
        ORDER BY table_name, ordinal_position
        """,
        (WAREHOUSE_SCHEMA, list(LOAD_ORDER)),
    )
    found: dict[str, dict[str, str]] = {}
    for table, column, data_type in cur.fetchall():
        found.setdefault(table, {})[column] = data_type
    return found


def check_schema_match(handover: Handover, cur) -> Check:
    check = Check("Schema match", "All target tables",
                  "All handover columns exist in the target with expected types; "
                  "no missing or extra columns", CRITICAL)
    target = _target_columns(cur)
    problems: list[str] = []
    for table, item in handover.tables.items():
        if item.header and item.columns and item.header != item.columns:
            problems.append(f"{table}: CSV header differs from its .schema.json")
        columns = target.get(table)
        if not columns:
            problems.append(f"{table}: table {WAREHOUSE_SCHEMA}.{table} not found")
            continue
        handover_cols = set(item.header or item.columns)
        target_cols = set(columns) - set(LOAD_COLUMNS)
        if handover_cols:
            missing = sorted(handover_cols - target_cols)
            extra = sorted(target_cols - handover_cols)
            if missing:
                problems.append(f"{table}: not in target {missing}")
            if extra:
                problems.append(f"{table}: not in handover {extra}")
        missing_load_cols = [c for c in LOAD_COLUMNS if c not in columns]
        if missing_load_cols:
            problems.append(f"{table}: load columns missing {missing_load_cols}")
        wrong = [f"{c} is {columns[c]}, expected {expected}"
                 for c, expected in EXPECTED_TYPES[table].items()
                 if c in columns and columns[c] != expected]
        if wrong:
            problems.append(f"{table}: " + "; ".join(wrong))
    if problems:
        check.status, check.detail = "FAIL", " | ".join(problems)
    else:
        check.detail = "5 tables match the handover columns and expected types"
    return check


# Row Level Security command codes in pg_policy.polcmd.
_POLICY_CMD = {"SELECT": "r", "INSERT": "a", "UPDATE": "w"}


def _rls_allows(cur, qualified: str, privilege: str) -> bool:
    """True unless Row Level Security would block `privilege` for the current role.

    With RLS on, a role that is not the owner (and has no BYPASSRLS) needs a
    policy for the command; without one it reads no rows and cannot write.
    """
    cur.execute(
        """
        SELECT NOT c.relrowsecurity
            OR pg_get_userbyid(c.relowner) = current_user
            OR (SELECT rolbypassrls FROM pg_roles WHERE rolname = current_user)
            OR EXISTS (
                SELECT 1 FROM pg_policy p
                WHERE p.polrelid = c.oid
                  AND p.polcmd IN (%s, '*')
                  AND (0 = ANY (p.polroles)
                       OR (SELECT oid FROM pg_roles WHERE rolname = current_user)
                          = ANY (p.polroles)))
        FROM pg_class c
        WHERE c.oid = to_regclass(%s)
        """,
        (_POLICY_CMD[privilege], qualified),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def check_write_permission(cur) -> Check:
    check = Check("Write permission", f"{WAREHOUSE_SCHEMA} schema, load role",
                  "Role can read, INSERT and UPDATE all five tables, read and INSERT into "
                  "load_log (GRANTs and row-level security policies), and create "
                  "temporary tables", CRITICAL)
    wanted = [(t, p) for t in LOAD_ORDER for p in ("SELECT", "INSERT", "UPDATE")]
    wanted += [("load_log", "SELECT"), ("load_log", "INSERT")]
    missing: list[str] = []
    for table, privilege in wanted:
        qualified = f"{WAREHOUSE_SCHEMA}.{table}"
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (qualified,))
        if not cur.fetchone()[0]:
            missing.append(f"{qualified} does not exist")
            continue
        cur.execute("SELECT has_table_privilege(current_user, %s, %s)", (qualified, privilege))
        if not cur.fetchone()[0]:
            missing.append(f"{privilege} on {qualified}")
        elif not _rls_allows(cur, qualified, privilege):
            missing.append(f"row-level security policy for {privilege} on {qualified}")
    cur.execute("SELECT has_database_privilege(current_user, current_database(), 'TEMP')")
    if not cur.fetchone()[0]:
        missing.append("TEMP on the database")
    if missing:
        check.status = "FAIL"
        check.detail = "missing: " + ", ".join(dict.fromkeys(missing))
    else:
        check.detail = "all required privileges present"
    return check


def check_duplicate_batch(handover: Handover, cur, force_reload: bool) -> Check:
    check = Check("Duplicate batch", f"{WAREHOUSE_SCHEMA}.load_log.batch_id",
                  "Batch ID not previously loaded with SUCCESS", CRITICAL)
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"{WAREHOUSE_SCHEMA}.load_log",))
    if not cur.fetchone()[0]:
        check.status, check.detail = "FAIL", f"{WAREHOUSE_SCHEMA}.load_log does not exist"
        return check
    cur.execute(
        f"SELECT count(DISTINCT run_id), max(load_end_ts) FROM {WAREHOUSE_SCHEMA}.load_log "
        "WHERE batch_id = %s AND status = 'SUCCESS'",
        (handover.batch_id,),
    )
    runs, last_end = cur.fetchone()
    if not runs:
        check.detail = f"{handover.batch_id} has not been loaded before"
    elif force_reload:
        check.detail = (f"{handover.batch_id} already loaded {runs} time(s), last at "
                        f"{last_end}; reloading because --force-reload was given")
    else:
        check.status = "FAIL"
        check.detail = (f"{handover.batch_id} already loaded with SUCCESS (last at {last_end}). "
                        "Use --force-reload to load it again on purpose")
    return check


def _storage_limit_mb() -> int:
    try:
        return int(os.environ.get("WAREHOUSE_STORAGE_LIMIT_MB", STORAGE_LIMIT_MB_DEFAULT))
    except ValueError:
        return STORAGE_LIMIT_MB_DEFAULT


def check_storage(cur) -> list[Check]:
    limit_mb = _storage_limit_mb()
    fail_check = Check("Storage space", "Warehouse database size",
                       f"Used space is below {STORAGE_FAIL_PCT}% of the {limit_mb} MB limit",
                       CRITICAL)
    warn_check = Check("Storage warning", "Warehouse database size",
                       f"Used space is below {STORAGE_WARN_PCT}% of the {limit_mb} MB limit",
                       NON_CRITICAL)
    cur.execute("SELECT pg_database_size(current_database())")
    used_mb = cur.fetchone()[0] / (1024 * 1024)
    used_pct = used_mb / limit_mb * 100 if limit_mb > 0 else 0.0
    detail = f"{used_mb:.1f} MB of {limit_mb} MB used ({used_pct:.1f}%)"
    fail_check.detail = warn_check.detail = detail
    if used_pct >= STORAGE_FAIL_PCT:
        fail_check.status = "FAIL"
    if used_pct >= STORAGE_WARN_PCT:
        warn_check.status = "WARNING"
    return [fail_check, warn_check]


# Checks that need the warehouse; reported as NOT RUN when it is unreachable.
_DB_CHECKS = (
    ("Write permission", f"{WAREHOUSE_SCHEMA} schema, load role"),
    ("Schema match", "All target tables"),
    ("Duplicate batch", f"{WAREHOUSE_SCHEMA}.load_log.batch_id"),
    ("Storage space", "Warehouse database size"),
    ("Storage warning", "Warehouse database size"),
)


def run_pre_load_checks(handover: Handover, dsn: str | None, *,
                        force_reload: bool = False,
                        released_by: str | None = None) -> tuple[list[Check], Any]:
    """Run every Section 4 check. Reads only; writes nothing.

    Returns the checks and the open warehouse connection (None when the
    connection failed), so the load can reuse it.
    """
    connection_check, conn = check_target_connection(dsn)
    checks = [
        connection_check,
        check_handover_completeness(handover),
        check_handover_status(handover, released_by),
        check_record_count(handover),
        check_file_integrity(handover),
    ]
    if conn is None:
        for name, target in _DB_CHECKS:
            severity = NON_CRITICAL if name == "Storage warning" else CRITICAL
            checks.append(Check(name, target, "", severity, "NOT RUN",
                                "warehouse not reachable"))
        return checks, None
    try:
        with conn.cursor() as cur:
            checks.append(check_write_permission(cur))
            checks.append(check_schema_match(handover, cur))
            checks.append(check_duplicate_batch(handover, cur, force_reload))
            checks.extend(check_storage(cur))
    except Exception as exc:  # noqa: BLE001 - a query error is a failed check
        checks.append(Check("Pre-load query", "Warehouse database",
                            "Pre-load queries run without errors", CRITICAL, "FAIL",
                            mask(f"{type(exc).__name__}: {str(exc).strip()}", dsn)))
    finally:
        conn.rollback()  # end the read-only transaction; nothing was written
    return checks, conn


def summarize_checks(checks: list[Check]) -> str:
    """`PASS`, or the names of the checks that did not pass (load_log.pre_load_result)."""
    bad = [f"{c.status}: {c.name}" for c in checks if c.status != "PASS"]
    return "PASS" if not bad else "; ".join(bad)


def decide(checks: list[Check]) -> str:
    """docs/load.md Section 8: any critical FAIL -> REJECTED, any WARNING -> MANUAL REVIEW."""
    if any(c.status in ("FAIL", "NOT RUN") and c.severity == CRITICAL for c in checks):
        return "REJECTED"
    if any(c.status in ("WARNING", "NOT RUN") for c in checks):
        return "MANUAL REVIEW"
    return "APPROVED"


def print_checks(title: str, checks: list[Check]) -> None:
    log(title)
    width = max(len(c.name) for c in checks)
    for c in checks:
        log(f"  {c.status:<8} {c.name:<{width}}  [{c.severity}] {c.detail}")


# ---------------------------------------------------------------------------
# Writing a table (docs/load.md Section 3, Write Logic)
# ---------------------------------------------------------------------------
# For each table:
#   1. Check every row against the target column types, lengths and NOT NULL.
#   2. Bulk-copy the valid rows into a temporary staging table.
#   3. Reject rows that break a CHECK constraint, have no parent row, or would
#      collide with a different record already in the target.
#   4. Upsert the rest: INSERT ... ON CONFLICT (match key) DO UPDATE.
# Rejected rows are not written; they are counted and kept with a reason.
# Nothing here commits: the caller owns the transaction.

@dataclass
class RejectedRow:
    table: str
    row_no: int                 # 1-based data row number in the handover CSV
    key: str | None             # match key value, for tracing
    reason: str
    values: dict[str, str]


@dataclass
class TableResult:
    table: str
    started_at: datetime
    ended_at: datetime | None = None
    received: int = 0
    inserted: int = 0
    updated: int = 0
    rejected: list[RejectedRow] = field(default_factory=list)

    @property
    def duration_sec(self) -> int:
        end = self.ended_at or datetime.now(PHT)
        return max(0, round((end - self.started_at).total_seconds()))


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    max_length: int | None
    precision: int | None
    scale: int | None
    nullable: bool


INT_RANGES = {"integer": (-2**31, 2**31 - 1), "smallint": (-2**15, 2**15 - 1),
              "bigint": (-2**63, 2**63 - 1)}
BOOL_TEXT = {"true", "false", "t", "f", "1", "0", "yes", "no"}


def target_column_info(cur, table: str) -> list[ColumnInfo]:
    cur.execute(
        """
        SELECT column_name, data_type, character_maximum_length,
               numeric_precision, numeric_scale, is_nullable = 'YES'
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (WAREHOUSE_SCHEMA, table),
    )
    return [ColumnInfo(*row) for row in cur.fetchall()]


def value_problem(value: str, col: ColumnInfo) -> str | None:
    """Why `value` cannot be stored in `col`, or None if it can. '' means NULL."""
    if value == "":
        return None if col.nullable else f"NOT_NULL: {col.name} is empty"
    kind = col.data_type
    try:
        if kind in INT_RANGES:
            low, high = INT_RANGES[kind]
            if not low <= int(value) <= high:
                return f"TYPE: {col.name}={value!r} is out of range for {kind}"
        elif kind == "numeric":
            number = Decimal(value)
            if not number.is_finite():
                return f"TYPE: {col.name}={value!r} is not a finite number"
            if col.precision is not None and col.scale is not None:
                whole_digits = len(str(abs(int(number)))) if abs(number) >= 1 else 0
                if whole_digits > col.precision - col.scale:
                    return (f"TYPE: {col.name}={value!r} does not fit "
                            f"numeric({col.precision},{col.scale})")
        elif kind == "date":
            date.fromisoformat(value)
        elif kind.startswith("timestamp"):
            datetime.fromisoformat(value)
        elif kind == "boolean":
            if value.strip().lower() not in BOOL_TEXT:
                return f"TYPE: {col.name}={value!r} is not a boolean"
    except (ValueError, ArithmeticError):
        return f"TYPE: {col.name}={value!r} is not a valid {kind}"
    if col.max_length is not None and len(value) > col.max_length:
        return f"TYPE: {col.name} is {len(value)} characters, limit {col.max_length}"
    return None


def _ident(*parts: str):
    return psycopg2_sql.Identifier(*parts)


def _check_expressions(cur, table: str) -> list[tuple[str, str]]:
    """(constraint name, boolean expression) for every CHECK on the target table."""
    cur.execute(
        """
        SELECT conname, pg_get_constraintdef(oid)
        FROM pg_constraint
        WHERE conrelid = to_regclass(%s) AND contype = 'c'
        ORDER BY conname
        """,
        (f"{WAREHOUSE_SCHEMA}.{table}",),
    )
    expressions = []
    for name, definition in cur.fetchall():
        expression = definition.strip()
        if expression.upper().startswith("CHECK"):
            expression = expression[5:].strip()
        if expression.upper().endswith("NOT VALID"):
            expression = expression[:-9].strip()
        expressions.append((name, expression))
    return expressions


def load_table(cur, item: TableHandover, batch_id: str) -> TableResult:
    """Write one handover table into the warehouse. Does not commit."""
    table = item.table
    result = TableResult(table=table, started_at=datetime.now(PHT), received=item.row_count)
    match_key, primary_key = MATCH_KEYS[table], PRIMARY_KEYS[table]
    columns = list(item.header)
    target = {c.name: c for c in target_column_info(cur, table)}

    def reject(row_no: int, reason: str) -> None:
        values = item.rows[row_no - 1]
        result.rejected.append(RejectedRow(table, row_no, values.get(match_key) or None,
                                           reason, values))

    # --- 1. Types, lengths, NOT NULL, and duplicate keys within the batch ----
    valid: list[tuple[int, dict[str, str]]] = []
    seen_match: set[str] = set()
    seen_primary: set[str] = set()
    for row_no, row in enumerate(item.rows, start=1):
        problem = next((p for c in columns
                        if (p := value_problem(row.get(c) or "", target[c])) is not None), None)
        if problem is None and row[match_key] in seen_match:
            problem = f"DUPLICATE_KEY: {match_key}={row[match_key]} appears more than once"
        if problem is None and row[primary_key] in seen_primary:
            problem = f"DUPLICATE_KEY: {primary_key}={row[primary_key]} appears more than once"
        if problem:
            reject(row_no, problem)
            continue
        seen_match.add(row[match_key])
        seen_primary.add(row[primary_key])
        valid.append((row_no, row))

    # --- 2. Bulk-copy valid rows into a typed staging table ------------------
    staging = _ident("pg_temp", f"stg_{table}")
    column_list = psycopg2_sql.SQL(", ").join(_ident(c) for c in columns)
    cur.execute(psycopg2_sql.SQL(
        "DROP TABLE IF EXISTS {stg}; "
        "CREATE TEMP TABLE {stg} ON COMMIT DROP AS "
        "SELECT 0::integer AS _row_no, {cols} FROM {target} WITH NO DATA"
    ).format(stg=staging, cols=column_list,
             target=_ident(WAREHOUSE_SCHEMA, table)))
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    for row_no, row in valid:
        writer.writerow([row_no, *(row.get(c) or "" for c in columns)])
    buffer.seek(0)
    cur.copy_expert(psycopg2_sql.SQL(
        "COPY {stg} (_row_no, {cols}) FROM STDIN WITH (FORMAT csv, NULL '')"
    ).format(stg=staging, cols=column_list).as_string(cur), buffer)

    # --- 3. Constraint checks that need SQL -----------------------------------
    sql_rejects: dict[int, str] = {}

    def collect(query, reason: str) -> None:
        cur.execute(query)
        for (row_no,) in cur.fetchall():
            sql_rejects.setdefault(row_no, reason)

    for name, expression in _check_expressions(cur, table):
        collect(psycopg2_sql.SQL("SELECT _row_no FROM {stg} WHERE ({expr}) IS FALSE").format(
            stg=staging, expr=psycopg2_sql.SQL(expression)),
            f"CHECK: {name} failed")
    for column, parent, parent_column in FOREIGN_KEYS[table]:
        collect(psycopg2_sql.SQL(
            "SELECT s._row_no FROM {stg} s WHERE s.{col} IS NOT NULL AND NOT EXISTS "
            "(SELECT 1 FROM {parent} p WHERE p.{pcol} = s.{col})"
        ).format(stg=staging, col=_ident(column),
                 parent=_ident(WAREHOUSE_SCHEMA, parent), pcol=_ident(parent_column)),
            f"FK_NOT_FOUND: {column} has no row in {parent}")
    # The same record must keep the same surrogate key, and one surrogate key
    # must not belong to two records; either would break the upsert.
    collect(psycopg2_sql.SQL(
        "SELECT s._row_no FROM {stg} s JOIN {target} t "
        "ON t.{pk} = s.{pk} AND t.{mk} IS DISTINCT FROM s.{mk} "
        "UNION SELECT s._row_no FROM {stg} s JOIN {target} t "
        "ON t.{mk} = s.{mk} AND t.{pk} IS DISTINCT FROM s.{pk}"
    ).format(stg=staging, target=_ident(WAREHOUSE_SCHEMA, table),
             pk=_ident(primary_key), mk=_ident(match_key)),
        f"KEY_CONFLICT: {primary_key}/{match_key} pairing differs from the warehouse")

    for row_no, reason in sorted(sql_rejects.items()):
        reject(row_no, reason)
    if sql_rejects:
        cur.execute(psycopg2_sql.SQL("DELETE FROM {stg} WHERE _row_no = ANY(%s)").format(
            stg=staging), (list(sql_rejects),))

    # --- 4. Upsert (SCD Type 1: overwrite every column except the keys) ------
    update_columns = [c for c in columns if c not in (primary_key, match_key)]
    assignments = psycopg2_sql.SQL(", ").join(
        psycopg2_sql.SQL("{c} = EXCLUDED.{c}").format(c=_ident(c))
        for c in [*update_columns, *LOAD_COLUMNS])
    cur.execute(psycopg2_sql.SQL(
        "INSERT INTO {target} AS t ({cols}, load_batch_id, loaded_at) "
        "SELECT {cols}, %s, now() FROM {stg} ORDER BY _row_no "
        "ON CONFLICT ({mk}) DO UPDATE SET {assignments} "
        "RETURNING (xmax = 0)"
    ).format(target=_ident(WAREHOUSE_SCHEMA, table), cols=column_list,
             stg=staging, mk=_ident(match_key), assignments=assignments),
        (batch_id,))
    flags = [inserted for (inserted,) in cur.fetchall()]
    result.inserted = sum(1 for f in flags if f)
    result.updated = len(flags) - result.inserted
    result.rejected.sort(key=lambda r: r.row_no)
    result.ended_at = datetime.now(PHT)
    return result


def load_batch(conn, handover: Handover, tables: tuple[str, ...] = LOAD_ORDER,
               results: dict[str, TableResult] | None = None) -> dict[str, TableResult]:
    """Load the tables in dependency order inside the caller's transaction.

    Dimensions first, then fact_consumption -> fact_billing -> fact_payment, so
    every foreign key check sees the parent rows written earlier in the same
    transaction. Nothing is committed here: the caller commits only when the
    batch is accepted, and rolls back otherwise (docs/load.md Section 3).

    Pass `results` to keep the tables that finished if a later one raises.
    """
    results = {} if results is None else results
    with conn.cursor() as cur:
        for table in tables:
            results[table] = load_table(cur, handover.tables[table], handover.batch_id)
    return results


def print_results(results: dict[str, TableResult]) -> None:
    for r in results.values():
        log(f"  {r.table}: received={r.received} inserted={r.inserted} "
            f"updated={r.updated} rejected={len(r.rejected)} ({r.duration_sec}s)")
        for rej in r.rejected[:10]:
            log(f"    rejected row {rej.row_no} ({MATCH_KEYS[r.table]}={rej.key}): {rej.reason}")
        if len(r.rejected) > 10:
            log(f"    ... {len(r.rejected) - 10} more rejected row(s)")


# ---------------------------------------------------------------------------
# Post-load validation and reconciliation (docs/load.md Section 5)
# ---------------------------------------------------------------------------
# These run inside the load transaction, after all five tables are written and
# before the commit, so a critical failure can still roll everything back.
# They only read and report; they never change data.
#
# "Rows written by this run" = load_batch_id is this batch AND loaded_at =
# now(). now() is fixed for the whole transaction, so this excludes rows that
# an earlier, committed load of the same batch wrote.

REJECT_LIMIT_PCT = Decimal("1.0")       # Rejected records: at most 1% per table
AMOUNT_TOLERANCE = Decimal("0.01")
OPTIONAL_NULL_LIMIT_PCT = 90
LOAD_DURATION_LIMIT_SEC = 15 * 60
AMOUNT_COLUMNS = {
    "fact_consumption": "consumption",
    "fact_billing": "total_bill",
    "fact_payment": "total_paid",
}
OPTIONAL_COLUMNS = {
    "dim_consumer": ("contact_number", "middle_name"),
    "fact_payment": ("reference_number",),
}


def _pct(part: int, whole: int) -> Decimal:
    return Decimal(0) if whole <= 0 else Decimal(part) * 100 / Decimal(whole)


def _written_this_run():
    return psycopg2_sql.SQL("load_batch_id = %s AND loaded_at = now()")


def check_record_reconciliation(results: dict[str, TableResult]) -> Check:
    check = Check("Record reconciliation", "All target tables",
                  "received = loaded + rejected, where loaded = inserted + updated", CRITICAL)
    bad = [f"{t}: {r.received} != {r.inserted} + {r.updated} + {len(r.rejected)}"
           for t, r in results.items()
           if r.received != r.inserted + r.updated + len(r.rejected)]
    if bad:
        check.status, check.detail = "FAIL", "; ".join(bad)
    else:
        check.detail = ", ".join(f"{t} {r.received}={r.inserted}+{r.updated}+{len(r.rejected)}"
                                 for t, r in results.items())
    return check


def check_batch_row_count(cur, handover: Handover, results: dict[str, TableResult]) -> Check:
    check = Check("Batch row count", "Rows with this load_batch_id",
                  "Row count in target = received - rejected", CRITICAL)
    bad, found = [], []
    for table, r in results.items():
        cur.execute(psycopg2_sql.SQL("SELECT count(*) FROM {t} WHERE {cond}").format(
            t=_ident(WAREHOUSE_SCHEMA, table), cond=_written_this_run()), (handover.batch_id,))
        count = cur.fetchone()[0]
        expected = r.received - len(r.rejected)
        found.append(f"{table}={count}")
        if count != expected:
            bad.append(f"{table}: {count} in target, expected {expected}")
    check.status, check.detail = ("FAIL", "; ".join(bad)) if bad else ("PASS", ", ".join(found))
    return check


def check_key_uniqueness(cur) -> Check:
    check = Check("Key uniqueness", "Primary key and match key of each table",
                  "Duplicate count = 0", CRITICAL)
    bad = []
    for table in LOAD_ORDER:
        for key in (PRIMARY_KEYS[table], MATCH_KEYS[table]):
            cur.execute(psycopg2_sql.SQL(
                "SELECT count(*) FROM (SELECT {k} FROM {t} GROUP BY {k} HAVING count(*) > 1) d"
            ).format(k=_ident(key), t=_ident(WAREHOUSE_SCHEMA, table)))
            duplicates = cur.fetchone()[0]
            if duplicates:
                bad.append(f"{table}.{key}: {duplicates} duplicated value(s)")
    check.status, check.detail = ("FAIL", "; ".join(bad)) if bad else (
        "PASS", "0 duplicates in 5 tables")
    return check


def check_referential_integrity(cur) -> Check:
    check = Check("Referential integrity",
                  "dim_consumer.purok_key, fact_consumption.consumer_key, "
                  "fact_billing.consumption_key, fact_billing.consumer_key, "
                  "fact_payment.billing_key",
                  "100% match in the parent table", CRITICAL)
    bad = []
    for table in LOAD_ORDER:
        for column, parent, parent_column in FOREIGN_KEYS[table]:
            cur.execute(psycopg2_sql.SQL(
                "SELECT count(*) FROM {t} c WHERE c.{col} IS NOT NULL AND NOT EXISTS "
                "(SELECT 1 FROM {p} p WHERE p.{pcol} = c.{col})"
            ).format(t=_ident(WAREHOUSE_SCHEMA, table), col=_ident(column),
                     p=_ident(WAREHOUSE_SCHEMA, parent), pcol=_ident(parent_column)))
            orphans = cur.fetchone()[0]
            if orphans:
                bad.append(f"{table}.{column}: {orphans} orphan row(s)")
    check.status, check.detail = ("FAIL", "; ".join(bad)) if bad else (
        "PASS", "0 orphans across 5 foreign keys")
    return check


def check_required_columns(cur, handover: Handover) -> Check:
    check = Check("Required columns", "NOT NULL columns of each table",
                  "Null count = 0", CRITICAL)
    bad = []
    for table, item in handover.tables.items():
        for column in item.required_not_null:
            cur.execute(psycopg2_sql.SQL(
                "SELECT count(*) FROM {t} WHERE {cond} AND {c} IS NULL"
            ).format(t=_ident(WAREHOUSE_SCHEMA, table), cond=_written_this_run(),
                     c=_ident(column)), (handover.batch_id,))
            nulls = cur.fetchone()[0]
            if nulls:
                bad.append(f"{table}.{column}: {nulls} NULL(s)")
    check.status, check.detail = ("FAIL", "; ".join(bad)) if bad else (
        "PASS", "0 NULLs in required columns")
    return check


def check_amount_reconciliation(cur, handover: Handover,
                                results: dict[str, TableResult]) -> Check:
    check = Check("Amount reconciliation",
                  ", ".join(f"{t}.{c}" for t, c in AMOUNT_COLUMNS.items()),
                  f"Source sum = target sum for the batch (within {AMOUNT_TOLERANCE})",
                  CRITICAL)
    bad, found = [], []
    for table, column in AMOUNT_COLUMNS.items():
        rejected_rows = {rej.row_no for rej in results[table].rejected}
        source = sum((Decimal(row[column])
                      for n, row in enumerate(handover.tables[table].rows, start=1)
                      if n not in rejected_rows and row.get(column)), Decimal("0"))
        cur.execute(psycopg2_sql.SQL(
            "SELECT coalesce(sum({c}), 0) FROM {t} WHERE {cond}"
        ).format(c=_ident(column), t=_ident(WAREHOUSE_SCHEMA, table),
                 cond=_written_this_run()), (handover.batch_id,))
        target = cur.fetchone()[0]
        found.append(f"{column} {target}")
        if abs(source - target) > AMOUNT_TOLERANCE:
            bad.append(f"{table}.{column}: source {source} != target {target}")
    check.status, check.detail = ("FAIL", "; ".join(bad)) if bad else (
        "PASS", "source = target: " + ", ".join(found))
    return check


def check_rejected_records(results: dict[str, TableResult]) -> list[Check]:
    limit = Check("Rejected records", "All target tables",
                  f"Rejected records are {REJECT_LIMIT_PCT}% or less of each table", CRITICAL)
    any_rejected = Check("Rejected records warning", "All target tables",
                         "Rejected count = 0", NON_CRITICAL)
    over = [f"{t}: {len(r.rejected)}/{r.received} ({_pct(len(r.rejected), r.received):.2f}%)"
            for t, r in results.items()
            if _pct(len(r.rejected), r.received) > REJECT_LIMIT_PCT]
    total = sum(len(r.rejected) for r in results.values())
    summary = ", ".join(f"{t}={len(r.rejected)}" for t, r in results.items() if r.rejected)
    if over:
        limit.status, limit.detail = "FAIL", "; ".join(over)
    else:
        limit.detail = f"{total} rejected, within {REJECT_LIMIT_PCT}% per table"
    if total:
        any_rejected.status, any_rejected.detail = "WARNING", f"{total} rejected: {summary}"
    else:
        any_rejected.detail = "0 rejected"
    return [limit, any_rejected]


def check_optional_nulls(cur, handover: Handover) -> Check:
    targets = [f"{t}.{c}" for t, cols in OPTIONAL_COLUMNS.items() for c in cols]
    check = Check("Null check", ", ".join(targets),
                  f"Null rate is {OPTIONAL_NULL_LIMIT_PCT}% or less", NON_CRITICAL)
    rates, high = [], []
    for table, columns in OPTIONAL_COLUMNS.items():
        for column in columns:
            cur.execute(psycopg2_sql.SQL(
                "SELECT count(*), count(*) FILTER (WHERE {c} IS NULL) FROM {t} WHERE {cond}"
            ).format(c=_ident(column), t=_ident(WAREHOUSE_SCHEMA, table),
                     cond=_written_this_run()), (handover.batch_id,))
            total, nulls = cur.fetchone()
            if not total:
                continue
            rate = _pct(nulls, total)
            rates.append(f"{table}.{column} {rate:.0f}%")
            if rate > OPTIONAL_NULL_LIMIT_PCT:
                high.append(f"{table}.{column} {rate:.0f}% ({nulls}/{total})")
    if high:
        check.status, check.detail = "WARNING", "; ".join(high)
    else:
        check.detail = ", ".join(rates) or "no rows to check"
    return check


def check_load_duration(started_at: datetime) -> Check:
    check = Check("Load duration", "Whole load run",
                  f"Load finishes within {LOAD_DURATION_LIMIT_SEC // 60} minutes", NON_CRITICAL)
    seconds = round((datetime.now(PHT) - started_at).total_seconds())
    check.detail = f"{seconds}s"
    if seconds > LOAD_DURATION_LIMIT_SEC:
        check.status = "WARNING"
    return check


def check_load_errors(load_error: str | None) -> Check:
    check = Check("Load errors", "Load process",
                  "No database error, constraint violation, timeout, or interruption",
                  CRITICAL)
    if load_error:
        check.status, check.detail = "FAIL", load_error
    else:
        check.detail = "no errors"
    return check


def run_post_load_checks(conn, handover: Handover, results: dict[str, TableResult] | None,
                         started_at: datetime, load_error: str | None = None) -> list[Check]:
    """Run every Section 5 check inside the open load transaction.

    When the load itself failed, the transaction is already aborted and only
    the "Load errors" check is reported.
    """
    checks = [check_load_errors(load_error)]
    if load_error or results is None:
        return checks
    checks.append(check_record_reconciliation(results))
    checks.extend(check_rejected_records(results))
    try:
        with conn.cursor() as cur:
            checks.append(check_batch_row_count(cur, handover, results))
            checks.append(check_key_uniqueness(cur))
            checks.append(check_referential_integrity(cur))
            checks.append(check_required_columns(cur, handover))
            checks.append(check_amount_reconciliation(cur, handover, results))
            checks.append(check_optional_nulls(cur, handover))
    except Exception as exc:  # noqa: BLE001 - a query error is a failed check
        checks.append(Check("Post-load query", "Warehouse database",
                            "Post-load queries run without errors", CRITICAL, "FAIL",
                            f"{type(exc).__name__}: {str(exc).strip()}"))
    checks.append(check_load_duration(started_at))
    return checks


# ---------------------------------------------------------------------------
# Load log, rejected records, and the acceptance decision
# (docs/load.md Sections 6, 7 and 8)
# ---------------------------------------------------------------------------
# Status of each load_log row:
#   SUCCESS - the batch was committed.
#   HELD    - MANUAL REVIEW: rolled back, waiting for a reviewer to rerun it
#             with --accept-warnings.
#   FAILED  - REJECTED: a critical check failed or the load raised an error;
#             rolled back.
# A successful run writes its load_log rows inside the batch transaction, so the
# data and its log are committed together. A held or failed run writes them on
# a separate connection after the rollback, so it is still recorded.

LOAD_LOG_COLUMNS = (
    "run_id", "transformation_run_id", "batch_id", "batch_window", "table_name",
    "load_type", "load_start_ts", "load_end_ts", "load_duration_sec",
    "records_received", "records_inserted", "records_updated", "records_rejected",
    "status", "pre_load_result", "post_load_result", "error_code", "error_message",
    "reviewed_by", "load_code_version",
)
STATUS_FOR_DECISION = {"APPROVED": "SUCCESS", "MANUAL REVIEW": "HELD", "REJECTED": "FAILED"}
QUARANTINE_COLUMNS = ("run_id", "batch_id", "table_name", "row_no", "match_key",
                      "reject_reason", "source_values")
PII_COLUMNS = ("email", "contact_number")


def mask_pii(column: str, value: str | None) -> str | None:
    """j***@mail.com / 09***1234: enough to recognise, not enough to use."""
    if not value:
        return value
    if column == "email" and "@" in value:
        local, _, domain = value.partition("@")
        return f"{local[:1]}***@{domain}"
    if column == "contact_number":
        return f"{value[:2]}***{value[-4:]}" if len(value) > 6 else "***"
    return value


def _code(name: str) -> str:
    return name.upper().replace(" ", "_").replace("-", "_")


def error_code_for(checks: list[Check],
                   post_checks: list[Check]) -> tuple[str | None, str | None]:
    """First failing check (critical first) as error_code, its detail as error_message."""
    for phase, group in (("PRE_LOAD", checks), ("POST_LOAD", post_checks)):
        for c in group:
            if c.status in ("FAIL", "NOT RUN") and c.severity == CRITICAL:
                if c.name == "Load errors":
                    return "LOAD_ERROR", c.detail
                return f"{phase}_{_code(c.name)}", c.detail
    for phase, group in (("PRE_LOAD", checks), ("POST_LOAD", post_checks)):
        for c in group:
            if c.status in ("WARNING", "NOT RUN"):
                return f"WARNING_{_code(c.name)}", c.detail
    return None, None


def build_log_records(run_id: str, handover: Handover, results: dict[str, TableResult],
                      checks: list[Check], post_checks: list[Check], decision: str,
                      status: str, started_at: datetime,
                      reviewed_by: str | None) -> list[dict[str, Any]]:
    """One load_log record per target table (docs/load.md Section 6).

    For HELD and FAILED runs the inserted/updated counts are what the
    rolled-back attempt wrote before it was undone; nothing of it remains.
    """
    error_code, error_message = (None, None) if status == "SUCCESS" else \
        error_code_for(checks, post_checks)
    ended_at = datetime.now(PHT)
    records = []
    for table in LOAD_ORDER:
        r = results.get(table)
        start = r.started_at if r else started_at
        end = (r.ended_at or ended_at) if r else ended_at
        records.append({
            "run_id": run_id,
            "transformation_run_id": handover.transformation_run_id,
            "batch_id": handover.batch_id,
            "batch_window": handover.batch_window,
            "table_name": table,
            "load_type": "INCREMENTAL",
            "load_start_ts": start.isoformat(),
            "load_end_ts": end.isoformat(),
            "load_duration_sec": max(0, round((end - start).total_seconds())),
            "records_received": handover.tables[table].row_count,
            "records_inserted": r.inserted if r else 0,
            "records_updated": r.updated if r else 0,
            "records_rejected": len(r.rejected) if r else 0,
            "status": status,
            "pre_load_result": summarize_checks(checks),
            "post_load_result": summarize_checks(post_checks) if post_checks else "NOT RUN",
            "error_code": error_code,
            "error_message": error_message,
            "reviewed_by": reviewed_by,
            "load_code_version": LOAD_CODE_VERSION,
            # JSON copy only (not columns in load_log): the full check details.
            "decision": decision,
            "pre_load_checks": [c.as_dict() for c in checks],
            "post_load_checks": [c.as_dict() for c in post_checks],
        })
    return records


def insert_load_log(cur, records: list[dict[str, Any]]) -> None:
    columns = psycopg2_sql.SQL(", ").join(_ident(c) for c in LOAD_LOG_COLUMNS)
    placeholders = psycopg2_sql.SQL(", ").join(
        psycopg2_sql.Placeholder() for _ in LOAD_LOG_COLUMNS)
    statement = psycopg2_sql.SQL("INSERT INTO {t} ({cols}) VALUES ({vals})").format(
        t=_ident(WAREHOUSE_SCHEMA, "load_log"), cols=columns, vals=placeholders)
    for record in records:
        cur.execute(statement, [record[c] for c in LOAD_LOG_COLUMNS])


def write_load_log_separately(dsn: str | None, records: list[dict[str, Any]]) -> bool:
    """Record a held or failed run on its own connection, after the rollback."""
    if not dsn:
        return False
    try:
        conn = connect_warehouse(dsn)
    except Exception as exc:  # noqa: BLE001 - the JSON log still has the record
        log(f"load_log not written (warehouse unreachable): {mask(str(exc).strip(), dsn)}")
        return False
    try:
        with conn.cursor() as cur:
            insert_load_log(cur, records)
        conn.commit()
        return True
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        log(f"load_log not written: {mask(f'{type(exc).__name__}: {exc}'.strip(), dsn)}")
        return False
    finally:
        conn.close()


def write_json_logs(records: list[dict[str, Any]], run_date: str, log_root: Path) -> Path:
    """Operational log: logs/waterwise-load/<run_date>/<run_id>_<table>.json."""
    folder = log_root / run_date
    folder.mkdir(parents=True, exist_ok=True)
    for record in records:
        path = folder / f"{record['run_id']}_{record['table_name']}.json"
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(record, indent=2, default=str), encoding="utf-8")
        os.replace(tmp, path)
    return folder


def write_quarantine(run_id: str, handover: Handover, results: dict[str, TableResult],
                     quarantine_root: Path) -> tuple[Path | None, int]:
    """Append rejected rows to quarantine/load/<run_date>/rejected.csv (PII masked).

    Appending keeps the rejected rows of every run of this batch; the run_id
    column tells them apart.
    """
    rejected = [rej for r in results.values() for rej in r.rejected]
    if not rejected:
        return None, 0
    path = quarantine_root / handover.run_date / "rejected.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=QUARANTINE_COLUMNS)
        if new_file:
            writer.writeheader()
        for rej in rejected:
            values = {k: (mask_pii(k, v) if k in PII_COLUMNS else v)
                      for k, v in rej.values.items()}
            writer.writerow({
                "run_id": run_id, "batch_id": handover.batch_id, "table_name": rej.table,
                "row_no": rej.row_no, "match_key": rej.key, "reject_reason": rej.reason,
                "source_values": json.dumps(values, ensure_ascii=False),
            })
    return path, len(rejected)


EXIT_RETRY = 4


def is_transient(message: str | None) -> bool:
    """A connection problem that a later retry can fix (docs/load.md Section 8).

    A wrong password is not transient: retrying only makes the Supabase pooler
    block the IP for longer. A pooler block (ECIRCUITBREAKER) is transient: it
    clears by itself within minutes, and retries are 5 minutes apart.
    """
    if not message:
        return False
    return (message.startswith(("OperationalError", "InterfaceError"))
            and "authentication failed" not in message)


def table_result_label(r: TableResult | None, load_error: str | None) -> str:
    if r is None or load_error:
        return "FAIL"
    if _pct(len(r.rejected), r.received) > REJECT_LIMIT_PCT:
        return "FAIL"
    return "WARNING" if r.rejected else "PASS"


def print_summary(results: dict[str, TableResult], load_error: str | None,
                  decision: str, status: str, reviewed_by: str | None) -> None:
    """Completion report (docs/load.md Section 8, Completion and Handover)."""
    log("Validation Summary:")
    for table in LOAD_ORDER:
        r = results.get(table)
        counts = (f"received={r.received} inserted={r.inserted} updated={r.updated} "
                  f"rejected={len(r.rejected)}") if r else "not loaded"
        log(f"  {table + ' load':<24} {table_result_label(r, load_error):<8} {counts}")
    shown = f"{decision} (reviewed by {reviewed_by})" if reviewed_by else decision
    log(f"  {'Overall Load':<24} {shown}  -> load_log status {status}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WaterWise Stage 3 load")
    parser.add_argument("--run-date", default=None,
                        help="transformation run date to load, YYYY-MM-DD (default: newest)")
    parser.add_argument("--run-id", default=None, help="load run_id (default: auto ww-load-...)")
    parser.add_argument("--force-reload", action="store_true",
                        help="load a batch that was already loaded with SUCCESS")
    parser.add_argument("--handover-released-by", default=None, metavar="NAME",
                        help="reviewer who released a transformation batch on MANUAL REVIEW")
    parser.add_argument("--accept-warnings", default=None, metavar="NAME",
                        help="reviewer who accepts non-critical warnings; publishes a "
                             "MANUAL REVIEW batch (never a critical failure)")
    parser.add_argument("--transformed-root", default=None,
                        help="handover root (default transformed/)")
    parser.add_argument("--transformation-log-root", default=None,
                        help="transformation log root (default logs/waterwise-transformation/)")
    parser.add_argument("--log-root", default=None,
                        help="load JSON log root (default logs/waterwise-load/)")
    parser.add_argument("--quarantine-root", default=None,
                        help="rejected-records root (default quarantine/load/)")
    parser.add_argument("--dry-run", action="store_true",
                        help="run every check and the full load, then roll back; writes "
                             "nothing (no data, no load_log, no JSON log, no quarantine)")
    return parser.parse_args(argv)


def new_run_id() -> str:
    now = datetime.now(PHT)
    suffix = hashlib.sha256(now.isoformat().encode()).hexdigest()[:6]
    return f"ww-load-{now.strftime('%Y-%m-%d-%H%M')}-{suffix}"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_id = args.run_id or new_run_id()
    log_root = Path(args.log_root) if args.log_root else LOAD_LOG_ROOT
    quarantine_root = Path(args.quarantine_root) if args.quarantine_root else QUARANTINE_ROOT
    if args.accept_warnings is not None and not args.accept_warnings.strip():
        print("--accept-warnings needs the reviewer's name", file=sys.stderr)
        return 2

    try:
        handover = read_handover(
            args.run_date,
            Path(args.transformed_root) if args.transformed_root else None,
            Path(args.transformation_log_root) if args.transformation_log_root else None,
        )
    except HandoverError as exc:
        print(f"No handover to load: {exc}", file=sys.stderr)
        return 2

    started_at = datetime.now(PHT)
    log(f"Run {run_id} | load_code_version={LOAD_CODE_VERSION}")
    describe_handover(handover)

    dsn = get_warehouse_dsn()
    checks, conn = run_pre_load_checks(
        handover, dsn,
        force_reload=args.force_reload,
        released_by=args.handover_released_by,
    )
    print_checks("Pre-load validation checks (docs/load.md Section 4):", checks)
    log(f"pre_load_result = {summarize_checks(checks)}")
    decision = decide(checks)

    results: dict[str, TableResult] = {}
    post_checks: list[Check] = []
    load_error: str | None = None
    reviewed_by = "; ".join(n for n in (args.handover_released_by, args.accept_warnings)
                            if n) or None
    committed = False

    # --- Load: the whole batch is one transaction (docs/load.md Section 3) ---
    # `committed` stays False on every path except the final commit, so the
    # `finally` block rolls back on any error, including Ctrl+C. If the process
    # dies outright, PostgreSQL discards the open transaction by itself.
    if decision != "REJECTED":
        try:
            log(f"Loading {handover.batch_id} in one transaction: " + " -> ".join(LOAD_ORDER))
            try:
                load_batch(conn, handover, results=results)
                print_results(results)
            except Exception as exc:  # noqa: BLE001 - any load error rejects the batch
                load_error = mask(f"{type(exc).__name__}: {str(exc).strip()}", dsn)

            post_checks = run_post_load_checks(
                conn, handover, None if load_error else results, started_at, load_error)
            print_checks("Post-load validation checks (docs/load.md Section 5):", post_checks)
            log(f"post_load_result = {summarize_checks(post_checks)}")
            decision = decide(checks + post_checks)

            # Section 8: a reviewer may accept warnings, never a critical failure.
            if decision == "MANUAL REVIEW" and args.accept_warnings:
                log(f"Warnings accepted by {args.accept_warnings}.")
                decision = "APPROVED"
            if decision == "APPROVED" and args.dry_run:
                log("DRY-RUN: the batch would be committed; rolling back instead.")
            elif decision == "APPROVED":
                records = build_log_records(run_id, handover, results, checks, post_checks,
                                            decision, "SUCCESS", started_at, reviewed_by)
                with conn.cursor() as cur:
                    insert_load_log(cur, records)   # committed together with the data
                conn.commit()
                committed = True
        except Exception as exc:  # noqa: BLE001 - e.g. the commit itself failed
            load_error = mask(f"{type(exc).__name__}: {str(exc).strip()}", dsn)
            post_checks.append(check_load_errors(load_error))
            decision = "REJECTED"
        finally:
            if not committed:
                conn.rollback()
            conn.close()
    elif conn is not None:
        conn.close()

    if args.dry_run:
        # Nothing was committed; also write no load_log, JSON log or quarantine.
        print_summary(results, load_error, decision, "not written (dry run)", reviewed_by)
        log(f"DRY-RUN finished: decision {decision}. Nothing was written to the warehouse, "
            "load_log, the JSON log, or quarantine.")
        return {"APPROVED": 0, "MANUAL REVIEW": 3}.get(decision, 1)

    status = "SUCCESS" if committed else STATUS_FOR_DECISION[decision]
    if not committed:
        records = build_log_records(run_id, handover, results, checks, post_checks,
                                    decision, status, started_at, reviewed_by)
        if not write_load_log_separately(dsn, records):
            log("load_log: written to the JSON log only.")

    json_folder = write_json_logs(records, handover.run_date, log_root)
    quarantine_path, quarantined = write_quarantine(run_id, handover, results, quarantine_root)
    log(f"Load log: {WAREHOUSE_SCHEMA}.load_log and {json_folder}")
    if quarantine_path:
        log(f"Rejected records: {quarantined} row(s) -> {quarantine_path}")

    print_summary(results, load_error, decision, status, reviewed_by)
    if committed:
        log(f"APPROVED: {handover.batch_id} committed and published "
            f"(transformation run {handover.transformation_run_id}). "
            f"{WAREHOUSE_SCHEMA}.latest_successful_load now shows this batch.")
        return 0
    if decision == "MANUAL REVIEW":
        log("MANUAL REVIEW: rolled back, nothing was published. Responsible: data engineer / "
            "Super Admin. Review the checks above and the rejected-records file, then rerun "
            'with --accept-warnings "<reviewer>" to publish.')
        return 3
    connection = next((c for c in checks if c.name == "Target connection"), None)
    if is_transient(load_error) or (connection and connection.status == "FAIL"
                                    and is_transient(connection.detail)):
        log("REJECTED: the warehouse could not be reached. Nothing was published; "
            "safe to retry (docs/load.md Section 8: up to 3 retries, 5 minutes apart).")
        return EXIT_RETRY
    log("REJECTED: rolled back, nothing was published. Fix the cause, then rerun the "
        "same batch.")
    return 1


if __name__ == "__main__":
    sys.exit(main())

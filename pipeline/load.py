"""WaterWise Stage 3: Data Load.

Implements docs/load.md:
- Input: Stage 2 handover ONLY (transformed/<table>/<run_date>/*.csv, the
  .schema.json sidecars, and the transformation run logs). This stage never
  connects to the main WaterWise database, never re-extracts and never
  re-transforms.
- Target: the WaterWise warehouse (second Supabase project, schema
  `warehouse`, created by db/warehouse_schema.sql).

Implemented so far:
- Reading and describing the transformation handover (Section 1, Load Input).
- Pre-load validation checks (Section 4). They only read; nothing is written.

Connection: WAREHOUSE_DB_URL (session pooler, role etl_loader), read from
pipeline/.env or the repo-root .env. Never committed, never logged.

Usage:
    # Check the newest handover against the warehouse
    python -m pipeline.load

    # Check the handover of a specific transformation run date
    python -m pipeline.load --run-date 2026-03-10

    # A batch that was already loaded with SUCCESS (intentional reload)
    python -m pipeline.load --run-date 2026-03-10 --force-reload

    # A transformation batch on MANUAL REVIEW, released by a named reviewer
    python -m pipeline.load --handover-released-by "Juan Dela Cruz"

Exit codes:
    0 = all pre-load checks PASS
    3 = no critical failure, but one or more non-critical WARNING
    1 = one or more critical FAIL (the load must not start)
    2 = usage / input error (no handover to read)
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
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
except ImportError:  # pragma: no cover - surfaced by the connection check
    psycopg2 = None

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent  # repo root (Waterwise_ETL_Pipeline/)
TRANSFORMED_ROOT = BASE_DIR / "transformed"
TRANSFORMATION_LOG_ROOT = BASE_DIR / "logs" / "waterwise-transformation"

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


def _fail_status(severity: str) -> str:
    return "FAIL" if severity == CRITICAL else "WARNING"


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


def check_write_permission(cur) -> Check:
    check = Check("Write permission", f"{WAREHOUSE_SCHEMA} schema, load role",
                  "Role can INSERT and UPDATE all five tables, INSERT into load_log, "
                  "and create temporary tables", CRITICAL)
    wanted = [(t, p) for t in LOAD_ORDER for p in ("INSERT", "UPDATE")]
    wanted += [("load_log", "INSERT"), ("load_log", "SELECT")]
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
    parser.add_argument("--transformed-root", default=None,
                        help="handover root (default transformed/)")
    parser.add_argument("--transformation-log-root", default=None,
                        help="transformation log root (default logs/waterwise-transformation/)")
    return parser.parse_args(argv)


def new_run_id() -> str:
    now = datetime.now(PHT)
    suffix = hashlib.sha256(now.isoformat().encode()).hexdigest()[:6]
    return f"ww-load-{now.strftime('%Y-%m-%d-%H%M')}-{suffix}"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_id = args.run_id or new_run_id()

    try:
        handover = read_handover(
            args.run_date,
            Path(args.transformed_root) if args.transformed_root else None,
            Path(args.transformation_log_root) if args.transformation_log_root else None,
        )
    except HandoverError as exc:
        print(f"No handover to load: {exc}", file=sys.stderr)
        return 2

    log(f"Run {run_id} | load_code_version={LOAD_CODE_VERSION}")
    describe_handover(handover)

    checks, conn = run_pre_load_checks(
        handover, get_warehouse_dsn(),
        force_reload=args.force_reload,
        released_by=args.handover_released_by,
    )
    if conn is not None:
        conn.close()
    print_checks("Pre-load validation checks (docs/load.md Section 4):", checks)

    decision = decide(checks)
    log(f"pre_load_result = {summarize_checks(checks)}")
    if decision == "REJECTED":
        log("REJECTED: a critical pre-load check failed. Nothing was written to the warehouse.")
        return 1
    if decision == "MANUAL REVIEW":
        log("Pre-load WARNING: no critical failure, but a non-critical check needs review.")
        return 3
    log(f"Pre-load PASS: {handover.batch_id} is ready to load ({handover.total_rows} rows).")
    return 0


if __name__ == "__main__":
    sys.exit(main())

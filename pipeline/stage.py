"""WaterWise Stage 1: Data Extraction.

Implements docs/extraction.md:
- Source: Supabase/PostgreSQL (direct DB, no CSV source files)
- Tool: Python (psycopg2) + orchestrated by Airflow DAG `waterwise_daily_extraction`
- Mode: scheduled batch, incremental via `last_successful_run`; first run = full load
- Targets: consumers, consumption, billing, payments -> rawdata/<table>/YYYY-MM-DD/
- Validation: extraction-level checks only (no cleaning/transform)
- Logging: per-table JSON log per Section 4

Usage:
    # Full load
    python -m pipeline.stage --full --run-id ww-extract-local-001
    # Incremental (explicit window, ISO timestamps)
    python -m pipeline.stage --window-start 2026-03-09T01:00:00+08:00 --window-end 2026-03-10T01:00:00+08:00
    # Dry run (no DB needed, validates contract + synthetic validation logic)
    python -m pipeline.stage --dry-run

Connection (service-role, server-side only, never frontend):
    Set one of (first match wins):
      WATERWISE_DB_URL / SUPABASE_DB_URL / POSTGRES_DSN / DATABASE_URL
    e.g. postgresql://postgres:<password>@db.<ref>.supabase.co:5432/postgres
    Passwords with ? & @ / # must be URL-encoded (?->%3F @->%40).
    Loads pipeline/.env then repo-root .env via python-dotenv.
    Optional: PGSCHEMA (default: public), PGCONNECT_TIMEOUT (default: 10s)

State:
    Last successful run timestamp stored in:
      rawdata/_state/last_successful_run.json
    Override with --window-start/--window-end or --full.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
    _HERE = Path(__file__).resolve().parent
    # Explicit load order: pipeline/.env first, then repo-root .env (neither committed).
    load_dotenv(_HERE / ".env")
    load_dotenv(_HERE.parent / ".env")
    load_dotenv()
except Exception:
    pass

try:
    import psycopg2
    import psycopg2.extras
except ImportError:  # pragma: no cover - surfaced clearly at runtime
    psycopg2 = None

# ---------------------------------------------------------------------------
# Contract (docs/extraction.md Section 2 + 6)
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent  # repo root (Waterwise_ETL_Pipeline/)
STAGING_ROOT = BASE_DIR / "rawdata"
LOG_ROOT = BASE_DIR / "logs" / "waterwise-extraction"
STATE_FILE = STAGING_ROOT / "_state" / "last_successful_run.json"

TABLE_CONTRACT: dict[str, dict[str, Any]] = {
    "consumers": {
        "columns": [
            "id", "username", "first_name", "middle_name", "last_name",
            "email", "contact_number", "purok_no", "status",
            "created_at", "updated_at",
        ],
        "pk": "id",
        "required_ids": ["id"],
    },
    "consumption": {
        "columns": [
            "id", "consumer_id", "reading_date",
            "previous_reading", "present_reading", "consumption",
            "idempotency_key", "created_at", "updated_at",
        ],
        "pk": "id",
        "required_ids": ["id", "consumer_id", "reading_date"],
    },
    "billing": {
        "columns": [
            "id", "consumption_id", "user_id", "billing_date", "due_date",
            "total_bill", "remaining_balance", "status",
            "created_at", "updated_at",
        ],
        "pk": "id",
        "required_ids": ["id", "consumption_id", "user_id"],
    },
    "payments": {
        "columns": [
            "id", "billing_id", "total_paid", "amount_tendered", "change_given",
            "remaining_balance", "payment_date", "payment_method",
            "reference_number", "idempotency_key", "created_at", "updated_at",
        ],
        "pk": "id",
        "required_ids": ["id", "billing_id"],
    },
}

# FK expectations for referential-completeness check (child.col -> parent.table.parent_col)
FK_CHECKS = [
    ("consumption", "consumer_id", "consumers", "id"),
    ("billing", "consumption_id", "consumption", "id"),
    ("billing", "user_id", "consumers", "id"),
    ("payments", "billing_id", "billing", "id"),
]

MAX_RETRIES = 3
RETRY_BACKOFF_S = (2, 4, 8)  # exponential-ish backoff per Section 1.4


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).astimezone().isoformat()
    print(f"[{ts}] {msg}", flush=True)


def get_dsn() -> str | None:
    for key in ("WATERWISE_DB_URL", "SUPABASE_DB_URL", "POSTGRES_DSN", "DATABASE_URL"):
        val = os.environ.get(key)
        if val:
            return val.strip().strip('"').strip("'")
    return None


def validate_dsn(dsn: str) -> tuple[bool, str]:
    """Pre-flight DSN check with masked errors (never logs the password).

    Catches the common failure: unencoded ? & @ / # in the password, which
    breaks URL parsing (e.g. "Port could not be cast...") before psycopg2 runs.
    """
    if not dsn.startswith("postgresql://"):
        return False, "DSN must start with postgresql://"
    try:
        from urllib.parse import urlparse
        u = urlparse(dsn)
        if not u.hostname or not u.username:
            return False, (
                "DSN missing host or user (format: postgresql://user:<pw>@db.<ref>.supabase.co:5432/postgres). "
                "If the password contains ? & @ / #, URL-encode them (?->%3F @->%40)."
            )
        _ = u.port  # raises ValueError on malformed password/host split
        if u.password is None:
            return False, "DSN missing password"
        return True, "OK"
    except ValueError as exc:
        return False, (
            f"Malformed DSN ({exc}). The password likely contains unencoded "
            "special chars (? & @ / #). URL-encode them, e.g. ?->%3F @->%40, "
            "or reset the DB password to alphanumeric in Supabase Settings > Database."
        )


def connect(dsn: str, timeout: int = 10):
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is not installed. Run: pip install -r pipeline/requirements.txt")
    schema = os.environ.get("PGSCHEMA", "public")
    conn = psycopg2.connect(dsn, connect_timeout=timeout)
    with conn.cursor() as cur:
        cur.execute(f"SET search_path TO {schema}, public")
    return conn


def with_retry(fn, *, label: str):
    """Retry transient failures up to MAX_RETRIES with backoff. Section 8.1."""
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - retry then re-raise
            last_exc = exc
            log(f"{label}: attempt {attempt}/{MAX_RETRIES} failed: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_S[min(attempt - 1, len(RETRY_BACKOFF_S) - 1)])
    raise last_exc  # type: ignore[misc]


def build_window_clause(table: str, full: bool) -> tuple[str, dict[str, Any], str]:
    """Return (where_sql, params, description) per Section 1.3.

    Incremental window uses stored last_successful_run -> current_run.
    The caller supplies window_start/window_end datetimes; this builds the filter.
    """
    if full:
        return "", {}, "full load (no window filter)"
    if table == "consumers":
        return (
            "WHERE updated_at >= %(start)s AND updated_at < %(end)s",
            {},
            "updated_at in [start, end)",
        )
    if table == "consumption":
        return (
            "WHERE (updated_at >= %(start)s AND updated_at < %(end)s) "
            "OR (reading_date >= %(start)s::date AND reading_date < %(end)s::date)",
            {},
            "updated_at in window OR reading_date in window",
        )
    if table == "billing":
        return (
            "WHERE (billing_date >= %(start)s::date AND billing_date < %(end)s::date) "
            "OR (updated_at >= %(start)s AND updated_at < %(end)s)",
            {},
            "billing_date in window OR updated_at in window",
        )
    if table == "payments":
        return (
            "WHERE (payment_date >= %(start)s::date AND payment_date < %(end)s::date) "
            "OR (created_at >= %(start)s AND created_at < %(end)s) "
            "OR (updated_at >= %(start)s AND updated_at < %(end)s)",
            {},
            "payment_date in window OR created_at/updated_at in window",
        )
    raise ValueError(f"Unknown table: {table}")


def extract_table(conn, table: str, window_start, window_end, full: bool) -> tuple[list[str], list[tuple]]:
    contract = TABLE_CONTRACT[table]
    cols = contract["columns"]
    col_sql = ", ".join(f'"{c}"' for c in cols)
    where_sql, _, _ = build_window_clause(table, full)
    params: dict[str, Any] = {} if full else {"start": window_start, "end": window_end}
    sql = f'SELECT {col_sql} FROM "{table}" {where_sql} ORDER BY "{contract["pk"]}"'

    def _run():
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            src_count = len(rows)
            # Independent source COUNT(*) for completeness check (Section 3)
            count_sql = f'SELECT COUNT(*) FROM "{table}" {where_sql}'
            cur.execute(count_sql, params)
            expected = cur.fetchone()[0]
            return rows, expected

    (rows, expected) = with_retry(_run, label=f"extract {table}")
    if expected != len(rows):
        log(f"WARNING {table}: fetched {len(rows)} but source COUNT(*)={expected}")
    return cols, rows


def check_schema(conn) -> tuple[bool, str]:
    """Required tables and columns exist (Section 3). Fail fast per Section 6.3."""
    schema = os.environ.get("PGSCHEMA", "public")
    try:
        with conn.cursor() as cur:
            for table, spec in TABLE_CONTRACT.items():
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema=%s AND table_name=%s",
                    (schema, table),
                )
                present = {r[0] for r in cur.fetchall()}
                missing = [c for c in spec["columns"] if c not in present]
                if table not in present and not present:
                    # table itself missing -> information_schema returns no rows
                    return False, f"FAIL: table {schema}.{table} missing"
                if missing:
                    return False, f"FAIL: {table} missing columns: {missing}"
        return True, "PASS"
    except Exception as exc:  # noqa: BLE001
        return False, f"FAIL: schema check error: {exc}"


def validate_rows(table: str, cols: list[str], rows: list[tuple], source_count: int) -> dict[str, Any]:
    """Extraction-level validation (Section 3). Returns summary dict.

    - identifiers present (NULL check) -> quarantine count
    - PK uniqueness -> fail table if dupes
    - completeness: extracted == source count
    - range sanity -> WARNING flags only, passed through for Transformation
    """
    idx = {c: i for i, c in enumerate(cols)}
    spec = TABLE_CONTRACT[table]
    rejected = 0
    notes: list[str] = []
    status = "PASS"

    # NULL identifiers
    null_rows = 0
    for r in rows:
        if any(r[idx[c]] is None for c in spec["required_ids"]):
            null_rows += 1
    if null_rows:
        rejected += null_rows
        status = "FAIL"
        notes.append(f"FAIL: {null_rows} rows with NULL identifiers (quarantine)")

    # PK uniqueness
    pk_vals = [r[idx[spec["pk"]]] for r in rows]
    if len(pk_vals) != len(set(pk_vals)):
        status = "FAIL"
        notes.append(f"FAIL: duplicate PKs in {table}")

    # Completeness
    if source_count != len(rows):
        diff = abs(source_count - len(rows))
        pct = (diff / source_count * 100) if source_count else 0
        if source_count > 0 and len(rows) == 0:
            status = "FAIL"
            notes.append(f"FAIL: 0 rows extracted but source has {source_count}")
        elif pct >= 1:
            status = "FAIL"
            notes.append(f"FAIL: count mismatch extracted={len(rows)} source={source_count}")
        elif diff > 0:
            if status == "PASS":
                status = "WARNING"
            notes.append(f"WARNING: count variance {diff} rows (<1%)")

    # Range sanity (extraction-level only, WARNING)
    range_flags = 0
    try:
        if table == "consumption" and "previous_reading" in idx:
            for r in rows:
                prev, pres, cons = r[idx["previous_reading"]], r[idx["present_reading"]], r[idx["consumption"]]
                if pres is not None and prev is not None and float(pres) < float(prev):
                    range_flags += 1
                elif cons is not None and float(cons) < 0:
                    range_flags += 1
        elif table == "billing" and "total_bill" in idx:
            for r in rows:
                if r[idx["total_bill"]] is not None and float(r[idx["total_bill"]]) < 0:
                    range_flags += 1
    except (TypeError, ValueError):
        pass
    if range_flags:
        if status == "PASS":
            status = "WARNING"
        notes.append(f"WARNING: {range_flags} range-sanity rows flagged for Transformation")

    return {
        "status": status,
        "rejected": rejected,
        "notes": notes or ["PASS"],
    }


def check_referential(all_data: dict[str, tuple[list[str], list[tuple]]]) -> list[str]:
    """Orphan FK check across extracted sets. Returns FAIL notes (empty if clean)."""
    notes: list[str] = []
    id_sets: dict[str, set] = {}
    for table, (cols, rows) in all_data.items():
        pk = TABLE_CONTRACT[table]["pk"]
        id_sets[table] = {r[cols.index(pk)] for r in rows} if rows else set()
    for child, col, parent, _pcol in FK_CHECKS:
        if child not in all_data or parent not in all_data:
            continue
        ccols, crows = all_data[child]
        ci = ccols.index(col)
        orphans = sum(1 for r in crows if r[ci] is not None and r[ci] not in id_sets[parent])
        if orphans:
            notes.append(f"FAIL: {orphans} orphan(s) {child}.{col} missing in {parent}")
    return notes


def write_staging(table: str, cols: list[str], rows: list[tuple], run_date: str) -> Path:
    out_dir = STAGING_ROOT / table / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{table}_{run_date}.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    # Minimal schema sidecar per Section 7.3 handover
    schema_path = out_dir / f"{table}_{run_date}.schema.json"
    schema_path.write_text(json.dumps({"table": table, "columns": cols}, indent=2), encoding="utf-8")
    return out_path


def write_run_log(record: dict[str, Any]) -> Path:
    day = record.get("extraction_window_end", datetime.now(timezone.utc).isoformat())[:10]
    log_dir = LOG_ROOT / day
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"{record['pipeline_run_id']}_{record['table_name']}.json"
    path.write_text(json.dumps(record, indent=2, default=str), encoding="utf-8")
    return path


def load_state() -> str | None:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8")).get("last_successful_run")
        except Exception:
            return None
    return None


def save_state(ts_iso: str) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps({"last_successful_run": ts_iso}, indent=2), encoding="utf-8")


def decide_overall(per_table_status: dict[str, str]) -> str:
    """Section 7.1: APPROVED / MANUAL REVIEW / REJECTED."""
    if any(s == "FAIL" for s in per_table_status.values()):
        return "REJECTED"
    if any(s == "WARNING" for s in per_table_status.values()):
        return "MANUAL REVIEW"
    return "APPROVED"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="WaterWise Stage 1 extraction")
    p.add_argument("--run-id", default=None, help="pipeline_run_id (default: auto ww-extract-...)")
    p.add_argument("--window-start", default=None, help="ISO timestamp, incremental start")
    p.add_argument("--window-end", default=None, help="ISO timestamp, incremental end (default: now)")
    p.add_argument("--full", action="store_true", help="full extraction, ignore window")
    p.add_argument("--dry-run", action="store_true", help="no DB: validate contract + synthetic checks")
    p.add_argument("--tables", default="consumers,consumption,billing,payments",
                   help="comma-separated subset to extract")
    return p.parse_args(argv)


def run_dry_run(run_id: str) -> int:
    log("DRY-RUN: no DB connection; exercising contract + validation logic.")
    for table, spec in TABLE_CONTRACT.items():
        log(f"  contract {table}: columns={spec['columns']} pk={spec['pk']}")
    # Synthetic validation smoke test
    cols = TABLE_CONTRACT["consumption"]["columns"]
    i = {c: n for n, c in enumerate(cols)}
    good = [tuple(1 if c in ("id", "consumer_id") else ("2026-03-10" if c == "reading_date" else 10) for c in cols)]
    bad = [tuple(None if c == "id" else (5 if c == "present_reading" else (10 if c == "previous_reading" else 1)) for c in cols)]
    r_good = validate_rows("consumption", cols, good, source_count=1)
    r_bad = validate_rows("consumption", cols, bad, source_count=1)
    log(f"  synthetic good -> {r_good['status']}; bad-null -> {r_bad['status']} {r_bad['notes']}")
    assert r_good["status"] == "PASS" and r_bad["status"] == "FAIL", "validation logic broken"
    log(f"DRY-RUN OK run_id={run_id}. Set WATERWISE_DB_URL to run a real extract.")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_id = args.run_id or f"ww-extract-{datetime.now(timezone.utc).strftime('%Y-%m-%d-%H%M')}-{uuid.uuid4().hex[:6]}"
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    unknown = [t for t in tables if t not in TABLE_CONTRACT]
    if unknown:
        print(f"Unknown tables: {unknown}. Valid: {list(TABLE_CONTRACT)}", file=sys.stderr)
        return 2

    if args.dry_run:
        return run_dry_run(run_id)

    # Resolve window
    now = datetime.now(timezone.utc)
    if args.full:
        window_start = window_end = None
        full = True
    else:
        start_raw = args.window_start or load_state()
        if not start_raw:
            log("No --window-start and no saved state: defaulting to FULL load (first run).")
            full = True
            window_start = window_end = None
        else:
            full = False
            window_start = datetime.fromisoformat(start_raw)
            window_end = datetime.fromisoformat(args.window_end) if args.window_end else now
            if window_start.tzinfo is None:
                window_start = window_start.replace(tzinfo=timezone.utc)
            if window_end.tzinfo is None:
                window_end = window_end.replace(tzinfo=timezone.utc)

    dsn = get_dsn()
    if not dsn:
        print(
            "Missing DB connection string.\n"
            "Set WATERWISE_DB_URL (or SUPABASE_DB_URL / POSTGRES_DSN / DATABASE_URL), e.g.:\n"
            "  postgresql://postgres:<password>@db.<ref>.supabase.co:5432/postgres\n"
            "Or run with --dry-run for a no-DB check.",
            file=sys.stderr,
        )
        return 2
    if psycopg2 is None:
        print("Missing dependency psycopg2. Run: pip install -r pipeline/requirements.txt", file=sys.stderr)
        return 2
    ok_dsn, dsn_msg = validate_dsn(dsn)
    if not ok_dsn:
        print(f"Bad DB connection string: {dsn_msg}", file=sys.stderr)
        return 2

    run_date = (window_end or now).astimezone().date().isoformat()
    log(f"Run {run_id}: tables={tables} full={full} window=[{window_start} -> {window_end}]")

    try:
        conn = with_retry(lambda: connect(dsn), label="source connection")
    except Exception as exc:  # noqa: BLE001 - Section 3: stop run, log error
        log(f"FAILED source connection after retries: {exc}")
        return 1
    log("Source connection OK.")

    ok, schema_msg = check_schema(conn)
    log(f"Schema check: {schema_msg}")
    if not ok:
        conn.close()
        print(schema_msg, file=sys.stderr)
        return 1

    all_data: dict[str, tuple[list[str], list[tuple]]] = {}
    source_counts: dict[str, int] = {}
    per_table_status: dict[str, str] = {}
    overall_rc = 0

    for table in tables:
        started = datetime.now(timezone.utc)
        t0 = time.perf_counter()
        record: dict[str, Any] = {
            "pipeline_run_id": run_id,
            "table_name": table,
            "extraction_started_at": started.isoformat(),
            "extraction_window_start": window_start.isoformat() if window_start else None,
            "extraction_window_end": (window_end or now).isoformat(),
        }
        try:
            cols, rows = extract_table(conn, table, window_start, window_end, full)
            # source count: rows fetched already cross-checked; recompute cheaply
            src_count = len(rows)
            # NOTE: extract_table already compared against COUNT(*); reuse len as read count
            v = validate_rows(table, cols, rows, source_count=src_count)
            all_data[table] = (cols, rows)
            source_counts[table] = src_count
            out_path = write_staging(table, cols, rows, run_date)
            record.update({
                "extraction_status": "SUCCESS" if v["status"] == "PASS" else ("WARNING" if v["status"] == "WARNING" else "FAILED"),
                "records_read": src_count,
                "records_extracted": len(rows) - v["rejected"],
                "records_rejected": v["rejected"],
                "validation_result": v["status"] + (": " + "; ".join(v["notes"]) if v["notes"] != ["PASS"] else ""),
                "error_message": None,
            })
            per_table_status[table] = v["status"]
            log(f"{table}: read={src_count} staged={len(rows) - v['rejected']} rejected={v['rejected']} "
                f"-> {out_path} [{v['status']}]")
            if v["status"] == "FAIL":
                overall_rc = 1
        except Exception as exc:  # noqa: BLE001 - Section 7: retry done inside, now quarantine/stop
            record.update({
                "extraction_status": "FAILED",
                "records_read": 0,
                "records_extracted": 0,
                "records_rejected": 0,
                "validation_result": f"FAIL: {exc}",
                "error_message": f"{type(exc).__name__}: {exc}",
            })
            per_table_status[table] = "FAIL"
            overall_rc = 1
            log(f"{table}: FAILED {exc}")
        finally:
            ended = datetime.now(timezone.utc)
            record["extraction_ended_at"] = ended.isoformat()
            record["extraction_duration_ms"] = int((time.perf_counter() - t0) * 1000)
            lp = write_run_log(record)
            log(f"{table}: log -> {lp}")

    conn.close()

    # Cross-table referential check (Section 3)
    if len(all_data) > 1:
        orphan_notes = check_referential(all_data)
        for n in orphan_notes:
            log(n)
        if orphan_notes:
            overall_rc = 1
            for t in all_data:
                if per_table_status.get(t) == "PASS":
                    per_table_status[t] = "FAIL"

    overall = decide_overall(per_table_status)
    log(f"Handover summary: {per_table_status} => Overall={overall}")
    log("Handover: staged rawdata + per-table JSON logs + validation results -> Transformation stage.")
    if overall == "APPROVED":
        save_state((window_end or now).isoformat())
        log(f"State saved: last_successful_run={(window_end or now).isoformat()}")
    elif overall == "MANUAL REVIEW":
        log("Held for manual review (data engineer / Super Admin) before promotion.")
        save_state((window_end or now).isoformat())
    else:
        log("REJECTED: quarantined. Fix cause, then re-run the same window (idempotent).")
    return overall_rc


if __name__ == "__main__":
    raise SystemExit(main())

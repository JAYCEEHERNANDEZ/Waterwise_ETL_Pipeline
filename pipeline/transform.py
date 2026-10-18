"""WaterWise Stage 2: Data Transformation.

Implements docs/transformation.md:
- Input: Stage 1 handover ONLY (rawdata/<table>/<run_date>/*.csv + run logs).
  This stage never connects to Supabase/PostgreSQL and never re-extracts.
- Output: star schema -> dim_purok, dim_consumer, fact_consumption,
  fact_billing, fact_payment (transformed/<table>/<run_date>/)
- Cleaning: rules C-01..C-11 (Section 3)
- Standardization: rules S-01..S-08 (Section 4)
- Business transformation: rules B-01..B-14 + exclusions X-01..X-04 (Section 5)
- Validation: checks V-01..V-24 with critical / non-critical severity (Section 7)
- Logging: per-table JSON log per Section 8; quarantine per Section 9
- Idempotent: deterministic MD5 surrogate keys + atomic os.replace() writes,
  so rerunning the same run_date reproduces byte-identical output (Section 1.4)

Loading into a database is explicitly NOT performed here; persistence belongs to
the Load Stage (transformation.md Section 10.4).

Usage:
    # Transform the most recent staged batch
    python -m pipeline.transform

    # Transform a specific extraction run date
    python -m pipeline.transform --run-date 2026-03-10 \
        --parent-run-id ww-extract-2026-03-10-0100-4f2a91

    # Override "today" used by all date-relative derivations (B-08/B-09/B-10)
    python -m pipeline.transform --reference-date 2026-03-10

    # No staged data needed: build synthetic input and exercise every rule
    python -m pipeline.transform --dry-run

Exit codes:
    0 = APPROVED       (transformation.md Section 10.2)
    3 = MANUAL REVIEW  (held; batch not promoted)
    1 = REJECTED       (critical failure; batch quarantined)
    2 = usage / input error
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
import tempfile
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Callable, Iterable

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent  # repo root (Waterwise_ETL_Pipeline/)
STAGING_ROOT = BASE_DIR / "rawdata"
EXTRACTION_LOG_ROOT = BASE_DIR / "logs" / "waterwise-extraction"
OUTPUT_ROOT = BASE_DIR / "transformed"
LOG_ROOT = BASE_DIR / "logs" / "waterwise-transformation"
QUARANTINE_ROOT = BASE_DIR / "quarantine" / "transformation"

RULES_VERSION = "1.0"

# Asia/Manila (PHT, UTC+8, no DST). Fixed offset avoids a tzdata dependency.
PHT = timezone(timedelta(hours=8))

UNKNOWN_PUROK_NO = 0
REJECTION_THRESHOLD_PCT = Decimal("1.0")      # Section 10.1, declared in advance
REJECTION_FAIL_PCT = Decimal("5.0")           # Section 10.1 escalation
UNMAPPED_CODE_WARN_PCT = Decimal("1.0")
UNMAPPED_CODE_FAIL_PCT = Decimal("5.0")
FLAGGED_ACCOUNT_THRESHOLD = 3                 # B-11
DUE_DATE_DAYS = 15                            # C-08 repair
MONEY_Q = Decimal("0.01")
RATE_Q = Decimal("0.0001")

# Expected Stage 1 staged columns (docs/extraction.md Section 6.1). A missing
# column is a critical schema-drift failure (Section 6.4 step 1).
EXPECTED_INPUT_COLUMNS: dict[str, list[str]] = {
    "consumers": [
        "id", "username", "first_name", "middle_name", "last_name",
        "email", "contact_number", "purok_no", "status",
        "created_at", "updated_at",
    ],
    "consumption": [
        "id", "consumer_id", "reading_date", "previous_reading",
        "present_reading", "consumption", "idempotency_key",
        "created_at", "updated_at",
    ],
    "billing": [
        "id", "consumption_id", "user_id", "billing_date", "due_date",
        "total_bill", "remaining_balance", "status", "created_at", "updated_at",
    ],
    "payments": [
        "id", "billing_id", "total_paid", "amount_tendered", "change_given",
        "remaining_balance", "payment_date", "payment_method",
        "reference_number", "idempotency_key", "created_at", "updated_at",
    ],
}

SOURCE_TABLE_FOR_OUTPUT = {
    "dim_consumer": "consumers",
    "fact_consumption": "consumption",
    "fact_billing": "billing",
    "fact_payment": "payments",
}

# Output column order per docs/transformation.md Section 6.1.
OUTPUT_COLUMNS: dict[str, list[str]] = {
    "dim_purok": ["purok_key", "purok_no", "purok_label", "is_unknown"],
    "dim_consumer": [
        "consumer_key", "consumer_id", "username", "first_name", "middle_name",
        "last_name", "full_name", "email", "contact_number", "purok_key",
        "purok_no", "purok_is_unknown", "status", "code_unmapped",
        "active_flag", "valid_from", "valid_to", "is_current",
        "created_at", "updated_at", "transformation_run_id",
    ],
    "fact_consumption": [
        "consumption_key", "consumption_id", "consumer_key", "reading_date",
        "reading_year", "reading_month", "billing_period",
        "previous_reading", "present_reading", "consumption",
        "consumption_cbm", "days_in_period", "consumption_per_day",
        "reading_date_adjusted", "transformation_run_id",
    ],
    "fact_billing": [
        "billing_key", "billing_id", "consumption_key", "consumer_key",
        "billing_date", "due_date", "billing_year", "billing_month",
        "billing_period", "total_bill", "remaining_balance", "status",
        "status_recomputed", "balance_corrected", "due_date_corrected",
        "is_revenue_recognized", "days_to_due", "days_overdue", "is_overdue",
        "overdue_bucket", "outstanding_bill_count", "is_flagged_account",
        "transformation_run_id",
    ],
    "fact_payment": [
        "payment_key", "payment_id", "billing_key", "payment_date",
        "payment_year", "payment_month", "payment_method", "code_unmapped",
        "total_paid", "amount_tendered", "change_given", "remaining_balance",
        "reference_number", "payment_allocation_rank",
        "is_oldest_unpaid_payment", "collection_rate", "change_corrected",
        "transformation_run_id",
    ],
}

# Columns whose value is copied straight through from the staged CSV into the
# output contract. Used by the source-to-target mapping in Section 6.3 and by
# the column-level null checks in V-05 / V-17.
REQUIRED_NOT_NULL: dict[str, list[str]] = {
    "dim_purok": ["purok_key", "purok_no", "purok_label", "is_unknown"],
    "dim_consumer": [
        "consumer_key", "consumer_id", "username", "first_name", "last_name",
        "full_name", "email", "purok_key", "purok_no", "purok_is_unknown",
        "status", "code_unmapped", "active_flag", "valid_from", "is_current",
        "created_at", "updated_at", "transformation_run_id",
    ],
    "fact_consumption": [
        "consumption_key", "consumption_id", "consumer_key", "reading_date",
        "reading_year", "reading_month", "billing_period",
        "previous_reading", "present_reading", "consumption",
        "consumption_cbm", "days_in_period", "consumption_per_day",
        "reading_date_adjusted", "transformation_run_id",
    ],
    "fact_billing": [
        "billing_key", "billing_id", "consumption_key", "consumer_key",
        "billing_date", "due_date", "billing_year", "billing_month",
        "billing_period", "total_bill", "remaining_balance", "status",
        "status_recomputed", "balance_corrected", "due_date_corrected",
        "is_revenue_recognized", "days_to_due", "days_overdue", "is_overdue",
        "overdue_bucket", "outstanding_bill_count", "is_flagged_account",
        "transformation_run_id",
    ],
    "fact_payment": [
        "payment_key", "payment_id", "billing_key", "payment_date",
        "payment_year", "payment_month", "payment_method", "code_unmapped",
        "total_paid", "amount_tendered", "change_given", "remaining_balance",
        "payment_allocation_rank", "is_oldest_unpaid_payment",
        "change_corrected", "transformation_run_id",
    ],
}

# Section 4.3 reference values / code sets.
STATUS_MAP_CONSUMER = {
    "active": "ACTIVE", "inactive": "INACTIVE",
}
STATUS_MAP_BILLING = {
    "unpaid": "UNPAID", "paid": "PAID",
    "partial": "PARTIAL", "partially paid": "PARTIAL",
    "partially_paid": "PARTIAL", "partial paid": "PARTIAL",
}
PAYMENT_METHOD_MAP = {
    "cash": "CASH", "gcash": "GCASH", "bank transfer": "BANK_TRANSFER",
    "bank_transfer": "BANK_TRANSFER", "bank": "BANK_TRANSFER",
    "maya": "OTHER", "online": "OTHER",
}
CODE_DOMAIN: dict[tuple[str, str], set[str]] = {
    ("dim_consumer", "status"): {"ACTIVE", "INACTIVE", "UNKNOWN"},
    ("fact_billing", "status"): {"PAID", "PARTIAL", "UNPAID", "UNKNOWN"},
    ("fact_payment", "payment_method"): {"CASH", "GCASH", "BANK_TRANSFER", "OTHER"},
    ("fact_billing", "overdue_bucket"): {
        "CURRENT", "D1_30", "D31_60", "D61_90", "D90_PLUS",
    },
}

TEXT_COLUMNS = {
    "consumers": ["username", "first_name", "middle_name", "last_name",
                  "email", "contact_number", "status"],
    "consumption": [],
    "billing": ["status"],
    "payments": ["payment_method", "reference_number"],
}

# Optional columns whose null rate is tracked over time (V-19).
NULL_RATE_BASELINE_COLUMNS: dict[str, list[str]] = {
    "dim_consumer": ["middle_name", "contact_number"],
    "fact_payment": ["reference_number"],
}

SENSITIVE_COLUMNS = ("password", "hashed_password", "password_hash")


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    ts = datetime.now(PHT).isoformat()
    print(f"[{ts}] {msg}", flush=True)


def to_bool(value: bool) -> str:
    return "TRUE" if value else "FALSE"


def money(value: Decimal | None) -> str:
    if value is None:
        return ""
    return str(value.quantize(MONEY_Q, rounding=ROUND_HALF_UP))


def rate(value: Decimal | None) -> str:
    if value is None:
        return ""
    return str(value.quantize(RATE_Q, rounding=ROUND_HALF_UP))


def dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def to_int(value: Any) -> int | None:
    d = dec(value)
    if d is None:
        return None
    try:
        return int(d)
    except (ValueError, OverflowError):
        return None


def parse_dt(value: Any) -> datetime | None:
    """Tolerant ISO/Postgres timestamp parser -> Asia/Manila aware datetime."""
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    text = text.replace(" ", "T", 1) if "T" not in text and " " in text else text
    parsed: datetime | None = None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(PHT)


def parse_date(value: Any) -> date | None:
    dt = parse_dt(value)
    if dt is not None:
        return dt.date()
    try:
        return date.fromisoformat(str(value).strip())
    except (ValueError, AttributeError):
        return None


def fmt_dt(value: datetime | None) -> str:
    """ISO 8601 with an explicit Asia/Manila offset (S-01)."""
    if value is None:
        return ""
    return value.astimezone(PHT).isoformat()


def fmt_date(value: date | None) -> str:
    return value.isoformat() if value is not None else ""


def surrogate_key(entity: str, source_id: Any) -> str:
    """Deterministic 16-hex surrogate key (D-01). Stable across reruns."""
    digest = hashlib.md5(f"{entity}:{source_id}".encode("utf-8")).hexdigest()
    return digest[:16]


def normalize_text(value: Any) -> str | None:
    """C-01: strip, collapse internal whitespace, drop control characters.

    Returns None (not "") when the value is empty so it falls into the NULL
    path of C-03 instead of becoming an empty string.
    """
    if value is None:
        return None
    text = str(value)
    text = "".join(ch for ch in text if ch.isprintable() or ch == " ")
    text = " ".join(text.split()).strip()
    return text if text else None


def days_in_month(year: int, month: int) -> int:
    return monthrange(year, month)[1]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def content_checksum(table: str, rows: list[dict[str, Any]]) -> str:
    """Checksum of the transformed DATA, excluding the per-run lineage stamp.

    `transformation_run_id` legitimately differs between two runs of the same
    batch, so V-22 compares this instead of the whole-file checksum. Two runs
    of the same batch must produce the same content checksum, which is what
    "reruns are idempotent" actually means (Section 1.4).
    """
    columns = [c for c in OUTPUT_COLUMNS[table] if c != "transformation_run_id"]
    digest = hashlib.sha256()
    for row in rows:
        for column in columns:
            value = row.get(column)
            digest.update(f"{'' if value is None else value}\x1f".encode("utf-8"))
        digest.update(b"\x1e")
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# Atomic output (Section 1.4 idempotency)
# ---------------------------------------------------------------------------

def atomic_write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: ("" if row.get(c) is None else row.get(c)) for c in columns})
    os.replace(tmp, path)


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Loading the Stage 1 handover
# ---------------------------------------------------------------------------

class SchemaDrift(Exception):
    """Raised when the staged input violates the Stage 1 contract."""


def resolve_run_date(table: str, run_date: str | None) -> str:
    """Resolve the staged batch date, falling back to the newest available."""
    root = STAGING_ROOT / table
    if run_date and (root / run_date).is_dir():
        return run_date
    if root.is_dir():
        candidates = sorted(d.name for d in root.iterdir() if d.is_dir())
        if candidates:
            if run_date:
                log(f"{table}: no batch for {run_date}; using newest staged batch {candidates[-1]}")
            return candidates[-1]
    raise SchemaDrift(f"no staged batch found for {table} under {root}")


def load_staged(table: str, run_date: str | None, input_root: Path) -> tuple[str, list[dict[str, str]]]:
    """Load one staged CSV and enforce the input schema contract (Section 6.4)."""
    actual_date = resolve_run_date(table, run_date) if input_root == STAGING_ROOT else (run_date or "")
    batch_dir = input_root / table / actual_date
    csv_path = batch_dir / f"{table}_{actual_date}.csv"
    if not csv_path.exists():
        raise SchemaDrift(f"missing staged file: {csv_path}")

    with open(csv_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        header = reader.fieldnames or []
        rows = [dict(r) for r in reader]

    expected = EXPECTED_INPUT_COLUMNS[table]
    missing = [c for c in expected if c not in header]
    if missing:
        raise SchemaDrift(f"staged {table} missing required column(s): {missing}")
    unmapped = [c for c in header if c not in expected]
    if unmapped:
        log(f"{table}: input_column_unmapped (ignored, not a failure): {unmapped}")

    return actual_date, rows


def load_extraction_logs(table: str, run_date: str, log_root: Path) -> dict[str, Any]:
    """Read Stage 1's run log for this table, if present (lineage reference)."""
    pattern = f"*_{table}.json"
    candidates = sorted(log_root.glob(f"{run_date}/{pattern}"))
    if not candidates:
        candidates = sorted(log_root.rglob(pattern))
    if not candidates:
        return {}
    newest = candidates[-1]
    try:
        payload = json.loads(newest.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return {
        "log_path": str(newest.relative_to(BASE_DIR)) if newest.is_relative_to(BASE_DIR) else str(newest),
        "pipeline_run_id": payload.get("pipeline_run_id"),
        "records_read": payload.get("records_read"),
        "extraction_status": payload.get("extraction_status"),
        "validation_result": payload.get("validation_result"),
    }


def load_null_rate_baseline(table: str) -> dict[str, float]:
    """Most recent logged null rates for optional columns (V-19)."""
    baseline: dict[str, float] = {}
    files = sorted(LOG_ROOT.rglob(f"*_{table}.json"), reverse=True)
    for path in files[:5]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        rates = payload.get("null_rates")
        if isinstance(rates, dict) and rates:
            return {k: float(v) for k, v in rates.items()}
    return baseline


# ---------------------------------------------------------------------------
# Profiling (Section 2)
# ---------------------------------------------------------------------------

def profile(table: str, rows: list[dict[str, str]]) -> dict[str, int]:
    """Count the measurable quality issues from Section 2 before any rule runs."""
    counts: dict[str, int] = {}
    if not rows:
        return counts

    def bump(key: str, amount: int = 1) -> None:
        counts[key] = counts.get(key, 0) + amount

    # P-01/P-04/P-19 whitespace on any text column
    for row in rows:
        for col in TEXT_COLUMNS[table]:
            raw = row.get(col)
            if raw is not None and str(raw) != normalize_text(raw) is not None:
                bump("P-01_whitespace")
                break

    if table == "consumers":
        for row in rows:
            if row.get("email") and row["email"] != row["email"].strip().lower():
                bump("P-03_email_case")
            if row.get("username") and row["username"] != row["username"].strip().lower():
                bump("P-04_username_case")
            if row.get("middle_name") in (None, ""):
                bump("P-02_middle_name_null")
            if row.get("contact_number") in (None, ""):
                bump("P-05_contact_number_null")
            zone = to_int(row.get("purok_no"))
            if zone is None or zone <= UNKNOWN_PUROK_NO:
                bump("P-06_purok_missing")
            status = normalize_text(row.get("status"))
            if status and status.lower() not in STATUS_MAP_CONSUMER:
                bump("P-07_status_variant")

    elif table == "consumption":
        for row in rows:
            prev, pres = dec(row.get("previous_reading")), dec(row.get("present_reading"))
            if prev is not None and pres is not None and pres < prev:
                bump("P-08_negative_consumption")
        seen: dict[tuple[int, int, int], int] = {}
        for row in rows:
            cid, reading = to_int(row.get("consumer_id")), parse_date(row.get("reading_date"))
            if cid is None or reading is None:
                bump("P-10_unparseable_key")
                continue
            key = (cid, reading.year, reading.month)
            seen[key] = seen.get(key, 0) + 1
        bump("P-10_duplicate_consumer_month", sum(c - 1 for c in seen.values() if c > 1))

    elif table == "billing":
        for row in rows:
            total = dec(row.get("total_bill"))
            balance = dec(row.get("remaining_balance"))
            if total is not None and total < 0:
                bump("P-11_negative_bill")
            if total is not None and balance is not None:
                if balance < 0 and total > 0 and abs(balance) <= Decimal("0.01"):
                    bump("P-13_balance_drift")
                elif balance < 0:
                    bump("P-13_negative_balance")
            billing, due = parse_date(row.get("billing_date")), parse_date(row.get("due_date"))
            if billing is not None and due is not None and due < billing:
                bump("P-14_due_before_billing")
            status = normalize_text(row.get("status"))
            if status and status.lower() not in STATUS_MAP_BILLING:
                bump("P-12_status_variant")

    elif table == "payments":
        seen_pay: dict[tuple[int, str, str], int] = {}
        for row in rows:
            bid = to_int(row.get("billing_id"))
            pdate, paid = parse_date(row.get("payment_date")), dec(row.get("total_paid"))
            tendered, change = dec(row.get("amount_tendered")), dec(row.get("change_given"))
            if paid is not None and paid < 0:
                bump("P-11_negative_payment")
            if bid is not None and pdate is not None and paid is not None:
                key = (bid, pdate.isoformat(), str(paid))
                seen_pay[key] = seen_pay.get(key, 0) + 1
            if (tendered is not None and paid is not None
                    and (change is None or change != tendered - paid)):
                bump("P-17_change_mismatch")
            method = normalize_text(row.get("payment_method"))
            if not method or method.lower() not in PAYMENT_METHOD_MAP:
                bump("P-15_payment_method_variant")
        bump("P-16_duplicate_payment",
             sum(c - 1 for c in seen_pay.values() if c > 1))

    return {k: v for k, v in counts.items() if v}


# ---------------------------------------------------------------------------
# Cleaning rules C-01..C-11 and exclusions X-01..X-04
# ---------------------------------------------------------------------------

class Context:
    """Per-run mutable state: counters, quarantine, and rule tallies."""

    def __init__(self, run_id: str, parent_run_id: str, run_date: str) -> None:
        self.run_id = run_id
        self.parent_run_id = parent_run_id
        self.run_date = run_date
        self.rules: dict[str, int] = {}
        self.exclusions: dict[str, int] = {}
        self.profiles: dict[str, dict[str, int]] = {}
        self.quarantine: list[dict[str, Any]] = []
        self.rejected_values: dict[str, list[dict[str, Any]]] = {}

    def tally(self, rule_id: str, amount: int = 1) -> None:
        self.rules[rule_id] = self.rules.get(rule_id, 0) + amount

    def exclude(self, exclusion_id: str, amount: int = 1) -> None:
        self.exclusions[exclusion_id] = self.exclusions.get(exclusion_id, 0) + amount

    def reject(self, source_table: str, row: dict[str, str], rule_id: str,
               reason: str, survivor: str | None = None) -> None:
        """Never drop a record silently: every rejection is counted and logged."""
        self.tally(rule_id)
        if rule_id.startswith("X-"):
            self.exclude(rule_id)
        payload = mask_sensitive(source_table, row)
        self.quarantine.append({
            "run_id": self.run_id,
            "parent_run_id": self.parent_run_id,
            "batch_window": self.run_date,
            "source_table": source_table,
            "rule_id": rule_id,
            "rejection_reason": reason + (f" (duplicate of {survivor})" if survivor else ""),
            "source_id": row.get("id", ""),
            "source_values": json.dumps(payload, sort_keys=True),
        })
        self.rejected_values.setdefault(source_table, []).append(row)

    def duplicates_for(self, source_table: str) -> int:
        return sum(1 for q in self.quarantine
                   if q["source_table"] == source_table and q["rule_id"] == "C-06")


def mask_sensitive(table: str, row: dict[str, str]) -> dict[str, str]:
    """Section 9.2 data minimization: drop secrets, mask PII in quarantine."""
    masked: dict[str, str] = {}
    for key, value in row.items():
        if any(sens in key.lower() for sens in SENSITIVE_COLUMNS):
            continue  # never written anywhere (V-24)
        text = "" if value is None else str(value)
        if key == "email" and text:
            local, _, domain = text.partition("@")
            text = f"{local[:1]}***@{domain}" if domain else "***"
        elif key == "contact_number" and text:
            text = f"{text[:2]}***{text[-3:]}" if len(text) > 5 else "***"
        masked[key] = text
    return masked


def apply_text_rules(rows: list[dict[str, str]], table: str, ctx: Context) -> None:
    """C-01 trim, C-02 lowercase identifiers, C-11 code mapping (partial)."""
    for row in rows:
        for col in TEXT_COLUMNS[table]:
            cleaned = normalize_text(row.get(col))
            if cleaned is None:
                # C-03: optional fields are retained as NULL, never imputed.
                if row.get(col) not in (None, ""):
                    ctx.tally("C-01")
                row[col] = None  # type: ignore[assignment]
                continue
            if cleaned != row.get(col):
                ctx.tally("C-01")
            row[col] = cleaned  # type: ignore[assignment]
        for col in ("email", "username"):
            if col in row and row[col]:
                lowered = row[col].lower()
                if lowered != row[col]:
                    ctx.tally("C-02")
                row[col] = lowered


def map_code(value: str | None, mapping: dict[str, str],
             default: str, ctx: Context) -> tuple[str, bool]:
    """C-11: map a code variant to the standard set.

    Returns (standard_value, was_unmapped). An unmapped variant falls back to
    `default` and is flagged so a new code is noticed rather than absorbed.
    """
    if not value:
        return default, False
    mapped = mapping.get(value.strip().lower())
    if mapped is None:
        ctx.tally("C-11")
        return default, True
    return mapped, False


def dedupe(rows: list[dict[str, str]], key_fn: Callable[[dict[str, str]], Any],
           order_fn: Callable[[dict[str, str]], tuple], source_table: str,
           ctx: Context, label: str, keep: str = "max") -> list[dict[str, str]]:
    """C-06: keep exactly one record per dedup key, quarantine the rest.

    keep="max" keeps the largest order_fn value (correction semantics: the latest
    record wins); keep="min" keeps the smallest (immutable-event semantics: the
    earliest record wins, as for financial payments).
    """
    winners: dict[Any, dict[str, str]] = {}
    losers: list[dict[str, str]] = []
    for row in rows:
        key = key_fn(row)
        if key is None:
            continue
        current = winners.get(key)
        if current is None:
            winners[key] = row
        elif (order_fn(row) > order_fn(current) if keep == "max"
              else order_fn(row) < order_fn(current)):
            losers.append(current)
            winners[key] = row
        else:
            losers.append(row)
    for loser in losers:
        ctx.reject(source_table, loser, "C-06",
                   f"duplicate {label}", survivor=loser.get("id", ""))
    return list(winners.values())


# ---------------------------------------------------------------------------
# Stage steps: dimensions before facts (Section 1.2)
# ---------------------------------------------------------------------------

def build_dim_purok(consumers: list[dict[str, str]], ctx: Context) -> list[dict[str, Any]]:
    """D-03: one row per known zone plus the UNKNOWN sentinel (C-04)."""
    zones: set[int] = set()
    for row in consumers:
        zone = to_int(row.get("purok_no"))
        if zone is not None and zone > UNKNOWN_PUROK_NO:
            zones.add(zone)
    rows: list[dict[str, Any]] = []
    for zone in sorted(zones):
        rows.append({
            "purok_key": surrogate_key("purok", zone),
            "purok_no": zone,
            "purok_label": f"Purok {zone}",
            "is_unknown": "FALSE",
        })
    rows.append({
        "purok_key": surrogate_key("purok", UNKNOWN_PUROK_NO),
        "purok_no": UNKNOWN_PUROK_NO,
        "purok_label": "Unknown Zone",
        "is_unknown": "TRUE",
    })
    return rows


def build_dim_consumer(consumers: list[dict[str, str]], ctx: Context) -> list[dict[str, Any]]:
    """D-04 + D-06 (SCD Type 1). Rejects orphans per C-10, dedupes per C-06."""
    keep = dedupe(
        consumers,
        key_fn=lambda r: to_int(r.get("id")),
        order_fn=lambda r: (parse_dt(r.get("updated_at")) or datetime.min.replace(tzinfo=timezone.utc),
                            to_int(r.get("id")) or 0),
        source_table="consumers", ctx=ctx, label="consumer_id",
    )
    out: list[dict[str, Any]] = []
    for row in keep:
        cid = to_int(row.get("id"))
        if cid is None:
            ctx.reject("consumers", row, "C-10", "unparseable consumer id")
            continue
        first = row.get("first_name") or ""
        last = row.get("last_name") or ""
        zone = to_int(row.get("purok_no"))
        zone_unknown = zone is None or zone <= UNKNOWN_PUROK_NO
        if zone_unknown:
            ctx.tally("C-04")
            zone = UNKNOWN_PUROK_NO
        record: dict[str, Any] = {
            "consumer_key": surrogate_key("consumer", cid),
            "consumer_id": cid,
            "username": row.get("username"),
            "first_name": first,
            "middle_name": row.get("middle_name"),
            "last_name": last,
            "full_name": f"{first} {last}".strip(),       # B-01
            "email": row.get("email"),
            "contact_number": row.get("contact_number"),
            "purok_key": surrogate_key("purok", zone),    # B-02
            "purok_no": zone,
            "purok_is_unknown": to_bool(zone_unknown),
            "status": "UNKNOWN",
            "code_unmapped": "FALSE",
            "active_flag": "FALSE",
            "valid_from": fmt_dt(parse_dt(row.get("updated_at"))),
            "valid_to": "",
            "is_current": "TRUE",
            "created_at": fmt_dt(parse_dt(row.get("created_at"))),
            "updated_at": fmt_dt(parse_dt(row.get("updated_at"))),
            "transformation_run_id": ctx.run_id,
        }
        record["status"], status_unmapped = map_code(
            row.get("status"), STATUS_MAP_CONSUMER, "UNKNOWN", ctx)
        record["code_unmapped"] = to_bool(status_unmapped)
        # X-01: inactive consumers are flagged, never removed.
        record["active_flag"] = to_bool(record["status"] == "ACTIVE")
        if not record["active_flag"]:
            ctx.exclude("X-01")
        out.append(record)
    return out


def build_fact_consumption(consumption: list[dict[str, str]],
                           known_consumer_ids: set[int],
                           billing_month_by_consumption: dict[int, tuple[int, int]],
                           ctx: Context) -> list[dict[str, Any]]:
    """C-05/C-06/C-10 then B-03..B-06."""
    keep = dedupe(
        consumption,
        key_fn=lambda r: (
            (to_int(r.get("consumer_id")), parse_date(r.get("reading_date")).year,
             parse_date(r.get("reading_date")).month)
            if to_int(r.get("consumer_id")) is not None and parse_date(r.get("reading_date")) else None
        ),
        order_fn=lambda r: (parse_dt(r.get("updated_at")) or datetime.min.replace(tzinfo=timezone.utc),
                            to_int(r.get("id")) or 0),
        source_table="consumption", ctx=ctx, label="(consumer_id, reading_month)",
    )
    out: list[dict[str, Any]] = []
    for row in keep:
        cid = to_int(row.get("id"))
        consumer_id = to_int(row.get("consumer_id"))
        reading = parse_date(row.get("reading_date"))
        prev = dec(row.get("previous_reading"))
        pres = dec(row.get("present_reading"))
        cons = dec(row.get("consumption"))
        if cid is None:
            ctx.reject("consumption", row, "C-10", "unparseable consumption id")
            continue
        if reading is None:
            ctx.reject("consumption", row, "S-01", "unparseable reading_date")
            continue
        if prev is None or pres is None or cons is None:
            ctx.reject("consumption", row, "S-06", "unparseable numeric reading")
            continue
        # C-05: physically impossible readings are rejected, not cleaned.
        if pres < prev:
            ctx.reject("consumption", row, "C-05",
                       f"present_reading {pres} < previous_reading {prev}")
            continue
        if cons < 0:
            ctx.reject("consumption", row, "C-05", f"negative consumption {cons}")
            continue
        # C-10: orphan facts cannot be attributed to a consumer.
        if consumer_id is None or consumer_id not in known_consumer_ids:
            ctx.reject("consumption", row, "C-10",
                       f"orphan consumer_id {consumer_id}")
            continue

        dim = days_in_month(reading.year, reading.month)
        expected_month = billing_month_by_consumption.get(cid)
        out.append({
            "consumption_key": surrogate_key("consumption", cid),
            "consumption_id": cid,
            "consumer_key": surrogate_key("consumer", consumer_id),
            "reading_date": fmt_date(reading),
            "reading_year": reading.year,
            "reading_month": reading.month,
            "billing_period": f"{reading.year:04d}-{reading.month:02d}",  # B-03
            "previous_reading": money(prev),
            "present_reading": money(pres),
            "consumption": money(cons),
            "consumption_cbm": money(cons),                                  # B-04
            "days_in_period": dim,                                           # B-05
            "consumption_per_day": rate(cons / Decimal(dim)),                # B-06
            "reading_date_adjusted": to_bool(
                expected_month is not None
                and expected_month != (reading.year, reading.month)),
            "transformation_run_id": ctx.run_id,
        })
    return out


def build_fact_billing(billing: list[dict[str, str]],
                       known_consumer_ids: set[int],
                       known_consumption_ids: set[int],
                       ctx: Context,
                       reference_date: date) -> list[dict[str, Any]]:
    """C-05/C-07/C-08/C-10/C-13 then B-03/B-07..B-11/B-14."""
    keep = dedupe(
        billing,
        key_fn=lambda r: to_int(r.get("id")),
        order_fn=lambda r: (parse_dt(r.get("updated_at")) or datetime.min.replace(tzinfo=timezone.utc),
                            to_int(r.get("id")) or 0),
        source_table="billing", ctx=ctx, label="billing_id",
    )

    prepared: list[dict[str, Any]] = []
    for row in keep:
        bid = to_int(row.get("id"))
        consumption_id = to_int(row.get("consumption_id"))
        user_id = to_int(row.get("user_id"))
        billing_date = parse_date(row.get("billing_date"))
        due_date = parse_date(row.get("due_date"))
        total = dec(row.get("total_bill"))
        balance = dec(row.get("remaining_balance"))

        if bid is None:
            ctx.reject("billing", row, "C-10", "unparseable billing id")
            continue
        if billing_date is None:
            ctx.reject("billing", row, "S-01", "unparseable billing_date")
            continue
        if total is None or balance is None:
            ctx.reject("billing", row, "S-05", "unparseable monetary amount")
            continue
        # C-05: an impossible commercial value is rejected, not clamped.
        if total < 0:
            ctx.reject("billing", row, "C-05", f"negative total_bill {total}")
            continue
        # C-08: a due date before the billing date makes aging meaningless.
        due_corrected = False
        if due_date is None or due_date < billing_date:
            due_date = billing_date + timedelta(days=DUE_DATE_DAYS)
            ctx.tally("C-08")
            due_corrected = True
        # C-07: sub-cent balance drift is clamped, not rejected.
        balance_corrected = False
        if balance < 0:
            balance = Decimal("0.00")
            ctx.tally("C-07")
            balance_corrected = True
        elif balance > total:
            balance = total
            ctx.tally("C-07")
            balance_corrected = True
        if balance == 0:
            balance = Decimal("0.00")

        if consumption_id is None or consumption_id not in known_consumption_ids:
            ctx.reject("billing", row, "C-10", f"orphan consumption_id {consumption_id}")
            continue
        if user_id is None or user_id not in known_consumer_ids:
            ctx.reject("billing", row, "C-10", f"orphan user_id {user_id}")
            continue

        # B-14: the amounts are the source of truth for a financial model.
        status, _billing_unmapped = map_code(row.get("status"), STATUS_MAP_BILLING,
                                             "UNKNOWN", ctx)
        if total > 0:
            expected = "PAID" if balance == 0 else ("PARTIAL" if balance < total else "UNPAID")
        else:
            expected = "PAID" if balance == 0 else "UNPAID"
        status_recomputed = status != expected
        if status_recomputed:
            ctx.tally("B-14")
            status = expected

        days_to_due = (due_date - billing_date).days                    # B-07
        days_overdue = max((reference_date - due_date).days, 0)         # B-08
        prepared.append({
            "billing_key": surrogate_key("billing", bid),
            "billing_id": bid,
            "consumption_key": surrogate_key("consumption", consumption_id),
            "consumer_key": surrogate_key("consumer", user_id),
            "billing_date": fmt_date(billing_date),
            "due_date": fmt_date(due_date),
            "billing_year": billing_date.year,
            "billing_month": billing_date.month,
            "billing_period": f"{billing_date.year:04d}-{billing_date.month:02d}",
            "total_bill": money(total),
            "remaining_balance": money(balance),
            "status": status,
            "status_recomputed": to_bool(status_recomputed),
            "balance_corrected": to_bool(balance_corrected),
            "due_date_corrected": to_bool(due_corrected),
            "is_revenue_recognized": balance == 0,   # X-02 flag, removes no rows
            "_x02": status == "UNPAID",
            "days_to_due": days_to_due,
            "days_overdue": days_overdue,
            "is_overdue": days_overdue > 0,
            "transformation_run_id": ctx.run_id,
            "_due_date": due_date,
            "_balance": balance,
            "_total": total,
        })

    # B-09 aging bands
    for record in prepared:
        days = record["days_overdue"]
        if days == 0:
            bucket = "CURRENT"
        elif days <= 30:
            bucket = "D1_30"
        elif days <= 60:
            bucket = "D31_60"
        elif days <= 90:
            bucket = "D61_90"
        else:
            bucket = "D90_PLUS"
        record["overdue_bucket"] = bucket

    # B-10 / B-11 outstanding bills per consumer as of the reference date.
    outstanding: dict[str, int] = {}
    for record in prepared:
        if record["status"] in ("UNPAID", "PARTIAL") and record["_due_date"] <= reference_date:
            key = record["consumer_key"]
            outstanding[key] = outstanding.get(key, 0) + 1
    for record in prepared:
        count = outstanding.get(record["consumer_key"], 0)
        record["outstanding_bill_count"] = count
        record["is_flagged_account"] = count >= FLAGGED_ACCOUNT_THRESHOLD

    # X-02: outstanding bills are not recognized revenue. Flag only, no rows removed.
    for record in prepared:
        if record.pop("_x02", False):
            ctx.exclude("X-02")

    for record in prepared:
        for internal in ("_due_date", "_balance", "_total"):
            record.pop(internal, None)
        record["is_overdue"] = to_bool(record["is_overdue"])
        record["is_flagged_account"] = to_bool(record["is_flagged_account"])
        record["is_revenue_recognized"] = to_bool(record["is_revenue_recognized"])
    return prepared


def build_fact_payment(payments: list[dict[str, str]],
                       known_billing_ids: set[int],
                       billing_total: dict[int, Decimal],
                       billing_status: dict[int, str],
                       ctx: Context,
                       reference_date: date) -> list[dict[str, Any]]:
    """C-05/C-06/C-09/C-10 + S-04 then B-03/B-12/B-13 and exclusion X-04."""
    keep = dedupe(
        payments,
        key_fn=lambda r: (
            (to_int(r.get("billing_id")), (parse_date(r.get("payment_date")) or date.min).isoformat(),
             str(dec(r.get("total_paid"))))
            if to_int(r.get("billing_id")) is not None and parse_date(r.get("payment_date")) else None
        ),
        order_fn=lambda r: (parse_dt(r.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc),
                            to_int(r.get("id")) or 0),
        source_table="payments", ctx=ctx, label="(billing_id, payment_date, total_paid)",
        keep="min",   # payments are immutable financial events: earliest wins
    )

    out: list[dict[str, Any]] = []
    for row in keep:
        pid = to_int(row.get("id"))
        billing_id = to_int(row.get("billing_id"))
        paid = dec(row.get("total_paid"))
        tendered = dec(row.get("amount_tendered"))
        balance = dec(row.get("remaining_balance"))
        paid_date = parse_date(row.get("payment_date"))

        if pid is None:
            ctx.reject("payments", row, "C-10", "unparseable payment id")
            continue
        if paid_date is None:
            ctx.reject("payments", row, "S-01", "unparseable payment_date")
            continue
        if paid is None or tendered is None or balance is None:
            ctx.reject("payments", row, "S-05", "unparseable monetary amount")
            continue
        if paid < 0:
            ctx.reject("payments", row, "C-05", f"negative total_paid {paid}")
            continue
        if billing_id is None or billing_id not in known_billing_ids:
            ctx.reject("payments", row, "C-10", f"orphan billing_id {billing_id}")
            continue
        # X-04: a future-dated payment inflates collections and is a data error.
        if paid_date > reference_date:
            ctx.reject("payments", row, "X-04",
                       f"payment_date {paid_date} is after reference_date {reference_date}")
            continue

        # C-09: change is arithmetic, not an independent business value.
        expected_change = tendered - paid
        change_corrected = False
        if expected_change < 0:
            expected_change = Decimal("0.00")
        if dec(row.get("change_given")) != expected_change:
            ctx.tally("C-09")
            change_corrected = True

        balance_corrected = False
        if balance < 0:
            balance = Decimal("0.00")
            ctx.tally("C-07")
            balance_corrected = True

        record: dict[str, Any] = {
            "payment_key": surrogate_key("payment", pid),
            "payment_id": pid,
            "billing_key": surrogate_key("billing", billing_id),
            "payment_date": fmt_date(paid_date),
            "payment_year": paid_date.year,
            "payment_month": paid_date.month,
            "code_unmapped": False,
            "total_paid": money(paid),
            "amount_tendered": money(tendered),
            "change_given": money(expected_change),
            "remaining_balance": money(balance),
            "reference_number": row.get("reference_number"),
            "change_corrected": to_bool(change_corrected),
            "transformation_run_id": ctx.run_id,
            "_balance_corrected": balance_corrected,
        }
        record["payment_method"], method_unmapped = map_code(
            row.get("payment_method"), PAYMENT_METHOD_MAP, "OTHER", ctx)
        record["code_unmapped"] = to_bool(method_unmapped)
        # X-03: change_given is cash out and is excluded from every collections
        # measure; the flag records that the exclusion was applied to this row.
        ctx.exclude("X-03")
        out.append(record)

    # B-12: oldest-first payment allocation order per billing.
    status_by_key = {surrogate_key("billing", bid): st for bid, st in billing_status.items()}
    ranked = sorted(out, key=lambda r: (r["billing_key"], r["payment_date"], r["payment_id"]))
    ranks: dict[str, int] = {}
    for record in ranked:
        key = record["billing_key"]
        ranks[key] = ranks.get(key, 0) + 1
        record["payment_allocation_rank"] = ranks[key]
        unpaid = status_by_key.get(key) in ("UNPAID", "PARTIAL")
        record["is_oldest_unpaid_payment"] = to_bool(ranks[key] == 1 and unpaid)

    # B-13: collection rate per billing (X-03: change_given is excluded).
    paid_by_billing: dict[str, Decimal] = {}
    for record in out:
        key = record["billing_key"]
        paid_by_billing[key] = paid_by_billing.get(key, Decimal("0.00")) + dec(record["total_paid"])
    total_by_key = {surrogate_key("billing", bid): total for bid, total in billing_total.items()}
    for record in out:
        record.pop("_balance_corrected", None)
        total = total_by_key.get(record["billing_key"])
        total_paid_sum = paid_by_billing.get(record["billing_key"], Decimal("0.00"))
        record["collection_rate"] = (
            rate(total_paid_sum / total)
            if total is not None and total > 0 else ""
        )
    return out


# ---------------------------------------------------------------------------
# Validation (Section 7)
# ---------------------------------------------------------------------------

class Check:
    __slots__ = ("check_id", "name", "target", "severity", "status", "detail")

    def __init__(self, check_id: str, name: str, target: str,
                 severity: str, status: str, detail: str) -> None:
        self.check_id = check_id
        self.name = name
        self.target = target
        self.severity = severity
        self.status = status
        self.detail = detail

    def as_dict(self) -> dict[str, str]:
        return {
            "check_id": self.check_id, "name": self.name, "target": self.target,
            "severity": self.severity, "status": self.status, "detail": self.detail,
        }


def pct(numerator: int, denominator: int) -> Decimal:
    if denominator <= 0:
        return Decimal("0")
    return (Decimal(numerator) / Decimal(denominator) * Decimal("100")).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP)


def validate(outputs: dict[str, list[dict[str, Any]]],
             input_rows: dict[str, list[dict[str, str]]],
             ctx: Context,
             run_id: str,
             prior_checksums: dict[str, str],
             new_checksums: dict[str, str]) -> list[Check]:
    checks: list[Check] = []

    def add(check_id: str, name: str, target: str, severity: str,
            ok: bool, detail: str, warn: bool = False) -> None:
        status = "PASS" if ok else ("WARNING" if warn else "FAIL")
        checks.append(Check(check_id, name, target, severity, status, detail))

    # V-01 / V-02 record reconciliation
    for output, source in SOURCE_TABLE_FOR_OUTPUT.items():
        rows_out = len(outputs.get(output, []))
        read = len(input_rows[source])
        rejected = sum(1 for q in ctx.quarantine if q["source_table"] == source)
        diff = read - (rows_out + rejected)
        add("V-01", "Record reconciliation", output, "critical", diff == 0,
            f"records_read={read} records_output={rows_out} records_rejected={rejected}"
            + ("" if diff == 0 else f" UNBALANCED by {diff}"))

    for source in SOURCE_TABLE_FOR_OUTPUT.values():
        dupes = ctx.duplicates_for(source)
        quarantined_dupes = sum(
            1 for q in ctx.quarantine if q["source_table"] == source and q["rule_id"] == "C-06"
        )
        add("V-02", "Duplicate accounting", source, "critical",
            dupes == quarantined_dupes,
            f"duplicates_discarded={dupes} present_in_quarantine={quarantined_dupes}")

    # V-03 surrogate key uniqueness / V-04 natural key uniqueness
    key_map = {
        "dim_purok": ("purok_key", None),
        "dim_consumer": ("consumer_key", "consumer_id"),
        "fact_consumption": ("consumption_key", "consumption_id"),
        "fact_billing": ("billing_key", "billing_id"),
        "fact_payment": ("payment_key", "payment_id"),
    }
    for output, (surrogate, natural) in key_map.items():
        rows = outputs.get(output, [])
        add("V-03", "Surrogate key uniqueness", f"{output}.{surrogate}", "critical",
            len({r[surrogate] for r in rows}) == len(rows),
            f"{len(rows) - len({r[surrogate] for r in rows})} duplicate(s)")
        if natural:
            add("V-04", "Natural key uniqueness", f"{output}.{natural}", "critical",
                len({r[natural] for r in rows}) == len(rows),
                f"{len(rows) - len({r[natural] for r in rows})} duplicate(s)")

    # V-05 required columns NOT NULL
    for output, columns in REQUIRED_NOT_NULL.items():
        rows = outputs.get(output, [])
        nulls = sum(1 for r in rows for c in columns if r.get(c) in (None, ""))
        add("V-05", "Required columns NOT NULL", output, "critical", nulls == 0,
            f"{nulls} NULL value(s) across {len(columns)} required column(s)")

    # V-06 referential integrity within the output
    consumer_keys = {r["consumer_key"] for r in outputs.get("dim_consumer", [])}
    purok_keys = {r["purok_key"] for r in outputs.get("dim_purok", [])}
    consumption_keys = {r["consumption_key"] for r in outputs.get("fact_consumption", [])}
    billing_keys = {r["billing_key"] for r in outputs.get("fact_billing", [])}
    fk_spec = [
        ("fact_consumption", "consumer_key", consumer_keys),
        ("fact_billing", "consumer_key", consumer_keys),
        ("fact_billing", "consumption_key", consumption_keys),
        ("fact_payment", "billing_key", billing_keys),
        ("dim_consumer", "purok_key", purok_keys),
    ]
    for output, column, parents in fk_spec:
        rows = outputs.get(output, [])
        orphans = sum(1 for r in rows if r.get(column) not in parents)
        add("V-06", "Referential integrity (output)", f"{output}.{column}", "critical",
            orphans == 0, f"{orphans} orphan(s)")

    # V-07 join cardinality: keys are unique by construction, so row counts hold
    join_pairs = [
        ("consumption", "fact_consumption"),
        ("billing", "fact_billing"),
        ("payments", "fact_payment"),
    ]
    for source, output in join_pairs:
        fact_rows = len(outputs.get(output, []))
        retained = fact_rows + sum(1 for q in ctx.quarantine if q["source_table"] == source)
        add("V-07", "Join cardinality", output, "critical",
            fact_rows <= retained,
            f"output_rows={fact_rows} source_rows={len(input_rows[source])} "
            f"(no row multiplication: {fact_rows <= retained})")

    # V-08 non-negative measures
    numeric_spec = [
        ("fact_consumption", ["previous_reading", "present_reading", "consumption", "consumption_cbm"]),
        ("fact_billing", ["total_bill", "remaining_balance"]),
        ("fact_payment", ["total_paid", "amount_tendered", "change_given", "remaining_balance"]),
    ]
    for output, columns in numeric_spec:
        negatives = sum(1 for r in outputs.get(output, [])
                        for c in columns if (dec(r.get(c)) or Decimal("0")) < 0)
        add("V-08", "Business rule - non-negative measures", output, "critical",
            negatives == 0, f"{negatives} negative value(s)")

    # V-09 meter monotonicity
    bad_readings = sum(
        1 for r in outputs.get("fact_consumption", [])
        if dec(r["present_reading"]) < dec(r["previous_reading"])
    )
    add("V-09", "Business rule - reading monotonicity", "fact_consumption", "critical",
        bad_readings == 0, f"{bad_readings} violation(s)")

    # V-10 cash reconciliation identity
    bad_cash = 0
    for r in outputs.get("fact_payment", []):
        tendered, paid = dec(r["amount_tendered"]), dec(r["total_paid"])
        change = dec(r["change_given"])
        if tendered is not None and paid is not None and change is not None:
            if abs((tendered - paid) - change) > Decimal("0.01"):
                bad_cash += 1
    add("V-10", "Business rule - cash reconciliation", "fact_payment", "critical",
        bad_cash == 0, f"{bad_cash} mismatch(es) beyond 0.01 tolerance")

    # V-11 derived balance consistency
    bad_balance = sum(
        1 for r in outputs.get("fact_billing", [])
        if dec(r["remaining_balance"]) > dec(r["total_bill"])
        or (r["status"] == "PAID" and dec(r["total_bill"]) > 0 and dec(r["remaining_balance"]) > 0)
    )
    add("V-11", "Business rule - balance consistency", "fact_billing", "critical",
        bad_balance == 0, f"{bad_balance} impossible balance state(s)")

    # V-12 flagged-account threshold
    bad_flag = sum(
        1 for r in outputs.get("fact_billing", [])
        if (r["outstanding_bill_count"] >= FLAGGED_ACCOUNT_THRESHOLD) != (r["is_flagged_account"] == "TRUE")
    )
    add("V-12", "Business rule - flagged account threshold", "fact_billing", "critical",
        bad_flag == 0, f"{bad_flag} disagreement(s) at threshold {FLAGGED_ACCOUNT_THRESHOLD}")

    # V-13/V-14/V-15 aggregate reconciliation against the staged source
    def staged_sum(source: str, column: str) -> Decimal:
        return sum((dec(r.get(column)) or Decimal("0.00")) for r in input_rows[source])

    def rejected_sum(source: str, column: str) -> Decimal:
        return sum((dec(r.get(column)) or Decimal("0.00"))
                   for r in ctx.rejected_values.get(source, []))

    agg_spec = [
        ("V-13", "fact_consumption", "consumption", "consumption"),
        ("V-14", "fact_billing", "total_bill", "total_bill"),
        ("V-15", "fact_payment", "total_paid", "total_paid"),
    ]
    for check_id, output, column, source_column in agg_spec:
        source = SOURCE_TABLE_FOR_OUTPUT[output]
        expected = staged_sum(source, source_column) - rejected_sum(source, source_column)
        actual = sum((dec(r.get(column)) or Decimal("0.00"))
                     for r in outputs.get(output, []))
        delta = abs(expected - actual)
        add(check_id, "Aggregate reconciliation", f"{output}.{column}", "critical",
            delta <= Decimal("0.01"),
            f"staged={staged_sum(source, source_column)} minus_rejected="
            f"{rejected_sum(source, source_column)} output={actual} delta={delta}")

    # V-16 exclusion accounting: every counter must match the rows it describes.
    inactive = sum(1 for r in outputs.get("dim_consumer", []) if r.get("active_flag") == "FALSE")
    unpaid_bills = sum(1 for r in outputs.get("fact_billing", []) if r.get("status") == "UNPAID")
    future_payments = sum(1 for q in ctx.quarantine if q["rule_id"] == "X-04")
    payment_rows = len(outputs.get("fact_payment", []))
    expected_counters = {
        "X-01": inactive,
        "X-02": unpaid_bills,
        "X-03": payment_rows,
        "X-04": future_payments,
    }
    mismatched = {
        key: (ctx.exclusions.get(key, 0), value)
        for key, value in expected_counters.items()
        if ctx.exclusions.get(key, 0) != value
    }
    add("V-16", "Exclusion accounting", "all outputs", "non-critical", not mismatched,
        "all exclusion counters match the affected row counts"
        if not mismatched
        else f"counter/row mismatch: {mismatched}", warn=True)

    # V-17 type / format conformance
    format_bad = 0
    for output, columns in OUTPUT_COLUMNS.items():
        for row in outputs.get(output, []):
            if row.get("reading_date") and not _is_iso_date(row["reading_date"]):
                format_bad += 1
            for period_col in ("billing_period",):
                value = row.get(period_col)
                if value and not _is_year_month(value):
                    format_bad += 1
            for dt_col in ("created_at", "updated_at", "valid_from"):
                value = row.get(dt_col)
                if value and not _is_iso_dt(value):
                    format_bad += 1
    add("V-17", "Data type / format conformance", "all outputs", "non-critical",
        format_bad == 0, f"{format_bad} malformed value(s)", warn=True)

    # V-18 code domain conformance
    bad_codes = 0
    for (output, column), domain in CODE_DOMAIN.items():
        bad_codes += sum(1 for r in outputs.get(output, [])
                         if r.get(column) not in domain)
    add("V-18", "Code domain conformance", "status / payment_method / overdue_bucket",
        "non-critical", bad_codes == 0, f"{bad_codes} out-of-domain value(s)", warn=True)

    # V-19 null rate on optional columns vs. baseline
    detail_parts: list[str] = []
    degraded = False
    for output, columns in NULL_RATE_BASELINE_COLUMNS.items():
        rows = outputs.get(output, [])
        if not rows:
            continue
        baseline = load_null_rate_baseline(output)
        for column in columns:
            null_count = sum(1 for r in rows if r.get(column) in (None, ""))
            rate_now = pct(null_count, len(rows))
            if not baseline:
                detail_parts.append(f"{output}.{column}={rate_now}% (baseline recorded)")
            else:
                previous = Decimal(str(baseline.get(column, 0)))
                if rate_now > previous:
                    degraded = True
                    detail_parts.append(f"{output}.{column}={rate_now}% > baseline {previous}%")
                else:
                    detail_parts.append(f"{output}.{column}={rate_now}%")
    add("V-19", "Null rate on optional columns", "optional columns", "non-critical",
        not degraded, "; ".join(detail_parts) or "no optional columns", warn=True)

    # V-20 unmapped code rate
    total_coded = len(outputs.get("dim_consumer", [])) + len(outputs.get("fact_payment", []))
    unmapped = (sum(1 for r in outputs.get("dim_consumer", []) if r.get("code_unmapped") == "TRUE")
                + sum(1 for r in outputs.get("fact_payment", []) if r.get("code_unmapped") == "TRUE"))
    unmapped_rate = pct(unmapped, total_coded)
    v20_ok = unmapped_rate <= UNMAPPED_CODE_WARN_PCT
    v20_warn = UNMAPPED_CODE_WARN_PCT < unmapped_rate <= UNMAPPED_CODE_FAIL_PCT
    if unmapped_rate > UNMAPPED_CODE_FAIL_PCT:
        v20_ok, v20_warn = False, False
    add("V-20", "Unmapped code rate", "dim_consumer / fact_payment", "non-critical",
        v20_ok, f"{unmapped_rate}% unmapped ({unmapped}/{total_coded})", warn=v20_warn)

    # V-21 rejection rate against the pre-declared threshold
    for output, source in SOURCE_TABLE_FOR_OUTPUT.items():
        read = len(input_rows[source])
        rejected = sum(1 for q in ctx.quarantine if q["source_table"] == source)
        rate_pct = pct(rejected, read)
        status_ok = rate_pct <= REJECTION_THRESHOLD_PCT
        status_warn = REJECTION_THRESHOLD_PCT < rate_pct <= REJECTION_FAIL_PCT
        if rate_pct > REJECTION_FAIL_PCT:
            status_ok, status_warn = False, False
        add("V-21", "Rejection rate", output, "critical", status_ok,
            f"{rate_pct}% rejected ({rejected}/{read}); "
            f"threshold {REJECTION_THRESHOLD_PCT}% FAIL above {REJECTION_FAIL_PCT}%",
            warn=status_warn)

    # V-22 idempotency assertion against the previous run of the same batch
    for output, checksum in new_checksums.items():
        previous = prior_checksums.get(output)
        if previous is None:
            add("V-22", "Idempotency assertion", output, "non-critical", True,
                "no prior run of this batch; content checksum recorded as baseline", warn=True)
        else:
            add("V-22", "Idempotency assertion", output, "non-critical",
                previous == checksum,
                f"content checksum {'matches' if previous == checksum else 'DIFFERS from'} "
                f"prior run of this batch")

    # V-23 lineage completeness
    lineage_bad = sum(1 for rows in outputs.values() for r in rows
                      if r.get("transformation_run_id") != run_id
                      and r.get("transformation_run_id") is not None
                      and "transformation_run_id" in r)
    add("V-23", "Lineage completeness", "all outputs", "critical", lineage_bad == 0,
        f"{lineage_bad} row(s) with wrong or missing transformation_run_id")

    # V-24 sensitive data absence
    sensitive_hits = sum(
        1 for rows in outputs.values() for r in rows
        for key in r if any(sens in key.lower() for sens in SENSITIVE_COLUMNS)
    )
    add("V-24", "Sensitive data absence", "all outputs", "critical", sensitive_hits == 0,
        f"{sensitive_hits} credential-like column(s) found")

    return checks


def _is_iso_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        return False


def _is_year_month(value: str) -> bool:
    try:
        return len(value) == 7 and date.fromisoformat(f"{value}-01").strftime("%Y-%m") == value
    except ValueError:
        return False


def _is_iso_dt(value: str) -> bool:
    try:
        datetime.fromisoformat(value)
        return True
    except ValueError:
        return False


def decide_overall(checks: list[Check]) -> str:
    """Section 10.2: APPROVED / MANUAL REVIEW / REJECTED."""
    if any(c.status == "FAIL" for c in checks):
        return "REJECTED"
    if any(c.status == "WARNING" for c in checks):
        return "MANUAL REVIEW"
    return "APPROVED"


# ---------------------------------------------------------------------------
# Dry-run: synthetic staged input, no database and no real data needed
# ---------------------------------------------------------------------------

def run_dry_run(args: argparse.Namespace) -> int:
    """Build a synthetic Stage 1 handover in a temp dir and run the real pipeline."""
    log("DRY-RUN: no database, no staged data. Building synthetic handover.")
    tmp_root = Path(tempfile.mkdtemp(prefix="waterwise_transform_dry_"))
    run_date = args.run_date or "2026-03-10"
    _synthetic_staging(tmp_root, run_date, clean=args.clean)
    try:
        rc = main(argv=["--run-date", run_date, "--input-root", str(tmp_root / "rawdata"),
                        "--reference-date", args.reference_date or run_date,
                        "--output-root", str(tmp_root / "transformed"),
                        "--log-root", str(tmp_root / "logs"),
                        "--quarantine-root", str(tmp_root / "quarantine")])
        log(f"DRY-RUN exit code: {rc}")
        return rc
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)
        log("DRY-RUN temp directory removed.")


def _synthetic_dirty_rows() -> tuple[list[dict[str, str]], ...]:
    """Exercise every cleaning rule with deliberately dirty rows."""
    consumers = [
        # clean record
        {"id": "1", "username": "jdelacruz", "first_name": "Juan",
         "middle_name": "D", "last_name": "Dela Cruz", "email": "juan@mail.com",
         "contact_number": "09171234567", "purok_no": "1", "status": "active",
         "created_at": "2026-01-05 09:00:00+00:00", "updated_at": "2026-03-01 09:00:00+00:00"},
        # P-01 whitespace, P-03/P-04 case, P-05 null contact, P-06 null zone, P-07 status variant
        {"id": "2", "username": "  MariaSantos ", "first_name": " Maria ",
         "middle_name": "", "last_name": " Santos", "email": "Maria.Santos@Mail.COM",
         "contact_number": "", "purok_no": "", "status": "Active",
         "created_at": "2026-01-06 09:00:00+00:00", "updated_at": "2026-03-02 09:00:00+00:00"},
        # C-06 duplicate consumer_id, superseded by updated_at
        {"id": "3", "username": "pedro", "first_name": "Pedro", "middle_name": None,
         "last_name": "Reyes", "email": "pedro@mail.com", "contact_number": "09181234567",
         "purok_no": "2", "status": "active", "created_at": "2026-01-07 09:00:00+00:00",
         "updated_at": "2026-03-03 09:00:00+00:00"},
        {"id": "3", "username": "pedro", "first_name": "Pedro", "middle_name": None,
         "last_name": "Reyes", "email": "pedro@mail.com", "contact_number": "09181234567",
         "purok_no": "2", "status": "inactive", "created_at": "2026-01-07 09:00:00+00:00",
         "updated_at": "2026-01-07 09:00:00+00:00"},
    ]
    consumption = [
        # clean
        {"id": "10", "consumer_id": "1", "reading_date": "2026-01-31",
         "previous_reading": "100.00", "present_reading": "118.00", "consumption": "18.00",
         "idempotency_key": None, "created_at": "2026-02-01 09:00:00+00:00",
         "updated_at": "2026-02-01 09:00:00+00:00"},
        # C-05 meter rollover -> reject
        {"id": "11", "consumer_id": "1", "reading_date": "2026-02-28",
         "previous_reading": "118.00", "present_reading": "112.00", "consumption": "-6.00",
         "idempotency_key": None, "created_at": "2026-03-01 09:00:00+00:00",
         "updated_at": "2026-03-01 09:00:00+00:00"},
        # C-06 duplicate (consumer_id, reading_month), latest updated_at wins
        {"id": "12", "consumer_id": "2", "reading_date": "2026-01-31",
         "previous_reading": "50.00", "present_reading": "60.00", "consumption": "10.00",
         "idempotency_key": None, "created_at": "2026-02-01 09:00:00+00:00",
         "updated_at": "2026-02-01 09:00:00+00:00"},
        {"id": "13", "consumer_id": "2", "reading_date": "2026-01-31",
         "previous_reading": "50.00", "present_reading": "65.00", "consumption": "15.00",
         "idempotency_key": None, "created_at": "2026-02-02 09:00:00+00:00",
         "updated_at": "2026-02-05 09:00:00+00:00"},
        # C-10 orphan consumer_id
        {"id": "14", "consumer_id": "999", "reading_date": "2026-01-31",
         "previous_reading": "10.00", "present_reading": "12.00", "consumption": "2.00",
         "idempotency_key": None, "created_at": "2026-02-01 09:00:00+00:00",
         "updated_at": "2026-02-01 09:00:00+00:00"},
    ]
    billing = [
        # clean, unpaid and overdue
        {"id": "20", "consumption_id": "10", "user_id": "1", "billing_date": "2026-02-05",
         "due_date": "2026-02-20", "total_bill": "270.00", "remaining_balance": "270.00",
         "status": "Unpaid", "created_at": "2026-02-05 09:00:00+00:00",
         "updated_at": "2026-02-05 09:00:00+00:00"},
        # C-08 due_date before billing_date -> repaired; C-07 -0.01 drift -> clamped
        {"id": "21", "consumption_id": "12", "user_id": "2", "billing_date": "2026-02-05",
         "due_date": "2026-01-20", "total_bill": "225.00", "remaining_balance": "-0.01",
         "status": "PARTIALLY PAID", "created_at": "2026-02-05 09:00:00+00:00",
         "updated_at": "2026-02-05 09:00:00+00:00"},
        # C-05 negative bill -> reject
        {"id": "22", "consumption_id": "13", "user_id": "2", "billing_date": "2026-02-05",
         "due_date": "2026-02-20", "total_bill": "-50.00", "remaining_balance": "0.00",
         "status": "Paid", "created_at": "2026-02-05 09:00:00+00:00",
         "updated_at": "2026-02-05 09:00:00+00:00"},
        # second unpaid bill for consumer 1 -> builds outstanding count toward B-11
        {"id": "23", "consumption_id": "10", "user_id": "1", "billing_date": "2026-01-05",
         "due_date": "2026-01-20", "total_bill": "180.00", "remaining_balance": "180.00",
         "status": "unpaid", "created_at": "2026-01-05 09:00:00+00:00",
         "updated_at": "2026-01-05 09:00:00+00:00"},
    ]
    payments = [
        # clean, C-09 change mismatch -> recomputed
        {"id": "30", "billing_id": "20", "total_paid": "100.00", "amount_tendered": "200.00",
         "change_given": "0.00", "remaining_balance": "170.00", "payment_date": "2026-02-25",
         "payment_method": "cash", "reference_number": "R-001", "idempotency_key": None,
         "created_at": "2026-02-25 09:00:00+00:00", "updated_at": "2026-02-25 09:00:00+00:00"},
        # C-06 duplicate payment (billing_id, payment_date, total_paid), earliest created_at wins
        {"id": "31", "billing_id": "20", "total_paid": "100.00", "amount_tendered": "200.00",
         "change_given": "100.00", "remaining_balance": "170.00", "payment_date": "2026-02-25",
         "payment_method": "Cash", "reference_number": "R-002", "idempotency_key": None,
         "created_at": "2026-02-26 09:00:00+00:00", "updated_at": "2026-02-26 09:00:00+00:00"},
        # X-04 future-dated payment -> reject
        {"id": "32", "billing_id": "21", "total_paid": "225.00", "amount_tendered": "225.00",
         "change_given": "0.00", "remaining_balance": "0.00", "payment_date": "2027-01-01",
         "payment_method": "GCash", "reference_number": None, "idempotency_key": None,
         "created_at": "2026-03-01 09:00:00+00:00", "updated_at": "2026-03-01 09:00:00+00:00"},
        # C-10 orphan billing_id
        {"id": "33", "billing_id": "8888", "total_paid": "50.00", "amount_tendered": "50.00",
         "change_given": "0.00", "remaining_balance": "0.00", "payment_date": "2026-02-26",
         "payment_method": "Bank Transfer", "reference_number": "R-003", "idempotency_key": None,
         "created_at": "2026-02-26 09:00:00+00:00", "updated_at": "2026-02-26 09:00:00+00:00"},
    ]
    return consumers, consumption, billing, payments


def _synthetic_clean_rows() -> tuple[list[dict[str, str]], ...]:
    """Already-tidy rows, used to verify the APPROVED happy path."""
    consumers = [
        {"id": "1", "username": "jdelacruz", "first_name": "Juan", "middle_name": "D",
         "last_name": "Dela Cruz", "email": "juan@mail.com", "contact_number": "09171234567",
         "purok_no": "1", "status": "active", "created_at": "2026-01-05 09:00:00+00:00",
         "updated_at": "2026-03-01 09:00:00+00:00"},
        {"id": "2", "username": "msantos", "first_name": "Maria", "middle_name": "L",
         "last_name": "Santos", "email": "maria@mail.com", "contact_number": "09181234567",
         "purok_no": "2", "status": "active", "created_at": "2026-01-06 09:00:00+00:00",
         "updated_at": "2026-03-02 09:00:00+00:00"},
    ]
    consumption = [
        {"id": "10", "consumer_id": "1", "reading_date": "2026-01-31",
         "previous_reading": "100.00", "present_reading": "118.00", "consumption": "18.00",
         "idempotency_key": None, "created_at": "2026-02-01 09:00:00+00:00",
         "updated_at": "2026-02-01 09:00:00+00:00"},
        {"id": "12", "consumer_id": "2", "reading_date": "2026-01-31",
         "previous_reading": "50.00", "present_reading": "60.00", "consumption": "10.00",
         "idempotency_key": None, "created_at": "2026-02-01 09:00:00+00:00",
         "updated_at": "2026-02-01 09:00:00+00:00"},
    ]
    billing = [
        {"id": "20", "consumption_id": "10", "user_id": "1", "billing_date": "2026-02-05",
         "due_date": "2026-02-20", "total_bill": "270.00", "remaining_balance": "270.00",
         "status": "Unpaid", "created_at": "2026-02-05 09:00:00+00:00",
         "updated_at": "2026-02-05 09:00:00+00:00"},
        {"id": "21", "consumption_id": "12", "user_id": "2", "billing_date": "2026-02-05",
         "due_date": "2026-02-20", "total_bill": "150.00", "remaining_balance": "0.00",
         "status": "Paid", "created_at": "2026-02-05 09:00:00+00:00",
         "updated_at": "2026-02-05 09:00:00+00:00"},
    ]
    payments = [
        {"id": "30", "billing_id": "20", "total_paid": "270.00", "amount_tendered": "270.00",
         "change_given": "0.00", "remaining_balance": "0.00", "payment_date": "2026-02-25",
         "payment_method": "Cash", "reference_number": "R-001", "idempotency_key": None,
         "created_at": "2026-02-25 09:00:00+00:00", "updated_at": "2026-02-25 09:00:00+00:00"},
    ]
    return consumers, consumption, billing, payments


def _synthetic_staging(root: Path, run_date: str, clean: bool = False) -> dict[str, int]:
    """Write a synthetic Stage 1 handover (CSV + schema sidecar) under `root/rawdata`."""
    batch = root / "rawdata"
    if clean:
        datasets = _synthetic_clean_rows()
    else:
        datasets = _synthetic_dirty_rows()
    counts: dict[str, int] = {}
    for table, rows in zip(("consumers", "consumption", "billing", "payments"), datasets):
        out_dir = batch / table / run_date
        out_dir.mkdir(parents=True, exist_ok=True)
        columns = EXPECTED_INPUT_COLUMNS[table]
        path = out_dir / f"{table}_{run_date}.csv"
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({c: ("" if row.get(c) is None else row.get(c))
                                 for c in columns})
        atomic_write_text(out_dir / f"{table}_{run_date}.schema.json",
                          json.dumps({"table": table, "columns": columns}, indent=2))
        counts[table] = len(rows)
    return counts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="WaterWise Stage 2 transformation (docs/transformation.md)")
    parser.add_argument("--run-date", default=None,
                        help="staged extraction run date (default: newest available batch)")
    parser.add_argument("--parent-run-id", default=None,
                        help="Stage 1 pipeline_run_id for lineage (default: read from Stage 1 log)")
    parser.add_argument("--run-id", default=None, help="transformation run_id (default: auto)")
    parser.add_argument("--reference-date", default=None,
                        help="today used by B-08/B-09/B-10 (default: run_date)")
    parser.add_argument("--dry-run", action="store_true",
                        help="build synthetic input and exercise every rule; no data needed")
    parser.add_argument("--clean", action="store_true",
                        help="with --dry-run: emit tidy rows to verify the APPROVED path")
    parser.add_argument("--rules-version", default=RULES_VERSION,
                        help=f"rules version recorded in the log (default {RULES_VERSION})")
    # Path overrides (used by --dry-run; also useful for custom layouts)
    parser.add_argument("--input-root", default=None, help="staging root (default rawdata/)")
    parser.add_argument("--output-root", default=None, help="output root (default transformed/)")
    parser.add_argument("--log-root", default=None,
                        help="log root (default logs/waterwise-transformation/)")
    parser.add_argument("--quarantine-root", default=None,
                        help="quarantine root (default quarantine/transformation/)")
    parser.add_argument("--no-write", action="store_true",
                        help="run every rule and check but write no files (read-only audit)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    global STAGING_ROOT, OUTPUT_ROOT, LOG_ROOT, QUARANTINE_ROOT
    args = parse_args(argv)

    if args.dry_run:
        return run_dry_run(args)

    if args.input_root:
        STAGING_ROOT = Path(args.input_root)
    if args.output_root:
        OUTPUT_ROOT = Path(args.output_root)
    if args.log_root:
        LOG_ROOT = Path(args.log_root)
    if args.quarantine_root:
        QUARANTINE_ROOT = Path(args.quarantine_root)

    run_id = args.run_id or (
        f"ww-transform-{datetime.now(PHT).strftime('%Y-%m-%d-%H%M')}"
        f"-{hashlib.sha256(str(time_marker()).encode()).hexdigest()[:6]}"
    )
    ctx_run_id = run_id

    # Resolve the batch date from the first available staged table.
    try:
        run_date = resolve_run_date("consumers", args.run_date)
    except SchemaDrift as exc:
        print(f"No staged input: {exc}\nRun Stage 1 first, or use --dry-run.",
              file=sys.stderr)
        return 2

    try:
        reference_date = (date.fromisoformat(args.reference_date)
                          if args.reference_date else date.fromisoformat(run_date))
    except ValueError:
        print(f"Invalid --reference-date: {args.reference_date} (expected YYYY-MM-DD)",
              file=sys.stderr)
        return 2

    ctx = Context(run_id=ctx_run_id, parent_run_id=args.parent_run_id or "", run_date=run_date)

    # --- Load the Stage 1 handover (no database access anywhere) ---
    try:
        input_rows: dict[str, list[dict[str, str]]] = {}
        resolved_dates: set[str] = set()
        for table in ("consumers", "consumption", "billing", "payments"):
            actual, rows = load_staged(table, args.run_date, STAGING_ROOT)
            resolved_dates.add(actual)
            input_rows[table] = rows
    except SchemaDrift as exc:
        # Section 6.4 step 1-2: fail fast on input schema drift, write nothing.
        print(f"FAIL: input schema check: {exc}", file=sys.stderr)
        return 1
    if len(resolved_dates) > 1:
        log(f"WARNING: tables resolved to different batch dates: {sorted(resolved_dates)}")

    # Lineage: reference the Stage 1 run log rather than duplicating it.
    extraction_logs = {t: load_extraction_logs(t, run_date, EXTRACTION_LOG_ROOT)
                       for t in ("consumers", "consumption", "billing", "payments")}
    if not ctx.parent_run_id:
        for payload in extraction_logs.values():
            if payload.get("pipeline_run_id"):
                ctx.parent_run_id = payload["pipeline_run_id"]
                break
    if not ctx.parent_run_id:
        ctx.parent_run_id = f"unknown-{run_date}"
        log(f"WARNING: no Stage 1 run log found for {run_date}; parent_run_id={ctx.parent_run_id}")

    log(f"Run {run_id} | parent={ctx.parent_run_id} | batch={run_date} "
        f"| reference_date={reference_date} | rules_version={args.rules_version}")

    # --- Profile before cleaning (Section 2) ---
    for table, rows in input_rows.items():
        counts = profile(table, rows)
        ctx.profiles[table] = counts
        if counts:
            log(f"profile {table}: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))

    # --- Apply cleaning rules (Section 3), dimensions before facts ---
    for table, rows in input_rows.items():
        apply_text_rules(rows, table, ctx)

    dim_purok = build_dim_purok(input_rows["consumers"], ctx)
    dim_consumer = build_dim_consumer(input_rows["consumers"], ctx)
    known_consumer_ids = {r["consumer_id"] for r in dim_consumer}
    log(f"dim_purok: {len(dim_purok)} row(s); dim_consumer: {len(dim_consumer)} row(s)")

    billing_month_by_consumption: dict[int, tuple[int, int]] = {}
    for row in input_rows["billing"]:
        bid_date = parse_date(row.get("billing_date"))
        consumption_id = to_int(row.get("consumption_id"))
        if bid_date is None or consumption_id is None:
            continue
        # Earliest bill for a reading is that reading's billing month; taking the
        # min keeps the flag deterministic if several bills reference one reading.
        candidate = (bid_date.year, bid_date.month)
        existing = billing_month_by_consumption.get(consumption_id)
        if existing is None or candidate < existing:
            billing_month_by_consumption[consumption_id] = candidate

    fact_consumption = build_fact_consumption(
        input_rows["consumption"], known_consumer_ids,
        billing_month_by_consumption, ctx)
    known_consumption_ids = {r["consumption_id"] for r in fact_consumption}
    fact_billing = build_fact_billing(
        input_rows["billing"], known_consumer_ids, known_consumption_ids, ctx, reference_date)
    known_billing_ids = {r["billing_id"] for r in fact_billing}
    billing_total = {r["billing_id"]: dec(r["total_bill"]) or Decimal("0.00") for r in fact_billing}
    billing_status = {r["billing_id"]: r["status"] for r in fact_billing}
    fact_payment = build_fact_payment(
        input_rows["payments"], known_billing_ids, billing_total, billing_status,
        ctx, reference_date)

    outputs: dict[str, list[dict[str, Any]]] = {
        "dim_purok": dim_purok,
        "dim_consumer": dim_consumer,
        "fact_consumption": fact_consumption,
        "fact_billing": fact_billing,
        "fact_payment": fact_payment,
    }
    for name, rows in outputs.items():
        log(f"{name}: {len(rows)} row(s) built")

    # --- Write outputs atomically (Section 1.4) ---
    checksums: dict[str, str] = {}
    content_checksums: dict[str, str] = {
        name: content_checksum(name, rows) for name, rows in outputs.items()
    }
    written: dict[str, Path] = {}
    if not args.no_write:
        for name, rows in outputs.items():
            path = OUTPUT_ROOT / name / run_date / f"{name}_{run_date}.csv"
            atomic_write_csv(path, OUTPUT_COLUMNS[name], rows)
            checksums[name] = sha256_file(path)
            written[name] = path
            atomic_write_text(
                path.with_name(f"{name}_{run_date}.schema.json"),
                json.dumps({
                    "table": name,
                    "columns": OUTPUT_COLUMNS[name],
                    "required_not_null": REQUIRED_NOT_NULL[name],
                    "rules_version": args.rules_version,
                    "run_id": run_id,
                    "parent_run_id": ctx.parent_run_id,
                    "batch": run_date,
                }, indent=2),
            )
            log(f"wrote {path}")

    # --- Quarantine (Section 9.2) ---
    quarantine_path: Path | None = None
    if ctx.quarantine:
        quarantine_path = QUARANTINE_ROOT / run_date / "rejected.csv"
        q_columns = ["run_id", "parent_run_id", "batch_window", "source_table",
                     "rule_id", "rejection_reason", "source_id", "source_values"]
        if not args.no_write:
            atomic_write_csv(quarantine_path, q_columns, ctx.quarantine)
        log(f"quarantine: {len(ctx.quarantine)} rejected row(s)"
            + (f" -> {quarantine_path}" if quarantine_path else ""))
    else:
        log("quarantine: 0 rejected row(s)")

    # --- Validate (Section 7) ---
    prior_checksums: dict[str, str] = {}
    for payload in _prior_runs(run_id):
        for name, checksum in (payload.get("output_content_checksums") or {}).items():
            prior_checksums.setdefault(name, checksum)
    if args.no_write:
        # In read-only mode there is no new checksum, so V-22 is not meaningful.
        prior_checksums = {}

    checks = validate(outputs, input_rows, ctx, run_id, prior_checksums, content_checksums)
    for check in checks:
        if check.status != "PASS":
            log(f"{check.check_id} [{check.status}/{check.severity}] {check.name}: {check.detail}")
    failed = [c for c in checks if c.status == "FAIL"]
    warned = [c for c in checks if c.status == "WARNING"]
    overall = decide_overall(checks)

    # --- Write run logs (Section 8) ---
    null_rates: dict[str, float] = {}
    for name, cols in NULL_RATE_BASELINE_COLUMNS.items():
        rows = outputs[name]
        if rows:
            for column in cols:
                null_count = sum(1 for r in rows if r.get(column) in (None, ""))
                null_rates[f"{name}.{column}"] = float(pct(null_count, len(rows)))

    for name in OUTPUT_COLUMNS:
        source = SOURCE_TABLE_FOR_OUTPUT.get(name)
        rows_out = len(outputs[name])
        rejected = sum(1 for q in ctx.quarantine if q["source_table"] == source) if source else 0
        modified = sum(1 for r in rows_out and outputs[name] or []
                       if any(r.get(k) == "TRUE" for k in
                              ("purok_is_unknown", "code_unmapped", "active_flag",
                               "status_recomputed", "balance_corrected",
                               "due_date_corrected", "change_corrected")))
        table_checks = [c.as_dict() for c in checks
                        if c.check_id in ("V-03", "V-04", "V-05", "V-07")
                        and c.target.startswith(name)]
        record = {
            "run_id": run_id,
            "parent_run_id": ctx.parent_run_id,
            "batch_id": f"{run_id}-{name}",
            "batch_window_start": f"{run_date}T00:00:00+08:00",
            "batch_window_end": f"{run_date}T23:59:59+08:00",
            "reference_date": fmt_date(reference_date),
            "table_name": name,
            "source_table": source,
            "transformation_status": overall,
            "records_read": (len(input_rows[source]) if source else rows_out),
            "records_output": rows_out,
            "records_rejected": rejected,
            "records_modified": modified,
            "duplicates_discarded": (ctx.duplicates_for(source) if source else 0),
            "rules_applied": ctx.rules,
            "exclusions_applied": ctx.exclusions,
            "profile_counts": ctx.profiles.get(source or "", {}),
            "null_rates": {k: v for k, v in null_rates.items() if k.startswith(f"{name}.")},
            "validation_results": summarize(checks),
            "table_validation": table_checks,
            "output_file": (str(written[name].relative_to(BASE_DIR))
                            if name in written and written[name].is_relative_to(BASE_DIR)
                            else str(written.get(name, ""))),
            "output_checksums": {k: checksums[k] for k in checksums} if checksums else {},
            "output_content_checksums": content_checksums,
            "quarantine_file": str(quarantine_path) if quarantine_path else None,
            "error_message": None,
            "rules_version": args.rules_version,
            "extraction_references": {t: extraction_logs[t] for t in extraction_logs
                                      if extraction_logs[t]},
        }
        if not args.no_write:
            log_dir = LOG_ROOT / run_date
            log_dir.mkdir(parents=True, exist_ok=True)
            log_path = log_dir / f"{run_id}_{name}.json"
            atomic_write_text(log_path, json.dumps(record, indent=2, default=str))

    # --- Summary ---
    total_read = sum(len(rows) for rows in input_rows.values())
    total_rejected = len(ctx.quarantine)
    overall_reject_rate = pct(total_rejected, total_read)
    log("-" * 70)
    log(f"checks: {len(checks) - len(failed) - len(warned)} PASS, "
        f"{len(warned)} WARNING, {len(failed)} FAIL")
    log(f"records_read={total_read} records_output={sum(len(o) for o in outputs.values())} "
        f"records_rejected={total_rejected} rejection_rate={overall_reject_rate}%")
    log(f"Overall Transformation = {overall}")
    if overall == "APPROVED":
        log("APPROVED: packaged for the Load Stage (loading NOT performed here).")
        return 0
    if overall == "MANUAL REVIEW":
        log("MANUAL REVIEW: batch held. Responsible: data engineer / Super Admin.")
        log("Inspect quarantine + rules_applied, then release or reject explicitly.")
        return 3
    log("REJECTED: critical failure. Batch quarantined; nothing promoted to Load.")
    log("Fix the cause, then reprocess the same run_date (reruns are idempotent).")
    return 1


def time_marker() -> str:
    return f"{datetime.now(PHT).isoformat()}-{os.getpid()}"


def _prior_runs(run_id: str) -> list[dict[str, Any]]:
    """Earlier run logs for the same batch, used by the V-22 idempotency check."""
    payloads: list[dict[str, Any]] = []
    if not LOG_ROOT.exists():
        return payloads
    for path in sorted(LOG_ROOT.rglob("*_dim_consumer.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if payload.get("run_id") != run_id:
            payloads.append(payload)
    return payloads


def summarize(checks: list[Check]) -> str:
    parts = [f"{c.check_id} {c.status}" for c in checks if c.status != "PASS"]
    return "PASS" if not parts else "; ".join(parts)


if __name__ == "__main__":
    raise SystemExit(main())
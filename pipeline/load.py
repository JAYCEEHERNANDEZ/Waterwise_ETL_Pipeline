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

Usage:
    # Read the newest handover
    python -m pipeline.load

    # Read the handover of a specific transformation run date
    python -m pipeline.load --run-date 2026-03-10

Exit codes:
    0 = handover read successfully
    1 = handover found but incomplete or inconsistent
    2 = usage / input error (no handover to read)
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent  # repo root (Waterwise_ETL_Pipeline/)
TRANSFORMED_ROOT = BASE_DIR / "transformed"
TRANSFORMATION_LOG_ROOT = BASE_DIR / "logs" / "waterwise-transformation"

LOAD_CODE_VERSION = "v1.0"

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
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WaterWise Stage 3 load")
    parser.add_argument("--run-date", default=None,
                        help="transformation run date to load, YYYY-MM-DD (default: newest)")
    parser.add_argument("--run-id", default=None, help="load run_id (default: auto ww-load-...)")
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

    if handover.problems:
        log(f"Handover incomplete: {len(handover.problems)} problem(s). Nothing loaded.")
        return 1
    log(f"Handover read: {handover.total_rows} row(s) in {len(handover.tables)} table(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())

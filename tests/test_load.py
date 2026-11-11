"""Tests for the Stage 3 load (pipeline/load.py).

Run from the repository root:

    python -m unittest discover -s tests -v

Two groups of tests:

- Offline tests need no database. They build a synthetic handover with the
  real transformation code (pipeline/transform.py) in a temporary folder.
- Warehouse tests run only when WAREHOUSE_DB_URL is set. They NEVER commit:
  every warehouse connection is wrapped so that commit() becomes a rollback.
  Just before rolling back, the wrapper records what would have been
  committed, and the tests check that. The warehouse is left unchanged.
"""

from __future__ import annotations

import contextlib
import copy
import io
import os
import shutil
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline import load as L          # noqa: E402
from pipeline import transform as T     # noqa: E402

RUN_DATE = "2026-03-10"
AMOUNTS = {"fact_consumption": "consumption", "fact_billing": "total_bill",
           "fact_payment": "total_paid"}
TMP: Path
_REAL_CONNECT = L.connect_warehouse


def setUpModule() -> None:
    """Build one synthetic, APPROVED transformation handover for all tests."""
    global TMP
    TMP = Path(tempfile.mkdtemp(prefix="waterwise_load_test_"))
    T._synthetic_staging(TMP, RUN_DATE, clean=True)
    with contextlib.redirect_stdout(io.StringIO()):
        rc = T.main(["--run-date", RUN_DATE, "--input-root", str(TMP / "rawdata"),
                     "--output-root", str(TMP / "transformed"),
                     "--log-root", str(TMP / "tlogs"),
                     "--quarantine-root", str(TMP / "tquarantine")])
    if rc != 0:
        raise RuntimeError(f"synthetic transformation failed with exit code {rc}")


def tearDownModule() -> None:
    shutil.rmtree(TMP, ignore_errors=True)


def handover() -> L.Handover:
    return L.read_handover(RUN_DATE, TMP / "transformed", TMP / "tlogs")


def set_status(item: L.Handover, table: str, status: str) -> L.Handover:
    item = copy.deepcopy(item)
    item.tables[table].transformation_status = status
    return item


class StubCursor:
    """Returns the given rows from fetchone(), in order."""

    def __init__(self, *rows):
        self.rows = list(rows)

    def execute(self, *args, **kwargs):
        pass

    def fetchone(self):
        return self.rows.pop(0)


# ---------------------------------------------------------------------------
# Offline tests
# ---------------------------------------------------------------------------

class HandoverTests(unittest.TestCase):

    def test_reads_all_five_tables(self):
        h = handover()
        self.assertEqual(list(h.tables), list(L.LOAD_ORDER))
        self.assertEqual(h.problems, [])
        self.assertEqual(h.batch_id, f"ww-batch-{RUN_DATE}")
        for item in h.tables.values():
            self.assertEqual(item.row_count, item.records_output)
            self.assertEqual(item.checksum, item.expected_checksum)
            self.assertEqual(item.transformation_status, "APPROVED")

    def test_unknown_run_date_is_an_error(self):
        with self.assertRaises(L.HandoverError):
            L.read_handover("2030-01-01", TMP / "transformed", TMP / "tlogs")

    def test_missing_csv_is_reported(self):
        copy_root = TMP / "missing_csv"
        shutil.copytree(TMP / "transformed", copy_root / "transformed")
        (copy_root / "transformed" / "fact_payment" / RUN_DATE /
         f"fact_payment_{RUN_DATE}.csv").unlink()
        h = L.read_handover(RUN_DATE, copy_root / "transformed", TMP / "tlogs")
        self.assertTrue(any("fact_payment: missing CSV" in p for p in h.problems))
        self.assertEqual(L.check_handover_completeness(h).status, "FAIL")


class PreLoadOfflineTests(unittest.TestCase):

    def test_handover_must_be_approved(self):
        for status in ("MANUAL REVIEW", "REJECTED", None):
            with self.subTest(status=status):
                h = set_status(handover(), "fact_billing", status)
                self.assertEqual(L.check_handover_status(h, None).status, "FAIL")

    def test_manual_review_handover_can_be_released(self):
        h = set_status(handover(), "fact_billing", "MANUAL REVIEW")
        self.assertEqual(L.check_handover_status(h, "Reviewer").status, "PASS")
        h = set_status(handover(), "fact_billing", "REJECTED")
        self.assertEqual(L.check_handover_status(h, "Reviewer").status, "FAIL")

    def test_record_count_and_checksum_must_match(self):
        h = handover()
        h.tables["fact_payment"].records_output += 1
        h.tables["dim_purok"].checksum = "0" * 64
        self.assertEqual(L.check_record_count(h).status, "FAIL")
        self.assertEqual(L.check_file_integrity(h).status, "FAIL")

    def test_main_database_is_never_a_target(self):
        main_dsn = "postgresql://postgres.mainref:pw@aws-0.pooler.supabase.com:5432/postgres"
        with mock.patch.dict(os.environ, {"WATERWISE_DB_URL": main_dsn}):
            check, conn = L.check_target_connection(
                "postgresql://etl_loader.mainref:pw@aws-0.pooler.supabase.com:5432/postgres")
        self.assertEqual(check.status, "FAIL")
        self.assertIsNone(conn)

    def test_duplicate_batch_and_force_reload(self):
        loaded = ((True,), (1, "2026-03-10 03:00+08"))
        self.assertEqual(L.check_duplicate_batch(handover(), StubCursor(*loaded), False).status,
                         "FAIL")
        self.assertEqual(L.check_duplicate_batch(handover(), StubCursor(*loaded), True).status,
                         "PASS")

    def test_storage_thresholds(self):
        mb = 1024 * 1024
        space, warning = L.check_storage(StubCursor((410 * mb,)))
        self.assertEqual((space.status, warning.status), ("PASS", "WARNING"))
        space, _ = L.check_storage(StubCursor((480 * mb,)))
        self.assertEqual(space.status, "FAIL")

    def test_decision_rules(self):
        ok = L.Check("a", "t", "c", L.CRITICAL)
        warn = L.Check("b", "t", "c", L.NON_CRITICAL, "WARNING")
        fail = L.Check("c", "t", "c", L.CRITICAL, "FAIL")
        self.assertEqual(L.decide([ok]), "APPROVED")
        self.assertEqual(L.decide([ok, warn]), "MANUAL REVIEW")
        self.assertEqual(L.decide([ok, warn, fail]), "REJECTED")


class ValueAndMaskingTests(unittest.TestCase):

    def column(self, data_type, nullable=True, max_length=None, precision=None, scale=None):
        return L.ColumnInfo("c", data_type, max_length, precision, scale, nullable)

    def test_value_problems(self):
        cases = [
            ("", self.column("integer", nullable=False), "NOT_NULL"),
            ("abc", self.column("integer"), "TYPE"),
            ("40000", self.column("smallint"), "TYPE"),
            ("12345678901.00", self.column("numeric", precision=12, scale=2), "TYPE"),
            ("2026-02-30", self.column("date"), "TYPE"),
            ("maybe", self.column("boolean"), "TYPE"),
            ("x" * 17, self.column("character", max_length=16), "TYPE"),
        ]
        for value, column, prefix in cases:
            with self.subTest(value=value[:20], type=column.data_type):
                self.assertTrue((L.value_problem(value, column) or "").startswith(prefix))
        self.assertIsNone(L.value_problem("", self.column("integer")))
        self.assertIsNone(L.value_problem("123.45", self.column("numeric", precision=12,
                                                                  scale=2)))
        self.assertIsNone(L.value_problem("2026-03-10T01:00:00+08:00",
                                          self.column("timestamp with time zone")))

    def test_password_is_masked(self):
        dsn = "postgresql://etl_loader.ref:S3cr%40t@host:5432/postgres"
        self.assertNotIn("S3cr", L.mask("failed for S3cr%40t and S3cr@t", dsn))

    def test_pii_is_masked(self):
        self.assertEqual(L.mask_pii("email", "juan.delacruz@mail.com"), "j***@mail.com")
        self.assertEqual(L.mask_pii("contact_number", "09171234567"), "09***4567")


# ---------------------------------------------------------------------------
# Warehouse tests (never commit)
# ---------------------------------------------------------------------------

class NoCommit:
    """Wraps a real connection: commit() records what would be committed, then rolls back."""

    snapshots: list[dict] = []

    def __init__(self, conn):
        object.__setattr__(self, "_conn", conn)

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def __setattr__(self, name, value):
        setattr(self._conn, name, value)

    def commit(self):
        with self._conn.cursor() as cur:
            cur.execute("SELECT table_name, status, error_code FROM warehouse.load_log "
                        "WHERE logged_at = now() ORDER BY load_log_id")
            log_rows = cur.fetchall()
            cur.execute("SELECT " + " + ".join(
                f"(SELECT count(*) FROM warehouse.{t} WHERE loaded_at = now())"
                for t in L.LOAD_ORDER))
            data_rows = cur.fetchone()[0]
        NoCommit.snapshots.append({"log": log_rows, "data_rows": data_rows})
        self._conn.rollback()


@unittest.skipUnless(os.environ.get("WAREHOUSE_DB_URL") and L.psycopg2,
                     "WAREHOUSE_DB_URL not set: warehouse tests skipped")
class WarehouseTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dsn = L.get_warehouse_dsn()
        cls.observer = _REAL_CONNECT(cls.dsn)      # sees committed data only
        cls.observer.autocommit = True
        cls.patcher = mock.patch.object(L, "connect_warehouse",
                                        lambda dsn: NoCommit(_REAL_CONNECT(dsn)))
        cls.patcher.start()

    @classmethod
    def tearDownClass(cls):
        cls.patcher.stop()
        cls.observer.close()

    def setUp(self):
        NoCommit.snapshots.clear()
        self.out = TMP / f"out_{self._testMethodName}"
        self.before = self.counts()

    def tearDown(self):
        self.assertEqual(self.counts(), self.before, "the warehouse must be unchanged")

    def counts(self):
        with self.observer.cursor() as cur:
            cur.execute(" UNION ALL ".join(
                f"SELECT '{t}', count(*) FROM warehouse.{t}"
                for t in (*L.LOAD_ORDER, "load_log")))
            return dict(cur.fetchall())

    def run_main(self, *extra, handover_override=None):
        args = ["--run-date", RUN_DATE,
                "--transformed-root", str(TMP / "transformed"),
                "--transformation-log-root", str(TMP / "tlogs"),
                "--log-root", str(self.out / "logs"),
                "--quarantine-root", str(self.out / "quarantine"), *extra]
        patches = [mock.patch.object(L, "read_handover", lambda *a, **k: handover_override)] \
            if handover_override else []
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            return L.main(args)

    def test_rerun_creates_no_duplicates_and_keeps_sums(self):
        h = handover()
        conn = L.connect_warehouse(self.dsn)
        try:
            first = L.load_batch(conn, h)
            after_first = self.counts_in(conn)
            sums_first = self.sums_in(conn, h.batch_id)
            second = L.load_batch(conn, h)
            self.assertTrue(all(r.inserted == r.received for r in first.values()))
            self.assertTrue(all(r.inserted == 0 and r.updated == r.received
                                for r in second.values()))
            self.assertEqual(self.counts_in(conn), after_first)
            self.assertEqual(self.sums_in(conn, h.batch_id), sums_first)
            expected = {t: sum((Decimal(r[c]) for r in h.tables[t].rows), Decimal(0))
                        for t, c in AMOUNTS.items()}
            self.assertEqual(sums_first, expected)
        finally:
            conn.rollback()
            conn.close()

    def test_failure_midway_leaves_warehouse_unchanged_and_logs_failed(self):
        real_load_table = L.load_table

        def fail_on_billing(cur, item, batch_id):
            if item.table == "fact_billing":
                raise RuntimeError("simulated failure while loading fact_billing")
            return real_load_table(cur, item, batch_id)

        with mock.patch.object(L, "load_table", fail_on_billing):
            rc = self.run_main()
        self.assertEqual(rc, 1)
        self.assertEqual(len(NoCommit.snapshots), 1, "only the separate load_log write")
        snapshot = NoCommit.snapshots[0]
        self.assertEqual(snapshot["data_rows"], 0)
        self.assertEqual([row[1] for row in snapshot["log"]], ["FAILED"] * 5)
        self.assertEqual(snapshot["log"][0][2], "LOAD_ERROR")

    def test_handover_not_approved_is_refused(self):
        rc = self.run_main(handover_override=set_status(handover(), "fact_billing",
                                                        "REJECTED"))
        self.assertEqual(rc, 1)
        snapshot = NoCommit.snapshots[0]
        self.assertEqual(snapshot["data_rows"], 0)
        self.assertEqual(snapshot["log"][0][1:], ("FAILED", "PRE_LOAD_HANDOVER_STATUS"))

    def test_approved_batch_commits_data_and_log_together(self):
        rc = self.run_main()
        self.assertEqual(rc, 0)
        self.assertEqual(len(NoCommit.snapshots), 1)
        snapshot = NoCommit.snapshots[0]
        self.assertEqual(snapshot["data_rows"], sum(t.row_count
                                                    for t in handover().tables.values()))
        self.assertEqual([row[1] for row in snapshot["log"]], ["SUCCESS"] * 5)

    def test_dry_run_writes_nothing(self):
        rc = self.run_main("--dry-run")
        self.assertEqual(rc, 0)
        self.assertEqual(NoCommit.snapshots, [], "dry run must not commit anything")
        self.assertFalse(self.out.exists(), "dry run must not write log or quarantine files")

    # helpers -----------------------------------------------------------------

    @staticmethod
    def counts_in(conn):
        with conn.cursor() as cur:
            cur.execute(" UNION ALL ".join(f"SELECT '{t}', count(*) FROM warehouse.{t}"
                                           for t in L.LOAD_ORDER))
            return dict(cur.fetchall())

    @staticmethod
    def sums_in(conn, batch_id):
        out = {}
        with conn.cursor() as cur:
            for table, column in AMOUNTS.items():
                cur.execute(f"SELECT coalesce(sum({column}), 0) FROM warehouse.{table} "
                            "WHERE load_batch_id = %s", (batch_id,))
                out[table] = cur.fetchone()[0]
        return out


if __name__ == "__main__":
    unittest.main()

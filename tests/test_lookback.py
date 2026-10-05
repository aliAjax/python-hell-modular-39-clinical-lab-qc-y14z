import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from src.domain import Actor, PermissionDenied, ValidationError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class LookbackTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "lookback.db"),
            RuleEngine(),
        )
        self.supervisor = Actor("qc-supervisor", "supervisor")
        self.other = Actor("qc-other", "supervisor")

    def tearDown(self):
        self.tmp.cleanup()

    def _base(self):
        assay = self.service.create(
            self.supervisor,
            "assay",
            {"name": "Glucose", "unit": "mmol/L", "allowed_low": 3.9, "allowed_high": 6.1},
        )
        lot = self.service.create(
            self.supervisor,
            "qc_lot",
            {"assay_id": assay["id"], "lot_no": "LOT-1", "target": 5.0, "sd": 0.1, "expires_at": "2099-01-01"},
        )
        lot = self.service.transition(self.supervisor, lot["id"], "activate", {"activated_by": "qc-1"})
        instrument = self.service.create(
            self.supervisor,
            "instrument",
            {"name": "Analyzer A", "serial": "A-100", "calibration_due": "2099-01-01"},
        )
        return assay, lot, instrument

    def _qc_run(self, assay, lot, instrument, value, at):
        run = self.service.create(
            self.supervisor,
            "qc_run",
            {
                "assay_id": assay["id"],
                "qc_lot_id": lot["id"],
                "instrument_id": instrument["id"],
                "value": value,
                "run_at": at,
            },
        )
        return self.service.transition(self.supervisor, run["id"], "evaluate", {"evaluated_by": "qc-1"})

    def _batch(self, assay, instrument, run, at, count=10):
        batch = self.service.create(
            self.supervisor,
            "result_batch",
            {
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "qc_run_id": run["id"],
                "run_at": at,
                "patient_count": count,
            },
        )
        return self.service.transition(self.supervisor, batch["id"], "release", {"reviewer_id": "qc-2"})

    def test_lookback_finds_batches_between_last_pass_and_failure(self):
        assay, lot, instrument = self._base()
        self._qc_run(assay, lot, instrument, 5.02, "2026-09-27T08:00:00Z")
        last_pass = self._qc_run(assay, lot, instrument, 4.98, "2026-09-27T12:00:00Z")
        before = self._batch(assay, instrument, last_pass, "2026-09-27T12:05:00Z")
        after = self._batch(assay, instrument, last_pass, "2026-09-27T12:06:00Z")
        failed = self._qc_run(assay, lot, instrument, 9.5, "2026-09-27T16:00:00Z")
        lookback = self.service.create(
            self.supervisor,
            "qc_lookback",
            {"instrument_id": instrument["id"], "trigger_run_id": failed["id"]},
        )
        self.assertEqual(lookback["status"], "open")
        self.assertEqual(lookback["data"]["from_run_id"], last_pass["id"])
        self.assertEqual(lookback["data"]["window_batches"], [before["id"], after["id"]])

    def test_qualified_batch_keeps_release(self):
        assay, lot, instrument = self._base()
        last_pass = self._qc_run(assay, lot, instrument, 5.02, "2026-09-27T08:00:00Z")
        batch = self._batch(assay, instrument, last_pass, "2026-09-27T08:05:00Z")
        failed = self._qc_run(assay, lot, instrument, 9.5, "2026-09-27T10:00:00Z")
        lookback = self.service.create(
            self.supervisor,
            "qc_lookback",
            {"instrument_id": instrument["id"], "trigger_run_id": failed["id"]},
        )
        lookback = self.service.transition(self.supervisor, lookback["id"], "process", {})
        self.assertEqual(lookback["status"], "completed")
        self.assertEqual(lookback["data"]["processed"][batch["id"]], "kept")
        self.assertEqual(self.service.get(batch["id"])["status"], "released")

    def test_affected_batch_marked_pending_recall(self):
        assay, lot, instrument = self._base()
        run = self._qc_run(assay, lot, instrument, 5.02, "2026-09-27T08:00:00Z")
        batch = self._batch(assay, instrument, run, "2026-09-27T08:05:00Z")
        # The QC run the batch was released against is later corrected and rejected.
        self.service.transition(self.supervisor, run["id"], "correct", {"reason": "drift", "value": 5.02})
        rejected = self._qc_run(assay, lot, instrument, 9.5, "2026-09-27T09:00:00Z")
        self.assertEqual(rejected["status"], "rejected")
        lookback = self.service.create(
            self.supervisor,
            "qc_lookback",
            {"instrument_id": instrument["id"], "trigger_run_id": rejected["id"]},
        )
        lookback = self.service.transition(self.supervisor, lookback["id"], "process", {})
        self.assertEqual(lookback["data"]["processed"][batch["id"]], "recalled")
        self.assertEqual(self.service.get(batch["id"])["status"], "pending_recall")

    def test_concurrent_lookbacks_merge_into_one_scope(self):
        assay, lot, instrument = self._base()
        last_pass = self._qc_run(assay, lot, instrument, 5.02, "2026-09-27T08:00:00Z")
        batch = self._batch(assay, instrument, last_pass, "2026-09-27T08:05:00Z")
        first_fail = self._qc_run(assay, lot, instrument, 9.5, "2026-09-27T10:00:00Z")
        second_fail = self._qc_run(assay, lot, instrument, 9.6, "2026-09-27T11:00:00Z")
        first = self.service.create(
            self.supervisor,
            "qc_lookback",
            {"instrument_id": instrument["id"], "trigger_run_id": first_fail["id"]},
        )
        second = self.service.create(
            self.other,
            "qc_lookback",
            {"instrument_id": instrument["id"], "trigger_run_id": second_fail["id"]},
        )
        self.assertEqual(first["id"], second["id"])
        merged = self.service.get(first["id"])
        self.assertEqual(merged["data"]["window_batches"], [batch["id"]])
        self.assertEqual(set(merged["data"]["reviewers"]), {self.supervisor.user_id, self.other.user_id})
        # The late reviewer processes and completes; re-processing is a no-op.
        done = self.service.transition(self.other, merged["id"], "process", {})
        self.assertEqual(done["status"], "completed")
        again = self.service.transition(self.other, merged["id"], "process", {})
        self.assertEqual(again["status"], "completed")

    def test_resume_does_not_reprocess_batches(self):
        assay, lot, instrument = self._base()
        last_pass = self._qc_run(assay, lot, instrument, 5.02, "2026-09-27T08:00:00Z")
        batches = [self._batch(assay, instrument, last_pass, "2026-09-27T08:0%d:00Z" % (5 + i)) for i in range(3)]
        failed = self._qc_run(assay, lot, instrument, 9.5, "2026-09-27T10:00:00Z")
        lookback = self.service.create(
            self.supervisor,
            "qc_lookback",
            {"instrument_id": instrument["id"], "trigger_run_id": failed["id"]},
        )
        # Simulate a write failure after the first batch: only its checkpoint is durable.
        self.service.repository.apply_lookback_review(
            lookback["id"], batches[0]["id"], "kept", self.supervisor.user_id, self.supervisor.role, False
        )
        before = self.service.get(batches[0]["id"])
        lookback = self.service.transition(self.supervisor, lookback["id"], "process", {})
        after = self.service.get(batches[0]["id"])
        self.assertEqual(before["version"], after["version"], "already-processed batch must not be re-modified")
        self.assertEqual(lookback["status"], "completed")
        self.assertEqual(
            lookback["data"]["processed"],
            {batches[0]["id"]: "kept", batches[1]["id"]: "kept", batches[2]["id"]: "kept"},
        )

    def test_legacy_data_located_by_compatibility(self):
        assay, lot, instrument = self._base()
        # Pre-existing legacy data: qc runs and released batches without timestamps.
        last_pass = self.service.repository.create_entity(
            str(uuid4()),
            "qc_run",
            "accepted",
            {"assay_id": assay["id"], "qc_lot_id": lot["id"], "instrument_id": instrument["id"], "value": 5.0},
            self.supervisor.user_id,
        )
        batch = self.service.repository.create_entity(
            str(uuid4()),
            "result_batch",
            "released",
            {
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "qc_run_id": last_pass["id"],
                "patient_count": 5,
            },
            self.supervisor.user_id,
        )
        failed = self.service.repository.create_entity(
            str(uuid4()),
            "qc_run",
            "rejected",
            {"assay_id": assay["id"], "qc_lot_id": lot["id"], "instrument_id": instrument["id"], "value": 9.5},
            self.supervisor.user_id,
        )
        lookback = self.service.create(
            self.supervisor,
            "qc_lookback",
            {"instrument_id": instrument["id"], "trigger_run_id": failed["id"]},
        )
        self.assertTrue(lookback["data"]["legacy"])
        self.assertIn(batch["id"], lookback["data"]["window_batches"])
        lookback = self.service.transition(self.supervisor, lookback["id"], "process", {})
        self.assertEqual(lookback["status"], "completed")
        self.assertEqual(self.service.get(batch["id"])["status"], "released")

    def test_process_requires_supervisor(self):
        assay, lot, instrument = self._base()
        last_pass = self._qc_run(assay, lot, instrument, 5.02, "2026-09-27T08:00:00Z")
        self._batch(assay, instrument, last_pass, "2026-09-27T08:05:00Z")
        failed = self._qc_run(assay, lot, instrument, 9.5, "2026-09-27T10:00:00Z")
        lookback = self.service.create(
            self.supervisor,
            "qc_lookback",
            {"instrument_id": instrument["id"], "trigger_run_id": failed["id"]},
        )
        with self.assertRaises(PermissionDenied):
            self.service.transition(Actor("viewer", "viewer"), lookback["id"], "process", {})

    def test_trigger_must_be_rejected(self):
        assay, lot, instrument = self._base()
        accepted = self._qc_run(assay, lot, instrument, 5.02, "2026-09-27T08:00:00Z")
        with self.assertRaises(ValidationError):
            self.service.create(
                self.supervisor,
                "qc_lookback",
                {"instrument_id": instrument["id"], "trigger_run_id": accepted["id"]},
            )

    def test_empty_window_completes(self):
        assay, lot, instrument = self._base()
        failed = self._qc_run(assay, lot, instrument, 9.5, "2026-09-27T10:00:00Z")
        lookback = self.service.create(
            self.supervisor,
            "qc_lookback",
            {"instrument_id": instrument["id"], "trigger_run_id": failed["id"]},
        )
        self.assertEqual(lookback["data"]["window_batches"], [])
        lookback = self.service.transition(self.supervisor, lookback["id"], "process", {})
        self.assertEqual(lookback["status"], "completed")


if __name__ == "__main__":
    unittest.main()

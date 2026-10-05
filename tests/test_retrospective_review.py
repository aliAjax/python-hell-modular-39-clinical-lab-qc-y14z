import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from src.domain import Actor, ValidationError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class RetrospectiveReviewTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repository = SQLiteRepository(Path(self.tmp.name) / "review.db")
        self.service = DomainService(self.repository, RuleEngine())
        self.supervisor = Actor("qc-supervisor", "supervisor")
        self.late_supervisor = Actor("late-supervisor", "supervisor")

    def tearDown(self):
        self.tmp.cleanup()

    def _instrument_assay_lot(self, name="Glucose", lot_no="LOT-1"):
        assay = self.service.create(
            self.supervisor,
            "assay",
            {"name": name, "unit": "mmol/L", "allowed_low": 0, "allowed_high": 10},
        )
        lot = self.service.create(
            self.supervisor,
            "qc_lot",
            {"assay_id": assay["id"], "lot_no": lot_no, "target": 5, "sd": 0.1, "expires_at": "2099"},
        )
        lot = self.service.transition(self.supervisor, lot["id"], "activate", {"activated_by": "a"})
        instrument = self.service.create(
            self.supervisor,
            "instrument",
            {"name": "Analyzer", "serial": "S-1", "calibration_due": "2099"},
        )
        return assay, lot, instrument

    def _qc_run(self, assay, lot, instrument, value, run_at):
        run = self.service.create(
            self.supervisor,
            "qc_run",
            {
                "assay_id": assay["id"],
                "qc_lot_id": lot["id"],
                "instrument_id": instrument["id"],
                "value": value,
                "run_at": run_at,
            },
        )
        return self.service.transition(
            self.supervisor, run["id"], "evaluate", {"evaluated_by": "a"}
        )

    def _release_batch(self, assay, instrument, accepted_run, batch_id, run_at, patient_count=1):
        batch = self.service.create(
            self.supervisor,
            "result_batch",
            {
                "id": batch_id,
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "qc_run_id": accepted_run["id"],
                "run_at": run_at,
                "patient_count": patient_count,
            },
        )
        return self.service.transition(
            self.supervisor, batch["id"], "release", {"reviewer_id": "r"}
        )

    def test_failed_qc_identifies_released_batches_between_accepted_and_failed_runs(self):
        assay, lot, instrument = self._instrument_assay_lot()
        accepted = self._qc_run(assay, lot, instrument, 5.0, "2026-10-01T08:00:00Z")
        self._release_batch(assay, instrument, accepted, "before", "2026-10-01T07:59:00Z")
        self._release_batch(assay, instrument, accepted, "first", "2026-10-01T09:00:00Z")
        self._release_batch(assay, instrument, accepted, "second", "2026-10-01T10:00:00Z")
        self._qc_run(assay, lot, instrument, 9.0, "2026-10-01T11:00:00Z")

        reviews = self.service.list("qc_review")
        self.assertEqual(1, len(reviews))
        review = reviews[0]
        self.assertEqual(["first", "second"], review["data"]["candidate_batch_ids"])

        review = self.service.review_batches(
            self.supervisor,
            review["id"],
            {"items": [
                {"batch_id": "first", "outcome": "retain"},
                {"batch_id": "second", "outcome": "recall"},
            ]},
        )
        self.assertEqual("completed", review["status"])
        self.assertEqual("released", self.service.get("first")["status"])
        self.assertEqual("recall_pending", self.service.get("second")["status"])
        self.assertEqual("released", self.service.get("before")["status"])

    def test_previous_accepted_run_is_matched_by_instrument_and_assay_across_qc_lots(self):
        assay, first_lot, instrument = self._instrument_assay_lot(lot_no="LOT-1")
        accepted = self._qc_run(assay, first_lot, instrument, 5.0, "2026-10-01T08:00:00Z")
        second_lot = self.service.create(
            self.supervisor,
            "qc_lot",
            {"assay_id": assay["id"], "lot_no": "LOT-2", "target": 5, "sd": 0.1, "expires_at": "2099"},
        )
        second_lot = self.service.transition(
            self.supervisor,
            second_lot["id"],
            "switch_in",
            {"previous_lot_id": first_lot["id"], "switched_at": "2026-10-01T08:30:00Z"},
        )
        self._release_batch(assay, instrument, accepted, "middle", "2026-10-01T09:00:00Z")
        self._qc_run(assay, second_lot, instrument, 9.0, "2026-10-01T10:00:00Z")

        review = self.service.list("qc_review")[0]
        self.assertEqual("2026-10-01T08:00:00Z", review["data"]["start_at"])
        self.assertEqual(["middle"], review["data"]["candidate_batch_ids"])

    def test_concurrent_supervisors_share_one_merged_scope_and_late_review_continues(self):
        assay, lot, instrument = self._instrument_assay_lot()
        accepted = self._qc_run(assay, lot, instrument, 5.0, "2026-10-01T08:00:00Z")
        self._release_batch(assay, instrument, accepted, "one", "2026-10-01T08:30:00Z")
        self._release_batch(assay, instrument, accepted, "two", "2026-10-01T09:00:00Z")

        def submit(actor, review_id):
            payload = {
                "id": review_id,
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "start_at": "2026-10-01T08:00:00Z",
                "end_at": "2026-10-01T10:00:00Z",
            }
            return self.service.create(actor, "qc_review", payload)

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(
                lambda args: submit(args[0], args[1]),
                [
                    (self.supervisor, "manual-review-one"),
                    (self.late_supervisor, "manual-review-two"),
                ],
            ))

        target_ids = {review["id"] for review in results}
        self.assertEqual(1, len(target_ids))
        target_id = next(iter(target_ids))
        target = self.service.get(target_id)
        self.assertEqual(["one", "two"], target["data"]["candidate_batch_ids"])
        self.assertEqual({"qc-supervisor", "late-supervisor"}, set(target["data"]["submitted_by"]))
        self.assertEqual(1, len(self.service.list("qc_review", status="processing")))
        self.assertEqual(0, len(self.service.list("qc_review", status="completed")))
        self.assertEqual(1, len(self.service.list("qc_review", status="merged")))

        completed = self.service.review_batches(
            self.late_supervisor,
            target_id,
            {"items": [
                {"batch_id": "one", "outcome": "retain"},
                {"batch_id": "two", "outcome": "recall"},
            ]},
        )
        self.assertEqual("completed", completed["status"])
        self.assertEqual("released", self.service.get("one")["status"])
        self.assertEqual("recall_pending", self.service.get("two")["status"])

    def test_resume_after_write_failure_does_not_change_an_already_processed_batch_again(self):
        assay, lot, instrument = self._instrument_assay_lot()
        accepted = self._qc_run(assay, lot, instrument, 5.0, "2026-10-01T08:00:00Z")
        self._release_batch(assay, instrument, accepted, "one", "2026-10-01T08:30:00Z")
        self._release_batch(assay, instrument, accepted, "two", "2026-10-01T09:00:00Z")
        failed = self._qc_run(assay, lot, instrument, 9.0, "2026-10-01T10:00:00Z")
        review_id = self.service.list("qc_review")[0]["id"]
        self.assertEqual(failed["status"], "rejected")

        original = SQLiteRepository._process_one_review_item
        calls = {"count": 0}

        def flaky(repo, *args, **kwargs):
            calls["count"] += 1
            result = original(repo, *args, **kwargs)
            if calls["count"] == 1:
                raise RuntimeError("simulated write failure after checkpoint")
            return result

        decisions = {"items": [
            {"batch_id": "one", "outcome": "retain"},
            {"batch_id": "two", "outcome": "recall"},
        ]}
        with patch.object(SQLiteRepository, "_process_one_review_item", flaky):
            with self.assertRaises(RuntimeError):
                self.service.review_batches(self.supervisor, review_id, decisions)

        partial = self.service.get(review_id)
        self.assertEqual("retained", partial["data"]["items"][0]["status"])
        self.assertEqual("pending", partial["data"]["items"][1]["status"])
        before_retry_version = self.service.get("one")["version"]

        resumed = self.service.resume_qc_review(self.late_supervisor, review_id)
        self.assertEqual("completed", resumed["status"])
        self.assertEqual(before_retry_version, self.service.get("one")["version"])
        self.assertEqual("released", self.service.get("one")["status"])
        self.assertEqual("recall_pending", self.service.get("two")["status"])

        repeated = self.service.review_batches(
            self.supervisor,
            review_id,
            {"items": [
                {"batch_id": "one", "outcome": "retain"},
                {"batch_id": "two", "outcome": "recall"},
            ]},
        )
        self.assertEqual("completed", repeated["status"])
        self.assertEqual(before_retry_version, self.service.get("one")["version"])

        one_audits = [
            row for row in self.service.audit_log("one")
            if row["action"] == "review_retain"
        ]
        self.assertEqual(1, len(one_audits))

    def test_legacy_batch_without_basis_timestamp_is_located_by_instrument_compatibility(self):
        assay, lot, instrument = self._instrument_assay_lot()
        accepted = self._qc_run(assay, lot, instrument, 5.0, "2026-10-01T08:00:00Z")
        legacy = self._release_batch(
            assay, instrument, accepted, "legacy", "2026-10-01T08:30:00Z"
        )
        legacy_data = dict(legacy["data"])
        legacy_data.pop("run_at", None)
        self.repository.update_entity(legacy["id"], legacy["version"], "released", legacy_data)
        failed = self._qc_run(assay, lot, instrument, 9.0, "2026-10-01T09:00:00Z")

        other_assay, _, other_instrument = self._instrument_assay_lot("Other", "OTHER")
        other = self._release_batch(
            other_assay,
            other_instrument,
            accepted,
            "other-instrument",
            "2026-10-01T08:30:00Z",
        )
        other_data = dict(other["data"])
        other_data.pop("run_at", None)
        self.repository.update_entity(other["id"], other["version"], "released", other_data)

        review = self.service.create(
            self.supervisor,
            "qc_review",
            {
                "assay_id": assay["id"],
                "instrument_id": instrument["id"],
                "failed_qc_run_id": failed["id"],
            },
        )
        self.assertEqual(["legacy"], review["data"]["candidate_batch_ids"])
        self.assertNotIn("other-instrument", review["data"]["candidate_batch_ids"])

    def test_explicit_window_requires_start_and_end_without_failed_run(self):
        assay, lot, instrument = self._instrument_assay_lot()
        with self.assertRaises(ValidationError):
            self.service.create(
                self.supervisor,
                "qc_review",
                {"assay_id": assay["id"], "instrument_id": instrument["id"], "start_at": "2026-10-01T08:00:00Z"},
            )


if __name__ == "__main__":
    unittest.main()

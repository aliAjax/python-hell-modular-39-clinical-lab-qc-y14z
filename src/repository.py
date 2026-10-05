import json
import sqlite3
from datetime import datetime, timezone

from .domain import ConflictError, InvalidTransition, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
            """)

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    @staticmethod
    def _append_audit_connection(connection, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        connection.execute(
            "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entity_id,
                actor_id,
                actor_role,
                action,
                from_status,
                to_status,
                json.dumps(detail, ensure_ascii=False, sort_keys=True),
                utcnow(),
            ),
        )

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            self._append_audit_connection(
                connection, entity_id, actor_id, actor_role, action, from_status, to_status, detail
            )

    def create_qc_review(self, review_id, review_data, actor):
        from .rules import intervals_overlap, merge_intervals

        now = utcnow()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            instrument_id = review_data.get("instrument_id")
            assay_id = review_data.get("assay_id")
            start_at = review_data.get("start_at")
            end_at = review_data.get("end_at")
            existing_rows = connection.execute(
                "SELECT * FROM entities WHERE kind = 'qc_review' AND status != 'merged' ORDER BY created_at, id"
            ).fetchall()
            duplicates = []
            for candidate_row in existing_rows:
                candidate_data = json.loads(candidate_row["data"])
                if candidate_data.get("instrument_id") != instrument_id:
                    continue
                if candidate_data.get("assay_id") != assay_id:
                    continue
                same_failed_run = (
                    review_data.get("failed_qc_run_id")
                    and review_data.get("failed_qc_run_id")
                    == candidate_data.get("failed_qc_run_id")
                )
                if same_failed_run or intervals_overlap(
                    start_at,
                    end_at,
                    candidate_data.get("start_at"),
                    candidate_data.get("end_at"),
                ):
                    duplicates.append((candidate_row, candidate_data))

            if duplicates:
                target_row, target_data = min(
                    duplicates, key=lambda item: (item[0]["created_at"], item[0]["id"])
                )
                target_id = target_row["id"]
                merged_start, merged_end = merge_intervals(
                    target_data.get("start_at"),
                    target_data.get("end_at"),
                    start_at,
                    end_at,
                )
                self._populate_qc_review_scope(
                    connection, target_data, instrument_id, assay_id, merged_start, merged_end
                )
                source_ids = list(target_data.get("failed_qc_run_ids") or [])
                for value in (target_data.get("failed_qc_run_id"), review_data.get("failed_qc_run_id")):
                    if value and value not in source_ids:
                        source_ids.append(value)
                target_data["failed_qc_run_ids"] = source_ids
                submitted_by = list(target_data.get("submitted_by") or [])
                for value in (target_data.get("opened_by"), review_data.get("opened_by"), actor.user_id):
                    if value and value not in submitted_by:
                        submitted_by.append(value)
                target_data["submitted_by"] = submitted_by
                pending = any(item.get("status") == "pending" for item in target_data.get("items") or [])
                target_status = "processing" if pending else "completed"
                connection.execute(
                    "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? WHERE id = ?",
                    (
                        target_status,
                        json.dumps(target_data, ensure_ascii=False, sort_keys=True),
                        now,
                        target_id,
                    ),
                )
                self._append_audit_connection(
                    connection,
                    target_id,
                    actor.user_id,
                    actor.role,
                    "merge_review",
                    target_row["status"],
                    target_status,
                    {"new_review_id": review_id},
                )

                merged_data = dict(review_data)
                merged_data["merged_into"] = target_id
                payload = json.dumps(merged_data, ensure_ascii=False, sort_keys=True)
                connection.execute(
                    "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                    "VALUES (?, 'qc_review', 'merged', 1, ?, ?, ?, ?)",
                    (review_id, payload, actor.user_id, now, now),
                )
                self._append_audit_connection(
                    connection,
                    review_id,
                    actor.user_id,
                    actor.role,
                    "review_merged",
                    None,
                    "merged",
                    {"merged_into": target_id},
                )
            else:
                target_id = review_id
                target_data = dict(review_data)
                self._populate_qc_review_scope(
                    connection, target_data, instrument_id, assay_id, start_at, end_at
                )
                target_data["submitted_by"] = [actor.user_id]
                pending = any(item.get("status") == "pending" for item in target_data.get("items") or [])
                target_status = "processing" if pending else "completed"
                payload = json.dumps(target_data, ensure_ascii=False, sort_keys=True)
                connection.execute(
                    "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                    "VALUES (?, 'qc_review', ?, 1, ?, ?, ?, ?)",
                    (review_id, target_status, payload, actor.user_id, now, now),
                )
                self._append_audit_connection(
                    connection,
                    review_id,
                    actor.user_id,
                    actor.role,
                    "open_review",
                    None,
                    target_status,
                    {"candidate_batch_ids": target_data.get("candidate_batch_ids", [])},
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(target_id)

    def _populate_qc_review_scope(self, connection, review_data, instrument_id, assay_id, start_at, end_at):
        from .rules import retrospective_candidate_batches

        rows = connection.execute(
            "SELECT * FROM entities WHERE kind = 'result_batch' ORDER BY created_at, id"
        ).fetchall()
        batches = [self._entity_from_row(item) for item in rows]
        candidates = retrospective_candidate_batches(batches, instrument_id, assay_id, start_at, end_at)
        review_data["start_at"] = start_at
        review_data["end_at"] = end_at
        candidate_ids = [batch["id"] for batch in candidates]
        review_data["candidate_batch_ids"] = candidate_ids
        existing_items = list(review_data.get("items") or [])
        known = {item.get("batch_id"): item for item in existing_items}
        items = []
        for batch in candidates:
            item = known.get(batch["id"])
            if item is None:
                item = {"batch_id": batch["id"], "status": "pending"}
            items.append(item)
        # Keep processed historical items even if another concurrent release changed scope.
        for item in existing_items:
            if item.get("batch_id") not in candidate_ids:
                items.append(item)
        review_data["items"] = items

    def process_qc_review_items(self, review_id, actor, items, expected_version=None, resume_pending=False):
        review = self.get_entity(review_id)
        if not review:
            raise NotFoundError("entity not found: " + review_id)
        if review["kind"] != "qc_review":
            raise ValidationError("entity is not a QC review")
        if review["status"] == "merged":
            target = review["data"].get("merged_into")
            if target:
                return self.process_qc_review_items(target, actor, items, expected_version, resume_pending)
            raise ConflictError("review was merged without a target")
        if review["status"] not in ("processing", "completed"):
            raise InvalidTransition("cannot review batches from status %s" % review["status"])

        requested = []
        if resume_pending and not items:
            requested = [
                {"batch_id": item["batch_id"], "outcome": "recall_pending"}
                for item in review["data"].get("items") or []
                if item.get("status") == "pending"
            ]
        else:
            requested = list(items or [])

        current_version = int(review["version"])
        if expected_version is not None and current_version != int(expected_version):
            raise ConflictError(
                "version conflict: expected %s, found %s" % (expected_version, current_version)
            )

        result = review
        for request in requested:
            result = self._process_one_review_item(review_id, actor, request, current_version)
            current_version = int(result["version"])
        final = self.get_entity(review_id)
        if not requested and final["status"] != "completed":
            if resume_pending:
                return final
            raise ValidationError("review items are required")
        return final

    def _process_one_review_item(self, review_id, actor, request, expected_version):
        batch_id = request.get("batch_id") or request.get("result_batch_id")
        outcome = self._normalize_review_outcome(request.get("outcome") or request.get("decision"))
        reason = request.get("reason")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            review_row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (review_id,)
            ).fetchone()
            if not review_row:
                raise NotFoundError("entity not found: " + review_id)
            current_version = int(review_row["version"])
            if current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s" % (expected_version, current_version)
                )
            review_data = json.loads(review_row["data"])
            item = next((entry for entry in review_data.get("items") or [] if entry.get("batch_id") == batch_id), None)
            if not item:
                raise ValidationError("batch is not in this review scope: " + str(batch_id))
            if item.get("status") != "pending":
                connection.commit()
                return self.get_entity(review_id)

            batch_row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (batch_id,)
            ).fetchone()
            if not batch_row or batch_row["kind"] != "result_batch":
                raise NotFoundError("result batch not found: " + str(batch_id))
            batch_data = json.loads(batch_row["data"])
            if batch_data.get("assay_id") != review_data.get("assay_id"):
                raise ValidationError("result batch does not match review assay")
            if batch_data.get("instrument_id") != review_data.get("instrument_id"):
                raise ValidationError("result batch does not match review instrument")

            marker = self._review_marker(batch_data, review_id)
            now = utcnow()
            if marker and marker.get("outcome") == outcome:
                batch_changed = False
            elif batch_row["status"] == "released" or (
                outcome == "recall_pending" and batch_row["status"] == "recall_pending"
            ):
                marker = {
                    "review_id": review_id,
                    "outcome": outcome,
                    "reviewed_by": actor.user_id,
                    "reviewed_at": now,
                }
                if reason:
                    marker["reason"] = reason
                markers = list(batch_data.get("retrospective_reviews") or [])
                markers = [entry for entry in markers if entry.get("review_id") != review_id]
                markers.append(marker)
                batch_data["retrospective_reviews"] = markers
                next_batch_status = "recall_pending" if outcome == "recall_pending" else "released"
                if outcome == "recall_pending":
                    batch_data["recall_reason"] = reason or "failed quality control retrospective review"
                connection.execute(
                    "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? WHERE id = ?",
                    (next_batch_status, json.dumps(batch_data, ensure_ascii=False, sort_keys=True), now, batch_id),
                )
                batch_changed = True
            else:
                raise ConflictError("result batch must be released for retrospective review: " + batch_id)

            item.update({
                "status": outcome,
                "reviewed_by": actor.user_id,
                "reviewed_at": now,
            })
            if reason:
                item["reason"] = reason
            pending = any(entry.get("status") == "pending" for entry in review_data.get("items") or [])
            next_review_status = "processing" if pending else "completed"
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? WHERE id = ?",
                (
                    next_review_status,
                    json.dumps(review_data, ensure_ascii=False, sort_keys=True),
                    now,
                    review_id,
                ),
            )
            if batch_changed:
                self._append_audit_connection(
                    connection,
                    batch_id,
                    actor.user_id,
                    actor.role,
                    "review_retain" if outcome == "retained" else "review_recall",
                    batch_row["status"],
                    "released" if outcome == "retained" else "recall_pending",
                    {"qc_review_id": review_id, "reason": reason},
                )
            self._append_audit_connection(
                connection,
                review_id,
                actor.user_id,
                actor.role,
                "review_item",
                review_row["status"],
                next_review_status,
                {"batch_id": batch_id, "outcome": outcome},
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(review_id)

    @staticmethod
    def _review_marker(batch_data, review_id):
        for marker in batch_data.get("retrospective_reviews") or []:
            if marker.get("review_id") == review_id:
                return marker
        return None

    @staticmethod
    def _normalize_review_outcome(value):
        aliases = {
            "retain": "retained",
            "retained": "retained",
            "keep": "retained",
            "keep_release": "retained",
            "retain_release": "retained",
            "released": "retained",
            "pass": "retained",
            "passed": "retained",
            "acceptable": "retained",
            "recall": "recall_pending",
            "recall_pending": "recall_pending",
            "affected": "recall_pending",
            "fail": "recall_pending",
            "failed": "recall_pending",
        }
        outcome = aliases.get(str(value or "").lower())
        if not outcome:
            raise ValidationError("review outcome must be retain or recall")
        return outcome

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True

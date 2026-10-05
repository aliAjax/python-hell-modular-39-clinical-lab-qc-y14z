import json
import sqlite3
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


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

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
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

    def merge_open_lookback(self, instrument_id, assay_id, validated, actor_id):
        """Merge a new lookback scope into the open one for the same instrument.

        Returns the merged lookback, or None when no open lookback exists.
        The window is widened to cover both scopes, overlapping batches are
        de-duplicated, the checkpoint is preserved and the late reviewer is
        added. Runs in one transaction so concurrent submissions keep a
        single scope.
        """
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT * FROM entities WHERE kind = 'qc_lookback' AND status = 'open'"
            ).fetchall()
            row = None
            for candidate in rows:
                data = json.loads(candidate["data"])
                if data.get("instrument_id") == instrument_id and data.get("assay_id") == assay_id:
                    row = candidate
                    break
            if row is None:
                connection.rollback()
                return None
            data = json.loads(row["data"])
            if validated.get("from_run_at") and (
                not data.get("from_run_at") or str(validated["from_run_at"]) < str(data["from_run_at"])
            ):
                data["from_run_at"] = validated["from_run_at"]
                data["from_run_id"] = validated["from_run_id"]
            if validated.get("to_run_at") and (
                not data.get("to_run_at") or str(validated["to_run_at"]) > str(data["to_run_at"])
            ):
                data["to_run_at"] = validated["to_run_at"]
                data["trigger_run_id"] = validated["trigger_run_id"]
            seen = list(data.get("window_batches") or [])
            for batch_id in validated.get("window_batches") or []:
                if batch_id not in seen:
                    seen.append(batch_id)
            data["window_batches"] = seen
            reviewers = list(data.get("reviewers") or [])
            if actor_id not in reviewers:
                reviewers.append(actor_id)
            data["reviewers"] = reviewers
            data["legacy"] = bool(data.get("legacy")) or bool(validated.get("legacy"))
            payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
            connection.execute(
                "UPDATE entities SET data = ?, version = version + 1, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (payload, utcnow(), row["id"], row["version"]),
            )
            connection.commit()
            merged = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (row["id"],)
            ).fetchone()
            return self._entity_from_row(merged) if merged else None

    def apply_lookback_review(self, lookback_id, batch_id, outcome, actor_id, actor_role, recall):
        """Apply the review result for one batch and advance the checkpoint.

        The batch status change (when recalled) and the lookback checkpoint
        update commit together. Already-processed batches are a no-op, so a
        resumed run never re-modifies them.
        """
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (lookback_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("lookback not found: " + lookback_id)
            data = json.loads(row["data"])
            processed = dict(data.get("processed") or {})
            if batch_id in processed:
                connection.rollback()
                return self._entity_from_row(row)
            if recall:
                batch = connection.execute(
                    "SELECT * FROM entities WHERE id = ?", (batch_id,)
                ).fetchone()
                if batch and batch["status"] == "released":
                    batch_data = json.loads(batch["data"])
                    batch_data["recall"] = {
                        "lookback_id": lookback_id,
                        "actor_id": actor_id,
                        "at": utcnow(),
                    }
                    connection.execute(
                        "UPDATE entities SET status = 'pending_recall', version = version + 1, "
                        "data = ?, updated_at = ? WHERE id = ?",
                        (json.dumps(batch_data, ensure_ascii=False, sort_keys=True), utcnow(), batch_id),
                    )
                    connection.execute(
                        "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, "
                        "to_status, detail, created_at) VALUES (?, ?, ?, 'recall', 'released', "
                        "'pending_recall', ?, ?)",
                        (
                            batch_id,
                            actor_id,
                            actor_role,
                            json.dumps({"lookback_id": lookback_id, "outcome": outcome}, sort_keys=True),
                            utcnow(),
                        ),
                    )
            processed[batch_id] = outcome
            data["processed"] = processed
            window = list(data.get("window_batches") or [])
            completed = bool(window) and all(batch in processed for batch in window)
            new_status = "completed" if completed else row["status"]
            payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ?",
                (new_status, payload, utcnow(), lookback_id),
            )
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, "
                "to_status, detail, created_at) VALUES (?, ?, ?, 'process', ?, ?, ?, ?)",
                (
                    lookback_id,
                    actor_id,
                    actor_role,
                    row["status"],
                    new_status,
                    json.dumps({"batch_id": batch_id, "outcome": outcome}, sort_keys=True),
                    utcnow(),
                ),
            )
            connection.commit()
            updated = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (lookback_id,)
            ).fetchone()
            return self._entity_from_row(updated) if updated else None

    def complete_lookback(self, lookback_id):
        """Mark a lookback completed once every window batch has an outcome."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (lookback_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("lookback not found: " + lookback_id)
            data = json.loads(row["data"])
            processed = dict(data.get("processed") or {})
            window = list(data.get("window_batches") or [])
            if all(batch in processed for batch in window):
                connection.execute(
                    "UPDATE entities SET status = 'completed', version = version + 1, updated_at = ? "
                    "WHERE id = ?",
                    (utcnow(), lookback_id),
                )
            connection.commit()
            updated = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (lookback_id,)
            ).fetchone()
            return self._entity_from_row(updated) if updated else None

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True

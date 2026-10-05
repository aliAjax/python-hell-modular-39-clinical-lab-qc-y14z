from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, PermissionDenied
from .rules import RuleEngine


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        if kind == "qc_lookback":
            return self._create_lookback(actor, data, idempotency_key)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        validated = self.rules.validate_create(actor, kind, payload, self._lookup)
        if validated:
            payload.update(validated)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        if entity["kind"] == "qc_lookback" and action == "process":
            return self._process_lookback(actor, entity)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        return updated

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)

    def _create_lookback(self, actor, data, idempotency_key):
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        validated = self.rules.validate_create(actor, "qc_lookback", payload, self._lookup)
        merged = self.repository.merge_open_lookback(
            validated["instrument_id"], validated["assay_id"], validated, actor.user_id
        )
        if merged:
            self.audit.record(
                merged["id"],
                actor,
                "merge",
                merged["status"],
                merged["status"],
                {"trigger_run_id": validated["trigger_run_id"], "window_batches": len(validated["window_batches"])},
            )
            if idempotency_key:
                self.repository.save_idempotency(actor.user_id, idempotency_key, merged["id"])
            return merged
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        lookback_data = {
            "instrument_id": validated["instrument_id"],
            "assay_id": validated["assay_id"],
            "trigger_run_id": validated["trigger_run_id"],
            "from_run_id": validated["from_run_id"],
            "from_run_at": validated["from_run_at"],
            "to_run_at": validated["to_run_at"],
            "window_batches": validated["window_batches"],
            "processed": {},
            "reviewers": [actor.user_id],
            "legacy": validated["legacy"],
        }
        entity = self.repository.create_entity(
            entity_id, "qc_lookback", "open", lookback_data, actor.user_id
        )
        self.audit.record(
            entity_id,
            actor,
            "create",
            None,
            "open",
            {
                "instrument_id": lookback_data["instrument_id"],
                "assay_id": lookback_data["assay_id"],
                "trigger_run_id": lookback_data["trigger_run_id"],
                "window_batches": len(lookback_data["window_batches"]),
            },
        )
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def _process_lookback(self, actor, lookback):
        if actor.role not in ("supervisor", "admin"):
            raise PermissionDenied("role %s is not allowed here" % actor.role)
        if lookback["status"] == "completed":
            return lookback
        data = dict(lookback["data"])
        processed = dict(data.get("processed") or {})
        for batch_id in list(data.get("window_batches") or []):
            if batch_id in processed:
                continue
            batch = self.repository.get_entity(batch_id)
            if not batch or batch["kind"] != "result_batch":
                self.repository.apply_lookback_review(
                    lookback["id"], batch_id, "missing", actor.user_id, actor.role, False
                )
                processed[batch_id] = "missing"
                continue
            linked = self.repository.get_entity(batch["data"].get("qc_run_id"))
            if linked and linked["status"] == "accepted":
                outcome, recall = "kept", False
            elif batch["status"] == "released":
                outcome, recall = "recalled", True
            elif batch["status"] == "pending_recall":
                outcome, recall = "recalled", False
            else:
                outcome, recall = "skipped", False
            updated = self.repository.apply_lookback_review(
                lookback["id"], batch_id, outcome, actor.user_id, actor.role, recall
            )
            processed[batch_id] = outcome
            if updated:
                lookback = updated
        if lookback["status"] != "completed":
            lookback = self.repository.complete_lookback(lookback["id"]) or lookback
        return lookback

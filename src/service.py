from uuid import uuid4

from .audit import AuditTrail
from .domain import Actor, ConflictError, InvalidTransition, NotFoundError, PermissionDenied, ValidationError
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
        payload = dict(data or {})
        if kind == "qc_review":
            self._normalize_qc_review_payload(payload, actor)
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    if entity.get("status") == "merged":
                        target = self.repository.get_entity(entity["data"].get("merged_into"))
                        if target:
                            return target
                    return entity
        validated = self.rules.validate_create(actor, kind, payload, self._lookup)
        if validated:
            payload.update(validated)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        if kind == "qc_review":
            entity = self.repository.create_qc_review(entity_id, payload, actor)
        else:
            entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
            self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]

        if entity["kind"] == "qc_review":
            review_actions = {
                "review_batches",
                "review",
                "retrospective_review",
                "resume_review",
                "resume",
                "continue_review",
                "continue",
            }
            if action not in review_actions:
                raise InvalidTransition("unknown action %s for qc_review" % action)
            resume_actions = {"resume_review", "resume", "continue_review", "continue"}
            return self.review_batches(
                actor,
                entity_id,
                data or {},
                expected_version=expected_version,
                resume_pending=action in resume_actions,
            )

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
        if entity["kind"] == "qc_run" and action == "evaluate" and updated["status"] == "rejected":
            self.open_qc_review_for_run(updated)
        return updated

    @staticmethod
    def _normalize_qc_review_payload(payload, actor=None):
        aliases = {
            "failed_run_id": "failed_qc_run_id",
            "qc_run_id": "failed_qc_run_id",
            "run_id": "failed_qc_run_id",
            "rejected_qc_run_id": "failed_qc_run_id",
            "from_at": "start_at",
            "window_start": "start_at",
            "last_passed_at": "start_at",
            "last_accepted_at": "start_at",
            "to_at": "end_at",
            "window_end": "end_at",
            "failed_at": "end_at",
        }
        for old, new in aliases.items():
            if old in payload and new not in payload:
                payload[new] = payload[old]
        if actor is not None:
            payload["opened_by"] = actor.user_id

    def _create_qc_review_unchecked(self, actor, payload):
        validated = self.rules.validate_create(actor, "qc_review", payload, self._lookup)
        if validated:
            payload.update(validated)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        return self.repository.create_qc_review(entity_id, payload, actor)

    def open_qc_review_for_run(self, qc_run):
        actor = Actor("system-qc", "supervisor")
        payload = {
            "assay_id": qc_run["data"].get("assay_id"),
            "instrument_id": qc_run["data"].get("instrument_id"),
            "failed_qc_run_id": qc_run["id"],
            "opened_by": "system-qc",
            "trigger": "qc_evaluation",
        }
        self._normalize_qc_review_payload(payload, actor)
        return self._create_qc_review_unchecked(actor, payload)

    def review_batches(self, actor, review_id, data=None, expected_version=None, resume_pending=False):
        if actor.role not in ("supervisor", "admin"):
            raise PermissionDenied("role %s is not allowed here" % actor.role)
        payload = dict(data or {})
        raw_items = payload.get("items")
        if raw_items is None:
            raw_items = payload.get("batch_decisions")
        if raw_items is None and ("batch_id" in payload or "result_batch_id" in payload):
            raw_items = [payload]
        if raw_items is None:
            raw_items = []
        if not isinstance(raw_items, list):
            raise ValidationError("review items must be a list")
        normalized = []
        for item in raw_items:
            if not isinstance(item, dict):
                raise ValidationError("each review item must be an object")
            normalized_item = {
                "batch_id": item.get("batch_id") or item.get("result_batch_id") or item.get("id"),
                "outcome": item.get("outcome") or item.get("decision") or item.get("result"),
                "reason": item.get("reason", payload.get("reason")),
            }
            if not normalized_item["batch_id"]:
                raise ValidationError("batch_id is required for each review item")
            normalized.append(normalized_item)
        return self.repository.process_qc_review_items(
            review_id,
            actor,
            normalized,
            expected_version=expected_version,
            resume_pending=resume_pending,
        )

    def resume_qc_review(self, actor, review_id, expected_version=None):
        return self.review_batches(
            actor,
            review_id,
            {"items": []},
            expected_version=expected_version,
            resume_pending=True,
        )

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

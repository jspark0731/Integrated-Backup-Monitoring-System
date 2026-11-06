"""Extract the real nested RAW response and the two existing Collector shapes."""

from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from .models import Evidence, Issue, positive_id, source_of, timestamp, unwrap
from .normalize import normalize_name

ACTION_ID = "*policy action jobid"
REQUIRED = (ACTION_ID, "*policy name", "*policy workflow name", "group")


@dataclass
class Extraction:
    evidence: list[Evidence] = field(default_factory=list)
    issues: list[Issue] = field(default_factory=list)
    incomplete_sources: set[str] = field(default_factory=set)


def extract_raw(hit: dict[str, Any]) -> Extraction:
    result = Extraction()
    try:
        document, reference = unwrap(hit)
        source = source_of(document, raw=True)
    except ValueError as exc:
        result.issues.append(Issue(str(exc), details=deepcopy(hit)))
        return result
    payload = document.get("payload")
    if not isinstance(payload, dict):
        result.issues.append(Issue("invalid_payload", source, reference=reference))
        result.incomplete_sources.add(source)
        return result
    if document.get("ok") is False or payload.get("collection_status") in {"partial", "error"} or payload.get("endpoint_errors"):
        result.incomplete_sources.add(source)
        result.issues.append(Issue("partial_collection", source, reference=reference))
    raw = payload.get("raw")
    response = raw.get("backups") if isinstance(raw, dict) else None
    if isinstance(response, dict):
        response = response.get("backups")
    if not isinstance(response, list):
        result.issues.append(Issue("invalid_backup_response", source, reference=reference))
        result.incomplete_sources.add(source)
        return result
    for position, backup in enumerate(response):
        ref = reference | {"raw_document_id": document.get("raw_document_id"), "position": position}
        _extract_backup(backup, source, ref, timestamp(document.get("@timestamp")), result)
    return result


def _extract_backup(backup: Any, source: str, reference: dict, observed_at: Any, result: Extraction) -> None:
    if not isinstance(backup, dict):
        result.issues.append(Issue("invalid_backup", source, reference=reference, details=deepcopy(backup)))
        result.incomplete_sources.add(source)
        return
    attributes = backup.get("attributes")
    values: dict[str, list[str]] = defaultdict(list)
    errors: list[str] = []
    if not isinstance(attributes, list) or not attributes:
        errors.append("missing_or_invalid_attributes")
    else:
        for attribute in attributes:
            if not isinstance(attribute, dict) or not isinstance(attribute.get("key"), str) or not attribute["key"].strip():
                errors.append("invalid_attribute_key")
                continue
            key, items = attribute["key"], attribute.get("values")
            if not isinstance(items, list) or not items or any(not isinstance(v, str) or not v.strip() for v in items):
                errors.append("invalid_attribute_values")
                continue
            values[key].extend(items)
    ids = set(values.get(ACTION_ID, []))
    action_id = positive_id(next(iter(ids))) if len(ids) == 1 else None
    if not action_id:
        errors.append("missing_multiple_or_invalid_action_id")
    for key in REQUIRED[1:]:
        if key not in values:
            errors.append("missing_attribute:" + key)
        elif len(set(values[key])) != 1:
            # Multiple dimensions have no declared pairing; never take a product.
            errors.append("multiple_attribute_values:" + key)
    if errors:
        result.issues.append(Issue(";".join(sorted(set(errors))), source, action_id, deepcopy(reference), deepcopy(backup)))
        if not action_id:
            result.incomplete_sources.add(source)
        return
    try:
        policy = normalize_name(values[REQUIRED[1]][0], strip_suffix=True)
        workflow = normalize_name(values[REQUIRED[2]][0], strip_suffix=True)
        group = normalize_name(values["group"][0], strip_suffix=False)
    except ValueError as exc:
        result.issues.append(Issue(str(exc), source, action_id, deepcopy(reference), deepcopy(backup)))
        return
    identity = backup.get("id")
    backup_id = identity.strip() if isinstance(identity, str) and identity.strip() else None
    result.evidence.append(Evidence(
        source, action_id, backup_id, policy, workflow, group,
        tuple(deepcopy(attributes)), deepcopy(reference) | {"backup_id": backup_id}, observed_at,
        deepcopy(backup.get("saveTime")), deepcopy(backup.get("completionTime")),
    ))

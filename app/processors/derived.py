from __future__ import annotations

from typing import Any


def build_derived_documents(raw_document: dict[str, Any]) -> list[dict[str, Any]]:
    payload = raw_document.get("payload")
    if not isinstance(payload, dict):
        return []

    target_type = raw_document.get("target_type")
    if target_type == "Networker":
        return _networker_documents(raw_document, payload)
    if target_type == "i6000":
        return _i6000_documents(raw_document, payload)
    if target_type == "ZFS":
        return _zfs_documents(raw_document, payload)
    if target_type in {"DD", "DXi"}:
        return _device_documents(raw_document, payload)
    return []


def _networker_documents(raw_document: dict[str, Any], payload: dict[str, Any]) -> list[dict[str, Any]]:
    specs = (
        ("job", "jobs", ("job_id", "name")),
        ("client", "clients", ("client_name", "client_id")),
        ("policy", "policies", ("policy_name",)),
        ("workflow", "workflows", ("policy_name", "workflow_name")),
        ("monthly-report", "monthly_report", ("policy_name", "workflow_name", "month")),
    )
    return _record_documents(raw_document, payload, "networker", specs)


def _i6000_documents(raw_document: dict[str, Any], payload: dict[str, Any]) -> list[dict[str, Any]]:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    specs = (
        ("drive", "drives", ("serial_number", "name")),
        ("media", "media", ("barcode", "name")),
        ("partition", "partitions", ("name",)),
        ("robot", "robots", ("name",)),
    )
    return _summary_document(raw_document, "i6000", summary) + _record_documents(raw_document, summary, "i6000", specs)


def _zfs_documents(raw_document: dict[str, Any], payload: dict[str, Any]) -> list[dict[str, Any]]:
    specs = (
        ("pool", "pools", ("name",)),
        ("project", "projects", ("pool", "name")),
        ("filesystem", "filesystems", ("pool", "project", "name")),
        ("lun", "luns", ("pool", "project", "name", "id")),
        ("event", "alerts", ("timestamp", "summary")),
    )
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    return _summary_document(raw_document, "zfs", summary) + _record_documents(raw_document, payload, "zfs", specs)


def _device_documents(raw_document: dict[str, Any], payload: dict[str, Any]) -> list[dict[str, Any]]:
    solution = _solution(raw_document)
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    return _summary_document(raw_document, solution, summary)


def _summary_document(raw_document: dict[str, Any], solution: str, summary: dict[str, Any]) -> list[dict[str, Any]]:
    if not summary:
        return []
    return [
        _derived_document(
            raw_document,
            solution=solution,
            document_type="summary",
            record=summary,
            record_id=_first_non_empty(summary, ("device_name", "server", "name")) or raw_document.get("collector"),
        )
    ]


def _record_documents(
    raw_document: dict[str, Any],
    payload: dict[str, Any],
    solution: str,
    specs: tuple[tuple[str, str, tuple[str, ...]], ...],
) -> list[dict[str, Any]]:
    documents = []
    for document_type, payload_key, id_fields in specs:
        records = payload.get(payload_key)
        if not isinstance(records, list):
            continue
        for index, record in enumerate(records):
            if not isinstance(record, dict):
                continue
            documents.append(
                _derived_document(
                    raw_document,
                    solution=solution,
                    document_type=document_type,
                    record=record,
                    record_id=_record_id(record, id_fields, index),
                )
            )
    return documents


def _derived_document(
    raw_document: dict[str, Any],
    *,
    solution: str,
    document_type: str,
    record: dict[str, Any],
    record_id: Any,
) -> dict[str, Any]:
    collector = raw_document.get("collector")
    month = _month(raw_document.get("@timestamp"))
    return {
        "@timestamp": raw_document.get("@timestamp"),
        "collector": collector,
        "target_type": raw_document.get("target_type"),
        "solution": solution,
        "document_family": "derived",
        "document_type": document_type,
        "processing_mode": "elt",
        "source_raw_id": raw_document.get("_id") or raw_document.get("raw_document_id"),
        "record_id": str(record_id or "unknown"),
        "derived_id": f"{collector}:{document_type}:{record_id or 'unknown'}:{month}",
        "payload": record,
    }


def _record_id(record: dict[str, Any], fields: tuple[str, ...], fallback_index: int) -> str:
    values = [_string(record.get(field)) for field in fields]
    values = [value for value in values if value]
    if values:
        return ":".join(values)
    return str(fallback_index)


def _first_non_empty(record: dict[str, Any], fields: tuple[str, ...]) -> str:
    for field in fields:
        value = _string(record.get(field))
        if value:
            return value
    return ""


def _solution(raw_document: dict[str, Any]) -> str:
    value = raw_document.get("solution")
    if value:
        return str(value)
    aliases = {"DD": "dd", "DXi": "dxi", "Networker": "networker", "ZFS": "zfs", "i6000": "i6000"}
    return aliases.get(str(raw_document.get("target_type")), str(raw_document.get("target_type", "")).lower())


def _month(timestamp: Any) -> str:
    text = _string(timestamp)
    if len(text) >= 7:
        return text[:7]
    return "unknown"


def _string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()

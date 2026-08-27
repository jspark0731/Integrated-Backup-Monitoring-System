from __future__ import annotations

from datetime import datetime, timezone
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
    legacy_documents = _record_documents(raw_document, payload, "networker", specs)
    return legacy_documents + _networker_job_run_documents(raw_document, payload)


def _networker_job_run_documents(
    raw_document: dict[str, Any],
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        return []

    documents = []
    for job in jobs:
        if not isinstance(job, dict):
            continue
        record = _job_run_record(job)
        if record is None:
            continue

        job_id = _string(record.get("job_id"))
        document = _derived_document(
            raw_document,
            solution="networker",
            document_type="job-run",
            record=record,
            record_id=job_id,
        )
        document["@timestamp"] = record["event_time"]
        document["observed_at"] = raw_document.get("@timestamp")
        document["index_month"] = record["job_run_month"]
        document["derived_id"] = f"{document['collector']}:job-run:{job_id}"
        documents.append(document)
    return documents


def _job_run_record(job: dict[str, Any]) -> dict[str, Any] | None:
    job_id = _string(job.get("job_id"))
    start_time = _parse_datetime(job.get("start_time"))
    end_time = _parse_datetime(job.get("end_time"))
    status = _string(job.get("status")).lower() or "unknown"
    event_time = start_time if status == "running" else end_time or start_time

    # A stable ID and event time are required to avoid duplicate analytical rows.
    if not job_id or event_time is None:
        return None

    duration_seconds = None
    if start_time is not None and end_time is not None and end_time >= start_time:
        duration_seconds = int((end_time - start_time).total_seconds())

    run_month_source = start_time or event_time
    return job | {
        "job_id": job_id,
        "start_time": _isoformat(start_time),
        "end_time": _isoformat(end_time),
        "event_time": _isoformat(event_time),
        "job_run_month": run_month_source.strftime("%Y-%m"),
        "duration_seconds": duration_seconds,
        "outcome": status,
    }


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
        "device_name": raw_document.get("device_name") or collector,
        "target_type": raw_document.get("target_type"),
        "solution": solution,
        "document_family": "derived",
        "document_type": document_type,
        "processing_mode": "elt",
        "source_raw_id": raw_document.get("_id") or raw_document.get("raw_document_id"),
        "record_id": str(record_id or "unknown"),
        "derived_id": f"{collector}:{document_type}:{record_id or 'unknown'}:{month}",
        "source_networker": record.get("source_networker"),
        "security_domain": record.get("security_domain"),
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
    aliases = {
        "DD": "vtl",
        "DXi": "vtl",
        "Networker": "networker",
        "ZFS": "zfs",
        "i6000": "ptl",
    }
    return aliases.get(str(raw_document.get("target_type")), str(raw_document.get("target_type", "")).lower())


def _month(timestamp: Any) -> str:
    text = _string(timestamp)
    if len(text) >= 7:
        return text[:7]
    return "unknown"


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        try:
            parsed = _datetime_from_epoch(float(value))
        except (ValueError, OverflowError, OSError):
            return None
    else:
        text = _string(value)
        if not text:
            return None
        try:
            parsed = _datetime_from_epoch(float(text))
        except (ValueError, OverflowError, OSError):
            normalized = f"{text[:-1]}+00:00" if text.endswith(("Z", "z")) else text
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _datetime_from_epoch(value: float) -> datetime:
    # NetWorker versions may return Unix seconds or milliseconds.
    seconds = value / 1000 if abs(value) >= 100_000_000_000 else value
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()

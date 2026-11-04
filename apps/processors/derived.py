from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any


def build_derived_documents(raw_document: dict[str, Any]) -> list[dict[str, Any]]:
    return list(iter_derived_documents(raw_document))


def iter_derived_documents(raw_document: dict[str, Any]) -> Iterator[dict[str, Any]]:
    payload = raw_document.get("payload")
    if not isinstance(payload, dict):
        return

    target_type = raw_document.get("target_type")
    if target_type == "Networker":
        yield from _iter_networker_documents(raw_document, payload)
    if target_type == "i6000":
        yield from _i6000_documents(raw_document, payload)
    if target_type == "ZFS":
        yield from _zfs_documents(raw_document, payload)
    if target_type in {"DD", "DXi"}:
        yield from _device_documents(raw_document, payload)


def _iter_networker_documents(
    raw_document: dict[str, Any],
    payload: dict[str, Any],
) -> Iterator[dict[str, Any]]:
    specs = (
        ("job", "jobs", ("job_id", "name")),
        ("client", "clients", ("client_name", "client_id")),
        ("policy", "policies", ("policy_name",)),
        ("workflow", "workflows", ("policy_name", "workflow_name")),
    )
    yield from _iter_record_documents(raw_document, payload, "networker", specs)
    yield from _iter_networker_backup_documents(raw_document, payload)
    yield from _iter_networker_job_run_documents(raw_document, payload)


def _iter_networker_backup_documents(
    raw_document: dict[str, Any],
    payload: dict[str, Any],
) -> Iterator[dict[str, Any]]:
    backups = payload.get("backups")
    if not isinstance(backups, list):
        return

    for backup in backups:
        if not isinstance(backup, dict):
            continue
        completion_time = _parse_datetime(backup.get("completion_time"))
        event_time = completion_time or _parse_datetime(backup.get("save_time"))
        backup_id, identity_source = _networker_backup_identity(
            backup,
            completion_time,
        )
        if not backup_id or event_time is None:
            continue
        record = backup | {
            "backup_document_id": backup_id,
            "backup_identity_source": identity_source,
            "event_time": _isoformat(event_time),
            "backup_month": event_time.strftime("%Y-%m"),
        }
        document = _derived_document(
            raw_document,
            solution="networker",
            document_type="backup",
            record=record,
            record_id=backup_id,
        )
        document["@timestamp"] = record["event_time"]
        document["observed_at"] = raw_document.get("@timestamp")
        document["index_month"] = record["backup_month"]
        document["derived_id"] = f"{document['collector']}:backup:{backup_id}"
        yield document


def _networker_backup_identity(
    backup: dict[str, Any],
    completion_time: datetime | None,
) -> tuple[str, str]:
    backup_id = _string(backup.get("backup_id"))
    if backup_id:
        return backup_id, "backup_id"

    client_name = _string(backup.get("client_name")).lower().rstrip(".")
    save_set = _string(backup.get("save_set")) or _string(backup.get("name"))
    if not client_name or not save_set or completion_time is None:
        return "", ""

    identity = "\0".join(
        (
            client_name,
            save_set,
            _isoformat(completion_time) or "",
        )
    )
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return f"fallback:{digest}", "client_save_set_completion_time"


def _iter_networker_job_run_documents(
    raw_document: dict[str, Any],
    payload: dict[str, Any],
) -> Iterator[dict[str, Any]]:
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        return

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
        document["derived_id"] = f"{document['collector']}:job:{job_id}"
        yield document


def _job_run_record(job: dict[str, Any]) -> dict[str, Any] | None:
    job_id = _string(job.get("job_id"))
    start_time = _parse_datetime(job.get("start_time"))
    end_time = _parse_datetime(job.get("end_time"))
    status = (
        _string(job.get("status_normalized")).lower()
        or _string(job.get("status")).lower()
        or "unknown"
    )
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
        "workflow_run_key": _workflow_run_key(job),
        "start_time": _isoformat(start_time),
        "end_time": _isoformat(end_time),
        "event_time": _isoformat(event_time),
        "job_run_month": run_month_source.strftime("%Y-%m"),
        "duration_seconds": duration_seconds,
        "outcome": status,
    }


def _workflow_run_key(job: dict[str, Any]) -> str:
    """Identify a workflow run using only verified type and positive API IDs."""
    source = _string(job.get("source_networker")).lower()
    if not source or ":" in source or any(char.isspace() for char in source):
        return ""
    job_type = _string(job.get("type")).lower()
    if job_type == "workflow job":
        value = job.get("job_id")
    elif job_type in {"backup action job", "save job"}:
        value = job.get("root_parent_job_id")
    else:
        return ""
    # NetWorker list API IDs are positive decimal integers. Reject ambiguous
    # booleans, fractional/negative IDs and placeholders without changing originals.
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return ""
    identity = str(value).strip()
    if not identity or not identity.isascii() or not identity.isdecimal() or not identity.strip("0"):
        return ""
    return f"{source}:{identity}"


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
    return list(_iter_record_documents(raw_document, payload, solution, specs))


def _iter_record_documents(
    raw_document: dict[str, Any],
    payload: dict[str, Any],
    solution: str,
    specs: tuple[tuple[str, str, tuple[str, ...]], ...],
) -> Iterator[dict[str, Any]]:
    for document_type, payload_key, id_fields in specs:
        records = payload.get(payload_key)
        if not isinstance(records, list):
            continue
        for index, record in enumerate(records):
            if not isinstance(record, dict):
                continue
            document = _derived_document(
                raw_document,
                solution=solution,
                document_type=document_type,
                record=record,
                record_id=_record_id(record, id_fields, index),
            )
            if solution == "networker" and document_type == "job" and record.get("job_id"):
                document["derived_id"] = f"{document['collector']}:job:{record['job_id']}"
            yield document


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
    document = {
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
    if solution == "networker":
        document["document_family"] = (
            "job" if document_type == "job-run" else document_type
        )
        document.update(_networker_document_fields(document_type, record))
    return document


def _networker_document_fields(
    document_type: str,
    record: dict[str, Any],
) -> dict[str, Any]:
    if document_type in {"job", "job-run"}:
        return {
            "job": {
                "id": record.get("job_id"),
                "name": record.get("name"),
                "type": record.get("type"),
                "state": record.get("state"),
                "completion_status": record.get("completion_status"),
                "status_normalized": record.get("status_normalized"),
                "exit_code": record.get("exit_code"),
                "start_time": record.get("start_time"),
                "end_time": record.get("end_time"),
                "duration_seconds": record.get("duration_seconds"),
                "level": record.get("level"),
                "data_size_bytes": record.get("data_size_bytes"),
                "parent_job_id": record.get("parent_job_id"),
                "root_parent_job_id": record.get("root_parent_job_id"),
                "previous_job_id": record.get("previous_job_id"),
            },
            "client": {"hostname": record.get("client_hostname") or record.get("client_name")},
            "run_on_host": record.get("run_on_host"),
        }
    if document_type == "backup":
        return {
            "backup": {
                "id": record.get("backup_id"),
                "save_set": record.get("save_set"),
                "level": record.get("level"),
                "save_time": record.get("save_time"),
                "completion_time": record.get("completion_time"),
                "size_bytes": record.get("size_bytes"),
            },
            "client": {"hostname": record.get("client_hostname") or record.get("client_name")},
            "policy": {"name": record.get("policy_name")},
            "workflow": {"name": record.get("workflow_name")},
            "action": {"name": record.get("action_name")},
        }
    if document_type == "client":
        return {
            "client": {
                "resource_id": record.get("resource_id") or record.get("client_id"),
                "hostname": record.get("client_hostname") or record.get("client_name"),
                "state": record.get("client_state"),
                "backup_type": record.get("backup_type"),
                "os": record.get("client_os"),
            }
        }
    return {}


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

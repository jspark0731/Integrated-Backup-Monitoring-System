from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import re
import shlex
from typing import Any

from apps.classifiers.source_networker import validate_source_networker


def parse_networker_rest_payload(
    payloads: dict[str, Any],
    server_name: str,
    *,
    source_networker: str | None = None,
) -> dict[str, Any]:
    source = validate_source_networker(source_networker)
    jobs = [_normalize_job(item, server_name, source) for item in _items(payloads.get("jobs"), "jobs")]
    clients = [_normalize_client(item, server_name, source) for item in _items(payloads.get("clients"), "clients")]
    policies, workflows = _normalize_policies(payloads.get("policies"), server_name)
    backups = [_normalize_backup(item, server_name, source) for item in _items(payloads.get("backups"), "backups")]

    jobs = [_classify_record(item, source) for item in jobs]
    clients = [_classify_record(item, source) for item in clients]
    backups = [_classify_record(item, source) for item in backups]
    policies = [_classify_record(item, source) for item in policies]
    workflows = [_classify_record(item, source) for item in workflows]

    summary = _build_summary(server_name, jobs, clients, policies, workflows, backups)
    summary["source_networker"] = source
    summary["client_count_by_domain"] = dict(Counter(client["security_domain"] for client in clients))
    query_window = payloads.get("_query_window")
    if isinstance(query_window, dict):
        summary["query_window"] = dict(query_window)

    return {
        "summary": summary,
        "jobs": jobs,
        "backups": backups,
        "clients": clients,
        "policies": policies,
        "workflows": workflows,
        "monthly_report": [
            _classify_record(item, source)
            for item in _build_monthly_report(server_name, jobs, clients, workflows, backups)
        ],
    }


def _normalize_job(item: dict[str, Any], server: str, source_networker: str) -> dict[str, Any]:
    state_raw = _string(item.get("state"))
    completion_status = _string(item.get("completionStatus"))
    state = state_raw or completion_status
    status_raw = completion_status or state_raw
    exit_code = _int(item.get("exitCode"))
    status = _job_status(completion_status, state_raw, exit_code)
    start_time = _parse_datetime(item.get("startTime"))
    end_time = _parse_datetime(item.get("endTime"))
    duration_seconds = None
    if start_time is not None and end_time is not None and end_time >= start_time:
        duration_seconds = int((end_time - start_time).total_seconds())
    job_type = _string(item.get("type"))
    is_save_job = job_type.lower() == "save job"
    # runOnHost identifies a backup client only for the verified save-job type.
    client_name = _string(item.get("clientName")) or _string(item.get("clientHostname"))
    if not client_name and is_save_job:
        client_name = _string(item.get("runOnHost"))

    return {
        "server": server,
        "source_networker": source_networker,
        "job_id": item.get("id") or item.get("jobId"),
        "name": _string(item.get("name")),
        "type": job_type,
        "state": state,
        "state_raw": state_raw,
        "completion_status": completion_status,
        "status_raw": status_raw,
        "status": status,
        "status_normalized": status,
        "adhoc_job": item.get("adhocJob") if type(item.get("adhocJob")) is bool else None,
        "message": item.get("message") if isinstance(item.get("message"), str) else "",
        "execution_evidence": _action_evidence(item.get("message")),
        "exit_code": exit_code,
        "policy_name": _string(item.get("policyName")),
        "workflow_name": _string(item.get("workflowName")),
        "protection_group": _protection_group(item.get("command")) if is_save_job else "",
        "client_name": client_name,
        "run_on_host": _string(item.get("runOnHost")),
        "start_time": _isoformat(start_time),
        "end_time": _isoformat(end_time),
        "duration_seconds": duration_seconds,
        "level": _string(item.get("level")),
        "data_size_bytes": _size_bytes(item.get("dataSize")),
        "stopped": item.get("stopped"),
        "root_parent_job_id": item.get("rootParentJobId"),
        "parent_job_id": item.get("parentJobId"),
        "previous_job_id": item.get("previousJobId"),
    }


def _action_evidence(message: Any) -> dict[str, Any]:
    """Record explicit Action skips without asserting the whole Workflow scope."""
    text = message if isinstance(message, str) else ""
    requested = set(re.findall(r"Request to run the enabled action ['\"]([^'\"]+)['\"]", text, re.I))
    skipped = set()
    for name in requested:
        escaped = re.escape(name)
        configured = re.search(
            r"Request to run the enabled action ['\"]" + escaped
            + r"['\"] that has level configured as ['\"]skip['\"]", text, re.I,
        )
        not_running = re.search(
            r"Not running action ['\"]" + escaped
            + r"['\"] because either the schedule says to skip it today", text, re.I,
        )
        if configured and not_running:
            skipped.add(name)
    return {
        "requested_actions": sorted(requested),
        "skipped_actions": sorted(skipped),
        "scope": "message_declared_actions_only",
    }


def _protection_group(command: Any) -> str:
    """Read save -g as a Protection Group, never as a workflow or policy.

    Use shell-style quoted tokens (including quoted Windows command paths).
    This tokenizes text only and never executes a shell command.
    """
    if not isinstance(command, str) or not command.strip():
        return ""
    lexer = shlex.shlex(command, posix=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    try:
        tokens = list(lexer)
    except ValueError:  # Unclosed quotes: do not guess a group from partial input.
        return ""
    positions = [index for index, token in enumerate(tokens) if token == "-g"]
    if len(positions) != 1 or positions[0] + 1 >= len(tokens):
        return ""
    value = tokens[positions[0] + 1].strip()
    return value if value and not value.startswith("-") else ""


def _normalize_client(item: dict[str, Any], server: str, source_networker: str) -> dict[str, Any]:
    hostname = _string(item.get("hostname")) or _string(item.get("name"))
    os_name = _first_string(
        item.get("operatingSystem"),
        item.get("os"),
        item.get("clientOS"),
        item.get("platform"),
    )

    return {
        "server": server,
        "source_networker": source_networker,
        "client_id": _resource_id(item),
        "resource_id": _resource_id(item),
        "client_name": hostname,
        "client_state": _normalize_client_state(item.get("clientState")),
        "client_os": os_name,
        "client_os_family": _os_family(os_name),
        "backup_type": _string(item.get("backupType")),
    }


def _normalize_policies(payload: Any, server: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    policies = []
    workflows = []

    for policy in _items(payload, "protectionPolicies"):
        policy_name = _string(policy.get("name"))
        policy_workflows = [item for item in _as_list(policy.get("workflows")) if isinstance(item, dict)]
        policies.append(
            {
                "server": server,
                "policy_name": policy_name,
                "comment": _string(policy.get("comment")),
                "workflow_count": len(policy_workflows),
                "resource_id": _resource_id(policy),
            }
        )
        for workflow in policy_workflows:
            actions = [item for item in _as_list(workflow.get("actions")) if isinstance(item, dict)]
            workflows.append(
                {
                    "server": server,
                    "policy_name": policy_name,
                    "workflow_name": _string(workflow.get("name")),
                    "enabled": workflow.get("enabled"),
                    "action_count": len(actions),
                    "actions": [_string(action.get("name")) for action in actions],
                    "protection_groups": workflow.get("protectionGroups") or [],
                    "start_time": workflow.get("startTime"),
                    "end_time": workflow.get("endTime"),
                }
            )

    return policies, workflows


def _normalize_backup(item: dict[str, Any], server: str, source_networker: str) -> dict[str, Any]:
    attributes = _attributes(item.get("attributes"))
    policy = _string(item.get("policyName")) or _strip_policy_suffix(attributes.get("*policy name") or attributes.get("policy name") or "")
    workflow = _string(item.get("workflowName")) or _strip_policy_suffix(attributes.get("*policy workflow name") or attributes.get("policy workflow name") or "")
    action = _string(item.get("actionName")) or _strip_policy_suffix(attributes.get("*policy action name") or attributes.get("policy action name") or "")
    size = item.get("size")
    size_raw = size.get("value") if isinstance(size, dict) else size
    size_unit_raw = _string(size.get("unit")) if isinstance(size, dict) else ""
    size_bytes = _size_bytes(size)
    save_set = _string(item.get("name"))

    return {
        "server": server,
        "source_networker": source_networker,
        "backup_id": _string(item.get("id")),
        "client_name": _string(item.get("clientHostname")),
        "client_id": _string(item.get("clientId")),
        "policy_name": policy,
        "workflow_name": workflow,
        "action_name": action,
        "group": attributes.get("group"),
        "name": save_set,
        "save_set": save_set,
        "type": _string(item.get("type")),
        "level": _string(item.get("level")),
        "save_time": _isoformat(_parse_datetime(item.get("saveTime"))),
        "completion_time": _isoformat(_parse_datetime(item.get("completionTime"))),
        "retention_time": _isoformat(_parse_datetime(item.get("retentionTime"))),
        "file_count": item.get("fileCount"),
        "size_raw": size_raw,
        "size_unit_raw": size_unit_raw,
        "size_bytes": size_bytes,
        "instances": item.get("instances") or [],
    }


def _build_summary(
    server: str,
    jobs: list[dict[str, Any]],
    clients: list[dict[str, Any]],
    policies: list[dict[str, Any]],
    workflows: list[dict[str, Any]],
    backups: list[dict[str, Any]],
) -> dict[str, Any]:
    success = Counter(job["policy_name"] or "unknown" for job in jobs if job["status"] == "success")
    failed = Counter(job["policy_name"] or "unknown" for job in jobs if job["status"] == "failed")
    running = Counter(job["policy_name"] or "unknown" for job in jobs if job["status"] == "running")
    workflow_count = Counter(workflow["policy_name"] or "unknown" for workflow in workflows)
    success_by_domain = Counter(
        (job["security_domain"], job["policy_name"] or "unknown")
        for job in jobs
        if job["status"] == "success"
    )
    failed_by_domain = Counter(
        (job["security_domain"], job["policy_name"] or "unknown")
        for job in jobs
        if job["status"] == "failed"
    )
    running_by_domain = Counter(
        (job["security_domain"], job["policy_name"] or "unknown")
        for job in jobs
        if job["status"] == "running"
    )
    workflows_by_domain = Counter(
        (workflow["security_domain"], workflow["policy_name"] or "unknown")
        for workflow in workflows
    )

    return {
        "server": server,
        "job_count": len(jobs),
        "client_count": len({client["client_name"] for client in clients if client["client_name"]}),
        "policy_count": len({policy["policy_name"] for policy in policies}),
        "workflow_count": len(
            {(workflow["policy_name"], workflow["workflow_name"]) for workflow in workflows}
        ),
        "backup_count": len(backups),
        "total_backup_bytes": sum(item.get("size_bytes") or 0 for item in backups),
        "job_success_count_by_policy": dict(success),
        "job_failed_count_by_policy": dict(failed),
        "job_running_count_by_policy": dict(running),
        "workflow_count_by_policy": dict(workflow_count),
        "job_success_count_by_domain_policy": _nested_counts(success_by_domain),
        "job_failed_count_by_domain_policy": _nested_counts(failed_by_domain),
        "job_running_count_by_domain_policy": _nested_counts(running_by_domain),
        "workflow_count_by_domain_policy": _nested_counts(workflows_by_domain),
        "recent_failed_jobs": [job for job in jobs if job["status"] == "failed"][:20],
    }


def _build_monthly_report(
    server: str,
    jobs: list[dict[str, Any]],
    clients: list[dict[str, Any]],
    workflows: list[dict[str, Any]],
    backups: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    clients_by_domain: dict[str, set[str]] = defaultdict(set)
    for client in clients:
        if client["client_name"]:
            clients_by_domain[client["security_domain"]].add(client["client_name"])

    rows: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "server": server,
            "month": datetime.now(timezone.utc).strftime("%Y-%m"),
            "source_networker": "",
            "security_domain": "unmapped",
            "classification_status": "derived",
            "policy_name": "unknown",
            "workflow_name": "",
            "total_backup_bytes": 0,
            "total_backup_tb": 0.0,
            "job_success_count": 0,
            "job_failed_count": 0,
            "job_running_count": 0,
            "workflow_count": 0,
            "client_count": 0,
        }
    )

    for workflow in workflows:
        key = (
            workflow["security_domain"],
            workflow["policy_name"] or "unknown",
            workflow["workflow_name"] or "",
        )
        rows[key]["security_domain"] = key[0]
        rows[key]["policy_name"] = key[1]
        rows[key]["workflow_name"] = key[2]
        rows[key]["source_networker"] = workflow["source_networker"]
        rows[key]["workflow_count"] += 1

    for backup in backups:
        key = (
            backup["security_domain"],
            backup["policy_name"] or "unknown",
            backup["workflow_name"] or "",
        )
        rows[key]["security_domain"] = key[0]
        rows[key]["policy_name"] = key[1]
        rows[key]["workflow_name"] = key[2]
        rows[key]["source_networker"] = backup["source_networker"]
        rows[key]["total_backup_bytes"] += backup.get("size_bytes") or 0

    for job in jobs:
        key = (
            job["security_domain"],
            job["policy_name"] or "unknown",
            job["workflow_name"] or "",
        )
        rows[key]["security_domain"] = key[0]
        rows[key]["policy_name"] = key[1]
        rows[key]["workflow_name"] = key[2]
        rows[key]["source_networker"] = job["source_networker"]
        if job["status"] == "success":
            rows[key]["job_success_count"] += 1
        elif job["status"] == "failed":
            rows[key]["job_failed_count"] += 1
        elif job["status"] == "running":
            rows[key]["job_running_count"] += 1

    for row in rows.values():
        row["total_backup_tb"] = round(row["total_backup_bytes"] / 1000**4, 3)
        row["client_count"] = len(clients_by_domain[row["security_domain"]])

    return list(rows.values())


def _classify_record(record: dict[str, Any], source_networker: str) -> dict[str, Any]:
    classification = {
        "source_networker": source_networker,
        "security_domain": source_networker,
        "classification_status": "matched_source_networker",
        "classification_source": "source_networker",
    }
    if "client_name" in record:
        # Keep the existing API name extraction and hostname normalization.
        classification["client_hostname"] = _string(record["client_name"]).lower().rstrip(".")
    return record | classification


def _nested_counts(counts: Counter[tuple[str, str]]) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = defaultdict(dict)
    for (domain, policy), count in counts.items():
        result[domain][policy] = count
    return dict(result)


def _items(payload: Any, key: str) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _attributes(value: Any) -> dict[str, str]:
    result = {}
    for item in _as_list(value):
        if not isinstance(item, dict):
            continue
        values = item.get("values") or []
        first_value = values[0] if values else ""
        result[_string(item.get("key"))] = _string(first_value)
    return result


def _job_status(
    completion_status: str,
    state: str,
    exit_code: int | None,
) -> str:
    completion = completion_status.lower()
    normalized_state = state.lower()
    if completion in {"running", "active", "started", "sessionactive", "queued", "pending"}:
        return "running"
    if completion in {"canceled", "cancelled", "aborted"}:
        return "canceled"
    if completion in {"completed", "succeeded", "success"}:
        return "success"
    if completion in {"failed", "failure"}:
        return "failed"
    if normalized_state in {"running", "active", "started", "sessionactive", "queued", "pending"}:
        return "running"
    if normalized_state in {"canceled", "cancelled", "aborted"}:
        return "canceled"
    if normalized_state in {"failed", "failure"}:
        return "failed"
    if exit_code is not None:
        return "success" if exit_code == 0 else "failed"
    if normalized_state in {"completed", "succeeded", "success"}:
        return "success"
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
    seconds = value / 1000 if abs(value) >= 100_000_000_000 else value
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def _isoformat(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _size_bytes(value: Any) -> int | None:
    if isinstance(value, dict):
        number = _number(value.get("value"))
        unit = _string(value.get("unit")).lower()
    else:
        number = _number(value)
        unit = ""
    if number is None:
        return None
    multipliers = {
        "": 1,
        "byte": 1,
        "bytes": 1,
        "kb": 1000,
        "kib": 1024,
        "mb": 1000**2,
        "mib": 1024**2,
        "gb": 1000**3,
        "gib": 1024**3,
        "tb": 1000**4,
        "tib": 1024**4,
    }
    multiplier = multipliers.get(unit)
    return int(number * multiplier) if multiplier is not None else None


def _resource_id(item: dict[str, Any]) -> str:
    resource_id = item.get("resourceId")
    if isinstance(resource_id, dict):
        return _string(resource_id.get("id"))
    return _string(resource_id)


def _policy_from_text(value: str) -> str:
    if not value:
        return ""
    return _strip_policy_suffix(value.split(":", 1)[0])


def _strip_policy_suffix(value: str) -> str:
    return value.split(":", 1)[0].strip()


def _os_family(value: str) -> str:
    normalized = value.lower()
    if "aix" in normalized:
        return "AIX"
    if "linux" in normalized or "rhel" in normalized or "sles" in normalized:
        return "Linux"
    if "win" in normalized:
        return "Windows"
    return "Unknown" if not value else "Other"


def _normalize_client_state(value: Any) -> str:
    normalized = _string(value).lower()
    if normalized in {"enabled", "active", "online"}:
        return "enabled"
    if normalized in {"disabled", "inactive", "offline"}:
        return "disabled"
    return normalized or "unknown"


def _first_string(*values: Any) -> str:
    for value in values:
        text = _string(value)
        if text:
            return text
    return ""


def _string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _number(value: Any) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]

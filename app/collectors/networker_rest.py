from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import httpx

from app.collectors.base import BaseCollector
from app.core.metrics import (
    NETWORKER_API_UP,
    NETWORKER_CLIENT_COUNT,
    NETWORKER_JOB_FAILED_COUNT,
    NETWORKER_JOB_RUNNING_COUNT,
    NETWORKER_JOB_SUCCESS_COUNT,
    NETWORKER_WORKFLOW_COUNT,
)


DEFAULT_ENDPOINTS = {
    "jobs": "nwrestapi/v3/global/jobs",
    "clients": "nwrestapi/v3/global/clients",
    "backups": "nwrestapi/v3/global/backups",
    "policies": "nwrestapi/v3/global/protectionpolicies",
    "protection_groups": "nwrestapi/v3/global/protectiongroups",
}


class NetworkerRestCollector(BaseCollector):
    async def _collect_payload(self) -> dict:
        raw = await self._fetch_payloads()
        parsed = parse_networker_rest_payload(raw, server_name=self.name)
        self._publish_metrics(parsed["summary"])
        return {
            "summary": parsed["summary"],
            "jobs": parsed["jobs"],
            "clients": parsed["clients"],
            "policies": parsed["policies"],
            "workflows": parsed["workflows"],
            "monthly_report": parsed["monthly_report"],
            "raw": raw,
        }

    async def _fetch_payloads(self) -> dict[str, Any]:
        endpoints = self.config.endpoints or DEFAULT_ENDPOINTS
        headers = {"Accept": "application/json"}
        auth = None

        if self.config.token:
            headers["Authorization"] = f"Bearer {self.config.token}"
        if self.config.username and self.config.password:
            auth = (self.config.username, self.config.password)

        async with httpx.AsyncClient(verify=self.config.verify_tls, timeout=30) as client:
            payloads = {}
            for name, endpoint in endpoints.items():
                response = await client.get(self._url(endpoint), headers=headers, auth=auth)
                response.raise_for_status()
                payloads[name] = response.json() if response.content else {}
        return payloads

    def _url(self, endpoint: str) -> str:
        base_url = f"{self.config.base_url.rstrip('/')}/"
        return urljoin(base_url, endpoint.lstrip("/"))

    def _publish_metrics(self, summary: dict[str, Any]) -> None:
        server = str(summary.get("server") or self.name)

        NETWORKER_API_UP.labels(server).set(1)
        NETWORKER_CLIENT_COUNT.labels(server).set(summary.get("client_count", 0))

        for policy, count in summary.get("job_success_count_by_policy", {}).items():
            NETWORKER_JOB_SUCCESS_COUNT.labels(server, policy).set(count)
        for policy, count in summary.get("job_failed_count_by_policy", {}).items():
            NETWORKER_JOB_FAILED_COUNT.labels(server, policy).set(count)
        for policy, count in summary.get("job_running_count_by_policy", {}).items():
            NETWORKER_JOB_RUNNING_COUNT.labels(server, policy).set(count)
        for policy, count in summary.get("workflow_count_by_policy", {}).items():
            NETWORKER_WORKFLOW_COUNT.labels(server, policy).set(count)


def parse_networker_rest_payload(payloads: dict[str, Any], server_name: str) -> dict[str, Any]:
    jobs = [_normalize_job(item, server_name) for item in _items(payloads.get("jobs"), "jobs")]
    clients = [_normalize_client(item, server_name) for item in _items(payloads.get("clients"), "clients")]
    policies, workflows = _normalize_policies(payloads.get("policies"), server_name)
    backups = [_normalize_backup(item, server_name) for item in _items(payloads.get("backups"), "backups")]

    summary = _build_summary(server_name, jobs, clients, policies, workflows, backups)

    return {
        "summary": summary,
        "jobs": jobs,
        "clients": clients,
        "policies": policies,
        "workflows": workflows,
        "monthly_report": _build_monthly_report(server_name, jobs, clients, workflows, backups),
    }


def _normalize_job(item: dict[str, Any], server: str) -> dict[str, Any]:
    state = _string(item.get("state"))
    exit_code = _int(item.get("exitCode"))
    status = _job_status(state, exit_code)
    policy = _policy_from_text(_string(item.get("policyName")) or _string(item.get("policy")) or _string(item.get("name")))

    return {
        "server": server,
        "job_id": item.get("id"),
        "name": _string(item.get("name")),
        "type": _string(item.get("type")),
        "state": state,
        "status": status,
        "exit_code": exit_code,
        "policy_name": policy,
        "workflow_name": _string(item.get("workflowName")) or _string(item.get("workflow")),
        "client_name": _string(item.get("clientName")) or _string(item.get("clientHostname")) or _string(item.get("runOnHost")),
        "run_on_host": _string(item.get("runOnHost")),
        "start_time": item.get("startTime"),
        "end_time": item.get("endTime"),
        "stopped": item.get("stopped"),
        "root_parent_job_id": item.get("rootParentJobId"),
        "parent_job_id": item.get("parentJobId"),
    }


def _normalize_client(item: dict[str, Any], server: str) -> dict[str, Any]:
    hostname = _string(item.get("hostname")) or _string(item.get("name"))
    os_name = _first_string(
        item.get("operatingSystem"),
        item.get("os"),
        item.get("clientOS"),
        item.get("platform"),
    )

    return {
        "server": server,
        "client_id": _string(item.get("clientId")) or _resource_id(item),
        "client_name": hostname,
        "client_os": os_name,
        "client_os_family": _os_family(os_name),
        "backup_type": _string(item.get("backupType")),
        "scheduled_backup": item.get("scheduledBackup"),
        "save_sets": item.get("saveSets") or [],
        "protection_groups": item.get("protectionGroups") or [],
        "storage_nodes": item.get("storageNodes") or [],
        "parallelism": item.get("parallelism"),
        "aliases": item.get("aliases") or [],
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


def _normalize_backup(item: dict[str, Any], server: str) -> dict[str, Any]:
    attributes = _attributes(item.get("attributes"))
    policy = _strip_policy_suffix(attributes.get("*policy name") or attributes.get("policy name") or "")
    workflow = _strip_policy_suffix(attributes.get("*policy workflow name") or attributes.get("policy workflow name") or "")
    size_bytes = _size_bytes(item.get("size"))

    return {
        "server": server,
        "backup_id": _string(item.get("id")),
        "client_name": _string(item.get("clientHostname")),
        "client_id": _string(item.get("clientId")),
        "policy_name": policy,
        "workflow_name": workflow,
        "group": attributes.get("group"),
        "name": _string(item.get("name")),
        "type": _string(item.get("type")),
        "level": _string(item.get("level")),
        "save_time": item.get("saveTime"),
        "completion_time": item.get("completionTime"),
        "retention_time": item.get("retentionTime"),
        "file_count": item.get("fileCount"),
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

    return {
        "server": server,
        "job_count": len(jobs),
        "client_count": len({client["client_name"] for client in clients if client["client_name"]}),
        "policy_count": len(policies),
        "workflow_count": len(workflows),
        "backup_count": len(backups),
        "total_backup_bytes": sum(item.get("size_bytes") or 0 for item in backups),
        "job_success_count_by_policy": dict(success),
        "job_failed_count_by_policy": dict(failed),
        "job_running_count_by_policy": dict(running),
        "workflow_count_by_policy": dict(workflow_count),
        "recent_failed_jobs": [job for job in jobs if job["status"] == "failed"][:20],
    }


def _build_monthly_report(
    server: str,
    jobs: list[dict[str, Any]],
    clients: list[dict[str, Any]],
    workflows: list[dict[str, Any]],
    backups: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "server": server,
            "month": datetime.now(timezone.utc).strftime("%Y-%m"),
            "policy_name": "unknown",
            "workflow_name": "",
            "total_backup_bytes": 0,
            "total_backup_tb": 0.0,
            "job_success_count": 0,
            "job_failed_count": 0,
            "job_running_count": 0,
            "workflow_count": 0,
            "client_count": len({client["client_name"] for client in clients if client["client_name"]}),
        }
    )

    for workflow in workflows:
        key = (workflow["policy_name"] or "unknown", workflow["workflow_name"] or "")
        rows[key]["policy_name"] = key[0]
        rows[key]["workflow_name"] = key[1]
        rows[key]["workflow_count"] += 1

    for backup in backups:
        key = (backup["policy_name"] or "unknown", backup["workflow_name"] or "")
        rows[key]["policy_name"] = key[0]
        rows[key]["workflow_name"] = key[1]
        rows[key]["total_backup_bytes"] += backup.get("size_bytes") or 0

    for job in jobs:
        key = (job["policy_name"] or "unknown", job["workflow_name"] or "")
        rows[key]["policy_name"] = key[0]
        rows[key]["workflow_name"] = key[1]
        if job["status"] == "success":
            rows[key]["job_success_count"] += 1
        elif job["status"] == "failed":
            rows[key]["job_failed_count"] += 1
        elif job["status"] == "running":
            rows[key]["job_running_count"] += 1

    for row in rows.values():
        row["total_backup_tb"] = round(row["total_backup_bytes"] / 1000**4, 3)

    return list(rows.values())


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


def _job_status(state: str, exit_code: int | None) -> str:
    normalized = state.lower()
    if normalized in {"running", "active", "queued", "pending"}:
        return "running"
    if exit_code is not None:
        return "success" if exit_code == 0 else "failed"
    if normalized in {"completed", "succeeded", "success"}:
        return "success"
    if normalized in {"failed", "aborted", "canceled", "cancelled"}:
        return "failed"
    return "unknown"


def _size_bytes(value: Any) -> int | None:
    if isinstance(value, dict):
        number = _int(value.get("value"))
        unit = _string(value.get("unit")).lower()
        if number is None:
            return None
        multipliers = {
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
        return int(number * multipliers.get(unit, 1))
    return _int(value)


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


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]

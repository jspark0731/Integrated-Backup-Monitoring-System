from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from app.classifiers.hostname import HostnameClassifier


def parse_networker_rest_payload(
    payloads: dict[str, Any],
    server_name: str,
    *,
    source_networker: str | None = None,
    classifier: HostnameClassifier | None = None,
) -> dict[str, Any]:
    source = (source_networker or server_name).strip().lower()
    jobs = [_normalize_job(item, server_name, source) for item in _items(payloads.get("jobs"), "jobs")]
    clients = [_normalize_client(item, server_name, source) for item in _items(payloads.get("clients"), "clients")]
    policies, workflows = _normalize_policies(payloads.get("policies"), server_name)
    backups = [_normalize_backup(item, server_name, source) for item in _items(payloads.get("backups"), "backups")]

    jobs = [_classify_record(item, classifier) for item in jobs]
    clients = [_classify_record(item, classifier) for item in clients]
    backups = [_classify_record(item, classifier) for item in backups]
    policies, workflows = _classify_inventory(policies, workflows, jobs, backups, source)

    summary = _build_summary(server_name, jobs, clients, policies, workflows, backups)
    summary["source_networker"] = source
    summary["client_count_by_domain"] = dict(Counter(client["security_domain"] for client in clients))

    return {
        "summary": summary,
        "jobs": jobs,
        "clients": clients,
        "policies": policies,
        "workflows": workflows,
        "monthly_report": _build_monthly_report(server_name, jobs, clients, workflows, backups),
    }


def _normalize_job(item: dict[str, Any], server: str, source_networker: str) -> dict[str, Any]:
    state = _string(item.get("state"))
    exit_code = _int(item.get("exitCode"))
    status = _job_status(state, exit_code)
    policy = _policy_from_text(_string(item.get("policyName")) or _string(item.get("policy")) or _string(item.get("name")))

    return {
        "server": server,
        "source_networker": source_networker,
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


def _normalize_backup(item: dict[str, Any], server: str, source_networker: str) -> dict[str, Any]:
    attributes = _attributes(item.get("attributes"))
    policy = _strip_policy_suffix(attributes.get("*policy name") or attributes.get("policy name") or "")
    workflow = _strip_policy_suffix(attributes.get("*policy workflow name") or attributes.get("policy workflow name") or "")
    size_bytes = _size_bytes(item.get("size"))

    return {
        "server": server,
        "source_networker": source_networker,
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


def _classify_record(record: dict[str, Any], classifier: HostnameClassifier | None) -> dict[str, Any]:
    hostname = record.get("client_name")
    if classifier is None:
        return record | {
            "client_hostname": _string(hostname).lower().rstrip("."),
            "security_domain": "unmapped",
            "classification_status": "classifier_unavailable",
            "classification_source": "hostname_csv",
        }

    classification = classifier.classify(hostname)
    return record | {
        "client_hostname": classification.hostname,
        "security_domain": classification.security_domain,
        "classification_status": classification.status,
        "classification_source": "hostname_csv",
    }


def _classify_inventory(
    policies: list[dict[str, Any]],
    workflows: list[dict[str, Any]],
    jobs: list[dict[str, Any]],
    backups: list[dict[str, Any]],
    source_networker: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    policy_domains: dict[str, set[str]] = defaultdict(set)
    workflow_domains: dict[tuple[str, str], set[str]] = defaultdict(set)
    for record in jobs + backups:
        domain = record["security_domain"]
        policy = record.get("policy_name") or ""
        workflow = record.get("workflow_name") or ""
        if policy:
            policy_domains[policy].add(domain)
        if policy or workflow:
            workflow_domains[(policy, workflow)].add(domain)

    classified_policies = []
    for policy in policies:
        domains = policy_domains.get(policy["policy_name"]) or {"unmapped"}
        for domain in sorted(domains):
            classified_policies.append(
                policy
                | {
                    "source_networker": source_networker,
                    "security_domain": domain,
                    "classification_status": "derived_from_workload",
                    "classification_source": "hostname_csv",
                }
            )

    classified_workflows = []
    for workflow in workflows:
        key = (workflow["policy_name"], workflow["workflow_name"])
        domains = workflow_domains.get(key) or policy_domains.get(workflow["policy_name"]) or {"unmapped"}
        for domain in sorted(domains):
            classified_workflows.append(
                workflow
                | {
                    "source_networker": source_networker,
                    "security_domain": domain,
                    "classification_status": "derived_from_workload",
                    "classification_source": "hostname_csv",
                }
            )
    return classified_policies, classified_workflows


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

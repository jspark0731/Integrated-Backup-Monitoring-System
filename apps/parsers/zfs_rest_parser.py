from __future__ import annotations

from collections import Counter
from typing import Any


def parse_zfs_rest_payload(payloads: dict[str, Any], fallback_name: str) -> dict[str, Any]:
    version = _first_mapping(payloads.get("version"), "version")
    device_name = _string(version.get("os_nodename")) or fallback_name

    pools = _normalize_pools(payloads, device_name)
    projects = _normalize_projects(payloads, device_name)
    filesystems = _normalize_filesystems(payloads, device_name)
    luns = _normalize_luns(payloads, device_name)
    alerts = _normalize_logs(payloads.get("alert_logs"), "alert", device_name) + _normalize_logs(
        payloads.get("fault_logs"),
        "fault",
        device_name,
    )
    alert_counts = Counter(item["severity"] for item in alerts)

    summary = {
        "device_name": device_name,
        "product": _string(version.get("ak_product")) or _string(version.get("hw_product")),
        "os_version": _string(version.get("os_version")),
        "serial_number": _string(version.get("hw_csn")),
        "pool_count": len(pools),
        "project_count": len(projects),
        "filesystem_count": len(filesystems),
        "lun_count": len(luns),
        "alert_count": alert_counts.get("alert", 0),
        "fault_count": alert_counts.get("fault", 0),
        "total_bytes": sum(pool.get("total_bytes") or 0 for pool in pools),
        "used_bytes": sum(pool.get("used_bytes") or 0 for pool in pools),
        "free_bytes": sum(pool.get("free_bytes") or 0 for pool in pools),
        "used_percent": _used_percent(
            sum(pool.get("used_bytes") or 0 for pool in pools),
            sum(pool.get("total_bytes") or 0 for pool in pools),
        ),
    }

    return {
        "summary": summary,
        "pools": pools,
        "projects": projects,
        "filesystems": filesystems,
        "luns": luns,
        "alerts": alerts,
    }


def _normalize_pools(payloads: dict[str, Any], device_name: str) -> list[dict[str, Any]]:
    rows = []
    for item in _items(payloads.get("pools"), "pools"):
        name = _string(item.get("name"))
        detail = _first_mapping(payloads.get(f"pool:{name}"), "pool")
        source = detail or item
        usage = source.get("usage") if isinstance(source.get("usage"), dict) else {}
        total = _float(usage.get("total"))
        used = _float(usage.get("used") or usage.get("usage_total"))
        free = _float(usage.get("free") or usage.get("available"))
        state = _string(source.get("state"))

        rows.append(
            {
                "device_name": device_name,
                "name": name,
                "state": state,
                "up": 1 if state.lower() == "online" else 0,
                "profile": _string(source.get("profile")),
                "owner": _string(source.get("owner")),
                "peer": _string(source.get("peer")),
                "asn": _string(source.get("asn")),
                "scrub_schedule": _string(source.get("scrub_schedule")),
                "total_bytes": total,
                "used_bytes": used,
                "free_bytes": free,
                "used_percent": _used_percent(used, total),
                "compression": _float(usage.get("compression")),
                "dedup_ratio": _float(usage.get("dedupratio")),
                "snapshot_bytes": _float(usage.get("usage_snapshots")),
                "replication_bytes": _float(usage.get("usage_replication")),
            }
        )
    return rows


def _normalize_projects(payloads: dict[str, Any], device_name: str) -> list[dict[str, Any]]:
    rows = []
    for key, payload in payloads.items():
        if not key.startswith("projects:"):
            continue
        pool = key.split(":", 1)[1]
        for project in _items(payload, "projects"):
            name = _string(project.get("name"))
            detail = _first_mapping(payloads.get(f"project:{pool}/{name}"), "project")
            source = detail or project
            rows.append(
                {
                    "device_name": device_name,
                    "pool": pool,
                    "name": name,
                    "mountpoint": _string(source.get("mountpoint")),
                    "creation": _string(source.get("creation")),
                    "dedup": source.get("dedup"),
                    "sharenfs": _string(source.get("sharenfs")),
                    "sharesmb": _string(source.get("sharesmb")),
                    "quota": _float(source.get("quota")),
                    "reservation": _float(source.get("reservation")),
                }
            )
    return rows


def _normalize_filesystems(payloads: dict[str, Any], device_name: str) -> list[dict[str, Any]]:
    rows = []
    for key, payload in payloads.items():
        if not key.startswith("filesystems:"):
            continue
        pool, project = key.split(":", 1)[1].split("/", 1)
        for item in _items(payload, "filesystems"):
            rows.append(
                {
                    "device_name": device_name,
                    "pool": _string(item.get("pool")) or pool,
                    "project": _string(item.get("project")) or project,
                    "name": _string(item.get("name")),
                    "mountpoint": _string(item.get("mountpoint")),
                    "quota": _float(item.get("quota")),
                    "reservation": _float(item.get("reservation")),
                    "usage": item.get("usage") if isinstance(item.get("usage"), dict) else {},
                }
            )
    return rows


def _normalize_luns(payloads: dict[str, Any], device_name: str) -> list[dict[str, Any]]:
    rows = []
    for key, payload in payloads.items():
        if not key.startswith("luns:"):
            continue
        pool, project = key.split(":", 1)[1].split("/", 1)
        for item in _items(payload, "luns"):
            rows.append(
                {
                    "device_name": device_name,
                    "pool": _string(item.get("pool")) or pool,
                    "project": _string(item.get("project")) or project,
                    "name": _string(item.get("name")),
                    "id": _string(item.get("id")),
                    "status": _string(item.get("status")),
                    "volsize": _float(item.get("volsize")),
                    "sparse": item.get("sparse"),
                    "usage": item.get("usage") if isinstance(item.get("usage"), dict) else {},
                }
            )
    return rows


def _normalize_logs(payload: Any, severity: str, device_name: str) -> list[dict[str, Any]]:
    rows = []
    for item in _items(payload, "logs"):
        rows.append(
            {
                "device_name": device_name,
                "severity": severity,
                "timestamp": _string(item.get("timestamp")),
                "summary": _string(item.get("summary")),
                "user": _string(item.get("user")),
                "address": _string(item.get("address")),
                "annotation": _string(item.get("annotation")),
            }
        )
    return rows


def _items(payload: Any, key: str) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _first_mapping(payload: Any, key: str) -> dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get(key), dict):
        return payload[key]
    return {}


def _used_percent(used: float | None, total: float | None) -> float | None:
    if used is None or not total:
        return None
    return round((used / total) * 100, 3)


def _float(value: Any) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()

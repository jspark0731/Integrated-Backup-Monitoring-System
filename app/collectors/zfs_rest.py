from __future__ import annotations

from collections import Counter
from typing import Any
from urllib.parse import quote, urljoin

import httpx

from app.collectors.base import BaseCollector
from app.core.metrics import (
    ZFS_ALERT_COUNT,
    ZFS_API_UP,
    ZFS_CAPACITY_USED_PERCENT,
    ZFS_POOL_STATUS,
)


DEFAULT_ENDPOINTS = {
    "version": "api/system/v1/version",
    "pools": "api/storage/v1/pools",
    "logs": "api/log/v1/logs",
    "alert_logs": "api/log/v1/logs/alert?limit=100",
    "fault_logs": "api/log/v1/logs/fault?limit=100",
}


class ZfsRestCollector(BaseCollector):
    async def _collect_payload(self) -> dict:
        raw = await self._fetch_payloads()
        parsed = parse_zfs_rest_payload(raw, fallback_name=self.name)
        self._publish_metrics(parsed["summary"], parsed["pools"])
        return {
            "summary": parsed["summary"],
            "pools": parsed["pools"],
            "projects": parsed["projects"],
            "filesystems": parsed["filesystems"],
            "luns": parsed["luns"],
            "alerts": parsed["alerts"],
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
                payloads[name] = await self._get_json(client, endpoint, headers, auth)

            for pool in _items(payloads.get("pools"), "pools"):
                pool_name = _string(pool.get("name"))
                if not pool_name:
                    continue
                pool_path = f"api/storage/v1/pools/{_quote(pool_name)}"
                payloads[f"pool:{pool_name}"] = await self._get_json(client, pool_path, headers, auth)
                projects_payload = await self._get_json(client, f"{pool_path}/projects", headers, auth)
                payloads[f"projects:{pool_name}"] = projects_payload

                for project in _items(projects_payload, "projects"):
                    project_name = _string(project.get("name"))
                    if not project_name:
                        continue
                    project_path = f"{pool_path}/projects/{_quote(project_name)}"
                    payloads[f"project:{pool_name}/{project_name}"] = await self._get_json(
                        client,
                        project_path,
                        headers,
                        auth,
                    )
                    payloads[f"filesystems:{pool_name}/{project_name}"] = await self._get_json(
                        client,
                        f"{project_path}/filesystems",
                        headers,
                        auth,
                    )
                    payloads[f"luns:{pool_name}/{project_name}"] = await self._get_json(
                        client,
                        f"{project_path}/luns",
                        headers,
                        auth,
                    )

        return payloads

    async def _get_json(
        self,
        client: httpx.AsyncClient,
        endpoint: str,
        headers: dict[str, str],
        auth: tuple[str, str] | None,
    ) -> Any:
        response = await client.get(self._url(endpoint), headers=headers, auth=auth)
        response.raise_for_status()
        return response.json() if response.content else {}

    def _url(self, endpoint: str) -> str:
        base_url = f"{self.config.base_url.rstrip('/')}/"
        return urljoin(base_url, endpoint.lstrip("/"))

    def _publish_metrics(self, summary: dict[str, Any], pools: list[dict[str, Any]]) -> None:
        device_name = str(summary.get("device_name") or self.name)

        ZFS_API_UP.labels(device_name).set(1)
        ZFS_ALERT_COUNT.labels(device_name, "alert").set(summary.get("alert_count", 0))
        ZFS_ALERT_COUNT.labels(device_name, "fault").set(summary.get("fault_count", 0))

        for pool in pools:
            pool_name = str(pool.get("name") or "unknown")
            ZFS_POOL_STATUS.labels(device_name, pool_name).set(pool.get("up", 0))
            used_percent = pool.get("used_percent")
            if used_percent is not None:
                ZFS_CAPACITY_USED_PERCENT.labels(device_name, pool_name).set(used_percent)


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


def _quote(value: str) -> str:
    return quote(value, safe="")


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

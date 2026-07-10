from __future__ import annotations

from typing import Any

from app.clients.zfs_rest_client import ZfsRestClient
from app.collectors.base import BaseCollector
from app.core.metrics import (
    ZFS_ALERT_COUNT,
    ZFS_API_UP,
    ZFS_CAPACITY_USED_PERCENT,
    ZFS_POOL_STATUS,
)
from app.parsers.zfs_rest_parser import parse_zfs_rest_payload


class ZfsRestCollector(BaseCollector):
    async def _collect_payload(self) -> dict[str, Any]:
        raw = await ZfsRestClient(self.config).fetch_payloads()
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


__all__ = ["ZfsRestCollector", "parse_zfs_rest_payload"]

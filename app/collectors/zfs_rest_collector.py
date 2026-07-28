from __future__ import annotations

from typing import Any

from app.clients.zfs_rest_client import ZfsRestClient
from app.clients.rest_client import endpoint_errors
from app.collectors.base import BaseCollector
from app.core.config import CollectionClass
from app.core.metrics import (
    ZFS_ALERT_COUNT,
    ZFS_API_UP,
    ZFS_CAPACITY_USED_PERCENT,
    ZFS_POOL_STATUS,
)
from app.parsers.zfs_rest_parser import parse_zfs_rest_payload


class ZfsRestCollector(BaseCollector):
    def __init__(self, config) -> None:
        super().__init__(config)
        self.client = ZfsRestClient(config)

    async def _collect_payload(self) -> dict[str, Any]:
        return await self._collect_payload_for_class("fast")

    async def _collect_payload_for_class(
        self,
        collection_class: CollectionClass,
    ) -> dict[str, Any]:
        active_class = collection_class if len(self.config.effective_schedules) > 1 else None
        raw = await self.client.fetch_payloads(active_class)
        parsed = parse_zfs_rest_payload(raw, fallback_name=self.name)
        self._publish_metrics(parsed["summary"], parsed["pools"], active_class)
        errors = endpoint_errors(raw)
        return {
            "summary": parsed["summary"],
            "pools": parsed["pools"],
            "projects": parsed["projects"],
            "filesystems": parsed["filesystems"],
            "luns": parsed["luns"],
            "alerts": parsed["alerts"],
            "raw": raw,
            "collection_status": "partial" if errors else "success",
            "endpoint_errors": errors,
        }

    async def close(self) -> None:
        await self.client.close()

    def _publish_metrics(
        self,
        summary: dict[str, Any],
        pools: list[dict[str, Any]],
        collection_class: CollectionClass | None,
    ) -> None:
        device_name = str(summary.get("device_name") or self.name)

        ZFS_API_UP.labels(device_name).set(1)
        if collection_class in {None, "fast"}:
            ZFS_ALERT_COUNT.labels(device_name, "alert").set(summary.get("alert_count", 0))
            ZFS_ALERT_COUNT.labels(device_name, "fault").set(summary.get("fault_count", 0))

            for pool in pools:
                pool_name = str(pool.get("name") or "unknown")
                ZFS_POOL_STATUS.labels(device_name, pool_name).set(pool.get("up", 0))
                used_percent = pool.get("used_percent")
                if used_percent is not None:
                    ZFS_CAPACITY_USED_PERCENT.labels(device_name, pool_name).set(used_percent)


__all__ = ["ZfsRestCollector", "parse_zfs_rest_payload"]

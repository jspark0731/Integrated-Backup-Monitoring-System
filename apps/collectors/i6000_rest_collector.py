from __future__ import annotations

from typing import Any

from apps.clients.i6000_rest_client import I6000RestClient
from apps.clients.rest_client import endpoint_errors
from apps.collectors.base import BaseCollector
from apps.core.config import CollectionClass
from apps.core.metrics import (
    DEVICE_UP,
    TAPE_DRIVE_ERROR_COUNT,
    TAPE_DRIVE_STATUS,
    TAPE_LIBRARY_STATUS,
    TAPE_MEDIA_COUNT,
    TAPE_ROBOT_STATUS,
    TAPE_SLOT_FREE_COUNT,
    TAPE_SLOT_USED_COUNT,
)
from apps.parsers.i6000_rest_parser import parse_i6000_rest_payload


class I6000RestCollector(BaseCollector):
    def __init__(self, config) -> None:
        super().__init__(config)
        self.client = I6000RestClient(config)

    async def _collect_payload(self) -> dict[str, Any]:
        return await self._collect_payload_for_class("fast")

    async def _collect_payload_for_class(
        self,
        collection_class: CollectionClass,
    ) -> dict[str, Any]:
        active_class = collection_class if len(self.config.effective_schedules) > 1 else None
        raw = await self.client.fetch_payloads(active_class)
        summary = parse_i6000_rest_payload(raw, fallback_name=self.name)
        self._publish_metrics(summary, active_class)
        errors = endpoint_errors(raw)
        return {
            "summary": summary,
            "raw": raw,
            "collection_status": "partial" if errors else "success",
            "endpoint_errors": errors,
        }

    async def close(self) -> None:
        await self.client.close()

    def _publish_metrics(
        self,
        summary: dict[str, Any],
        collection_class: CollectionClass | None,
    ) -> None:
        device_name = str(summary.get("device_name") or self.name)

        if collection_class in {None, "fast"}:
            DEVICE_UP.labels("i6000", device_name).set(1)

        if collection_class in {None, "fast"} and summary.get("library_status") is not None:
            TAPE_LIBRARY_STATUS.labels(device_name).set(summary["library_status"])
        if collection_class in {None, "fast"} and summary.get("slot_used_count") is not None:
            TAPE_SLOT_USED_COUNT.labels(device_name).set(summary["slot_used_count"])
        if collection_class in {None, "fast"} and summary.get("slot_free_count") is not None:
            TAPE_SLOT_FREE_COUNT.labels(device_name).set(summary["slot_free_count"])
        if collection_class in {None, "slow"} and summary.get("media_count") is not None:
            TAPE_MEDIA_COUNT.labels(device_name).set(summary["media_count"])

        if collection_class in {None, "fast"}:
            for robot in summary.get("robots", []):
                TAPE_ROBOT_STATUS.labels(device_name, robot["name"]).set(robot["up"])
            for drive in summary.get("drives", []):
                TAPE_DRIVE_STATUS.labels(device_name, drive["name"]).set(drive["up"])
                TAPE_DRIVE_ERROR_COUNT.labels(device_name, drive["name"]).set(
                    drive.get("error_count", 0)
                )


__all__ = ["I6000RestCollector", "parse_i6000_rest_payload"]

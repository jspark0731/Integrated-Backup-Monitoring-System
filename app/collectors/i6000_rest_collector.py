from __future__ import annotations

from typing import Any

from app.clients.i6000_rest_client import I6000RestClient
from app.collectors.base import BaseCollector
from app.core.metrics import (
    DEVICE_UP,
    TAPE_DRIVE_ERROR_COUNT,
    TAPE_DRIVE_STATUS,
    TAPE_LIBRARY_STATUS,
    TAPE_MEDIA_COUNT,
    TAPE_ROBOT_STATUS,
    TAPE_SLOT_FREE_COUNT,
    TAPE_SLOT_USED_COUNT,
)
from app.parsers.i6000_rest_parser import parse_i6000_rest_payload


class I6000RestCollector(BaseCollector):
    async def _collect_payload(self) -> dict[str, Any]:
        raw = await I6000RestClient(self.config).fetch_payloads()
        summary = parse_i6000_rest_payload(raw, fallback_name=self.name)
        self._publish_metrics(summary)
        return {
            "summary": summary,
            "raw": raw,
        }

    def _publish_metrics(self, summary: dict[str, Any]) -> None:
        device_name = str(summary.get("device_name") or self.name)

        DEVICE_UP.labels("i6000", device_name).set(1)

        if summary.get("library_status") is not None:
            TAPE_LIBRARY_STATUS.labels(device_name).set(summary["library_status"])
        if summary.get("slot_used_count") is not None:
            TAPE_SLOT_USED_COUNT.labels(device_name).set(summary["slot_used_count"])
        if summary.get("slot_free_count") is not None:
            TAPE_SLOT_FREE_COUNT.labels(device_name).set(summary["slot_free_count"])
        if summary.get("media_count") is not None:
            TAPE_MEDIA_COUNT.labels(device_name).set(summary["media_count"])

        for robot in summary.get("robots", []):
            TAPE_ROBOT_STATUS.labels(device_name, robot["name"]).set(robot["up"])
        for drive in summary.get("drives", []):
            TAPE_DRIVE_STATUS.labels(device_name, drive["name"]).set(drive["up"])
            TAPE_DRIVE_ERROR_COUNT.labels(device_name, drive["name"]).set(drive.get("error_count", 0))


__all__ = ["I6000RestCollector", "parse_i6000_rest_payload"]

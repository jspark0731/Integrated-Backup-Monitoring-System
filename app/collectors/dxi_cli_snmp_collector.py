from __future__ import annotations

import asyncio
from typing import Any

from app.clients.dxi_cli_client import DxiCliClient
from app.clients.snmp_client import SnmpClient
from app.collectors.base import BaseCollector
from app.core.metrics import (
    DEVICE_ALERT_COUNT,
    DEVICE_CAPACITY_TOTAL_BYTES,
    DEVICE_CAPACITY_USED_BYTES,
    DEVICE_CAPACITY_USED_PERCENT,
    DEVICE_DEDUP_RATIO,
    DEVICE_INTERFACE_UP,
    DEVICE_REPLICATION_UP,
    DEVICE_UP,
)
from app.parsers.dxi_cli_parser import parse_dxi_cli_outputs


class DXiCliSnmpCollector(BaseCollector):
    async def _collect_payload(self) -> dict[str, Any]:
        snmp_payload = await asyncio.to_thread(SnmpClient(self.config).collect_values)
        cli_outputs = await asyncio.to_thread(DxiCliClient(self.config).run_commands)
        cli_summary = parse_dxi_cli_outputs(cli_outputs, fallback_name=self.name)
        self._publish_metrics(cli_summary)

        return {
            "summary": cli_summary,
            "snmp": snmp_payload,
            "raw": {
                "snmp": snmp_payload,
                "cli": cli_outputs,
            },
        }

    def _publish_metrics(self, summary: dict[str, Any]) -> None:
        device_name = str(summary.get("device_name") or self.name)
        device_type = "dxi"

        DEVICE_UP.labels(device_type, device_name).set(1)

        capacity = summary.get("capacity", {})
        if capacity.get("total_bytes") is not None:
            DEVICE_CAPACITY_TOTAL_BYTES.labels(device_type, device_name).set(capacity["total_bytes"])
        if capacity.get("used_bytes") is not None:
            DEVICE_CAPACITY_USED_BYTES.labels(device_type, device_name).set(capacity["used_bytes"])
        if capacity.get("used_percent") is not None:
            DEVICE_CAPACITY_USED_PERCENT.labels(device_type, device_name).set(capacity["used_percent"])

        if summary.get("dedup_ratio") is not None:
            DEVICE_DEDUP_RATIO.labels(device_type, device_name).set(summary["dedup_ratio"])

        for severity, count in summary.get("alert_counts", {}).items():
            DEVICE_ALERT_COUNT.labels(device_type, device_name, severity).set(count)

        for item in summary.get("replication", []):
            DEVICE_REPLICATION_UP.labels(device_type, device_name, item["name"]).set(item["up"])

        for item in summary.get("interfaces", []):
            DEVICE_INTERFACE_UP.labels(device_type, device_name, item["name"]).set(item["up"])


DxiCliSnmpCollector = DXiCliSnmpCollector

__all__ = ["DXiCliSnmpCollector", "DxiCliSnmpCollector", "parse_dxi_cli_outputs"]

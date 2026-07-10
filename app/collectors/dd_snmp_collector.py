from __future__ import annotations

import asyncio
from typing import Any

from app.clients.snmp_client import SnmpClient
from app.collectors.base import BaseCollector
from app.core.metrics import (
    DD_DDBOOST_CONNECTIONS,
    DD_DDBOOST_STORAGE_UNIT_BYTES,
    DD_DDBOOST_STORAGE_UNIT_COMPRESSION,
    DD_DDBOOST_THROUGHPUT_KBPS,
    DD_DDBOOST_UP,
    DEVICE_ALERT_COUNT,
    DEVICE_CAPACITY_TOTAL_BYTES,
    DEVICE_CAPACITY_USED_BYTES,
    DEVICE_CAPACITY_USED_PERCENT,
    DEVICE_DEDUP_RATIO,
    DEVICE_REPLICATION_UP,
    DEVICE_UP,
)
from app.parsers.dd_snmp_parser import parse_dd_snmp_payload


class DDSnmpCollector(BaseCollector):
    async def _collect_payload(self) -> dict[str, Any]:
        snmp_payload = await asyncio.to_thread(SnmpClient(self.config).collect_values)
        summary = parse_dd_snmp_payload(snmp_payload, fallback_name=self.name)
        self._publish_metrics(summary)
        return {
            "summary": summary,
            "snmp": snmp_payload,
            "raw": {
                "snmp": snmp_payload,
            },
        }

    def _publish_metrics(self, summary: dict[str, Any]) -> None:
        device_name = str(summary.get("device_name") or self.name)
        device_type = "dd"

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
            DEVICE_REPLICATION_UP.labels(device_type, device_name, str(item["name"])).set(item["up"])

        ddboost = summary.get("ddboost", {})
        if ddboost.get("enabled") is not None:
            DD_DDBOOST_UP.labels(device_name).set(1 if ddboost["enabled"] else 0)

        for direction, value in ddboost.get("connections", {}).items():
            if value is not None:
                DD_DDBOOST_CONNECTIONS.labels(device_name, direction).set(value)

        for stream, value in ddboost.get("throughput_kbps", {}).items():
            if value is not None:
                DD_DDBOOST_THROUGHPUT_KBPS.labels(device_name, stream).set(value)

        for storage_unit in ddboost.get("storage_units", []):
            name = str(storage_unit.get("name") or storage_unit.get("instance"))
            for key in ("bytes", "metadata_bytes", "pre_compression_bytes", "report_physical_size", "bytes_hc"):
                if storage_unit.get(key) is not None:
                    DD_DDBOOST_STORAGE_UNIT_BYTES.labels(device_name, name, key).set(storage_unit[key])
            for key in ("global_compression", "local_compression"):
                if storage_unit.get(key) is not None:
                    DD_DDBOOST_STORAGE_UNIT_COMPRESSION.labels(device_name, name, key).set(storage_unit[key])


__all__ = ["DDSnmpCollector", "parse_dd_snmp_payload"]

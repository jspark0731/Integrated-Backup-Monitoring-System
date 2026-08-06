from __future__ import annotations

from apps.collectors.base import BaseCollector
from apps.core.config import CollectorConfig


def build_collector(config: CollectorConfig) -> BaseCollector:
    if config.type == "i6000" and config.protocol == "snmp":
        raise ValueError("i6000 SNMP collection is not supported; use protocol=rest")
    if config.type == "DXi" and config.protocol in {"snmp", "ssh"}:
        raise ValueError("DXi SNMP/legacy collection is not supported; use protocol=cli")
    if config.protocol == "snmp" and config.type == "DD":
        from apps.collectors.dd_snmp_collector import DDSnmpCollector

        return DDSnmpCollector(config)
    if config.protocol == "rest" and config.type == "i6000":
        from apps.collectors.i6000_rest_collector import I6000RestCollector

        return I6000RestCollector(config)
    if config.protocol == "rest" and config.type == "Networker":
        from apps.collectors.networker_rest_collector import NetworkerRestCollector

        return NetworkerRestCollector(config)
    if config.protocol == "rest" and config.type == "ZFS":
        from apps.collectors.zfs_rest_collector import ZfsRestCollector

        return ZfsRestCollector(config)
    if config.protocol == "cli" and config.type == "DXi":
        from apps.collectors.dxi_cli_collector import DXiCliCollector

        return DXiCliCollector(config)
    raise ValueError(f"Unsupported collector protocol: {config.protocol}")

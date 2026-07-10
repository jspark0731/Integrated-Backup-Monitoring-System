from __future__ import annotations

from app.collectors.base import BaseCollector
from app.core.config import CollectorConfig


def build_collector(config: CollectorConfig) -> BaseCollector:
    if config.type == "i6000" and config.protocol == "snmp":
        raise ValueError("i6000 SNMP collection is not supported; use protocol=rest")
    if config.type == "DXi" and config.protocol in {"snmp", "ssh", "cli"}:
        raise ValueError("DXi standalone SNMP/CLI collection is not supported; use protocol=cli_snmp")
    if config.protocol == "snmp" and config.type == "DD":
        from app.collectors.dd_snmp_collector import DDSnmpCollector

        return DDSnmpCollector(config)
    if config.protocol == "rest" and config.type == "i6000":
        from app.collectors.i6000_rest_collector import I6000RestCollector

        return I6000RestCollector(config)
    if config.protocol == "rest" and config.type == "Networker":
        from app.collectors.networker_rest_collector import NetworkerRestCollector

        return NetworkerRestCollector(config)
    if config.protocol == "rest" and config.type == "ZFS":
        from app.collectors.zfs_rest_collector import ZfsRestCollector

        return ZfsRestCollector(config)
    if config.protocol == "cli_snmp" and config.type == "DXi":
        from app.collectors.dxi_cli_snmp_collector import DXiCliSnmpCollector

        return DXiCliSnmpCollector(config)
    raise ValueError(f"Unsupported collector protocol: {config.protocol}")

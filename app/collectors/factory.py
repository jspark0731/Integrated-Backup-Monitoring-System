from __future__ import annotations

from app.collectors.base import BaseCollector
from app.core.config import CollectorConfig


def build_collector(config: CollectorConfig) -> BaseCollector:
    if config.protocol == "snmp":
        from app.collectors.snmp import SnmpCollector

        return SnmpCollector(config)
    if config.protocol == "rest" and config.type == "i6000":
        from app.collectors.i6000_rest import I6000RestCollector

        return I6000RestCollector(config)
    if config.protocol == "rest" and config.type == "Networker":
        from app.collectors.networker_rest import NetworkerRestCollector

        return NetworkerRestCollector(config)
    if config.protocol == "rest":
        from app.collectors.rest import RestCollector

        return RestCollector(config)
    if config.protocol in {"ssh", "cli"} and config.type == "DXi":
        from app.collectors.dxi_cli import DxiCliCollector

        return DxiCliCollector(config)
    raise ValueError(f"Unsupported collector protocol: {config.protocol}")

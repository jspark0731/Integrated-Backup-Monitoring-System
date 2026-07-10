from __future__ import annotations

import asyncio

from app.clients.snmp_client import SnmpClient
from app.collectors.base import BaseCollector


class DDSnmpCollector(BaseCollector):
    async def _collect_payload(self) -> dict:
        return await asyncio.to_thread(SnmpClient(self.config).collect_values)


__all__ = ["DDSnmpCollector"]

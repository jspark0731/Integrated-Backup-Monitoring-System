from __future__ import annotations

import asyncio
from typing import Any

from app.core.config import CollectorConfig


class SnmpClient:
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config

    def collect_values(self) -> dict:
        return asyncio.run(self._collect_values())

    async def _collect_values(self) -> dict:
        from pysnmp.hlapi.v3arch.asyncio import (
            CommunityData,
            ContextData,
            ObjectIdentity,
            ObjectType,
            SnmpEngine,
            UdpTransportTarget,
            get_cmd,
            walk_cmd,
        )

        mp_model = 0 if self.config.version == "1" else 1
        values = {}

        for metric_name, oid in self.config.oids.items():
            error_indication, error_status, error_index, var_binds = await get_cmd(
                SnmpEngine(),
                CommunityData(self.config.community, mpModel=mp_model),
                await UdpTransportTarget.create(
                    (self.config.host, self.config.snmp_port or self.config.port),
                    timeout=self.config.timeout_seconds,
                    retries=self.config.retries,
                ),
                ContextData(),
                ObjectType(ObjectIdentity(oid)),
                lookupMib=False,
            )

            self._raise_on_error(error_indication, error_status, error_index, var_binds)

            for _, value in var_binds:
                values[metric_name] = value.prettyPrint()

        for metric_name, base_oid in self.config.walk_oids.items():
            values[metric_name] = await self._walk_oid(
                walk_cmd=walk_cmd,
                snmp_engine=SnmpEngine(),
                community_data=CommunityData(self.config.community, mpModel=mp_model),
                transport_target=await UdpTransportTarget.create(
                    (self.config.host, self.config.snmp_port or self.config.port),
                    timeout=self.config.timeout_seconds,
                    retries=self.config.retries,
                ),
                context_data=ContextData(),
                object_type=ObjectType(ObjectIdentity(base_oid)),
                base_oid=base_oid,
            )

        return values

    @staticmethod
    async def _walk_oid(
        *,
        walk_cmd,
        snmp_engine,
        community_data,
        transport_target,
        context_data,
        object_type,
        base_oid: str,
    ) -> list[dict[str, str]]:
        rows = []
        iterator = walk_cmd(
            snmp_engine,
            community_data,
            transport_target,
            context_data,
            object_type,
            lexicographicMode=False,
            lookupMib=False,
        )

        async for error_indication, error_status, error_index, var_binds in iterator:
            SnmpClient._raise_on_error(error_indication, error_status, error_index, var_binds)

            for oid, value in var_binds:
                oid_text = oid.prettyPrint()
                rows.append(
                    {
                        "oid": oid_text,
                        "instance": SnmpClient._instance_from_oid(base_oid, oid_text),
                        "value": value.prettyPrint(),
                    }
                )

        return rows

    @staticmethod
    def _raise_on_error(error_indication: Any, error_status: Any, error_index: Any, var_binds: Any) -> None:
        if error_indication:
            raise RuntimeError(str(error_indication))
        if error_status:
            failing_oid = SnmpClient._failing_oid(error_index, var_binds)
            error_text = error_status.prettyPrint() if hasattr(error_status, "prettyPrint") else str(error_status)
            raise RuntimeError(f"{error_text} at {failing_oid}")

    @staticmethod
    def _failing_oid(error_index: Any, var_binds: Any) -> str:
        try:
            index = int(error_index)
        except (TypeError, ValueError):
            return "unknown"

        if index <= 0:
            return "unknown"

        try:
            failing_bind = var_binds[index - 1]
            return str(failing_bind[0])
        except (IndexError, TypeError, KeyError):
            return "unknown"

    @staticmethod
    def _instance_from_oid(base_oid: str, oid: str) -> str:
        normalized_base = base_oid.rstrip(".")
        if oid == normalized_base:
            return ""
        if oid.startswith(f"{normalized_base}."):
            return oid[len(normalized_base) + 1 :]
        return oid

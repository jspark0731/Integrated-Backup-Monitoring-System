from __future__ import annotations

from app.core.config import CollectorConfig


class SnmpClient:
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config

    def collect_values(self) -> dict:
        from pysnmp.hlapi import (
            CommunityData,
            ContextData,
            ObjectIdentity,
            ObjectType,
            SnmpEngine,
            UdpTransportTarget,
            getCmd,
            nextCmd,
        )

        mp_model = 0 if self.config.version == "1" else 1
        values = {}

        for metric_name, oid in self.config.oids.items():
            iterator = getCmd(
                SnmpEngine(),
                CommunityData(self.config.community, mpModel=mp_model),
                UdpTransportTarget(
                    (self.config.host, self.config.snmp_port or self.config.port),
                    timeout=self.config.timeout_seconds,
                    retries=self.config.retries,
                ),
                ContextData(),
                ObjectType(ObjectIdentity(oid)),
            )
            error_indication, error_status, error_index, var_binds = next(iterator)

            if error_indication:
                raise RuntimeError(str(error_indication))
            if error_status:
                failing_oid = var_binds[int(error_index) - 1][0] if error_index else "unknown"
                raise RuntimeError(f"{error_status.prettyPrint()} at {failing_oid}")

            for _, value in var_binds:
                values[metric_name] = value.prettyPrint()

        for metric_name, base_oid in self.config.walk_oids.items():
            values[metric_name] = self._walk_oid(
                next_cmd=nextCmd,
                snmp_engine=SnmpEngine(),
                community_data=CommunityData(self.config.community, mpModel=mp_model),
                transport_target=UdpTransportTarget(
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
    def _walk_oid(
        *,
        next_cmd,
        snmp_engine,
        community_data,
        transport_target,
        context_data,
        object_type,
        base_oid: str,
    ) -> list[dict[str, str]]:
        rows = []
        iterator = next_cmd(
            snmp_engine,
            community_data,
            transport_target,
            context_data,
            object_type,
            lexicographicMode=False,
        )

        for error_indication, error_status, error_index, var_binds in iterator:
            if error_indication:
                raise RuntimeError(str(error_indication))
            if error_status:
                failing_oid = var_binds[int(error_index) - 1][0] if error_index else "unknown"
                raise RuntimeError(f"{error_status.prettyPrint()} at {failing_oid}")

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
    def _instance_from_oid(base_oid: str, oid: str) -> str:
        normalized_base = base_oid.rstrip(".")
        if oid == normalized_base:
            return ""
        if oid.startswith(f"{normalized_base}."):
            return oid[len(normalized_base) + 1 :]
        return oid

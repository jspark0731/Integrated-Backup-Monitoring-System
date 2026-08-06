from __future__ import annotations

from typing import Any

GIB = 1024**3


def parse_dd_snmp_payload(payload: dict[str, Any], fallback_name: str) -> dict[str, Any]:
    device_name = _first_value(payload.get("system_hostname")) or fallback_name
    capacity = _parse_capacity(payload)
    ddboost = _parse_ddboost(payload)

    return {
        "device_name": device_name,
        "serial_number": _first_value(payload.get("system_serial_number")),
        "model": _first_value(payload.get("system_model")),
        "version": _first_value(payload.get("system_version")),
        "file_system_status": _first_value(payload.get("file_system_status")),
        "capacity": capacity,
        "dedup_ratio": _first_float(payload.get("file_system_total_compression_factor")),
        "reduction_percent": _first_float(payload.get("file_system_reduction_percent")),
        "alert_counts": _count_values(payload.get("current_alert_severity")),
        "replication": _parse_replication(payload),
        "ddboost": ddboost,
    }


def _parse_capacity(payload: dict[str, Any]) -> dict[str, float | None]:
    total = _first_float(payload.get("file_system_space_size"))
    used = _first_float(payload.get("file_system_space_used"))
    available = _first_float(payload.get("file_system_space_available"))
    used_percent = _first_float(payload.get("file_system_percent_used"))
    return {
        "total_bytes": total * GIB if total is not None else None,
        "used_bytes": used * GIB if used is not None else None,
        "available_bytes": available * GIB if available is not None else None,
        "used_percent": used_percent,
    }


def _parse_ddboost(payload: dict[str, Any]) -> dict[str, Any]:
    status = _first_value(payload.get("ddboost_status"))
    return {
        "enabled": _enabled(status),
        "status": status,
        "throughput_kbps": {
            "pre_compression": _first_float(payload.get("ddboost_pre_comp_kbps")),
            "post_compression": _first_float(payload.get("ddboost_post_comp_kbps")),
            "network": _first_float(payload.get("ddboost_network_kbps")),
            "read": _first_float(payload.get("ddboost_read_kbps")),
        },
        "connections": {
            "backup": _first_float(payload.get("ddboost_backup_connections")),
            "restore": _first_float(payload.get("ddboost_restore_connections")),
        },
        "compression_ratio": _first_float(payload.get("ddboost_compression_ratio")),
        "ifgroups": _table_rows(
            {
                "name": payload.get("ddboost_ifgroup_name"),
                "status": payload.get("ddboost_ifgroup_status"),
            }
        ),
        "users": _table_rows(
            {
                "name": payload.get("ddboost_user_name"),
                "tenant_unit": payload.get("ddboost_user_default_tenant_unit"),
            }
        ),
        "storage_units": _table_rows(
            {
                "name": payload.get("ddboost_storage_unit_name"),
                "bytes": payload.get("ddboost_storage_unit_bytes"),
                "global_compression": payload.get("ddboost_storage_unit_global_comp"),
                "local_compression": payload.get("ddboost_storage_unit_local_comp"),
                "metadata_bytes": payload.get("ddboost_storage_unit_metadata"),
                "status": payload.get("ddboost_storage_unit_status"),
                "pre_compression_bytes": payload.get("ddboost_storage_unit_pre_comp"),
                "user": payload.get("ddboost_storage_unit_user"),
                "report_physical_size": payload.get("ddboost_storage_unit_report_physical_size"),
                "bytes_hc": payload.get("ddboost_storage_unit_bytes_hc"),
            },
            numeric_keys={
                "bytes",
                "global_compression",
                "local_compression",
                "metadata_bytes",
                "pre_compression_bytes",
                "report_physical_size",
                "bytes_hc",
            },
        ),
    }


def _parse_replication(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = _table_rows(
        {
            "source": payload.get("replication_source"),
            "destination": payload.get("replication_destination"),
            "state": payload.get("replication_state"),
            "status": payload.get("replication_status"),
        }
    )
    for row in rows:
        name = row.get("destination") or row.get("source") or row["instance"]
        state = row.get("state") or row.get("status")
        row["name"] = name
        row["up"] = 1 if _state_up(state) else 0
    return rows


def _table_rows(
    columns: dict[str, Any],
    *,
    numeric_keys: set[str] | None = None,
) -> list[dict[str, Any]]:
    numeric_keys = numeric_keys or set()
    by_instance: dict[str, dict[str, Any]] = {}
    for key, values in columns.items():
        for row in _walk_rows(values):
            instance = row["instance"]
            target = by_instance.setdefault(instance, {"instance": instance})
            target[key] = _to_float(row["value"]) if key in numeric_keys else row["value"]
    return list(by_instance.values())


def _walk_rows(value: Any) -> list[dict[str, str]]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict) and "instance" in row and "value" in row]
    return []


def _first_value(value: Any) -> str | None:
    if isinstance(value, list):
        rows = _walk_rows(value)
        if rows:
            return rows[0]["value"]
        return None
    if value is None:
        return None
    return str(value)


def _first_float(value: Any) -> float | None:
    return _to_float(_first_value(value))


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip().rstrip("%").split(":")[0])
    except ValueError:
        return None


def _count_values(value: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in _walk_rows(value):
        normalized = row["value"].strip().lower()
        if normalized:
            counts[normalized] = counts.get(normalized, 0) + 1
    return counts


def _enabled(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"1", "enabled", "up", "true"}:
        return True
    if normalized in {"2", "disabled", "down", "false"}:
        return False
    return None


def _state_up(value: Any) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "enabled", "up", "ok", "online", "normal", "connected"}

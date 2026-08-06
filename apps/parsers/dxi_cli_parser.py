from __future__ import annotations

import re
from typing import Any


_STATUS_RANK = {"normal": 0, "unknown": 1, "warning": 2, "critical": 3}


def parse_dxi_cli_outputs(outputs: dict[str, str], fallback_name: str) -> dict[str, Any]:
    hardware = {
        "common_components": _parse_records(outputs.get("common_components", "")),
        "storage_arrays": _parse_records(outputs.get("storage_arrays", "")),
        "system_board": _parse_records(outputs.get("system_board", "")),
    }
    vtls = _parse_vtls(outputs.get("vtls", ""))
    admin_alerts = _parse_records(outputs.get("admin_alerts", ""))
    service_tickets = _parse_records(outputs.get("service_tickets", ""))
    state = _overall_hardware_state(hardware)
    hostname = _first_match(
        outputs.get("network_config", ""),
        [r"^Hostname\s*=\s*([^\r\n]+)", r"^HOSTNAME\s*=\s*([^\r\n]+)"],
    )
    reduction = _parse_data_reduction(outputs.get("dedup", ""))
    alert_counts = _alert_counts(admin_alerts, service_tickets)

    return {
        "device_name": hostname or fallback_name,
        "state": state,
        "capacity": _parse_capacity(outputs.get("capacity", "")),
        # Kept for the existing Prometheus and Elasticsearch consumers.
        "dedup_ratio": reduction["deduplication_ratio"],
        "data_reduction": reduction,
        "hardware": hardware,
        "vtls": vtls,
        "interfaces": _parse_interfaces(outputs.get("interfaces", "")),
        "network": _parse_network_config(outputs.get("network_config", "")),
        "admin_alerts": admin_alerts,
        "service_tickets": service_tickets,
        "alert_counts": alert_counts,
    }


def _parse_capacity(text: str) -> dict[str, float | None]:
    total = _size_field(text, "Disk Capacity")
    available = _size_field(text, "Available Disk Space")
    free = _size_field(text, "Free Space")
    reclaimable = _size_field(text, "Reclaimable Space")
    used = _size_field(text, "Used Disk Space")
    deduplicated = _size_field(text, "Deduplicated Data")
    metadata = _size_field(text, "System Metadata")
    not_deduplicated = _size_field(text, "Data Not Intended for Deduplication")
    used_percent = round((used / total) * 100, 3) if total and used is not None else None
    return {
        "total_bytes": total,
        "used_bytes": used,
        "available_bytes": available,
        "free_bytes": free,
        "reclaimable_bytes": reclaimable,
        "deduplicated_bytes": deduplicated,
        "metadata_bytes": metadata,
        "not_deduplicated_bytes": not_deduplicated,
        "used_percent": used_percent,
    }


def _parse_data_reduction(text: str) -> dict[str, float | None]:
    return {
        "before_bytes": _size_field(text, "Data Size Before Reduction"),
        "after_bytes": _size_field(text, "Data Size After Reduction"),
        "total_reduction_ratio": _ratio_field(text, "Total Reduction Ratio"),
        "deduplication_ratio": _ratio_field(text, "Deduplication Ratio"),
        "compression_ratio": _ratio_field(text, "Compression Ratio"),
    }


def _parse_vtls(text: str) -> list[dict[str, Any]]:
    rows = []
    for record in _parse_records(text):
        if "name" not in record:
            continue
        rows.append(
            {
                "name": record.get("name"),
                "mode": _lower(record.get("mode")),
                "online": 1 if _lower(record.get("mode")) == "online" else 0,
                "model": record.get("model"),
                "drive_model": record.get("drivemodel"),
                "drive_count": _int(record.get("drives")),
                "media_count": _int(record.get("media")),
                "slot_count": _int(record.get("slots")),
                "ie_slot_count": _int(record.get("ieslots")),
                "serial": record.get("serial"),
                "dedup_enabled": _lower(record.get("dedup")) == "enabled",
                "replication_enabled": _lower(record.get("replication")) == "enabled",
            }
        )
    return rows


def _parse_interfaces(text: str) -> list[dict[str, Any]]:
    rows = []
    for record in _parse_records(text):
        name = record.get("Name")
        if not name:
            continue
        status = _lower(record.get("Status")) or "unknown"
        speed = _first_match(str(record.get("Value", "")), [r"([\d.]+)\s*Mb/s"])
        rows.append(
            {
                "name": name,
                "state": status,
                "up": 1 if status == "up" else 0,
                "speed_bps": float(speed) * 1_000_000 if speed else None,
            }
        )
    return rows


def _parse_network_config(text: str) -> dict[str, Any]:
    hostname = _first_match(text, [r"^Hostname\s*=\s*([^\r\n]+)", r"^HOSTNAME\s*=\s*([^\r\n]+)"])
    gateway = _first_match(text, [r"^GATEWAY\s*=\s*([^\r\n]+)"])
    configured = []
    for block in re.split(r"^\*{5,}\s*$", text, flags=re.MULTILINE):
        device = _first_match(block, [r"^DEVICE\s*=\s*([^\r\n]+)"])
        if not device:
            continue
        configured.append(
            {
                "name": device,
                "type": _first_match(block, [r"^TYPE\s*=\s*([^\r\n]+)"]),
                "master": _first_match(block, [r"^MASTER\s*=\s*([^\r\n]+)"]),
                "ip_address": _first_match(block, [r"^IPADDR\s*=\s*([^\r\n]+)"]),
                "netmask": _first_match(block, [r"^NETMASK\s*=\s*([^\r\n]+)"]),
                "mtu": _int(_first_match(block, [r"^MTU\s*=\s*(\d+)"])),
            }
        )
    return {"hostname": hostname, "gateway": gateway, "configured_interfaces": configured}


def _parse_records(text: str) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for line in text.splitlines():
        if re.match(r"^\s*\[[^]]+\]\s*$", line):
            if current:
                records.append(current)
            current = {}
            continue
        match = re.match(r"^\s*([^=]+?)\s*=\s*(.*?)\s*$", line)
        if match and current is not None:
            current[match.group(1).strip()] = match.group(2).strip()
    if current:
        records.append(current)
    return records


def _overall_hardware_state(hardware: dict[str, list[dict[str, str]]]) -> str:
    overall = "normal"
    found = False
    for records in hardware.values():
        for record in records:
            if "Status" not in record:
                continue
            found = True
            normalized = _normalize_status(record["Status"])
            if _STATUS_RANK[normalized] > _STATUS_RANK[overall]:
                overall = normalized
    return overall if found else "unknown"


def _normalize_status(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in {"normal", "ok", "up", "online", "healthy"}:
        return "normal"
    if normalized in {"warning", "degraded"}:
        return "warning"
    if normalized in {"critical", "failed", "failure", "error"}:
        return "critical"
    return "unknown"


def _alert_counts(admin_alerts: list[dict[str, str]], tickets: list[dict[str, str]]) -> dict[str, int]:
    counts = {"critical": 0, "warning": 0, "info": 0, "unclassified": len(admin_alerts)}
    priority_map = {"high": "critical", "middle": "warning", "low": "info"}
    for ticket in tickets:
        severity = priority_map.get(_lower(ticket.get("Priority")))
        if severity:
            counts[severity] += 1
        else:
            counts["unclassified"] += 1
    counts["total"] = len(admin_alerts) + len(tickets)
    return counts


def _size_field(text: str, label: str) -> float | None:
    value = _first_match(text, [rf"^\s*-?\s*{re.escape(label)}\s*=\s*([\d.]+\s*[KMGTPE]?i?B)"])
    return _parse_size(value)


def _ratio_field(text: str, label: str) -> float | None:
    value = _first_match(text, [rf"^\s*-?\s*{re.escape(label)}\s*=\s*([\d.]+)\s*:\s*1"])
    return _float(value)


def _first_match(text: str, patterns: list[str]) -> str | None:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        if match:
            return match.group(1).strip()
    return None


def _parse_size(value: str | None) -> float | None:
    if value is None:
        return None
    match = re.match(r"([\d.]+)\s*([KMGTPE]?i?B)", value.strip(), flags=re.IGNORECASE)
    if not match:
        return None
    number = float(match.group(1))
    unit = match.group(2).upper()
    binary = "IB" in unit
    exponent = {"B": 0, "KB": 1, "KIB": 1, "MB": 2, "MIB": 2, "GB": 3, "GIB": 3,
                "TB": 4, "TIB": 4, "PB": 5, "PIB": 5, "EB": 6, "EIB": 6}[unit]
    return int(round(number * ((1024 if binary else 1000) ** exponent)))


def _lower(value: Any) -> str:
    return str(value).strip().lower() if value is not None else ""


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

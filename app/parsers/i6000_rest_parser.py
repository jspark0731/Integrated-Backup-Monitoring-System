from __future__ import annotations

from collections import Counter
from typing import Any


def parse_i6000_rest_payload(payloads: dict[str, Any], fallback_name: str) -> dict[str, Any]:
    ping = _first_mapping(_find_values(payloads.get("ping"), "ping"))
    physical_library = _first_mapping(_find_values(payloads.get("physical_library"), "physicalLibrary"))
    status = _first_mapping(_find_values(payloads.get("status"), "libraryStatus"))

    device_name = (
        _string(physical_library.get("name"))
        or _string(ping.get("serialNumber"))
        or _string(ping.get("productName"))
        or fallback_name
    )
    state = _first_not_none(_int(status.get("state")), _int(physical_library.get("state")))
    mode = _first_not_none(_int(status.get("mode")), _int(physical_library.get("mode")))
    library_status = 1 if state == 1 and mode == 1 else 0 if state is not None or mode is not None else None

    drive_error_counts = _drive_error_counts(payloads.get("ras_tickets"))
    drives = _parse_drives(status, payloads.get("drives"), drive_error_counts)
    ras_status = _parse_ras_status(status, payloads.get("ras_status"))
    towers = _parse_towers(status, payloads.get("towers"))
    ie_stations = _parse_ie_stations(payloads.get("ie_stations"))

    return {
        "device_name": device_name,
        "product_name": _string(ping.get("productName")) or _string(physical_library.get("productId")),
        "serial_number": _string(ping.get("serialNumber")) or _string(physical_library.get("serialNumber")),
        "firmware_version": _string(ping.get("firmwareVersion")) or _string(physical_library.get("firmwareVersion")),
        "vendor": _string(ping.get("vendor")) or _string(physical_library.get("vendorId")),
        "library_state": state,
        "library_mode": mode,
        "library_status": library_status,
        "snmp_started": _boolish(status.get("snmpStarted")),
        "ras_status": ras_status,
        "ras_opened_tickets": _first_not_none(_ras_opened_tickets(status), _ticket_count(payloads.get("ras_tickets"))),
        "robots": _parse_robots(status),
        "partitions": _parse_partitions(status),
        "drives": drives,
        "towers": towers,
        "library_main_door_open": any(item.get("door_opened") is True for item in towers),
        "ie_stations": ie_stations,
        "slot_used_count": _segment_slot_count(payloads.get("segments_storage_used")),
        "slot_free_count": _segment_slot_count(payloads.get("segments_storage_available")),
        "media_count": len(_find_values(payloads.get("media"), "media")),
        "ras_ticket_counts": _ras_ticket_counts(payloads.get("ras_tickets")),
    }


def _parse_robots(status: dict[str, Any]) -> list[dict[str, Any]]:
    robots = []
    for item in _as_list(status.get("robot")):
        if not isinstance(item, dict):
            continue
        name = _string(item.get("serialNumber")) or _string(item.get("location")) or "robot"
        robot_status = _int(item.get("status"))
        state = _int(item.get("state"))
        robots.append(
            {
                "name": name,
                "status": robot_status,
                "state": state,
                "up": 1 if robot_status in {1, None} and state in {1, None} else 0,
            }
        )
    return robots


def _parse_partitions(status: dict[str, Any]) -> list[dict[str, Any]]:
    partitions = []
    for item in _as_list(status.get("partition")):
        if not isinstance(item, dict):
            continue
        partitions.append(
            {
                "name": _string(item.get("name")) or "partition",
                "mode": _int(item.get("mode")),
                "type": _int(item.get("type")),
            }
        )
    return partitions


def _parse_drives(
    status: dict[str, Any],
    drive_payload: Any,
    drive_error_counts: Counter[str],
) -> list[dict[str, Any]]:
    status_drives = [item for item in _as_list(status.get("drive")) if isinstance(item, dict)]
    detailed_drives = [_normalize_drive_detail(item) for item in _find_values(drive_payload, "drive") if isinstance(item, dict)]

    detail_by_serial = {
        detail["serial_number"]: detail for detail in detailed_drives if detail.get("serial_number")
    }

    rows = []
    for item in status_drives or detailed_drives:
        serial = _drive_serial(item)
        detail = detail_by_serial.get(serial, {})
        name = serial or _string(detail.get("name")) or _string(item.get("location")) or "drive"
        mode = _int(item.get("mode"))
        state = _int(item.get("state"))
        rows.append(
            {
                "name": name,
                "serial_number": serial,
                "model": _string(detail.get("model")),
                "mode": mode,
                "state": state,
                "up": 1 if mode in {1, None} and state in {1, None} else 0,
                "error_count": drive_error_counts.get(serial, 0),
            }
        )
    return rows


def _parse_towers(status: dict[str, Any], tower_payload: Any) -> list[dict[str, Any]]:
    source = _find_values(tower_payload, "tower")
    if not source:
        source = _as_list(status.get("tower"))

    rows = []
    for item in source:
        if not isinstance(item, dict):
            continue
        tower_id = _string(item.get("id")) or _string(item.get("frameNumber")) or _string(item.get("serialNumber")) or "tower"
        door_opened = _boolish(item.get("doorOpened"))
        rows.append(
            {
                "name": tower_id,
                "serial_number": _string(item.get("serialNumber")),
                "mode": _int(item.get("mode")),
                "state": _int(item.get("state")),
                "status": _int(item.get("status")),
                "door_opened": door_opened,
            }
        )
    return rows


def _parse_ie_stations(payload: Any) -> list[dict[str, Any]]:
    rows = []
    for item in _find_values(payload, "ieStation"):
        if not isinstance(item, dict):
            continue
        station_id = _string(item.get("number")) or _string(item.get("id")) or _string(item.get("name")) or "ieStation"
        rows.append(
            {
                "name": station_id,
                "status": _int(item.get("status")),
                "state": _int(item.get("state")),
                "mode": _int(item.get("mode")),
                "lock": _int(item.get("lock")),
            }
        )
    return rows


def _parse_ras_status(status: dict[str, Any], ras_payload: Any) -> list[dict[str, int | None]]:
    raw_status = []
    ras = status.get("ras") if isinstance(status.get("ras"), dict) else {}
    raw_status.extend(_as_list(ras.get("status")))
    raw_status.extend(_find_values(ras_payload, "RASGroupStatus"))
    raw_status.extend(_find_values(ras_payload, "status"))

    rows = []
    for item in raw_status:
        if not isinstance(item, dict):
            continue
        if "group" not in item or "status" not in item:
            continue
        rows.append({"group": _int(item.get("group")), "status": _int(item.get("status"))})
    return rows


def _ras_opened_tickets(status: dict[str, Any]) -> int | None:
    ras = status.get("ras") if isinstance(status.get("ras"), dict) else {}
    return _first_not_none(_int(ras.get("openedTickets")), _int(status.get("openedTickets")))


def _normalize_drive_detail(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": _string(item.get("name")),
        "serial_number": _drive_serial(item),
        "model": _string(item.get("model")) or _string(item.get("productId")),
    }


def _drive_serial(item: dict[str, Any]) -> str:
    return (
        _string(item.get("logicalSerialNumber"))
        or _string(item.get("physicalSerialNumber"))
        or _string(item.get("serialNumber"))
        or _string(item.get("serial_number"))
        or ""
    )


def _drive_error_counts(payload: Any) -> Counter[str]:
    counts: Counter[str] = Counter()
    for ticket in _find_values(payload, "ticket") + _find_values(payload, "rasTicket"):
        if not isinstance(ticket, dict):
            continue
        group_status = ticket.get("groupStatus") if isinstance(ticket.get("groupStatus"), dict) else {}
        group = _int(group_status.get("group"))
        if group == 4:
            counts[_string(ticket.get("serialNumber")) or "unknown"] += 1
    return counts


def _ticket_count(payload: Any) -> int:
    return len(_find_values(payload, "ticket") + _find_values(payload, "rasTicket"))


def _ras_ticket_counts(payload: Any) -> dict[str, int]:
    counts: Counter[str] = Counter()
    severities = {
        1: "critical",
        2: "degraded",
        3: "warning",
        4: "attention",
        5: "informational",
    }
    for ticket in _find_values(payload, "ticket") + _find_values(payload, "rasTicket"):
        if not isinstance(ticket, dict):
            continue
        severity = severities.get(_int(ticket.get("severity")), "unknown")
        counts[severity] += 1
    return dict(counts)


def _segment_slot_count(payload: Any) -> int:
    total = 0
    for segment in _find_values(payload, "segment"):
        if not isinstance(segment, dict):
            continue
        size = _int(segment.get("size"))
        total += size if size is not None else 1
    return total


def _find_values(value: Any, key: str) -> list[Any]:
    matches = []
    if isinstance(value, dict):
        for item_key, item_value in value.items():
            if item_key.lower() == key.lower():
                matches.extend(_as_list(item_value))
            matches.extend(_find_values(item_value, key))
    elif isinstance(value, list):
        for item in value:
            matches.extend(_find_values(item, key))
    return matches


def _first_mapping(values: list[Any]) -> dict[str, Any]:
    for value in values:
        if isinstance(value, dict):
            return value
    return {}


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _boolish(value: Any) -> bool | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    return None


def _first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None

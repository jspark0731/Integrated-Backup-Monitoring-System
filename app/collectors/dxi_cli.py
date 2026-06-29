from __future__ import annotations

import asyncio
import re
from typing import Any

from app.collectors.base import BaseCollector
from app.core.metrics import (
    DEVICE_ALERT_COUNT,
    DEVICE_CAPACITY_TOTAL_BYTES,
    DEVICE_CAPACITY_USED_BYTES,
    DEVICE_CAPACITY_USED_PERCENT,
    DEVICE_DEDUP_RATIO,
    DEVICE_INTERFACE_UP,
    DEVICE_REPLICATION_UP,
    DEVICE_UP,
)


class DxiCliCollector(BaseCollector):
    async def _collect_payload(self) -> dict:
        return await asyncio.to_thread(self._collect_sync)

    def _collect_sync(self) -> dict:
        raw_outputs = self._run_commands()
        summary = parse_dxi_cli_outputs(raw_outputs, fallback_name=self.name)
        self._publish_metrics(summary)
        return {
            "summary": summary,
            "raw": raw_outputs,
        }

    def _run_commands(self) -> dict[str, str]:
        import paramiko

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs: dict[str, Any] = {
            "hostname": self.config.host,
            "port": self.config.port or 22,
            "username": self.config.username,
            "timeout": self.config.command_timeout,
            "banner_timeout": self.config.command_timeout,
            "auth_timeout": self.config.command_timeout,
            "look_for_keys": False,
        }
        if self.config.ssh_key_path:
            connect_kwargs["key_filename"] = self.config.ssh_key_path
        else:
            connect_kwargs["password"] = self.config.password

        try:
            client.connect(**connect_kwargs)
            outputs = {}
            for name, command in self.config.commands.items():
                _, stdout, stderr = client.exec_command(command, timeout=self.config.command_timeout)
                exit_status = stdout.channel.recv_exit_status()
                output = stdout.read().decode("utf-8", errors="replace")
                error = stderr.read().decode("utf-8", errors="replace")
                if exit_status != 0:
                    raise RuntimeError(f"DXi CLI command failed: {name} ({exit_status}) {error.strip()}")
                outputs[name] = output
            return outputs
        finally:
            client.close()

    def _publish_metrics(self, summary: dict[str, Any]) -> None:
        device_name = str(summary.get("device_name") or self.name)
        device_type = "dxi"

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
            DEVICE_REPLICATION_UP.labels(device_type, device_name, item["name"]).set(item["up"])

        for item in summary.get("interfaces", []):
            DEVICE_INTERFACE_UP.labels(device_type, device_name, item["name"]).set(item["up"])


def parse_dxi_cli_outputs(outputs: dict[str, str], fallback_name: str) -> dict[str, Any]:
    combined = "\n".join(outputs.values())
    capacity_text = outputs.get("capacity", combined)
    dedup_text = outputs.get("dedup", combined)
    replication_text = outputs.get("replication", "")
    interfaces_text = outputs.get("interfaces", "")
    alerts_text = outputs.get("alerts", combined)
    status_text = outputs.get("status", combined)

    return {
        "device_name": _first_match(status_text, [r"(?:Device|System|Host)\s+Name\s*[:=]\s*(.+)", r"Name\s*[:=]\s*(.+)"])
        or fallback_name,
        "state": _first_match(status_text, [r"(?:State|Status)\s*[:=]\s*([A-Za-z][\w -]+)"]),
        "capacity": _parse_capacity(capacity_text),
        "dedup_ratio": _parse_dedup_ratio(dedup_text),
        "replication": _parse_named_states(replication_text, default_name="replication"),
        "interfaces": _parse_named_states(interfaces_text, default_name="interface"),
        "alert_counts": _parse_alert_counts(alerts_text),
    }


def _parse_capacity(text: str) -> dict[str, float | None]:
    total = _parse_size(_first_match(text, [r"Total(?:\s+Capacity)?\s*[:=]\s*([\d.]+\s*[KMGTPE]?i?B)", r"Capacity\s*[:=]\s*([\d.]+\s*[KMGTPE]?i?B)"]))
    used = _parse_size(_first_match(text, [r"Used(?:\s+Capacity)?\s*[:=]\s*([\d.]+\s*[KMGTPE]?i?B)", r"Disk\s+Used\s*[:=]\s*([\d.]+\s*[KMGTPE]?i?B)"]))
    used_percent = _parse_float(
        _first_match(text, [r"Used\s*(?:Percent|%)\s*[:=]\s*([\d.]+)\s*%?", r"Capacity\s+Used\s*[:=]\s*([\d.]+)\s*%"])
    )
    if used_percent is None and total and used:
        used_percent = round((used / total) * 100, 3)
    return {
        "total_bytes": total,
        "used_bytes": used,
        "used_percent": used_percent,
    }


def _parse_dedup_ratio(text: str) -> float | None:
    value = _first_match(text, [r"(?:Dedup(?:lication)?|Reduction)\s+(?:Ratio|Rate)\s*[:=]\s*([\d.]+)\s*:?\s*1?", r"Dedup\s*[:=]\s*([\d.]+)"])
    return _parse_float(value)


def _parse_alert_counts(text: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for severity in ("critical", "warning", "error", "info", "informational"):
        value = _first_match(text, [rf"{severity}\s*(?:alerts?|tickets?|count)?\s*[:=]\s*(\d+)"])
        if value is not None:
            normalized = "info" if severity == "informational" else severity
            counts[normalized] = int(value)
    return counts


def _parse_named_states(text: str, *, default_name: str) -> list[dict[str, Any]]:
    rows = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or set(stripped) <= {"-", "=", " "}:
            continue

        key_value = re.match(r"(?P<name>[A-Za-z0-9_.:/-]+(?:\s+[A-Za-z0-9_.:/-]+)?)\s*[:=]\s*(?P<state>[A-Za-z][\w -]+)$", stripped)
        tableish = re.match(r"(?P<name>[A-Za-z0-9_.:/-]+)\s{2,}(?P<state>[A-Za-z][\w -]+)$", stripped)
        match = key_value or tableish
        if not match:
            continue

        state = match.group("state").strip()
        rows.append(
            {
                "name": match.group("name").strip() or default_name,
                "state": state,
                "up": 1 if _state_is_up(state) else 0,
            }
        )
    return rows


def _state_is_up(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized in {"up", "online", "enabled", "ok", "good", "healthy", "running", "active", "available", "success"}


def _first_match(text: str, patterns: list[str]) -> str | None:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        if match:
            return match.group(1).strip()
    return None


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _parse_size(value: str | None) -> float | None:
    if value is None:
        return None

    match = re.match(r"([\d.]+)\s*([KMGTPE]?i?B)", value.strip(), flags=re.IGNORECASE)
    if not match:
        return None

    number = float(match.group(1))
    unit = match.group(2).upper().replace("IB", "B")
    multipliers = {
        "B": 1,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "TB": 1000**4,
        "PB": 1000**5,
        "EB": 1000**6,
    }
    return number * multipliers.get(unit, 1)

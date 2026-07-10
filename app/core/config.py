from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
import logging

import yaml

TO_BE_FILLED = "TO_BE_FILLED"
LOGGER = logging.getLogger(__name__)


Protocol = Literal["snmp", "rest", "ssh", "cli", "cli_snmp"]


@dataclass(frozen=True)
class ScheduleConfig:
    interval_minutes: int = 5
    minute_offset: int = 0
    second: int = 0
    legacy_schedule_second: int | None = None

    @property
    def skip_reason(self) -> str | None:
        if self.interval_minutes <= 0:
            return f"invalid schedule.interval_minutes: {self.interval_minutes}"
        if self.minute_offset < 0 or self.minute_offset >= self.interval_minutes:
            return f"invalid schedule.minute_offset: {self.minute_offset}"
        if self.second < 0 or self.second > 59:
            return f"invalid schedule.second: {self.second}"
        return None


@dataclass(frozen=True)
class ElasticsearchConfig:
    enabled: bool = False
    hosts: tuple[str, ...] = ("http://elasticsearch:9200",)
    index_prefix: str = "backup-dashboard"
    username: str | None = None
    password: str | None = None
    verify_certs: bool = True

    @property
    def is_ready(self) -> bool:
        return self.enabled and not has_unfilled_values({"hosts": self.hosts})


@dataclass(frozen=True)
class CollectorConfig:
    name: str
    type: str
    protocol: Protocol
    enabled: bool
    schedule: ScheduleConfig | None = None
    schedule_second: int | None = None
    host: str | None = None
    port: int = 161
    snmp_port: int | None = None
    ssh_port: int | None = None
    community: str | None = None
    version: str = "2c"
    oids: dict[str, str] = field(default_factory=dict)
    walk_oids: dict[str, str] = field(default_factory=dict)
    base_url: str | None = None
    endpoint: str | None = None
    endpoints: dict[str, str] = field(default_factory=dict)
    method: str = "GET"
    token: str | None = None
    username: str | None = None
    password: str | None = None
    verify_tls: bool = True
    commands: dict[str, str] = field(default_factory=dict)
    ssh_key_path: str | None = None
    command_timeout: int = 30

    @property
    def effective_schedule(self) -> ScheduleConfig:
        if self.schedule is not None:
            return self.schedule
        if self.schedule_second is not None:
            return ScheduleConfig(
                interval_minutes=1,
                minute_offset=0,
                second=self.schedule_second,
                legacy_schedule_second=self.schedule_second,
            )
        return default_schedule(self.type)

    @property
    def skip_reason(self) -> str | None:
        if not self.enabled:
            return "collector is disabled"
        schedule_skip_reason = self.effective_schedule.skip_reason
        if schedule_skip_reason:
            return schedule_skip_reason
        if self.protocol == "snmp":
            return self._snmp_skip_reason()
        if self.protocol == "rest":
            return self._rest_skip_reason()
        if self.protocol in {"ssh", "cli"}:
            return self._ssh_skip_reason()
        if self.protocol == "cli_snmp":
            return self._cli_snmp_skip_reason()
        return f"unsupported protocol: {self.protocol}"

    def _snmp_skip_reason(self) -> str | None:
        required = {
            "host": self.host,
            "community": self.community,
            "oids": self.oids,
            "walk_oids": self.walk_oids,
        }
        if has_unfilled_values(required):
            return "SNMP config contains TO_BE_FILLED values"
        if not self.oids and not self.walk_oids:
            return "SNMP OID list is empty"
        return None

    def _rest_skip_reason(self) -> str | None:
        required = {"base_url": self.base_url}
        if self.type == "i6000":
            required.update(
                {
                    "username": self.username,
                    "password": self.password,
                    "endpoints": self.endpoints,
                }
            )
        elif self.type == "Networker":
            if self.endpoints:
                required["endpoints"] = self.endpoints
        elif self.type == "ZFS":
            if self.endpoints:
                required["endpoints"] = self.endpoints
            if not self.token and not (self.username and self.password):
                return "REST config requires token or username/password"
        else:
            required["endpoint"] = self.endpoint
        if has_unfilled_values(required):
            return "REST config contains TO_BE_FILLED values"
        if self.type not in {"Networker", "ZFS"} and not self.endpoint and not self.endpoints:
            return "REST endpoint list is empty"
        return None

    def _ssh_skip_reason(self) -> str | None:
        required = {
            "host": self.host,
            "username": self.username,
            "commands": self.commands,
        }
        if not self.password and not self.ssh_key_path:
            return "SSH config requires password or ssh_key_path"
        if has_unfilled_values(required):
            return "SSH config contains TO_BE_FILLED values"
        if has_unfilled_values({"password": self.password}) and has_unfilled_values({"ssh_key_path": self.ssh_key_path}):
            return "SSH config contains TO_BE_FILLED values"
        if not self.commands:
            return "SSH command list is empty"
        return None

    def _cli_snmp_skip_reason(self) -> str | None:
        snmp_skip_reason = self._snmp_skip_reason()
        if snmp_skip_reason:
            return snmp_skip_reason
        ssh_skip_reason = self._ssh_skip_reason()
        if ssh_skip_reason:
            return ssh_skip_reason
        return None


@dataclass(frozen=True)
class AppConfig:
    name: str
    log_level: str
    elasticsearch: ElasticsearchConfig
    collectors: tuple[CollectorConfig, ...]


def load_config(path: Path) -> AppConfig:
    with path.open("r", encoding="utf-8") as fp:
        raw = yaml.safe_load(fp) or {}

    app = raw.get("app", {})
    elasticsearch = _parse_elasticsearch(raw.get("elasticsearch", {}))
    collectors = tuple(_parse_collector(item) for item in raw.get("collectors", []))

    return AppConfig(
        name=app.get("name", "backup-dashboard-collector"),
        log_level=app.get("log_level", "INFO"),
        elasticsearch=elasticsearch,
        collectors=collectors,
    )


def has_unfilled_values(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return is_unfilled_placeholder(value)
    if isinstance(value, dict):
        return any(has_unfilled_values(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return any(has_unfilled_values(item) for item in value)
    return False


def _parse_elasticsearch(raw: dict[str, Any]) -> ElasticsearchConfig:
    return ElasticsearchConfig(
        enabled=bool(raw.get("enabled", False)),
        hosts=tuple(raw.get("hosts", ["http://elasticsearch:9200"])),
        index_prefix=raw.get("index_prefix", "backup-dashboard"),
        username=_optional_secret(raw, "username"),
        password=_optional_secret(raw, "password"),
        verify_certs=bool(raw.get("verify_certs", True)),
    )


def _parse_collector(raw: dict[str, Any]) -> CollectorConfig:
    schedule, legacy_schedule_second = _parse_schedule(raw, raw["type"])
    return CollectorConfig(
        name=raw["name"],
        type=raw["type"],
        protocol=raw["protocol"],
        enabled=bool(raw.get("enabled", True)),
        schedule=schedule,
        schedule_second=legacy_schedule_second,
        host=_optional(raw.get("host")),
        port=int(raw.get("port", 161)),
        snmp_port=int(raw["snmp_port"]) if raw.get("snmp_port") is not None else None,
        ssh_port=int(raw["ssh_port"]) if raw.get("ssh_port") is not None else None,
        community=_optional_secret(raw, "community"),
        version=str(raw.get("version", "2c")),
        oids=dict(raw.get("oids", {})),
        walk_oids=dict(raw.get("walk_oids", {})),
        base_url=_optional(raw.get("base_url")),
        endpoint=_optional(raw.get("endpoint")),
        endpoints=dict(raw.get("endpoints", {})),
        method=str(raw.get("method", "GET")).upper(),
        token=_optional_secret(raw, "token"),
        username=_optional_secret(raw, "username"),
        password=_optional_secret(raw, "password"),
        verify_tls=bool(raw.get("verify_tls", True)),
        commands=dict(raw.get("commands", {})),
        ssh_key_path=_optional(raw.get("ssh_key_path")),
        command_timeout=int(raw.get("command_timeout", 30)),
    )


def _parse_schedule(raw: dict[str, Any], target_type: str) -> tuple[ScheduleConfig, int | None]:
    if "schedule" in raw:
        schedule_raw = raw.get("schedule") or {}
        return (
            ScheduleConfig(
                interval_minutes=int(schedule_raw.get("interval_minutes", 5)),
                minute_offset=int(schedule_raw.get("minute_offset", default_minute_offset(target_type))),
                second=int(schedule_raw.get("second", 0)),
            ),
            None,
        )

    if "schedule_second" in raw:
        schedule_second = int(raw["schedule_second"])
        LOGGER.warning(
            "Collector %s uses deprecated schedule_second; use schedule.interval_minutes, "
            "schedule.minute_offset, and schedule.second instead",
            raw.get("name", target_type),
        )
        return (
            ScheduleConfig(
                interval_minutes=1,
                minute_offset=0,
                second=schedule_second,
                legacy_schedule_second=schedule_second,
            ),
            schedule_second,
        )

    return default_schedule(target_type), None


def default_schedule(target_type: str) -> ScheduleConfig:
    return ScheduleConfig(interval_minutes=5, minute_offset=default_minute_offset(target_type), second=0)


def default_minute_offset(target_type: str) -> int:
    schedules = {
        "DXi": 0,
        "DD": 1,
        "i6000": 2,
        "Networker": 3,
        "ZFS": 4,
    }
    return schedules[target_type]


def default_second(target_type: str) -> int:
    return default_schedule(target_type).second


def _optional(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if is_unfilled_placeholder(normalized):
        return None
    return normalized


def _optional_secret(raw: dict[str, Any], key: str) -> str | None:
    value = _optional(raw.get(key))
    if value is not None:
        return value

    path = _optional(raw.get(f"{key}_file"))
    if path is None:
        return None

    return _optional(Path(path).read_text(encoding="utf-8"))


def is_unfilled_placeholder(value: str) -> bool:
    normalized = value.strip()
    return normalized == "" or normalized == TO_BE_FILLED or normalized.endswith(f"_{TO_BE_FILLED}")


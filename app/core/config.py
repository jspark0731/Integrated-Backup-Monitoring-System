from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml

TO_BE_FILLED = "TO_BE_FILLED"


Protocol = Literal["snmp", "rest", "ssh", "cli"]


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
    schedule_second: int
    host: str | None = None
    port: int = 161
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
    def skip_reason(self) -> str | None:
        if not self.enabled:
            return "collector is disabled"
        if self.schedule_second < 0 or self.schedule_second > 59:
            return f"invalid schedule_second: {self.schedule_second}"
        if self.protocol == "snmp":
            return self._snmp_skip_reason()
        if self.protocol == "rest":
            return self._rest_skip_reason()
        if self.protocol in {"ssh", "cli"}:
            return self._ssh_skip_reason()
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
    return CollectorConfig(
        name=raw["name"],
        type=raw["type"],
        protocol=raw["protocol"],
        enabled=bool(raw.get("enabled", True)),
        schedule_second=int(raw.get("schedule_second", default_second(raw["type"]))),
        host=_optional(raw.get("host")),
        port=int(raw.get("port", 161)),
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


def default_second(target_type: str) -> int:
    schedules = {
        "DXi": 0,
        "DD": 0,
        "i6000": 15,
        "Networker": 30,
        "ZFS": 45,
    }
    return schedules[target_type]


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


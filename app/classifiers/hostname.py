from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class HostnameClassification:
    hostname: str
    security_domain: str
    status: str


class HostnameClassifier:
    def __init__(
        self,
        mapping: dict[str, str],
        *,
        allowed_domains: tuple[str, ...] = ("core", "chnl", "info", "ifrs"),
        unmapped_domain: str = "unmapped",
    ) -> None:
        self.allowed_domains = tuple(domain.strip().lower() for domain in allowed_domains)
        self.unmapped_domain = unmapped_domain.strip().lower()
        normalized_mapping: dict[str, str] = {}
        short_names: dict[str, str | None] = {}

        for raw_hostname, raw_domain in mapping.items():
            hostname = normalize_hostname(raw_hostname)
            domain = raw_domain.strip().lower()
            if not hostname:
                raise ValueError("hostname classification contains an empty hostname")
            if domain not in self.allowed_domains:
                raise ValueError(f"unsupported security domain for {raw_hostname}: {raw_domain}")
            previous = normalized_mapping.get(hostname)
            if previous is not None and previous != domain:
                raise ValueError(f"hostname is assigned to multiple security domains: {raw_hostname}")
            normalized_mapping[hostname] = domain

            short_name = hostname.split(".", 1)[0]
            short_domain = short_names.get(short_name)
            if short_name not in short_names:
                short_names[short_name] = domain
            elif short_domain != domain:
                short_names[short_name] = None

        self.mapping = normalized_mapping
        self.short_names = short_names

    @classmethod
    def from_csv(
        cls,
        path: Path,
        *,
        allowed_domains: tuple[str, ...] = ("core", "chnl", "info", "ifrs"),
        unmapped_domain: str = "unmapped",
    ) -> "HostnameClassifier":
        with path.open("r", encoding="utf-8-sig", newline="") as fp:
            reader = csv.DictReader(fp)
            required = {"hostname", "security_domain"}
            if not reader.fieldnames or not required.issubset(reader.fieldnames):
                raise ValueError("hostname CSV must contain hostname and security_domain columns")
            mapping: dict[str, str] = {}
            for row in reader:
                hostname = str(row.get("hostname") or "")
                domain = str(row.get("security_domain") or "")
                normalized = normalize_hostname(hostname)
                previous = mapping.get(normalized)
                if previous is not None and previous.strip().lower() != domain.strip().lower():
                    raise ValueError(f"hostname is assigned to multiple security domains: {hostname}")
                mapping[hostname] = domain
        return cls(mapping, allowed_domains=allowed_domains, unmapped_domain=unmapped_domain)

    def classify(self, hostname: object) -> HostnameClassification:
        normalized = normalize_hostname(hostname)
        if not normalized:
            return HostnameClassification("", self.unmapped_domain, "missing_hostname")

        domain = self.mapping.get(normalized)
        if domain is not None:
            return HostnameClassification(normalized, domain, "matched")

        short_name = normalized.split(".", 1)[0]
        short_domain = self.short_names.get(short_name)
        if short_domain is not None:
            return HostnameClassification(normalized, short_domain, "matched_short_hostname")

        return HostnameClassification(normalized, self.unmapped_domain, "unmapped")


def normalize_hostname(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().lower().rstrip(".")

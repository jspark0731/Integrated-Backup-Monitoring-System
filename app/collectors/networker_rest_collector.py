from __future__ import annotations

from pathlib import Path
from typing import Any

from app.classifiers.hostname import HostnameClassifier
from app.clients.networker_rest_client import NetworkerRestClient
from app.collectors.base import BaseCollector
from app.core.metrics import (
    NETWORKER_API_UP,
    NETWORKER_CLIENT_COUNT,
    NETWORKER_JOB_FAILED_COUNT,
    NETWORKER_JOB_RUNNING_COUNT,
    NETWORKER_JOB_SUCCESS_COUNT,
    NETWORKER_WORKFLOW_COUNT,
)
from app.parsers.networker_rest_parser import parse_networker_rest_payload


class NetworkerRestCollector(BaseCollector):
    def __init__(self, config) -> None:
        super().__init__(config)
        self.classifier: HostnameClassifier | None = None
        if config.skip_reason is None and config.hostname_csv_path:
            self.classifier = HostnameClassifier.from_csv(
                Path(config.hostname_csv_path),
                allowed_domains=config.allowed_security_domains,
                unmapped_domain=config.unmapped_security_domain,
            )

    async def _collect_payload(self) -> dict[str, Any]:
        raw = await NetworkerRestClient(self.config).fetch_payloads()
        parsed = parse_networker_rest_payload(
            raw,
            server_name=self.name,
            source_networker=str(self.config.source_networker or self.name),
            classifier=self.classifier,
        )
        self._publish_metrics(parsed["summary"])
        return {
            "summary": parsed["summary"],
            "jobs": parsed["jobs"],
            "clients": parsed["clients"],
            "policies": parsed["policies"],
            "workflows": parsed["workflows"],
            "monthly_report": parsed["monthly_report"],
            "raw": raw,
        }

    def _publish_metrics(self, summary: dict[str, Any]) -> None:
        server = str(summary.get("server") or self.name)

        NETWORKER_API_UP.labels(server).set(1)
        for domain, count in summary.get("client_count_by_domain", {}).items():
            NETWORKER_CLIENT_COUNT.labels(server, domain).set(count)

        self._publish_domain_policy_metric(
            NETWORKER_JOB_SUCCESS_COUNT,
            server,
            summary.get("job_success_count_by_domain_policy", {}),
        )
        self._publish_domain_policy_metric(
            NETWORKER_JOB_FAILED_COUNT,
            server,
            summary.get("job_failed_count_by_domain_policy", {}),
        )
        self._publish_domain_policy_metric(
            NETWORKER_JOB_RUNNING_COUNT,
            server,
            summary.get("job_running_count_by_domain_policy", {}),
        )
        self._publish_domain_policy_metric(
            NETWORKER_WORKFLOW_COUNT,
            server,
            summary.get("workflow_count_by_domain_policy", {}),
        )

    @staticmethod
    def _publish_domain_policy_metric(metric: Any, server: str, values: dict[str, Any]) -> None:
        for domain, policy_counts in values.items():
            if not isinstance(policy_counts, dict):
                continue
            for policy, count in policy_counts.items():
                metric.labels(server, policy, domain).set(count)


__all__ = ["NetworkerRestCollector", "parse_networker_rest_payload"]

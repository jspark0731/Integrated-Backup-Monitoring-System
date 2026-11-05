from __future__ import annotations

from typing import Any

from apps.clients.networker_rest_client import NetworkerRestClient
from apps.clients.rest_client import endpoint_errors
from apps.collectors.base import BaseCollector
from apps.core.config import CollectionClass
from apps.core.metrics import (
    NETWORKER_API_UP,
    NETWORKER_CLIENT_COUNT,
    NETWORKER_JOB_FAILED_COUNT,
    NETWORKER_JOB_RUNNING_COUNT,
    NETWORKER_JOB_SUCCESS_COUNT,
    NETWORKER_WORKFLOW_COUNT,
)
from apps.parsers.networker_rest_parser import parse_networker_rest_payload


class NetworkerRestCollector(BaseCollector):
    def __init__(self, config) -> None:
        super().__init__(config)
        self.client = NetworkerRestClient(config)

    async def _collect_payload(self) -> dict[str, Any]:
        return await self._collect_payload_for_class("fast")

    async def _collect_payload_for_class(
        self,
        collection_class: CollectionClass,
    ) -> dict[str, Any]:
        active_class = collection_class if len(self.config.effective_schedules) > 1 else None
        raw = await self.client.fetch_payloads(active_class)
        parsed = parse_networker_rest_payload(
            raw,
            server_name=self.name,
            source_networker=self.config.source_networker,
        )
        errors = endpoint_errors(raw)
        _mark_failed_endpoint_counts_unknown(parsed["summary"], errors)
        self._publish_metrics(parsed["summary"], active_class)
        return {
            "summary": parsed["summary"],
            "jobs": parsed["jobs"],
            "backups": parsed["backups"],
            "clients": parsed["clients"],
            "policies": parsed["policies"],
            "workflows": parsed["workflows"],
            "monthly_report": parsed["monthly_report"],
            "raw": raw,
            "collection_status": "partial" if errors else "success",
            "endpoint_errors": errors,
        }

    async def close(self) -> None:
        await self.client.close()

    def _publish_metrics(
        self,
        summary: dict[str, Any],
        collection_class: CollectionClass | None,
    ) -> None:
        server = str(summary.get("server") or self.name)

        NETWORKER_API_UP.labels(server).set(1)
        if collection_class in {None, "slow", "inventory"}:
            for domain, count in summary.get("client_count_by_domain", {}).items():
                NETWORKER_CLIENT_COUNT.labels(server, domain).set(count)

        if collection_class in {None, "fast", "jobs"}:
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
        if collection_class in {None, "slow", "inventory"}:
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


def _mark_failed_endpoint_counts_unknown(
    summary: dict[str, Any],
    errors: dict[str, str],
) -> None:
    if "jobs" in errors:
        summary["job_count"] = None
    if "backups" in errors:
        summary["backup_count"] = None
        summary["total_backup_bytes"] = None


__all__ = ["NetworkerRestCollector", "parse_networker_rest_payload"]

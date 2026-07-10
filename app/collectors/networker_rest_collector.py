from __future__ import annotations

from typing import Any

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
    async def _collect_payload(self) -> dict[str, Any]:
        raw = await NetworkerRestClient(self.config).fetch_payloads()
        parsed = parse_networker_rest_payload(raw, server_name=self.name)
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
        NETWORKER_CLIENT_COUNT.labels(server).set(summary.get("client_count", 0))

        for policy, count in summary.get("job_success_count_by_policy", {}).items():
            NETWORKER_JOB_SUCCESS_COUNT.labels(server, policy).set(count)
        for policy, count in summary.get("job_failed_count_by_policy", {}).items():
            NETWORKER_JOB_FAILED_COUNT.labels(server, policy).set(count)
        for policy, count in summary.get("job_running_count_by_policy", {}).items():
            NETWORKER_JOB_RUNNING_COUNT.labels(server, policy).set(count)
        for policy, count in summary.get("workflow_count_by_policy", {}).items():
            NETWORKER_WORKFLOW_COUNT.labels(server, policy).set(count)


__all__ = ["NetworkerRestCollector", "parse_networker_rest_payload"]

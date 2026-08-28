from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin

import httpx

from apps.clients.rest_client import ERRORS_KEY, ReusableRestClient
from apps.core.config import CollectionClass, CollectorConfig


DEFAULT_ENDPOINTS = {
    "jobs": "nwrestapi/v3/global/jobs",
    "clients": "nwrestapi/v3/global/clients",
    "backups": "nwrestapi/v3/global/backups",
    "policies": "nwrestapi/v3/global/protectionpolicies",
}
FAST_ENDPOINTS = {"jobs", "backups"}
SLOW_ENDPOINTS = {"clients", "policies"}

FIELD_FILTERS = {
    "jobs": ",".join(
        (
            "id",
            "jobId",
            "name",
            "type",
            "state",
            "completionStatus",
            "exitCode",
            "policyName",
            "workflowName",
            "clientName",
            "clientHostname",
            "runOnHost",
            "startTime",
            "endTime",
            "stopped",
            "rootParentJobId",
            "parentJobId",
        )
    ),
    "backups": ",".join(
        (
            "id",
            "clientHostname",
            "clientId",
            "name",
            "type",
            "level",
            "saveTime",
            "completionTime",
            "retentionTime",
            "fileCount",
            "size",
            "attributes",
        )
    ),
    "clients": ",".join(
        (
            "clientId",
            "resourceId",
            "hostname",
            "name",
            "operatingSystem",
            "backupType",
            "scheduledBackup",
            "saveSets",
            "protectionGroups",
            "storageNodes",
            "parallelism",
            "aliases",
        )
    ),
    "policies": "name,comment,resourceId,workflows",
}


@dataclass(frozen=True)
class _RequestSpec:
    result_name: str
    endpoint_name: str
    endpoint: str
    params: dict[str, str]


class NetworkerRestClient:
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config
        self._rest = ReusableRestClient(
            verify_tls=config.verify_tls,
            ca_certs=config.ca_certs,
            timeout_seconds=30,
            max_concurrency=config.rest_max_concurrency,
        )

    async def fetch_payloads(
        self,
        collection_class: CollectionClass | None = None,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        headers = {"Accept": "application/json"}
        auth = (str(self.config.username), str(self.config.password))

        endpoints = _endpoints_for_class(
            self.config.endpoints or DEFAULT_ENDPOINTS,
            collection_class,
        )
        window_end = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        window_start = window_end - timedelta(
            minutes=self.config.networker_query_window_minutes
        )
        request_plan = _build_request_plan(
            endpoints,
            window_start=window_start,
            window_end=window_end,
            active_jobs_query=self.config.networker_active_jobs_query,
            field_filter=self.config.networker_field_filter,
        )

        responses: dict[str, Any] = {}
        errors: dict[str, str] = {}
        for index, request in enumerate(request_plan):
            if index and self.config.rest_request_spacing_seconds:
                await asyncio.sleep(self.config.rest_request_spacing_seconds)
            try:
                responses[request.result_name] = await self._get_json(
                    self._rest.client,
                    request.endpoint,
                    headers,
                    auth,
                    request.params,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors[request.result_name] = f"{type(exc).__name__}: {exc}"

        payloads = _combine_responses(endpoints, responses)
        if any(name in endpoints for name in FAST_ENDPOINTS):
            payloads["_query_window"] = {
                "start": _format_query_time(window_start),
                "end": _format_query_time(window_end),
                "minutes": self.config.networker_query_window_minutes,
            }
        if errors:
            payloads[ERRORS_KEY] = errors
        return payloads

    async def close(self) -> None:
        await self._rest.close()

    async def _get_json(
        self,
        client: httpx.AsyncClient,
        endpoint: str,
        headers: dict[str, str],
        auth: tuple[str, str] | None,
        params: dict[str, str] | None = None,
    ) -> Any:
        response = await client.get(
            self._url(endpoint),
            headers=headers,
            auth=auth,
            params=params,
        )
        response.raise_for_status()
        return response.json() if response.content else {}

    def _url(self, endpoint: str) -> str:
        base_url = f"{self.config.base_url.rstrip('/')}/"
        return urljoin(base_url, endpoint.lstrip("/"))


def _endpoints_for_class(
    endpoints: dict[str, str],
    collection_class: CollectionClass | None,
) -> dict[str, str]:
    if collection_class is None:
        return dict(endpoints)
    selected = FAST_ENDPOINTS if collection_class == "fast" else SLOW_ENDPOINTS
    return {name: endpoint for name, endpoint in endpoints.items() if name in selected}


def _build_request_plan(
    endpoints: dict[str, str],
    *,
    window_start: datetime,
    window_end: datetime,
    active_jobs_query: str | None,
    field_filter: bool,
) -> list[_RequestSpec]:
    plan: list[_RequestSpec] = []
    for name, endpoint in endpoints.items():
        field_params = {"fl": FIELD_FILTERS[name]} if field_filter and name in FIELD_FILTERS else {}
        if name == "jobs":
            plan.append(
                _RequestSpec(
                    "jobs_completed",
                    name,
                    endpoint,
                    field_params
                    | {"q": _time_range_query("endTime", window_start, window_end)},
                )
            )
            if active_jobs_query:
                plan.append(
                    _RequestSpec(
                        "jobs_active",
                        name,
                        endpoint,
                        field_params | {"q": active_jobs_query},
                    )
                )
            continue
        if name == "backups":
            plan.append(
                _RequestSpec(
                    name,
                    name,
                    endpoint,
                    field_params
                    | {"q": _time_range_query("saveTime", window_start, window_end)},
                )
            )
            continue
        plan.append(_RequestSpec(name, name, endpoint, field_params))
    return plan


def _combine_responses(
    endpoints: dict[str, str],
    responses: dict[str, Any],
) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    if "jobs" in endpoints:
        job_payloads = [
            responses[name]
            for name in ("jobs_completed", "jobs_active")
            if name in responses
        ]
        if job_payloads:
            payloads["jobs"] = _merge_collection_payloads(job_payloads, "jobs")
    for name in endpoints:
        if name != "jobs" and name in responses:
            payloads[name] = responses[name]
    return payloads


def _merge_collection_payloads(payloads: list[Any], key: str) -> dict[str, Any]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for payload in payloads:
        if isinstance(payload, dict):
            values = payload.get(key)
        else:
            values = payload
        if not isinstance(values, list):
            continue
        for item in values:
            if not isinstance(item, dict):
                continue
            identity = _record_identity(item)
            if identity and identity in seen:
                continue
            if identity:
                seen.add(identity)
            merged.append(item)
    return {key: merged}


def _record_identity(item: dict[str, Any]) -> str:
    for key in ("id", "jobId", "resourceId"):
        value = item.get(key)
        if value is not None and str(value).strip():
            return f"{key}:{value}"
    return ""


def _time_range_query(field: str, start: datetime, end: datetime) -> str:
    return f'{field}:["{_format_query_time(start)}" TO "{_format_query_time(end)}"]'


def _format_query_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

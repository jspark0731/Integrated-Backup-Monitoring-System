from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin

import httpx

from apps.clients.rest_client import ERRORS_KEY, ReusableRestClient
from apps.core.config import CollectionClass, CollectorConfig


LOGGER = logging.getLogger(__name__)
RETRY_BACKOFF_SECONDS = 1.0


DEFAULT_ENDPOINTS = {
    "jobs": "nwrestapi/v3/global/jobs",
    "clients": "nwrestapi/v3/global/clients",
    "backups": "nwrestapi/v3/global/backups",
    "policies": "nwrestapi/v3/global/protectionpolicies",
}
FAST_ENDPOINTS = {"jobs", "backups"}
SLOW_ENDPOINTS = {"clients", "policies"}
CLASS_ENDPOINTS = {
    "fast": FAST_ENDPOINTS,
    "slow": SLOW_ENDPOINTS,
    "jobs": {"jobs"},
    "backups": {"backups"},
    "inventory": SLOW_ENDPOINTS,
}

FIELD_FILTERS = {
    # Keep fl off jobs/backups: NetWorker can apply it before range filtering.
    "clients": ",".join(
        (
            "resourceId",
            "hostname",
            "clientState",
            "operatingSystem",
            "backupType",
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
        query_windows = _query_windows(self.config, endpoints, window_end)
        request_plan = _build_request_plan(
            endpoints,
            query_windows=query_windows,
            window_end=window_end,
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
        timed_windows = {
            name: {
                "start": _format_query_time(start),
                "end": _format_query_time(window_end),
                "minutes": int((window_end - start).total_seconds() / 60),
            }
            for name, start in query_windows.items()
        }
        if timed_windows:
            longest = max(timed_windows.values(), key=lambda item: item["minutes"])
            payloads["_query_window"] = dict(longest)
            payloads["_query_window"]["endpoints"] = timed_windows
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
        max_attempts = max(1, self.config.retries + 1)
        for attempt in range(1, max_attempts + 1):
            LOGGER.debug(
                "NetWorker REST request endpoint=%s q=%r fl=%r attempt=%d/%d",
                endpoint,
                (params or {}).get("q"),
                (params or {}).get("fl"),
                attempt,
                max_attempts,
            )
            try:
                started_at = time.monotonic()
                response = await client.get(
                    self._url(endpoint),
                    headers=headers,
                    auth=auth,
                    params=params,
                )
                response.raise_for_status()
                payload = response.json() if response.content else {}
                LOGGER.info(
                    "NetWorker REST response endpoint=%s q=%r returned_items=%d elapsed_seconds=%.3f status=success",
                    endpoint,
                    (params or {}).get("q"),
                    _response_item_count(payload),
                    time.monotonic() - started_at,
                )
                LOGGER.debug(
                    "NetWorker REST raw fields endpoint=%s fields=%s",
                    endpoint,
                    _response_field_names(payload),
                )
                return payload
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code
                LOGGER.warning(
                    "NetWorker REST error endpoint=%s status_code=%d params=%r response_body=%r",
                    endpoint,
                    status_code,
                    params or {},
                    _safe_response_body(exc.response),
                )
                if status_code != 429 and status_code < 500:
                    raise
                if attempt == max_attempts:
                    raise
            except httpx.TimeoutException as exc:
                LOGGER.warning(
                    "NetWorker REST timeout endpoint=%s params=%r error=%s",
                    endpoint,
                    params or {},
                    exc,
                )
                if attempt == max_attempts:
                    raise
            await asyncio.sleep(RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)))
        raise RuntimeError("NetWorker REST request retry loop exited unexpectedly")

    def _url(self, endpoint: str) -> str:
        base_url = f"{self.config.base_url.rstrip('/')}/"
        return urljoin(base_url, endpoint.lstrip("/"))


def _endpoints_for_class(
    endpoints: dict[str, str],
    collection_class: CollectionClass | None,
) -> dict[str, str]:
    if collection_class is None:
        return dict(endpoints)
    selected = CLASS_ENDPOINTS.get(collection_class, set())
    return {name: endpoint for name, endpoint in endpoints.items() if name in selected}


def _query_windows(
    config: CollectorConfig,
    endpoints: dict[str, str],
    window_end: datetime,
) -> dict[str, datetime]:
    minutes_by_endpoint = {
        "jobs": config.networker_jobs_query_window_minutes,
        "backups": config.networker_backups_query_window_minutes,
    }
    return {
        name: window_end - timedelta(minutes=minutes_by_endpoint[name])
        for name in endpoints
        if name in minutes_by_endpoint
    }


def _build_request_plan(
    endpoints: dict[str, str],
    *,
    query_windows: dict[str, datetime],
    window_end: datetime,
    field_filter: bool,
) -> list[_RequestSpec]:
    plan: list[_RequestSpec] = []
    for name, endpoint in endpoints.items():
        field_params = {}
        if name == "clients":
            field_params = {"fl": FIELD_FILTERS[name]}
        elif field_filter and name in FIELD_FILTERS:
            field_params = {"fl": FIELD_FILTERS[name]}
        if name == "jobs":
            params = {
                "q": _time_range_query("endTime", query_windows[name], window_end)
            }
            params.update(field_params)
            plan.append(
                _RequestSpec(
                    name,
                    name,
                    endpoint,
                    params,
                )
            )
            continue
        if name == "backups":
            params = {
                "q": _time_range_query("saveTime", query_windows[name], window_end)
            }
            params.update(field_params)
            plan.append(
                _RequestSpec(
                    name,
                    name,
                    endpoint,
                    params,
                )
            )
            continue
        plan.append(_RequestSpec(name, name, endpoint, field_params))
    return plan


def _combine_responses(
    endpoints: dict[str, str],
    responses: dict[str, Any],
) -> dict[str, Any]:
    return {name: responses[name] for name in endpoints if name in responses}


def _time_range_query(field: str, start: datetime, end: datetime) -> str:
    return f'{field}:["{_format_query_time(start)}" TO "{_format_query_time(end)}"]'


def _format_query_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _response_item_count(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for value in payload.values():
            if isinstance(value, list):
                return len(value)
    return 0


def _response_field_names(payload: Any) -> list[str]:
    items: list[Any] = []
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        for value in payload.values():
            if isinstance(value, list):
                items = value
                break
    first = next((item for item in items if isinstance(item, dict)), None)
    return sorted(first) if isinstance(first, dict) else []


def _safe_response_body(response: httpx.Response, limit: int = 2000) -> str:
    try:
        body = response.text
    except Exception:
        return "<unavailable>"
    return body[:limit]

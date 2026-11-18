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
ACTIVE_JOB_QUERY = "state:Active,Started,SessionActive,Queued"
WORKFLOW_CONFIRM_LIMIT = 20
WORKFLOW_CONFIRM_SECONDS = 30.0
JOB_PAGE_LIMIT = 100


DEFAULT_ENDPOINTS = {
    "jobs": "nwrestapi/v3/global/jobs",
    "clients": "nwrestapi/v3/global/clients",
    "backups": "nwrestapi/v3/global/backups",
    "policies": "nwrestapi/v3/global/protectionpolicies",
    "groups": "nwrestapi/v3/global/protectiongroups",
}
FAST_ENDPOINTS = {"jobs", "backups"}
SLOW_ENDPOINTS = {"clients", "policies", "groups"}
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
            "clientId",
            "hostname",
            "clientState",
            "backupType",
            "protectionGroups",
            "os",
        )
    ),
    "policies": "name,comment,resourceId,workflows",
    "groups": "name,resourceId,workItemType,workItemSource,workItemSubType,workItems,workItemQueries,workItemExclusions,dynamicAssociation",
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
        self._unfinished_workflows: dict[str, str | None] = {}

    def remember_workflows(self, workflows: dict[str, str | None]) -> None:
        """Restore unfinished IDs from current, without assigning any outcome."""
        for identity, start in workflows.items():
            if str(identity).isdigit() and int(identity) > 0:
                self._unfinished_workflows.setdefault(str(identity), start)

    async def fetch_payloads(
        self,
        collection_class: CollectionClass | None = None,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        headers = {"Accept": "application/json"}
        auth = (str(self.config.username), str(self.config.password))
        self._request_count = 0

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
        completed_pages = 0
        for request in request_plan:
            try:
                if request.endpoint_name == "jobs":
                    items, completed_pages = await self._job_pages(request.endpoint, request.params, headers, auth)
                    responses[request.result_name] = {"jobs": items, "count": len(items)}
                elif request.endpoint_name in SLOW_ENDPOINTS:
                    key = {"clients": "clients", "policies": "protectionPolicies", "groups": "protectionGroups"}[request.endpoint_name]
                    items, _ = await self._inventory_pages(request.endpoint, request.params, headers, auth, key)
                    responses[request.result_name] = {key: items, "count": len(items)}
                else:
                    await self._request_gap()
                    responses[request.result_name] = await self._get_json(
                        self._rest.client, request.endpoint, headers, auth, request.params)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors[request.result_name] = f"{type(exc).__name__}: {exc}"

        if "jobs" in endpoints:
            # Keep the historical RAW jobs.jobs[] path, supplementing it with
            # active observations and bounded individual completion checks.
            merged, audit = await self._observe_jobs(
                endpoints["jobs"], responses.get("jobs"), headers, auth, errors)
            audit["completed_pages"] = completed_pages
            responses["jobs"] = merged
        else:
            audit = None

        payloads = _combine_responses(endpoints, responses)
        if audit is not None:
            payloads["_jobs_collection"] = audit
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

    async def _observe_jobs(self, endpoint, completed, headers, auth, errors):
        audit = {"completed_success": False, "active_success": False,
                 "active_pages": 0, "active_count": None, "confirmation_checked": 0,
                 "confirmation_pending": 0, "unconfirmed_ids": []}
        try:
            done = _job_items(completed)
            audit["completed_success"] = "jobs" not in errors
        except ValueError:
            done = []
            errors.setdefault("jobs", "invalid_jobs_response")
        active = []
        try:
            active, pages = await self._job_pages(endpoint, {"q": ACTIVE_JOB_QUERY}, headers, auth)
            if any(not isinstance(j.get("type"), str) or not j["type"] or not j.get("state") for j in active):
                raise ValueError("incomplete_active_job_fields")
            audit.update(active_success=True, active_pages=pages, active_count=len(active))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            errors["jobs_active"] = type(exc).__name__
            active = []
        all_jobs = list(done) + active
        known = {str(j.get("id") or j.get("jobId")) for j in all_jobs}
        terminal_ids = {str(j.get("id") or j.get("jobId")) for j in all_jobs if _terminal_job(j)}
        for j in all_jobs:
            identity = str(j.get("id") or j.get("jobId"))
            if j.get("type", "").lower() == "workflow job":
                if identity in terminal_ids:
                    self._unfinished_workflows.pop(identity, None)
                else:
                    self.remember_workflows({identity: j.get("startTime")})
        pending = [i for i in self._unfinished_workflows if i not in known]
        # Only reconfirm observed Workflow IDs. Never scan historical jobs or
        # confirm all Save Jobs. Rotate failures so one missing ID cannot starve others.
        attempted, confirmation_started = [], time.monotonic()
        for identity in pending[:WORKFLOW_CONFIRM_LIMIT]:
            # One existing request (including configured retries/spacing) may
            # exceed the soft budget; do not start another afterwards.
            if attempted and time.monotonic() - confirmation_started >= WORKFLOW_CONFIRM_SECONDS:
                break
            attempted.append(identity)
            start = self._unfinished_workflows.pop(identity)
            self._unfinished_workflows[identity] = start
            try:
                await self._request_gap()
                item = await self._get_json(self._rest.client, endpoint.rstrip("/") + "/" + identity,
                                            headers, auth, {})
                if (not isinstance(item, dict) or str(item.get("id") or item.get("jobId")) != identity
                        or str(item.get("type", "")).lower() != "workflow job"
                        or start and (not _job_start(start) or _job_start(item.get("startTime")) != _job_start(start))):
                    raise ValueError("workflow_confirmation_identity_conflict")
                all_jobs.append(item)
                if _terminal_job(item):
                    self._unfinished_workflows.pop(identity, None)
                audit["confirmation_checked"] += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors["jobs_confirm:" + identity] = type(exc).__name__
                audit["unconfirmed_ids"].append(identity)
        remaining = [identity for identity in pending if identity not in attempted]
        audit["confirmation_pending"] = len(remaining)
        audit["unconfirmed_ids"].extend(remaining)
        merged = {}
        for j in all_jobs:
            key = (str(j.get("id") or j.get("jobId")), j.get("startTime"))
            old = merged.get(key)
            if old is None or not _terminal_job(old) or _terminal_job(j):
                merged[key] = j
        audit["observed_workflow_ids"] = sorted({str(j.get("id") or j.get("jobId")) for j in merged.values()
                                                if j.get("type", "").lower() == "workflow job"})
        return {"jobs": list(merged.values()), "count": len(merged)}, audit

    async def _request_gap(self):
        if getattr(self, "_request_count", 0) and self.config.rest_request_spacing_seconds:
            await asyncio.sleep(self.config.rest_request_spacing_seconds)
        self._request_count = getattr(self, "_request_count", 0) + 1

    async def _job_pages(self, endpoint, params, headers, auth):
        """Follow server-supplied same-endpoint next links; invent no paging parameters."""
        out, seen, visited, declared = [], set(), set(), 0
        for page in range(1, JOB_PAGE_LIMIT + 1):
            marker = (endpoint, tuple(sorted(params.items())))
            if marker in visited:
                raise ValueError("jobs_pagination_cycle")
            visited.add(marker)
            await self._request_gap()
            response = await self._get_json(self._rest.client, endpoint, headers, auth, params,
                                            reject_continuation=True)
            items = _job_items(response)
            for item in items:
                key = (str(item.get("id") or item.get("jobId")), item.get("startTime"))
                if key in seen:
                    raise ValueError("jobs_pagination_duplicate")
                seen.add(key)
            out.extend(items)
            if isinstance(response, dict):
                count = response.get("count")
                if count is not None and (type(count) is not int or count < 0):
                    raise ValueError("invalid_jobs_count")
                declared = max(declared, count or 0)
                links = response.get("links", [])
                if not isinstance(links, list):
                    raise ValueError("invalid_jobs_links")
                next_links = [l.get("href") for l in links if isinstance(l, dict) and l.get("rel") == "next"]
            else:
                next_links = []
            if not next_links:
                if declared > len(out):
                    raise ValueError("jobs_response_truncated")
                return out, page
            if len(next_links) != 1 or not isinstance(next_links[0], str):
                raise ValueError("invalid_jobs_next_link")
            following = httpx.URL(urljoin(self._url(endpoint), next_links[0]))
            origin = httpx.URL(self._url(endpoint))
            if (following.scheme, following.host, following.port, following.path) != (origin.scheme, origin.host, origin.port, origin.path):
                raise ValueError("unsafe_jobs_next_link")
            next_params = dict(following.params)
            if next_params.get("q") != params.get("q"):
                raise ValueError("jobs_next_filter_changed")
            params = next_params
        raise ValueError("jobs_page_budget_exhausted")

    async def _inventory_pages(self, endpoint, params, headers, auth, key):
        """Read list endpoints once per Inventory cycle, following only server next links."""
        out, visited, seen, declared = [], set(), set(), 0
        for page in range(1, JOB_PAGE_LIMIT + 1):
            marker = (endpoint, tuple(sorted(params.items())))
            if marker in visited:
                raise ValueError("inventory_pagination_cycle")
            visited.add(marker)
            await self._request_gap()
            response = await self._get_json(self._rest.client, endpoint, headers, auth, params,
                                            reject_continuation=True)
            if isinstance(response, list):
                items, links, count = response, [], len(response)
            elif isinstance(response, dict):
                items, links = response.get(key), response.get("links", [])
                count = response.get("count", len(items) if isinstance(items, list) else None)
            else:
                raise ValueError("invalid_inventory_response")
            if (not isinstance(items, list) or any(not isinstance(i, dict) for i in items)
                    or not isinstance(links, list) or type(count) is not int or count < 0):
                raise ValueError("invalid_inventory_list_or_count")
            for item in items:
                resource = item.get("resourceId")
                identity = (resource.get("id") if isinstance(resource, dict) else resource) if key == "clients" else item.get("name")
                if isinstance(identity, str) and identity:
                    if identity in seen:
                        raise ValueError("inventory_pagination_duplicate")
                    seen.add(identity)
            out.extend(items)
            declared = max(declared, count)
            following = [l.get("href") for l in links if isinstance(l, dict) and l.get("rel") == "next"]
            if not following:
                if declared > len(out):
                    raise ValueError("inventory_response_truncated")
                return out, page
            if len(following) != 1 or not isinstance(following[0], str):
                raise ValueError("invalid_inventory_next_link")
            url, origin = httpx.URL(urljoin(self._url(endpoint), following[0])), httpx.URL(self._url(endpoint))
            if ((url.scheme, url.host, url.port, url.path) != (origin.scheme, origin.host, origin.port, origin.path)
                    or any(url.params.get(k) != v for k, v in params.items() if k in {"q", "fl", "type"})):
                raise ValueError("unsafe_inventory_next_link")
            params = dict(url.params)
        raise ValueError("inventory_page_budget_exhausted")

    async def close(self) -> None:
        await self._rest.close()

    async def _get_json(
        self,
        client: httpx.AsyncClient,
        endpoint: str,
        headers: dict[str, str],
        auth: tuple[str, str] | None,
        params: dict[str, str] | None = None,
        *, reject_continuation: bool = False,
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
                if reject_continuation and response.headers.get("X-NW-CONTINUATION-TOKEN"):
                    # The known Jobs contract does not document this header.
                    # Fail closed rather than report a single page as complete.
                    raise ValueError("unsupported_jobs_continuation")
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
            field_params = {"fl": FIELD_FILTERS[name], "type": "advanced"}
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


def _job_items(response):
    items = response if isinstance(response, list) else response.get("jobs") if isinstance(response, dict) else None
    if not isinstance(items, list) or any(not isinstance(j, dict) or not (j.get("id") or j.get("jobId")) for j in items):
        raise ValueError("invalid_jobs_response")
    return items


def _terminal_job(item):
    completion = str(item.get("completionStatus") or "").lower()
    return bool(item.get("endTime")) or completion in {
        "succeeded", "success", "failed", "failure", "canceled", "cancelled", "aborted", "abandoned",
        "unknown", "communicationlost", "neverstarted", "didnotrun", "missedtheschedule"}


def _job_start(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except ValueError:
        return None


def _time_range_query(field: str, start: datetime, end: datetime) -> str:
    return f'{field}:["{_format_query_time(start)}" TO "{_format_query_time(end)}"]'


def _format_query_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


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

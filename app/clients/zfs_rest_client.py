from __future__ import annotations

from typing import Any
from urllib.parse import quote, urljoin

import httpx

from app.clients.rest_client import ERRORS_KEY, ReusableRestClient
from app.core.config import CollectionClass, CollectorConfig


DEFAULT_ENDPOINTS = {
    "version": "api/system/v1/version",
    "pools": "api/storage/v1/pools",
    "logs": "api/log/v1/logs",
    "alert_logs": "api/log/v1/logs/alert?limit=100",
    "fault_logs": "api/log/v1/logs/fault?limit=100",
}
FAST_ENDPOINTS = {"version", "pools", "logs", "alert_logs", "fault_logs"}
SLOW_ENDPOINTS = {"version", "pools"}


class ZfsRestClient:
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config
        self._rest = ReusableRestClient(
            verify_tls=config.verify_tls,
            timeout_seconds=30,
            max_concurrency=config.rest_max_concurrency,
        )

    async def fetch_payloads(
        self,
        collection_class: CollectionClass | None = None,
    ) -> dict[str, Any]:
        headers = {"Accept": "application/json"}
        auth = None

        if self.config.token:
            headers["Authorization"] = f"Bearer {self.config.token}"
        if self.config.username and self.config.password:
            auth = (self.config.username, self.config.password)

        endpoint_names = (
            set(self.config.endpoints or DEFAULT_ENDPOINTS)
            if collection_class is None
            else FAST_ENDPOINTS
            if collection_class == "fast"
            else SLOW_ENDPOINTS
        )
        endpoints = {
            name: endpoint
            for name, endpoint in (self.config.endpoints or DEFAULT_ENDPOINTS).items()
            if name in endpoint_names
        }
        payloads = await self._rest.fetch_named(
            endpoints,
            lambda client, endpoint: self._get_json(client, endpoint, headers, auth),
        )
        await self._fetch_pool_details(payloads, headers, auth)
        if collection_class in {None, "slow"}:
            await self._fetch_inventory(payloads, headers, auth)
        return payloads

    async def close(self) -> None:
        await self._rest.close()

    async def _fetch_pool_details(
        self,
        payloads: dict[str, Any],
        headers: dict[str, str],
        auth: tuple[str, str] | None,
    ) -> None:
        endpoints = {}
        for pool_name in _pool_names(payloads):
            endpoints[f"pool:{pool_name}"] = (
                f"api/storage/v1/pools/{quote(pool_name, safe='')}"
            )
        _merge_fetch_result(
            payloads,
            await self._rest.fetch_named(
                endpoints,
                lambda client, endpoint: self._get_json(client, endpoint, headers, auth),
            ),
        )

    async def _fetch_inventory(
        self,
        payloads: dict[str, Any],
        headers: dict[str, str],
        auth: tuple[str, str] | None,
    ) -> None:
        project_list_endpoints = {}
        for pool_name in _pool_names(payloads):
            pool_path = f"api/storage/v1/pools/{quote(pool_name, safe='')}"
            project_list_endpoints[f"projects:{pool_name}"] = f"{pool_path}/projects"
        _merge_fetch_result(
            payloads,
            await self._rest.fetch_named(
                project_list_endpoints,
                lambda client, endpoint: self._get_json(client, endpoint, headers, auth),
            ),
        )

        inventory_endpoints = {}
        for pool_name in _pool_names(payloads):
            projects_payload = payloads.get(f"projects:{pool_name}")
            for project in _items(projects_payload, "projects"):
                project_name = _string(project.get("name"))
                if not project_name:
                    continue

                pool_path = f"api/storage/v1/pools/{quote(pool_name, safe='')}"
                project_path = f"{pool_path}/projects/{quote(project_name, safe='')}"
                key = f"{pool_name}/{project_name}"
                inventory_endpoints[f"project:{key}"] = project_path
                inventory_endpoints[f"filesystems:{key}"] = f"{project_path}/filesystems"
                inventory_endpoints[f"luns:{key}"] = f"{project_path}/luns"
        _merge_fetch_result(
            payloads,
            await self._rest.fetch_named(
                inventory_endpoints,
                lambda client, endpoint: self._get_json(client, endpoint, headers, auth),
            ),
        )

    async def _get_json(
        self,
        client: httpx.AsyncClient,
        endpoint: str,
        headers: dict[str, str],
        auth: tuple[str, str] | None,
    ) -> Any:
        response = await client.get(self._url(endpoint), headers=headers, auth=auth)
        response.raise_for_status()
        return response.json() if response.content else {}

    def _url(self, endpoint: str) -> str:
        base_url = f"{self.config.base_url.rstrip('/')}/"
        return urljoin(base_url, endpoint.lstrip("/"))


def _items(payload: Any, key: str) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _string(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _pool_names(payloads: dict[str, Any]) -> list[str]:
    return [
        name
        for pool in _items(payloads.get("pools"), "pools")
        if (name := _string(pool.get("name")))
    ]


def _merge_fetch_result(payloads: dict[str, Any], fetched: dict[str, Any]) -> None:
    new_errors = fetched.pop(ERRORS_KEY, {})
    payloads.update(fetched)
    if new_errors:
        payloads.setdefault(ERRORS_KEY, {}).update(new_errors)

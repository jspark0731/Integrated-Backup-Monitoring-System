from __future__ import annotations

from typing import Any
from urllib.parse import urljoin

import httpx

from apps.clients.rest_client import ReusableRestClient
from apps.core.config import CollectionClass, CollectorConfig


DEFAULT_ENDPOINTS = {
    "jobs": "nwrestapi/v3/global/jobs",
    "clients": "nwrestapi/v3/global/clients",
    "backups": "nwrestapi/v3/global/backups",
    "policies": "nwrestapi/v3/global/protectionpolicies",
    "protection_groups": "nwrestapi/v3/global/protectiongroups",
}
FAST_ENDPOINTS = {"jobs", "backups"}
SLOW_ENDPOINTS = {"clients", "policies", "protection_groups"}


class NetworkerRestClient:
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

        endpoints = _endpoints_for_class(
            self.config.endpoints or DEFAULT_ENDPOINTS,
            collection_class,
        )
        return await self._rest.fetch_named(
            endpoints,
            lambda client, endpoint: self._get_json(client, endpoint, headers, auth),
        )

    async def close(self) -> None:
        await self._rest.close()

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


def _endpoints_for_class(
    endpoints: dict[str, str],
    collection_class: CollectionClass | None,
) -> dict[str, str]:
    if collection_class is None:
        return dict(endpoints)
    selected = FAST_ENDPOINTS if collection_class == "fast" else SLOW_ENDPOINTS
    return {name: endpoint for name, endpoint in endpoints.items() if name in selected}

from __future__ import annotations

from typing import Any
from urllib.parse import quote, urljoin

import httpx

from app.core.config import CollectorConfig


DEFAULT_ENDPOINTS = {
    "version": "api/system/v1/version",
    "pools": "api/storage/v1/pools",
    "logs": "api/log/v1/logs",
    "alert_logs": "api/log/v1/logs/alert?limit=100",
    "fault_logs": "api/log/v1/logs/fault?limit=100",
}


class ZfsRestClient:
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config

    async def fetch_payloads(self) -> dict[str, Any]:
        headers = {"Accept": "application/json"}
        auth = None

        if self.config.token:
            headers["Authorization"] = f"Bearer {self.config.token}"
        if self.config.username and self.config.password:
            auth = (self.config.username, self.config.password)

        async with httpx.AsyncClient(verify=self.config.verify_tls, timeout=30) as client:
            payloads = {}
            for name, endpoint in (self.config.endpoints or DEFAULT_ENDPOINTS).items():
                payloads[name] = await self._get_json(client, endpoint, headers, auth)

            await self._fetch_pool_children(client, payloads, headers, auth)
        return payloads

    async def _fetch_pool_children(
        self,
        client: httpx.AsyncClient,
        payloads: dict[str, Any],
        headers: dict[str, str],
        auth: tuple[str, str] | None,
    ) -> None:
        for pool in _items(payloads.get("pools"), "pools"):
            pool_name = _string(pool.get("name"))
            if not pool_name:
                continue

            pool_path = f"api/storage/v1/pools/{quote(pool_name, safe='')}"
            payloads[f"pool:{pool_name}"] = await self._get_json(client, pool_path, headers, auth)
            projects_payload = await self._get_json(client, f"{pool_path}/projects", headers, auth)
            payloads[f"projects:{pool_name}"] = projects_payload

            for project in _items(projects_payload, "projects"):
                project_name = _string(project.get("name"))
                if not project_name:
                    continue

                project_path = f"{pool_path}/projects/{quote(project_name, safe='')}"
                payloads[f"project:{pool_name}/{project_name}"] = await self._get_json(
                    client,
                    project_path,
                    headers,
                    auth,
                )
                payloads[f"filesystems:{pool_name}/{project_name}"] = await self._get_json(
                    client,
                    f"{project_path}/filesystems",
                    headers,
                    auth,
                )
                payloads[f"luns:{pool_name}/{project_name}"] = await self._get_json(
                    client,
                    f"{project_path}/luns",
                    headers,
                    auth,
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

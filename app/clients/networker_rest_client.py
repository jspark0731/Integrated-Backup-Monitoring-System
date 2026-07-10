from __future__ import annotations

from typing import Any
from urllib.parse import urljoin

import httpx

from app.core.config import CollectorConfig


DEFAULT_ENDPOINTS = {
    "jobs": "nwrestapi/v3/global/jobs",
    "clients": "nwrestapi/v3/global/clients",
    "backups": "nwrestapi/v3/global/backups",
    "policies": "nwrestapi/v3/global/protectionpolicies",
    "protection_groups": "nwrestapi/v3/global/protectiongroups",
}


class NetworkerRestClient:
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
                response = await client.get(self._url(endpoint), headers=headers, auth=auth)
                response.raise_for_status()
                payloads[name] = response.json() if response.content else {}
        return payloads

    def _url(self, endpoint: str) -> str:
        base_url = f"{self.config.base_url.rstrip('/')}/"
        return urljoin(base_url, endpoint.lstrip("/"))

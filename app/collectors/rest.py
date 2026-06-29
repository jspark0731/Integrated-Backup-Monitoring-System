from __future__ import annotations

import httpx

from app.collectors.base import BaseCollector


class RestCollector(BaseCollector):
    async def _collect_payload(self) -> dict:
        headers = {}
        auth = None

        if self.config.token:
            headers["Authorization"] = f"Bearer {self.config.token}"
        if self.config.username and self.config.password:
            auth = (self.config.username, self.config.password)

        async with httpx.AsyncClient(verify=self.config.verify_tls, timeout=10) as client:
            response = await client.request(
                method=self.config.method,
                url=self._url(),
                headers=headers,
                auth=auth,
            )
            response.raise_for_status()

        if not response.content:
            return {}
        content_type = response.headers.get("content-type", "")
        if "json" in content_type.lower():
            return response.json()
        return {"body": response.text}

    def _url(self) -> str:
        base_url = self.config.base_url.rstrip("/")
        endpoint = self.config.endpoint
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        return f"{base_url}{endpoint}"

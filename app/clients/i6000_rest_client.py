from __future__ import annotations

from typing import Any
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

import httpx

from app.core.config import CollectorConfig


DEFAULT_ENDPOINTS = {
    "ping": "aml/",
    "physical_library": "aml/physicalLibrary",
    "status": "aml/physicalLibrary/status",
    "drives": "aml/drives",
    "media": "aml/media?start=0&length=-1",
    "segments_storage_used": "aml/physicalLibrary/segments?type=storage&status=used&start=0&length=-1",
    "segments_storage_available": "aml/physicalLibrary/segments?type=storage&status=available&start=0&length=-1",
    "towers": "aml/devices/towers",
    "ie_stations": "aml/devices/ieStations",
    "ras_status": "aml/system/ras",
    "ras_tickets": "aml/system/ras/tickets",
}


class I6000RestClient:
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config

    async def fetch_payloads(self) -> dict[str, Any]:
        headers = {"Accept": "application/json"}

        async with httpx.AsyncClient(verify=self.config.verify_tls, timeout=20) as client:
            if self.config.username and self.config.password:
                await self._login(client, headers)

            try:
                return {
                    name: await self._get(client, endpoint, headers)
                    for name, endpoint in (self.config.endpoints or DEFAULT_ENDPOINTS).items()
                }
            finally:
                if self.config.username and self.config.password:
                    await self._logout(client, headers)

    async def _login(self, client: httpx.AsyncClient, headers: dict[str, str]) -> None:
        response = await client.post(
            self._url("aml/users/login"),
            data={
                "name": self.config.username,
                "password": self.config.password,
                "ldap": "false",
                "forceLogin": "true",
            },
            headers=headers,
        )
        response.raise_for_status()

    async def _logout(self, client: httpx.AsyncClient, headers: dict[str, str]) -> None:
        try:
            await client.delete(self._url("aml/users/login"), headers=headers)
        except httpx.HTTPError:
            pass

    async def _get(self, client: httpx.AsyncClient, endpoint: str, headers: dict[str, str]) -> Any:
        response = await client.get(self._url(endpoint), headers=headers)
        response.raise_for_status()
        return _decode_response(response)

    def _url(self, endpoint: str) -> str:
        base_url = f"{self.config.base_url.rstrip('/')}/"
        return urljoin(base_url, endpoint.lstrip("/"))


def _decode_response(response: httpx.Response) -> Any:
    content_type = response.headers.get("content-type", "").lower()
    if "json" in content_type:
        return response.json()
    text = response.text.strip()
    if text.startswith("{") or text.startswith("["):
        return response.json()
    if text.startswith("<"):
        return _xml_to_obj(ET.fromstring(text))
    return {"body": text}


def _xml_to_obj(element: ET.Element) -> dict[str, Any]:
    key = element.tag.rsplit("}", 1)[-1]
    children = list(element)
    if not children:
        return {key: (element.text or "").strip()}

    data: dict[str, Any] = {}
    for child in children:
        child_key, child_value = next(iter(_xml_to_obj(child).items()))
        if child_key in data:
            if not isinstance(data[child_key], list):
                data[child_key] = [data[child_key]]
            data[child_key].append(child_value)
        else:
            data[child_key] = child_value
    return {key: data}

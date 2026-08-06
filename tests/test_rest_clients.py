import asyncio

import httpx
import pytest

from apps.clients.i6000_rest_client import I6000RestClient
from apps.clients.rest_client import ERRORS_KEY, ReusableRestClient
from apps.core.config import CollectorConfig


def i6000_config(max_concurrency: int = 4) -> CollectorConfig:
    return CollectorConfig(
        name="i6000_core_rest",
        type="i6000",
        protocol="rest",
        enabled=True,
        base_url="https://i6000.example.com",
        username="admin",
        password="secret",
        endpoints={
            "status": "status",
            "drives": "drives",
            "media": "media",
        },
        rest_max_concurrency=max_concurrency,
    )


@pytest.mark.asyncio
async def test_reusable_client_limits_endpoint_concurrency() -> None:
    active = 0
    peak = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return httpx.Response(200, json={"path": request.url.path})

    rest = ReusableRestClient(
        verify_tls=True,
        timeout_seconds=10,
        max_concurrency=2,
    )
    rest._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    endpoints = {f"endpoint_{index}": f"https://example.com/{index}" for index in range(6)}

    payloads = await rest.fetch_named(
        endpoints,
        lambda client, endpoint: _get_json(client, endpoint),
    )
    await rest.close()

    assert len(payloads) == 6
    assert peak == 2


@pytest.mark.asyncio
async def test_endpoint_failure_keeps_successful_payloads() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        status = 500 if request.url.path == "/failed" else 200
        return httpx.Response(status, json={"path": request.url.path})

    rest = ReusableRestClient(
        verify_tls=True,
        timeout_seconds=10,
        max_concurrency=4,
    )
    rest._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    payloads = await rest.fetch_named(
        {
            "successful": "https://example.com/successful",
            "failed": "https://example.com/failed",
        },
        lambda client, endpoint: _get_json(client, endpoint),
    )
    await rest.close()

    assert payloads["successful"] == {"path": "/successful"}
    assert "failed" not in payloads
    assert "HTTPStatusError" in payloads[ERRORS_KEY]["failed"]


@pytest.mark.asyncio
async def test_i6000_reuses_http_client_and_filters_collection_class() -> None:
    requested_paths = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requested_paths.append((request.method, request.url.path))
        return httpx.Response(200, json={"ok": True})

    client = I6000RestClient(i6000_config())
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client._rest._client = http_client

    first_client = client._rest.client
    fast_payloads = await client.fetch_payloads("fast")
    second_client = client._rest.client
    slow_payloads = await client.fetch_payloads("slow")
    await client.close()

    assert first_client is second_client
    assert set(fast_payloads) == {"status", "drives"}
    assert set(slow_payloads) == {"media"}
    assert requested_paths.count(("POST", "/aml/users/login")) == 2
    assert requested_paths.count(("DELETE", "/aml/users/login")) == 2
    assert http_client.is_closed


async def _get_json(client: httpx.AsyncClient, endpoint: str) -> dict:
    response = await client.get(endpoint)
    response.raise_for_status()
    return response.json()

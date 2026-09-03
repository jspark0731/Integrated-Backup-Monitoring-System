import asyncio
import logging
from datetime import datetime, timezone

import httpx
import pytest

from apps.clients.i6000_rest_client import I6000RestClient
from apps.clients.networker_rest_client import NetworkerRestClient
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


@pytest.mark.asyncio
async def test_networker_uses_basic_auth_without_bearer_token() -> None:
    authorization_headers = []
    requests = []

    async def handler(request: httpx.Request) -> httpx.Response:
        authorization_headers.append(request.headers.get("Authorization"))
        requests.append((request.url.path, dict(request.url.params)))
        if request.url.path.endswith("/jobs"):
            return httpx.Response(200, json={"jobs": [{"id": "done-1"}]})
        return httpx.Response(200, json={"backups": []})

    client = NetworkerRestClient(
        CollectorConfig(
            name="networker_core",
            type="Networker",
            protocol="rest",
            enabled=True,
            base_url="https://networker.example.com:9090",
            username="rest-user",
            password="secret",
            source_networker="core",
            verify_tls=False,
        )
    )
    client._rest._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    payloads = await client.fetch_payloads(
        "fast",
        now=datetime(2026, 8, 28, 12, 30, tzinfo=timezone.utc),
    )
    await client.close()

    assert set(payloads) == {"jobs", "backups", "_query_window"}
    assert [item["id"] for item in payloads["jobs"]["jobs"]] == ["done-1"]
    assert [path for path, _ in requests] == [
        "/nwrestapi/v3/global/jobs",
        "/nwrestapi/v3/global/backups",
    ]
    assert requests[0][1]["q"] == (
        'endTime:["2026-08-28T12:20:00" TO "2026-08-28T12:30:00"]'
    )
    assert requests[1][1]["q"] == (
        'saveTime:["2026-08-28T12:10:00" TO "2026-08-28T12:30:00"]'
    )
    assert all("fl" not in params for _, params in requests)
    assert authorization_headers
    assert all(value and value.startswith("Basic ") for value in authorization_headers)
    assert not any(value and value.startswith("Bearer ") for value in authorization_headers)


@pytest.mark.asyncio
async def test_networker_spaces_sequential_requests(monkeypatch) -> None:
    sleeps = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    async def handler(request: httpx.Request) -> httpx.Response:
        key = "jobs" if request.url.path.endswith("/jobs") else "backups"
        return httpx.Response(200, json={key: []})

    monkeypatch.setattr("apps.clients.networker_rest_client.asyncio.sleep", fake_sleep)
    client = NetworkerRestClient(
        CollectorConfig(
            name="networker_core",
            type="Networker",
            protocol="rest",
            enabled=True,
            base_url="https://networker.example.com:9090",
            username="rest-user",
            password="secret",
            source_networker="core",
            verify_tls=False,
            rest_request_spacing_seconds=30,
        )
    )
    client._rest._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    await client.fetch_payloads("fast")
    await client.close()

    assert sleeps == [30]


@pytest.mark.asyncio
async def test_networker_uses_class_specific_endpoints_and_windows() -> None:
    requests = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.url.path, dict(request.url.params)))
        key = "jobs" if request.url.path.endswith("/jobs") else "backups"
        return httpx.Response(200, json={key: []})

    client = NetworkerRestClient(
        CollectorConfig(
            name="networker_core",
            type="Networker",
            protocol="rest",
            enabled=True,
            base_url="https://networker.example.com:9090",
            username="rest-user",
            password="secret",
            source_networker="core",
            verify_tls=False,
            networker_jobs_query_window_minutes=10,
            networker_backups_query_window_minutes=20,
        )
    )
    client._rest._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    now = datetime(2026, 8, 28, 12, 30, tzinfo=timezone.utc)

    jobs = await client.fetch_payloads("jobs", now=now)
    backups = await client.fetch_payloads("backups", now=now)
    await client.close()

    assert [path for path, _ in requests] == [
        "/nwrestapi/v3/global/jobs",
        "/nwrestapi/v3/global/backups",
    ]
    assert requests[0][1]["q"] == (
        'endTime:["2026-08-28T12:20:00" TO "2026-08-28T12:30:00"]'
    )
    assert requests[1][1]["q"] == (
        'saveTime:["2026-08-28T12:10:00" TO "2026-08-28T12:30:00"]'
    )
    assert jobs["_query_window"]["minutes"] == 10
    assert backups["_query_window"]["minutes"] == 20


@pytest.mark.asyncio
async def test_networker_http_400_does_not_retry_without_query() -> None:
    requests = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(dict(request.url.params))
        return httpx.Response(400, json={"message": "Query filter error"})

    client = NetworkerRestClient(
        CollectorConfig(
            name="networker_core",
            type="Networker",
            protocol="rest",
            enabled=True,
            base_url="https://networker.example.com:9090",
            username="rest-user",
            password="secret",
            source_networker="core",
            verify_tls=False,
            retries=2,
        )
    )
    client._rest._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    payloads = await client.fetch_payloads("backups")
    await client.close()

    assert len(requests) == 1
    assert requests[0]["q"].startswith('saveTime:[')
    assert "fl" not in requests[0]
    assert "backups" in payloads[ERRORS_KEY]


@pytest.mark.asyncio
async def test_networker_http_500_retries_only_bounded_filtered_request(
    monkeypatch,
) -> None:
    requests = []

    async def no_sleep(_seconds: float) -> None:
        return None

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(dict(request.url.params))
        return httpx.Response(500, json={"message": "server error"})

    monkeypatch.setattr("apps.clients.networker_rest_client.asyncio.sleep", no_sleep)
    client = NetworkerRestClient(
        CollectorConfig(
            name="networker_core",
            type="Networker",
            protocol="rest",
            enabled=True,
            base_url="https://networker.example.com:9090",
            username="rest-user",
            password="secret",
            source_networker="core",
            verify_tls=False,
            retries=1,
        )
    )
    client._rest._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    payloads = await client.fetch_payloads("jobs")
    await client.close()

    assert len(requests) == 2
    assert all(params["q"].startswith('endTime:[') for params in requests)
    assert all("fl" not in params for params in requests)
    assert "jobs" in payloads[ERRORS_KEY]


@pytest.mark.asyncio
async def test_networker_timeout_retries_only_bounded_filtered_request(
    monkeypatch,
) -> None:
    requests = []

    async def no_sleep(_seconds: float) -> None:
        return None

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(dict(request.url.params))
        raise httpx.ReadTimeout("timed out", request=request)

    monkeypatch.setattr("apps.clients.networker_rest_client.asyncio.sleep", no_sleep)
    client = NetworkerRestClient(
        CollectorConfig(
            name="networker_core",
            type="Networker",
            protocol="rest",
            enabled=True,
            base_url="https://networker.example.com:9090",
            username="rest-user",
            password="secret",
            source_networker="core",
            verify_tls=False,
            retries=1,
        )
    )
    client._rest._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    payloads = await client.fetch_payloads("backups")
    await client.close()

    assert len(requests) == 2
    assert all(params["q"].startswith('saveTime:[') for params in requests)
    assert all("fl" not in params for params in requests)
    assert "backups" in payloads[ERRORS_KEY]


@pytest.mark.asyncio
async def test_networker_http_429_retries_with_backoff(monkeypatch) -> None:
    attempts = 0
    sleeps = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            return httpx.Response(429, json={"message": "rate limited"})
        return httpx.Response(200, json={"jobs": []})

    monkeypatch.setattr("apps.clients.networker_rest_client.asyncio.sleep", fake_sleep)
    client = NetworkerRestClient(
        CollectorConfig(
            name="networker_core",
            type="Networker",
            protocol="rest",
            enabled=True,
            base_url="https://networker.example.com:9090",
            username="rest-user",
            password="secret",
            verify_tls=False,
            retries=2,
        )
    )
    client._rest._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    payloads = await client.fetch_payloads("jobs")
    await client.close()

    assert attempts == 3
    assert sleeps == [1.0, 2.0]
    assert "jobs" in payloads


@pytest.mark.asyncio
async def test_networker_debug_log_contains_q_without_fl(caplog) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"jobs": []})

    caplog.set_level(logging.DEBUG, logger="apps.clients.networker_rest_client")
    client = NetworkerRestClient(
        CollectorConfig(
            name="networker_core",
            type="Networker",
            protocol="rest",
            enabled=True,
            base_url="https://networker.example.com:9090",
            username="rest-user",
            password="secret",
            source_networker="core",
            verify_tls=False,
        )
    )
    client._rest._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    await client.fetch_payloads(
        "jobs",
        now=datetime(2026, 8, 28, 12, 30, tzinfo=timezone.utc),
    )
    await client.close()

    message = next(
        record.getMessage()
        for record in caplog.records
        if record.name == "apps.clients.networker_rest_client"
    )
    assert 'endTime:["2026-08-28T12:20:00" TO "2026-08-28T12:30:00"]' in message
    assert "fl=None" in message


@pytest.mark.asyncio
async def test_networker_inventory_only_requests_clients_and_policies() -> None:
    requests = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.url.path, dict(request.url.params)))
        if request.url.path.endswith("/clients"):
            return httpx.Response(200, json={"clients": []})
        return httpx.Response(200, json={"protectionPolicies": []})

    client = NetworkerRestClient(
        CollectorConfig(
            name="networker_core",
            type="Networker",
            protocol="rest",
            enabled=True,
            base_url="https://networker.example.com:9090",
            username="rest-user",
            password="secret",
            source_networker="core",
            verify_tls=False,
            networker_field_filter=False,
        )
    )
    client._rest._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    payloads = await client.fetch_payloads("inventory")
    await client.close()

    assert [path for path, _ in requests] == [
        "/nwrestapi/v3/global/clients",
        "/nwrestapi/v3/global/protectionpolicies",
    ]
    assert requests[0][1]["fl"] == (
        "resourceId,hostname,clientState,operatingSystem,backupType"
    )
    assert "fl" not in requests[1][1]
    assert set(payloads) == {"clients", "policies"}


async def _get_json(client: httpx.AsyncClient, endpoint: str) -> dict:
    response = await client.get(endpoint)
    response.raise_for_status()
    return response.json()

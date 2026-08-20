from __future__ import annotations

import asyncio
import ssl
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import httpx


ERRORS_KEY = "_endpoint_errors"


class ReusableRestClient:
    def __init__(
        self,
        *,
        verify_tls: bool,
        ca_certs: str | None = None,
        timeout_seconds: float,
        max_concurrency: int,
    ) -> None:
        self._verify_tls = verify_tls
        self._ca_certs = ca_certs
        self._timeout_seconds = timeout_seconds
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._client: httpx.AsyncClient | None = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            verify: bool | ssl.SSLContext = self._verify_tls
            if self._verify_tls and self._ca_certs:
                verify = ssl.create_default_context(cafile=self._ca_certs)
            self._client = httpx.AsyncClient(
                verify=verify,
                timeout=self._timeout_seconds,
            )
        return self._client

    async def fetch_named(
        self,
        endpoints: Mapping[str, str],
        fetch: Callable[[httpx.AsyncClient, str], Awaitable[Any]],
    ) -> dict[str, Any]:
        names = list(endpoints)

        async def fetch_limited(name: str) -> Any:
            async with self._semaphore:
                return await fetch(self.client, endpoints[name])

        results = await asyncio.gather(
            *(fetch_limited(name) for name in names),
            return_exceptions=True,
        )
        payloads: dict[str, Any] = {}
        errors: dict[str, str] = {}
        for name, result in zip(names, results, strict=True):
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, BaseException):
                errors[name] = f"{type(result).__name__}: {result}"
            else:
                payloads[name] = result
        if errors:
            payloads[ERRORS_KEY] = errors
        return payloads

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


def endpoint_errors(payloads: Mapping[str, Any]) -> dict[str, str]:
    errors = payloads.get(ERRORS_KEY)
    return dict(errors) if isinstance(errors, dict) else {}

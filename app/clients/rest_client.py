from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import httpx


ERRORS_KEY = "_endpoint_errors"


class ReusableRestClient:
    def __init__(
        self,
        *,
        verify_tls: bool,
        timeout_seconds: float,
        max_concurrency: int,
    ) -> None:
        self._verify_tls = verify_tls
        self._timeout_seconds = timeout_seconds
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._client: httpx.AsyncClient | None = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                verify=self._verify_tls,
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

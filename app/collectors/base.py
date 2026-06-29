from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
import logging
import time

from app.core.config import CollectorConfig
from app.core.metrics import COLLECTION_DURATION, COLLECTION_TOTAL, COLLECTOR_LAST_SUCCESS_TIMESTAMP, COLLECTOR_SKIPPED
from app.models import CollectionResult

LOGGER = logging.getLogger(__name__)


class BaseCollector(ABC):
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def target_type(self) -> str:
        return self.config.type

    @property
    def protocol(self) -> str:
        return self.config.protocol

    async def collect(self) -> CollectionResult:
        skip_reason = self.config.skip_reason
        if skip_reason:
            LOGGER.warning("Skipping collector %s: %s", self.name, skip_reason)
            COLLECTOR_SKIPPED.labels(self.name, self.target_type, skip_reason).set(1)
            COLLECTION_TOTAL.labels(self.name, self.target_type, self.protocol, "skipped").inc()
            return CollectionResult.skipped_result(self.name, self.target_type, self.protocol, skip_reason)

        started = time.monotonic()
        try:
            payload = await self._collect_payload()
            result = CollectionResult(
                collector=self.name,
                target_type=self.target_type,
                protocol=self.protocol,
                collected_at=datetime.now(timezone.utc),
                ok=True,
                payload=payload,
            )
            COLLECTION_TOTAL.labels(self.name, self.target_type, self.protocol, "success").inc()
            COLLECTOR_LAST_SUCCESS_TIMESTAMP.labels(self.name).set(result.collected_at.timestamp())
            return result
        except Exception as exc:
            LOGGER.exception("Collector %s failed", self.name)
            COLLECTION_TOTAL.labels(self.name, self.target_type, self.protocol, "error").inc()
            return CollectionResult(
                collector=self.name,
                target_type=self.target_type,
                protocol=self.protocol,
                collected_at=datetime.now(timezone.utc),
                ok=False,
                error=str(exc),
            )
        finally:
            duration = time.monotonic() - started
            COLLECTION_DURATION.labels(self.name, self.target_type, self.protocol).observe(duration)

    @abstractmethod
    async def _collect_payload(self) -> dict:
        raise NotImplementedError

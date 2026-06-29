from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import logging

from app.collectors.base import BaseCollector
from app.models import CollectionResult
from app.writers.elasticsearch import ElasticsearchWriter

LOGGER = logging.getLogger(__name__)


class CollectorScheduler:
    def __init__(self, collectors: list[BaseCollector], writer: ElasticsearchWriter) -> None:
        self.collectors = collectors
        self.writer = writer
        self._tasks: list[asyncio.Task] = []
        self._last_results: dict[str, CollectionResult] = {}

    @property
    def last_results(self) -> dict[str, CollectionResult]:
        return self._last_results

    async def start(self) -> None:
        for collector in self.collectors:
            task = asyncio.create_task(self._run_collector_forever(collector))
            self._tasks.append(task)
            LOGGER.info(
                "Scheduled collector %s/%s at every minute %02d second",
                collector.target_type,
                collector.name,
                collector.config.schedule_second,
            )

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.writer.close()

    async def run_once(self) -> list[CollectionResult]:
        results = await asyncio.gather(*(collector.collect() for collector in self.collectors))
        await self.writer.write_many(results)
        self._last_results.update({result.collector: result for result in results})
        return list(results)

    async def _run_collector_forever(self, collector: BaseCollector) -> None:
        while True:
            await asyncio.sleep(seconds_until_next_run(collector.config.schedule_second))
            result = await collector.collect()
            await self.writer.write_many([result])
            self._last_results[result.collector] = result


def seconds_until_next_run(second: int, now: datetime | None = None) -> float:
    if second < 0 or second > 59:
        raise ValueError(f"Invalid schedule second: {second}")

    current = now or datetime.now(timezone.utc)
    current = current.astimezone(timezone.utc)
    target = current.replace(second=second, microsecond=0)
    if target <= current:
        target += timedelta(minutes=1)
    return max(0.0, (target - current).total_seconds())


from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import logging

from app.collectors.base import BaseCollector
from app.core.config import CollectionClass, ScheduleConfig
from app.models import CollectionResult
from app.writers.elasticsearch import ElasticsearchWriter

LOGGER = logging.getLogger(__name__)


class CollectorScheduler:
    def __init__(self, collectors: list[BaseCollector], writer: ElasticsearchWriter) -> None:
        self.collectors = collectors
        self.writer = writer
        self._tasks: list[asyncio.Task] = []
        self._last_results: dict[str, CollectionResult] = {}
        self._last_results_by_class: dict[str, dict[CollectionClass, CollectionResult]] = {}

    @property
    def last_results(self) -> dict[str, CollectionResult]:
        return self._last_results

    @property
    def last_results_by_class(self) -> dict[str, dict[CollectionClass, CollectionResult]]:
        return self._last_results_by_class

    async def start(self) -> None:
        for collector in self.collectors:
            for collection_class, schedule in collector.config.effective_schedules:
                task = asyncio.create_task(
                    self._run_collector_forever(collector, collection_class, schedule)
                )
                self._tasks.append(task)
                LOGGER.info(
                    "Scheduled collector %s/%s class=%s every %d minutes "
                    "at minute offset %d second %02d",
                    collector.target_type,
                    collector.name,
                    collection_class,
                    schedule.interval_minutes,
                    schedule.minute_offset,
                    schedule.second,
                )

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.writer.close()

    async def run_once(self) -> list[CollectionResult]:
        jobs = (
            collector.collect(collection_class)
            for collector in self.collectors
            for collection_class, _ in collector.config.effective_schedules
        )
        results = await asyncio.gather(*jobs)
        await self.writer.write_many(results)
        for result in results:
            self._record_result(result)
        return list(results)

    async def _run_collector_forever(
        self,
        collector: BaseCollector,
        collection_class: CollectionClass = "fast",
        schedule: ScheduleConfig | None = None,
    ) -> None:
        while True:
            active_schedule = schedule or collector.config.effective_schedule
            await asyncio.sleep(
                seconds_until_next_run(
                    active_schedule.interval_minutes,
                    active_schedule.minute_offset,
                    active_schedule.second,
                )
            )
            result = await collector.collect(collection_class)
            await self.writer.write_many([result])
            self._record_result(result)

    def _record_result(self, result: CollectionResult) -> None:
        self._last_results[result.collector] = result
        self._last_results_by_class.setdefault(result.collector, {})[
            result.collection_class
        ] = result


def seconds_until_next_run(
    interval_minutes: int | ScheduleConfig,
    minute_offset: int | None = None,
    second: int | None = None,
    now: datetime | None = None,
) -> float:
    if isinstance(interval_minutes, ScheduleConfig):
        schedule = interval_minutes
    else:
        if minute_offset is None or second is None:
            raise ValueError("minute_offset and second are required")
        schedule = ScheduleConfig(
            interval_minutes=interval_minutes,
            minute_offset=minute_offset,
            second=second,
        )

    schedule_skip_reason = schedule.skip_reason
    if schedule_skip_reason:
        raise ValueError(schedule_skip_reason)

    current = now or datetime.now(timezone.utc)
    current = current.astimezone(timezone.utc)

    minute_base = current.minute - (current.minute % schedule.interval_minutes)
    target_minute = minute_base + schedule.minute_offset
    target = current.replace(minute=0, second=schedule.second, microsecond=0) + timedelta(minutes=target_minute)
    if target <= current:
        target += timedelta(minutes=schedule.interval_minutes)
    return max(0.0, (target - current).total_seconds())

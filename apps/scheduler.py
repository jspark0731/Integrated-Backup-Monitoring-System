from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import logging
import time

from apps.collectors.base import BaseCollector
from apps.core.config import CollectionClass, ScheduleConfig
from apps.core.metrics import (
    SCHEDULER_LOOP_ERROR_TOTAL,
    SCHEDULER_OVERLAP_SKIPPED_TOTAL,
)
from apps.models import CollectionResult
from apps.writers.elasticsearch import ElasticsearchWriter

LOGGER = logging.getLogger(__name__)


class CollectorScheduler:
    def __init__(self, collectors: list[BaseCollector], writer: ElasticsearchWriter) -> None:
        self.collectors = collectors
        self.writer = writer
        self._tasks: list[asyncio.Task] = []
        self._last_results: dict[str, CollectionResult] = {}
        self._last_results_by_class: dict[str, dict[CollectionClass, CollectionResult]] = {}
        self._collector_locks = {
            collector.name: asyncio.Lock() for collector in collectors
        }
        self._collector_finished_at: dict[str, float] = {}

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
                    self._run_collector_forever(collector, collection_class, schedule),
                    name=f"collector:{collector.name}:{collection_class}",
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
        self._tasks.clear()
        close_results = await asyncio.gather(
            *(collector.close() for collector in self.collectors),
            return_exceptions=True,
        )
        for collector, result in zip(self.collectors, close_results, strict=True):
            if isinstance(result, BaseException):
                LOGGER.error(
                    "Failed to close collector %s",
                    collector.name,
                    exc_info=(type(result), result, result.__traceback__),
                )
        await self.writer.close()

    async def run_once(self) -> list[CollectionResult]:
        async def collect_one(collector: BaseCollector) -> list[CollectionResult]:
            return [
                await collector.collect(collection_class)
                for collection_class, _ in collector.config.effective_schedules
            ]

        grouped_results = await asyncio.gather(
            *(collect_one(collector) for collector in self.collectors)
        )
        results = [
            result for collector_results in grouped_results for result in collector_results
        ]
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
            try:
                active_schedule = schedule or collector.config.effective_schedule
                await asyncio.sleep(
                    seconds_until_next_run(
                        active_schedule.interval_minutes,
                        active_schedule.minute_offset,
                        active_schedule.second,
                    )
                )
                await self._collect_write_record(collector, collection_class)
            except asyncio.CancelledError:
                raise
            except Exception:
                SCHEDULER_LOOP_ERROR_TOTAL.labels(
                    collector.name,
                    collection_class,
                ).inc()
                LOGGER.exception(
                    "Unexpected scheduler error for collector %s class=%s; "
                    "continuing with the next schedule",
                    collector.name,
                    collection_class,
                )

    async def _collect_write_record(
        self,
        collector: BaseCollector,
        collection_class: CollectionClass,
    ) -> None:
        lock = self._collector_locks[collector.name]
        if collection_class == "jobs" and lock.locked():
            SCHEDULER_OVERLAP_SKIPPED_TOTAL.labels(
                collector.name,
                collection_class,
            ).inc()
            LOGGER.warning(
                "Skipping overlapping collector run %s class=%s",
                collector.name,
                collection_class,
            )
            return

        async with lock:
            await self._wait_for_networker_cooldown(collector)
            try:
                result = await collector.collect(collection_class)
                await self.writer.write_many([result])
                self._record_result(result)
            finally:
                self._collector_finished_at[collector.name] = time.monotonic()

    async def _wait_for_networker_cooldown(self, collector: BaseCollector) -> None:
        if collector.target_type != "Networker":
            return
        finished_at = self._collector_finished_at.get(collector.name)
        if finished_at is None:
            return
        remaining = (
            collector.config.rest_request_spacing_seconds
            - (time.monotonic() - finished_at)
        )
        if remaining > 0:
            await asyncio.sleep(remaining)

    def _record_result(self, result: CollectionResult) -> None:
        status_result = _status_result(result)
        self._last_results[result.collector] = status_result
        self._last_results_by_class.setdefault(result.collector, {})[
            result.collection_class
        ] = status_result


def _status_result(result: CollectionResult) -> CollectionResult:
    payload = result.payload if isinstance(result.payload, dict) else {}
    status_payload = {
        key: payload[key]
        for key in ("summary", "collection_status", "endpoint_errors")
        if key in payload
    }
    return replace(result, payload=status_payload)


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

    # Anchor intervals to UTC midnight. This preserves sub-hour schedules and
    # lets 1440 minutes represent a stable daily execution time.
    day_start = current.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed_minutes = int((current - day_start).total_seconds() // 60)
    minute_base = elapsed_minutes - (elapsed_minutes % schedule.interval_minutes)
    target_minute = minute_base + schedule.minute_offset
    target = day_start + timedelta(minutes=target_minute, seconds=schedule.second)
    if target <= current:
        target += timedelta(minutes=schedule.interval_minutes)
    return max(0.0, (target - current).total_seconds())

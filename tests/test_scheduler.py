from datetime import datetime, timezone

import pytest

from app.collectors.base import BaseCollector
from app.core.config import (
    CollectionSchedulesConfig,
    CollectorConfig,
    ScheduleConfig,
)
from app.scheduler import CollectorScheduler, seconds_until_next_run


def test_seconds_until_next_run_dxi_offset_zero() -> None:
    now = datetime(2026, 6, 26, 12, 0, 10, tzinfo=timezone.utc)

    assert seconds_until_next_run(5, 0, 0, now) == 290


def test_seconds_until_next_run_dd_offset_one_minute() -> None:
    now = datetime(2026, 6, 26, 12, 0, 30, tzinfo=timezone.utc)

    assert seconds_until_next_run(5, 1, 0, now) == 30


def test_seconds_until_next_run_i6000_rolls_to_next_window() -> None:
    now = datetime(2026, 6, 26, 12, 2, 0, tzinfo=timezone.utc)

    assert seconds_until_next_run(5, 2, 0, now) == 300


def test_seconds_until_next_run_rejects_invalid_offset() -> None:
    with pytest.raises(ValueError):
        seconds_until_next_run(5, 5, 0)


class RecordingCollector(BaseCollector):
    def __init__(self, config: CollectorConfig) -> None:
        super().__init__(config)
        self.calls = 0

    async def _collect_payload(self) -> dict:
        self.calls += 1
        return {"calls": self.calls}


class RecordingWriter:
    def __init__(self) -> None:
        self.results = []

    async def write_many(self, results) -> None:
        self.results.extend(results)

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_run_once_runs_both_configured_collection_classes() -> None:
    collector = RecordingCollector(
        CollectorConfig(
            name="i6000_core_rest",
            type="i6000",
            protocol="rest",
            enabled=True,
            schedule=CollectionSchedulesConfig(
                fast=ScheduleConfig(5, 2, 0),
                slow=ScheduleConfig(60, 2, 30),
            ),
            base_url="https://i6000.example.com",
            username="admin",
            password="secret",
            endpoints={"status": "status"},
        )
    )
    writer = RecordingWriter()
    scheduler = CollectorScheduler([collector], writer)

    results = await scheduler.run_once()

    assert [result.collection_class for result in results] == ["fast", "slow"]
    assert [result.collection_class for result in writer.results] == ["fast", "slow"]
    assert scheduler.last_results["i6000_core_rest"].collection_class == "slow"
    assert set(scheduler.last_results_by_class["i6000_core_rest"]) == {"fast", "slow"}


@pytest.mark.asyncio
async def test_run_once_keeps_single_schedule_fast_only() -> None:
    collector = RecordingCollector(
        CollectorConfig(
            name="DD4500",
            type="DD",
            protocol="snmp",
            enabled=True,
            schedule=ScheduleConfig(5, 1, 0),
            host="192.0.2.10",
            community="public",
            oids={"state": "1.3.6.1"},
        )
    )
    scheduler = CollectorScheduler([collector], RecordingWriter())

    results = await scheduler.run_once()

    assert [result.collection_class for result in results] == ["fast"]

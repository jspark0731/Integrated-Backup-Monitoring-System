import asyncio
from datetime import datetime, timezone

import pytest

from apps.collectors.base import BaseCollector
from apps.core.config import (
    CollectionSchedulesConfig,
    CollectorConfig,
    ScheduleConfig,
)
from apps.models import CollectionResult
from apps.scheduler import CollectorScheduler, seconds_until_next_run


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
        self.closed = False

    async def write_many(self, results) -> None:
        self.results.extend(results)

    async def close(self) -> None:
        self.closed = True


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


class UnexpectedlyFailingCollector(RecordingCollector):
    def __init__(self, config: CollectorConfig, recovered: asyncio.Event) -> None:
        super().__init__(config)
        self.recovered = recovered

    async def collect(self, collection_class="fast") -> CollectionResult:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("unexpected collector failure")
        self.recovered.set()
        return CollectionResult(
            collector=self.name,
            target_type=self.target_type,
            protocol=self.protocol,
            collected_at=datetime.now(timezone.utc),
            ok=True,
            collection_class=collection_class,
        )


class FailingWriter(RecordingWriter):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0
        self.recovered = asyncio.Event()

    async def write_many(self, results) -> None:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("unexpected writer failure")
        await super().write_many(results)
        self.recovered.set()


def scheduler_test_config(name: str = "DD4500") -> CollectorConfig:
    return CollectorConfig(
        name=name,
        type="DD",
        protocol="snmp",
        enabled=True,
        schedule=ScheduleConfig(5, 1, 0),
        host="192.0.2.10",
        community="public",
        oids={"state": "1.3.6.1"},
    )


@pytest.mark.asyncio
async def test_collector_loop_runs_again_after_unexpected_exception(monkeypatch) -> None:
    monkeypatch.setattr("apps.scheduler.seconds_until_next_run", lambda *args: 0)
    recovered = asyncio.Event()
    collector = UnexpectedlyFailingCollector(scheduler_test_config(), recovered)
    writer = RecordingWriter()
    scheduler = CollectorScheduler([collector], writer)

    task = asyncio.create_task(scheduler._run_collector_forever(collector))
    await asyncio.wait_for(recovered.wait(), timeout=1)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert collector.calls >= 2
    assert len(writer.results) >= 1


@pytest.mark.asyncio
async def test_collector_loop_runs_again_after_writer_exception(monkeypatch) -> None:
    monkeypatch.setattr("apps.scheduler.seconds_until_next_run", lambda *args: 0)
    collector = RecordingCollector(scheduler_test_config())
    writer = FailingWriter()
    scheduler = CollectorScheduler([collector], writer)

    task = asyncio.create_task(scheduler._run_collector_forever(collector))
    await asyncio.wait_for(writer.recovered.wait(), timeout=1)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert writer.calls >= 2
    assert collector.calls >= 2
    assert scheduler.last_results[collector.name].ok


@pytest.mark.asyncio
async def test_one_collector_failure_does_not_stop_another(monkeypatch) -> None:
    monkeypatch.setattr("apps.scheduler.seconds_until_next_run", lambda *args: 0)
    failing_recovered = asyncio.Event()
    healthy_collected = asyncio.Event()
    failing = UnexpectedlyFailingCollector(
        scheduler_test_config("DD-failing"),
        failing_recovered,
    )

    class HealthyCollector(RecordingCollector):
        async def collect(self, collection_class="fast") -> CollectionResult:
            result = await super().collect(collection_class)
            healthy_collected.set()
            return result

    healthy = HealthyCollector(scheduler_test_config("DD-healthy"))
    scheduler = CollectorScheduler([failing, healthy], RecordingWriter())

    await scheduler.start()
    await asyncio.wait_for(healthy_collected.wait(), timeout=1)
    await asyncio.wait_for(failing_recovered.wait(), timeout=1)
    await scheduler.stop()

    assert healthy.calls >= 1
    assert failing.calls >= 2


@pytest.mark.asyncio
async def test_stop_closes_collector_resources_and_writer() -> None:
    class CloseTrackingCollector(RecordingCollector):
        def __init__(self, config: CollectorConfig) -> None:
            super().__init__(config)
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    collector = CloseTrackingCollector(scheduler_test_config())
    writer = RecordingWriter()
    scheduler = CollectorScheduler([collector], writer)

    await scheduler.stop()

    assert collector.closed
    assert writer.closed


@pytest.mark.asyncio
async def test_four_networker_collectors_run_independently_in_one_scheduler(
    tmp_path,
) -> None:
    classification = tmp_path / "hostname_domain.csv"
    classification.write_text(
        "hostname,security_domain\n",
        encoding="utf-8",
    )

    class FailingPayloadCollector(RecordingCollector):
        async def _collect_payload(self) -> dict:
            self.calls += 1
            raise RuntimeError("CORE collection failed")

    collectors = []
    for source in ("core", "chnl", "info", "ifrs"):
        config = CollectorConfig(
            name=f"networker_{source}",
            type="Networker",
            protocol="rest",
            enabled=True,
            schedule=ScheduleConfig(5, 3, 0),
            base_url=f"https://networker-{source}.example.com",
            token="secret",
            source_networker=source,
            hostname_csv_path=str(classification),
        )
        collector_type = FailingPayloadCollector if source == "core" else RecordingCollector
        collectors.append(collector_type(config))

    scheduler = CollectorScheduler(collectors, RecordingWriter())

    results = await scheduler.run_once()

    assert len(results) == 4
    assert not scheduler.last_results["networker_core"].ok
    assert all(
        scheduler.last_results[f"networker_{source}"].ok
        for source in ("chnl", "info", "ifrs")
    )

"""Aggregation compatibility, bounded execution and phase observability regressions."""
import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import logging
import threading

import pytest

from apps.core.config import ElasticsearchConfig, NetworkerCurrentConfig
from apps.processors import networker_current as processor
from apps.scheduler import CollectorScheduler
from apps.writers.elasticsearch import ElasticsearchWriter
from apps.writers import networker_current as current_writer
from test_networker_current import DAY, NOW, FakeES, hierarchy, hits, job, proof, result, rule, skip


def compatibility_cases():
    """Anonymous inputs; hashes below were captured from the pre-change implementation."""
    cases = {}

    def add(name, documents, *, config=None, days=None, now=NOW):
        cases[name] = (documents, "core", days or [DAY], now,
                       config or NetworkerCurrentConfig(enabled=True, rebuild_days=1))

    add("normal", hits(hierarchy()))
    add("skip", hits([job(message=skip())]))
    add("failed_with_skip", hits([job(completionStatus="Failed", message=skip())]))
    add("unsupported_skip_conflict", hits([job(message=skip()),
        job(7200, type="synthetic auxiliary action", parentJobId=7100, rootParentJobId=7100)]))
    add("retries_and_skip", hits([job(completionStatus="Failed"),
        job(7101, startTime="2026-11-11T13:00:00Z", endTime="2026-11-11T13:10:00Z"),
        job(7102, startTime="2026-11-11T14:00:00Z", endTime="2026-11-11T14:10:00Z", message=skip())]))
    add("running_and_manual_save", hits([job(state="SessionActive", completionStatus="", endTime=None),
        job(7990, type="save job", adhocJob=True)]))
    for missing in (None, 0, ""):
        jobs = hierarchy()
        for child in jobs[1:]:
            child["rootParentJobId"] = missing
        add("parent_only_" + repr(missing), hits(jobs))
    jobs = hierarchy() + [job(7101, workflowName="SYNTH_OTHER")]
    jobs[1]["parentJobId"] = 7101
    add("conflicting_parent_root", hits(jobs))
    jobs = hierarchy()
    jobs[2]["rootParentJobId"] = 7999
    add("conflicting_save_root", hits(jobs))
    jobs = hierarchy()
    jobs[1]["startTime"] = "2026-11-11T11:00:00Z"
    jobs[2]["endTime"] = "2026-11-11T13:00:00Z"
    add("invalid_child_times", hits(jobs))
    add("source_collision", hits(hierarchy()) + hits(hierarchy(), "chnl"))
    documents = hits(hierarchy())
    duplicates = deepcopy(documents)
    for copy in duplicates:
        copy["_index"] = "nw-unmapped-job-run-2026-11"
    add("domain_duplicates", documents + duplicates)
    conflicting = deepcopy(documents[1])
    conflicting["_index"] = "nw-unmapped-job-run-2026-11"
    conflicting["_source"]["payload"]["end_time"] = "2026-11-11T12:06:00+00:00"
    clone = NetworkerCurrentConfig(enabled=True, run_evidence={"core:7100": proof(workflow_kind="clone")})
    add("conflicting_action_copies", documents + [conflicting], config=clone)
    add("terminal_copies", hits([job(), job(completionStatus="Failed")]))
    add("long_start_rule", hits([job(startTime="2026-11-11T14:00:00Z", endTime="2026-11-12T13:00:00Z")]),
        config=NetworkerCurrentConfig(enabled=True, workflow_rules=[rule(backup_day_basis="start_window")]),
        days=[DAY, "2026-11-12"], now=datetime(2026, 11, 13, 0, 10, tzinfo=timezone.utc))
    add("no_end_failure", hits([job(completionStatus="Abandoned", endTime=None)]),
        config=NetworkerCurrentConfig(enabled=True, workflow_rules=[rule(backup_day_basis="start_window")]))
    for name, (documents, source, days, now, config) in list(cases.items()):
        cases[name + "_reversed"] = (list(reversed(documents)), source, days, now, config)
    return cases


# SHA-256 of the COMPLETE summary/detail output, including generation and reasons.
# Fixed NOW and synthetic inputs avoid runtime/repository/tool dependencies.
BASELINE_HASHES = {
    "conflicting_action_copies": "48129260f5d60bd4946b6f94dc3f34f39b9816145ef68ff10884c32654822620",
    "conflicting_action_copies_reversed": "d7164fca55d31652fc47e8cd86484d7a1ad80103f05bebceca343e38138e428e",
    "conflicting_parent_root": "69033f54b8368b9fe6bce9039e0e87d37506169ca4186b07fc29631bebd31aea",
    "conflicting_parent_root_reversed": "69033f54b8368b9fe6bce9039e0e87d37506169ca4186b07fc29631bebd31aea",
    "conflicting_save_root": "c478159a2b7042bd6be0c56fae87060c420ac01d1bc2d77a4c6248782f504f48",
    "conflicting_save_root_reversed": "c478159a2b7042bd6be0c56fae87060c420ac01d1bc2d77a4c6248782f504f48",
    "domain_duplicates": "5758059a63cb891ff9d71cb61bc71885f08d4d2b3613762719eb7475f845c86c",
    "domain_duplicates_reversed": "5758059a63cb891ff9d71cb61bc71885f08d4d2b3613762719eb7475f845c86c",
    "failed_with_skip": "fd544e70ab73d50733e025df11454f7e1bb6deec9ad514e5927ab007c47c1219",
    "failed_with_skip_reversed": "fd544e70ab73d50733e025df11454f7e1bb6deec9ad514e5927ab007c47c1219",
    "invalid_child_times": "c478159a2b7042bd6be0c56fae87060c420ac01d1bc2d77a4c6248782f504f48",
    "invalid_child_times_reversed": "c478159a2b7042bd6be0c56fae87060c420ac01d1bc2d77a4c6248782f504f48",
    "long_start_rule": "96f58e24496daf0990cceec0024ed8d3758ad6e3c93d153cb1671f6844ce6b7c",
    "long_start_rule_reversed": "96f58e24496daf0990cceec0024ed8d3758ad6e3c93d153cb1671f6844ce6b7c",
    "no_end_failure": "1d0dae793a9d0deec8ddc2a9ba2a131f37d5fbe1f7e9a1e4bf7cb17e50af378f",
    "no_end_failure_reversed": "1d0dae793a9d0deec8ddc2a9ba2a131f37d5fbe1f7e9a1e4bf7cb17e50af378f",
    "normal": "5758059a63cb891ff9d71cb61bc71885f08d4d2b3613762719eb7475f845c86c",
    "normal_reversed": "5758059a63cb891ff9d71cb61bc71885f08d4d2b3613762719eb7475f845c86c",
    "parent_only_''": "c478159a2b7042bd6be0c56fae87060c420ac01d1bc2d77a4c6248782f504f48",
    "parent_only_''_reversed": "c478159a2b7042bd6be0c56fae87060c420ac01d1bc2d77a4c6248782f504f48",
    "parent_only_0": "c478159a2b7042bd6be0c56fae87060c420ac01d1bc2d77a4c6248782f504f48",
    "parent_only_0_reversed": "c478159a2b7042bd6be0c56fae87060c420ac01d1bc2d77a4c6248782f504f48",
    "parent_only_None": "c478159a2b7042bd6be0c56fae87060c420ac01d1bc2d77a4c6248782f504f48",
    "parent_only_None_reversed": "c478159a2b7042bd6be0c56fae87060c420ac01d1bc2d77a4c6248782f504f48",
    "retries_and_skip": "38327c63c7ea14460da2587ded0e3fa0f946374aede2c19fa7e5cd989d062cd6",
    "retries_and_skip_reversed": "38327c63c7ea14460da2587ded0e3fa0f946374aede2c19fa7e5cd989d062cd6",
    "running_and_manual_save": "6a0812c8bdad5c3fc0be19e88e4867a7e0f0c1283d2e0f5917dca3794c80982a",
    "running_and_manual_save_reversed": "6a0812c8bdad5c3fc0be19e88e4867a7e0f0c1283d2e0f5917dca3794c80982a",
    "skip": "fa5598be735bdb0151302f6433ffc95cdb944162216e99420c7957541a44c0dd",
    "skip_reversed": "fa5598be735bdb0151302f6433ffc95cdb944162216e99420c7957541a44c0dd",
    "source_collision": "5758059a63cb891ff9d71cb61bc71885f08d4d2b3613762719eb7475f845c86c",
    "source_collision_reversed": "5758059a63cb891ff9d71cb61bc71885f08d4d2b3613762719eb7475f845c86c",
    "terminal_copies": "2a14bef0b0e89611039b0f959511cb3ee35522281156dfd7d5914391c449af62",
    "terminal_copies_reversed": "2a14bef0b0e89611039b0f959511cb3ee35522281156dfd7d5914391c449af62",
    "unsupported_skip_conflict": "21d41cdfc0bd4c3f0b6af547e16bfef20306c09cc9fe0625c24eedc0b650bdc9",
    "unsupported_skip_conflict_reversed": "21d41cdfc0bd4c3f0b6af547e16bfef20306c09cc9fe0625c24eedc0b650bdc9"
}


@pytest.mark.parametrize("name", sorted(compatibility_cases()))
def test_complete_output_matches_pre_optimization_baseline(name):
    output = processor.build_daily(*compatibility_cases()[name])
    # New hostname and failure display evidence intentionally participate in generation. Compare the
    # complete OLD field contract against the captured baseline, without changing
    # any outcome, reasons, selection, attribution or duration expectations.
    for summary, rows in output.values():
        for row in rows:
            for field in ("hostname", "hostnames", "hostname_status", "hostname_sources"):
                row.pop(field)
            for field in ("failure_message", "failure_message_job_id"):
                row.pop(field, None)
        semantic = [{k: v for k, v in row.items() if k not in {
            "source_observed_at", "document_kind", "generation", "updated_at", "window_start", "window_end"}}
            for row in rows]
        generation = processor.digest([semantic, {k: v for k, v in summary.items() if k not in {"generation", "updated_at"}}])[:24]
        summary["generation"] = generation
        for row in rows:
            row["generation"] = generation
    actual = hashlib.sha256(json.dumps(output, sort_keys=True).encode()).hexdigest()
    assert actual == BASELINE_HASHES[name]


def test_parent_only_and_conflicting_candidates_are_retained_in_original_order(monkeypatch):
    jobs = hierarchy() + [job(7101, workflowName="SYNTH_OTHER")]
    jobs[1]["rootParentJobId"] = None
    jobs[2]["rootParentJobId"] = 7101
    documents = hits(jobs) + hits(hierarchy(), "chnl")
    duplicate = deepcopy(documents[1])
    duplicate["_index"] = "nw-unmapped-job-run-2026-11"
    documents.insert(0, duplicate)
    original, seen = processor.classify, {}

    def capture(workflow, evidence, children, source):
        seen[workflow["job_id"]] = children
        return original(workflow, evidence, children, source)

    monkeypatch.setattr(processor, "classify", capture)
    processor.build_daily(documents, "core", [DAY], NOW, NetworkerCurrentConfig())
    assert [c["job_id"] for c in seen["7100"]] == ["7200", "7200", "7300"]
    assert all(c["source_networker"] == "core" for children in seen.values() for c in children)


def test_timestamp_work_is_bounded_by_connected_input_not_workflow_product(monkeypatch):
    documents = []
    for i in range(30):
        documents += hits([job(10000 + i, workflowName="SYNTH_" + str(i)),
            job(20000 + i, type="backup action job", parentJobId=10000 + i, rootParentJobId=10000 + i)])
        documents += hits([job(30000 + i * 30 + j, type="save job", parentJobId=20000 + i,
            rootParentJobId=10000 + i) for j in range(30)])
    calls, original = 0, processor.timestamp

    def counted(value):
        nonlocal calls
        calls += 1
        return original(value)

    monkeypatch.setattr(processor, "timestamp", counted)
    output = processor.build_daily(documents, "core", [DAY], NOW, NetworkerCurrentConfig())
    assert output[DAY][0]["success_count"] == 30
    assert calls < 20 * len(documents)  # Structural bound, not a machine timing threshold.


async def wait_until(predicate):
    # A deadlock guard, not a performance acceptance threshold.
    async with asyncio.timeout(10):
        while not predicate():
            await asyncio.sleep(0.001)


def configured_writer(es, monkeypatch):
    from apps.writers import elasticsearch as es_writer
    monkeypatch.setattr(es_writer, "datetime", type("Clock", (), {"now": staticmethod(lambda zone: NOW)}))
    writer = ElasticsearchWriter(ElasticsearchConfig(networker_current=NetworkerCurrentConfig(enabled=True, rebuild_days=1)))
    writer.client, writer._async_bulk = es, es.bulk
    return writer


@pytest.mark.asyncio
async def test_computation_is_off_loop_and_io_and_publication_order_stay_on_loop(monkeypatch):
    es, main_thread = FakeES(hits(hierarchy())), threading.get_ident()
    for name in ("get", "mget", "field_caps", "open_point_in_time", "close_point_in_time", "search", "bulk"):
        original = getattr(es, name)

        async def checked(*args, method=original, **kwargs):
            assert threading.get_ident() == main_thread
            return await method(*args, **kwargs)

        monkeypatch.setattr(es, name, checked)
    writer = configured_writer(es, monkeypatch)
    entered, release = threading.Event(), threading.Event()
    original_build = current_writer.build_daily

    def controlled(*args, **kwargs):
        assert threading.get_ident() != main_thread
        entered.set()
        assert release.wait(10)
        return original_build(*args, **kwargs)

    monkeypatch.setattr(current_writer, "build_daily", controlled)
    task = asyncio.create_task(writer._write_networker_current([result(hierarchy())]))
    try:
        await wait_until(entered.is_set)
        # This coroutine runs while computation is blocked in its worker thread.
        assert not task.done() and es.bulks == []
    finally:
        release.set()
        await task
    kinds = [a["_source"]["document_kind"] for batch in es.bulks for a in batch]
    daily = [a for batch in es.bulks for a in batch if a["_source"]["document_kind"] in {"workflow_result", "daily_summary"}]
    assert [a["_source"]["document_kind"] for a in daily] == ["workflow_result", "daily_summary", "daily_summary"]
    assert [a["_source"].get("summary_role") for a in daily] == [None, "generation", "head"]
    assert kinds[-1] == "daily_policy_host"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["cancel", "timeout", "worker_error_after_cancel"])
async def test_cancelled_computation_drains_under_source_lock_without_late_publication(monkeypatch, caplog, mode):
    es = FakeES(hits(hierarchy()))
    writer = configured_writer(es, monkeypatch)
    entered = [threading.Event(), threading.Event()]
    release = [threading.Event(), threading.Event()]
    original_build, calls, active, maximum = current_writer.build_daily, 0, 0, 0
    guard = threading.Lock()

    def controlled(*args, **kwargs):
        nonlocal calls, active, maximum
        with guard:
            ordinal = calls
            calls += 1
            active += 1
            maximum = max(maximum, active)
        try:
            entered[ordinal].set()
            assert release[ordinal].wait(10)
            if ordinal == 0 and mode == "worker_error_after_cancel":
                raise ValueError("discarded synthetic failure")
            return original_build(*args, **kwargs)
        finally:
            with guard:
                active -= 1

    monkeypatch.setattr(current_writer, "build_daily", controlled)
    timeout_scope = []

    async def first_call():
        async with asyncio.timeout(None) as scope:
            timeout_scope.append(scope)
            await writer._write_networker_current([result(hierarchy())])

    first = asyncio.create_task(first_call())
    second = None
    try:
        await wait_until(entered[0].is_set)
        if mode == "timeout":
            timeout_scope[0].reschedule(asyncio.get_running_loop().time())
        else:
            first.cancel()
        await asyncio.sleep(0)
        if mode != "timeout":
            first.cancel()  # Repeated cancellation cannot release the source early.
        second = asyncio.create_task(writer._write_networker_current([result(hierarchy())]))
        await asyncio.sleep(0)
        assert writer._current_locks["core"].locked()
        assert not first.done() and not entered[1].is_set()
        release[0].set()
        with pytest.raises(TimeoutError if mode == "timeout" else asyncio.CancelledError):
            await first
        await wait_until(entered[1].is_set)
        assert es.bulks == []  # Cancelled run did not publish, including failure metadata.
        release[1].set()
        await second
        assert maximum == 1 and calls == 2
        assert es.store[("nw-current-core", "daily:core:" + DAY)]["success_count"] == 1
        assert "stage=aggregate status=cancelled" in caplog.text
        assert "stage=total status=cancelled last_stage=aggregate" in caplog.text
    finally:
        for event in release:
            event.set()
        await asyncio.gather(*(t for t in (first, second) if t is not None), return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("first_failure", [False, True])
async def test_completed_or_failed_calls_for_one_source_compute_serially(monkeypatch, first_failure):
    es = FakeES(hits(hierarchy()))
    writer = configured_writer(es, monkeypatch)
    entered, release = threading.Event(), threading.Event()
    original_build, calls, active, maximum = current_writer.build_daily, 0, 0, 0

    def controlled(*args, **kwargs):
        nonlocal calls, active, maximum
        calls += 1
        ordinal = calls
        active += 1
        maximum = max(maximum, active)
        entered.set()
        try:
            assert release.wait(10)
            if first_failure and ordinal == 1:
                raise ValueError("synthetic computation failure")
            return original_build(*args, **kwargs)
        finally:
            active -= 1

    monkeypatch.setattr(current_writer, "build_daily", controlled)
    first = asyncio.create_task(writer._write_networker_current([result(hierarchy())]))
    second = None
    try:
        await wait_until(entered.is_set)
        second = asyncio.create_task(writer._write_networker_current([result(hierarchy())]))
        await asyncio.sleep(0)
        assert calls == 1
    finally:
        release.set()
        await asyncio.gather(*(t for t in (first, second) if t is not None))
    assert calls == 2 and maximum == 1


@pytest.mark.asyncio
async def test_scheduler_shutdown_waits_for_actual_computation_and_discards_it(monkeypatch):
    es = FakeES(hits(hierarchy()))
    writer = configured_writer(es, monkeypatch)
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    original_build = current_writer.build_daily
    closed = []

    async def close():
        assert finished.is_set()
        closed.append(True)

    monkeypatch.setattr(es, "close", close, raising=False)

    def controlled(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(10)
            return original_build(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(current_writer, "build_daily", controlled)
    task = asyncio.create_task(writer._write_networker_current([result(hierarchy())]))
    scheduler = CollectorScheduler([], writer)
    scheduler._tasks = [task]
    stopping = None
    try:
        await wait_until(entered.is_set)
        stopping = asyncio.create_task(scheduler.stop())
        await asyncio.sleep(0)
        assert not stopping.done() and not closed and writer._current_locks["core"].locked()
    finally:
        release.set()
        if stopping is not None:
            await stopping
        else:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert closed == [True] and es.bulks == [] and not writer._current_locks["core"].locked()


@pytest.mark.asyncio
async def test_worker_failure_is_reported_and_next_call_can_publish(monkeypatch, caplog):
    es = FakeES(hits(hierarchy()))
    publisher = current_writer.NetworkerDailyCurrent(es, es.bulk, NetworkerCurrentConfig(enabled=True, rebuild_days=1))
    original_build = current_writer.build_daily

    def failing(*args, **kwargs):
        raise ValueError("SYNTHETIC_PRIVATE_EXCEPTION_CONTENT")

    with caplog.at_level(logging.INFO, logger=current_writer.__name__):
        monkeypatch.setattr(current_writer, "build_daily", failing)
        with pytest.raises(ValueError):
            await publisher.publish("core", [], NOW, True)
        assert "stage=aggregate status=failed" in caplog.text
        assert "stage=total status=failed last_stage=aggregate" in caplog.text
        assert "SYNTHETIC_PRIVATE_EXCEPTION_CONTENT" not in caplog.text
        monkeypatch.setattr(current_writer, "build_daily", original_build)
        await publisher.publish("core", [], NOW, True)
    assert es.store[("nw-current-core", "daily:core:" + DAY)]["aggregation_status"] == "ok"


@pytest.mark.asyncio
async def test_phase_logs_count_existing_reads_without_sensitive_document_contents(caplog):
    source_writer = ElasticsearchWriter(ElasticsearchConfig())
    actions = source_writer._actions_for_result(result(hierarchy()))
    initial = [{k: a[k] for k in ("_index", "_id", "_source")} for a in actions
               if a["_source"].get("document_type") == "job-run" or a["_source"].get("document_family") == "raw"]
    for doc in initial:
        if doc["_source"].get("document_type") == "job-run":
            doc["_source"]["payload"].pop("message", None)
        else:
            doc["_source"]["payload"]["raw"]["jobs"]["jobs"][0]["message"] = "SYNTHETIC_PRIVATE_MESSAGE"
    es = FakeES(initial)
    with caplog.at_level(logging.INFO, logger=current_writer.__name__):
        await current_writer.NetworkerDailyCurrent(es, es.bulk, NetworkerCurrentConfig(enabled=True, rebuild_days=1)).publish("core", [], NOW, True)
    messages = [record.getMessage() for record in caplog.records if record.name == current_writer.__name__]
    for stage in ("job_read", "raw_enrichment", "aggregate", "publish", "total"):
        assert sum("stage=" + stage + " status=completed" in line for line in messages) == 1
    for expected in ("'workflow_hits': 1", "'child_hits': 2", "'page_responses': 4",
                     "'requested_raw_documents': 1", "'returned_raw_documents': 1", "'processed_jobs': 3",
                     "'input_hits': 3", "'workflow_executions': 1", "'published_days': 1"):
        assert expected in caplog.text
    assert "SYNTHETIC_PRIVATE_MESSAGE" not in caplog.text

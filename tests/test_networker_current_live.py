"""Anonymous fixtures only: active API observations and current-only dashboard views."""
from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import httpx
import pytest
from elasticsearch import ConflictError

from apps.clients.networker_rest_client import ACTIVE_JOB_QUERY, NetworkerRestClient, WORKFLOW_CONFIRM_LIMIT
from apps.clients.rest_client import ERRORS_KEY
from apps.collectors.networker_rest_collector import NetworkerRestCollector
from apps.core.config import CollectorConfig, ElasticsearchConfig, NetworkerCurrentConfig
from apps.processors.networker_current import build_daily, daily_failure_view, policy_host_key, window
from apps.writers.elasticsearch import ElasticsearchWriter
from apps.writers.networker_current import NetworkerDailyCurrent
from test_networker_current import DAY, END, NOW, START, FakeES, config, hierarchy, hits, job, result, rule, skip


def client_config(**changes):
    return CollectorConfig(name="networker_core", type="Networker", protocol="rest", enabled=True,
                           base_url="https://synthetic.example:9090", username="synthetic", password="synthetic",
                           source_networker="core", retries=0, **changes)


def client(handler):
    instance = NetworkerRestClient(client_config())
    instance._rest._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return instance


def active(identity=7100, **changes):
    return job(identity, state="SessionActive", completionStatus="", endTime=None, exitCode=None,
               clientHostname="synthetic-client", **changes)


def observation(*identities, success=True):
    return {"active_success": success, "observed_workflow_ids": [str(i) for i in identities]}


def rows_for(jobs, days=(DAY,), source="core"):
    return build_daily(hits(jobs, source), source, list(days), NOW, NetworkerCurrentConfig())


def test_policy_not_job_type_defines_separate_policy_host_keys():
    jobs = [job(7100 + n, name=policy, clientHostname="SYNTHETIC-CLIENT.", completionStatus="Failed")
            for n, policy in enumerate(("Database", "Filesystem", "NAS"))]
    summary, rows = rows_for(jobs)[DAY]
    view = daily_failure_view(summary, rows)
    assert view["failed_policy_host_keys"] == [
        "core:Database:synthetic-client", "core:Filesystem:synthetic-client", "core:NAS:synthetic-client"]
    assert all(row["hostname"] == "synthetic-client" for row in rows)
    assert policy_host_key("core", "A-B", "C") != policy_host_key("core", "A", "B-C")
    assert policy_host_key("core", "A", "C") != policy_host_key("info", "A", "C")


def test_hostname_uses_validated_save_client_fields_not_server_or_workflow_name():
    jobs = hierarchy()
    jobs[0].update(runOnHost="synthetic-backup-server", workflowName="client_W")
    jobs[-1].update(clientHostname="synthetic-client")
    _, rows = rows_for(jobs)[DAY]
    assert rows[0]["hostnames"] == ["synthetic-client"]
    assert rows[0]["hostname_sources"] == ["clientHostname"]
    jobs[-1].pop("clientHostname")
    jobs[-1]["runOnHost"] = "synthetic-target"
    summary, rows = rows_for(jobs)[DAY]
    assert rows[0]["hostnames"] == ["synthetic-target"]
    assert rows[0]["hostname_sources"] == ["save_job.runOnHost"]
    assert summary["success_count"] == 1  # Host identification does not change Workflow outcome.
    jobs[-1].update(clientName="synthetic-client", rootParentJobId=9999)
    assert rows_for(jobs)[DAY][1][0]["hostnames"] == []


def test_multiple_clients_do_not_inflate_workflow_results_or_duplicate_failure_keys():
    jobs = hierarchy()
    jobs[0]["completionStatus"] = "Failed"
    jobs[-1]["clientName"] = "synthetic-one"
    jobs.append(job(7301, type="save job", parentJobId=7200, rootParentJobId=7100,
                    clientHostname="synthetic-two", startTime="2026-11-11T12:05:00Z"))
    summary, rows = rows_for(jobs)[DAY]
    assert summary["failure_count"] == 1
    assert rows[0]["hostname"] is None and rows[0]["hostname_status"] == "multiple"
    assert len(daily_failure_view(summary, rows)["failed_policy_host_keys"]) == 2


def test_duplicate_sources_and_conflicting_hostname_copies_are_not_guessed():
    jobs = hierarchy()
    jobs[-1]["clientName"] = "synthetic-one"
    documents = hits(jobs)
    duplicate = deepcopy(documents[-1])
    duplicate["_index"] = "nw-unmapped-job-run-2026-11"
    duplicate["_source"]["payload"]["client_name"] = "synthetic-two"
    output = build_daily(documents + [duplicate] + hits(jobs, "info"), "core", [DAY], NOW, config())
    assert output[DAY][1][0]["hostnames"] == []
    assert output[DAY][0]["success_count"] == 1


def test_rerun_success_removes_failure_day_but_other_failed_workflow_keeps_one_day():
    base = [job(clientHostname="synthetic-client", completionStatus="Failed"),
            job(7101, clientHostname="synthetic-client", startTime="2026-11-11T13:00:00Z",
                endTime="2026-11-11T13:10:00Z")]
    s, r = rows_for(base)[DAY]
    assert daily_failure_view(s, r)["failed_policy_host_keys"] == []
    base.extend([job(7102, clientHostname="synthetic-client", workflowName="OTHER", completionStatus="Failed"),
                 job(7103, clientHostname="synthetic-client", workflowName="THIRD", completionStatus="Failed")])
    s, r = rows_for(base)[DAY]
    assert s["failure_count"] == 2
    assert daily_failure_view(s, r)["failed_policy_host_keys"] == ["core:SYNTH_POLICY:synthetic-client"]


def test_distinct_failure_days_month_boundary_and_kst_cutoff():
    days = ["2026-10-31", "2026-11-01"]
    jobs = [job(7100 + n, clientHostname="synthetic-client", completionStatus="Failed",
                startTime=f"{day}T12:00:00Z", endTime=f"{day}T12:10:00Z") for n, day in enumerate(days)]
    output = rows_for(jobs, days)
    counts = Counter(k for s, r in output.values() for k in daily_failure_view(s, r)["failed_policy_host_keys"])
    assert counts == {"core:SYNTH_POLICY:synthetic-client": 2}
    assert window("2026-11-01")[0].isoformat() == "2026-10-31T23:00:00+00:00"


def stored_save_hierarchy(*, legacy=True, failed=False, running=False, include_save=True):
    """Anonymous equivalent of the confirmed legacy Save/Action/Workflow shape."""
    workflow = job(name="Filesystem", workflowName="synthetic_target_W", runOnHost="synthetic-backup-server",
                   completionStatus="Failed" if failed else "Succeeded", exitCode=1 if failed else 0)
    action = job(7200, type="backup action job", name="savegrp", parentJobId=7100, rootParentJobId=7100,
                 runOnHost="synthetic-backup-server", startTime="2026-11-11T12:01:00Z",
                 endTime="2026-11-11T12:19:00Z")
    save = job(7300, type="save job", name="/synthetic", parentJobId=7200, rootParentJobId=7100,
               runOnHost="synthetic-target", startTime="2026-11-11T12:05:00Z",
               endTime="2026-11-11T12:10:00Z")
    if running:
        for item in (workflow, action):
            item.update(state="SessionActive", completionStatus="", endTime=None, exitCode=None)
    documents = hits([workflow, action] + ([save] if include_save else []))
    if legacy:
        for h in documents:
            h["_source"]["payload"].pop("hostname_source", None)
    return documents


@pytest.mark.parametrize("legacy", [True, False])
def test_confirmed_save_fallback_is_available_without_raw_message_parsing(legacy):
    documents = stored_save_hierarchy(legacy=legacy)
    source = documents[-1]["_source"]
    assert source["payload"]["client_name"] == source["payload"]["client_hostname"] == source["client"]["hostname"] == "synthetic-target"
    if not legacy:
        assert source["payload"]["hostname_source"] == "save_job.runOnHost"
    summary, rows = build_daily(documents, "core", [DAY], NOW, config())[DAY]
    row = rows[0]
    assert row["policy_name"] == "Filesystem" and row["workflow_name"] == "synthetic_target_W"
    assert row["hostname"] == "synthetic-target" and row["hostname_status"] == "identified"
    assert row["hostname_sources"] == ["save_job.stored_client" if legacy else "save_job.runOnHost"]
    assert row["dashboard_result"] == "success" and summary["success_count"] == 1
    assert "synthetic-backup-server" not in row["hostnames"]


@pytest.mark.parametrize("field", ["client_name", "client_hostname", "document_client"])
def test_legacy_save_client_storage_aliases_are_supported_but_not_a_bare_execution_host(field):
    documents = stored_save_hierarchy()
    source = documents[-1]["_source"]
    source["payload"].update(client_name="", client_hostname="")
    source["client"]["hostname"] = ""
    if field == "document_client":
        source["client"]["hostname"] = "synthetic-target"
    else:
        source["payload"][field] = "synthetic-target"
    row = build_daily(documents, "core", [DAY], NOW, config())[DAY][1][0]
    assert row["hostname"] == "synthetic-target"
    source["payload"].update(client_name="", client_hostname="")
    source["client"]["hostname"] = ""
    row = build_daily(documents, "core", [DAY], NOW, config())[DAY][1][0]
    assert row["hostname"] is None and row["hostname_status"] == "unidentified"


@pytest.mark.parametrize("fault", ["parent", "root", "action_parent", "action_root", "source", "before_action", "after_action"])
def test_legacy_save_hostname_does_not_bypass_relationship_or_time_checks(fault):
    documents = stored_save_hierarchy()
    action, save = documents[1]["_source"]["payload"], documents[2]["_source"]["payload"]
    if fault in {"parent", "root"}:
        save["parent_job_id" if fault == "parent" else "root_parent_job_id"] = 9999
    elif fault in {"action_parent", "action_root"}:
        action["parent_job_id" if fault == "action_parent" else "root_parent_job_id"] = 9999
    elif fault == "source":
        documents[2]["_source"]["source_networker"] = "info"
        save["source_networker"] = "info"
    elif fault == "before_action":
        save["start_time"] = "2026-11-11T12:00:30Z"
    else:
        save["end_time"] = "2026-11-11T12:19:30Z"
    summary, rows = build_daily(documents, "core", [DAY], NOW, config())[DAY]
    assert rows[0]["hostname"] is None and rows[0]["hostname_status"] == "unidentified"
    assert summary["success_count"] == 1  # Only hostname evidence was excluded.


@pytest.mark.parametrize("legacy", [True, False])
@pytest.mark.parametrize("conflict", ["client_name", "client_hostname", "document_client", "run_on_host", "duplicate"])
def test_legacy_and_new_save_conflicts_never_choose_a_target(legacy, conflict):
    documents = stored_save_hierarchy(legacy=legacy)
    source = documents[-1]["_source"]
    if conflict == "duplicate":
        copy = deepcopy(documents[-1])
        copy["_index"] = "nw-unmapped-job-run-2026-11"
        copy["_source"]["payload"].update(client_name="synthetic-other", client_hostname="synthetic-other", run_on_host="synthetic-other")
        copy["_source"]["client"]["hostname"] = "synthetic-other"
        documents.append(copy)
    elif conflict == "document_client":
        source["client"]["hostname"] = "synthetic-other"
    else:
        source["payload"][conflict] = "synthetic-other"
    for ordered in (documents, list(reversed(documents))):
        summary, rows = build_daily(ordered, "core", [DAY], NOW, config())[DAY]
        assert rows[0]["hostnames"] == [] and rows[0]["hostname_status"] == "unidentified"
        assert summary["success_count"] == 1


def test_distinct_legacy_save_ids_can_represent_multiple_confirmed_targets():
    documents = stored_save_hierarchy(failed=True)
    extra = deepcopy(documents[-1])
    extra["_id"] = "synthetic-extra-save"
    extra["_source"]["payload"].update(job_id="7301", client_name="synthetic-other", client_hostname="synthetic-other", run_on_host="synthetic-other")
    extra["_source"]["client"]["hostname"] = "synthetic-other"
    summary, rows = build_daily(documents + [extra], "core", [DAY], NOW, config())[DAY]
    assert rows[0]["hostname_status"] == "multiple" and rows[0]["hostname"] is None
    assert rows[0]["hostnames"] == ["synthetic-other", "synthetic-target"]
    assert summary["failure_count"] == 1
    assert daily_failure_view(summary, rows)["failed_policy_host_keys"] == [
        "core:Filesystem:synthetic-other", "core:Filesystem:synthetic-target"]


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [True, False])
async def test_stored_save_target_reaches_actual_writer_and_failure_projection_then_rerun_success(monkeypatch, legacy):
    from apps.writers import elasticsearch as module
    monkeypatch.setattr(module, "datetime", type("Clock", (), {"now": staticmethod(lambda zone: NOW)}))
    original = stored_save_hierarchy(legacy=legacy, failed=True)
    es = FakeES(original)
    writer = ElasticsearchWriter(ElasticsearchConfig(networker_current=config()))
    writer.client, writer._async_bulk = es, es.bulk
    # Empty incoming jobs: use the stored legacy documents, not newly parsed replacements.
    await writer.write_many([result([])])
    head = es.store[("nw-current-core", "daily:core:" + DAY)]
    row = es.store[("nw-current-core", head["detail_ids"][0])]
    assert row["policy_name"] == "Filesystem" and row["hostname"] == "synthetic-target"
    assert row["dashboard_result"] == "failed" and head["failure_count"] == 1
    projection_key = ("nw-current-core", "policy-host:core:" + DAY)
    assert es.store[projection_key]["failed_policy_host_keys"] == ["core:Filesystem:synthetic-target"]
    for hit in original:
        assert es.store[(hit["_index"], hit["_id"])] == hit["_source"]
    retry = [job(7101, name="Filesystem", workflowName="synthetic_target_W", runOnHost="synthetic-backup-server",
                 startTime="2026-11-11T13:00:00Z", endTime="2026-11-11T13:20:00Z"),
             job(7201, type="backup action job", parentJobId=7101, rootParentJobId=7101, runOnHost="synthetic-backup-server",
                 startTime="2026-11-11T13:01:00Z", endTime="2026-11-11T13:19:00Z"),
             job(7301, type="save job", parentJobId=7201, rootParentJobId=7101, runOnHost="synthetic-target",
                 startTime="2026-11-11T13:05:00Z", endTime="2026-11-11T13:10:00Z")]
    await writer.write_many([result(retry)])
    head = es.store[("nw-current-core", "daily:core:" + DAY)]
    row = es.store[("nw-current-core", head["detail_ids"][0])]
    assert row["workflow_job_id"] == "7101" and row["hostname"] == "synthetic-target"
    assert head["success_count"] == 1 and head["failure_count"] == 0
    assert es.store[projection_key]["projection_status"] == "ready"
    assert es.store[projection_key]["failed_policy_host_keys"] == []


@pytest.mark.asyncio
async def test_running_without_save_is_visible_then_target_is_enriched_on_same_id():
    documents = stored_save_hierarchy(running=True, include_save=False)
    es = FakeES(documents)
    publisher = NetworkerDailyCurrent(es, es.bulk, config())
    await publisher.publish("core", documents, NOW, True, observation=observation(7100))
    key = ("nw-current-core", "running:core:7100")
    before = deepcopy(es.store[key])
    assert before["execution_status"] == "running" and before["hostname"] is None
    assert before["policy_name"] == "Filesystem" and before["workflow_name"] == "synthetic_target_W"
    observed = NOW + timedelta(minutes=5)
    incoming = stored_save_hierarchy(running=True)
    for hit in incoming:
        hit["_source"]["observed_at"] = observed.isoformat()
        es.store[(hit["_index"], hit["_id"])] = deepcopy(hit["_source"])
    await publisher.publish("core", incoming, observed, True, observation=observation(7100))
    assert es.store[key]["hostname"] == "synthetic-target" and es.store[key]["execution_status"] == "running"
    assert es.store[("nw-current-core", "running:core")]["running_count"] == 1


@pytest.mark.asyncio
async def test_active_query_has_no_time_window_and_terminal_wins_race():
    requests = []
    async def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"jobs": [active()] if request.url.params["q"] == ACTIVE_JOB_QUERY else [job()]})
    instance = client(handler)
    try:
        raw = await instance.fetch_payloads("jobs", now=NOW)
    finally:
        await instance.close()
    assert [r.url.params["q"] for r in requests] == [
        'endTime:["2026-11-12T00:00:00+00:00" TO "2026-11-12T00:10:00+00:00"]', ACTIVE_JOB_QUERY]
    assert raw["jobs"]["jobs"][0]["completionStatus"] == "Succeeded"
    assert raw["_jobs_collection"]["active_success"] is True
    assert instance._unfinished_workflows == {}


@pytest.mark.asyncio
async def test_active_disappearance_reconfirms_individual_id_outside_completion_window():
    calls, round_number = [], 0
    async def handler(request):
        calls.append(str(request.url))
        if request.url.path.endswith("/7100"):
            return httpx.Response(200, json=job())
        return httpx.Response(200, json={"jobs": [active()] if round_number == 0 and request.url.params["q"] == ACTIVE_JOB_QUERY else []})
    instance = client(handler)
    try:
        await instance.fetch_payloads("jobs", now=NOW)
        round_number = 1
        raw = await instance.fetch_payloads("jobs", now=NOW + timedelta(days=1))
    finally:
        await instance.close()
    assert calls[-1].endswith("/jobs/7100")
    assert raw["jobs"]["jobs"][0]["completionStatus"] == "Succeeded"
    assert raw["_jobs_collection"]["confirmation_checked"] == 1


@pytest.mark.asyncio
async def test_failed_confirmation_and_active_failure_are_not_successful_zero():
    async def handler(request):
        if request.url.path.endswith("/7100"):
            return httpx.Response(404, json={})
        if request.url.params["q"] == ACTIVE_JOB_QUERY:
            return httpx.Response(503, json={})
        return httpx.Response(200, json={"jobs": []})
    instance = client(handler)
    instance.remember_workflows({"7100": START})
    try:
        raw = await instance.fetch_payloads("jobs", now=NOW)
    finally:
        await instance.close()
    assert raw["_jobs_collection"]["active_count"] is None
    assert raw["_jobs_collection"]["unconfirmed_ids"] == ["7100"]
    assert set(raw[ERRORS_KEY]) == {"jobs_active", "jobs_confirm:7100"}
    assert "7100" in instance._unfinished_workflows


@pytest.mark.asyncio
async def test_completion_checks_are_bounded_and_rotate_without_confirming_saves():
    requests = []
    async def handler(request):
        requests.append(request.url.path)
        return httpx.Response(200, json={"jobs": []}) if request.url.path.endswith("/jobs") else httpx.Response(404, json={})
    instance = client(handler)
    instance.remember_workflows({str(8000 + i): START for i in range(WORKFLOW_CONFIRM_LIMIT + 2)})
    try:
        first = await instance.fetch_payloads("jobs", now=NOW)
        requests.clear()
        await instance.fetch_payloads("jobs", now=NOW)
    finally:
        await instance.close()
    assert first["_jobs_collection"]["confirmation_pending"] == 2
    assert len(requests) == WORKFLOW_CONFIRM_LIMIT + 2
    assert requests[2].endswith("/8020")


@pytest.mark.asyncio
async def test_confirmation_soft_budget_defers_remaining_ids(monkeypatch):
    monkeypatch.setattr("apps.clients.networker_rest_client.WORKFLOW_CONFIRM_SECONDS", 0)
    async def handler(request):
        return httpx.Response(200, json={"jobs": []}) if request.url.path.endswith("/jobs") else httpx.Response(404, json={})
    instance = client(handler)
    instance.remember_workflows({"7100": START, "7101": START})
    try:
        raw = await instance.fetch_payloads("jobs", now=NOW)
    finally:
        await instance.close()
    assert raw["_jobs_collection"]["confirmation_pending"] == 1
    assert set(raw["_jobs_collection"]["unconfirmed_ids"]) == {"7100", "7101"}


@pytest.mark.asyncio
async def test_manual_save_is_collected_but_never_tracked_as_running_workflow():
    requests = []
    async def handler(request):
        requests.append(request.url.path)
        return httpx.Response(200, json={"jobs": [active(type="save job", parentJobId=0, rootParentJobId=0, adhocJob=True)]
                                       if request.url.params["q"] == ACTIVE_JOB_QUERY else []})
    instance = client(handler)
    try:
        raw = await instance.fetch_payloads("jobs", now=NOW)
        await instance.fetch_payloads("jobs", now=NOW)
    finally:
        await instance.close()
    assert raw["_jobs_collection"]["observed_workflow_ids"] == []
    assert instance._unfinished_workflows == {}
    assert len(requests) == 4 and all(path.endswith("/jobs") for path in requests)


@pytest.mark.asyncio
async def test_confirmation_compares_start_instants_not_offset_string_spelling():
    async def handler(request):
        if request.url.path.endswith("/7100"):
            return httpx.Response(200, json=job(startTime="2026-11-11T21:00:00+09:00"))
        return httpx.Response(200, json={"jobs": []})
    instance = client(handler)
    instance.remember_workflows({"7100": START})
    try:
        raw = await instance.fetch_payloads("jobs", now=NOW)
    finally:
        await instance.close()
    assert raw["_jobs_collection"]["confirmation_checked"] == 1
    assert ERRORS_KEY not in raw


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["truncated", "cycle", "cross_origin", "changed_filter", "malformed"])
async def test_partial_pagination_is_not_a_complete_active_list(fault):
    async def handler(request):
        if request.url.params["q"] != ACTIVE_JOB_QUERY:
            return httpx.Response(200, json={"jobs": []})
        payload = {"jobs": [active()], "count": 2}
        if fault == "cycle":
            payload["links"] = [{"rel": "next", "href": str(request.url)}]
        elif fault == "cross_origin":
            payload["links"] = [{"rel": "next", "href": "https://other.invalid/jobs"}]
        elif fault == "changed_filter":
            payload["links"] = [{"rel": "next", "href": "/nwrestapi/v3/global/jobs?q=type:save"}]
        elif fault == "malformed":
            payload = {"jobs": None}
        return httpx.Response(200, json=payload)
    instance = client(handler)
    try:
        raw = await instance.fetch_payloads("jobs", now=NOW)
    finally:
        await instance.close()
    assert not raw["_jobs_collection"]["active_success"]
    assert raw["_jobs_collection"]["active_count"] is None


@pytest.mark.asyncio
async def test_follow_all_server_next_links_preserving_query_and_count():
    async def handler(request):
        if request.url.params["q"] != ACTIVE_JOB_QUERY:
            return httpx.Response(200, json={"jobs": []})
        if "cursor" in request.url.params:
            return httpx.Response(200, json={"jobs": [active(7101)], "count": 2})
        next_url = request.url.copy_add_param("cursor", "synthetic")
        return httpx.Response(200, json={"jobs": [active()], "count": 2,
                                       "links": [{"rel": "next", "href": str(next_url)}]})
    instance = client(handler)
    try:
        raw = await instance.fetch_payloads("jobs", now=NOW)
    finally:
        await instance.close()
    assert raw["_jobs_collection"]["active_count"] == 2
    assert raw["_jobs_collection"]["active_pages"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["Succeeded", "Failed"])
async def test_running_fixed_ids_terminal_transition_stale_regression_and_restart(outcome):
    es = FakeES()
    publisher = NetworkerDailyCurrent(es, es.bulk, config())
    running = hits([active()])
    await publisher.publish("core", running, NOW, True, observation=observation(7100))
    await publisher.publish("core", running, NOW, True, observation=observation(7100))
    key = ("nw-current-core", "running:core:7100")
    assert es.store[key]["execution_status"] == "running"
    assert es.store[("nw-current-core", "running:core")]["running_count"] == 1
    collector = NetworkerRestCollector(client_config())
    writer = ElasticsearchWriter(ElasticsearchConfig(networker_current=config()))
    writer.client = es
    await writer.restore_networker_workflows(collector, "jobs")
    assert collector.client._unfinished_workflows == {"7100": START}
    terminal = hits([job(clientHostname="synthetic-client", completionStatus=outcome)])
    await publisher.publish("core", terminal, NOW + timedelta(minutes=5), True, observation=observation(7100))
    assert es.store[key]["execution_status"] == "completed" and not es.store[key]["is_running"]
    await publisher.publish_running("core", running, running, NOW + timedelta(minutes=10), observation(7100))
    assert es.store[key]["execution_status"] == "completed"
    assert es.store[("nw-current-core", "running:core")]["running_count"] == 0
    assert es.store[("nw-current-core", "running:core")]["unfinished_workflows"] == {}
    await collector.close()


@pytest.mark.asyncio
async def test_same_host_concurrent_workflow_ids_and_sources_remain_separate():
    es = FakeES()
    publisher = NetworkerDailyCurrent(es, es.bulk, config())
    running = hits([active(), active(7101)])
    await publisher.publish("core", running, NOW, True, observation=observation(7100, 7101))
    await publisher.publish("info", hits([active()], "info"), NOW, True, observation=observation(7100))
    assert es.store[("nw-current-core", "running:core")]["running_count"] == 2
    assert es.store[("nw-current-info", "running:info")]["running_count"] == 1
    assert es.store[("nw-current-core", "running:core:7100")]["hostname"] == "synthetic-client"


@pytest.mark.asyncio
async def test_future_running_start_is_unconfirmed_not_an_active_fact():
    es = FakeES()
    publisher = NetworkerDailyCurrent(es, es.bulk, config())
    incoming = hits([active(startTime="2026-12-01T00:00:00Z")])
    await publisher.publish("core", incoming, NOW, True, observation=observation(7100))
    assert es.store[("nw-current-core", "running:core:7100")]["execution_status"] == "unconfirmed"
    assert es.store[("nw-current-core", "running:core")]["data_status"] == "partial"


@pytest.mark.asyncio
async def test_whole_collection_exception_still_marks_current_unavailable(monkeypatch):
    collector = NetworkerRestCollector(client_config())
    async def failed(*args):
        raise RuntimeError("synthetic transport failure")
    monkeypatch.setattr(collector.client, "fetch_payloads", failed)
    failed_result = await collector.collect("jobs")
    assert not failed_result.ok and failed_result.payload["summary"]["source_networker"] == "core"
    es = FakeES()
    from apps.writers import elasticsearch as module
    monkeypatch.setattr(module, "datetime", type("Clock", (), {"now": staticmethod(lambda zone: NOW)}))
    writer = ElasticsearchWriter(ElasticsearchConfig(networker_current=config()))
    writer.client, writer._async_bulk = es, es.bulk
    await writer.write_many([failed_result])
    assert es.store[("nw-current-core", "running:core")]["running_count"] is None
    assert es.store[("nw-current-core", "running:core")]["data_status"] == "unavailable"
    assert es.store[("nw-current-core", "policy-host:core:" + DAY)]["projection_status"] == "unavailable"
    await collector.close()


@pytest.mark.asyncio
async def test_long_running_failure_and_disappearance_remain_distinct_from_confirmed_zero():
    es = FakeES()
    publisher = NetworkerDailyCurrent(es, es.bulk, config())
    running = hits([active(startTime="2026-10-01T12:00:00Z")])
    await publisher.publish("core", running, NOW, True, observation=observation(7100))
    assert es.store[("nw-current-core", "running:core")]["running_count"] == 1
    await publisher.publish("core", [], NOW + timedelta(minutes=5), False, observation=observation(success=False))
    status = es.store[("nw-current-core", "running:core")]
    row = es.store[("nw-current-core", "running:core:7100")]
    assert status["data_status"] == "unavailable" and status["running_count"] is None
    assert row["execution_status"] == "unconfirmed" and row["last_observed_at"] == NOW.isoformat()
    await publisher.publish("core", [], NOW + timedelta(minutes=10), True, observation=observation())
    status = es.store[("nw-current-core", "running:core")]
    assert status["data_status"] == "partial" and status["unconfirmed_workflow_count"] == 1
    empty = FakeES()
    await NetworkerDailyCurrent(empty, empty.bulk, config()).publish("core", [], NOW, True, observation=observation())
    assert empty.store[("nw-current-core", "running:core")]["data_status"] == "complete"
    assert empty.store[("nw-current-core", "running:core")]["running_count"] == 0


@pytest.mark.asyncio
async def test_daily_projection_replaces_failed_keys_after_success_and_never_counts_generations():
    es = FakeES()
    publisher = NetworkerDailyCurrent(es, es.bulk, config())
    failure = hits([job(clientHostname="synthetic-client", completionStatus="Failed")])
    for h in failure:
        es.store[h["_index"], h["_id"]] = h["_source"]
    await publisher.publish("core", [], NOW, True)
    key = ("nw-current-core", "policy-host:core:" + DAY)
    assert es.store[key]["failed_policy_host_keys"] == ["core:SYNTH_POLICY:synthetic-client"]
    success = hits([job(7101, clientHostname="synthetic-client", startTime="2026-11-11T13:00:00Z", endTime="2026-11-11T13:10:00Z")])
    await NetworkerDailyCurrent(es, es.bulk, config()).publish("core", success, NOW, True)
    assert es.store[key]["failed_policy_host_keys"] == []
    assert es.store[key]["projection_status"] == "ready"
    assert len([d for d in es.store.values() if d.get("document_kind") == "daily_policy_host"]) == 1
    assert len([d for d in es.store.values() if d.get("summary_role") == "generation"]) == 2
    for h in success:
        es.store[h["_index"], h["_id"]] = h["_source"]
    es.conflict_summary = True
    with pytest.raises(ConflictError):
        await publisher.publish("core", [], NOW, True)
    assert es.store[key]["projection_status"] == "updating"
    assert es.store[key]["failed_policy_host_keys"] == []


@pytest.mark.asyncio
async def test_projection_bulk_failure_never_exposes_unposted_failure():
    class FailingES(FakeES):
        async def bulk(self, client, actions, **kwargs):
            actions = list(actions)
            if any(a.get("_source", {}).get("projection_status") == "ready" for a in actions):
                raise RuntimeError("synthetic projection failure")
            return await super().bulk(client, actions, **kwargs)
    es = FailingES(hits([job(clientHostname="synthetic-client", completionStatus="Failed")]))
    with pytest.raises(RuntimeError, match="synthetic projection"):
        await NetworkerDailyCurrent(es, es.bulk, config()).publish("core", [], NOW, True)
    assert es.store[("nw-current-core", "policy-host:core:" + DAY)]["projection_status"] == "updating"


@pytest.mark.asyncio
async def test_collector_to_actual_writer_preserves_audit_and_produces_running_and_failures(monkeypatch):
    async def handler(request):
        items = [active(7101)] if request.url.params["q"] == ACTIVE_JOB_QUERY else [job(clientHostname="synthetic-client", completionStatus="Failed")]
        return httpx.Response(200, json={"jobs": items})
    collector = NetworkerRestCollector(client_config())
    collector.client._rest._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    collected = await collector.collect("jobs")
    from dataclasses import replace
    collected = replace(collected, collected_at=NOW)
    from apps.writers import elasticsearch as module
    monkeypatch.setattr(module, "datetime", type("Clock", (), {"now": staticmethod(lambda zone: NOW)}))
    es = FakeES()
    writer = ElasticsearchWriter(ElasticsearchConfig(networker_current=config()))
    writer.client, writer._async_bulk = es, es.bulk
    await writer.write_many([collected])
    assert es.store[("nw-current-core", "running:core")]["running_count"] == 1
    assert es.store[("nw-current-core", "policy-host:core:" + DAY)]["failed_policy_host_count"] == 1
    raw = next(d for (i, _), d in es.store.items() if i.startswith("nw-ops-raw-"))
    assert isinstance(raw["payload"]["raw"]["jobs"]["jobs"], list)
    await collector.close()


def test_new_fields_are_explicitly_mapped_without_changing_index_patterns():
    mapping = json.loads(Path("config/elasticsearch_networker_current_index_template.example.json").read_text())
    p = mapping["template"]["mappings"]["properties"]
    assert mapping["version"] == 5 and mapping["index_patterns"] == ["nw-current-core", "nw-current-chnl", "nw-current-info"]
    assert p["failed_policy_host_keys"]["type"] == "keyword"
    assert p["last_observed_at"]["type"] == "date"
    assert p["unfinished_workflows"] == {"type": "object", "enabled": False}
    jobs = json.loads(Path("config/elasticsearch_networker_job_run_index_template.example.json").read_text())
    assert jobs["template"]["mappings"]["properties"]["payload"]["properties"]["hostname_source"]["type"] == "keyword"

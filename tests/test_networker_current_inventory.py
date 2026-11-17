"""Synthetic Inventory to current integration. No operating API or ES connection."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import httpx
import pytest

from apps.clients.rest_client import ERRORS_KEY
from apps.core.config import ElasticsearchConfig, CollectionSchedulesConfig, ScheduleConfig
from apps.collectors.networker_rest_collector import NetworkerRestCollector
from apps.models import CollectionResult
from apps.parsers.networker_rest_parser import parse_networker_rest_payload
from apps.processors.networker_current import InventoryTargets, build_daily, daily_failure_view, running_candidates
from apps.writers.elasticsearch import ElasticsearchWriter
from apps.writers.networker_current import NetworkerDailyCurrent, read_inventory
from test_networker_current import DAY, NOW, START, FakeES, config, hierarchy, hits, job, timestamp
from test_networker_current_live import active, client, client_config, observation

OBSERVED = "2026-11-11T01:00:00+00:00"


def raw_inventory(*, policies=("SYNTH_POLICY",), names=("node-a",), source="core", **group_changes):
    return {
        "clients": {"count": len(names), "clients": [
            {"hostname": name, "resourceId": {"id": f"resource-{i}"}, "protectionGroups": ["GROUP"]}
            for i, name in enumerate(names)]},
        "policies": {"count": len(policies), "protectionPolicies": [
            {"name": policy, "workflows": [{"name": "SYNTH_W", "protectionGroups": ["GROUP"], "actions": []}]}
            for policy in policies]},
        "groups": {"count": 1, "protectionGroups": [{"name": "GROUP", "workItemType": "Client",
            "workItemSource": "Static", "workItemSubType": "None", "workItems": [],
            "workItemQueries": [], "workItemExclusions": [], "dynamicAssociation": True} | group_changes]}}


def inventory_hit(raw=None, *, source="core", observed=OBSERVED, status="success", ok=True):
    raw = raw_inventory() if raw is None else raw
    parsed = parse_networker_rest_payload(raw, "networker_" + source, source_networker=source)
    result = CollectionResult(collector="networker_" + source, target_type="Networker", protocol="rest",
        collected_at=datetime.fromisoformat(observed), ok=ok, collection_class="inventory",
        payload=parsed | {"raw": raw, "collection_status": status, "endpoint_errors": {}})
    writer = ElasticsearchWriter(ElasticsearchConfig())
    action = next(a for a in writer._actions_for_result(result) if a["_index"].startswith("nw-ops-raw-"))
    return {k: action[k] for k in ("_index", "_id", "_source")}


def build(jobs, snapshots=None, *, source="core", now=NOW, days=(DAY,)):
    inventory = InventoryTargets(snapshots or [inventory_hit()], source, now)
    return build_daily(hits(jobs, source), source, list(days), now, config(), inventory=inventory)


def test_no_save_failed_workflow_and_running_get_configured_clients_without_changing_outcome():
    summary, rows = build([job(completionStatus="Failed")])[DAY]
    assert rows[0]["hostnames"] == ["node-a"]
    assert rows[0]["observed_hostnames"] == []
    assert rows[0]["hostname_sources"] == ["inventory.workflow_group_client"]
    assert rows[0]["inventory_observed_at"] == OBSERVED
    assert rows[0]["inventory_temporal_basis"] == "latest_successful_inventory"
    assert rows[0]["inventory_id"] == inventory_hit()["_id"]
    assert rows[0]["inventory_last_collection_status"] == "success"
    assert summary["failure_count"] == summary["workflow_count"] == 1
    assert daily_failure_view(summary, rows)["failed_policy_host_keys"] == ["core:SYNTH_POLICY:node-a"]
    inventory = InventoryTargets([inventory_hit()], "core", NOW)
    running = running_candidates(hits([active() | {"clientHostname": ""}]), "core", inventory)["7100"]
    assert running["hostnames"] == ["node-a"] and not running["end_time"]


def test_policy_source_and_duplicate_resource_membership_are_separate():
    snapshots = [inventory_hit(raw_inventory(policies=("Database", "Filesystem", "NAS"), names=("Node-A.", "node-a"))),
                 inventory_hit(raw_inventory(names=("other-source",)), source="chnl")]
    jobs = [job(7100+i, name=p, completionStatus="Failed") for i,p in enumerate(("Database", "Filesystem", "NAS"))]
    summary, rows = build(jobs, snapshots)[DAY]
    assert {tuple(r["hostnames"]) for r in rows} == {("node-a",)}
    assert daily_failure_view(summary, rows)["failed_policy_host_keys"] == [
        "core:Database:node-a", "core:Filesystem:node-a", "core:NAS:node-a"]
    assert summary["workflow_count"] == 3


def test_multiple_groups_and_clients_deduplicate_and_do_not_multiply_workflow_counts():
    raw = raw_inventory(names=("node-a", "node-b", "node-a"))
    raw["groups"]["protectionGroups"].append(raw["groups"]["protectionGroups"][0] | {"name": "SECOND"})
    raw["groups"]["count"] = 2
    raw["clients"]["clients"][0]["protectionGroups"].append("SECOND")
    raw["policies"]["protectionPolicies"][0]["workflows"][0]["protectionGroups"].append("SECOND")
    summary, rows = build([job(completionStatus="Failed")], [inventory_hit(raw)])[DAY]
    assert summary["failure_count"] == 1
    assert rows[0]["hostnames"] == ["node-a", "node-b"]
    assert daily_failure_view(summary, rows)["failed_policy_host_count"] == 2


def test_retry_removes_only_its_contribution_while_other_failed_workflow_keeps_key():
    raw = raw_inventory()
    raw["policies"]["protectionPolicies"][0]["workflows"].append({"name": "OTHER", "protectionGroups": ["GROUP"]})
    snapshots = [inventory_hit(raw)]
    jobs = [job(completionStatus="Failed"), job(7101, startTime="2026-11-11T13:00:00Z", endTime="2026-11-11T13:20:00Z")]
    summary, rows = build(jobs, snapshots)[DAY]
    assert summary["success_count"] == 1 and not daily_failure_view(summary, rows)["failed_policy_host_keys"]
    summary, rows = build(jobs+[job(7102, workflowName="OTHER", completionStatus="Failed")], snapshots)[DAY]
    assert daily_failure_view(summary, rows)["failed_policy_host_keys"] == ["core:SYNTH_POLICY:node-a"]


@pytest.mark.parametrize("change,status", [
    ({"workItemSource": "Dynamic"}, "dynamic_group_not_resolved"),
    ({"workItemType": "VMware"}, "group_type_not_supported_or_unknown"),
    ({"workItemSource": None}, "group_type_not_supported_or_unknown"),
    ({"workItemQueries": None}, "group_metadata_incomplete"),
    ({"workItemQueries": ["runtime query"]}, "group_query_or_exclusions_not_resolved"),
    ({"workItemExclusions": ["resource-1"]}, "group_query_or_exclusions_not_resolved"),
])
def test_dynamic_and_unsupported_group_interpretations_never_invent_static_targets(change, status):
    summary, rows = build([job(completionStatus="Failed")], [inventory_hit(raw_inventory(**change))])[DAY]
    assert rows[0]["inventory_status"] == status and rows[0]["hostnames"] == []
    assert daily_failure_view(summary, rows)["unidentified_failure_count"] == 1


@pytest.mark.parametrize("status,ok", [("partial", True), ("error", False)])
def test_failed_or_partial_attempt_keeps_last_complete_inventory_without_age_limit(status, ok):
    good = inventory_hit(observed="2026-11-08T01:00:00+00:00")
    failed = inventory_hit(observed="2026-11-11T11:00:00+00:00", status=status, ok=ok)
    failed["_source"]["payload"]["clients"] = []
    summary, rows = build([job(completionStatus="Failed")], [good, failed])[DAY]
    assert rows[0]["hostnames"] == ["node-a"]
    assert rows[0]["inventory_status"] == "resolved"
    assert rows[0]["inventory_id"] == good["_id"]
    assert rows[0]["inventory_last_attempt_at"] == failed["_source"]["@timestamp"]
    assert rows[0]["inventory_last_collection_status"] == status
    assert "latest_inventory_partial_or_failed" in rows[0]["inventory_reasons"]
    assert daily_failure_view(summary, rows)["failed_policy_host_keys"] == ["core:SYNTH_POLICY:node-a"]


def test_missing_relationship_empty_target_and_partial_collection_are_different():
    missing = inventory_hit()
    missing["_source"]["payload"]["protection_groups"] = []
    _, rows = build([job()], [missing])[DAY]
    assert rows[0]["inventory_status"] == "group_definition_missing"
    empty = inventory_hit(raw_inventory(names=()))
    _, rows = build([job()], [empty])[DAY]
    assert rows[0]["hostname_relation_status"] == "configured_empty"
    partial = inventory_hit(status="partial")
    _, rows = build([job()], [partial])[DAY]
    assert rows[0]["inventory_status"] == "unavailable"


def test_latest_normal_inventory_after_workflow_execution_replaces_older_pre_execution_data():
    older = inventory_hit()
    newer = inventory_hit(raw_inventory(names=("node-b",)), observed="2026-11-11T23:00:00+00:00")
    for snapshots in ([newer, older], [older, newer], [newer]):
        summary, rows = build([job(completionStatus="Failed")], snapshots)[DAY]
        assert rows[0]["hostnames"] == ["node-b"]
        assert rows[0]["inventory_id"] == newer["_id"]
        assert rows[0]["inventory_temporal_basis"] == "latest_successful_inventory"
        assert rows[0]["inventory_reasons"] == []
        assert daily_failure_view(summary, rows)["failed_policy_host_keys"] == ["core:SYNTH_POLICY:node-b"]


def test_only_latest_complete_same_source_inventory_is_selected():
    good = inventory_hit(raw_inventory(names=("latest-good",)), observed="2026-11-11T22:00:00+00:00")
    failed = inventory_hit(raw_inventory(names=("failed-host",)), observed="2026-11-11T23:00:00+00:00", status="error", ok=False)
    incomplete = inventory_hit(raw_inventory(names=("incomplete-host",)), observed="2026-11-12T00:00:00+00:00")
    incomplete["_source"]["payload"]["inventory_status"]["complete"] = False
    other_source = inventory_hit(raw_inventory(names=("other-source",)), source="chnl", observed="2026-11-12T00:05:00+00:00")
    future = inventory_hit(raw_inventory(names=("future",)), observed="2026-11-13T00:00:00+00:00")
    summary, rows = build([job(completionStatus="Failed")], [other_source, incomplete, failed, inventory_hit(), future, good])[DAY]
    assert rows[0]["inventory_id"] == good["_id"]
    assert rows[0]["hostnames"] == ["latest-good"]
    assert rows[0]["inventory_last_attempt_at"] == incomplete["_source"]["@timestamp"]
    assert rows[0]["inventory_last_collection_status"] == "partial"
    assert daily_failure_view(summary, rows)["failed_policy_host_keys"] == ["core:SYNTH_POLICY:latest-good"]


def test_same_latest_inventory_applies_to_all_rebuilt_days_and_running():
    previous_day = "2026-11-10"
    older_job = job(7103, completionStatus="Failed", startTime="2026-11-10T12:00:00Z", endTime="2026-11-10T12:20:00Z")
    newer = inventory_hit(raw_inventory(names=("newest-target",)), observed="2026-11-11T23:00:00+00:00")
    inventory = InventoryTargets([inventory_hit(), newer], "core", NOW)
    documents = hits([older_job, job(completionStatus="Failed"), active(7102) | {"clientHostname": ""}])
    output = build_daily(documents, "core", [previous_day, DAY], NOW, config(), inventory=inventory)
    for summary, rows in output.values():
        assert {r["inventory_id"] for r in rows} == {newer["_id"]}
        assert {tuple(r["hostnames"]) for r in rows} == {("newest-target",)}
        assert daily_failure_view(summary, rows)["failed_policy_host_keys"] == ["core:SYNTH_POLICY:newest-target"]
    assert running_candidates(documents, "core", inventory)["7102"]["inventory_id"] == newer["_id"]


def test_save_observation_conflict_does_not_merge_targets_and_unavailable_inventory_keeps_legacy_save():
    jobs = hierarchy()
    jobs[-1]["clientHostname"] = "node-b"
    jobs[0]["completionStatus"] = "Failed"
    summary, rows = build(jobs)[DAY]
    assert rows[0]["hostname_status"] == "conflict" and rows[0]["hostnames"] == []
    assert rows[0]["configured_hostnames"] == ["node-a"] and rows[0]["observed_hostnames"] == ["node-b"]
    assert summary["failure_count"] == 1
    documents = hits(jobs)
    for hit in documents:
        hit["_source"]["payload"].pop("hostname_source", None)
    _, rows = build_daily(documents, "core", [DAY], NOW, config(), inventory=InventoryTargets([], "core", NOW))[DAY]
    assert rows[0]["hostnames"] == ["node-b"]
    assert rows[0]["hostname_sources"] == ["save_job.stored_client"]


def test_subset_of_configured_clients_is_observation_not_configuration_conflict():
    jobs = hierarchy()
    jobs[-1]["clientHostname"] = "node-a"
    _, rows = build(jobs, [inventory_hit(raw_inventory(names=("node-a", "node-b")))])[DAY]
    assert rows[0]["hostnames"] == ["node-a", "node-b"]
    assert rows[0]["observed_hostnames"] == ["node-a"]
    assert rows[0]["hostname_relation_status"] == "configured_and_observed"


def test_conflicting_legacy_save_claims_are_not_erased_by_configured_targets():
    jobs = hierarchy()
    jobs[-1]["clientHostname"] = "node-a"
    documents = hits(jobs)
    duplicate = deepcopy(documents[-1])
    duplicate["_index"] = "nw-unmapped-job-run-2026-11"
    duplicate["_source"]["payload"]["client_name"] = "node-b"
    inventory = InventoryTargets([inventory_hit()], "core", NOW)
    _, rows = build_daily(documents+[duplicate], "core", [DAY], NOW, config(), inventory=inventory)[DAY]
    assert rows[0]["observed_hostname_conflict"] is True
    assert rows[0]["hostnames"] == [] and rows[0]["hostname_status"] == "conflict"


@pytest.mark.asyncio
async def test_inventory_api_requests_are_list_only_and_roundtrip_existing_writer_raw_snapshot():
    raw, requests = raw_inventory(), []
    async def handler(request):
        requests.append(request)
        key = {"clients": "clients", "protectionpolicies": "policies", "protectiongroups": "groups"}[request.url.path.rsplit("/",1)[-1]]
        return httpx.Response(200, json=raw[key])
    collector = NetworkerRestCollector(client_config(schedule=CollectionSchedulesConfig(
        jobs=ScheduleConfig(interval_minutes=5), inventory=ScheduleConfig(interval_minutes=1440))))
    collector.client._rest._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        collected = await collector.collect("inventory")
    finally:
        await collector.close()
    assert collected.ok and collected.payload["inventory_status"]["complete"] is True
    assert len(requests) == 3 and all(r.method == "GET" for r in requests)
    assert "protectionGroups" in requests[0].url.params["fl"]
    assert collected.payload["clients"][0]["protection_groups"] == ["GROUP"]


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["truncated", "cross_origin", "cycle", "malformed", "duplicate"])
async def test_inventory_invalid_pages_are_partial_and_never_healthy_empty(fault):
    async def handler(request):
        if request.url.path.endswith("/clients"):
            response = {"clients": [], "count": 1}
            if fault in {"cross_origin", "cycle"}:
                response["links"] = [{"rel": "next", "href": "https://other.invalid/clients" if fault == "cross_origin" else str(request.url)}]
            if fault == "malformed":
                response = {"clients": None}
            if fault == "duplicate":
                response = {"clients": [{"resourceId": {"id": "same-resource"}, "hostname": "node-a"}]*2, "count": 2}
            return httpx.Response(200, json=response)
        return httpx.Response(200, json={"protectionGroups": [], "protectionPolicies": [], "count": 0})
    instance = client(handler)
    try:
        payload = await instance.fetch_payloads("inventory")
    finally:
        await instance.close()
    assert "clients" in payload[ERRORS_KEY]
    assert not parse_networker_rest_payload(payload, "networker_core", source_networker="core")["inventory_status"]["complete"]


@pytest.mark.asyncio
async def test_inventory_pagination_keeps_memberships_and_filters_without_per_client_requests():
    calls = []
    raw = raw_inventory(names=("node-a", "node-b"))
    async def handler(request):
        calls.append(request)
        if request.url.path.endswith("/clients"):
            offset = int(request.url.params.get("cursor", "0"))
            page = {"count": 2, "clients": [raw["clients"]["clients"][offset]]}
            if offset == 0:
                page["links"] = [{"rel": "next", "href": str(request.url.copy_add_param("cursor", "1"))}]
            return httpx.Response(200, json=page)
        key = "policies" if request.url.path.endswith("/protectionpolicies") else "groups"
        return httpx.Response(200, json=raw[key])
    instance = client(handler)
    try:
        response = await instance.fetch_payloads("inventory")
    finally:
        await instance.close()
    assert ERRORS_KEY not in response
    assert len(response["clients"]["clients"]) == 2
    assert len(calls) == 4 and all(r.url.params.get("fl") == calls[0].url.params["fl"] for r in calls[:2])


def test_partial_inventory_without_membership_and_same_timestamp_conflict_are_not_used():
    raw = raw_inventory()
    raw["clients"]["clients"][0].pop("protectionGroups")
    partial = inventory_hit(raw)
    assert not partial["_source"]["payload"]["inventory_status"]["complete"]
    _, rows = build([job()], [partial])[DAY]
    assert rows[0]["inventory_status"] == "unavailable"
    conflict = inventory_hit(raw_inventory(names=("other",)))
    _, rows = build([job()], [inventory_hit(), conflict])[DAY]
    assert rows[0]["hostnames"] == []
    assert "inventory_snapshot_conflict" in rows[0]["inventory_reasons"]


class InventoryES(FakeES):
    async def search(self, **args):
        if args["sort"] != [{"@timestamp": "desc"}, "_shard_doc"]:
            return await super().search(**args)
        self.requests.append(("search", deepcopy(args)))
        records = [(key, d) for key,d in self.pits[args["pit"]["id"]] if d.get("collection_class") in {"inventory", "slow"}]
        records.sort(key=lambda item: item[1]["@timestamp"], reverse=True)
        values = [{"_index":key[0], "_id":key[1], "_source":d, "sort":[d["@timestamp"], i]} for i,(key,d) in enumerate(records)]
        if "search_after" in args:
            after = args["search_after"]
            values = [h for h in values if h["sort"][0] < after[0] or h["sort"][0] == after[0] and h["sort"][1] > after[1]]
        return {"pit_id":"pit", "_shards":{"failed":0}, "hits":{"hits":values[:args["size"]]}}


@pytest.mark.asyncio
async def test_inventory_reader_real_current_publication_running_retry_and_generation():
    es = InventoryES([inventory_hit()])
    publisher = NetworkerDailyCurrent(es, es.bulk, config())
    incoming = hits([job(completionStatus="Failed"), active(7102, workflowName="RUNNING") | {"clientHostname": ""}])
    raw = inventory_hit()
    raw["_source"]["payload"]["workflows"].append(raw["_source"]["payload"]["workflows"][0] | {"workflow_name":"RUNNING"})
    es.store[(raw["_index"],raw["_id"])] = raw["_source"]
    for hit in incoming:
        es.store[(hit["_index"], hit["_id"])] = hit["_source"]
    await publisher.publish("core", incoming, NOW, True, observation=observation(7102))
    assert es.store[("nw-current-core","running:core:7102")]["hostname"] == "node-a"
    projection = ("nw-current-core", "policy-host:core:"+DAY)
    assert es.store[projection]["failed_policy_host_keys"] == ["core:SYNTH_POLICY:node-a"]
    await publisher.publish("core", hits([job(7101, startTime="2026-11-11T13:00:00Z", endTime="2026-11-11T13:20:00Z")]), NOW+timedelta(minutes=5), True)
    assert es.store[projection]["failed_policy_host_keys"] == []
    raw_queries = [a for kind,a in es.requests if kind=="search" and a["sort"]==[{"@timestamp":"desc"},"_shard_doc"]]
    assert raw_queries and all("payload." not in json.dumps(a["query"]) for a in raw_queries)
    assert all("nw-ops-raw-core" in a["index"] for kind,a in es.requests if kind=="open" and "raw" in a["index"])


@pytest.mark.asyncio
async def test_next_calculation_updates_daily_and_running_from_one_new_inventory(monkeypatch):
    before = inventory_hit()
    es = InventoryES([before])
    publisher = NetworkerDailyCurrent(es, es.bulk, config())
    jobs = [job(completionStatus="Failed"), active(7102) | {"clientHostname": ""}]
    instances, original = [], InventoryTargets.identify
    def record(instance, *args):
        instances.append(id(instance))
        return original(instance, *args)
    monkeypatch.setattr(InventoryTargets, "identify", record)
    async def publish(at):
        documents = hits(jobs, collected=at)
        for hit in documents:
            es.store[(hit["_index"], hit["_id"])] = hit["_source"]
        await publisher.publish("core", documents, at, True, observation=observation(7102))
    await publish(NOW)
    assert len(set(instances)) == 1 and len(instances) >= 3
    projection_key = ("nw-current-core", "policy-host:core:"+DAY)
    assert es.store[projection_key]["failed_policy_host_keys"] == ["core:SYNTH_POLICY:node-a"]
    newer = inventory_hit(raw_inventory(names=("node-b",)), observed=(NOW+timedelta(minutes=1)).isoformat())
    es.store[(newer["_index"], newer["_id"])] = newer["_source"]
    instances.clear()
    await publish(NOW+timedelta(minutes=5))
    assert len(set(instances)) == 1 and len(instances) >= 3
    assert es.store[projection_key]["failed_policy_host_keys"] == ["core:SYNTH_POLICY:node-b"]
    head = es.store[("nw-current-core", "daily:core:"+DAY)]
    rows = [d for d in es.store.values() if d.get("document_kind")=="workflow_result" and d.get("generation")==head["generation"]]
    running = es.store[("nw-current-core", "running:core:7102")]
    assert {r["inventory_id"] for r in rows} == {newer["_id"]}
    assert running["inventory_id"] == newer["_id"] and running["hostnames"] == ["node-b"]
    assert head["failure_count"] == 1


@pytest.mark.asyncio
async def test_reader_partial_snapshot_is_unavailable_but_save_fallback_survives():
    class PartialInventory(InventoryES):
        async def search(self, **args):
            value = await super().search(**args)
            value["timed_out"] = True
            return value
    es = PartialInventory([inventory_hit()])
    value = await read_inventory(es, config(), "core", NOW)
    assert value.read_reason == "inventory_partial_search"
    assert value.latest_complete is None and value.inventory_id is None
    assert any(kind=="close" for kind,_ in es.requests)


@pytest.mark.asyncio
async def test_reader_stops_after_latest_complete_inventory_and_keeps_ties_across_pages():
    good = inventory_hit()
    other = inventory_hit(raw_inventory(names=("different",)))
    other["_id"] += "-copy"
    older = inventory_hit(observed="2026-11-10T01:00:00+00:00")
    ancient = inventory_hit(observed="2026-09-01T00:00:00+00:00")
    es = InventoryES([good, other, older, ancient])
    value = await read_inventory(es, config(page_size=1), "core", NOW)
    # Both equal-time conflicting snapshots are read; the ancient snapshot is not.
    assert value.read_reason == "inventory_snapshot_conflict"
    assert len([a for k,a in es.requests if k=="search"]) == 3
    assert value.latest_complete == timestamp(OBSERVED)
    assert value.inventory_id is None  # Conflicting latest copies are not arbitrarily selected.


@pytest.mark.asyncio
async def test_whole_inventory_failure_retains_authoritative_source_for_following_current_reads(monkeypatch):
    collector = NetworkerRestCollector(client_config())
    async def fail(*args):
        raise RuntimeError("synthetic failure")
    monkeypatch.setattr(collector.client, "fetch_payloads", fail)
    try:
        result = await collector.collect("inventory")
    finally:
        await collector.close()
    assert result.payload["summary"]["source_networker"] == "core"
    assert result.payload["collection_status"] == "error"


def test_current_inventory_fields_are_explicitly_mapped():
    value = json.loads(Path("config/elasticsearch_networker_current_index_template.example.json").read_text())
    fields = value["template"]["mappings"]["properties"]
    for key in ("configured_hostnames", "observed_hostnames", "inventory_status", "inventory_temporal_basis", "hostname_relation_status", "inventory_reasons", "inventory_id", "inventory_last_collection_status"):
        assert fields[key] == {"type":"keyword"}
    assert fields["inventory_observed_at"] == fields["inventory_last_attempt_at"] == {"type":"date"}


def test_workflow_identification_does_not_rescan_inventory_lists():
    class Counted(list):
        traversals = 0
        def __iter__(self):
            self.traversals += 1
            return super().__iter__()
    snapshot = inventory_hit()
    payload = snapshot["_source"]["payload"]
    lists = [Counted(payload[k]) for k in ("clients", "workflows", "protection_groups")]
    for key,value in zip(("clients", "workflows", "protection_groups"), lists):
        payload[key] = value
    inventory = InventoryTargets([snapshot], "core", NOW)
    traversals = [v.traversals for v in lists]
    normalized = hits([job()])[0]["_source"]["payload"]
    for _ in range(400):
        assert inventory.identify(normalized, {"hostnames": []})["hostnames"] == ["node-a"]
    assert [v.traversals for v in lists] == traversals

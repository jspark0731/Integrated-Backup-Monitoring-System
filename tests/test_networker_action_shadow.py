"""Synthetic HTTP only; operational identifiers and inputs are never read."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from urllib.parse import unquote

import httpx
import pytest

from apps.lookups.networker_action.adapters import IncompleteRead, VersionConflict
from apps.lookups.networker_action_shadow.reader import PITReader, range_query, validate_indices
from apps.lookups.networker_action.models import digest
from apps.lookups.networker_action_shadow.analysis import analyse, execute
from apps.lookups.networker_action_shadow.publisher import HTTPTransport, ShadowHTTPError, ShadowPublisher, ShadowState
from apps.lookups.networker_action_shadow.schema import SHADOW_INDEX, lookup_schema

START = datetime(2026, 11, 9, 3, tzinfo=timezone.utc)
END = START + timedelta(hours=2, minutes=30)
RAW_END = START + timedelta(hours=2)
OBS = START + timedelta(minutes=30)
JOB = "tmp-nw-job-run-4c2-20261109"
RAW = "nw-ops-raw-core-2026-11"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    import socket
    original_connect = socket.socket.connect

    def denied(*args, **kwargs):
        raise AssertionError("external_network_forbidden")

    def loopback_only(sock, address):
        # Windows asyncio creates a loopback socketpair for its event loop.
        if isinstance(address, tuple) and address[0] in {"127.0.0.1", "::1"}:
            return original_connect(sock, address)
        return denied()

    monkeypatch.setattr("socket.socket.connect", loopback_only)
    monkeypatch.setattr("socket.getaddrinfo", denied)


def job(identity="8100", kind="backup action job", parent="8000", root="8000", status="success", source="core"):
    return {"_index": JOB, "_id": source + ":" + identity, "_source": {
        "source_networker": source, "observed_at": OBS.isoformat(), "document_type": "job-run",
        "payload": {"source_networker": source, "job_id": identity, "type": kind,
                    "parent_job_id": parent, "root_parent_job_id": root,
                    "start_time": START.isoformat(), "end_time": (START + timedelta(minutes=20)).isoformat() if status != "running" else None,
                    "status_normalized": status, "protection_group": "synthetic-group"}}}


def raw(group="synthetic-group", *, identity="raw-1", backup_id="backup-1", source="core"):
    return {"_index": RAW, "_id": identity, "_source": {
        "source_networker": source, "@timestamp": OBS.isoformat(), "collection_class": "backups", "ok": True,
        "payload": {"summary": {"source_networker": source}, "collection_status": "success",
            "raw": {"backups": {"backups": [{"id": backup_id,
                "saveTime": (START + timedelta(minutes=1)).isoformat(),
                "completionTime": (START + timedelta(minutes=19)).isoformat(),
                "attributes": [{"key": k, "values": [v]} for k, v in [
                    ("*policy action jobid", "8100"), ("*policy name", "synthetic-policy: 99"),
                    ("*policy workflow name", "synthetic-workflow: 99"), ("group", group)]]}]}}}}}


def hierarchy():
    return [job(), job("8000", "workflow job", "0", "0"), job("8200", "save job", "8100")]


class FakeHTTP:
    def __init__(self, jobs=None, raws=None):
        self.inputs = {JOB: deepcopy(jobs if jobs is not None else hierarchy()), RAW: deepcopy(raws if raws is not None else [raw()])}
        self.calls, self.pits, self.docs = [], {}, {}
        self.resource, self.search_number, self.puts, self.conflicts = None, 0, 0, 0
        self.mutate_search = None
        self.fail_put = None
        self.on_conflict = None

    async def request(self, method, path, **kwargs):
        self.calls.append((method, path, deepcopy(kwargs)))
        if path.endswith("/_field_caps"):
            fields = kwargs["params"]["fields"].split(",")
            return {"fields": {f: {"date" if f in {"observed_at", "@timestamp"} else "keyword": {"searchable": True}}
                               for f in fields}}
        if path.endswith("/_pit") and path != "/_pit":
            indices = path[1:-5].split(",")
            hits = [h for i in indices for h in (deepcopy(self.inputs[i]) if i != SHADOW_INDEX else
                [{"_index": SHADOW_INDEX, "_id": key, "_source": deepcopy(value["_source"])} for key, value in self.docs.items()])]
            pit = "pit-" + str(len(self.calls))
            self.pits[pit] = hits
            return {"id": pit, "_shards": {"failed": 0}}
        if path == "/_search":
            body = kwargs["json"]
            snapshot = self.pits[body["pit"]["id"]]
            hits = snapshot
            if "bool" in body["query"]:
                filters = body["query"]["bool"]["filter"]
                bounds = filters[0]["range"]
                field = next(iter(bounds))
                window = bounds[field]
                hits = [h for h in snapshot if datetime.fromisoformat(window["gte"]) <= datetime.fromisoformat(h["_source"][field]) < datetime.fromisoformat(window["lt"])]
            start = body.get("search_after", [-1])[0] + 1
            page = deepcopy(hits[start:start + body["size"]])
            for n, hit in enumerate(page, start):
                hit["sort"] = [n]
            pit = "rotated-" + str(len(self.calls))
            self.pits[pit] = snapshot
            result = {"pit_id": pit, "hits": {"total": {"value": len(hits), "relation": "eq"}, "hits": page},
                      "timed_out": False, "_shards": {"failed": 0}}
            self.search_number += 1
            if self.mutate_search:
                self.mutate_search(result, self.search_number)
            return result
        if path == "/_pit":
            assert method == "DELETE" and kwargs["json"]["id"] in self.pits
            return {"succeeded": True}
        if path == "/" + SHADOW_INDEX:
            if method == "PUT":
                assert self.resource is None
                schema = kwargs["json"]
                self.resource = {"settings": {"index.mode": "lookup", "index.number_of_shards": "1", "index.mapping.ignore_malformed": "false"},
                                 "mappings": deepcopy(schema["mappings"]), "aliases": {}}
                return {"acknowledged": True}
            if self.resource is None:
                raise ShadowHTTPError(404)
            return {SHADOW_INDEX: deepcopy(self.resource)}
        prefix = "/" + SHADOW_INDEX + "/_doc/"
        assert path.startswith(prefix), "non_shadow_write_forbidden"
        key = unquote(path[len(prefix):])
        if method == "GET":
            if key not in self.docs:
                raise ShadowHTTPError(404)
            return deepcopy(self.docs[key])
        assert method == "PUT"
        if self.fail_put:
            raise RuntimeError("synthetic_write_failure")
        if self.conflicts:
            self.conflicts -= 1
            if self.on_conflict:
                self.on_conflict(self, key)
            raise ShadowHTTPError(409)
        params = kwargs["params"]
        old = self.docs.get(key)
        if old:
            if params.get("if_seq_no") != old["_seq_no"] or params.get("if_primary_term") != old["_primary_term"]:
                raise ShadowHTTPError(409)
        elif params != {"op_type": "create"}:
            raise ShadowHTTPError(409)
        self.puts += 1
        self.docs[key] = {"_source": deepcopy(kwargs["json"]), "_seq_no": old["_seq_no"] + 1 if old else 0, "_primary_term": 1}
        return {"result": "updated" if old else "created", "_shards": {"failed": 0}}


async def run(http, **kwargs):
    return await execute(http, source="core", job_indices=[JOB], raw_indices=[RAW],
        job_start=START, job_end=END, raw_start=START, raw_end=RAW_END, **kwargs)


def plan(raws, jobs, generation):
    return analyse(raws, jobs, "core", synced_at=END, generation=generation)


@pytest.mark.asyncio
async def test_reader_rotated_pit_empty_page_exact_total_and_cleanup():
    http = FakeHTTP()
    reader = PITReader(http, page_size=1)
    hits = await reader.read([JOB], "jobs", range_query("core", "jobs", START, END))
    assert len(hits) == 3
    assert reader.audit.pages == 4 and reader.audit.complete and reader.audit.pit_closed
    searches = [c[2]["json"] for c in http.calls if c[1] == "/_search"]
    assert searches[0]["track_total_hits"] is True
    assert searches[1]["search_after"] == [0]
    assert searches[-1]["pit"]["id"] == http.calls[-2][2]["json"]["pit"]["id"]
    assert http.calls[-1][2]["json"]["id"].startswith("rotated-")


@pytest.mark.parametrize("defect", ["duplicate", "cursor", "total", "timeout", "shard", "clusters", "source", "total_relation"])
@pytest.mark.asyncio
async def test_reader_fail_closed_and_closes(defect):
    http = FakeHTTP()

    def mutate(result, page):
        hits = result["hits"]["hits"]
        if defect == "duplicate" and page == 2:
            hits[0]["_id"] = "core:8100"
        if defect == "cursor" and page == 2:
            hits[0]["sort"] = [0]
        if defect == "total" and page == 1:
            result["hits"]["total"]["value"] += 1
        if defect == "total_relation":
            result["hits"]["total"]["relation"] = "gte"
        if defect == "timeout":
            result["timed_out"] = True
        if defect == "shard":
            result["_shards"]["failed"] = 1
        if defect == "clusters":
            result["_clusters"] = {"partial": 1}
        if defect == "source" and hits:
            hits[0].pop("_source")
    http.mutate_search = mutate
    reader = PITReader(http, page_size=1)
    with pytest.raises(IncompleteRead):
        await reader.read([JOB], "jobs", range_query("core", "jobs", START, END))
    assert reader.audit.pit_closed and not reader.audit.complete


@pytest.mark.asyncio
async def test_reader_budget_and_pit_close_failure():
    http = FakeHTTP()
    reader = PITReader(http, max_pages=1)
    with pytest.raises(IncompleteRead, match="page_budget"):
        await reader.read([JOB], "jobs", range_query("core", "jobs", START, END))
    assert reader.audit.pit_closed


@pytest.mark.asyncio
async def test_pit_snapshot_does_not_mix_updated_sources():
    http = FakeHTTP()
    def mutate(_result, page):
        if page == 1:
            http.inputs[JOB][1]["_source"]["payload"]["status_normalized"] = "failed"
    http.mutate_search = mutate
    reader = PITReader(http, page_size=1)
    hits = await reader.read([JOB], "jobs", range_query("core", "jobs", START, END))
    assert hits[1]["_source"]["payload"]["status_normalized"] == "success"


@pytest.mark.asyncio
async def test_half_open_observed_window_independent_of_execution_time():
    jobs = hierarchy()
    jobs[0]["_source"]["observed_at"] = START.isoformat()
    jobs[1]["_source"]["observed_at"] = END.isoformat()
    jobs[2]["_source"]["payload"]["start_time"] = (START - timedelta(days=31)).isoformat()
    http = FakeHTTP(jobs=jobs)
    reader = PITReader(http)
    hits = await reader.read([JOB], "jobs", range_query("core", "jobs", START, END))
    assert len(hits) == 2
    assert hits[1]["_source"]["payload"]["start_time"].startswith("2026-10")


@pytest.mark.asyncio
async def test_dry_run_no_index_writes_no_coverage_promotion():
    http = FakeHTTP()
    result = await run(http, page_size=1)
    assert result["job_run_documents"] == 3 and result["raw_documents"] == 1
    assert result["verification_status"]["verified"] == 0
    assert result["hierarchy"]["save_to_action_linked"] == 1
    assert result["hierarchy"]["action_to_workflow_linked"] == 1
    assert result["read_coverage"]["stored_scope_complete"] and not result["read_coverage"]["api_coverage_proven"]
    assert not any(method == "PUT" for method, _, _ in http.calls)


def test_manual_group_and_missing_parent_are_not_proof():
    jobs = [job("8200", "save job", "0", "0"), job("8201", "save job", "9999", "9998")]
    result = analyse([], jobs, "core", synced_at=END)["report"]
    assert result["direct_execution_candidates"] == 1 and result["direct_execution_confirmed"] == 0
    assert result["predeployment_parent_missing_possible"] == 1
    assert result["predeployment_parent_missing_confirmed"] == 0
    assert result["missing_action_job"] == 1
    assert result["verification_status"]["verified"] == 0


def test_missing_workflow_and_unsupported_types_preserve_counts():
    jobs = [job(status="failed"), job("8200", "savefs job", "8100"), job("8201", "recover pipe to save job", "8100")]
    built = analyse([], jobs, "core", synced_at=END)
    assert built["documents"]["core:8100"]["action_status"] == "failed"
    assert built["report"]["workflow_job_missing"] == 1
    assert built["report"]["job_types"]["savefs job"] == 1
    assert built["report"]["hierarchy"].get("save_to_action_linked", 0) == 0


def test_source_mismatch_rejected_without_hostname_inference():
    with pytest.raises(ValueError, match="input_source"):
        analyse([raw()], [job(source="chnl")], "core")


@pytest.mark.asyncio
async def test_425_baseline_is_job_count_not_lookup_count():
    jobs = [job(str(8000 + i), "workflow job", "0", "0") for i in range(9)]
    jobs += [job(str(8100 + i), root=str(8000 + i), parent=str(8000 + i)) for i in range(5)]
    jobs += [job(str(10000 + i), "save job", str(8100 + i % 5), str(8000 + i % 5)) for i in range(61)]
    jobs += [job(str(11000 + i), "save job", "9900" if i < 323 else "9901", "9990") for i in range(338)]
    jobs += [job(str(20000 + i), "savefs job") for i in range(5)]
    jobs += [job(str(21000 + i), "recover pipe to save job") for i in range(7)]
    # Some baseline rows must lie in the corrected final 30 minutes, so reverting
    # to 05:00 cannot accidentally keep this test passing.
    jobs[-2]["_source"]["observed_at"] = "2026-11-09T05:00:00+00:00"
    jobs[-1]["_source"]["observed_at"] = "2026-11-09T05:29:59+00:00"
    outside = [job("99001", "save job"), job("99002", "save job")]
    outside[0]["_source"]["observed_at"] = (START - timedelta(seconds=1)).isoformat()
    outside[1]["_source"]["observed_at"] = END.isoformat()
    http = FakeHTTP(jobs=jobs + outside, raws=[])
    report = await run(http)
    assert report["job_run_documents"] == 425
    assert report["baseline_comparison"]["total_matches"]
    assert all(v["matches"] for v in report["baseline_comparison"]["types"].values())
    assert report["hierarchy"]["save_to_action_linked"] == 61
    assert report["hierarchy"]["action_to_workflow_linked"] == 5
    assert report["hierarchy"]["save_parent_missing"] == 338
    assert report["action_lookup_documents"] == 7
    assert report["baseline_comparison"]["client_subgroups_evaluated"] is False
    assert report["reads"]["jobs"]["expected_total"] == 425
    assert report["read_coverage"]["job_observed_to"] == "2026-11-09T05:30:00+00:00"
    assert report["read_coverage"]["raw_collected_to"] == "2026-11-09T05:00:00+00:00"


@pytest.mark.asyncio
async def test_shadow_create_idempotency_and_stable_checkpoint(tmp_path):
    http = FakeHTTP()
    state = ShadowState(tmp_path / "state.sqlite", "scope")
    try:
        first = await run(http, state=state, publisher=ShadowPublisher(http), create_shadow=True)
        assert first["publisher"]["created"] == 1 and http.puts == 1
        before = deepcopy(http.docs)
        second = await run(http, state=state, publisher=ShadowPublisher(http))
        assert second["publisher"]["unchanged"] == 1
        assert http.docs == before and http.puts == 1 and state.pending() is None
        assert all(method != "PUT" or path.startswith("/" + SHADOW_INDEX) for method, path, _ in http.calls)
    finally:
        state.close()


@pytest.mark.asyncio
async def test_partial_requery_retains_candidates_and_detects_new_ambiguity(tmp_path):
    state = ShadowState(tmp_path / "state.sqlite", "scope")
    http = FakeHTTP()
    try:
        await run(http, state=state, publisher=ShadowPublisher(http), create_shadow=True)
        http.inputs[RAW] = [raw("other-group", identity="raw-2", backup_id="backup-2")]
        result = await run(http, state=state, publisher=ShadowPublisher(http))
        assert result["cumulative_ledger"]["mapping_status"]["ambiguous"] == 1
        assert http.docs["core:8100"]["_source"]["candidate_count"] == 2
        assert http.docs["core:8100"]["_source"]["protection_group"] is None
    finally:
        state.close()


@pytest.mark.asyncio
async def test_checkpoint_crash_replay_without_rescan(tmp_path):
    path = tmp_path / "state.sqlite"
    state = ShadowState(path, "scope")
    http = FakeHTTP()
    http.fail_put = True
    with pytest.raises(RuntimeError):
        await run(http, state=state, publisher=ShadowPublisher(http), create_shadow=True)
    assert state.pending()
    state.close()
    restored = ShadowState(path, "scope")
    http.calls.clear()
    http.fail_put = False
    try:
        result = await run(http, state=restored, publisher=ShadowPublisher(http))
        assert result["resumed_pending"] and not restored.pending()
        assert not any(path.startswith("/" + JOB) or path.startswith("/" + RAW) for _, path, _ in http.calls)
    finally:
        restored.close()


@pytest.mark.asyncio
async def test_replay_after_partial_write_skips_already_written_documents(tmp_path):
    state = ShadowState(tmp_path / "state.sqlite", "scope")
    publisher = ShadowPublisher(FakeHTTP())
    publisher.http.inputs[JOB].append(job("8101"))
    staged = state.stage([raw()], publisher.http.inputs[JOB], plan)
    await publisher.prepare(create=True)
    one = next(iter(staged["documents"]))
    await publisher.publish({one: staged["documents"][one]})
    result = await state.publish(publisher, staged)
    assert result["unchanged"] == 1 and result["created"] == 1
    state.close()


@pytest.mark.asyncio
async def test_cas_retry_reloads_then_checks_latest_evidence():
    http = FakeHTTP()
    publisher = ShadowPublisher(http)
    documents = plan([raw()], hierarchy(), "one")["documents"]
    await publisher.prepare(create=True)
    http.conflicts = 1
    result = await publisher.publish(documents)
    assert result["cas_retries"] == 1 and result["created"] == 1
    incoming = plan([raw()], hierarchy(), "two")["documents"]
    http.conflicts = 1
    def conflicting(server, key):
        server.docs[key]["_source"]["action_start_time"] = (START - timedelta(days=1)).isoformat()
        server.docs[key]["_seq_no"] += 1
    http.on_conflict = conflicting
    with pytest.raises(ValueError, match="execution_replacement"):
        await publisher.publish(incoming)


@pytest.mark.asyncio
async def test_cas_exhaustion_does_not_advance_checkpoint(tmp_path):
    state = ShadowState(tmp_path / "state.sqlite", "scope")
    http = FakeHTTP()
    http.conflicts = 10
    with pytest.raises(VersionConflict):
        await run(http, state=state, publisher=ShadowPublisher(http), create_shadow=True)
    assert state.pending()
    assert state.db.execute("SELECT value FROM metadata WHERE name='completed_generation'").fetchone() is None
    state.close()


@pytest.mark.parametrize("defect", ["mode", "mapping", "alias", "pipeline"])
@pytest.mark.asyncio
async def test_existing_shadow_contract_is_checked(defect):
    http = FakeHTTP()
    publisher = ShadowPublisher(http)
    await publisher.prepare(create=True)
    if defect == "mode":
        http.resource["settings"]["index.mode"] = "standard"
    elif defect == "mapping":
        http.resource["mappings"]["properties"]["action_run_key"] = {"type": "text"}
    elif defect == "alias":
        http.resource["aliases"] = {"unexpected": {}}
    else:
        http.resource["settings"]["index.final_pipeline"] = "unexpected"
    with pytest.raises(ValueError, match="schema"):
        await publisher.prepare()


@pytest.mark.asyncio
async def test_http_transport_encodes_stable_id_and_cas_parameters():
    requests = []
    async def handle(request):
        requests.append(request)
        return httpx.Response(200, json={"result": "updated"})
    async with httpx.AsyncClient(base_url="https://synthetic.invalid", transport=httpx.MockTransport(handle)) as client:
        await HTTPTransport(client).request("PUT", f"/{SHADOW_INDEX}/_doc/core%3A8100", json={},
                                             params={"if_seq_no": 4, "if_primary_term": 2})
    assert requests[0].url.params["if_seq_no"] == "4"
    assert requests[0].url.raw_path.startswith(("/" + SHADOW_INDEX).encode())


@pytest.mark.asyncio
async def test_http_error_does_not_expose_body_or_url():
    async def handle(request):
        return httpx.Response(401, json={"secret": "sensitive-sentinel"})
    async with httpx.AsyncClient(base_url="https://synthetic.invalid", transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ShadowHTTPError) as error:
            await HTTPTransport(client).request("GET", "/")
    assert "sentinel" not in str(error.value) and "synthetic" not in str(error.value)


def test_only_explicit_inputs_and_exact_shadow_target():
    for indices, channel in [(["nw-*"], "jobs"), ([SHADOW_INDEX], "jobs"), ([JOB], "raw"), (["nw-core-job-run-2026-13"], "jobs")]:
        with pytest.raises(ValueError):
            validate_indices(indices, channel)
    with pytest.raises(ValueError):
        ShadowPublisher(FakeHTTP(), index="nw-ops-lookup-actionrun-v1")
    with pytest.raises(ValueError):
        range_query("core", "jobs", END, START)


def test_schema_exactly_reuses_4a_contract():
    from tools.generate_networker_step4b_poc import document_schema
    prior = document_schema("docs/networker_job_mapping_step4_implementation_plan.md")[0]
    assert lookup_schema() == prior


def test_scope_binding_and_ledger_budget(tmp_path):
    path = tmp_path / "state.sqlite"
    state = ShadowState(path, "one")
    with pytest.raises(ValueError, match="ledger_budget"):
        state.stage([raw()], hierarchy(), plan, max_records=1)
    assert state.hits("raw") == []
    state.close()
    with pytest.raises(ValueError, match="scope"):
        ShadowState(path, "two")


def test_shadow_artifacts_mapping_and_request_boundaries():
    from pathlib import Path
    import re
    root = Path(__file__).resolve().parents[1]
    mapping = json.loads((root / "docs/networker_job_mapping_step4c2_shadow_mapping.json").read_text(encoding="utf-8"))
    assert mapping == lookup_schema()
    text = (root / "docs/networker_job_mapping_step4c2_queries.http").read_text(encoding="utf-8")
    requests = list(re.finditer(r"^(GET|POST|PUT|DELETE|PATCH) ([^\r\n]+)\r?$", text, re.M))
    assert len(requests) == 13
    esql_count = 0
    for n, request in enumerate(requests):
        method, endpoint = request.groups()
        if method != "GET":
            assert method == "POST" and endpoint in {"_query", SHADOW_INDEX + "/_refresh"}
        body = text[request.end():requests[n + 1].start() if n + 1 < len(requests) else len(text)]
        body = "\n".join(line for line in body.splitlines() if not line.startswith("#")).strip()
        if body:
            body = re.sub(r'"""\s*\n(.*?)"""', lambda m: json.dumps(m.group(1)), body, flags=re.S)
            parsed = json.loads(body)
            if endpoint == "_query":
                assert "FROM " in parsed["query"]
                assert not re.search(r"FROM nw-", parsed["query"])
                if "observed_at" in parsed["query"]:
                    assert 'observed_at >= TO_DATETIME("2026-11-09T03:00:00Z")' in parsed["query"]
                    assert 'observed_at < TO_DATETIME("2026-11-09T05:30:00Z")' in parsed["query"]
                esql_count += 1
    assert esql_count == 8


@pytest.mark.asyncio
async def test_bad_mapping_stops_before_reads_and_writes():
    http = FakeHTTP()
    original = http.request
    async def bad(method, path, **kwargs):
        if path.endswith("/_field_caps"):
            return {"fields": {}}
        return await original(method, path, **kwargs)
    http.request = bad
    with pytest.raises(IncompleteRead, match="mapping"):
        await run(http)
    assert http.puts == 0


def test_unmapped_duplicate_and_terminal_preference():
    copies = hierarchy()
    older = job(status="running")
    older["_index"] = "nw-unmapped-job-run-2026-11"
    copies.append(older)
    built = analyse([raw(), raw()], copies, "core", synced_at=END)
    assert built["documents"]["core:8100"]["action_status"] == "success"
    assert built["documents"]["core:8100"]["candidate_count"] == 1
    assert built["report"]["raw_evidence"]["unique_backup_identities"] == 1


@pytest.mark.asyncio
async def test_duplicate_lookup_key_blocks_publication():
    http = FakeHTTP()
    publisher = ShadowPublisher(http)
    documents = plan([raw()], hierarchy(), "one")["documents"]
    await publisher.prepare(create=True)
    await publisher.publish(documents)
    http.docs["rogue-id"] = deepcopy(http.docs["core:8100"])
    before = http.puts
    with pytest.raises(ValueError, match="noncanonical_key"):
        await publisher.publish(documents)
    assert http.puts == before


@pytest.mark.asyncio
async def test_raw_failure_and_invalid_attributes_remain_incomplete():
    raw_hit = raw()
    raw_hit["_source"]["payload"]["collection_status"] = "partial"
    raw_hit["_source"]["payload"]["raw"]["backups"]["backups"][0]["attributes"] = []
    report = (await run(FakeHTTP(raws=[raw_hit])))
    assert report["incomplete"]
    assert report["raw_evidence"]["quarantined_issues"] > 0
    assert report["verification_status"]["verified"] == 0


@pytest.mark.asyncio
async def test_id_reuse_blocks_and_preserves_prior_execution_metadata(tmp_path):
    http = FakeHTTP()
    state = ShadowState(tmp_path / "state.sqlite", "scope")
    try:
        await run(http, state=state, publisher=ShadowPublisher(http), create_shadow=True)
        original = http.docs["core:8100"]["_source"]["action_start_time"]
        http.inputs[JOB][0]["_source"]["payload"]["start_time"] = (START - timedelta(hours=1)).isoformat()
        await run(http, state=state, publisher=ShadowPublisher(http))
        result = http.docs["core:8100"]["_source"]
        assert result["verification_status"] == "blocked"
        assert result["action_start_time"] == original and result["policy_name"] is None
        before = http.puts
        await run(http, state=state, publisher=ShadowPublisher(http))
        assert http.puts == before
    finally:
        state.close()


@pytest.mark.asyncio
async def test_stale_plan_and_stale_observation_are_blocked(tmp_path):
    http = FakeHTTP()
    publisher = ShadowPublisher(http)
    state = ShadowState(tmp_path / "state.sqlite", "scope")
    try:
        first = state.stage([raw()], hierarchy(), plan)
        await publisher.prepare(create=True)
        await state.publish(publisher, first)
        second = state.stage([raw(identity="new", backup_id="new")], hierarchy(), plan)
        await state.publish(publisher, second)
        with pytest.raises(ValueError, match="stale_local"):
            await state.publish(publisher, first)
        stale = deepcopy(second["documents"])
        stale["core:8100"]["lookup_observed_at"] = START.isoformat()
        with pytest.raises(ValueError, match="stale_evidence"):
            await publisher.publish(stale)
    finally:
        state.close()


@pytest.mark.asyncio
async def test_pit_close_failure_prevents_complete_channel():
    http = FakeHTTP()
    original = http.request
    async def request(method, path, **kwargs):
        if path == "/_pit":
            return {"succeeded": False}
        return await original(method, path, **kwargs)
    http.request = request
    reader = PITReader(http)
    with pytest.raises(IncompleteRead, match="pit_close"):
        await reader.read([JOB], "jobs", range_query("core", "jobs", START, END))
    assert not reader.audit.complete


@pytest.mark.asyncio
async def test_cli_uses_mock_http_and_writes_aggregate_report(tmp_path, monkeypatch, capsys):
    from tools import networker_action_shadow as cli
    # main() owns its event loop; exercise its async run() here and the filesystem
    # output path in the synchronous CLI test below.
    http = FakeHTTP()
    original_client = httpx.AsyncClient
    seen = []

    async def handle(request):
        seen.append(request)
        kwargs = {"params": dict(request.url.params)}
        if request.content:
            kwargs["json"] = json.loads(request.content)
        try:
            response = await http.request(request.method, request.url.path, **kwargs)
            return httpx.Response(200, json=response)
        except ShadowHTTPError as exc:
            return httpx.Response(exc.status, json={"error": "synthetic"})

    monkeypatch.setenv("NW_LOOKUP_ES_URL", "https://synthetic.invalid")
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: original_client(**kwargs, transport=httpx.MockTransport(handle)))
    args = cli.parser().parse_args(["--raw-start", START.isoformat(), "--raw-end", END.isoformat(), "--output", str(tmp_path / "report")])
    report = await cli.run(args)
    assert report["mode"] == "dry-run" and report["job_run_documents"] == 3
    assert all(r.method != "PUT" for r in seen)


def test_cli_output_and_error_privacy(tmp_path, monkeypatch, capsys):
    from tools import networker_action_shadow as cli
    report = analyse([raw()], hierarchy(), "core", synced_at=END)["report"] | {"mode": "dry-run"}
    async def fake_run(args):
        return report
    monkeypatch.setattr(cli, "run", fake_run)
    args = ["--raw-start", START.isoformat(), "--raw-end", END.isoformat(), "--output", str(tmp_path / "aggregate")]
    assert cli.main(args) == 3
    data = (tmp_path / "aggregate" / "summary.json").read_text(encoding="utf-8")
    assert json.loads(data)["job_run_documents"] == 3
    assert "synthetic-policy" not in data and "backup-1" not in data and "8100" not in data
    assert (tmp_path / "aggregate" / "summary.md").exists()
    assert cli.main(args) == 2  # never overwrite a prior result
    assert "synthetic-policy" not in capsys.readouterr().err


@pytest.mark.asyncio
async def test_dry_mode_rejects_state_and_invalid_url_before_connection(tmp_path, monkeypatch):
    from tools import networker_action_shadow as cli
    args = cli.parser().parse_args(["--raw-start", START.isoformat(), "--raw-end", END.isoformat(), "--output", str(tmp_path / "out")])
    monkeypatch.setenv("NW_LOOKUP_ES_URL", "https://user:secret@synthetic.invalid")
    with pytest.raises(ValueError, match="https"):
        await cli.run(args)
    args.state = tmp_path / "state.sqlite"
    with pytest.raises(ValueError, match="dry_run"):
        await cli.run(args)


def test_cli_corrected_job_window_and_independent_raw_bounds():
    from tools import networker_action_shadow as cli
    from apps.lookups.networker_action_shadow.reader import DEFAULT_JOB_START, DEFAULT_JOB_END
    args = cli.parser().parse_args(["--raw-start", START.isoformat(), "--raw-end", RAW_END.isoformat(), "--output", "unused"])
    assert args.source == "core"
    assert args.job_start == DEFAULT_JOB_START == "2026-11-09T03:00:00Z"
    assert args.job_end == DEFAULT_JOB_END == "2026-11-09T05:30:00Z"
    assert args.raw_end == "2026-11-09T05:00:00+00:00"
    query = range_query(args.source, "jobs", args.job_start, args.job_end)
    assert query["bool"]["filter"][0] == {"range": {"observed_at": {
        "gte": "2026-11-09T03:00:00+00:00", "lt": "2026-11-09T05:30:00+00:00"}}}
    with pytest.raises(ValueError, match="explicit_input"):
        validate_indices(["tmp-nw-parent-lookup-20261109"], "jobs")


@pytest.mark.asyncio
async def test_explicit_original_index_without_fixed_index():
    original = "nw-core-job-run-2026-11"
    http = FakeHTTP()
    http.inputs[original] = http.inputs.pop(JOB)
    for hit in http.inputs[original]:
        hit["_index"] = original
    result = await execute(http, source="core", job_indices=[original], raw_indices=[RAW],
        job_start=START, job_end=END, raw_start=START, raw_end=RAW_END)
    assert result["mode"] == "dry-run" and result["job_run_documents"] == 3
    assert not any(JOB in path for _, path, _ in http.calls)
    assert not any(method == "PUT" for method, _, _ in http.calls)

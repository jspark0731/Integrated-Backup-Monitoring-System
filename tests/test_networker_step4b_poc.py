"""Offline artifact contracts. These tests do not execute ES|QL or any HTTP."""

from collections import Counter
from copy import deepcopy
from datetime import datetime
import json
import re

import pytest

from apps.lookups.networker_action import ActionLookupBuilder, Coverage
from tools import generate_networker_step4b_poc as poc


@pytest.fixture(scope="module")
def fixture():
    return poc.build_fixture()


def test_serialized_raw_jobs_replay_through_real_builder(fixture):
    # Replay the exact exported JSON, not a separately handwritten Lookup stub.
    serialized = json.loads(json.dumps(fixture))
    value = ActionLookupBuilder()
    value.add_jobs(serialized["job_run_hits"])
    value.add_raw(serialized["raw_initial_inputs"])
    coverages = []
    for item in serialized["coverage"]:
        item["start"], item["end"] = datetime.fromisoformat(item["start"]), datetime.fromisoformat(item["end"])
        coverages.append(Coverage(**item))
    assert value.build(synced_at=poc.SYNCED, coverage=coverages, generation=poc.GENERATION) == fixture["builder_initial"]
    value.add_raw(serialized["raw_initial_inputs"])
    assert value.build(synced_at=poc.SYNCED, coverage=coverages, generation=poc.GENERATION) == fixture["builder_initial"]
    value.add_raw([serialized["raw_delayed_input"]])
    assert value.build(synced_at=poc.SYNCED.replace(minute=40), coverage=coverages,
                       generation=poc.GENERATION) == fixture["builder_after_delay"]


def test_normal_multi_backup_and_explicit_synthetic_verification(fixture):
    normal = fixture["builder_initial"]["core:8101"]
    assert normal["candidate_count"] == 1 and normal["backup_reference_count"] == 3
    assert normal["mapping_status"] == "resolved" and normal["verification_status"] == "verified"
    assert normal["candidate_evidence"]["coverage_proof"].startswith("SYNTHETIC-ONLY-")
    assert normal["policy_name"] == "POC_CORE_POLICY"
    assert len(fixture["es_action_initial"]) == 8


def test_delay_updates_use_actual_python_output_and_never_forge_coverage(fixture):
    before, after = fixture["builder_initial"][poc.DELAYED], fixture["builder_after_delay"][poc.DELAYED]
    assert before["mapping_status"] == "unresolved" and before["candidate_count"] == 0
    assert before["evidence_state"] == "awaiting_backups"
    assert after["mapping_status"] == "resolved" and after["candidate_count"] == 1
    assert after["verification_status"] == "provisional" and after["policy_name"] is None
    assert after["candidate_evidence"]["coverage_proof"] == ""
    published = fixture["es_action_after_delay"]
    assert published[poc.DELAYED] == after
    for key in set(published) - {poc.DELAYED}:
        assert published[key] == fixture["es_action_initial"][key]


def test_failed_action_conflicts_and_missing_action_have_distinct_states(fixture):
    documents = fixture["es_action_initial"]
    failed = documents["core:8103"]
    assert failed["action_status"] == "failed" and failed["mapping_status"] == "unresolved"
    assert failed["evidence_state"] == "no_backup_in_covered_window"
    conflict = documents["core:8104"]
    assert conflict["mapping_status"] == "ambiguous" and conflict["candidate_count"] == 4
    assert conflict["policy_name"] is conflict["workflow_name"] is conflict["protection_group"] is None
    assert documents["core:8105"]["evidence_state"] == "missing_action_job"


def test_expected_save_rows_do_not_join_by_root_or_client(fixture):
    rows = {row["source_networker"] + ":" + row["job_id"]: row for row in fixture["expected_initial_saves"]}
    wrong_parent = rows["core:8207"]
    assert wrong_parent["action_run_key"] == "core:8999"
    assert wrong_parent["relation_policy"] is None and wrong_parent["evidence_state"] == "missing_action_job"
    assert fixture["es_action_initial"]["core:8106"]["verification_status"] == "verified"
    assert rows["core:8208"]["relation_state"] == "invalid_parent"
    assert rows["core:8209"]["relation_state"] == "lookup_missing"
    assert rows["core:8209"]["lookup_source_networker"] is None
    assert rows["core:8209"]["source_networker"] == "core"
    # Omitting an explicitly enumerated record simulates publication lag only.
    assert poc.OMITTED in fixture["builder_initial"] and poc.OMITTED not in fixture["es_action_initial"]


def test_source_separation_source_row_counts_and_a_b_contract(fixture):
    rows = fixture["expected_initial_saves"]
    assert len(rows) == 10
    assert Counter(row["original_status"] for row in rows) == {"success": 7, "failed": 3}
    assert sum(row["relation_policy"] is not None for row in rows) == 3
    assert sum(row["action_job_id"] is not None for row in rows) == 8
    assert sum(row["a_workflow_job_id"] is not None for row in rows) == 8
    policies = {r["source_networker"]: r["relation_policy"] for r in rows if r["action_run_key"] in ["core:8101", "chnl:8101"]}
    assert policies == {"core": "POC_CORE_POLICY", "chnl": "POC_CHNL_POLICY"}
    assert Counter(hit["_source"]["payload"]["type"] for hit in fixture["job_run_hits"]) == {
        "save job": 10, "backup action job": 6, "workflow job": 7}


def test_action_mapping_and_source_mapping_keep_existing_contract(fixture):
    mapping = deepcopy(fixture["mappings"][poc.ACTION])
    assert mapping["settings"].pop("number_of_replicas") == 0
    assert mapping == poc.document_schema("docs/networker_job_mapping_step4_implementation_plan.md")[0]
    fields = mapping["mappings"]["properties"]
    assert len(fields) == 29
    for key in ["action_run_key", "source_networker", "action_job_id"]:
        assert fields[key] == {"type": "keyword"}
    for identity, document in fixture["es_action_initial"].items():
        assert identity == document["action_run_key"]
        assert set(document) == set(fields)
        if document["verification_status"] != "verified" or document["mapping_status"] != "resolved":
            assert all(document[name] is None for name in ["policy_name", "workflow_name", "protection_group"])
    source = fixture["mappings"][poc.JOB]["mappings"]["properties"]
    assert source["payload"]["properties"]["exit_code"]["type"] == "long"
    assert source["payload"]["properties"]["parent_job_id"]["type"] == "keyword"


def test_bulk_inputs_equal_exported_python_documents_without_target_overrides(fixture):
    requests = {r["id"]: r for r in poc.requests(fixture)}
    lines = requests["M05"]["body"].splitlines()
    actual = {json.loads(lines[n])["create"]["_id"]: json.loads(lines[n + 1]) for n in range(0, len(lines), 2)}
    assert actual == fixture["es_action_initial"]
    assert requests["U01"]["body"] == fixture["builder_after_delay"][poc.DELAYED]
    assert requests["D01"]["body"] == fixture["builder_initial"]["core:8101"]
    assert requests["D01"]["path"].endswith("/_create/" + poc.DUPLICATE_ID)


def test_negative_duplicate_fixture_reveals_row_explosion_not_distinct_masking(fixture):
    counts = Counter(d["action_run_key"] for d in fixture["es_action_initial"].values())
    counts["core:8101"] += 1
    rows = fixture["expected_initial_saves"]
    assert sum(max(1, counts[row["action_run_key"]]) for row in rows) == 12
    assert len([key for key, count in counts.items() if count > 1]) == 1
    assert all("COUNT_DISTINCT" not in r["body"].get("query", "") for r in poc.requests(fixture) if isinstance(r["body"], dict))


def test_http_and_fixture_are_reproducible_and_contain_no_real_identifiers(fixture):
    for path, content in poc.artifacts().items():
        assert path.read_text(encoding="utf-8") == content
        for forbidden in ["3250942", "3250390", "3250389", "3258300", "p1ficdb1"]:
            assert forbidden not in content
    assert fixture["synthetic_only"] is True
    assert len(poc.requests(fixture)) == 44
    assert [r["id"] for r in poc.requests(fixture)[-3:]] == ["C01", "C02", "C03"]


def test_requests_reference_only_exact_poc_indices_and_sync_queries(fixture):
    request_list = poc.requests(fixture)
    poc.validate_requests(request_list)
    for r in request_list:
        if r["path"].startswith("_query"):
            assert r["method"] == "POST" and "allow_partial_results=false" in r["path"]
            q = r["body"]["query"]
            assert "nw-ops-lookup" not in q
            if "LOOKUP JOIN " + poc.ACTION in q:
                assert q.index("RENAME source_networker AS job_source_networker") < q.index("LOOKUP JOIN " + poc.ACTION)
                assert "RENAME job_source_networker AS source_networker" in q
                assert "EVAL action_run_key = requested_action_run_key" in q


@pytest.mark.parametrize("bad", [
    {"method": "PUT", "path": "nw-ops-lookup-actionrun-v1", "body": {}},
    {"method": "DELETE", "path": "poc-nw-s4b-*", "body": None},
    {"method": "POST", "path": poc.ACTION + "/_bulk", "body": '{"create":{"_id":"x","_index":"nw-core-job-run-2026-11"}}\n{}\n'},
    {"method": "POST", "path": "_query", "body": {"query": "FROM nw-core-job-run-2026-11"}},
])
def test_target_guard_rejects_production_targets_and_bulk_overrides(bad):
    with pytest.raises(AssertionError):
        poc.validate_requests([bad])


def response_for(fixture, phase="initial"):
    documents = fixture["es_action_initial" if phase == "initial" else "es_action_after_delay"]
    return {"timed_out": False, "_shards": {"failed": 0}, "hits": {
        "total": {"value": len(documents), "relation": "eq"},
        "hits": [{"_index": poc.ACTION, "_id": k, "_source": deepcopy(v)} for k, v in documents.items()]}}


def test_saved_response_comparison_validates_entire_document_not_just_counts(fixture):
    poc.verify_action_response(response_for(fixture), fixture)
    poc.verify_action_response(response_for(fixture, "after-delay"), fixture, phase="after-delay")
    response = response_for(fixture)
    response["hits"]["hits"][0]["_source"]["policy_name"] = "WRONG"
    with pytest.raises(ValueError, match="mismatch"):
        poc.verify_action_response(response, fixture)


@pytest.mark.parametrize("mutation", ["timeout", "shard", "total", "missing", "duplicate", "wrong_phase"])
def test_saved_response_comparison_rejects_incomplete_or_wrong_snapshot(fixture, mutation):
    response = response_for(fixture)
    if mutation == "timeout":
        response["timed_out"] = True
    elif mutation == "shard":
        response["_shards"]["failed"] = 1
    elif mutation == "total":
        response["hits"]["total"]["relation"] = "gte"
    elif mutation == "missing":
        response["hits"]["hits"].pop()
    elif mutation == "duplicate":
        response["hits"]["hits"].append(deepcopy(response["hits"]["hits"][0]))
    elif mutation == "wrong_phase":
        response = response_for(fixture, "after-delay")
    with pytest.raises(ValueError):
        poc.verify_action_response(response, fixture)


def test_http_json_ndjson_framing_is_valid():
    text = poc.HTTP_PATH.read_text(encoding="utf-8")
    requests, current = [], None
    for line in text.splitlines():
        match = re.fullmatch(r"(GET|POST|PUT|DELETE) (\S+)", line)
        if match:
            if current:
                requests.append(current)
            current = [match[1], match[2], []]
        elif current and line.strip() and not line.startswith("#"):
            current[2].append(line)
    requests.append(current)
    assert len(requests) == 44
    for method, path, body in requests:
        if not body:
            continue
        if "/_bulk" in path:
            assert len(body) % 2 == 0
            for line in body:
                json.loads(line)
        else:
            json.loads("\n".join(body))


@pytest.mark.parametrize("protected", [b"\x9b DRMONE encrypted", b"\x00binary"])
def test_generator_never_overwrites_unreadable_or_binary_artifact(tmp_path, monkeypatch, protected):
    normal, binary = tmp_path / "normal.http", tmp_path / "protected.json"
    normal.write_text("original text", encoding="utf-8")
    binary.write_bytes(protected)
    monkeypatch.setattr(poc, "artifacts", lambda: {normal: "new text", binary: "new text"})
    monkeypatch.setattr("sys.argv", ["generate_networker_step4b_poc.py"])
    with pytest.raises((UnicodeDecodeError, ValueError)):
        poc.main()
    assert normal.read_text(encoding="utf-8") == "original text"
    assert binary.read_bytes() == protected

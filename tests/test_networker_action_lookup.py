"""Synthetic observations only. Never instantiate a real ES/NetWorker client."""

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from apps.lookups.networker_action import ActionLookupBuilder, Coverage
from apps.lookups.networker_action.adapters import (
    Checkpoint, ElasticsearchReadAdapter, IncompleteRead, InMemoryLookupStore, VersionConflict,
)
from apps.lookups.networker_action.extract import extract_raw
from apps.lookups.networker_action.models import positive_id
from apps.lookups.networker_action.normalize import normalize_name
from apps.lookups.networker_action.validate import read_job, validate_save

START = datetime(2026, 11, 5, tzinfo=timezone.utc)
END = START + timedelta(minutes=20)
OBSERVED = END + timedelta(minutes=5)
SYNCED = OBSERVED + timedelta(minutes=5)
ACTION = "3258300"
ROOT = "3258299"
KEY = "core:" + ACTION


def backup(identity="synthetic-backup-1", *, action=ACTION, policy="SYNTH_POLICY: 1793823604",
           workflow="SYNTH_WORKFLOW: 1793823604", group="SYNTH_GROUP"):
    return {"id": identity, "clientHostname": "synthetic-client", "saveTime": (START + timedelta(minutes=1)).isoformat(),
            "completionTime": (END - timedelta(minutes=1)).isoformat(),
            "attributes": [{"key": k, "values": [v]} for k, v in [
                ("*policy action jobid", action), ("*policy action name", "backup: 1793823604"),
                ("*policy name", policy), ("*policy workflow name", workflow), ("group", group)]]}


def raw(items=None, *, source="core", nested=True):
    response = items if items is not None else [backup()]
    return {"_index": f"nw-ops-raw-{source}-2026-11", "_id": "synthetic-raw",
            "_source": {"@timestamp": OBSERVED.isoformat(), "source_networker": source, "ok": True,
                        "raw_document_id": "synthetic-raw", "payload": {
                            "summary": {"source_networker": source}, "collection_status": "success",
                            "endpoint_errors": {}, "raw": {"backups": {"backups": response} if nested else response}}}}


def job(identity=ACTION, kind="backup action job", *, source="core", parent=ROOT, root=ROOT,
        start=START, end=END, status="success", domain=None, observed=OBSERVED):
    return {"_index": f"nw-{domain or source}-job-run-2026-11", "_id": "synthetic:" + identity,
            "_source": {"document_type": "job-run", "source_networker": source,
                        "observed_at": observed.isoformat(), "payload": {
                            "source_networker": source, "job_id": identity, "type": kind,
                            "parent_job_id": parent, "root_parent_job_id": root,
                            "status_normalized": status, "start_time": start.isoformat() if start else None,
                            "end_time": end.isoformat() if end else None}}}


def hierarchy(source="core"):
    return [job(source=source), job(ROOT, "workflow job", source=source, parent=0, root=0,
                                  start=START - timedelta(minutes=1), end=END + timedelta(minutes=1)),
            job("3258301", "save job", source=source, parent=ACTION,
                start=START + timedelta(minutes=1), end=END - timedelta(minutes=1))]


def complete(source="core"):
    return Coverage(source, ACTION, START, END, jobs_complete=True, backups_complete=True,
                    suffix_reviewed=True, proof_reference="synthetic-complete-action-window")


def builder(items=None, jobs=None, source="core"):
    value = ActionLookupBuilder()
    value.add_jobs(hierarchy(source) if jobs is None else jobs)
    value.add_raw([raw(items, source=source)])
    return value


def build(value, *coverage):
    return value.build(synced_at=SYNCED, coverage=coverage)


def test_real_nested_response_and_collector_list_shape_preserve_originals():
    for nested in [True, False]:
        result = extract_raw(raw(nested=nested))
        assert not result.issues
        evidence = result.evidence[0]
        assert evidence.action_id == ACTION
        assert evidence.candidate == ("SYNTH_POLICY", "SYNTH_WORKFLOW", "SYNTH_GROUP")
        assert evidence.policy.original == "SYNTH_POLICY: 1793823604"
        assert evidence.policy.suffix == "1793823604"
        assert evidence.attributes[1]["values"] == ["backup: 1793823604"]
        assert evidence.reference["position"] == 0


def test_current_collector_summary_source_is_authoritative_without_top_level_field():
    hit = raw()
    del hit["_source"]["source_networker"]
    assert extract_raw(hit).evidence[0].source == "core"


@pytest.mark.parametrize("source", [None, "other", "", 2])
def test_missing_invalid_source_is_not_inferred_from_index_or_host(source):
    hit = raw()
    hit["_source"]["source_networker"] = source
    hit["_source"]["payload"]["summary"] = {}
    assert not extract_raw(hit).evidence
    assert extract_raw(hit).issues


def test_conflicting_sources_are_quarantined():
    hit = raw()
    hit["_source"]["payload"]["summary"]["source_networker"] = "chnl"
    assert not extract_raw(hit).evidence


@pytest.mark.parametrize("response", [None, {}, {"backups": None}, {"backups": {}}, "bad"])
def test_malformed_response_does_not_masquerade_as_empty_success(response):
    hit = raw()
    hit["_source"]["payload"]["raw"]["backups"] = response
    result = extract_raw(hit)
    assert result.incomplete_sources == {"core"}
    assert result.issues[0].reason == "invalid_backup_response"


@pytest.mark.parametrize("attributes", [None, [], {}, "bad", [None], [{"values": ["x"]}],
                                       [{"key": "group", "values": "x"}],
                                       [{"key": "group", "values": [1]}]])
def test_bad_attributes_quarantined_without_guessing_action(attributes):
    item = backup()
    item["attributes"] = attributes
    result = extract_raw(raw([item]))
    assert not result.evidence and result.issues and result.incomplete_sources
    assert result.issues[0].details == item


@pytest.mark.parametrize("values", [[], ["0"], ["-1"], ["1.0"], [True], [123], ["01"], ["１２"],
                                   [ACTION, "3258302"], [""], [None]])
def test_missing_multiple_or_noncanonical_action_id(values):
    item = backup()
    item["attributes"][0]["values"] = values
    result = extract_raw(raw([item]))
    assert not result.evidence and result.issues[0].action_id is None


def test_identical_repeated_keys_and_values_are_preserved_without_multiplication():
    item = backup()
    item["attributes"].append(deepcopy(item["attributes"][0]))
    item["attributes"][0]["values"] *= 2
    result = extract_raw(raw([item]))
    assert len(result.evidence) == 1
    assert len(result.evidence[0].attributes) == 6
    assert len(result.evidence[0].attributes[0]["values"]) == 2


@pytest.mark.parametrize("key", ["*policy name", "*policy workflow name", "group"])
def test_multi_value_dimensions_are_not_cartesian_products(key):
    item = backup()
    item["attributes"].append({"key": key, "values": ["OTHER"]})
    value = builder([item])
    document = build(value, complete())[KEY]
    assert document["mapping_status"] == "invalid"
    assert document["verification_status"] == "blocked"
    assert value.issues[0].action_id == ACTION


@pytest.mark.parametrize("original,normalized,suffix", [
    ("POL:ARCHIVE: 123", "POL:ARCHIVE", "123"), ("A:B", "A:B", None),
    ("A: 12x", "A: 12x", None), ("A: １２", "A: １２", None),
    (" A: 123 ", "A", "123"), ("A: 0001", "A", "0001"),
])
def test_trailing_numeric_suffix_preserves_internal_colons(original, normalized, suffix):
    name = normalize_name(original, strip_suffix=True)
    assert (name.original, name.normalized, name.suffix) == (original, normalized, suffix)
    assert normalize_name(original, strip_suffix=False).normalized == original.strip()


@pytest.mark.parametrize("value", [None, "", "  ", 1, ": 123"])
def test_invalid_names_rejected(value):
    with pytest.raises(ValueError):
        normalize_name(value, strip_suffix=True)


def test_540_backups_and_repeated_observations_produce_one_candidate_and_key():
    items = [backup(str(i)) for i in range(540)]
    value = builder(items)
    value.add_raw([raw(items)])
    documents = build(value)
    assert list(documents) == [KEY]
    document = documents[KEY]
    assert document["candidate_count"] == 1
    assert document["backup_reference_count"] == 540
    assert document["backup_reference_truncated"] is True
    assert len(document["backup_references"]["items"]) == 20
    assert document["policy_name"] is None
    assert "size_bytes" not in document


@pytest.mark.parametrize("changes", [{"group": "OTHER_GROUP"}, {"policy": "OTHER_POLICY: 12"},
                                     {"workflow": "OTHER_WORKFLOW: 12"}])
def test_relation_conflicts_are_ambiguous_and_never_published(changes):
    document = build(builder([backup(), backup("other", **changes)]), complete())[KEY]
    assert document["mapping_status"] == "ambiguous"
    assert document["candidate_count"] == 2
    assert document["policy_name"] is document["workflow_name"] is document["protection_group"] is None


def test_different_suffixes_are_auditable_before_normalized_candidate_deduplication():
    document = build(builder([backup(), backup("other", policy="SYNTH_POLICY: 42")]))[KEY]
    assert document["candidate_count"] == 1 and document["raw_candidate_count"] == 2
    assert document["verification_status"] == "provisional"


def test_missing_action_job_is_not_inferred_from_backup_or_save():
    document = build(builder(jobs=[]), complete())[KEY]
    assert document["evidence_state"] == "missing_action_job"
    assert document["action_job_type"] is None and document["policy_name"] is None


def test_save_parent_match_and_mismatch_are_explicit_diagnostics():
    action = read_job(job())
    save = read_job(hierarchy()[2])
    assert validate_save(save, action) == set()
    assert "save_parent_mismatch" in validate_save(replace(save, parent="999"), action)
    assert "source_mismatch" in validate_save(replace(save, source="chnl"), action)
    value = builder(jobs=hierarchy() + [job("9999", "save job", parent="999", root=ROOT)])
    documents = build(value)
    assert documents["core:999"]["evidence_state"] == "missing_action_job"
    assert documents[KEY]["candidate_count"] == 1


@pytest.mark.parametrize("parent", [0, None, "", "00"])
def test_invalid_parent_is_not_assigned_by_root(parent):
    value = builder(jobs=[job("9999", "save job", parent=parent)])
    documents = build(value)
    assert list(documents) == [KEY]  # RAW explicitly supplies this key, not the Save.
    assert documents[KEY]["evidence_state"] == "missing_action_job"
    assert any(i.reason == "invalid_save_parent" for i in value.issues)


def test_source_namespaces_are_separate_even_with_identical_ids_and_client():
    value = builder()
    value.add_raw([raw(source="chnl")])
    value.add_jobs(hierarchy("chnl"))
    documents = build(value, complete(), complete("chnl"))
    assert set(documents) == {KEY, "chnl:" + ACTION}
    assert all(d["verification_status"] == "verified" for d in documents.values())


def test_unmapped_and_current_index_copies_do_not_duplicate_keys_or_candidates():
    value = builder()
    duplicates = hierarchy()
    for hit in duplicates:
        hit["_index"] = "nw-unmapped-job-run-2026-11"
    value.add_jobs(duplicates)
    documents = build(value, complete())
    assert list(documents) == [KEY]
    assert documents[KEY]["verification_status"] == "verified"


def test_job_id_reuse_blocks_the_existing_key_instead_of_replacing_execution():
    value = builder()
    value.add_jobs([job(start=START - timedelta(days=1), end=END - timedelta(days=1))])
    document = build(value, complete())[KEY]
    assert document["verification_status"] == "blocked"
    assert "job_id_reuse_or_execution_conflict" in document["reason"]
    assert document["policy_name"] is None


def test_backup_identity_revision_is_not_last_wins_even_when_action_changes():
    value = builder()
    value.add_raw([raw([backup(action="999")])])
    documents = build(value, complete())
    for key in [KEY, "core:999"]:
        assert documents[key]["verification_status"] == "blocked"
        assert "backup_revision_conflict" in documents[key]["reason"]


def test_incremental_partial_rereads_never_remove_existing_candidates():
    value = builder()
    assert build(value, complete())[KEY]["policy_name"] == "SYNTH_POLICY"
    value.add_raw([raw([backup("other", group="OTHER")])])
    assert build(value, complete())[KEY]["mapping_status"] == "ambiguous"
    value.add_raw([raw([])])
    document = build(value, complete())[KEY]
    assert document["candidate_count"] == 2 and document["policy_name"] is None


def test_running_terminal_reverse_arrival_preserves_terminal_without_source_mutation():
    jobs = hierarchy()
    original = deepcopy(jobs)
    value = builder(jobs=jobs)
    value.add_jobs([job(status="running", end=None, observed=SYNCED)])
    document = build(value, complete())[KEY]
    assert document["action_status"] == "success"
    assert jobs == original


def test_failed_action_without_backup_survives_as_unresolved():
    hit = job(status="failed")
    original = deepcopy(hit)
    value = builder([], jobs=[hit, hierarchy()[1]])
    document = build(value, complete())[KEY]
    assert document["action_status"] == "failed"
    assert document["mapping_status"] == "unresolved"
    assert document["evidence_state"] == "no_backup_in_covered_window"
    assert hit == original


@pytest.mark.parametrize("change,reason", [
    ({"partial": True}, "partial_collection"), ({"query_window_gap": True}, "query_window_gap"),
    ({"backups_complete": False}, "coverage_unverified"), ({"jobs_complete": False}, "coverage_unverified"),
    ({"proof_reference": ""}, "coverage_unverified"), ({"suffix_reviewed": False}, "suffix_rule_unreviewed"),
    ({"start": START + timedelta(minutes=1)}, "coverage_unverified"),
])
def test_coverage_gates_public_names(change, reason):
    document = build(builder(), replace(complete(), **change))[KEY]
    assert document["mapping_status"] == "resolved"
    assert document["verification_status"] == "provisional"
    assert document["policy_name"] is None and reason in document["reason"]


def test_raw_partial_collection_cannot_be_overridden_by_attestation():
    value = builder()
    partial = raw([])
    partial["_source"]["payload"]["collection_status"] = "partial"
    value.add_raw([partial])
    assert build(value, complete())[KEY]["verification_status"] == "provisional"


def test_delayed_backup_can_resolve_but_not_verify_without_coverage():
    value = builder([])
    assert build(value)[KEY]["evidence_state"] == "awaiting_backups"
    value.add_raw([raw()])
    document = build(value)[KEY]
    assert document["mapping_status"] == "resolved" and document["verification_status"] == "provisional"


def test_verified_requires_explicit_action_scope_full_window_and_valid_hierarchy():
    value = builder()
    assert build(value)[KEY]["policy_name"] is None
    document = build(value, complete())[KEY]
    assert document["verification_status"] == "verified"
    assert (document["policy_name"], document["workflow_name"], document["protection_group"]) == (
        "SYNTH_POLICY", "SYNTH_WORKFLOW", "SYNTH_GROUP")
    assert document["action_workflow_run_key"] == "core:" + ROOT


@pytest.mark.parametrize("changes,reason", [
    ({"kind": "action job"}, "target_type_mismatch"),
    ({"root": "999"}, "workflow_job_missing"),
    ({"root": 0}, "workflow_root_unconfirmed"),
    ({"start": None}, "invalid_job_time_or_status"),
    ({"end": START - timedelta(seconds=1)}, "invalid_job_time_or_status"),
])
def test_unknown_action_alias_or_invalid_hierarchy_never_verified(changes, reason):
    jobs = [job(**changes), hierarchy()[1]]
    document = build(builder(jobs=jobs), complete())[KEY]
    assert reason in document["reason"] and document["policy_name"] is None


def test_save_root_conflict_blocks_relationship_publication():
    jobs = hierarchy()
    jobs[2]["_source"]["payload"]["root_parent_job_id"] = "999"
    document = build(builder(jobs=jobs), complete())[KEY]
    assert "save_root_conflict" in document["reason"]
    assert document["verification_status"] == "blocked"


def test_backup_times_must_match_action_and_epoch_values_are_supported():
    item = backup()
    item["saveTime"] = (START + timedelta(minutes=1)).timestamp()
    item["completionTime"] = (END - timedelta(minutes=1)).timestamp() * 1000
    assert build(builder([item]), complete())[KEY]["verification_status"] == "verified"
    item["saveTime"] = (START - timedelta(days=1)).isoformat()
    document = build(builder([item]), complete())[KEY]
    assert "backup_execution_time_conflict" in document["reason"]
    assert document["policy_name"] is None


def test_lookup_output_matches_documented_mapping_and_is_json_serializable():
    plan = Path('docs/networker_job_mapping_step4_implementation_plan.md').read_text(encoding='utf-8')
    schema = json.loads(plan.split('```json\n')[1].split('```')[0])
    document = build(builder(), complete())[KEY]
    assert set(document) == set(schema['mappings']['properties'])
    assert json.loads(json.dumps(document)) == document


class FakeReadTransport:
    def __init__(self, pages):
        self.pages = iter(pages)
        self.searches = []
        self.closed = []
        self.opened = []

    async def open_point_in_time(self, **kwargs):
        self.opened.append(kwargs)
        return {"id": "pit-1"}

    async def search(self, **kwargs):
        self.searches.append(kwargs)
        page = next(self.pages)
        if isinstance(page, Exception):
            raise page
        return page

    async def close_point_in_time(self, **kwargs):
        self.closed.append(kwargs)


def page(hits, **extra):
    return {"hits": {"hits": hits}, "_shards": {"failed": 0}} | extra


@pytest.mark.asyncio
async def test_fake_read_adapter_paginates_pit_and_never_creates_a_real_client():
    hit = raw() | {"sort": [1]}
    transport = FakeReadTransport([page([hit], pit_id="pit-2"), page([])])
    adapter = ElasticsearchReadAdapter(transport)
    hits = [h async for h in adapter.scan(["nw-ops-raw-core-2026-11"], {"match_all": {}})]
    assert hits == [hit]
    assert transport.searches[1]["search_after"] == [1]
    assert transport.searches[1]["pit"]["id"] == "pit-2"
    assert transport.closed == [{"id": "pit-2"}]
    assert all(c["allow_partial_search_results"] is False for c in transport.searches)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [page([], timed_out=True), page([], _shards={"failed": 1}),
                                     page([raw()]), RuntimeError("synthetic transport error")])
async def test_partial_or_failed_reads_raise_and_close_pit(failure):
    transport = FakeReadTransport([failure])
    adapter = ElasticsearchReadAdapter(transport)
    with pytest.raises((IncompleteRead, RuntimeError)):
        _ = [h async for h in adapter.scan(["nw-core-job-run-2026-11"], {"match_all": {}})]
    assert transport.closed


@pytest.mark.asyncio
async def test_read_adapter_rejects_wildcards_and_page_budget_exhaustion():
    transport = FakeReadTransport([page([raw() | {"sort": [1]}])])
    adapter = ElasticsearchReadAdapter(transport, max_pages=1)
    with pytest.raises(ValueError):
        _ = [h async for h in adapter.scan(["nw-*"], {})]
    assert transport.opened == []
    with pytest.raises(IncompleteRead, match="page_budget_exhausted"):
        _ = [h async for h in adapter.scan(["nw-core-job-run-2026-11"], {})]
    assert transport.closed


def test_fake_commit_cas_is_atomic_and_uses_copies():
    store = InMemoryLookupStore()
    documents = build(builder(), complete())
    checkpoint = Checkpoint("page-1", "test")
    assert store.commit(documents, expected_version=0, checkpoint=checkpoint) == 1
    documents[KEY]["reason"] = "mutated by test"
    assert store.documents[KEY]["reason"] != "mutated by test"
    with pytest.raises(VersionConflict):
        store.commit({}, expected_version=0, checkpoint=Checkpoint("page-2", "test"))
    assert store.checkpoint == checkpoint and store.version == 1


def test_fake_store_rejects_partial_ledger_candidate_regression():
    store = InMemoryLookupStore()
    value = builder([backup(), backup("other", group="OTHER")])
    store.commit(build(value), expected_version=0, checkpoint=Checkpoint("a", "test"))
    with pytest.raises(ValueError, match="candidate_ledger"):
        store.commit(build(builder()), expected_version=1, checkpoint=Checkpoint("b", "test"))
    assert store.version == 1 and store.documents[KEY]["candidate_count"] == 2


def test_backup_attribute_order_and_identical_duplicates_do_not_create_revisions():
    value = builder()
    item = backup()
    item["attributes"].reverse()
    item["attributes"].append(deepcopy(item["attributes"][0]))
    value.add_raw([raw([item])])
    document = build(value, complete())[KEY]
    assert document["verification_status"] == "verified"
    assert document["backup_reference_count"] == 1


def test_missing_action_attribute_is_quarantined_not_attributed_by_client():
    item = backup()
    item["attributes"] = item["attributes"][1:]
    value = builder([item])
    document = build(value, complete())[KEY]
    assert document["mapping_status"] == "unresolved"
    assert document["verification_status"] != "verified"
    assert value.issues[0].action_id is None


def test_backup_identity_missing_does_not_fall_back_to_save_or_client():
    item = backup()
    del item["id"]
    document = build(builder([item]), complete())[KEY]
    assert "backup_identity_missing" in document["reason"]
    assert document["verification_status"] == "blocked"


def test_empty_successful_poll_cannot_establish_api_coverage():
    value = builder([])
    document = build(value)[KEY]
    assert document["verification_status"] == "provisional"
    assert "coverage_unverified" in document["reason"]


@pytest.mark.parametrize("flag", ["jobs_complete", "backups_complete", "partial", "suffix_reviewed"])
def test_coverage_cannot_be_enabled_by_truthy_strings(flag):
    with pytest.raises(ValueError, match="boolean"):
        replace(complete(), **{flag: "false"})


def test_wrong_source_coverage_does_not_authorize_other_namespace():
    document = build(builder(), complete("chnl"))[KEY]
    assert document["verification_status"] == "provisional"
    assert document["policy_name"] is None


def test_month_boundary_uses_explicit_inputs_without_source_rerouting():
    start = datetime(2026, 10, 31, 23, 55, tzinfo=timezone.utc)
    end = start + timedelta(minutes=20)
    jobs = [job(start=start, end=end), job(ROOT, "workflow job", parent=0, root=0,
                start=start - timedelta(minutes=1), end=end + timedelta(minutes=1))]
    for hit in jobs:
        hit["_index"] = "nw-core-job-run-2026-10"
    original = deepcopy(jobs)
    item = backup()
    item["saveTime"], item["completionTime"] = (start + timedelta(minutes=1)).isoformat(), end.isoformat()
    value = builder([item], jobs=jobs)
    attestation = replace(complete(), start=start, end=end)
    document = build(value, attestation)[KEY]
    assert document["action_run_key"] == KEY
    assert document["verification_status"] == "verified"
    assert all(hit["_index"] == "nw-core-job-run-2026-10" for hit in jobs)
    assert jobs == original
    assert document["action_start_time"].startswith("2026-10-31")
    assert document["action_end_time"].startswith("2026-11-01")


def test_input_and_output_mutation_do_not_modify_collected_sources_or_ledger():
    hit = raw()
    before = deepcopy(hit)
    value = ActionLookupBuilder()
    value.add_raw([hit])
    value.add_jobs(hierarchy())
    document = build(value, complete())[KEY]
    document["candidate_evidence"]["items"][0]["attributes"][0]["values"][0] = "bad"
    assert hit == before
    assert build(value, complete())[KEY]["verification_status"] == "verified"


def test_conflicting_terminal_job_states_are_blocked():
    value = builder()
    value.add_jobs([job(status="failed")])
    document = build(value, complete())[KEY]
    assert document["verification_status"] == "blocked"
    assert "terminal_state_conflict" in document["reason"]


@pytest.mark.asyncio
async def test_transactional_read_batch_does_not_return_partial_results():
    transport = FakeReadTransport([page([raw() | {"sort": [1]}]), RuntimeError("page-2 failed")])
    adapter = ElasticsearchReadAdapter(transport)
    with pytest.raises(RuntimeError, match="page-2 failed"):
        await adapter.read_batch(["nw-ops-raw-core-2026-11"], {})
    assert transport.closed


def test_fake_store_rejects_stale_running_and_replaced_single_candidate():
    store = InMemoryLookupStore()
    documents = build(builder(), complete())
    store.commit(documents, expected_version=0, checkpoint=Checkpoint("a", "test"))
    stale = deepcopy(documents)
    stale[KEY]["action_status"] = "running"
    with pytest.raises(ValueError, match="terminal_state_regression"):
        store.commit(stale, expected_version=1, checkpoint=Checkpoint("b", "test"))
    replacement = build(builder([backup(policy="UNRELATED: 123")]), complete())
    with pytest.raises(ValueError, match="candidate_ledger"):
        store.commit(replacement, expected_version=1, checkpoint=Checkpoint("b", "test"))
    assert store.checkpoint.cursor == "a"


def test_fake_store_rejects_unverified_names_and_execution_replacement_atomically():
    store = InMemoryLookupStore()
    documents = build(builder(), complete())
    store.commit(documents, expected_version=0, checkpoint=Checkpoint("a", "test"))
    invalid = deepcopy(documents)
    invalid[KEY]["verification_status"] = "provisional"
    with pytest.raises(ValueError, match="unverified_public"):
        store.commit(invalid, expected_version=1, checkpoint=Checkpoint("b", "test"))
    replaced = deepcopy(documents)
    replaced[KEY]["action_start_time"] = (START + timedelta(days=1)).isoformat()
    with pytest.raises(ValueError, match="execution_replacement"):
        store.commit(replaced, expected_version=1, checkpoint=Checkpoint("b", "test"))
    assert store.version == 1


def test_fake_store_does_not_hide_candidate_removal_behind_an_increased_count():
    store = InMemoryLookupStore()
    store.commit(build(builder()), expected_version=0, checkpoint=Checkpoint("a", "test"))
    unrelated = builder([backup("other-1", group="OTHER_1"), backup("other-2", group="OTHER_2")])
    with pytest.raises(ValueError, match="candidate_ledger"):
        store.commit(build(unrelated), expected_version=1, checkpoint=Checkpoint("b", "test"))
    assert store.documents[KEY]["candidate_count"] == 1


@pytest.mark.parametrize("hit", [None, [], "not a document", {"_source": []}])
def test_non_document_raw_input_is_quarantined(hit):
    result = extract_raw(hit)
    assert not result.evidence and result.issues[0].reason == "invalid_source_document"


def test_fake_store_revokes_names_on_id_reuse_without_replacing_prior_execution():
    value = builder()
    store = InMemoryLookupStore()
    before = build(value, complete())
    store.commit(before, expected_version=0, checkpoint=Checkpoint("a", "test"))
    value.add_jobs([job(start=START + timedelta(days=1), end=END + timedelta(days=1),
                        observed=OBSERVED + timedelta(days=1))])
    conflicted = value.build(synced_at=SYNCED + timedelta(days=1), coverage=[complete()])
    store.commit(conflicted, expected_version=1, checkpoint=Checkpoint("b", "test"))
    document = store.documents[KEY]
    assert document["verification_status"] == "blocked"
    assert document["policy_name"] is None
    assert document["action_start_time"] == before[KEY]["action_start_time"]
    assert "conflicting_execution" in document["candidate_evidence"]

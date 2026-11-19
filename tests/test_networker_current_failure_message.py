"""Anonymous failed-Workflow display evidence through the real current path."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from apps.core.config import ElasticsearchConfig
from apps.processors.networker_current import build_daily
from apps.writers.elasticsearch import ElasticsearchWriter
from apps.writers.networker_current import NetworkerDailyCurrent
from test_networker_current import DAY, NOW, FakeES, build, config, hierarchy, hits, job, result, skip


def failed_hierarchy():
    jobs = hierarchy()
    for item in jobs:
        item.update(completionStatus="Failed", exitCode=1)
    jobs[0]["message"] = "Workflow failed."
    jobs[1]["message"] = "Backup action could not complete its targets."
    jobs[2]["message"] = "Unable to open synthetic save set: permission denied."
    return jobs


def test_no_children_or_only_successful_children_do_not_invent_a_failure_reason():
    for children in ([], hierarchy()[1:]):
        summary, rows = build([job(completionStatus="Failed", message="Workflow connection error.")] + children)[DAY]
        assert summary["failure_count"] == 1
        assert rows[0]["failure_message"] == "Check NMC"
        assert rows[0]["failure_message_job_id"] is None


def test_failed_save_is_preferred_to_later_action_message_without_changing_outcome():
    jobs = failed_hierarchy()
    summary, rows = build(jobs)[DAY]
    assert rows[0]["failure_message"] == jobs[2]["message"]
    assert rows[0]["failure_message_job_id"] == "7300"
    assert rows[0]["workflow_job_id"] == "7100"
    assert rows[0]["source_completion_status"] == "Failed"
    assert summary["failure_count"] == 1 and summary["success_count"] == 0


@pytest.mark.parametrize("message", [None, "", " \n ", "Failed", "FAILED.", "Job failed", "Status: Failed", "Error: Failed", "Failed\nStatus: Failed", 42])
def test_action_message_is_used_when_save_has_no_specific_message(message):
    jobs = failed_hierarchy()
    jobs[2]["message"] = message
    # Test malformed stored values as well as API-normalized messages.
    documents = hits(jobs)
    documents[2]["_source"]["payload"]["message"] = message
    _, rows = build_daily(documents, "core", [DAY], NOW, config())[DAY]
    assert rows[0]["failure_message"] == jobs[1]["message"]
    assert rows[0]["failure_message_job_id"] == "7200"
    documents[1]["_source"]["payload"]["message"] = "Failed"
    _, rows = build_daily(documents, "core", [DAY], NOW, config())[DAY]
    assert rows[0]["failure_message"] == "Check NMC"
    assert rows[0]["failure_message_job_id"] is None


@pytest.mark.parametrize("changes", [
    {"parentJobId": 9991}, {"rootParentJobId": 9992}, {"rootParentJobId": 0},
    {"startTime": "2026-11-11T11:00:00Z"}, {"endTime": "2026-11-11T12:21:00Z"},
    {"endTime": None}, {"type": "savefs job"},
])
def test_invalid_or_unsupported_save_relationship_does_not_supply_a_message(changes):
    jobs = failed_hierarchy()
    jobs[1]["message"] = "Failed"
    jobs[2].update(changes)
    _, rows = build(jobs)[DAY]
    assert rows[0]["failure_message"] == "Check NMC"


def test_source_and_parent_action_interval_are_required_for_save_evidence():
    documents = hits(failed_hierarchy())
    documents[1]["_source"]["payload"]["message"] = "Failed"
    documents[2]["_source"]["source_networker"] = "chnl"
    _, rows = build_daily(documents, "core", [DAY], NOW, config())[DAY]
    assert rows[0]["failure_message"] == "Check NMC"
    documents = hits(failed_hierarchy())
    copy = deepcopy(documents[1])
    copy["_index"] = "nw-unmapped-job-run-2026-11"
    copy["_source"]["payload"]["end_time"] = "2026-11-11T12:15:00Z"
    _, rows = build_daily(documents + [copy], "core", [DAY], NOW, config())[DAY]
    assert rows[0]["failure_message"] == "Check NMC"


def test_same_source_action_is_required_even_with_a_matching_save_root():
    jobs = failed_hierarchy()
    for changes in ({"rootParentJobId": 0}, {"parentJobId": 9991}):
        changed = deepcopy(jobs)
        changed[1].update(changes)
        _, rows = build(changed)[DAY]
        assert rows[0]["failure_message"] == "Check NMC"
    documents = hits(jobs)
    documents[1]["_source"]["source_networker"] = "chnl"
    _, rows = build_daily(documents, "core", [DAY], NOW, config())[DAY]
    assert rows[0]["failure_message"] == "Check NMC"


def test_latest_failed_save_then_job_id_selects_one_representative_stably():
    jobs = failed_hierarchy()
    jobs += [job(identity, type="save job", parentJobId=7200, rootParentJobId=7100,
                 startTime="2026-11-11T12:06:00Z", endTime="2026-11-11T12:11:00Z",
                 completionStatus="Failed", message=f"Synthetic target error from {identity}.")
             for identity in (7301, 7302)]
    documents = hits(jobs)
    duplicate = deepcopy(documents[-1])
    duplicate["_index"] = "nw-unmapped-job-run-2026-11"
    documents += [duplicate]
    original = deepcopy(documents)
    expected = build_daily(documents, "core", [DAY], NOW, config())
    assert expected == build_daily(list(reversed(documents)), "core", [DAY], NOW, config())
    assert documents == original
    row = expected[DAY][1][0]
    assert row["failure_message_job_id"] == "7302"
    assert row["failure_message"] == jobs[-1]["message"]


@pytest.mark.parametrize("outcome", ["Succeeded", "Failed"])
def test_previous_failed_execution_message_never_leaks_into_the_final_rerun(outcome):
    jobs = failed_hierarchy() + [job(7101, completionStatus=outcome,
        startTime="2026-11-11T13:00:00Z", endTime="2026-11-11T13:20:00Z")]
    summary, rows = build(jobs)[DAY]
    assert rows[0]["workflow_job_id"] == "7101"
    if outcome == "Succeeded":
        assert summary["failure_count"] == 0 and summary["success_count"] == 1
        assert "failure_message" not in rows[0] and "failure_message_job_id" not in rows[0]
    else:
        assert summary["failure_count"] == 1
        assert rows[0]["failure_message"] == "Check NMC"
        assert rows[0]["failure_message_job_id"] is None


def test_success_skip_running_and_ambiguous_final_selection_have_no_failure_message():
    scenarios = [[job()], [job(message=skip())],
                 [job(state="Active", completionStatus="", endTime=None)],
                 [job(completionStatus="Failed"), job(7101, completionStatus="Failed")]]
    for jobs in scenarios:
        _, rows = build(jobs)[DAY]
        assert rows[0]["dashboard_result"] != "failed"
        assert "failure_message" not in rows[0] and "failure_message_job_id" not in rows[0]
    jobs = failed_hierarchy()
    jobs[0]["completionStatus"] = "Succeeded"
    _, rows = build(jobs)[DAY]
    assert rows[0]["dashboard_result"] == "success" and "failure_message" not in rows[0]


def test_final_failed_rerun_uses_only_its_own_children_and_skip_cannot_replace_it():
    old = failed_hierarchy()
    new = failed_hierarchy()
    ids = {7100: 8100, 7200: 8200, 7300: 8300, 0: 0}
    for item in new:
        item["id"] = ids[item["id"]]
        item["parentJobId"] = ids[item["parentJobId"]]
        item["rootParentJobId"] = ids[item["rootParentJobId"]]
        item["startTime"] = item["startTime"].replace("T12:", "T13:")
        item["endTime"] = item["endTime"].replace("T12:", "T13:")
    new[2]["message"] = "Synthetic retry save: device unavailable."
    later_skip = job(9100, message=skip(), startTime="2026-11-11T14:00:00Z",
                     endTime="2026-11-11T14:01:00Z")
    summary, rows = build(old + new + [later_skip])[DAY]
    assert rows[0]["workflow_job_id"] == "8100"
    assert rows[0]["failure_message_job_id"] == "8300"
    assert rows[0]["failure_message"] == new[2]["message"]
    assert summary["failure_count"] == 1 and summary["success_count"] == 0


def test_message_changes_generation_but_never_success_failure_counts_or_duration():
    jobs = failed_hierarchy()
    before, rows_before = build(jobs)[DAY]
    jobs[2]["message"] = "Another synthetic save error: device unavailable."
    after, rows_after = build(jobs)[DAY]
    assert rows_before[0]["failure_message"] != rows_after[0]["failure_message"]
    assert before["generation"] != after["generation"]
    assert {k: v for k, v in before.items() if k != "generation"} == {
        k: v for k, v in after.items() if k != "generation"}


@pytest.mark.asyncio
async def test_existing_writer_publishes_message_with_no_extra_child_or_api_requests(monkeypatch):
    es = FakeES()
    writer = ElasticsearchWriter(ElasticsearchConfig(networker_current=config()))
    writer.client, writer._async_bulk = es, es.bulk
    # Fix current time without replacing aggregation, input reads or publication.
    from apps.writers import elasticsearch as writer_module
    class FixedClock:
        @staticmethod
        def now(tz):
            return NOW
    monkeypatch.setattr(writer_module, "datetime", FixedClock)
    collected = result(failed_hierarchy())
    await writer.write_many([collected])
    head = es.store[("nw-current-core", "daily:core:" + DAY)]
    row = es.store[("nw-current-core", head["detail_ids"][0])]
    assert row["failure_message"] == failed_hierarchy()[2]["message"]
    assert row["failure_message_job_id"] == "7300"
    assert row["generation"] == head["generation"] and head["failure_count"] == 1
    assert not [r for r in es.requests if r[0] == "mget" and "raw-" in r[1].get("index", "")]
    before = deepcopy(es.store)
    await writer.write_many([collected])
    assert es.store == before
    # A process restart keeps the same immutable generation and stable IDs.
    incoming = hits(failed_hierarchy())
    await NetworkerDailyCurrent(es, es.bulk, config()).publish("core", incoming, NOW, True)
    assert es.store == before


def test_display_message_mapping_does_not_index_large_message_text():
    template = json.loads(Path("config/elasticsearch_networker_current_index_template.example.json").read_text())
    properties = template["template"]["mappings"]["properties"]
    assert template["version"] == 6
    assert properties["failure_message"] == {"type": "text", "index": False}
    assert properties["failure_message_job_id"] == {"type": "keyword"}

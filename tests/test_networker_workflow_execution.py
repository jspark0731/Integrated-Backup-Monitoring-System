"""Retained Job evidence contracts only; no current, Lookup, cache or publisher."""

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from apps.core.config import ElasticsearchConfig
from apps.models import CollectionResult
from apps.parsers.networker_rest_parser import parse_networker_rest_payload
from apps.writers.elasticsearch import ElasticsearchWriter


def api_job(**changes):
    return {
        "id": 7100, "type": "workflow job", "name": "SYNTH_POLICY",
        "workflowName": "SYNTH_W", "state": "Completed",
        "completionStatus": "Succeeded", "exitCode": 0,
        "parentJobId": 0, "rootParentJobId": 0,
        "startTime": "2026-10-31T23:50:00+00:00",
        "endTime": "2026-11-01T00:10:00+00:00",
    } | changes


def skip_message(action="backup"):
    return (
        "Starting synthetic workflow.\n"
        f"Consider starting action '{action}', enabled 1, schedule action 'skip'\n"
        f"Request to run the enabled action '{action}' that has level configured as 'skip'.\n"
        f"Not running action '{action}' because either the schedule says to skip it today, "
        "or, the previously failed action was not restarted within the specified restart window.\n"
        "Synthetic workflow succeeded."
    )


def parse(jobs, source="core"):
    raw = {"jobs": {"jobs": jobs}}
    parsed = parse_networker_rest_payload(raw, server_name=f"networker_{source}", source_networker=source)
    return parsed | {"raw": raw}


def writer_actions(jobs, source="core"):
    result = CollectionResult(
        collector=f"networker_{source}", target_type="Networker", protocol="rest", ok=True,
        collected_at=datetime(2026, 11, 1, 0, 15, tzinfo=timezone.utc),
        payload=parse(jobs, source), collection_class="jobs",
    )
    writer = ElasticsearchWriter(ElasticsearchConfig(enabled=False))
    assert writer.client is None
    return writer._actions_for_result(result)


def test_explicit_backup_skip_preserves_source_success_and_only_action_scope():
    message = skip_message()
    record = parse([api_job(message=message, adhocJob=False)])["jobs"][0]
    assert record["message"] == message
    assert record["state_raw"] == "Completed"
    assert record["completion_status"] == "Succeeded"
    assert record["status_raw"] == "Succeeded"
    assert record["status"] == record["status_normalized"] == "success"
    assert record["exit_code"] == 0 and record["adhoc_job"] is False
    assert record["execution_evidence"] == {
        "requested_actions": ["backup"], "skipped_actions": ["backup"],
        "scope": "message_declared_actions_only",
    }
    # A message is not an attestation of every target in this Workflow execution.
    assert "execution_disposition" not in record
    assert "skip_confirmed" not in record
    assert "all_declared_actions_skipped" not in record["execution_evidence"]


@pytest.mark.parametrize("message", [
    "skip this informational note", "schedule action 'skip'",
    "Not running action 'backup'",
    "Request to run the enabled action 'backup' that has level configured as 'skip'.",
    "Request to run the enabled action 'backup' that has level configured as 'full'. "
    "Not running action 'backup' because either the schedule says to skip it today.",
    "Request to run the enabled action 'backup' that has level configured as 'skip'. "
    "Not running action 'other' because either the schedule says to skip it today.",
])
def test_keywords_partial_messages_and_different_action_names_do_not_prove_skip(message):
    record = parse([api_job(message=message)])["jobs"][0]
    assert record["execution_evidence"]["skipped_actions"] == []
    assert record["status_normalized"] == "success"


def test_backup_skip_with_another_requested_action_is_not_a_whole_workflow_skip():
    message = skip_message() + "\nRequest to run the enabled action 'other' that has level configured as 'full'."
    record = parse([api_job(message=message)])["jobs"][0]
    assert record["execution_evidence"]["requested_actions"] == ["backup", "other"]
    assert record["execution_evidence"]["skipped_actions"] == ["backup"]
    assert record["status_normalized"] == "success"
    assert "execution_disposition" not in record


@pytest.mark.parametrize("value", [None, 123, {"message": "not a string"}])
def test_malformed_or_missing_message_is_not_stringified_into_evidence(value):
    record = parse([api_job(message=value)])["jobs"][0]
    assert record["message"] == ""
    assert record["execution_evidence"]["skipped_actions"] == []


@pytest.mark.parametrize("value,expected", [(True, True), (False, False), (None, None), (0, None), (1, None), ("true", None)])
def test_adhoc_is_nullable_boolean_without_inference_from_root_zero(value, expected):
    record = parse([api_job(adhocJob=value)])["jobs"][0]
    assert record["adhoc_job"] is expected
    assert "execution_disposition" not in record


@pytest.mark.parametrize("suffix", ["_D", "_W"])
def test_suffix_empty_level_and_missing_size_do_not_invent_execution_or_skip(suffix):
    record = parse([api_job(workflowName="SYNTH" + suffix, level="", dataSize=None)])["jobs"][0]
    assert record["workflow_name"] == "SYNTH" + suffix
    assert record["message"] == "" and record["adhoc_job"] is None
    assert record["execution_evidence"]["skipped_actions"] == []
    assert "execution_disposition" not in record


@pytest.mark.parametrize("state", ["SessionActive", "Started"])
def test_active_direct_save_without_end_is_running_without_workflow_affiliation(state):
    job = api_job(type="save job", name="save", state=state, adhocJob=True)
    for key in ("endTime", "completionStatus", "exitCode"):
        job.pop(key)
    record = next(a["_source"] for a in writer_actions([job]) if a["_source"]["document_type"] == "job-run")
    assert record["payload"]["status_normalized"] == "running"
    assert record["payload"]["state_raw"] == state
    assert record["payload"]["adhoc_job"] is True
    assert record["payload"]["end_time"] is None
    assert record["payload"]["workflow_run_key"] == ""


@pytest.mark.parametrize("completion,code,expected", [("Succeeded", 0, "success"), ("Failed", 1, "failed"), ("Canceled", 1, "canceled")])
def test_terminal_completion_remains_authoritative_over_active_state(completion, code, expected):
    record = parse([api_job(state="SessionActive", completionStatus=completion, exitCode=code)])["jobs"][0]
    assert record["status_normalized"] == expected
    assert record["completion_status"] == completion


def test_missing_end_alone_does_not_prove_running():
    job = api_job(state="Unrecognized")
    for key in ("endTime", "completionStatus", "exitCode"):
        job.pop(key)
    assert parse([job])["jobs"][0]["status_normalized"] == "unknown"


def test_message_and_evidence_reach_job_run_without_changing_identity_or_start_month():
    job = api_job(message=skip_message(), adhocJob=False)
    first = writer_actions([job])
    second = writer_actions([job])
    run = next(a for a in first if a["_source"]["document_type"] == "job-run")
    repeated = next(a for a in second if a["_source"]["document_type"] == "job-run")
    raw = next(a for a in first if a["_source"]["document_family"] == "raw")
    assert run["_id"] == repeated["_id"] == "networker_core:job:7100"
    assert run["_index"] == repeated["_index"] == "nw-core-job-run-2026-10"
    assert run["_op_type"] == "index"
    assert run["_source"]["payload"]["outcome"] == "success"
    assert run["_source"]["payload"]["message"] == job["message"]
    assert run["_source"]["payload"]["execution_evidence"]["skipped_actions"] == ["backup"]
    assert raw["_source"]["payload"]["raw"]["jobs"]["jobs"][0] == job
    assert run["_source"]["source_raw_id"] == raw["_id"]


def test_failed_workflow_without_backup_evidence_keeps_its_source_failure():
    parsed = parse([api_job(completionStatus="Failed", exitCode=1)])
    assert parsed["jobs"][0]["status_normalized"] == "failed"
    assert parsed["jobs"][0]["execution_evidence"]["skipped_actions"] == []
    assert parsed["summary"]["job_failed_count_by_policy"] == {"unknown": 1}


def test_minimal_mapping_contains_only_retained_evidence_fields():
    template = json.loads(Path("config/elasticsearch_networker_job_run_index_template.example.json").read_text(encoding="utf-8"))
    props = template["template"]["mappings"]["properties"]["payload"]["properties"]
    assert template["version"] == 6
    assert props["adhoc_job"] == {"type": "boolean"}
    assert props["message"] == {"type": "text", "index": False}
    assert props["execution_evidence"] == {"type": "object", "enabled": False}
    assert props["exit_code"] == {"type": "long"}
    for name in ("execution_disposition", "action_scope_status", "relationship_evidence", "last_observed_at"):
        assert name not in props

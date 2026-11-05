"""Synthetic list-API records only: no NetWorker/Elasticsearch connection."""

from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from apps.core.config import ElasticsearchConfig
from apps.models import CollectionResult
from apps.parsers.networker_rest_parser import parse_networker_rest_payload
from apps.processors.derived import build_derived_documents
from apps.writers.elasticsearch import ElasticsearchWriter


def api_job(**overrides):
    return {
        "id": 3234696,
        "type": "save job",
        "name": "not-a-policy",
        "state": "Completed",
        "exitCode": 0,
        "startTime": "2026-10-31T23:50:00Z",
        "endTime": "2026-11-01T00:10:00Z",
        "parentJobId": 3231071,
        "rootParentJobId": 3231062,
        "previousJobId": 3234000,
        "runOnHost": "synthetic-client",
        "command": "save -g FULL_GROUP",
    } | overrides


def parse(jobs, source="core"):
    return parse_networker_rest_payload(
        {"jobs": {"jobs": jobs}},
        server_name=f"networker_{source}",
        source_networker=source,
    )


def actions(jobs, source="core", collected_at=None):
    result = CollectionResult(
        collector=f"networker_{source}", target_type="Networker", protocol="rest",
        collected_at=collected_at or datetime(2026, 11, 1, 0, 15, tzinfo=timezone.utc),
        ok=True, payload=parse(jobs, source), collection_class="jobs",
    )
    writer = ElasticsearchWriter(ElasticsearchConfig(enabled=False))
    assert writer.client is None
    return writer._actions_for_result(result)


def runs(jobs, source="core", collected_at=None):
    return [a for a in actions(jobs, source, collected_at)
            if a["_source"].get("document_type") == "job-run"]


@pytest.mark.parametrize("command,expected", [
    ("save -g FULL_GROUP -s server", "FULL_GROUP"),
    ('save\t-g\t"GROUP WITH SPACE" -s server', "GROUP WITH SPACE"),
    ("save -g 'GROUP WITH SPACE'", "GROUP WITH SPACE"),
    (r'"C:\Program Files\NetWorker\nsr\bin\save.exe" -g "GROUP WITH SPACE"', "GROUP WITH SPACE"),
    (r'save -g "GROUP\NAME"', r"GROUP\NAME"),
    (r'save -g "GROUP \"QUOTED\""', 'GROUP "QUOTED"'),
    (r'save -g GROUP\ WITH\ SPACE', "GROUP WITH SPACE"),
    ("save -g 'GROUP#1'", "GROUP#1"),
    (None, ""), ("", ""), (123, ""),
    ("save -s server", ""),
    ('save --note "-g WRONG"', ""),
    ("save -group WRONG", ""),
    ("save -g", ""), ("save -g -s server", ""), ("save -g ''", ""),
    ('save -g "BROKEN', ""),
    ('save -g VALID --note "BROKEN', ""),
    ("save -g FIRST -g SECOND", ""),
])
def test_protection_group_quotes_missing_and_malformed_commands(command, expected):
    record = parse([api_job(command=command)])["jobs"][0]
    assert record["protection_group"] == expected
    assert record["policy_name"] == record["workflow_name"] == ""
    assert "command" not in record


@pytest.mark.parametrize("exit_code,completion,status", [
    (0, "Succeeded", "success"),
    (4294967295, "Failed", "failed"),
    (-1, "Failed", "failed"),
    (1, "Canceled", "canceled"),
])
def test_save_job_retains_status_exit_ids_and_times(exit_code, completion, status):
    record = runs([api_job(exitCode=exit_code, completionStatus=completion)])[0]["_source"]
    payload = record["payload"]
    assert payload["status_normalized"] == status
    assert payload["exit_code"] == record["job"]["exit_code"] == exit_code
    assert payload["workflow_run_key"] == "core:3231062"
    assert payload["protection_group"] == "FULL_GROUP"
    assert payload["job_id"] == "3234696"
    assert payload["type"] == "save job"
    assert payload["parent_job_id"] == record["job"]["parent_job_id"] == 3231071
    assert payload["root_parent_job_id"] == 3231062
    assert payload["previous_job_id"] == 3234000
    assert payload["duration_seconds"] == record["job"]["duration_seconds"] == 1200
    assert payload["client_name"] == "synthetic-client"
    assert "command" not in payload and "workflow_run_key" not in record


def test_all_four_hierarchy_members_share_root_key_without_join():
    jobs = [
        api_job(id=3231062, type="workflow job", parentJobId=0, rootParentJobId=0),
        api_job(id=3231071, type="backup action job", parentJobId=3231062),
        api_job(), api_job(id=3234695),
    ]
    documents = runs(jobs)
    assert len(documents) == 4
    assert {a["_source"]["payload"]["workflow_run_key"] for a in documents} == {"core:3231062"}
    assert len({a["_id"] for a in documents}) == 4
    for action in documents[:2]:
        record = action["_source"]
        assert record["payload"]["client_name"] == ""
        assert record["client"]["hostname"] == ""
        assert record["security_domain"] == "core"
        assert record["payload"]["protection_group"] == ""
        assert record["run_on_host"] == "synthetic-client"


@pytest.mark.parametrize("job_type", ["workflow job", "backup action job", "clone action job", "backup", ""])
def test_unverified_hosts_are_not_inferred_as_clients(job_type):
    record = parse([api_job(type=job_type)])["jobs"][0]
    assert record["client_name"] == record["client_hostname"] == ""
    assert record["run_on_host"] == "synthetic-client"
    assert record["protection_group"] == ""


def test_explicit_api_names_preserved_but_no_alias_or_name_inference():
    explicit = parse([api_job(type="workflow job", policyName="POLICY:EXPLICIT",
                             workflowName="WF", clientName="explicit-client")])["jobs"][0]
    assert explicit["policy_name"] == "POLICY:EXPLICIT"
    assert explicit["workflow_name"] == "WF"
    assert explicit["client_name"] == "explicit-client"
    absent = parse([api_job(policy="inferred-policy", workflow="inferred-wf")])["jobs"][0]
    assert absent["policy_name"] == absent["workflow_name"] == ""


@pytest.mark.parametrize("root", [0, "0", "000", None, "", " ", -1, "-1", True,
                                  1.5, "1.5", "not-an-id", [], {}, "１２３"])
def test_invalid_root_is_preserved_and_never_replaced_with_parent(root):
    record = runs([api_job(rootParentJobId=root)])[0]["_source"]["payload"]
    assert record["workflow_run_key"] == ""
    assert record["root_parent_job_id"] == root
    assert record["parent_job_id"] == 3231071


def test_parent_zero_does_not_prevent_valid_explicit_root_key():
    record = runs([api_job(parentJobId=0)])[0]["_source"]["payload"]
    assert record["parent_job_id"] == 0
    assert record["workflow_run_key"] == "core:3231062"


@pytest.mark.parametrize("job_type", ["clone action job", "backup", "", "action job"])
def test_other_types_have_no_workflow_key(job_type):
    assert runs([api_job(type=job_type)])[0]["_source"]["payload"]["workflow_run_key"] == ""


def test_missing_source_does_not_fall_back_to_collector_or_domain():
    payload = parse([api_job()])
    payload["jobs"][0]["source_networker"] = ""
    docs = build_derived_documents({"@timestamp": "2026-11-01T01:00:00Z",
                                   "collector": "networker_core", "target_type": "Networker",
                                   "payload": payload})
    record = next(d["payload"] for d in docs if d["document_type"] == "job-run")
    assert record["workflow_run_key"] == ""


def test_repeat_collection_cross_source_and_month_boundary_keep_existing_routing():
    first = runs([api_job()])[0]
    updated = runs([api_job(exitCode=4294967295, completionStatus="Failed")],
                   collected_at=datetime(2026, 11, 1, 0, 20, tzinfo=timezone.utc))[0]
    other = runs([api_job()], source="chnl")[0]
    assert first["_id"] == updated["_id"] == "networker_core:job:3234696"
    assert first["_index"] == updated["_index"] == "nw-core-job-run-2026-10"
    assert other["_id"] == "networker_chnl:job:3234696"
    assert other["_source"]["payload"]["workflow_run_key"] == "chnl:3231062"
    assert first["_source"]["payload"]["workflow_run_key"] == "core:3231062"
    assert first["_source"]["index_month"] == "2026-10"
    assert first["_source"]["@timestamp"] == "2026-11-01T00:10:00+00:00"
    running = runs([api_job(state="Running", endTime=None)])[0]
    assert running["_id"] == first["_id"]
    assert running["_index"] == first["_index"]
    assert running["_source"]["@timestamp"] == "2026-10-31T23:50:00+00:00"


@pytest.mark.parametrize("old_domain", ["unmapped", "chnl", "info"])
def test_reclassification_keeps_id_but_old_index_document_can_coexist(old_domain):
    payload = parse([api_job()])
    payload["jobs"][0]["security_domain"] = old_domain
    result = CollectionResult(
        collector="networker_core", target_type="Networker", protocol="rest",
        collected_at=datetime(2026, 11, 1, 0, 15, tzinfo=timezone.utc),
        ok=True, payload=payload, collection_class="jobs",
    )
    writer = ElasticsearchWriter(ElasticsearchConfig(enabled=False))
    old = next(a for a in writer._actions_for_result(result)
               if a["_source"].get("document_type") == "job-run")
    new = runs([api_job()])[0]
    assert old["_index"] == f"nw-{old_domain}-job-run-2026-10"
    assert new["_index"] == "nw-core-job-run-2026-10"
    assert old["_id"] == new["_id"] == "networker_core:job:3234696"
    # Elasticsearch identity includes the index: indexing the new record cannot
    # replace the old document in a different domain index.
    stored = {(old["_index"], old["_id"]): old}
    stored[(new["_index"], new["_id"])] = new
    assert len(stored) == 2
    assert old["_op_type"] == new["_op_type"] == "index"


def test_version_five_put_body_matches_template_and_preserves_step_one_contract():
    root = Path(__file__).resolve().parents[1]
    template = json.loads((root / "config/elasticsearch_networker_job_run_index_template.example.json").read_text(encoding="utf-8"))
    request = (root / "docs/networker_job_mapping_step2_template_put.http").read_text(encoding="utf-8")
    lines = [line for line in request.splitlines() if not line.startswith("#")]
    assert lines[0] == "PUT _index_template/nw-job-run-template"
    assert json.loads("\n".join(lines[1:])) == template
    assert template["version"] == 5
    assert template["priority"] == 400
    assert template["composed_of"] == ["nw-common-mappings"]
    assert template["template"]["mappings"]["dynamic"] is False
    props = template["template"]["mappings"]["properties"]
    for field in ("protection_group", "workflow_run_key"):
        assert props["payload"]["properties"][field] == {"type": "keyword"}
    for field in ("job", "payload"):
        assert props[field]["properties"]["exit_code"] == {"type": "long"}

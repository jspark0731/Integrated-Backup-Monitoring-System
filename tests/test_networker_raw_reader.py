"""RAW payload.enabled=false regression: synthetic PIT scans only."""

from copy import deepcopy
from datetime import timedelta

import pytest

from apps.lookups.networker_action.adapters import IncompleteRead
from apps.lookups.networker_action_shadow.analysis import analyse, execute
from apps.lookups.networker_action_shadow.raw_source import select_raw_sources
from apps.lookups.networker_action_shadow.reader import PITReader, range_query
from test_networker_action_shadow import (
    FakeHTTP, START, END, JOB, RAW, hierarchy, job, raw, no_network,
)


class DisabledPayloadHTTP(FakeHTTP):
    raw_mapping = {"properties": {"@timestamp": {"type": "date"},
        "collection_class": {"type": "keyword"}, "payload": {"type": "object", "enabled": False}}}

    async def request(self, method, path, **kwargs):
        if path == f"/{RAW}/_field_caps":
            self.calls.append((method, path, deepcopy(kwargs)))
            result = {}
            for field in kwargs["params"]["fields"].split(","):
                mapped = self.raw_mapping["properties"].get(field)
                result[field] = {mapped["type"]: {"searchable": True}} if mapped and mapped.get("enabled") is not False else {"unmapped": {"searchable": False}}
            return {"fields": result}
        if path == "/_search" and "@timestamp" in str(kwargs["json"]["query"]):
            filters = kwargs["json"]["query"]["bool"]["filter"]
            # A real disabled payload cannot satisfy a source term/match query.
            assert len(filters) == 2
            assert filters[1] == {"term": {"collection_class": "backups"}}
            assert "source_networker" not in str(filters) and "payload" not in str(filters)
            pit = kwargs["json"]["pit"]["id"]
            self.pits[pit] = [h for h in self.pits[pit] if h["_source"].get("collection_class") == "backups"]
        return await super().request(method, path, **kwargs)


async def dry(http, page_size=3):
    return await execute(http, source="core", job_indices=[JOB], raw_indices=[RAW],
        job_start=START, job_end=END, raw_start=START, raw_end=END, page_size=page_size)


def nested_only(**kwargs):
    hit = raw(**kwargs)
    hit["_source"].pop("source_networker")
    return hit


@pytest.mark.asyncio
async def test_disabled_payload_41_raw_documents_selects_10_backups():
    records = [nested_only(identity=f"raw-{n}", backup_id=f"backup-{n}") for n in range(10)]
    # More than one Backup per RAW, and one repeated Backup across polls.
    first = records[0]["_source"]["payload"]["raw"]["backups"]["backups"][0]
    records[0]["_source"]["payload"]["raw"]["backups"]["backups"].append(dict(deepcopy(first), id="extra-backup"))
    records[1]["_source"]["payload"]["raw"]["backups"]["backups"].append(deepcopy(first))
    other = []
    for n in range(31):
        item = nested_only(identity=f"non-backups-{n}")
        item["_source"]["collection_class"] = "jobs" if n < 30 else "inventory"
        other.append(item)
    http = DisabledPayloadHTTP(raws=records + other)
    original = deepcopy(http.inputs)
    report = await dry(http)
    assert report["raw_documents"] == report["raw_hits_total"] == 10
    assert report["raw_source_accepted"] == 10 and report["raw_source_rejected"] == 0
    assert report["raw_evidence"]["backup_items"] == 12
    assert report["raw_evidence"]["unique_backup_identities"] == 11
    audit = report["reads"]["raw"]
    assert audit["hits"] == audit["expected_total"] == 10 and audit["pages"] == 5
    assert audit["complete"] and audit["terminal_empty"] and audit["pit_closed"]
    assert not report["raw_evidence"]["api_coverage_proven"]
    assert report["mapping_status"]["resolved"] == 1 and report["verification_status"]["verified"] == 0
    assert report["hierarchy"]["save_to_action_linked"] == report["hierarchy"]["action_to_workflow_linked"] == 1
    assert http.inputs == original
    assert all(method == "GET" or (method == "POST" and (path.endswith("/_pit") or path == "/_search")) or
               (method == "DELETE" and path == "/_pit") for method, path, _ in http.calls)
    caps = next(c for c in http.calls if c[1] == f"/{RAW}/_field_caps")
    assert caps[2]["params"]["fields"] == "@timestamp,collection_class"


@pytest.mark.parametrize("defect,reason", [
    ("missing", "missing"), ("null", "missing"), ("empty", "empty"), ("blank", "empty"),
    ("number", "type_error"), ("list", "type_error"), ("summary_type", "type_error"),
    ("payload_type", "type_error"), ("mismatch", "mismatch"), ("invalid", "invalid"),
    ("conflict", "conflict"), ("top_type", "type_error"),
])
def test_source_rejection_is_aggregate_only_and_not_inferred(defect, reason):
    hit = nested_only()
    summary = hit["_source"]["payload"]["summary"]
    changes = {"null": None, "empty": "", "blank": "  ", "number": 4, "list": ["core"],
               "mismatch": "chnl", "invalid": "private-source-sentinel"}
    if defect in changes:
        summary["source_networker"] = changes[defect]
    elif defect == "missing":
        summary.pop("source_networker")
        hit["_source"]["source_networker"] = "core"  # Cannot replace required summary.
    elif defect == "summary_type":
        hit["_source"]["payload"]["summary"] = []
    elif defect == "payload_type":
        hit["_source"]["payload"] = []
    elif defect == "conflict":
        hit["_source"]["source_networker"] = "chnl"
    elif defect == "top_type":
        hit["_source"]["source_networker"] = 5
    original = deepcopy(hit)
    accepted, audit = select_raw_sources([hit], "core")
    assert accepted == [] and hit == original
    assert audit["raw_source_rejected"] == 1 and audit["raw_source_accepted"] == 0
    assert audit["raw_source_rejection_reasons"][reason] == 1
    assert "private-source-sentinel" not in str(audit)
    built = analyse([hit], hierarchy(), "core", synced_at=END)
    assert built["report"]["raw_documents"] == 1
    assert built["report"]["raw_evidence"]["backup_items"] == 0
    assert not built["report"]["raw_source_validation_passed"]
    assert "raw_source_validation_failed" in built["report"]["incomplete_reasons"]
    assert built["documents"]["core:8100"]["verification_status"] != "verified"
    assert built["documents"]["core:8100"]["policy_name"] is None


@pytest.mark.asyncio
async def test_mixed_sources_do_not_change_pit_total_or_cursor_integrity():
    records = [nested_only(identity=f"raw-{n}", backup_id=f"backup-{n}") for n in range(8)]
    records[1]["_source"]["payload"]["summary"]["source_networker"] = "chnl"
    records[3]["_source"]["payload"]["summary"].clear()
    records[5]["_source"]["payload"]["summary"]["source_networker"] = 5
    records[6]["_source"]["payload"]["summary"]["source_networker"] = ""
    report = await dry(DisabledPayloadHTTP(raws=records), page_size=2)
    assert report["raw_hits_total"] == report["raw_documents"] == 8
    assert report["raw_source_accepted"] == report["raw_source_rejected"] == 4
    assert report["raw_source_missing"] == report["raw_source_mismatch"] == report["raw_source_type_error"] == report["raw_source_empty"] == 1
    assert report["reads"]["raw"]["complete"] and report["reads"]["raw"]["expected_total"] == 8
    assert report["reads"]["raw"]["hits"] == 8 and report["reads"]["raw"]["pages"] == 5
    assert report["raw_evidence"]["unique_backup_identities"] == report["raw_evidence"]["backup_items"] == 4
    assert report["incomplete"] and not report["raw_source_validation_passed"]


@pytest.mark.parametrize("defect", ["text_class", "unsearchable_class", "missing_time"])
@pytest.mark.asyncio
async def test_raw_preflight_still_requires_searchable_metadata(defect):
    http = DisabledPayloadHTTP()
    http.raw_mapping = deepcopy(http.raw_mapping)
    if defect == "text_class":
        http.raw_mapping["properties"]["collection_class"]["type"] = "text"
    elif defect == "missing_time":
        http.raw_mapping["properties"].pop("@timestamp")
    original = http.request
    async def request(method, path, **kwargs):
        response = await original(method, path, **kwargs)
        if defect == "unsearchable_class" and path.endswith("/_field_caps"):
            response["fields"]["collection_class"]["keyword"]["searchable"] = False
        return response
    http.request = request
    with pytest.raises(IncompleteRead, match="mapping"):
        await PITReader(http).preflight([RAW], "raw")


@pytest.mark.parametrize("shape", ["nested", "list", "partial", "failed", "invalid_item"])
@pytest.mark.asyncio
async def test_original_backup_extraction_and_failure_rules_are_reused(shape):
    hit = nested_only()
    if shape == "list":
        hit["_source"]["payload"]["raw"]["backups"] = hit["_source"]["payload"]["raw"]["backups"]["backups"]
    elif shape == "partial":
        hit["_source"]["payload"]["collection_status"] = "partial"
    elif shape == "failed":
        hit["_source"]["ok"] = False
        hit["_source"]["payload"]["collection_status"] = "error"
        hit["_source"]["payload"]["raw"] = None
    elif shape == "invalid_item":
        hit["_source"]["payload"]["raw"]["backups"]["backups"] = [None]
    report = await dry(DisabledPayloadHTTP(raws=[hit]))
    assert report["raw_source_accepted"] == 1 and report["verification_status"]["verified"] == 0
    assert report["raw_evidence"]["backup_items"] == (0 if shape == "failed" else 1)
    assert report["raw_evidence"]["unique_backup_identities"] == (0 if shape in {"failed", "invalid_item"} else 1)
    if shape in {"partial", "failed", "invalid_item"}:
        assert report["raw_evidence"]["quarantined_issues"] > 0 and report["incomplete"]


@pytest.mark.asyncio
async def test_425_baseline_and_61_5_relationships_with_10_raw_records():
    jobs = [job(str(8000 + i), "workflow job", "0", "0") for i in range(9)]
    jobs += [job(str(8100 + i), root=str(8000 + i), parent=str(8000 + i)) for i in range(5)]
    jobs += [job(str(10000 + i), "save job", str(8100 + i % 5), str(8000 + i % 5)) for i in range(61)]
    jobs += [job(str(11000 + i), "save job", "9900", "9990") for i in range(338)]
    jobs += [job(str(20000 + i), "savefs job") for i in range(5)]
    jobs += [job(str(21000 + i), "recover pipe to save job") for i in range(7)]
    jobs[-1]["_source"]["observed_at"] = (END - timedelta(seconds=1)).isoformat()
    raws = [nested_only(identity=f"raw-{i}", backup_id=f"backup-{i}") for i in range(10)]
    report = await dry(DisabledPayloadHTTP(jobs=jobs, raws=raws), page_size=30)
    assert report["job_run_documents"] == 425 and report["baseline_comparison"]["total_matches"]
    assert all(v["matches"] for v in report["baseline_comparison"]["types"].values())
    assert report["hierarchy"]["save_to_action_linked"] == 61
    assert report["hierarchy"]["action_to_workflow_linked"] == 5
    assert report["raw_hits_total"] == report["raw_source_accepted"] == 10
    assert report["raw_source_rejected"] == report["conflict_documents"] == 0
    assert report["verification_status"]["verified"] == 0


@pytest.mark.asyncio
async def test_rejected_raw_provenance_cannot_be_staged_or_published():
    from types import SimpleNamespace
    def denied(*args, **kwargs):
        pytest.fail("rejected_input_must_not_reach_ledger_or_publisher")
    state = SimpleNamespace(pending=lambda: None, stage=denied)
    publisher = SimpleNamespace(prepare=denied)
    http = DisabledPayloadHTTP(raws=[nested_only(source="chnl")])
    with pytest.raises(ValueError, match="raw_source_validation_failed"):
        await execute(http, source="core", job_indices=[JOB], raw_indices=[RAW], job_start=START,
            job_end=END, raw_start=START, raw_end=END, state=state, publisher=publisher)
    assert all(method != "PUT" for method, _, _ in http.calls)

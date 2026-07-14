from datetime import datetime, timezone

from app.core.config import ElasticsearchConfig
from app.models import CollectionResult
from app.processors.derived import build_derived_documents
from app.writers.elasticsearch import ElasticsearchWriter


def test_result_writes_raw_and_current_documents() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    result = CollectionResult(
        collector="DXi_1",
        target_type="DXi",
        protocol="cli_snmp",
        collected_at=datetime(2026, 6, 29, tzinfo=timezone.utc),
        ok=True,
        payload={"summary": {"device_name": "DXi_1"}},
    )

    actions = writer._actions_for_result(result)

    assert len(actions) == 2
    assert actions[0]["_index"] == "VTL-DXI_1-2026-06-29-1"
    assert actions[0]["_source"]["processing_mode"] == "elt"
    assert actions[0]["_source"]["document_family"] == "raw"
    assert actions[0]["_source"]["raw_document_id"] == actions[0]["_id"]
    assert actions[1]["_index"] == "VTL-DXI_1-2026-06-29-1"
    assert actions[1]["_id"] == "DXi_1:current"
    assert actions[1]["_source"]["current_document_id"] == "DXi_1:current"
    assert actions[1]["_source"]["processing_mode"] == "etl"
    assert actions[1]["_source"]["summary"]["device_name"] == "DXi_1"


def test_raw_document_ids_keep_collection_history() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    result = CollectionResult(
        collector="DD4500",
        target_type="DD",
        protocol="snmp",
        collected_at=datetime(2026, 6, 29, 12, 30, 1, tzinfo=timezone.utc),
        ok=True,
    )

    actions = writer._actions_for_result(result)

    assert actions[0]["_id"].startswith("DD4500:raw:20260629T123001.")
    assert actions[0]["_index"] == "VTL-DD4500-2026-06-29-1"
    assert actions[1]["_index"] == "VTL-DD4500-2026-06-29-1"


def test_index_names_follow_target_specific_design() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    collected_at = datetime(2026, 6, 29, tzinfo=timezone.utc)
    cases = [
        ("DXi_2", "DXi", "cli_snmp", "VTL-DXI_2-2026-06-29-1"),
        ("DD6900_1", "DD", "snmp", "VTL-DD6900_1-2026-06-29-1"),
        ("DD6900_2", "DD", "snmp", "VTL-DD6900_2-2026-06-29-1"),
        ("i6000_core_rest", "i6000", "rest", "PTL-CORE-2026-06-29-1"),
        ("i6000_chnl_rest", "i6000", "rest", "PTL-CHNL-2026-06-29-1"),
        ("i6000_info_rest", "i6000", "rest", "PTL-INFO-2026-06-29-1"),
        ("i6000_ifrs_rest", "i6000", "rest", "PTL-IFRS-2026-06-29-1"),
        ("ZFS_1", "ZFS", "rest", "ZFS-1-2026-06-29-1"),
        ("ZFS_4", "ZFS", "rest", "ZFS-4-2026-06-29-1"),
        ("networker_core", "Networker", "rest", "NW-CORE-2026-06-29-1"),
        ("networker_chnl", "Networker", "rest", "NW-CHNL-2026-06-29-1"),
        ("networker_info", "Networker", "rest", "NW-INFO-2026-06-29-1"),
        ("networker_ifrs", "Networker", "rest", "NW-IFRS-2026-06-29-1"),
    ]

    for collector, target_type, protocol, expected_index in cases:
        result = CollectionResult(
            collector=collector,
            target_type=target_type,
            protocol=protocol,
            collected_at=collected_at,
            ok=True,
        )

        assert writer._index_name(result) == expected_index


def test_networker_raw_document_can_be_transformed_to_derived_documents() -> None:
    raw_document = {
        "_id": "networker_core:raw:20260629T000000.000000Z",
        "@timestamp": "2026-06-29T00:00:00+00:00",
        "collector": "networker_core",
        "target_type": "Networker",
        "solution": "networker",
        "protocol": "rest",
        "payload": {
            "jobs": [{"job_id": 1}],
            "clients": [{"client_name": "client01"}],
            "policies": [{"policy_name": "Bronze"}],
            "workflows": [{"workflow_name": "Filesystem"}],
            "monthly_report": [{"policy_name": "Bronze", "month": "2026-06"}],
        },
    }

    documents = build_derived_documents(raw_document)

    assert [document["document_type"] for document in documents] == [
        "job",
        "client",
        "policy",
        "workflow",
        "monthly-report",
    ]
    assert documents[0]["processing_mode"] == "elt"
    assert documents[0]["derived_id"] == "networker_core:job:1:2026-06"


def test_zfs_raw_document_can_be_transformed_to_derived_documents() -> None:
    raw_document = {
        "_id": "ZFS_1:raw:20260629T000000.000000Z",
        "@timestamp": "2026-06-29T00:00:00+00:00",
        "collector": "ZFS_1",
        "target_type": "ZFS",
        "solution": "zfs",
        "protocol": "rest",
        "payload": {
            "summary": {"device_name": "zfs-prod-1"},
            "pools": [{"name": "p1"}],
            "alerts": [{"summary": "Disk fault"}],
        },
    }

    documents = build_derived_documents(raw_document)

    assert [document["document_type"] for document in documents] == ["summary", "pool", "event"]
    assert documents[1]["derived_id"] == "ZFS_1:pool:p1:2026-06"

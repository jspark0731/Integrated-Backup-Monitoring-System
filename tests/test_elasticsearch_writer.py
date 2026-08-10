from datetime import datetime, timezone

from apps.core.config import ElasticsearchConfig
from apps.models import CollectionResult
from apps.processors.derived import build_derived_documents
from apps.writers.elasticsearch import ElasticsearchWriter


def test_result_writes_raw_and_current_documents() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    result = CollectionResult(
        collector="DXi_1",
        target_type="DXi",
        protocol="cli",
        collected_at=datetime(2026, 6, 29, tzinfo=timezone.utc),
        ok=True,
        payload={"summary": {"device_name": "DXi_1"}},
    )

    actions = writer._actions_for_result(result)

    assert len(actions) == 2
    assert actions[0]["_index"] == "vtl-raw-2026-06"
    assert actions[0]["_source"]["processing_mode"] == "elt"
    assert actions[0]["_source"]["document_family"] == "raw"
    assert actions[0]["_source"]["raw_document_id"] == actions[0]["_id"]
    assert actions[0]["_source"]["device_name"] == "DXi_1"
    assert actions[0]["_source"]["solution"] == "vtl"
    assert actions[0]["_source"]["collection_class"] == "fast"
    assert actions[1]["_index"] == "vtl-current"
    assert actions[1]["_id"] == "DXi_1:current"
    assert actions[1]["_source"]["current_document_id"] == "DXi_1:current"
    assert actions[1]["_source"]["processing_mode"] == "etl"
    assert actions[1]["_source"]["device_name"] == "DXi_1"
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
    assert actions[0]["_index"] == "vtl-raw-2026-06"
    assert actions[1]["_index"] == "vtl-current"


def test_index_names_follow_target_specific_design() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    collected_at = datetime(2026, 6, 29, tzinfo=timezone.utc)
    cases = [
        ("DXi_2", "DXi", "cli", "vtl-raw-2026-06", "vtl-current"),
        ("DD6900_1", "DD", "snmp", "vtl-raw-2026-06", "vtl-current"),
        ("DD6900_2", "DD", "snmp", "vtl-raw-2026-06", "vtl-current"),
        ("i6000_core_rest", "i6000", "rest", "ptl-raw-2026-06", "ptl-current"),
        ("i6000_chnl_rest", "i6000", "rest", "ptl-raw-2026-06", "ptl-current"),
        ("i6000_info_rest", "i6000", "rest", "ptl-raw-2026-06", "ptl-current"),
        ("i6000_ifrs_rest", "i6000", "rest", "ptl-raw-2026-06", "ptl-current"),
        ("ZFS_1", "ZFS", "rest", "zfs-raw-2026-06", "zfs-current"),
        ("ZFS_4", "ZFS", "rest", "zfs-raw-2026-06", "zfs-current"),
    ]

    for collector, target_type, protocol, expected_raw, expected_current in cases:
        result = CollectionResult(
            collector=collector,
            target_type=target_type,
            protocol=protocol,
            collected_at=collected_at,
            ok=True,
        )

        assert writer._index_name(result) == expected_raw
        assert writer._index_name(result, "current") == expected_current


def test_current_document_id_and_index_are_stable_across_months() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    results = [
        CollectionResult(
            collector="DD4500",
            target_type="DD",
            protocol="snmp",
            collected_at=datetime(2026, month, 1, tzinfo=timezone.utc),
            ok=True,
        )
        for month in (6, 7)
    ]

    actions = [writer._actions_for_result(result) for result in results]

    assert actions[0][0]["_index"] == "vtl-raw-2026-06"
    assert actions[1][0]["_index"] == "vtl-raw-2026-07"
    assert actions[0][1]["_index"] == actions[1][1]["_index"] == "vtl-current"
    assert actions[0][1]["_id"] == actions[1][1]["_id"] == "DD4500:current"


def test_slow_collection_does_not_overwrite_current_status() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    result = CollectionResult(
        collector="i6000_core_rest",
        target_type="i6000",
        protocol="rest",
        collected_at=datetime(2026, 7, 25, tzinfo=timezone.utc),
        ok=True,
        collection_class="slow",
    )

    actions = writer._actions_for_result(result)

    assert len(actions) == 1
    assert actions[0]["_index"] == "ptl-raw-2026-07"
    assert actions[0]["_source"]["collection_class"] == "slow"


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


def test_networker_actions_route_ops_and_domain_entity_indexes() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    result = CollectionResult(
        collector="NETWORKER_CHNL",
        target_type="Networker",
        protocol="rest",
        collected_at=datetime(2026, 7, 25, tzinfo=timezone.utc),
        ok=True,
        payload={
            "summary": {"source_networker": "CHNL", "device_name": "NW-CHNL-01"},
            "jobs": [
                {
                    "job_id": "job-1",
                    "client_name": "core-db01.example.com",
                    "source_networker": "chnl",
                    "security_domain": "CORE",
                }
            ],
            "clients": [
                {
                    "client_name": "unknown-host",
                    "source_networker": "chnl",
                    "security_domain": "unmapped",
                }
            ],
            "policies": [
                {
                    "policy_name": "Bronze",
                    "source_networker": "CHNL",
                    "security_domain": "CORE",
                }
            ],
            "workflows": [
                {
                    "policy_name": "Bronze",
                    "workflow_name": "Filesystem",
                    "source_networker": "CHNL",
                    "security_domain": "CORE",
                }
            ],
            "monthly_report": [
                {
                    "policy_name": "Bronze",
                    "month": "2026-07",
                    "source_networker": "CHNL",
                    "security_domain": "CORE",
                }
            ],
        },
    )

    actions = writer._actions_for_result(result)

    assert [action["_index"] for action in actions] == [
        "nw-ops-raw-chnl-2026-07",
        "nw-ops-current-chnl",
        "nw-core-job-2026-07",
        "nw-unmapped-client-2026-07",
        "nw-core-policy-2026-07",
        "nw-core-workflow-2026-07",
        "nw-core-monthly-2026-07",
    ]
    required_fields = {
        "collector",
        "device_name",
        "target_type",
        "solution",
        "document_family",
        "document_type",
        "@timestamp",
    }
    assert required_fields <= actions[0]["_source"].keys()
    assert required_fields <= actions[1]["_source"].keys()
    assert required_fields <= actions[2]["_source"].keys()


def test_networker_slow_collection_keeps_derived_data_without_current_write() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    result = CollectionResult(
        collector="networker_info",
        target_type="Networker",
        protocol="rest",
        collected_at=datetime(2026, 7, 25, tzinfo=timezone.utc),
        ok=True,
        collection_class="slow",
        payload={
            "summary": {"source_networker": "info"},
            "clients": [
                {
                    "client_name": "client01",
                    "source_networker": "info",
                    "security_domain": "info",
                }
            ],
        },
    )

    actions = writer._actions_for_result(result)

    assert [action["_index"] for action in actions] == [
        "nw-ops-raw-info-2026-07",
        "nw-info-client-2026-07",
    ]


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

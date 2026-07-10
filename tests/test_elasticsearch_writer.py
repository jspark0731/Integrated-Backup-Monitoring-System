from datetime import datetime, timezone

from app.core.config import ElasticsearchConfig
from app.models import CollectionResult
from app.writers.elasticsearch import ElasticsearchWriter


def test_dxi_cli_snmp_results_use_summary_index() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    result = CollectionResult(
        collector="DXi_1",
        target_type="DXi",
        protocol="cli_snmp",
        collected_at=datetime(2026, 6, 29, tzinfo=timezone.utc),
        ok=True,
    )

    assert writer._index_name(result).startswith("backup-dxi-summary-")


def test_dd_snmp_results_use_status_index() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    result = CollectionResult(
        collector="DD4500",
        target_type="DD",
        protocol="snmp",
        collected_at=datetime(2026, 6, 29, tzinfo=timezone.utc),
        ok=True,
    )

    assert writer._index_name(result).startswith("backup-dashboard-")


def test_i6000_rest_results_expand_to_status_drive_media_indexes() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    result = CollectionResult(
        collector="i6000_core_rest",
        target_type="i6000",
        protocol="rest",
        collected_at=datetime(2026, 6, 29, tzinfo=timezone.utc),
        ok=True,
    )

    actions = writer._actions_for_result(result)

    assert len(actions) == 3
    assert actions[0]["_index"].startswith("backup-i6000-status-")
    assert actions[1]["_index"].startswith("backup-i6000-drive-")
    assert actions[2]["_index"].startswith("backup-i6000-media-")


def test_networker_rest_results_expand_to_domain_indexes() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    result = CollectionResult(
        collector="networker_core",
        target_type="Networker",
        protocol="rest",
        collected_at=datetime(2026, 6, 29, tzinfo=timezone.utc),
        ok=True,
        payload={
            "jobs": [{"job_id": 1}],
            "clients": [{"client_name": "client01"}],
            "policies": [{"policy_name": "Bronze"}],
            "workflows": [{"workflow_name": "Filesystem"}],
            "monthly_report": [{"policy_name": "Bronze"}],
        },
    )

    actions = writer._actions_for_result(result)

    assert len(actions) == 5
    assert actions[0]["_index"].startswith("backup-networker-job-")
    assert actions[1]["_index"].startswith("backup-networker-client-")
    assert actions[2]["_index"].startswith("backup-networker-policy-")
    assert actions[3]["_index"].startswith("backup-networker-workflow-")
    assert actions[4]["_index"].startswith("backup-networker-monthly-report-")


def test_zfs_rest_results_expand_to_domain_indexes() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    result = CollectionResult(
        collector="ZFS_1",
        target_type="ZFS",
        protocol="rest",
        collected_at=datetime(2026, 6, 29, tzinfo=timezone.utc),
        ok=True,
        payload={
            "summary": {"device_name": "zfs-prod-1"},
            "pools": [{"name": "p1"}],
            "alerts": [{"summary": "Disk fault"}],
        },
    )

    actions = writer._actions_for_result(result)

    assert len(actions) == 3
    assert actions[0]["_index"].startswith("backup-zfs-summary-")
    assert actions[1]["_index"].startswith("backup-zfs-pool-")
    assert actions[2]["_index"].startswith("backup-zfs-status-")

from datetime import datetime, timezone

from app.core.config import ElasticsearchConfig
from app.models import CollectionResult
from app.writers.elasticsearch import ElasticsearchWriter


def test_dxi_ssh_results_use_summary_index() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    result = CollectionResult(
        collector="DXi_1_cli",
        target_type="DXi",
        protocol="ssh",
        collected_at=datetime(2026, 6, 29, tzinfo=timezone.utc),
        ok=True,
    )

    assert writer._index_name(result).startswith("backup-dxi-summary-")


def test_dxi_snmp_results_use_status_index() -> None:
    writer = ElasticsearchWriter(ElasticsearchConfig())
    result = CollectionResult(
        collector="DXi_1",
        target_type="DXi",
        protocol="snmp",
        collected_at=datetime(2026, 6, 29, tzinfo=timezone.utc),
        ok=True,
    )

    assert writer._index_name(result).startswith("backup-dxi-status-")


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

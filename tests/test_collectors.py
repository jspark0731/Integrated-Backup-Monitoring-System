import pytest

from app.collectors.factory import build_collector
from app.collectors.dxi_cli import DxiCliCollector, parse_dxi_cli_outputs
from app.collectors.i6000_rest import I6000RestCollector, parse_i6000_rest_payload
from app.core.config import CollectorConfig


@pytest.mark.asyncio
async def test_snmp_collector_skips_unfilled_config() -> None:
    collector = build_collector(
        CollectorConfig(
            name="DXi_1",
            type="DXi",
            protocol="snmp",
            enabled=True,
            schedule_second=0,
            host="DXi_1_host_TO_BE_FILLED",
            community="DXi_1_community_TO_BE_FILLED",
            oids={"capacity": "DXi_1_capacity_oid_TO_BE_FILLED"},
        )
    )

    result = await collector.collect()

    assert result.skipped
    assert result.skip_reason


def test_snmp_collector_accepts_walk_only_config() -> None:
    collector = build_collector(
        CollectorConfig(
            name="DD_walk",
            type="DD",
            protocol="snmp",
            enabled=True,
            schedule_second=0,
            host="192.0.2.10",
            community="publicCmtyStr",
            walk_oids={"drive_health": "1.3.6.1.4.1.3764.1.1.200.20.80.110.1.31"},
        )
    )

    assert collector.config.skip_reason is None


def test_dxi_ssh_collector_accepts_command_config() -> None:
    collector = build_collector(
        CollectorConfig(
            name="DXi_1_cli",
            type="DXi",
            protocol="ssh",
            enabled=True,
            schedule_second=0,
            host="192.0.2.20",
            port=22,
            username="admin",
            password="secret",
            commands={"capacity": "show capacity"},
        )
    )

    assert isinstance(collector, DxiCliCollector)
    assert collector.config.skip_reason is None


def test_i6000_rest_collector_accepts_endpoint_map() -> None:
    collector = build_collector(
        CollectorConfig(
            name="i6000_core_rest",
            type="i6000",
            protocol="rest",
            enabled=True,
            schedule_second=15,
            base_url="https://192.0.2.40",
            username="admin",
            password="secret",
            endpoints={"status": "aml/physicalLibrary/status"},
        )
    )

    assert isinstance(collector, I6000RestCollector)
    assert collector.config.skip_reason is None


def test_dxi_cli_parser_extracts_summary_values() -> None:
    summary = parse_dxi_cli_outputs(
        {
            "status": "Device Name: DXi_1\nState: online\n",
            "capacity": "Total Capacity: 100 TB\nUsed Capacity: 72 TB\n",
            "dedup": "Deduplication Ratio: 12.5:1\n",
            "replication": "target-a: enabled\n",
            "interfaces": "eth0: up\neth1: down\n",
            "alerts": "Critical Alerts: 2\nWarning Alerts: 3\n",
        },
        fallback_name="DXi_1_cli",
    )

    assert summary["device_name"] == "DXi_1"
    assert summary["state"] == "online"
    assert summary["capacity"]["total_bytes"] == 100_000_000_000_000
    assert summary["capacity"]["used_percent"] == 72
    assert summary["dedup_ratio"] == 12.5
    assert summary["replication"][0]["up"] == 1
    assert summary["interfaces"][1]["up"] == 0
    assert summary["alert_counts"]["critical"] == 2


def test_i6000_rest_parser_extracts_tape_summary_values() -> None:
    summary = parse_i6000_rest_payload(
        {
            "ping": {
                "ping": {
                    "productName": "Scalar i6000",
                    "serialNumber": "LIB123",
                    "firmwareVersion": "700G",
                    "vendor": "Quantum",
                }
            },
            "physical_library": {
                "physicalLibrary": {
                    "name": "i6000-prod",
                    "serialNumber": "LIB123",
                    "productId": "Scalar i6000",
                    "mode": 1,
                    "state": 1,
                }
            },
            "status": {
                "libraryStatus": {
                    "state": 1,
                    "mode": 1,
                    "snmpStarted": "true",
                    "ras": {
                        "status": {"group": 6, "status": 1},
                        "openedTickets": 1,
                    },
                    "robot": {"serialNumber": "ROB1", "status": 1, "state": 1},
                    "partition": {"name": "default", "mode": 1, "type": 1},
                    "drive": {"logicalSerialNumber": "DRV1", "mode": 1, "state": 1},
                }
            },
            "drives": {"driveList": {"drive": {"logicalSerialNumber": "DRV1", "model": "LTO"}}},
            "media": {"mediaList": {"media": [{"barcode": "TAPE001"}, {"barcode": "TAPE002"}]}},
            "segments_storage_used": {"segmentList": {"segment": {"size": 12}}},
            "segments_storage_available": {"segmentList": {"segment": [{"size": 5}, {"size": 7}]}},
            "towers": {"towerList": {"tower": {"id": 1, "doorOpened": "true", "mode": 1, "state": 1}}},
            "ie_stations": {"ieStationList": {"ieStation": {"number": 1, "lock": 1}}},
            "ras_status": {"RASGroupStatusList": {"RASGroupStatus": {"group": 4, "status": 1}}},
            "ras_tickets": {
                "RASTicketList": {
                    "RASTicket": {
                        "severity": 3,
                        "serialNumber": "DRV1",
                        "groupStatus": {"group": 4, "status": 3},
                    }
                }
            },
        },
        fallback_name="i6000_core_rest",
    )

    assert summary["device_name"] == "i6000-prod"
    assert summary["library_status"] == 1
    assert summary["snmp_started"] is True
    assert summary["media_count"] == 2
    assert summary["slot_used_count"] == 12
    assert summary["slot_free_count"] == 12
    assert summary["robots"][0]["up"] == 1
    assert summary["drives"][0]["error_count"] == 1
    assert summary["ras_status"][0]["group"] == 6
    assert summary["library_main_door_open"] is True
    assert summary["ie_stations"][0]["lock"] == 1
    assert summary["ras_ticket_counts"]["warning"] == 1


@pytest.mark.asyncio
async def test_rest_collector_skips_unfilled_config() -> None:
    collector = build_collector(
        CollectorConfig(
            name="networker_core",
            type="Networker",
            protocol="rest",
            enabled=True,
            schedule_second=30,
            base_url="networker_core_base_url_TO_BE_FILLED",
            endpoint="networker_core_endpoint_TO_BE_FILLED",
        )
    )

    result = await collector.collect()

    assert result.skipped
    assert result.skip_reason

import pytest

from app.collectors.factory import build_collector
from app.collectors.dxi_cli import DxiCliCollector, parse_dxi_cli_outputs
from app.collectors.i6000_rest import I6000RestCollector, parse_i6000_rest_payload
from app.collectors.networker_rest import NetworkerRestCollector, parse_networker_rest_payload
from app.collectors.zfs_rest import ZfsRestCollector, parse_zfs_rest_payload
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


def test_networker_rest_collector_uses_default_endpoint_map() -> None:
    collector = build_collector(
        CollectorConfig(
            name="networker_core",
            type="Networker",
            protocol="rest",
            enabled=True,
            schedule_second=30,
            base_url="https://networker.example.com:9090",
            username="administrator",
            password="secret",
        )
    )

    assert isinstance(collector, NetworkerRestCollector)
    assert collector.config.skip_reason is None


def test_zfs_rest_collector_uses_default_endpoint_map() -> None:
    collector = build_collector(
        CollectorConfig(
            name="ZFS_1",
            type="ZFS",
            protocol="rest",
            enabled=True,
            schedule_second=45,
            base_url="https://zfs-storage.example.com:215",
            username="root",
            password="secret",
        )
    )

    assert isinstance(collector, ZfsRestCollector)
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


def test_networker_rest_parser_extracts_backup_summary_values() -> None:
    parsed = parse_networker_rest_payload(
        {
            "jobs": {
                "jobs": [
                    {
                        "id": 1,
                        "name": "Filesystem",
                        "type": "backup",
                        "state": "Completed",
                        "exitCode": 0,
                        "runOnHost": "client01",
                    },
                    {
                        "id": 2,
                        "name": "Filesystem",
                        "type": "backup",
                        "state": "Completed",
                        "exitCode": 1,
                        "runOnHost": "client02",
                    },
                    {
                        "id": 3,
                        "name": "Database",
                        "type": "backup",
                        "state": "Running",
                        "runOnHost": "db01",
                    },
                ]
            },
            "clients": {
                "clients": [
                    {
                        "clientId": "client-1",
                        "hostname": "client01",
                        "operatingSystem": "Linux",
                        "backupType": "Filesystem",
                        "saveSets": ["All"],
                        "protectionGroups": ["Bronze-Filesystem"],
                    },
                    {
                        "clientId": "client-2",
                        "hostname": "db01",
                        "operatingSystem": "Windows Server",
                        "backupType": "Database",
                    },
                ]
            },
            "policies": {
                "protectionPolicies": [
                    {
                        "name": "Bronze",
                        "workflows": [
                            {
                                "name": "Filesystem",
                                "enabled": True,
                                "actions": [{"name": "Backup"}],
                                "protectionGroups": ["Bronze-Filesystem"],
                            },
                            {
                                "name": "Database",
                                "enabled": True,
                                "actions": [{"name": "Backup"}],
                            },
                        ],
                    }
                ]
            },
            "backups": {
                "backups": [
                    {
                        "id": "backup-1",
                        "clientHostname": "client01",
                        "name": "C:\\data",
                        "level": "Full",
                        "size": {"unit": "Byte", "value": 1000},
                        "attributes": [
                            {"key": "*policy name", "values": ["Bronze: 1539851250"]},
                            {"key": "*policy workflow name", "values": ["Filesystem: 1539851250"]},
                        ],
                    }
                ]
            },
        },
        server_name="networker_core",
    )

    assert parsed["summary"]["client_count"] == 2
    assert parsed["summary"]["workflow_count"] == 2
    assert parsed["summary"]["total_backup_bytes"] == 1000
    assert parsed["summary"]["job_success_count_by_policy"]["Filesystem"] == 1
    assert parsed["summary"]["job_failed_count_by_policy"]["Filesystem"] == 1
    assert parsed["summary"]["job_running_count_by_policy"]["Database"] == 1
    assert parsed["clients"][1]["client_os_family"] == "Windows"
    assert parsed["workflows"][0]["action_count"] == 1
    assert parsed["monthly_report"][0]["client_count"] == 2


def test_zfs_rest_parser_extracts_storage_summary_values() -> None:
    parsed = parse_zfs_rest_payload(
        {
            "version": {
                "version": {
                    "os_nodename": "zfs-prod-1",
                    "ak_product": "Oracle ZFS Storage Appliance",
                    "os_version": "nas/generic@2021.08.01",
                    "hw_csn": "AK123",
                }
            },
            "pools": {
                "pools": [
                    {
                        "name": "p1",
                        "state": "online",
                        "profile": "raidz1",
                    }
                ]
            },
            "pool:p1": {
                "pool": {
                    "name": "p1",
                    "state": "online",
                    "profile": "raidz1",
                    "usage": {
                        "total": 1000,
                        "used": 250,
                        "free": 750,
                        "usage_snapshots": 50,
                        "usage_replication": 25,
                    },
                }
            },
            "projects:p1": {"projects": [{"name": "proj-01"}]},
            "project:p1/proj-01": {
                "project": {
                    "name": "proj-01",
                    "mountpoint": "/export",
                    "dedup": False,
                    "sharenfs": "on",
                    "sharesmb": "off",
                }
            },
            "filesystems:p1/proj-01": {
                "filesystems": [
                    {
                        "name": "fs-01",
                        "pool": "p1",
                        "project": "proj-01",
                        "mountpoint": "/export/fs-01",
                    }
                ]
            },
            "luns:p1/proj-01": {
                "luns": [
                    {
                        "id": "lun-id",
                        "name": "lun-01",
                        "status": "online",
                        "volsize": 100,
                    }
                ]
            },
            "alert_logs": {"logs": [{"summary": "Alert raised", "timestamp": "20210701T00:00:00"}]},
            "fault_logs": {"logs": [{"summary": "Disk fault", "timestamp": "20210701T00:01:00"}]},
        },
        fallback_name="ZFS_1",
    )

    assert parsed["summary"]["device_name"] == "zfs-prod-1"
    assert parsed["summary"]["pool_count"] == 1
    assert parsed["summary"]["project_count"] == 1
    assert parsed["summary"]["filesystem_count"] == 1
    assert parsed["summary"]["lun_count"] == 1
    assert parsed["summary"]["alert_count"] == 1
    assert parsed["summary"]["fault_count"] == 1
    assert parsed["summary"]["used_percent"] == 25
    assert parsed["pools"][0]["up"] == 1
    assert parsed["pools"][0]["used_percent"] == 25
    assert parsed["projects"][0]["mountpoint"] == "/export"
    assert parsed["filesystems"][0]["name"] == "fs-01"
    assert parsed["luns"][0]["status"] == "online"


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

from pathlib import Path

import pytest

from apps.collectors.factory import build_collector
from apps.collectors.dd_snmp_collector import DDSnmpCollector, parse_dd_snmp_payload
from apps.collectors.dxi_cli_collector import DXiCliCollector, parse_dxi_cli_outputs
from apps.collectors.i6000_rest_collector import I6000RestCollector, parse_i6000_rest_payload
from apps.collectors.networker_rest_collector import (
    NetworkerRestCollector,
    _mark_failed_endpoint_counts_unknown,
    parse_networker_rest_payload,
)
from apps.collectors.zfs_rest_collector import ZfsRestCollector, parse_zfs_rest_payload
from apps.core.config import CollectorConfig
from apps.core.metrics import (
    BACKUP_COMP_RATIO,
    BACKUP_COMPRESSION_RATIO,
    BACKUP_COMPRESSION_SIZE_BYTES,
    BACKUP_DEDUP_RATIO,
)


@pytest.mark.asyncio
async def test_snmp_collector_skips_unfilled_config() -> None:
    collector = build_collector(
        CollectorConfig(
            name="DD4500",
            type="DD",
            protocol="snmp",
            enabled=True,
            schedule_second=0,
            host="DD4500_host_TO_BE_FILLED",
            community="DD4500_community_TO_BE_FILLED",
            oids={"capacity": "DD4500_capacity_oid_TO_BE_FILLED"},
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


def test_dd_snmp_parser_extracts_ddboost_summary_values() -> None:
    summary = parse_dd_snmp_payload(
        {
            "system_serial_number": "SN123",
            "system_model": "DD6900",
            "system_version": "6.0",
            "file_system_status": "enabled",
            "ddboost_status": "1",
            "file_system_space_size": [{"oid": "x.1", "instance": "1", "value": "100"}],
            "file_system_space_used": [{"oid": "x.1", "instance": "1", "value": "72"}],
            "file_system_percent_used": [{"oid": "x.1", "instance": "1", "value": "72"}],
            "file_system_compression_period": [
                {"oid": "x.1", "instance": "1", "value": "last-24-hours"},
                {"oid": "x.2", "instance": "2", "value": "last-7-days"},
            ],
            "file_system_pre_compression_size": [
                {"oid": "x.1", "instance": "1", "value": "125 GiB"},
                {"oid": "x.2", "instance": "2", "value": "700 GiB"},
            ],
            "file_system_post_compression_size": [
                {"oid": "x.1", "instance": "1", "value": "10 GiB"},
                {"oid": "x.2", "instance": "2", "value": "50 GiB"},
            ],
            "file_system_global_compression_factor": [
                {"oid": "x.1", "instance": "1", "value": "6.25x"},
                {"oid": "x.2", "instance": "2", "value": "7x"},
            ],
            "file_system_local_compression_factor": [
                {"oid": "x.1", "instance": "1", "value": "2x"},
                {"oid": "x.2", "instance": "2", "value": "2x"},
            ],
            "file_system_total_compression_factor": [
                {"oid": "x.1", "instance": "1", "value": "12.5 : 1"},
                {"oid": "x.2", "instance": "2", "value": "14 : 1"},
            ],
            "current_alert_severity": [
                {"oid": "x.1", "instance": "1", "value": "critical"},
                {"oid": "x.2", "instance": "2", "value": "warning"},
                {"oid": "x.3", "instance": "3", "value": "warning"},
            ],
            "replication_destination": [{"oid": "x.1", "instance": "1", "value": "target-a"}],
            "replication_state": [{"oid": "x.1", "instance": "1", "value": "enabled"}],
            "ddboost_backup_connections": [{"oid": "x.1", "instance": "1", "value": "3"}],
            "ddboost_restore_connections": [{"oid": "x.1", "instance": "1", "value": "1"}],
            "ddboost_pre_comp_kbps": [{"oid": "x.1", "instance": "1", "value": "2048"}],
            "ddboost_storage_unit_name": [{"oid": "x.1", "instance": "1", "value": "stu-a"}],
            "ddboost_storage_unit_bytes": [{"oid": "x.1", "instance": "1", "value": "1024"}],
            "ddboost_storage_unit_global_comp": [{"oid": "x.1", "instance": "1", "value": "8.5"}],
        },
        fallback_name="DD6900_1",
    )

    assert summary["device_name"] == "DD6900_1"
    assert summary["serial_number"] == "SN123"
    assert summary["capacity"]["total_bytes"] == 100 * 1024**3
    assert summary["capacity"]["used_percent"] == 72
    assert summary["dedup_ratio"] == 6.25
    assert summary["compression_ratio"] == 2
    assert summary["compression"][0]["period"] == "last-24-hours"
    assert summary["compression"][0]["pre_compression_bytes"] == 125 * 1024**3
    assert summary["compression"][0]["post_compression_bytes"] == 10 * 1024**3
    assert summary["compression"][0]["total_ratio"] == 12.5
    assert summary["compression"][1]["total_ratio"] == 14
    assert summary["alert_counts"]["warning"] == 2
    assert summary["replication"][0]["up"] == 1
    assert summary["ddboost"]["enabled"] is True
    assert summary["ddboost"]["connections"]["backup"] == 3
    assert summary["ddboost"]["throughput_kbps"]["pre_compression"] == 2048
    assert summary["ddboost"]["storage_units"][0]["name"] == "stu-a"


def test_dd_snmp_collector_accepts_data_domain_mib_oids() -> None:
    collector = build_collector(
        CollectorConfig(
            name="DD6900_1",
            type="DD",
            protocol="snmp",
            enabled=True,
            schedule_second=0,
            host="192.0.2.30",
            community="public",
            oids={
                "file_system_status": "1.3.6.1.4.1.19746.1.3.1.1.0",
                "ddboost_status": "1.3.6.1.4.1.19746.1.12.1.1.0",
            },
            walk_oids={
                "file_system_space_used": "1.3.6.1.4.1.19746.1.3.2.1.1.5",
                "ddboost_storage_unit_name": "1.3.6.1.4.1.19746.1.12.4.1.1.2",
            },
        )
    )

    assert isinstance(collector, DDSnmpCollector)
    assert collector.config.skip_reason is None


def test_dd_snmp_collector_publishes_file_system_compression_metrics() -> None:
    collector = DDSnmpCollector(
        CollectorConfig(
            name="DD_compression_test",
            type="DD",
            protocol="snmp",
            enabled=True,
            host="192.0.2.31",
            community="public",
            oids={"ddboost_status": "1.3.6.1.4.1.19746.1.12.1.1.0"},
        )
    )
    collector._publish_metrics(
        {
            "device_name": "DD_compression_test",
            "capacity": {},
            "dedup_ratio": 6.25,
            "compression_ratio": 2,
            "compression": [
                {
                    "instance": "1",
                    "period": "last-24-hours",
                    "pre_compression_bytes": 125 * 1024**3,
                    "post_compression_bytes": 10 * 1024**3,
                    "total_ratio": 12.5,
                }
            ],
            "ddboost": {},
        }
    )

    assert BACKUP_COMPRESSION_RATIO.labels(
        "dd", "DD_compression_test", "last-24-hours"
    )._value.get() == 12.5
    assert BACKUP_COMPRESSION_SIZE_BYTES.labels(
        "dd", "DD_compression_test", "last-24-hours", "pre_compression"
    )._value.get() == 125 * 1024**3
    assert BACKUP_COMPRESSION_SIZE_BYTES.labels(
        "dd", "DD_compression_test", "last-24-hours", "post_compression"
    )._value.get() == 10 * 1024**3
    assert BACKUP_DEDUP_RATIO.labels("dd", "DD_compression_test")._value.get() == 6.25
    assert BACKUP_COMP_RATIO.labels("dd", "DD_compression_test")._value.get() == 2


def test_dxi_legacy_ssh_collector_is_rejected() -> None:
    with pytest.raises(ValueError, match="DXi SNMP/legacy"):
        build_collector(
            CollectorConfig(
                name="DXi_1_legacy_ssh",
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


def test_dxi_cli_collector_accepts_cli_config() -> None:
    collector = build_collector(
        CollectorConfig(
            name="DXi_1",
            type="DXi",
            protocol="cli",
            enabled=True,
            schedule_second=0,
            host="192.0.2.20",
            ssh_port=22,
            username="admin",
            password="secret",
            commands={"capacity": "show capacity"},
        )
    )

    assert isinstance(collector, DXiCliCollector)
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


def test_i6000_snmp_collector_is_rejected() -> None:
    with pytest.raises(ValueError, match="i6000 SNMP"):
        build_collector(
            CollectorConfig(
                name="i6000_core_snmp",
                type="i6000",
                protocol="snmp",
                enabled=True,
                schedule_second=15,
                host="192.0.2.40",
                community="public",
                oids={"library_status": "1.3.6.1.2.1.1.1.0"},
            )
        )


def test_networker_rest_collector_uses_default_endpoint_map(tmp_path: Path) -> None:
    hostname_csv = tmp_path / "hostname_domain.csv"
    hostname_csv.write_text("hostname,security_domain\nclient01,core\n", encoding="utf-8")
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
            source_networker="core",
            hostname_csv_path=str(hostname_csv),
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
            "common_components": """
  [Common Component = 1]
    Component Name = Storage Arrays
    Status = Normal
""",
            "storage_arrays": """
  [Array = 1]
    Name = Qarray1
    Status = Normal
""",
            "system_board": """
  [Component = 1]
    Name = Fan 1 RPM
    Type = Fan
    Value = 3840 RPM
    Status = Normal
""",
            "capacity": """
    Disk Capacity = 25.84 TB
    Available Disk Space = 5.91 TB
    - Free Space = 1.86 TB  (7.21% of capacity)
    - Reclaimable Space = 4.05 TB  (15.67% of capacity)
    Used Disk Space = 19.93 TB
    - Deduplicated Data = 17.85 TB  (69.10% of capacity)
    - System Metadata = 2.07 TB  (8.02% of capacity)
""",
            "dedup": """
    Data Size Before Reduction = 80.95 TB
    Data Size After Reduction = 17.85 TB
    Total Reduction Ratio = 4.53 : 1
    - Deduplication Ratio = 2.35 : 1
    - Compression Ratio = 1.93 : 1
""",
            "vtls": """
  [vtl = 1]
    name = DXI6801_VTL1
    mode = online
    model = ADIC Scalar i2000
    drivemodel = IBM LTO-3
    drives = 52
    media = 300
    slots = 300
    ieslots = 240
    serial = REDACTED
    dedup = enabled
    replication = disabled
""",
            "interfaces": """
  [Port = 1]
    Name = ETH1
    Value = 1000 Mb/s
    Status = Up
  [Port = 2]
    Name = ETH2
    Value = NA
    Status = Down
""",
            "network_config": "HOSTNAME=P1-VTL\nGATEWAY=192.0.2.1\n",
            "admin_alerts": """
  [Alert Number = 1]
    Alert = SpaceManagementDaemon
    Last Update = 2026-06-21T20:36:23+00:00
    Summary = Space Management: Cleared Low Space Mode
""",
            "service_tickets": "",
        },
        fallback_name="DXi_1",
    )

    assert summary["device_name"] == "P1-VTL"
    assert summary["state"] == "normal"
    assert summary["capacity"]["total_bytes"] == 25_840_000_000_000
    assert summary["capacity"]["available_bytes"] == 5_910_000_000_000
    assert summary["capacity"]["used_percent"] == pytest.approx(77.128, abs=0.001)
    assert summary["dedup_ratio"] == 2.35
    assert summary["data_reduction"]["total_reduction_ratio"] == 4.53
    assert summary["data_reduction"]["compression_ratio"] == 1.93
    assert summary["vtls"][0]["dedup_enabled"] is True
    assert summary["vtls"][0]["replication_enabled"] is False
    assert summary["vtls"][0]["drive_count"] == 52
    assert summary["interfaces"][1]["up"] == 0
    assert summary["interfaces"][0]["speed_bps"] == 1_000_000_000
    assert summary["alert_counts"]["unclassified"] == 1
    assert summary["alert_counts"]["total"] == 1

    collector = DXiCliCollector(
        CollectorConfig(
            name="DXi_1",
            type="DXi",
            protocol="cli",
            enabled=True,
            host="192.0.2.32",
            username="admin",
            password="secret",
            commands={"dedup": "show data reduction"},
        )
    )
    collector._publish_metrics(summary)
    assert BACKUP_DEDUP_RATIO.labels("dxi", "P1-VTL")._value.get() == 2.35
    assert BACKUP_COMP_RATIO.labels("dxi", "P1-VTL")._value.get() == 1.93


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
                        "jobId": 3,
                        "name": "Database",
                        "type": "backup",
                        "completionStatus": "Running",
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
            "_query_window": {
                "start": "2026-08-28T12:10:00",
                "end": "2026-08-28T12:30:00",
                "minutes": 20,
            },
        },
        server_name="networker_core",
    )

    assert parsed["summary"]["client_count"] == 2
    assert parsed["summary"]["workflow_count"] == 2
    assert parsed["summary"]["total_backup_bytes"] == 1000
    # Job names are not policy evidence; absent API policyName stays unknown.
    assert parsed["summary"]["job_success_count_by_policy"]["unknown"] == 1
    assert parsed["summary"]["job_failed_count_by_policy"]["unknown"] == 1
    assert parsed["summary"]["job_running_count_by_policy"]["unknown"] == 1
    assert parsed["summary"]["query_window"]["minutes"] == 20
    assert parsed["jobs"][2]["job_id"] == 3
    assert parsed["jobs"][2]["status"] == "running"
    assert parsed["backups"][0]["backup_id"] == "backup-1"
    assert parsed["clients"][1]["client_os_family"] == "Windows"
    assert parsed["workflows"][0]["action_count"] == 1
    assert parsed["monthly_report"][0]["client_count"] == 2


def test_networker_rest_parser_normalizes_job_phase_three_fields() -> None:
    parsed = parse_networker_rest_payload(
        {
            "jobs": {
                "jobs": [
                    {
                        "id": "job-failed",
                        "state": "Completed",
                        "completionStatus": "Failed",
                        "exitCode": 0,
                        "startTime": "2026-08-31T15:20:00+09:00",
                        "endTime": "2026-08-31T06:30:00Z",
                        "level": "Full",
                        "dataSize": {"unit": "MiB", "value": 2},
                        "previousJobId": "job-previous",
                    },
                    {
                        "id": "job-canceled",
                        "state": "Completed",
                        "completionStatus": "Canceled",
                        "exitCode": 1,
                        "startTime": 1788157200000,
                        "endTime": 1788157260000,
                    },
                    {
                        "id": "job-running",
                        "state": "Running",
                        "startTime": "2026-08-31T06:30:00Z",
                    },
                ]
            }
        },
        server_name="networker_core",
    )

    failed, canceled, running = parsed["jobs"]
    assert failed["state_raw"] == "Completed"
    assert failed["completion_status"] == "Failed"
    assert failed["status_raw"] == "Failed"
    assert failed["status"] == "failed"
    assert failed["status_normalized"] == "failed"
    assert failed["start_time"] == "2026-08-31T06:20:00+00:00"
    assert failed["end_time"] == "2026-08-31T06:30:00+00:00"
    assert failed["duration_seconds"] == 600
    assert failed["level"] == "Full"
    assert failed["data_size_bytes"] == 2_097_152
    assert failed["previous_job_id"] == "job-previous"

    assert canceled["status_raw"] == "Canceled"
    assert canceled["status_normalized"] == "canceled"
    assert canceled["duration_seconds"] == 60

    assert running["status_normalized"] == "running"
    assert running["end_time"] is None
    assert running["duration_seconds"] is None


def test_networker_rest_parser_normalizes_backup_phase_four_fields() -> None:
    parsed = parse_networker_rest_payload(
        {
            "backups": {
                "backups": [
                    {
                        "id": "backup-42",
                        "clientHostname": "server01",
                        "name": "/data",
                        "level": "Full",
                        "saveTime": "2026-08-31T15:20:00+09:00",
                        "completionTime": "2026-08-31T06:30:00Z",
                        "size": {"unit": "GiB", "value": 1.5},
                        "actionName": "Backup",
                    }
                ]
            }
        },
        server_name="networker_core",
    )

    backup = parsed["backups"][0]
    assert backup["backup_id"] == "backup-42"
    assert backup["save_set"] == "/data"
    assert backup["save_time"] == "2026-08-31T06:20:00+00:00"
    assert backup["completion_time"] == "2026-08-31T06:30:00+00:00"
    assert backup["size_raw"] == 1.5
    assert backup["size_unit_raw"] == "GiB"
    assert backup["size_bytes"] == 1_610_612_736
    assert backup["action_name"] == "Backup"


def test_networker_rest_parser_keeps_only_client_inventory_metadata() -> None:
    parsed = parse_networker_rest_payload(
        {
            "clients": {
                "clients": [
                    {
                        "resourceId": {"id": "resource-1"},
                        "hostname": "server01",
                        "clientState": "Active",
                        "operatingSystem": "Linux",
                        "backupType": "Filesystem",
                        "saveSets": ["/should/not/be/stored"],
                    }
                ]
            }
        },
        server_name="networker_core",
    )

    client = parsed["clients"][0]
    assert client["resource_id"] == "resource-1"
    assert client["client_state"] == "enabled"
    assert client["client_os_family"] == "Linux"
    assert "save_sets" not in client


def test_networker_failed_endpoint_counts_are_unknown_not_zero() -> None:
    summary = {
        "job_count": 0,
        "backup_count": 0,
        "total_backup_bytes": 0,
    }

    _mark_failed_endpoint_counts_unknown(
        summary,
        {"jobs": "HTTPStatusError: 500", "backups": "ReadTimeout"},
    )

    assert summary["job_count"] is None
    assert summary["backup_count"] is None
    assert summary["total_backup_bytes"] is None


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

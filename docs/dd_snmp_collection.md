# DD SNMP Collection

The DD4500 and DD6900 MIBs expose the dashboard fields needed for Data Domain
and DD Boost monitoring under the Dell enterprise tree `1.3.6.1.4.1.19746`.

## SNMP-Covered Data

| Group | Examples | Source |
| --- | --- | --- |
| System identity | serial number, DD OS version, model | `dataDomainSystem` |
| File system capacity | status, total GiB, used GiB, available GiB, percent used | `fileSystem` |
| Dedup/compression | total compression factor, reduction percent | `fileSystemCompression` |
| Alerts | current alert severity rows | `alerts` |
| Replication | state, status, source, destination | `replication` |
| DD Boost state | enabled/disabled | `ddboostProperties` |
| DD Boost users and interface groups | user names, ifgroup names/status | `ddboostProperties` |
| DD Boost throughput/connections | pre-comp, post-comp, network, read KiB/s, backup/restore connections | `ddboostStats` |
| DD Boost storage units | name, bytes, compression, metadata, status, user | `ddboostStorageUnit` |

DD6900 MIBs include newer storage-unit columns such as
`ddboostStorageUnitStatus`, `ddboostStorageUnitPreComp`,
`ddboostStorageUnitUser`, `ddboostStorageUnitReportPhysicalSize`, and
`ddboostStorageUnitBytesHC`. If a DD4500 does not expose those OIDs, remove those
walk entries from that device config.

## CLI Fallback

No CLI fallback is required for the standard dashboard fields above because the
provided MIBs expose them through SNMP.

CLI is still useful when operators want command-native detail that is not modeled
as a MIB table, such as exact `ddboost storage-unit show` formatting, ad hoc
troubleshooting output, or version-specific fields absent from an older device MIB.
Those outputs should be added only when a real command sample is available.

## Key OIDs

Scalar OIDs use the `.0` instance suffix:

| Metric | OID |
| --- | --- |
| `system_serial_number` | `1.3.6.1.4.1.19746.1.13.1.1.0` |
| `system_version` | `1.3.6.1.4.1.19746.1.13.1.3.0` |
| `system_model` | `1.3.6.1.4.1.19746.1.13.1.4.0` |
| `file_system_status` | `1.3.6.1.4.1.19746.1.3.1.1.0` |
| `ddboost_status` | `1.3.6.1.4.1.19746.1.12.1.1.0` |

Walk OIDs:

| Metric | OID |
| --- | --- |
| `file_system_space_size` | `1.3.6.1.4.1.19746.1.3.2.1.1.4` |
| `file_system_space_used` | `1.3.6.1.4.1.19746.1.3.2.1.1.5` |
| `file_system_space_available` | `1.3.6.1.4.1.19746.1.3.2.1.1.6` |
| `file_system_percent_used` | `1.3.6.1.4.1.19746.1.3.2.1.1.7` |
| `file_system_total_compression_factor` | `1.3.6.1.4.1.19746.1.3.3.1.1.9` |
| `current_alert_severity` | `1.3.6.1.4.1.19746.1.4.1.1.1.4` |
| `replication_state` | `1.3.6.1.4.1.19746.1.8.1.1.1.3` |
| `replication_status` | `1.3.6.1.4.1.19746.1.8.1.1.1.4` |
| `replication_source` | `1.3.6.1.4.1.19746.1.8.1.1.1.7` |
| `replication_destination` | `1.3.6.1.4.1.19746.1.8.1.1.1.8` |
| `ddboost_user_name` | `1.3.6.1.4.1.19746.1.12.1.4.1.2` |
| `ddboost_ifgroup_name` | `1.3.6.1.4.1.19746.1.12.1.5.1.2` |
| `ddboost_ifgroup_status` | `1.3.6.1.4.1.19746.1.12.1.5.1.3` |
| `ddboost_pre_comp_kbps` | `1.3.6.1.4.1.19746.1.12.2.1.1.2` |
| `ddboost_post_comp_kbps` | `1.3.6.1.4.1.19746.1.12.2.1.1.3` |
| `ddboost_network_kbps` | `1.3.6.1.4.1.19746.1.12.2.1.1.4` |
| `ddboost_read_kbps` | `1.3.6.1.4.1.19746.1.12.2.1.1.5` |
| `ddboost_backup_connections` | `1.3.6.1.4.1.19746.1.12.2.1.1.6` |
| `ddboost_restore_connections` | `1.3.6.1.4.1.19746.1.12.2.1.1.7` |
| `ddboost_compression_ratio` | `1.3.6.1.4.1.19746.1.12.2.1.1.16` |
| `ddboost_storage_unit_name` | `1.3.6.1.4.1.19746.1.12.4.1.1.2` |
| `ddboost_storage_unit_bytes` | `1.3.6.1.4.1.19746.1.12.4.1.1.3` |
| `ddboost_storage_unit_global_comp` | `1.3.6.1.4.1.19746.1.12.4.1.1.4` |
| `ddboost_storage_unit_local_comp` | `1.3.6.1.4.1.19746.1.12.4.1.1.5` |
| `ddboost_storage_unit_metadata` | `1.3.6.1.4.1.19746.1.12.4.1.1.6` |
| `ddboost_storage_unit_status` | `1.3.6.1.4.1.19746.1.12.4.1.1.7` |
| `ddboost_storage_unit_pre_comp` | `1.3.6.1.4.1.19746.1.12.4.1.1.8` |
| `ddboost_storage_unit_user` | `1.3.6.1.4.1.19746.1.12.4.1.1.9` |
| `ddboost_storage_unit_report_physical_size` | `1.3.6.1.4.1.19746.1.12.4.1.1.10` |
| `ddboost_storage_unit_bytes_hc` | `1.3.6.1.4.1.19746.1.12.4.1.1.11` |

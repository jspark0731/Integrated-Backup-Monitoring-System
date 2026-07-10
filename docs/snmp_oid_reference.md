# SNMP OID Reference

This file summarizes the Quantum SNMP guide values used by the example collector
configuration.

## DXi-Series

The DXi 2.2.1 SNMP reference documents the Quantum MIB under
`1.3.6.1.4.1.2036.2.1`. The guide exposes system identity and state OIDs, not
capacity totals.

Capacity, deduplication, replication, interface, and alert count collection is
handled by the DXi SSH collector. See `docs/dxi_cli_collection.md`.

## Data Domain

The DD4500 and DD6900 MIBs expose Data Domain system, file-system, alert,
replication, MTree, and DD Boost objects under `1.3.6.1.4.1.19746`.

DD collection uses SNMP for the standard dashboard fields, including DD Boost
status, connection counts, throughput, compression, and storage-unit rows. See
`docs/dd_snmp_collection.md`.

Scalar GET OIDs use the `.0` instance suffix:

| Metric | OID |
| --- | --- |
| `device_name` | `1.3.6.1.4.1.2036.2.1.1.1.0` |
| `assigned_name` | `1.3.6.1.4.1.2036.2.1.1.2.0` |
| `location` | `1.3.6.1.4.1.2036.2.1.1.3.0` |
| `vendor_id` | `1.3.6.1.4.1.2036.2.1.1.4.0` |
| `product_id` | `1.3.6.1.4.1.2036.2.1.1.5.0` |
| `product_revision` | `1.3.6.1.4.1.2036.2.1.1.6.0` |
| `state` | `1.3.6.1.4.1.2036.2.1.1.7.0` |
| `serial_number` | `1.3.6.1.4.1.2036.2.1.1.12.0` |

## Scalar i6000

i6000 collection no longer uses SNMP. The Scalar i6000 RESTful Web Services API
covers the status values that were previously mapped from SNMP and also exposes
slot, media, tower door, I/E station, and RAS ticket data. See
`docs/i6000_rest_collection.md`.

The OIDs below are kept only as historical reference for the earlier SNMP
prototype.

Single-instance GET OIDs:

| Metric | OID |
| --- | --- |
| `product_name` | `1.3.6.1.4.1.3764.1.1.10.3.0` |
| `physical_library_online_status` | `1.3.6.1.4.1.3764.1.1.200.20.80.10.1.5.8.0.0.0.0.0.1.0.59` |
| `robotics_readiness` | `1.3.6.1.4.1.3764.1.1.200.20.80.10.1.6.8.0.0.0.0.0.1.0.59` |
| `library_main_door_status` | `1.3.6.1.4.1.3764.1.1.200.20.80.10.1.16.8.0.0.0.0.0.1.0.59` |

Walk OIDs:

| Metric | OID |
| --- | --- |
| `ras_subsystem_status` | `1.3.6.1.4.1.3764.1.1.200.20.100.10.1.2` |
| `partition_names` | `1.3.6.1.4.1.3764.1.1.200.20.90.20.1.3` |
| `partition_online_status` | `1.3.6.1.4.1.3764.1.1.200.20.90.20.1.12` |
| `drive_online_status` | `1.3.6.1.4.1.3764.1.1.200.20.80.110.1.29` |
| `drive_overall_health` | `1.3.6.1.4.1.3764.1.1.200.20.80.110.1.31` |
| `ie_station_door_status` | `1.3.6.1.4.1.3764.1.1.200.20.80.75.1.2` |

Common value maps:

| Metric group | Values |
| --- | --- |
| RAS and drive health | `good(1)`, `failed(2)`, `degraded(3)`, `warning(4)`, `informational(5)`, `unknown(6)`, `invalid(7)` |
| Online status | `online(1)`, `offline(2)`, `shutdown(3)` |
| Robotics readiness | `ready(1)`, `notReady(2)` |
| Door status | `open(1)`, `closed(2)`, `closedAndLocked(3)`, `closedAndUnlocked(4)`, `controllerFailed(5)` |

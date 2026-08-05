# DXi CLI Collection

DXi collection uses SSH CLI exclusively. The CLI provides the operational
values needed by the dashboard, including capacity, data reduction, VTL and
interface status, hardware status, alerts, and service tickets.

## Collection Flow

```text
collector schedule
  -> SSH login to DXi and run configured CLI commands
  -> capture stdout
  -> parse text into a normalized summary
  -> store raw + summary payload in Elasticsearch
  -> publish normalized Prometheus gauges
```

## Configuration

Use `protocol: cli` for DXi collection.

```yaml
- name: DXi_1
  type: DXi
  protocol: cli
  enabled: true
  schedule:
    interval_minutes: 5
    minute_offset: 0
    second: 0
  host: DXi_1_host_TO_BE_FILLED
  ssh_port: 22
  username: DXi_1_username_TO_BE_FILLED
  password: DXi_1_password_TO_BE_FILLED
  command_timeout: 30
  commands:
    common_components: "syscli --getstatus commoncomponent"
    storage_arrays: "syscli --getstatus storagearray"
    system_board: "syscli --getstatus systemboard"
    capacity: "syscli --get diskusage"
    dedup: "syscli --get datareductionstat"
    vtls: "syscli --list vtl"
    interfaces: "syscli --getstatus networkport"
    network_config: "syscli --show netcfg"
    admin_alerts: "syscli --list adminalert"
    service_tickets: "syscli --list serviceticket --open"
```

If key-based authentication is preferred, set `ssh_key_path` instead of
`password`.

```yaml
ssh_key_path: /run/secrets/dxi_ssh_key
```

The command names on the left are parser inputs. These commands model the
DXi6802 output used by this deployment. Replication polling is intentionally
omitted because both configured VTL appliances have replication disabled.
`No open tickets available` from the service-ticket command is normalized to
an empty successful result; other individual command failures are retained in
`command_errors` and mark the payload as `partial`.

## Parsed Payload

The collector stores normalized and raw CLI output together in
`VTL-RAW-YYYY-MM`. The latest summary overwrites `{collector}:current` in
`VTL-CURRENT`.

```json
{
  "summary": {
    "device_name": "DXi_1",
    "state": "online",
    "capacity": {
      "total_bytes": 100000000000000,
      "used_bytes": 72000000000000,
      "used_percent": 72.0
    },
    "dedup_ratio": 2.35,
    "data_reduction": {
      "total_reduction_ratio": 4.53,
      "deduplication_ratio": 2.35,
      "compression_ratio": 1.93
    },
    "vtls": [
      {"name": "VTL1", "mode": "online", "dedup_enabled": true}
    ],
    "interfaces": [
      {"name": "eth0", "state": "up", "up": 1}
    ],
    "alert_counts": {
      "critical": 0,
      "warning": 1
    }
  },
  "raw": {"cli": {"capacity": "... original CLI output ..."}}
}
```

## Prometheus Metrics

The DXi CLI collector publishes these normalized gauges when values can be
parsed:

```text
backup_device_up{device_type="dxi",device_name="DXi_1"} 1
backup_device_capacity_total_bytes{device_type="dxi",device_name="DXi_1"} 100000000000000
backup_device_capacity_used_bytes{device_type="dxi",device_name="DXi_1"} 72000000000000
backup_device_capacity_used_percent{device_type="dxi",device_name="DXi_1"} 72
backup_device_dedup_ratio{device_type="dxi",device_name="DXi_1"} 2.35
backup_dxi_total_reduction_ratio{device_name="DXi_1"} 4.53
backup_dxi_compression_ratio{device_name="DXi_1"} 1.93
backup_dxi_vtl_online{device_name="DXi_1",vtl="VTL1"} 1
backup_dxi_vtl_dedup_enabled{device_name="DXi_1",vtl="VTL1"} 1
backup_device_alert_count{device_type="dxi",device_name="DXi_1",severity="critical"} 0
backup_device_interface_up{device_type="dxi",device_name="DXi_1",interface="eth0"} 1
backup_collector_last_success_timestamp{collector="DXi_1"} 1710000000
```

## Notes

The parser is intentionally conservative because DXi CLI output varies by
software version. If a command output does not parse, the raw text is still
stored in Elasticsearch so the parser can be adjusted safely from real samples.

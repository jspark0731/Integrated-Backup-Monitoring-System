# DXi CLI Collection

DXi 2.2.1 SNMP documentation exposes device identity and state OIDs, but it does
not provide the full set of dashboard targets from section 7.1 of
`Codex_Implementation_Guide.md`, such as capacity, deduplication ratio,
replication status, interface status, and alert counts.

For those operational values, this project uses the combined DXi CLI + SNMP collector.

## Collection Flow

```text
collector schedule
  -> SNMP get/walk for identity and basic state
  -> SSH login to DXi and run configured CLI commands
  -> capture stdout
  -> parse text into a normalized summary
  -> store raw + summary payload in Elasticsearch
  -> publish normalized Prometheus gauges
```

## Configuration

Use `protocol: cli_snmp` for DXi collection.

```yaml
- name: DXi_1
  type: DXi
  protocol: cli_snmp
  enabled: true
  schedule:
    interval_minutes: 5
    minute_offset: 0
    second: 0
  host: DXi_1_host_TO_BE_FILLED
  snmp_port: 161
  ssh_port: 22
  community: DXi_1_community_TO_BE_FILLED
  version: "2c"
  username: DXi_1_username_TO_BE_FILLED
  password: DXi_1_password_TO_BE_FILLED
  command_timeout: 30
  oids:
    state: 1.3.6.1.4.1.2036.2.1.1.7.0
    serial_number: 1.3.6.1.4.1.2036.2.1.1.12.0
  commands:
    status: "show status"
    capacity: "show capacity"
    dedup: "show dedup"
    replication: "show replication"
    interfaces: "show network"
    alerts: "show alerts"
```

If key-based authentication is preferred, set `ssh_key_path` instead of
`password`.

```yaml
ssh_key_path: /run/secrets/dxi_ssh_key
```

The command names on the left are parser inputs. The command strings on the
right can be adjusted to match the actual DXi CLI.

## Parsed Payload

The collector stores both normalized and raw output in
`backup-dxi-summary-YYYY.MM`.

DXi SNMP identity/state results are stored separately in
`backup-dxi-status-YYYY.MM`.

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
    "dedup_ratio": 12.5,
    "replication": [
      {"name": "target-a", "state": "enabled", "up": 1}
    ],
    "interfaces": [
      {"name": "eth0", "state": "up", "up": 1}
    ],
    "alert_counts": {
      "critical": 0,
      "warning": 1
    }
  },
  "raw": {
    "capacity": "... original CLI output ..."
  }
}
```

## Prometheus Metrics

The DXi CLI + SNMP collector publishes these normalized gauges when values can be
parsed:

```text
backup_device_up{device_type="dxi",device_name="DXi_1"} 1
backup_device_capacity_total_bytes{device_type="dxi",device_name="DXi_1"} 100000000000000
backup_device_capacity_used_bytes{device_type="dxi",device_name="DXi_1"} 72000000000000
backup_device_capacity_used_percent{device_type="dxi",device_name="DXi_1"} 72
backup_device_dedup_ratio{device_type="dxi",device_name="DXi_1"} 12.5
backup_device_alert_count{device_type="dxi",device_name="DXi_1",severity="critical"} 0
backup_device_replication_up{device_type="dxi",device_name="DXi_1",replication="target-a"} 1
backup_device_interface_up{device_type="dxi",device_name="DXi_1",interface="eth0"} 1
backup_collector_last_success_timestamp{collector="DXi_1"} 1710000000
```

## Notes

The parser is intentionally conservative because DXi CLI output varies by
software version. If a command output does not parse, the raw text is still
stored in Elasticsearch so the parser can be adjusted safely from real samples.

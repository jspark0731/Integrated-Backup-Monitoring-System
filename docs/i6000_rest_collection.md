# i6000 REST Collection

i6000 collection uses the Scalar i6000 RESTful Web Services API only. The REST
API covers the former SNMP status points and also exposes richer slot, media,
and RAS ticket data, so operators only need to maintain one access method for
i6000 devices.

## Flow

```text
reused HTTP connection pool
  -> POST aml/users/login
  -> fast or slow endpoints in parallel, max_concurrency=4
  -> keep successful payloads when one endpoint fails
  -> DELETE aml/users/login
```

The collector requests JSON with `Accept: application/json`, but it can also
parse XML responses. Login/logout occurs for every collection cycle while the
HTTP client and TCP/TLS connections are reused for the application lifespan.

## Configuration

Use `protocol: rest` with `type: i6000`. The base URL is the library Web
Services root host; endpoints are relative paths under that host.

```yaml
- name: i6000_core_rest
  type: i6000
  protocol: rest
  enabled: true
  schedule:
    fast:
      interval_minutes: 5
      minute_offset: 2
      second: 0
    slow:
      interval_minutes: 60
      minute_offset: 2
      second: 30
  base_url: https://i6000_core.example.com
  username: admin
  password: secret
  verify_tls: true
  rest:
    max_concurrency: 4
  endpoints:
    ping: aml/
    physical_library: aml/physicalLibrary
    status: aml/physicalLibrary/status
    drives: aml/drives
    media: aml/media?start=0&length=-1
    segments_storage_used: aml/physicalLibrary/segments?type=storage&status=used&start=0&length=-1
    segments_storage_available: aml/physicalLibrary/segments?type=storage&status=available&start=0&length=-1
    towers: aml/devices/towers
    ie_stations: aml/devices/ieStations
    ras_status: aml/system/ras
    ras_tickets: aml/system/ras/tickets
```

Set `verify_tls: false` only when the library uses a certificate that the
collector trust store cannot validate.

Fast collection includes reachability, library/drive/RAS status, RAS tickets,
and storage slot counts. Slow collection includes physical library, media,
tower, and I/E station inventory. Partial endpoint failures are recorded in
`payload.endpoint_errors`.

## Prometheus

The collector publishes normalized tape metrics when values can be extracted:

```text
backup_device_up{device_type="i6000",device_name="..."} 1
backup_tape_library_status{device_name="..."} 1
backup_tape_robot_status{device_name="...",robot="..."} 1
backup_tape_drive_status{device_name="...",drive="..."} 1
backup_tape_drive_error_count{device_name="...",drive="..."} 0
backup_tape_slot_used_count{device_name="..."} 120
backup_tape_slot_free_count{device_name="..."} 30
backup_tape_media_count{device_name="..."} 150
```

## Elasticsearch

Fast and slow raw results are stored in `PTL-RAW-YYYY-MM` with
`collection_class`. Fast results also overwrite `{collector}:current` in
`PTL-CURRENT`. Slow inventory does not overwrite the current operational
status.

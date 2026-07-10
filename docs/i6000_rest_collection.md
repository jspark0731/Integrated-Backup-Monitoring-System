# i6000 REST Collection

i6000 collection uses the Scalar i6000 RESTful Web Services API only. The REST
API covers the former SNMP status points and also exposes richer slot, media,
and RAS ticket data, so operators only need to maintain one access method for
i6000 devices.

## Flow

```text
POST aml/users/login
  -> receive session cookie
GET aml/
GET aml/physicalLibrary
GET aml/physicalLibrary/status
GET aml/drives
GET aml/media?start=0&length=-1
GET aml/physicalLibrary/segments?type=storage&status=used&start=0&length=-1
GET aml/physicalLibrary/segments?type=storage&status=available&start=0&length=-1
GET aml/devices/towers
GET aml/devices/ieStations
GET aml/system/ras
GET aml/system/ras/tickets
DELETE aml/users/login
```

The collector requests JSON with `Accept: application/json`, but it can also
parse XML responses. The raw endpoint payloads and the normalized `summary`
are both stored in Elasticsearch.

## Configuration

Use `protocol: rest` with `type: i6000`. The base URL is the library Web
Services root host; endpoints are relative paths under that host.

```yaml
- name: i6000_core_rest
  type: i6000
  protocol: rest
  enabled: true
  schedule:
    interval_minutes: 5
    minute_offset: 2
    second: 0
  base_url: https://i6000_core.example.com
  username: admin
  password: secret
  verify_tls: true
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

i6000 REST results are expanded into three documents so dashboards can query by
purpose:

```text
backup-i6000-status-YYYY.MM
backup-i6000-drive-YYYY.MM
backup-i6000-media-YYYY.MM
```

Each REST document contains the same collector document plus
`document_type: status`, `document_type: drive`, or `document_type: media`.

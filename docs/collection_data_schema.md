# 수집 데이터 스키마 및 모델

> 기준 코드: `app/models.py`, `app/writers/elasticsearch.py`, `app/processors/derived.py`,
> `app/parsers/*`, `app/collectors/*`, `app/core/metrics.py`  
> 작성 기준일: 2026-07-25

## 1. 데이터 흐름

```text
DD / DXi / i6000 / NetWorker / ZFS
                 │
                 ▼
        Collector + Parser
          ├─ Elasticsearch ── raw / current / derived ──┐
          │                                             │
          └─ /metrics ─────── Prometheus ───────────────┤
                                                        ▼
                                                     Grafana
```

- **Grafana는 Elasticsearch와 Prometheus를 모두 datasource로 사용한다.**
- **Elasticsearch**는 목록, 이력, 원본, 스냅샷, 집계 및 보고서성 데이터를
  제공한다. 수집 1회마다 이력 보존용 `raw` 문서와 최신 상태용 `current`
  문서가 같은 일자별 인덱스에 저장된다.
- **Prometheus**는 장비 상태, 용량 사용률, 성공/실패 건수처럼 즉시 갱신되는
  숫자형 시계열을 제공한다. 수집기가 `/metrics`에 노출한 값을 Prometheus가
  scrape한다.
- Collector가 Grafana로 데이터를 직접 push하지는 않는다. Grafana가 두
  datasource를 각각 조회한다.
- NetWorker는 hostname CSV로 업무영역을 분류한 뒤 엔티티 단위 `derived`
  문서를 영역별 인덱스에 저장한다. 다른 제품군의 derived 변환 모델은 아직
  `ElasticsearchWriter` 저장 경로에 연결되어 있지 않다.

### 1.1 Datasource 역할

| Grafana 용도 | Datasource | 대표 데이터 |
|---|---|---|
| Stat / Gauge | Prometheus | 장비 up/down, 사용률, alert 수, job 수 |
| Time series | Prometheus | 수집 성공률, 수집 시간, 용량 및 상태 변화 |
| Alert rule | Prometheus | collector 실패, 장비 장애, 임계 사용률 |
| Table | Elasticsearch | job, client, policy, workflow, drive, pool, event 목록 |
| Logs / 상세 조회 | Elasticsearch | 수집 원본, 오류, 장비별 상세 payload |
| 보고서 | Elasticsearch | 월간 백업량, inventory snapshot, client diff |

### 1.2 현재 구현 범위

| 경로 | 상태 | 설명 |
|---|---|---|
| Collector → Prometheus | 구현됨 | `/metrics`에 Gauge, Counter, Histogram 노출 |
| Prometheus → Grafana | 인프라 설정 영역 | Grafana에 Prometheus datasource 등록 필요 |
| Collector → Elasticsearch `raw` | 구현됨 | 정규화 payload와 원천 응답 저장 |
| Collector → Elasticsearch `current` | 구현됨 | collector별 최신 summary 저장 |
| NetWorker `raw` → Elasticsearch `derived` | 구현됨 | 업무영역·엔티티별 인덱스에 저장 |
| 기타 제품 `raw` → Elasticsearch `derived` | 변환 코드만 존재 | writer 또는 별도 처리 작업에 연결되지 않음 |
| Elasticsearch → Grafana | 인프라 설정 영역 | Grafana에 인덱스 패턴별 datasource 등록 필요 |

> NetWorker Table/보고서 패널은 영역별 derived 인덱스를 직접 사용할 수 있다.
> i6000/ZFS 등의 엔티티별 derived 문서는 아직 자동 적재되지 않으므로
> `raw.payload.*` 또는 `current.summary.*`를 직접 조회해야 한다.

## 2. Elasticsearch 인덱스

### 2.1 이름 규칙

장비 데이터는 `{FAMILY}-{SEGMENT}-{YYYY-MM-DD}-1`, NetWorker 분류 데이터는
별도의 월별 규칙을 사용한다.

| 대상 | FAMILY | SEGMENT 규칙 | 예시 |
|---|---|---|---|
| DD | `VTL` | collector 이름 대문자 | `VTL-DD6900_1-2026-07-25-1` |
| DXi | `VTL` | collector 이름 대문자 | `VTL-DXI_1-2026-07-25-1` |
| i6000 | `PTL` | collector 이름의 사이트 토큰 | `PTL-CORE-2026-07-25-1` |
| ZFS | `ZFS` | 대문자화 후 `ZFS_` 제거 | `ZFS-1-2026-07-25-1` |

**NetWorker 인덱스**

| 데이터 | 이름 규칙 | 예시 | 접근 대상 |
|---|---|---|---|
| Raw | `NW-OPS-RAW-{SOURCE}-{YYYY-MM}` | `NW-OPS-RAW-CHNL-2026-07` | 백업 운영자 |
| Current | `NW-OPS-CURRENT-{SOURCE}-{YYYY-MM}` | `NW-OPS-CURRENT-CHNL-2026-07` | 백업 운영자 |
| Derived | `NW-{DOMAIN}-{ENTITY}-{YYYY-MM}` | `NW-CORE-JOB-2026-07` | 해당 업무영역 운영자 |

`SOURCE`는 호출한 NetWorker 서버의 `core/chnl/info/ifrs`이고, `DOMAIN`은
hostname CSV로 판정한 백업 대상 서버의 업무영역이다. 예를 들어 CHNL
NetWorker가 백업하는 CORE 서버의 job은 `NW-CORE-JOB-*`에 저장되며 문서의
`source_networker`는 `chnl`이다. 미분류 문서는 `NW-UNMAPPED-{ENTITY}-*`에
격리한다.

### 2.2 공통 타입 표기

| 표기 | 의미 / 권장 Elasticsearch 타입 |
|---|---|
| `datetime` | ISO 8601 UTC 시각 / `date` |
| `string` | 식별·집계 필드는 `keyword`, 본문 검색 필드는 `text` |
| `integer` | 정수 / `long` |
| `number` | 실수 포함 숫자 / `double` |
| `boolean` | `boolean` |
| `object` | JSON 객체 / `object` |
| `array<T>` | T의 배열. 객체 배열은 쿼리 방식에 따라 `nested` 검토 |
| `any` | 원천 API/SNMP 값에 따라 타입이 달라질 수 있음 |
| `nullable` | 값이 없으면 `null` 가능 |

## 3. Elasticsearch 문서 모델

### 3.1 Raw 문서 — 실제 저장

수집 시점별 전체 payload를 보존한다.

| 필드 | 타입 | 설명 |
|---|---|---|
| `@timestamp` | `datetime` | 수집 완료 시각(UTC) |
| `raw_document_id` | `string` | `{collector}:raw:{YYYYMMDDTHHMMSS.ffffffZ}` |
| `collector` | `string` | 설정된 수집기 이름 |
| `target_type` | `string` | `DD`, `DXi`, `i6000`, `Networker`, `ZFS` |
| `solution` | `string` | `dd`, `dxi`, `i6000`, `networker`, `zfs` |
| `protocol` | `string` | `snmp`, `cli_snmp`, `rest` 등 |
| `ok` | `boolean` | 수집 성공 여부 |
| `payload` | `object` | 대상별 정규화 데이터와 원본 데이터 |
| `error` | `string nullable` | 실패 메시지 |
| `skipped` | `boolean` | 설정 미완성 등으로 수집을 건너뛰었는지 여부 |
| `skip_reason` | `string nullable` | 건너뛴 이유 |
| `document_family` | `string` | 고정값 `raw` |
| `document_type` | `string` | 고정값 `collection` |
| `processing_mode` | `string` | 고정값 `elt` |

```json
{
  "@timestamp": "2026-07-25T01:23:45.123456+00:00",
  "raw_document_id": "DXi_1:raw:20260725T012345.123456Z",
  "collector": "DXi_1",
  "target_type": "DXi",
  "solution": "dxi",
  "protocol": "cli_snmp",
  "ok": true,
  "payload": {
    "summary": {},
    "raw": {}
  },
  "error": null,
  "skipped": false,
  "skip_reason": null,
  "document_family": "raw",
  "document_type": "collection",
  "processing_mode": "elt"
}
```

### 3.2 Current 문서 — 실제 저장

collector별 최신 요약 상태를 제공한다. 문서 ID가 고정되어 같은 일자 인덱스
안에서는 새 수집 결과가 이전 값을 덮어쓴다.

| 필드 | 타입 | 설명 |
|---|---|---|
| `@timestamp` | `datetime` | 최신 수집 완료 시각 |
| `current_document_id` | `string` | `{collector}:current` |
| `collector` | `string` | 수집기 이름 |
| `target_type` | `string` | 대상 제품군 |
| `solution` | `string` | 정규화된 솔루션명 |
| `protocol` | `string` | 수집 프로토콜 |
| `ok` | `boolean` | 수집 성공 여부 |
| `error` | `string nullable` | 실패 메시지 |
| `skipped` | `boolean` | 수집 생략 여부 |
| `skip_reason` | `string nullable` | 생략 사유 |
| `document_family` | `string` | 고정값 `current` |
| `document_type` | `string` | 고정값 `status` |
| `processing_mode` | `string` | 고정값 `etl` |
| `summary` | `object` | 아래 대상별 `payload.summary` |

> 실패 또는 생략 결과에서는 payload가 비어 있으므로 `summary`는 `{}`이다.

### 3.3 Derived 문서

| 필드 | 타입 | 설명 |
|---|---|---|
| `@timestamp` | `datetime` | 원본 수집 시각 |
| `collector` | `string` | 원본 collector |
| `target_type` | `string` | 원본 대상 제품군 |
| `solution` | `string` | 정규화된 솔루션명 |
| `document_family` | `string` | 고정값 `derived` |
| `document_type` | `string` | 아래 엔티티 타입 |
| `processing_mode` | `string` | 고정값 `elt` |
| `source_raw_id` | `string` | 원본 `_id` 또는 `raw_document_id` |
| `record_id` | `string` | 엔티티 식별 필드 조합 |
| `derived_id` | `string` | `{collector}:{document_type}:{record_id}:{YYYY-MM}` |
| `source_networker` | `string nullable` | 원천 NetWorker 서버; NetWorker 문서에서 사용 |
| `security_domain` | `string nullable` | CSV로 판정한 업무영역 |
| `payload` | `object` | 해당 엔티티 레코드 |

| 대상 | 생성 가능한 `document_type` | `record_id` 우선순위 |
|---|---|---|
| DD / DXi | `summary` | `device_name`, `server`, `name`, collector |
| i6000 | `summary`, `drive`, `media`, `partition`, `robot` | 장비별 `serial_number`, `barcode`, `name` |
| NetWorker | `job`, `client`, `policy`, `workflow`, `monthly-report` | 각 엔티티 ID/이름 조합 |
| ZFS | `summary`, `pool`, `project`, `filesystem`, `lun`, `event` | 이름, 상위 경로, 이벤트 시각/요약 조합 |

## 4. 대상별 Elasticsearch Payload

아래 모델은 `raw.payload`의 정규화 영역이다. `raw.payload.raw` 및
DD/DXi의 `raw.payload.snmp`는 원천 응답을 그대로 담으므로 별도 고정 스키마를
적용하지 않는다.

### 4.1 DD

```text
payload
├─ summary: DD Summary
├─ snmp: object (원천 SNMP key/value)
└─ raw.snmp: object (원천 SNMP key/value)
```

**DD Summary**

| 필드 | 타입 |
|---|---|
| `device_name` | `string` |
| `serial_number`, `model`, `version`, `file_system_status` | `string nullable` |
| `capacity.total_bytes`, `capacity.used_bytes`, `capacity.available_bytes` | `number nullable` |
| `capacity.used_percent` | `number nullable` |
| `dedup_ratio`, `reduction_percent` | `number nullable` |
| `alert_counts` | `object<string, integer>` |
| `replication` | `array<Replication>` |
| `ddboost` | `DD Boost` |

**Replication**

| 필드 | 타입 |
|---|---|
| `instance` | `string` |
| `source`, `destination`, `state`, `status` | `string nullable` |
| `name` | `string` |
| `up` | `integer` (`0` 또는 `1`) |

**DD Boost**

| 필드 | 타입 |
|---|---|
| `enabled` | `boolean nullable` |
| `status` | `string nullable` |
| `throughput_kbps.pre_compression` | `number nullable` |
| `throughput_kbps.post_compression` | `number nullable` |
| `throughput_kbps.network`, `throughput_kbps.read` | `number nullable` |
| `connections.backup`, `connections.restore` | `number nullable` |
| `compression_ratio` | `number nullable` |
| `ifgroups` | `array<{instance, name?, status?}>` |
| `users` | `array<{instance, name?, tenant_unit?}>` |
| `storage_units` | `array<DD Boost Storage Unit>` |

**DD Boost Storage Unit**

| 필드 | 타입 |
|---|---|
| `instance`, `name`, `status`, `user` | `string nullable` |
| `bytes`, `metadata_bytes`, `pre_compression_bytes` | `number nullable` |
| `report_physical_size`, `bytes_hc` | `number nullable` |
| `global_compression`, `local_compression` | `number nullable` |

### 4.2 DXi

```text
payload
├─ summary: DXi Summary
├─ snmp: object (원천 SNMP key/value)
└─ raw
   ├─ snmp: object
   └─ cli: object<string, string>
```

| DXi Summary 필드 | 타입 |
|---|---|
| `device_name` | `string` |
| `state` | `string nullable` |
| `capacity.total_bytes`, `capacity.used_bytes` | `number nullable` |
| `capacity.used_percent` | `number nullable` |
| `dedup_ratio` | `number nullable` |
| `replication` | `array<Named State>` |
| `interfaces` | `array<Named State>` |
| `alert_counts` | `object<string, integer>` |

`Named State`는 `name: string`, `state: string`, `up: integer(0|1)`로 구성된다.

### 4.3 i6000

```text
payload
├─ summary: i6000 Summary
└─ raw: object (REST endpoint별 원천 응답)
```

| i6000 Summary 필드 | 타입 |
|---|---|
| `device_name`, `product_name`, `serial_number` | `string` |
| `firmware_version`, `vendor` | `string` |
| `library_state`, `library_mode`, `library_status` | `integer nullable` |
| `snmp_started` | `boolean nullable` |
| `ras_status` | `array<{group: integer nullable, status: integer nullable}>` |
| `ras_opened_tickets` | `integer nullable` |
| `robots` | `array<Robot>` |
| `partitions` | `array<Partition>` |
| `drives` | `array<Drive>` |
| `towers` | `array<Tower>` |
| `library_main_door_open` | `boolean` |
| `ie_stations` | `array<IE Station>` |
| `slot_used_count`, `slot_free_count`, `media_count` | `integer` |
| `ras_ticket_counts` | `object<string, integer>` |

| 객체 | 필드 |
|---|---|
| `Robot` | `name: string`, `status/state: integer nullable`, `up: integer(0\|1)` |
| `Partition` | `name: string`, `mode/type: integer nullable` |
| `Drive` | `name/serial_number/model: string`, `mode/state: integer nullable`, `up: integer(0\|1)`, `error_count: integer` |
| `Tower` | `name/serial_number: string`, `mode/state/status: integer nullable`, `door_opened: boolean nullable` |
| `IE Station` | `name: string`, `status/state/mode/lock: integer nullable` |

> 현재 collector payload에는 별도 `media` 배열이 없고 `summary.media_count`만
> 저장된다. 따라서 derived 모델의 `media` 문서는 현재 payload만으로는 생성되지 않는다.

### 4.4 NetWorker

```text
payload
├─ summary: NetWorker Summary
├─ jobs: array<Job>
├─ clients: array<Client>
├─ policies: array<Policy>
├─ workflows: array<Workflow>
├─ monthly_report: array<Monthly Report>
└─ raw: object (REST endpoint별 원천 응답)
```

**NetWorker Summary**

| 필드 | 타입 |
|---|---|
| `server` | `string` |
| `source_networker` | `string` |
| `client_count_by_domain` | `object<string, integer>` |
| `job_count`, `client_count`, `policy_count` | `integer` |
| `workflow_count`, `backup_count`, `total_backup_bytes` | `integer` |
| `job_success_count_by_policy` | `object<string, integer>` |
| `job_failed_count_by_policy` | `object<string, integer>` |
| `job_running_count_by_policy` | `object<string, integer>` |
| `workflow_count_by_policy` | `object<string, integer>` |
| `recent_failed_jobs` | `array<Job>` (최대 20건) |

**Job**

| 필드 | 타입 |
|---|---|
| `server` | `string` |
| `source_networker`, `client_hostname`, `security_domain` | `string` |
| `classification_status`, `classification_source` | `string` |
| `job_id` | `any nullable` |
| `name`, `type`, `state`, `status` | `string` |
| `exit_code` | `integer nullable` |
| `policy_name`, `workflow_name`, `client_name`, `run_on_host` | `string` |
| `start_time`, `end_time`, `stopped` | `any nullable` |
| `root_parent_job_id`, `parent_job_id` | `any nullable` |

`status` 정규화 값은 `success`, `failed`, `running`, `unknown`이다.

**Client**

| 필드 | 타입 |
|---|---|
| `server`, `client_id`, `client_name`, `client_os` | `string` |
| `source_networker`, `client_hostname`, `security_domain` | `string` |
| `classification_status`, `classification_source` | `string` |
| `client_os_family` | `string` (`AIX`, `Linux`, `Windows`, `Other`, `Unknown`) |
| `backup_type` | `string` |
| `scheduled_backup`, `parallelism` | `any nullable` |
| `save_sets`, `protection_groups`, `storage_nodes`, `aliases` | `array<any>` |

**Policy / Workflow**

| 객체 | 필드 |
|---|---|
| `Policy` | `server`, `policy_name`, `comment`, `resource_id`, `source_networker`, `security_domain`: string; `workflow_count`: integer |
| `Workflow` | `server`, `policy_name`, `workflow_name`, `source_networker`, `security_domain`: string; `enabled`: any; `action_count`: integer; `actions`: array<string>; `protection_groups`: array<any>; `start_time`, `end_time`: any |

**Monthly Report**

| 필드 | 타입 |
|---|---|
| `server`, `month`, `policy_name`, `workflow_name` | `string` |
| `source_networker`, `security_domain`, `classification_status` | `string` |
| `total_backup_bytes` | `integer` |
| `total_backup_tb` | `number` |
| `job_success_count`, `job_failed_count`, `job_running_count` | `integer` |
| `workflow_count`, `client_count` | `integer` |

원천 backup 레코드는 정규화 과정에서 집계에만 사용되며 `payload.backups`에는
포함되지 않는다.

### 4.5 ZFS

```text
payload
├─ summary: ZFS Summary
├─ pools: array<Pool>
├─ projects: array<Project>
├─ filesystems: array<Filesystem>
├─ luns: array<LUN>
├─ alerts: array<Event>
└─ raw: object (REST endpoint별 원천 응답)
```

**ZFS Summary**

| 필드 | 타입 |
|---|---|
| `device_name`, `product`, `os_version`, `serial_number` | `string` |
| `pool_count`, `project_count`, `filesystem_count`, `lun_count` | `integer` |
| `alert_count`, `fault_count` | `integer` |
| `total_bytes`, `used_bytes`, `free_bytes` | `number` |
| `used_percent` | `number nullable` |

**Pool**

| 필드 | 타입 |
|---|---|
| `device_name`, `name`, `state`, `profile`, `owner`, `peer` | `string` |
| `asn`, `scrub_schedule` | `string` |
| `up` | `integer` (`0` 또는 `1`) |
| `total_bytes`, `used_bytes`, `free_bytes`, `used_percent` | `number nullable` |
| `compression`, `dedup_ratio`, `snapshot_bytes`, `replication_bytes` | `number nullable` |

**Project / Filesystem / LUN / Event**

| 객체 | 필드 |
|---|---|
| `Project` | `device_name`, `pool`, `name`, `mountpoint`, `creation`, `sharenfs`, `sharesmb`: string; `dedup`: any; `quota`, `reservation`: number nullable |
| `Filesystem` | `device_name`, `pool`, `project`, `name`, `mountpoint`: string; `quota`, `reservation`: number nullable; `usage`: object |
| `LUN` | `device_name`, `pool`, `project`, `name`, `id`, `status`: string; `volsize`: number nullable; `sparse`: any; `usage`: object |
| `Event` | `device_name`, `severity`, `timestamp`, `summary`, `user`, `address`, `annotation`: string |

`Event.severity`는 원천 종류에 따라 `alert` 또는 `fault`이다.

## 5. Prometheus 시계열 모델

Prometheus 타입은 코드 기준으로 `Counter`, `Gauge`, `Histogram`이다.
Grafana의 Prometheus datasource 쿼리에서 `{label="value"}` 형태로 아래
라벨을 사용한다.

### 5.1 수집기 공통

| 메트릭 | 타입 | 라벨 | 값 / 의미 |
|---|---|---|---|
| `backup_collector_collection_total` | Counter | `collector,target_type,protocol,status` | 수집 시도 누계; status=`success/error/skipped` |
| `backup_collector_collection_duration_seconds` | Histogram | `collector,target_type,protocol` | 수집 소요 시간(초) |
| `backup_collector_skipped` | Gauge | `collector,target_type,reason` | 생략 상태, `1`=생략 |
| `backup_collector_elasticsearch_write_total` | Counter | `status` | ES write 누계; `success/error/skipped` |
| `backup_collector_last_success_timestamp` | Gauge | `collector` | 마지막 성공 Unix timestamp |

Histogram은 Prometheus에서 자동으로 `_bucket`, `_sum`, `_count` 시계열을 만든다.
Counter는 노출 시 `_total` 이름을 유지하며 런타임에 `_created`가 추가될 수 있다.

### 5.2 공통 장비(DD / DXi 중심)

| 메트릭 | 타입 | 라벨 | 단위 / 값 |
|---|---|---|---|
| `backup_device_up` | Gauge | `device_type,device_name` | `1`=수집 성공/도달 가능 |
| `backup_device_capacity_total_bytes` | Gauge | `device_type,device_name` | bytes |
| `backup_device_capacity_used_bytes` | Gauge | `device_type,device_name` | bytes |
| `backup_device_capacity_used_percent` | Gauge | `device_type,device_name` | percent |
| `backup_device_dedup_ratio` | Gauge | `device_type,device_name` | ratio |
| `backup_device_alert_count` | Gauge | `device_type,device_name,severity` | 건수 |
| `backup_device_replication_up` | Gauge | `device_type,device_name,replication` | `1`=정상/활성 |
| `backup_device_interface_up` | Gauge | `device_type,device_name,interface` | `1`=up |

`device_type`은 현재 `dd`, `dxi`, `i6000` 중 해당 collector가 발행하는 값을
사용한다. i6000은 `backup_device_up`만 공통 장비 메트릭으로 발행한다.

### 5.3 DD Boost

| 메트릭 | 타입 | 라벨 | 단위 / 값 |
|---|---|---|---|
| `backup_dd_ddboost_up` | Gauge | `device_name` | `1`=enabled |
| `backup_dd_ddboost_connections` | Gauge | `device_name,direction` | 연결 수; direction=`backup/restore` |
| `backup_dd_ddboost_throughput_kbps` | Gauge | `device_name,stream` | KiB/s; stream=`pre_compression/post_compression/network/read` |
| `backup_dd_ddboost_storage_unit_bytes` | Gauge | `device_name,storage_unit,size_type` | bytes 계열 값 |
| `backup_dd_ddboost_storage_unit_compression` | Gauge | `device_name,storage_unit,compression_type` | 압축 계수 |

`size_type`은 `bytes`, `metadata_bytes`, `pre_compression_bytes`,
`report_physical_size`, `bytes_hc` 중 하나이고, `compression_type`은
`global_compression` 또는 `local_compression`이다.

### 5.4 i6000 Tape Library

| 메트릭 | 타입 | 라벨 | 값 |
|---|---|---|---|
| `backup_tape_library_status` | Gauge | `device_name` | `1`=ready/online |
| `backup_tape_robot_status` | Gauge | `device_name,robot` | `1`=정상 |
| `backup_tape_drive_status` | Gauge | `device_name,drive` | `1`=정상 |
| `backup_tape_drive_error_count` | Gauge | `device_name,drive` | open drive RAS ticket 수 |
| `backup_tape_slot_used_count` | Gauge | `device_name` | 사용 슬롯 수 |
| `backup_tape_slot_free_count` | Gauge | `device_name` | 가용 슬롯 수 |
| `backup_tape_media_count` | Gauge | `device_name` | 미디어 수 |

### 5.5 NetWorker

| 메트릭 | 타입 | 라벨 | 값 |
|---|---|---|---|
| `backup_networker_api_up` | Gauge | `server` | `1`=REST API 도달 가능 |
| `backup_networker_job_success_count` | Gauge | `server,policy,security_domain` | 성공 job 수 |
| `backup_networker_job_failed_count` | Gauge | `server,policy,security_domain` | 실패 job 수 |
| `backup_networker_job_running_count` | Gauge | `server,policy,security_domain` | 실행 중 job 수 |
| `backup_networker_workflow_count` | Gauge | `server,policy,security_domain` | workflow 수 |
| `backup_networker_client_count` | Gauge | `server,security_domain` | 고유 client 수 |

### 5.6 ZFS

| 메트릭 | 타입 | 라벨 | 값 |
|---|---|---|---|
| `backup_zfs_api_up` | Gauge | `device_name` | `1`=REST API 도달 가능 |
| `backup_zfs_pool_status` | Gauge | `device_name,pool` | `1`=online |
| `backup_zfs_capacity_used_percent` | Gauge | `device_name,pool` | 사용률(%) |
| `backup_zfs_alert_count` | Gauge | `device_name,severity` | 건수; severity=`alert/fault` |

## 6. Grafana Datasource 및 쿼리 예시

### 6.1 Elasticsearch datasource

Grafana에서 대상별 인덱스 패턴을 datasource에 설정한다.

| 대상 | Index pattern 예시 | Time field |
|---|---|---|
| DD / DXi | `VTL-*` | `@timestamp` |
| i6000 | `PTL-*` | `@timestamp` |
| NetWorker 기간계 | `NW-CORE-*` | `@timestamp` |
| NetWorker 채널계 | `NW-CHNL-*` | `@timestamp` |
| NetWorker 정보계 | `NW-INFO-*` | `@timestamp` |
| NetWorker 대외계 | `NW-IFRS-*` | `@timestamp` |
| NetWorker 백업 운영 | `NW-OPS-*`, `NW-UNMAPPED-*` | `@timestamp` |
| ZFS | `ZFS-*` | `@timestamp` |

영역별 Grafana datasource 계정에 적용할 Elasticsearch role 예시는
`config/elasticsearch_roles.example.json`에 있다. 기간계 계정은
`NW-CORE-*`만, 채널계 계정은 `NW-CHNL-*`만 읽을 수 있으며 장비 인덱스와
`NW-OPS-*`는 백업 운영 계정만 읽도록 구성한다.

모든 대상 인덱스를 하나의 datasource로 묶어야 한다면 `VTL-*,PTL-*,NW-*,ZFS-*`
같은 멀티 패턴의 지원 여부를 Grafana/Elasticsearch 버전에서 확인하고,
지원되지 않으면 대상별 datasource로 분리한다.

현재 저장 문서를 조회하는 Grafana Elasticsearch 쿼리 예시는 다음과 같다.

| 패널 | Query / 필터 | 표시 필드 또는 집계 |
|---|---|---|
| 최신 장비 상태 Table | `document_family:"current"` | `collector`, `target_type`, `ok`, `summary.*` |
| 수집 실패 이력 Table | `document_family:"raw" AND ok:false` | `@timestamp`, `collector`, `error` |
| NetWorker 최근 실패 job | `document_type:"job" AND payload.status:"failed"` | `payload.*` |
| ZFS pool 목록 | `document_family:"raw" AND target_type:"ZFS"` | `payload.pools.*` |
| 장비별 수집 건수 | `document_family:"raw"` | Terms=`collector`, Metric=`Count` |

> NetWorker는 derived 문서를 실제 적재하므로 업무영역별 datasource에서 바로
> 조회할 수 있다. 아직 derived 저장이 연결되지 않은 다른 제품의 객체 배열은
> Grafana Table에서 직접 펼치는 데 제약이 있다.

NetWorker derived 쿼리와 향후 다른 제품의 권장 쿼리 형태:

```text
# NetWorker 실패 job Table
document_family:"derived" AND document_type:"job" AND payload.status:"failed"

# ZFS pool Table
document_family:"derived" AND document_type:"pool"

# i6000 drive Table
document_family:"derived" AND document_type:"drive"
```

### 6.2 Prometheus datasource

```promql
# 장비 가용성
backup_device_up

# 최근 5분 수집 실패율
sum(rate(backup_collector_collection_total{status="error"}[5m]))
/
sum(rate(backup_collector_collection_total[5m]))

# collector별 p95 수집 시간
histogram_quantile(
  0.95,
  sum by (le, collector) (
    rate(backup_collector_collection_duration_seconds_bucket[5m])
  )
)

# ZFS pool 사용률
backup_zfs_capacity_used_percent

# NetWorker 정책별 실패 job
backup_networker_job_failed_count
```

## 7. 모델링 시 주의사항

1. 원천 `raw` 객체는 장비 펌웨어/API 응답에 따라 필드와 타입이 달라질 수 있다.
2. NetWorker 이외 제품은 Elasticsearch에서 `raw`와 `current`가 같은
   인덱스를 사용하므로
   Grafana Elasticsearch 쿼리에 `document_family` 필터를 항상 명시하는 것이
   안전하다.
3. 일자가 바뀌면 새 인덱스에 새 `current` 문서가 생기므로 전체 기간 검색에서
   진짜 최신 상태를 얻으려면 collector별 `@timestamp` 최댓값을 선택해야 한다.
4. `alert_counts`, 정책별 count 객체처럼 key가 동적으로 늘어나는 필드는
   Elasticsearch mapping explosion을 피하려면 `flattened` 타입이 적합하다.
5. `payload.raw`처럼 구조가 불안정한 원천 데이터는 검색이 필요 없다면
   `enabled: false` object로 저장하는 방안을 권장한다.
6. Prometheus label에는 client ID, job ID 같은 고카디널리티 값을 사용하지
   않고 있으며, 현재 모델에서도 이를 유지하는 것이 좋다.
7. Grafana Stat/Time series/Alert 패널은 Prometheus를 우선 사용하고,
   목록·상세·보고서 Table 패널은 Elasticsearch를 사용하는 것이 현재 설계의
   역할 분리에 맞다.

# 백업 통합 대시보드 코드 작성 지시서

## 1. 프로젝트 목적

백업 운영 관점에서 주요 백업 장비와 솔루션의 상태, 용량, 성능, 백업 운영 지표를 주기적으로 수집하고 정제하여 Grafana 대시보드에서 볼 수 있도록 한다.

대상 시스템은 다음과 같다.

| 구분      | 대상                          | 수집 방식    | 주기     |
| ------- | --------------------------- | -------- | ------ |
| VTL     | DXi                         | SNMP     | 매분 00초 |
| VTL     | Data Domain DD4500 / DD6900 | SNMP     | 매분 00초 |
| PTL     | Quantum i6000               | REST API | 매분 15초 |
| 백업 솔루션  | Dell EMC NetWorker          | REST API | 매분 30초 |
| 백업 스토리지 | ZFS Appliance               | REST API | 매분 30초 |

## 2. 기본 아키텍처

전체 구조는 다음을 기준으로 작성한다.

```text
[DXi / DD / i6000 / NetWorker / ZFS]
        ↓
[Python Collector Pods]
        ↓
[정제 / 변환 Layer]
        ↓
[Prometheus + Elasticsearch]
        ↓
[Grafana Dashboard]
```

### 저장소 역할 분리

#### Prometheus

Prometheus에는 장비 상태, 헬스 체크, 시계열 메트릭을 저장한다.

예시:

* 장비 up/down
* SNMP 응답 여부
* REST API 응답 여부
* CPU / Memory / Capacity / Throughput / Error Count
* Tape Library 상태
* Drive 상태
* Data Domain / DXi 상태
* Collector 자체 상태

Prometheus는 가능하면 직접 write 방식이 아니라 exporter 방식으로 구현한다.

즉, 각 collector 또는 metrics service가 `/metrics` 엔드포인트를 제공하고 Prometheus가 scrape하는 구조를 기본으로 한다.

#### Elasticsearch

Elasticsearch에는 정제된 운영 지표, 집계 결과, 목록성 데이터, 보고서성 데이터를 저장한다.

예시:

* 월 백업량
* Policy / Workflow / Action 기준 집계
* 클라이언트 목록
* 신규 클라이언트 / 삭제된 클라이언트 비교 결과
* 백업 성공 / 실패 이력 요약
* 장비별 최종 상태 스냅샷
* Grafana Table Panel에서 조회할 데이터
* 운영자가 수치로 확인해야 하는 정제 데이터

## 3. 배포 구조

Kubernetes 환경에 배포할 것을 전제로 한다.

초기에는 단일 노드 Kubernetes 환경을 가정한다.

Collector는 역할별로 분리한다.

```text
namespace: backup-monitoring

pods:
  - backup-api
  - dxi-snmp-collector
  - dd-snmp-collector
  - i6000-rest-collector
  - networker-rest-collector
  - zfs-rest-collector
  - prometheus
  - elasticsearch
  - grafana
```

FastAPI 서버와 Collector는 분리한다.

FastAPI 서버는 다음 역할을 담당한다.

* 현재 상태 조회 API
* 수동 수집 트리거 API
* Collector 상태 조회 API
* Elasticsearch 조회용 API
* Grafana에서 필요한 보조 API 제공 가능

Collector는 다음 역할을 담당한다.

* 정해진 주기에 맞춰 장비에서 데이터 수집
* raw data validation
* normalized data 생성
* Prometheus metric 갱신
* Elasticsearch document 적재

## 4. 코드 작성 원칙

Python 기반으로 작성한다.

다음 원칙을 따른다.

* 장비별 collector 파일을 분리한다.
* 공통 SNMP client, REST client, Elasticsearch writer, metric exporter는 재사용 가능하게 작성한다.
* 설정값은 코드에 하드코딩하지 않는다.
* 장비 IP, 계정, 패스워드, SNMP community, API endpoint는 `.env` 또는 YAML config에서 읽는다.
* 장애가 발생해도 전체 collector가 죽지 않도록 예외 처리를 한다.
* 장비별 수집 실패는 로그와 metric으로 남긴다.
* 로그는 JSON 형태 또는 key-value 형태로 구조화한다.
* 테스트 가능한 구조로 작성한다.
* 실제 장비가 없어도 mock adapter로 단위 테스트가 가능해야 한다.

## 5. 권장 디렉터리 구조

```text
backup-monitoring/
├── README.md
├── CODEX_IMPLEMENTATION_GUIDE.md
├── pyproject.toml
├── .env.example
├── config/
│   ├── devices.yaml
│   ├── oid_map.yaml
│   ├── networker.yaml
│   └── zfs.yaml
├── app/
│   ├── main.py
│   ├── api/
│   │   ├── routes_health.py
│   │   ├── routes_collectors.py
│   │   ├── routes_devices.py
│   │   └── routes_reports.py
│   ├── core/
│   │   ├── settings.py
│   │   ├── logging.py
│   │   ├── scheduler.py
│   │   └── exceptions.py
│   ├── domain/
│   │   ├── models.py
│   │   ├── device_status.py
│   │   ├── backup_report.py
│   │   └── normalization.py
│   ├── collectors/
│   │   ├── base.py
│   │   ├── dxi_snmp_collector.py
│   │   ├── dd_snmp_collector.py
│   │   ├── i6000_rest_collector.py
│   │   ├── networker_rest_collector.py
│   │   └── zfs_rest_collector.py
│   ├── clients/
│   │   ├── snmp_client.py
│   │   ├── rest_client.py
│   │   ├── networker_client.py
│   │   ├── zfs_client.py
│   │   └── elasticsearch_client.py
│   ├── metrics/
│   │   ├── prometheus_exporter.py
│   │   └── metric_registry.py
│   ├── writers/
│   │   ├── elasticsearch_writer.py
│   │   └── prometheus_metrics.py
│   └── utils/
│       ├── time.py
│       ├── retry.py
│       └── json.py
├── tests/
│   ├── test_normalization.py
│   ├── test_snmp_client.py
│   ├── test_networker_client.py
│   └── test_collectors.py
├── docker/
│   └── Dockerfile
└── k8s/
    ├── namespace.yaml
    ├── configmap.yaml
    ├── secret.yaml
    ├── backup-api-deployment.yaml
    ├── dxi-snmp-collector-deployment.yaml
    ├── dd-snmp-collector-deployment.yaml
    ├── i6000-rest-collector-deployment.yaml
    ├── networker-rest-collector-deployment.yaml
    ├── zfs-rest-collector-deployment.yaml
    └── service.yaml
```

## 6. Collector 스케줄 요구사항

각 collector는 초 단위 offset을 맞춰 실행한다.

| Collector                | 실행 시점  |
| ------------------------ | ------ |
| DXi SNMP / CLI Collector | 매분 00초 |
| DD SNMP Collector        | 매분 00초 |
| i6000 REST Collector     | 매분 15초 |
| NetWorker REST Collector | 매분 30초 |
| ZFS REST Collector       | 매분 30초 |

스케줄러는 drift를 최소화해야 한다.

예시:

```text
12:00:00 DXi, DD 수집
12:00:15 i6000 수집
12:00:30 NetWorker, ZFS 수집
12:01:00 DXi, DD 수집
12:01:15 i6000 수집
12:01:30 NetWorker, ZFS 수집
```

## 7. 장비별 수집 요구사항

### 7.1 DXi SNMP / CLI Collector

수집 대상:

* 장비 응답 여부
* 장비 상태
* 전체 용량
* 사용 용량
* 사용률
* 중복제거율
* Replication 상태
* Interface 상태
* 주요 alert count
* SNMP 수집 성공 여부

수집 방식:

* SNMP(v2c): 장비 이름, 제품 정보, serial number, 기본 state 등 Quantum SNMP MIB에서 제공하는 식별/상태 값을 수집한다.
* SSH CLI: DXi SNMP Reference Guide 2.2.1에 OID가 명시되지 않은 전체 용량, 사용 용량, 사용률, 중복제거율, Replication 상태, Interface 상태, 주요 alert count를 CLI 명령 출력에서 수집한다.

CLI 수집 흐름:

```text
collector -> SSH 접속 -> DXi CLI 명령 실행 -> stdout 수집 -> 텍스트 파싱 -> summary/raw payload 생성 -> Elasticsearch/Prometheus 반영
```

CLI collector 설정 예시:

```yaml
- name: DXi_1_cli
  type: DXi
  protocol: ssh
  enabled: true
  schedule_second: 0
  host: DXi_1_host_TO_BE_FILLED
  port: 22
  username: DXi_1_username_TO_BE_FILLED
  password: DXi_1_password_TO_BE_FILLED
  command_timeout: 30
  commands:
    status: "show status"
    capacity: "show capacity"
    dedup: "show dedup"
    replication: "show replication"
    interfaces: "show network"
    alerts: "show alerts"
```

실제 DXi CLI 명령어는 장비 OS/버전에 따라 다를 수 있으므로, 운영 환경에서 `help`, `?`, `show ?` 결과에 맞춰 `commands` 값을 조정한다. 파서가 값을 추출하지 못하더라도 raw CLI 출력은 Elasticsearch payload에 저장하여 추후 parser를 보정할 수 있게 한다.

Prometheus metric 예시:

```text
backup_device_up{device_type="dxi",device_name="..."} 1
backup_device_capacity_total_bytes{device_type="dxi",device_name="..."} 100000000000000
backup_device_capacity_used_bytes{device_type="dxi",device_name="..."} 72100000000000
backup_device_capacity_used_percent{device_type="dxi",device_name="..."} 72.1
backup_device_dedup_ratio{device_type="dxi",device_name="..."} 12.5
backup_device_alert_count{device_type="dxi",device_name="...",severity="critical"} 0
backup_device_replication_up{device_type="dxi",device_name="...",replication="target-a"} 1
backup_device_interface_up{device_type="dxi",device_name="...",interface="eth0"} 1
backup_collector_last_success_timestamp{collector="DXi_1_cli"} 1710000000
```

Elasticsearch index 예시:

```text
backup-dxi-status-YYYY.MM
backup-dxi-summary-YYYY.MM
```

### 7.2 Data Domain SNMP Collector

대상 장비:

* DD4500
* DD6900

수집 대상:

* 장비 응답 여부
* 장비 상태
* 파일시스템 사용률
* 전체 용량
* 사용 용량
* 가용 용량
* 중복제거율
* Replication 상태
* 주요 alert
* Interface 상태
* SNMP 수집 성공 여부

Prometheus metric 예시:

```text
backup_device_up{device_type="dd",device_name="..."} 1
backup_device_capacity_used_percent{device_type="dd",device_name="..."} 81.4
backup_dd_filesystem_status{device_name="..."} 1
backup_dd_replication_status{device_name="...",pair="..."} 1
```

Elasticsearch index 예시:

```text
backup-dd-status-YYYY.MM
backup-dd-summary-YYYY.MM
```

### 7.3 Quantum i6000 REST Collector

수집 대상:

* Library 상태
* Robot 상태
* Drive 상태
* Tape slot 상태
* Tape media count
* Drive online/offline
* Drive error count
* Door open 상태
* REST 수집 성공 여부

수집 방식:

* i6000은 운영 포인트 단순화를 위해 REST API만 사용한다.
* 기존 SNMP 수집 대상이던 product name, physical library online status, robotics readiness, RAS subsystem status, partition online status, drive online/health, door status는 Scalar i6000 RESTful Web Services API의 endpoint로 대체한다.
* REST API는 `POST aml/users/login`으로 세션 cookie를 받은 뒤 `GET aml/`, `GET aml/physicalLibrary`, `GET aml/physicalLibrary/status`, `GET aml/drives`, `GET aml/media`, `GET aml/physicalLibrary/segments`, `GET aml/devices/towers`, `GET aml/devices/ieStations`, `GET aml/system/ras`, `GET aml/system/ras/tickets`를 호출한다.

REST 수집 흐름:

```text
collector -> i6000 REST login -> 상태/드라이브/미디어/슬롯/RAS endpoint 호출 -> JSON/XML 파싱 -> summary/raw payload 생성 -> Elasticsearch/Prometheus 반영
```

Prometheus metric 예시:

```text
backup_device_up{device_type="i6000",device_name="..."} 1
backup_tape_library_status{device_name="..."} 1
backup_tape_robot_status{device_name="...",robot="robot01"} 1
backup_tape_drive_status{device_name="...",drive="drive01"} 1
backup_tape_drive_error_count{device_name="...",drive="drive01"} 0
backup_tape_slot_used_count{device_name="..."} 120
backup_tape_slot_free_count{device_name="..."} 30
backup_tape_media_count{device_name="..."} 150
```

Elasticsearch index 예시:

```text
backup-i6000-status-YYYY.MM  # REST status document
backup-i6000-drive-YYYY.MM   # REST drive summary document
backup-i6000-media-YYYY.MM   # REST media/slot summary document
```

### 7.4 NetWorker REST Collector

NetWorker는 REST API 기반으로 수집한다.

수집 대상:

* 서버 상태
* Policy 목록
* Workflow 목록
* Action 목록
* Client 목록
* Client OS 정보
* Backup job 결과
* 성공 / 실패 / running 상태
* 월간 백업량
* Filesystem / Database policy 기준 백업량
* 클라이언트 증감 비교용 스냅샷
* Workflow count
* 최근 실패 job 목록

중요 기준:

* 기존에는 Action 개수를 기준으로 보려 했으나, 운영 기준상 Workflow count가 더 적절할 수 있으므로 Workflow 기준 집계를 우선 구현한다.
* Action 정보도 수집은 하되, 대시보드 주요 수치는 Workflow 기준으로 계산한다.
* Client 목록은 중복 제거한다.
* Client OS는 AIX / Linux / Windows 정도로 정규화한다.

Prometheus metric 예시:

```text
backup_networker_api_up{server="..."} 1
backup_networker_job_success_count{server="...",policy="Filesystem"} 100
backup_networker_job_failed_count{server="...",policy="Filesystem"} 2
backup_networker_workflow_count{server="...",policy="Database"} 12
backup_networker_client_count{server="..."} 350
```

Elasticsearch index 예시:

```text
backup-networker-job-YYYY.MM
backup-networker-client-YYYY.MM
backup-networker-policy-YYYY.MM
backup-networker-workflow-YYYY.MM
backup-networker-monthly-report-YYYY.MM
```

월간 보고용 document 예시:

```json
{
  "@timestamp": "2026-06-01T00:00:00+09:00",
  "report_month": "2026-05",
  "server": "networker01",
  "policy_type": "Filesystem",
  "total_backup_bytes": 1234567890,
  "total_backup_tb": 1.12,
  "success_count": 100,
  "failed_count": 2,
  "workflow_count": 14,
  "client_count": 300
}
```

클라이언트 변경 비교 document 예시:

```json
{
  "@timestamp": "2026-06-01T00:00:00+09:00",
  "report_month": "2026-05",
  "server": "networker01",
  "new_clients": ["client01", "client02"],
  "removed_clients": ["client99"],
  "new_client_count": 2,
  "removed_client_count": 1
}
```

### 7.5 ZFS REST Collector

ZFS Appliance는 REST API 기반으로 수집한다.

수집 대상:

* 장비 응답 여부
* Pool 상태
* Project / Share 상태
* 전체 용량
* 사용 용량
* 가용 용량
* 사용률
* Replication 상태
* Alert 정보
* REST API 수집 성공 여부

Prometheus metric 예시:

```text
backup_zfs_api_up{device_name="..."} 1
backup_zfs_pool_status{device_name="...",pool="pool01"} 1
backup_zfs_capacity_used_percent{device_name="...",pool="pool01"} 76.3
backup_zfs_alert_count{device_name="...",severity="critical"} 0
```

Elasticsearch index 예시:

```text
backup-zfs-status-YYYY.MM
backup-zfs-pool-YYYY.MM
backup-zfs-summary-YYYY.MM
```

## 8. 설정 파일 요구사항

### 8.1 .env.example

```env
APP_ENV=dev
LOG_LEVEL=INFO
TZ=Asia/Seoul

PROMETHEUS_METRICS_PORT=9100

ELASTICSEARCH_URL=http://elasticsearch:9200
ELASTICSEARCH_USERNAME=
ELASTICSEARCH_PASSWORD=

SNMP_VERSION=2c
SNMP_COMMUNITY=public
SNMP_TIMEOUT_SECONDS=5
SNMP_RETRIES=2

NETWORKER_BASE_URL=https://networker.example.com:9090
NETWORKER_USERNAME=admin
NETWORKER_PASSWORD=password
NETWORKER_VERIFY_SSL=false

ZFS_BASE_URL=https://zfs.example.com
ZFS_USERNAME=admin
ZFS_PASSWORD=password
ZFS_VERIFY_SSL=false
```

### 8.2 devices.yaml

```yaml
devices:
  dxi:
    - name: dxi01
      host: 10.0.0.10
      snmp_port: 161
      community_env: SNMP_COMMUNITY

  dd:
    - name: dd4500-01
      host: 10.0.0.20
      snmp_port: 161
      community_env: SNMP_COMMUNITY
    - name: dd6900-01
      host: 10.0.0.21
      snmp_port: 161
      community_env: SNMP_COMMUNITY

  i6000:
    - name: i6000-01
      host: 10.0.0.30
      snmp_port: 161
      community_env: SNMP_COMMUNITY

  networker:
    - name: networker01
      base_url_env: NETWORKER_BASE_URL
      username_env: NETWORKER_USERNAME
      password_env: NETWORKER_PASSWORD

  zfs:
    - name: zfs01
      base_url_env: ZFS_BASE_URL
      username_env: ZFS_USERNAME
      password_env: ZFS_PASSWORD
```

### 8.3 oid_map.yaml

SNMP OID는 코드에 하드코딩하지 말고 설정 파일로 분리한다.

초기에는 실제 OID가 확정되지 않아도 동작 가능한 구조로 작성한다.

```yaml
dd:
  system_status: "TO_BE_FILLED"
  capacity_total: "TO_BE_FILLED"
  capacity_used: "TO_BE_FILLED"
  capacity_available: "TO_BE_FILLED"
  alert_count: "TO_BE_FILLED"

dxi:
  system_status: "TO_BE_FILLED"
  capacity_total: "TO_BE_FILLED"
  capacity_used: "TO_BE_FILLED"
  dedup_ratio: "TO_BE_FILLED"

i6000:
  library_status: "TO_BE_FILLED"
  drive_status_table: "TO_BE_FILLED"
  slot_status_table: "TO_BE_FILLED"
  media_count: "TO_BE_FILLED"
```

OID가 `TO_BE_FILLED`인 경우 collector는 죽지 않고 warning log를 남긴 뒤 해당 metric을 skip해야 한다.

## 9. Elasticsearch Index Naming

Index 이름은 백업 대시보드 전용임을 알 수 있게 `backup-*` prefix를 사용한다.

권장 index:

```text
backup-device-status-YYYY.MM
backup-dxi-status-YYYY.MM
backup-dd-status-YYYY.MM
backup-i6000-status-YYYY.MM
backup-zfs-status-YYYY.MM
backup-networker-job-YYYY.MM
backup-networker-client-YYYY.MM
backup-networker-policy-YYYY.MM
backup-networker-workflow-YYYY.MM
backup-networker-monthly-report-YYYY.MM
backup-collector-log-YYYY.MM
```

환경 또는 업무계 구분이 필요한 경우 다음처럼 확장 가능하게 한다.

```text
backup-ptl-i6000-status-YYYY.MM
backup-vtl-dd-status-YYYY.MM
backup-vtl-dxi-status-YYYY.MM
backup-networker-job-YYYY.MM
```

## 10. 공통 데이터 모델

### 10.1 DeviceStatus

```python
class DeviceStatus:
    device_name: str
    device_type: str
    status: str
    status_code: int
    collected_at: datetime
    source: str
    detail: dict
```

### 10.2 CollectorResult

```python
class CollectorResult:
    collector_name: str
    success: bool
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    item_count: int
    error_message: str | None
```

### 10.3 BackupJobSummary

```python
class BackupJobSummary:
    server: str
    policy_name: str
    workflow_name: str
    action_name: str | None
    client_name: str
    client_os: str | None
    status: str
    started_at: datetime
    finished_at: datetime | None
    backup_bytes: int | None
```

### 10.4 NetworkerClientSnapshot

```python
class NetworkerClientSnapshot:
    server: str
    client_name: str
    client_os: str
    collected_month: str
    collected_at: datetime
```

## 11. FastAPI 요구사항

FastAPI는 최소 다음 API를 제공한다.

```text
GET /health
GET /collectors
GET /collectors/{collector_name}
POST /collectors/{collector_name}/run
GET /devices
GET /devices/{device_name}/status
GET /reports/networker/monthly?month=YYYY-MM
GET /reports/networker/clients/diff?month=YYYY-MM
GET /metrics
```

`/metrics`는 Prometheus scrape가 가능한 형식이어야 한다.

수동 수집 API는 운영 편의용이다.

```text
POST /collectors/networker-rest/run
POST /collectors/dd-snmp/run
POST /collectors/i6000-rest/run
```

## 12. 에러 처리 요구사항

다음 상황은 반드시 처리한다.

* SNMP timeout
* SNMP authentication/community 오류
* SNMP OID 미설정
* REST API timeout
* REST API 인증 실패
* SSL verification 실패
* 장비 응답 없음
* Elasticsearch 연결 실패
* Elasticsearch indexing 실패
* 일부 장비만 수집 실패
* 잘못된 응답 포맷
* null 값 또는 누락 필드

Collector는 한 장비 수집 실패 때문에 전체 루프가 종료되면 안 된다.

실패 결과도 Prometheus metric과 Elasticsearch log에 남긴다.

예시 metric:

```text
backup_collector_success{collector="dd-snmp"} 0
backup_collector_error_count{collector="dd-snmp"} 1
backup_collector_duration_seconds{collector="dd-snmp"} 3.2
```

## 13. Logging 요구사항

로그는 다음 정보를 포함한다.

* timestamp
* level
* collector_name
* device_name
* action
* success
* duration_ms
* error_message
* traceback 여부

예시:

```json
{
  "timestamp": "2026-06-26T10:30:00+09:00",
  "level": "ERROR",
  "collector_name": "dd-snmp",
  "device_name": "dd4500-01",
  "action": "collect",
  "success": false,
  "duration_ms": 5001,
  "error_message": "SNMP timeout"
}
```

## 14. 테스트 요구사항

최소 다음 테스트를 작성한다.

```text
tests/test_normalization.py
tests/test_scheduler.py
tests/test_snmp_client.py
tests/test_networker_client.py
tests/test_elasticsearch_writer.py
tests/test_collectors.py
```

테스트 기준:

* 실제 장비 없이 mock 응답으로 테스트 가능해야 한다.
* SNMP timeout 시 collector가 죽지 않아야 한다.
* REST API 실패 시 실패 metric이 증가해야 한다.
* OID가 비어 있으면 skip 처리되어야 한다.
* NetWorker client list 중복 제거가 되어야 한다.
* Client OS가 AIX / Linux / Windows / Unknown으로 정규화되어야 한다.
* 월간 백업량 계산이 가능해야 한다.
* 전월/당월 client diff 계산이 가능해야 한다.

## 15. Grafana 대시보드에서 사용할 주요 지표

### Overview

* 전체 장비 Health
* Critical Alert Count
* Warning Alert Count
* VTL 상태
* PTL 상태
* NetWorker 상태
* ZFS 상태
* 최근 수집 성공 여부
* Collector별 마지막 성공 시간

### Backup Operation

* 오늘 백업 성공 수
* 오늘 백업 실패 수
* 최근 실패 Job 목록
* Policy별 성공/실패 현황
* Workflow별 성공/실패 현황
* 월간 백업량
* Filesystem 백업량
* Database 백업량

### Device Capacity

* DD 사용률
* DXi 사용률
* ZFS Pool 사용률
* 용량 임계치 초과 장비 목록

### Tape Library

* i6000 Library 상태
* Drive 상태
* Drive error count
* Slot 사용량
* Media count

### Client Management

* 전체 client 수
* AIX client 수
* Linux client 수
* Windows client 수
* 신규 client 수
* 삭제 client 수
* 신규/삭제 client 목록

## 16. 보안 요구사항

* 계정과 패스워드는 코드에 하드코딩하지 않는다.
* `.env`는 git에 포함하지 않는다.
* `.env.example`만 제공한다.
* Kubernetes Secret 사용을 고려한다.
* REST API SSL verify 옵션은 설정으로 분리한다.
* SNMP community는 Secret 또는 환경변수로 관리한다.
* SNMP는 가능하면 collector 서버 IP만 허용하는 구성을 전제로 한다.

## 17. Kubernetes 리소스 기준

초기 단일 노드 기준으로 가볍게 동작하도록 작성한다.

권장 request/limit 예시:

```yaml
backup-api:
  requests:
    cpu: 100m
    memory: 256Mi
  limits:
    cpu: 500m
    memory: 512Mi

collector:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    cpu: 500m
    memory: 512Mi
```

Collector는 각자 독립 deployment로 작성한다.

각 collector는 동일한 이미지를 사용하되 실행 command 또는 env로 collector type을 구분해도 된다.

예시:

```yaml
env:
  - name: COLLECTOR_TYPE
    value: "dd-snmp"
```

## 18. 구현 우선순위

### 1단계

* 프로젝트 기본 구조 생성
* 설정 로딩
* 공통 logging
* 공통 scheduler
* FastAPI `/health`
* Prometheus `/metrics`
* Elasticsearch writer skeleton

### 2단계

* SNMP client 구현
* DD collector skeleton
* DXi collector skeleton
* i6000 collector skeleton
* OID config 기반 수집 구조 구현

### 3단계

* NetWorker REST client 구현
* Policy / Workflow / Client / Job 수집
* 월간 백업량 계산
* Client diff 계산

### 4단계

* ZFS REST client 구현
* Pool / Capacity / Alert 수집

### 5단계

* Kubernetes manifest 작성
* Dockerfile 작성
* 테스트 코드 작성
* README 작성

## 19. Codex에게 요청할 최종 산출물

Codex는 다음 산출물을 작성해야 한다.

```text
1. Python 프로젝트 전체 skeleton
2. FastAPI 서버 코드
3. Collector 코드
4. SNMP client 코드
5. REST client 코드
6. Prometheus metrics exporter 코드
7. Elasticsearch writer 코드
8. Config loader 코드
9. 예시 devices.yaml / oid_map.yaml
10. .env.example
11. Dockerfile
12. Kubernetes manifest
13. pytest 기반 테스트 코드
14. README.md
```

## 20. 주의사항

* SAN/스토리지 모니터링 프로젝트와 혼동하지 않는다.
* 이 프로젝트는 백업 통합 대시보드 전용이다.
* 대상은 DXi, DD, i6000, NetWorker, ZFS다.
* PowerMAX, Unity, ECS, PowerStore, NetApp 등 일반 스토리지 모니터링 대상은 제외한다.
* Kafka는 사용하지 않는다.
* Kibana는 사용하지 않는다.
* Grafana를 대시보드로 사용한다.
* Prometheus와 Elasticsearch를 함께 사용한다.
* Prometheus는 시계열 메트릭, Elasticsearch는 정제/집계/목록성 데이터 담당이다.
* FastAPI와 Collector는 분리한다.
* Collector는 장비군별로 독립 실행 가능해야 한다.

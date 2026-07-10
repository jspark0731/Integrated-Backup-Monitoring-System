# 백업 통합 대시보드 Codex 구현 가이드

## 1. 프로젝트 목적

이 프로젝트는 백업 운영 관점에서 주요 백업 장비와 백업 솔루션의 상태, 용량, 성능, 백업 결과, 클라이언트 증감, 월간 백업량을 수집하여 Grafana 대시보드에서 볼 수 있도록 만드는 백업 통합 대시보드 시스템이다.

본 프로젝트의 대상은 다음으로 한정한다.

| 구분      | 대상                          | 수집 방식          | 수집 주기   |
| ------- | --------------------------- | -------------- | ------- |
| VTL     | Quantum DXi                 | SNMP + SSH CLI | 5분마다 1회 |
| VTL     | Data Domain DD4500 / DD6900 | SNMP           | 5분마다 1회 |
| PTL     | Quantum i6000               | REST API       | 5분마다 1회 |
| 백업 솔루션  | Dell EMC NetWorker          | REST API       | 5분마다 1회 |
| 백업 스토리지 | ZFS Appliance               | REST API       | 5분마다 1회 |

중요 변경 사항:

```text id="0wdkl2"
1. i6000은 SNMP를 사용하지 않는다.
2. i6000은 REST API Collector로 통일한다.
3. 모든 Collector는 5분마다 1회 수행한다.
4. 단, DXi / DD / i6000 / NetWorker / ZFS가 동시에 수행되지 않도록 1분 간격으로 분산 실행한다.
5. Collector 파일명은 수집 대상과 프로토콜이 명확히 드러나도록 정리한다.
```

---

## 2. 작업 기준

Codex는 현재 local repository의 기존 코드, 설정 파일, Kubernetes manifest, README를 기준으로 수정한다.

기존 코드를 무시하고 새 프로젝트를 처음부터 다시 만들지 않는다.

기존 파일, class, config, k8s manifest를 최대한 살리되, 구조가 불명확하거나 이 가이드와 충돌하는 부분은 이 가이드를 기준으로 리팩터링한다.

현재 가이드 파일은 다음 파일을 기준으로 한다.

```text id="fex6n2"
Codex_Implementation_Guide.md
```

Codex는 이 파일을 수정하거나 대체하는 방향으로 작업한다.

---

## 3. 전체 아키텍처

전체 구조는 다음을 기준으로 유지한다.

```text id="mlz2qu"
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

저장소는 Prometheus와 Elasticsearch를 함께 사용한다.

Kibana는 사용하지 않는다.
Kafka는 사용하지 않는다.

---

## 4. Prometheus와 Elasticsearch 역할 분리

### 4.1 Prometheus 역할

Prometheus에는 순간 상태와 시계열 메트릭을 저장한다.

대상 예시는 다음과 같다.

```text id="d0bi14"
장비 up/down
SNMP 응답 여부
REST API 응답 여부
Collector 성공/실패
Collector 수행 시간
Collector 마지막 성공 시각
장비 상태 코드
용량 사용률
Alert count
Job count
Drive 상태
Library 상태
Pool 상태
```

Prometheus는 direct write 방식이 아니라 `/metrics` exporter 방식을 기본으로 한다.

각 Collector Pod는 `/metrics` endpoint를 제공해야 한다.

### 4.2 Elasticsearch 역할

Elasticsearch에는 정제된 운영 지표, 집계 결과, 목록성 데이터, 보고서성 데이터를 저장한다.

대상 예시는 다음과 같다.

```text id="si3b78"
NetWorker job 목록
최근 실패 job 목록
월간 백업량
Filesystem / Database 백업량
Client snapshot
Client diff
Policy / Workflow / Action inventory
Device status snapshot
Alert list
i6000 drive list
i6000 media list
ZFS pool summary
Grafana Table Panel에서 조회할 데이터
```

---

## 5. Kubernetes 배포 전제

이 프로젝트는 Kubernetes Pod 배포를 전제로 한다.

초기 환경은 단일 노드 Kubernetes를 가정한다.

Namespace는 다음을 기본으로 한다.

```text id="8ankdr"
backup-monitoring
```

Collector는 CronJob이 아니라 long-running Deployment 방식으로 구현한다.

이유는 다음과 같다.

```text id="a60evx"
1. Prometheus가 /metrics endpoint를 지속적으로 scrape해야 한다.
2. Collector 상태를 항상 확인할 수 있어야 한다.
3. Pod 생성/종료를 반복하는 CronJob보다 장기 실행 Deployment가 관리하기 쉽다.
4. 각 Collector의 실행 시점을 내부 scheduler로 정밀하게 제어해야 한다.
```

각 Collector Deployment는 기본 replicas 1이다.

```yaml id="s834co"
replicas: 1
```

동일 Collector가 2개 이상 실행되면 같은 장비를 중복 수집할 수 있으므로 초기 구현에서는 scale-out하지 않는다.

향후 HA가 필요하면 다음 중 하나를 별도 구현한다.

```text id="xv12xy"
Kubernetes leader election
장비 목록 shard 분배
collector instance ID 기반 device assignment
외부 lock 사용
```

---

## 6. Pod 구성

Kubernetes에는 다음 Collector workload를 배포한다.

```text id="wvqtki"
backup-dashboard-collector-dxi
backup-dashboard-collector-dd
backup-dashboard-collector-i6000
backup-dashboard-collector-networker
backup-dashboard-collector-zfs
```

각 workload는 독립된 Deployment, Service, ConfigMap을 가진다.

Prometheus, Elasticsearch, Grafana는 이미 외부 또는 별도 namespace에 있을 수 있으므로, 해당 manifest는 optional로 둔다.

---

## 7. FastAPI와 Collector 구조

현재 코드는 FastAPI 기반 collector service 구조를 가진다.

Codex는 기존 FastAPI 구조를 유지하되, 다음 역할을 명확히 분리한다.

### 7.1 FastAPI 공통 API 역할

FastAPI는 다음 endpoint를 제공한다.

```text id="3m7dp3"
GET /healthz
GET /readyz
GET /collectors
POST /collectors/run-once
GET /metrics
```

기존 endpoint와 호환성을 유지한다.

추가 endpoint를 구현할 수 있다면 다음도 제공한다.

```text id="bhxlfo"
GET /collectors/{collector_name}
POST /collectors/{collector_name}/run
GET /devices
GET /devices/{device_name}/status
GET /reports/networker/monthly?month=YYYY-MM
GET /reports/networker/clients/diff?month=YYYY-MM
```

### 7.2 Collector 역할

Collector는 다음 역할을 담당한다.

```text id="g67h0i"
정해진 주기에 맞춰 장비에서 데이터 수집
raw data validation
normalized data 생성
Prometheus metric 갱신
Elasticsearch document 적재
장비별 실패를 전체 collector 실패로 전파하지 않음
수집 실패 시 error metric과 log 기록
```

---

## 8. 수집 주기 변경 요구사항

기존 매분 실행 구조를 5분 주기로 변경한다.

모든 Collector는 5분마다 1회 실행한다.

단, 모든 Collector가 동시에 실행되면 부하가 몰릴 수 있으므로 1분 간격으로 분산 실행한다.

| Collector           | 실행 기준       |
| ------------------- | ----------- |
| DXi Collector       | 매 5분 주기의 0분 |
| DD Collector        | 매 5분 주기의 1분 |
| i6000 Collector     | 매 5분 주기의 2분 |
| NetWorker Collector | 매 5분 주기의 3분 |
| ZFS Collector       | 매 5분 주기의 4분 |

예시:

```text id="y1h6gb"
12:00:00 DXi 수집
12:01:00 DD 수집
12:02:00 i6000 수집
12:03:00 NetWorker 수집
12:04:00 ZFS 수집

12:05:00 DXi 수집
12:06:00 DD 수집
12:07:00 i6000 수집
12:08:00 NetWorker 수집
12:09:00 ZFS 수집

12:10:00 DXi 수집
12:11:00 DD 수집
12:12:00 i6000 수집
12:13:00 NetWorker 수집
12:14:00 ZFS 수집
```

이 방식은 다음 의미다.

```text id="wd26ng"
각 Collector의 개별 수집 주기 = 5분
Collector 간 실행 간격 = 1분
실행 기준 = wall-clock 기준
```

스케줄러는 단순히 “수집 완료 후 300초 sleep” 방식으로 구현하지 않는다.

반드시 wall-clock 기준으로 다음 실행 시각을 계산해야 한다.

예시 설정 필드:

```yaml id="e67v9o"
schedule:
  interval_minutes: 5
  minute_offset: 0
  second: 0
```

Collector별 예시:

```yaml id="9d05ox"
dxi:
  schedule:
    interval_minutes: 5
    minute_offset: 0
    second: 0

dd:
  schedule:
    interval_minutes: 5
    minute_offset: 1
    second: 0

i6000:
  schedule:
    interval_minutes: 5
    minute_offset: 2
    second: 0

networker:
  schedule:
    interval_minutes: 5
    minute_offset: 3
    second: 0

zfs:
  schedule:
    interval_minutes: 5
    minute_offset: 4
    second: 0
```

스케줄러 함수는 다음 조건을 만족해야 한다.

```text id="xbtog4"
1. interval_minutes와 minute_offset을 기준으로 다음 실행 시각을 계산한다.
2. minute_offset은 0 이상 interval_minutes 미만이어야 한다.
3. second는 0 이상 59 이하이어야 한다.
4. 현재 시각이 실행 시각을 지났다면 다음 interval로 넘어간다.
5. Pod가 재시작되어도 다음 wall-clock 기준 실행 시각에 맞춰 실행한다.
6. 이전 수집이 아직 끝나지 않았으면 중복 실행하지 않는다.
7. 이전 수집이 길어져 다음 실행 시각과 겹치면 skip 또는 warning 처리한다.
```

기존 `schedule_second` 중심 구조는 다음 형태로 확장하거나 대체한다.

```python id="pqpo4m"
class ScheduleConfig:
    interval_minutes: int = 5
    minute_offset: int = 0
    second: int = 0
```

권장 함수명:

```python id="n4012k"
seconds_until_next_run(
    interval_minutes: int,
    minute_offset: int,
    second: int,
    now: datetime | None = None,
) -> float
```

---

## 9. Collector 파일명 및 코드 구조 리팩터링 요구사항

현재 collector 파일명은 일부는 프로토콜이 드러나지만, 일부는 너무 일반적이거나 대상과 프로토콜이 명확하지 않다.

Codex는 collector 파일명을 다음 원칙으로 정리한다.

```text id="2myuz8"
{target}_{protocol}_collector.py
```

복합 프로토콜을 사용하는 경우 다음처럼 작성한다.

```text id="aw6sl6"
{target}_{protocol1}_{protocol2}_collector.py
```

권장 파일명은 다음과 같다.

| 대상                  | 변경 권장 파일명                          |
| ------------------- | ---------------------------------- |
| DXi                 | `dxi_cli_snmp_collector.py`        |
| DD                  | `dd_snmp_collector.py`             |
| i6000               | `i6000_rest_collector.py`          |
| NetWorker           | `networker_rest_collector.py`      |
| ZFS                 | `zfs_rest_collector.py`            |
| 공통 REST client/base | `rest_client.py` 또는 `rest_base.py` |
| 공통 SNMP client/base | `snmp_client.py` 또는 `snmp_base.py` |

중요:

```text id="slq6yl"
1. i6000 관련 SNMP collector 파일은 만들지 않는다.
2. i6000 관련 SNMP 설정도 제거하거나 deprecated 처리한다.
3. DXi는 SNMP + CLI를 함께 쓰므로 dxi_cli_snmp_collector.py로 명명한다.
4. DD는 SNMP만 사용하므로 dd_snmp_collector.py로 명명한다.
5. NetWorker와 ZFS는 REST만 사용한다.
6. factory.py는 변경된 파일명/class명을 기준으로 collector를 생성해야 한다.
7. 기존 import path가 깨지지 않도록 필요한 경우 compatibility wrapper를 둔다.
```

예시 compatibility wrapper:

```python id="5sumo3"
# app/collectors/i6000_rest.py
from app.collectors.i6000_rest_collector import I6000RestCollector

__all__ = ["I6000RestCollector"]
```

리팩터링 후 권장 구조:

```text id="64bsfb"
app/
├── collectors/
│   ├── __init__.py
│   ├── base.py
│   ├── factory.py
│   ├── dxi_cli_snmp_collector.py
│   ├── dd_snmp_collector.py
│   ├── i6000_rest_collector.py
│   ├── networker_rest_collector.py
│   └── zfs_rest_collector.py
├── clients/
│   ├── __init__.py
│   ├── snmp_client.py
│   ├── ssh_client.py
│   ├── rest_client.py
│   ├── i6000_rest_client.py
│   ├── networker_rest_client.py
│   ├── zfs_rest_client.py
│   └── elasticsearch_client.py
├── parsers/
│   ├── __init__.py
│   ├── dxi_cli_parser.py
│   ├── dd_snmp_parser.py
│   ├── i6000_rest_parser.py
│   ├── networker_rest_parser.py
│   └── zfs_rest_parser.py
├── writers/
│   ├── elasticsearch.py
│   └── prometheus.py
└── scheduler.py
```

---

## 10. Config 구조 요구사항

현재 config는 collector별 example yaml을 사용한다.

다음 파일 구조를 유지한다.

```text id="nfhplp"
config/
├── collector.dxi.example.yaml
├── collector.dd.example.yaml
├── collector.i6000.example.yaml
├── collector.networker.example.yaml
├── collector.zfs.example.yaml
└── collector.example.yaml
```

각 collector config에는 반드시 다음 항목이 포함되어야 한다.

```yaml id="h9tp9p"
name: backup-dashboard-collector-dxi
type: dxi
enabled: true

schedule:
  interval_minutes: 5
  minute_offset: 0
  second: 0

elasticsearch:
  enabled: true
  url: http://elasticsearch:9200

prometheus:
  enabled: true
```

기존 `schedule_second`만 있는 config는 다음처럼 migration한다.

```yaml id="1o5dm0"
# old
schedule_second: 15

# new
schedule:
  interval_minutes: 5
  minute_offset: 2
  second: 0
```

단, 호환성을 위해 일정 기간 다음 로직을 허용한다.

```text id="22n5so"
1. schedule.interval_minutes가 있으면 새 스케줄 구조 사용
2. schedule이 없고 schedule_second만 있으면 기존 방식으로 동작하되 warning log 출력
3. 최종적으로는 schedule_second를 deprecated 처리
```

---

## 11. Collector별 스케줄 기본값

### 11.1 DXi

```yaml id="cunbtf"
schedule:
  interval_minutes: 5
  minute_offset: 0
  second: 0
```

### 11.2 DD

```yaml id="64xf07"
schedule:
  interval_minutes: 5
  minute_offset: 1
  second: 0
```

### 11.3 i6000

```yaml id="b9ec8b"
schedule:
  interval_minutes: 5
  minute_offset: 2
  second: 0
```

### 11.4 NetWorker

```yaml id="0wyen5"
schedule:
  interval_minutes: 5
  minute_offset: 3
  second: 0
```

### 11.5 ZFS

```yaml id="jc71fk"
schedule:
  interval_minutes: 5
  minute_offset: 4
  second: 0
```

---

## 12. i6000 수집 방식 변경 요구사항

i6000은 REST API Collector로 통일한다.

SNMP는 사용하지 않는다.

다음 항목은 모두 REST API 기반으로 수집한다.

```text id="pmpnvl"
Library 상태
Physical Library 상태
Robot / Robotics 상태
RAS subsystem 상태
RAS ticket 상태
Partition 상태
Drive 상태
Drive online/offline
Drive health
Door 상태
Slot 상태
Media count
Media 목록
IE Station 상태
Tower / segment 상태
REST 수집 성공 여부
```

i6000 Collector 파일명은 다음을 사용한다.

```text id="l0jv9u"
app/collectors/i6000_rest_collector.py
```

i6000 REST client 파일명은 다음을 사용한다.

```text id="55wjxy"
app/clients/i6000_rest_client.py
```

i6000 parser 파일명은 다음을 사용한다.

```text id="c6g3t7"
app/parsers/i6000_rest_parser.py
```

i6000 config 파일은 다음을 사용한다.

```text id="2kotlb"
config/collector.i6000.example.yaml
```

i6000 config에는 SNMP 관련 필드를 넣지 않는다.

잘못된 예:

```yaml id="5e7xf6"
snmp_port: 161
community_env: SNMP_COMMUNITY
oid_map:
  library_status: TO_BE_FILLED
```

올바른 예:

```yaml id="8sgw8b"
name: backup-dashboard-collector-i6000
type: i6000
protocol: rest
enabled: true

schedule:
  interval_minutes: 5
  minute_offset: 2
  second: 0

targets:
  - name: i6000-01
    base_url: https://i6000.example.com
    username_env: I6000_USERNAME
    password_env: I6000_PASSWORD
    verify_ssl: false
    timeout_seconds: 30
```

i6000 REST endpoint는 config에서 조정 가능해야 한다.

기본 endpoint 예시:

```yaml id="1kf4cw"
endpoints:
  login: /aml/users/login
  root: /aml/
  physical_library: /aml/physicalLibrary
  physical_library_status: /aml/physicalLibrary/status
  drives: /aml/drives
  media: /aml/media
  segments: /aml/physicalLibrary/segments
  towers: /aml/devices/towers
  ie_stations: /aml/devices/ieStations
  ras: /aml/system/ras
  ras_tickets: /aml/system/ras/tickets
```

REST 응답이 JSON 또는 XML일 수 있으므로 parser는 두 형식을 모두 고려한다.

---

## 13. DXi 수집 방식 요구사항

DXi는 SNMP와 SSH CLI를 함께 사용한다.

DXi Collector 파일명은 다음을 사용한다.

```text id="rnzfca"
app/collectors/dxi_cli_snmp_collector.py
```

DXi는 다음 기준을 따른다.

```text id="ed6bk2"
SNMP:
  장비 식별 정보
  제품 정보
  serial number
  기본 상태
  SNMP 응답 여부

SSH CLI:
  전체 용량
  사용 용량
  사용률
  중복제거율
  Replication 상태
  Interface 상태
  Alert count
```

DXi config 예시:

```yaml id="khxhf6"
name: backup-dashboard-collector-dxi
type: dxi
protocol: cli_snmp
enabled: true

schedule:
  interval_minutes: 5
  minute_offset: 0
  second: 0

targets:
  - name: dxi01
    host: dxi01.example.com

    snmp:
      enabled: true
      port: 161
      version: 2c
      community_env: DXI_SNMP_COMMUNITY
      timeout_seconds: 5
      retries: 2
      oids:
        system_name: TO_BE_FILLED
        product_name: TO_BE_FILLED
        serial_number: TO_BE_FILLED
        system_status: TO_BE_FILLED

    cli:
      enabled: true
      port: 22
      username_env: DXI_USERNAME
      password_env: DXI_PASSWORD
      command_timeout: 30
      commands:
        status: "show status"
        capacity: "show capacity"
        dedup: "show dedup"
        replication: "show replication"
        interfaces: "show network"
        alerts: "show alerts"
```

DXi CLI 명령어는 장비 OS/버전에 따라 다를 수 있으므로 config에서 수정 가능해야 한다.

parser가 값을 추출하지 못하더라도 raw CLI output은 Elasticsearch payload에 저장한다.

---

## 14. DD 수집 방식 요구사항

DD는 SNMP Collector를 사용한다.

DD Collector 파일명은 다음을 사용한다.

```text id="1nzcna"
app/collectors/dd_snmp_collector.py
```

DD config 예시:

```yaml id="wiqk1u"
name: backup-dashboard-collector-dd
type: dd
protocol: snmp
enabled: true

schedule:
  interval_minutes: 5
  minute_offset: 1
  second: 0

targets:
  - name: dd4500-01
    host: dd4500.example.com
    port: 161
    version: 2c
    community_env: DD_SNMP_COMMUNITY
    timeout_seconds: 5
    retries: 2
    oids:
      system_status: TO_BE_FILLED
      filesystem_status: TO_BE_FILLED
      capacity_total: TO_BE_FILLED
      capacity_used: TO_BE_FILLED
      capacity_available: TO_BE_FILLED
      dedup_ratio: TO_BE_FILLED
      replication_status: TO_BE_FILLED
      alert_critical_count: TO_BE_FILLED
      alert_warning_count: TO_BE_FILLED
      interface_status_table: TO_BE_FILLED
```

OID가 `TO_BE_FILLED`이면 collector는 죽지 않고 해당 항목만 skip한다.

---

## 15. NetWorker 수집 방식 요구사항

NetWorker는 REST API Collector를 사용한다.

NetWorker Collector 파일명은 다음을 사용한다.

```text id="47uf0a"
app/collectors/networker_rest_collector.py
```

NetWorker REST client 파일명은 다음을 사용한다.

```text id="iil1zq"
app/clients/networker_rest_client.py
```

NetWorker config 예시:

```yaml id="j8f4xe"
name: backup-dashboard-collector-networker
type: networker
protocol: rest
enabled: true

schedule:
  interval_minutes: 5
  minute_offset: 3
  second: 0

targets:
  - name: networker01
    base_url: https://networker.example.com:9090
    username_env: NETWORKER_USERNAME
    password_env: NETWORKER_PASSWORD
    verify_ssl: false
    timeout_seconds: 30
```

NetWorker 수집 대상:

```text id="q9cs76"
서버 상태
Policy 목록
Workflow 목록
Action 목록
Client 목록
Client OS 정보
Backup job 결과
성공 / 실패 / running 상태
월간 백업량
Filesystem / Database policy 기준 백업량
클라이언트 증감 비교용 스냅샷
Workflow count
최근 실패 job 목록
```

중요 기준:

```text id="79u6ci"
1. 운영 지표는 Workflow 기준 집계를 우선한다.
2. Action 정보도 수집하되 주요 대시보드 수치는 Workflow 기준으로 계산한다.
3. Client 목록은 hostname 기준으로 중복 제거한다.
4. Client OS는 AIX / Linux / Windows / Unknown으로 정규화한다.
5. 월간 백업량은 중복 집계되지 않도록 ssid 또는 job id 기준 중복 제거 구조를 준비한다.
6. Filesystem / Database policy를 구분할 수 있어야 한다.
```

---

## 16. ZFS 수집 방식 요구사항

ZFS는 REST API Collector를 사용한다.

ZFS Collector 파일명은 다음을 사용한다.

```text id="q72qqy"
app/collectors/zfs_rest_collector.py
```

ZFS REST client 파일명은 다음을 사용한다.

```text id="u0jlyo"
app/clients/zfs_rest_client.py
```

ZFS config 예시:

```yaml id="zl8ni2"
name: backup-dashboard-collector-zfs
type: zfs
protocol: rest
enabled: true

schedule:
  interval_minutes: 5
  minute_offset: 4
  second: 0

targets:
  - name: zfs01
    base_url: https://zfs.example.com:215
    username_env: ZFS_USERNAME
    password_env: ZFS_PASSWORD
    verify_ssl: false
    timeout_seconds: 30
```

ZFS 수집 대상:

```text id="kyu5ix"
장비 응답 여부
Appliance version
Pool 상태
Pool 용량
Project 목록
Filesystem 목록
LUN 목록
Alert log
Fault log
Replication 상태
REST API 수집 성공 여부
```

---

## 17. Collector Factory 요구사항

`app/collectors/factory.py`는 config의 `type`과 `protocol`을 기준으로 적절한 collector class를 생성한다.

예시 매핑:

```python id="vbxezw"
COLLECTOR_CLASS_MAP = {
    ("dxi", "cli_snmp"): DXiCliSnmpCollector,
    ("dd", "snmp"): DDSnmpCollector,
    ("i6000", "rest"): I6000RestCollector,
    ("networker", "rest"): NetworkerRestCollector,
    ("zfs", "rest"): ZfsRestCollector,
}
```

지원하지 않는 조합이 들어오면 명확한 error를 남긴다.

예:

```text id="0n87wz"
Unsupported collector type/protocol: i6000/snmp
```

i6000/snmp 조합은 허용하지 않는다.

---

## 18. Prometheus Metric 요구사항

공통 collector metric:

```text id="zbuknw"
backup_collector_success{collector="...",target_type="...",protocol="..."} 1
backup_collector_error_count{collector="...",target_type="...",protocol="..."} 0
backup_collector_duration_seconds{collector="...",target_type="...",protocol="..."} 3.2
backup_collector_last_success_timestamp{collector="..."} 1710000000
backup_collector_skipped{collector="...",target_type="...",reason="..."} 0
```

DXi metric 예시:

```text id="6su8vr"
backup_device_up{device_type="dxi",device_name="dxi01"} 1
backup_device_capacity_total_bytes{device_type="dxi",device_name="dxi01"} 100000000000000
backup_device_capacity_used_bytes{device_type="dxi",device_name="dxi01"} 72100000000000
backup_device_capacity_used_percent{device_type="dxi",device_name="dxi01"} 72.1
backup_device_dedup_ratio{device_type="dxi",device_name="dxi01"} 12.5
backup_device_alert_count{device_type="dxi",device_name="dxi01",severity="critical"} 0
backup_device_replication_up{device_type="dxi",device_name="dxi01",replication="target-a"} 1
backup_device_interface_up{device_type="dxi",device_name="dxi01",interface="eth0"} 1
```

DD metric 예시:

```text id="l3nz9q"
backup_device_up{device_type="dd",device_name="dd4500-01"} 1
backup_device_capacity_used_percent{device_type="dd",device_name="dd4500-01"} 81.4
backup_dd_filesystem_status{device_name="dd4500-01"} 1
backup_dd_replication_status{device_name="dd4500-01",pair="pair01"} 1
```

i6000 metric 예시:

```text id="1y7a7u"
backup_device_up{device_type="i6000",device_name="i6000-01"} 1
backup_tape_library_status{device_name="i6000-01"} 1
backup_tape_robot_status{device_name="i6000-01",robot="robot01"} 1
backup_tape_drive_status{device_name="i6000-01",drive="drive01"} 1
backup_tape_drive_error_count{device_name="i6000-01",drive="drive01"} 0
backup_tape_slot_used_count{device_name="i6000-01"} 120
backup_tape_slot_free_count{device_name="i6000-01"} 30
backup_tape_media_count{device_name="i6000-01"} 150
```

NetWorker metric 예시:

```text id="u5kkpz"
backup_networker_api_up{server="networker01"} 1
backup_networker_job_success_count{server="networker01",policy="Filesystem"} 100
backup_networker_job_failed_count{server="networker01",policy="Filesystem"} 2
backup_networker_workflow_count{server="networker01",policy="Database"} 12
backup_networker_client_count{server="networker01"} 350
```

ZFS metric 예시:

```text id="acuiqz"
backup_zfs_api_up{device_name="zfs01"} 1
backup_zfs_pool_status{device_name="zfs01",pool="pool01"} 1
backup_zfs_capacity_used_percent{device_name="zfs01",pool="pool01"} 76.3
backup_zfs_alert_count{device_name="zfs01",severity="critical"} 0
```

---

## 19. Elasticsearch Index Naming

Index 이름은 백업 대시보드 전용임을 알 수 있게 `backup-*` prefix를 사용한다.

권장 index:

```text id="erjws5"
backup-device-status-YYYY.MM
backup-dxi-status-YYYY.MM
backup-dxi-summary-YYYY.MM
backup-dd-status-YYYY.MM
backup-dd-summary-YYYY.MM
backup-i6000-status-YYYY.MM
backup-i6000-drive-YYYY.MM
backup-i6000-media-YYYY.MM
backup-zfs-status-YYYY.MM
backup-zfs-pool-YYYY.MM
backup-networker-job-YYYY.MM
backup-networker-client-YYYY.MM
backup-networker-policy-YYYY.MM
backup-networker-workflow-YYYY.MM
backup-networker-monthly-report-YYYY.MM
backup-networker-client-diff-YYYY.MM
backup-collector-log-YYYY.MM
```

환경 또는 업무계 구분이 필요한 경우 다음처럼 확장 가능하게 한다.

```text id="40ppzt"
backup-ptl-i6000-status-YYYY.MM
backup-vtl-dd-status-YYYY.MM
backup-vtl-dxi-status-YYYY.MM
backup-networker-job-YYYY.MM
```

---

## 20. Elasticsearch Document ID 전략

Pod restart 또는 동일 시간대 재수집으로 인한 과도한 중복 적재를 방지하기 위해 deterministic document ID를 사용한다.

예시:

```text id="b9ozpl"
{device_name}-{metric_type}-{timestamp_minute}
{server}-{client_name}-{report_month}
{server}-{policy}-{workflow}-{report_month}
{server}-{job_id}
{server}-{ssid}
{i6000_device}-{drive_id}-{timestamp_minute}
{i6000_device}-{media_id}-{timestamp_minute}
```

월간 보고서성 데이터는 upsert 가능해야 한다.

---

## 21. 백업 대시보드 지표 정의

아래 지표는 반드시 코드 구조에 반영한다.

### 21.1 Overview 지표

| metric_id                       | 지표명              | Source    | 수집 방식         | 저장소             | 용도            |
| ------------------------------- | ---------------- | --------- | ------------- | --------------- | ------------- |
| overview_total_device_count     | 전체 수집 대상 장비 수    | config    | config        | ES              | Overview Stat |
| overview_device_up_count        | 정상 응답 장비 수       | collector | SNMP/REST/CLI | Prometheus      | Overview Stat |
| overview_device_down_count      | 응답 실패 장비 수       | collector | SNMP/REST/CLI | Prometheus      | Overview Stat |
| overview_critical_alert_count   | Critical Alert 수 | 전체 대상     | SNMP/REST/CLI | Prometheus + ES | Overview Stat |
| overview_warning_alert_count    | Warning Alert 수  | 전체 대상     | SNMP/REST/CLI | Prometheus + ES | Overview Stat |
| overview_last_collect_time      | 마지막 수집 시각        | collector | internal      | Prometheus + ES | Overview Stat |
| overview_collector_success_rate | Collector 성공률    | collector | internal      | Prometheus      | Trend         |

### 21.2 NetWorker 운영 지표

| metric_id                   | 지표명               | Source    | 수집 방식                       | 저장소             | 집계 기준                     |
| --------------------------- | ----------------- | --------- | --------------------------- | --------------- | ------------------------- |
| nw_policy_count             | Policy 수          | NetWorker | REST API                    | ES              | server 기준                 |
| nw_workflow_count           | Workflow 수        | NetWorker | REST API                    | ES + Prometheus | policy 기준                 |
| nw_action_count             | Action 수          | NetWorker | REST API                    | ES              | workflow 기준               |
| nw_client_count             | Client 수          | NetWorker | REST API                    | ES + Prometheus | 중복 제거                     |
| nw_client_os_count          | OS별 Client 수      | NetWorker | REST API                    | ES              | AIX/Linux/Windows/Unknown |
| nw_job_success_count        | 백업 성공 수           | NetWorker | REST API                    | Prometheus + ES | 기간/policy/workflow        |
| nw_job_failed_count         | 백업 실패 수           | NetWorker | REST API                    | Prometheus + ES | 기간/policy/workflow        |
| nw_job_running_count        | 실행 중 Job 수        | NetWorker | REST API                    | Prometheus      | 현재 상태                     |
| nw_job_failure_rate         | 백업 실패율            | NetWorker | REST API                    | ES              | failed / total            |
| nw_recent_failed_jobs       | 최근 실패 Job 목록      | NetWorker | REST API                    | ES              | 최근 N건                     |
| nw_monthly_backup_bytes     | 월간 백업량            | NetWorker | REST API 또는 mminfo 연계 가능 구조 | ES              | report_month/policy       |
| nw_monthly_filesystem_bytes | 월간 Filesystem 백업량 | NetWorker | REST API                    | ES              | policy=Filesystem         |
| nw_monthly_database_bytes   | 월간 Database 백업량   | NetWorker | REST API                    | ES              | policy=Database           |
| nw_new_client_count         | 신규 Client 수       | NetWorker | REST API                    | ES              | 전월 대비                     |
| nw_removed_client_count     | 삭제 Client 수       | NetWorker | REST API                    | ES              | 전월 대비                     |
| nw_new_client_list          | 신규 Client 목록      | NetWorker | REST API                    | ES              | 전월 대비                     |
| nw_removed_client_list      | 삭제 Client 목록      | NetWorker | REST API                    | ES              | 전월 대비                     |

### 21.3 DXi 지표

| metric_id                    | 지표명              | Source | 수집 방식    | 저장소             | 용도       |
| ---------------------------- | ---------------- | ------ | -------- | --------------- | -------- |
| dxi_up                       | DXi 응답 여부        | DXi    | SNMP/CLI | Prometheus      | Stat     |
| dxi_system_status            | DXi 장비 상태        | DXi    | SNMP/CLI | Prometheus + ES | Health   |
| dxi_capacity_total_bytes     | 전체 용량            | DXi    | CLI      | Prometheus + ES | Capacity |
| dxi_capacity_used_bytes      | 사용 용량            | DXi    | CLI      | Prometheus + ES | Capacity |
| dxi_capacity_available_bytes | 가용 용량            | DXi    | CLI      | Prometheus + ES | Capacity |
| dxi_capacity_used_percent    | 사용률              | DXi    | CLI      | Prometheus      | Gauge    |
| dxi_dedup_ratio              | 중복제거율            | DXi    | CLI      | Prometheus + ES | Trend    |
| dxi_replication_status       | Replication 상태   | DXi    | CLI      | Prometheus + ES | Health   |
| dxi_alert_critical_count     | Critical Alert 수 | DXi    | CLI      | Prometheus + ES | Alert    |
| dxi_alert_warning_count      | Warning Alert 수  | DXi    | CLI      | Prometheus + ES | Alert    |
| dxi_interface_status         | Interface 상태     | DXi    | CLI      | Prometheus + ES | Network  |

### 21.4 DD 지표

| metric_id                   | 지표명              | Source | 수집 방식 | 저장소             | 용도       |
| --------------------------- | ---------------- | ------ | ----- | --------------- | -------- |
| dd_up                       | DD 응답 여부         | DD     | SNMP  | Prometheus      | Stat     |
| dd_system_status            | DD 장비 상태         | DD     | SNMP  | Prometheus + ES | Health   |
| dd_capacity_total_bytes     | 전체 용량            | DD     | SNMP  | Prometheus + ES | Capacity |
| dd_capacity_used_bytes      | 사용 용량            | DD     | SNMP  | Prometheus + ES | Capacity |
| dd_capacity_available_bytes | 가용 용량            | DD     | SNMP  | Prometheus + ES | Capacity |
| dd_capacity_used_percent    | 사용률              | DD     | SNMP  | Prometheus      | Gauge    |
| dd_dedup_ratio              | 중복제거율            | DD     | SNMP  | Prometheus + ES | Trend    |
| dd_replication_status       | Replication 상태   | DD     | SNMP  | Prometheus + ES | Health   |
| dd_alert_critical_count     | Critical Alert 수 | DD     | SNMP  | Prometheus + ES | Alert    |
| dd_alert_warning_count      | Warning Alert 수  | DD     | SNMP  | Prometheus + ES | Alert    |
| dd_interface_status         | Interface 상태     | DD     | SNMP  | Prometheus + ES | Network  |

### 21.5 i6000 PTL 지표

| metric_id                 | 지표명             | Source | 수집 방식    | 저장소             | 용도        |
| ------------------------- | --------------- | ------ | -------- | --------------- | --------- |
| i6000_up                  | i6000 응답 여부     | i6000  | REST API | Prometheus      | Stat      |
| i6000_library_status      | Library 상태      | i6000  | REST API | Prometheus + ES | Health    |
| i6000_robot_status        | Robot 상태        | i6000  | REST API | Prometheus + ES | Health    |
| i6000_ras_status          | RAS 상태          | i6000  | REST API | Prometheus + ES | Health    |
| i6000_ras_ticket_count    | RAS Ticket 수    | i6000  | REST API | Prometheus + ES | Alert     |
| i6000_partition_status    | Partition 상태    | i6000  | REST API | Prometheus + ES | Health    |
| i6000_drive_total_count   | 전체 Drive 수      | i6000  | REST API | ES              | Inventory |
| i6000_drive_online_count  | Online Drive 수  | i6000  | REST API | Prometheus + ES | Stat      |
| i6000_drive_offline_count | Offline Drive 수 | i6000  | REST API | Prometheus + ES | Stat      |
| i6000_drive_error_count   | Drive Error 수   | i6000  | REST API | Prometheus + ES | Alert     |
| i6000_slot_total_count    | 전체 Slot 수       | i6000  | REST API | ES              | Inventory |
| i6000_slot_used_count     | 사용 Slot 수       | i6000  | REST API | Prometheus + ES | Capacity  |
| i6000_slot_free_count     | 빈 Slot 수        | i6000  | REST API | Prometheus + ES | Capacity  |
| i6000_media_count         | Media 수         | i6000  | REST API | Prometheus + ES | Inventory |
| i6000_door_open_status    | Door Open 상태    | i6000  | REST API | Prometheus + ES | Alert     |

### 21.6 ZFS 지표

| metric_id                    | 지표명              | Source | 수집 방식    | 저장소             | 용도        |
| ---------------------------- | ---------------- | ------ | -------- | --------------- | --------- |
| zfs_up                       | ZFS API 응답 여부    | ZFS    | REST API | Prometheus      | Stat      |
| zfs_pool_status              | Pool 상태          | ZFS    | REST API | Prometheus + ES | Health    |
| zfs_capacity_total_bytes     | 전체 용량            | ZFS    | REST API | Prometheus + ES | Capacity  |
| zfs_capacity_used_bytes      | 사용 용량            | ZFS    | REST API | Prometheus + ES | Capacity  |
| zfs_capacity_available_bytes | 가용 용량            | ZFS    | REST API | Prometheus + ES | Capacity  |
| zfs_capacity_used_percent    | 사용률              | ZFS    | REST API | Prometheus      | Gauge     |
| zfs_project_count            | Project 수        | ZFS    | REST API | ES              | Inventory |
| zfs_share_count              | Share 수          | ZFS    | REST API | ES              | Inventory |
| zfs_replication_status       | Replication 상태   | ZFS    | REST API | Prometheus + ES | Health    |
| zfs_alert_critical_count     | Critical Alert 수 | ZFS    | REST API | Prometheus + ES | Alert     |
| zfs_alert_warning_count      | Warning Alert 수  | ZFS    | REST API | Prometheus + ES | Alert     |

---

## 22. Grafana 패널 매핑

### 22.1 Overview Dashboard

| Panel                  | Data Source   |
| ---------------------- | ------------- |
| 전체 장비 상태               | Prometheus    |
| Critical Alert Count   | Prometheus    |
| Warning Alert Count    | Prometheus    |
| Collector Last Success | Prometheus    |
| 장비별 Health Table       | Elasticsearch |
| 최근 장애/경고 목록            | Elasticsearch |

### 22.2 Backup Operation Dashboard

| Panel                   | Data Source      |
| ----------------------- | ---------------- |
| 오늘 백업 성공 수              | Prometheus 또는 ES |
| 오늘 백업 실패 수              | Prometheus 또는 ES |
| Policy별 성공/실패           | ES               |
| Workflow별 성공/실패         | ES               |
| 최근 실패 Job 목록            | ES               |
| 월간 백업량                  | ES               |
| Filesystem/Database 백업량 | ES               |

### 22.3 Client Dashboard

| Panel        | Data Source |
| ------------ | ----------- |
| 전체 Client 수  | Prometheus  |
| OS별 Client 수 | ES          |
| 신규 Client 수  | ES          |
| 삭제 Client 수  | ES          |
| 신규 Client 목록 | ES          |
| 삭제 Client 목록 | ES          |

### 22.4 Device Capacity Dashboard

| Panel        | Data Source |
| ------------ | ----------- |
| DD 사용률       | Prometheus  |
| DXi 사용률      | Prometheus  |
| ZFS Pool 사용률 | Prometheus  |
| 용량 위험 장비 목록  | ES          |

### 22.5 Tape Library Dashboard

| Panel             | Data Source     |
| ----------------- | --------------- |
| i6000 Library 상태  | Prometheus      |
| i6000 RAS 상태      | Prometheus + ES |
| Drive 상태          | Prometheus + ES |
| Drive Error Count | Prometheus      |
| Slot 사용량          | Prometheus      |
| Media 목록          | ES              |

---

## 23. Error Handling 요구사항

다음 상황은 반드시 처리한다.

```text id="4l9ibr"
SNMP timeout
SNMP authentication/community 오류
SNMP OID 미설정
SSH timeout
SSH 인증 실패
CLI 명령 실패
CLI 파싱 실패
REST API timeout
REST API 인증 실패
REST API session 만료
SSL verification 실패
장비 응답 없음
Elasticsearch 연결 실패
Elasticsearch indexing 실패
일부 장비만 수집 실패
잘못된 응답 포맷
JSON/XML 파싱 실패
null 값 또는 누락 필드
```

Collector는 한 장비 수집 실패 때문에 전체 loop가 종료되면 안 된다.

실패 결과도 Prometheus metric과 Elasticsearch log에 남긴다.

---

## 24. Logging 요구사항

Kubernetes 환경에서는 파일 로그보다 stdout/stderr 출력이 기본이다.

모든 애플리케이션 로그는 stdout으로 출력한다.

로그는 JSON 또는 key-value 형태로 구조화한다.

로그에는 다음 정보를 포함한다.

```text id="p93n0i"
timestamp
level
collector_name
device_name
target_type
protocol
action
success
duration_ms
error_message
traceback 여부
```

예시:

```json id="ul0blc"
{
  "timestamp": "2026-07-10T12:03:00+09:00",
  "level": "ERROR",
  "collector_name": "backup-dashboard-collector-networker",
  "device_name": "networker01",
  "target_type": "networker",
  "protocol": "rest",
  "action": "collect",
  "success": false,
  "duration_ms": 5001,
  "error_message": "REST API timeout"
}
```

---

## 25. Health Check 요구사항

모든 Collector Pod는 다음 endpoint를 제공한다.

```text id="1o8n91"
GET /healthz
GET /readyz
GET /metrics
```

readiness 기준:

```text id="t9l3c2"
설정 파일 로드 성공
Collector 생성 성공
Prometheus metrics endpoint 기동 성공
수집 대상 config 로드 성공
```

Elasticsearch 연결 실패만으로 readiness를 무조건 실패 처리하지 않는다.

Elasticsearch 장애 상황에서도 Prometheus metric은 계속 제공할 수 있어야 한다.

liveness 기준:

```text id="y5ofc9"
프로세스가 살아 있음
scheduler loop가 멈추지 않음
마지막 loop heartbeat가 일정 시간 이내 갱신됨
```

---

## 26. Graceful Shutdown 요구사항

Kubernetes에서 Pod 종료 시 SIGTERM을 받으면 다음 순서로 종료한다.

```text id="qb6w3h"
1. 신규 수집 시작 중단
2. 현재 진행 중인 수집이 있으면 가능한 범위에서 완료
3. Elasticsearch write 중이면 완료 또는 timeout 처리
4. 마지막 collector 상태 log 기록
5. metrics server 종료
6. 프로세스 종료
```

`terminationGracePeriodSeconds`는 30초 이상으로 설정한다.

```yaml id="bl7ra3"
terminationGracePeriodSeconds: 30
```

---

## 27. Kubernetes Manifest 요구사항

Codex는 기존 `k8s/` 구조를 유지하되, 변경된 collector 이름과 스케줄 설정을 반영한다.

필수 파일:

```text id="elvg42"
k8s/kustomization.yaml
k8s/dxi.yaml
k8s/dd.yaml
k8s/i6000.yaml
k8s/networker.yaml
k8s/zfs.yaml
```

각 manifest는 다음을 포함해야 한다.

```text id="8nedv4"
Deployment
Service
ConfigMap
Secret 참조
resource requests/limits
readinessProbe
livenessProbe
```

Collector별 workload 이름:

```text id="oc8qk6"
backup-dashboard-collector-dxi
backup-dashboard-collector-dd
backup-dashboard-collector-i6000
backup-dashboard-collector-networker
backup-dashboard-collector-zfs
```

i6000 manifest에는 SNMP 관련 환경변수나 Secret을 넣지 않는다.

잘못된 예:

```yaml id="nzhxel"
- name: SNMP_COMMUNITY
  valueFrom:
    secretKeyRef:
      name: backup-dashboard-secret
      key: SNMP_COMMUNITY
```

i6000은 REST 인증 정보만 사용한다.

올바른 예:

```yaml id="pb9w1l"
- name: I6000_USERNAME
  valueFrom:
    secretKeyRef:
      name: backup-dashboard-secret
      key: I6000_USERNAME
- name: I6000_PASSWORD
  valueFrom:
    secretKeyRef:
      name: backup-dashboard-secret
      key: I6000_PASSWORD
```

---

## 28. Dockerfile 요구사항

현재 프로젝트는 collector별 Docker target을 사용할 수 있다.

빌드 대상은 다음과 같다.

```text id="gmphwb"
dxi
dd
i6000
networker
zfs
```

각 이미지명 예시:

```text id="vpglho"
backup-dashboard-collector-dxi:latest
backup-dashboard-collector-dd:latest
backup-dashboard-collector-i6000:latest
backup-dashboard-collector-networker:latest
backup-dashboard-collector-zfs:latest
```

i6000 image에는 SNMP 의존성이 필요하지 않다.

DXi와 DD image에는 SNMP 관련 의존성이 필요하다.

DXi image에는 SSH CLI 수집 의존성도 필요하다.

---

## 29. Security 요구사항

```text id="zm2bjx"
계정과 패스워드는 코드에 하드코딩하지 않는다.
.env는 git에 포함하지 않는다.
.env.example만 제공한다.
Kubernetes Secret을 사용한다.
REST API SSL verify 옵션은 설정으로 분리한다.
SNMP community는 Secret 또는 환경변수로 관리한다.
i6000은 SNMP community를 사용하지 않는다.
SNMP는 가능하면 collector 서버 IP만 허용하는 구성을 전제로 한다.
```

---

## 30. 테스트 요구사항

최소 다음 테스트를 작성하거나 기존 테스트를 수정한다.

```text id="7uvwsj"
tests/test_scheduler.py
tests/test_collectors.py
tests/test_dxi_cli_snmp_collector.py
tests/test_dd_snmp_collector.py
tests/test_i6000_rest_collector.py
tests/test_networker_rest_collector.py
tests/test_zfs_rest_collector.py
tests/test_elasticsearch_writer.py
tests/test_normalization.py
```

테스트 기준:

```text id="lgu0c4"
실제 장비 없이 mock 응답으로 테스트 가능해야 한다.
5분 주기 스케줄 계산이 가능해야 한다.
minute_offset 0/1/2/3/4에 따라 다음 실행 시간이 올바르게 계산되어야 한다.
이전 수집이 아직 끝나지 않았으면 중복 실행하지 않아야 한다.
i6000은 REST collector만 생성되어야 한다.
i6000/snmp 조합은 error 처리되어야 한다.
SNMP timeout 시 collector가 죽지 않아야 한다.
REST API 실패 시 실패 metric이 증가해야 한다.
OID가 TO_BE_FILLED이면 skip 처리되어야 한다.
NetWorker client list 중복 제거가 되어야 한다.
Client OS가 AIX / Linux / Windows / Unknown으로 정규화되어야 한다.
월간 백업량 계산이 가능해야 한다.
전월/당월 client diff 계산이 가능해야 한다.
Elasticsearch writer가 deterministic document_id로 upsert할 수 있어야 한다.
```

스케줄 테스트 예시:

```python id="qucxc7"
def test_next_run_dxi_every_five_minutes():
    # 12:00:00, 12:05:00, 12:10:00에 실행되어야 한다.
    ...

def test_next_run_dd_offset_one_minute():
    # 12:01:00, 12:06:00, 12:11:00에 실행되어야 한다.
    ...

def test_next_run_i6000_offset_two_minutes():
    # 12:02:00, 12:07:00, 12:12:00에 실행되어야 한다.
    ...
```

---

## 31. 구현 우선순위

### 1단계: 현재 코드 분석

```text id="6gwwzd"
현재 local repository 구조 분석
Codex_Implementation_Guide.md 분석
app/collectors 구조 분석
app/scheduler.py 구조 분석
config/*.yaml 구조 분석
k8s/*.yaml 구조 분석
Dockerfile target 분석
```

### 2단계: 스케줄 구조 변경

```text id="ghhk6k"
schedule_second 중심 구조를 schedule.interval_minutes / minute_offset / second 구조로 확장
기존 schedule_second 호환성 유지
5분 주기 + 1분 간격 분산 실행 구현
scheduler test 수정
```

### 3단계: i6000 REST 전용화

```text id="7earrr"
i6000 SNMP 관련 config 제거
i6000 REST Collector만 사용하도록 factory 수정
i6000 REST endpoint config 정리
i6000 REST parser 정리
i6000 k8s manifest에서 SNMP Secret 제거
```

### 4단계: Collector 파일명 리팩터링

```text id="hupt7n"
dxi_cli.py -> dxi_cli_snmp_collector.py
i6000_rest.py -> i6000_rest_collector.py
networker_rest.py -> networker_rest_collector.py
zfs_rest.py -> zfs_rest_collector.py
snmp.py -> snmp_base.py 또는 snmp_client.py
rest.py -> rest_base.py 또는 rest_client.py
factory.py import 경로 수정
필요 시 compatibility wrapper 작성
```

### 5단계: Config와 K8s 반영

```text id="95tml4"
config/collector.*.example.yaml 수정
k8s/*.yaml 수정
ConfigMap 내용 수정
Secret 참조 수정
README 실행 예시 수정
```

### 6단계: 테스트와 문서 정리

```text id="ihapti"
pytest 수정
README.md 수정
Codex_Implementation_Guide.md 수정
기존 문서와 코드의 수집 방식 불일치 제거
```

---

## 32. Codex 최종 산출물

Codex는 다음 산출물을 수정 또는 생성해야 한다.

```text id="dlv517"
1. 수정된 Codex_Implementation_Guide.md
2. 수정된 README.md
3. 5분 주기 스케줄러 코드
4. ScheduleConfig 모델
5. collector factory 수정
6. i6000 REST 전용 collector 구조
7. collector 파일명 리팩터링
8. config/collector.dxi.example.yaml
9. config/collector.dd.example.yaml
10. config/collector.i6000.example.yaml
11. config/collector.networker.example.yaml
12. config/collector.zfs.example.yaml
13. k8s/dxi.yaml
14. k8s/dd.yaml
15. k8s/i6000.yaml
16. k8s/networker.yaml
17. k8s/zfs.yaml
18. scheduler 관련 pytest
19. i6000 REST collector pytest
20. factory pytest
```

---

## 33. Codex에게 전달할 요청 문구

Codex에게는 다음 문구와 함께 이 문서를 전달한다.

```text id="jphn6z"
이 프로젝트는 백업 통합 대시보드입니다.

현재 local repository의 코드를 먼저 분석한 뒤,
Codex_Implementation_Guide.md와 코드 구조를 이 가이드에 맞게 수정해주세요.

중요 변경 사항은 다음과 같습니다.

1. i6000은 SNMP를 사용하지 않습니다.
   i6000은 REST API Collector로 통일합니다.
   i6000 관련 SNMP config, OID, Secret, collector 생성 로직은 제거하거나 deprecated 처리해주세요.

2. 모든 collector의 수행 주기를 5분으로 변경합니다.
   단, 모든 collector가 동시에 실행되지 않도록 1분 간격으로 분산 실행합니다.

   DXi: 매 5분 주기의 0분
   DD: 매 5분 주기의 1분
   i6000: 매 5분 주기의 2분
   NetWorker: 매 5분 주기의 3분
   ZFS: 매 5분 주기의 4분

   예:
   12:00 DXi
   12:01 DD
   12:02 i6000
   12:03 NetWorker
   12:04 ZFS
   12:05 DXi
   12:06 DD
   ...

   기존 schedule_second 구조는 schedule.interval_minutes, schedule.minute_offset, schedule.second 구조로 확장하거나 대체해주세요.
   기존 schedule_second는 호환성을 위해 읽을 수는 있게 하되 deprecated warning을 남겨주세요.

3. 코드 파일명을 더 명확하게 정리해주세요.
   dxi가 CLI와 SNMP를 함께 쓰면 dxi_cli_snmp_collector.py처럼 대상과 프로토콜이 드러나는 파일명을 사용해주세요.

   권장 파일명:
   app/collectors/dxi_cli_snmp_collector.py
   app/collectors/dd_snmp_collector.py
   app/collectors/i6000_rest_collector.py
   app/collectors/networker_rest_collector.py
   app/collectors/zfs_rest_collector.py

4. factory.py는 변경된 type/protocol 조합에 맞게 collector를 생성해야 합니다.
   i6000/snmp 조합은 지원하지 않는 것으로 처리해주세요.

5. Kubernetes manifest, ConfigMap, example config, README, pytest까지 함께 수정해주세요.

기존 코드를 완전히 새로 만들지 말고, 현재 repository의 구조를 최대한 살리면서 위 기준에 맞게 리팩터링해주세요.
```

---

## 34. 주의사항

```text id="4bdv6d"
SAN/스토리지 모니터링 프로젝트와 혼동하지 않는다.
이 프로젝트는 백업 통합 대시보드 전용이다.
대상은 DXi, DD, i6000, NetWorker, ZFS다.
VTL은 DXi와 DD를 의미한다.
PTL은 i6000을 의미한다.
i6000은 REST API만 사용한다.
i6000은 SNMP를 사용하지 않는다.
DXi는 SNMP + SSH CLI를 사용한다.
DD는 SNMP를 사용한다.
NetWorker는 REST API를 사용한다.
ZFS는 REST API를 사용한다.
Kafka는 사용하지 않는다.
Kibana는 사용하지 않는다.
Grafana를 대시보드로 사용한다.
Prometheus와 Elasticsearch를 함께 사용한다.
Prometheus는 시계열 메트릭, Elasticsearch는 정제/집계/목록성 데이터 담당이다.
Collector는 Kubernetes Deployment로 실행한다.
Collector replicas는 기본 1이다.
Collector별 수집 주기는 5분이다.
Collector 간 실행 시점은 1분씩 분산한다.
실제 SNMP OID가 미확정된 지표는 config에 TO_BE_FILLED로 두고 skip 처리한다.
```

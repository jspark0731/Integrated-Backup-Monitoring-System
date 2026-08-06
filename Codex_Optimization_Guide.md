# Integrated Backup Monitoring System 성능 및 리소스 최적화 요청

현재 저장소의 최신 코드를 기준으로 기존 아키텍처와 기능을 최대한 유지하면서 성능 및 리소스 효율을 개선해줘.

Repository:
`https://github.com/jspark0731/Integrated-Backup-Monitoring-System`

기존 Collector 구조, Prometheus Metric, Elasticsearch 저장 구조, Kubernetes 배포 구조를 먼저 분석한 뒤 아래 요구사항을 반영해줘.

## 1. 기본 원칙

* 기존 Collector 기능을 제거하지 않는다.
* 기존 장비별 수집 방식은 유지한다.

  * DXi: SNMP + SSH CLI
  * DD: SNMP
  * i6000: REST API
  * NetWorker: REST API
  * ZFS: REST API
* 기존 FastAPI 기반 구조는 유지한다.
* `/healthz`, `/readyz`, `/collectors`, `/collectors/run-once`, `/metrics` API는 유지한다.
* Prometheus Metric 이름은 기존 호환성을 최대한 유지한다.
* 현재 동작하는 테스트를 깨뜨리지 않는다.
* 새로운 구조에 맞는 테스트를 추가한다.
* 단일 노드 Kubernetes 환경에서 운영하는 것을 기본 전제로 한다.
* 불필요하게 복잡한 메시지 큐, Kafka, Celery 등의 신규 컴포넌트는 추가하지 않는다.

---

# 2. Elasticsearch Index 구조 최적화

현재 일반 장비의 Elasticsearch Index가 Collector/장비별 + 일자별로 생성되고 있다.

예:

```text
VTL-DD4500-2026-07-28-1
VTL-DD6900_1-2026-07-28-1
PTL-CORE-2026-07-28-1
```

이 구조는 장기적으로 Index/Shard 수가 과도하게 증가할 수 있으므로 변경한다.

일반 장비는 Solution 단위 + 월 단위 RAW Index를 사용하도록 변경한다.

예:

```text
VTL-RAW-2026-07
PTL-RAW-2026-07
ZFS-RAW-2026-07
```

CURRENT 데이터는 날짜 suffix 없이 Solution별 고정 Index를 사용한다.

```text
VTL-CURRENT
PTL-CURRENT
ZFS-CURRENT
```

장비 구분은 Index가 아니라 document field를 이용한다.

각 Document에 최소 다음 필드가 존재하도록 한다.

```text
collector
device_name
target_type
solution
document_family
document_type
@timestamp
```

CURRENT document의 `_id`는 Collector별로 고정하여 동일 Collector의 최신 상태가 overwrite되도록 한다.

예:

```text
DD4500:current
i6000_core_rest:current
```

NetWorker의 보안 도메인 기반 Index 구조는 현재 설계를 유지한다.

예:

```text
NW-OPS-RAW-CORE-2026-07
NW-OPS-CURRENT-CORE
NW-CORE-JOB-2026-07
NW-CHNL-CLIENT-2026-07
NW-INFO-WORKFLOW-2026-07
NW-IFRS-MONTHLY-2026-07
```

단, NetWorker CURRENT Index도 가능하면 월별 생성 대신 고정 Index로 변경한다.

---

# 3. Fast / Slow Collection 분리

현재 Collector의 모든 데이터를 5분마다 수집하지 않도록 개선한다.

Collector 내부에서 수집 항목별로 주기를 구분할 수 있도록 한다.

최소 다음 두 종류를 지원한다.

```yaml
schedule:
  fast:
    interval_minutes: 5
    minute_offset: 2
    second: 0

  slow:
    interval_minutes: 60
    minute_offset: 2
    second: 30
```

기존 단일 `schedule` 설정과의 하위 호환성도 가능하면 유지한다.

## i6000

FAST 수집:

* library status
* drive status
* RAS status
* RAS tickets
* storage slot used/free
* 장비 reachability

SLOW 수집:

* 전체 media inventory
* tower inventory
* I/E station inventory
* physical library inventory 등 변경 빈도가 낮은 정보

## NetWorker

FAST 수집:

* jobs
* backup execution/status
* 실패/성공/running 현황
* 현재 운영 상태

SLOW 수집:

* clients
* policies
* workflows
* protection groups
* inventory 성격의 데이터
* 월간 집계용 source data

기본적으로 Client/Policy/Workflow 계열은 30~60분 주기를 사용할 수 있도록 한다.

## ZFS

FAST:

* API 상태
* pool 상태
* capacity 사용률
* alert/fault

SLOW:

* projects
* filesystems
* LUN inventory
* 변경 빈도가 낮은 appliance inventory

DXi/DD는 현재 구조를 우선 유지하되 fast/slow 분리가 의미 있는 항목이 있으면 동일한 framework를 사용할 수 있도록 설계한다.

---

# 4. REST API 병렬 수집

현재 i6000 등 REST Collector에서 여러 endpoint를 순차적으로 호출하는 부분을 개선한다.

독립적인 endpoint는 `asyncio.gather()` 또는 이에 준하는 async concurrency 방식으로 병렬 호출한다.

단, 장비에 과도한 요청이 발생하지 않도록 concurrency limit를 설정한다.

기본값:

```yaml
rest:
  max_concurrency: 4
```

`asyncio.Semaphore` 등을 사용하여 동시에 호출되는 API 요청 수를 제한한다.

하나의 endpoint 실패 때문에 전체 수집이 무조건 실패하지 않도록 한다.

예를 들어 10개 endpoint 중 1개가 실패하면:

* 성공한 endpoint 데이터는 정상 처리
* 실패 endpoint와 오류 내용은 payload/error metadata에 기록
* Collector 전체 상태는 partial failure를 표현할 수 있도록 한다.

가능하면 `asyncio.gather(..., return_exceptions=True)` 방식 또는 이에 준하는 안정적인 구현을 사용한다.

---

# 5. HTTP Client Connection Reuse

REST Collector가 수집마다 `httpx.AsyncClient`를 새로 생성하지 않도록 개선한다.

Pod/Application lifespan 동안 재사용 가능한 HTTP Client connection pool을 사용하도록 한다.

목표:

* TCP connection reuse
* TLS session/connection reuse
* 불필요한 socket 생성 감소
* 수집 latency 감소

단, 인증 세션 정책은 장비별로 다를 수 있으므로 HTTP connection과 application login session은 분리해서 생각한다.

i6000은 우선:

```text
HTTP Client 재사용
+
각 collection cycle에서 login/logout
```

방식을 기본으로 한다.

장기간 로그인 세션 유지 기능은 구현하지 않아도 된다.

FastAPI 종료 시 AsyncClient가 정상 close되도록 lifespan/collector shutdown 처리도 구현한다.

---

# 6. RAW 데이터 저장 최적화

현재 매 수집마다 전체 REST payload를 RAW Document에 저장하는 구조를 검토한다.

상태성 데이터와 Inventory 데이터를 구분한다.

FAST 데이터:

```text
5분 주기 RAW 저장 가능
```

SLOW / Inventory 데이터:

```text
30분~60분 주기 RAW 저장
```

특히 i6000의 전체 Media Inventory와 같이 payload가 큰 데이터는 5분마다 Elasticsearch에 저장하지 않는다.

가능하면 Collector 결과를 다음과 같이 분리한다.

```text
fast_raw
slow_raw
current
derived
```

또는 기존 `raw/current` 구조를 유지하되 document에 다음 metadata를 추가한다.

```text
collection_class: fast | slow
```

구조는 지나치게 복잡하게 만들지 않는다.

---

# 7. Kubernetes Pod 구조 최적화

현재 단일 Kubernetes Node에서 운영하는 것을 전제로 한다.

Collector는 Solution 단위로 하나의 Pod를 기본 구조로 한다.

권장 Deployment:

```text
backup-dashboard-collector-dxi
backup-dashboard-collector-dd
backup-dashboard-collector-i6000
backup-dashboard-collector-networker
backup-dashboard-collector-zfs
```

NetWorker의 CORE / CHNL / INFO / IFRS를 각각 별도 Deployment로 실행하는 기존 구조는 변경한다.

하나의 NetWorker Collector Pod 안에서 다음 Collector들이 각각 독립적으로 Scheduler task를 실행하도록 한다.

```text
networker_core
networker_chnl
networker_info
networker_ifrs
```

현재 `CollectorScheduler`가 여러 Collector를 asyncio task로 실행할 수 있으므로 해당 구조를 활용한다.

단 다음 조건은 유지한다.

* CORE 수집 장애가 CHNL 수집을 중단시키지 않아야 한다.
* 하나의 Collector 예외가 전체 scheduler process를 종료시키면 안 된다.
* Collector별 last_result는 각각 유지한다.
* Prometheus label로 source/domain 구분이 가능해야 한다.
* Elasticsearch의 domain 기반 index routing은 기존 동작을 유지한다.

---

# 8. Scheduler 안정성 개선

현재 Collector task에서 예상하지 못한 exception이 발생했을 때 해당 task가 영구 종료되지 않도록 한다.

각 Collector loop는 대략 다음 형태가 되도록 한다.

```python
while True:
    try:
        await sleep_until_next_schedule()
        result = await collector.collect()
        await writer.write(...)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception(...)
        metrics...
        continue
```

Collector 하나의 장애가 다른 Collector 실행에 영향을 주지 않아야 한다.

---

# 9. Elasticsearch Bulk Write

가능한 경우 Elasticsearch write는 한 collection cycle에서 생성된 document를 모아서 Bulk API로 전송한다.

현재 `async_bulk` 구조가 이미 있다면 이를 유지하고 효율적으로 활용한다.

불필요하게 Document마다 개별 Elasticsearch request를 생성하지 않는다.

Bulk 실패 시 전체 성공/실패뿐 아니라 가능한 범위에서 실패 원인을 로그로 남긴다.

---

# 10. Config 및 Secret 분리

실제 운영 환경에서는 다음 정보가 ConfigMap에 평문으로 들어가지 않도록 Kubernetes manifest를 정리한다.

Secret 대상:

```text
SNMP community
SSH username/password
REST username/password
REST token
Elasticsearch username/password
```

ConfigMap 대상:

```text
host
port
endpoint
schedule
OID
index 관련 설정
TLS verify 여부
```

Certificate는 Secret volume mount 방식으로 사용할 수 있도록 한다.

예:

```text
/apps/secrets/elasticsearch/ca.crt
/apps/secrets/i6000/ca.crt
/apps/secrets/networker/ca.crt
```

기존 환경변수 또는 YAML config 방식과 자연스럽게 연결되도록 구현한다.

---

# 11. 리소스 설정

Kubernetes manifest에 Collector별 기본 resource requests/limits를 추가한다.

초기값은 과도하게 크게 잡지 않는다.

예:

```yaml
resources:
  requests:
    cpu: 50m
    memory: 128Mi
  limits:
    cpu: 500m
    memory: 512Mi
```

단, 코드/Dependency 특성을 분석하여 필요하면 Collector별로 다르게 제안해도 된다.

NetWorker, i6000 등 payload가 상대적으로 큰 Collector는 메모리 limit가 너무 작지 않도록 판단한다.

---

# 12. 테스트

변경 후 다음 테스트를 추가하거나 보강한다.

1. Elasticsearch Index naming 테스트
2. CURRENT document overwrite용 ID 테스트
3. fast/slow scheduler 계산 테스트
4. REST concurrency limit 테스트
5. endpoint partial failure 테스트
6. HTTP Client reuse 테스트
7. NetWorker 4 Collector가 동일 Scheduler 내에서 독립 실행되는 테스트
8. Collector exception 이후 다음 schedule에 다시 실행되는 테스트
9. 기존 security domain classification 테스트 유지
10. 기존 Prometheus metric 테스트 유지

모든 pytest가 통과하도록 한다.

---

# 13. 문서화

README와 관련 docs를 최신 구조에 맞게 수정한다.

특히 다음 내용을 명확히 문서화한다.

* 전체 아키텍처
* Collector별 protocol
* fast/slow 수집 대상
* 수집 주기
* Elasticsearch Index naming
* Prometheus와 Elasticsearch 역할 차이
* Kubernetes Deployment 구성
* Secret/Certificate mount 방식
* 각 Collector의 resource requests/limits

---

# 14. 구현 시 주의사항

이번 작업의 목표는 새로운 플랫폼을 다시 만드는 것이 아니다.

현재 코드의 다음 장점은 유지한다.

* 하나의 공통 Collector codebase
* 장비/솔루션별 Docker target
* FastAPI 관리 endpoint
* asyncio 기반 scheduler
* Prometheus metrics
* Elasticsearch raw/current/derived 개념
* NetWorker security domain classification
* Kubernetes 기반 독립 배포

가능한 한 작은 변경으로 현재 구조를 개선한다.

과도한 abstraction이나 framework 추가는 피한다.

구현 전에 현재 코드에서 변경 대상 파일과 변경 방향을 먼저 분석하고, 그 분석을 기반으로 작업한다.

작업 완료 후에는 다음 형식으로 결과를 정리해줘.

1. 변경한 아키텍처
2. 변경한 파일 목록
3. 주요 코드 변경 내용
4. 기존 대비 성능/리소스 개선 포인트
5. 기존 기능과의 호환성
6. 새로 추가한 설정값
7. Kubernetes 배포 시 변경사항
8. 테스트 결과
9. 실제 운영 환경에서 사용자가 채워야 하는 값
10. 추가로 권장하지만 이번 작업에서는 수행하지 않은 사항

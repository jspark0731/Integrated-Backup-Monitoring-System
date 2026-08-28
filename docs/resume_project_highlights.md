# 이력서용 프로젝트 포인트와 코드 근거

## 1. 프로젝트 한 줄 소개

> SNMP, SSH CLI, REST API를 사용하는 5종의 백업·스토리지 장비를 통합하고,
> Prometheus 실시간 메트릭과 Elasticsearch 이력·분석 데이터를 제공하는
> Kubernetes 기반 비동기 수집 플랫폼을 설계·구현했다.

이 프로젝트는 화면 중심의 대시보드보다는 **운영 장비에 부하를 주지 않으면서
데이터를 안정적으로 수집·정규화·저장하는 플랫폼**에 가깝다. 따라서 백엔드,
플랫폼 엔지니어링, DevOps/SRE, 데이터 엔지니어링 직무에서 활용도가 높다.

## 2. 권장 프로젝트 제목

- 이기종 백업 인프라 통합 모니터링 플랫폼 개발
- Kubernetes 기반 백업·스토리지 데이터 수집 플랫폼 개발
- 운영 부하를 고려한 비동기 백업 인프라 Collector 설계

## 3. 핵심 기술 스택

`Python`, `FastAPI`, `asyncio`, `httpx`, `Prometheus`, `Elasticsearch`,
`Kubernetes`, `Docker`, `SNMP`, `SSH`, `REST API`, `pytest`

## 4. 이력서에 쓸 만한 핵심 포인트와 근거

아래의 "이력서 표현"은 현재 코드와 테스트로 증명할 수 있는 범위에서 작성했다.
성능 개선 비율이나 운영 규모처럼 실측이 필요한 값은 임의로 넣지 않는다.

### 4.1 이기종 장비 수집 방식 통합

**이력서 표현**

> SNMP, SSH CLI, REST API를 사용하는 DXi, Data Domain, i6000, NetWorker,
> ZFS 장비의 수집 과정을 공통 Collector 모델로 통합했다.

**왜 의미 있는가**

- 장비별 프로토콜뿐 아니라 인증, 오류 형태, 응답 구조가 서로 다르다.
- client, parser, collector를 분리해 장비별 차이를 격리했다.
- 공통 `CollectionResult`를 통해 scheduler와 writer는 장비별 구현을 몰라도 된다.

**코드 근거**

- Collector 공통 실행 모델: [`apps/collectors/base.py` 31~84행](../apps/collectors/base.py#L31-L84)
- SNMP client: [`apps/clients/snmp_client.py` 16~68행](../apps/clients/snmp_client.py#L16-L68)
- DXi SSH/CLI client: [`apps/clients/dxi_cli_client.py` 58~107행](../apps/clients/dxi_cli_client.py#L58-L107)
- 공통 REST client: [`apps/clients/rest_client.py` 14~72행](../apps/clients/rest_client.py#L14-L72)
- 장비별 parser/collector 검증: [`tests/test_collectors.py` 41~675행](../tests/test_collectors.py#L41-L675)

### 4.2 비동기 스케줄러와 장애 격리

**이력서 표현**

> Collector와 수집 클래스별 독립 asyncio task를 구성하고, 개별 수집 또는
> Elasticsearch 저장 실패가 다른 Collector와 다음 실행 주기에 전파되지
> 않도록 장애 격리 및 자동 복구 구조를 구현했다.

**왜 의미 있는가**

- 장시간 실행되는 운영 프로세스는 일시적인 API·네트워크 오류로 종료되면 안 된다.
- Collector마다 독립 task를 만들고 루프 내부에서 예외를 처리한다.
- 종료 시 task, HTTP client, Elasticsearch client를 정리한다.

**코드 근거**

- 클래스별 task 생성: [`apps/scheduler.py` 41~58행](../apps/scheduler.py#L41-L58)
- 예외 발생 후 다음 스케줄 계속 실행: [`apps/scheduler.py` 96~124행](../apps/scheduler.py#L96-L124)
- resource close 처리: [`apps/scheduler.py` 60~76행](../apps/scheduler.py#L60-L76)
- Collector/Writer 복구 테스트: [`tests/test_scheduler.py` 187~219행](../tests/test_scheduler.py#L187-L219)
- 한 Collector의 실패가 다른 Collector를 중단하지 않는 테스트:
  [`tests/test_scheduler.py` 221~247행](../tests/test_scheduler.py#L221-L247)

### 4.3 NetWorker 운영 서버 부하 보호

**이력서 표현**

> NetWorker 수집을 jobs·backups·inventory로 세분화하고 조회 시간 범위,
> endpoint, 요청 간격, 실행 중복을 제어하여 운영 서버에 대한 REST 요청 집중과
> 수집 backlog를 방지했다.

**구현 내용**

- jobs 5분, backups 15분, inventory 60분 등 수집 성격별 주기 분리
- jobs와 backups에 서로 다른 조회 window 적용
- inventory는 client와 policy 등 필요한 endpoint만 조회
- 동일 서버 요청 직렬화 및 요청 사이 cooldown
- 이전 실행과 겹치는 jobs 실행은 queue에 쌓지 않고 skip
- CORE, CHNL, INFO 서버의 실행 시점 분산

**코드 근거**

- 수집 클래스별 endpoint와 조회 범위 선택:
  [`apps/clients/networker_rest_client.py` 107~160행](../apps/clients/networker_rest_client.py#L107-L160)
- 요청 순차 실행과 spacing:
  [`apps/clients/networker_rest_client.py` 132~153행](../apps/clients/networker_rest_client.py#L132-L153)
- 중복 jobs skip, 서버별 lock과 cooldown:
  [`apps/scheduler.py` 127~168행](../apps/scheduler.py#L127-L168)
- 운영 스케줄·동시성·조회 window 설정:
  [`k8s/networker-core.yaml` 13~38행](../k8s/networker-core.yaml#L13-L38)
- 실행 중복과 cooldown 테스트:
  [`tests/test_scheduler.py` 269~379행](../tests/test_scheduler.py#L269-L379)
- endpoint·window·요청 spacing 테스트:
  [`tests/test_rest_clients.py` 169~283행](../tests/test_rest_clients.py#L169-L283)

**관련 커밋**

- `de872b3 perf(networker): protect servers with scoped REST collection`
- `6db02c3 perf(networker): split collection schedules to protect servers`

### 4.4 REST connection 재사용, 제한적 병렬 처리, 부분 실패 허용

**이력서 표현**

> application lifespan 동안 HTTP connection pool을 재사용하고 Semaphore로
> endpoint 동시성을 제한했으며, 일부 endpoint 실패 시 성공한 응답은 유지하는
> partial-success 수집 모델을 구현했다.

**왜 의미 있는가**

- 매 수집마다 client를 생성하는 비용과 connection churn을 줄인다.
- 무제한 병렬 요청으로 장비 API에 부하를 주지 않는다.
- endpoint 하나의 장애 때문에 전체 수집 데이터를 버리지 않는다.

**코드 근거**

- lazy `httpx.AsyncClient`와 TLS/CA 설정:
  [`apps/clients/rest_client.py` 14~39행](../apps/clients/rest_client.py#L14-L39)
- Semaphore 기반 제한적 병렬 호출과 endpoint별 오류 보존:
  [`apps/clients/rest_client.py` 41~67행](../apps/clients/rest_client.py#L41-L67)
- partial 상태 생성 예시:
  [`apps/collectors/networker_rest_collector.py` 37~61행](../apps/collectors/networker_rest_collector.py#L37-L61)
- 동시성·부분 실패·connection reuse 테스트:
  [`tests/test_rest_clients.py` 31~114행](../tests/test_rest_clients.py#L31-L114)

### 4.5 Elasticsearch 메모리 사용 최적화와 데이터 모델링

**이력서 표현**

> 수집 결과를 generator 기반으로 변환하고 Elasticsearch Bulk API에 제한된
> chunk로 스트리밍하여 대용량 NetWorker payload 처리 시 중간 메모리
> materialization을 줄였다.

**코드 근거**

- generator 기반 bulk action 생성과 chunk 제한:
  [`apps/writers/elasticsearch.py` 43~64행](../apps/writers/elasticsearch.py#L43-L64)
- 결과를 raw/current/derived action으로 스트리밍:
  [`apps/writers/elasticsearch.py` 76~124행](../apps/writers/elasticsearch.py#L76-L124)
- derived document iterator:
  [`apps/processors/derived.py` 7~40행](../apps/processors/derived.py#L7-L40)
- bounded chunk 테스트:
  [`tests/test_elasticsearch_writer.py` 40~73행](../tests/test_elasticsearch_writer.py#L40-L73)

**관련 커밋**

- `bb53bfe perf(networker): reduce memory usage during collection`

### 4.6 실시간 상태와 분석 이력의 저장 모델 분리

**이력서 표현**

> Prometheus는 실시간 상태와 수치, Elasticsearch는 RAW·CURRENT·Derived
> 이력으로 역할을 분리하고, NetWorker job을 실제 이벤트 시각 기준으로 분석할
> 수 있는 하위 호환 job-run 문서 모델을 설계했다.

**구현 내용**

- RAW: 월별 수집 이력
- CURRENT: `{collector}:current` 고정 ID로 최신 상태 overwrite
- Derived: job, backup, client, policy, workflow 등 분석 엔티티
- Job-run: 실제 시작·종료 시각, duration, outcome을 포함한 분석 문서
- inventory/backups 수집이 jobs의 CURRENT 상태를 덮어쓰지 않도록 분리
- 안정적인 ID가 없는 이벤트는 중복 분석 문서를 만들지 않음

**코드 근거**

- CURRENT 기록 조건과 고정 ID:
  [`apps/writers/elasticsearch.py` 76~91행](../apps/writers/elasticsearch.py#L76-L91),
  [`apps/writers/elasticsearch.py` 222~223행](../apps/writers/elasticsearch.py#L222-L223)
- 보안 도메인·엔티티·월별 derived index routing:
  [`apps/writers/elasticsearch.py` 96~124행](../apps/writers/elasticsearch.py#L96-L124)
- job-run event time, duration, stable ID:
  [`apps/processors/derived.py` 78~134행](../apps/processors/derived.py#L78-L134)
- CURRENT와 job-run 호환성 테스트:
  [`tests/test_elasticsearch_writer.py` 121~159행](../tests/test_elasticsearch_writer.py#L121-L159),
  [`tests/test_elasticsearch_writer.py` 337~420행](../tests/test_elasticsearch_writer.py#L337-L420)
- 전체 스키마 설명: [`docs/collection_data_schema.md`](collection_data_schema.md)

**관련 커밋**

- `7e2caac feat(networker): add backward-compatible job-run analytics model`

### 4.7 Kubernetes 장애·인증정보 격리

**이력서 표현**

> NetWorker CORE·CHNL·INFO 서버를 독립 Kubernetes Deployment로 분리하고,
> 공용 Secret을 사용하되 각 Pod에는 해당 서버의 CA·계정 key만 선택적으로
> mount하도록 구성해 장애 범위와 credential 노출 범위를 축소했다.

**구현 내용**

- 서버별 ConfigMap, Deployment, Service
- `replicas: 1`과 `Recreate`로 배포 중 중복 수집 방지
- liveness/readiness probe
- 서버별 `hostAliases`
- 공통 Elasticsearch Secret + 자기 서버의 CA/username/password만 mount
- resource requests/limits 지정

**코드 근거**

- CORE Deployment 예시:
  [`k8s/networker-core.yaml` 58~129행](../k8s/networker-core.yaml#L58-L129)
- CHNL/INFO 독립 manifest:
  [`k8s/networker-chnl.yaml`](../k8s/networker-chnl.yaml),
  [`k8s/networker-info.yaml`](../k8s/networker-info.yaml)
- 서버별 workload와 스케줄 분리 검증:
  [`tests/test_k8s.py` 40~129행](../tests/test_k8s.py#L40-L129)
- Pod별 Secret key 격리 검증:
  [`tests/test_k8s.py` 131~152행](../tests/test_k8s.py#L131-L152)

**관련 커밋**

- `e0c0629 refactor(networker): isolate servers into separate workloads`

### 4.8 hostname 기반 보안 도메인 분류

**이력서 표현**

> NetWorker client hostname을 FQDN·short name 기준으로 정규화하여
> CORE·CHNL·INFO 보안 도메인으로 분류하고, 충돌·미매핑 데이터를 명시적으로
> 검증하는 라우팅 계층을 구현했다.

**코드 근거**

- 정규화, 중복·충돌 검증, 분류 fallback:
  [`apps/classifiers/hostname.py` 15~94행](../apps/classifiers/hostname.py#L15-L94)
- FQDN/short name/미매핑/충돌 테스트:
  [`tests/test_hostname_classifier.py` 9~76행](../tests/test_hostname_classifier.py#L9-L76)

### 4.9 기존 설정과 데이터 모델의 하위 호환성

**이력서 표현**

> 기존 단일 schedule 설정을 유지하면서 fast/slow 및
> jobs/backups/inventory 클래스별 스케줄로 확장하고, 기존 job 문서를 변경하지
> 않은 채 job-run 분석 모델을 병행 도입했다.

**코드 근거**

- legacy 단일 스케줄 호환 계층:
  [`apps/core/config.py` 116~138행](../apps/core/config.py#L116-L138)
- legacy·단일·복수 schedule 회귀 테스트:
  [`tests/test_config.py` 79~198행](../tests/test_config.py#L79-L198)
- 기존 job과 job-run 병행 라우팅 테스트:
  [`tests/test_elasticsearch_writer.py` 369~420행](../tests/test_elasticsearch_writer.py#L369-L420)

### 4.10 테스트 가능한 운영 요구사항

**이력서 표현**

> 설정 호환성, parser, REST 동시성·부분 실패, scheduler 복구·중복 방지,
> Elasticsearch routing, Kubernetes Secret 격리를 포함한 자동화 테스트
> 82개를 구축했다.

**검증 기록**

- 2026-08-28 기준 전체 테스트: `82 passed`
- 로컬 Python 환경의 손상된 외부 pytest plugin metadata를 피하기 위해
  plugin auto-load를 끄고 `pytest-asyncio`를 직접 등록하여 검증했다.
- 검증 명령:

```powershell
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD='1'
py -c "import pytest, pytest_asyncio.plugin as plugin; raise SystemExit(pytest.main(['-q'], plugins=[plugin]))"
```

테스트 통과는 코드 동작의 근거이지만, 실제 운영 환경의 처리량·가용성·성능을
증명하는 것은 아니다.

## 5. 이력서용 최종 문장

### 5.1 간결한 버전

**이기종 백업 인프라 통합 모니터링 플랫폼**

- SNMP·SSH CLI·REST 기반 5종 백업/스토리지 장비 수집기를 Python·FastAPI·asyncio로 통합
- NetWorker 수집을 jobs·backups·inventory로 분리하고 조회 window, 요청 간격,
  중복 실행을 제어하여 운영 서버 부하와 backlog 방지
- Prometheus 실시간 메트릭과 Elasticsearch RAW·CURRENT·Derived·Job-run 모델을
  설계하고 generator 기반 Bulk 적재로 대용량 데이터 처리
- CORE·CHNL·INFO를 독립 Kubernetes 워크로드로 분리하고 CA·계정 Secret key를
  Pod별 선택 mount하여 장애와 인증정보 격리
- 부분 endpoint 실패 허용, scheduler 자동 복구, 데이터 routing 및 배포 구성을
  검증하는 자동화 테스트 82개 구축

### 5.2 한 줄 버전

> 이기종 백업 장비 5종의 SNMP·SSH·REST 데이터를 통합 수집하고, 장비 부하
> 보호·부분 장애 복구·스트리밍 저장·Kubernetes 격리를 구현한 비동기 모니터링
> 플랫폼을 개발했다.

### 5.3 경력기술서용 상세 버전

> Python/FastAPI 기반의 백업 인프라 Collector를 설계하여 DXi, DD, i6000,
> NetWorker, ZFS의 이기종 데이터를 통합했다. asyncio task와 수집 클래스별
> scheduler를 구현하고 NetWorker의 jobs·backups·inventory 요청을 서로 다른
> 주기와 조회 범위로 분리했으며, 서버별 lock·cooldown·overlap skip을 통해
> 운영 API 요청 집중을 방지했다. 수집 데이터는 Prometheus 실시간 메트릭과
> Elasticsearch RAW·CURRENT·Derived·Job-run 모델로 분리하고 generator 및 Bulk
> API로 적재했다. Kubernetes에서는 CORE·CHNL·INFO를 별도 Deployment로
> 격리하고 Pod별 필요한 Secret key만 mount했으며, 관련 동작을 82개 자동화
> 테스트로 검증했다.

## 6. 면접에서 설명하기 좋은 문제 해결 사례

### 사례 A: "Pod를 합칠 것인가, 나눌 것인가"

1. 초기에는 리소스 효율을 위해 NetWorker Collector 통합을 검토했다.
2. 실제 운영 관점에서는 한 서버의 장애·지연이 다른 서버에 영향을 주고,
   인증서와 계정도 함께 노출되는 문제가 있었다.
3. CORE·CHNL·INFO를 별도 Deployment로 분리하고 각 Pod에 자기 Secret key만
   mount했다.
4. 서버별 장애 범위와 credential 노출 범위를 줄이고 독립 배포가 가능해졌다.

이 사례는 단순히 "마이크로서비스로 나눴다"가 아니라 **리소스 효율과 장애
격리 사이의 트레이드오프를 운영 요구에 따라 판단했다**는 점을 강조한다.

### 사례 B: "빠른 수집보다 서버 보호"

1. NetWorker 전체 데이터를 동일한 5분 주기로 조회하면 API 요청과 payload가
   불필요하게 커질 수 있었다.
2. jobs, backups, inventory의 최신성 요구가 서로 다름을 확인했다.
3. 수집 주기·endpoint·조회 window를 분리하고 요청을 직렬화했다.
4. 겹치는 jobs 실행은 대기시키지 않고 skip하여 backlog 증가를 막았다.

이 사례에서는 최적화 목표가 최대 처리량이 아니라 **운영 장비를 보호하면서
필요한 최신성을 만족하는 것**이었다고 설명한다.

### 사례 C: "일부 실패에서도 데이터 보존"

1. 여러 REST endpoint 중 하나의 장애가 전체 수집 실패로 이어질 수 있었다.
2. `asyncio.gather(..., return_exceptions=True)`로 endpoint별 결과를 분리했다.
3. 성공 응답은 유지하고 실패 정보는 `endpoint_errors`에 기록했다.
4. 저장 문서는 `partial` 상태로 표시해 데이터와 장애 가시성을 모두 보존했다.

## 7. 추가 측정 후 넣을 수 있는 정량 지표

다음 값은 현재 코드만으로 확정할 수 없다. 운영 로그, Prometheus 또는
Kubernetes metric으로 측정한 뒤 사용한다.

| 측정 항목 | 측정 전 | 측정 후 | 이력서 표현 예시 |
|---|---:|---:|---|
| NetWorker 시간당 REST 요청 수 | TBD | TBD | 요청량 `N%` 감소 |
| Collector peak working set | TBD MiB | TBD MiB | 최대 메모리 `N%` 절감 |
| 평균/95% 수집 시간 | TBD | TBD | 수집 시간 `N%` 단축 |
| Elasticsearch 요청 수 | TBD | TBD | Bulk 적용으로 요청 수 `N%` 감소 |
| 운영 장비/서버 수 | - | TBD대 | `N`대 통합 모니터링 |
| 하루 적재 문서 수 | - | TBD건 | 일 `N`건 이력 처리 |
| 수동 점검 소요 시간 | TBD | TBD | 점검 시간 `N%` 단축 |
| 장애 후 다음 성공까지 시간 | TBD | TBD | 평균 `N`분 내 자동 복구 |

추천 관찰 metric:

- `backup_collector_collection_duration_seconds`
- `backup_collector_collection_total`
- `backup_collector_scheduler_loop_error_total`
- `backup_collector_scheduler_overlap_skipped_total`
- Pod `container_memory_working_set_bytes`
- NetWorker access log의 endpoint별 request count

## 8. 근거 없이 쓰면 안 되는 표현

현재 저장소만으로는 다음을 주장하면 안 된다.

- "메모리를 40% 절감했다" — 전후 실측값 필요
- "API 요청을 70% 줄였다" — access log 비교 필요
- "무중단 배포를 구현했다" — NetWorker는 의도적으로 `Recreate` 사용
- "고가용성 클러스터" — 단일 노드·replica 1 운영 전제
- "exactly-once 수집" — 안정적인 document ID로 중복을 줄이지만 수집 자체가
  exactly-once인 것은 아님
- "운영 장애 0건" 또는 특정 SLA — 운영 기록 필요
- "보안 인증을 획득했다" — Secret 격리는 구현했지만 인증·감사 증거는 없음
- "실서비스 운영" — 실제 배포·운영 사실을 본인이 확인한 경우에만 사용

## 9. 포트폴리오 공개 전 확인 사항

- 실제 장비 hostname, IP, username, certificate, Secret이 포함되지 않았는지 확인
- 사내 장비 구성과 보안 도메인 이름을 공개해도 되는지 확인
- `TO_BE_FILLED` 예제만 남기고 운영 overlay와 Secret은 제외
- 아키텍처 그림과 데이터 흐름을 README 또는 포트폴리오에 포함
- 정량 수치는 측정 방법과 기간을 함께 기록
- 본인이 직접 담당한 범위와 팀이 담당한 범위를 구분

## 10. 핵심 근거 커밋

| 커밋 | 증명하는 내용 |
|---|---|
| `9f42b09` | DD/DXi dedup·compression metric 통합 |
| `7e2caac` | 하위 호환 NetWorker job-run 분석 모델 |
| `bb53bfe` | derived/bulk 처리의 메모리 최적화 |
| `de872b3` | 조회 범위·필드·요청 간격을 통한 서버 보호 |
| `6db02c3` | jobs/backups/inventory 주기 분리와 overlap 방지 |
| `e0c0629` | CORE/CHNL/INFO 서버별 Kubernetes workload 격리 |

커밋 이력과 코드 라인은 면접에서 구현 기여도를 설명하는 근거로 사용할 수
있지만, 회사 외부에 저장소를 공개할 수 있는지는 별도로 확인해야 한다.

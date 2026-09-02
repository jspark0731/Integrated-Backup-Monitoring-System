# NetWorker Collector Implementation Guide

## 1. 목적

Integrated Backup Monitoring System의 NetWorker Collector를 개선한다.

목표는 NetWorker REST API에서 대시보드에 필요한 최소 Raw Data만 안전하게 수집하고,
데이터 정제 후 Elasticsearch에 저장하는 것이다.

특히 `/nwrestapi/v3/global/jobs` 전체 조회는 금지한다.

실제 NetWorker 서버에서 필터 없이 `/jobs` 전체 조회 시 다음 문제가 확인되었다.

- HTTP 500
- `java.lang.OutOfMemoryError: GC overhead limit exceeded`
- `XmlJobList` serialization 오류
- `Broken pipe`
- NetWorker WebAPI JVM 부하 증가

따라서 모든 대량 Endpoint는 반드시 시간 범위 필터, 필요한 경우 field filtering, overlap 기반 증분 수집을 적용한다.

---

## 2. 전체 수집 구조

NetWorker 데이터는 다음 3개 Collection Class로 분리한다.

```text
jobs
    └─ 실행 상태 / 성공 / 실패 / 수행시간

backups
    └─ 실제 백업 이력 / Saveset / Level / Backup Size

inventory
    └─ Client / Policy / Workflow 등 구성정보
```

기본 Scheduler:

```text
jobs       : 5분
backups    : 15분
inventory  : 60분
```

각 collection은 독립적으로 실패할 수 있어야 하며,
하나의 collection 실패가 다른 collection 실행을 막지 않아야 한다.

---

## 3. 필요한 데이터

### 3.1 Jobs

대시보드에서 다음 정보를 표현하기 위해 필요하다.

- 성공 Job 수
- 실패 Job 수
- 실행 중 Job
- 최근 Backup Job 결과
- Client별 성공/실패
- Policy별 성공률
- Workflow별 성공률
- Job 수행시간
- 장기 수행 Job

필요한 Raw Field 후보:

```text
id
name
type
state
completionStatus
exitCode
startTime
endTime
clientName
clientHostname
policyName
workflowName
```

단, 위 필드가 현재 NetWorker 버전에서 모두 `fl=` filtering을 지원한다고 가정하지 않는다.

구현 시 field selection 목록을 설정값으로 관리하고,
실제 API에서 지원하지 않는 field가 확인되면 제거할 수 있도록 한다.

### 3.2 Backups

대시보드에서 다음 정보를 표현하기 위해 필요하다.

- 일별 Backup Volume
- 월별 Backup Volume
- Client별 Backup Volume
- Saveset별 Backup Volume
- Backup Level별 통계
- 월간 백업 이력

필요한 Raw Data:

```text
backup_id
client_name
save_set
backup_level
completion_time
backup_size
backup_size_unit
```

실제 NetWorker Response field 이름에 맞게 adapter에서 mapping한다.

최종 저장 시 size는 반드시 bytes 단위로 정규화한다.

### 3.3 Inventory

필요한 정보:

```text
client
hostname
os
enabled/status

policy
workflow
action (필요 시)
```

Inventory 정보는 변경 빈도가 낮으므로 실시간 수집하지 않는다.

---

## 4. Endpoint 설계

### Jobs

```text
GET /nwrestapi/v3/global/jobs
```

용도:

```text
Job 상태
성공/실패
실행시간
Client
Policy
Workflow
```

중요:

```text
GET /nwrestapi/v3/global/jobs
```

형태의 전체 조회는 절대 운영 코드에서 사용하지 않는다.

반드시 `q=endTime:[...]` 형태로 기간을 제한한다.

현재 서버에서 정상 동작이 확인된 예:

```text
q=endTime:["2026-08-31T15:20:00" TO "2026-08-31T15:30:00"]
```

HTTP 200 및 해당 기간 내 Job 반환이 확인되었다.

### Backups

```text
GET /nwrestapi/v3/global/backups
```

용도:

```text
실제 Backup History
Backup Size
Saveset
Backup Level
Client
Completion Time
```

`/backups` 역시 전체 history 조회를 피한다.

`completionTime` 등 해당 API가 지원하는 시간 조건을 이용하여 증분 조회한다.

### Inventory

NetWorker REST API에서 Client / Protection Policy / Workflow 관련 Endpoint를 사용한다.

현재 코드에 이미 존재하는 endpoint/configuration을 우선 재사용한다.

Endpoint 이름을 임의로 새로 정의하지 말고 현재 repository 및 실제 NetWorker REST response를 기준으로 구현한다.

---

## 5. Jobs 수집 방식

### 5.1 기본 원칙

수집 주기:

```text
every 5 minutes
```

조회 범위:

```text
최근 10분
```

예:

```text
15:30 실행
→ 15:20 ~ 15:30

15:35 실행
→ 15:25 ~ 15:35
```

의도적으로 5분 overlap을 둔다.

이유:

- Collector 일시 장애
- NetWorker 응답 지연
- Job completion 반영 지연
- Scheduler 지연

등에 의한 누락 방지.

### 5.2 Query 생성

Python HTTP Client에서는 query string을 직접 문자열 concat하거나 수동 URL encoding하지 않는다.

예:

```python
params = {
    "q": f'endTime:["{start_time}" TO "{end_time}"]'
}
```

`httpx` 또는 현재 repository의 HTTP Client가 query parameter encoding을 담당하도록 한다.

shell 테스트 시 사용한 `curl --data-urlencode`와 동일한 의미가 되어야 한다.

### 5.3 Field Filtering

시간 filtering이 정상 동작하는 것을 우선 보장한 후 `fl`을 적용한다.

처음에는 최소 field:

```text
id
name
type
state
completionStatus
startTime
endTime
```

을 사용한다.

이후 실제 API 지원 여부를 검증하면서 다음을 추가한다.

```text
exitCode
clientName
clientHostname
policyName
workflowName
```

`fl`에 지원하지 않는 field가 들어가 HTTP 400이 발생할 가능성이 있으므로,
오류 발생 시 어떤 field list로 요청했는지 로그에 남겨야 한다.

---

## 6. Active Job 처리

기존 구현에서:

```text
q=completionStatus:Running
```

형태의 요청이 HTTP 400을 반환한 이력이 있다.

따라서 `Running`을 completionStatus 값으로 가정하여 별도 API 요청하지 않는다.

우선 최근 시간 범위 Job을 조회한 뒤,
실제 반환되는:

```text
state
completionStatus
```

값을 기준으로 active/completed 상태를 Collector 내부에서 분류한다.

즉 가능하면:

```text
1번의 최근 Jobs 조회
    ↓
Collector 내부
    ├─ active
    ├─ succeeded
    └─ failed
```

구조로 구현한다.

불필요하게 동일 `/jobs` endpoint를 여러 번 호출하지 않는다.

---

## 7. Backups 수집 방식

수집 주기:

```text
15분
```

권장 조회 범위:

```text
최근 20~30분
```

또는:

```text
last_successful_collection_time - overlap
~
current_time
```

방식으로 구현한다.

Backups 역시 overlap을 둔다.

중복은 Elasticsearch Document ID로 제거한다.

Backup history를 매번 전체 조회하지 않는다.

---

## 8. Inventory 수집 방식

수집 주기:

```text
60분
```

Inventory는 full snapshot 형태를 허용할 수 있으나,
Client / Policy / Workflow 개수가 많을 경우 필요 없는 nested data는 제거한다.

목적은 다음의 mapping 정보를 확보하는 것이다.

```text
Client
→ Hostname
→ OS

Policy
→ Workflow
→ Action
```

---

## 9. Raw Data 정제

### 9.1 Timestamp

NetWorker timestamp:

```text
startTime
endTime
completionTime
```

은 Elasticsearch에 UTC ISO-8601 형식으로 저장한다.

예:

```text
2026-08-31T06:30:00Z
```

KST 변환은 Grafana/Kibana presentation layer에서 처리한다.

### 9.2 Job Duration

다음 derived field를 생성한다.

```text
duration_seconds
```

계산:

```text
endTime - startTime
```

Running Job 등 endTime이 없는 경우 null 처리한다.

### 9.3 Status Normalization

Raw NetWorker 상태는 보존한다.

추가로 dashboard용 normalized status를 생성한다.

예:

```text
Succeeded → success
Failed    → failed
Running   → running
Canceled  → canceled
Unknown   → unknown
```

필드 예:

```text
job.status_raw
job.status_normalized
```

raw 값을 잃지 않는다.

### 9.4 Backup Size

KB / MB / GB 등의 값은 모두 bytes로 변환한다.

최종 field:

```text
backup.size_bytes
```

원본 값이 필요하다면:

```text
backup.size_raw
backup.size_unit_raw
```

도 같이 보존한다.

### 9.5 Client Normalization

Client aggregation 오류를 방지하기 위해 normalized field를 생성한다.

예:

```text
SERVER01
server01
server01.example.com
```

같은 값이 업무상 동일 장비일 경우 별도 canonical hostname 정책을 적용할 수 있도록 한다.

단, 현재 raw clientName / hostname 값을 임의로 삭제하지 않는다.

---

## 10. Elasticsearch Document 설계

Jobs와 Backups는 반드시 별도 document family로 관리한다.

Jobs example:

```json
{
  "@timestamp": "2026-08-31T06:30:00Z",
  "collector": "networker_core",
  "document_family": "job",
  "job": {
    "id": "2884270",
    "name": "nsrrecopy",
    "type": "save",
    "state": "Completed",
    "completion_status": "Succeeded",
    "status_normalized": "success",
    "exit_code": 0,
    "start_time": "2026-08-31T06:24:00Z",
    "end_time": "2026-08-31T06:30:00Z",
    "duration_seconds": 360
  },
  "client": {
    "name": "server01",
    "hostname": "server01.example.com"
  },
  "policy": {
    "name": "FS_DAILY"
  },
  "workflow": {
    "name": "DAILY_BACKUP"
  }
}
```

Backup example:

```json
{
  "@timestamp": "2026-08-31T06:30:00Z",
  "collector": "networker_core",
  "document_family": "backup",
  "backup": {
    "id": "backup-id",
    "save_set": "/data",
    "level": "Full",
    "size_bytes": 1234567890,
    "completion_time": "2026-08-31T06:30:00Z"
  },
  "client": {
    "name": "server01"
  }
}
```

---

## 11. Deduplication / Upsert

증분 수집은 overlap 방식이므로 동일 data가 반복 수집되는 것이 정상이다.

중복 document를 생성하지 않는다.

Jobs:

```text
Elasticsearch _id =
<networker_instance>:job:<job_id>
```

예:

```text
networker_core:job:2884270
```

Backups:

```text
Elasticsearch _id =
<networker_instance>:backup:<backup_id>
```

Backup ID가 안정적으로 제공되지 않는 경우
client + saveset + completionTime 등 deterministic key를 사용하되,
기존 repository의 ID 생성 규칙이 있다면 이를 우선 사용한다.

---

## 12. Summary 데이터

Raw data가 정상 수집되지 않았을 때:

```text
job_count = 0
backup_count = 0
```

만 저장하면 실제 데이터가 0건인 것으로 오해할 수 있다.

따라서 반드시 collection 상태를 같이 저장한다.

예:

```json
{
  "collection_status": "success",
  "endpoint_errors": {}
}
```

또는:

```json
{
  "collection_status": "partial",
  "endpoint_errors": {
    "jobs": "HTTP 400 ..."
  }
}
```

규칙:

```text
success
    모든 필요한 endpoint 성공

partial
    일부 endpoint 실패

failed
    해당 collection 자체가 유효한 데이터를 만들 수 없음
```

Raw API 실패 시 summary의 0을 실제 업무 데이터 0건으로 해석하지 않도록 한다.

---

## 13. Error Handling

다음 HTTP 상태는 구분하여 처리한다.

```text
400
→ query/filter/field 문제
→ retry 금지 또는 제한

401 / 403
→ 인증/권한 문제

429
→ backoff 후 retry

500
→ NetWorker WebAPI server-side failure
→ 즉시 무제한 retry 금지

timeout
→ 제한된 retry + backoff
```

특히 `/jobs`에서 500이 발생한 경우
fallback으로 full `/jobs` 요청을 시도하지 않는다.

절대 다음과 같은 fallback을 만들지 않는다.

```python
try:
    filtered_jobs()
except:
    get_all_jobs()
```

이는 NetWorker WebAPI OOM을 재발시킬 수 있다.

---

## 14. Concurrency Protection

동일 collection이 중첩 실행되지 않도록 한다.

예:

```text
jobs scheduler 실행 중
    ↓
다음 5분 scheduler 도착
    ↓
기존 실행이 끝나지 않았다면 skip
```

process-local lock 또는 현재 scheduler가 지원하는
`max_instances=1`에 준하는 기능을 사용한다.

동일 NetWorker 서버에 여러 대형 REST request가 동시에 발생하지 않도록 한다.

---

## 15. Logging

INFO:

```text
collector
collection_class
endpoint
query window
returned item count
elapsed time
ES document count
collection_status
```

예:

```text
Networker/networker_core
class=jobs
window=2026-08-31T06:20:00Z..2026-08-31T06:30:00Z
count=12
duration=1.83s
status=success
```

DEBUG에서만:

```text
full request URL
NetWorker raw response 일부
HTTP Client internal trace
```

인증정보는 절대 로그에 기록하지 않는다.

---

## 16. Dashboard 요구사항과 Raw Data 매핑

| Dashboard | Source |
|---|---|
| 성공 Job 수 | jobs.completionStatus |
| 실패 Job 수 | jobs.completionStatus |
| 실행중 Job | jobs.state / completionStatus |
| 최근 작업 결과 | jobs |
| Client별 실패 | jobs.client |
| Policy별 성공률 | jobs.policy |
| Workflow별 성공률 | jobs.workflow |
| 수행시간 | startTime + endTime |
| 장기 수행 Job | duration_seconds |
| 일별 Backup Volume | backups.size_bytes + completionTime |
| 월별 Backup Volume | backups.size_bytes + completionTime |
| Client별 Backup Volume | backups.client + size_bytes |
| Saveset별 Backup Volume | backups.saveSet + size_bytes |
| Client Inventory | clients |
| OS 분포 | clients.os |

---

## 17. 구현 우선순위

기존 repository 구조를 최대한 유지하면서 다음 순서로 수정한다.

### Phase 1

`/jobs` 안전성 개선

- full jobs 조회 제거
- endTime range filter 적용
- 최근 10분 / 5분 주기
- overlap 적용
- 중첩 실행 방지

### Phase 2

`/jobs` field 최소화

- 실제 지원 field 검증
- `fl` 적용
- 필요한 field만 반환

### Phase 3

Jobs normalization

- timestamp
- duration
- status
- deterministic Elasticsearch ID

### Phase 4

Backups 증분화

- completionTime 기준 filtering
- overlap
- size_bytes 변환
- deterministic ID

### Phase 5

Inventory 정리

- Client
- Policy
- Workflow
- 필요 없는 Raw Data 제거

---

## 18. 구현 시 중요한 제한사항

1. 기존 working code를 불필요하게 대규모 refactoring하지 않는다.
2. NetWorker endpoint 이름이나 field 이름을 추측하지 않는다.
3. repository의 기존 adapter/model/config 구조를 우선 재사용한다.
4. API에서 확인되지 않은 field는 optional로 처리한다.
5. full `/jobs` 조회를 fallback으로 사용하지 않는다.
6. 기존 Elasticsearch Writer 호환성을 유지한다.
7. Prometheus export 기능이 있다면 기존 metric contract를 깨지 않는다.
8. collection 실패가 Pod crash로 이어지지 않게 한다.
9. 하나의 endpoint 실패가 다른 collection 실행을 중단하지 않게 한다.
10. 변경 후 기존 테스트가 있다면 모두 실행하고 필요한 테스트를 추가한다.

---

## 19. Acceptance Criteria

### Jobs

- `/global/jobs` full request가 코드에서 발생하지 않는다.
- 모든 정상 jobs 수집 요청에 시간 filter가 존재한다.
- 5분마다 최근 10분을 조회한다.
- HTTP 200 응답을 정상 parsing한다.
- Job ID 기반 중복 제거가 된다.
- API failure가 Pod crash로 연결되지 않는다.

### Backups

- 전체 backup history를 반복 조회하지 않는다.
- 시간 범위 기반 증분 수집한다.
- Backup Size가 bytes로 저장된다.
- 중복 document가 생성되지 않는다.

### Elasticsearch

- Raw Job/Backup 데이터가 조회 가능하다.
- collection_status가 명확하다.
- endpoint error가 존재하면 저장된다.
- 실제 API 오류 시 summary 0건과 정상 0건을 구분할 수 있다.

### NetWorker Server

Collector 운영 중 다음 로그가 재발하지 않는 것이 목표다.

```text
java.lang.OutOfMemoryError: GC overhead limit exceeded
```

`/jobs` 요청으로 인한 지속적인 HTTP 500 및 WebAPI JVM 부하가 발생하지 않아야 한다.

---

## 20. Codex 작업 방식

먼저 repository 전체에서 다음을 찾는다.

```text
NetWorker collector implementation
/global/jobs
/global/backups
scheduler
Elasticsearch Writer
networker models
settings/config
tests
```

현재 구현을 분석한 후 바로 코드를 작성하지 말고 먼저:

1. 현재 jobs 요청 방식
2. 현재 backups 요청 방식
3. scheduler 구조
4. ES document 구조
5. full endpoint 호출이 발생하는 위치
6. 수정해야 하는 파일 목록

을 정리한다.

이후 최소 변경으로 구현한다.

구현이 끝나면 다음을 보고한다.

```text
Modified files
주요 변경사항
기존 문제의 원인
새 query 예시
Elasticsearch document 변경사항
테스트 결과
남아 있는 위험요소
```

# Kubernetes 운영 가이드

## 배포 구조

단일 Kubernetes node에서 solution별 Deployment 하나를 기본으로 사용한다.

| Deployment | 내부 Collector | Service |
|---|---:|---|
| `backup-dashboard-collector-dxi` | 2 | 동일 이름 |
| `backup-dashboard-collector-dd` | 3 | 동일 이름 |
| `backup-dashboard-collector-i6000` | 4 | 동일 이름 |
| `backup-dashboard-collector-networker` | 3 | 동일 이름 |
| `backup-dashboard-collector-zfs` | 4 | 동일 이름 |

모든 Deployment는 replica 1을 기본으로 한다. 같은 Collector를 여러 replica로
실행하면 raw 데이터가 중복 저장되고 CURRENT 문서 write가 경합할 수 있으므로
leader election을 추가하기 전에는 replica를 늘리지 않는다.

## ConfigMap과 Secret

ConfigMap에는 다음 비밀이 아닌 값을 둔다.

- 장비 host, port, base URL, endpoint
- OID와 CLI command
- fast/slow schedule
- `rest.max_concurrency`
- TLS 검증 여부
- Elasticsearch host와 index 관련 설정

Secret에는 다음 값을 둔다.

- SNMP community
- SSH username, password 또는 private key
- REST token, username, password
- Elasticsearch username, password
- CA certificate

각 manifest의 `volumes[].secret.items`가 실제 Secret key와 mount 경로의
기준이다. Secret 이름은 다음과 같다.

| Solution | Secret |
|---|---|
| DXi | `backup-dashboard-collector-dxi-secrets` |
| DD | `backup-dashboard-collector-dd-secrets` |
| i6000 | `backup-dashboard-collector-i6000-secrets` |
| NetWorker | `backup-dashboard-collector-networker-secrets` |
| ZFS | `backup-dashboard-collector-zfs-secrets` |

공통 Elasticsearch certificate는 container에서
`/apps/secrets/elasticsearch/ca.crt`로 mount된다. 운영 CA를 사용하고
`verify_certs: true`를 유지한다. 장비 자체 CA를 추가해야 한다면 image trust
store 또는 별도 certificate volume을 운영 overlay에서 구성한다.

NetWorker 통합 Secret은 다음 key를 포함해야 한다.

```text
elasticsearch-username
elasticsearch-password
elasticsearch-ca-crt
networker-core-ca-crt
networker-core-username
networker-core-password
networker-chnl-ca-crt
networker-chnl-username
networker-chnl-password
networker-info-ca-crt
networker-info-username
networker-info-password
```

NetWorker REST API는 username/password Basic 인증을 사용한다. CORE, CHNL,
INFO 서버 인증서가 서로 다를 수 있으므로 CA 또는 self-signed certificate를
각각 `--from-file`로 저장하고 Collector별 `ca_certs` 경로로 검증한다.

```bash
kubectl create secret generic backup-dashboard-collector-networker-secrets \
  --namespace backup-dashboard \
  --from-file=elasticsearch-ca-crt=/secure/elasticsearch-ca.crt \
  --from-literal=elasticsearch-username=ELASTICSEARCH_USERNAME \
  --from-literal=elasticsearch-password=ELASTICSEARCH_PASSWORD \
  --from-file=networker-core-ca-crt=/secure/networker-core.cer \
  --from-literal=networker-core-username=CORE_USERNAME \
  --from-literal=networker-core-password=CORE_PASSWORD \
  --from-file=networker-chnl-ca-crt=/secure/networker-chnl.cer \
  --from-literal=networker-chnl-username=CHNL_USERNAME \
  --from-literal=networker-chnl-password=CHNL_PASSWORD \
  --from-file=networker-info-ca-crt=/secure/networker-info.cer \
  --from-literal=networker-info-username=INFO_USERNAME \
  --from-literal=networker-info-password=INFO_PASSWORD \
  --dry-run=client -o yaml | kubectl apply -f -
```

## NetWorker 서버 부하 보호

운영 manifest는 CORE, CHNL, INFO fast 수집을 매 10분의 3분, 6분, 9분으로
분산한다. 완료 job은 `endTime`, backup은 `saveTime` 기준 최근 20분만 조회하며
실행 중 job은 `completionStatus:Running` 필터로 별도 조회한다. 각 요청은 한
서버에서 순차 실행되고 요청 사이에 30초를 둔다.

```yaml
rest:
  max_concurrency: 1
  request_spacing_seconds: 30
  query_window_minutes: 20
  active_jobs_query: completionStatus:Running
  field_filter: true
```

slow client/policy inventory는 6시간 간격이다. `active_jobs_query`가 특정
NetWorker 버전에서 지원되지 않으면 실제 `/global/jobs` 응답의 필터 가능
필드를 확인한 뒤 값을 변경한다. 해당 요청만 실패해도 완료 job과 backup
응답은 `collection_status: partial`로 계속 처리된다.

시간 구간을 겹쳐 조회하므로 월간 집계는 raw의 `monthly_report`가 아니라
`NW-{DOMAIN}-BACKUP-YYYY-MM`의 안정적인 backup 이벤트 문서를 사용한다.

## 기본 리소스

| Collector | CPU request | Memory request | CPU limit | Memory limit |
|---|---:|---:|---:|---:|
| DXi | `100m` | `256Mi` | `500m` | `512Mi` |
| DD | `50m` | `128Mi` | `300m` | `256Mi` |
| i6000 | `100m` | `256Mi` | `500m` | `512Mi` |
| NetWorker | `100m` | `256Mi` | `1` | `1Gi` |
| ZFS | `100m` | `256Mi` | `500m` | `512Mi` |

NetWorker와 i6000은 inventory payload 크기에 따라 memory peak가 달라진다.
운영 첫 주에는 working set과 collection duration을 관찰하고 limit에 근접하면
memory를 우선 조정한다.

## 배포 전 입력값

1. 모든 ConfigMap의 `TO_BE_FILLED` host/base URL을 실제 값으로 바꾼다.
2. SNMP OID 및 DXi CLI command가 장비 firmware와 일치하는지 확인한다.
3. hostname classification CSV에 NetWorker 대상 host와 security domain을
   입력한다.
4. Elasticsearch를 사용할 경우 `enabled: true`, host, 인증 정보와 CA를
   구성한다.
5. 각 manifest가 참조하는 Secret을 생성한다.
6. REST TLS 인증서 검증과 방화벽 연결을 확인한다.
7. image tag를 운영 registry의 immutable tag로 바꾼다.

## 검증 및 배포

```bash
kubectl kustomize k8s >/tmp/backup-collector-rendered.yaml
kubectl apply -k k8s
kubectl get pods,svc -l app=backup-dashboard-collector
```

배포 후 각 Service에서 다음 endpoint를 확인한다.

```text
/healthz
/readyz
/collectors
/metrics
```

`/collectors`에서 `skip_reason`이 없어야 하며 fast/slow Collector는 두 schedule과
각각의 `last_results`가 표시되어야 한다.

## Elasticsearch 전환 주의사항

새 수집분부터 월간 RAW와 고정 CURRENT 인덱스에 기록한다. 기존 장비별 일자
인덱스는 자동 이전하거나 삭제하지 않는다. 기존 dashboard를 새 index pattern으로
전환하고, 과거 데이터가 필요하면 별도 검증된 reindex 작업으로 이전한다.

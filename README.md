# Backup Dashboard Collector

FastAPI 기반 통합 백업 인프라 수집기입니다. 하나의 공통 codebase에서
DXi, DD, i6000, NetWorker, ZFS용 이미지를 만들고 Prometheus용 실시간
메트릭과 Elasticsearch용 이력·현재 상태 데이터를 함께 생성합니다.

## Architecture

```text
DXi ── SSH CLI ────────┐
DD ─── SNMP ───────────┤
i6000 ─ REST ──────────┤
NetWorker ─ REST ──────┼─> CollectorScheduler ─┬─> Prometheus /metrics
ZFS ── REST ───────────┘                      └─> Elasticsearch bulk write
```

Kubernetes에서는 solution별로 다음 5개 Deployment를 사용합니다.

- `backup-dashboard-collector-dxi`
- `backup-dashboard-collector-dd`
- `backup-dashboard-collector-i6000`
- `backup-dashboard-collector-networker`
- `backup-dashboard-collector-zfs`

NetWorker의 CORE, CHNL, INFO, IFRS Collector는 NetWorker Deployment 하나에서
독립적인 asyncio scheduler task로 실행됩니다. 한 Collector의 실패는 다른
Collector task를 종료하지 않습니다.

## Collection Schedule

DXi와 DD는 기존 단일 5분 schedule을 사용합니다. REST Collector는 상태성
데이터와 inventory 데이터를 분리합니다.

| Collector | Fast, 기본 5분 | Slow, 기본 60분 |
|---|---|---|
| i6000 | library/drive/RAS 상태, RAS ticket, slot 사용량, reachability | media, tower, I/E station, physical library inventory |
| NetWorker | job, backup 실행 및 상태 | client, policy, workflow, protection group inventory |
| ZFS | API, pool, capacity, alert/fault | project, filesystem, LUN inventory |

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

기존 `schedule.interval_minutes` 형식과 `schedule_second`도 호환됩니다. 단일
schedule을 사용하면 기존과 같이 해당 Collector의 전체 데이터를 수집합니다.
`POST /collectors/run-once`는 설정된 fast와 slow 수집을 모두 실행합니다.

REST 요청은 application lifespan 동안 `httpx.AsyncClient` connection pool을
재사용합니다. 독립 endpoint는 병렬 호출하며 기본 동시 요청 수는 다음처럼
제한합니다.

```yaml
rest:
  max_concurrency: 4
```

일부 endpoint만 실패하면 성공 응답은 계속 처리하고 `collection_status:
partial` 및 `endpoint_errors`를 payload에 기록합니다.

## Storage

Prometheus는 장비 상태, 용량, job 수와 같은 즉시 갱신되는 숫자형 시계열을
담습니다. Elasticsearch는 raw 이력, 최신 summary, NetWorker entity 문서를
담습니다.

| 데이터 | 인덱스 |
|---|---|
| DD / DXi raw | `VTL-RAW-YYYY-MM` |
| DD / DXi current | `VTL-CURRENT` |
| i6000 raw | `PTL-RAW-YYYY-MM` |
| i6000 current | `PTL-CURRENT` |
| ZFS raw | `ZFS-RAW-YYYY-MM` |
| ZFS current | `ZFS-CURRENT` |
| NetWorker raw | `NW-OPS-RAW-{SOURCE}-YYYY-MM` |
| NetWorker current | `NW-OPS-CURRENT-{SOURCE}` |
| NetWorker derived | `NW-{DOMAIN}-{ENTITY}-YYYY-MM` |

CURRENT 문서는 `{collector}:current` ID로 overwrite됩니다. slow inventory
수집은 RAW와 derived 데이터만 기록하고 CURRENT 운영 상태를 덮어쓰지
않습니다. 기존 일자별 인덱스는 자동 reindex하거나 삭제하지 않습니다.

상세 문서 모델은 [collection_data_schema.md](docs/collection_data_schema.md)를
참조하세요.

## API

- `GET /healthz`
- `GET /readyz`
- `GET /collectors`
- `POST /collectors/run-once`
- `GET /metrics`

`GET /collectors`는 기존 `schedule`, `last_result` 필드와 함께
`schedules`, `last_results`의 fast/slow 상태를 제공합니다.

## Local Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp config/collector.example.yaml config/collector.yaml
APP_CONFIG=config/collector.yaml uvicorn apps.main:app --host 0.0.0.0 --port 8080
```

Windows PowerShell에서는 활성화 명령을
`.\.venv\Scripts\Activate.ps1`, 복사를
`Copy-Item config\collector.example.yaml config\collector.yaml`로 바꾸면 됩니다.

장비별 예제는 `config/collector.{dxi,dd,i6000,networker,zfs}.example.yaml`에
있습니다. `TO_BE_FILLED`가 남은 Collector는 안전하게 skipped 처리됩니다.

## Container Build

```bash
docker build --target dxi -t backup-dashboard-collector-dxi:latest .
docker build --target dd -t backup-dashboard-collector-dd:latest .
docker build --target i6000 -t backup-dashboard-collector-i6000:latest .
docker build --target networker -t backup-dashboard-collector-networker:latest .
docker build --target zfs -t backup-dashboard-collector-zfs:latest .
```

각 target에는 해당 Collector에 필요한 dependency만 설치됩니다.

## Kubernetes

ConfigMap의 host, endpoint, schedule 등을 운영 값으로 바꾸고 manifest가
참조하는 Secret을 생성한 다음 배포합니다.

```bash
kubectl apply -k k8s
```

Secret 키, 인증서 mount 경로, resource requests/limits, 운영자가 입력해야 할
값은 [operations.md](docs/operations.md)에 정리되어 있습니다.

## Test

```bash
pip install -e ".[dev]"
pytest -q
```

테스트는 schedule 호환성, fast/slow 실행, scheduler 예외 복구, REST
connection reuse·동시성·부분 실패, Elasticsearch 라우팅, NetWorker 단일
Deployment와 security-domain 분류를 포함합니다.

## Collector Documentation

- [DXi CLI](docs/dxi_cli_collection.md)
- [DD SNMP](docs/dd_snmp_collection.md)
- [i6000 REST](docs/i6000_rest_collection.md)
- [Collection data schema](docs/collection_data_schema.md)
- [Kubernetes operations](docs/operations.md)

# Backup Dashboard Collector

FastAPI based collector service for backup infrastructure visibility.

The project builds one collector codebase into five target-specific images:

- `backup-dashboard-collector-dxi`
- `backup-dashboard-collector-dd`
- `backup-dashboard-collector-i6000`
- `backup-dashboard-collector-networker`
- `backup-dashboard-collector-zfs`

## Collectors

- DXi CLI + SNMP: device identity and basic state from SNMP, plus capacity, deduplication, replication, interface, and alert data from CLI output
- DD SNMP: system identity, file-system capacity/compression, alerts, replication, and DD Boost status/storage-unit metrics from the Data Domain MIB
- i6000 REST: library, RAS subsystem, partition, drive, door, slot, and media status from the Scalar Web Services API
- Networker REST: job, client, protection policy, workflow, backup, and monthly summary data from the NetWorker REST API
- ZFS REST: appliance version, pool capacity, project, filesystem, LUN, alert, and fault data from the Oracle ZFS Storage Appliance REST API

Collectors with `TO_BE_FILLED` settings are skipped safely and logged.

## Local Run

```powershell
python -m venv .venv
.\.venv\Scripts\pip install -e .[dev]
copy config\collector.example.yaml config\collector.yaml
.\.venv\Scripts\uvicorn app.main:app --host 0.0.0.0 --port 8080
```

Edit `config\collector.yaml` before running against real devices.

Group-specific example configs are also available:

- `config/collector.dxi.example.yaml`
- `config/collector.dd.example.yaml`
- `config/collector.i6000.example.yaml`
- `config/collector.networker.example.yaml`
- `config/collector.zfs.example.yaml`

## Container Build

Build each collector image from the matching Docker target:

```powershell
docker build --target dxi -t backup-dashboard-collector-dxi:latest .
docker build --target dd -t backup-dashboard-collector-dd:latest .
docker build --target i6000 -t backup-dashboard-collector-i6000:latest .
docker build --target networker -t backup-dashboard-collector-networker:latest .
docker build --target zfs -t backup-dashboard-collector-zfs:latest .
```

Each image contains the common FastAPI scheduler plus only the dependencies needed
for that collector group.

## Kubernetes

The Kubernetes manifests deploy five independent collector workloads:

```powershell
kubectl apply -k k8s
```

Each workload has its own `Deployment`, `Service`, and `ConfigMap`:

- `backup-dashboard-collector-dxi`
- `backup-dashboard-collector-dd`
- `backup-dashboard-collector-i6000`
- `backup-dashboard-collector-networker`
- `backup-dashboard-collector-zfs`

Edit the matching ConfigMap before deployment, or replace it with an environment
specific config source.

## DXi CLI Collection

DXi capacity, deduplication, replication, interface, and alert counts are
collected through SSH CLI because the available DXi SNMP reference does not
document those OIDs.

See [DXi CLI Collection](docs/dxi_cli_collection.md).

## DD SNMP Collection

DD collection uses the Data Domain MIB under `1.3.6.1.4.1.19746`. The provided
DD4500/DD6900 MIBs cover the standard dashboard fields, including DD Boost
storage-unit metrics, so CLI fallback is not required unless command-native
troubleshooting output is needed. See [DD SNMP Collection](docs/dd_snmp_collection.md).

## i6000 REST Collection

i6000 collection uses the Scalar i6000 RESTful Web Services API only. This
keeps the operational setup to one access method while covering the former SNMP
status values and the richer slot/media data. See [i6000 REST Collection](docs/i6000_rest_collection.md).

## NetWorker REST Collection

NetWorker collection uses the REST API under `/nwrestapi/v3/global` by default.
It gathers jobs, clients, backups, protection policies, and protection groups,
then writes normalized job/client/policy/workflow/monthly report documents.
Configure `base_url` as the NetWorker server root, for example
`https://networker.example.com:9090`.

## ZFS REST Collection

ZFS collection uses the Oracle ZFS Storage Appliance REST API under `/api` by
default. Configure `base_url` as the appliance root, for example
`https://zfs-storage.example.com:215`. The collector gathers appliance version,
pools, pool details, projects, filesystems, LUNs, alert logs, and fault logs.

## Endpoints

- `GET /healthz`
- `GET /readyz`
- `GET /collectors`
- `POST /collectors/run-once`
- `GET /metrics`

## Storage

Elasticsearch receives the full collection document, including raw payloads.
Prometheus receives collector health metrics plus normalized DXi, DD, and i6000
gauges when the collectors can extract those values. NetWorker also publishes
API reachability, job counts by policy, workflow counts, and client count. ZFS
publishes API reachability, pool status, pool used capacity percent, and
alert/fault counts.

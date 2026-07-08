from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

COLLECTION_TOTAL = Counter(
    "backup_collector_collection_total",
    "Total collection attempts",
    ["collector", "target_type", "protocol", "status"],
)

COLLECTION_DURATION = Histogram(
    "backup_collector_collection_duration_seconds",
    "Collection duration in seconds",
    ["collector", "target_type", "protocol"],
)

COLLECTOR_SKIPPED = Gauge(
    "backup_collector_skipped",
    "Collector skip state, 1 means skipped",
    ["collector", "target_type", "reason"],
)

ELASTICSEARCH_WRITE_TOTAL = Counter(
    "backup_collector_elasticsearch_write_total",
    "Total Elasticsearch write attempts",
    ["status"],
)

COLLECTOR_LAST_SUCCESS_TIMESTAMP = Gauge(
    "backup_collector_last_success_timestamp",
    "Unix timestamp of the last successful collection",
    ["collector"],
)

DEVICE_UP = Gauge(
    "backup_device_up",
    "Device reachability, 1 means reachable",
    ["device_type", "device_name"],
)

DEVICE_CAPACITY_TOTAL_BYTES = Gauge(
    "backup_device_capacity_total_bytes",
    "Total device capacity in bytes",
    ["device_type", "device_name"],
)

DEVICE_CAPACITY_USED_BYTES = Gauge(
    "backup_device_capacity_used_bytes",
    "Used device capacity in bytes",
    ["device_type", "device_name"],
)

DEVICE_CAPACITY_USED_PERCENT = Gauge(
    "backup_device_capacity_used_percent",
    "Used device capacity percent",
    ["device_type", "device_name"],
)

DEVICE_DEDUP_RATIO = Gauge(
    "backup_device_dedup_ratio",
    "Device deduplication ratio",
    ["device_type", "device_name"],
)

DEVICE_ALERT_COUNT = Gauge(
    "backup_device_alert_count",
    "Device alert count by severity",
    ["device_type", "device_name", "severity"],
)

DEVICE_REPLICATION_UP = Gauge(
    "backup_device_replication_up",
    "Replication state, 1 means healthy or enabled",
    ["device_type", "device_name", "replication"],
)

DEVICE_INTERFACE_UP = Gauge(
    "backup_device_interface_up",
    "Interface state, 1 means up",
    ["device_type", "device_name", "interface"],
)

TAPE_LIBRARY_STATUS = Gauge(
    "backup_tape_library_status",
    "Tape library status, 1 means ready and online",
    ["device_name"],
)

TAPE_ROBOT_STATUS = Gauge(
    "backup_tape_robot_status",
    "Tape library robot status, 1 means healthy or ready",
    ["device_name", "robot"],
)

TAPE_DRIVE_STATUS = Gauge(
    "backup_tape_drive_status",
    "Tape drive status, 1 means online and ready",
    ["device_name", "drive"],
)

TAPE_DRIVE_ERROR_COUNT = Gauge(
    "backup_tape_drive_error_count",
    "Open drive-related RAS ticket count",
    ["device_name", "drive"],
)

TAPE_SLOT_USED_COUNT = Gauge(
    "backup_tape_slot_used_count",
    "Used tape storage slot count",
    ["device_name"],
)

TAPE_SLOT_FREE_COUNT = Gauge(
    "backup_tape_slot_free_count",
    "Available tape storage slot count",
    ["device_name"],
)

TAPE_MEDIA_COUNT = Gauge(
    "backup_tape_media_count",
    "Known tape media count",
    ["device_name"],
)

NETWORKER_API_UP = Gauge(
    "backup_networker_api_up",
    "NetWorker REST API reachability, 1 means reachable",
    ["server"],
)

NETWORKER_JOB_SUCCESS_COUNT = Gauge(
    "backup_networker_job_success_count",
    "NetWorker successful job count by policy",
    ["server", "policy"],
)

NETWORKER_JOB_FAILED_COUNT = Gauge(
    "backup_networker_job_failed_count",
    "NetWorker failed job count by policy",
    ["server", "policy"],
)

NETWORKER_JOB_RUNNING_COUNT = Gauge(
    "backup_networker_job_running_count",
    "NetWorker running job count by policy",
    ["server", "policy"],
)

NETWORKER_WORKFLOW_COUNT = Gauge(
    "backup_networker_workflow_count",
    "NetWorker workflow count by policy",
    ["server", "policy"],
)

NETWORKER_CLIENT_COUNT = Gauge(
    "backup_networker_client_count",
    "NetWorker unique client count",
    ["server"],
)

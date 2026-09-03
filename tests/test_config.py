from pathlib import Path

from apps.core.config import (
    CollectionSchedulesConfig,
    default_minute_offset,
    default_second,
    has_unfilled_values,
    load_config,
)


def test_default_schedule_offsets() -> None:
    assert default_minute_offset("DXi") == 0
    assert default_minute_offset("DD") == 1
    assert default_minute_offset("i6000") == 2
    assert default_minute_offset("Networker") == 3
    assert default_minute_offset("ZFS") == 4


def test_default_schedule_second_is_zero() -> None:
    assert default_second("DXi") == 0
    assert default_second("DD") == 0
    assert default_second("i6000") == 0
    assert default_second("Networker") == 0
    assert default_second("ZFS") == 0


def test_snmp_timeout_and_retries_have_defaults() -> None:
    config = load_config(Path("config/collector.dd.example.yaml"))

    assert config.collectors[0].timeout_seconds == 5
    assert config.collectors[0].retries == 1


def test_to_be_filled_is_detected() -> None:
    assert has_unfilled_values({"host": "TO_BE_FILLED"})
    assert has_unfilled_values({"host": "DXi_1_host_TO_BE_FILLED"})
    assert has_unfilled_values({"base_url": "networker_core_base_url_TO_BE_FILLED"})
    assert has_unfilled_values(["value", "TO_BE_FILLED"])
    assert not has_unfilled_values({"host": "192.0.2.10"})


def test_example_config_loads_and_marks_collectors_skipped() -> None:
    config = load_config(Path("config/collector.example.yaml"))

    assert len(config.collectors) == 16
    assert all(collector.skip_reason for collector in config.collectors)
    assert not any(collector.type == "i6000" and collector.protocol == "snmp" for collector in config.collectors)


def test_group_example_configs_load_expected_collectors() -> None:
    expected = {
        "dxi": (2, {"DXi"}),
        "dd": (3, {"DD"}),
        "i6000": (4, {"i6000"}),
        "networker": (3, {"Networker"}),
        "zfs": (4, {"ZFS"}),
    }

    for group, (count, collector_types) in expected.items():
        config = load_config(Path(f"config/collector.{group}.example.yaml"))

        assert len(config.collectors) == count
        assert {collector.type for collector in config.collectors} == collector_types
        expected_interval = 5
        assert all(
            collector.effective_schedule.interval_minutes == expected_interval
            for collector in config.collectors
        )
        if group == "networker":
            assert all(
                tuple(collection_class for collection_class, _ in collector.effective_schedules)
                == ("jobs", "backups", "inventory")
                for collector in config.collectors
            )
        assert all(collector.skip_reason for collector in config.collectors)


def test_legacy_schedule_second_loads_with_compatibility(tmp_path: Path) -> None:
    config_path = tmp_path / "collector.yaml"
    config_path.write_text(
        """
app:
  name: test
collectors:
  - name: networker_core
    type: Networker
    protocol: rest
    enabled: true
    schedule_second: 30
    base_url: https://networker.example.com:9090
    username: admin
    password: secret
    source_networker: core
    classification:
      hostname_csv_path: /apps/config/hostname_domain.csv
""",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.collectors[0].schedule_second == 30
    assert config.collectors[0].effective_schedule.interval_minutes == 1
    assert config.collectors[0].effective_schedule.second == 30
    assert config.collectors[0].source_networker == "core"
    assert config.collectors[0].hostname_csv_path == "/apps/config/hostname_domain.csv"


def test_fast_and_slow_schedules_load_with_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "collector.yaml"
    config_path.write_text(
        """
collectors:
  - name: i6000_core_rest
    type: i6000
    protocol: rest
    enabled: true
    schedule:
      fast:
        interval_minutes: 5
        minute_offset: 2
      slow:
        interval_minutes: 60
        minute_offset: 2
    base_url: https://i6000.example.com
    username: admin
    password: secret
    endpoints:
      status: aml/physicalLibrary/status
""",
        encoding="utf-8",
    )

    collector = load_config(config_path).collectors[0]

    assert isinstance(collector.schedule, CollectionSchedulesConfig)
    assert collector.effective_schedule == collector.schedule.fast
    assert collector.effective_schedules == (
        ("fast", collector.schedule.fast),
        ("slow", collector.schedule.slow),
    )
    assert collector.schedule.fast.second == 0
    assert collector.schedule.slow is not None
    assert collector.schedule.slow.second == 30
    assert collector.skip_reason is None


def test_single_schedule_remains_a_fast_only_schedule(tmp_path: Path) -> None:
    config_path = tmp_path / "collector.yaml"
    config_path.write_text(
        """
collectors:
  - name: DD4500
    type: DD
    protocol: snmp
    enabled: true
    schedule:
      interval_minutes: 5
      minute_offset: 1
      second: 0
    host: 192.0.2.10
    community: public
    oids:
      state: 1.3.6.1.4.1.19746.1.1.1.0
""",
        encoding="utf-8",
    )

    collector = load_config(config_path).collectors[0]

    assert collector.effective_schedules == (("fast", collector.effective_schedule),)


def test_invalid_slow_schedule_marks_collector_skipped(tmp_path: Path) -> None:
    config_path = tmp_path / "collector.yaml"
    config_path.write_text(
        """
collectors:
  - name: ZFS_1
    type: ZFS
    protocol: rest
    enabled: true
    schedule:
      fast:
        interval_minutes: 5
      slow:
        interval_minutes: 60
        second: 60
    base_url: https://zfs.example.com
    token: secret
""",
        encoding="utf-8",
    )

    collector = load_config(config_path).collectors[0]

    assert collector.skip_reason == "slow invalid schedule.second: 60"


def test_rest_max_concurrency_loads_and_validates(tmp_path: Path) -> None:
    config_path = tmp_path / "collector.yaml"
    config_path.write_text(
        """
collectors:
  - name: ZFS_1
    type: ZFS
    protocol: rest
    enabled: true
    base_url: https://zfs.example.com
    token: secret
    rest:
      max_concurrency: 2
""",
        encoding="utf-8",
    )

    collector = load_config(config_path).collectors[0]

    assert collector.rest_max_concurrency == 2
    assert collector.skip_reason is None

    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "max_concurrency: 2",
            "max_concurrency: 0",
        ),
        encoding="utf-8",
    )

    collector = load_config(config_path).collectors[0]

    assert collector.skip_reason == "invalid rest.max_concurrency: 0"


def test_networker_server_protection_settings_load_and_validate(tmp_path: Path) -> None:
    classification = tmp_path / "hostname_domain.csv"
    classification.write_text("hostname,security_domain\n", encoding="utf-8")
    config_path = tmp_path / "collector.yaml"
    config_path.write_text(
        f"""
collectors:
  - name: networker_core
    type: Networker
    protocol: rest
    enabled: true
    base_url: https://networker.example.com:9090
    username: rest-user
    password: secret
    source_networker: core
    classification:
      hostname_csv_path: {classification}
    rest:
      max_concurrency: 1
      request_spacing_seconds: 30
      jobs_query_window_minutes: 10
      backups_query_window_minutes: 20
      field_filter: true
""",
        encoding="utf-8",
    )

    collector = load_config(config_path).collectors[0]

    assert collector.rest_max_concurrency == 1
    assert collector.rest_request_spacing_seconds == 30
    assert collector.networker_jobs_query_window_minutes == 10
    assert collector.networker_backups_query_window_minutes == 20
    assert collector.networker_field_filter
    assert collector.skip_reason is None

    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "jobs_query_window_minutes: 10",
            "jobs_query_window_minutes: 0",
        ),
        encoding="utf-8",
    )

    collector = load_config(config_path).collectors[0]
    assert collector.skip_reason == "invalid rest.jobs_query_window_minutes: 0"


def test_networker_three_collection_schedules_load(tmp_path: Path) -> None:
    classification = tmp_path / "hostname_domain.csv"
    classification.write_text("hostname,security_domain\n", encoding="utf-8")
    config_path = tmp_path / "collector.yaml"
    config_path.write_text(
        f"""
collectors:
  - name: networker_core
    type: Networker
    protocol: rest
    enabled: true
    schedule:
      jobs: {{interval_minutes: 5, minute_offset: 3, second: 0}}
      backups: {{interval_minutes: 15, minute_offset: 6, second: 0}}
      inventory: {{interval_minutes: 1440, minute_offset: 195, second: 0}}
    base_url: https://networker.example.com:9090
    username: rest-user
    password: secret
    source_networker: core
    classification:
      hostname_csv_path: {classification}
""",
        encoding="utf-8",
    )

    collector = load_config(config_path).collectors[0]

    assert tuple(
        (collection_class, schedule.interval_minutes, schedule.minute_offset)
        for collection_class, schedule in collector.effective_schedules
    ) == (
        ("jobs", 5, 3),
        ("backups", 15, 6),
        ("inventory", 1440, 195),
    )
    assert collector.skip_reason is None


def test_networker_requires_basic_auth_and_rejects_ifrs(tmp_path: Path) -> None:
    classification = tmp_path / "hostname_domain.csv"
    classification.write_text("hostname,security_domain\n", encoding="utf-8")

    missing_basic_auth = load_config(
        _write_networker_config(tmp_path, classification, source="core", credentials="")
    ).collectors[0]
    assert missing_basic_auth.skip_reason == "REST config contains TO_BE_FILLED values"

    ifrs = load_config(
        _write_networker_config(
            tmp_path,
            classification,
            source="ifrs",
            credentials="    username: rest-user\n    password: secret\n",
        )
    ).collectors[0]
    assert ifrs.skip_reason == "unsupported source_networker: ifrs"


def _write_networker_config(
    tmp_path: Path,
    classification: Path,
    *,
    source: str,
    credentials: str,
) -> Path:
    config_path = tmp_path / f"collector-{source}.yaml"
    config_path.write_text(
        f"""
collectors:
  - name: networker_{source}
    type: Networker
    protocol: rest
    enabled: true
    base_url: https://networker.example.com:9090
{credentials}    source_networker: {source}
    classification:
      hostname_csv_path: {classification}
""",
        encoding="utf-8",
    )
    return config_path


def test_config_loads_secret_values_from_files(tmp_path: Path) -> None:
    es_username = tmp_path / "es-username"
    es_password = tmp_path / "es-password"
    dxi_username = tmp_path / "dxi-username"
    dxi_password = tmp_path / "dxi-password"
    es_username.write_text("elastic\n", encoding="utf-8")
    es_password.write_text("elastic-password\n", encoding="utf-8")
    dxi_username.write_text("dxi-user\n", encoding="utf-8")
    dxi_password.write_text("dxi-password\n", encoding="utf-8")

    config_path = tmp_path / "collector.yaml"
    config_path.write_text(
        f"""
app:
  name: test
elasticsearch:
  enabled: true
  username_file: {es_username}
  password_file: {es_password}
  ca_certs: /apps/secrets/elasticsearch/ca.crt
collectors:
  - name: DXi_1
    type: DXi
    protocol: cli
    enabled: true
    host: 192.0.2.10
    username_file: {dxi_username}
    password_file: {dxi_password}
    commands:
      status: show status
""",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.elasticsearch.username == "elastic"
    assert config.elasticsearch.password == "elastic-password"
    assert config.elasticsearch.ca_certs == "/apps/secrets/elasticsearch/ca.crt"
    assert config.collectors[0].username == "dxi-user"
    assert config.collectors[0].password == "dxi-password"
    assert config.collectors[0].skip_reason is None

from pathlib import Path

from app.core.config import default_minute_offset, default_second, has_unfilled_values, load_config


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

    assert len(config.collectors) == 17
    assert all(collector.skip_reason for collector in config.collectors)
    assert not any(collector.type == "i6000" and collector.protocol == "snmp" for collector in config.collectors)


def test_group_example_configs_load_expected_collectors() -> None:
    expected = {
        "dxi": (2, {"DXi"}),
        "dd": (3, {"DD"}),
        "i6000": (4, {"i6000"}),
        "networker": (4, {"Networker"}),
        "zfs": (4, {"ZFS"}),
    }

    for group, (count, collector_types) in expected.items():
        config = load_config(Path(f"config/collector.{group}.example.yaml"))

        assert len(config.collectors) == count
        assert {collector.type for collector in config.collectors} == collector_types
        assert all(collector.effective_schedule.interval_minutes == 5 for collector in config.collectors)
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
""",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.collectors[0].schedule_second == 30
    assert config.collectors[0].effective_schedule.interval_minutes == 1
    assert config.collectors[0].effective_schedule.second == 30


def test_config_loads_secret_values_from_files(tmp_path: Path) -> None:
    es_username = tmp_path / "es-username"
    es_password = tmp_path / "es-password"
    dxi_community = tmp_path / "dxi-community"
    dxi_username = tmp_path / "dxi-username"
    dxi_password = tmp_path / "dxi-password"
    es_username.write_text("elastic\n", encoding="utf-8")
    es_password.write_text("elastic-password\n", encoding="utf-8")
    dxi_community.write_text("public\n", encoding="utf-8")
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
  ca_certs: /app/secrets/elasticsearch/ca.crt
collectors:
  - name: DXi_1
    type: DXi
    protocol: cli_snmp
    enabled: true
    host: 192.0.2.10
    community_file: {dxi_community}
    username_file: {dxi_username}
    password_file: {dxi_password}
    oids:
      state: 1.3.6.1.4.1.2036.2.1.1.7.0
    commands:
      status: show status
""",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.elasticsearch.username == "elastic"
    assert config.elasticsearch.password == "elastic-password"
    assert config.elasticsearch.ca_certs == "/app/secrets/elasticsearch/ca.crt"
    assert config.collectors[0].community == "public"
    assert config.collectors[0].username == "dxi-user"
    assert config.collectors[0].password == "dxi-password"
    assert config.collectors[0].skip_reason is None

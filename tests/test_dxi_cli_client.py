from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from app.clients.dxi_cli_client import DxiCliClient
from app.core.config import CollectorConfig, load_config


def _config(**overrides: object) -> CollectorConfig:
    values: dict[str, object] = {
        "name": "DXi_1",
        "type": "DXi",
        "protocol": "cli_snmp",
        "enabled": True,
        "host": "vtl.example",
        "ssh_port": 22,
        "username": "root",
        "ssh_key_path": "/app/secrets/ssh/id_ed25519",
        "ssh_known_hosts_path": "/app/secrets/ssh/known_hosts",
        "jump_host": "relay.example",
        "jump_port": 22,
        "jump_username": "123456789",
        "jump_ssh_key_path": "/app/secrets/ssh/id_ed25519",
        "commands": {"status": "show status"},
    }
    values.update(overrides)
    return CollectorConfig(**values)


def test_jump_config_is_ready_with_key_and_known_hosts() -> None:
    assert _config()._ssh_skip_reason() is None


def test_jump_config_requires_username_and_known_hosts() -> None:
    assert (
        _config(jump_username=None)._ssh_skip_reason()
        == "SSH jump config contains TO_BE_FILLED values"
    )
    assert (
        _config(ssh_known_hosts_path=None)._ssh_skip_reason()
        == "SSH jump config contains TO_BE_FILLED values"
    )


def test_jump_config_loads_from_yaml(tmp_path: Path) -> None:
    config_path = tmp_path / "collector.yaml"
    config_path.write_text(
        """
collectors:
  - name: DXi_1
    type: DXi
    protocol: cli_snmp
    host: vtl.example
    username: root
    ssh_key_path: /app/secrets/ssh/id_ed25519
    ssh_known_hosts_path: /app/secrets/ssh/known_hosts
    jump_host: relay.example
    jump_port: 2222
    jump_username: "123456789"
    jump_ssh_key_path: /app/secrets/ssh/id_ed25519
    commands:
      status: show status
""",
        encoding="utf-8",
    )

    collector = load_config(config_path).collectors[0]

    assert collector.jump_host == "relay.example"
    assert collector.jump_port == 2222
    assert collector.jump_username == "123456789"
    assert collector.jump_ssh_key_path == "/app/secrets/ssh/id_ed25519"
    assert collector.ssh_known_hosts_path == "/app/secrets/ssh/known_hosts"


def test_run_commands_uses_direct_tcpip_channel(monkeypatch) -> None:
    jump_client = MagicMock()
    vtl_client = MagicMock()
    transport = MagicMock()
    channel = MagicMock()
    stdout = MagicMock()
    stderr = MagicMock()

    transport.is_active.return_value = True
    transport.open_channel.return_value = channel
    jump_client.get_transport.return_value = transport
    stdout.channel.recv_exit_status.return_value = 0
    stdout.read.return_value = b"State: online\n"
    stderr.read.return_value = b""
    vtl_client.exec_command.return_value = (MagicMock(), stdout, stderr)

    ssh_client_factory = MagicMock(side_effect=[vtl_client, jump_client])
    fake_paramiko = SimpleNamespace(
        SSHClient=ssh_client_factory,
        RejectPolicy=MagicMock,
        AutoAddPolicy=MagicMock,
    )
    monkeypatch.setitem(sys.modules, "paramiko", fake_paramiko)

    result = DxiCliClient(_config()).run_commands()

    assert result == {"status": "State: online\n"}
    transport.open_channel.assert_called_once_with(
        kind="direct-tcpip",
        dest_addr=("vtl.example", 22),
        src_addr=("127.0.0.1", 0),
    )
    jump_client.connect.assert_called_once_with(
        hostname="relay.example",
        port=22,
        username="123456789",
        key_filename="/app/secrets/ssh/id_ed25519",
        timeout=30,
        banner_timeout=30,
        auth_timeout=30,
        look_for_keys=False,
        allow_agent=False,
    )
    assert vtl_client.connect.call_args.kwargs["sock"] is channel
    assert vtl_client.connect.call_args.kwargs["username"] == "root"
    channel.close.assert_called_once()
    jump_client.close.assert_called_once()
    vtl_client.close.assert_called_once()

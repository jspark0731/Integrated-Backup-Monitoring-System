from __future__ import annotations

from typing import Any

from app.core.config import CollectorConfig


class DxiCliClient:
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config

    def run_commands(self) -> dict[str, str]:
        import paramiko

        client = self._new_client(paramiko)
        jump_client = None
        channel = None

        try:
            if self.config.jump_host:
                jump_client = self._new_client(paramiko)
                jump_client.connect(**self._jump_connect_kwargs())
                transport = jump_client.get_transport()
                if transport is None or not transport.is_active():
                    raise RuntimeError("SSH jump transport is not active")
                channel = transport.open_channel(
                    kind="direct-tcpip",
                    dest_addr=(
                        self.config.host,
                        self.config.ssh_port or self.config.port or 22,
                    ),
                    src_addr=("127.0.0.1", 0),
                )

            client.connect(**self._connect_kwargs(sock=channel))
            return {
                name: self._run_command(client, name, command)
                for name, command in self.config.commands.items()
            }
        finally:
            client.close()
            if channel is not None:
                channel.close()
            if jump_client is not None:
                jump_client.close()

    def _new_client(self, paramiko: Any) -> Any:
        client = paramiko.SSHClient()
        if self.config.ssh_known_hosts_path:
            client.load_host_keys(self.config.ssh_known_hosts_path)
            client.set_missing_host_key_policy(paramiko.RejectPolicy())
        else:
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        return client

    def _connect_kwargs(self, sock: Any | None = None) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "hostname": self.config.host,
            "port": self.config.ssh_port or self.config.port or 22,
            "username": self.config.username,
            "timeout": self.config.command_timeout,
            "banner_timeout": self.config.command_timeout,
            "auth_timeout": self.config.command_timeout,
            "look_for_keys": False,
            "allow_agent": False,
        }
        if sock is not None:
            kwargs["sock"] = sock
        if self.config.ssh_key_path:
            kwargs["key_filename"] = self.config.ssh_key_path
        else:
            kwargs["password"] = self.config.password
        return kwargs

    def _jump_connect_kwargs(self) -> dict[str, Any]:
        return {
            "hostname": self.config.jump_host,
            "port": self.config.jump_port,
            "username": self.config.jump_username,
            "key_filename": self.config.jump_ssh_key_path or self.config.ssh_key_path,
            "timeout": self.config.command_timeout,
            "banner_timeout": self.config.command_timeout,
            "auth_timeout": self.config.command_timeout,
            "look_for_keys": False,
            "allow_agent": False,
        }

    def _run_command(self, client: Any, name: str, command: str) -> str:
        _, stdout, stderr = client.exec_command(command, timeout=self.config.command_timeout)
        exit_status = stdout.channel.recv_exit_status()
        output = stdout.read().decode("utf-8", errors="replace")
        error = stderr.read().decode("utf-8", errors="replace")
        if exit_status != 0:
            raise RuntimeError(f"DXi CLI command failed: {name} ({exit_status}) {error.strip()}")
        return output

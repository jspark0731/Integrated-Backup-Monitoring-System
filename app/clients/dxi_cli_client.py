from __future__ import annotations

from typing import Any

from app.core.config import CollectorConfig


class DxiCliClient:
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config

    def run_commands(self) -> dict[str, str]:
        import paramiko

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            client.connect(**self._connect_kwargs())
            return {
                name: self._run_command(client, name, command)
                for name, command in self.config.commands.items()
            }
        finally:
            client.close()

    def _connect_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "hostname": self.config.host,
            "port": self.config.ssh_port or self.config.port or 22,
            "username": self.config.username,
            "timeout": self.config.command_timeout,
            "banner_timeout": self.config.command_timeout,
            "auth_timeout": self.config.command_timeout,
            "look_for_keys": False,
        }
        if self.config.ssh_key_path:
            kwargs["key_filename"] = self.config.ssh_key_path
        else:
            kwargs["password"] = self.config.password
        return kwargs

    def _run_command(self, client: Any, name: str, command: str) -> str:
        _, stdout, stderr = client.exec_command(command, timeout=self.config.command_timeout)
        exit_status = stdout.channel.recv_exit_status()
        output = stdout.read().decode("utf-8", errors="replace")
        error = stderr.read().decode("utf-8", errors="replace")
        if exit_status != 0:
            raise RuntimeError(f"DXi CLI command failed: {name} ({exit_status}) {error.strip()}")
        return output

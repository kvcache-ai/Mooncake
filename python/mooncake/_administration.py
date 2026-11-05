"""Optional dependencies for the SSD administration tools."""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def require_paramiko():
    """Load SSH support only when an administration operation needs it."""
    try:
        import paramiko
    except ModuleNotFoundError as exc:
        if exc.name != "paramiko":
            raise
        raise RuntimeError(
            "SSH administration requires paramiko. Install it with: "
            "python -m pip install 'mooncake-transfer-engine[administration]'"
        ) from None
    return paramiko


class SSHConnectionPool:
    """Reuses a single paramiko.SSHClient per (ip, port, creds) tuple so that
    repeated commands against the same SPDK target do not rebuild TCP and
    SSH handshakes every time. Pooled clients are closed when close_all() is
    called, or automatically on garbage collection.

    The two SSD admin tools (mooncake_ssd_register / mooncake_ssd_unregister)
    both perform multi-step discovery (probe RPC path, list subsystems, fetch
    bdev info), so the per-target handshake cost was multiplied by the number
    of steps. This pool removes that multiplier at the cost of one extra kept-
    alive TCP socket per unique target.
    """

    _instance: Optional["SSHConnectionPool"] = None
    _instance_lock = threading.Lock()

    @classmethod
    def instance(cls) -> "SSHConnectionPool":
        # One process-wide pool is enough for both admin CLI tools.
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_for_test(cls) -> None:
        """Forcefully teardown the singleton (tests only)."""
        if cls._instance is not None:
            cls._instance.close_all()
        cls._instance = None

    def __init__(self) -> None:
        self._clients: Dict[tuple, Any] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _make_key(ip: str, port: int, username: str,
                  password: Optional[str], key_file: Optional[str]) -> tuple:
        return (
            ip, int(port or 22), username or "root",
            password, key_file,
        )

    def _get_client(self, key: tuple) -> Any:
        paramiko = require_paramiko()
        client = self._clients.get(key)
        if client is not None:
            # Paramiko does not expose a clean "is this client still usable"
            # predicate. We try the cheap no-op transport check first; any
            # socket-level failure during a real exec_command is retried once
            # by the caller (see exec_command).
            try:
                if getattr(client, "get_transport", None) is not None and \
                        client.get_transport() is not None and \
                        client.get_transport().is_active():
                    return client
            except Exception:
                pass
            self._close_client(client)
            self._clients.pop(key, None)

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ip, port, username, password, key_file = key
        try:
            client.connect(
                ip,
                port=port,
                username=username,
                password=password,
                key_filename=key_file,
                timeout=10,
            )
        except Exception:
            client.close()
            raise
        self._clients[key] = client
        return client

    def _close_client(self, client: Any) -> None:
        try:
            client.close()
        except Exception:
            # Best effort. Paramiko raises EOFError on double-close in some
            # versions; we silently swallow.
            pass

    def close_all(self) -> None:
        with self._lock:
            for client in list(self._clients.values()):
                self._close_client(client)
            self._clients.clear()

    def exec_command(self, ip: str, command: str, *,
                     port: int = 22,
                     username: str = "root",
                     password: Optional[str] = None,
                     key_file: Optional[str] = None,
                     command_timeout: int = 30) -> tuple:
        """Run a command on *ip* and return (exit_status, stdout, stderr).

        The SSH connection is reused across calls with the same credentials.
        If the pooled client turns out to be broken (transport dropped), we
        rebuild it once and retry.
        """
        key = self._make_key(ip, port, username, password, key_file)
        last_exc: Optional[BaseException] = None
        for attempt in (0, 1):
            with self._lock:
                client = self._get_client(key)
            try:
                stdin, stdout, stderr = client.exec_command(
                    command, timeout=command_timeout)
                exit_status = stdout.channel.recv_exit_status()
                out = stdout.read().decode("utf-8")
                err = stderr.read().decode("utf-8")
                return exit_status, out, err
            except Exception as exc:
                last_exc = exc
                # Force a reconnect on the next attempt.
                with self._lock:
                    stale = self._clients.pop(key, None)
                    if stale is not None:
                        self._close_client(stale)
                if attempt == 0:
                    logger.warning(
                        "SSH command failed on %s, reconnecting once: %s",
                        ip, exc,
                    )
                    continue
                break
        assert last_exc is not None
        raise last_exc  # type: ignore[misc]

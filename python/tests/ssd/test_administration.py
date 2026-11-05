import builtins
from unittest.mock import Mock, patch

import pytest

from mooncake._administration import (
    SSHConnectionPool,
    require_paramiko,
)
from mooncake.spdk_tgt_create import SPDKTgtCreator


def test_missing_paramiko_reports_install_command():
    real_import = builtins.__import__

    def without_paramiko(name, *args, **kwargs):
        if name == "paramiko":
            raise ModuleNotFoundError("No module named 'paramiko'", name=name)
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=without_paramiko):
        creator = SPDKTgtCreator(["ip:127.0.0.1 path:/home/spdk"])
        with pytest.raises(RuntimeError, match=r"pip install .*\[administration\]"):
            creator._ssh_connect("127.0.0.1")


def test_paramiko_is_loaded_on_demand():
    paramiko = Mock()
    with patch.dict("sys.modules", {"paramiko": paramiko}):
        assert require_paramiko() is paramiko


def test_broken_transitive_dependency_is_not_hidden():
    error = ModuleNotFoundError("No module named 'cryptography'", name="cryptography")
    with patch("builtins.__import__", side_effect=error):
        with pytest.raises(ModuleNotFoundError) as caught:
            require_paramiko()
    assert caught.value is error


class _FakeTransport:
    def __init__(self, active=True):
        self._active = active

    def is_active(self):
        return self._active


class _FakeChannel:
    def __init__(self, status=0, out=b"", err=b""):
        self._status = status
        self._out = out
        self._err = err

    def recv_exit_status(self):
        return self._status

    def recv(self, n):
        # Mooncake code only calls recv_exit_status; recv is never used.
        return b""


class _FakeStdInOut:
    def __init__(self, body=b""):
        self._body = body

    def read(self):
        return self._body


class _FakeClient:
    """Minimal paramiko.SSHClient stand-in that tracks connect/close counts.

    The *module-level* helper functions reset these counters explicitly;
    _FakeClient must not touch its own class vars in __init__, or every
    construction would wipe the previous observation.
    """

    connect_calls = 0
    close_calls = 0

    def __init__(self):
        self.transport = _FakeTransport()

    def set_missing_host_key_policy(self, _policy):
        pass

    def connect(self, *_args, **_kwargs):
        _FakeClient.connect_calls += 1
        self.transport = _FakeTransport()

    def close(self):
        _FakeClient.close_calls += 1

    def get_transport(self):
        return self.transport

    def exec_command(self, _cmd, timeout=None):
        return (
            _FakeStdInOut(),
            _FakeStdInOut(b"ok"),
            _FakeStdInOut(b""),
        )


def _fake_paramiko_module():
    m = Mock()
    m.SSHClient = _FakeClient
    m.AutoAddPolicy = Mock()
    return m


@pytest.fixture(autouse=True)
def _reset_pool_between_tests():
    SSHConnectionPool.reset_for_test()
    yield
    SSHConnectionPool.reset_for_test()


def test_pool_reuses_single_client_for_same_host():
    with patch.dict("sys.modules", {"paramiko": _fake_paramiko_module()}):
        pool = SSHConnectionPool.instance()
        pool.exec_command("10.0.0.1", "echo 1")
        pool.exec_command("10.0.0.1", "echo 2")
        pool.exec_command("10.0.0.1", "echo 3")
        assert _FakeClient.connect_calls == 1


def test_pool_creates_new_client_for_different_hosts():
    with patch.dict("sys.modules", {"paramiko": _fake_paramiko_module()}):
        pool = SSHConnectionPool.instance()
        pool.exec_command("10.0.0.1", "echo 1")
        pool.exec_command("10.0.0.2", "echo 1")
        pool.exec_command("10.0.0.1", "echo 1")
        assert _FakeClient.connect_calls == 2  # second 10.0.0.1 reuses


class _DyingClient(_FakeClient):
    exec_calls = 0

    def exec_command(self, _cmd, timeout=None):
        _DyingClient.exec_calls += 1
        if _DyingClient.exec_calls == 1:
            raise EOFError("connection reset by peer")
        return super().exec_command(_cmd, timeout)


def test_pool_reconnects_once_after_transport_drops():
    """When a pooled client raises on exec_command, the pool must rebuild
    it once and retry, so transient SSH drops do not fail the caller."""
    _DyingClient.exec_calls = 0
    SSHConnectionPool.reset_for_test()
    with patch.dict("sys.modules", {"paramiko": _fake_paramiko_module()}):
        # Replace SSHClient with the dying variant AFTER the pool's lazily
        # required module is resolved.
        import mooncake._administration as mod  # noqa: F401
        import sys as _sys
        _sys.modules["paramiko"].SSHClient = _DyingClient

        pool = SSHConnectionPool.instance()
        exit_status, out, err = pool.exec_command("10.0.0.1", "echo hi")
        assert exit_status == 0
        # One original + one reconnect.
        assert _FakeClient.connect_calls == 2
        assert out == "ok"

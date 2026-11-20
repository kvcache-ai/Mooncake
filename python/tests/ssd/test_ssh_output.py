"""Exercise SSH output flow control without an SSH server or SPDK hardware."""

import importlib
import io
import sys
import threading
import types
from unittest.mock import Mock

import pytest

from mooncake._administration import read_ssh_output
from mooncake.spdk_tgt_create import SPDKTgtCreator


class WindowedCommand:
    """A remote process cannot exit until both output streams fit in its window."""

    def __init__(self, output, error, status=0):
        self.condition = threading.Condition()
        self.buffers = [bytearray(), bytearray()]
        self.done = False
        self.closed = False
        self.status = status
        self.streams = [self.Stream(self, index) for index in range(2)]
        self.producer = threading.Thread(
            target=self.produce, args=(output, error), daemon=True
        )
        self.producer.start()

    def produce(self, output, error):
        with self.condition:
            for index, data in enumerate((output, error)):
                for offset in range(0, len(data), 1024):
                    self.condition.wait_for(
                        lambda: self.closed or sum(map(len, self.buffers)) < 1024
                    )
                    if self.closed:
                        return
                    self.buffers[index].extend(data[offset : offset + 1024])
                    self.condition.notify_all()
            self.done = True
            self.condition.notify_all()

    def recv_exit_status(self):
        with self.condition:
            if not self.condition.wait_for(lambda: self.done or self.closed, timeout=2):
                raise TimeoutError("remote process blocked by a full SSH output window")
        return self.status

    def close(self):
        with self.condition:
            self.closed = True
            self.condition.notify_all()

    class Stream:
        def __init__(self, channel, index):
            self.channel = channel
            self.index = index

        def read(self):
            result = bytearray()
            with self.channel.condition:
                while True:
                    ready = self.channel.condition.wait_for(
                        lambda: self.channel.buffers[self.index]
                        or self.channel.done
                        or self.channel.closed,
                        timeout=2,
                    )
                    if not ready:
                        raise TimeoutError(
                            "the other SSH output stream was not drained"
                        )
                    result.extend(self.channel.buffers[self.index])
                    self.channel.buffers[self.index].clear()
                    self.channel.condition.notify_all()
                    if self.channel.done or self.channel.closed:
                        return bytes(result)


def invoke_tool(tool, channel, monkeypatch):
    ssh = Mock()
    if tool == "create":
        ssh.exec_command.return_value = (io.BytesIO(), *channel.streams)
        creator = SPDKTgtCreator(["ip:127.0.0.1 path:/home/spdk"])
        return creator._execute_command(ssh, "scripts/rpc.py nvmf_get_subsystems")

    native = types.ModuleType("mooncake.store")
    native.MooncakeDistributedNoFRegister = Mock()
    monkeypatch.setitem(sys.modules, "mooncake.store", native)
    module = importlib.import_module(f"mooncake.mooncake_ssd_{tool}")
    class_name = (
        "MooncakeNoFRegister" if tool == "register" else "MooncakeNoFUnregister"
    )
    instance = getattr(module, class_name).__new__(getattr(module, class_name))
    instance.cli_config = {}
    paramiko = Mock()
    paramiko.SSHClient.return_value = ssh
    monkeypatch.setattr(module, "require_paramiko", lambda: paramiko)
    exists_stdout = io.BytesIO()
    exists_stdout.channel = Mock()
    exists_stdout.channel.recv_exit_status.return_value = 0
    ssh.exec_command.side_effect = [
        (io.BytesIO(), exists_stdout, io.BytesIO()),
        (io.BytesIO(), *channel.streams),
    ]
    try:
        return instance._execute_ssh_command(
            "127.0.0.1", "scripts/rpc.py nvmf_get_subsystems", "/home/spdk"
        )
    finally:
        ssh.close.assert_called_once_with()


@pytest.mark.parametrize("tool", ["create", "register", "unregister"])
@pytest.mark.parametrize(
    ("output", "error"),
    [(b"x" * 65536, b""), (b"", b"e" * 65536), (b"x" * 32768, b"e" * 32768)],
    ids=["stdout", "stderr", "both-streams"],
)
def test_commands_drain_output_larger_than_the_ssh_window(
    tool, output, error, monkeypatch
):
    channel = WindowedCommand(output, error)
    try:
        result = invoke_tool(tool, channel, monkeypatch)
        expected = (
            (output.decode(), error.decode()) if tool == "create" else output.decode()
        )
        assert result == expected
        assert channel.done
    finally:
        channel.close()
        channel.producer.join(timeout=2)
        assert not channel.producer.is_alive()


@pytest.mark.parametrize("tool", ["create", "register", "unregister"])
def test_commands_preserve_failure_diagnostics(tool, monkeypatch):
    channel = WindowedCommand(b"command output", b"RPC failed", status=7)
    try:
        with pytest.raises(RuntimeError, match="RPC failed"):
            invoke_tool(tool, channel, monkeypatch)
    finally:
        channel.close()
        channel.producer.join(timeout=2)


@pytest.mark.parametrize("failed_stream", [0, 1], ids=["stdout", "stderr"])
def test_read_failure_closes_channel_and_unblocks_the_other_stream(failed_stream):
    channel = WindowedCommand(b"x" * 65536, b"e" * 65536)
    failure = OSError("SSH connection lost")
    channel.streams[failed_stream].read = Mock(side_effect=failure)
    try:
        with pytest.raises(OSError) as caught:
            read_ssh_output(*channel.streams)
        assert caught.value is failure
        assert channel.closed
    finally:
        channel.close()
        channel.producer.join(timeout=2)
        assert not channel.producer.is_alive()

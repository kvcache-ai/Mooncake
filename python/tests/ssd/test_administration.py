import builtins
from unittest.mock import Mock, patch

import pytest

from mooncake._administration import require_paramiko
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

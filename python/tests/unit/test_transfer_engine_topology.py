import json

import pytest


def test_get_local_topology_accepts_default_device():
    engine = pytest.importorskip("mooncake.engine").TransferEngine()
    default = json.loads(engine.get_local_topology())
    assert json.loads(engine.get_local_topology(device_name=None)) == default
    assert json.loads(engine.get_local_topology("")) == default

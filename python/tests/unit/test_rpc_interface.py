import gc
import weakref

import pytest


@pytest.mark.parametrize(
    "register", ["set_data_receive_callback", "set_tensor_receive_callback"]
)
def test_rpc_receive_callback_cycle_is_collected(register):
    engine = pytest.importorskip("mooncake.engine")

    def install_callback():
        rpc = engine.RpcInterface()

        def callback(message):
            return rpc

        getattr(rpc, register)(callback)
        return weakref.ref(callback)

    callback_ref = install_callback()
    gc.collect()

    assert callback_ref() is None

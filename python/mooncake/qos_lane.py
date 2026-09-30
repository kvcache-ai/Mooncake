# SPDX-License-Identifier: Apache-2.0
"""Per-QoS Store clients sharing one master/key namespace (legacy Ascend TE).

Each client owns a TE with immutable GlobalResourceConfig. Only the default
client contributes pool capacity; all clients register the SAME caller buffers.
No request-time environment changes and no QoS suffix is added to cache keys.
"""

import json
import os
from contextlib import ExitStack
from threading import RLock


class QosStorePool:
    def __init__(
        self,
        *,
        qos_values,
        default_qos,
        setup_kwargs,
        resource_config=None,
        engine_factory=None,
        store_factory=None,
    ):
        values = list(qos_values)
        if not values or any(type(q) is not int or not 0 <= q <= 7 for q in values):
            raise ValueError("QoS values must be integers in 0..7")
        values = sorted(set(values))
        if type(default_qos) is not int or default_qos not in values:
            raise ValueError("default_qos must be in qos_values")
        if setup_kwargs.get("protocol") != "ascend":
            raise ValueError("QoS lanes require protocol=ascend")
        if (
            setup_kwargs.get("enable_ssd_offload")
            or os.getenv("ASCEND_ENABLE_USE_FABRIC_MEM", "0") == "1"
        ):
            raise ValueError(
                "QoS lanes currently require direct memory transfer without SSD/fabric memory"
            )
        if engine_factory is None:
            from mooncake.engine import TransferEngine

            engine_factory = TransferEngine
        if store_factory is None:
            from mooncake.store import MooncakeDistributedStore

            store_factory = MooncakeDistributedStore
        base = (
            json.loads(os.getenv("ASCEND_GLOBAL_RESOURCE_CONFIG", "{}"))
            if resource_config is None
            else dict(resource_config)
        )
        if (
            not isinstance(base, dict)
            or "store" in base
            or "comm_resource_config" in base
        ):
            raise ValueError(
                "resource_config must use literal flat comm_resource_config.* keys"
            )
        self.default_qos = default_qos
        self._lanes = {}
        self._buffers = {}
        self._closed = False
        self._broken = False
        self._rollback_pending = []
        try:
            for q in values:
                engine = engine_factory()
                config = dict(base, **{"comm_resource_config.qos": q})
                init = getattr(engine, "initialize_with_ascend_resource_config", None)
                if init is None:
                    raise RuntimeError(
                        "Mooncake native per-engine QoS patch is missing"
                    )
                host = setup_kwargs["local_hostname"]
                rc = init(
                    host,
                    "P2PHANDSHAKE",
                    setup_kwargs.get("rdma_devices", ""),
                    json.dumps(config, separators=(",", ":")),
                )
                if rc != 0:
                    raise RuntimeError(f"QoS {q}: TransferEngine init failed: {rc}")
                store = store_factory()
                segment = f"{host}:{engine.get_rpc_port()}"
                self._lanes[q] = (engine, store, segment, RLock())
                kw = dict(
                    setup_kwargs, local_hostname=segment, engine=engine.get_engine()
                )
                if q != default_qos:
                    kw.update(global_segment_size=0, local_buffer_size=0)
                rc = store.setup(**kw)
                if rc != 0:
                    raise RuntimeError(f"QoS {q}: Store setup failed: {rc}")
        except BaseException as cause:
            try:
                self.close()
            except Exception as cleanup:  # noqa: BLE001 - report both setup and cleanup failures
                raise RuntimeError(
                    f"Store setup failed; cleanup also failed: {cleanup}"
                ) from cause
            raise

    @property
    def default_store(self):
        return self._lanes[self.default_qos][1]

    @property
    def default_segment(self):
        return self._lanes[self.default_qos][2]

    def _require_open(self):
        if self._closed:
            raise RuntimeError("QoS pool is closed")
        if self._broken:
            raise RuntimeError("QoS pool is broken after registration rollback")

    def register_buffers(self, ptrs, lengths):
        if len(ptrs) != len(lengths):
            raise ValueError("buffer pointers and lengths have different counts")
        with ExitStack() as locks:
            for lane in self._lanes.values():
                locks.enter_context(lane[3])
            self._require_open()
            added = []
            try:
                for ptr, size in zip(ptrs, lengths):
                    if (
                        type(ptr) is not int
                        or type(size) is not int
                        or ptr <= 0
                        or size <= 0
                    ):
                        raise ValueError(
                            "buffer pointer and size must be positive integers"
                        )
                    if ptr in self._buffers and self._buffers[ptr] != size:
                        raise ValueError("a registered pointer cannot change size")
                    if ptr in self._buffers:
                        continue
                    if ptr + size > (1 << 64) or any(
                        ptr < base + n and base < ptr + size
                        for base, n in self._buffers.items()
                    ):
                        raise ValueError(
                            "overlapping or overflowing registration range"
                        )
                    done = []
                    added.append((ptr, done))
                    for engine, _, _, _ in self._lanes.values():
                        rc = engine.register_memory(ptr, size)
                        if rc != 0:
                            raise RuntimeError(f"memory registration failed: {rc}")
                        done.append(engine)
                    self._buffers[ptr] = size
            except BaseException as cause:
                for ptr, engines in reversed(added):
                    for engine in reversed(engines):
                        try:
                            failed = engine.unregister_memory(ptr) != 0
                        except Exception:  # noqa: BLE001 - preserve cleanup failures until all lanes are handled
                            failed = True
                        if failed:
                            self._rollback_pending.append((engine, ptr))
                    self._buffers.pop(ptr, None)
                if self._rollback_pending:
                    self._broken = True
                    raise RuntimeError(
                        "registration rollback failed; pool is broken"
                    ) from cause
                raise

    def transfer(self, qos, operation, keys, addrs, sizes, replicate_config=None):
        if type(qos) is not int or qos not in self._lanes:
            raise ValueError(f"QoS {qos} has no lane")
        if operation not in ("get", "put"):
            raise ValueError("operation must be get or put")
        if len(keys) != len(addrs) or len(keys) != len(sizes):
            raise ValueError("key/buffer batch shape mismatch")
        for aa, ss in zip(addrs, sizes):
            if (
                len(aa) != len(ss)
                or not aa
                or any(type(n) is not int or n <= 0 for n in ss)
            ):
                raise ValueError("invalid multi-buffer shape")
        _, store, _, lock = self._lanes[qos]
        with lock:
            self._require_open()
            for aa, ss in zip(addrs, sizes):
                for addr, size in zip(aa, ss):
                    if not any(
                        base <= addr and addr + size <= base + length
                        for base, length in self._buffers.items()
                    ):
                        raise ValueError(
                            "transfer range is not registered in every lane"
                        )
            if not keys:
                return []
            if operation == "get":
                result = list(store.batch_get_into_multi_buffers(keys, addrs, sizes))
            else:
                result = list(
                    store.batch_put_from_multi_buffers(
                        keys, addrs, sizes, replicate_config
                    )
                )
            if len(result) != len(keys) or any(r < 0 for r in result):
                raise RuntimeError(f"QoS {qos} {operation} failed: {result}")
            expected = (
                [sum(parts) for parts in sizes]
                if operation == "get"
                else [0] * len(keys)
            )
            if result != expected:
                raise RuntimeError(
                    f"QoS {qos} {operation}: unexpected byte count/status {result}, expected {expected}"
                )
            return [0] * len(result)

    def close(self):
        with ExitStack() as locks:
            for lane in self._lanes.values():
                locks.enter_context(lane[3])
            if self._closed:
                return
            self._closed = True
            errors = []
            # Stop Store clients before unregistering caller-owned buffers.
            for engine, store, _, _ in reversed(list(self._lanes.values())):
                try:
                    rc = store.close()
                    if rc != 0:
                        errors.append(f"store close: {rc}")
                except Exception as e:  # noqa: BLE001 - aggregate native cleanup failures
                    errors.append(str(e))
                for ptr in self._buffers:
                    try:
                        rc = engine.unregister_memory(ptr)
                        if rc != 0:
                            errors.append(f"unregister: {rc}")
                    except Exception as e:  # noqa: BLE001 - aggregate native cleanup failures
                        errors.append(str(e))
            pending = []
            for engine, ptr in self._rollback_pending:
                try:
                    failed = engine.unregister_memory(ptr) != 0
                except Exception:  # noqa: BLE001 - preserve cleanup failures until all lanes are handled
                    failed = True
                if failed:
                    errors.append("rollback unregister failed")
                    pending.append((engine, ptr))
            self._rollback_pending = pending
            self._buffers.clear()
            self._lanes.clear()
            if errors:
                raise RuntimeError("; ".join(errors))

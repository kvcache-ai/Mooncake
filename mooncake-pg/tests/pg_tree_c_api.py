"""Direct C API coverage for Tree out-of-place buffers and shared workspace."""

import ctypes as ct
import math
from pathlib import Path

import torch
from mooncake import pg

from pg_test_utils import require_test_device


class CommConfig(ct.Structure):
    _fields_ = [
        ("structSize", ct.c_size_t),
        ("magic", ct.c_uint),
        ("version", ct.c_uint),
        ("groupId", ct.c_char_p),
        ("rank", ct.c_int),
        ("size", ct.c_int),
        ("maxGroupSize", ct.c_int),
        ("globalRanks", ct.POINTER(ct.c_int32)),
        ("globalRankCount", ct.c_size_t),
        ("deviceType", ct.c_int),
        ("deviceIndex", ct.c_int),
        ("idResolvePolicy", ct.c_int),
        ("autoDeactivateOnFailure", ct.c_int),
        ("autoSyncOnFailure", ct.c_int),
        ("activeRanksMirror", ct.POINTER(ct.c_int32)),
        ("activeRanksMirrorCount", ct.c_size_t),
        ("activeRanksMirrorIsDevice", ct.c_int),
        ("activeRanksMirrorDeviceIndex", ct.c_int),
        ("preferredGpuCollectiveBackend", ct.c_int),
    ]


def c_api_worker(context, address, coordinator_ready, finished):
    device = require_test_device(context.rank, "cuda")
    library = ct.CDLL(str(Path(pg.__file__).with_name("libmooncake_pg.so")))
    signatures = {
        "mooncakePgContextCreate": [ct.POINTER(ct.c_void_p)],
        "mooncakePgContextInitialize": [ct.c_void_p, ct.c_int, ct.c_int],
        "mooncakePgContextSetHostIp": [ct.c_void_p, ct.c_char_p],
        "mooncakePgContextLaunchCoordinator": [ct.c_void_p, ct.c_void_p, ct.c_size_t],
        "mooncakePgContextConnectCoordinator": [ct.c_void_p, ct.c_char_p],
        "mooncakePgCommCreate": [
            ct.c_void_p,
            ct.POINTER(CommConfig),
            ct.POINTER(ct.c_void_p),
        ],
        "mooncakePgAllReduceGpu": [
            ct.c_void_p,
            ct.c_void_p,
            ct.c_size_t,
            ct.c_int,
            ct.c_int,
            ct.c_void_p,
            ct.c_void_p,
            ct.POINTER(ct.c_int32),
            ct.c_size_t,
        ],
        "mooncakePgCommDestroy": [ct.c_void_p],
        "mooncakePgContextDestroy": [ct.c_void_p],
    }
    library.mooncakePgGetLastError.restype = ct.c_char_p
    for name, args in signatures.items():
        function = getattr(library, name)
        function.argtypes = args
        function.restype = ct.c_int

    def call(name, *args):
        result = getattr(library, name)(*args)
        if result:
            message = library.mooncakePgGetLastError()
            raise RuntimeError(
                f"{name}: {result}: {message.decode() if message else ''}"
            )

    handle = ct.c_void_p()
    communicators = []
    call("mooncakePgContextCreate", ct.byref(handle))
    try:
        call("mooncakePgContextSetHostIp", handle, b"127.0.0.1")
        call("mooncakePgContextInitialize", handle, context.rank, context.world_size)
        if context.rank == 0:
            buffer = ct.create_string_buffer(512)
            call("mooncakePgContextLaunchCoordinator", handle, buffer, len(buffer))
            address["coordinator"] = buffer.value
            coordinator_ready.set()
        elif not coordinator_ready.wait(timeout=30):
            raise TimeoutError("C API coordinator did not start")
        call("mooncakePgContextConnectCoordinator", handle, address["coordinator"])
        ranks = (ct.c_int32 * context.world_size)(*range(context.world_size))
        for name in [b"tree-c-api-a", b"tree-c-api-b"]:
            config = CommConfig()
            config.structSize = ct.sizeof(config)
            config.magic = 0x20B6DE6B
            config.version = 1
            config.groupId = name
            config.rank = context.rank
            config.size = config.maxGroupSize = context.world_size
            config.globalRanks = ranks
            config.globalRankCount = context.world_size
            config.deviceType = 1
            config.deviceIndex = context.rank
            config.autoDeactivateOnFailure = config.autoSyncOnFailure = 1
            config.activeRanksMirrorDeviceIndex = -(2**31)
            config.preferredGpuCollectiveBackend = 1
            comm = ct.c_void_p()
            call("mooncakePgCommCreate", handle, ct.byref(config), ct.byref(comm))
            communicators.append(comm)

        stream = torch.cuda.Stream(device=device)
        hints = (ct.c_int32 * context.world_size)()
        checked = 0
        expected = context.world_size * (context.world_size + 1) // 2
        for index, count in enumerate([0, 3, 65539, 8388609, 1, 65539]):
            with torch.cuda.stream(stream):
                source = torch.full(
                    (count,), context.rank + 1, dtype=torch.float32, device=device
                )
                output = torch.full_like(source, -1)
                call(
                    "mooncakePgAllReduceGpu",
                    source.data_ptr(),
                    output.data_ptr(),
                    count,
                    9,
                    0,
                    communicators[index % 2],
                    stream.cuda_stream,
                    hints,
                    context.world_size,
                )
            stream.synchronize()
            if not bool(torch.all(output == expected).item()):
                raise AssertionError(f"C API output mismatch at {count}")
            if not bool(torch.all(source == context.rank + 1).item()):
                raise AssertionError("out-of-place Tree modified its input")
            checked += 1

        datatypes = [
            (torch.int8, 0),
            (torch.uint8, 1),
            (torch.int16, 2),
            (torch.int32, 4),
            (torch.int64, 6),
            (torch.float16, 8),
            (torch.float32, 9),
            (torch.float64, 10),
            (torch.bfloat16, 11),
            (torch.bool, 12),
        ]
        for dtype, datatype in datatypes:
            for operation in [0, 2, 3, 4]:
                values = [rank + 1 for rank in range(context.world_size)]
                if operation == 2:
                    values = [2] * context.world_size
                if dtype == torch.bool:
                    values = [bool(rank % 2) for rank in range(context.world_size)]
                result = (
                    sum(values)
                    if operation == 0
                    else math.prod(values)
                    if operation == 2
                    else min(values)
                    if operation == 3
                    else max(values)
                )
                if dtype == torch.bool:
                    result = bool(result)
                with torch.cuda.stream(stream):
                    source = torch.full(
                        (65539,), values[context.rank], dtype=dtype, device=device
                    )
                    output = torch.empty_like(source)
                    call(
                        "mooncakePgAllReduceGpu",
                        source.data_ptr(),
                        output.data_ptr(),
                        source.numel(),
                        datatype,
                        operation,
                        communicators[0],
                        stream.cuda_stream,
                        hints,
                        context.world_size,
                    )
                stream.synchronize()
                if not bool(torch.all(output == result).item()):
                    raise AssertionError(f"C API output mismatch: {dtype}/{operation}")
                if not bool(torch.all(source == values[context.rank]).item()):
                    raise AssertionError("out-of-place input changed")
                checked += 1

        # Two communicators borrow the same context workspace in one Graph.
        inputs = [
            torch.empty(65539, dtype=torch.float32, device=device)
            for _ in communicators
        ]
        outputs = [torch.empty_like(tensor) for tensor in inputs]
        graph = torch.cuda.CUDAGraph()
        try:
            with torch.cuda.graph(graph, stream=stream):
                for comm, source, output in zip(communicators, inputs, outputs):
                    source.fill_(context.rank + 1)
                    call(
                        "mooncakePgAllReduceGpu",
                        source.data_ptr(),
                        output.data_ptr(),
                        source.numel(),
                        9,
                        0,
                        comm,
                        stream.cuda_stream,
                        hints,
                        context.world_size,
                    )
            for _ in range(5):
                graph.replay()
                torch.cuda.synchronize(device)
                for source, output in zip(inputs, outputs):
                    if not bool(torch.all(output == expected).item()):
                        raise AssertionError("shared-workspace Graph result mismatch")
                    if not bool(torch.all(source == context.rank + 1).item()):
                        raise AssertionError("Graph modified out-of-place input")
        finally:
            graph.reset()
        finished.wait(timeout=45)
        context.record_result(
            {
                "out_of_place": True,
                "communicators": 2,
                "same_graph": True,
                "checked": checked,
            }
        )
    finally:
        for comm in reversed(communicators):
            call("mooncakePgCommDestroy", comm)
        call("mooncakePgContextDestroy", handle)

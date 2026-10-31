"""Table API contracts; CUDA tests require a USE_CUDA build on NVIDIA hardware."""

import gc

import numpy as np
import pytest

from mooncake.engram import EngramStore


def test_store_client_is_not_the_deepep_backend_selector():
    with pytest.raises(
        NotImplementedError, match="store_client is not an Engram transport"
    ):
        EngramStore(
            tables={"a": {"num_rows": 11, "row_bytes": 17}},
            namespace="test/v1",
            store_client=object(),
        )


def make_store(path, width=17, persistent=True):
    tables = {name: {"num_rows": 11, "row_bytes": width} for name in ("a", "b")}
    store = EngramStore(
        tables=tables, namespace="test/v1", local_dir=path, persistent=persistent
    )
    rows = np.arange(11 * width, dtype=np.uint8).reshape(11, width)
    store.populate("a", rows)
    store.populate("b", rows ^ 127)
    return store, rows, tables


def test_anonymous_tables_leave_no_files_and_do_not_share_names(tmp_path):
    store, rows, specs = make_store(tmp_path, persistent=False)
    other = EngramStore(
        tables=specs, namespace="test/v1", local_dir=tmp_path, persistent=False
    )
    other.populate("a", rows ^ 127)
    output = np.empty((1, 1, 17), np.uint8)
    store.lookup(["a"], np.array([[3]], np.int64), output)
    np.testing.assert_array_equal(output[0, 0], rows[3])
    other.lookup(["a"], np.array([[3]], np.int64), output)
    np.testing.assert_array_equal(output[0, 0], rows[3] ^ 127)
    assert list(tmp_path.iterdir()) == []
    with pytest.raises(FileExistsError):
        store.populate("a", rows)
    with pytest.raises(FileNotFoundError):
        other.lookup(["b"], np.array([[0]], np.int64), output)
    store.close()
    other.close()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("width", [1, 17, 264, 8192])
@pytest.mark.parametrize("head_major", [False, True])
def test_host_lookup_and_reopen(tmp_path, width, head_major):
    store, rows, specs = make_store(tmp_path, width)
    ids = np.array([[0, 12], [-1, 13], [10, -1]], dtype=np.int32)
    output = np.full((2, 3, width) if head_major else (3, 2, width), 221, np.uint8)
    if head_major:
        output = output.transpose(1, 0, 2)
    store.lookup(["a", "b"], ids, output, offsets=[0, 11])
    np.testing.assert_array_equal(output[[0, 2], 0], rows[[0, 10]])
    np.testing.assert_array_equal(output[:2, 1], rows[[1, 2]] ^ 127)
    assert np.all(output[1, 0] == 221) and np.all(output[2, 1] == 221)
    rows[:] = 0
    store.close()
    with EngramStore(tables=specs, namespace="test/v1", local_dir=tmp_path) as reopened:
        reopened.lookup(["a", "a"], np.array([[1, 1]], np.int64), output[:1])
        np.testing.assert_array_equal(
            output[0, 0], np.arange(11 * width, dtype=np.uint8).reshape(11, width)[1]
        )
        np.testing.assert_array_equal(output[0, 0], output[0, 1])


def test_validation_is_before_writes(tmp_path):
    store, _, _ = make_store(tmp_path)
    output = np.full((1, 2, 17), 201, np.uint8)
    for invalid in (-2, 11):
        with pytest.raises(IndexError):
            store.lookup(["a", "b"], np.array([[0, invalid]]), output)
        assert np.all(output == 201)
    with pytest.raises(ValueError):
        store.lookup(["a", "b"], np.zeros((1, 2), np.float32), output)
    with pytest.raises(ValueError):
        store.lookup(
            ["a", "b"],
            np.zeros((1, 2), np.int64),
            np.lib.stride_tricks.as_strided(output, (1, 2, 17), (34, 0, 1)),
        )
    with pytest.raises(FileExistsError):
        store.populate("a", np.zeros((11, 17), np.uint8))
    store.close()
    with pytest.raises(RuntimeError):
        store.lookup(["a"], np.zeros((1, 1), np.int64), output[:, :1])


@pytest.mark.parametrize("failure", ["missing", "layout", "size"])
def test_later_table_open_failure_preserves_output(tmp_path, failure):
    store, _, specs = make_store(tmp_path)
    store.close()
    if failure == "layout":
        specs = dict(specs, b={"num_rows": 12, "row_bytes": 17})
    with EngramStore(tables=specs, namespace="test/v1", local_dir=tmp_path) as reader:
        if failure == "missing":
            reader.remove("b")
        elif failure == "size":
            (reader._path("b") / "rows").write_bytes(b"")
        output = np.full((2, 2, 17), 201, np.uint8)
        error = FileNotFoundError if failure == "missing" else ValueError
        with pytest.raises(error):
            reader.lookup(["a", "b"], np.array([[0, 1], [2, 3]], np.int64), output)
        assert np.all(output == 201)


@pytest.mark.parametrize("device", ["cpu", "cuda"])
def test_lookup_snapshots_table_name_generator(tmp_path, device):
    if device == "cuda":
        torch = pytest.importorskip("torch")
        pytest.importorskip("mooncake._engram_cuda")
        if not torch.cuda.is_available():
            pytest.skip("requires CUDA")
    store, rows, _ = make_store(tmp_path)
    ids = np.array([[0, 12, 4], [10, -1, 9]], np.int64)
    output = np.full((2, 3, 17), 201, np.uint8)
    expected = np.stack([rows[[0, 10]], rows[[1, 0]] ^ 127, rows[[4, 9]]], axis=1)
    expected[1, 1] = 201
    try:
        if device == "cuda":
            ids = torch.from_numpy(ids).cuda()
            output = torch.from_numpy(output).cuda()
            stream = torch.cuda.current_stream().cuda_stream
        else:
            stream = None
        store.lookup(
            (name for name in ("a", "b", "a")),
            ids,
            output,
            stream=stream,
            offsets=[0, 11, 0],
        )
        actual = output.cpu().numpy() if device == "cuda" else output
        np.testing.assert_array_equal(actual, expected)
    finally:
        store.close()


def test_namespace_layout_and_empty_table(tmp_path):
    store, _, specs = make_store(tmp_path)
    store.close()
    wrong = dict(specs, a={"num_rows": 11, "row_bytes": 18})
    with EngramStore(tables=wrong, namespace="test/v1", local_dir=tmp_path) as other:
        with pytest.raises(ValueError):
            other.lookup(
                ["a"], np.zeros((1, 1), np.int64), np.zeros((1, 1, 18), np.uint8)
            )
    with EngramStore(
        tables={"a": {"num_rows": 0, "row_bytes": 17}},
        namespace="other",
        local_dir=tmp_path,
    ) as empty:
        empty.populate("a", np.empty((0, 17), np.uint8))
        output = np.full((1, 1, 17), 123, np.uint8)
        empty.lookup(["a"], np.array([[-1]], np.int64), output)
        assert np.all(output == 123)
        empty.lookup(["a"], np.empty((0, 1), np.int64), output[:0])


@pytest.mark.parametrize("head_major", [False, True])
@pytest.mark.parametrize("dtype_name", ["int32", "int64"])
@pytest.mark.parametrize("persistent", [True, False])
def test_cuda_streams_and_graph_replay(tmp_path, head_major, dtype_name, persistent):
    torch = pytest.importorskip("torch")
    pytest.importorskip("mooncake._engram_cuda")
    if not torch.cuda.is_available():
        pytest.skip("requires CUDA")
    store, rows, _ = make_store(tmp_path, 264, persistent=persistent)
    ids_storage = torch.zeros((5, 4), device="cuda", dtype=getattr(torch, dtype_name))
    ids = ids_storage[:, ::2]
    ids[:, 1] = 11
    output = torch.empty(
        (2, 5, 264) if head_major else (5, 2, 264), dtype=torch.uint8, device="cuda"
    )
    if head_major:
        output = output.permute(1, 0, 2)
    prefetch = torch.cuda.Stream()
    current = torch.cuda.current_stream()
    prefetch.wait_stream(current)
    store.lookup(["a", "b"], ids, output, stream=prefetch.cuda_stream, offsets=[0, 11])
    current.wait_stream(prefetch)
    torch.cuda.synchronize()
    graph = torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph, stream=prefetch):
        store.lookup(
            ["a", "b"], ids, output, stream=prefetch.cuda_stream, offsets=[0, 11]
        )
    for row in (1, 8, 3):
        ids[:, 0] = row
        ids[:, 1] = row + 11
        ids[0] = -1
        output.fill_(201)
        prefetch.wait_stream(current)
        graph.replay()
        current.wait_stream(prefetch)
        result = output.cpu().numpy()
        assert np.all(result[0] == 201)
        np.testing.assert_array_equal(result[1:, 0], np.tile(rows[row], (4, 1)))
        np.testing.assert_array_equal(result[1:, 1], np.tile(rows[row] ^ 127, (4, 1)))
    torch.cuda.synchronize()
    del graph
    gc.collect()
    store.close()

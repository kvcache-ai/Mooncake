"""Exercise /metrics with the Prometheus parser and real ZMQ deliveries.

Requires pytest, msgpack, pyzmq and prometheus-client. Run with:
    CONDUCTOR_BINARY=/path/to/mooncake_conductor \
        python -m pytest mooncake-conductor/tests/python/test_metrics.py -v
"""

import json
import os
import socket
import struct
import subprocess
import time
from urllib.request import Request, urlopen

import msgpack
import pytest
import zmq
from prometheus_client.parser import text_string_to_metric_families


@pytest.fixture
def conductor(tmp_path):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    config = tmp_path / "conductor.json"
    config.write_text(
        json.dumps({"http_server_port": port, "rpc_server_port": 0, "services": []})
    )

    def request(path, body=None):
        payload = None if body is None else msgpack.packb(body, use_bin_type=True)
        req = Request(
            f"http://127.0.0.1:{port}{path}",
            data=payload,
            headers={"Content-Type": "application/msgpack"},
        )
        with urlopen(req, timeout=3) as response:
            data = response.read()
            if path == "/metrics":
                assert response.headers["Content-Type"].startswith("text/plain")
                return data.decode()
            return msgpack.unpackb(data, raw=False)

    context = zmq.Context()
    publishers = []
    with (tmp_path / "conductor.log").open("w+") as log:
        proc = subprocess.Popen(
            [os.environ["CONDUCTOR_BINARY"]],
            env=dict(os.environ, CONDUCTOR_CONFIG_PATH=str(config)),
            stdout=log,
            stderr=log,
        )
        try:
            deadline = time.monotonic() + 10
            while True:
                try:
                    request("/services")
                    break
                except OSError:
                    if proc.poll() is not None or time.monotonic() >= deadline:
                        log.seek(0)
                        pytest.fail("Conductor failed to start:\n" + log.read())
                    time.sleep(0.05)
            for instance in ['engine"a', "engine-b"]:
                publisher = context.socket(zmq.XPUB)
                publisher.setsockopt(zmq.LINGER, 0)
                publishers.append(publisher)
                publisher_port = publisher.bind_to_random_port("tcp://127.0.0.1")
                request(
                    "/register",
                    {
                        "type": "vLLM",
                        "endpoint": f"tcp://127.0.0.1:{publisher_port}",
                        "modelname": "metrics-test",
                        "tenant_id": "tenant\\a",
                        "instance_id": instance,
                        "block_size": 16,
                        "dp_rank": 0,
                        "hash_profile": {
                            "strategy": "vllm_v1",
                            "algorithm": "sha256",
                            "python_hash_seed": "0",
                            "index_projection": "low64_be",
                        },
                    },
                )
                assert publisher.poll(5000), "Conductor did not subscribe"
                assert publisher.recv().startswith(b"\x01")
            yield request, publishers
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            for publisher in publishers:
                publisher.close()
            context.term()


def stored(block_hash, block_size=16):
    return {
        "type": "BlockStored",
        "block_hashes": [block_hash],
        "parent_block_hash": None,
        "token_ids": None,
        "block_size": block_size,
        "lora_id": None,
        "medium": "GPU",
        "lora_name": None,
        "group_idx": 0,
    }


def publish(publisher, sequence, events):
    publisher.send_multipart(
        [b"", struct.pack("!Q", sequence), msgpack.packb([time.time(), events, 0])]
    )


def value(text, name, **labels):
    matches = [
        sample.value
        for family in text_string_to_metric_families(text)
        for sample in family.samples
        if sample.name == "mooncake_conductor_" + name
        and all(sample.labels.get(key) == val for key, val in labels.items())
    ]
    assert len(matches) == 1, (name, labels, matches)
    return matches[0]


def wait_for(request, predicate):
    deadline = time.monotonic() + 5
    while True:
        text = request("/metrics")
        if predicate(text):
            return text
        if time.monotonic() >= deadline:
            pytest.fail("Metrics did not reach the expected values:\n" + text)
        time.sleep(0.02)


def test_families_keep_types_across_subscriptions(conductor):
    request, publishers = conductor
    for publisher in publishers:
        publish(publisher, 1, [stored(11)])
    text = wait_for(
        request,
        lambda text: all(
            value(
                text,
                "events_total",
                instance_id=instance,
                origin="live",
                event_type="stored",
                result="applied",
            )
            == 1
            for instance in ['engine"a', "engine-b"]
        ),
    )
    families = list(text_string_to_metric_families(text))
    assert all(family.type in {"counter", "gauge"} for family in families), [
        (family.name, family.type) for family in families
    ]
    for family in families:
        assert family.type in {"counter", "gauge"}, family
        assert family.documentation
        if family.name == "mooncake_conductor_metrics_info":
            continue
        assert {sample.labels["instance_id"] for sample in family.samples} == {
            'engine"a',
            "engine-b",
        }
        assert all(
            sample.labels["tenant_id"] == "tenant\\a" for sample in family.samples
        )
        expected = "counter" if family.name + "_total" in text else "gauge"
        assert family.type == expected
    # Every family's declarations and samples form one contiguous block.
    current = None
    declared = set()
    for line in text.splitlines():
        if line.startswith("# HELP "):
            current = line.split()[2]
            assert current not in declared
            declared.add(current)
        elif line.startswith("# TYPE "):
            assert line.split()[2] == current
        elif line and not line.startswith("#"):
            assert line.split("{", 1)[0] == current
    assert len(declared) == len(families)


def test_corrected_same_sequence_and_partial_batch_are_counted(conductor):
    request, publishers = conductor
    labels = dict(instance_id='engine"a', origin="live", event_type="stored")
    publish(publishers[0], 1, [stored(11), stored(22, 32)])
    wait_for(
        request,
        lambda text: value(text, "events_total", **labels, result="rejected") == 1,
    )
    publish(publishers[0], 1, [stored(11), stored(22)])
    text = wait_for(
        request,
        lambda text: value(text, "events_total", **labels, result="applied") == 3,
    )
    assert value(text, "events_total", **labels, result="rejected") == 1
    assert value(text, "event_block_references_total", **labels) == 3
    assert (
        value(
            text,
            "event_batch_redeliveries_total",
            instance_id='engine"a',
            origin="live",
        )
        == 1
    )
    assert (
        value(text, "last_applied_event_timestamp_seconds", instance_id='engine"a') > 0
    )
    # A successful repeat is an additional handling attempt, not unique state.
    publish(publishers[0], 1, [stored(11), stored(22)])
    text = wait_for(
        request,
        lambda text: value(text, "events_total", **labels, result="applied") == 5,
    )
    assert (
        value(
            text,
            "event_batch_redeliveries_total",
            instance_id='engine"a',
            origin="live",
        )
        == 2
    )
    assert value(text, "events_total", **labels, result="rejected") == 1
    assert (
        value(
            text,
            "events_total",
            instance_id="engine-b",
            origin="live",
            event_type="stored",
            result="applied",
        )
        == 0
    )

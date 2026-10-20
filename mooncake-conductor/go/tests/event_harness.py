"""CPU-only event fixture publisher for the Go RPC integration tests.

Run beside a Conductor configured with HTTP and RPC enabled and no services.
Requires pyzmq/msgpack; Go consumers do not require Python.
"""

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.request import Request, urlopen

import msgpack
import zmq


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--http-port", type=int, required=True)
    args = parser.parse_args()
    fixtures = Path(__file__).resolve().parents[2] / "tests/fixtures"
    sg = json.loads((fixtures / "sglang_map_event_vectors.json").read_text())[
        "integration"
    ]
    vl = json.loads((fixtures / "hash_golden_vectors_sha256.json").read_text())
    context = zmq.Context()
    sockets, sequences, subscribed = {}, {}, set()
    configs = []
    sg_profile = dict(
        strategy="sglang",
        algorithm="sha256_raw",
        python_hash_seed="",
        index_projection="first64_be",
    )
    for name, publisher, model, rank, profile in [
        ("sg0", "SGLang", "go-sg", 0, sg_profile),
        ("sg12", "SGLang", "go-sg", 12, sg_profile),
        ("pool", "Mooncake", "go-sg", 0, sg_profile),
        (
            "vl0",
            "vLLM",
            "go-vl",
            0,
            {k: v for k, v in vl["profile"].items() if k != "root_digest"},
        ),
    ]:
        socket = context.socket(zmq.XPUB)
        socket.setsockopt(zmq.LINGER, 0)
        port = socket.bind_to_random_port("tcp://127.0.0.1")
        sockets[name], sequences[name] = socket, 0
        configs.append(
            dict(
                instance_id=name,
                endpoint=f"tcp://127.0.0.1:{port}",
                publisher_type=publisher,
                model_name=model,
                block_size=4,
                dp_rank=rank,
                hash_profile=profile,
            )
        )

    def publish(name, payload):
        socket = sockets[name]
        if name not in subscribed:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if socket.poll(100) and socket.recv().startswith(b"\x01"):
                    subscribed.add(name)
                    break
            else:
                raise RuntimeError(f"No subscriber for {name}")
        socket.send_multipart([b"", sequences[name].to_bytes(8, "big"), payload])
        sequences[name] += 1

    def engine(name, action):
        if name == "vl0":
            case = vl["cases"][0]
            hashes = [int(h["projected_decimal"]) for h in case["expected"]]
            if action == "seed":
                events = [
                    dict(
                        type="BlockStored",
                        block_hashes=hashes,
                        parent_block_hash=None,
                        token_ids=case["token_ids"],
                        block_size=4,
                        lora_id=None,
                        lora_name=None,
                        medium="GPU",
                    )
                ]
            elif action == "remove":
                events = [
                    dict(type="BlockRemoved", block_hashes=[hashes[0]], medium="GPU")
                ]
            else:
                events = [dict(type="AllBlocksCleared")]
            publish(name, msgpack.packb([time.time(), events, 0]))
        else:
            rank = 12 if name == "sg12" else 0
            payloads = (
                [p["payload_hex"] for p in sg["tier_publishes"]]
                if action == "seed"
                else [
                    sg["remove_first_block_hex"]
                    if action == "remove"
                    else sg["clear_all_hex"]
                ]
            )
            for payload in payloads:
                batch = msgpack.unpackb(bytes.fromhex(payload), raw=False)
                batch[2] = rank
                publish(name, msgpack.packb(batch))

    def pool(action):
        timestamp = int(time.time() * 1000)
        common = dict(
            timestamp=timestamp,
            model_name="go-sg",
            block_size=4,
            additional_salt=None,
            lora_name=None,
            tenant_id="default",
            backend_id="pool",
            dp_rank=0,
        )
        events = []
        if action == "seed":
            for medium in ["CPU", "DISK"]:
                for digest in sg["upstream_hex_digests"]:
                    events.append(
                        dict(
                            common,
                            event_type="stored",
                            event_id=len(events),
                            medium=medium,
                            group_id=None,
                            seq_hashes=[],
                            base_block_idx=None,
                            object_key=f"go_{digest}_k",
                            parent_hash=None,
                            token_ids=None,
                        )
                    )
        elif action == "remove":
            events.append(
                dict(
                    common,
                    event_type="removed",
                    event_id=0,
                    medium="CPU",
                    group_id=None,
                    seq_hashes=[],
                    base_block_idx=None,
                    object_key=f"go_{sg['upstream_hex_digests'][-1]}_k",
                )
            )
        else:
            events.append(dict(common, event_type="cleared", event_id=0, medium=None))
        publish("pool", msgpack.packb([timestamp, events, 0]))

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def respond(self, value, status=200):
            body = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            self.respond(
                dict(
                    services=configs,
                    sglang_tokens=sg["token_ids"],
                    vllm_tokens=vl["cases"][0]["token_ids"],
                )
            )

        def do_POST(self):
            try:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path == "/query":
                    request = Request(
                        f"http://127.0.0.1:{args.http_port}/query",
                        data=msgpack.packb(body),
                        headers={"Content-Type": "application/msgpack"},
                    )
                    with urlopen(request, timeout=5) as response:
                        self.respond(msgpack.unpackb(response.read(), raw=False))
                else:
                    name, action = body["name"], body["action"]
                    if action not in {"seed", "remove", "clear"}:
                        raise ValueError("invalid action")
                    pool(action) if name == "pool" else engine(name, action)
                    self.respond({"ok": True})
            except Exception as exc:
                self.respond({"error": str(exc)}, 500)

    try:
        HTTPServer(("127.0.0.1", args.port), Handler).serve_forever()
    finally:
        for socket in sockets.values():
            socket.close()
        context.term()


if __name__ == "__main__":
    main()

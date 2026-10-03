# End-to-end: compare GPU-only, plus host memory, plus Mooncake as three cache
# tiers over cold miss, full hit, partial hit and multi-turn hit patterns. This
# is a client and a launcher only; it does not modify the server.

import json
import os
import random
import shutil
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request

from .config import CACHE_TIER_DESCRIPTION
from .metrics import diff_counters, http_get, scrape, split_metric

SERVER_START_TIMEOUT = 2400.0
HEALTH_POLL_INTERVAL = 5.0
SHUTDOWN_GRACE_SECONDS = 60.0

# /flush_cache attempts and interval, to let HiCache drain prefetch and write-back
FLUSH_ATTEMPTS = 6
FLUSH_RETRY_INTERVAL = 10.0

# HiCache storage backend name as SGLang spells it
HICACHE_BACKEND = "mooncake"


def find_free_port(start=30000, end=40000):
    for port in range(start, end):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise RuntimeError(f"no free port between {start} and {end}")


def wait_for_port(host, port, timeout, process=None):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if process is not None and process.poll() is not None:
            raise RuntimeError(
                f"process exited with {process.returncode} before port {port} opened"
            )
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.settimeout(2.0)
            if probe.connect_ex((host, port)) == 0:
                return True
        time.sleep(0.5)
    raise TimeoutError(f"timed out after {timeout}s waiting for {host}:{port}")


class MooncakeStoreService:
    """Start a local Mooncake master to provide L3 for HiCache. Both the client
    and the master are on 127.0.0.1, so this only represents a local loopback
    path."""

    def __init__(
        self,
        master_binary,
        work_dir,
        port=50051,
        metadata_port=8080,
        log_name="mooncake_master.log",
    ):
        self.master_binary = master_binary
        self.work_dir = work_dir
        self.port = port
        self.metadata_port = metadata_port
        self.log_path = os.path.join(work_dir, log_name)
        self.process = None
        self.log_handle = None

    @property
    def metadata_server(self):
        # The Transfer Engine exchanges segment metadata on this HTTP endpoint,
        # which is a different listener from the master's RPC port
        return f"http://127.0.0.1:{self.metadata_port}/metadata"

    def start(self):
        if not os.path.isfile(self.master_binary):
            raise FileNotFoundError(
                f"mooncake master binary not found: {self.master_binary}"
            )
        self.log_handle = open(self.log_path, "w", encoding="utf-8")
        try:
            self.process = subprocess.Popen(
                [
                    self.master_binary,
                    f"--rpc_port={self.port}",
                    "--enable_http_metadata_server=true",
                    f"--http_metadata_server_port={self.metadata_port}",
                ],
                stdout=self.log_handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                cwd=self.work_dir,
            )
            wait_for_port("127.0.0.1", self.port, timeout=120.0, process=self.process)
            wait_for_port(
                "127.0.0.1", self.metadata_port, timeout=120.0, process=self.process
            )
        except BaseException:
            # Reap anything already started, plus the log handle, then re-raise
            self.stop()
            raise
        return self

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=SHUTDOWN_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                self.process.wait(timeout=30)
        if self.log_handle is not None:
            self.log_handle.close()
            self.log_handle = None
        self.process = None

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, traceback):
        self.stop()
        return False


def build_server_command(spec):
    """Assemble the SGLang launch command for a cache tier."""
    command = [
        spec["python"],
        "-m",
        "sglang.launch_server",
        "--model-path",
        spec["model_path"],
        "--tp-size",
        str(spec["tp_size"]),
        "--page-size",
        str(spec["page_size"]),
        "--host",
        "127.0.0.1",
        "--port",
        str(spec["port"]),
        "--mem-fraction-static",
        str(spec["mem_fraction_static"]),
        "--trust-remote-code",
    ]
    if spec.get("context_length"):
        command += ["--context-length", str(spec["context_length"])]
    if spec.get("attention_backend"):
        command += ["--attention-backend", spec["attention_backend"]]
    if spec.get("max_running_requests"):
        command += ["--max-running-requests", str(spec["max_running_requests"])]
    if spec.get("chunked_prefill_size"):
        command += ["--chunked-prefill-size", str(spec["chunked_prefill_size"])]
    if spec.get("max_prefill_tokens"):
        command += ["--max-prefill-tokens", str(spec["max_prefill_tokens"])]
    if spec.get("disable_radix_cache"):
        command.append("--disable-radix-cache")
    if spec.get("enable_metrics", True):
        command.append("--enable-metrics")
    if spec.get("hicache_mem_layout"):
        command += ["--hicache-mem-layout", spec["hicache_mem_layout"]]
    if spec.get("hicache_io_backend"):
        command += ["--hicache-io-backend", spec["hicache_io_backend"]]

    tier = spec["cache_tier"]
    if tier in ("host", "mooncake"):
        command += [
            "--enable-hierarchical-cache",
            "--hicache-write-policy",
            spec.get("write_policy", "write_through"),
            "--hicache-ratio",
            str(spec.get("hicache_ratio", 2)),
        ]
        if spec.get("hicache_size"):
            command += ["--hicache-size", str(spec["hicache_size"])]
    if tier == "mooncake":
        command += [
            "--hicache-storage-backend",
            HICACHE_BACKEND,
            "--hicache-storage-prefetch-policy",
            spec.get("prefetch_policy", "wait_complete"),
        ]
    return command


class SGLangServer:
    """Start an SGLang server, wait for it to become healthy, and reap it."""

    def __init__(self, spec, work_dir):
        self.spec = spec
        self.work_dir = work_dir
        self.port = spec["port"]
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.log_path = os.path.join(work_dir, spec["log_name"])
        self.command = build_server_command(spec)
        self.env = dict(os.environ)
        self.env.update(spec.get("env", {}))
        self.process = None
        self.log_handle = None

    def start(self):
        self.log_handle = open(self.log_path, "w", encoding="utf-8")
        self.log_handle.write("COMMAND: " + " ".join(self.command) + "\n")
        for key, value in sorted(self.spec.get("env", {}).items()):
            self.log_handle.write(f"ENV {key}={value}\n")
        self.log_handle.flush()
        try:
            self.process = subprocess.Popen(
                self.command,
                stdout=self.log_handle,
                stderr=subprocess.STDOUT,
                env=self.env,
                start_new_session=True,
                cwd=self.work_dir,
            )
            self.wait_until_healthy()
        except BaseException:
            # When the health check fails, __enter__ has not returned yet, so
            # __exit__ never runs; reap the process and the log handle here
            self.stop()
            raise
        return self

    def wait_until_healthy(self, timeout=SERVER_START_TIMEOUT):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"SGLang server exited with {self.process.returncode} before "
                    f"becoming healthy; log at {self.log_path}"
                )
            try:
                body = http_get(f"{self.base_url}/health", timeout=5.0)
                if "200" in body or body.strip() == "":
                    return True
            except (urllib.error.URLError, OSError, TimeoutError):
                pass
            time.sleep(HEALTH_POLL_INTERVAL)
        raise TimeoutError(
            f"timed out waiting for the SGLang server; log at {self.log_path}"
        )

    def metrics(self):
        return scrape(self.base_url, timeout=20.0)

    def server_info(self):
        """Read the configuration that actually took effect, such as the
        attention backend SGLang really selected."""
        try:
            payload = http_get(f"{self.base_url}/get_server_info", timeout=20.0)
        except (urllib.error.URLError, OSError, TimeoutError):
            return {}
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return {}

    def flush_cache(self, attempts=FLUSH_ATTEMPTS, interval=FLUSH_RETRY_INTERVAL):
        """Flush the server's local cache, raising if it ultimately fails.

        SGLang refuses the operation with HTTP 400 while requests are in flight
        or prefetch and write-back have not drained (its log says "Cache not
        flushed because there are pending requests", even when the client side
        queue and running counts are both 0). So give the backend time to drain
        and retry a few times; if it still fails, raise. Never continue with a
        stale cache, because that makes the following cold miss, eviction and
        throughput results incomparable.
        """
        request = urllib.request.Request(
            f"{self.base_url}/flush_cache", data=b"{}", method="POST"
        )
        request.add_header("Content-Type", "application/json")
        last_error = None
        for attempt in range(1, attempts + 1):
            try:
                with urllib.request.urlopen(request, timeout=120.0) as response:
                    if response.status == 200:
                        return True
                    last_error = f"HTTP {response.status} {response.read()[:200]!r}"
            except urllib.error.HTTPError as exc:
                last_error = f"HTTP {exc.code} {exc.read()[:200]!r}"
            except (urllib.error.URLError, OSError, TimeoutError) as exc:
                last_error = f"request failed: {exc}"
            if attempt < attempts:
                print(
                    f"[e2e] /flush_cache attempt {attempt} failed ({last_error}), "
                    f"retrying in {interval}s"
                )
                time.sleep(interval)
        raise RuntimeError(
            f"/flush_cache still failing after {attempts} attempts, the cache may "
            f"be stale: {last_error}"
        )

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=SHUTDOWN_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                self.process.wait(timeout=30)
        if self.log_handle is not None:
            self.log_handle.close()
            self.log_handle = None
        self.process = None

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, traceback):
        self.stop()
        return False


def send_generate_stream(
    base_url, input_ids, max_new_tokens, request_id, timeout=1800.0
):
    """Send one streaming request through SGLang's native /generate and measure
    TTFT and ITL. input_ids go in directly so the input length is exact and hit
    patterns can be built by token count."""
    payload = {
        "input_ids": list(input_ids),
        "sampling_params": {
            "max_new_tokens": max_new_tokens,
            "temperature": 0.0,
            "top_p": 1.0,
            "ignore_eos": True,
        },
        "stream": True,
        "rid": request_id,
    }
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(f"{base_url}/generate", data=body, method="POST")
    request.add_header("Content-Type", "application/json")

    begin = time.perf_counter()
    first_token_time = None
    token_times = []
    text_pieces = []
    meta_info = None
    chunk_count = 0
    last_completion_tokens = 0
    generated_ids = []

    with urllib.request.urlopen(request, timeout=timeout) as response:
        for raw_line in response:
            line = raw_line.decode("utf-8", errors="replace").strip()
            if not line.startswith("data:"):
                continue
            payload_text = line[len("data:") :].strip()
            if not payload_text or payload_text == "[DONE]":
                continue
            chunk = json.loads(payload_text)
            chunk_count += 1
            now = time.perf_counter()

            piece = chunk.get("text") or ""
            if piece:
                text_pieces.append(piece)
            if chunk.get("meta_info"):
                meta_info = chunk["meta_info"]
            incoming = chunk.get("output_ids")
            if incoming:
                generated_ids = merge_output_ids(generated_ids, list(incoming))

            # A token can decode to an empty string, so the completion_tokens
            # delta is what says whether this chunk really carried a new token
            completion_tokens = 0
            if meta_info:
                value = meta_info.get("completion_tokens")
                if isinstance(value, int):
                    completion_tokens = value
            new_tokens = completion_tokens - last_completion_tokens
            if new_tokens <= 0 and piece:
                new_tokens = 1
            last_completion_tokens = max(last_completion_tokens, completion_tokens)

            if new_tokens > 0:
                if first_token_time is None:
                    first_token_time = now
                # One chunk can merge several tokens, so the interval has to be
                # shared out over the new token count; otherwise concurrency or
                # server-side batching visibly inflates ITL
                token_times.append((now, new_tokens))

    end = time.perf_counter()
    if first_token_time is None:
        raise RuntimeError(
            f"request {request_id} returned no token: saw {chunk_count} chunks, "
            f"last meta_info={meta_info}"
        )

    intervals = []
    for index in range(1, len(token_times)):
        now, count = token_times[index]
        previous = token_times[index - 1][0]
        per_token = (now - previous) / count
        intervals.extend([per_token] * count)

    output_tokens = last_completion_tokens or sum(count for _, count in token_times)
    return {
        "request_id": request_id,
        "ttft_ms": (first_token_time - begin) * 1000.0,
        "e2e_ms": (end - begin) * 1000.0,
        "itl_ms": [value * 1000.0 for value in intervals],
        "output_tokens": output_tokens,
        "output_ids": generated_ids,
        "text_chars": sum(len(piece) for piece in text_pieces),
        "meta_info": meta_info or {},
    }


def merge_output_ids(accumulated, incoming):
    """Merge the output_ids from streamed chunks into a full list.

    By default (incremental_streaming_output=False) SGLang sends the complete
    list so far in every chunk; with that switch on it sends only the delta for
    the chunk. Both have to work: a new list that starts with what has been
    accumulated is treated as cumulative, anything else as a delta to append.
    """
    if not accumulated:
        return list(incoming)
    if (
        len(incoming) >= len(accumulated)
        and incoming[: len(accumulated)] == accumulated
    ):
        return list(incoming)
    return accumulated + list(incoming)


def make_token_ids(rng, length, vocab_size):
    return [rng.randrange(0, vocab_size) for _ in range(length)]


def shared_prefix(pattern, case, rng, vocab_size):
    """The tokens every request of a hit pattern has in common, or None.

    This is what a warmup may cache: sending a whole request would also cache
    that request's unique suffix, and the measured batch would then show a better
    hit rate than the pattern it is labelled with.
    """
    length = case.input_len
    if pattern == "full_hit":
        return make_token_ids(rng, length, vocab_size)
    if pattern == "partial_hit":
        return make_token_ids(rng, max(1, length // 2), vocab_size)
    return None


def build_prompts(pattern, case, rng, vocab_size, shared=None):
    """Build the input_ids of every request for a hit pattern.

    Each repeat uses its own random prefix, so a cold start really is cold and
    nothing depends on remove_all() for cleanup. A multi-turn conversation's
    context is maintained by the caller per request and does not go through here.
    """
    length = case.input_len

    if pattern == "cold_miss":
        # Entirely independent token sequences, so no tier can possibly hit
        return [
            make_token_ids(rng, length, vocab_size) for _ in range(case.num_requests)
        ]

    if pattern in ("full_hit", "partial_hit") and shared is None:
        raise ValueError(f"{pattern} needs the shared prefix of the pattern")

    if pattern == "full_hit":
        return [list(shared) for _ in range(case.num_requests)]

    if pattern == "partial_hit":
        shared_len = len(shared)
        prompts = []
        for _ in range(case.num_requests):
            suffix = make_token_ids(rng, length - shared_len, vocab_size)
            prompts.append(list(shared) + suffix)
        return prompts

    if pattern == "multiturn":
        # Every request is its own conversation with its own prefix, matching the
        # semantics of SGLang's bench_multiturn.py
        return [
            make_token_ids(rng, length, vocab_size) for _ in range(case.num_requests)
        ]

    raise ValueError(f"unknown hit pattern {pattern!r}")


def run_e2e_case(
    server, config, case, run_id, repeat_index, vocab_size, kv_meta=None, tier_meta=None
):
    """Run one end-to-end measurement point: warm up by pattern, then measure.
    kv_meta carries the KV bytes per token, to turn hit tokens into fetched bytes.
    tier_meta carries the pool sizes and the storage backend of this cache tier."""
    pattern = config.hit_pattern
    rng = random.Random(f"{config.seed}:{run_id}:{repeat_index}:{case.case_id}")

    case_payload = dict(case.as_dict())
    if kv_meta:
        case_payload.update(kv_meta)
        case_payload["total_kv_bytes_prompt"] = case.input_len * kv_meta.get(
            "kv_bytes_per_token_aggregate", 0
        )
    if tier_meta:
        case_payload.update(tier_meta)

    rounds = config.rounds if pattern == "multiturn" else 1
    records = []
    # A multi-turn run keeps one context per request; the next round appends the
    # tokens this round actually generated
    conversations = None

    for round_index in range(rounds):
        if pattern == "multiturn" and conversations is not None:
            prompts = [list(context) for context in conversations]
        else:
            shared = shared_prefix(pattern, case, rng, vocab_size)
            prompts = build_prompts(pattern, case, rng, vocab_size, shared)

        # Warmup: the shared prefix is written to the cache first, and the warmup
        # request's latency is recorded apart from the measured ones. Warming with
        # a whole request would also cache that request's unique suffix, which for
        # partial_hit would turn one measured request into a full hit.
        warmup_meta = None
        if config.warmups > 0 and shared is not None:
            warm = send_generate_stream(
                server.base_url,
                shared,
                case.output_len,
                f"{run_id}-warm-{round_index}",
            )
            warmup_meta = {"ttft_ms": warm["ttft_ms"], "e2e_ms": warm["e2e_ms"]}

        metrics_before = server.metrics()

        results = []
        concurrent = case.max_concurrency > 1
        measure_begin = time.perf_counter()
        if concurrent:
            results = _run_concurrent(
                server, prompts, case, run_id, round_index, pattern
            )
        else:
            for index, prompt in enumerate(prompts):
                record = send_generate_stream(
                    server.base_url,
                    prompt,
                    case.output_len,
                    f"{run_id}-r{round_index}-q{index}",
                )
                results.append(record)
        run_wall_ms = (time.perf_counter() - measure_begin) * 1000.0

        metrics_after = server.metrics()
        delta, appeared = diff_counters(metrics_before, metrics_after)

        for index, record in enumerate(results):
            records.append(
                {
                    "kind": "e2e",
                    "run_id": run_id,
                    "config": config.as_dict(),
                    "case": case_payload,
                    "cache_tier_description": CACHE_TIER_DESCRIPTION[config.cache_tier],
                    "repeat_index": repeat_index,
                    "round_index": round_index,
                    "request_index": index,
                    "input_len_requested": case.input_len,
                    "output_len_requested": case.output_len,
                    "ttft_ms": record["ttft_ms"],
                    "e2e_ms": record["e2e_ms"],
                    "itl_ms": record["itl_ms"],
                    "output_tokens": record["output_tokens"],
                    "meta_info": record["meta_info"],
                    "warmup": warmup_meta if index == 0 else None,
                }
            )

        if pattern == "multiturn":
            # Append this round's tokens to each conversation's own context.
            # The appended length has to match completion_tokens: a mismatch means
            # the streamed chunks lost a token, and the next round would not be
            # "previous input plus full output". Fail loudly such cases.
            for index, record in enumerate(results):
                expected = record["output_tokens"]
                actual = len(record["output_ids"])
                if actual != expected:
                    raise RuntimeError(
                        f"request {record['request_id']} parsed {actual} tokens but "
                        f"completed {expected}; the streamed chunks lost a token"
                    )
            conversations = [
                list(prompts[index]) + list(results[index]["output_ids"])
                for index in range(len(results))
            ]

        records.append(
            {
                "kind": "e2e_run_metrics",
                "run_id": run_id,
                "config": config.as_dict(),
                "case": case_payload,
                "repeat_index": repeat_index,
                "round_index": round_index,
                "run_wall_ms": run_wall_ms,
                "metrics_delta": delta,
                "metrics_appeared_in_window": appeared,
                "metrics_present": sorted(
                    {split_metric(name)[0] for name in metrics_after}
                ),
            }
        )

    return records


def _run_concurrent(server, prompts, case, run_id, round_index, pattern):
    """Send the requests concurrently, bounded by max_concurrency."""
    import threading

    results = [None] * len(prompts)
    errors = []
    semaphore = threading.Semaphore(case.max_concurrency)

    def worker(index, prompt):
        with semaphore:
            try:
                results[index] = send_generate_stream(
                    server.base_url,
                    prompt,
                    case.output_len,
                    f"{run_id}-r{round_index}-q{index}",
                )
            except Exception as exc:  # noqa: BLE001 - collect and re-raise below
                errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=(index, prompt))
        for index, prompt in enumerate(prompts)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise RuntimeError(f"{len(errors)} concurrent requests failed: {errors[0]}")
    return results


def resolve_mooncake_binary(explicit=None):
    if explicit:
        return explicit
    candidates = [
        os.path.expanduser("~/mc-main/build/mooncake-store/src/mooncake_master"),
        shutil.which("mooncake_master"),
    ]
    for candidate in candidates:
        if candidate and os.path.isfile(candidate):
            return candidate
    raise FileNotFoundError("mooncake_master not found; pass --mooncake-master")

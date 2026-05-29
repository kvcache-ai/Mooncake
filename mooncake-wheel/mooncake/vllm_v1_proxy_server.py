# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the vLLM project
# This is a copy from vLLM repo at tests/v1/kv_connector/nixl_integration/toy_proxy_server.py

import argparse
import asyncio
import itertools
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Proxy migration constants
VLLM_MOONCAKE_MIGRATION_HTTP_PORT = int(
    os.getenv("VLLM_MOONCAKE_MIGRATION_HTTP_PORT", "18900")
)
VLLM_MOONCAKE_MIGRATION_TIMEOUT = int(
    os.getenv("VLLM_MOONCAKE_MIGRATION_TIMEOUT", "300")
)
VLLM_MOONCAKE_MAX_REMIGRATION_HOPS = int(
    os.getenv("VLLM_MOONCAKE_MAX_REMIGRATION_HOPS", "3")
)


@dataclass
class ProxyMigrationState:
    """Tracks a single request's migration on the proxy."""
    request_id: str
    source_node_id: str
    source_host: str
    source_migration_port: int
    target_node_id: str
    target_host: str
    target_migration_port: int
    target_rpc_port: int
    target_base_addr: list[int] = field(default_factory=list)
    block_id_map: dict[str, int] = field(default_factory=dict)
    started_at: float = 0.0
    hop_count: int = 0


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager to handle startup and shutdown events.
    """
    # Startup: Initialize client pools for prefiller and decoder services
    app.state.prefill_clients = []
    app.state.decode_clients = []

    # Create prefill clients
    for i, (host, port) in enumerate(global_args.prefiller_instances):
        prefiller_base_url = f"http://{host}:{port}/v1"
        app.state.prefill_clients.append(
            {
                "client": httpx.AsyncClient(
                    timeout=None,
                    base_url=prefiller_base_url,
                    limits=httpx.Limits(
                        max_connections=None,
                        max_keepalive_connections=None,
                    ),
                ),
                "host": host,
                "port": port,
                "id": i,
            }
        )

    # Create decode clients
    for i, (host, port) in enumerate(global_args.decoder_instances):
        decoder_base_url = f"http://{host}:{port}/v1"
        app.state.decode_clients.append(
            {
                "client": httpx.AsyncClient(
                    timeout=None,
                    base_url=decoder_base_url,
                    limits=httpx.Limits(
                        max_connections=None,
                        max_keepalive_connections=None,
                    ),
                ),
                "host": host,
                "port": port,
                "id": i,
            }
        )

    # Initialize round-robin iterators
    app.state.prefill_iterator = itertools.cycle(range(len(app.state.prefill_clients)))
    app.state.decode_iterator = itertools.cycle(range(len(app.state.decode_clients)))

    # Migration state
    app.state.draining_nodes: set[str] = set()
    app.state.migrating_reqs: dict[str, ProxyMigrationState] = {}

    print(
        f"Initialized {len(app.state.prefill_clients)} prefill clients "
        f"and {len(app.state.decode_clients)} decode clients."
    )

    yield

    # Shutdown: Close all clients
    for client_info in app.state.prefill_clients:
        await client_info["client"].aclose()

    for client_info in app.state.decode_clients:
        await client_info["client"].aclose()


# Update FastAPI app initialization to use lifespan
app = FastAPI(lifespan=lifespan)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--port", type=int, default=8000)
    # Always use 127.0.0.1 as localhost binds to IPv6 which is blocked on CI
    parser.add_argument("--host", type=str, default="127.0.0.1")

    # For prefiller instances
    parser.add_argument(
        "--prefiller-hosts",
        "--prefiller-host",
        type=str,
        nargs="+",
        default=["localhost"],
    )
    parser.add_argument(
        "--prefiller-ports", "--prefiller-port", type=int, nargs="+", default=[8100]
    )

    # For decoder instances
    parser.add_argument(
        "--decoder-hosts", "--decoder-host", type=str, nargs="+", default=["localhost"]
    )
    parser.add_argument(
        "--decoder-ports", "--decoder-port", type=int, nargs="+", default=[8200]
    )

    args = parser.parse_args()

    # Validate and pair hosts with ports
    if len(args.prefiller_hosts) != len(args.prefiller_ports):
        raise ValueError(
            "Number of prefiller hosts must match number of prefiller ports"
        )

    if len(args.decoder_hosts) != len(args.decoder_ports):
        raise ValueError("Number of decoder hosts must match number of decoder ports")

    # Create tuples of (host, port) for each service type
    args.prefiller_instances = list(zip(args.prefiller_hosts, args.prefiller_ports))
    args.decoder_instances = list(zip(args.decoder_hosts, args.decoder_ports))

    return args


def get_next_client(app, service_type: str):
    """
    Get the next client in round-robin fashion.

    Args:
        app: The FastAPI app instance
        service_type: Either 'prefill' or 'decode'

    Returns:
        The next client to use
    """
    if service_type == "prefill":
        client_idx = next(app.state.prefill_iterator)
        return app.state.prefill_clients[client_idx]
    elif service_type == "decode":
        client_idx = next(app.state.decode_iterator)
        return app.state.decode_clients[client_idx]
    else:
        raise ValueError(f"Unknown service type: {service_type}")


async def send_request_to_service(
    client_info: dict, endpoint: str, req_data: dict, request_id: str
):
    """
    Send a request to a service using a client from the pool.
    """
    req_data = req_data.copy()
    req_data["kv_transfer_params"] = {
        "do_remote_decode": True,
        "do_remote_prefill": False,
        "remote_engine_id": None,
        "remote_block_ids": None,
        "remote_host": None,
        "remote_port": None,
    }
    req_data["stream"] = False
    req_data["max_tokens"] = 1
    if "max_completion_tokens" in req_data:
        req_data["max_completion_tokens"] = 1
    if "stream_options" in req_data:
        del req_data["stream_options"]
    headers = {
        "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY')}",
        "X-Request-Id": request_id,
    }

    response = await client_info["client"].post(
        endpoint, json=req_data, headers=headers
    )
    response.raise_for_status()

    # read/consume the response body to release the connection
    # otherwise, it would http.ReadError
    await response.aread()

    return response


async def stream_service_response(
    client_info: dict, endpoint: str, req_data: dict, request_id: str
):
    """
    Asynchronously stream response from a service using a client from the pool.
    """
    headers = {
        "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY')}",
        "X-Request-Id": request_id,
    }

    async with client_info["client"].stream(
        "POST", endpoint, json=req_data, headers=headers
    ) as response:
        response.raise_for_status()
        async for chunk in response.aiter_bytes():
            yield chunk


async def _handle_completions(api: str, request: Request):
    try:
        req_data = await request.json()
        request_id = str(uuid.uuid4())

        # Get the next prefill client in round-robin fashion
        prefill_client_info = get_next_client(request.app, "prefill")

        # Send request to prefill service
        response = await send_request_to_service(
            prefill_client_info, api, req_data, request_id
        )

        # Extract the needed fields
        response_json = response.json()
        await response.aclose()  # CRITICAL: Release connection back to pool
        kv_transfer_params = response_json.get("kv_transfer_params", {})
        if kv_transfer_params:
            req_data["kv_transfer_params"] = kv_transfer_params

        # Route decode: check if this request is being migrated
        decode_client_info = None
        if request_id in request.app.state.migrating_reqs:
            mig_state = request.app.state.migrating_reqs[request_id]
            target_node_id = mig_state.target_node_id

            # Check if target node is now draining (cascade trigger)
            if target_node_id in request.app.state.draining_nodes:
                logger.warning(
                    "Target %s is draining, re-migrating request %s",
                    target_node_id, request_id,
                )
                new_state = await _migrate_request(
                    request.app, request_id, mig_state.source_node_id,
                    hop_count=mig_state.hop_count + 1,
                )
                if new_state is not None:
                    target_node_id = new_state.target_node_id
                else:
                    logger.error(
                        "Re-migration failed for %s, falling back to RR",
                        request_id,
                    )

            decode_client_info = _get_decode_client_by_node_id(
                request.app, target_node_id
            )

        if decode_client_info is None:
            # Normal round-robin routing
            decode_client_info = get_next_client(request.app, "decode")

        logger.debug("Using %s %s", prefill_client_info, decode_client_info)

        # Stream response from decode service
        async def generate_stream():
            async for chunk in stream_service_response(
                decode_client_info, api, req_data, request_id=request_id
            ):
                yield chunk

        return StreamingResponse(generate_stream(), media_type="application/json")

    except Exception as e:
        import sys
        import traceback

        exc_info = sys.exc_info()
        print(f"Error occurred in disagg prefill proxy server - {api} endpoint")
        print(e)
        print("".join(traceback.format_exception(*exc_info)))
        raise


@app.post("/v1/completions")
async def handle_completions(request: Request):
    return await _handle_completions("/completions", request)


@app.post("/v1/chat/completions")
async def handle_chat_completions(request: Request):
    return await _handle_completions("/chat/completions", request)


@app.get("/healthcheck")
async def healthcheck():
    """Simple endpoint to check if the server is running."""
    return {
        "status": "ok",
        "prefill_instances": len(app.state.prefill_clients),
        "decode_instances": len(app.state.decode_clients),
    }


# ──── Migration Helpers ────

async def _proxy_http_request(
    host: str, port: int, method: str, path: str,
    body: dict | None = None, timeout: float = 10.0,
) -> dict:
    """Make an HTTP request to a migration endpoint."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=5.0
        )
        body_bytes = json.dumps(body).encode() if body else b""
        request_line = f"{method} {path} HTTP/1.1\r\n".encode()
        headers = (
            f"Host: {host}:{port}\r\n"
            f"Content-Length: {len(body_bytes)}\r\n"
            f"Content-Type: application/json\r\n"
            f"Connection: close\r\n\r\n"
        ).encode()
        writer.write(request_line + headers + body_bytes)
        await writer.drain()

        while True:
            line = await reader.readuntil(b"\r\n")
            if line == b"\r\n":
                break
        resp_body = b""
        while True:
            chunk = await reader.read(4096)
            if not chunk:
                break
            resp_body += chunk
        writer.close()
        await writer.wait_closed()
        return json.loads(resp_body) if resp_body else {}
    except Exception as e:
        logger.error("Proxy HTTP request to %s:%d%s failed: %s",
                     host, port, path, e)
        return {"error": str(e)}


def _get_node_id(host: str, port: int) -> str:
    """Generate a node identifier from host and port."""
    return f"{host}:{port}"


def _get_decode_client_by_node_id(
    app, node_id: str,
) -> dict | None:
    """Find decoder client info by node ID (host:port)."""
    for cinfo in app.state.decode_clients:
        cid = _get_node_id(cinfo["host"], cinfo["port"])
        if cid == node_id:
            return cinfo
    return None


async def _migrate_request(
    app, request_id: str, source_node_id: str, hop_count: int = 0,
) -> ProxyMigrationState | None:
    """
    Orchestrate migration of a single request from source to a healthy target.
    Returns ProxyMigrationState on success, None on failure.
    """
    # Find source node info
    source_client = _get_decode_client_by_node_id(app, source_node_id)
    if source_client is None:
        logger.error("Source node %s not found in decode clients", source_node_id)
        return None

    # Build target candidates: all decode nodes except source and draining
    target_candidates = []
    for cinfo in app.state.decode_clients:
        nid = _get_node_id(cinfo["host"], cinfo["port"])
        if nid != source_node_id and nid not in app.state.draining_nodes:
            target_candidates.append((nid, cinfo))

    if not target_candidates:
        logger.error("No available target nodes for migration from %s", source_node_id)
        return None

    # Pick target (first candidate = round-robin-like)
    target_node_id, target_client = target_candidates[0]
    target_host = target_client["host"]
    target_rpc_port = target_client["port"]
    target_migration_port = target_rpc_port + VLLM_MOONCAKE_MIGRATION_HTTP_PORT - 18900

    logger.info(
        "Migrating request %s from %s to %s (hop=%d)",
        request_id, source_node_id, target_node_id, hop_count,
    )

    # Get active requests from source to find block_ids
    active_resp = await _proxy_http_request(
        source_client["host"],
        source_client["port"] + VLLM_MOONCAKE_MIGRATION_HTTP_PORT - 18900,
        "GET", f"/api/v1/active_requests",
    )
    request_ids = active_resp.get("request_ids", [])
    if request_id not in request_ids:
        logger.warning(
            "Request %s not active on source %s, skipping migration",
            request_id, source_node_id,
        )
        return None

    # Step 1: prepare migration on target (pre-allocate blocks)
    source_host = source_client["host"]
    source_migration_port = source_client["port"] + VLLM_MOONCAKE_MIGRATION_HTTP_PORT - 18900

    prep_resp = await _proxy_http_request(
        target_host, target_migration_port,
        "POST", "/api/v1/prepare_migration",
        {
            "request_id": request_id,
            "num_blocks": 256,
            "extra_blocks": 128,
        },
        timeout=30.0,
    )
    if "error" in prep_resp:
        logger.error("Prepare migration failed on target %s: %s",
                     target_node_id, prep_resp.get("error"))
        return None

    target_block_ids = prep_resp.get("target_block_ids", [])
    extra_target_block_ids = prep_resp.get("extra_target_block_ids", [])
    target_base_addr = prep_resp.get("kv_caches_base_addr", [])

    # Build block_id_map: source_block_id -> target_block_id
    # We use sequential block IDs starting from target_block_ids
    block_id_map = {}
    for i, tb_id in enumerate(target_block_ids):
        block_id_map[str(i)] = tb_id

    # Step 2: start migration on source
    start_resp = await _proxy_http_request(
        source_host, source_migration_port,
        "POST", "/api/v1/start_migration",
        {
            "request_id": request_id,
            "target_host": target_host,
            "target_port": target_rpc_port,
            "migration_port": target_migration_port,
            "target_base_addr": target_base_addr,
            "block_id_map": block_id_map,
            "extra_target_block_ids": extra_target_block_ids,
        },
        timeout=10.0,
    )
    if "error" in start_resp:
        logger.error("Start migration failed on source %s: %s",
                     source_node_id, start_resp.get("error"))
        return None

    # Record migration state on proxy
    state = ProxyMigrationState(
        request_id=request_id,
        source_node_id=source_node_id,
        source_host=source_host,
        source_migration_port=source_migration_port,
        target_node_id=target_node_id,
        target_host=target_host,
        target_migration_port=target_migration_port,
        target_rpc_port=target_rpc_port,
        target_base_addr=target_base_addr,
        block_id_map=block_id_map,
        started_at=time.time(),
        hop_count=hop_count,
    )
    app.state.migrating_reqs[request_id] = state

    logger.info(
        "Migration started for request %s: %s -> %s",
        request_id, source_node_id, target_node_id,
    )
    return state


async def _check_migration_completed(
    app, request_id: str, timeout: float = 300.0,
) -> bool:
    """Poll migration status until COMPLETED or timeout."""
    state = app.state.migrating_reqs.get(request_id)
    if state is None:
        return False

    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = await _proxy_http_request(
            state.source_host, state.source_migration_port,
            "GET", f"/api/v1/migration/status/{request_id}",
        )
        phase = resp.get("phase", "UNKNOWN")
        if phase == "COMPLETED":
            return True
        if phase == "IDLE":
            logger.warning("Migration for %s entered IDLE state (failed)", request_id)
            return False

        # Check if target is now draining (cascade trigger)
        if state.target_node_id in app.state.draining_nodes:
            logger.warning(
                "Target %s is now draining during migration of %s",
                state.target_node_id, request_id,
            )
            return False

        await asyncio.sleep(2.0)

    logger.warning("Migration poll timeout for %s after %.0fs", request_id, timeout)
    return False


# ──── Migration API Endpoints ────


@app.post("/api/v1/drain")
async def handle_drain(request: Request):
    """
    Drain a decode node: migrate all its active requests to other nodes.
    Called by K8s pre-stop hook or operator.
    """
    try:
        body = await request.json()
    except Exception:
        return {"status": "error", "message": "invalid JSON body"}

    node_id = body.get("node_id", "")
    timeout = body.get("timeout", VLLM_MOONCAKE_MIGRATION_TIMEOUT)

    if not node_id:
        return {"status": "error", "message": "node_id required"}

    source_client = _get_decode_client_by_node_id(request.app, node_id)
    if source_client is None:
        return {"status": "error", "message": f"node {node_id} not found"}

    # Mark the node as draining
    request.app.state.draining_nodes.add(node_id)

    # Get active requests from the source node
    source_host = source_client["host"]
    source_migration_port = source_client["port"] + VLLM_MOONCAKE_MIGRATION_HTTP_PORT - 18900
    active_resp = await _proxy_http_request(
        source_host, source_migration_port,
        "GET", "/api/v1/active_requests",
    )
    active_request_ids = active_resp.get("request_ids", [])

    logger.info(
        "Draining node %s: %d active requests",
        node_id, len(active_request_ids),
    )

    # Migrate each active request
    migrated_count = 0
    failed_count = 0
    for req_id in list(active_request_ids):
        state = await _migrate_request(
            request.app, req_id, node_id, hop_count=0,
        )
        if state is not None:
            completed = await _check_migration_completed(
                request.app, req_id, timeout=timeout,
            )
            if completed:
                migrated_count += 1
            else:
                failed_count += 1
                logger.error("Migration failed for request %s", req_id)
        else:
            failed_count += 1

    # Remove from draining set (migration is done)
    # Note: pod termination is handled by the operator/caller
    request.app.state.draining_nodes.discard(node_id)

    return {
        "status": "completed",
        "node_id": node_id,
        "migrated_count": migrated_count,
        "failed_count": failed_count,
        "request_ids": active_request_ids,
    }


@app.get("/api/v1/migration/status")
async def handle_migration_status(request: Request):
    """Return status of all migrations."""
    migrations = []
    for req_id, state in request.app.state.migrating_reqs.items():
        # Query current phase from the source node
        phase = "UNKNOWN"
        resp = await _proxy_http_request(
            state.source_host, state.source_migration_port,
            "GET", f"/api/v1/migration/status/{req_id}",
        )
        phase = resp.get("phase", "UNKNOWN")

        migrations.append({
            "request_id": req_id,
            "phase": phase,
            "source": state.source_node_id,
            "target": state.target_node_id,
            "hop_count": state.hop_count,
            "elapsed_sec": round(time.time() - state.started_at, 1),
        })

    return {"migrations": migrations}


@app.post("/api/v1/remigrate")
async def handle_remigrate(request: Request):
    """
    Re-migrate a request from its current target to a new target.
    Used for cascade re-migration when a target node becomes draining.
    """
    try:
        body = await request.json()
    except Exception:
        return {"status": "error", "message": "invalid JSON body"}

    request_id = body.get("request_id", "")
    new_target_node_id = body.get("new_target_node_id", "")

    if not request_id:
        return {"status": "error", "message": "request_id required"}

    state = request.app.state.migrating_reqs.get(request_id)
    if state is None:
        return {"status": "error", "message": f"no migration state for {request_id}"}

    if state.hop_count >= VLLM_MOONCAKE_MAX_REMIGRATION_HOPS:
        return {
            "status": "error",
            "message": f"max re-migration hops ({VLLM_MOONCAKE_MAX_REMIGRATION_HOPS}) exceeded",
        }

    # The current target becomes the new source
    new_source_node_id = state.target_node_id

    logger.info(
        "Re-migrating request %s from %s (hop %d -> %d)",
        request_id, new_source_node_id,
        state.hop_count, state.hop_count + 1,
    )

    new_state = await _migrate_request(
        request.app, request_id, new_source_node_id,
        hop_count=state.hop_count + 1,
    )

    if new_state is None:
        return {"status": "error", "message": "re-migration failed"}

    return {
        "status": "re-migrating",
        "request_id": request_id,
        "source": new_source_node_id,
        "target": new_state.target_node_id,
        "hop_count": new_state.hop_count,
    }


@app.get("/healthcheck/migration")
async def healthcheck_migration():
    """Migration-specific health endpoint."""
    return {
        "status": "ok",
        "draining_nodes": list(app.state.draining_nodes),
        "active_migrations": len(app.state.migrating_reqs),
    }
    global global_args
    global_args = parse_args()

    import uvicorn

    uvicorn.run(app, host=global_args.host, port=global_args.port)

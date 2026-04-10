# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the vLLM project
# Adapted from the vLLM repository's toy_proxy_server.py in tests/v1/kv_connector/nixl_integration/.

import argparse
import itertools
import logging
import os
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class CacheAwareRouter():
    def __init__(self, address, endpoint, block_size):
        self.address = address
        self.endpoint = endpoint
        self.client = httpx.AsyncClient(timeout=None, base_url=f'http://{address}')
        self.block_size = block_size

    async def get_best_prefiller(self, token_ids: list, req_data):
        # call conductor restful api to get cache hit status
        model_name = req_data.get("model", "deepseek")
        request_data = {
            "token_ids": token_ids,
            "model": model_name,
            "block_size": self.block_size,
        }
        headers = {
            "Content-Type": "application/json",
        }
        logger.debug(f"conductor request_data: {request_data}")
        response = await self.client.post(self.endpoint, json=request_data, headers=headers)
        response.raise_for_status()
        return response.json()["HitStatus"]

    async def close(self) -> None:
        if self.client:
            await self.client.aclose()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager to handle startup and shutdown events.
    """
    # Startup: Initialize client pools for prefiller and decoder services
    app.state.prefill_clients = []
    app.state.decode_clients = []

    # Create prefill clients
    for i, endpoint in enumerate(global_args.prefiller_endpoints):
        prefiller_base_url = f'http://{endpoint}'
        app.state.prefill_clients.append({
            'client':
            httpx.AsyncClient(timeout=None, base_url=prefiller_base_url),
            'endpoint':
            endpoint,
            'name':
            global_args.prefill_names[i],
            'id':
            i
        })

    # Create decode clients
    for i, endpoint in enumerate(global_args.decoder_endpoints):
        decoder_base_url = f'http://{endpoint}'
        app.state.decode_clients.append({
            'client':
            httpx.AsyncClient(timeout=None, base_url=decoder_base_url),
            'endpoint':
            endpoint,
            'id':
            i
        })

    # Create conductor client
    app.state.conductor_client = CacheAwareRouter(global_args.conductor_address, "/query", global_args.block_size)

    # Initialize round-robin iterators
    app.state.prefill_iterator = itertools.cycle(
        range(len(app.state.prefill_clients)))
    app.state.decode_iterator = itertools.cycle(
        range(len(app.state.decode_clients)))

    logger.info(f"Initialized {len(app.state.prefill_clients)} prefill clients "
          f"and {len(app.state.decode_clients)} decode clients.")

    yield

    # Shutdown: Close all clients
    for client_info in app.state.prefill_clients:
        await client_info['client'].aclose()

    for client_info in app.state.decode_clients:
        await client_info['client'].aclose()

    # Close conductor client
    await app.state.conductor_client.close()


# Update FastAPI app initialization to use lifespan
app = FastAPI(lifespan=lifespan)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--host", type=str, default="localhost")

    # For prefiller instances (format: host:port)
    parser.add_argument("--prefiller-endpoints",
                        "--prefiller-endpoint",
                        type=str,
                        nargs="+",
                        default=["localhost:8100"],
                        help="Prefiller instances in host:port format")
    parser.add_argument("--prefill-names",
                        "--prefill-name",
                        type=str,
                        nargs="+",
                        default=None,
                        help="Names for prefill instances")

    # For decoder instances (format: host:port)
    parser.add_argument("--decoder-endpoints",
                        "--decoder-endpoint",
                        type=str,
                        nargs="+",
                        default=["localhost:8200"],
                        help="Decoder endpoints in host:port format")
    
    parser.add_argument("--conductor-address", type=str, default="127.0.0.1:13333")
    parser.add_argument("--block-size", type=int, default=128)

    args = parser.parse_args()

    # Parse prefill instances (host:port format)
    for instance_str in args.prefiller_endpoints:
        if ':' not in instance_str:
            raise ValueError(
                f"Prefiller endpoint '{instance_str}' must be in host:port format")
        _, port_str = instance_str.rsplit(':', 1)
        try:
            port = int(port_str)
        except ValueError:
            raise ValueError(
                f"Invalid port in prefiller endpoint '{instance_str}'")

    # Validate and pair prefill names
    if args.prefill_names is None:
        args.prefill_names = [None] * len(args.prefiller_endpoints)
    elif len(args.prefill_names) != len(args.prefiller_endpoints):
        raise ValueError(
            "Number of prefill names must match number of prefiller endpoints")

    # Parse decode instances (host:port format)
    for instance_str in args.decoder_endpoints:
        if ':' not in instance_str:
            raise ValueError(
                f"Decoder endpoint '{instance_str}' must be in host:port format")
        _, port_str = instance_str.rsplit(':', 1)
        try:
            port = int(port_str)
        except ValueError:
            raise ValueError(
                f"Invalid port in decoder endpoint '{instance_str}'")

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
    if service_type == 'prefill':
        client_idx = next(app.state.prefill_iterator)
        return app.state.prefill_clients[client_idx]
    elif service_type == 'decode':
        client_idx = next(app.state.decode_iterator)
        return app.state.decode_clients[client_idx]
    else:
        raise ValueError(f"Unknown service type: {service_type}")


async def get_best_prefiller(app, token_ids: list, round_robin_prefill, req_data):
    index_map = {}
    for index, client_info in enumerate(app.state.prefill_clients):
        if client_info['client'].is_closed:
            continue
        index_map[client_info['name']] = index
    
    cache_hit_status = await app.state.conductor_client.get_best_prefiller(token_ids, req_data)

    if not cache_hit_status:
        return round_robin_prefill
    
    # The current demo is only suitable for quick verification, 
    # so it does not consider the scenario where an instance runs multiple DPs
    max_hit_value = -1
    best_prefiller_index = None
    instances = cache_hit_status.get("default", {})
    for instance_name, hit_value in instances.items():
        if hit_value["longest_matched"] > max_hit_value:
            max_hit_value = hit_value["longest_matched"]
            best_prefiller_index = index_map[instance_name]
    
    return app.state.prefill_clients[best_prefiller_index]


async def get_tokenid(client_info: dict, req_data: dict, request_id: str):
    req_data = req_data.copy()
    req_data["stream"] = False
    req_data["max_tokens"] = 1
    if "stream_options" in req_data:
        del req_data["stream_options"]
    headers = {
        "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY')}",
        "X-Request-Id": request_id
    }

    response = await client_info['client'].post("/tokenize",
                                                json=req_data,
                                                headers=headers)
    response.raise_for_status()
    token_id = response.json()["tokens"]
    return token_id


async def send_request_to_service(client_info: dict, endpoint: str,
                                  req_data: dict, request_id: str):
    """
    Send a request to a service using a client from the pool.
    """
    req_data = req_data.copy()
    req_data['kv_transfer_params'] = {
        "do_remote_decode": True,
        "do_remote_prefill": False,
        "remote_engine_id": None,
        "remote_block_ids": None,
        "remote_host": None,
        "remote_port": None
    }
    req_data["stream"] = False
    req_data["max_tokens"] = 1
    if "max_completion_tokens" in req_data:
        req_data["max_completion_tokens"] = 1
    if "stream_options" in req_data:
        del req_data["stream_options"]
    headers = {
        "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY')}",
        "X-Request-Id": request_id
    }
    logger.debug(f"req_data: {req_data}")

    response = await client_info['client'].post(endpoint,
                                                json=req_data,
                                                headers=headers)
    response.raise_for_status()

    return response


async def stream_service_response(client_info: dict, endpoint: str,
                                  req_data: dict, request_id: str):
    """
    Asynchronously stream response from a service using a client from the pool.
    """
    headers = {
        "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY')}",
        "X-Request-Id": request_id
    }

    async with client_info['client'].stream("POST",
                                            endpoint,
                                            json=req_data,
                                            headers=headers) as response:
        response.raise_for_status()
        async for chunk in response.aiter_bytes():
            yield chunk


async def _handle_completions(api: str, request: Request):
    try:
        req_data = await request.json()
        request_id = str(uuid.uuid4())
        
        # select tokenizer client in round-robin fashion
        remote_tokenizer_client_info = get_next_client(request.app, 'prefill')
        token_ids = await get_tokenid(remote_tokenizer_client_info, req_data, request_id)
        logger.debug(f"Successfully get token_ids: {token_ids}")

        # choice best cache hit prefill instance
        prefill_client_info = await get_best_prefiller(request.app, token_ids, remote_tokenizer_client_info, req_data)
        response = await send_request_to_service(prefill_client_info, api,
                                                 req_data, request_id)

        # Extract the needed fields
        response_json = response.json()
        kv_transfer_params = response_json.get('kv_transfer_params', {})
        if kv_transfer_params:
            req_data["kv_transfer_params"] = kv_transfer_params

        # Get the next decode client in round-robin fashion
        decode_client_info = get_next_client(request.app, 'decode')

        logger.debug("Using %s %s", prefill_client_info, decode_client_info)

        # Stream response from decode service
        async def generate_stream():
            async for chunk in stream_service_response(decode_client_info,
                                                       api,
                                                       req_data,
                                                       request_id=request_id):
                yield chunk

        return StreamingResponse(generate_stream(),
                                 media_type="application/json")

    except Exception as e:
        import sys
        import traceback
        exc_info = sys.exc_info()
        logger.error("Error occurred in disagg prefill proxy server"
              f" - {api} endpoint")
        logger.error(e)
        logger.error("".join(traceback.format_exception(*exc_info)))
        raise


@app.post("/v1/completions")
async def handle_completions(request: Request):
    return await _handle_completions("/v1/completions", request)


@app.post("/v1/chat/completions")
async def handle_chat_completions(request: Request):
    return await _handle_completions("/v1/chat/completions", request)


@app.get("/healthcheck")
async def healthcheck():
    """Simple endpoint to check if the server is running."""
    return {
        "status": "ok",
        "prefill_instances": len(app.state.prefill_clients),
        "decode_instances": len(app.state.decode_clients)
    }


if __name__ == '__main__':
    global global_args
    global_args = parse_args()

    import uvicorn
    uvicorn.run(app, host=global_args.host, port=global_args.port)
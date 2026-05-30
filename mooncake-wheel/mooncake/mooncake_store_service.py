#!/usr/bin/env python3
# mooncake_store_service.py - Integrated Mooncake store service with REST API

import argparse
import asyncio
import json
import logging
import time

from aiohttp import web
from mooncake.store import MooncakeDistributedStore
from mooncake.mooncake_config import MooncakeConfig


def _timed_handler(operation_name, handler):
    async def wrapper(request):
        start_time = time.perf_counter()
        try:
            return await handler(request)
        finally:
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            logging.info(f"{operation_name} operation completed in {elapsed_ms:.2f} ms")
    return wrapper


def _shm_name_to_path(name):
    if not isinstance(name, str) or not name:
        return None
    normalized = name[1:] if name.startswith("/") else name
    if not normalized or "/" in normalized or normalized in {".", ".."}:
        return None
    return f"/dev/shm/{normalized}"


class MooncakeStoreService:
    """
    Mooncake Store Service with REST API.

    Configuration Example (JSON format):
    {
        "local_hostname": "localhost",
        "metadata_server": "localhost:8080",
        "global_segment_size": 3355443200,
        "local_buffer_size": 1073741824,
        "protocol": "tcp",
        "device_name": "",
        "master_server_address": "localhost:8081"
    }

    Explanation of Key Fields:
    - local_hostname: Hostname for the local service.
    - metadata_server: The address of the metadata server.
    - global_segment_size: Size of each global segment in bytes. 0 means do not set up store.
    - local_buffer_size: Size of the local buffer in bytes.
    - protocol: Communication protocol (tcp or rdma).
    - device_name: The name of the device to use.
    - master_server_address: The address of the master server.
    """

    def __init__(self, config_path: str = None, cli_config: dict = None):
        self.store = None
        self.config = None
        self._setup_logging()

        # State for /api/reconfigure (Prefill/Decode mode switch)
        self.current_mode = "prefill"          # "prefill" or "decode"
        self.mounted_segment_ids = []          # persisted segment_ids from last decode mount
        self.last_mount_info = {}              # last mount parameters for debugging
        self._state_lock = asyncio.Lock()      # serialize reconfigure/mount/unmount state changes

        try:
            if config_path:
                self.config = MooncakeConfig.from_file(config_path)
            else:
                self.config = MooncakeConfig.load_from_env()

            # Override with CLI config if provided
            if cli_config:
                for key, value in cli_config.items():
                    if hasattr(self.config, key):
                        setattr(self.config, key, value)

            logging.info("Mooncake configuration loaded")
        except Exception as e:
            logging.error("Configuration load failed: %s", e)
            raise

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    async def start_store_service(self, max_wait_time: float = 60):
        """
        Start the store service with retry mechanism.

        Args:
            max_wait_time: Maximum total wait time in seconds (default: 60)

        Returns:
            True if successful, False otherwise
        """
        retry_count = 0
        retry_interval = 1.0  # Fixed retry interval: 1 second
        start_time = time.perf_counter()

        while True:
            elapsed = time.perf_counter() - start_time
            if elapsed >= max_wait_time:
                logging.error(
                    f"Store startup failed: exceeded max wait time of {max_wait_time}s after {retry_count} attempts"
                )
                return False

            try:
                retry_count += 1
                logging.info(
                    f"Attempting to start store service (attempt {retry_count}, "
                    f"elapsed: {elapsed:.1f}s/{max_wait_time}s)"
                )

                self.store = MooncakeDistributedStore()
                ret = self.store.setup(
                    self.config.local_hostname,
                    self.config.metadata_server,
                    self.config.global_segment_size,
                    self.config.local_buffer_size,
                    self.config.protocol,
                    self.config.device_name,
                    self.config.master_server_address
                )

                if ret != 0:
                    raise RuntimeError("Store initialization failed")

                logging.info(f"Store service started successfully on {self.config.local_hostname}")
                return True

            except Exception as e:
                # Recalculate remaining time after store.setup() attempt
                elapsed_after_attempt = time.perf_counter() - start_time
                remaining_time = max_wait_time - elapsed_after_attempt

                # Calculate actual sleep duration
                actual_sleep_time = min(retry_interval, remaining_time) if remaining_time > 0 else 0

                logging.warning(
                    f"Store startup failed (attempt {retry_count}): {e}. "
                    f"Retrying in {actual_sleep_time:.1f}s... (remaining time: {remaining_time:.1f}s)"
                )

                # Wait before retry, but don't exceed max_wait_time
                if actual_sleep_time > 0:
                    await asyncio.sleep(actual_sleep_time)


    async def start_http_service(self, port: int = 8080):
        app = web.Application(client_max_size=1024 * 1024 * 100)  # 100MB limit
        app.add_routes([
            web.post('/api/reconfigure', _timed_handler("RECONFIGURE", self.handle_reconfigure)),
            web.post('/api/mount_shm', _timed_handler("MOUNT_SHM", self.handle_mount_shm)),
            web.post('/api/unmount_shm', _timed_handler("UNMOUNT_SHM", self.handle_unmount_shm)),
            web.post('/api/mount', _timed_handler("MOUNT", self.handle_mount)),
            web.post('/api/unmount', _timed_handler("UNMOUNT", self.handle_unmount)),
            web.put('/api/put', _timed_handler("PUT", self.handle_put)),
            web.get('/api/get/{key}', _timed_handler("GET", self.handle_get)),
            web.get('/api/exist/{key}', _timed_handler("EXIST", self.handle_exist)),
            web.delete('/api/remove/{key}', _timed_handler("REMOVE", self.handle_remove)),
            web.delete('/api/remove_all', _timed_handler("REMOVE_ALL", self.handle_remove_all))
        ])
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        logging.info(f"REST API started on port {port}")
        return True

    # REST API handlers
    async def handle_reconfigure(self, request):
        try:
            data = await request.json()
            mode = data.get("mode", "").lower()

            if mode == "decode":
                path = data.get("path")
                size = data.get("size")
                offset = data.get("offset", 0)
                protocol = data.get("protocol", self.config.protocol)
                location = data.get("location", "")

                if not path or size is None:
                    return web.Response(
                        status=400,
                        text=json.dumps({"error": "Missing path or size for decode mode"}),
                        content_type="application/json"
                    )

                async with self._state_lock:
                    # If already in decode mode with mounted segments, unmount them first
                    if self.mounted_segment_ids:
                        logging.info("Reconfigure decode: unmounting previous segments before remount")
                        ret = self.store.unmount_segment(self.mounted_segment_ids)
                        if ret != 0:
                            return web.Response(
                                status=500,
                                text=json.dumps({"error": f"Unmount of previous segments failed, ret={ret}"}),
                                content_type="application/json"
                            )
                        self.mounted_segment_ids.clear()

                    result = self.store.mount_segment(path, size, offset, protocol, location)
                    if result["ret"] != 0:
                        self.current_mode = "prefill"
                        self.mounted_segment_ids.clear()
                        self.last_mount_info.clear()
                        return web.Response(
                            status=500,
                            text=json.dumps(
                                {
                                    "error": (
                                        f"Mount failed, ret={result['ret']}; "
                                        "rolled back to prefill"
                                    ),
                                    "mode": self.current_mode,
                                }
                            ),
                            content_type="application/json"
                        )

                    self.mounted_segment_ids = list(result["segment_ids"])
                    self.current_mode = "decode"
                    self.last_mount_info = {
                        "path": path, "offset": offset, "size": size,
                        "protocol": protocol, "location": location
                    }

                return web.Response(
                    status=200,
                    text=json.dumps({
                        "status": "success",
                        "mode": self.current_mode,
                        "segment_ids": self.mounted_segment_ids,
                    }),
                    content_type="application/json"
                )

            elif mode == "prefill":
                async with self._state_lock:
                    if self.mounted_segment_ids:
                        ret = self.store.unmount_segment(self.mounted_segment_ids)
                        if ret != 0:
                            return web.Response(
                                status=500,
                                text=json.dumps({"error": f"Unmount failed, ret={ret}"}),
                                content_type="application/json"
                            )
                        self.mounted_segment_ids.clear()

                    self.current_mode = "prefill"
                    self.last_mount_info.clear()

                return web.Response(
                    status=200,
                    text=json.dumps({"status": "success", "mode": self.current_mode}),
                    content_type="application/json"
                )

            else:
                return web.Response(
                    status=400,
                    text=json.dumps({"error": "Invalid mode. Use 'decode' or 'prefill'"}),
                    content_type="application/json"
                )
        except Exception as e:
            logging.error("RECONFIGURE error: %s", e)
            return web.Response(
                status=500,
                text=json.dumps({"error": str(e)}),
                content_type="application/json"
            )

    async def handle_mount_shm(self, request):
        try:
            data = await request.json()
            name = data.get("name")
            path = _shm_name_to_path(name)
            offset = data.get("offset", 0)
            size = data.get("size")
            protocol = data.get("protocol", self.config.protocol)
            location = data.get("location", "")

            if not path or size is None:
                return web.Response(
                    status=400,
                    text=json.dumps({"error": "Missing or invalid name or size"}),
                    content_type="application/json"
                )

            result = self.store.mount_segment(path, size, offset, protocol, location)
            if result["ret"] != 0:
                return web.Response(
                    status=500,
                    text=json.dumps({"error": f"Mount failed, ret={result['ret']}"}),
                    content_type="application/json"
                )

            return web.Response(
                status=200,
                text=json.dumps(
                    {
                        "status": "success",
                        "segment_ids": list(result["segment_ids"]),
                    }
                ),
                content_type="application/json",
            )
        except Exception as e:
            logging.error("MOUNT_SHM error: %s", e)
            return web.Response(
                status=500,
                text=json.dumps({"error": str(e)}),
                content_type="application/json"
            )

    async def handle_unmount_shm(self, request):
        try:
            data = await request.json()
            segment_ids = data.get("segment_ids", [])
            if isinstance(segment_ids, str):
                segment_ids = [segment_ids]
            if not segment_ids:
                return web.Response(
                    status=400,
                    text=json.dumps({"error": "Missing segment_ids"}),
                    content_type="application/json",
                )

            grace_period_seconds = data.get("grace_period_seconds", 0)
            failed_segment_ids = []
            async with self._state_lock:
                for sid in segment_ids:
                    ret = self.store.unmount_segment([sid], grace_period_seconds)
                    if ret != 0:
                        failed_segment_ids.append(sid)
                        continue
                    if sid in self.mounted_segment_ids:
                        self.mounted_segment_ids.remove(sid)
                if not self.mounted_segment_ids:
                    self.current_mode = "prefill"

            if failed_segment_ids:
                return web.Response(
                    status=500,
                    text=json.dumps(
                        {
                            "error": "Unmount failed for one or more segments",
                            "failed_segment_ids": failed_segment_ids,
                        }
                    ),
                    content_type="application/json",
                )

            return web.Response(
                status=200,
                text=json.dumps({"status": "success"}),
                content_type="application/json",
            )
        except Exception as e:
            logging.error("UNMOUNT_SHM error: %s", e)
            return web.Response(
                status=500,
                text=json.dumps({"error": str(e)}),
                content_type="application/json"
            )

    async def handle_mount(self, request):
        try:
            data = await request.json()
            size = data.get("size")
            protocol = data.get("protocol", self.config.protocol)
            location = data.get("location", "")

            if type(size) is not int or size <= 0:
                return web.Response(
                    status=400,
                    text=json.dumps({"error": "Invalid size, must be a positive integer"}),
                    content_type="application/json"
                )

            result = self.store.allocate_and_mount_segment(size, protocol, location)
            if result["ret"] != 0:
                return web.Response(
                    status=500,
                    text=json.dumps({"error": f"Allocate and mount failed, ret={result['ret']}"}),
                    content_type="application/json"
                )

            return web.Response(
                status=200,
                text=json.dumps(
                    {
                        "status": "success",
                        "segment_ids": list(result["segment_ids"]),
                        "allocated_size": result["allocated_size"],
                    }
                ),
                content_type="application/json",
            )
        except Exception as e:
            logging.error("MOUNT error: %s", e)
            return web.Response(
                status=500,
                text=json.dumps({"error": str(e)}),
                content_type="application/json"
            )

    async def handle_unmount(self, request):
        try:
            data = await request.json()
            segment_ids = data.get("segment_ids", [])
            if isinstance(segment_ids, str):
                segment_ids = [segment_ids]
            if not segment_ids:
                return web.Response(
                    status=400,
                    text=json.dumps({"error": "Missing segment_ids"}),
                    content_type="application/json",
                )

            grace_period_seconds = data.get("grace_period_seconds", 0)
            ret = self.store.unmount_and_free_segment(
                segment_ids, grace_period_seconds
            )
            if ret != 0:
                return web.Response(
                    status=500,
                    text=json.dumps(
                        {"error": f"Unmount and free failed, ret={ret}"}
                    ),
                    content_type="application/json",
                )

            return web.Response(
                status=200,
                text=json.dumps({"status": "success"}),
                content_type="application/json",
            )
        except Exception as e:
            logging.error("UNMOUNT error: %s", e)
            return web.Response(
                status=500,
                text=json.dumps({"error": str(e)}),
                content_type="application/json"
            )

    async def handle_put(self, request):
        try:
            data = await request.json()
            key = data.get('key')
            value = data.get('value').encode()

            if not key or not value:
                return web.Response(
                    status=400,
                    text=json.dumps({'error': 'Missing key or value'}),
                    content_type='application/json'
                )

            ret = self.store.put(key, value)
            if ret != 0:
                return web.Response(
                    status=500,
                    text=json.dumps({'error': 'PUT operation failed'}),
                    content_type='application/json'
                )

            return web.Response(
                status=200,
                text=json.dumps({'status': 'success'}),
                content_type='application/json'
            )
        except Exception as e:
            logging.error("PUT error: %s", e)
            return web.Response(
                status=500,
                text=json.dumps({'error': str(e)}),
                content_type='application/json'
            )

    async def handle_get(self, request):
        try:
            key = request.match_info['key']
            value = self.store.get(key)

            if not value:
                return web.Response(
                    status=404,
                    text=json.dumps({'error': 'Key not found'}),
                    content_type='application/json'
                )

            return web.Response(
                status=200,
                body=value,
                content_type='application/octet-stream'
            )
        except Exception as e:
            logging.error("GET error: %s", e)
            return web.Response(
                status=500,
                text=json.dumps({'error': str(e)}),
                content_type='application/json'
            )

    async def handle_exist(self, request):
        try:
            key = request.match_info['key']
            exists = self.store.is_exist(key)

            return web.Response(
                status=200,
                text=json.dumps({'exists': bool(exists)}),
                content_type='application/json'
            )
        except Exception as e:
            logging.error("EXIST error: %s", e)
            return web.Response(
                status=500,
                text=json.dumps({'error': str(e)}),
                content_type='application/json'
            )

    async def handle_remove(self, request):
        try:
            key = request.match_info['key']
            ret = self.store.remove(key)

            if ret != 0:
                return web.Response(
                    status=500,
                    text=json.dumps({'error': 'Remove operation failed'}),
                    content_type='application/json'
                )

            return web.Response(
                status=200,
                text=json.dumps({'status': 'success'}),
                content_type='application/json'
            )
        except Exception as e:
            logging.error("REMOVE error: %s", e)
            return web.Response(
                status=500,
                text=json.dumps({'error': str(e)}),
                content_type='application/json'
            )

    async def handle_remove_all(self, request):
        try:
            ret = self.store.remove_all()

            if ret < 0:
                return web.Response(
                    status=500,
                    text=json.dumps({'error': 'RemoveAll operation failed'}),
                    content_type='application/json'
                )

            return web.Response(
                status=200,
                text=json.dumps({'status': 'success removed ' + str(ret) + ' keys'}),
                content_type='application/json'
            )
        except Exception as e:
            logging.error("REMOVE_ALL error: %s", e)
            return web.Response(
                status=500,
                text=json.dumps({'error': str(e)}),
                content_type='application/json'
            )

    async def stop(self):
        if self.store:
            self.store.close()
            logging.info("Mooncake service stopped")

def parse_arguments():
    parser = argparse.ArgumentParser(description='Mooncake Store Service with REST API')
    parser.add_argument('--config', type=str,
                        help='Path to Mooncake config file',
                        required=False)
    parser.add_argument('-D', '--define', action='append',
                        help='Override configuration with key=value pairs (e.g., -Dlocal_hostname=example.com)',
                        default=[])
    parser.add_argument('--port', type=int,
                        help='HTTP API port (default: 8080)',
                        default=8080,
                        required=False)
    parser.add_argument('--max-wait-time', type=float,
                        help='Maximum total wait time in seconds (default: 60)',
                        default=60,
                        required=False)
    return parser.parse_args()

async def main():
    args = parse_arguments()

    # Parse -D key=value pairs into a dictionary
    cli_config = {}
    for item in args.define:
        if '=' in item:
            key, value = item.split('=', 1)
            cli_config[key] = value
        else:
            logging.warning(f"Ignoring invalid CLI config: {item}")

    service = MooncakeStoreService(args.config, cli_config)

    try:
        if not await service.start_store_service(max_wait_time=args.max_wait_time):
            raise RuntimeError("Failed to start store service")

        if not await service.start_http_service(args.port):
            raise RuntimeError("Failed to start HTTP service")

        logging.info("Mooncake Store Service is running. Press Ctrl+C to stop.")
        while True:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        logging.info("Received shutdown signal")
        await service.stop()
    except Exception as e:
        logging.error("Service error: %s", e)
        await service.stop()
        raise

if __name__ == "__main__":
    asyncio.run(main())

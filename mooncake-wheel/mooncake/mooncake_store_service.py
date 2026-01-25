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
                remaining_time = max_wait_time - elapsed
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
                logging.warning(
                    f"Store startup failed (attempt {retry_count}): {e}. "
                    f"Retrying in {retry_interval}s... (remaining time: {remaining_time:.1f}s)"
                )

                # Wait before retry, but don't exceed max_wait_time
                await asyncio.sleep(min(retry_interval, remaining_time))


    async def start_http_service(self, port: int = 8080):
        app = web.Application(client_max_size=1024 * 1024 * 100)  # 100MB limit
        app.add_routes([
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

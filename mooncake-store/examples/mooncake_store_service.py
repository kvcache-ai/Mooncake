#!/usr/bin/env python3
# mooncake_store_service.py - Integrated Mooncake store service with REST API

import argparse
import asyncio
import json
import logging
import os
from aiohttp import web
from mooncake.store import MooncakeDistributedStore

class MooncakeStoreConfig:
    def __init__(self, local_hostname: str, metadata_server: str,
                 global_segment_size: int = 3355443200,
                 local_buffer_size: int = 1073741824,
                 protocol: str = "tcp",
                 device_name: str = "",
                 master_server_address: str = ""):
        self.local_hostname = local_hostname
        self.metadata_server = metadata_server
        self.global_segment_size = global_segment_size
        self.local_buffer_size = local_buffer_size
        self.protocol = protocol
        self.device_name = device_name
        self.master_server_address = master_server_address

    @staticmethod
    def from_file(file_path: str) -> 'MooncakeStoreConfig':
        """Load config from JSON file"""
        with open(file_path) as f:
            config = json.load(f)
            return MooncakeStoreConfig(
                local_hostname=config.get("local_hostname"),
                metadata_server=config.get("metadata_server"),
                global_segment_size=config.get("global_segment_size", 3355443200),
                local_buffer_size=config.get("local_buffer_size", 1073741824),
                protocol=config.get("protocol", "tcp"),
                device_name=config.get("device_name", ""),
                master_server_address=config.get("master_server_address", "")
            )

    @staticmethod
    def load_from_env() -> 'MooncakeStoreConfig':
        """Load config from environment variable"""
        config_file = os.getenv('MOONCAKE_CONFIG_PATH')
        if not config_file:
            raise ValueError("MOONCAKE_CONFIG_PATH environment variable not set")
        return MooncakeStoreConfig.from_file(config_file)

class MooncakeService:
    def __init__(self, config_path: str = None):
        self.store = None
        self.config = None
        self._setup_logging()

        try:
            if config_path:
                self.config = MooncakeStoreConfig.from_file(config_path)
            else:
                self.config = MooncakeStoreConfig.load_from_env()
            logging.info("Mooncake configuration loaded")
        except Exception as e:
            logging.error("Configuration load failed: %s", e)
            raise

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    async def start_store_service(self):
        try:
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
            logging.info(f"Store service started on {self.config.local_hostname}")
            return True
        except Exception as e:
            logging.error("Store startup failed: %s", e)
            return False

    async def start_http_service(self, port: int = 8080):
        app = web.Application()
        app.add_routes([
            web.put('/api/put', self.handle_put),
            web.get('/api/get/{key}', self.handle_get),
            web.get('/api/exist/{key}', self.handle_exist),
            web.delete('/api/remove/{key}', self.handle_remove),
            web.delete('/api/remove_all', self.handle_remove_all)
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

            if ret != 0:
                return web.Response(
                    status=500,
                    text=json.dumps({'error': 'RemoveAll operation failed'}),
                    content_type='application/json'
                )

            return web.Response(
                status=200,
                text=json.dumps({'status': 'success'}),
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
    parser.add_argument('--port', type=int,
                        help='HTTP API port (default: 8080)',
                        default=8080,
                        required=False)
    return parser.parse_args()

async def main():
    args = parse_arguments()
    service = MooncakeService(args.config)

    try:
        if not await service.start_store_service():
            raise RuntimeError("Failed to start store service")

        if not await service.start_http_service(args.port):
            raise RuntimeError("Failed to start HTTP service")

        while True:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        await service.stop()
    except Exception as e:
        logging.error("Service error: %s", e)
        await service.stop()

if __name__ == "__main__":
    asyncio.run(main())
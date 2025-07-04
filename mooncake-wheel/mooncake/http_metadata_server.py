#!/usr/bin/env python3
"""
HTTP Metadata Server for Mooncake.

This module provides a simple HTTP server for storing and retrieving metadata
used by Mooncake. It can be used as an alternative to etcd for metadata storage.
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
import threading
from enum import Enum
from time import sleep

from aiohttp import web


class KVPoll(Enum):
    """Status of the KV server."""
    Failed = 0
    Bootstrapping = 1
    WaitingForInput = 2
    Transferring = 3
    Success = 4


class KVBootstrapServer:
    """HTTP server for storing and retrieving metadata."""
    
    def __init__(self, port: int, host: str = "0.0.0.0"):
        """Initialize the server.
        
        Args:
            port: Port to listen on
            host: Host to bind to (default: 0.0.0.0)
        """
        self.port = port
        self.host = host
        self.app = web.Application()
        self.store = dict()
        self.lock = asyncio.Lock()
        self._loop = None
        self._runner = None
        self.thread = None
        self._setup_routes()
    
    def run(self):
        """Start the server in a background thread."""
        self.thread = threading.Thread(target=self._run_server, daemon=True)
        self.thread.start()
        logging.info(f"HTTP Metadata Server started on {self.host}:{self.port}")
        return self.thread

    def _setup_routes(self):
        """Set up the HTTP routes."""
        self.app.router.add_route('*', '/metadata', self._handle_metadata)

    async def _handle_metadata(self, request: web.Request):
        """Handle metadata requests."""
        key = request.query.get('key', '')
        
        if request.method == 'GET':
            return await self._handle_get(key)
        elif request.method == 'PUT':
            return await self._handle_put(key, request)
        elif request.method == 'DELETE':
            return await self._handle_delete(key)
        return web.Response(text='Method not allowed', status=405,
                          content_type='application/json')

    async def _handle_get(self, key):
        """Handle GET requests."""
        async with self.lock:
            value = self.store.get(key)
        if value is None:
            return web.Response(text='metadata not found', status=404,
                              content_type='application/json')
        return web.Response(body=value, status=200,
                          content_type='application/json')

    async def _handle_put(self, key, request):
        """Handle PUT requests."""
        data = await request.read()
        async with self.lock:
            self.store[key] = data
        return web.Response(text='metadata updated', status=200,
                          content_type='application/json')

    async def _handle_delete(self, key):
        """Handle DELETE requests."""
        async with self.lock:
            if key not in self.store:
                return web.Response(text='metadata not found', status=404,
                                  content_type='application/json')
            del self.store[key]
        return web.Response(text='metadata deleted', status=200,
                          content_type='application/json')
                          
    def _run_server(self):
        """Run the server in the current thread."""
        try:
            # Event Loop
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            
            self._runner = web.AppRunner(self.app)
            self._loop.run_until_complete(self._runner.setup())
            
            site = web.TCPSite(self._runner, host=self.host, port=self.port)
            self._loop.run_until_complete(site.start())
            self._loop.run_forever()
        except Exception as e:
            logging.error(f"Server error: {str(e)}")
        finally:
            # Cleanup
            if self._runner is not None:
                self._loop.run_until_complete(self._runner.cleanup())
            if self._loop is not None:
                self._loop.close()

    def close(self):
        """Shutdown the server."""
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
            logging.info("Stopping server loop...")
        
        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=2)
            logging.info("Server thread stopped")
            
    def poll(self) -> KVPoll:
        """Poll the server status."""
        if self.thread is None:
            return KVPoll.Failed
        if not self.thread.is_alive():
            return KVPoll.Failed
        return KVPoll.Success


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="HTTP Metadata Server for Mooncake",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--port", 
        type=int, 
        default=8080, 
        help="Port to listen on"
    )
    parser.add_argument(
        "--host", 
        type=str, 
        default="0.0.0.0", 
        help="Host to bind to"
    )
    parser.add_argument(
        "--log-level", 
        type=str, 
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO", 
        help="Logging level"
    )
    return parser.parse_args()


def main():
    """Main entry point for the mooncake_http_metadata_server command."""
    args = parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and start the server
    server = KVBootstrapServer(port=args.port, host=args.host)
    server.run()
    
    # Handle graceful shutdown
    def signal_handler(sig, frame):
        logging.info("Shutting down...")
        server.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Keep the main thread alive
    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        server.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

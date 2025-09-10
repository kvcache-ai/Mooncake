from enum import Enum
from time import sleep
from aiohttp import web
import threading
import asyncio

class KVPoll(Enum):
    Failed = 0
    Bootstrapping = 1
    WaitingForInput = 2
    Transferring = 3
    Success = 4

class KVBootstrapServer:
    def __init__(self, port: int):
        self.port = port
        self.app = web.Application()
        self.store = dict()
        self.lock = asyncio.Lock()
        self._setup_routes()
    
    def run(self):
        self.thread = threading.Thread(target=self._run_server, daemon=True)
        self.thread.start()

    def _setup_routes(self):
        self.app.router.add_route('*', '/metadata', self._handle_metadata)

    async def _handle_metadata(self, request: web.Request):
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
        async with self.lock:
            value = self.store.get(key)
        if value is None:
            return web.Response(text='metadata not found', status=404,
                              content_type='application/json')
        return web.Response(body=value, status=200,
                          content_type='application/json')

    async def _handle_put(self, key, request):
        data = await request.read()
        async with self.lock:
            if key.find("rpc_meta") != -1 and key in self.store:
                return web.Response(text='Duplicate rpc_meta key not allowed', status=400,
                                  content_type='application/json')
            self.store[key] = data
        return web.Response(text='metadata updated', status=200,
                          content_type='application/json')

    async def _handle_delete(self, key):
        async with self.lock:
            if key not in self.store:
                return web.Response(text='metadata not found', status=404,
                                  content_type='application/json')
            del self.store[key]
        return web.Response(text='metadata deleted', status=200,
                          content_type='application/json')
    def _run_server(self):
        try:
            # Event Loop
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            
            self._runner = web.AppRunner(self.app)
            self._loop.run_until_complete(self._runner.setup())
            
            site = web.TCPSite(self._runner, port=self.port)
            self._loop.run_until_complete(site.start())
            self._loop.run_forever()
        except Exception as e:
            print(f"Server error: {str(e)}")
        finally:
            # Cleanup
            self._loop.run_until_complete(self._runner.cleanup())
            self._loop.close()

    def close(self):
        """Shuttedown"""
        if self._loop is not None and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
            print("Stopping server loop...")
        
        if self.thread.is_alive():
            self.thread.join(timeout=2)
            print("Server thread stopped")
    def poll(self) -> KVPoll: ...

if __name__ == '__main__':
    server = KVBootstrapServer(port=8080)
    server.run()
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        server.close()
from enum import Enum
from aiohttp import web
import threading
import asyncio
import json

# Path-segment marker shared with the production http_metadata_server.py and
# the C++ client (transfer_metadata.cpp). We require the literal path segment
# "rpc_meta" to avoid misclassifying any other key that just happens to
# contain the substring "rpc_meta".
_RPC_META_SEGMENT = "rpc_meta"


def _is_rpc_meta_key(key: str) -> bool:
    parts = [p for p in key.split("/") if p]
    return _RPC_META_SEGMENT in parts


def _field_value(obj, *names):
    if isinstance(obj, dict):
        for name in names:
            if name in obj and obj[name] is not None:
                return obj[name]
    return None


def _put_rpc_meta_is_valid(new_payload: bytes, existing_payload: bytes):
    """(ok, reason) for an rpc_meta PUT against an existing entry.

    Accepts identical bytes (idempotent), rejects a rollback of
    metadata_version or a conflicting identity change, and otherwise allows
    a legitimate version bump matching TransferMetadata::rePublishRpcMetaEntry.
    """
    if new_payload == existing_payload:
        return True, "unchanged"
    try:
        new_obj = json.loads(new_payload)
        old_obj = json.loads(existing_payload)
    except (ValueError, TypeError):
        return False, "payload-not-json"
    new_version = _field_value(new_obj, "metadata_version")
    old_version = _field_value(old_obj, "metadata_version")
    if (
        isinstance(new_version, int)
        and isinstance(old_version, int)
        and new_version < old_version
    ):
        return False, "metadata_version-rollback"
    if (
        isinstance(new_version, int)
        and isinstance(old_version, int)
        and new_version == old_version
        and new_obj != old_obj
    ):
        return False, "metadata_version-conflict"
    new_host = _field_value(new_obj, "ip_or_host_name")
    old_host = _field_value(old_obj, "ip_or_host_name")
    new_port = _field_value(new_obj, "rpc_port")
    old_port = _field_value(old_obj, "rpc_port")
    if (
        new_host is not None
        and old_host is not None
        and new_host != old_host
    ) or (
        new_port is not None
        and old_port is not None
        and int(new_port) != int(old_port)
    ):
        return False, "identity-conflict"
    return True, "version-bumped"


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
            if _is_rpc_meta_key(key) and key in self.store:
                ok, reason = _put_rpc_meta_is_valid(data, self.store[key])
                if not ok:
                    return web.Response(
                        text=f'rpc_meta conflict: {reason}',
                        status=400,
                        content_type='application/json',
                    )
                if reason == 'unchanged':
                    return web.Response(
                        text='metadata unchanged',
                        status=200,
                        content_type='application/json',
                    )
                self.store[key] = data
                return web.Response(
                    text='metadata republished',
                    status=200,
                    content_type='application/json',
                )
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
            
            self._runner = web.AppRunner(self._loop)
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
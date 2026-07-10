import asyncio
import functools
from mooncake.store import MooncakeDistributedStore

class MooncakeDistributedStoreAsync(MooncakeDistributedStore):
    def __getattr__(self, name: str):
        if not name.startswith("async_"):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        sync_method_name = name[6:]

        if not hasattr(self, sync_method_name):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}' (nor '{sync_method_name}')")

        sync_method = getattr(self, sync_method_name)

        if not callable(sync_method):
            raise AttributeError(f"'{sync_method_name}' is not callable")

        async_method = self._make_async_wrapper(sync_method)
        setattr(self, name, async_method)
        return async_method

    def _make_async_wrapper(self, sync_method):
        @functools.wraps(sync_method)
        async def wrapper(*args, **kwargs):
            loop = asyncio.get_running_loop()

            func = functools.partial(sync_method, *args, **kwargs)
            return await loop.run_in_executor(None, func)

        return wrapper
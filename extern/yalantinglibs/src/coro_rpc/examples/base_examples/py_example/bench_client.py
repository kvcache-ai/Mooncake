import asyncio
import py_coro_rpc

async def async_request(pool, parr):
    loop = asyncio.get_event_loop()
    buf = b"hello world"
    while(True):
        tasks = [pool.async_send_msg(loop, buf) for i in range(1, parr)]
        await asyncio.gather(*tasks)


def send_request(pool):
    buf = bytes(2 * 1024 *1024)
    while(True):
        pool.sync_send_msg1(buf) #17420
        # pool.sync_send_msg(b"hello world") #17272

if __name__ == "__main__":
    pool = py_coro_rpc.py_coro_rpc_client_pool("0.0.0.0:9004")
    # send_request(pool)
    asyncio.run(async_request(pool, 20))
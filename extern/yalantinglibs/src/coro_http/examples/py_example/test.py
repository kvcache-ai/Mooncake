import asyncio
import py_example

async def async_get(url):
    loop = asyncio.get_running_loop()
    future = loop.create_future()    
    def cpp_callback(message):
        def set_result():
            future.set_result(message)
            print("set result ok")

        loop.call_soon_threadsafe(set_result)
        print("call_soon_threadsafe")

    caller = py_example.caller(cpp_callback)
    caller.async_get(url)
    print("await")
    result = await future
    print(result)

async def async_get1(url):
    buf = bytearray(2 * 1024)
    loop = asyncio.get_running_loop()
    result, len = await py_example.async_get(loop, url, buf)
    print(result)
    print(buf[:len])


async def main():
    await async_get1("http://taobao.com")
    await async_get("http://taobao.com")

if __name__ == "__main__":
    asyncio.run(main())
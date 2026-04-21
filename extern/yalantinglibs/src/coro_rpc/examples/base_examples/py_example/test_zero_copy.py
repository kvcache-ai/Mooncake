import asyncio
import zero_copy
import time

def callback(mv):
    len(mv)

c = zero_copy.caller(callback)

buf = bytearray(1 * 1024 * 1024)
c.hello(buf)

start_time = time.time()
for i in range(10000):
    c.hello(buf)

end_time = time.time()
elapsed = end_time - start_time

print(f"elapsed: {elapsed:.6f} s")

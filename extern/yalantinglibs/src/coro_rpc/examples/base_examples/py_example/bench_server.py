import asyncio
import time
import threading
import py_coro_rpc

class AtomicCounter:
    def __init__(self, initial=0):
        self._value = initial
        self._lock = threading.Lock()
    
    def inc(self, num=1):
        with self._lock:
            self._value += num
            return self._value
    
    def dec(self, num=1):
        with self._lock:
            self._value -= num
            return self._value
    
    def get(self):
        with self._lock:
            r = self._value
            self._value = 0
            return r

counter = AtomicCounter()

def handle_msg(con, msg):
    # begin to handle msg from client, do business
    counter.inc()
    # after finish business, response result
    con.response_msg(msg, lambda r: msg)

def print_qps():
    round = 25
    warmup = 2
    total = 0
    index = 0
    while(True):
        time.sleep(1)
        val = counter.get()
        if(val == 0):
            index = 0
            total = 0
            continue

        print("qps:", val)

        if(index > warmup and index < warmup + round):
            total += val
        elif(index == warmup + round):
            total += val
            print("avg qps:", total/round)

        index+=1

if __name__ == "__main__":
    server = py_coro_rpc.coro_rpc_server(2, "0.0.0.0:9004", handle_msg, 10)
    r = server.async_start()
    print("server start", r)
    thread = threading.Thread(target=print_qps)
    thread.start()
    thread.join()
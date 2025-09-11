import torch
import numpy as np
import time
import sys
import threading
import struct
from typing import List, Tuple, Dict, Any

import mooncake.engine as engine

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

size_1mb = 1024 * 1024
test_data = b'\x00' * size_1mb
url = "127.0.0.1:9004"

def print_qps():
    while(True):
        time.sleep(1)
        val = counter.get()
        if(val == 0):   
            continue

        print("qps:", val)

def send_data(client):
    while True:
        result = client.send_data(url, test_data)
        counter.inc()

CoroRPCInterface = engine.coro_rpc_interface.CoroRPCInterface

server = CoroRPCInterface()
# client = CoroRPCInterface()
server.initialize("0.0.0.0:9004", 8, 30, 4)
server.start_server()
# client.initialize("", 0, 30, 100)

thread = threading.Thread(target=print_qps)
thread.start()

for i in range(64):
    thread1 = threading.Thread(target=send_data, args=(client,))
    thread1.start()
    # while True:
    # result = client.send_data(url, test_data)
    # counter.inc()

thread.join()
# print(f"Send result: {result}")
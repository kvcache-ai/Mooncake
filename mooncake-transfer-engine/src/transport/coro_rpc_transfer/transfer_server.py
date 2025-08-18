#!/usr/bin/env python3
import asyncio
import threading
import time
import torch
import numpy as np
import coro_rpc_transfer

def tensor_handler(ctx, view):
    print(f"Received tensor data, size: {len(view)} bytes")
    response_data = b"Tensor received and processed successfully"
    response_buffer = memoryview(response_data)
    
    def done_callback(success):
        if success:
            print("Response sent successfully")
        else:
            print("Failed to send response")
    
    ctx.response_msg(response_buffer, done_callback)

def start_server():
    print("Starting RPC server...")

    server = coro_rpc_transfer.coro_rpc_server(
        4, 
        "127.0.0.1:8801", 
        tensor_handler, 
        10 
    )
    
    print("Server created, starting...")
    
    success = server.async_start()
    
    if success:
        print("RPC Server started successfully on 127.0.0.1:8801")
        print("\nServer is running... Press Ctrl+C to stop")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nServer stopping...")
    else:
        print("Failed to start server")
        return False
    
    return True

if __name__ == "__main__":
    start_server()
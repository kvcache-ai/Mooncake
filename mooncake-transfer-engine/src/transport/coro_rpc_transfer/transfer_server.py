#!/usr/bin/env python3
import asyncio
import threading
import time
import torch
import numpy as np
import coro_rpc_transfer

def tensor_handler(ctx, view):
    """
    处理接收到的tensor数据的回调函数
    ctx: py_rpc_context 对象
    view: memoryview 对象包含接收到的数据
    """
    print(f"Received tensor data, size: {len(view)} bytes")
    print()
    
    # 这里可以添加你的tensor处理逻辑
    # 例如：保存到文件、进行计算等
    response_data = b"Tensor received and processed successfully"
    response_buffer = memoryview(response_data)
    
    def done_callback(success):
        if success:
            print("Response sent successfully")
        else:
            print("Failed to send response")
    
    ctx.response_msg(response_buffer, done_callback)

def start_server():
    """启动RPC服务器"""
    print("Starting RPC server...")

    server = coro_rpc_transfer.coro_rpc_server(
        4, 
        "127.0.0.1:8801", 
        tensor_handler, 
        10 
    )
    
    print("Server created, starting...")
    
    # 启动服务器
    success = server.async_start()
    
    if success:
        print("✅ RPC Server started successfully on 127.0.0.1:8801")
        print("Available endpoints:")
        print("  - handle_msg: 处理普通消息")
        print("  - handle_tensor: 处理tensor传输")
        print("  - reflect_tensor: 回显tensor")
        print("\nServer is running... Press Ctrl+C to stop")
        
        try:
            # 保持服务器运行
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Server stopping...")
    else:
        print("❌ Failed to start server")
        return False
    
    return True

if __name__ == "__main__":
    start_server()
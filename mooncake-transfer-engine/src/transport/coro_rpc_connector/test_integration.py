#!/usr/bin/env python3
"""
Test the integration of coro_rpc_interface with mooncake_transfer_engine
"""

print("=== Testing integration of coro_rpc_interface with mooncake_transfer_engine ===\n")

import mooncake.engine as mooncake_transfer_engine
print("Imported mooncake_transfer_engine successfully")

rpc_interface = mooncake_transfer_engine.coro_rpc_interface
print("Accessed coro_rpc_interface submodule successfully")


interface = rpc_interface.CoroRPCInterface()
print("Created CoroRPCInterface instance successfully")

public_methods = [m for m in dir(interface) if not m.startswith('_')]
print(f"Number of available public methods: {len(public_methods)}")

received_data = rpc_interface.ReceivedData()
print("Created ReceivedData instance successfully")

received_tensor = rpc_interface.ReceivedTensor()
print("Created ReceivedTensor instance successfully")

client = rpc_interface.create_rpc_client()
print("Called create_rpc_client function successfully")

print("\n=== Integration test completed ===")
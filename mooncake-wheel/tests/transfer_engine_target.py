import os
from mooncake.engine import TransferEngine

target_server_name = os.getenv("TARGET_SERVER_NAME", "127.0.0.1:12345")
initiator_server_name = os.getenv("INITIATOR_SERVER_NAME", "127.0.0.1:12347")
metadata_server = os.getenv("MC_METADATA_SERVER", "127.0.0.1:2379")
protocol = os.getenv("PROTOCOL", "tcp")       # Protocol type: "rdma" or "tcp"

target = TransferEngine()
target.initialize(target_server_name,metadata_server, protocol, "")

while( True ):
    pass


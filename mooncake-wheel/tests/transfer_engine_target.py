import os
from mooncake.engine import TransferEngine

target_server_name = os.getenv("TARGET_SERVER_NAME", "")
metadata_server = os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE")
protocol = os.getenv("PROTOCOL", "tcp")       # Protocol type: "rdma" or "tcp"

target = TransferEngine()
target.initialize(target_server_name,metadata_server, protocol, "")

while( True ):
    pass


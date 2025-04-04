import os
import subprocess
import argparse
from importlib.resources import files
from mooncake import *

parser = argparse.ArgumentParser(description="Start Mooncake Master")
parser.add_argument("--port", type=int, help="The port number to use", default=50051)
parser.add_argument("--max_threads", type=int, help="Maximum number of threads to use", default=4)
parser.add_argument("--enable_gc", action="store_true", help="Enable garbage collection", default=False)

args = parser.parse_args()

bin_path = files("mooncake") / "mooncake_master"
print("bin path:", bin_path)

os.chmod(bin_path, 0o755)

command = [bin_path, "--port", str(args.port), "--max_threads", str(args.max_threads)]

if args.enable_gc:
    command.append("--enable_gc")

result = subprocess.run(command)

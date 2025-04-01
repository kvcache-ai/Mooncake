import os
import subprocess
import argparse
from importlib.resources import files
from mooncake import *

parser = argparse.ArgumentParser(description="Start Mooncake Master")
parser.add_argument("--port", type=int, help="The port number to use", default=50051)

args = parser.parse_args()

bin_path = files("mooncake") / "mooncake_master"
print("bin path:", bin_path)

os.chmod(bin_path, 0o755)

result = subprocess.run([bin_path, "--port", str(args.port)])

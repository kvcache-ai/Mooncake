import os
from importlib.resources import files
from mooncake import *
import subprocess
bin_path = files("mooncake") / "mooncake_master"
print("bin path:", bin_path)
os.chmod(bin_path, 0o755)
result = subprocess.run([bin_path])

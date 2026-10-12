import pathlib
import sys

# Put the repository root on sys.path, so the tests import the benchmark package
# from the tree they live in whether pytest is started from the root or from the
# test directory.
REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[3]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

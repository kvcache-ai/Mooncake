#!/bin/bash
# Script to test the installation of the mooncake wheel package
# Usage: ./scripts/test_installation.sh
# Set EXPECTED_STORE_ARCH=p2p for a P2P wheel (default: centralized).

set -e  # Exit immediately if a command exits with a non-zero status

# Ensure LD_LIBRARY_PATH includes /usr/local/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

echo "Creating a clean Python environment for testing..."
python -m venv test_env
source test_env/bin/activate

echo "Verifying that import fails before installation..."
# Verify that importing mooncake.engine fails before installation
python -c "import mooncake.engine" 2>/dev/null && { echo "ERROR: Import succeeded when it should have failed!"; exit 1; } || echo "Good: Import failed as expected before installation"

echo "Installing the wheel package..."
# Install the wheel package
pip install mooncake-wheel/dist/*.whl

# install dependencies
SYSTEM_PACKAGES="build-essential \
                  cmake \
                  libibverbs-dev \
                  libnuma-dev \
                  libssl-dev \
                  libcurl4-openssl-dev"

sudo apt-get install -y $SYSTEM_PACKAGES


echo "Verifying that import succeeds after installation..."
python -c "import mooncake.engine" && echo "Success: Import succeeded after installation" || { echo "ERROR: Import failed after installation!"; exit 1; }
python -c "import mooncake.cli, mooncake.cli_bench, mooncake.cli_client, mooncake.transfer_engine_topology_dump" && echo "Success: CLI imports succeeded after installation" || { echo "ERROR: CLI imports failed after installation!"; exit 1; }

echo "Verifying installed Store artifact architecture..."
STORE_PACKAGE_DIR=$(python -c 'import mooncake; from pathlib import Path; print(Path(mooncake.__file__).parent)')
python mooncake-wheel/setup.py --check-store-artifacts "$STORE_PACKAGE_DIR" \
    --with-store ON --expected-architecture "${EXPECTED_STORE_ARCH:-centralized}"

# TODO: Follow F01-T113 (owner/replace-in=UPG2-F01-public-build) for U's
# Reshard package checks, and F01-T114 (owner/replace-in=UPG1-F15) for
# structured/fast-copy checks. Add their U import/test calls together with
# the real installed providers; do not add calls to unavailable modules.
echo "Running import structure test..."
# Run the import structure test
cp -r mooncake-wheel/tests test_env/
cd test_env
# TODO: Follow F01-T113 (owner/replace-in=UPG2-F01-public-build) to restore
# U's unpinned Torch install with its EP/PG CMake/JIT providers. The current
# legacy wheel builds extensions for explicit Torch versions.
pip install torch==2.10.0 numpy
python tests/test_import_structure.py

echo "Running mooncake config test..."
python tests/test_mooncake_config.py

echo "Verifying mooncake_master entry point..."
# Check if the mooncake_master entry point is installed and executable
which mooncake_master || { echo "ERROR: mooncake_master entry point not found!"; exit 1; }
mooncake_master --version
echo "Success: mooncake_master entry point found"

# Check if the mooncake_client entry point is installed and executable
which mooncake_client || { echo "ERROR: mooncake_client entry point not found!"; exit 1; }
echo "Success: mooncake_client entry point found"

echo "Verifying transfer_engine_bench entry point..."
# Check if the transfer_engine_bench entry point is installed and executable
which transfer_engine_bench || { echo "ERROR: transfer_engine_bench entry point not found!"; exit 1; }
echo "Success: transfer_engine_bench entry point found"

echo "Verifying mooncake_http_metadata_server entry point..."
which mooncake_http_metadata_server || { echo "ERROR: mooncake_http_metadata_server entry point not found!"; exit 1; }
mooncake_http_metadata_server --help >/dev/null
echo "Success: mooncake_http_metadata_server entry point found"
echo "Verifying transfer_engine_topology_dump entry point..."
transfer_engine_topology_dump --help >/dev/null
python -m mooncake.transfer_engine_topology_dump --help >/dev/null
echo "Success: transfer_engine_topology_dump entry point and module execution work"

cd ..

echo "Installation test completed successfully!"

# Note: We don't deactivate the virtual environment here
# so that subsequent scripts can use it

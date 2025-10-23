#!/bin/bash
# Script to test the installation of the mooncake wheel package
# Usage: ./scripts/test_installation.sh

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

echo "Running import structure test..."
# Run the import structure test
cp -r mooncake-wheel/tests test_env/
cd test_env
pip install torch numpy
python tests/test_import_structure.py

echo "Running mooncake config test..."
python tests/test_mooncake_config.py

echo "Verifying mooncake_master entry point..."
# Check if the mooncake_master entry point is installed and executable
which mooncake_master || { echo "ERROR: mooncake_master entry point not found!"; exit 1; }
echo "Success: mooncake_master entry point found"

echo "Verifying transfer_engine_bench entry point..."
# Check if the transfer_engine_bench entry point is installed and executable
which transfer_engine_bench || { echo "ERROR: transfer_engine_bench entry point not found!"; exit 1; }
echo "Success: transfer_engine_bench entry point found"

cd ..

echo "Installation test completed successfully!"

# Note: We don't deactivate the virtual environment here
# so that subsequent scripts can use it

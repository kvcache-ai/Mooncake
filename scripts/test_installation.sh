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
# Verify that importing mooncake.transfer fails before installation
python -c "import mooncake.transfer" 2>/dev/null && { echo "ERROR: Import succeeded when it should have failed!"; exit 1; } || echo "Good: Import failed as expected before installation"

echo "Installing the wheel package..."
# Install the wheel package
pip install mooncake-wheel/dist/*.whl

echo "Verifying that import succeeds after installation before dependencies..."
# Verify that importing mooncake.transfer fails after installation
python -c "import mooncake.transfer" 2>/dev/null && { echo "ERROR: Import succeeded when it should have failed!"; exit 1; } || echo "Good: Import failed as expected before installation"

# install dependencies
SYSTEM_PACKAGES="build-essential \
                  cmake \
                  libibverbs-dev \
                  libgoogle-glog-dev \
                  libgtest-dev \
                  libjsoncpp-dev \
                  libunwind-dev \
                  libnuma-dev \
                  libpython3-dev \
                  libboost-all-dev \
                  libssl-dev \
                  libgrpc-dev \
                  libgrpc++-dev \
                  libprotobuf-dev \
                  protobuf-compiler-grpc \
                  pybind11-dev \
                  libcurl4-openssl-dev \
                  libhiredis-dev \
                  pkg-config \
                  patchelf"

sudo apt-get install -y $SYSTEM_PACKAGES

# expected success now
echo "Verifying that import succeeds after installation after dependencies..."
python -c "import mooncake.transfer" && echo "Success: Import succeeded after installation" || { echo "ERROR: Import failed after installation!"; exit 1; }

echo "Running import structure test..."
# Run the import structure test
cp -r mooncake-wheel/tests test_env/
cd test_env
python tests/test_import_structure.py

echo "Verifying mooncake_master entry point..."
# Check if the mooncake_master entry point is installed and executable
which mooncake_master || { echo "ERROR: mooncake_master entry point not found!"; exit 1; }
echo "Success: mooncake_master entry point found"

cd ..

echo "Installation test completed successfully!"

# Note: We don't deactivate the virtual environment here
# so that subsequent scripts can use it

#!/bin/bash
# Copyright 2026 KVCache.AI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds and installs opentelemetry-cpp (OTLP/HTTP + OTLP/gRPC exporters)
# into the default prefix (/usr/local) so that Mooncake can link it when
# MOONCAKE_ENABLE_OTEL_TRACING=ON. Run ./dependencies.sh first -- this
# script assumes the system packages it installs (cmake, unzip, wget,
# build-essential, libcurl4-openssl-dev, libgrpc*-dev, libprotobuf-dev,
# ...) are already present.

# Color definitions
GREEN="\033[0;32m"
BLUE="\033[0;34m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
NC="\033[0m" # No Color

# Configuration
REPO_ROOT=`pwd`
GITHUB_PROXY=${GITHUB_PROXY:-"https://github.com"}
OTELCPP_VERSION=1.28.0
OTELCPP_ZIPFILE="otelcpp-v${OTELCPP_VERSION}.zip"

# Function to print section headers
print_section() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

# Function to print success messages
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

# Function to print error messages and exit
print_error() {
    echo -e "${RED}✗ ERROR: $1${NC}"
    exit 1
}

# Function to check command success
check_success() {
    if [ $? -ne 0 ]; then
        print_error "$1"
    fi
}

# Root permission is needed for apt-get install and `cmake --install`
# (which installs into /usr/local by default).
if [ $(id -u) -ne 0 ]; then
    print_error "Require root permission, try sudo ./install_otel.sh"
fi

# This script depends on tools that dependencies.sh installs. Bail out
# early with a clear hint instead of failing later inside the build.
for tool in cmake unzip wget make g++; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        print_error "Required tool '$tool' not found. Run ./dependencies.sh first."
    fi
done

# Update package lists
print_section "Updating package lists"
apt-get update
check_success "Failed to update package lists"

# opentelemetry-cpp builds its tests and benchmarks by default
# (BUILD_TESTING=ON, WITH_BENCHMARK=ON), so it needs gmock and benchmark.
print_section "Installing OpenTelemetry-cpp build dependencies"
OTEL_DEP_PACKAGES="libgmock-dev libbenchmark-dev"
apt-get install -y ${OTEL_DEP_PACKAGES}
check_success "Failed to install OpenTelemetry-cpp dependencies"
print_success "OpenTelemetry-cpp dependencies installed successfully"

# Ensure thirdparties directory exists
WORK_DIR="${REPO_ROOT}/thirdparties"
if [ ! -d "${WORK_DIR}" ]; then
    mkdir -p "${WORK_DIR}"
    check_success "Failed to create thirdparties directory"
fi

# =====================================================================
# Build & install utf8_range (required by opentelemetry-cpp's protobuf)
# =====================================================================
print_section "Installing utf8_range"

cd "${WORK_DIR}"
check_success "Failed to change to thirdparties directory"

if [ -d utf8_range ]; then
    echo -e "${YELLOW}utf8_range directory already exists. Removing for fresh install...${NC}"
    rm -rf utf8_range
    check_success "Failed to remove existing utf8_range directory"
fi

echo "Cloning utf8_range from ${GITHUB_PROXY}/protocolbuffers/utf8_range.git"
git clone ${GITHUB_PROXY}/protocolbuffers/utf8_range.git
check_success "Failed to clone utf8_range"

cd utf8_range
check_success "Failed to change to utf8_range directory"

mkdir -p build
check_success "Failed to create utf8_range build directory"
cd build
check_success "Failed to change to utf8_range build directory"

echo "Configuring utf8_range..."
cmake .. -DCMAKE_BUILD_TYPE=Release
check_success "Failed to configure utf8_range"

echo "Building utf8_range (using $(nproc) cores)..."
cmake --build . -j$(nproc)
check_success "Failed to build utf8_range"

echo "Installing utf8_range..."
cmake --install .
check_success "Failed to install utf8_range"

print_success "utf8_range installed successfully"

# =====================================================================
# Build & install opentelemetry-cpp
# =====================================================================
print_section "Installing opentelemetry-cpp ${OTELCPP_VERSION}"

cd "${WORK_DIR}"
check_success "Failed to change to thirdparties directory"

# Check if opentelemetry-cpp is already installed
if [ -d "opentelemetry-cpp-${OTELCPP_VERSION}" ]; then
    echo -e "${YELLOW}opentelemetry-cpp-${OTELCPP_VERSION} directory already exists. Removing for fresh install...${NC}"
    rm -rf opentelemetry-cpp-${OTELCPP_VERSION}
    check_success "Failed to remove existing opentelemetry-cpp directory"
fi

echo "Downloading opentelemetry-cpp ${OTELCPP_VERSION} from ${GITHUB_PROXY}/open-telemetry/opentelemetry-cpp/archive/refs/tags/v${OTELCPP_VERSION}.zip"
wget -q --show-progress -O ${OTELCPP_ZIPFILE} ${GITHUB_PROXY}/open-telemetry/opentelemetry-cpp/archive/refs/tags/v${OTELCPP_VERSION}.zip
check_success "Failed to download opentelemetry-cpp"

# Extract opentelemetry-cpp
echo "Extracting opentelemetry-cpp..."
unzip -q ${OTELCPP_ZIPFILE}
check_success "Failed to extract opentelemetry-cpp"

# Clean up downloaded ZIP file
rm -f ${OTELCPP_ZIPFILE}
check_success "Failed to clean up downloaded ZIP file"

# Build and install opentelemetry-cpp
cd opentelemetry-cpp-${OTELCPP_VERSION}
check_success "Failed to change to opentelemetry-cpp directory"

mkdir -p build
check_success "Failed to create opentelemetry-cpp build directory"
cd build
check_success "Failed to change to opentelemetry-cpp build directory"

echo "Configuring opentelemetry-cpp..."
# WITH_HTTP_CLIENT_CURL=ON: builds the curl-based HttpClient that the
# OTLP/HTTP exporter ships spans with. Mooncake uses the stock
# opentelemetry-cpp HTTP transport (libcurl) rather than a custom one,
# so libcurl becomes a runtime shared dep of mooncake_master/client.
# The OTLP/HTTP and OTLP/gRPC exporters are both enabled for Mooncake.
cmake .. -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DWITH_OTLP_HTTP=ON -DWITH_OTLP_GRPC=ON -DWITH_HTTP_CLIENT_CURL=ON
check_success "Failed to configure opentelemetry-cpp"

echo "Building opentelemetry-cpp (using $(nproc) cores)..."
cmake --build . -j$(nproc)
check_success "Failed to build opentelemetry-cpp"

echo "Installing opentelemetry-cpp..."
cmake --install .
check_success "Failed to install opentelemetry-cpp"

print_success "opentelemetry-cpp installed successfully"

# Return to the repository root
cd "${REPO_ROOT}"

print_section "Installation Complete"
echo -e "${GREEN}opentelemetry-cpp ${OTELCPP_VERSION} has been successfully installed!${NC}"
echo -e "You can now build Mooncake with MOONCAKE_ENABLE_OTEL_TRACING=ON."

#!/bin/bash

# Color definitions
GREEN="\033[0;32m"
BLUE="\033[0;34m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
NC="\033[0m" # No Color

REPO_ROOT=`pwd`
GITHUB_PROXY=${GITHUB_PROXY:-"https://github.com"}
GITHUB_PROXY="https://github.ednovas.xyz/https://github.com"

OTEL_DEP_PACKAGES="libgmock-dev libbenchmark-dev"
apt install -y ${OTEL_DEP_PACKAGES}

# Function to print error messages and exit
print_error() {
    echo -e "${RED}✗ ERROR: $1${NC}"
    exit 1
}

# Function to print success messages
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

check_success() {
    if [ $? -ne 0 ]; then
        print_error "$1"
    fi
}
# build & install utf8_range firstly

git clone https://github.com/protocolbuffers/utf8_range.git
cd utf8_range
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make
make install

OTELCPP_VERSION=1.28.0
OTELCPP_ZIPFILE="otelcpp-v${OTELCPP_VERSION}.zip"

# Check if thirdparties directory exists
if [ ! -d "${REPO_ROOT}/thirdparties" ]; then
    mkdir -p "${REPO_ROOT}/thirdparties"
    check_success "Failed to create thirdparties directory"
fi

# Change to thirdparties directory
cd "${REPO_ROOT}/thirdparties"
check_success "Failed to change to thirdparties directory"

# Check if opentelemetry-cpp is already installed
if [ -d "opentelemetry-cpp-${OTELCPP_VERSION}" ]; then
    echo -e "${YELLOW}opentelemetry-cpp--${OTELCPP_VERSION} directory already exists. Removing for fresh install...${NC}"
    rm -rf opentelemetry-cpp-${OTELCPP_VERSION}
    check_success "Failed to remove existing opentelemetry-cpp directory"
fi

echo "Downloading opentelemetry-cpp ${OTELCPP_VERSION} from ${GITHUB_PROXY}/open-telemetry/open-telemetry-cpp/archive/refs/tags/v${OTELCPP_VERSION}.zip"
#wget -q --show-progress -O ${OTELCPP_ZIPFILE} ${GITHUB_PROXY}/open-telemetry/opentelemetry-cpp/archive/refs/tags/v${OTELCPP_VERSION}.zip
check_success "Failed to download opentelemetry-cpp"

# Extract opentelemetry-cpp
echo "Extracting opentelemetry-cpp..."
unzip -q ${OTELCPP_ZIPFILE}
check_success "Failed to extract opentelemetry-cpp"

# Clean up downloaded ZIP file
#rm -f ${OTELCPP_ZIPFILE}
check_success "Failed to clean up downloaded ZIP file"

# Build and install opentelemetry-cpp
cd opentelemetry-cpp-${OTELCPP_VERSION}
#check_success "Failed to change to opentelemetry-cpp directory"
mkdir -p build
check_success "Failed to create build directory"

cd build
check_success "Failed to change to build directory"

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

# Dev image for Mooncake on Ubuntu 22.04 (CPU-only, ARM-friendly)
FROM ubuntu:22.04

WORKDIR /app

# Copy repo into container
COPY . .

# Environment for Go (dependencies.sh will install it under /usr/local/go)
ENV GOROOT=/usr/local/go
ENV PATH=$GOROOT/bin:$PATH

# Install basic build tools + Python
RUN apt-get update \
    && apt-get install -y \
       build-essential \
       cmake \
       git \
       wget \
       unzip \
       sudo \
       python3 \
       python3-pip \
    && pip3 install --no-cache-dir pybind11

# Run Mooncake's dependency installer
RUN bash dependencies.sh \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /tmp/* /var/tmp/* \
    && find /var/cache/apt/archives /var/lib/apt/lists -not -name lock -type f -delete \
    && find /var/cache -type f -delete

# Configure & build Mooncake
# Use limited parallelism to avoid OOM in the container
RUN rm -rf build || true \
    && mkdir build && cd build \
    && cmake .. -DSTORE_USE_ETCD=ON \
    && make -j4 \
    && make install

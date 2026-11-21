# Base Image from Alibaba Cloud AC2
FROM ac2-registry.cn-hangzhou.cr.aliyuncs.com/ac2/pytorch-ubuntu:2.3.0-cuda12.1.1-ubuntu22.04

WORKDIR /app

COPY . .

ENV GOROOT=/usr/local/go
ENV PATH=$GOROOT/bin:$PATH

RUN apt update \
     && apt install -y unzip wget cmake git sudo \
     && pip install pybind11

# Execute installation in the container
RUN bash dependencies.sh \
     && apt autoremove -y \
     && apt clean -y \
     && rm -rf /tmp/* /var/tmp/* \
     && find /var/cache/apt/archives /var/lib/apt/lists -not -name lock -type f -delete \
     && find /var/cache -type f -delete

ARG WITH_STORE=ON
ARG WITH_STORE_P2P=OFF

RUN rm -rf build || true \
     && mkdir build && cd build \
     && cmake .. -DSTORE_USE_ETCD=ON -DWITH_STORE="${WITH_STORE}" -DWITH_STORE_P2P="${WITH_STORE_P2P}" && make -j$(nproc) && make install

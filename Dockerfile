# Base Image from Alibaba Cloud AC2
FROM ac2-registry.cn-hangzhou.cr.aliyuncs.com/ac2/pytorch-ubuntu:2.3.0-cuda12.1.1-ubuntu22.04

# Execute installation in the container
RUN  apt update
RUN  apt install -y unzip wget cmake git sudo
RUN  pip install pybind11
RUN  wget https://github.com/kvcache-ai/Mooncake/archive/refs/heads/main.zip
RUN  unzip main.zip
RUN  cd Mooncake-main && bash dependencies.sh && mkdir build && cd build \
       && cmake .. && make -j && make install
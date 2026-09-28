#!/bin/bash

ip=$(hostname -I | awk '{print $1}')
echo $ip

MC_MIN_PRC_PORT=25565 MC_MAX_PRC_PORT=25565 ./build/mooncake-transfer-engine/tests/cxi_transfer_test \
    --mode target \
    --use_device \
    --server $ip \
    --num_bufs 4 \
    --buf_size_gb 1 &
TARGET_PID=$!

sleep 10

./build/mooncake-transfer-engine/tests/cxi_transfer_test --mode initiator \
    --server 127.0.0.1:12346 \
    --target $ip:25565 \
    --transfer_mb 128 \
    --threads 4 \
    --use_device \
    --warmup 0

kill -INT $TARGET_PID
wait $TARGET_PID
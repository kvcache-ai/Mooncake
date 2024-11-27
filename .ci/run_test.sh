set -ex
TEST_DIR=$1
OUTPUT_DIR=$2


run_test(){
    echo "$OUTPUT_DIR"
    ./"$1" 
    LOCAL_SUCCESS=$?
    if [[ $LOCAL_SUCCESS == 0 ]]; then
        echo "PASSED : $1 " >> "$OUTPUT_DIR"/results.txt
    else
        echo "FAILED : $1 " >> "$OUTPUT_DIR"/results.txt
    fi
}


pushd $TEST_DIR
etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://10.0.0.1:2379 &
export MC_GID_INDEX=1
run_test transport_uint_test
run_test tcp_transport_test
export MC_GID_INDEX=1

# sudo pkill etcd
# sleep 1
# etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://10.0.0.1:2379 &
# sudo fuser -k 14345/tcp
export MC_GID_INDEX=1 && ./rdma_transport_test --mode=target  --metadata_server=127.0.0.1:2379 --local_server_name=127.0.0.2:14345 --device_name=erdma_0 &
export MC_GID_INDEX=1 && ./rdma_transport_test --metadata_server=127.0.0.1:2379 --segment_id=127.0.0.2:14345 --local_server_name=127.0.0.3:14346 --device_name=erdma_1
LOCAL_SUCCESS=$?
if [[ $LOCAL_SUCCESS == 0 ]]; then
    echo "PASSED : rdma_transport_test " >> "$OUTPUT_DIR"/results.txt
else
    echo "FAILED : rdma_transport_test " >> "$OUTPUT_DIR"/results.txt
fi

# sudo pkill etcd
# sleep 1
# etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://10.0.0.1:2379 &
# export MC_GID_INDEX=1
# sudo fuser -k 15345/tcp
# export MC_GID_INDEX=1 && ./rdma_transport_test --mode=target  --metadata_server=127.0.0.1:2379 --local_server_name=127.0.0.2:14345 --device_name=erdma_0 &
export MC_GID_INDEX=1 && ./rdma_transport_test2 --metadata_server=127.0.0.1:2379 --segment_id=127.0.0.2:14345 --local_server_name=127.0.0.3:14346 --device_name=erdma_1
LOCAL_SUCCESS=$?
if [[ $LOCAL_SUCCESS == 0 ]]; then
    echo "PASSED : rdma_transport_test2 " >> "$OUTPUT_DIR"/results.txt
else
    echo "FAILED : rdma_transport_test2 " >> "$OUTPUT_DIR"/results.txt
fi
popd
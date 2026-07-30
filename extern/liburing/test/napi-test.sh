#! /usr/bin/env bash

if [ ! -x "$(command -v ip)" ]; then
	echo "Need ip installed"
	exit 77
fi
if [ ! -x "$(command -v ethtool)" ]; then
	echo "Need ethool installed"
	exit 77
fi

function clean_namespaces {
	ip netns del iou-nscl
	ip netns del iou-nsserv
}
trap clean_namespaces EXIT

ip link add iou-ptp-cl type veth peer name iou-ptp-serv

ip netns add iou-nscl
ip link set iou-ptp-cl netns iou-nscl
ip netns exec iou-nscl ip addr add '10.10.10.10/24' dev iou-ptp-cl
ip netns exec iou-nscl ethtool -K iou-ptp-cl tcp-segmentation-offload off
ip netns exec iou-nscl ethtool -K iou-ptp-cl generic-receive-offload on
ip netns exec iou-nscl ip link set dev iou-ptp-cl up

ip netns add iou-nsserv
ip link set iou-ptp-serv netns iou-nsserv
ip netns exec iou-nsserv ip addr add '10.10.10.20/24' dev iou-ptp-serv
ip netns exec iou-nsserv ethtool -K iou-ptp-serv tcp-segmentation-offload off
ip netns exec iou-nsserv ethtool -K iou-ptp-serv generic-receive-offload on
ip netns exec iou-nsserv ip link set dev iou-ptp-serv up

# test basic init, defer_taskrun, and sqpoll
QUEUE_FLAGS="0x0 0x3000 0x2"
for flags in $QUEUE_FLAGS; do
	if [ -f "napi-test.t" ]; then
		NAPI_TEST="./napi-test.t"
	elif [ -f "test/napi-test.t" ]; then
		NAPI_TEST="test/napi-test.t"
	else
		echo "Can't find napi-test.t"
		exit 77
	fi
	ip netns exec iou-nsserv $NAPI_TEST receive $flags &
	ip netns exec iou-nscl $NAPI_TEST send $flags
	wait
done

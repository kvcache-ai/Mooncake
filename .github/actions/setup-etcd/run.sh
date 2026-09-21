#!/usr/bin/env bash
set -e -o pipefail

wget -q https://github.com/etcd-io/etcd/releases/download/v3.6.1/etcd-v3.6.1-linux-amd64.tar.gz
tar xzf etcd-v3.6.1-linux-amd64.tar.gz
sudo mv etcd-v3.6.1-linux-amd64/etcd* /usr/local/bin/
etcd --advertise-client-urls http://127.0.0.1:2379 --listen-client-urls http://127.0.0.1:2379 &
sleep 3
ETCDCTL_API=3 etcdctl --endpoints=http://127.0.0.1:2379 endpoint health

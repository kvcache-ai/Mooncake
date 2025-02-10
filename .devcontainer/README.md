# Mooncake Dev Container (experimental)

This directory contains some experimental tools for Mooncake Development in [VSCode Remote - Containers](https://code.visualstudio.com/docs/remote/containers).

## How to use

Open with VSCode with the Container extension installed. Follow the [official guide](https://code.visualstudio.com/docs/remote/containers) to open this
repository directly from GitHub or from checked-out source tree.

### Build

```
cd /workspaces/Mooncake
mkdir build
cd build
cmake .. -DUSE_ETCD=OFF -DUSE_HTTP=ON
make -j`nproc` VERBOSE=1
```

### Test

1. start the http metadata server

```
cd /workspaces/Mooncake/mooncake-transfer-engine/example/http-metadata-server
go run . --addr=:8090
```

2. run tests

```
cd /workspaces/Mooncake/build
MC_METADATA_SERVER=http://127.0.0.1:8090/metadata make test -j ARGS="-V"
```

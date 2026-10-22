# TENT plugin SDK example

A CMake project that builds the loopback sample transport plugin
(`libtent_xport_loopback.so`) against an installed TENT, as a plugin outside
the Mooncake source tree would be built.

From the Mooncake source directory, install the SDK of a TENT build and the
yalantinglibs it fetched into one prefix:

```bash
cmake --install build --component tent_plugin_sdk --prefix /opt/tent
cmake --install build/_deps/yalantinglibs-build --prefix /opt/tent
```

Then, from this directory:

```bash
cmake -S . -B build -DCMAKE_PREFIX_PATH=/opt/tent -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

The sample's sources come from the SDK (`share/tent/examples/loopback/`,
`tentPlugin_EXAMPLE_DIR`); while this directory is inside the Mooncake source
tree they come from `plugins/transport/loopback/` instead. See
`docs/source/design/tent/transport-plugins.md` for loading the result.

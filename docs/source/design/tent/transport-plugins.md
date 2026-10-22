# TENT Transport Plugins

TENT can load transports from shared objects at run time. A plugin is a
`libtent_xport_<name>.so` that exports the C entry point declared in
`tent/transport_plugin.h`; TENT loads the plugins in the directories named by
`plugins/dir` (or `MC_TENT_PLUGIN_DIR`) and does nothing when neither is set.

## Building a plugin out of tree

An installed TENT provides the headers and a CMake package, `tentPlugin`, for
building plugins outside the Mooncake source tree.

1. Install the plugin SDK of a TENT build (`-DUSE_TENT=ON`), and the
   yalantinglibs headers TENT's headers include, into one prefix. The build
   fetches yalantinglibs, so it is installed from the build directory:

   ```bash
   cmake --install build --component tent_plugin_sdk --prefix /opt/tent
   cmake --install build/_deps/yalantinglibs-build --prefix /opt/tent
   ```

   The component `tent_plugin_sdk` is `include/tent/`,
   `lib/cmake/tentPlugin/` and the loopback sample's sources in
   `share/tent/examples/loopback/`. A full `cmake --install build` installs it
   as well, together with `lib/libtent_shared.so` and the in-tree plugins in
   `lib/tent_plugins/`. Note that a full install also puts Mooncake's Python
   modules into the site-packages of the interpreter the build was configured
   with, whatever `--prefix` says.

2. Link the plugin against `tent::plugin`. The file must be named
   `libtent_xport_<name>.so`, where `<name>` is the name the plugin registers:

   ```cmake
   cmake_minimum_required(VERSION 3.16)
   project(my_transport LANGUAGES CXX)

   find_package(tentPlugin 1 REQUIRED)

   add_library(tent_xport_mine MODULE mine_transport.cpp plugin_entry.cpp)
   target_link_libraries(tent_xport_mine PRIVATE tent::plugin)
   set_target_properties(tent_xport_mine PROPERTIES PREFIX "lib")
   ```

   `mooncake-transfer-engine/tent/examples/tent-plugin-sdk/` is such a
   project for the loopback sample. It builds the sample's sources from the
   SDK (`tentPlugin_EXAMPLE_DIR`), or from the source tree while it is still in
   it.

3. Configure with the install prefix and build:

   ```bash
   cmake -S . -B build -DCMAKE_PREFIX_PATH=/opt/tent -DCMAKE_BUILD_TYPE=Release
   cmake --build build
   ```

4. Point TENT at the directory that holds the plugin, and enable the plugin
   unless it is enabled by default (`TENT_XPORT_FLAG_ENABLED_BY_DEFAULT`). The
   loopback sample is not, so it also needs `transports/loopback/enable` set to
   `true` in the engine's configuration:

   ```bash
   export MC_TENT_PLUGIN_DIR=/path/to/plugins
   ```

   An entry that is `$ORIGIN` or starts with `$ORIGIN/` (not `${ORIGIN}`) is
   relative to the directory of the file TENT was loaded from, as
   `/proc/self/maps` names it (symlinks resolved; TENT runs on Linux only): the
   executable that links it, `libtent_shared.so`, or the Python extension
   `engine.so` (or `libtransfer_engine.so` in a build with `BUILD_SHARED_LIBS`).
   A wheel built with `scripts/build_wheel.sh` from a TENT build ships the
   plugins of that build (the loopback sample, and TCP and RDMA when they are
   built as plugins) in `mooncake/tent_plugins/`, next to `engine.so`, so
   `MC_TENT_PLUGIN_DIR='$ORIGIN/tent_plugins'` selects them. Only one TENT copy
   in a process takes part in plugins, and `store.so` carries a copy of its own.
   If both modules are imported before the first engine loads plugins, the copy
   that tries second is refused (`host_symbols_not_exported`). A copy that is
   loaded only after another one has loaded plugins binds to that copy's symbols
   instead, unless it is linked with `-Bsymbolic`.

What the package guarantees and what it expects:

- **The package version is the plugin ABI version**
  (`TENT_TRANSPORT_PLUGIN_ABI_VERSION`); `find_package(tentPlugin 1)` only
  accepts an installation with ABI version 1.
- **`tent::plugin` carries the compile definitions of the TENT build it was
  installed from** (and `-fcoroutines` when that build used it). A plugin has
  to see the same types as the core that loads it. Build it with the same
  compiler, C++ standard library and C++ standard as the core: the core
  rejects a plugin of another ABI version (`rejected_abi_version`) and one
  whose ABI types differ in size, or whose GCC major version, libstdc++
  release, dual-ABI setting or C++ standard differs
  (`rejected_abi_fingerprint`), but it cannot detect every layout difference.
- **A plugin links nothing from TENT.** The process that loads it provides
  TENT's symbols (an executable that links TENT statically must export them;
  `libtent_shared.so` and the Python extensions do). `tent::plugin` links no
  library.
- **Link the versioned system libraries your own sources call.** A reference
  to a function of a library with versioned symbols (for example
  `libibverbs` or `libnuma`) only gets a symbol version if the plugin links
  that library; without one, the dynamic linker binds the oldest version.

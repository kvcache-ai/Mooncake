# TENT Transport Plugins

TENT can load transports from shared objects at run time. A plugin is a
`libtent_xport_<name>.so` that exports the C entry point declared in
`tent/transport_plugin.h`; TENT loads the plugins in the directories named by
`plugins/dir` (or `MC_TENT_PLUGIN_DIR`) and does nothing when neither is set.

## Why plugins

Every built-in transport is compiled into the core, selected with `USE_*`
options, and has a value in the closed `TransportType` enum. Adding one means
editing the core in a dozen places, and every build ships the vendor backends
that happened to be enabled when it was compiled. Plugins let the core be
packaged once, while each device, vendor or environment supplies its
transports as shared objects that are built, upgraded and shipped separately.
A missing, conflicting or incompatible plugin is reported precisely, and the
loader does not fail the process because of a bad file (a plugin's own code
can still crash it).

## The plugin interface

The entry point `tent_transport_plugin_register` fills a plain C descriptor,
`tent_transport_plugin_t`:

| Field | Meaning |
|---|---|
| `struct_size`, `abi_version` | Set by the core before the call. A plugin whose descriptor is larger than the core's returns `TENT_XPORT_REGISTER_E_STRUCT_SIZE`; otherwise it writes its own size and ABI version, which must equal the core's. |
| `abi_fingerprint` | A hash of the ABI version, the sizes of the types the core and the plugin exchange, the compiler's and C++ library's version macros (`__GNUC__`, `__GLIBCXX__`, `_GLIBCXX_USE_CXX11_ABI`) and the C++ standard (`tent_transport_abi_fingerprint()`). It must equal the core's. |
| `name` | `[a-z0-9_]+`, at most 64 characters, equal to the `<name>` in the file name. Policies, `transport_hint`, Python hints and logs use it. |
| `version` | Free text, logged and reported. |
| `builtin_type` | `0` for a new transport, or the `TransportType` of a built-in transport the plugin provides; it then has to use that transport's name. |
| `flags` | `TENT_XPORT_FLAG_*`: on by default; same machine only; local segment only; may carry the host hop of a staged transfer; notifications; cancellation. The three routing flags apply to plugin slots only (a plugin in a built-in slot keeps that slot's rules); the last two are upper bounds, like `declared_caps`. |
| `declared_caps` | `TENT_XPORT_CAP_*`: the most that `capabilities()` may report. |
| `probe` | Optional. Runs when the loader loads the plugin, once per process (`tent_plugin_inspect` runs it as well); a non-zero result means the plugin does not apply to this host. |
| `create`, `destroy` | `create()` returns a `mooncake::tent::Transport*` for one engine; `destroy()` deletes it. |
| `last_error` | Text explaining a failed `probe()` or `create()`. |

`create()` receives a `tent_transport_plugin_host_t`: the slot the transport
was assigned (it puts this value in `BufferDesc::transports`), its name, and
the engine's topology and configuration. A C++ plugin does not fill the
descriptor by hand; `tent/transport_plugin.hpp` defines the entry point from
one line:

```cpp
#include "my_transport.h"
#include "tent/transport_plugin.hpp"

// Calls MyTransport(host) in create().
TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(mine, MyTransport,
                                       mooncake::tent::UNSPEC,
                                       TENT_XPORT_FLAG_NOTIFICATION,
                                       TENT_XPORT_CAP_DRAM_TO_DRAM)
```

`TENT_DEFINE_TRANSPORT_PLUGIN` constructs the class without arguments, and
`TENT_DEFINE_TRANSPORT_PLUGIN_EX` takes a `probe` function and any `create`
expression. `TENT_TRANSPORT_PLUGIN_VERSION`, defined before the header is
included (for example on the compiler command line), sets the version string.

## How TENT loads plugins

Loading happens once per process, when the first engine with `plugins/dir` is
constructed, in three steps:

1. **Load** (before the transport selector parses policies). The candidate
   files are the `libtent_xport_<name>.so` files with a valid name, listed in
   `plugins/load` when that is set; of several files with one name, the one in
   the first directory. Each is opened with `dlopen(RTLD_NOW | RTLD_LOCAL)` and
   checked in this order: entry point, descriptor size and ABI version,
   fingerprint, names, `builtin_type`, then `probe()`. A plugin that passes
   gets a slot: its built-in slot, or one of 8 plugin slots after the
   built-in transports.
2. **Create** (per engine, after the built-in transports). The engine calls
   `create()` for every loaded plugin that is enabled for it, then `install()`
   with the other transports.
3. **Verify** (per engine, after `install()`). A transport whose
   `capabilities()` exceed `declared_caps`, or which reports notification or
   cancellation support without the matching flag, is uninstalled and dropped.
   `plugins/required` is enforced here.

Plugins are never unloaded. A plugin links no TENT library: its references to
TENT resolve to the TENT copy of the process that loads it, so there is one copy
of every TENT singleton. An executable that links TENT statically must export
its symbols (`ENABLE_EXPORTS`, i.e. `-rdynamic`; in `plugin` and `both` builds
TENT's CMake adds it to every such executable); `libtent_shared.so` exports
them. A core loaded with `RTLD_LOCAL` (the Python extension, or
`libtransfer_engine.so` with `BUILD_SHARED_LIBS`) is moved to the global symbol
scope once a plugin directory holds a candidate file and TENT's symbols are not
already global. If the process's global scope holds another TENT copy, every
plugin is refused with one `host_symbols_not_exported` report instead of failing
on an undefined symbol.

## Configuration

| Key | Meaning |
|---|---|
| `plugins/dir` | `:`-separated plugin directories. Nothing is loaded when it is not set. An entry that is `$ORIGIN` or starts with `$ORIGIN/` is relative to the directory of the file TENT was loaded from (see below). |
| `MC_TENT_PLUGIN_DIR` | Sets `plugins/dir`; it is applied after `MC_TENT_CONF`. |
| `plugins/load` | A list of plugin names (or one name): only these are opened and created. A malformed value loads nothing. |
| `plugins/required` | A list of plugin names that must be installed in the engine; otherwise construction fails with `PluginError` and names the reason. |
| `transports/<name>/enable` | Enables a plugin for an engine. Its default is the plugin's `TENT_XPORT_FLAG_ENABLED_BY_DEFAULT`. |

`MC_TENT_CONF` replaces the whole configuration, so use it only when it
carries the complete configuration; to add a plugin directory to a
configuration an application builds itself, set `MC_TENT_PLUGIN_DIR`.
`plugins/dir` and `plugins/load` are read when the first engine that sets
`plugins/dir` loads the plugins; a later engine with other values gets a
warning and the plugins already loaded, which its own `plugins/load` and
`transports/<name>/enable` can narrow. An engine without `plugins/dir` gets no
plugin transports and treats plugin names in its policies as unknown, whatever
other engines of the process loaded.

## Diagnostics

Every `libtent_xport_*.so` file in a plugin directory gets one outcome, logged
as `[TransportPlugin] <outcome> <name>: <detail>`; each engine logs again its
own outcome for every loaded plugin, whether it installed it, left it disabled
or rejected it (for example `loaded loopback: slot 19, version 1,
installed`). Other files are ignored. When the host does not export TENT's
symbols, one `host_symbols_not_exported` report replaces the reports of the
files that would have been opened.

| Outcome | When |
|---|---|
| `loaded` | Every check passed (per engine: created and installed). |
| `disabled_by_config` | Not listed in `plugins/load`, or `transports/<name>/enable` is false (or unset for a plugin that is off by default). |
| `not_applicable` | `probe()` returned non-zero or threw. |
| `rejected_name_mismatch` | The file name is not `libtent_xport_<name>.so` with a valid name, the descriptor's name differs, or a plugin claims a built-in transport under another name. |
| `rejected_bad_entry` | `dlopen()` failed (for example on an undefined symbol), there is no entry point, it threw or returned an error, `create()` or `destroy()` is missing, or `builtin_type` is out of range. |
| `rejected_abi_version` | Another ABI version or descriptor size. |
| `rejected_abi_fingerprint` | The fingerprint differs: other TENT headers, compiler or C++ library. |
| `rejected_duplicate_name` | An earlier directory provided the name, or the name is a built-in transport's and `builtin_type` is not set. |
| `rejected_conflict_with_builtin` | The engine already has the built-in transport the plugin claims. |
| `rejected_no_free_slot` | All 8 plugin slots are in use. |
| `rejected_create_failed` | `create()` returned NULL or threw. |
| `rejected_caps_overclaim` | After `install()`, the transport reports more than its descriptor declares. |
| `install_failed` | `install()` failed. |
| `host_symbols_not_exported` | Once per process: the host does not provide TENT's symbols to plugins. |

With `verbose` set, the engine banner adds one line for the engine's plugins,
for example `- Transport plugins:  loopback(loaded,slot 19)`, with the file
of a plugin that provides a built-in transport. A core built with
`TENT_XPORT_TCP=plugin` (or `RDMA`) logs a warning with a hint when no plugin
provided that transport to an engine that enables it (for RDMA, one whose
topology has an RDMA NIC). `tent_plugin_inspect` shows the outcomes that need
no engine, and with `--install-check` the others as well.

## Peers and the wire format

Plugin slots are local to a process. A segment descriptor carries a plugin
transport by name: on export, plugin slots move to the optional keys
`plugin_transports` and `plugin_transport_attrs`; on import, names are mapped
to this process's slots, and integers that are not built-in transports are
dropped. The descriptor of a process without plugins is byte for byte the
same as before, and peers that do not know the new keys ignore them. A plugin
that provides a built-in transport uses that transport's integer and is
therefore compatible with peers that have it built in.

## TCP and RDMA as plugins

`TENT_XPORT_TCP` and `TENT_XPORT_RDMA` choose how the two transports are
built:

- `static` (the default): the core contains the transport, as before.
- `plugin`: the core does not; `libtent_xport_tcp.so` / `libtent_xport_rdma.so`
  provide it and must be found through `plugins/dir`.
- `both`: the core contains it and the plugin is built too; the built-in wins,
  and the plugin is reported as `rejected_conflict_with_builtin` for every
  engine that has the built-in transport.

The plugins compile the core's own transport sources and must come from the
same build as the core: they share inline code and types that the
fingerprint does not cover. The RDMA plugin's `probe()` declines a host
without an RDMA device that has an active port, and its `create()` declines
an engine whose topology has no RDMA NIC. It links `libibverbs` and
`libnuma`, which version their symbols, and its build fails if any of their
functions is imported without a version.

## Writing a plugin

A plugin is a `mooncake::tent::Transport` subclass plus the entry point. The
loopback sample, `plugins/transport/loopback/`, copies memory between engines
of one process and shows the whole interface: slot tagging in
`addMemoryBuffer()`, all-or-nothing submission, notifications and
cancellation. In the TENT tree, a plugin is a directory under
`plugins/transport/`, added there with `add_subdirectory(mine)`, whose
`CMakeLists.txt` calls

```cmake
tent_add_transport_plugin(tent_xport_mine mine mine_transport.cpp
                          plugin_entry.cpp)
```

which builds `libtent_xport_mine.so` into `<build>/tent_plugins/` and installs
it to `lib/tent_plugins/`. Things a transport must get right:

- tag every buffer it registers with `host->assigned_type`, not a fixed value;
- in `submitTransferTasks()`, either accept the whole request list or return
  an error with nothing queued (all or nothing);
- bring every task to a terminal state, also after cancellation, and report
  the bytes a completed task moved;
- tolerate `destroy()` without `install()` and repeated `quiesce()`.

The conformance suite below checks these.

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

## Checking a plugin

### tent_plugin_inspect

`tent_plugin_inspect` (built in `<build>/mooncake-transfer-engine/tent/tools/`
and installed to `bin/` by a full install) shows what TENT would make of
plugin files without loading them into an engine: it opens each file and
runs the checks that need no engine, including `probe()` (which gets no
topology or configuration here).

```bash
tent_plugin_inspect --dir <build>/tent_plugins
```

```text
FILE                       NAME      VERSION  BUILTIN  FLAGS                                   CAPS                                             ABI  FINGERPRINT                            RESULT
libtent_xport_loopback.so  loopback  1        -        same_machine|notification|cancellation  dram_to_dram                                     1/1  0x60248201715fecc0/0x60248201715fecc0  ok
libtent_xport_rdma.so      rdma      1        rdma     default_on|notification|cancellation    dram_to_dram|dram_to_gpu|gpu_to_dram|gpu_to_gpu  1/1  0x60248201715fecc0/0x60248201715fecc0  ok
libtent_xport_tcp.so       tcp       1        tcp      default_on|notification                 dram_to_dram|dram_to_gpu|gpu_to_dram|gpu_to_gpu  1/1  0x60248201715fecc0/0x60248201715fecc0  ok
```

ABI and fingerprint are shown as plugin/core. A rejected file shows the loader's
outcome and detail, for example `rejected_abi_version: plugin ABI v2 (80-byte
descriptor), core v1 (80-byte descriptor)` or `not_applicable: probe() returned
3: ...`. `--install-check` also loads the files that pass into one engine in the
tool's process, which runs `create()`, `install()` and the capability check (and
therefore starts an RPC server and opens the devices the transports use); a
plugin that overclaims its capabilities is then reported as
`rejected_caps_overclaim`. `MC_TENT_PLUGIN_DIR`, `MC_TENT_CONF` and
`MC_FORCE_TCP` do not apply to that engine. The engine searches the directories
as the loader does (the `--dir` directories in order, then those of the files
named), so another file of the same name in an earlier directory is loaded
instead, and the named file is reported as `rejected_duplicate_name`; a file the
engine did not report on is `not_checked`. `--json` prints the descriptor
fields, both ABI values, the outcome, its detail and the `probe()` result as
JSON; `--dir` lists other files as `ignored`; `--verbose` keeps what the plugins
and the engine log. The exit code is 0 when there are plugin files and every one
is `ok`, 1 otherwise, and 2 on a usage error.

### The conformance suite

`tent_transport_conformance_test` loads one plugin into two engines of one
process (p2p metadata on `127.0.0.1`) and checks what the engine relies on:

| Case | Checks |
|---|---|
| C1 | The descriptor's name, ABI version and fingerprint; installed capabilities within `declared_caps`, notification and cancellation support within the flags. |
| C2 | Engines constructed and destroyed three times; `create()` then `destroy()` without `install()`; `quiesce()` twice. |
| C3 | A registered buffer carries the plugin in the local descriptor and on the wire, and loses it when unregistered. |
| C4 | 8 x 4 MiB written and read back byte for byte; a submission with one request outside the target's registered memory is refused as a whole or fails only that task; the same sub-batch then takes valid requests. |
| C5 | Every task ends `COMPLETED` or `FAILED` within 10 s; a completed task moved its whole length, by the plugin's own status. |
| C6 | 16 notifications each way arrive in order within 10 s (transports that support notifications). |
| C7 | 64 x 16 MiB cancelled right after submission all end `CANCELED` or `COMPLETED` within 10 s (transports that support cancellation). |
| C8 | When the plugin starts refusing submissions, the engine fails the transfer over to another transport. |
| C9 | Both engines destroyed with 64 tasks in flight. |

C4, C5 and C7 to C9 move host memory and are skipped for a transport that
does not move DRAM to DRAM.

The plugin is chosen by the environment:

```bash
TENT_CONFORMANCE_PLUGIN=mine \
TENT_CONFORMANCE_PLUGIN_DIR=/path/to/plugins \
TENT_CONFORMANCE_CONFIG='{"transports/mine/some_option": 1}' \
  <build>/mooncake-transfer-engine/tent/tests/tent_transport_conformance_test
```

`TENT_CONFORMANCE_CONFIG` is a JSON object of configuration keys for both
engines (nested objects are merged key by key); objects under `"target"` and
`"initiator"` apply to one engine only. C8 fails over to TCP, or to SHM for
the TCP plugin itself (`TENT_CONFORMANCE_FALLBACK` names another transport);
the fallback must be available, as a built-in or a plugin in the same
directories. The suite clears `MC_TENT_PLUGIN_DIR`, `MC_TENT_CONF` and
`MC_FORCE_TCP`, which would override its configuration. A plugin whose
`probe()` declines the host is skipped.

The suite is not installed: it runs from a TENT build tree, against any plugin
directory, so a plugin built out of tree is tested the same way (the SDK test
runs it against the SDK example). The build registers it for loopback, and
for TCP and RDMA in builds where they are plugins (`TENT_XPORT_<NAME>=plugin`;
RDMA is skipped without an RDMA device). Loopback variants that each break one
rule (`badc4`, `badc5`, `badc5b`, `badc6`, `badc7`, `badc8`) check the suite
itself: each must fail its case with the message of that rule and pass the
others.

## Limitations

- **No unloading.** Replacing a plugin means restarting the process.
- **One TENT copy per process.** Only one TENT copy in a process takes part in
  plugins; another copy gets `host_symbols_not_exported`. In Python,
  `store.so` carries its own copy (see above).
- **A core loaded with `RTLD_LOCAL` joins the global scope.** When TENT's
  symbols are not already global and a plugin directory holds a candidate file,
  TENT promotes the object its core is in (the Python extension, or
  `libtransfer_engine.so` with `BUILD_SHARED_LIBS`) to global scope, so its
  exported symbols can take precedence over same-named symbols in libraries
  loaded afterwards.
- **`libmooncake_store.so`** (`WITH_STORE_C_SHARED`) hides TENT's symbols, so
  its engines cannot load plugins.
- **Per-device attributes** (`DeviceDesc::transport_attrs`) of plugins are not
  sent to peers; buffer and segment attributes are.
- **Built-in plugins come from the same build as the core**; a plugin left
  over from another build is not guaranteed to be detected.
- **Calls across objects** cost a little throughput: on RDMA, one thread
  with 128 KiB to 2 MiB transfers was 0.9-2.4% slower through the plugin.
- **`both` cannot prefer the plugin**: the built-in transport always wins.

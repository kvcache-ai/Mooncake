# TENT Transport Plugins

A transport plugin is a shared library that provides one of TENT's transports.
TENT loads plugins when an engine creates its transports, so a transport can be
built and shipped apart from the TENT library that uses it. Nothing is loaded
unless a plugin directory is set.

## Loading

TENT looks for plugins in the directory named by the `MC_TENT_PLUGIN_DIR`
environment variable or, if it is not set, by the `plugins/dir` configuration
key:

```json
{ "plugins": { "dir": "/opt/mooncake/lib/tent_plugins" } }
```

After creating its built-in transports, an engine opens every regular file in
that directory named `libtent_xport_<name>.so`, in file name order, and asks
each one for its transport. A plugin only fills an empty slot: when the core,
or a plugin earlier in the directory, already provides the transport type, the
plugin is skipped. A plugin from another build (another commit or another
compiler), or one without a build ID, stops the program with a fatal error that
shows both build IDs. Any other file that cannot be used is skipped instead of
failing the engine's construction. Each file gets one log line, for example:

```text
Transport plugin /opt/mooncake/lib/tent_plugins/libtent_xport_tcp.so loaded as tcp
Transport plugin /opt/mooncake/lib/tent_plugins/libtent_xport_rdma.so declined this engine
Transport plugin /opt/mooncake/lib/tent_plugins/libtent_xport_memcpy.so was built for 796fe57882aede3f798f49e89880aa1118edc61b GNU-11.4.0 cxx11abi=1, but this TENT is c8ed7e16d1ef4d1f8fe5a35e0c91d380a9c1f2f6 GNU-11.4.0 cxx11abi=1; rebuild the plugin against this TENT
```

## Writing a plugin

A plugin includes `tent/transport_plugin.h` and exports two functions:
`tent_transport_plugin_build_id()` returns `TENT_BUILD_ID`, and
`tent_register_transport_plugin()` returns a `TransportPluginDesc`:

- `type`: the `TransportType` the plugin provides.
- `create`: called once per engine with the engine's configuration and
  topology. It returns the transport, or `nullptr` when the plugin does not
  apply to that engine.

The TCP plugin (`TENT_XPORT_TCP=plugin`) is a typical entry point: its
`create()` declines when `transports/tcp/enable` is false, like the built-in
TCP.

```cpp
#include "tent/common/config.h"
#include "tent/transport/tcp/tcp_transport.h"
#include "tent/transport_plugin.h"

using namespace mooncake::tent;

static std::shared_ptr<Transport> create(const TransportPluginContext& ctx) {
    if (!ctx.conf->get("transports/tcp/enable", true)) return nullptr;
    return std::make_shared<TcpTransport>();
}

extern "C" const char* tent_transport_plugin_build_id() {
    return TENT_BUILD_ID;
}

extern "C" const TransportPluginDesc* tent_register_transport_plugin() {
    static const TransportPluginDesc desc{TCP, &create};
    return &desc;
}
```

`tent/build_id.h` is generated in TENT's build directory, so a plugin is built
against a configured TENT build.

The engine installs a plugin's transport like a built-in one; a transport
whose `install()` fails is skipped with the usual warning.

## Limitations

- **Same build.** A plugin uses TENT's C++ classes directly, so it only loads
  into a TENT built from the same source revision with the same compiler and
  C++ library ABI. `TENT_BUILD_ID` records the first two when CMake configures
  the build and is compared at load time; a mismatch stops the program with a
  fatal error, through `LOG(FATAL)` like TENT's other configuration errors. It
  does not see uncommitted changes, the revision is `unknown` outside a git
  checkout, and the comparison happens after the library is opened, when the
  plugin's static initializers have already run.
- **Existing types only.** A plugin provides one of the existing
  `TransportType` values. A new kind of transport still needs its enumerator
  added to TENT.
- **Never unloaded.** The transport objects, and the code that destroys them,
  live in the plugin, so TENT keeps every plugin it opened loaded.
- **Symbols.** A plugin resolves TENT's symbols in the process that loads it.
  An executable that links TENT statically must export them (`-rdynamic`, or
  `ENABLE_EXPORTS` in CMake). When the shared object that contains TENT was
  loaded with `RTLD_LOCAL`, as a Python extension is, TENT promotes that
  object to `RTLD_GLOBAL` before loading plugins; the object must export
  TENT's symbols. The promotion lasts for the rest of the process, so a module
  loaded later with its own copy of TENT may bind to the promoted one.

## Building TCP and RDMA as plugins

The CMake options `TENT_XPORT_TCP` and `TENT_XPORT_RDMA` choose how these two
transports are built:

- `static` (the default): inside TENT, as before.
- `plugin`: only as `libtent_xport_tcp.so` or `libtent_xport_rdma.so`, in
  `tent_plugins` of the build tree and installed to `lib/tent_plugins`. TENT
  itself then has no such transport, so an engine gets it only from the plugin
  directory. The plugin is created under the same conditions as the built-in
  transport: `transports/<name>/enable`, and for RDMA an RDMA NIC in the
  engine's topology. It carries the build ID of its build tree; a TENT from
  another build stops with a fatal error when it loads the plugin. Executables
  that CMake links with TENT statically get `-rdynamic` in this mode; others,
  such as the Rust bindings' binaries, must export TENT's symbols themselves.
  Plugin mode is exercised by TENT's own tests and `tent_python_guards_test`;
  the Store and integration tests assume TCP/RDMA inside TENT and are not run
  in plugin mode.

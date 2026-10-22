/*
 * Copyright 2025 KVCache.AI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Binary interface between the TENT core and a transport plugin.
 *
 * A transport plugin is a shared object named libtent_xport_<name>.so that
 * exports TENT_TRANSPORT_PLUGIN_ENTRY with C linkage. The core calls it once
 * with a zeroed descriptor whose struct_size and abi_version are preset; the
 * plugin fills in the remaining fields and returns 0.
 *
 * The descriptor itself is plain C, but create() hands back a C++ object
 * (mooncake::tent::Transport*), so a plugin must be built against the same
 * TENT headers, compiler and C++ standard library as the core. abi_version
 * and abi_fingerprint detect most mismatches; see tent/transport_plugin.hpp.
 *
 * Plugins are never unloaded; replacing one requires a process restart.
 */

#ifndef TENT_TRANSPORT_PLUGIN_H
#define TENT_TRANSPORT_PLUGIN_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Bump whenever a header that defines the plugin-visible ABI changes; see
 * tests/transport_abi_manifest.txt. */
#define TENT_TRANSPORT_PLUGIN_ABI_VERSION 1u

#define TENT_TRANSPORT_PLUGIN_ENTRY "tent_transport_plugin_register"
#define TENT_TRANSPORT_PLUGIN_FILE_PREFIX "libtent_xport_"
#define TENT_TRANSPORT_PLUGIN_FILE_SUFFIX ".so"
#define TENT_TRANSPORT_PLUGIN_HOST_ABI_SYMBOL "tent_transport_plugin_host_abi"

/* Return codes of the entry point. */
#define TENT_XPORT_REGISTER_OK 0
/* The core's descriptor is smaller than the plugin's: the plugin was built
 * against a newer ABI than the core provides. */
#define TENT_XPORT_REGISTER_E_STRUCT_SIZE (-2)

/* Capability bits, mirroring mooncake::tent::Capabilities. declared_caps is
 * an upper bound for what capabilities() may report after install(). */
#define TENT_XPORT_CAP_DRAM_TO_DRAM (1u << 0)
#define TENT_XPORT_CAP_DRAM_TO_GPU (1u << 1)
#define TENT_XPORT_CAP_GPU_TO_DRAM (1u << 2)
#define TENT_XPORT_CAP_GPU_TO_GPU (1u << 3)
#define TENT_XPORT_CAP_DRAM_TO_FILE (1u << 4)
#define TENT_XPORT_CAP_GPU_TO_FILE (1u << 5)

/* Flags. */
/* Default of transports/<name>/enable, also for a plugin that claims a
 * built-in slot. */
#define TENT_XPORT_FLAG_ENABLED_BY_DEFAULT (1u << 0)
/* Routing rules, applied to plugin slots only (a plugin that claims a
 * built-in slot keeps that slot's rules): */
/* only between segments on the same machine (like nvlink, shm, tpu); */
#define TENT_XPORT_FLAG_SAME_MACHINE_ONLY (1u << 1)
/* only for the local segment (like xpu); */
#define TENT_XPORT_FLAG_LOCAL_SEGMENT_ONLY (1u << 2)
/* may carry the host-to-host hop of a staged device transfer. */
#define TENT_XPORT_FLAG_HOST_STAGING_CAPABLE (1u << 3)
/* Upper bounds checked after install(), like declared_caps: */
/* supportNotification() may return true; */
#define TENT_XPORT_FLAG_NOTIFICATION (1u << 4)
/* supportsCancellation() may return true. */
#define TENT_XPORT_FLAG_CANCELLATION (1u << 5)

/* Filled by the core before probe() and create(). */
typedef struct tent_transport_plugin_host {
    uint32_t struct_size;     /* sizeof(tent_transport_plugin_host_t) */
    uint32_t abi_version;     /* core's TENT_TRANSPORT_PLUGIN_ABI_VERSION */
    uint64_t abi_fingerprint; /* core's tent_transport_abi_fingerprint() */
    /* Slot the transport must put in BufferDesc::transports. Valid in
     * create(); 0 (UNSPEC) in probe(). */
    int assigned_type;
    const char* plugin_name; /* canonical name, equals the descriptor name */
    /* const mooncake::tent::Topology*, may be NULL. */
    const void* topology;
    /* const mooncake::tent::Config*, may be NULL. */
    const void* config;
} tent_transport_plugin_host_t;

typedef struct tent_transport_plugin {
    /* Preset by the core to its sizeof(tent_transport_plugin_t); the plugin
     * overwrites it with its own size. */
    uint32_t struct_size;
    /* Preset by the core; overwritten with the plugin's build-time value. */
    uint32_t abi_version;
    uint64_t abi_fingerprint;
    /* [a-z0-9_]+, equal to <name> in libtent_xport_<name>.so. */
    const char* name;
    /* Free-form, shown in logs. */
    const char* version;
    /* 0 (UNSPEC) for a new transport; otherwise the built-in TransportType
     * this plugin provides (name must then be that transport's name). */
    int builtin_type;
    uint32_t flags;         /* TENT_XPORT_FLAG_* */
    uint32_t declared_caps; /* TENT_XPORT_CAP_* */
    /* Optional. 0 if the plugin can run on this host; nonzero to skip it
     * (reason via last_error). */
    int (*probe)(const tent_transport_plugin_host_t* host);
    /* Returns a new mooncake::tent::Transport*, or NULL (reason via
     * last_error). */
    void* (*create)(const tent_transport_plugin_host_t* host);
    /* Deletes an object returned by create(). */
    void (*destroy)(void* transport);
    /* Message for the last failed probe() or create() on this thread. */
    const char* (*last_error)(void);
} tent_transport_plugin_t;

typedef int (*tent_transport_plugin_register_fn)(tent_transport_plugin_t* out);

/* Exported by the core. Plugins do not call it; the loader uses it to check
 * that the core's symbols are visible to plugins, and tools use it to read
 * the core's ABI version and fingerprint. */
const tent_transport_plugin_host_t* tent_transport_plugin_host_abi(void);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* TENT_TRANSPORT_PLUGIN_H */

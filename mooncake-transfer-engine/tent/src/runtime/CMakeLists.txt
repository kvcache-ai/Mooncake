# Split runtime into two parts to avoid circular dependency:
# 1. tent_platform - Platform class (used by transports)
# 2. tent_runtime - Rest of runtime (depends on transports)

# Platform library - used by both transports and runtime
# In BUILTIN mode, include backend.cpp from platform/ for static linking
if(TENT_USE_PLUGINS)
    set(PLATFORM_SOURCES
        platform.cpp
        slab.cpp
        topology.cpp
        segment.cpp
        segment_manager.cpp
        segment_registry.cpp
        metastore.cpp
    )
else()
    set(PLATFORM_SOURCES
        platform.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../platform/backend.cpp
        slab.cpp
        topology.cpp
        segment.cpp
        segment_manager.cpp
        segment_registry.cpp
        metastore.cpp
    )
endif()

add_library(tent_platform STATIC ${PLATFORM_SOURCES})

# Add platform include directory (needed for backend.cpp in BUILTIN mode)
target_include_directories(tent_platform PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../../include)

# Metastore support
if (TARGET metastore_redis)
  add_definitions(-DUSE_REDIS)
  target_link_libraries(tent_platform PUBLIC metastore_redis)
endif()

if (TARGET metastore_http)
  add_definitions(-DUSE_HTTP)
  target_link_libraries(tent_platform PUBLIC metastore_http)
endif()

if (TARGET metastore_etcd)
  add_definitions(-DUSE_ETCD)
  target_link_libraries(tent_platform PUBLIC metastore_etcd)
endif()

target_link_libraries(tent_platform PUBLIC numa tent_common)

# ============================================================================
# Build mode configuration
# ============================================================================

if(TENT_USE_PLUGINS)
    target_compile_definitions(tent_platform PUBLIC TENT_PLUGIN_TRANSPORTS)
    message(STATUS "Platform: PLUGIN mode (dlopen loading)")
else()
    target_compile_definitions(tent_platform PUBLIC TENT_BUILTIN_TRANSPORTS)
    message(STATUS "Platform: BUILTIN mode (static linking)")

    # BUILTIN mode: statically link backend.cpp with appropriate platform support
    # Only one platform can be active in BUILTIN mode
    if(CUDAToolkit_FOUND OR USE_CUDA)
        target_compile_definitions(tent_platform PUBLIC USE_CUDA HAS_DEVICE_SUPPORT)
        target_include_directories(tent_platform PUBLIC ${CUDAToolkit_INCLUDE_DIRS})
        target_link_libraries(tent_platform PUBLIC CUDA::cudart CUDA::cuda_driver)
        message(STATUS "  BUILTIN platform backend: CUDA")
    elseif(USE_HIP)
        target_compile_definitions(tent_platform PUBLIC USE_HIP HAS_DEVICE_SUPPORT)
        target_link_libraries(tent_platform PUBLIC amdhip64)
        message(STATUS "  BUILTIN platform backend: HIP")
    elseif(USE_MUSA)
        target_compile_definitions(tent_platform PUBLIC USE_MUSA HAS_DEVICE_SUPPORT)
        target_link_libraries(tent_platform PUBLIC musa musart)
        message(STATUS "  BUILTIN platform backend: MUSA")
    elseif(USE_MACA)
        target_compile_definitions(tent_platform PUBLIC USE_MACA HAS_DEVICE_SUPPORT)
        target_link_libraries(tent_platform PUBLIC mcr)
        message(STATUS "  BUILTIN platform backend: MACA")
    elseif(USE_ASCEND OR USE_ASCEND_DIRECT)
        target_compile_definitions(tent_platform PUBLIC USE_ASCEND HAS_DEVICE_SUPPORT)
        target_link_libraries(tent_platform PUBLIC ascendcl)
        message(STATUS "  BUILTIN platform backend: Ascend")
    else()
        message(STATUS "  BUILTIN platform backend: CPU only (no GPU support)")
    endif()
endif()

# ============================================================================
# Runtime library implementation
# ============================================================================

add_library(tent_runtime_impl STATIC
    control_plane.cpp
    memory_prober.cpp
    proxy_manager.cpp
    segment_tracker.cpp
    transfer_engine_impl.cpp
    transport_loader.cpp
    transport_selector.cpp
)

# Define build mode for transport_loader.cpp
if(TENT_USE_PLUGINS)
    target_compile_definitions(tent_runtime_impl PUBLIC TENT_PLUGIN_TRANSPORTS)
    message(STATUS "Transport loader: PLUGIN mode (dlopen loading)")
else()
    target_compile_definitions(tent_runtime_impl PUBLIC TENT_BUILTIN_TRANSPORTS)
    message(STATUS "Transport loader: BUILTIN mode (static linking)")
endif()

target_link_libraries(tent_runtime_impl PUBLIC
    numa
    tent_common
    tent_rpc
    tent_transport_all
    tent_metrics
    tent_platform
)

# Combine into final tent_runtime alias library
add_library(tent_runtime ALIAS tent_runtime_impl)
add_library(tent_runtime_all INTERFACE)
target_link_libraries(tent_runtime_all INTERFACE tent_runtime_impl)

message(STATUS "Runtime library built, build mode: ${TENT_BUILD_MODE}")

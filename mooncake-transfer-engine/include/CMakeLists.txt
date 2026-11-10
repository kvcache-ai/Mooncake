configure_file(mooncake_te_build_config.h.in mooncake_te_build_config.h @ONLY)

install(FILES transfer_engine_c.h DESTINATION include)
install(FILES buffer_range_index.h DESTINATION include)
install(FILES common.h DESTINATION include)
install(FILES config.h DESTINATION include)
install(FILES error.h DESTINATION include)
install(FILES memory_location.h DESTINATION include)
install(FILES multi_transport.h DESTINATION include)
install(FILES topology.h DESTINATION include)
install(FILES transfer_engine.h DESTINATION include)
install(FILES transfer_metadata.h DESTINATION include)
install(FILES ub_allocator.h DESTINATION include)
install(FILES common/base/status.h DESTINATION include/common/base)
install(FILES transport/transport.h DESTINATION include/transport)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/mooncake_te_build_config.h
        DESTINATION include)

if(MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL)
  install(
    FILES
      ${CMAKE_CURRENT_SOURCE_DIR}/../src/common/adaptive_congestion_control/adaptive_congestion_control.h
    DESTINATION include)
endif()

# Device API headers (header-only, consumed by EP kernel)
install(FILES transport/device/device_transport.h
        DESTINATION include/transport/device)
install(FILES transport/device/device_ops.cuh
        DESTINATION include/transport/device)
install(FILES transport/device/comm_device.cuh
        DESTINATION include/transport/device)
install(FILES transport/device/p2p_device.cuh
        DESTINATION include/transport/device)
install(FILES transport/device/ibgda_device.cuh
        DESTINATION include/transport/device)
install(FILES transport/device/nccl_device_transport.h
        DESTINATION include/transport/device)
install(FILES transport/device/nccl_device.cuh
        DESTINATION include/transport/device)
install(FILES transport/device/cuda/cuda_ops.cuh
        DESTINATION include/transport/device/cuda)
install(FILES transport/device/musa/musa_ops.cuh
        DESTINATION include/transport/device/musa)
install(FILES transport/device/maca/maca_ops.cuh
        DESTINATION include/transport/device/maca)

# IBGDA library headers
install(
  DIRECTORY transport/device/ibgda/
  DESTINATION include/transport/device/ibgda
  FILES_MATCHING
  PATTERN "*.h")

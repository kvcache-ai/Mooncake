set(MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT
    mooncake_transfer_engine_headers)

install(FILES transfer_engine_c.h DESTINATION include
        COMPONENT ${MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT})
install(FILES buffer_range_index.h DESTINATION include
        COMPONENT ${MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT})
install(FILES common.h DESTINATION include
        COMPONENT ${MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT})
install(FILES config.h DESTINATION include
        COMPONENT ${MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT})
install(FILES error.h DESTINATION include
        COMPONENT ${MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT})
install(FILES memory_location.h DESTINATION include
        COMPONENT ${MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT})
install(FILES multi_transport.h DESTINATION include
        COMPONENT ${MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT})
install(FILES topology.h DESTINATION include
        COMPONENT ${MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT})
install(FILES task_congestion_status.h DESTINATION include
        COMPONENT ${MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT})
install(FILES transfer_engine.h DESTINATION include
        COMPONENT ${MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT})
install(FILES transfer_metadata.h DESTINATION include
        COMPONENT ${MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT})
install(FILES ub_allocator.h DESTINATION include
        COMPONENT ${MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT})
install(FILES common/base/status.h DESTINATION include/common/base
        COMPONENT ${MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT})
install(FILES transport/transport.h DESTINATION include/transport
        COMPONENT ${MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT})

if(MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL)
  install(
    FILES
      ${CMAKE_CURRENT_SOURCE_DIR}/../src/common/adaptive_congestion_control/adaptive_congestion_control.h
    DESTINATION include
    COMPONENT ${MOONCAKE_TRANSFER_ENGINE_HEADERS_COMPONENT})
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

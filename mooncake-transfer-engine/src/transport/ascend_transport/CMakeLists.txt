if(USE_ASCEND)
  file(GLOB ASCEND_SOURCES "hccl_transport/*.cpp")
  add_subdirectory(hccl_transport/ascend_transport_c)
elseif(USE_ASCEND_HETEROGENEOUS)
  file(GLOB ASCEND_SOURCES "heterogeneous_rdma_transport/*.cpp")
elseif(USE_UBSHMEM)
  file(GLOB ASCEND_SOURCES "ubshmem_transport/*.cpp" "*.cpp")
else()
  file(GLOB ASCEND_SOURCES "ascend_direct_transport/*.cpp" "*.cpp")
endif()

if(USE_ASCEND_DIRECT OR USE_UBSHMEM)
  set(ACL_RUNTIME_HEADER_PATH "${ASCEND_INCLUDE_DIR}/acl/acl_rt.h")
  if(EXISTS "${ACL_RUNTIME_HEADER_PATH}")
    file(READ "${ACL_RUNTIME_HEADER_PATH}" ACL_RUNTIME_CONTENT)
    if("${ACL_RUNTIME_CONTENT}" MATCHES "aclrtMemRetainAllocationHandle")
      message(STATUS "Ascend support fabric mem.")
      add_compile_definitions(ASCEND_SUPPORT_FABRIC_MEM)
    else()
      message(STATUS "Ascend do not support fabric mem.")
    endif()
  else()
    message(WARNING "Acl runtime header file is not exist")
  endif()
  add_library(ascend_transport SHARED ${ASCEND_SOURCES})
  find_library(
    FOUND_METADEF
    NAMES libmetadef.so
    PATHS ${ASCEND_LIB_DIR}
    NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)
  # QOS_ADXL_PROVIDER_BEGIN SDKs may expose ADXL directly in HIXL, or in
  # llm_datadist above HIXL. Recompute on every configure; never trust a cached
  # name-only selection.
  if(NOT CMAKE_NM)
    find_program(CMAKE_NM NAMES nm)
  endif()
  if(NOT CMAKE_NM)
    message(FATAL_ERROR "nm is required to verify the ADXL provider")
  endif()
  set(_qos_adxl_provider "")
  foreach(_qos_name cann_hixl llm_datadist)
    foreach(_qos_dir IN LISTS ASCEND_LIB_DIR)
      set(_qos_candidate "${_qos_dir}/lib${_qos_name}.so")
      if(EXISTS "${_qos_candidate}")
        execute_process(
          COMMAND "${CMAKE_NM}" -D -C --defined-only "${_qos_candidate}"
          RESULT_VARIABLE _qos_nm_rc
          OUTPUT_VARIABLE _qos_symbols
          ERROR_VARIABLE _qos_nm_error
          TIMEOUT 30)
        if(_qos_nm_rc STREQUAL "0"
           AND _qos_symbols MATCHES "[TW][ \t]+adxl::AdxlEngine::Initialize\\("
           AND _qos_symbols MATCHES
               "[TW][ \t]+adxl::AdxlEngine::TransferSync\\(")
          get_filename_component(_qos_adxl_provider "${_qos_candidate}"
                                 REALPATH)
          break()
        endif()
      endif()
    endforeach()
    if(_qos_adxl_provider)
      break()
    endif()
  endforeach()
  if(NOT _qos_adxl_provider)
    message(
      FATAL_ERROR
        "Neither HIXL nor llm_datadist in ${ASCEND_LIB_DIR} defines both ADXL Initialize and TransferSync"
    )
  endif()
  set(ASCEND_XFER_LIBRARY
      "${_qos_adxl_provider}"
      CACHE FILEPATH "Verified ADXL provider" FORCE)
  message(STATUS "QoS ADXL provider: ${ASCEND_XFER_LIBRARY}")
  # QOS_ADXL_PROVIDER_END
  find_library(
    ASCEND_ACL_LIBRARY
    NAMES acl_rt ascendcl
    PATHS ${ASCEND_LIB_DIR}
    NO_DEFAULT_PATH REQUIRED)
  if(FOUND_METADEF)
    message(STATUS "Found library: ${FOUND_METADEF}")
    target_link_libraries(
      ascend_transport PRIVATE ${ASCEND_ACL_LIBRARY} ${ASCEND_XFER_LIBRARY}
                               metadef)
  else()
    target_link_libraries(
      ascend_transport PRIVATE ${ASCEND_ACL_LIBRARY} ${ASCEND_XFER_LIBRARY}
                               graph_base graph)
  endif()
  install(TARGETS ascend_transport DESTINATION lib)
else()
  add_library(ascend_transport OBJECT ${ASCEND_SOURCES})
endif()

target_link_libraries(ascend_transport PRIVATE glog::glog)

if(USE_ASCEND)
  target_link_libraries(ascend_transport PRIVATE ascend_transport_mem)
endif()

if(USE_UBSHMEM)
  target_link_libraries(ascend_transport PRIVATE ascendcl)
endif()

set_target_properties(ascend_transport PROPERTIES PREFIX "")

target_compile_options(ascend_transport PRIVATE -O2 -Xlinker -export-dynamic)

target_link_options(ascend_transport PRIVATE -s)
mooncake_isolate_ascend_ge(ascend_transport)

include_guard(GLOBAL)

include(CMakeParseArguments)
include(FetchContent)

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()
if(POLICY CMP0169)
  cmake_policy(SET CMP0169 OLD)
endif()

set(MOONCAKE_DEPENDENCIES_DIR "${CMAKE_CURRENT_LIST_DIR}")
get_filename_component(MOONCAKE_SOURCE_ROOT
                       "${MOONCAKE_DEPENDENCIES_DIR}/../.." ABSOLUTE)
set(MOONCAKE_THIRD_PARTY_LICENSE_DIR "${CMAKE_BINARY_DIR}/third-party/licenses")
set(MOONCAKE_THIRD_PARTY_NOTICE
    "${CMAKE_BINARY_DIR}/third-party/THIRD-PARTY-NOTICES.txt")

function(mooncake_configure_bundled_target target)
  if(NOT TARGET "${target}")
    message(FATAL_ERROR "Bundled dependency target ${target} does not exist")
  endif()
  set_target_properties(
    "${target}"
    PROPERTIES POSITION_INDEPENDENT_CODE ON
               C_VISIBILITY_PRESET hidden
               CXX_VISIBILITY_PRESET hidden
               VISIBILITY_INLINES_HIDDEN ON)
endfunction()

function(mooncake_register_dependency)
  set(options)
  set(one_value_args NAME VERSION REVISION URL SHA256 SOURCE_DIR)
  set(multi_value_args LICENSE_FILES)
  cmake_parse_arguments(DEP "${options}" "${one_value_args}"
                        "${multi_value_args}" ${ARGN})

  foreach(required NAME VERSION REVISION URL SHA256 SOURCE_DIR)
    if(NOT DEP_${required})
      message(FATAL_ERROR "Dependency metadata is missing ${required}")
    endif()
  endforeach()
  if(NOT DEP_LICENSE_FILES)
    message(FATAL_ERROR "${DEP_NAME} does not declare a license file")
  endif()

  set(copied_licenses)
  foreach(license_file IN LISTS DEP_LICENSE_FILES)
    set(source_license "${DEP_SOURCE_DIR}/${license_file}")
    if(NOT EXISTS "${source_license}")
      message(
        FATAL_ERROR
          "${DEP_NAME} license file was not found in the verified source archive: ${source_license}"
      )
    endif()
    get_filename_component(license_name "${license_file}" NAME)
    set(license_destination "${MOONCAKE_THIRD_PARTY_LICENSE_DIR}/${DEP_NAME}")
    file(MAKE_DIRECTORY "${license_destination}")
    configure_file("${source_license}" "${license_destination}/${license_name}"
                   COPYONLY)
    list(APPEND copied_licenses "  - licenses/${DEP_NAME}/${license_name}\n")
  endforeach()

  string(JOIN "" license_lines ${copied_licenses})
  set(entry
      "Name: ${DEP_NAME}\nVersion: ${DEP_VERSION}\nRevision: ${DEP_REVISION}\nSource: ${DEP_URL}\nSHA256: ${DEP_SHA256}\nLicense files:\n${license_lines}\n"
  )
  set_property(GLOBAL APPEND PROPERTY MOONCAKE_THIRD_PARTY_ENTRIES "${entry}")
endfunction()

function(mooncake_finalize_third_party_notices)
  get_property(entries GLOBAL PROPERTY MOONCAKE_THIRD_PARTY_ENTRIES)
  if(NOT entries)
    message(
      FATAL_ERROR "No bundled dependency compliance metadata was registered")
  endif()

  get_filename_component(notice_dir "${MOONCAKE_THIRD_PARTY_NOTICE}" DIRECTORY)
  file(MAKE_DIRECTORY "${notice_dir}")
  file(
    WRITE "${MOONCAKE_THIRD_PARTY_NOTICE}"
    "Mooncake bundled third-party dependencies\n===========================================\n\n"
  )
  foreach(entry IN LISTS entries)
    file(APPEND "${MOONCAKE_THIRD_PARTY_NOTICE}" "${entry}")
  endforeach()

  install(FILES "${MOONCAKE_THIRD_PARTY_NOTICE}" DESTINATION share/mooncake)
  install(DIRECTORY "${MOONCAKE_THIRD_PARTY_LICENSE_DIR}/"
          DESTINATION share/mooncake/licenses)
endfunction()

function(mooncake_provide_core_dependencies)
  include("${MOONCAKE_DEPENDENCIES_DIR}/Asio.cmake")
  include("${MOONCAKE_DEPENDENCIES_DIR}/Gflags.cmake")
  include("${MOONCAKE_DEPENDENCIES_DIR}/Glog.cmake")
  include("${MOONCAKE_DEPENDENCIES_DIR}/JsonCpp.cmake")
  include("${MOONCAKE_DEPENDENCIES_DIR}/YamlCpp.cmake")
  include("${MOONCAKE_DEPENDENCIES_DIR}/Pybind11.cmake")
  include("${MOONCAKE_DEPENDENCIES_DIR}/Yalantinglibs.cmake")

  if(WITH_STORE)
    include("${MOONCAKE_DEPENDENCIES_DIR}/XxHash.cmake")
    include("${MOONCAKE_DEPENDENCIES_DIR}/Zstd.cmake")
    include("${MOONCAKE_DEPENDENCIES_DIR}/Liburing.cmake")
  elseif(USE_TENT)
    include("${MOONCAKE_DEPENDENCIES_DIR}/Liburing.cmake")
  endif()

  if(USE_REDIS OR STORE_USE_REDIS)
    include("${MOONCAKE_DEPENDENCIES_DIR}/Hiredis.cmake")
  endif()

  if(ENABLE_KV_EVENTS)
    include("${MOONCAKE_DEPENDENCIES_DIR}/LibZmq.cmake")
  endif()
endfunction()

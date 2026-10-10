# Checks that the headers defining the transport plugin ABI still match
# transport_abi_manifest.txt.
#
# cmake -DTENT_INCLUDE_DIR=<tent>/include -DMANIFEST=<manifest> -P <this>
#
# A mismatch means a plugin-visible header changed. If the change can affect a
# plugin (any type layout, virtual function, or the descriptor), bump
# TENT_TRANSPORT_PLUGIN_ABI_VERSION in tent/transport_plugin.h and refresh the
# manifest with -DUPDATE=ON. If it cannot (comments, new non-virtual helpers),
# refresh it with -DUPDATE=ON -DNO_ABI_CHANGE=ON instead.

if(NOT TENT_INCLUDE_DIR OR NOT MANIFEST)
  message(FATAL_ERROR "TENT_INCLUDE_DIR and MANIFEST are required")
endif()

file(READ ${TENT_INCLUDE_DIR}/tent/transport_plugin.h plugin_header)
string(REGEX MATCH "#define TENT_TRANSPORT_PLUGIN_ABI_VERSION ([0-9]+)u"
             version_match "${plugin_header}")
if(NOT version_match)
  message(FATAL_ERROR "TENT_TRANSPORT_PLUGIN_ABI_VERSION not found")
endif()
set(header_version ${CMAKE_MATCH_1})

file(STRINGS ${MANIFEST} manifest_lines REGEX "^[^#]")
set(manifest_version "")
set(headers "")
set(failures "")
foreach(line IN LISTS manifest_lines)
  if(line MATCHES "^abi_version ([0-9]+)$")
    set(manifest_version ${CMAKE_MATCH_1})
  elseif(line MATCHES "^([^ ]+) ([0-9a-f]+)$")
    set(path ${CMAKE_MATCH_1})
    set(expected ${CMAKE_MATCH_2})
    list(APPEND headers ${path})
    file(SHA256 ${TENT_INCLUDE_DIR}/${path} actual)
    if(NOT actual STREQUAL expected)
      list(APPEND failures "${path}")
    endif()
  endif()
endforeach()

if(UPDATE)
  if(failures
     AND manifest_version STREQUAL header_version
     AND NOT NO_ABI_CHANGE)
    message(
      FATAL_ERROR
        "ABI headers changed but TENT_TRANSPORT_PLUGIN_ABI_VERSION is still "
        "${header_version}: bump it, or pass -DNO_ABI_CHANGE=ON if no plugin "
        "can be affected")
  endif()
  file(READ ${MANIFEST} old_manifest)
  string(REGEX MATCH "^(#[^\n]*\n)*" preamble "${old_manifest}")
  set(content "${preamble}abi_version ${header_version}\n")
  foreach(path IN LISTS headers)
    file(SHA256 ${TENT_INCLUDE_DIR}/${path} digest)
    string(APPEND content "${path} ${digest}\n")
  endforeach()
  file(WRITE ${MANIFEST} "${content}")
  message(STATUS "Updated ${MANIFEST} for ABI version ${header_version}")
  return()
endif()

if(NOT manifest_version STREQUAL header_version)
  list(APPEND failures
       "abi_version: manifest ${manifest_version}, header ${header_version}")
endif()
if(failures)
  string(REPLACE ";" "\n  " failure_list "${failures}")
  message(
    FATAL_ERROR
      "Transport plugin ABI headers changed without updating "
      "${MANIFEST}:\n  ${failure_list}\n"
      "If plugins can be affected (type layout, virtual functions, the "
      "descriptor), bump TENT_TRANSPORT_PLUGIN_ABI_VERSION in "
      "tent/transport_plugin.h and rerun this check with -DUPDATE=ON; "
      "otherwise rerun it with -DUPDATE=ON -DNO_ABI_CHANGE=ON.")
endif()
message(STATUS "Transport plugin ABI v${header_version}: headers unchanged")

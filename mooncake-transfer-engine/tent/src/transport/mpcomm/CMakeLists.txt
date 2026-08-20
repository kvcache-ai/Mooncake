# Copyright 2026 KVCache.AI
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

# Built in every configuration. Only mpcomm_adapter.cpp includes mpcomm.h, and
# it compiles to an unavailable stub when the provider is absent; the remaining
# translation units reach MPComm solely through MpcommAdapter. That is what
# keeps the TENT-side logic - peer caching, endpoint parsing, request and
# completion mapping - unit-testable in a build without libmpcomm, the same
# arrangement TpuPjrtShim and UrmaAdapter use for their providers.
#
# When USE_MPCOMM is off the transport is not registered by transport_loader, so
# these objects are simply never pulled out of the archive.
add_library(
  tent_xport_mpcomm STATIC mpcomm_adapter.cpp mpcomm_peer_registry.cpp
                           mpcomm_task_mapping.cpp mpcomm_transport.cpp)
target_link_libraries(tent_xport_mpcomm PUBLIC tent_common tent_rpc)

if(USE_MPCOMM)
  # MPCOMM_INCLUDE_DIR / MPCOMM_LIBRARY are resolved in
  # mooncake-common/common.cmake.
  target_include_directories(tent_xport_mpcomm PRIVATE ${MPCOMM_INCLUDE_DIR})
  target_link_libraries(tent_xport_mpcomm PUBLIC ${MPCOMM_LIBRARY})
endif()

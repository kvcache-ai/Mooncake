#pragma once

#include <msgpack.hpp>
#include <ylt/util/tl/expected.hpp>

#include "local_ssd/persisted_state.h"
#include "serialize/serializer.h"

namespace mooncake::ha {

class LocalSsdCodec {
   public:
    static tl::expected<void, SerializationError> Encode(
        const LocalSsdPersistedState& state, MsgpackPacker& packer);

    static tl::expected<LocalSsdPersistedState, SerializationError> Decode(
        const msgpack::object* ld_value);
};

}  // namespace mooncake::ha

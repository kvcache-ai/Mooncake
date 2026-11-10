#pragma once

#include "client_liveness.h"
#include "types.h"

namespace mooncake {

// One published liveness transition. The session is the incarnation identity:
// an event for an old record must not be applied to a new registration with
// the same client ID.
struct ClientSessionEvent {
    UUID client_id;
    ClientSessionSharedPtr session;
    ClientLivenessState previous;
    ClientLivenessState current;
};

}  // namespace mooncake

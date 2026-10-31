#pragma once

namespace mooncake {

// How a key is locked: shared to read it, exclusive to write it.
enum class LockMode { kRead, kWrite };

}  // namespace mooncake

#ifndef MOONCAKE_PG_TORCH_UTILS_H
#define MOONCAKE_PG_TORCH_UTILS_H

#include <mooncake_pg.h>

#include <c10/util/Exception.h>

namespace mooncake {

inline void checkResult(mooncakePgResult_t result, const char* operation) {
    TORCH_CHECK(result == mooncakePgSuccess, operation,
                " failed: ", mooncakePgGetErrorString(result), ": ",
                mooncakePgGetLastError());
}

}  // namespace mooncake

#endif  // MOONCAKE_PG_TORCH_UTILS_H

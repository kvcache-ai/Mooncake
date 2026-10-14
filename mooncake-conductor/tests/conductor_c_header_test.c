// SPDX-License-Identifier: Apache-2.0
#include "conductor/client/conductor_c.h"

int main(void) {
    mooncake_conductor_client_t client = NULL;
    int32_t status = -1;
    if (mooncake_conductor_abi_version() != MOONCAKE_CONDUCTOR_ABI_VERSION)
        return 1;
    if (mooncake_conductor_create(&client) != MOONCAKE_CONDUCTOR_OK) return 2;
    if (mooncake_conductor_health_check(client, &status) != 0 || status != 1)
        return 3;
    if (mooncake_conductor_close(client) != 0 ||
        mooncake_conductor_close(client) != 0)
        return 4;
    return mooncake_conductor_destroy(client) != 0;
}

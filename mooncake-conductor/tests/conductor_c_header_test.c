// SPDX-License-Identifier: Apache-2.0
#include "conductor/client/conductor_c.h"

int main(void) {
    mooncake_conductor_t client = mooncake_conductor_create();
    int status = -1;
    if (!client) return 1;
    if (mooncake_conductor_health_check_ex(client, &status) != 0 || status != 1)
        return 3;
    mooncake_conductor_close(client);
    mooncake_conductor_close(client);
    mooncake_conductor_destroy(client);
    return 0;
}

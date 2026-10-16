#pragma once

namespace mooncake {

struct ClientAutoPortConfig {
    int max_retries = 20;
    int min_port = 12300;
    int max_port = 14300;

    static ClientAutoPortConfig FromEnvironment();
};

}  // namespace mooncake

#pragma once

namespace mooncake {

class Environ;

struct ClientAutoPortConfig {
    int max_retries = 20;
    int min_port = 12300;
    int max_port = 14300;

    static ClientAutoPortConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake

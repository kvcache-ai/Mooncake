# Bundled portable dependencies

Mooncake builds the following fixed upstream releases directly into its
artifacts. Each directory is a fixed source snapshot and retains the upstream
license text. The liburing snapshot additionally checks in the generated
configuration headers for Mooncake's libc-backed build; configure and build do
not download or generate source dependencies.

| Dependency | Version | Upstream | Archive SHA-256 | License |
| --- | --- | --- | --- | --- |
| xxHash | 0.8.3 | https://github.com/Cyan4973/xxHash | `aae608dfe8213dfd05d909a57718ef82f30722c392344583d3f39050c7f29a80` | BSD-2-Clause |
| Zstandard | 1.5.7 | https://github.com/facebook/zstd | `37d7284556b20954e56e1ca85b80226768902e2edabd3b649e9e72c0c9012ee3` | BSD-3-Clause |
| JsonCpp | 1.9.6 | https://github.com/open-source-parsers/jsoncpp | `f93b6dd7ce796b13d02c108bc9f79812245a82e577581c4c9aabe57075c90ea2` | MIT or public domain |
| yaml-cpp | 0.8.0 | https://github.com/jbeder/yaml-cpp | `fbe74bbdcee21d656715688706da3c8becfd946d92cd44705cc6098bb23b3a16` | MIT |
| standalone Asio | 1.30.2 | https://github.com/chriskohlhoff/asio | `755bd7f85a4b269c67ae0ea254907c078d408cce8e1a352ad2ed664d233780e8` | Boost Software License 1.0 |
| gflags | 2.3.0 | https://github.com/gflags/gflags | `f619a51371f41c0ad6837b2a98af9d4643b3371015d873887f7e8d3237320b2f` | BSD-3-Clause |
| glog | 0.7.1 | https://github.com/google/glog | `00e4a87e87b7e7612f519a41e491f16623b12423620006f59f5688bfd8d13b08` | BSD-3-Clause |
| hiredis | 1.4.0 | https://github.com/redis/hiredis | `5fa6e719e59cd4f8ae435c52a18ac4035d135251f9ee54e7a045bccf59107ed8` | BSD-3-Clause |
| liburing | 2.14 | https://github.com/axboe/liburing | `5f80964108981c6ad979c735f0b4877d5f49914c2a062f8e88282f26bf61de0c` | MIT |
| libzmq | 4.3.5 | https://github.com/zeromq/libzmq | `6c972d1e6a91a0ecd79c3236f04cf0126f2f4dfbbad407d72b4606a7ba93f9c6` | MPL-2.0 |

The hashes identify the exact GitHub tag archives used for the import.  CMake
never downloads these dependencies during configure or build.

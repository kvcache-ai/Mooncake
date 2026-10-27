// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Hardware-free unit tests for the ubmem peer-device-map configuration
// parsing (MC_UBMEM_PEER_DEV_MAP / MC_UBMEM_PEER_DEV_MAP_FILE). The helpers
// under test are pure functions over strings/JSON, so every environment
// shape — including the malformed ones the transport must survive — is
// covered without a shmdev or a cluster.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "ubmem_peer_map.h"

namespace mooncake {
namespace {

const char* kSepChar = "\x1f";

// lookup that fails the test instead of silently inserting a default
const std::string* lookup(const std::unordered_map<std::string, std::string>& m,
                          const std::string& host, const std::string& dev) {
    static const std::string kMissing = "<missing>";
    static thread_local std::string found;
    auto it = m.find(host + kSepChar + dev);
    if (it == m.end()) return &kMissing;
    found = it->second;
    return &found;
}

class TempFile {
   public:
    explicit TempFile(const std::string& content) {
        path_ =
            "/tmp/ubmem_peer_map_test_" + std::to_string(getpid()) + ".json";
        std::ofstream out(path_, std::ios::trunc);
        out << content;
    }
    ~TempFile() { std::remove(path_.c_str()); }
    const char* path() const { return path_.c_str(); }

   private:
    std::string path_;
};

TEST(PeerDevMapEnvTest, ParsesValidEntriesAndTrimsSpaces) {
    auto map = parsePeerDevMap(
        "hw01:/dev/obmm_shmdev13=/dev/obmm_shmdev5, hw02 : /dev/d2 : x, "
        "hw01:/dev/obmm_shmdev14=/dev/obmm_shmdev6");
    // " hw02 : /dev/d2 : x" has no '=' -> dropped; the rest survive trimming
    EXPECT_EQ(map.size(), 2u);
    EXPECT_STREQ(lookup(map, "hw01", "/dev/obmm_shmdev13")->c_str(),
                 "/dev/obmm_shmdev5");
    EXPECT_STREQ(lookup(map, "hw01", "/dev/obmm_shmdev14")->c_str(),
                 "/dev/obmm_shmdev6");
}

TEST(PeerDevMapEnvTest, SkipsMalformedEntries) {
    // every malformed shape, plus one valid entry among them
    auto map = parsePeerDevMap(
        "no_separator_at_all,=leading_eq,host:=empty_remote,"
        "host:/dev/d=,:/dev/d=/dev/m,:missing_all");
    EXPECT_TRUE(map.empty());

    map = parsePeerDevMap("bad1,bad2,hw01:/dev/d1=/dev/m1,trailing_bad");
    EXPECT_EQ(map.size(), 1u);
    EXPECT_STREQ(lookup(map, "hw01", "/dev/d1")->c_str(), "/dev/m1");
}

TEST(PeerDevMapEnvTest, EmptyOrNullSpecYieldsEmptyMap) {
    EXPECT_TRUE(parsePeerDevMap("").empty());
    EXPECT_TRUE(parsePeerDevMap(nullptr).empty());
}

TEST(PeerDevMapFileTest, KeepsOnlyEntriesForSelf) {
    TempFile file(
        "{"
        "\"hw01\": {"
        "\"/dev/obmm_shmdev13\": {\"hw02\": \"/dev/obmm_shmdev5\","
        "                       \"hw03\": \"/dev/obmm_shmdev7\"},"
        "\"/dev/obmm_shmdev14\": {\"hw03\": \"/dev/obmm_shmdev8\"}"
        "},"
        "\"hw02\": {\"/dev/obmm_shmdev5\": {\"hw01\": \"/dev/obmm_shmdev13\"}}"
        "}");
    // every node reads the same file; hw02 keeps only its own reader rows
    auto map = parsePeerDevMapFile(file.path(), {"hw02"});
    EXPECT_EQ(map.size(), 1u);
    EXPECT_STREQ(lookup(map, "hw01", "/dev/obmm_shmdev13")->c_str(),
                 "/dev/obmm_shmdev5");

    // a node matching several identities keeps one entry per source device:
    // each node mounts exactly one local mirror per peer device, so the
    // dev13 rows of hw02 and hw03 collapse into a single mapping
    map = parsePeerDevMapFile(file.path(), {"hw02", "hw03"});
    EXPECT_EQ(map.size(), 2u);
    EXPECT_STREQ(lookup(map, "hw01", "/dev/obmm_shmdev14")->c_str(),
                 "/dev/obmm_shmdev8");
}

TEST(PeerDevMapFileTest, PureExporterGetsNoMappings) {
    TempFile file(
        "{"
        "\"hw01\": {\"/dev/obmm_shmdev13\": {\"hw02\": \"/dev/obmm_shmdev5\"}}"
        "}");
    // hw01 appears only as a source host: it exports, imports nothing
    auto map = parsePeerDevMapFile(file.path(), {"hw01"});
    EXPECT_TRUE(map.empty());
}

TEST(PeerDevMapFileTest, BadReaderValueIsSkipped) {
    TempFile file(
        "{"
        "\"hw01\": {"
        "\"/dev/d1\": {\"hw02\": \"\"},"
        "\"/dev/d2\": {\"hw02\": \"/dev/m2\"}"
        "}"
        "}");
    auto map = parsePeerDevMapFile(file.path(), {"hw02"});
    EXPECT_EQ(map.size(), 1u);
    EXPECT_STREQ(lookup(map, "hw01", "/dev/d2")->c_str(), "/dev/m2");
}

TEST(PeerDevMapFileTest, BadStructureSkipsSubtree) {
    TempFile file(
        "{"
        "\"hw01\": \"/not/an/object\","
        "\"hw02\": {\"/dev/d1\": \"also_not_an_object\","
        "          \"/dev/d2\": {\"hw03\": \"/dev/m2\"}}"
        "}");
    auto map = parsePeerDevMapFile(file.path(), {"hw03"});
    EXPECT_EQ(map.size(), 1u);
    EXPECT_STREQ(lookup(map, "hw02", "/dev/d2")->c_str(), "/dev/m2");
}

TEST(PeerDevMapFileTest, MissingFileYieldsEmptyMap) {
    auto map = parsePeerDevMapFile("/nonexistent/ubmem_peer_map.json", {"hw"});
    EXPECT_TRUE(map.empty());
}

TEST(PeerDevMapFileTest, MalformedJsonYieldsEmptyMap) {
    TempFile file("{\"hw01\": {\"/dev/d1\": ");  // truncated JSON
    auto map = parsePeerDevMapFile(file.path(), {"hw02"});
    EXPECT_TRUE(map.empty());
}

TEST(HostOfTest, ExtractsHostFromEndpointShapes) {
    EXPECT_EQ(hostOf("hw01"), "hw01");
    EXPECT_EQ(hostOf("hw01:12345"), "hw01");
    EXPECT_EQ(hostOf("http://hw01:8080/metadata"), "hw01");
    EXPECT_EQ(hostOf(""), "");
}

}  // namespace
}  // namespace mooncake

/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <optional>
#include <string>
#include <vector>

struct ResponseCode {
  int32_t retcode;
  std::optional<std::string> error_message;
};

struct AliMessage {
  int32_t message_type;
  // uuid for tracking purpose etc (aka, request id)
  std::optional<std::string> session_no;
  std::optional<bool> tint_flag;
  std::optional<uint32_t> source_entity;
  std::optional<uint32_t> dest_entity;
  std::optional<std::string> client_ip;
  std::optional<ResponseCode> rc;
  std::optional<int32_t> version;
};

struct ValidateRequest {
  AliMessage msg;
  std::optional<int32_t> job_id;
  std::vector<std::string> query_keys;
  std::optional<bool> clean;
};
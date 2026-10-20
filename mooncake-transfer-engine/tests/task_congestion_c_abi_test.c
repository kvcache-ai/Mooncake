// Copyright 2026 KVCache.AI
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

#include "transfer_engine_c.h"

#ifdef USE_TENT
#include "tent/transfer_engine.h"
#endif

int main(void) {
    int state = TASK_CONGESTION_UNKNOWN;
    task_congestion_detail_t detail = {0};
    size_t required_path_length = 0;
    if (getTaskCongestionState(NULL, 0, 0, &state) != 1) return 1;
    if (getTaskCongestionDetail(NULL, 0, 0, &detail, NULL, 0,
                                &required_path_length) != 1)
        return 2;
#ifdef USE_TENT
    if (tent_task_congestion_state(NULL, 0, 0, &state) != -1) return 3;
    if (tent_task_congestion_detail(NULL, 0, 0, &detail, NULL, 0,
                                    &required_path_length) != -1)
        return 4;
#endif
    return 0;
}

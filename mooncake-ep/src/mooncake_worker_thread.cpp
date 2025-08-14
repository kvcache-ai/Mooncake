#include <thread>
#include <mooncake_worker.cuh>
#include <transfer_engine.h>

namespace mooncake {

void MooncakeWorker::initWorker() {
    std::thread([this] {
        while (true) {
            for (size_t i = 0; i < kNumTasks_; ++i) {
                auto &task = tasks_[i];
                if (task.status == READY) {
                    task.status = DONE;
                }
            }
        }
    }).detach();
}

}  // namespace mooncake
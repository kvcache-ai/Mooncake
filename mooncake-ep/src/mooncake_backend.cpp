#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <mooncake_backend.h>
#include <iostream>
#include <thread>
#include <future>

namespace torch {
namespace distributed {

MooncakeBackend::MooncakeBackend(c10::intrusive_ptr<::c10d::Store> store,
                                 int rank, int size,
                                 c10::intrusive_ptr<Options> options)
    : ::c10d::Backend(rank, size) {}

}  // namespace distributed
}  // namespace torch

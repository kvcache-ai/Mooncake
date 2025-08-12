#ifndef MOONCAKE_BACKEND_H
#define MOONCAKE_BACKEND_H

#include <torch/torch.h>
#include <torch/csrc/distributed/c10d/Backend.hpp>

namespace torch {
namespace distributed {

class MooncakeBackend : public ::c10d::Backend {
   public:
    MooncakeBackend(c10::intrusive_ptr<::c10d::Store> store, int rank, int size,
                    c10::intrusive_ptr<Options> options);
};

}  // namespace distributed
}  // namespace torch

#endif  // MOONCAKE_BACKEND_H

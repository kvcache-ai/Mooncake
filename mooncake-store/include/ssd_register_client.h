#include "master_client.h"
#include "types.h"
#include <string>

namespace mooncake {
   #define OPERATION_OK 0
   #define OPERATION_FAILED -1

class NoFRegisterClient {
   public:
      NoFRegisterClient();
      ~NoFRegisterClient();

      int set_register(const std::string &nqn, size_t nsid, const std::string &traddr, size_t trsvcid, uintptr_t base, size_t size, const std::string &master_server_addr);

      /**
       * @brief Unregister a NoF SSD segment by its te_endpoint
       * @param nqn NQN of the SSD
       * @param nsid Namespace ID
       * @param traddr Transport address
       * @param trsvcid Transport service ID
       * @param master_server_addr Master server address
       * @return int OPERATION_OK or OPERATION_FAILED
       */
      int set_unregister_by_endpoint(const std::string &nqn, size_t nsid, const std::string &traddr, size_t trsvcid, const std::string &master_server_addr);

   private:
      MasterClient master_client_;
   };
}

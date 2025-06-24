#pragma once

#include <glog/logging.h>

#include "types.h"

namespace mooncake {

/*
 * @brief A helper class for etcd operations.
 *        This class is used to handle the requests to the etcd cluster.
 *        All methods of this class are thread-safe.
 */
class EtcdHelper {
   public:
    /*
     * @brief Connect to the etcd store client. There is a global etcd client in
     * libetcd. It is used for all the etcd operations for mooncake-store. This
     * function ensures the client is only connected once.
     * @param etcd_endpoints: The endpoints of the etcd store client.
     *        Multiple endpoints are separated by semicolons.
     * @return: Error code.
     */
    static ErrorCode ConnectToEtcdStoreClient(
        const std::string& etcd_endpoints);

    /*
     * @brief Get the value of a key from the etcd.
     * @param key: The key to get the value of.
     * @param key_size: The size of the key in bytes.
     * @param value: Output param, the value of the key.
     * @param revision_id: Output param, the create revision id of the key.
     * @return: Error code.
     */
    static ErrorCode Get(const char* key, const size_t key_size,
                         std::string& value, EtcdRevisionId& revision_id);

    /*
     * @brief Create a key-value pair that binds to a given lease.
     * @param key: The key to create.
     * @param key_size: The size of the key in bytes.
     * @param value: The value to create.
     * @param value_size: The size of the value in bytes.
     * @param lease_id: The lease id to bind to the key.
     * @param revision_id: Output param, the create revision id of the key.
     * @return: Error code.
     */
    static ErrorCode CreateWithLease(const char* key, const size_t key_size,
                                     const char* value, const size_t value_size,
                                     EtcdLeaseId lease_id,
                                     EtcdRevisionId& revision_id);

    /*
     * @brief Grant a lease from the etcd.
     * @param lease_ttl: The ttl of the lease, in seconds.
     * @param lease_id: Output param, the lease id.
     * @return: Error code.
     */
    static ErrorCode GrantLease(int64_t lease_ttl, EtcdLeaseId& lease_id);

    /*
     * @brief Watch a key until it is deleted. This is a blocking function.
     * @param key: The key to watch.
     * @param key_size: The size of the key in bytes.
     * @return: Error code.
     */
    static ErrorCode WatchUntilDeleted(const char* key, const size_t key_size);

    /*
     * @brief Cancel watching a key
     * @param key: The key to cancel watch.
     * @param key_size: The size of the key in bytes.
     * @return: Error code.
     */
    static ErrorCode CancelWatch(const char* key, const size_t key_size);

    /*
     * @brief Keep a lease alive. This is a blocking function.
     * @param lease_id: The lease id to keep alive.
     * @return: Error code.
     */
    static ErrorCode KeepAlive(EtcdLeaseId lease_id);

    /*
     * @brief Cancel a lease keep alive. Returning error means
     *        the lease id does not exist or the goroutine to
     *        keep the lease alive is already closed.
     * @param lease_id: The lease id to cancel keep alive.
     * @return: Error code.
     */
    static ErrorCode CancelKeepAlive(EtcdLeaseId lease_id);

   private:
    // Variables that are used to ensure the etcd client
    // is only connected once.
    static std::string connected_endpoints_;
    static std::mutex etcd_mutex_;
    static bool etcd_connected_;
};

}  // namespace mooncake
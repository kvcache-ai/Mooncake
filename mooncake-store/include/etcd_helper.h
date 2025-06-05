#pragma once

#include <glog/logging.h>

#include "libetcd_wrapper.h"
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
     * @brief Connect to the etcd store client. There is a global etcd client in libetcd.
     *        It is used for all the etcd operations for mooncake-store. This function
     *        ensures the client is only connected once.
     * @param etcd_endpoints: The endpoints of the etcd store client.
     *        Multiple endpoints are separated by semicolons.
     * @return: Error code.
     */
    static ErrorCode ConnectToEtcdStoreClient(const std::string& etcd_endpoints);

    /*
     * @brief Get the value of a key from the etcd.
     * @param key: The key to get the value of.
     * @param key_size: The size of the key in bytes.
     * @param value: Output param, the value of the key.
     * @param revision_id: Output param, the create revision id of the key.
     * @return: Error code.
     */
    static ErrorCode EtcdGet(const char* key, const size_t key_size,
        std::string& value, GoInt64& revision_id);

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
    static ErrorCode EtcdCreateWithLease(const char* key, const size_t key_size,
        const char* value, const size_t value_size, GoInt64 lease_id, GoInt64& revision_id);

    /*
     * @brief Grant a lease from the etcd.
     * @param lease_ttl: The ttl of the lease.
     * @param lease_id: Output param, the lease id.
     * @return: Error code.
     */
    static ErrorCode EtcdGrantLease(int64_t lease_ttl, GoInt64& lease_id);

    /*
     * @brief Watch a key until it is deleted. This is blocking function.
     * @param key: The key to watch.
     * @param key_size: The size of the key in bytes.
     * @return: Error code.
     */
    static ErrorCode EtcdWatchUntilDeleted(const char* key, const size_t key_size);
    
    /*
     * @brief Keep a lease alive. This is blocking function.
     * @param lease_id: The lease id to keep alive.
     * @return: Error code.
    */
    static ErrorCode EtcdKeepAlive(int64_t lease_id);
private:
    // Variables that are used to ensure the etcd client
    // is only connected once.
    static std::string connected_endpoints_;
    static std::mutex etcd_mutex_;
    static bool etcd_connected_;
};

}  // namespace mooncake
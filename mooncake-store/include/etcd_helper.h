#pragma once

#include <glog/logging.h>
#include <string>
#include <vector>

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

    /*
     * @brief Put a key-value pair to etcd.
     * @param key: The key to put.
     * @param key_size: The size of the key in bytes.
     * @param value: The value to put.
     * @param value_size: The size of the value in bytes.
     * @return: Error code.
     */
    static ErrorCode Put(const char* key, const size_t key_size,
                         const char* value, const size_t value_size);

    /*
     * @brief Create a key-value pair in etcd if the key does not already exist.
     *        This is implemented via etcd transaction (CreateRevision == 0).
     * @return: OK on success; ETCD_TRANSACTION_FAIL if key already exists.
     */
    static ErrorCode Create(const char* key, const size_t key_size,
                            const char* value, const size_t value_size);

    /*
     * @brief Get all key-value pairs with a given prefix.
     * @param prefix: The prefix to search for.
     * @param prefix_size: The size of the prefix in bytes.
     * @param keys: Output param, vector of keys.
     * @param values: Output param, vector of values.
     * @return: Error code.
     */
    static ErrorCode GetWithPrefix(const char* prefix, const size_t prefix_size,
                                   std::vector<std::string>& keys,
                                   std::vector<std::string>& values);

    /*
     * @brief Range get in etcd and return result as a JSON array string.
     *        This avoids complex cross-language memory management for key/value
     * arrays.
     * @param start_key: Start key (inclusive).
     * @param start_key_size: Size in bytes.
     * @param end_key: End key (exclusive).
     * @param end_key_size: Size in bytes.
     * @param limit: Maximum number of kvs to return (0 means no limit).
     * @param json: Output JSON string, format: [{"key":"...","value":"..."}]
     * @param revision_id: Output etcd revision of this read
     * (resp.Header.Revision).
     */
    static ErrorCode GetRangeAsJson(const char* start_key,
                                    const size_t start_key_size,
                                    const char* end_key,
                                    const size_t end_key_size, size_t limit,
                                    std::string& json,
                                    EtcdRevisionId& revision_id);

    /*
     * @brief Get the first key with a given prefix (sorted by key).
     * @param prefix: The prefix to search for.
     * @param prefix_size: The size of the prefix in bytes.
     * @param first_key: Output param, the first key found.
     * @return: Error code. ETCD_KEY_NOT_EXIST if no key found.
     */
    static ErrorCode GetFirstKeyWithPrefix(const char* prefix,
                                           const size_t prefix_size,
                                           std::string& first_key);

    /*
     * @brief Get the last key with a given prefix (sorted by key descending).
     * @param prefix: The prefix to search for.
     * @param prefix_size: The size of the prefix in bytes.
     * @param last_key: Output param, the last key found.
     * @return: Error code. ETCD_KEY_NOT_EXIST if no key found.
     */
    static ErrorCode GetLastKeyWithPrefix(const char* prefix,
                                          const size_t prefix_size,
                                          std::string& last_key);

    /*
     * @brief Delete a range of keys from etcd.
     * @param start_key: The start key (inclusive).
     * @param start_key_size: The size of the start key in bytes.
     * @param end_key: The end key (exclusive).
     * @param end_key_size: The size of the end key in bytes.
     * @return: Error code.
     */
    static ErrorCode DeleteRange(const char* start_key,
                                 const size_t start_key_size,
                                 const char* end_key,
                                 const size_t end_key_size);

    /*
     * @brief Watch all keys with a given prefix for changes.
     *        This is a non-blocking function that starts watching in a
     * background goroutine. Events are delivered via the callback function.
     * @param prefix: The prefix to watch.
     * @param prefix_size: The size of the prefix in bytes.
     * @param callback_context: User context passed to the callback function.
     * @param callback_func: Callback function called for each watch event.
     *        Signature: void callback(void* context, const char* key, size_t
     * key_size, const char* value, size_t value_size, int event_type)
     *        event_type: 0 = PUT, 1 = DELETE
     * @return: Error code.
     */
    static ErrorCode WatchWithPrefix(const char* prefix,
                                     const size_t prefix_size,
                                     void* callback_context,
                                     void (*callback_func)(void*, const char*,
                                                           size_t, const char*,
                                                           size_t, int));

    /*
     * @brief Watch all keys with a given prefix from a specific etcd revision.
     *        Callback includes `mod_revision` for precise resume.
     *        (Implementation may pass max(event.ModRevision,
     * watchResp.Header.Revision).)
     * @param callback_func: void cb(void* ctx, const char* key, size_t
     * key_size, const char* value, size_t value_size, int event_type, int64_t
     * mod_revision) event_type: 0=PUT, 1=DELETE, 2=WATCH_BROKEN (watch ended;
     * reconnect)
     */
    static ErrorCode WatchWithPrefixFromRevision(
        const char* prefix, const size_t prefix_size,
        EtcdRevisionId start_revision, void* callback_context,
        void (*callback_func)(void*, const char*, size_t, const char*, size_t,
                              int, int64_t));

    /*
     * @brief Cancel watching a prefix.
     * @param prefix: The prefix to stop watching.
     * @param prefix_size: The size of the prefix in bytes.
     * @return: Error code.
     */
    static ErrorCode CancelWatchWithPrefix(const char* prefix,
                                           const size_t prefix_size);

    /*
     * @brief Wait until a prefix watch goroutine fully exits (no more
     * callbacks). This should be used after CancelWatchWithPrefix to avoid
     * shutdown races.
     * @param timeout_ms: Wait timeout in milliseconds.
     */
    static ErrorCode WaitWatchWithPrefixStopped(const char* prefix,
                                                const size_t prefix_size,
                                                int timeout_ms);

   private:
    // Variables that are used to ensure the etcd client
    // is only connected once.
    static std::string connected_endpoints_;
    static std::mutex etcd_mutex_;
    static bool etcd_connected_;
};

}  // namespace mooncake

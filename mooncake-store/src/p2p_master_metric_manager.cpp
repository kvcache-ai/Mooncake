#include "p2p_master_metric_manager.h"

#include <new>
#include <sstream>  // For string building during serialization

namespace mooncake {

P2PMasterMetricManager& P2PMasterMetricManager::instance() {
    static P2PMasterMetricManager static_instance;
    return static_instance;
}

P2PMasterMetricManager::P2PMasterMetricManager()
    : get_write_route_requests_("master_get_write_route_requests_total",
                                "Total number of get write route requests"),
      get_write_route_failures_(
          "master_get_write_route_failures_total",
          "Total number of failed get write route requests"),
      add_replica_requests_("master_add_replica_requests_total",
                            "Total number of add replica requests"),
      add_replica_failures_("master_add_replica_failures_total",
                            "Total number of failed add replica requests"),
      remove_replica_requests_("master_remove_replica_requests_total",
                               "Total number of remove replica requests"),
      remove_replica_failures_(
          "master_remove_replica_failures_total",
          "Total number of failed remove replica requests"),

      batch_remove_replica_requests_(
          "master_batch_remove_replica_requests_total",
          "Total number of BatchRemoveReplica requests received"),
      batch_remove_replica_failures_(
          "master_batch_remove_replica_failures_total",
          "Total number of failed BatchRemoveReplica requests"),
      batch_remove_replica_partial_successes_(
          "master_batch_remove_replica_partial_successes_total",
          "Total number of partially successful BatchRemoveReplica requests"),
      batch_remove_replica_items_(
          "master_batch_remove_replica_items_total",
          "Total number of items processed in BatchRemoveReplica requests"),
      batch_remove_replica_failed_items_(
          "master_batch_remove_replica_failed_items_total",
          "Total number of failed items in BatchRemoveReplica requests"),
      batch_get_write_route_requests_(
          "master_batch_get_write_route_requests_total",
          "Total number of BatchGetWriteRoute requests received"),
      batch_get_write_route_failures_(
          "master_batch_get_write_route_failures_total",
          "Total number of failed BatchGetWriteRoute requests"),
      batch_get_write_route_partial_successes_(
          "master_batch_get_write_route_partial_successes_total",
          "Total number of partially successful BatchGetWriteRoute requests"),
      batch_get_write_route_items_(
          "master_batch_get_write_route_items_total",
          "Total number of items processed in BatchGetWriteRoute requests"),
      batch_get_write_route_failed_items_(
          "master_batch_get_write_route_failed_items_total",
          "Total number of failed items in BatchGetWriteRoute requests") {
    update_arch_metrics_for_zero_output();
    RegisterInstance(this);
}

void P2PMasterMetricManager::reset_all_metrics() {
    this->~P2PMasterMetricManager();
    new (this) P2PMasterMetricManager();
}

void P2PMasterMetricManager::update_arch_metrics_for_zero_output() {
    // inc(0) marks metrics changed so zeros serialize.
    get_write_route_requests_.inc(0);
    get_write_route_failures_.inc(0);
    add_replica_requests_.inc(0);
    add_replica_failures_.inc(0);
    remove_replica_requests_.inc(0);
    remove_replica_failures_.inc(0);

    batch_remove_replica_requests_.inc(0);
    batch_remove_replica_failures_.inc(0);
    batch_remove_replica_partial_successes_.inc(0);
    batch_remove_replica_items_.inc(0);
    batch_remove_replica_failed_items_.inc(0);
    batch_get_write_route_requests_.inc(0);
    batch_get_write_route_failures_.inc(0);
    batch_get_write_route_partial_successes_.inc(0);
    batch_get_write_route_items_.inc(0);
    batch_get_write_route_failed_items_.inc(0);
}

// Operation Statistics (Counters)
void P2PMasterMetricManager::inc_get_write_route_requests(int64_t val) {
    get_write_route_requests_.inc(val);
}
void P2PMasterMetricManager::inc_get_write_route_failures(int64_t val) {
    get_write_route_failures_.inc(val);
}
void P2PMasterMetricManager::inc_add_replica_requests(int64_t val) {
    add_replica_requests_.inc(val);
}
void P2PMasterMetricManager::inc_add_replica_failures(int64_t val) {
    add_replica_failures_.inc(val);
}
void P2PMasterMetricManager::inc_remove_replica_requests(int64_t val) {
    remove_replica_requests_.inc(val);
}
void P2PMasterMetricManager::inc_remove_replica_failures(int64_t val) {
    remove_replica_failures_.inc(val);
}

// Batch Operation Statistics (Counters)
void P2PMasterMetricManager::inc_batch_remove_replica_requests(int64_t items) {
    batch_remove_replica_requests_.inc(1);
    batch_remove_replica_items_.inc(items);
}
void P2PMasterMetricManager::inc_batch_remove_replica_failures(
    int64_t failed_items) {
    batch_remove_replica_failures_.inc(1);
    batch_remove_replica_failed_items_.inc(failed_items);
}
void P2PMasterMetricManager::inc_batch_remove_replica_partial_success(
    int64_t failed_items) {
    batch_remove_replica_partial_successes_.inc(1);
    batch_remove_replica_failed_items_.inc(failed_items);
}
void P2PMasterMetricManager::inc_batch_get_write_route_requests(int64_t items) {
    batch_get_write_route_requests_.inc(1);
    batch_get_write_route_items_.inc(items);
}
void P2PMasterMetricManager::inc_batch_get_write_route_failures(
    int64_t failed_items) {
    batch_get_write_route_failures_.inc(1);
    batch_get_write_route_failed_items_.inc(failed_items);
}
void P2PMasterMetricManager::inc_batch_get_write_route_partial_success(
    int64_t failed_items) {
    batch_get_write_route_partial_successes_.inc(1);
    batch_get_write_route_failed_items_.inc(failed_items);
}

// Operation Statistics Getters
int64_t P2PMasterMetricManager::get_get_write_route_requests() {
    return get_write_route_requests_.value();
}
int64_t P2PMasterMetricManager::get_get_write_route_failures() {
    return get_write_route_failures_.value();
}
int64_t P2PMasterMetricManager::get_add_replica_requests() {
    return add_replica_requests_.value();
}
int64_t P2PMasterMetricManager::get_add_replica_failures() {
    return add_replica_failures_.value();
}
int64_t P2PMasterMetricManager::get_remove_replica_requests() {
    return remove_replica_requests_.value();
}
int64_t P2PMasterMetricManager::get_remove_replica_failures() {
    return remove_replica_failures_.value();
}

// Batch Operation Statistics Getters
int64_t P2PMasterMetricManager::get_batch_remove_replica_requests() {
    return batch_remove_replica_requests_.value();
}

int64_t P2PMasterMetricManager::get_batch_remove_replica_failures() {
    return batch_remove_replica_failures_.value();
}

int64_t P2PMasterMetricManager::get_batch_remove_replica_partial_successes() {
    return batch_remove_replica_partial_successes_.value();
}

int64_t P2PMasterMetricManager::get_batch_remove_replica_items() {
    return batch_remove_replica_items_.value();
}

int64_t P2PMasterMetricManager::get_batch_remove_replica_failed_items() {
    return batch_remove_replica_failed_items_.value();
}

int64_t P2PMasterMetricManager::get_batch_get_write_route_requests() {
    return batch_get_write_route_requests_.value();
}

int64_t P2PMasterMetricManager::get_batch_get_write_route_failures() {
    return batch_get_write_route_failures_.value();
}

int64_t P2PMasterMetricManager::get_batch_get_write_route_partial_successes() {
    return batch_get_write_route_partial_successes_.value();
}

int64_t P2PMasterMetricManager::get_batch_get_write_route_items() {
    return batch_get_write_route_items_.value();
}

int64_t P2PMasterMetricManager::get_batch_get_write_route_failed_items() {
    return batch_get_write_route_failed_items_.value();
}

// --- Serialization ---
std::string P2PMasterMetricManager::serialize_arch_metrics() {
    std::stringstream ss;

    auto serialize_metric = [&ss](auto& metric) {
        std::string metric_str;
        metric.serialize(metric_str);
        ss << metric_str;
    };

    serialize_metric(get_write_route_requests_);
    serialize_metric(get_write_route_failures_);
    serialize_metric(add_replica_requests_);
    serialize_metric(add_replica_failures_);
    serialize_metric(remove_replica_requests_);
    serialize_metric(remove_replica_failures_);

    serialize_metric(batch_remove_replica_requests_);
    serialize_metric(batch_remove_replica_failures_);
    serialize_metric(batch_remove_replica_partial_successes_);
    serialize_metric(batch_remove_replica_items_);
    serialize_metric(batch_remove_replica_failed_items_);
    serialize_metric(batch_get_write_route_requests_);
    serialize_metric(batch_get_write_route_failures_);
    serialize_metric(batch_get_write_route_partial_successes_);
    serialize_metric(batch_get_write_route_items_);
    serialize_metric(batch_get_write_route_failed_items_);

    return ss.str();
}

// --- Human-Readable Summary ---
std::string P2PMasterMetricManager::get_arch_summary_string(
    const std::string& shared_summary) {
    std::string summary = "[Arch: P2P] ";
    summary += shared_summary;
    std::stringstream ss;

    int64_t get_write_routes = get_write_route_requests_.value();
    int64_t get_write_route_fails = get_write_route_failures_.value();
    int64_t add_replicas = add_replica_requests_.value();
    int64_t add_replica_fails = add_replica_failures_.value();
    int64_t remove_replicas = remove_replica_requests_.value();
    int64_t remove_replica_fails = remove_replica_failures_.value();

    int64_t batch_get_write_route_requests =
        batch_get_write_route_requests_.value();
    int64_t batch_get_write_route_fails =
        batch_get_write_route_failures_.value();
    int64_t batch_get_write_route_partial_successes =
        batch_get_write_route_partial_successes_.value();
    int64_t batch_remove_replica_requests =
        batch_remove_replica_requests_.value();
    int64_t batch_remove_replica_fails = batch_remove_replica_failures_.value();
    int64_t batch_remove_replica_partial_successes =
        batch_remove_replica_partial_successes_.value();
    int64_t batch_remove_replica_items = batch_remove_replica_items_.value();
    int64_t batch_remove_replica_failed_items =
        batch_remove_replica_failed_items_.value();

    ss << "Requests (Success/Total): ";
    ss << "GetWriteRoute=" << get_write_routes - get_write_route_fails << "/"
       << get_write_routes << ", ";
    ss << "AddReplica=" << add_replicas - add_replica_fails << "/"
       << add_replicas << ", ";
    ss << "RemoveReplica=" << remove_replicas - remove_replica_fails << "/"
       << remove_replicas;

    ss << " | Batch Requests "
          "(Req=Success/PartialSuccess/Total, Item=Success/Total): ";
    ss << "GetWriteRoute:(Req="
       << batch_get_write_route_requests - batch_get_write_route_fails -
              batch_get_write_route_partial_successes
       << "/" << batch_get_write_route_partial_successes << "/"
       << batch_get_write_route_requests << "), ";
    ss << "RemoveReplica:(Req="
       << batch_remove_replica_requests - batch_remove_replica_fails -
              batch_remove_replica_partial_successes
       << "/" << batch_remove_replica_partial_successes << "/"
       << batch_remove_replica_requests << ", Item="
       << batch_remove_replica_items - batch_remove_replica_failed_items << "/"
       << batch_remove_replica_items << ")";

    summary += " | ";
    summary += ss.str();
    return summary;
}

}  // namespace mooncake

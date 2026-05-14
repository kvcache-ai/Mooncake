#include "urma_api.h"
#include <map>
#include <vector>
#include <cstring>
#include <mutex>

namespace {
std::mutex mock_mutex;
bool initialized = false;
std::vector<urma_device_t *> device_list;
std::map<urma_context_t *, int> context_map;
std::map<urma_jfce_t *, int> jfce_map;
std::map<urma_jfc_t *, std::vector<uint64_t>> jfc_user_ctx_map;
std::map<urma_jfr_t *, int> jfr_map;
std::map<urma_target_seg_t *, int> seg_map;
std::map<urma_jetty_t *, int> jetty_map;
std::map<urma_target_jetty_t *, int> target_jetty_map;

urma_device_attr_t mock_device_attr = {
    .guid = {.raw = {10}},
    .dev_cap = {},
    .port_cnt = 1,
    .port_attr = {{.max_mtu = URMA_MTU_4096,
                   .state = URMA_PORT_ACTIVE,
                   .active_width = URMA_LINK_X1,
                   .active_speed = URMA_SP_100G,
                   .active_mtu = URMA_MTU_4096}},
    .reserved_jetty_id_min = 0,
    .reserved_jetty_id_max = 1024};

urma_eid_info_t mock_eid_info = {
    .eid = {{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B,
             0x0C, 0x0D, 0x0E, 0x0F, 0x10}},
    .eid_index = 0};
}  // namespace

urma_status_t urma_init(urma_init_attr_t *init_attr) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (initialized) {
        return URMA_EEXIST;
    }
    initialized = true;
    return URMA_SUCCESS;
}

urma_status_t urma_uninit(void) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    initialized = false;
    for (auto device : device_list) {
        delete device;
    }
    device_list.clear();
    context_map.clear();
    jfce_map.clear();
    jfc_user_ctx_map.clear();
    jfr_map.clear();
    seg_map.clear();
    jetty_map.clear();
    target_jetty_map.clear();
    return URMA_SUCCESS;
}

urma_device_t **urma_get_device_list(int *num_devices) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!initialized) {
        *num_devices = 0;
        return nullptr;
    }

    if (device_list.empty()) {
        urma_device_t *device = new urma_device_t;
        strcpy(device->name, "mock_urma_device");
        strcpy(device->path, "/sys/class/infiniband/mock_device");
        device->type = URMA_TRANSPORT_UB;
        device->ops = nullptr;
        device->sysfs_dev = nullptr;
        device_list.push_back(device);
    }

    *num_devices = device_list.size();
    urma_device_t **devices = new urma_device_t *[device_list.size()];
    for (size_t i = 0; i < device_list.size(); ++i) {
        devices[i] = device_list[i];
    }
    return devices;
}

urma_device_t *urma_get_device_by_name(const char *name) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!initialized) {
        return nullptr;
    }

    if (device_list.empty()) {
        auto *device = new urma_device_t;
        strcpy(device->name, "mock_urma_device");
        strcpy(device->path, "/sys/class/infiniband/mock_device");
        device->type = URMA_TRANSPORT_UB;
        device->ops = nullptr;
        device->sysfs_dev = nullptr;
        device_list.push_back(device);
    }

    for (auto device : device_list) {
        if (strcmp(device->name, name) == 0) {
            return device;
        }
    }

    return device_list.empty() ? nullptr : device_list[0];
}

void urma_free_device_list(urma_device_t **device_list) {
    if (device_list) {
        delete[] device_list;
    }
}

urma_status_t urma_query_device(urma_device_t *device,
                                urma_device_attr_t *attr) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!device || !attr) {
        return URMA_EINVAL;
    }
    mock_device_attr.dev_cap.max_jfc = 1024;
    mock_device_attr.dev_cap.max_jetty = 1024;
    memcpy(attr, &mock_device_attr, sizeof(urma_device_attr_t));
    return URMA_SUCCESS;
}

urma_eid_info_t *urma_get_eid_list(urma_device_t *device, uint32_t *eid_cnt) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!device || !eid_cnt) {
        return nullptr;
    }
    *eid_cnt = 1;
    auto *eid_list = new urma_eid_info_t[1];
    memcpy(eid_list, &mock_eid_info, sizeof(urma_eid_info_t));
    return eid_list;
}

void urma_free_eid_list(urma_eid_info_t *eid_list) {
    if (eid_list) {
        delete[] eid_list;
    }
}

urma_context_t *urma_create_context(urma_device_t *device, uint32_t eid_index) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!device) {
        return nullptr;
    }
    urma_context_t *ctx = new urma_context_t;
    ctx->async_fd = 0;
    ctx->dev = device;
    context_map[ctx] = 1;
    return ctx;
}

urma_status_t urma_delete_context(urma_context_t *ctx) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!ctx || context_map.find(ctx) == context_map.end()) {
        return URMA_EINVAL;
    }
    context_map.erase(ctx);
    delete ctx;
    return URMA_SUCCESS;
}

urma_jfce_t *urma_create_jfce(urma_context_t *ctx) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!ctx || context_map.find(ctx) == context_map.end()) {
        return nullptr;
    }
    urma_jfce_t *jfce = reinterpret_cast<urma_jfce_t *>(new int(1));
    jfce_map[jfce] = 1;
    return jfce;
}

urma_status_t urma_delete_jfce(urma_jfce_t *jfce) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!jfce || jfce_map.find(jfce) == jfce_map.end()) {
        return URMA_EINVAL;
    }
    jfce_map.erase(jfce);
    delete reinterpret_cast<int *>(jfce);
    return URMA_SUCCESS;
}

urma_jfc_t *urma_create_jfc(urma_context_t *ctx, urma_jfc_cfg_t *cfg) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!ctx || !cfg || context_map.find(ctx) == context_map.end()) {
        return nullptr;
    }
    urma_jfc_t *jfc = new urma_jfc_t;
    memset(&jfc->jfc_id.eid, 0, sizeof(urma_eid_t));
    jfc->jfc_id.eid.raw[0] = 1;
    jfc->jfc_id.uasid = 0;
    jfc->jfc_id.id = 1;
    jfc->handle = cfg->user_ctx;
    jfc->comp_events_acked = 0;
    jfc->async_events_acked = 0;
    jfc->jfc_cfg = *cfg;
    jfc_user_ctx_map[jfc] = std::vector<uint64_t>();
    return jfc;
}

urma_status_t urma_delete_jfc(urma_jfc_t *jfc) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!jfc || jfc_user_ctx_map.find(jfc) == jfc_user_ctx_map.end()) {
        return URMA_EINVAL;
    }
    jfc_user_ctx_map.erase(jfc);
    delete jfc;
    return URMA_SUCCESS;
}

urma_jfr_t *urma_create_jfr(urma_context_t *ctx, urma_jfr_cfg_t *cfg) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!ctx || !cfg || context_map.find(ctx) == context_map.end()) {
        return nullptr;
    }
    urma_jfr_t *jfr = reinterpret_cast<urma_jfr_t *>(new int(1));
    jfr_map[jfr] = 1;
    return jfr;
}

urma_status_t urma_delete_jfr(urma_jfr_t *jfr) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!jfr || jfr_map.find(jfr) == jfr_map.end()) {
        return URMA_EINVAL;
    }
    jfr_map.erase(jfr);
    delete reinterpret_cast<int *>(jfr);
    return URMA_SUCCESS;
}

urma_target_seg_t *urma_register_seg(urma_context_t *ctx, urma_seg_cfg_t *cfg) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!ctx || !cfg || context_map.find(ctx) == context_map.end()) {
        return nullptr;
    }
    urma_target_seg_t *seg = new urma_target_seg_t;
    memset(&seg->seg.ubva.eid, 0, sizeof(urma_eid_t));
    seg->seg.ubva.eid.raw[0] = 1;
    seg->seg.ubva.uasid = 0;
    seg->seg.ubva.va = cfg->va;
    seg->seg.len = cfg->len;
    seg->seg.token_id = cfg->token_value.token;
    seg_map[seg] = 1;
    return seg;
}

urma_status_t urma_unregister_seg(urma_target_seg_t *seg) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!seg || seg_map.find(seg) == seg_map.end()) {
        return URMA_EINVAL;
    }
    seg_map.erase(seg);
    delete seg;
    return URMA_SUCCESS;
}

urma_target_seg_t *urma_import_seg(urma_context_t *ctx, urma_seg_t *seg,
                                   urma_token_t *token_value, uint64_t addr,
                                   urma_import_seg_flag_t flag) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!ctx || !seg || !token_value ||
        context_map.find(ctx) == context_map.end()) {
        return nullptr;
    }
    urma_target_seg_t *tseg = new urma_target_seg_t;
    tseg->seg = *seg;
    *token_value = {.token = seg->token_id};
    seg_map[tseg] = 1;
    return tseg;
}

urma_status_t urma_unimport_seg(urma_target_seg_t *tseg) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!tseg || seg_map.find(tseg) == seg_map.end()) {
        return URMA_EINVAL;
    }
    seg_map.erase(tseg);
    delete tseg;
    return URMA_SUCCESS;
}

urma_status_t urma_get_async_event(urma_context_t *ctx,
                                   urma_async_event_t *event) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!ctx || !event || context_map.find(ctx) == context_map.end()) {
        return URMA_EINVAL;
    }
    return URMA_ETIMEOUT;
}

void urma_ack_async_event(urma_async_event_t *event) {}

urma_jetty_t *urma_create_jetty(urma_context_t *ctx, urma_jetty_cfg_t *cfg) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!ctx || !cfg || context_map.find(ctx) == context_map.end()) {
        return nullptr;
    }
    urma_jetty_t *jetty = new urma_jetty_t;
    memset(&jetty->jetty_id.eid, 0, sizeof(urma_eid_t));
    jetty->jetty_id.eid.raw[0] = 1;
    jetty->jetty_id.uasid = 0;
    jetty->jetty_id.id = 1;
    jetty->jetty_cfg = *cfg;
    jetty->remote_jetty = nullptr;
    jetty_map[jetty] = 1;
    return jetty;
}

urma_status_t urma_delete_jetty(urma_jetty_t *jetty) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!jetty || jetty_map.find(jetty) == jetty_map.end()) {
        return URMA_EINVAL;
    }
    jetty_map.erase(jetty);
    delete jetty;
    return URMA_SUCCESS;
}

urma_status_t urma_unbind_jetty(urma_jetty_t *jetty) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!jetty || jetty_map.find(jetty) == jetty_map.end()) {
        return URMA_EINVAL;
    }
    jetty->remote_jetty = nullptr;
    return URMA_SUCCESS;
}

urma_target_jetty_t *urma_import_jetty(urma_context_t *ctx,
                                       urma_rjetty_t *rjetty,
                                       urma_token_t *token_value) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!ctx || !rjetty || !token_value ||
        context_map.find(ctx) == context_map.end()) {
        return nullptr;
    }
    urma_target_jetty_t *tjetty =
        reinterpret_cast<urma_target_jetty_t *>(new int(1));
    target_jetty_map[tjetty] = 1;
    *token_value = {.token = 1};
    return tjetty;
}

urma_status_t urma_unimport_jetty(urma_target_jetty_t *tjetty) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!tjetty || target_jetty_map.find(tjetty) == target_jetty_map.end()) {
        return URMA_EINVAL;
    }
    target_jetty_map.erase(tjetty);
    delete reinterpret_cast<int *>(tjetty);
    return URMA_SUCCESS;
}

urma_status_t urma_bind_jetty(urma_jetty_t *jetty,
                              urma_target_jetty_t *tjetty) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!jetty || !tjetty || jetty_map.find(jetty) == jetty_map.end() ||
        target_jetty_map.find(tjetty) == target_jetty_map.end()) {
        return URMA_EINVAL;
    }
    jetty->remote_jetty = tjetty;
    return URMA_SUCCESS;
}

urma_status_t urma_modify_jetty(urma_jetty_t *jetty, urma_jetty_attr_t *attr) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!jetty || !attr || jetty_map.find(jetty) == jetty_map.end()) {
        return URMA_EINVAL;
    }
    return URMA_SUCCESS;
}

urma_status_t urma_post_jetty_send_wr(urma_jetty_t *jetty, urma_jfs_wr_t *wr,
                                      urma_jfs_wr_t **bad_wr) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!jetty || !wr || jetty_map.find(jetty) == jetty_map.end()) {
        if (bad_wr) {
            *bad_wr = wr;
        }
        return URMA_EINVAL;
    }

    urma_jfs_wr_t *current_wr = wr;
    while (current_wr) {
        jfc_user_ctx_map[jetty->jetty_cfg.jfs_cfg.jfc].push_back(
            current_wr->user_ctx);
        current_wr = current_wr->next;
    }

    if (bad_wr) {
        *bad_wr = nullptr;
    }
    return URMA_SUCCESS;
}

int urma_poll_jfc(urma_jfc_t *jfc, int num_entries, urma_cr_t *cr_list) {
    std::lock_guard<std::mutex> lock(mock_mutex);
    if (!jfc || !cr_list ||
        jfc_user_ctx_map.find(jfc) == jfc_user_ctx_map.end()) {
        return -1;
    }
    int available = jfc_user_ctx_map[jfc].size();
    int num_completed = std::min(num_entries, available);
    for (int i = 0; i < num_completed; ++i) {
        cr_list[i].status = URMA_CR_SUCCESS;
        cr_list[i].user_ctx = jfc_user_ctx_map[jfc][i];
    }
    jfc_user_ctx_map[jfc].erase(jfc_user_ctx_map[jfc].begin(),
                                jfc_user_ctx_map[jfc].begin() + num_completed);
    return num_completed;
}

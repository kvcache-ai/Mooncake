use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mooncake_store_core::error::QuotaKind;
use mooncake_store_core::{
    CasResult, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId,
    ColdBackingRouteFilter, ColdTierDeviceFilter, ColdTierDeviceRecord, ColdTierDeviceUpdate,
    ColdTierPutDeviceResult, ColdTierUsageDelta, HandoffPlan, MetadataBackend, ObjectKey,
    ObjectRoute, Result, RoutePolicy, RoutePolicyDomain, RouteVersion, SegmentAnnouncement,
    SegmentLifecycleState, SegmentName, SegmentReservation, StoreError, TenantObjectAccounting,
    TenantObjectAccountingState, TenantPolicy, TenantPolicyScope, TenantQuotaAbortOutcome,
    TenantQuotaFinalizeOutcome, TenantQuotaFinalizeRequest, TenantQuotaReservation,
    TenantQuotaReservationOutcome, TenantQuotaReservationRequest, TenantQuotaState,
};
use parking_lot::{Mutex, MutexGuard};
use redis::cmd;
use redis::{
    Commands, ConnectionInfo, ConnectionLike, IntoConnectionInfo, RedisConnectionInfo, Script,
};

use crate::cold_tier::cold_tier_device_matches_filter;
use crate::keyspace::{
    parse_route_policy_domain, parse_tenant_eviction_candidate_key, parse_tenant_policy_scope,
};
use crate::segment_state::StoredSegmentState;
use crate::MetadataKeyspace;

const APPLY_COLD_TIER_USAGE_DELTA_SCRIPT: &str = r#"
local key = KEYS[1]
local used_delta = tonumber(ARGV[1])
local reserved_delta = tonumber(ARGV[2])
local updated_at_ms = tonumber(ARGV[3])

local payload = redis.call('GET', key)
if not payload then
    return {0, ''}
end
local device = cjson.decode(payload)
local tags = device['tags']
local used = tonumber(device['used_bytes'] or 0) + used_delta
local reserved = tonumber(device['reserved_bytes'] or 0) + reserved_delta
if used < 0 or reserved < 0 then
    return {1, payload}
end
device['used_bytes'] = used
device['reserved_bytes'] = reserved
device['updated_at_ms'] = updated_at_ms
if tags and next(tags) == nil then
    device['tags'] = cjson.empty_array
end
local next_payload = cjson.encode(device)
redis.call('SET', key, next_payload)
return {2, next_payload}
"#;

const CAS_OBJECT_ROUTE_SCRIPT: &str = r#"
local key = KEYS[1]
local index = KEYS[2]
local old_device_index = KEYS[3]
local old_state_index = KEYS[4]
local old_owner_index = KEYS[5]
local new_device_index = KEYS[6]
local new_state_index = KEYS[7]
local new_owner_index = KEYS[8]
local expected = ARGV[1]
local next_version = ARGV[2]
local payload = ARGV[3]
local has_old_device_index = ARGV[4]
local has_old_state_index = ARGV[5]
local has_old_owner_index = ARGV[6]
local has_new_device_index = ARGV[7]
local has_new_state_index = ARGV[8]
local has_new_owner_index = ARGV[9]

local current_payload = redis.call('HGET', key, 'payload')
local current_version = redis.call('HGET', key, 'version')

if expected == '__none__' then
    if current_payload then
        return {0, current_payload}
    end
else
    if (not current_version) or current_version ~= expected then
        return {0, current_payload or ''}
    end
end

if has_old_device_index == '1' then redis.call('SREM', old_device_index, key) end
if has_old_state_index == '1' then redis.call('SREM', old_state_index, key) end
if has_old_owner_index == '1' then redis.call('SREM', old_owner_index, key) end

if payload == '__delete__' then
    redis.call('DEL', key)
    redis.call('SREM', index, key)
    return {1, ''}
end

redis.call('HSET', key, 'version', next_version, 'payload', payload)
redis.call('SADD', index, key)
if has_new_device_index == '1' then redis.call('SADD', new_device_index, key) end
if has_new_state_index == '1' then redis.call('SADD', new_state_index, key) end
if has_new_owner_index == '1' then redis.call('SADD', new_owner_index, key) end
return {1, payload}
"#;

const DELETE_STALE_SEGMENT_WITH_MARKER_SCRIPT: &str = r#"
local segment_key = KEYS[1]
local marker_key = KEYS[2]
local client_key = KEYS[3]
local expected_owner = ARGV[1]
local cleanup_id = ARGV[2]
local expected_segment_key = ARGV[3]

local marker_owner = redis.call('HGET', marker_key, 'owner')
local marker_cleanup_id = redis.call('HGET', marker_key, 'cleanup_id')
local marker_segment_key = redis.call('HGET', marker_key, 'segment_key')
if marker_owner ~= expected_owner or marker_cleanup_id ~= cleanup_id or marker_segment_key ~= expected_segment_key then
    return {-1, ''}
end

if redis.call('EXISTS', client_key) == 1 then
    return {0, 'live'}
end

local current_owner = redis.call('HGET', segment_key, 'owner')
if not current_owner then
    return {0, 'missing'}
end
if current_owner ~= expected_owner then
    return {0, current_owner}
end

redis.call('DEL', segment_key)
return {1, expected_owner}
"#;

const RESERVE_SEGMENT_SCRIPT: &str = r#"
local key = KEYS[1]
local requested = tonumber(ARGV[1])

local payload = redis.call('HGET', key, 'state_json')
if not payload then
    return {0, -1}
end

local state = cjson.decode(payload)
local announcement = state["announcement"]
if announcement["state"] ~= "Active" then
    return {0, -2}
end

local alignment = tonumber(announcement["alignment_bytes"] or 1)
if alignment < 1 then
    alignment = 1
end

local function align_up(value, align)
    if align <= 1 then
        return value
    end
    local rem = value % align
    if rem == 0 then
        return value
    end
    return value + (align - rem)
end

local reserved = align_up(requested, alignment)
local offset = nil
local free_spans = state["free_spans"] or {}
for index, span in ipairs(free_spans) do
    if tonumber(span["length_bytes"]) >= reserved then
        offset = tonumber(span["offset_bytes"])
        local remaining = tonumber(span["length_bytes"]) - reserved
        if remaining > 0 then
            span["offset_bytes"] = offset + reserved
            span["length_bytes"] = remaining
        else
            table.remove(free_spans, index)
        end
        break
    end
end

if offset == nil then
    local cursor = tonumber(state["cursor_bytes"] or 0)
    offset = align_up(cursor, alignment)
    local next_cursor = offset + reserved
    local capacity = tonumber(announcement["capacity_bytes"])
    if next_cursor > capacity then
        return {0, capacity - offset}
    end
    state["cursor_bytes"] = next_cursor
end

announcement["used_bytes"] = tonumber(announcement["used_bytes"] or 0) + reserved
state["announcement"] = announcement
state["free_spans"] = free_spans

local next_payload = cjson.encode(state)
redis.call('HSET', key,
    'state_json', next_payload,
    'used_bytes', tostring(announcement["used_bytes"]),
    'state', announcement["state"],
    'alignment_bytes', tostring(announcement["alignment_bytes"] or 1),
    'capacity_bytes', tostring(announcement["capacity_bytes"]))
return {1, offset}
"#;

const RELEASE_SEGMENT_SCRIPT: &str = r#"
local key = KEYS[1]
local offset = tonumber(ARGV[1])
local requested = tonumber(ARGV[2])

local payload = redis.call('HGET', key, 'state_json')
if not payload then
    return {0, -1}
end

local state = cjson.decode(payload)
local announcement = state["announcement"]
local alignment = tonumber(announcement["alignment_bytes"] or 1)
if alignment < 1 then
    alignment = 1
end

local function align_up(value, align)
    if align <= 1 then
        return value
    end
    local rem = value % align
    if rem == 0 then
        return value
    end
    return value + (align - rem)
end

local reserved = align_up(requested, alignment)
local capacity = tonumber(announcement["capacity_bytes"])
if offset + reserved > capacity then
    return {0, -2}
end

local free_spans = state["free_spans"] or {}
table.insert(free_spans, { offset_bytes = offset, length_bytes = reserved })
table.sort(free_spans, function(left, right)
    return tonumber(left["offset_bytes"]) < tonumber(right["offset_bytes"])
end)

local merged = {}
for _, span in ipairs(free_spans) do
    local current_offset = tonumber(span["offset_bytes"])
    local current_length = tonumber(span["length_bytes"])
    local previous = merged[#merged]
    if previous then
        local previous_end = tonumber(previous["offset_bytes"]) + tonumber(previous["length_bytes"])
        if previous_end >= current_offset then
            local merged_end = math.max(previous_end, current_offset + current_length)
            previous["length_bytes"] = merged_end - tonumber(previous["offset_bytes"])
        else
            table.insert(merged, { offset_bytes = current_offset, length_bytes = current_length })
        end
    else
        table.insert(merged, { offset_bytes = current_offset, length_bytes = current_length })
    end
end

local cursor = tonumber(state["cursor_bytes"] or 0)
while #merged > 0 do
    local last = merged[#merged]
    local last_end = tonumber(last["offset_bytes"]) + tonumber(last["length_bytes"])
    if last_end ~= cursor then
        break
    end
    cursor = tonumber(last["offset_bytes"])
    table.remove(merged, #merged)
end

local used = tonumber(announcement["used_bytes"] or 0) - reserved
if used < 0 then
    used = 0
end
announcement["used_bytes"] = used
if used == 0 then
    cursor = 0
    merged = {}
end

state["announcement"] = announcement
state["cursor_bytes"] = cursor
state["free_spans"] = merged
local next_payload = cjson.encode(state)
redis.call('HSET', key,
    'state_json', next_payload,
    'used_bytes', tostring(announcement["used_bytes"]),
    'state', announcement["state"],
    'alignment_bytes', tostring(announcement["alignment_bytes"] or 1),
    'capacity_bytes', tostring(announcement["capacity_bytes"]))
return {1, used}
"#;

const PRUNE_MISSING_TENANT_POLICY_INDEX_SCRIPT: &str = r#"
local index_key = KEYS[1]
local policy_key = KEYS[2]

if redis.call('EXISTS', policy_key) == 0 then
    redis.call('SREM', index_key, policy_key)
    return 1
end
return 0
"#;

const PRUNE_UNCHANGED_TENANT_POLICY_INDEX_SCRIPT: &str = r#"
local index_key = KEYS[1]
local policy_key = KEYS[2]
local stale_payload = ARGV[1]

if redis.call('GET', policy_key) == stale_payload then
    redis.call('SREM', index_key, policy_key)
    return 1
end
return 0
"#;

const CAS_TENANT_POLICY_SCRIPT: &str = r#"
local key = KEYS[1]
local index_key = KEYS[2]
local expected = ARGV[1]
local payload = ARGV[2]

local index_type = redis.call('TYPE', index_key)
if type(index_type) == 'table' then
    index_type = index_type['ok']
end
if index_type ~= 'none' and index_type ~= 'set' then
    return redis.error_reply('tenant policy index has wrong type')
end

local current_payload = redis.call('GET', key)
if not current_payload then
    if expected ~= '__none__' then
        return {0, ''}
    end
else
    local current = cjson.decode(current_payload)
    local current_version = tonumber(current['version'])
    local expected_version = tonumber(expected)
    if expected == '__none__' or current_version ~= expected_version then
        return {0, current_payload}
    end
end

if payload == '__delete__' then
    redis.call('DEL', key)
    redis.call('SREM', index_key, key)
    return {1, current_payload or ''}
end

redis.call('SET', key, payload)
redis.call('SADD', index_key, key)
return {1, payload}
"#;

const ALLOCATE_CLIENT_LEASE_SCRIPT: &str = r#"
local lease_key_prefix = KEYS[1]
local by_stable_index = KEYS[2]
local hwm_key = KEYS[3]
local global_index = KEYS[4]
local expiry_index = KEYS[5]
local stable_key = KEYS[6]
local payload_template = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local expires_at_ms = tonumber(ARGV[3])
local stable_id = ARGV[4]

local active_max = 0
local members = redis.call('SMEMBERS', by_stable_index)
for _, member in ipairs(members) do
    local lease_key = lease_key_prefix .. tostring(member)
    if redis.call('EXISTS', lease_key) == 1 then
        local candidate = tonumber(member)
        if candidate and candidate > active_max then
            active_max = candidate
        end
    else
        redis.call('SREM', by_stable_index, tostring(member))
        redis.call('SREM', global_index, lease_key)
        redis.call('ZREM', expiry_index, lease_key)
    end
end

local hwm_raw = redis.call('GET', hwm_key)
local hwm_value = 0
if hwm_raw then
    hwm_value = tonumber(hwm_raw) or 0
end

local floor = active_max
if hwm_value > floor then
    floor = hwm_value
end
local new_epoch = floor + 1

local lease = cjson.decode(payload_template)
lease['runtime']['epoch'] = new_epoch
local payload = cjson.encode(lease)

local lease_key = lease_key_prefix .. tostring(new_epoch)
redis.call('SET', lease_key, payload, 'PX', ttl_ms)
redis.call('SADD', global_index, lease_key)
redis.call('SADD', by_stable_index, tostring(new_epoch))
redis.call('SET', hwm_key, tostring(new_epoch))
redis.call('ZADD', expiry_index, expires_at_ms, lease_key)
local runtime_key = stable_id .. ':' .. tostring(new_epoch)
if lease['state'] == 'Active' then
    redis.call('SET', stable_key, runtime_key, 'PX', ttl_ms)
elseif redis.call('GET', stable_key) == runtime_key then
    redis.call('DEL', stable_key)
end
return tostring(new_epoch)
"#;

const UPSERT_CLIENT_LEASE_STRICT_GREATER_SCRIPT: &str = r#"
local lease_key = KEYS[1]
local lease_key_prefix = KEYS[2]
local by_stable_index = KEYS[3]
local hwm_key = KEYS[4]
local global_index = KEYS[5]
local expiry_index = KEYS[6]
local stable_key = KEYS[7]
local new_epoch = tonumber(ARGV[1])
local payload = ARGV[2]
local ttl_ms = tonumber(ARGV[3])
local expires_at_ms = tonumber(ARGV[4])
local runtime_key = ARGV[5]

local function update_stable_runtime_index()
    local lease = cjson.decode(payload)
    if lease['state'] == 'Active' then
        redis.call('SET', stable_key, runtime_key, 'PX', ttl_ms)
    elseif redis.call('GET', stable_key) == runtime_key then
        redis.call('DEL', stable_key)
    end
end

if redis.call('EXISTS', lease_key) == 1 then
    redis.call('SET', lease_key, payload, 'PX', ttl_ms)
    redis.call('SADD', global_index, lease_key)
    redis.call('SADD', by_stable_index, tostring(new_epoch))
    redis.call('ZADD', expiry_index, expires_at_ms, lease_key)
    update_stable_runtime_index()
    return {1, ''}
end

local active_max = 0
local members = redis.call('SMEMBERS', by_stable_index)
for _, member in ipairs(members) do
    local candidate_key = lease_key_prefix .. tostring(member)
    if redis.call('EXISTS', candidate_key) == 1 then
        local candidate = tonumber(member)
        if candidate and candidate > active_max then
            active_max = candidate
        end
    else
        redis.call('SREM', by_stable_index, tostring(member))
        redis.call('SREM', global_index, candidate_key)
        redis.call('ZREM', expiry_index, candidate_key)
    end
end

local hwm_raw = redis.call('GET', hwm_key)
local hwm_value = 0
if hwm_raw then
    hwm_value = tonumber(hwm_raw) or 0
end

local floor = active_max
if hwm_value > floor then
    floor = hwm_value
end

local allow_reclaim = new_epoch == hwm_value and new_epoch > active_max
if new_epoch <= floor and not allow_reclaim then
    return {0, tostring(floor)}
end

redis.call('SET', lease_key, payload, 'PX', ttl_ms)
redis.call('SADD', global_index, lease_key)
redis.call('SADD', by_stable_index, tostring(new_epoch))
redis.call('SET', hwm_key, tostring(math.max(new_epoch, hwm_value)))
redis.call('ZADD', expiry_index, expires_at_ms, lease_key)
update_stable_runtime_index()
return {1, ''}
"#;

const UPDATE_CLIENT_STATE_SCRIPT: &str = r#"
local lease_key = KEYS[1]
local stable_key = KEYS[2]
local next_state = ARGV[1]
local runtime_key = ARGV[2]
local now_ms = tonumber(ARGV[3])

local payload = redis.call('GET', lease_key)
if not payload then
    return {0, ''}
end

local ttl_ms = redis.call('PTTL', lease_key)
local lease = cjson.decode(payload)
lease['state'] = next_state
local next_payload = cjson.encode(lease)
local stable_ttl_ms = tonumber(lease['expires_at_ms'] or 0) - now_ms
if stable_ttl_ms < 1 then
    stable_ttl_ms = 1
end

if ttl_ms > 0 then
    redis.call('SET', lease_key, next_payload, 'PX', ttl_ms)
else
    redis.call('SET', lease_key, next_payload)
end
if next_state == 'Active' then
    redis.call('SET', stable_key, runtime_key, 'PX', stable_ttl_ms)
elseif redis.call('GET', stable_key) == runtime_key then
    redis.call('DEL', stable_key)
end
return {1, next_payload}
"#;

const RESERVE_TENANT_QUOTA_SCRIPT: &str = r#"
local quota_key = KEYS[1]
local object_key = KEYS[2]
local reservation_key = KEYS[3]
local reservation_index_key = KEYS[4]

local reservation_id = ARGV[1]
local scope_payload = ARGV[2]
local object_storage_key = ARGV[3]
local expected_object_version = ARGV[4]
local delta_bytes = tonumber(ARGV[5])
local delta_objects = tonumber(ARGV[6])
local limit_max_bytes = ARGV[7]
local limit_max_objects = ARGV[8]
local expires_at_ms = tonumber(ARGV[9])
local created_at_ms = tonumber(ARGV[10])
local writer_runtime_payload = ARGV[11]

local function expected_version_or_nil(value)
    if value == '__none__' then
        return nil
    end
    return tonumber(value)
end

local function positive(value)
    if value > 0 then
        return value
    end
    return 0
end

local existing_reservation_payload = redis.call('GET', reservation_key)
if existing_reservation_payload then
    local reservation = cjson.decode(existing_reservation_payload)
    local existing_expected = reservation['expected_object_version']
    if reservation['state'] == 'Pending'
        and reservation['scope']['tenant'] == cjson.decode(scope_payload)['tenant']
        and reservation['key'] == object_storage_key
        and existing_expected == expected_version_or_nil(expected_object_version)
        and tonumber(reservation['delta_bytes']) == delta_bytes
        and tonumber(reservation['delta_objects']) == delta_objects then
        local quota_payload = redis.call('GET', quota_key)
        local object_payload = redis.call('GET', object_key)
        return {1, quota_payload or '', object_payload or '', existing_reservation_payload}
    end
    return {-2, existing_reservation_payload}
end

local object_payload = redis.call('GET', object_key)
local object = nil
if object_payload then
    object = cjson.decode(object_payload)
end
local actual_object_version = nil
if object then
    actual_object_version = tonumber(object['version'])
end
local expected_object = expected_version_or_nil(expected_object_version)
if actual_object_version ~= expected_object then
    return {-3, object_payload or '', '', ''}
end
if object and object['scope']['tenant'] ~= cjson.decode(scope_payload)['tenant'] then
    return {-4, object_payload or '', '', ''}
end

local quota_payload = redis.call('GET', quota_key)
local quota = nil
if quota_payload then
    quota = cjson.decode(quota_payload)
else
    quota = {
        scope = cjson.decode(scope_payload),
        version = 0,
        used_bytes = 0,
        used_objects = 0,
        pending_reserved_bytes = 0,
        pending_reserved_objects = 0,
        updated_at_ms = created_at_ms,
        updated_by = writer_runtime_payload,
    }
end

local positive_bytes = positive(delta_bytes)
local positive_objects = positive(delta_objects)
if limit_max_bytes ~= '__none__' then
    local admitted = tonumber(quota['used_bytes']) + tonumber(quota['pending_reserved_bytes']) + positive_bytes
    if admitted > tonumber(limit_max_bytes) then
        return {-5, quota_payload or cjson.encode(quota), '', ''}
    end
end
if limit_max_objects ~= '__none__' then
    local admitted = tonumber(quota['used_objects']) + tonumber(quota['pending_reserved_objects']) + positive_objects
    if admitted > tonumber(limit_max_objects) then
        return {-6, quota_payload or cjson.encode(quota), '', ''}
    end
end

quota['pending_reserved_bytes'] = tonumber(quota['pending_reserved_bytes']) + positive_bytes
quota['pending_reserved_objects'] = tonumber(quota['pending_reserved_objects']) + positive_objects
quota['version'] = tonumber(quota['version']) + 1
quota['updated_at_ms'] = created_at_ms
quota['updated_by'] = writer_runtime_payload
local next_quota_payload = cjson.encode(quota)

local reservation = {
    reservation_id = reservation_id,
    scope = cjson.decode(scope_payload),
    key = object_storage_key,
    version = 1,
    expected_object_version = expected_object,
    delta_bytes = delta_bytes,
    delta_objects = delta_objects,
    state = 'Pending',
    expires_at_ms = expires_at_ms,
    created_at_ms = created_at_ms,
    writer_runtime = cjson.decode(writer_runtime_payload),
}
local reservation_payload = cjson.encode(reservation)
redis.call('SET', quota_key, next_quota_payload)
redis.call('SET', reservation_key, reservation_payload)
redis.call('SET', reservation_index_key, reservation_key)
return {2, next_quota_payload, object_payload or '', reservation_payload}
"#;

const FINALIZE_TENANT_QUOTA_SCRIPT: &str = r#"
local quota_key = KEYS[1]
local object_key = KEYS[2]
local reservation_key = KEYS[3]
local reservation_index_key = KEYS[4]
local frontier_key = KEYS[5]

local reservation_id = ARGV[1]
local expected_object_version = ARGV[2]
local committed_length = ARGV[3]
local route_version = ARGV[4]
local object_state = ARGV[5]
local updated_at_ms = tonumber(ARGV[6])
local updated_by = ARGV[7]
local terminal_ttl_ms = tonumber(ARGV[8])
local old_frontier_member = ARGV[9]
local new_frontier_member = ARGV[10]

local function parse_optional_number(value)
    if value == '__none__' then
        return nil
    end
    return tonumber(value)
end

local function positive(value)
    if value > 0 then
        return value
    end
    return 0
end

local function apply_signed(base, delta)
    local next = base + delta
    if next < 0 then
        return nil
    end
    return next
end

local function trim_frontier()
    local frontier_size = redis.call('ZCARD', frontier_key)
    if frontier_size <= 16 then
        return
    end
    local overflow = frontier_size - 16
    local overflow_members = redis.call('ZREVRANGE', frontier_key, 0, overflow - 1)
    for _, member in ipairs(overflow_members) do
        redis.call('ZREM', frontier_key, member)
    end
end

local reservation_payload = redis.call('GET', reservation_key)
if not reservation_payload then
    return {-1, ''}
end
local reservation = cjson.decode(reservation_payload)
if reservation['reservation_id'] ~= reservation_id then
    return {-2, reservation_payload}
end
if reservation['state'] == 'Finalized' then
    local quota_payload = redis.call('GET', quota_key)
    local object_payload = redis.call('GET', object_key)
    return {1, quota_payload or '', object_payload or '', reservation_payload}
end
if reservation['state'] == 'Aborted' then
    return {-3, reservation_payload}
end

local object_payload = redis.call('GET', object_key)
local object = nil
if object_payload then
    object = cjson.decode(object_payload)
end
local actual_object_version = nil
if object then
    actual_object_version = tonumber(object['version'])
end
local expected_object = parse_optional_number(expected_object_version)
if expected_object == nil then
    expected_object = reservation['expected_object_version']
end
if actual_object_version ~= expected_object then
    return {-4, object_payload or ''}
end
if object and object['scope']['tenant'] ~= reservation['scope']['tenant'] then
    return {-5, object_payload}
end

local quota_payload = redis.call('GET', quota_key)
if not quota_payload then
    return {-6, ''}
end
local quota = cjson.decode(quota_payload)
local positive_bytes = positive(tonumber(reservation['delta_bytes']))
local positive_objects = positive(tonumber(reservation['delta_objects']))
quota['pending_reserved_bytes'] = apply_signed(tonumber(quota['pending_reserved_bytes']), -positive_bytes)
quota['pending_reserved_objects'] = apply_signed(tonumber(quota['pending_reserved_objects']), -positive_objects)
quota['used_bytes'] = apply_signed(tonumber(quota['used_bytes']), tonumber(reservation['delta_bytes']))
quota['used_objects'] = apply_signed(tonumber(quota['used_objects']), tonumber(reservation['delta_objects']))
if quota['pending_reserved_bytes'] == nil or quota['pending_reserved_objects'] == nil or quota['used_bytes'] == nil or quota['used_objects'] == nil then
    return {-7, quota_payload}
end
quota['version'] = tonumber(quota['version']) + 1
quota['updated_at_ms'] = updated_at_ms
quota['updated_by'] = updated_by
local next_quota_payload = cjson.encode(quota)

local next_object_payload = ''
if old_frontier_member ~= '__none__' then
    redis.call('ZREM', frontier_key, old_frontier_member)
end
if object_state == 'Active' then
    local next_object = {
        key = reservation['key'],
        scope = reservation['scope'],
        version = (expected_object or 0) + 1,
        committed_length = tonumber(committed_length),
        route_version = parse_optional_number(route_version),
        state = 'Active',
        last_writer = updated_by,
        updated_at_ms = updated_at_ms,
    }
    next_object_payload = cjson.encode(next_object)
    redis.call('SET', object_key, next_object_payload)
    redis.call('ZADD', frontier_key, 0, new_frontier_member)
    trim_frontier()
else
    redis.call('DEL', object_key)
end

reservation['state'] = 'Finalized'
reservation['version'] = tonumber(reservation['version']) + 1
local next_reservation_payload = cjson.encode(reservation)
redis.call('SET', quota_key, next_quota_payload)
redis.call('SET', reservation_key, next_reservation_payload)
if terminal_ttl_ms and terminal_ttl_ms > 0 then
    redis.call('PEXPIRE', reservation_key, terminal_ttl_ms)
    redis.call('PEXPIRE', reservation_index_key, terminal_ttl_ms)
end
return {2, next_quota_payload, next_object_payload, next_reservation_payload}
"#;

const ABORT_TENANT_QUOTA_SCRIPT: &str = r#"
local quota_key = KEYS[1]
local reservation_key = KEYS[2]
local reservation_index_key = KEYS[3]

local reservation_id = ARGV[1]
local terminal_ttl_ms = tonumber(ARGV[2])

local function positive(value)
    if value > 0 then
        return value
    end
    return 0
end

local function apply_signed(base, delta)
    local next = base + delta
    if next < 0 then
        return nil
    end
    return next
end

local reservation_payload = redis.call('GET', reservation_key)
if not reservation_payload then
    return {-1, '', ''}
end
local reservation = cjson.decode(reservation_payload)
if reservation['reservation_id'] ~= reservation_id then
    return {-2, '', reservation_payload}
end
if reservation['state'] == 'Aborted' then
    local quota_payload = redis.call('GET', quota_key)
    return {1, quota_payload or '', reservation_payload}
end
if reservation['state'] == 'Finalized' then
    return {-3, '', reservation_payload}
end

local quota_payload = redis.call('GET', quota_key)
if not quota_payload then
    return {-4, '', reservation_payload}
end
local quota = cjson.decode(quota_payload)
local positive_bytes = positive(tonumber(reservation['delta_bytes']))
local positive_objects = positive(tonumber(reservation['delta_objects']))
quota['pending_reserved_bytes'] = apply_signed(tonumber(quota['pending_reserved_bytes']), -positive_bytes)
quota['pending_reserved_objects'] = apply_signed(tonumber(quota['pending_reserved_objects']), -positive_objects)
if quota['pending_reserved_bytes'] == nil or quota['pending_reserved_objects'] == nil then
    return {-5, quota_payload, reservation_payload}
end
quota['version'] = tonumber(quota['version']) + 1
quota['updated_at_ms'] = tonumber(reservation['created_at_ms'])
quota['updated_by'] = cjson.encode(reservation['writer_runtime'])
local next_quota_payload = cjson.encode(quota)

reservation['state'] = 'Aborted'
reservation['version'] = tonumber(reservation['version']) + 1
local next_reservation_payload = cjson.encode(reservation)
redis.call('SET', quota_key, next_quota_payload)
redis.call('SET', reservation_key, next_reservation_payload)
if terminal_ttl_ms and terminal_ttl_ms > 0 then
    redis.call('PEXPIRE', reservation_key, terminal_ttl_ms)
    redis.call('PEXPIRE', reservation_index_key, terminal_ttl_ms)
end
return {2, next_quota_payload, next_reservation_payload}
"#;

const EVICTION_FRONTIER_LIMIT: usize = 16;

const REDIS_CONNECT_TIMEOUT_ENV: &str = "MC_STORE_RS_REDIS_CONNECT_TIMEOUT_MS";
const REDIS_IO_TIMEOUT_ENV: &str = "MC_STORE_RS_REDIS_IO_TIMEOUT_MS";
const REDIS_AUTH_PROBE_TIMEOUT_ENV: &str = "MC_STORE_RS_REDIS_AUTH_PROBE_TIMEOUT_MS";
const REDIS_RETRY_ATTEMPTS_ENV: &str = "MC_STORE_RS_REDIS_RETRY_ATTEMPTS";
const REDIS_RETRY_DELAY_ENV: &str = "MC_STORE_RS_REDIS_RETRY_DELAY_MS";
const REDIS_CONNECTION_POOL_SIZE_ENV: &str = "MC_STORE_RS_REDIS_CONNECTION_POOL_SIZE";
const DEFAULT_REDIS_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_REDIS_IO_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_REDIS_AUTH_PROBE_TIMEOUT: Duration = Duration::from_millis(500);
const DEFAULT_REDIS_RETRY_ATTEMPTS: usize = 3;
const DEFAULT_REDIS_RETRY_DELAY: Duration = Duration::from_millis(50);
const DEFAULT_REDIS_CONNECTION_POOL_SIZE: usize = 16;
const MAX_REDIS_CONNECTION_POOL_SIZE: usize = 256;
const DEFAULT_TENANT_QUOTA_TERMINAL_TTL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Debug)]
pub struct RedisMetadataConfig {
    pub url: String,
    pub keyspace: MetadataKeyspace,
    pub tenant_quota_terminal_ttl: Duration,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResolvedRedisAuth {
    pub username: Option<String>,
    pub password: Option<String>,
}

impl ResolvedRedisAuth {
    fn from_connection_info(info: &RedisConnectionInfo) -> Self {
        Self {
            username: info.username.clone(),
            password: info.password.clone(),
        }
    }

    fn with_env_fallback(mut self) -> Self {
        if self.username.is_some() || self.password.is_some() {
            return self;
        }

        let password = std::env::var("MC_REDIS_PASSWORD")
            .ok()
            .filter(|value| !value.is_empty());
        let Some(password) = password else {
            return self;
        };

        self.password = Some(password);
        self.username = std::env::var("MC_REDIS_USERNAME")
            .ok()
            .filter(|value| !value.is_empty());
        self
    }

    fn normalize_for_endpoint(self, url: &str) -> Self {
        let Some(legacy_auth) = self.password_only_fallback() else {
            return self;
        };
        if redis_accepts_auth(url, &legacy_auth) {
            return legacy_auth;
        }
        self
    }

    fn apply_to_connection_info(&self, info: &mut RedisConnectionInfo) {
        info.username = self.username.clone();
        info.password = self.password.clone();
    }

    fn password_only_fallback(&self) -> Option<Self> {
        if self.username.is_none() || self.password.is_none() {
            return None;
        }
        Some(Self {
            username: None,
            password: self.password.clone(),
        })
    }
}

impl RedisMetadataConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            keyspace: MetadataKeyspace::default(),
            tenant_quota_terminal_ttl: DEFAULT_TENANT_QUOTA_TERMINAL_TTL,
        }
    }

    pub fn keyspace(mut self, keyspace: MetadataKeyspace) -> Self {
        self.keyspace = keyspace;
        self
    }

    pub fn tenant(mut self, tenant: &str) -> Self {
        self.keyspace = self.keyspace.tenant_prefixed(tenant);
        self
    }

    pub fn tenant_quota_terminal_ttl(mut self, ttl: Duration) -> Self {
        self.tenant_quota_terminal_ttl = ttl;
        self
    }
}

struct RedisConnectionPool {
    client: redis::Client,
    slots: Vec<Mutex<Option<redis::Connection>>>,
    next_slot: AtomicUsize,
}

struct RedisConnectionLease<'a> {
    guard: MutexGuard<'a, Option<redis::Connection>>,
    discard: bool,
}

impl RedisConnectionPool {
    fn new(client: redis::Client) -> Self {
        let slots = (0..redis_connection_pool_size())
            .map(|_| Mutex::new(None))
            .collect();
        Self {
            client,
            slots,
            next_slot: AtomicUsize::new(0),
        }
    }

    fn checkout(&self) -> std::result::Result<RedisConnectionLease<'_>, redis::RedisError> {
        let start = self.next_slot.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        for offset in 0..self.slots.len() {
            let index = (start + offset) % self.slots.len();
            if let Some(guard) = self.slots[index].try_lock() {
                return self.lease_from_guard(guard);
            }
        }
        self.lease_from_guard(self.slots[start].lock())
    }

    fn lease_from_guard<'a>(
        &'a self,
        mut guard: MutexGuard<'a, Option<redis::Connection>>,
    ) -> std::result::Result<RedisConnectionLease<'a>, redis::RedisError> {
        let reconnect = guard
            .as_ref()
            .is_none_or(|connection| !connection.is_open());
        if reconnect {
            *guard = Some(open_redis_connection(&self.client)?);
        }
        Ok(RedisConnectionLease {
            guard,
            discard: false,
        })
    }
}

impl RedisConnectionLease<'_> {
    fn discard(&mut self) {
        self.discard = true;
    }
}

impl std::ops::Deref for RedisConnectionLease<'_> {
    type Target = redis::Connection;

    fn deref(&self) -> &Self::Target {
        self.guard
            .as_ref()
            .expect("redis connection lease always holds a connection")
    }
}

impl std::ops::DerefMut for RedisConnectionLease<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard
            .as_mut()
            .expect("redis connection lease always holds a connection")
    }
}

impl Drop for RedisConnectionLease<'_> {
    fn drop(&mut self) {
        let should_discard = self.discard
            || self
                .guard
                .as_ref()
                .is_some_and(|connection| !connection.is_open());
        if should_discard {
            self.guard.take();
        }
    }
}

pub struct RedisMetadataBackend {
    connection_pool: Arc<RedisConnectionPool>,
    legacy_auth_pool: Option<Arc<RedisConnectionPool>>,
    prefer_legacy_auth: Arc<AtomicBool>,
    connection_diagnostic_events: Arc<AtomicU64>,
    url: String,
    keyspace: MetadataKeyspace,
    route_namespace: String,
    tenant_quota_terminal_ttl: Duration,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RedisMetadataCleanupReport {
    pub live_clients: usize,
    pub inspected_segment_keys: usize,
    pub removed_segment_keys: usize,
    pub removed_segment_index_entries: usize,
    pub removed_owner_segment_index_entries: usize,
    pub stale_missing_segment_index_entries: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RedisSegmentCleanupStats {
    inspected_segment_keys: usize,
    removed_segment_keys: usize,
    removed_segment_index_entries: usize,
    removed_owner_segment_index_entries: usize,
    stale_missing_segment_index_entries: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientLeaseLiveness {
    Missing,
    Live(ClientLease),
    Expired(ClientLease),
}

impl RedisMetadataBackend {
    pub fn new(config: RedisMetadataConfig) -> Result<Self> {
        let connection_info = redis_connection_info(&config.url)?;
        let legacy_auth_connection_info = legacy_auth_connection_info(&connection_info);
        let route_namespace = format!(
            "redis://{}#{}",
            redacted_route_namespace_source(&config.url),
            config.keyspace.prefix()
        );
        let client = redis::Client::open(connection_info)
            .map_err(|error| metadata_error("redis client open", error))?;
        let legacy_auth_pool = legacy_auth_connection_info
            .map(redis::Client::open)
            .transpose()
            .map_err(|error| metadata_error("redis client open legacy auth", error))?
            .map(RedisConnectionPool::new)
            .map(Box::new);
        Ok(Self {
            connection_pool: Arc::new(RedisConnectionPool::new(client)),
            legacy_auth_pool: legacy_auth_pool.map(Arc::from),
            prefer_legacy_auth: Arc::new(AtomicBool::new(false)),
            connection_diagnostic_events: Arc::new(AtomicU64::new(0)),
            url: config.url,
            keyspace: config.keyspace,
            route_namespace,
            tenant_quota_terminal_ttl: config.tenant_quota_terminal_ttl,
        })
    }

    fn connection_raw(&self) -> std::result::Result<RedisConnectionLease<'_>, redis::RedisError> {
        if self.prefer_legacy_auth.load(Ordering::Relaxed) {
            if let Some(pool) = &self.legacy_auth_pool {
                return pool.checkout();
            }
        }

        match self.connection_pool.checkout() {
            Ok(connection) => Ok(connection),
            Err(error) => {
                if !is_legacy_redis_auth_arity_error(&error.to_string()) {
                    return Err(error);
                }
                let Some(pool) = &self.legacy_auth_pool else {
                    return Err(error);
                };
                let connection = pool.checkout()?;
                self.prefer_legacy_auth.store(true, Ordering::Relaxed);
                Ok(connection)
            }
        }
    }

    fn connection(&self, operation: &str) -> Result<RedisConnectionLease<'_>> {
        self.connection_raw().map_err(|error| {
            self.log_redis_diagnostic("connect", operation, &error, 1, 1);
            metadata_error(operation, error)
        })
    }

    fn query_with_retry<T, F>(&self, operation: &str, mut query: F) -> Result<T>
    where
        F: FnMut(&mut redis::Connection) -> std::result::Result<T, redis::RedisError>,
    {
        let attempts = redis_retry_attempts();
        let delay = redis_retry_delay();
        for attempt in 0..attempts {
            let mut connection = match self.connection_raw() {
                Ok(connection) => connection,
                Err(error)
                    if attempt + 1 < attempts && should_retry_transient_redis_error(&error) =>
                {
                    self.log_redis_diagnostic("connect", operation, &error, attempt + 1, attempts);
                    std::thread::sleep(delay);
                    continue;
                }
                Err(error) => {
                    self.log_redis_diagnostic("connect", operation, &error, attempt + 1, attempts);
                    return Err(metadata_error(operation, error));
                }
            };
            match query(&mut connection) {
                Ok(value) => return Ok(value),
                Err(error)
                    if attempt + 1 < attempts && should_retry_transient_redis_error(&error) =>
                {
                    connection.discard();
                    self.log_redis_diagnostic("query", operation, &error, attempt + 1, attempts);
                    std::thread::sleep(delay);
                }
                Err(error) => {
                    if should_retry_transient_redis_error(&error) {
                        connection.discard();
                    }
                    self.log_redis_diagnostic("query", operation, &error, attempt + 1, attempts);
                    return Err(metadata_error(operation, error));
                }
            }
        }
        unreachable!("redis query either returns or exhausts retries");
    }

    fn log_redis_diagnostic(
        &self,
        phase: &'static str,
        operation: &str,
        error: &redis::RedisError,
        attempt: usize,
        attempts: usize,
    ) {
        let occurrence = self
            .connection_diagnostic_events
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        if !should_log_redis_diagnostic_occurrence(occurrence) {
            return;
        }
        let hint = redis_error_diagnostic_hint(error);
        tracing::warn!(
            phase,
            operation,
            route_namespace = %self.route_namespace,
            occurrence,
            attempt,
            attempts,
            retryable = should_retry_transient_redis_error(error),
            connect_timeout_ms = redis_connect_timeout().as_millis() as u64,
            io_timeout_ms = redis_io_timeout().as_millis() as u64,
            retry_delay_ms = redis_retry_delay().as_millis() as u64,
            connection_pool_size = redis_connection_pool_size(),
            prefer_legacy_auth = self.prefer_legacy_auth.load(Ordering::Relaxed),
            diagnostic_hint = hint.unwrap_or(""),
            error = %error,
            "redis metadata operation failed"
        );
    }

    fn query_readonly<T, F>(&self, operation: &str, mut query: F) -> Result<T>
    where
        F: FnMut(&mut redis::Connection) -> std::result::Result<T, redis::RedisError>,
    {
        self.query_with_retry(operation, |connection| query(connection))
    }

    fn query_idempotent_write<T, F>(&self, operation: &str, mut query: F) -> Result<T>
    where
        F: FnMut(&mut redis::Connection) -> std::result::Result<T, redis::RedisError>,
    {
        self.query_with_retry(operation, |connection| query(connection))
    }

    fn load_object_routes_from_keys(
        &self,
        keys: &mut Vec<String>,
        limit: Option<usize>,
    ) -> Result<(Vec<ObjectRoute>, Vec<String>)> {
        let entries = self.query_readonly("redis load object routes", |connection| {
            let mut entries = Vec::with_capacity(keys.len());
            for key in keys.iter() {
                let payload: Option<String> = redis::cmd("HGET")
                    .arg(key.as_str())
                    .arg("payload")
                    .query(connection)?;
                entries.push((key.clone(), payload));
            }
            Ok(entries)
        })?;

        let mut stale = Vec::new();
        let mut live_keys = Vec::new();
        let mut routes = Vec::new();
        for (key, payload) in entries {
            if let Some(payload) = payload {
                live_keys.push(key);
                routes.push(serde_json::from_str(&payload).map_err(json_error)?);
                if limit.is_some_and(|limit| routes.len() >= limit) {
                    break;
                }
            } else {
                stale.push(key);
            }
        }
        if !stale.is_empty() {
            let mut connection = self.connection("redis srem stale object index")?;
            connection
                .srem::<_, _, ()>(self.keyspace.object_index(), stale.clone())
                .map_err(|error| metadata_error("redis srem stale object index", error))?;
        }
        *keys = live_keys.clone();
        Ok((routes, live_keys))
    }

    fn cold_backing_filter_index(&self, filter: &ColdBackingRouteFilter) -> Option<String> {
        if let Some(device_id) = filter.device_id.as_deref() {
            return Some(self.keyspace.object_cold_tier_device_index(device_id));
        }
        if let Some(state) = filter.state {
            return Some(self.keyspace.object_cold_backing_state_index(state));
        }
        filter
            .owner
            .as_ref()
            .map(|owner| self.keyspace.object_cold_backing_owner_index(owner))
    }

    fn cleanup_stale_segments_for_owner_storage_key(
        &self,
        owner_storage_key: &str,
    ) -> Result<RedisSegmentCleanupStats> {
        let owner_index = self.keyspace.segment_index_for_owner_key(owner_storage_key);
        let global_index = self.keyspace.segment_index(None);
        let mut connection = self.connection("redis cleanup stale segments")?;
        let keys: Vec<String> = connection
            .smembers(&owner_index)
            .map_err(|error| metadata_error("redis smembers owner segment index", error))?;
        if keys.is_empty() {
            let _ = connection.del::<_, usize>(owner_index.as_str());
            return Ok(RedisSegmentCleanupStats::default());
        }

        let cleanup_id = format!("{}-{}", std::process::id(), now_ms());
        let mut stats = RedisSegmentCleanupStats {
            inspected_segment_keys: keys.len(),
            ..RedisSegmentCleanupStats::default()
        };
        let mut stale_global_entries = Vec::new();

        for key in &keys {
            let owner: Option<String> = redis::cmd("HGET")
                .arg(key.as_str())
                .arg("owner")
                .query(&mut connection)
                .map_err(|error| metadata_error("redis hget segment owner", error))?;
            match owner {
                Some(owner) if owner == owner_storage_key => {
                    let Some((stable_id, epoch)) = owner.rsplit_once(':') else {
                        return Err(StoreError::Metadata(format!(
                            "redis cleanup stale segment: invalid owner storage key {owner}"
                        )));
                    };
                    let epoch = epoch.parse::<u64>().map_err(|error| {
                        StoreError::Metadata(format!(
                            "redis cleanup stale segment: invalid owner epoch {owner}: {error}"
                        ))
                    })?;
                    let client_key = self
                        .keyspace
                        .client(&ClientRuntimeId::new(stable_id, ClientEpoch(epoch)));
                    let marker_key = self.keyspace.segment_cleanup_marker(&cleanup_id, key);
                    connection
                        .hset_multiple::<_, _, _, ()>(
                            marker_key.as_str(),
                            &[
                                ("cleanup_id", cleanup_id.as_str()),
                                ("segment_key", key.as_str()),
                                ("owner", owner.as_str()),
                            ],
                        )
                        .map_err(|error| {
                            metadata_error("redis hset segment cleanup marker", error)
                        })?;
                    connection
                        .expire::<_, ()>(marker_key.as_str(), 300)
                        .map_err(|error| {
                            metadata_error("redis expire segment cleanup marker", error)
                        })?;

                    let result: (i32, String) =
                        Script::new(DELETE_STALE_SEGMENT_WITH_MARKER_SCRIPT)
                            .key(key.as_str())
                            .key(marker_key.as_str())
                            .key(client_key.as_str())
                            .arg(owner.as_str())
                            .arg(cleanup_id.as_str())
                            .arg(key.as_str())
                            .invoke(&mut connection)
                            .map_err(|error| {
                                metadata_error("redis delete stale segment with marker", error)
                            })?;
                    connection
                        .del::<_, ()>(marker_key.as_str())
                        .map_err(|error| {
                            metadata_error("redis del segment cleanup marker", error)
                        })?;

                    match result.0 {
                        1 => {
                            stale_global_entries.push(key.clone());
                            stats.removed_segment_keys += 1;
                        }
                        0 => {}
                        code => {
                            return Err(StoreError::Metadata(format!(
                                "redis delete stale segment with marker: unexpected status code {code}"
                            )));
                        }
                    }
                }
                Some(_) => {
                    connection
                        .srem::<_, _, ()>(owner_index.as_str(), key.as_str())
                        .map_err(|error| {
                            metadata_error("redis srem mismatched owner segment index", error)
                        })?;
                    stats.stale_missing_segment_index_entries += 1;
                }
                None => {
                    stale_global_entries.push(key.clone());
                    stats.stale_missing_segment_index_entries += 1;
                }
            }
        }

        if !stale_global_entries.is_empty() {
            stats.removed_segment_index_entries = connection
                .srem(&global_index, stale_global_entries.clone())
                .map_err(|error| metadata_error("redis srem stale segment index", error))?;
            stats.removed_owner_segment_index_entries = connection
                .srem(owner_index.as_str(), stale_global_entries)
                .map_err(|error| metadata_error("redis srem stale owner segment index", error))?;
        }

        let remaining: usize = connection
            .scard(owner_index.as_str())
            .map_err(|error| metadata_error("redis scard owner segment index", error))?;
        if remaining == 0 {
            let _ = connection.del::<_, usize>(owner_index.as_str());
        }
        Ok(stats)
    }

    fn load_segment_state(
        &self,
        connection: &mut redis::Connection,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
    ) -> Result<StoredSegmentState> {
        let key = self.keyspace.segment(owner, segment);
        let payload: Option<String> = redis::cmd("HGET")
            .arg(&key)
            .arg("state_json")
            .query(connection)
            .map_err(|error| metadata_error("redis hget segment state", error))?;
        let payload = payload.ok_or(StoreError::NotFound(key))?;
        serde_json::from_str(&payload).map_err(json_error)
    }

    fn store_segment_state(
        &self,
        connection: &mut redis::Connection,
        state: &StoredSegmentState,
    ) -> Result<()> {
        let key = self
            .keyspace
            .segment(&state.announcement.owner, &state.announcement.segment_name);
        let payload = serde_json::to_string(state).map_err(json_error)?;
        let tags = serde_json::to_string(&state.announcement.tags).map_err(json_error)?;
        connection
            .hset_multiple::<_, _, _, ()>(
                &key,
                &[
                    ("owner", state.announcement.owner.storage_key()),
                    ("segment_name", state.announcement.segment_name.0.clone()),
                    (
                        "capacity_bytes",
                        state.announcement.capacity_bytes.to_string(),
                    ),
                    ("used_bytes", state.announcement.used_bytes.to_string()),
                    ("state", format!("{:?}", state.announcement.state)),
                    (
                        "alignment_bytes",
                        state.announcement.alignment_bytes.to_string(),
                    ),
                    ("tags_json", tags),
                    ("state_json", payload),
                ],
            )
            .map_err(|error| metadata_error("redis hset segment state", error))?;
        connection
            .sadd::<_, _, ()>(self.keyspace.segment_index(None), key.as_str())
            .map_err(|error| metadata_error("redis sadd segment index", error))?;
        connection
            .sadd::<_, _, ()>(
                self.keyspace.segment_index(Some(&state.announcement.owner)),
                key,
            )
            .map_err(|error| metadata_error("redis sadd owner segment index", error))
    }

    pub fn cleanup_stale_segments(&self) -> Result<RedisMetadataCleanupReport> {
        let live_clients = self.list_live_clients()?;
        let live_runtime_keys = live_clients
            .iter()
            .map(|lease| lease.runtime.storage_key())
            .collect::<std::collections::BTreeSet<_>>();
        let mut connection = self.connection("redis cleanup stale segments")?;
        let global_index = self.keyspace.segment_index(None);
        let mut keys: Vec<String> = connection
            .smembers(&global_index)
            .map_err(|error| metadata_error("redis smembers segment index", error))?;
        if keys.is_empty() {
            keys = scan_keys(&mut connection, &self.keyspace.segment_pattern(None))?;
        }

        let mut stats = RedisSegmentCleanupStats {
            inspected_segment_keys: keys.len(),
            ..RedisSegmentCleanupStats::default()
        };
        let mut stale_owners = std::collections::BTreeSet::<String>::new();

        for key in keys {
            let owner: Option<String> = redis::cmd("HGET")
                .arg(key.as_str())
                .arg("owner")
                .query(&mut connection)
                .map_err(|error| metadata_error("redis hget segment owner", error))?;
            let Some(owner) = owner else {
                let removed: usize = connection
                    .srem(&global_index, key.as_str())
                    .map_err(|error| metadata_error("redis srem dangling segment index", error))?;
                stats.removed_segment_index_entries += removed;
                stats.stale_missing_segment_index_entries += 1;
                continue;
            };
            if live_runtime_keys.contains(&owner) {
                continue;
            }
            stale_owners.insert(owner);
        }

        for owner_storage_key in stale_owners {
            let owner_stats =
                self.cleanup_stale_segments_for_owner_storage_key(&owner_storage_key)?;
            stats.removed_segment_keys += owner_stats.removed_segment_keys;
            stats.removed_segment_index_entries += owner_stats.removed_segment_index_entries;
            stats.removed_owner_segment_index_entries +=
                owner_stats.removed_owner_segment_index_entries;
            stats.stale_missing_segment_index_entries +=
                owner_stats.stale_missing_segment_index_entries;
        }

        Ok(RedisMetadataCleanupReport {
            live_clients: live_clients.len(),
            inspected_segment_keys: stats.inspected_segment_keys,
            removed_segment_keys: stats.removed_segment_keys,
            removed_segment_index_entries: stats.removed_segment_index_entries,
            removed_owner_segment_index_entries: stats.removed_owner_segment_index_entries,
            stale_missing_segment_index_entries: stats.stale_missing_segment_index_entries,
        })
    }

    pub fn cleanup_stale_segments_for_owner(
        &self,
        owner: &ClientRuntimeId,
    ) -> Result<RedisMetadataCleanupReport> {
        let stats = self.cleanup_stale_segments_for_owner_storage_key(&owner.storage_key())?;
        Ok(RedisMetadataCleanupReport {
            live_clients: 0,
            inspected_segment_keys: stats.inspected_segment_keys,
            removed_segment_keys: stats.removed_segment_keys,
            removed_segment_index_entries: stats.removed_segment_index_entries,
            removed_owner_segment_index_entries: stats.removed_owner_segment_index_entries,
            stale_missing_segment_index_entries: stats.stale_missing_segment_index_entries,
        })
    }

    pub fn client_lease_liveness(&self, runtime: &ClientRuntimeId) -> Result<ClientLeaseLiveness> {
        let key = self.keyspace.client(runtime);
        let payload: Option<String> = self
            .query_readonly("redis get client lease", |connection| {
                connection.get(key.as_str())
            })?;
        let Some(payload) = payload else {
            return Ok(ClientLeaseLiveness::Missing);
        };
        let lease: ClientLease = serde_json::from_str(&payload).map_err(json_error)?;
        if lease.expires_at_ms >= now_ms() {
            Ok(ClientLeaseLiveness::Live(lease))
        } else {
            Ok(ClientLeaseLiveness::Expired(lease))
        }
    }

    pub fn list_due_client_lease_expiries(
        &self,
        expires_before_ms: u64,
        limit: usize,
    ) -> Result<Vec<String>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let expiry_index = self.keyspace.client_lease_expiry_index();
        self.query_readonly("redis zrangebyscore client lease expiry", |connection| {
            cmd("ZRANGEBYSCORE")
                .arg(&expiry_index)
                .arg("-inf")
                .arg(expires_before_ms)
                .arg("LIMIT")
                .arg(0)
                .arg(limit)
                .query(connection)
        })
    }

    pub fn refresh_client_lease_expiry(
        &self,
        runtime: &ClientRuntimeId,
        expires_at_ms: u64,
    ) -> Result<()> {
        let expiry_index = self.keyspace.client_lease_expiry_index();
        let lease_key = self.keyspace.client(runtime);
        self.query_idempotent_write("redis zadd client lease expiry", |connection| {
            connection.zadd::<_, _, _, ()>(&expiry_index, lease_key.as_str(), expires_at_ms)
        })
    }

    pub fn remove_client_lease_expiry(&self, runtime: &ClientRuntimeId) -> Result<bool> {
        self.remove_client_lease_expiry_entry(&self.keyspace.client(runtime))
    }

    pub fn remove_client_lease_expiry_entry(&self, lease_key: &str) -> Result<bool> {
        let expiry_index = self.keyspace.client_lease_expiry_index();
        self.query_idempotent_write("redis zrem client lease expiry", |connection| {
            connection.zrem(&expiry_index, lease_key)
        })
    }
}

fn redis_connection_info(url: &str) -> Result<ConnectionInfo> {
    let mut info = url
        .to_string()
        .into_connection_info()
        .map_err(|error| metadata_error("redis connection info", error))?;
    resolve_redis_auth(url)?.apply_to_connection_info(&mut info.redis);
    Ok(info)
}

fn legacy_auth_connection_info(info: &ConnectionInfo) -> Option<ConnectionInfo> {
    let legacy_auth =
        ResolvedRedisAuth::from_connection_info(&info.redis).password_only_fallback()?;
    let mut legacy = info.clone();
    legacy_auth.apply_to_connection_info(&mut legacy.redis);
    Some(legacy)
}

pub fn resolve_redis_auth(url: &str) -> Result<ResolvedRedisAuth> {
    let info = url
        .to_string()
        .into_connection_info()
        .map_err(|error| metadata_error("redis auth resolution", error))?;
    Ok(ResolvedRedisAuth::from_connection_info(&info.redis)
        .with_env_fallback()
        .normalize_for_endpoint(url))
}

fn redis_accepts_auth(url: &str, auth: &ResolvedRedisAuth) -> bool {
    let Ok(mut info) = url.to_string().into_connection_info() else {
        return false;
    };
    auth.apply_to_connection_info(&mut info.redis);
    let Ok(client) = redis::Client::open(info) else {
        return false;
    };
    client
        .get_connection_with_timeout(redis_auth_probe_timeout())
        .is_ok()
}

fn redacted_route_namespace_source(url: &str) -> String {
    match redis::parse_redis_url(url) {
        Some(mut parsed) => {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.to_string()
        }
        None => url.to_string(),
    }
}

fn redis_connect_timeout() -> Duration {
    duration_from_env_ms(REDIS_CONNECT_TIMEOUT_ENV, DEFAULT_REDIS_CONNECT_TIMEOUT)
}

fn redis_io_timeout() -> Duration {
    duration_from_env_ms(REDIS_IO_TIMEOUT_ENV, DEFAULT_REDIS_IO_TIMEOUT)
}

fn redis_auth_probe_timeout() -> Duration {
    duration_from_env_ms(
        REDIS_AUTH_PROBE_TIMEOUT_ENV,
        DEFAULT_REDIS_AUTH_PROBE_TIMEOUT,
    )
}

fn redis_connection_pool_size() -> usize {
    std::env::var(REDIS_CONNECTION_POOL_SIZE_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|size| *size > 0)
        .map(|size| size.min(MAX_REDIS_CONNECTION_POOL_SIZE))
        .unwrap_or(DEFAULT_REDIS_CONNECTION_POOL_SIZE)
}

fn open_redis_connection(
    client: &redis::Client,
) -> std::result::Result<redis::Connection, redis::RedisError> {
    let connection = client.get_connection_with_timeout(redis_connect_timeout())?;
    connection.set_read_timeout(Some(redis_io_timeout()))?;
    connection.set_write_timeout(Some(redis_io_timeout()))?;
    Ok(connection)
}

fn duration_from_env_ms(name: &str, default: Duration) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|millis| *millis > 0)
        .map(Duration::from_millis)
        .unwrap_or(default)
}

fn scan_keys(connection: &mut redis::Connection, pattern: &str) -> Result<Vec<String>> {
    let mut cursor = 0u64;
    let mut keys = Vec::new();
    loop {
        let (next, batch): (u64, Vec<String>) = cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .query(connection)
            .map_err(|error| metadata_error("redis scan keys", error))?;
        keys.extend(batch);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    Ok(keys)
}

fn legacy_tenant_policy_keys(
    keyspace: &MetadataKeyspace,
    tenant: &str,
    connection: &mut redis::Connection,
) -> Result<Vec<String>> {
    let root_key = keyspace.tenant_policy(&TenantPolicyScope::new(
        tenant,
        None::<String>,
        None::<String>,
    ));
    let mut keys = Vec::from([root_key.clone()]);
    keys.extend(scan_keys(connection, &format!("{}/*", root_key))?);
    keys.sort();
    keys.dedup();
    Ok(keys)
}

fn route_matches_cold_backing_filter(route: &ObjectRoute, filter: &ColdBackingRouteFilter) -> bool {
    let Some(backing) = route.cold_backing.as_ref() else {
        return false;
    };
    if filter
        .device_id
        .as_ref()
        .is_some_and(|device_id| backing.cold_tier_id != *device_id)
    {
        return false;
    }
    if filter.state.is_some_and(|state| backing.state != state) {
        return false;
    }
    if filter
        .owner
        .as_ref()
        .is_some_and(|owner| backing.owner != *owner)
    {
        return false;
    }
    true
}

impl MetadataBackend for RedisMetadataBackend {
    fn route_namespace(&self) -> String {
        self.route_namespace.clone()
    }

    fn backend_kind(&self) -> &'static str {
        "redis"
    }

    fn for_tenant(&self, tenant: &str) -> Option<Arc<dyn MetadataBackend>> {
        let keyspace = self.keyspace.tenant_prefixed(tenant);
        let route_namespace = format!(
            "{}#{}",
            redacted_route_namespace_source(&self.url),
            keyspace.prefix()
        );
        Some(Arc::new(Self {
            connection_pool: self.connection_pool.clone(),
            legacy_auth_pool: self.legacy_auth_pool.clone(),
            prefer_legacy_auth: self.prefer_legacy_auth.clone(),
            connection_diagnostic_events: self.connection_diagnostic_events.clone(),
            url: self.url.clone(),
            keyspace,
            route_namespace,
            tenant_quota_terminal_ttl: self.tenant_quota_terminal_ttl,
        }))
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> Result<()> {
        let stable_id = lease.runtime.stable_id.clone();
        let new_epoch = lease.runtime.epoch.0;
        let lease_key = self.keyspace.client(&lease.runtime);
        let lease_key_prefix = self.keyspace.client_prefix_for_stable(&stable_id);
        let by_stable_key = self.keyspace.client_by_stable_index(&stable_id);
        let hwm_key = self.keyspace.client_epoch_hwm(&stable_id);
        let global_index_key = self.keyspace.client_index();
        let expiry_index_key = self.keyspace.client_lease_expiry_index();
        let stable_key = self.keyspace.stable_runtime(&lease.runtime.stable_id);
        let payload = serde_json::to_string(lease).map_err(json_error)?;
        let result = self.query_idempotent_write("redis upsert client lease", |connection| {
            let ttl_ms = lease.expires_at_ms.saturating_sub(now_ms()).max(1);
            let result = Script::new(UPSERT_CLIENT_LEASE_STRICT_GREATER_SCRIPT)
                .key(&lease_key)
                .key(&lease_key_prefix)
                .key(&by_stable_key)
                .key(&hwm_key)
                .key(&global_index_key)
                .key(&expiry_index_key)
                .key(&stable_key)
                .arg(new_epoch.to_string())
                .arg(payload.as_str())
                .arg(ttl_ms.to_string())
                .arg(lease.expires_at_ms.to_string())
                .arg(lease.runtime.storage_key())
                .invoke::<(i32, String)>(connection)?;
            Ok(result)
        })?;
        if result.0 == 1 {
            return Ok(());
        }
        Err(StoreError::StaleEpoch(format!(
            "lease rejected: proposed epoch {new_epoch} <= floor {} for stable_id {}",
            result.1, stable_id.0
        )))
    }

    fn allocate_client_lease(&self, template: &ClientLease) -> Result<ClientRuntimeId> {
        let stable_id = template.runtime.stable_id.clone();
        let lease_key_prefix = self.keyspace.client_prefix_for_stable(&stable_id);
        let by_stable_key = self.keyspace.client_by_stable_index(&stable_id);
        let hwm_key = self.keyspace.client_epoch_hwm(&stable_id);
        let global_index_key = self.keyspace.client_index();
        let expiry_index_key = self.keyspace.client_lease_expiry_index();
        let stable_key = self.keyspace.stable_runtime(&stable_id);
        let payload_template = serde_json::to_string(template).map_err(json_error)?;
        let assigned_epoch: String =
            self.query_idempotent_write("redis allocate client lease", |connection| {
                let ttl_ms = template.expires_at_ms.saturating_sub(now_ms()).max(1);
                let assigned_epoch = Script::new(ALLOCATE_CLIENT_LEASE_SCRIPT)
                    .key(&lease_key_prefix)
                    .key(&by_stable_key)
                    .key(&hwm_key)
                    .key(&global_index_key)
                    .key(&expiry_index_key)
                    .key(&stable_key)
                    .arg(payload_template.as_str())
                    .arg(ttl_ms.to_string())
                    .arg(template.expires_at_ms.to_string())
                    .arg(stable_id.0.as_str())
                    .invoke::<String>(connection)?;
                Ok(assigned_epoch)
            })?;
        let epoch = assigned_epoch.parse::<u64>().map_err(|error| {
            StoreError::Metadata(format!(
                "redis allocate client lease: invalid epoch payload {assigned_epoch:?}: {error}"
            ))
        })?;
        Ok(ClientRuntimeId {
            stable_id,
            epoch: ClientEpoch(epoch),
        })
    }

    fn update_client_state(
        &self,
        runtime: &ClientRuntimeId,
        next: ClientLifecycleState,
    ) -> Result<()> {
        let key = self.keyspace.client(runtime);
        let next_state = match next {
            ClientLifecycleState::Standby => "Standby",
            ClientLifecycleState::Active => "Active",
            ClientLifecycleState::Draining => "Draining",
            ClientLifecycleState::Sealed => "Sealed",
            ClientLifecycleState::Offline => "Offline",
        };
        let stable_key = self.keyspace.stable_runtime(&runtime.stable_id);
        let result: (i32, String) =
            self.query_idempotent_write("redis update client state", |connection| {
                let result: (i32, String) = Script::new(UPDATE_CLIENT_STATE_SCRIPT)
                    .key(&key)
                    .key(&stable_key)
                    .arg(next_state)
                    .arg(runtime.storage_key())
                    .arg(now_ms().to_string())
                    .invoke(connection)?;
                Ok(result)
            })?;
        if result.0 == 1 {
            return Ok(());
        }
        Err(StoreError::NotFound(key))
    }

    fn get_client_lease(&self, runtime: &ClientRuntimeId) -> Result<Option<ClientLease>> {
        let key = self.keyspace.client(runtime);
        let payload: Option<String> = self
            .query_readonly("redis get client lease by runtime", |connection| {
                connection.get(&key)
            })?;
        let lease = payload
            .map(|payload| serde_json::from_str::<ClientLease>(&payload).map_err(json_error))
            .transpose()?;
        Ok(lease.filter(|lease| lease.expires_at_ms >= now_ms()))
    }

    fn get_live_runtime_by_stable_id(
        &self,
        stable_id: &ClientStableId,
    ) -> Result<Option<ClientLease>> {
        let stable_key = self.keyspace.stable_runtime(stable_id);
        let runtime_key: Option<String> = self
            .query_readonly("redis get stable runtime index", |connection| {
                connection.get(&stable_key)
            })?;
        let Some(runtime_key) = runtime_key else {
            return Ok(None);
        };
        let Some(runtime) = ClientRuntimeId::from_storage_key(&runtime_key) else {
            return Err(StoreError::Metadata(format!(
                "invalid runtime storage key in stable runtime index: {runtime_key}"
            )));
        };
        let key = self.keyspace.client(&runtime);
        let payload: Option<String> = self
            .query_readonly("redis get client lease by stable runtime", |connection| {
                connection.get(&key)
            })?;
        let lease = payload
            .map(|payload| serde_json::from_str::<ClientLease>(&payload).map_err(json_error))
            .transpose()?;
        Ok(lease.filter(|lease| lease.state.serves_reads() && lease.expires_at_ms >= now_ms()))
    }

    fn list_live_clients(&self) -> Result<Vec<ClientLease>> {
        let index = self.keyspace.client_index();
        let (keys, backfill_index) =
            self.query_readonly("redis list live clients", |connection| {
                let mut keys: Vec<String> = connection.smembers(&index)?;
                let mut backfill_index = false;
                if keys.is_empty() {
                    keys = redis::cmd("KEYS")
                        .arg(self.keyspace.client_pattern())
                        .query(connection)?;
                    backfill_index = !keys.is_empty();
                }
                Ok((keys, backfill_index))
            })?;
        if backfill_index && !keys.is_empty() {
            let mut connection = self.connection("redis sadd legacy client index")?;
            connection
                .sadd::<_, _, ()>(&index, keys.clone())
                .map_err(|error| metadata_error("redis sadd legacy client index", error))?;
        }
        let entries = self.query_readonly("redis fetch client leases", |connection| {
            if keys.is_empty() {
                return Ok(Vec::new());
            }
            let payloads: Vec<Option<String>> = redis::cmd("MGET").arg(&keys).query(connection)?;
            Ok(keys.iter().cloned().zip(payloads).collect::<Vec<_>>())
        })?;
        let now = now_ms();
        let mut stale = Vec::new();
        let mut stale_by_stable: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        let mut stale_expiry_entries = Vec::new();
        let mut leases = Vec::with_capacity(entries.len());
        for (key, payload) in entries {
            let parsed = self.keyspace.parse_client_key(&key);
            match payload {
                Some(payload) => {
                    let lease =
                        serde_json::from_str::<ClientLease>(&payload).map_err(json_error)?;
                    if lease.expires_at_ms >= now {
                        leases.push(lease);
                    } else {
                        let by_stable_key = self
                            .keyspace
                            .client_by_stable_index(&lease.runtime.stable_id);
                        stale_by_stable
                            .entry(by_stable_key)
                            .or_default()
                            .push(lease.runtime.epoch.0.to_string());
                        stale.push(key);
                        stale_expiry_entries.push(self.keyspace.client(&lease.runtime));
                    }
                }
                None => {
                    if let Some((stable_id, epoch)) = parsed {
                        let by_stable_key = self
                            .keyspace
                            .client_by_stable_index(&ClientStableId::new(stable_id));
                        stale_by_stable
                            .entry(by_stable_key)
                            .or_default()
                            .push(epoch.to_string());
                    }
                    stale_expiry_entries.push(key.clone());
                    stale.push(key);
                }
            }
        }
        if !stale.is_empty() || !stale_by_stable.is_empty() || !stale_expiry_entries.is_empty() {
            let mut connection = self.connection("redis prune stale client metadata")?;
            if !stale.is_empty() {
                connection
                    .srem::<_, _, ()>(&index, stale)
                    .map_err(|error| metadata_error("redis srem stale client index", error))?;
            }
            for (by_stable_key, epochs) in stale_by_stable {
                connection
                    .srem::<_, _, ()>(&by_stable_key, epochs)
                    .map_err(|error| {
                        metadata_error("redis srem stale by-stable client index", error)
                    })?;
            }
            if !stale_expiry_entries.is_empty() {
                connection
                    .zrem::<_, _, ()>(
                        self.keyspace.client_lease_expiry_index(),
                        stale_expiry_entries,
                    )
                    .map_err(|error| {
                        metadata_error("redis zrem stale client lease expiry", error)
                    })?;
            }
        }
        Ok(leases)
    }

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> Result<()> {
        self.query_idempotent_write("redis publish segment", |connection| {
            let key = self.keyspace.segment(&segment.owner, &segment.segment_name);
            let owner_key = self.keyspace.segment_owner(&segment.segment_name);
            let existing_owner: Option<String> = connection.get(&owner_key)?;
            if let Some(existing_owner) = existing_owner.as_ref() {
                let Some(runtime) = ClientRuntimeId::from_storage_key(existing_owner) else {
                    return Err(store_error_to_redis_error(StoreError::Metadata(format!(
                        "invalid runtime storage key in segment owner index: {existing_owner}"
                    ))));
                };
                if runtime != segment.owner {
                    return Err(store_error_to_redis_error(StoreError::Conflict(format!(
                        "segment {} is already owned by live runtime {}",
                        segment.segment_name.0, runtime
                    ))));
                }
            }
            let current_payload: Option<String> = redis::cmd("HGET")
                .arg(&key)
                .arg("state_json")
                .query(connection)?;
            let mut state = match current_payload.as_ref() {
                Some(payload) => serde_json::from_str::<StoredSegmentState>(payload)
                    .map_err(|error| store_error_to_redis_error(json_error(error)))?,
                None => StoredSegmentState::new(segment.clone()),
            };
            state.merge_announcement(segment);
            redis::cmd("WATCH")
                .arg(&key)
                .arg(&owner_key)
                .query::<()>(connection)?;
            let watched_owner: Option<String> = connection.get(&owner_key)?;
            if watched_owner != existing_owner {
                redis::cmd("UNWATCH").query::<()>(connection)?;
                return Err(redis::RedisError::from((
                    redis::ErrorKind::BusyLoadingError,
                    "segment owner changed during publish",
                )));
            }
            let watched_payload: Option<String> = redis::cmd("HGET")
                .arg(&key)
                .arg("state_json")
                .query(connection)?;
            if watched_payload != current_payload {
                redis::cmd("UNWATCH").query::<()>(connection)?;
                return Err(redis::RedisError::from((
                    redis::ErrorKind::BusyLoadingError,
                    "segment state changed during publish",
                )));
            }
            let mut pipe = redis::pipe();
            pipe.atomic();
            pipe.cmd("HSET")
                .arg(&key)
                .arg("owner")
                .arg(segment.owner.storage_key())
                .arg("segment_name")
                .arg(&segment.segment_name.0)
                .arg("used_bytes")
                .arg(state.announcement.used_bytes.to_string())
                .arg("capacity_bytes")
                .arg(state.announcement.capacity_bytes.to_string())
                .arg("state")
                .arg(format!("{:?}", state.announcement.state))
                .arg("alignment_bytes")
                .arg(state.announcement.alignment_bytes.to_string())
                .arg("state_json")
                .arg(
                    serde_json::to_string(&state)
                        .map_err(|error| store_error_to_redis_error(json_error(error)))?,
                );
            pipe.cmd("SADD")
                .arg(self.keyspace.segment_index(None))
                .arg(key.as_str());
            pipe.cmd("SADD")
                .arg(self.keyspace.segment_index(Some(&segment.owner)))
                .arg(key.as_str());
            pipe.cmd("SET")
                .arg(&owner_key)
                .arg(segment.owner.storage_key());
            pipe.query::<()>(connection)
        })
    }

    fn unpublish_segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> Result<()> {
        let key = self.keyspace.segment(owner, segment);
        let owner_key = self.keyspace.segment_owner(segment);
        self.query_idempotent_write("redis unpublish segment", |connection| {
            let indexed_owner: Option<String> = connection.get(&owner_key)?;
            let mut pipe = redis::pipe();
            pipe.atomic();
            pipe.del(&key).ignore();
            if indexed_owner.as_deref() == Some(owner.storage_key().as_str()) {
                pipe.del(&owner_key).ignore();
            }
            pipe.srem(self.keyspace.segment_index(None), key.as_str())
                .ignore();
            pipe.srem(self.keyspace.segment_index(Some(owner)), key.as_str())
                .ignore();
            pipe.query::<()>(connection)
        })
    }

    fn get_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
    ) -> Result<Option<SegmentAnnouncement>> {
        let key = self.keyspace.segment(owner, segment);
        let payload: Option<String> =
            self.query_readonly("redis get segment by owner and name", |connection| {
                redis::cmd("HGET")
                    .arg(&key)
                    .arg("state_json")
                    .query(connection)
            })?;
        payload
            .map(|payload| {
                serde_json::from_str::<StoredSegmentState>(&payload)
                    .map(|state| state.announcement)
                    .map_err(json_error)
            })
            .transpose()
    }

    fn list_segments(&self, owner: Option<&ClientRuntimeId>) -> Result<Vec<SegmentAnnouncement>> {
        let index = self.keyspace.segment_index(owner);
        let keys = self.query_readonly("redis list segments", |connection| {
            let mut keys: Vec<String> = connection.smembers(&index)?;
            if keys.is_empty() {
                keys = redis::cmd("KEYS")
                    .arg(self.keyspace.segment_pattern(owner))
                    .query(connection)?;
            }
            Ok(keys)
        })?;
        let entries = self.query_readonly("redis fetch segment states", |connection| {
            let mut entries = Vec::with_capacity(keys.len());
            for key in &keys {
                let payload: Option<String> = redis::cmd("HGET")
                    .arg(key.as_str())
                    .arg("state_json")
                    .query(connection)?;
                entries.push((key.clone(), payload));
            }
            Ok(entries)
        })?;
        let mut stale = Vec::new();
        let mut segments = Vec::with_capacity(entries.len());
        for (key, payload) in entries {
            if let Some(payload) = payload {
                let state =
                    serde_json::from_str::<StoredSegmentState>(&payload).map_err(json_error)?;
                segments.push(state.announcement);
            } else {
                stale.push(key);
            }
        }
        if !stale.is_empty() {
            let mut connection = self.connection("redis srem stale segment index")?;
            connection
                .srem::<_, _, ()>(&index, stale)
                .map_err(|error| metadata_error("redis srem stale segment index", error))?;
        }
        Ok(segments)
    }

    fn update_segment_state(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        next: SegmentLifecycleState,
    ) -> Result<()> {
        self.query_idempotent_write("redis update segment state", |connection| {
            let mut state = self
                .load_segment_state(connection, owner, segment)
                .map_err(store_error_to_redis_error)?;
            state.announcement.state = next;
            self.store_segment_state(connection, &state)
                .map_err(store_error_to_redis_error)
        })
    }

    fn reserve_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        length_bytes: u64,
    ) -> Result<SegmentReservation> {
        let mut connection = self.connection("redis reserve segment")?;
        let key = self.keyspace.segment(owner, segment);
        let result = Script::new(RESERVE_SEGMENT_SCRIPT)
            .key(&key)
            .arg(length_bytes)
            .invoke::<(i32, i64)>(&mut connection)
            .map_err(|error| metadata_error("redis reserve segment", error))?;
        match result.0 {
            1 => Ok(SegmentReservation {
                owner: owner.clone(),
                segment_name: segment.clone(),
                offset_bytes: result.1 as u64,
                length_bytes,
            }),
            _ if result.1 == -1 => Err(StoreError::NotFound(key)),
            _ if result.1 == -2 => Err(StoreError::InvalidState(format!(
                "segment {}:{} is not active",
                owner, segment.0
            ))),
            _ => Err(StoreError::Allocator(format!(
                "segment capacity exhausted for {}:{} requested={} remaining={}",
                owner, segment.0, length_bytes, result.1
            ))),
        }
    }

    fn release_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        let mut connection = self.connection("redis release segment")?;
        let key = self.keyspace.segment(owner, segment);
        let result = Script::new(RELEASE_SEGMENT_SCRIPT)
            .key(&key)
            .arg(offset_bytes)
            .arg(length_bytes)
            .invoke::<(i32, i64)>(&mut connection)
            .map_err(|error| metadata_error("redis release segment", error))?;
        match result.0 {
            1 => Ok(()),
            _ if result.1 == -1 => Err(StoreError::NotFound(key)),
            _ => Err(StoreError::Allocator(format!(
                "segment release rejected for {}:{} offset={} len={}",
                owner, segment.0, offset_bytes, length_bytes
            ))),
        }
    }

    fn get_object_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        let key = self.keyspace.object(key);
        let payload: Option<String> =
            self.query_readonly("redis hget route payload", |connection| {
                redis::cmd("HGET")
                    .arg(key.as_str())
                    .arg("payload")
                    .query(connection)
            })?;
        payload
            .map(|payload| serde_json::from_str(&payload).map_err(json_error))
            .transpose()
    }

    fn list_object_routes(&self) -> Result<Vec<ObjectRoute>> {
        let index = self.keyspace.object_index();
        let mut index_empty = false;
        let mut keys = self.query_readonly("redis list object routes", |connection| {
            let mut keys: Vec<String> = connection.smembers(&index)?;
            if keys.is_empty() {
                index_empty = true;
                keys = redis::cmd("KEYS")
                    .arg(self.keyspace.object_pattern())
                    .query(connection)?;
            }
            Ok(keys)
        })?;
        let (routes, _live_keys) = self.load_object_routes_from_keys(&mut keys, None)?;
        if !keys.is_empty() {
            let mut connection = self.connection("redis sadd legacy object index")?;
            connection
                .sadd::<_, _, ()>(&index, keys.clone())
                .map_err(|error| metadata_error("redis sadd legacy object index", error))?;
        }
        Ok(routes)
    }

    fn list_object_routes_by_cold_backing(
        &self,
        filter: &ColdBackingRouteFilter,
    ) -> Result<Vec<ObjectRoute>> {
        let Some(index) = self.cold_backing_filter_index(filter) else {
            return MetadataBackend::list_object_routes_by_cold_backing(self, filter);
        };
        let mut keys = self
            .query_readonly("redis list object routes by cold backing", |connection| {
                connection.smembers::<_, Vec<String>>(&index)
            })?;
        let original_keys = keys.clone();
        let (mut routes, live_keys) = self.load_object_routes_from_keys(&mut keys, filter.limit)?;
        let stale_keys = original_keys
            .into_iter()
            .filter(|key| !keys.contains(key))
            .collect::<Vec<_>>();
        if !stale_keys.is_empty() {
            let mut connection = self.connection("redis prune stale cold backing route index")?;
            connection
                .srem::<_, _, ()>(&index, stale_keys)
                .map_err(|error| metadata_error("redis srem cold backing route index", error))?;
        }
        if keys.is_empty() || routes.is_empty() {
            let all_routes = self.list_object_routes()?;
            routes = all_routes
                .into_iter()
                .filter(|route| route_matches_cold_backing_filter(route, filter))
                .take(filter.limit.unwrap_or(usize::MAX))
                .collect();
            if !routes.is_empty() {
                let index_keys = routes
                    .iter()
                    .map(|route| self.keyspace.object(&route.key))
                    .collect::<Vec<_>>();
                let mut connection = self.connection("redis backfill cold backing route index")?;
                connection
                    .sadd::<_, _, ()>(&index, index_keys)
                    .map_err(|error| {
                        metadata_error("redis sadd cold backing route index", error)
                    })?;
            }
            return Ok(routes);
        }
        if !live_keys.is_empty() {
            let mut connection = self.connection("redis refresh cold backing route index")?;
            connection
                .sadd::<_, _, ()>(&index, live_keys)
                .map_err(|error| metadata_error("redis sadd cold backing route index", error))?;
        }
        Ok(routes
            .into_iter()
            .filter(|route| route_matches_cold_backing_filter(route, filter))
            .take(filter.limit.unwrap_or(usize::MAX))
            .collect())
    }

    fn compare_and_swap_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        let mut connection = self.connection("redis cas object route")?;
        let payload = next
            .map(|route| serde_json::to_string(route).map_err(json_error))
            .transpose()?;
        let next_version = next.map(|route| route.version.0).unwrap_or_default();
        let object_key = self.keyspace.object(key);
        let current = self.get_object_route(key)?;
        let old_device_index = current
            .as_ref()
            .and_then(|route| route.cold_backing.as_ref())
            .map(|backing| {
                self.keyspace
                    .object_cold_tier_device_index(&backing.cold_tier_id)
            })
            .unwrap_or_default();
        let old_state_index = current
            .as_ref()
            .and_then(|route| route.cold_backing.as_ref())
            .map(|backing| self.keyspace.object_cold_backing_state_index(backing.state))
            .unwrap_or_default();
        let old_owner_index = current
            .as_ref()
            .and_then(|route| route.cold_backing.as_ref())
            .map(|backing| {
                self.keyspace
                    .object_cold_backing_owner_index(&backing.owner)
            })
            .unwrap_or_default();
        let new_device_index = next
            .and_then(|route| route.cold_backing.as_ref())
            .map(|backing| {
                self.keyspace
                    .object_cold_tier_device_index(&backing.cold_tier_id)
            })
            .unwrap_or_default();
        let new_state_index = next
            .and_then(|route| route.cold_backing.as_ref())
            .map(|backing| self.keyspace.object_cold_backing_state_index(backing.state))
            .unwrap_or_default();
        let new_owner_index = next
            .and_then(|route| route.cold_backing.as_ref())
            .map(|backing| {
                self.keyspace
                    .object_cold_backing_owner_index(&backing.owner)
            })
            .unwrap_or_default();
        let current_payload = Script::new(CAS_OBJECT_ROUTE_SCRIPT)
            .key(&object_key)
            .key(self.keyspace.object_index())
            .key(old_device_index.as_str())
            .key(old_state_index.as_str())
            .key(old_owner_index.as_str())
            .key(new_device_index.as_str())
            .key(new_state_index.as_str())
            .key(new_owner_index.as_str())
            .arg(
                expected
                    .map(|version| version.0.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(next_version.to_string())
            .arg(payload.clone().unwrap_or_else(|| "__delete__".to_string()))
            .arg(if old_device_index.is_empty() {
                "0"
            } else {
                "1"
            })
            .arg(if old_state_index.is_empty() { "0" } else { "1" })
            .arg(if old_owner_index.is_empty() { "0" } else { "1" })
            .arg(if new_device_index.is_empty() {
                "0"
            } else {
                "1"
            })
            .arg(if new_state_index.is_empty() { "0" } else { "1" })
            .arg(if new_owner_index.is_empty() { "0" } else { "1" })
            .invoke::<(i32, String)>(&mut connection)
            .map_err(|error| metadata_error("redis cas object route", error))?;

        let current: Option<ObjectRoute> = if current_payload.1.is_empty() {
            None
        } else {
            Some(serde_json::from_str(&current_payload.1).map_err(json_error)?)
        };
        Ok(CasResult {
            applied: current_payload.0 == 1,
            current,
            version_floor: None,
        })
    }

    fn put_cold_tier_device_if_absent(
        &self,
        device: &ColdTierDeviceRecord,
    ) -> Result<ColdTierPutDeviceResult> {
        let key = self.keyspace.cold_tier_device(&device.device_id);
        let payload = serde_json::to_string(device).map_err(json_error)?;
        let mut connection = self.connection("redis create cold tier device")?;
        let inserted: bool = connection
            .set_nx(key.as_str(), payload.as_str())
            .map_err(|error| metadata_error("redis setnx cold tier device", error))?;
        if inserted {
            connection
                .sadd::<_, _, ()>(self.keyspace.cold_tier_device_index(), key)
                .map_err(|error| metadata_error("redis sadd cold tier device index", error))?;
            return Ok(ColdTierPutDeviceResult::Created(device.clone()));
        }
        let payload: String = connection
            .get(key.as_str())
            .map_err(|error| metadata_error("redis get existing cold tier device", error))?;
        Ok(ColdTierPutDeviceResult::Existing(
            serde_json::from_str(&payload).map_err(json_error)?,
        ))
    }

    fn get_cold_tier_device(&self, device_id: &str) -> Result<Option<ColdTierDeviceRecord>> {
        let key = self.keyspace.cold_tier_device(device_id);
        let mut connection = self.connection("redis get cold tier device")?;
        let payload: Option<String> = connection
            .get(key)
            .map_err(|error| metadata_error("redis get cold tier device", error))?;
        payload
            .map(|payload| serde_json::from_str(&payload).map_err(json_error))
            .transpose()
    }

    fn list_cold_tier_devices(
        &self,
        filter: &ColdTierDeviceFilter,
    ) -> Result<Vec<ColdTierDeviceRecord>> {
        let index = self.keyspace.cold_tier_device_index();
        let prefix = self.keyspace.cold_tier_device_prefix();
        let mut connection = self.connection("redis list cold tier devices")?;
        let mut keys: Vec<String> = connection
            .smembers(&index)
            .map_err(|error| metadata_error("redis smembers cold tier device index", error))?;
        if keys.is_empty() {
            keys = scan_keys(&mut connection, &format!("{}*", prefix))?;
            if !keys.is_empty() {
                connection
                    .sadd::<_, _, ()>(&index, keys.clone())
                    .map_err(|error| metadata_error("redis sadd cold tier device index", error))?;
            }
        }
        let mut stale = Vec::new();
        let mut devices = Vec::new();
        for key in keys {
            let payload: Option<String> = connection
                .get(&key)
                .map_err(|error| metadata_error("redis get cold tier device list entry", error))?;
            let Some(payload) = payload else {
                stale.push(key);
                continue;
            };
            let device: ColdTierDeviceRecord =
                serde_json::from_str(&payload).map_err(json_error)?;
            if cold_tier_device_matches_filter(&device, filter) {
                devices.push(device);
            }
        }
        if !stale.is_empty() {
            connection
                .srem::<_, _, ()>(&index, stale)
                .map_err(|error| {
                    metadata_error("redis srem stale cold tier device index", error)
                })?;
        }
        devices.sort_by(|left, right| left.device_id.cmp(&right.device_id));
        Ok(devices)
    }

    fn update_cold_tier_device(
        &self,
        device_id: &str,
        update: ColdTierDeviceUpdate,
    ) -> Result<ColdTierDeviceRecord> {
        let key = self.keyspace.cold_tier_device(device_id);
        let mut connection = self.connection("redis update cold tier device")?;
        let payload: Option<String> = connection
            .get(&key)
            .map_err(|error| metadata_error("redis get cold tier device for update", error))?;
        let Some(payload) = payload else {
            return Err(StoreError::NotFound(format!(
                "cold tier device {device_id} not found"
            )));
        };
        let mut device: ColdTierDeviceRecord =
            serde_json::from_str(&payload).map_err(json_error)?;
        if let Some(expected) = update.expected_updated_at_ms {
            if device.updated_at_ms != expected {
                return Err(StoreError::Conflict(format!(
                    "cold tier device {device_id} changed concurrently"
                )));
            }
        }
        update.apply(&mut device);
        let payload = serde_json::to_string(&device).map_err(json_error)?;
        connection
            .set::<_, _, ()>(key, payload)
            .map_err(|error| metadata_error("redis set cold tier device", error))?;
        Ok(device)
    }

    fn apply_cold_tier_usage_delta(
        &self,
        device_id: &str,
        delta: ColdTierUsageDelta,
        updated_at_ms: u64,
    ) -> Result<ColdTierDeviceRecord> {
        let key = self.keyspace.cold_tier_device(device_id);
        let script = Script::new(APPLY_COLD_TIER_USAGE_DELTA_SCRIPT);
        let mut connection = self.connection("redis apply cold tier usage delta")?;
        let response: (u8, String) = script
            .key(key)
            .arg(delta.used_bytes)
            .arg(delta.reserved_bytes)
            .arg(updated_at_ms)
            .invoke(&mut connection)
            .map_err(|error| metadata_error("redis apply cold tier usage delta", error))?;
        match response.0 {
            0 => Err(StoreError::NotFound(format!(
                "cold tier device {device_id} not found"
            ))),
            1 => Err(StoreError::InvalidState(format!(
                "cold tier device {device_id} usage delta would make accounting negative"
            ))),
            2 => serde_json::from_str(&response.1).map_err(json_error),
            status => Err(StoreError::Transport(format!(
                "unexpected redis cold tier usage delta status {status}"
            ))),
        }
    }

    fn get_route_policy(&self, domain: &RoutePolicyDomain) -> Result<Option<RoutePolicy>> {
        let key = self.keyspace.route_policy(domain);
        let payload: Option<String> =
            self.query_readonly("redis get route policy", |connection| connection.get(&key))?;
        payload
            .map(|payload| serde_json::from_str(&payload).map_err(json_error))
            .transpose()
    }

    fn put_route_policy_if_absent(
        &self,
        domain: &RoutePolicyDomain,
        policy: &RoutePolicy,
    ) -> Result<bool> {
        let key = self.keyspace.route_policy(domain);
        let payload = serde_json::to_string(policy).map_err(json_error)?;
        self.query_idempotent_write("redis setnx route policy", |connection| {
            connection.set_nx(key.as_str(), payload.as_str())
        })
    }

    fn put_route_policy(&self, domain: &RoutePolicyDomain, policy: &RoutePolicy) -> Result<()> {
        let mut connection = self.connection("redis set route policy")?;
        let key = self.keyspace.route_policy(domain);
        let payload = serde_json::to_string(policy).map_err(json_error)?;
        connection
            .set::<_, _, ()>(key, payload)
            .map_err(|error| metadata_error("redis set route policy", error))
    }

    fn delete_route_policy(&self, domain: &RoutePolicyDomain) -> Result<bool> {
        let mut connection = self.connection("redis del route policy")?;
        let key = self.keyspace.route_policy(domain);
        let removed: usize = connection
            .del(key)
            .map_err(|error| metadata_error("redis del route policy", error))?;
        Ok(removed != 0)
    }

    fn list_route_policies(&self) -> Result<Vec<(RoutePolicyDomain, RoutePolicy)>> {
        let mut connection = self.connection("redis list route policies")?;
        let prefix = self.keyspace.route_policy_prefix();
        let keys = scan_keys(&mut connection, &format!("{}*", prefix))?;
        let mut policies = Vec::new();
        for key in keys {
            let payload: Option<String> = connection
                .get(&key)
                .map_err(|error| metadata_error("redis get route policy", error))?;
            let Some(payload) = payload else {
                continue;
            };
            let Some(domain) = parse_route_policy_domain(&self.keyspace, &key) else {
                continue;
            };
            policies.push((domain, serde_json::from_str(&payload).map_err(json_error)?));
        }
        policies.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(policies)
    }

    fn get_tenant_policy(&self, scope: &TenantPolicyScope) -> Result<Option<TenantPolicy>> {
        let mut connection = self.connection("redis get tenant policy")?;
        let key = self.keyspace.tenant_policy(scope);
        let payload: Option<String> = connection
            .get(&key)
            .map_err(|error| metadata_error("redis get tenant policy", error))?;
        payload
            .map(|payload| serde_json::from_str(&payload).map_err(json_error))
            .transpose()
    }

    fn get_tenant_policies(
        &self,
        scopes: &[TenantPolicyScope],
    ) -> Result<Vec<Option<TenantPolicy>>> {
        if scopes.is_empty() {
            return Ok(Vec::new());
        }
        let mut connection = self.connection("redis mget tenant policies")?;
        let keys = scopes
            .iter()
            .map(|scope| self.keyspace.tenant_policy(scope))
            .collect::<Vec<_>>();
        let payloads: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&keys)
            .query(&mut connection)
            .map_err(|error| metadata_error("redis mget tenant policies", error))?;
        payloads
            .into_iter()
            .map(|payload| {
                payload
                    .map(|payload| serde_json::from_str(&payload).map_err(json_error))
                    .transpose()
            })
            .collect()
    }

    fn list_tenant_policies(&self, tenant: Option<&str>) -> Result<Vec<TenantPolicy>> {
        let mut connection = self.connection("redis list tenant policies")?;
        let keys = if let Some(tenant) = tenant {
            let index_key = self.keyspace.tenant_policy_index(tenant);
            let ready_key = self.keyspace.tenant_policy_index_ready(tenant);
            if connection
                .exists(&ready_key)
                .map_err(|error| metadata_error("redis check tenant policy index ready", error))?
            {
                connection
                    .smembers(&index_key)
                    .map_err(|error| metadata_error("redis list tenant policy index", error))?
            } else {
                let lock_key = format!("{ready_key}:lock");
                let acquired_lock = redis::cmd("SET")
                    .arg(&lock_key)
                    .arg("1")
                    .arg("NX")
                    .arg("EX")
                    .arg(60)
                    .query::<Option<String>>(&mut connection)
                    .map_err(|error| {
                        metadata_error("redis acquire tenant policy index backfill lock", error)
                    })?
                    .is_some();

                if acquired_lock {
                    if connection.exists(&ready_key).map_err(|error| {
                        metadata_error("redis recheck tenant policy index ready", error)
                    })? {
                        connection.smembers(&index_key).map_err(|error| {
                            metadata_error("redis list tenant policy index", error)
                        })?
                    } else {
                        // The first scoped list migrates legacy Redis tenant-policy keys into the
                        // per-tenant index. After the ready marker is set, tenant-policy writers
                        // are expected to go through this backend so CAS writes maintain the index.
                        let keys =
                            legacy_tenant_policy_keys(&self.keyspace, tenant, &mut connection)?;
                        let mut pipe = redis::pipe();
                        pipe.atomic();
                        if !keys.is_empty() {
                            pipe.cmd("SADD").arg(&index_key).arg(&keys).ignore();
                        }
                        pipe.cmd("SET").arg(&ready_key).arg("1").ignore();
                        pipe.cmd("DEL").arg(&lock_key).ignore();
                        pipe.query::<()>(&mut connection).map_err(|error| {
                            metadata_error("redis mark tenant policy index ready", error)
                        })?;
                        keys
                    }
                } else if connection.exists(&ready_key).map_err(|error| {
                    metadata_error("redis recheck tenant policy index ready", error)
                })? {
                    connection
                        .smembers(&index_key)
                        .map_err(|error| metadata_error("redis list tenant policy index", error))?
                } else {
                    legacy_tenant_policy_keys(&self.keyspace, tenant, &mut connection)?
                }
            }
        } else {
            let prefix = self.keyspace.tenant_policy_prefix(None);
            scan_keys(&mut connection, &format!("{}*", prefix))?
        };
        let mut policies: Vec<TenantPolicy> = Vec::new();
        for key in keys {
            let Some(scope) = parse_tenant_policy_scope(&self.keyspace, &key) else {
                if let Some(tenant) = tenant {
                    connection
                        .srem::<_, _, ()>(self.keyspace.tenant_policy_index(tenant), &key)
                        .map_err(|error| {
                            metadata_error("redis prune malformed tenant policy index", error)
                        })?;
                }
                continue;
            };
            if let Some(tenant) = tenant {
                if scope.tenant != tenant {
                    connection
                        .srem::<_, _, ()>(self.keyspace.tenant_policy_index(tenant), &key)
                        .map_err(|error| {
                            metadata_error("redis prune mismatched tenant policy index", error)
                        })?;
                    continue;
                }
            }
            let payload: Option<String> = connection
                .get(&key)
                .map_err(|error| metadata_error("redis get tenant policy", error))?;
            let Some(payload) = payload else {
                if let Some(tenant) = tenant {
                    Script::new(PRUNE_MISSING_TENANT_POLICY_INDEX_SCRIPT)
                        .key(self.keyspace.tenant_policy_index(tenant))
                        .key(&key)
                        .invoke::<usize>(&mut connection)
                        .map_err(|error| {
                            metadata_error("redis prune stale tenant policy index", error)
                        })?;
                }
                continue;
            };
            let policy = parse_tenant_policy_payload(&payload, &key)?;
            if policy.scope != scope {
                if let Some(tenant) = tenant {
                    Script::new(PRUNE_UNCHANGED_TENANT_POLICY_INDEX_SCRIPT)
                        .key(self.keyspace.tenant_policy_index(tenant))
                        .key(&key)
                        .arg(&payload)
                        .invoke::<usize>(&mut connection)
                        .map_err(|error| {
                            metadata_error("redis prune inconsistent tenant policy index", error)
                        })?;
                }
                continue;
            }
            policies.push(policy);
        }
        policies.sort_by(|left, right| left.scope.cmp(&right.scope));
        Ok(policies)
    }

    fn put_tenant_policy(
        &self,
        policy: &TenantPolicy,
        expected_version: Option<u64>,
    ) -> Result<TenantPolicy> {
        policy.validate()?;
        let mut connection = self.connection("redis cas tenant policy")?;
        let key = self.keyspace.tenant_policy(&policy.scope);
        let index_key = self.keyspace.tenant_policy_index(&policy.scope.tenant);
        let payload = serde_json::to_string(policy).map_err(json_error)?;
        let result = Script::new(CAS_TENANT_POLICY_SCRIPT)
            .key(&key)
            .key(&index_key)
            .arg(
                expected_version
                    .map(|version| version.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(payload)
            .invoke::<(i32, String)>(&mut connection)
            .map_err(|error| metadata_error("redis cas tenant policy", error))?;
        if result.0 == 1 {
            return Ok(policy.clone());
        }
        let current = if result.1.is_empty() {
            None
        } else {
            Some(parse_tenant_policy_cas_payload(
                &result.1,
                "redis cas tenant policy",
            )?)
        };
        match (expected_version, current.as_ref()) {
            (None, Some(_)) => Err(StoreError::Conflict(format!(
                "tenant policy already exists for {}",
                policy.scope.tenant
            ))),
            (Some(expected), Some(current)) => Err(StoreError::Conflict(format!(
                "tenant policy version mismatch for {}: expected={} actual={}",
                policy.scope.tenant, expected, current.version
            ))),
            (Some(expected), None) => Err(StoreError::Conflict(format!(
                "tenant policy missing for {} at expected version {}",
                policy.scope.tenant, expected
            ))),
            (None, None) => Err(StoreError::Conflict(format!(
                "tenant policy write rejected for {}",
                policy.scope.tenant
            ))),
        }
    }

    fn delete_tenant_policy(
        &self,
        scope: &TenantPolicyScope,
        expected_version: Option<u64>,
    ) -> Result<bool> {
        let mut connection = self.connection("redis delete tenant policy")?;
        let key = self.keyspace.tenant_policy(scope);
        let index_key = self.keyspace.tenant_policy_index(&scope.tenant);
        let result = Script::new(CAS_TENANT_POLICY_SCRIPT)
            .key(&key)
            .key(&index_key)
            .arg(
                expected_version
                    .map(|version| version.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg("__delete__")
            .invoke::<(i32, String)>(&mut connection)
            .map_err(|error| metadata_error("redis delete tenant policy", error))?;
        if result.0 == 1 {
            return Ok(true);
        }
        let current = if result.1.is_empty() {
            None
        } else {
            Some(parse_tenant_policy_cas_payload(
                &result.1,
                "redis delete tenant policy",
            )?)
        };
        match (expected_version, current.as_ref()) {
            (None, None) => Ok(false),
            (Some(expected), Some(current)) => Err(StoreError::Conflict(format!(
                "tenant policy version mismatch for {}: expected={} actual={}",
                scope.tenant, expected, current.version
            ))),
            (Some(expected), None) => Err(StoreError::Conflict(format!(
                "tenant policy missing for {} at expected version {}",
                scope.tenant, expected
            ))),
            (None, Some(_)) => Err(StoreError::Conflict(format!(
                "tenant policy delete rejected for {}",
                scope.tenant
            ))),
        }
    }

    fn get_tenant_quota_state(
        &self,
        scope: &TenantPolicyScope,
    ) -> Result<Option<TenantQuotaState>> {
        let scope = quota_scope(scope)?;
        let mut connection = self.connection("redis get tenant quota state")?;
        let key = self.keyspace.tenant_quota_state(&scope);
        let payload: Option<String> = connection
            .get(&key)
            .map_err(|error| metadata_error("redis get tenant quota state", error))?;
        payload
            .map(|payload| {
                parse_tenant_quota_state_payload(&payload, "redis get tenant quota state")
            })
            .transpose()
    }

    fn get_tenant_object_accounting(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<TenantObjectAccounting>> {
        let mut connection = self.connection("redis get tenant object accounting")?;
        let storage_key = self.keyspace.tenant_object_accounting(key);
        let payload: Option<String> = connection
            .get(&storage_key)
            .map_err(|error| metadata_error("redis get tenant object accounting", error))?;
        payload
            .map(|payload| {
                parse_tenant_object_accounting_payload(
                    &payload,
                    "redis get tenant object accounting",
                )
            })
            .transpose()
    }

    fn get_tenant_quota_reservation(
        &self,
        reservation_id: &str,
    ) -> Result<Option<TenantQuotaReservation>> {
        let mut connection = self.connection("redis get tenant quota reservation")?;
        let reservation_key = self.keyspace.tenant_quota_reservation(reservation_id);
        let payload: Option<String> = connection
            .get(&reservation_key)
            .map_err(|error| metadata_error("redis get tenant quota reservation", error))?;
        payload
            .map(|payload| {
                parse_tenant_quota_reservation_payload(
                    &payload,
                    "redis get tenant quota reservation",
                )
            })
            .transpose()
    }

    fn list_tenant_eviction_candidates(
        &self,
        scope: &TenantPolicyScope,
        limit: usize,
    ) -> Result<Vec<TenantObjectAccounting>> {
        let scope = quota_scope(scope)?;
        let read_limit = limit.min(EVICTION_FRONTIER_LIMIT);
        if read_limit == 0 {
            return Ok(Vec::new());
        }
        let mut connection = self.connection("redis list tenant eviction frontier")?;
        let frontier_key = self.keyspace.tenant_eviction_frontier(&scope);
        let members: Vec<String> = cmd("ZRANGE")
            .arg(&frontier_key)
            .arg(0)
            .arg(read_limit.saturating_sub(1))
            .query(&mut connection)
            .map_err(|error| metadata_error("redis list tenant eviction frontier", error))?;
        let mut candidates = Vec::new();
        for member in members {
            let Some(key) = parse_tenant_eviction_candidate_key(&self.keyspace, &scope, &member)
            else {
                continue;
            };
            let payload: Option<String> = connection
                .get(self.keyspace.tenant_object_accounting(&key))
                .map_err(|error| {
                    metadata_error("redis get tenant eviction candidate object", error)
                })?;
            let Some(payload) = payload else {
                continue;
            };
            let object = parse_tenant_object_accounting_payload(
                &payload,
                "redis list tenant eviction candidates",
            )?;
            if object.scope == scope && object.state == TenantObjectAccountingState::Active {
                candidates.push(object);
            }
        }
        candidates.sort_by(|left, right| {
            left.updated_at_ms
                .cmp(&right.updated_at_ms)
                .then_with(|| right.committed_length.cmp(&left.committed_length))
                .then_with(|| left.key.cmp(&right.key))
        });
        candidates.truncate(read_limit);
        Ok(candidates)
    }

    fn list_tenant_quota_reservations(
        &self,
        scope: &TenantPolicyScope,
    ) -> Result<Vec<TenantQuotaReservation>> {
        let scope = quota_scope(scope)?;
        let mut connection = self.connection("redis list tenant quota reservations")?;
        let index_prefix = self.keyspace.tenant_quota_reservation_prefix(Some(&scope));
        let index_keys = scan_keys(&mut connection, &format!("{}*", index_prefix))?;
        let mut reservations = Vec::new();
        for index_key in index_keys {
            let reservation_key: Option<String> =
                connection.get(index_key.as_str()).map_err(|error| {
                    metadata_error("redis get tenant quota reservation index", error)
                })?;
            let Some(reservation_key) = reservation_key else {
                connection
                    .del::<_, ()>(index_key.as_str())
                    .map_err(|error| {
                        metadata_error("redis del stale tenant quota reservation index", error)
                    })?;
                continue;
            };
            let payload: Option<String> = connection
                .get(reservation_key.as_str())
                .map_err(|error| metadata_error("redis get tenant quota reservation", error))?;
            let Some(payload) = payload else {
                connection
                    .del::<_, ()>(index_key.as_str())
                    .map_err(|error| {
                        metadata_error("redis del dangling tenant quota reservation index", error)
                    })?;
                continue;
            };
            let reservation = parse_tenant_quota_reservation_payload(
                &payload,
                "redis list tenant quota reservations",
            )?;
            if reservation.scope == scope {
                reservations.push(reservation);
            }
        }
        reservations.sort_by(|left, right| left.reservation_id.cmp(&right.reservation_id));
        Ok(reservations)
    }

    fn reserve_tenant_quota(
        &self,
        request: &TenantQuotaReservationRequest,
    ) -> Result<TenantQuotaReservationOutcome> {
        request.validate()?;
        let scope = quota_scope(&request.scope)?;
        let mut normalized = request.clone();
        normalized.scope = scope.clone();
        let mut connection = self.connection("redis reserve tenant quota")?;
        let quota_key = self.keyspace.tenant_quota_state(&scope);
        let object_key = self.keyspace.tenant_object_accounting(&normalized.key);
        let reservation_key = self
            .keyspace
            .tenant_quota_reservation(&normalized.reservation_id);
        let reservation_index_key = self
            .keyspace
            .tenant_quota_reservation_index(&scope, &normalized.reservation_id);
        let scope_payload = serde_json::to_string(&scope).map_err(json_error)?;
        let writer_runtime_payload =
            serde_json::to_string(&normalized.writer_runtime).map_err(json_error)?;
        let result = Script::new(RESERVE_TENANT_QUOTA_SCRIPT)
            .key(&quota_key)
            .key(&object_key)
            .key(&reservation_key)
            .key(&reservation_index_key)
            .arg(&normalized.reservation_id)
            .arg(scope_payload)
            .arg(&normalized.key.0)
            .arg(
                normalized
                    .expected_object_version
                    .map(|version| version.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(normalized.delta_bytes)
            .arg(normalized.delta_objects)
            .arg(
                normalized
                    .limit
                    .max_bytes
                    .map(|limit| limit.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(
                normalized
                    .limit
                    .max_objects
                    .map(|limit| limit.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(normalized.expires_at_ms)
            .arg(normalized.created_at_ms)
            .arg(writer_runtime_payload)
            .invoke::<(i32, String, String, String)>(&mut connection)
            .map_err(|error| metadata_error("redis reserve tenant quota", error))?;

        match result.0 {
            1 | 2 => Ok(TenantQuotaReservationOutcome {
                quota: parse_tenant_quota_state_payload(&result.1, "redis reserve tenant quota")?,
                object: parse_optional_tenant_object_accounting_payload(
                    &result.2,
                    "redis reserve tenant quota",
                )?,
                reservation: parse_tenant_quota_reservation_payload(
                    &result.3,
                    "redis reserve tenant quota",
                )?,
            }),
            -2 => Err(StoreError::Conflict(format!(
                "tenant quota reservation {} already exists with different parameters",
                normalized.reservation_id
            ))),
            -3 => Err(version_conflict(
                "tenant object accounting",
                &scope,
                normalized.expected_object_version,
                parse_optional_tenant_object_accounting_payload(
                    &result.1,
                    "redis reserve tenant quota conflict",
                )?
                .as_ref()
                .map(|object| object.version),
            )),
            -4 => Err(StoreError::InvalidState(format!(
                "tenant object accounting scope mismatch for key {}",
                normalized.key.0
            ))),
            -5 => {
                let quota =
                    parse_tenant_quota_state_payload(&result.1, "redis reserve tenant quota")?;
                Err(StoreError::QuotaExceeded {
                    kind: QuotaKind::Bytes,
                    message: format!(
                        "tenant quota bytes exceeded for {}: used={} pending={} requested={} limit={}",
                        scope.tenant,
                        quota.used_bytes,
                        quota.pending_reserved_bytes,
                        normalized.delta_bytes.max(0),
                        normalized.limit.max_bytes.unwrap_or_default()
                    ),
                })
            }
            -6 => {
                let quota =
                    parse_tenant_quota_state_payload(&result.1, "redis reserve tenant quota")?;
                Err(StoreError::QuotaExceeded {
                    kind: QuotaKind::Objects,
                    message: format!(
                        "tenant quota objects exceeded for {}: used={} pending={} requested={} limit={}",
                        scope.tenant,
                        quota.used_objects,
                        quota.pending_reserved_objects,
                        normalized.delta_objects.max(0),
                        normalized.limit.max_objects.unwrap_or_default()
                    ),
                })
            }
            code => Err(StoreError::Metadata(format!(
                "redis reserve tenant quota: unexpected status code {code}"
            ))),
        }
    }

    fn finalize_tenant_quota(
        &self,
        request: &TenantQuotaFinalizeRequest,
    ) -> Result<TenantQuotaFinalizeOutcome> {
        request.validate()?;
        let reservation_key = self
            .keyspace
            .tenant_quota_reservation(&request.reservation_id);
        let reservation_payload: Option<String> = {
            let mut connection = self.connection("redis finalize tenant quota")?;
            connection.get(&reservation_key).map_err(|error| {
                metadata_error("redis get tenant quota reservation before finalize", error)
            })?
        };
        let reservation_payload = reservation_payload
            .ok_or_else(|| StoreError::NotFound(request.reservation_id.clone()))?;
        let reservation = parse_tenant_quota_reservation_payload(
            &reservation_payload,
            "redis finalize tenant quota prefetch",
        )?;
        let scope = quota_scope(&reservation.scope)?;
        let quota_key = self.keyspace.tenant_quota_state(&scope);
        let object_key = self.keyspace.tenant_object_accounting(&reservation.key);
        let reservation_index_key = self
            .keyspace
            .tenant_quota_reservation_index(&scope, &request.reservation_id);
        let frontier_key = self.keyspace.tenant_eviction_frontier(&scope);
        let old_frontier_member = self
            .get_tenant_object_accounting(&reservation.key)?
            .map(|object| {
                self.keyspace.tenant_eviction_candidate(
                    &scope,
                    object.updated_at_ms,
                    object.committed_length,
                    &object.key,
                )
            })
            .unwrap_or_else(|| "__none__".to_string());
        let new_frontier_member = if request.state == TenantObjectAccountingState::Active {
            self.keyspace.tenant_eviction_candidate(
                &scope,
                request.updated_at_ms,
                request
                    .committed_length
                    .expect("validated active finalize request"),
                &reservation.key,
            )
        } else {
            "__none__".to_string()
        };
        let mut connection = self.connection("redis finalize tenant quota")?;
        let result = Script::new(FINALIZE_TENANT_QUOTA_SCRIPT)
            .key(&quota_key)
            .key(&object_key)
            .key(&reservation_key)
            .key(&reservation_index_key)
            .key(&frontier_key)
            .arg(&request.reservation_id)
            .arg(
                request
                    .expected_object_version
                    .map(|version| version.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(
                request
                    .committed_length
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(
                request
                    .route_version
                    .map(|version| version.0.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(match request.state {
                TenantObjectAccountingState::Active => "Active",
                TenantObjectAccountingState::Deleted => "Deleted",
            })
            .arg(request.updated_at_ms)
            .arg(&request.updated_by)
            .arg(self.tenant_quota_terminal_ttl.as_millis().max(1) as u64)
            .arg(old_frontier_member)
            .arg(new_frontier_member)
            .invoke::<(i32, String, String, String)>(&mut connection)
            .map_err(|error| metadata_error("redis finalize tenant quota", error))?;

        match result.0 {
            1 | 2 => Ok(TenantQuotaFinalizeOutcome {
                quota: parse_tenant_quota_state_payload(&result.1, "redis finalize tenant quota")?,
                object: parse_optional_tenant_object_accounting_payload(
                    &result.2,
                    "redis finalize tenant quota",
                )?,
                reservation: parse_tenant_quota_reservation_payload(
                    &result.3,
                    "redis finalize tenant quota",
                )?,
            }),
            -3 => Err(StoreError::Conflict(format!(
                "tenant quota reservation {} is already aborted",
                request.reservation_id
            ))),
            -4 => Err(version_conflict(
                "tenant object accounting",
                &scope,
                request
                    .expected_object_version
                    .or(reservation.expected_object_version),
                parse_optional_tenant_object_accounting_payload(
                    &result.1,
                    "redis finalize tenant quota conflict",
                )?
                .as_ref()
                .map(|object| object.version),
            )),
            -5 => Err(StoreError::InvalidState(format!(
                "tenant object accounting scope mismatch for key {}",
                reservation.key.0
            ))),
            -6 => Err(StoreError::InvalidState(format!(
                "tenant quota state missing for {} while finalizing reservation {}",
                scope.tenant, request.reservation_id
            ))),
            -7 => Err(StoreError::InvalidState(format!(
                "tenant quota state underflow while finalizing reservation {}",
                request.reservation_id
            ))),
            code => Err(StoreError::Metadata(format!(
                "redis finalize tenant quota: unexpected status code {code}"
            ))),
        }
    }

    fn abort_tenant_quota(&self, reservation_id: &str) -> Result<TenantQuotaAbortOutcome> {
        if reservation_id.is_empty() {
            return Err(StoreError::InvalidState(
                "tenant quota abort reservation_id must not be empty".to_string(),
            ));
        }
        let mut connection = self.connection("redis abort tenant quota")?;
        let reservation_key = self.keyspace.tenant_quota_reservation(reservation_id);
        let reservation_payload: Option<String> =
            connection.get(&reservation_key).map_err(|error| {
                metadata_error("redis get tenant quota reservation before abort", error)
            })?;
        let reservation_payload =
            reservation_payload.ok_or_else(|| StoreError::NotFound(reservation_id.to_string()))?;
        let reservation = parse_tenant_quota_reservation_payload(
            &reservation_payload,
            "redis abort tenant quota prefetch",
        )?;
        let scope = quota_scope(&reservation.scope)?;
        let quota_key = self.keyspace.tenant_quota_state(&scope);
        let reservation_index_key = self
            .keyspace
            .tenant_quota_reservation_index(&scope, reservation_id);
        let result = Script::new(ABORT_TENANT_QUOTA_SCRIPT)
            .key(&quota_key)
            .key(&reservation_key)
            .key(&reservation_index_key)
            .arg(reservation_id)
            .arg(self.tenant_quota_terminal_ttl.as_millis().max(1) as u64)
            .invoke::<(i32, String, String)>(&mut connection)
            .map_err(|error| metadata_error("redis abort tenant quota", error))?;
        match result.0 {
            1 | 2 => Ok(TenantQuotaAbortOutcome {
                quota: parse_tenant_quota_state_payload(&result.1, "redis abort tenant quota")?,
                reservation: parse_tenant_quota_reservation_payload(
                    &result.2,
                    "redis abort tenant quota",
                )?,
            }),
            -3 => Err(StoreError::Conflict(format!(
                "tenant quota reservation {} is already finalized",
                reservation_id
            ))),
            -4 => Err(StoreError::InvalidState(format!(
                "tenant quota state missing for {} while aborting reservation {}",
                scope.tenant, reservation_id
            ))),
            -5 => Err(StoreError::InvalidState(format!(
                "tenant quota state underflow while aborting reservation {}",
                reservation_id
            ))),
            code => Err(StoreError::Metadata(format!(
                "redis abort tenant quota: unexpected status code {code}"
            ))),
        }
    }

    fn put_handoff(&self, handoff: &HandoffPlan) -> Result<()> {
        let key = self.keyspace.handoff(&handoff.stable_id);
        let payload = serde_json::to_string(handoff).map_err(json_error)?;
        self.query_idempotent_write("redis set handoff", |connection| {
            connection.set::<_, _, ()>(key.as_str(), payload.as_str())
        })
    }

    fn get_handoff(&self, stable_id: &ClientStableId) -> Result<Option<HandoffPlan>> {
        let key = self.keyspace.handoff(stable_id);
        let payload: Option<String> =
            self.query_readonly("redis get handoff", |connection| connection.get(&key))?;
        payload
            .map(|payload| serde_json::from_str(&payload).map_err(json_error))
            .transpose()
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn metadata_error(operation: &str, error: redis::RedisError) -> StoreError {
    let mut message = format!("{operation}: {error}");
    if let Some(hint) = redis_error_diagnostic_hint(&error) {
        message.push_str(" (");
        message.push_str(hint);
        message.push(')');
    }
    StoreError::Metadata(message)
}

fn redis_error_diagnostic_hint(error: &redis::RedisError) -> Option<&'static str> {
    let detail = error.to_string().to_ascii_lowercase();
    let mut source: Option<&(dyn Error + 'static)> = error.source();
    while let Some(current) = source {
        if let Some(io_error) = current.downcast_ref::<std::io::Error>() {
            if io_error.kind() == std::io::ErrorKind::AddrNotAvailable
                || io_error.raw_os_error() == Some(99)
            {
                return Some(
                    "possible local ephemeral/source-port exhaustion while opening Redis metadata connections",
                );
            }
        }
        source = current.source();
    }
    if detail.contains("cannot assign requested address")
        || detail.contains("os error 99")
        || detail.contains("addrnotavailable")
    {
        return Some(
            "possible local ephemeral/source-port exhaustion while opening Redis metadata connections",
        );
    }
    None
}

fn should_log_redis_diagnostic_occurrence(occurrence: u64) -> bool {
    occurrence <= 8 || occurrence.is_power_of_two()
}

pub fn is_legacy_redis_auth_arity_error(message: &str) -> bool {
    let detail = message.to_ascii_lowercase();
    detail.contains("wrong number of arguments for 'auth' command")
        || detail.contains("wrong number of arguments for `auth` command")
        || detail.contains("wrong number of arguments for auth command")
}

#[cfg(test)]
fn should_retry_readonly_redis_error(error: &redis::RedisError) -> bool {
    should_retry_transient_redis_error(error)
}

fn should_retry_transient_redis_error(error: &redis::RedisError) -> bool {
    let detail = error.to_string().to_ascii_lowercase();
    error.is_timeout()
        || error.is_connection_dropped()
        || error.is_io_error()
        || detail.contains("interrupted")
        || detail.contains("connection refused")
        || detail.contains("connection reset")
        || detail.contains("connection aborted")
        || detail.contains("connection closed")
        || detail.contains("not connected")
        || detail.contains("broken pipe")
        || detail.contains("temporarily unavailable")
        || detail.contains("timed out")
}

fn json_error(error: serde_json::Error) -> StoreError {
    StoreError::Metadata(format!("json serialization: {error}"))
}

fn store_error_to_redis_error(error: StoreError) -> redis::RedisError {
    redis::RedisError::from((
        redis::ErrorKind::TypeError,
        "metadata error",
        error.to_string(),
    ))
}

fn redis_retry_attempts() -> usize {
    std::env::var(REDIS_RETRY_ATTEMPTS_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|attempts| *attempts > 0)
        .unwrap_or(DEFAULT_REDIS_RETRY_ATTEMPTS)
}

fn redis_retry_delay() -> Duration {
    duration_from_env_ms(REDIS_RETRY_DELAY_ENV, DEFAULT_REDIS_RETRY_DELAY)
}

fn parse_tenant_policy_cas_payload(payload: &str, operation: &str) -> Result<TenantPolicy> {
    serde_json::from_str::<TenantPolicy>(payload).map_err(|error| {
        StoreError::Metadata(format!(
            "{operation}: corrupted tenant policy payload returned from redis CAS: {error}"
        ))
    })
}

fn parse_tenant_policy_payload(payload: &str, key: &str) -> Result<TenantPolicy> {
    serde_json::from_str::<TenantPolicy>(payload).map_err(|error| {
        StoreError::Metadata(format!(
            "redis get tenant policy {key}: corrupted tenant policy payload from redis: {error}"
        ))
    })
}

fn parse_tenant_quota_state_payload(payload: &str, operation: &str) -> Result<TenantQuotaState> {
    serde_json::from_str::<TenantQuotaState>(payload).map_err(|error| {
        StoreError::Metadata(format!(
            "{operation}: corrupted tenant quota state payload from redis: {error}"
        ))
    })
}

fn parse_tenant_object_accounting_payload(
    payload: &str,
    operation: &str,
) -> Result<TenantObjectAccounting> {
    serde_json::from_str::<TenantObjectAccounting>(payload).map_err(|error| {
        StoreError::Metadata(format!(
            "{operation}: corrupted tenant object accounting payload from redis: {error}"
        ))
    })
}

fn parse_optional_tenant_object_accounting_payload(
    payload: &str,
    operation: &str,
) -> Result<Option<TenantObjectAccounting>> {
    if payload.is_empty() {
        return Ok(None);
    }
    parse_tenant_object_accounting_payload(payload, operation).map(Some)
}

fn parse_tenant_quota_reservation_payload(
    payload: &str,
    operation: &str,
) -> Result<TenantQuotaReservation> {
    serde_json::from_str::<TenantQuotaReservation>(payload).map_err(|error| {
        StoreError::Metadata(format!(
            "{operation}: corrupted tenant quota reservation payload from redis: {error}"
        ))
    })
}

fn quota_scope(scope: &TenantPolicyScope) -> Result<TenantPolicyScope> {
    scope.validate()?;
    Ok(scope.clone())
}

fn version_conflict(
    entity: &str,
    scope: &TenantPolicyScope,
    expected: Option<u64>,
    actual: Option<u64>,
) -> StoreError {
    match (expected, actual) {
        (Some(expected), Some(actual)) => StoreError::Conflict(format!(
            "{entity} version mismatch for {}: expected={} actual={}",
            scope.tenant, expected, actual
        )),
        (Some(expected), None) => StoreError::Conflict(format!(
            "{entity} missing for {} at expected version {}",
            scope.tenant, expected
        )),
        (None, Some(actual)) => StoreError::Conflict(format!(
            "{entity} already exists for {} at version {}",
            scope.tenant, actual
        )),
        (None, None) => {
            StoreError::Conflict(format!("{entity} write rejected for {}", scope.tenant))
        }
    }
}

#[cfg(test)]
mod tests {
    use mooncake_store_core::error::QuotaKind;
    use std::collections::BTreeMap;
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};
    use std::sync::{Arc, Condvar, Mutex, OnceLock};
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
        ClientStableId, ColdTierDeviceRecord, ColdTierDeviceState, ColdTierTargetSpec,
        ColdTierUsageDelta, CompatibilityDescriptor, HandoffKind, HandoffPlan, MetadataBackend,
        ObjectKey, ObjectRoute, ReplicaRoute, ReplicaTier, RouteControlMode, RoutePolicy,
        RoutePolicyDomain, RouteState, RouteVersion, SegmentAnnouncement, SegmentLifecycleState,
        SegmentName, StoreError, TenantObjectAccountingState, TenantPolicy, TenantPolicyScope,
        TenantPolicySpec, TenantQuotaFinalizeRequest, TenantQuotaPolicy,
        TenantQuotaReservationRequest, TenantQuotaReservationState,
    };
    use redis::Commands;

    use super::{
        is_legacy_redis_auth_arity_error, legacy_auth_connection_info, metadata_error,
        redacted_route_namespace_source, redis_auth_probe_timeout, redis_connect_timeout,
        redis_connection_info, redis_connection_pool_size, redis_error_diagnostic_hint,
        redis_io_timeout, redis_retry_attempts, redis_retry_delay, resolve_redis_auth,
        should_log_redis_diagnostic_occurrence, should_retry_readonly_redis_error,
        MetadataKeyspace, RedisMetadataBackend, RedisMetadataConfig,
        DEFAULT_REDIS_AUTH_PROBE_TIMEOUT, DEFAULT_REDIS_CONNECTION_POOL_SIZE,
        DEFAULT_REDIS_CONNECT_TIMEOUT, DEFAULT_REDIS_IO_TIMEOUT, DEFAULT_REDIS_RETRY_ATTEMPTS,
        DEFAULT_REDIS_RETRY_DELAY, DEFAULT_TENANT_QUOTA_TERMINAL_TTL,
        MAX_REDIS_CONNECTION_POOL_SIZE, REDIS_AUTH_PROBE_TIMEOUT_ENV,
        REDIS_CONNECTION_POOL_SIZE_ENV, REDIS_CONNECT_TIMEOUT_ENV, REDIS_IO_TIMEOUT_ENV,
        REDIS_RETRY_ATTEMPTS_ENV, REDIS_RETRY_DELAY_ENV,
    };

    struct RedisTestServer {
        child: Child,
        url: String,
        dir: PathBuf,
        _env_guard: Option<EnvLockGuard>,
    }

    impl RedisTestServer {
        fn start() -> Option<Self> {
            Self::start_with_password(None)
        }

        fn start_with_password(password: Option<&str>) -> Option<Self> {
            let env_guard = env_lock();
            let listener = TcpListener::bind("127.0.0.1:0").ok()?;
            let port = listener.local_addr().ok()?.port();
            drop(listener);
            Self::start_on_port_with_guard(port, password, Some(env_guard))
        }

        fn start_on_port(port: u16, password: Option<&str>) -> Option<Self> {
            Self::start_on_port_with_guard(port, password, None)
        }

        fn start_on_port_with_guard(
            port: u16,
            password: Option<&str>,
            env_guard: Option<EnvLockGuard>,
        ) -> Option<Self> {
            let dir = std::env::temp_dir().join(format!("mooncake-store-rs-redis-{port}"));
            let _ = std::fs::create_dir_all(&dir);
            let mut command = Command::new("redis-server");
            command
                .arg("--save")
                .arg("")
                .arg("--appendonly")
                .arg("no")
                .arg("--bind")
                .arg("127.0.0.1")
                .arg("--port")
                .arg(port.to_string())
                .arg("--dir")
                .arg(&dir)
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            if let Some(password) = password {
                command.arg("--requirepass").arg(password);
            }
            let child = command.spawn().ok()?;

            let url = match password {
                Some(password) => format!("redis://:{password}@127.0.0.1:{port}/0"),
                None => format!("redis://127.0.0.1:{port}/0"),
            };
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                if redis::Client::open(url.as_str())
                    .ok()
                    .and_then(|client| client.get_connection().ok())
                    .is_some()
                {
                    return Some(Self {
                        child,
                        url,
                        dir,
                        _env_guard: env_guard,
                    });
                }
                sleep(Duration::from_millis(25));
            }
            None
        }

        fn url(&self) -> &str {
            &self.url
        }
    }

    impl Drop for RedisTestServer {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn sample_runtime() -> ClientRuntimeId {
        ClientRuntimeId::new("writer", ClientEpoch(5))
    }

    fn scan_command_calls(connection: &mut redis::Connection) -> Option<usize> {
        let info: String = redis::cmd("INFO")
            .arg("commandstats")
            .query(connection)
            .ok()?;
        info.lines()
            .find_map(|line| {
                line.strip_prefix("cmdstat_scan:").and_then(|stats| {
                    stats.split(',').find_map(|field| {
                        field
                            .strip_prefix("calls=")
                            .and_then(|calls| calls.parse::<usize>().ok())
                    })
                })
            })
            .or(Some(0))
    }

    fn sample_lease(state: ClientLifecycleState) -> ClientLease {
        ClientLease {
            runtime: sample_runtime(),
            state,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet {
                rpc_address: "127.0.0.1:7777".to_string(),
                segment_name: Some(SegmentName::new("segment-a")),
                labels: BTreeMap::from([("pool".to_string(), "local".to_string())]),
            },
            expires_at_ms: super::now_ms() + 60_000,
        }
    }

    fn sample_segment(state: SegmentLifecycleState) -> SegmentAnnouncement {
        SegmentAnnouncement {
            owner: sample_runtime(),
            segment_name: SegmentName::new("segment-a"),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 128,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state,
            alignment_bytes: 16,
            tags: vec!["dram".to_string()],
        }
    }

    fn sample_cold_tier_device(device_id: &str) -> ColdTierDeviceRecord {
        ColdTierDeviceRecord {
            device_id: device_id.to_string(),
            stable_id: "storage-a".to_string(),
            epoch: Some(1),
            cold_tier_id: device_id.to_string(),
            kind: "ssd".to_string(),
            target: ColdTierTargetSpec::Directory {
                path: format!("/tmp/{device_id}"),
            },
            root_dir: None,
            state: ColdTierDeviceState::Healthy,
            capacity_bytes: Some(1024),
            used_bytes: 0,
            reserved_bytes: 0,
            failure_count: 0,
            last_error: None,
            tags: Vec::new(),
            updated_at_ms: 1,
        }
    }

    fn sample_cold_backing(
        device_id: &str,
        state: mooncake_store_core::ColdBackingState,
    ) -> mooncake_store_core::ColdBackingRoute {
        mooncake_store_core::ColdBackingRoute {
            owner: sample_runtime(),
            cold_tier_id: device_id.to_string(),
            object_locator: format!("{device_id}-locator"),
            length: 12,
            checksum: Some(7),
            state,
            replicas: Vec::new(),
        }
    }

    fn sample_route(version: u64) -> ObjectRoute {
        ObjectRoute {
            key: ObjectKey::new("object-a"),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(version),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: sample_runtime(),
                segment_name: SegmentName::new("segment-a"),
                offset: Some(32),
                segment_offset: 32,
                length: 12,
                checksum: Some(7),
                tier: ReplicaTier::Dram,
                priority: 1,
            }],
            cold_backing: None,
        }
    }

    struct EnvLockGuard;

    impl Drop for EnvLockGuard {
        fn drop(&mut self) {
            let (lock, ready) = ENV_LOCK.get_or_init(|| (Mutex::new(false), Condvar::new()));
            let mut held = lock.lock().expect("env lock should not be poisoned");
            *held = false;
            ready.notify_one();
        }
    }

    static ENV_LOCK: OnceLock<(Mutex<bool>, Condvar)> = OnceLock::new();

    fn env_lock() -> EnvLockGuard {
        let (lock, ready) = ENV_LOCK.get_or_init(|| (Mutex::new(false), Condvar::new()));
        let mut held = lock.lock().expect("env lock should not be poisoned");
        while *held {
            held = ready.wait(held).expect("env lock should not be poisoned");
        }
        *held = true;
        EnvLockGuard
    }

    struct EnvVarGuard(&'static str);

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            std::env::remove_var(self.0);
        }
    }

    #[test]
    fn redis_backend_round_trips_full_metadata_surface() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(MetadataKeyspace::new("test/redis")),
        )
        .expect("redis backend should initialize");

        assert!(backend.route_namespace().contains(server.url()));
        assert!(backend.route_namespace().contains("test/redis"));

        let lease = sample_lease(ClientLifecycleState::Active);
        backend
            .upsert_client_lease(&lease)
            .expect("client lease upsert should succeed");
        backend
            .update_client_state(&lease.runtime, ClientLifecycleState::Draining)
            .expect("client state update should succeed");
        let live_clients = backend
            .list_live_clients()
            .expect("listing live clients should succeed");
        assert_eq!(live_clients.len(), 1);
        assert_eq!(live_clients[0].state, ClientLifecycleState::Draining);
        assert!(matches!(
            backend.update_client_state(
                &ClientRuntimeId::new("missing", ClientEpoch(1)),
                ClientLifecycleState::Active
            ),
            Err(StoreError::NotFound(_))
        ));

        let segment = sample_segment(SegmentLifecycleState::Active);
        backend
            .publish_segment(&segment)
            .expect("segment publish should succeed");
        let listed = backend
            .list_segments(Some(&segment.owner))
            .expect("segment listing should succeed");
        assert_eq!(listed, vec![segment.clone()]);

        let reservation = backend
            .reserve_segment(&segment.owner, &segment.segment_name, 13)
            .expect("segment reservation should succeed");
        assert_eq!(reservation.offset_bytes, 0);
        backend
            .release_segment(
                &segment.owner,
                &segment.segment_name,
                reservation.offset_bytes,
                reservation.length_bytes,
            )
            .expect("segment release should succeed");

        backend
            .update_segment_state(
                &segment.owner,
                &segment.segment_name,
                SegmentLifecycleState::Draining,
            )
            .expect("segment state update should succeed");
        assert!(matches!(
            backend.reserve_segment(&segment.owner, &segment.segment_name, 8),
            Err(StoreError::InvalidState(_))
        ));
        backend
            .unpublish_segment(&segment.owner, &segment.segment_name)
            .expect("segment unpublish should succeed");
        assert!(backend
            .list_segments(Some(&segment.owner))
            .expect("segment listing after delete should succeed")
            .is_empty());
        assert!(matches!(
            backend.release_segment(&segment.owner, &segment.segment_name, 0, 1),
            Err(StoreError::NotFound(_))
        ));

        let route_key = ObjectKey::new("object-a");
        assert!(backend
            .get_object_route(&route_key)
            .expect("route get should succeed")
            .is_none());

        let route_v1 = sample_route(1);
        let created = backend
            .compare_and_swap_object_route(&route_key, None, Some(&route_v1))
            .expect("route create should succeed");
        assert!(created.applied);
        assert_eq!(created.current, Some(route_v1.clone()));

        let route_v2 = sample_route(2);
        let rejected = backend
            .compare_and_swap_object_route(&route_key, None, Some(&route_v2))
            .expect("route cas rejection should succeed");
        assert!(!rejected.applied);
        assert_eq!(rejected.current, Some(route_v1.clone()));

        let replaced = backend
            .compare_and_swap_object_route(&route_key, Some(RouteVersion(1)), Some(&route_v2))
            .expect("route cas update should succeed");
        assert!(replaced.applied);
        assert_eq!(replaced.current, Some(route_v2.clone()));
        assert_eq!(
            backend
                .get_object_route(&route_key)
                .expect("route get after update should succeed"),
            Some(route_v2.clone())
        );
        assert_eq!(
            backend
                .list_object_routes()
                .expect("route listing should succeed"),
            vec![route_v2.clone()]
        );

        let deleted = backend
            .compare_and_swap_object_route(&route_key, Some(RouteVersion(2)), None)
            .expect("route delete should succeed");
        assert!(deleted.applied);
        assert!(deleted.current.is_none());
        assert!(backend
            .list_object_routes()
            .expect("route listing after delete should succeed")
            .is_empty());

        let handoff = HandoffPlan {
            stable_id: ClientStableId::new("writer"),
            from: sample_runtime(),
            to: ClientRuntimeId::new("writer", ClientEpoch(6)),
            kind: HandoffKind::HotUpgrade,
            barrier_version: 9,
            created_at_ms: 10,
            deadline_ms: Some(20),
        };
        backend
            .put_handoff(&handoff)
            .expect("handoff put should succeed");
        assert_eq!(
            backend
                .get_handoff(&handoff.stable_id)
                .expect("handoff get should succeed"),
            Some(handoff)
        );
    }

    #[test]
    fn redis_backend_reuses_bounded_connections_under_concurrency() {
        let _guard = env_lock();
        std::env::set_var(REDIS_CONNECTION_POOL_SIZE_ENV, "2");
        let _pool_env = EnvVarGuard(REDIS_CONNECTION_POOL_SIZE_ENV);

        let listener = TcpListener::bind("127.0.0.1:0").expect("port probe should bind");
        let port = listener
            .local_addr()
            .expect("port probe should resolve")
            .port();
        drop(listener);
        let Some(server) = RedisTestServer::start_on_port(port, None) else {
            return;
        };
        let backend = Arc::new(
            RedisMetadataBackend::new(
                RedisMetadataConfig::new(server.url())
                    .keyspace(MetadataKeyspace::new("test/redis-pool")),
            )
            .expect("redis backend should initialize"),
        );

        let workers = (0..16)
            .map(|worker| {
                let backend = Arc::clone(&backend);
                std::thread::spawn(move || {
                    for iter in 0..100 {
                        let key = ObjectKey::new(format!("missing-{worker}-{iter}"));
                        assert!(backend
                            .get_object_route(&key)
                            .expect("missing route lookup should succeed")
                            .is_none());
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().expect("worker should not panic");
        }

        let mut connection = redis::Client::open(server.url())
            .expect("inspection client should open")
            .get_connection()
            .expect("inspection connection should open");
        let clients: String = redis::cmd("CLIENT")
            .arg("LIST")
            .query(&mut connection)
            .expect("client list should succeed");
        let client_count = clients.lines().count();
        assert!(
            client_count <= 3,
            "pool size 2 plus inspection connection should cap Redis clients, got {client_count}: {clients}"
        );
    }

    #[test]
    fn redis_backend_point_lookups_only_return_present_leases() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-live-point-lookups")),
        )
        .expect("redis backend should initialize");

        let active = sample_lease(ClientLifecycleState::Active);
        backend
            .upsert_client_lease(&active)
            .expect("active lease should upsert");
        assert_eq!(
            backend
                .get_client_lease(&active.runtime)
                .expect("active lease lookup should succeed"),
            Some(active.clone())
        );
        assert_eq!(
            backend
                .get_live_runtime_by_stable_id(&active.runtime.stable_id)
                .expect("active stable lookup should succeed"),
            Some(active.clone())
        );

        backend
            .update_client_state(&active.runtime, ClientLifecycleState::Draining)
            .expect("client state update should succeed");
        let draining = backend
            .get_client_lease(&active.runtime)
            .expect("draining lease lookup should succeed")
            .expect("draining lease should remain present");
        assert_eq!(draining.state, ClientLifecycleState::Draining);
        assert!(backend
            .get_live_runtime_by_stable_id(&active.runtime.stable_id)
            .expect("draining stable lookup should succeed")
            .is_none());
        let live = backend
            .list_live_clients()
            .expect("live client listing should succeed");
        assert_eq!(live, vec![draining]);
    }

    #[test]
    fn redis_backend_rejects_stale_segment_owner_index_on_publish() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-stale-segment-owner");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let owner_a = ClientRuntimeId::new("writer-a", ClientEpoch(1));
        let owner_b = ClientRuntimeId::new("writer-b", ClientEpoch(1));
        let segment_name = SegmentName::new("shared-segment");
        let segment_a = SegmentAnnouncement {
            owner: owner_a.clone(),
            segment_name: segment_name.clone(),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 128,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 16,
            tags: vec![],
        };
        backend
            .publish_segment(&segment_a)
            .expect("first owner should publish");
        backend
            .unpublish_segment(&owner_a, &segment_name)
            .expect("first owner should unpublish");

        let mut connection = redis::Client::open(server.url())
            .expect("redis client should open")
            .get_connection()
            .expect("redis connection should open");
        let owner_key = keyspace.segment_owner(&segment_name);
        connection
            .set::<_, _, ()>(&owner_key, owner_a.storage_key())
            .expect("stale owner index should write");

        let segment_b = SegmentAnnouncement {
            owner: owner_b,
            segment_name,
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 128,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 16,
            tags: vec![],
        };
        let error = backend
            .publish_segment(&segment_b)
            .expect_err("stale owner index should still block publish today");
        assert!(matches!(
            error,
            StoreError::Metadata(message)
                if message.contains("already owned by live runtime")
        ));
    }

    #[test]
    fn redis_backend_list_segments_accepts_empty_tags_reencoded_by_lua() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-empty-tags")),
        )
        .expect("redis backend should initialize");

        let mut segment = sample_segment(SegmentLifecycleState::Active);
        segment.tags.clear();
        segment.capacity_bytes = 1024;
        segment.alignment_bytes = 16;
        backend
            .publish_segment(&segment)
            .expect("segment publish should succeed");

        let reservation = backend
            .reserve_segment(&segment.owner, &segment.segment_name, 13)
            .expect("segment reservation should succeed");
        assert_eq!(reservation.offset_bytes, 0);

        let reserved_segments = backend
            .list_segments(Some(&segment.owner))
            .expect("segment listing after reserve should succeed");
        assert_eq!(reserved_segments.len(), 1);
        assert!(reserved_segments[0].tags.is_empty());
        assert_eq!(reserved_segments[0].used_bytes, 16);

        backend
            .release_segment(
                &segment.owner,
                &segment.segment_name,
                reservation.offset_bytes,
                reservation.length_bytes,
            )
            .expect("segment release should succeed");

        let released_segments = backend
            .list_segments(Some(&segment.owner))
            .expect("segment listing after release should succeed");
        assert_eq!(released_segments, vec![segment]);
    }

    #[test]
    fn redis_backend_cold_tier_usage_delta_preserves_empty_tags() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-cold-tier-empty-tags")),
        )
        .expect("redis backend should initialize");

        let device = sample_cold_tier_device("cold-a");
        backend
            .put_cold_tier_device_if_absent(&device)
            .expect("cold tier device should insert");

        let updated = backend
            .apply_cold_tier_usage_delta(
                &device.device_id,
                ColdTierUsageDelta {
                    used_bytes: 13,
                    reserved_bytes: 0,
                },
                2,
            )
            .expect("usage delta should deserialize after Lua roundtrip");
        assert!(updated.tags.is_empty());
        assert_eq!(updated.used_bytes, 13);

        let devices = backend
            .list_cold_tier_devices(&Default::default())
            .expect("device listing should tolerate Lua roundtrip tags");
        assert_eq!(devices.len(), 1);
        assert!(devices[0].tags.is_empty());
        assert_eq!(devices[0].used_bytes, 13);
    }

    #[test]
    fn redis_backend_backfills_and_prunes_client_index() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-client-index");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let lease = sample_lease(ClientLifecycleState::Active);
        let client_key = keyspace.client(&lease.runtime);
        let index_key = keyspace.client_index();
        let payload = serde_json::to_string(&lease).expect("lease should serialize");

        let client = redis::Client::open(server.url()).expect("redis client should open");
        let mut connection = client
            .get_connection()
            .expect("redis connection should succeed");

        redis::cmd("SET")
            .arg(&client_key)
            .arg(payload)
            .arg("PX")
            .arg(60_000)
            .query::<()>(&mut connection)
            .expect("legacy client lease insert should succeed");

        let listed = backend
            .list_live_clients()
            .expect("legacy client listing should succeed");
        assert_eq!(listed, vec![lease.clone()]);

        let indexed: Vec<String> = connection
            .smembers(&index_key)
            .expect("client index read should succeed");
        assert_eq!(indexed, vec![client_key.clone()]);

        connection
            .del::<_, ()>(&client_key)
            .expect("client lease delete should succeed");

        let listed = backend
            .list_live_clients()
            .expect("stale client listing should succeed");
        assert!(listed.is_empty());

        let indexed: Vec<String> = connection
            .smembers(&index_key)
            .expect("client index read after prune should succeed");
        assert!(indexed.is_empty());
    }

    #[test]
    fn redis_backend_upsert_client_lease_rejects_stale_epoch() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-client-epoch-cas");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let make_lease = |epoch: u64| ClientLease {
            runtime: ClientRuntimeId::new("client-cas", ClientEpoch(epoch)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            expires_at_ms: super::now_ms() + 60_000,
        };

        backend
            .upsert_client_lease(&make_lease(5))
            .expect("first lease should publish");

        let rejection = backend
            .upsert_client_lease(&make_lease(3))
            .expect_err("older epoch must be rejected");
        assert!(matches!(rejection, StoreError::StaleEpoch(_)));

        backend
            .upsert_client_lease(&make_lease(5))
            .expect("same (stable_id, epoch) is a refresh");

        backend
            .upsert_client_lease(&make_lease(6))
            .expect("strictly greater epoch must succeed");

        let client = redis::Client::open(server.url()).expect("redis client should open");
        let mut connection = client
            .get_connection()
            .expect("redis connection should succeed");
        let hwm: Option<String> = connection
            .get(keyspace.client_epoch_hwm(&ClientStableId::new("client-cas")))
            .expect("hwm read should succeed");
        assert_eq!(hwm.as_deref(), Some("6"));
        let members: Vec<String> = connection
            .smembers(keyspace.client_by_stable_index(&ClientStableId::new("client-cas")))
            .expect("by-stable index read should succeed");
        let mut sorted = members.clone();
        sorted.sort();
        assert_eq!(sorted, vec!["5".to_string(), "6".to_string()]);
    }

    #[test]
    fn redis_backend_allocate_client_lease_assigns_monotonic_epochs() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-client-allocate");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let template = |stable_id: &str| ClientLease {
            runtime: ClientRuntimeId::new(stable_id, ClientEpoch(0)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            expires_at_ms: super::now_ms() + 60_000,
        };

        let first = backend
            .allocate_client_lease(&template("client-alloc"))
            .expect("first allocation should succeed");
        let second = backend
            .allocate_client_lease(&template("client-alloc"))
            .expect("second allocation should succeed");
        let isolated = backend
            .allocate_client_lease(&template("client-other"))
            .expect("independent stable_id gets its own counter");
        assert_eq!(first.epoch, ClientEpoch(1));
        assert_eq!(second.epoch, ClientEpoch(2));
        assert_eq!(isolated.epoch, ClientEpoch(1));

        let client = redis::Client::open(server.url()).expect("redis client should open");
        let mut connection = client
            .get_connection()
            .expect("redis connection should succeed");
        let hwm: Option<String> = connection
            .get(keyspace.client_epoch_hwm(&ClientStableId::new("client-alloc")))
            .expect("hwm read should succeed");
        assert_eq!(hwm.as_deref(), Some("2"));

        let listed = backend
            .list_live_clients()
            .expect("list live clients should succeed");
        assert_eq!(listed.len(), 3);
        let stored_epochs: Vec<ClientEpoch> = listed
            .iter()
            .filter(|lease| lease.runtime.stable_id.0 == "client-alloc")
            .map(|lease| lease.runtime.epoch)
            .collect();
        let mut sorted = stored_epochs.clone();
        sorted.sort();
        assert_eq!(sorted, vec![ClientEpoch(1), ClientEpoch(2)]);
    }

    #[test]
    fn redis_backend_upsert_client_lease_cleans_by_stable_index_on_expiry() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-client-by-stable-prune");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let lease = ClientLease {
            runtime: ClientRuntimeId::new("prune-me", ClientEpoch(2)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            expires_at_ms: super::now_ms() + 60_000,
        };
        backend
            .upsert_client_lease(&lease)
            .expect("lease should publish");

        let client = redis::Client::open(server.url()).expect("redis client should open");
        let mut connection = client
            .get_connection()
            .expect("redis connection should succeed");
        connection
            .del::<_, ()>(keyspace.client(&lease.runtime))
            .expect("simulate lease expiry by deleting key");

        let listed = backend
            .list_live_clients()
            .expect("list should succeed with stale key");
        assert!(listed.is_empty());

        let members: Vec<String> = connection
            .smembers(keyspace.client_by_stable_index(&ClientStableId::new("prune-me")))
            .expect("by-stable index read should succeed");
        assert!(
            members.is_empty(),
            "by-stable index should prune expired epoch"
        );
        let hwm: Option<String> = connection
            .get(keyspace.client_epoch_hwm(&ClientStableId::new("prune-me")))
            .expect("hwm read should succeed");
        assert_eq!(hwm.as_deref(), Some("2"), "HWM must not regress on expiry");
        let expiry_members: Vec<String> = connection
            .zrange(keyspace.client_lease_expiry_index(), 0, -1)
            .expect("expiry index read should succeed");
        assert!(
            expiry_members.is_empty(),
            "expiry index should prune expired lease"
        );
    }

    #[test]
    fn redis_backend_upsert_client_lease_reclaims_expired_same_epoch() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-client-reclaim-same-epoch");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let lease = ClientLease {
            runtime: ClientRuntimeId::new("reclaim-me", ClientEpoch(4)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            expires_at_ms: super::now_ms() + 60_000,
        };
        backend
            .upsert_client_lease(&lease)
            .expect("initial lease publish should succeed");

        let client = redis::Client::open(server.url()).expect("redis client should open");
        let mut connection = client
            .get_connection()
            .expect("redis connection should succeed");
        connection
            .del::<_, ()>(keyspace.client(&lease.runtime))
            .expect("simulate lease expiry by deleting key");

        backend
            .upsert_client_lease(&lease)
            .expect("same epoch reclaim should succeed");

        let hwm: Option<String> = connection
            .get(keyspace.client_epoch_hwm(&ClientStableId::new("reclaim-me")))
            .expect("hwm read should succeed");
        assert_eq!(hwm.as_deref(), Some("4"));
        let expiry_members: Vec<String> = connection
            .zrange(keyspace.client_lease_expiry_index(), 0, -1)
            .expect("expiry index read should succeed");
        assert_eq!(expiry_members, vec![keyspace.client(&lease.runtime)]);
    }

    #[test]
    fn redis_backend_indexes_cold_backing_routes() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-cold-route-index");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let mut first = sample_route(1);
        first.key = ObjectKey::new("cold-index-a");
        first.cold_backing = Some(sample_cold_backing(
            "device-a",
            mooncake_store_core::ColdBackingState::Materialized,
        ));
        let mut second = sample_route(1);
        second.key = ObjectKey::new("cold-index-b");
        second.cold_backing = Some(sample_cold_backing(
            "device-b",
            mooncake_store_core::ColdBackingState::PendingDelete,
        ));
        backend
            .compare_and_swap_object_route(&first.key, None, Some(&first))
            .expect("first route create should succeed");
        backend
            .compare_and_swap_object_route(&second.key, None, Some(&second))
            .expect("second route create should succeed");

        let by_device = backend
            .list_object_routes_by_cold_backing(&mooncake_store_core::ColdBackingRouteFilter {
                device_id: Some("device-a".to_string()),
                ..mooncake_store_core::ColdBackingRouteFilter::default()
            })
            .expect("device index listing should succeed");
        assert_eq!(by_device, vec![first.clone()]);
        let by_state = backend
            .list_object_routes_by_cold_backing(&mooncake_store_core::ColdBackingRouteFilter {
                state: Some(mooncake_store_core::ColdBackingState::PendingDelete),
                ..mooncake_store_core::ColdBackingRouteFilter::default()
            })
            .expect("state index listing should succeed");
        assert_eq!(by_state, vec![second.clone()]);

        let mut updated = first.clone();
        updated.version = updated.version.next();
        updated.cold_backing = Some(sample_cold_backing(
            "device-b",
            mooncake_store_core::ColdBackingState::PendingDelete,
        ));
        backend
            .compare_and_swap_object_route(&first.key, Some(first.version), Some(&updated))
            .expect("route update should succeed");
        let old_device = backend
            .list_object_routes_by_cold_backing(&mooncake_store_core::ColdBackingRouteFilter {
                device_id: Some("device-a".to_string()),
                ..mooncake_store_core::ColdBackingRouteFilter::default()
            })
            .expect("old device listing should succeed");
        assert!(old_device.is_empty());
        let pending_delete = backend
            .list_object_routes_by_cold_backing(&mooncake_store_core::ColdBackingRouteFilter {
                state: Some(mooncake_store_core::ColdBackingState::PendingDelete),
                limit: Some(1),
                ..mooncake_store_core::ColdBackingRouteFilter::default()
            })
            .expect("limited state listing should succeed");
        assert_eq!(pending_delete.len(), 1);
    }

    #[test]
    fn redis_backend_backfills_and_prunes_object_index() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-object-index");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let route = sample_route(3);
        backend
            .compare_and_swap_object_route(&route.key, None, Some(&route))
            .expect("route create should succeed");

        let client = redis::Client::open(server.url()).expect("redis client should open");
        let mut connection = client
            .get_connection()
            .expect("redis connection should succeed");
        let indexed_after_create: Vec<String> = connection
            .smembers(keyspace.object_index())
            .expect("object index read after create should succeed");
        assert_eq!(indexed_after_create, vec![keyspace.object(&route.key)]);
        connection
            .del::<_, ()>(keyspace.object_index())
            .expect("object index delete should succeed");

        let listed = backend
            .list_object_routes()
            .expect("legacy object listing should succeed");
        assert_eq!(listed, vec![route.clone()]);

        let indexed: Vec<String> = connection
            .smembers(keyspace.object_index())
            .expect("object index read should succeed");
        assert_eq!(indexed, vec![keyspace.object(&route.key)]);

        backend
            .compare_and_swap_object_route(&route.key, Some(route.version), None)
            .expect("route delete should succeed");
        let indexed_after_delete: Vec<String> = connection
            .smembers(keyspace.object_index())
            .expect("object index read after CAS delete should succeed");
        assert!(indexed_after_delete.is_empty());

        connection
            .sadd::<_, _, ()>(keyspace.object_index(), keyspace.object(&route.key))
            .expect("stale object index seed should succeed");

        let listed = backend
            .list_object_routes()
            .expect("stale object listing should succeed");
        assert!(listed.is_empty());

        let indexed: Vec<String> = connection
            .smembers(keyspace.object_index())
            .expect("object index read after prune should succeed");
        assert!(indexed.is_empty());
    }

    #[test]
    fn redis_backend_route_policy_supports_overwrite_list_and_delete() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-route-policy");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");
        let creator = ClientRuntimeId::new("route-owner", ClientEpoch(7));
        let default_policy = RoutePolicy {
            route_topk: 2,
            route_control: RouteControlMode::EmbeddedWrh,
            created_by: creator.clone(),
            created_at_ms: 11,
        };
        let tenant_domain = RoutePolicyDomain::Tenant("tenant/a".to_string());
        let tenant_policy = RoutePolicy {
            route_topk: 4,
            route_control: RouteControlMode::MetadataOnly,
            created_by: creator,
            created_at_ms: 22,
        };

        assert!(backend
            .put_route_policy_if_absent(&RoutePolicyDomain::Default, &default_policy)
            .expect("default route policy bootstrap should succeed"));
        backend
            .put_route_policy(&tenant_domain, &tenant_policy)
            .expect("tenant route policy overwrite should succeed");
        let listed = backend
            .list_route_policies()
            .expect("route policy listing should succeed");
        assert_eq!(listed.len(), 2);
        assert!(listed.iter().any(|(domain, policy)| {
            *domain == RoutePolicyDomain::Default && *policy == default_policy
        }));
        assert!(listed
            .iter()
            .any(|(domain, policy)| *domain == tenant_domain && *policy == tenant_policy));
        assert!(backend
            .delete_route_policy(&tenant_domain)
            .expect("tenant route policy delete should succeed"));
        assert_eq!(
            backend
                .get_route_policy(&tenant_domain)
                .expect("tenant route policy read should succeed"),
            None,
        );
    }

    #[test]
    fn redis_backend_idempotent_write_retries_until_reconnected() {
        let _guard = env_lock();
        if Command::new("redis-server")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }

        std::env::set_var(REDIS_RETRY_ATTEMPTS_ENV, "20");
        std::env::set_var(REDIS_RETRY_DELAY_ENV, "50");

        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            return;
        };
        let port = listener
            .local_addr()
            .expect("free redis port should have addr")
            .port();
        drop(listener);
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(format!("redis://127.0.0.1:{port}/0"))
                .keyspace(MetadataKeyspace::new("test/redis-retry")),
        )
        .expect("redis backend should initialize before server is reachable");

        let server = std::thread::spawn(move || {
            sleep(Duration::from_millis(150));
            RedisTestServer::start_on_port(port, None).expect("delayed redis server should start")
        });

        let lease = sample_lease(ClientLifecycleState::Active);
        backend
            .upsert_client_lease(&lease)
            .expect("lease upsert should reconnect and retry");
        let _server = server.join().expect("redis server thread should join");
        assert_eq!(
            backend
                .list_live_clients()
                .expect("live clients should list after reconnect"),
            vec![lease]
        );

        std::env::remove_var(REDIS_RETRY_ATTEMPTS_ENV);
        std::env::remove_var(REDIS_RETRY_DELAY_ENV);
    }

    #[test]
    fn readonly_metadata_retry_classifier_accepts_transient_io() {
        let interrupted =
            redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::Interrupted));
        let timed_out = redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::TimedOut));
        let dropped = redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::BrokenPipe));
        let refused =
            redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));

        assert!(should_retry_readonly_redis_error(&interrupted));
        assert!(should_retry_readonly_redis_error(&timed_out));
        assert!(should_retry_readonly_redis_error(&dropped));
        assert!(should_retry_readonly_redis_error(&refused));
    }

    #[test]
    fn redis_backend_cleanup_stale_segments_removes_dead_owner_metadata() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-cleanup");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let live = sample_lease(ClientLifecycleState::Active);
        backend
            .upsert_client_lease(&live)
            .expect("live lease should upsert");

        let live_segment = sample_segment(SegmentLifecycleState::Active);
        backend
            .publish_segment(&live_segment)
            .expect("live segment publish should succeed");

        let dead_runtime = ClientRuntimeId::new("dead", ClientEpoch(8));
        let dead_segment = SegmentAnnouncement {
            owner: dead_runtime.clone(),
            segment_name: SegmentName::new("dead-segment"),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 256,
            used_bytes: 64,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 16,
            tags: vec!["dram".to_string()],
        };
        backend
            .publish_segment(&dead_segment)
            .expect("dead segment publish should succeed");

        let report = backend
            .cleanup_stale_segments()
            .expect("stale segment cleanup should succeed");
        assert_eq!(report.live_clients, 1);
        assert!(report.inspected_segment_keys >= 2);
        assert_eq!(report.removed_segment_keys, 1);

        let segments = backend
            .list_segments(None)
            .expect("segment listing after cleanup should succeed");
        assert_eq!(segments, vec![live_segment.clone()]);

        let client = redis::Client::open(server.url()).expect("redis client should open");
        let mut connection = client
            .get_connection()
            .expect("redis connection should succeed");
        let global_index: Vec<String> = connection
            .smembers(keyspace.segment_index(None))
            .expect("global segment index should load");
        assert_eq!(
            global_index,
            vec![keyspace.segment(&live_segment.owner, &live_segment.segment_name)]
        );
        let dead_owner_index: Vec<String> = connection
            .smembers(keyspace.segment_index(Some(&dead_runtime)))
            .expect("dead owner index should load");
        assert!(dead_owner_index.is_empty());
        let cleanup_markers: Vec<String> = redis::cmd("KEYS")
            .arg(format!(
                "{{{}}}/maintenance/segment-cleanup/*",
                keyspace.prefix()
            ))
            .query(&mut connection)
            .expect("cleanup markers should scan");
        assert!(cleanup_markers.is_empty());
    }

    #[test]
    fn redis_backend_cleanup_stale_segments_for_owner_is_scoped() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-cleanup-owner-scoped");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let dead_runtime = ClientRuntimeId::new("dead-owner", ClientEpoch(2));
        let other_runtime = ClientRuntimeId::new("other-owner", ClientEpoch(3));
        let dead_segment = SegmentAnnouncement {
            owner: dead_runtime.clone(),
            segment_name: SegmentName::new("dead-segment"),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 128,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 16,
            tags: vec!["dram".to_string()],
        };
        let other_segment = SegmentAnnouncement {
            owner: other_runtime.clone(),
            segment_name: SegmentName::new("other-segment"),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 128,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 16,
            tags: vec!["dram".to_string()],
        };
        backend
            .publish_segment(&dead_segment)
            .expect("dead segment publish should succeed");
        backend
            .publish_segment(&other_segment)
            .expect("other segment publish should succeed");

        let report = backend
            .cleanup_stale_segments_for_owner(&dead_runtime)
            .expect("owner cleanup should succeed");
        assert_eq!(report.removed_segment_keys, 1);

        let remaining = backend
            .list_segments(None)
            .expect("segment listing after owner cleanup should succeed");
        assert_eq!(remaining, vec![other_segment.clone()]);

        let client = redis::Client::open(server.url()).expect("redis client should open");
        let mut connection = client
            .get_connection()
            .expect("redis connection should succeed");
        let dead_index: Vec<String> = connection
            .smembers(keyspace.segment_index(Some(&dead_runtime)))
            .expect("dead owner index should load");
        assert!(dead_index.is_empty());
        let other_index: Vec<String> = connection
            .smembers(keyspace.segment_index(Some(&other_runtime)))
            .expect("other owner index should load");
        assert_eq!(
            other_index,
            vec![keyspace.segment(&other_runtime, &other_segment.segment_name)]
        );
    }

    #[test]
    fn redis_backend_tenant_policy_supports_versioned_put_list_and_delete() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-tenant-policy")),
        )
        .expect("redis backend should initialize");
        let scope = TenantPolicyScope::new("tenant/a", Some("domain-1"), None::<String>);
        let policy = TenantPolicy {
            scope: scope.clone(),
            spec: TenantPolicySpec {
                quota: Some(TenantQuotaPolicy {
                    max_bytes: Some(128),
                    max_objects: Some(8),
                }),
                ..TenantPolicySpec::default()
            },
            version: 1,
            updated_at_ms: 10,
            updated_by: "admin".to_string(),
        };

        let stored = backend
            .put_tenant_policy(&policy, None)
            .expect("tenant policy insert should succeed");
        assert_eq!(stored, policy);
        assert_eq!(
            backend
                .get_tenant_policy(&scope)
                .expect("tenant policy read should succeed"),
            Some(policy.clone())
        );
        let missing_scope =
            TenantPolicyScope::new("tenant-missing", None::<String>, None::<String>);
        assert_eq!(
            backend
                .get_tenant_policies(&[scope.clone(), missing_scope.clone()])
                .expect("tenant policy batch read should succeed"),
            vec![Some(policy.clone()), None]
        );

        let mut updated = policy.clone();
        updated.version = 2;
        updated.updated_at_ms = 20;
        updated.updated_by = "admin-2".to_string();
        updated.spec.quota.as_mut().unwrap().max_objects = Some(16);
        let error = backend
            .put_tenant_policy(&updated, Some(3))
            .expect_err("mismatched version should fail");
        assert!(matches!(error, StoreError::Conflict(_)));

        backend
            .put_tenant_policy(&updated, Some(1))
            .expect("tenant policy update should succeed");
        let other_scope = TenantPolicyScope::new("tenant-b", None::<String>, None::<String>);
        let other_policy = TenantPolicy {
            scope: other_scope,
            spec: TenantPolicySpec::default(),
            version: 1,
            updated_at_ms: 30,
            updated_by: "admin".to_string(),
        };
        backend
            .put_tenant_policy(&other_policy, None)
            .expect("other tenant policy insert should succeed");
        assert_eq!(
            backend
                .list_tenant_policies(Some("tenant/a"))
                .expect("tenant-scoped policy listing should succeed"),
            vec![updated.clone()]
        );
        assert_eq!(
            backend
                .list_tenant_policies(Some("tenant-b"))
                .expect("other tenant-scoped policy listing should succeed"),
            vec![other_policy.clone()]
        );
        assert!(backend
            .list_tenant_policies(Some("missing"))
            .expect("missing tenant policy listing should succeed")
            .is_empty());
        assert_eq!(
            backend
                .list_tenant_policies(None)
                .expect("tenant policy listing should succeed"),
            vec![other_policy, updated.clone()]
        );
        let mut connection = redis::Client::open(server.url())
            .expect("redis client should open")
            .get_connection()
            .expect("redis connection should succeed");
        let tenant_index: Vec<String> = connection
            .smembers(backend.keyspace.tenant_policy_index("tenant/a"))
            .expect("tenant policy index should load");
        assert_eq!(tenant_index, vec![backend.keyspace.tenant_policy(&scope)]);

        assert!(backend
            .delete_tenant_policy(&scope, Some(2))
            .expect("tenant policy delete should succeed"));
        assert_eq!(
            backend
                .get_tenant_policy(&scope)
                .expect("tenant policy read after delete should succeed"),
            None
        );
        let tenant_index: Vec<String> = connection
            .smembers(backend.keyspace.tenant_policy_index("tenant/a"))
            .expect("tenant policy index should load after delete");
        assert!(tenant_index.is_empty());
    }

    #[test]
    fn redis_backend_backfills_legacy_tenant_policy_indexes_on_first_scoped_list() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-tenant-policy-index-backfill");
        let scope = TenantPolicyScope::new("tenant-a", None::<String>, None::<String>);
        let policy = TenantPolicy {
            scope: scope.clone(),
            spec: TenantPolicySpec::default(),
            version: 1,
            updated_at_ms: 10,
            updated_by: "legacy-admin".to_string(),
        };
        let mut connection = redis::Client::open(server.url())
            .expect("redis client should open")
            .get_connection()
            .expect("redis connection should succeed");
        connection
            .set::<_, _, ()>(
                keyspace.tenant_policy(&scope),
                serde_json::to_string(&policy).expect("policy should serialize"),
            )
            .expect("legacy tenant policy should be written");
        assert!(connection
            .smembers::<_, Vec<String>>(keyspace.tenant_policy_index("tenant-a"))
            .expect("tenant policy index should load before backfill")
            .is_empty());
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        assert_eq!(
            backend
                .list_tenant_policies(Some("tenant-a"))
                .expect("tenant-scoped policy listing should succeed"),
            vec![policy]
        );
        assert_eq!(
            connection
                .smembers::<_, Vec<String>>(keyspace.tenant_policy_index("tenant-a"))
                .expect("tenant policy index should load after backfill"),
            vec![keyspace.tenant_policy(&scope)]
        );
        assert!(connection
            .exists::<_, bool>(keyspace.tenant_policy_index_ready("tenant-a"))
            .expect("tenant policy index ready marker should exist"));
    }

    #[test]
    fn redis_backend_backfills_legacy_tenant_policy_indexes_even_after_new_write() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-tenant-policy-index-mixed-backfill");
        let legacy_scope = TenantPolicyScope::new("tenant-a", None::<String>, None::<String>);
        let legacy_policy = TenantPolicy {
            scope: legacy_scope.clone(),
            spec: TenantPolicySpec::default(),
            version: 1,
            updated_at_ms: 10,
            updated_by: "legacy-admin".to_string(),
        };
        let mut connection = redis::Client::open(server.url())
            .expect("redis client should open")
            .get_connection()
            .expect("redis connection should succeed");
        connection
            .set::<_, _, ()>(
                keyspace.tenant_policy(&legacy_scope),
                serde_json::to_string(&legacy_policy).expect("policy should serialize"),
            )
            .expect("legacy tenant policy should be written");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");
        let new_scope = TenantPolicyScope::new("tenant-a", Some("domain-1"), None::<String>);
        let new_policy = TenantPolicy {
            scope: new_scope,
            spec: TenantPolicySpec::default(),
            version: 1,
            updated_at_ms: 11,
            updated_by: "admin".to_string(),
        };
        backend
            .put_tenant_policy(&new_policy, None)
            .expect("new tenant policy insert should succeed");

        assert_eq!(
            backend
                .list_tenant_policies(Some("tenant-a"))
                .expect("tenant-scoped policy listing should succeed"),
            vec![legacy_policy, new_policy]
        );
    }

    #[test]
    fn redis_backend_tenant_scoped_policy_listing_does_not_scan_keyspace() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-tenant-policy-list-index")),
        )
        .expect("redis backend should initialize");
        let target = TenantPolicyScope::new("tenant-a", None::<String>, None::<String>);
        let target_policy = TenantPolicy {
            scope: target.clone(),
            spec: TenantPolicySpec::default(),
            version: 1,
            updated_at_ms: 10,
            updated_by: "admin".to_string(),
        };
        backend
            .put_tenant_policy(&target_policy, None)
            .expect("target tenant policy insert should succeed");
        for index in 0..32 {
            let policy = TenantPolicy {
                scope: TenantPolicyScope::new(
                    format!("tenant-{index}"),
                    None::<String>,
                    None::<String>,
                ),
                spec: TenantPolicySpec::default(),
                version: 1,
                updated_at_ms: 20 + index,
                updated_by: "admin".to_string(),
            };
            backend
                .put_tenant_policy(&policy, None)
                .expect("other tenant policy insert should succeed");
        }

        let mut connection = redis::Client::open(server.url())
            .expect("redis client should open")
            .get_connection()
            .expect("redis connection should succeed");
        redis::cmd("CONFIG")
            .arg("RESETSTAT")
            .query::<()>(&mut connection)
            .expect("redis stats should reset");

        assert_eq!(
            backend
                .list_tenant_policies(Some("tenant-a"))
                .expect("tenant-scoped policy listing should succeed"),
            vec![target_policy.clone()]
        );
        if let Some(scan_calls) = scan_command_calls(&mut connection) {
            assert!(
                scan_calls > 0,
                "first tenant-scoped policy listing may scan once to mark the index ready"
            );
        }

        redis::cmd("CONFIG")
            .arg("RESETSTAT")
            .query::<()>(&mut connection)
            .expect("redis stats should reset");
        assert_eq!(
            backend
                .list_tenant_policies(Some("tenant-a"))
                .expect("tenant-scoped policy listing should succeed"),
            vec![target_policy]
        );
        if let Some(scan_calls) = scan_command_calls(&mut connection) {
            assert_eq!(
                scan_calls, 0,
                "tenant-scoped policy listing must use the ready per-tenant index instead of SCAN MATCH"
            );
        }

        backend
            .list_tenant_policies(None)
            .expect("all-tenant policy listing should succeed");
        if let Some(scan_calls) = scan_command_calls(&mut connection) {
            assert!(
                scan_calls > 0,
                "all-tenant policy listing still uses SCAN for full keyspace enumeration"
            );
        }
    }

    #[test]
    fn redis_backend_tenant_scoped_policy_listing_prunes_bad_index_entries() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(RedisMetadataConfig::new(server.url()).keyspace(
            MetadataKeyspace::new("test/redis-tenant-policy-index-prune"),
        ))
        .expect("redis backend should initialize");
        let target_scope = TenantPolicyScope::new("tenant-a", None::<String>, None::<String>);
        let target_policy = TenantPolicy {
            scope: target_scope.clone(),
            spec: TenantPolicySpec::default(),
            version: 1,
            updated_at_ms: 10,
            updated_by: "admin".to_string(),
        };
        let other_scope = TenantPolicyScope::new("tenant-b", None::<String>, None::<String>);
        let other_policy = TenantPolicy {
            scope: other_scope.clone(),
            spec: TenantPolicySpec::default(),
            version: 1,
            updated_at_ms: 11,
            updated_by: "admin".to_string(),
        };
        backend
            .put_tenant_policy(&target_policy, None)
            .expect("target tenant policy insert should succeed");
        backend
            .put_tenant_policy(&other_policy, None)
            .expect("other tenant policy insert should succeed");

        let mut connection = redis::Client::open(server.url())
            .expect("redis client should open")
            .get_connection()
            .expect("redis connection should succeed");
        let tenant_index = backend.keyspace.tenant_policy_index("tenant-a");
        let other_key = backend.keyspace.tenant_policy(&other_scope);
        let missing_key = backend.keyspace.tenant_policy(&TenantPolicyScope::new(
            "tenant-a",
            Some("missing"),
            None::<String>,
        ));
        connection
            .sadd::<_, _, ()>(&tenant_index, &other_key)
            .expect("wrong-tenant index entry should be added");
        connection
            .sadd::<_, _, ()>(&tenant_index, &missing_key)
            .expect("stale index entry should be added");
        connection
            .sadd::<_, _, ()>(&tenant_index, "not-a-tenant-policy-key")
            .expect("malformed index entry should be added");
        connection
            .set::<_, _, ()>(backend.keyspace.tenant_policy_index_ready("tenant-a"), "1")
            .expect("tenant policy index should be marked ready");

        assert_eq!(
            backend
                .list_tenant_policies(Some("tenant-a"))
                .expect("tenant-scoped policy listing should succeed"),
            vec![target_policy]
        );
        let mut index_members: Vec<String> = connection
            .smembers(&tenant_index)
            .expect("tenant policy index should load");
        index_members.sort();
        assert_eq!(
            index_members,
            vec![backend.keyspace.tenant_policy(&target_scope)]
        );
    }

    #[test]
    fn redis_backend_tenant_quota_reservation_finalize_and_abort_follow_versioned_state_machine() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let ttl = Duration::from_secs(60);
        let keyspace = MetadataKeyspace::new("test/redis-tenant-quota");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(keyspace.clone())
                .tenant_quota_terminal_ttl(ttl),
        )
        .expect("redis backend should initialize");
        let scope = TenantPolicyScope::new("tenant-a", None::<String>, None::<String>);
        let key = ObjectKey::new("tenant-a::alpha");
        let writer = ClientRuntimeId::new("writer", ClientEpoch(1));

        let reserved = backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-create".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: None,
                delta_bytes: 32,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(2),
                },
                expires_at_ms: 200,
                created_at_ms: 100,
                writer_runtime: writer.clone(),
            })
            .expect("reservation should succeed");
        assert_eq!(reserved.quota.pending_reserved_bytes, 32);
        assert_eq!(reserved.quota.pending_reserved_objects, 1);
        assert!(reserved.object.is_none());
        assert_eq!(
            reserved.reservation.state,
            TenantQuotaReservationState::Pending
        );

        let finalized = backend
            .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                reservation_id: "resv-create".to_string(),
                expected_object_version: None,
                committed_length: Some(32),
                route_version: None,
                state: TenantObjectAccountingState::Active,
                updated_at_ms: 120,
                updated_by: "writer".to_string(),
            })
            .expect("finalize should succeed");
        assert_eq!(finalized.quota.used_bytes, 32);
        assert_eq!(finalized.quota.used_objects, 1);
        assert_eq!(finalized.quota.pending_reserved_bytes, 0);
        assert_eq!(finalized.quota.pending_reserved_objects, 0);
        assert_eq!(
            finalized
                .object
                .as_ref()
                .expect("object accounting should exist")
                .committed_length,
            32
        );
        assert_eq!(
            finalized.reservation.state,
            TenantQuotaReservationState::Finalized
        );
        let mut connection = backend
            .connection("redis test inspect finalized reservation ttl")
            .expect("redis connection should succeed");
        let finalized_reservation_key = keyspace.tenant_quota_reservation("resv-create");
        let finalized_index_key = keyspace.tenant_quota_reservation_index(&scope, "resv-create");
        let finalized_reservation_ttl: i64 = redis::cmd("PTTL")
            .arg(finalized_reservation_key.as_str())
            .query(&mut connection)
            .expect("finalized reservation ttl should be readable");
        let finalized_index_ttl: i64 = redis::cmd("PTTL")
            .arg(finalized_index_key.as_str())
            .query(&mut connection)
            .expect("finalized reservation index ttl should be readable");
        assert!(
            finalized_reservation_ttl > 0 && finalized_reservation_ttl <= ttl.as_millis() as i64,
            "finalized reservation ttl should be set: {finalized_reservation_ttl}"
        );
        assert!(
            finalized_index_ttl > 0 && finalized_index_ttl <= ttl.as_millis() as i64,
            "finalized reservation index ttl should be set: {finalized_index_ttl}"
        );

        let duplicate_finalize = backend
            .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                reservation_id: "resv-create".to_string(),
                expected_object_version: Some(1),
                committed_length: Some(32),
                route_version: None,
                state: TenantObjectAccountingState::Active,
                updated_at_ms: 121,
                updated_by: "writer".to_string(),
            })
            .expect("duplicate finalize should be idempotent");
        assert_eq!(duplicate_finalize.quota.used_bytes, 32);
        assert_eq!(duplicate_finalize.reservation.version, 2);

        let overwrite = backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-overwrite".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: Some(1),
                delta_bytes: 8,
                delta_objects: 0,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(2),
                },
                expires_at_ms: 260,
                created_at_ms: 140,
                writer_runtime: writer.clone(),
            })
            .expect("overwrite reservation should succeed");
        assert_eq!(
            overwrite
                .object
                .expect("object accounting should exist")
                .version,
            1
        );
        assert_eq!(overwrite.quota.pending_reserved_bytes, 8);

        let conflict = backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-conflict".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: Some(99),
                delta_bytes: 1,
                delta_objects: 0,
                limit: TenantQuotaPolicy::default(),
                expires_at_ms: 300,
                created_at_ms: 150,
                writer_runtime: writer.clone(),
            })
            .expect_err("stale object version should fail");
        assert!(matches!(conflict, StoreError::Conflict(_)));

        let aborted = backend
            .abort_tenant_quota("resv-overwrite")
            .expect("abort should succeed");
        assert_eq!(aborted.quota.pending_reserved_bytes, 0);
        assert_eq!(aborted.quota.used_bytes, 32);
        assert_eq!(
            aborted.reservation.state,
            TenantQuotaReservationState::Aborted
        );
        let aborted_reservation_key = keyspace.tenant_quota_reservation("resv-overwrite");
        let aborted_index_key = keyspace.tenant_quota_reservation_index(&scope, "resv-overwrite");
        let aborted_reservation_ttl: i64 = redis::cmd("PTTL")
            .arg(aborted_reservation_key.as_str())
            .query(&mut connection)
            .expect("aborted reservation ttl should be readable");
        let aborted_index_ttl: i64 = redis::cmd("PTTL")
            .arg(aborted_index_key.as_str())
            .query(&mut connection)
            .expect("aborted reservation index ttl should be readable");
        assert!(
            aborted_reservation_ttl > 0 && aborted_reservation_ttl <= ttl.as_millis() as i64,
            "aborted reservation ttl should be set: {aborted_reservation_ttl}"
        );
        assert!(
            aborted_index_ttl > 0 && aborted_index_ttl <= ttl.as_millis() as i64,
            "aborted reservation index ttl should be set: {aborted_index_ttl}"
        );

        let duplicate_abort = backend
            .abort_tenant_quota("resv-overwrite")
            .expect("duplicate abort should be idempotent");
        assert_eq!(duplicate_abort.quota.pending_reserved_bytes, 0);
        assert_eq!(duplicate_abort.reservation.version, 2);

        let delete_reserved = backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-delete".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: Some(1),
                delta_bytes: -32,
                delta_objects: -1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(2),
                },
                expires_at_ms: 400,
                created_at_ms: 200,
                writer_runtime: writer.clone(),
            })
            .expect("delete reservation should succeed");
        assert_eq!(delete_reserved.quota.pending_reserved_bytes, 0);
        assert_eq!(delete_reserved.quota.used_bytes, 32);

        let deleted = backend
            .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                reservation_id: "resv-delete".to_string(),
                expected_object_version: Some(1),
                committed_length: None,
                route_version: None,
                state: TenantObjectAccountingState::Deleted,
                updated_at_ms: 220,
                updated_by: "writer".to_string(),
            })
            .expect("delete finalize should succeed");
        assert_eq!(deleted.quota.used_bytes, 0);
        assert_eq!(deleted.quota.used_objects, 0);
        assert!(deleted.object.is_none());
        assert!(backend
            .get_tenant_object_accounting(&key)
            .expect("object accounting lookup should succeed")
            .is_none());
    }

    #[test]
    fn redis_backend_uses_default_terminal_ttl_for_tenant_quota_reservations() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-tenant-quota-default-ttl")),
        )
        .expect("redis backend should initialize");
        assert_eq!(
            backend.tenant_quota_terminal_ttl,
            DEFAULT_TENANT_QUOTA_TERMINAL_TTL
        );
    }

    #[test]
    fn redis_backend_terminal_reservations_expire_after_short_ttl() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let ttl = Duration::from_millis(75);
        let keyspace = MetadataKeyspace::new("test/redis-tenant-quota-expire");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(keyspace.clone())
                .tenant_quota_terminal_ttl(ttl),
        )
        .expect("redis backend should initialize");
        let scope = TenantPolicyScope::new("tenant-expire", None::<String>, None::<String>);
        let key = ObjectKey::new("tenant-expire::alpha");
        let writer = ClientRuntimeId::new("writer", ClientEpoch(1));

        backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-expire-finalized".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: None,
                delta_bytes: 16,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(2),
                },
                expires_at_ms: 200,
                created_at_ms: 100,
                writer_runtime: writer.clone(),
            })
            .expect("reservation should succeed");
        backend
            .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                reservation_id: "resv-expire-finalized".to_string(),
                expected_object_version: None,
                committed_length: Some(16),
                route_version: None,
                state: TenantObjectAccountingState::Active,
                updated_at_ms: 120,
                updated_by: "writer".to_string(),
            })
            .expect("finalize should succeed");

        backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-expire-aborted".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: Some(1),
                delta_bytes: 8,
                delta_objects: 0,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(2),
                },
                expires_at_ms: 260,
                created_at_ms: 140,
                writer_runtime: writer,
            })
            .expect("overwrite reservation should succeed");
        backend
            .abort_tenant_quota("resv-expire-aborted")
            .expect("abort should succeed");

        sleep(ttl + Duration::from_millis(80));

        let finalized_reservation_key = keyspace.tenant_quota_reservation("resv-expire-finalized");
        let finalized_index_key =
            keyspace.tenant_quota_reservation_index(&scope, "resv-expire-finalized");
        let aborted_reservation_key = keyspace.tenant_quota_reservation("resv-expire-aborted");
        let aborted_index_key =
            keyspace.tenant_quota_reservation_index(&scope, "resv-expire-aborted");
        let mut connection = backend
            .connection("redis test inspect reservation expiration")
            .expect("redis connection should succeed");
        for key in [
            finalized_reservation_key.as_str(),
            finalized_index_key.as_str(),
            aborted_reservation_key.as_str(),
            aborted_index_key.as_str(),
        ] {
            let exists: bool = connection
                .exists(key)
                .expect("expiration existence lookup should succeed");
            assert!(!exists, "terminal reservation key should expire: {key}");
        }
    }

    #[test]
    fn redis_backend_tenant_quota_listing_prunes_dangling_indexes() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-tenant-quota-dangling-index");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");
        let scope = TenantPolicyScope::new("tenant-z", None::<String>, None::<String>);
        let index_key = keyspace.tenant_quota_reservation_index(&scope, "dangling");
        let reservation_key = keyspace.tenant_quota_reservation("dangling");
        let mut connection = backend
            .connection("redis test seed dangling reservation index")
            .expect("redis connection should succeed");
        connection
            .set::<_, _, ()>(index_key.as_str(), reservation_key.as_str())
            .expect("should seed dangling index");
        drop(connection);

        let reservations = backend
            .list_tenant_quota_reservations(&scope)
            .expect("reservation listing should succeed");
        assert!(reservations.is_empty());

        let mut connection = backend
            .connection("redis test inspect dangling reservation index")
            .expect("redis connection should succeed");
        let dangling_index_exists: bool = connection
            .exists(index_key.as_str())
            .expect("index existence lookup should succeed");
        assert!(
            !dangling_index_exists,
            "dangling reservation index should be pruned during listing"
        );
    }

    #[test]
    fn redis_backend_tenant_quota_listing_prunes_empty_index_entries() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-tenant-quota-empty-index");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");
        let scope = TenantPolicyScope::new("tenant-empty", None::<String>, None::<String>);
        let index_key = keyspace.tenant_quota_reservation_index(&scope, "empty");
        let mut connection = backend
            .connection("redis test seed empty reservation index")
            .expect("redis connection should succeed");
        connection
            .set::<_, _, ()>(index_key.as_str(), "")
            .expect("should seed empty index entry");
        drop(connection);

        let reservations = backend
            .list_tenant_quota_reservations(&scope)
            .expect("reservation listing should succeed");
        assert!(reservations.is_empty());

        let mut connection = backend
            .connection("redis test inspect empty reservation index")
            .expect("redis connection should succeed");
        let empty_index_exists: bool = connection
            .exists(index_key.as_str())
            .expect("index existence lookup should succeed");
        assert!(
            !empty_index_exists,
            "empty reservation index should be pruned during listing"
        );
    }

    #[test]
    fn redis_backend_tenant_eviction_frontier_orders_and_refreshes_candidates() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-tenant-eviction-frontier")),
        )
        .expect("redis backend should initialize");
        let scope = TenantPolicyScope::new("tenant-evict", None::<String>, None::<String>);
        let writer = ClientRuntimeId::new("writer", ClientEpoch(1));

        let seed = |backend: &RedisMetadataBackend,
                    reservation_id: &str,
                    key: &str,
                    expected_object_version: Option<u64>,
                    delta_bytes: i64,
                    delta_objects: i64,
                    committed_length: Option<u64>,
                    state: TenantObjectAccountingState,
                    updated_at_ms: u64| {
            backend
                .reserve_tenant_quota(&TenantQuotaReservationRequest {
                    reservation_id: reservation_id.to_string(),
                    scope: scope.clone(),
                    key: ObjectKey::new(key),
                    expected_object_version,
                    delta_bytes,
                    delta_objects,
                    limit: TenantQuotaPolicy {
                        max_bytes: Some(4096),
                        max_objects: Some(64),
                    },
                    expires_at_ms: updated_at_ms + 100,
                    created_at_ms: updated_at_ms,
                    writer_runtime: writer.clone(),
                })
                .expect("reservation should succeed");
            backend
                .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                    reservation_id: reservation_id.to_string(),
                    expected_object_version,
                    committed_length,
                    route_version: None,
                    state,
                    updated_at_ms,
                    updated_by: "writer".to_string(),
                })
                .expect("finalize should succeed");
        };

        seed(
            &backend,
            "resv-alpha",
            "tenant-evict::alpha",
            None,
            10,
            1,
            Some(10),
            TenantObjectAccountingState::Active,
            100,
        );
        seed(
            &backend,
            "resv-beta",
            "tenant-evict::beta",
            None,
            20,
            1,
            Some(20),
            TenantObjectAccountingState::Active,
            100,
        );
        seed(
            &backend,
            "resv-gamma",
            "tenant-evict::gamma",
            None,
            5,
            1,
            Some(5),
            TenantObjectAccountingState::Active,
            90,
        );

        let listed = backend
            .list_tenant_eviction_candidates(&scope, 16)
            .expect("candidate listing should succeed");
        assert_eq!(
            listed
                .iter()
                .map(|object| object.key.0.as_str())
                .collect::<Vec<_>>(),
            vec![
                "tenant-evict::gamma",
                "tenant-evict::beta",
                "tenant-evict::alpha"
            ]
        );

        seed(
            &backend,
            "resv-beta-refresh",
            "tenant-evict::beta",
            Some(1),
            5,
            0,
            Some(25),
            TenantObjectAccountingState::Active,
            130,
        );
        let refreshed = backend
            .list_tenant_eviction_candidates(&scope, 16)
            .expect("candidate listing after refresh should succeed");
        assert_eq!(
            refreshed
                .iter()
                .map(|object| (
                    object.key.0.as_str(),
                    object.updated_at_ms,
                    object.committed_length
                ))
                .collect::<Vec<_>>(),
            vec![
                ("tenant-evict::gamma", 90, 5),
                ("tenant-evict::alpha", 100, 10),
                ("tenant-evict::beta", 130, 25),
            ]
        );

        seed(
            &backend,
            "resv-gamma-delete",
            "tenant-evict::gamma",
            Some(1),
            -5,
            -1,
            None,
            TenantObjectAccountingState::Deleted,
            140,
        );
        let after_delete = backend
            .list_tenant_eviction_candidates(&scope, 16)
            .expect("candidate listing after delete should succeed");
        assert_eq!(
            after_delete
                .iter()
                .map(|object| object.key.0.as_str())
                .collect::<Vec<_>>(),
            vec!["tenant-evict::alpha", "tenant-evict::beta"]
        );
    }

    #[test]
    fn redis_backend_tenant_quota_reservation_enforces_limits_and_lists_by_tenant() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-tenant-quota-limits")),
        )
        .expect("redis backend should initialize");
        let scope = TenantPolicyScope::new("tenant-b", None::<String>, None::<String>);
        let writer = ClientRuntimeId::new("writer", ClientEpoch(1));

        backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-1".to_string(),
                scope: scope.clone(),
                key: ObjectKey::new("tenant-b::one"),
                expected_object_version: None,
                delta_bytes: 40,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(1),
                },
                expires_at_ms: 100,
                created_at_ms: 10,
                writer_runtime: writer.clone(),
            })
            .expect("first reservation should succeed");

        let byte_limit = backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-2".to_string(),
                scope: scope.clone(),
                key: ObjectKey::new("tenant-b::two"),
                expected_object_version: None,
                delta_bytes: 32,
                delta_objects: 0,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(2),
                },
                expires_at_ms: 110,
                created_at_ms: 11,
                writer_runtime: writer.clone(),
            })
            .expect_err("bytes over limit should fail");
        assert!(matches!(
            byte_limit,
            StoreError::QuotaExceeded {
                kind: QuotaKind::Bytes,
                ..
            }
        ));

        let object_limit = backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-3".to_string(),
                scope: scope.clone(),
                key: ObjectKey::new("tenant-b::three"),
                expected_object_version: None,
                delta_bytes: 1,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(128),
                    max_objects: Some(1),
                },
                expires_at_ms: 120,
                created_at_ms: 12,
                writer_runtime: writer,
            })
            .expect_err("objects over limit should fail");
        assert!(matches!(
            object_limit,
            StoreError::QuotaExceeded {
                kind: QuotaKind::Objects,
                ..
            }
        ));

        let reservations = backend
            .list_tenant_quota_reservations(&scope)
            .expect("reservation listing should succeed");
        assert_eq!(reservations.len(), 1);
        assert_eq!(reservations[0].reservation_id, "resv-1");
        let quota = backend
            .get_tenant_quota_state(&scope)
            .expect("quota state lookup should succeed")
            .expect("quota state should exist");
        assert_eq!(quota.pending_reserved_bytes, 40);
        assert_eq!(quota.pending_reserved_objects, 1);
    }

    #[test]
    fn redis_connection_info_injects_env_auth_when_url_has_no_credentials() {
        let _guard = env_lock();
        std::env::set_var("MC_REDIS_USERNAME", "cloud-user");
        std::env::set_var("MC_REDIS_PASSWORD", "cloud-pass");

        let info = redis_connection_info("redis://127.0.0.1:6379/7")
            .expect("redis connection info should parse");
        assert_eq!(info.redis.username.as_deref(), Some("cloud-user"));
        assert_eq!(info.redis.password.as_deref(), Some("cloud-pass"));
        assert_eq!(info.redis.db, 7);

        std::env::remove_var("MC_REDIS_USERNAME");
        std::env::remove_var("MC_REDIS_PASSWORD");
    }

    #[test]
    fn redis_connection_info_prefers_explicit_url_credentials() {
        let _guard = env_lock();
        std::env::set_var("MC_REDIS_USERNAME", "env-user");
        std::env::set_var("MC_REDIS_PASSWORD", "env-pass");

        let info = redis_connection_info("redis://url-user:url-pass@127.0.0.1:6379/3")
            .expect("redis connection info should parse");
        assert_eq!(info.redis.username.as_deref(), Some("url-user"));
        assert_eq!(info.redis.password.as_deref(), Some("url-pass"));
        assert_eq!(info.redis.db, 3);

        std::env::remove_var("MC_REDIS_USERNAME");
        std::env::remove_var("MC_REDIS_PASSWORD");
    }

    #[test]
    fn legacy_auth_connection_info_strips_username_when_password_exists() {
        let info = redis_connection_info("redis://url-user:url-pass@127.0.0.1:6379/3")
            .expect("redis connection info should parse");
        let legacy =
            legacy_auth_connection_info(&info).expect("legacy auth info should be derived");
        assert_eq!(legacy.redis.username, None);
        assert_eq!(legacy.redis.password.as_deref(), Some("url-pass"));
        assert_eq!(legacy.redis.db, 3);
    }

    #[test]
    fn redis_auth_normalization_strips_username_when_password_auth_works() {
        let Some(server) = RedisTestServer::start_with_password(Some("legacy-pass")) else {
            return;
        };
        let url = server
            .url()
            .replacen("redis://:", "redis://legacy-user:", 1);

        let auth = resolve_redis_auth(&url).expect("redis auth should resolve");
        assert_eq!(auth.username, None);
        assert_eq!(auth.password.as_deref(), Some("legacy-pass"));

        let info = redis_connection_info(&url).expect("redis connection info should parse");
        assert_eq!(info.redis.username, None);
        assert_eq!(info.redis.password.as_deref(), Some("legacy-pass"));
    }

    #[test]
    fn legacy_auth_message_matcher_accepts_auth_arity_errors() {
        assert!(is_legacy_redis_auth_arity_error(
            "redis get_connection: An error was signalled by the server - ERR wrong number of arguments for 'auth' command"
        ));
        assert!(is_legacy_redis_auth_arity_error(
            "redis get_connection: ERR wrong number of arguments for `auth` command"
        ));
        assert!(!is_legacy_redis_auth_arity_error(
            "redis get_connection: WRONGPASS invalid username-password pair"
        ));
    }

    #[test]
    fn redis_timeout_helpers_honor_env_overrides() {
        let _guard = env_lock();
        std::env::set_var(REDIS_CONNECT_TIMEOUT_ENV, "7000");
        std::env::set_var(REDIS_IO_TIMEOUT_ENV, "11000");
        std::env::set_var(REDIS_AUTH_PROBE_TIMEOUT_ENV, "900");
        std::env::set_var(REDIS_RETRY_ATTEMPTS_ENV, "9");
        std::env::set_var(REDIS_RETRY_DELAY_ENV, "80");
        std::env::set_var(REDIS_CONNECTION_POOL_SIZE_ENV, "24");

        assert_eq!(redis_connect_timeout(), Duration::from_secs(7));
        assert_eq!(redis_io_timeout(), Duration::from_secs(11));
        assert_eq!(redis_auth_probe_timeout(), Duration::from_millis(900));
        assert_eq!(redis_retry_attempts(), 9);
        assert_eq!(redis_retry_delay(), Duration::from_millis(80));
        assert_eq!(redis_connection_pool_size(), 24);

        std::env::remove_var(REDIS_CONNECT_TIMEOUT_ENV);
        std::env::remove_var(REDIS_IO_TIMEOUT_ENV);
        std::env::remove_var(REDIS_AUTH_PROBE_TIMEOUT_ENV);
        std::env::remove_var(REDIS_RETRY_ATTEMPTS_ENV);
        std::env::remove_var(REDIS_RETRY_DELAY_ENV);
        std::env::remove_var(REDIS_CONNECTION_POOL_SIZE_ENV);
        assert_eq!(redis_connect_timeout(), DEFAULT_REDIS_CONNECT_TIMEOUT);
        assert_eq!(redis_io_timeout(), DEFAULT_REDIS_IO_TIMEOUT);
        assert_eq!(redis_auth_probe_timeout(), DEFAULT_REDIS_AUTH_PROBE_TIMEOUT);
        assert_eq!(redis_retry_attempts(), DEFAULT_REDIS_RETRY_ATTEMPTS);
        assert_eq!(redis_retry_delay(), DEFAULT_REDIS_RETRY_DELAY);
        assert_eq!(
            redis_connection_pool_size(),
            DEFAULT_REDIS_CONNECTION_POOL_SIZE
        );
    }

    #[test]
    fn route_namespace_redacts_url_credentials() {
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new("redis://user:secret@127.0.0.1:6379/0")
                .keyspace(MetadataKeyspace::new("test/redacted")),
        )
        .expect("redis backend should initialize");

        let namespace = backend.route_namespace();
        assert!(namespace.contains("127.0.0.1:6379/0"));
        assert!(namespace.contains("test/redacted"));
        assert!(!namespace.contains("user:secret"));
        assert_eq!(
            redacted_route_namespace_source("redis://user:secret@127.0.0.1:6379/0"),
            "redis://127.0.0.1:6379/0".to_string()
        );
    }

    // -----------------------------------------------------------------------
    // Adversarial: transient-error classification
    // -----------------------------------------------------------------------

    #[test]
    fn transient_error_classifier_covers_all_io_error_kinds() {
        use super::should_retry_transient_redis_error;
        let transient_kinds = [
            std::io::ErrorKind::Interrupted,
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::ConnectionRefused,
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::NotConnected,
        ];
        for kind in &transient_kinds {
            let err = redis::RedisError::from(std::io::Error::from(*kind));
            assert!(
                should_retry_transient_redis_error(&err),
                "IO kind {kind:?} must classify as transient"
            );
        }
    }

    #[test]
    fn transient_error_classifier_matches_message_patterns() {
        use super::should_retry_transient_redis_error;
        let transient_messages = [
            "connection refused",
            "Connection reset by peer",
            "CONNECTION ABORTED",
            "connection closed unexpectedly",
            "not connected to server",
            "broken pipe",
            "resource temporarily unavailable",
            "operation timed out",
            "interrupted system call",
        ];
        for message in &transient_messages {
            let err =
                redis::RedisError::from((redis::ErrorKind::IoError, "test", message.to_string()));
            assert!(
                should_retry_transient_redis_error(&err),
                "message '{message}' must classify as transient"
            );
        }
    }

    #[test]
    fn transient_error_classifier_rejects_non_transient_errors() {
        use super::should_retry_transient_redis_error;
        let non_transient = [
            redis::RedisError::from((
                redis::ErrorKind::AuthenticationFailed,
                "WRONGPASS",
                "invalid username-password pair".to_string(),
            )),
            redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "WRONGTYPE",
                "Operation against a key holding the wrong kind of value".to_string(),
            )),
            redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "ERR",
                "unknown command 'FOOBAR'".to_string(),
            )),
            redis::RedisError::from((
                redis::ErrorKind::ReadOnly,
                "READONLY",
                "You can't write against a read only replica".to_string(),
            )),
        ];
        for err in &non_transient {
            assert!(
                !should_retry_transient_redis_error(err),
                "err {err:?} must NOT classify as transient"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Adversarial: retry-config env-var handling
    // -----------------------------------------------------------------------

    #[test]
    fn redis_retry_config_defaults_are_sane_when_env_unset() {
        let _guard = env_lock();
        std::env::remove_var(REDIS_RETRY_ATTEMPTS_ENV);
        std::env::remove_var(REDIS_RETRY_DELAY_ENV);

        assert_eq!(redis_retry_attempts(), DEFAULT_REDIS_RETRY_ATTEMPTS);
        assert_eq!(redis_retry_delay(), DEFAULT_REDIS_RETRY_DELAY);
        assert_eq!(redis_connect_timeout(), DEFAULT_REDIS_CONNECT_TIMEOUT);
        assert_eq!(redis_io_timeout(), DEFAULT_REDIS_IO_TIMEOUT);
    }

    #[test]
    fn redis_retry_config_rejects_zero_and_invalid_values() {
        let _guard = env_lock();
        std::env::set_var(REDIS_RETRY_ATTEMPTS_ENV, "0");
        assert_eq!(
            redis_retry_attempts(),
            DEFAULT_REDIS_RETRY_ATTEMPTS,
            "zero attempts must fall back to default"
        );
        std::env::set_var(REDIS_RETRY_ATTEMPTS_ENV, "not-a-number");
        assert_eq!(
            redis_retry_attempts(),
            DEFAULT_REDIS_RETRY_ATTEMPTS,
            "non-numeric must fall back to default"
        );
        std::env::set_var(REDIS_RETRY_DELAY_ENV, "");
        assert_eq!(
            redis_retry_delay(),
            DEFAULT_REDIS_RETRY_DELAY,
            "empty string must fall back to default"
        );
        std::env::remove_var(REDIS_RETRY_ATTEMPTS_ENV);
        std::env::remove_var(REDIS_RETRY_DELAY_ENV);
    }

    #[test]
    fn redis_connection_pool_size_rejects_zero_and_caps_large_values() {
        let _guard = env_lock();

        std::env::set_var(REDIS_CONNECTION_POOL_SIZE_ENV, "0");
        assert_eq!(
            redis_connection_pool_size(),
            DEFAULT_REDIS_CONNECTION_POOL_SIZE
        );

        std::env::set_var(REDIS_CONNECTION_POOL_SIZE_ENV, "not-a-number");
        assert_eq!(
            redis_connection_pool_size(),
            DEFAULT_REDIS_CONNECTION_POOL_SIZE
        );

        std::env::set_var(
            REDIS_CONNECTION_POOL_SIZE_ENV,
            (MAX_REDIS_CONNECTION_POOL_SIZE + 1).to_string(),
        );
        assert_eq!(redis_connection_pool_size(), MAX_REDIS_CONNECTION_POOL_SIZE);

        std::env::remove_var(REDIS_CONNECTION_POOL_SIZE_ENV);
    }

    #[test]
    fn redis_error_diagnostic_hint_flags_addr_not_available() {
        let err = redis::RedisError::from((
            redis::ErrorKind::IoError,
            "test",
            "Cannot assign requested address (os error 99)".to_string(),
        ));
        assert_eq!(
            redis_error_diagnostic_hint(&err),
            Some(
                "possible local ephemeral/source-port exhaustion while opening Redis metadata connections"
            )
        );
    }

    #[test]
    fn metadata_error_appends_addr_not_available_hint() {
        let err = redis::RedisError::from((
            redis::ErrorKind::IoError,
            "test",
            "Cannot assign requested address (os error 99)".to_string(),
        ));
        let rendered = metadata_error("redis list live clients", err).to_string();
        assert!(rendered.contains("redis list live clients"));
        assert!(rendered.contains("possible local ephemeral/source-port exhaustion"));
    }

    #[test]
    fn redis_diagnostic_occurrence_rate_limit_logs_early_and_powers_of_two() {
        assert!(should_log_redis_diagnostic_occurrence(1));
        assert!(should_log_redis_diagnostic_occurrence(8));
        assert!(should_log_redis_diagnostic_occurrence(16));
        assert!(!should_log_redis_diagnostic_occurrence(15));
    }

    // -----------------------------------------------------------------------
    // Adversarial: Redis integration (skipped if redis-server unavailable)
    // -----------------------------------------------------------------------

    #[test]
    fn redis_backend_concurrent_route_cas_only_one_wins() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = std::sync::Arc::new(
            RedisMetadataBackend::new(
                RedisMetadataConfig::new(server.url())
                    .keyspace(MetadataKeyspace::new("test/redis-concurrent-cas")),
            )
            .expect("redis backend"),
        );

        let key = ObjectKey::new("contested-key");
        let wins = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let handles: Vec<_> = (0..5)
            .map(|i| {
                let backend = backend.clone();
                let key = key.clone();
                let wins = wins.clone();
                std::thread::spawn(move || {
                    let mut route = sample_route(1);
                    route.key = key.clone();
                    route.replicas[0].owner =
                        ClientRuntimeId::new(format!("racer-{i}"), ClientEpoch(1));
                    let res = backend
                        .compare_and_swap_object_route(&key, None, Some(&route))
                        .unwrap();
                    if res.applied {
                        wins.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            wins.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "exactly one insert CAS must win"
        );
    }

    #[test]
    fn redis_backend_route_cas_delete_with_wrong_version_does_not_apply() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-cas-delete")),
        )
        .expect("redis backend");

        let key = ObjectKey::new("delete-me");
        let route = sample_route(1);
        let inserted = backend
            .compare_and_swap_object_route(&key, None, Some(&route))
            .expect("insert");
        assert!(inserted.applied);

        let wrong = backend
            .compare_and_swap_object_route(&key, Some(RouteVersion(99)), None)
            .expect("cas must not crash on wrong version");
        assert!(!wrong.applied);
        assert!(
            backend.get_object_route(&key).unwrap().is_some(),
            "route must survive the failed CAS-delete"
        );
    }

    #[test]
    fn redis_backend_route_cas_version_chain_is_sequential() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-cas-chain")),
        )
        .expect("redis backend");

        let key = ObjectKey::new("versioned-key");
        let r1 = {
            let mut r = sample_route(1);
            r.key = key.clone();
            r
        };
        let r2 = {
            let mut r = sample_route(2);
            r.key = key.clone();
            r
        };
        let r3 = {
            let mut r = sample_route(3);
            r.key = key.clone();
            r
        };
        assert!(
            backend
                .compare_and_swap_object_route(&key, None, Some(&r1))
                .unwrap()
                .applied
        );
        assert!(
            backend
                .compare_and_swap_object_route(&key, Some(RouteVersion(1)), Some(&r2))
                .unwrap()
                .applied
        );
        assert!(
            backend
                .compare_and_swap_object_route(&key, Some(RouteVersion(2)), Some(&r3))
                .unwrap()
                .applied
        );

        let current = backend.get_object_route(&key).unwrap().unwrap();
        assert_eq!(current.version, RouteVersion(3));
    }

    #[test]
    fn redis_backend_multiple_keyspaces_are_isolated() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let url = server.url();

        let backend_a = RedisMetadataBackend::new(
            RedisMetadataConfig::new(url).keyspace(MetadataKeyspace::new("test/ns-a")),
        )
        .expect("backend A");
        let backend_b = RedisMetadataBackend::new(
            RedisMetadataConfig::new(url).keyspace(MetadataKeyspace::new("test/ns-b")),
        )
        .expect("backend B");

        let lease_a = ClientLease {
            runtime: ClientRuntimeId::new("node-a", ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: Default::default(),
            expires_at_ms: super::now_ms() + 60_000,
        };
        let lease_b = ClientLease {
            runtime: ClientRuntimeId::new("node-b", ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: Default::default(),
            expires_at_ms: super::now_ms() + 60_000,
        };
        backend_a.upsert_client_lease(&lease_a).unwrap();
        backend_b.upsert_client_lease(&lease_b).unwrap();

        let listed_a = backend_a.list_live_clients().unwrap();
        let listed_b = backend_b.list_live_clients().unwrap();
        assert_eq!(listed_a.len(), 1);
        assert_eq!(listed_b.len(), 1);
        assert_eq!(listed_a[0].runtime.stable_id.0, "node-a");
        assert_eq!(listed_b[0].runtime.stable_id.0, "node-b");
    }

    #[test]
    fn redis_backend_handoff_put_get_round_trip() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-handoff")),
        )
        .expect("redis backend");

        let stable_id = ClientStableId::new("handoff-node");
        let plan = HandoffPlan {
            stable_id: stable_id.clone(),
            from: ClientRuntimeId::new("old-runtime", ClientEpoch(1)),
            to: ClientRuntimeId::new("new-runtime", ClientEpoch(2)),
            kind: HandoffKind::GracefulDrain,
            barrier_version: 42,
            created_at_ms: 1_000,
            deadline_ms: Some(5_000),
        };
        backend.put_handoff(&plan).unwrap();

        let got = backend
            .get_handoff(&stable_id)
            .unwrap()
            .expect("handoff must be retrievable");
        assert_eq!(got.stable_id.0, "handoff-node");
        assert!(matches!(got.kind, HandoffKind::GracefulDrain));
        assert_eq!(got.barrier_version, 42);
        assert_eq!(got.deadline_ms, Some(5_000));
    }

    #[test]
    fn redis_backend_update_client_state_on_nonexistent_returns_not_found() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-update-ghost")),
        )
        .expect("redis backend");

        let ghost = ClientRuntimeId::new("ghost-node", ClientEpoch(99));
        let err = backend
            .update_client_state(&ghost, ClientLifecycleState::Draining)
            .expect_err("phantom client must not update");
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    #[test]
    fn redis_backend_update_client_state_preserves_existing_lease() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-update-state")),
        )
        .expect("redis backend");

        let runtime = ClientRuntimeId::new("update-node", ClientEpoch(1));
        let lease = ClientLease {
            runtime: runtime.clone(),
            state: ClientLifecycleState::Standby,
            compatibility: Default::default(),
            endpoints: Default::default(),
            expires_at_ms: super::now_ms() + 60_000,
        };
        backend.upsert_client_lease(&lease).unwrap();
        backend
            .update_client_state(&runtime, ClientLifecycleState::Draining)
            .unwrap();

        let key = backend.keyspace.client(&runtime);
        let payload: Option<String> = backend
            .connection("redis test inspect updated client lease")
            .expect("redis connection should succeed")
            .get(&key)
            .expect("updated lease lookup should succeed");
        let updated: ClientLease =
            serde_json::from_str(&payload.expect("updated lease should remain stored in redis"))
                .expect("updated lease payload should deserialize");
        assert_eq!(updated.runtime, runtime);
        assert_eq!(updated.state, ClientLifecycleState::Draining);
    }

    #[test]
    fn redis_backend_unpublish_then_republish_segment() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-unpub-repub")),
        )
        .expect("redis backend");

        let owner = sample_runtime();
        let segment_name = SegmentName::new("volatile-seg");
        let segment = SegmentAnnouncement {
            owner: owner.clone(),
            segment_name: segment_name.clone(),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 256,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 16,
            tags: vec![],
        };

        backend.publish_segment(&segment).unwrap();
        assert_eq!(backend.list_segments(Some(&owner)).unwrap().len(), 1);
        backend.unpublish_segment(&owner, &segment_name).unwrap();
        assert_eq!(backend.list_segments(Some(&owner)).unwrap().len(), 0);
        backend.publish_segment(&segment).unwrap();
        assert_eq!(backend.list_segments(Some(&owner)).unwrap().len(), 1);
    }

    #[test]
    fn redis_backend_segment_draining_rejects_new_reservations() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-seg-drain")),
        )
        .expect("redis backend");

        let owner = sample_runtime();
        let segment_name = SegmentName::new("drain-seg");
        let segment = SegmentAnnouncement {
            owner: owner.clone(),
            segment_name: segment_name.clone(),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 512,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 1,
            tags: vec![],
        };
        backend.publish_segment(&segment).unwrap();
        backend
            .update_segment_state(&owner, &segment_name, SegmentLifecycleState::Draining)
            .unwrap();

        let err = backend
            .reserve_segment(&owner, &segment_name, 64)
            .expect_err("draining segment must reject new reservations");
        assert!(matches!(
            err,
            StoreError::InvalidState(_) | StoreError::NotFound(_)
        ));
    }

    #[test]
    fn redis_backend_segment_exhaustion_returns_error() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-seg-exhaust")),
        )
        .expect("redis backend");

        let owner = sample_runtime();
        backend
            .publish_segment(&SegmentAnnouncement {
                owner: owner.clone(),
                segment_name: SegmentName::new("tiny-seg"),
                transport_endpoint: None,
                transport_segment_descriptor: None,
                capacity_bytes: 64,
                used_bytes: 0,
                target_chunks: Vec::new(),
                state: SegmentLifecycleState::Active,
                alignment_bytes: 1,
                tags: vec![],
            })
            .unwrap();

        backend
            .reserve_segment(&owner, &SegmentName::new("tiny-seg"), 32)
            .unwrap();
        backend
            .reserve_segment(&owner, &SegmentName::new("tiny-seg"), 32)
            .unwrap();
        let err = backend
            .reserve_segment(&owner, &SegmentName::new("tiny-seg"), 1)
            .expect_err("must reject reserve past capacity");
        assert!(matches!(err, StoreError::Allocator(_)));
    }
}

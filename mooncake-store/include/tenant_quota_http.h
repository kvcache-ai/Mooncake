#pragma once

// Tenant-quota HTTP admin endpoints.
//
// Registered on MasterAdminServer's coro_http server alongside the
// pre-existing /metrics, /health, and /role endpoints. The
// implementation lives in src/tenant_quota_http.cpp and is friended
// by MasterAdminServer so it can read the server's private state
// without forcing the registration call into MasterAdminServer's own
// translation unit.
//
// Endpoints (see the deployment guide for the full schema):
//   GET    /tenants/quota                        - list every tenant
//   GET    /tenants/quota/_default               - read default policy
//   PUT    /tenants/quota/_default               - update default policy
//   GET    /tenants/quota/query?tenant_id=<id>   - inspect one tenant
//   PUT    /tenants/quota/query?tenant_id=<id>   - upsert one tenant
//   DELETE /tenants/quota/query?tenant_id=<id>   - drop explicit policy

namespace mooncake {

class MasterAdminServer;

// Register the /tenants/quota* admin endpoints on `admin`'s underlying
// coro_http server. Called once from MasterAdminServer::InitHttpServer.
void RegisterTenantQuotaAdminEndpoints(MasterAdminServer* admin);

}  // namespace mooncake

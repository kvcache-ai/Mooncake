# Securing the etcd Metadata Service (RBAC + TLS)

Mooncake's Go etcd client (`libetcd_wrapper.so`) can authenticate against an
etcd cluster with RBAC and/or encrypt the connection with one-way TLS. Both
are configured purely with environment variables on the Mooncake node; no
code or endpoint string changes are needed.

## Environment variables

| Variable | Purpose |
|---|---|
| `MC_ETCD_CONF_FILE` | Path to a file with `username=...` and `password=...` lines (one per line, `#` lines and empty lines ignored) |
| `MC_ETCD_TLS_CA_CERT` | Path to the CA certificate used to verify the etcd server (one-way TLS) |
| `MC_ETCD_TLS_SERVER_NAME` | Optional: override the server name used for TLS SNI / certificate hostname verification (e.g. connecting via IP) |
| `MC_ETCD_TLS_INSECURE_SKIP_VERIFY` | Set to `"true"` to skip certificate verification (testing only, never in production) |

All variables are optional. When none are set, the client behaves exactly as
before (plaintext, no authentication).

## Example

```bash
# credentials file /etc/mooncake/etcd/credentials:
#   username=mooncake
#   password=<secret>

export MC_ETCD_CONF_FILE=/etc/mooncake/etcd/credentials
export MC_ETCD_TLS_CA_CERT=/etc/mooncake/etcd/ca.crt
```

The endpoint string keeps its usual form (`etcd://host:port` or bare
`host:port`; the C++ callers already strip the scheme before the Go client
sees it). When TLS is enabled the client upgrades automatically to an
encrypted connection.
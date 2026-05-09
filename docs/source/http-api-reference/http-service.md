# Mooncake Store HTTP Service

The Mooncake Store HTTP Service provides RESTful endpoints for cluster management, monitoring, and data operations. This service is embedded within the `mooncake_master` process and can be enabled alongside the primary RPC services.

## Overview

The HTTP service serves multiple purposes:
- **Metrics & Monitoring**: Prometheus-compatible metrics endpoints
- **Cluster Management**: Query and manage distributed storage segments
- **Data Inspection**: Examine stored objects and their replicas
- **Health Checks**: Service availability and status verification

The Python `mooncake.mooncake_store_service` module also provides a lightweight
Store REST API for data operations and standalone segment mount/unmount
workflows. Unless configured otherwise, it listens on port `8080`.

## HTTP Endpoints

### Metrics Endpoints

#### `/metrics`
Prometheus-compatible metrics endpoint providing detailed system metrics in text format.

**Method**: `GET`
**Content-Type**: `text/plain; version=0.0.4`
**Response**: Comprehensive metrics including request counts, error rates, latency statistics, and resource utilization

**Example**:
```bash
curl http://localhost:8080/metrics
```

#### `/metrics/summary`
Human-readable metrics summary with key performance indicators.

**Method**: `GET`
**Content-Type**: `text/plain; version=0.0.4`
**Response**: Condensed overview of system health and performance metrics

**Example**:
```bash
curl http://localhost:8080/metrics/summary
```

### Data Management Endpoints

#### `/query_key`
Retrieve replica information for a specific key, including memory locations and transport endpoints.

**Method**: `GET`
**Parameters**: `key` (query parameter) - The object key to query
**Content-Type**: `text/plain; version=0.0.4`
**Response**: JSON-formatted replica descriptors for memory replicas

**Example**:
```bash
curl "http://localhost:8080/query_key?key=my_object"
```

**Response Format**:
```json
{
  "transport_endpoint_": "hostname:port",
  "buffer_descriptors": [...]
}
```

#### `/batch_query_keys`
Retrieve replica information for multiple keys in a single request, including memory locations and transport endpoints for each key.

**Method**: `GET`
**Parameters**: `keys` (query parameter) - Comma-separated list of object keys to query (format: key1,key2,key3)
**Content-Type**: `application/json; charset=utf-8`
**Response**: JSON-formatted mapping of keys to their respective replica descriptors

**Example**:
```bash
curl "http://localhost:8080/batch_query_keys?keys=key1,key2,key3"
```

**Response Format**:
```json
{
  "success": true,
  "data": {
    "key1": {
      "ok": true,
      "values": [
        {
          "transport_endpoint_": "hostname:port",
          "buffer_descriptor": {...}
        }
      ]
    },
    "key2": {
      "ok": false,
      "error": "error message"
    }
  }
}
```

#### `/get_all_keys`
List all keys currently stored in the distributed system.

**Method**: `GET`
**Content-Type**: `text/plain; version=0.0.4`
**Response**: Newline-separated list of all stored keys

**Example**:
```bash
curl http://localhost:8080/get_all_keys
```

### Segment Management Endpoints

#### `/get_all_segments`
List all mounted segments in the cluster.

**Method**: `GET`
**Content-Type**: `text/plain; version=0.0.4`
**Response**: Newline-separated list of segment names

**Example**:
```bash
curl http://localhost:8080/get_all_segments
```

#### `/query_segment`
Query detailed information about a specific segment, including used and available capacity.

**Method**: `GET`
**Parameters**: `segment` (query parameter) - Segment name to query
**Content-Type**: `text/plain; version=0.0.4`
**Response**: Multi-line text with segment details

**Example**:
```bash
curl "http://localhost:8080/query_segment?segment=segment_name"
```

**Response Format**:
```
segment_name
Used(bytes): 1073741824
Capacity(bytes): 4294967296
```

### Health Check Endpoints

#### `/health`
Basic health check endpoint for service availability verification.

**Method**: `GET`
**Content-Type**: `text/plain; version=0.0.4`
**Response**: `OK` when service is healthy
**Status Codes**: 
- `200 OK`: Service is healthy
- Other: Service may be experiencing issues

**Example**:
```bash
curl http://localhost:8080/health
```

## Store REST API Endpoints

The following endpoints are served by the Python store REST service, which wraps
`MooncakeDistributedStore` with an aiohttp service. The HTTP handlers live in
Python, while mount and unmount operations are delegated to the underlying store
binding. Start the service with:

```bash
python -m mooncake.mooncake_store_service \
  --config /path/to/mooncake_config.json \
  --port 8080
```

If the wheel console scripts are installed, the equivalent command is:

```bash
mc_store_rest_server --config /path/to/mooncake_config.json --port 8080
```

### `/api/mount_shm`
Mount a named shared memory object as one or more Mooncake store segments. If
the requested size exceeds the maximum registration size, the service may split
the region and return multiple segment ids.

**Method**: `POST`
**Content-Type**: `application/json`

**Request Body**:
```json
{
  "name": "mooncake_segment",
  "size": 16777216,
  "offset": 0,
  "protocol": "tcp",
  "location": ""
}
```

**Fields**:
- `name` (string, required): Named shared memory object name. A leading `/` is
  accepted, but path separators are not.
- `size` (integer, required): Number of bytes to mount.
- `offset` (integer, optional): File offset in bytes. Defaults to `0`.
- `protocol` (string, optional): Transfer protocol. Defaults to the service
  configuration protocol.
- `location` (string, optional): Device or locality hint. Defaults to an empty
  string.

**Success Response**:
```json
{
  "status": "success",
  "segment_ids": ["00000000-0000-0000-0000-000000000001"]
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/api/mount_shm \
  -H "Content-Type: application/json" \
  -d '{
        "name": "mooncake_segment",
        "size": 16777216,
        "offset": 0,
        "protocol": "tcp",
        "location": ""
      }'
```

### `/api/unmount_shm`
Unmount one or more segment ids previously returned by `/api/mount_shm`.

**Method**: `POST`
**Content-Type**: `application/json`

**Request Body**:
```json
{
  "segment_ids": ["00000000-0000-0000-0000-000000000001"]
}
```

`segment_ids` may also be provided as a single string for one segment.

**Success Response**:
```json
{
  "status": "success"
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/api/unmount_shm \
  -H "Content-Type: application/json" \
  -d '{"segment_ids": ["00000000-0000-0000-0000-000000000001"]}'
```

### `/api/mount`
Allocate memory inside the store process and mount it as one or more Mooncake
store segments. If the requested size exceeds the maximum registration size,
the service may split it and return multiple segment ids. The response includes
the actual allocated size after alignment.

**Method**: `POST`
**Content-Type**: `application/json`

**Request Body**:
```json
{
  "size": 16777216,
  "protocol": "tcp",
  "location": ""
}
```

**Fields**:
- `size` (integer, required): Number of bytes requested. Must be positive.
- `protocol` (string, optional): Transfer protocol. Defaults to the service
  configuration protocol.
- `location` (string, optional): Device or locality hint. Defaults to an empty
  string.

**Success Response**:
```json
{
  "status": "success",
  "segment_ids": ["00000000-0000-0000-0000-000000000002"],
  "allocated_size": 16777216
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/api/mount \
  -H "Content-Type: application/json" \
  -d '{"size": 16777216, "protocol": "tcp", "location": ""}'
```

### `/api/unmount`
Unmount one or more segment ids previously returned by `/api/mount` and free
the memory allocated by the store process.

**Method**: `POST`
**Content-Type**: `application/json`

**Request Body**:
```json
{
  "segment_ids": ["00000000-0000-0000-0000-000000000002"]
}
```

`segment_ids` may also be provided as a single string for one segment.

**Success Response**:
```json
{
  "status": "success"
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/api/unmount \
  -H "Content-Type: application/json" \
  -d '{"segment_ids": ["00000000-0000-0000-0000-000000000002"]}'
```

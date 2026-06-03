# Mooncake Store HTTP Service

The Mooncake Store HTTP Service provides RESTful endpoints for cluster management, monitoring, and data operations. This service is embedded within the `mooncake_master` process and can be enabled alongside the primary RPC services.

## Overview

The HTTP service serves multiple purposes:
- **Metrics & Monitoring**: Prometheus-compatible metrics endpoints
- **Cluster Management**: Query and manage distributed storage segments
- **Data Inspection**: Examine stored objects and their replicas
- **Health Checks**: Service availability and status verification

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


# conductor-ctrl API Documentation

## Overview

conductor-ctrl provides HTTP APIs for cache query and dynamic service management.

**Base URL**: `http://localhost:13333`

---

## Endpoints

### POST /query

Query cache hit status for given token IDs.

**Content-Type**: `application/json`

**Request Body:**
```json
{
  "model": "string",           // Required. Model name.
  "lora_name": "string|null", // Optional. LoRA adapter name.
  "lora_id": "int64|null",    // Optional. LoRA adapter ID.
  "token_ids": [int32],      // Required. Array of token IDs.
  "instance_id": "string|null", // Optional. Specific instance to query.
  "tenant_id": "string|null", // Optional. Tenant ID (default: "default").
  "block_size": "int64",      // Required. Block size.
  "cache_salt": "string|null" // Optional. Cache salt for isolation.
}
```

**Response:**
```json
{
  "<tenant_id>": {
    "<instance_id>": {
      "longest_matched": "int64",  // Longest matched prefix in tokens.
      "DP": {                      // Data parallel ranks and matched tokens.
        "<dp_rank>": "int64"
      },
      "GPU": "int64",             // Matched tokens on GPU.
      "CPU": "int64",            // Matched tokens on CPU.
      "DISK": "int64"            // Matched tokens on DISK.
    }
  }
}
```

**Example:**
```bash
curl -X POST http://localhost:13333/query \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama-2-7b",
    "block_size": 16,
    "token_ids": [1, 2, 3, 4, 5, 6, 7, 8],
    "instance_id": "instance-1",
    "tenant_id": "default"
  }'
```

---

### POST /register

Dynamically register a new KV event service.

**Content-Type**: `application/json`

**Request Body:**
```json
{
  "endpoint": "string",           // Required. ZMQ publisher endpoint.
  "replay_endpoint": "string",   // Required. ZMQ replay endpoint.
  "type": "string",              // Required. "vLLM" or "Mooncake".
  "modelname": "string",         // Required. Model name.
  "lora_name": "string|null",    // Optional. LoRA adapter name.
  "tenant_id": "string|null",    // Optional. Tenant ID (default: "default").
  "instance_id": "string",       // Required. Unique instance identifier.
  "block_size": "int64",          // Required. Block size.
  "dp_rank": "int",               // Required. Data parallel rank.
  "additionalsalt": "string|null" // Optional. Additional salt.
}
```

**Response:**
```json
{
  "status": "string",
  "instance_id": "string"
}
```

**Example:**
```bash
curl -X POST http://localhost:13333/register \
  -H "Content-Type: application/json" \
  -d '{
    "endpoint": "tcp://192.168.1.100:5555",
    "replay_endpoint": "tcp://192.168.1.100:5556",
    "type": "vLLM",
    "modelname": "llama-2-7b",
    "instance_id": "instance-2",
    "block_size": 16,
    "dp_rank": 0
  }'
```

---

### POST /unregister

Unregister an existing KV event service.

**Content-Type**: `application/json`

**Request Body:**
```json
{
  "type": "string",       // Required. "vLLM" or "Mooncake".
  "modelname": "string",  // Required. Model name.
  "lora_name": "string|null", // Optional. LoRA adapter name.
  "tenant_id": "string|null", // Optional. Tenant ID (default: "default").
  "instance_id": "string",    // Required. Instance identifier.
  "block_size": "int",         // Required. Block size.
  "dp_rank": "int"             // Required. Data parallel rank.
}
```

**Response:**
```json
{
  "status": "string",
  "removed_instances": ["string"]
}
```

**Example:**
```bash
curl -X POST http://localhost:13333/unregister \
  -H "Content-Type: application/json" \
  -d '{
    "type": "vLLM",
    "modelname": "llama-2-7b",
    "instance_id": "instance-2",
    "block_size": 16,
    "dp_rank": 0
  }'
```

---

### GET /global_view

Get global view of all cached prefixes across all tenants and models.

**Response:**
```json
{
  "context_count": "int32",         // Number of model contexts.
  "model_contexts": [              // Array of model contexts.
    {
      "model_name": "string",
      "lora_name": "string",
      "block_size": "int64",
      "additional_salt": "string",
      "tenant_id": "string"
    }
  ],
  "hashmap": [[map[uint64]uint64]], // Array of uint64 maps (engine_block_hash -> conductor_prefix_hash).
}
```

**Example:**
```bash
curl http://localhost:13333/global_view
```

---

## Error Responses

| Status Code | Description |
|-------------|-------------|
| 400 | Bad Request - Invalid JSON or missing required fields |
| 404 | Not Found - Service not found (for unregister) |
| 405 | Method Not Allowed - HTTP method not supported |
| 500 | Internal Server Error - Server-side error |

**Example Error Response:**
```json
{
  "error": "Invalid JSON"
}
```
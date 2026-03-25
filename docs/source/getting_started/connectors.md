# Mooncake Store Data Connectors

Data connectors provide a unified interface for importing data from external sources into Mooncake Store.

## Supported Connectors

### 1. OSS Connector (Alibaba Cloud Object Storage)

Import data from Alibaba Cloud OSS (S3-compatible).

**Configuration:**
```bash
export MOONCAKE_OSS_ENDPOINT="oss-cn-hangzhou.aliyuncs.com"
export MOONCAKE_OSS_BUCKET="my-bucket"
export MOONCAKE_OSS_ACCESS_KEY_ID="your-access-key"
export MOONCAKE_OSS_SECRET_ACCESS_KEY="your-secret-key"
export MOONCAKE_OSS_REGION="oss-cn-hangzhou"  # optional
```

**Requirements:** AWS SDK (compiled with `HAVE_AWS_SDK`)

### 2. Hugging Face Connector

Download models and datasets from Hugging Face Hub.

**Configuration:**
```bash
export MOONCAKE_HF_TOKEN="hf_xxxxx"  # optional, for private repos
export MOONCAKE_HF_ENDPOINT="https://huggingface.co"  # optional
```

**Key Format:** `repo_id/path/to/file` (e.g., `bert-base-uncased/pytorch_model.bin`)

**Requirements:** libcurl (standard dependency)

### 3. Redis Connector

Import key-value pairs from Redis.

**Configuration:**
```bash
export MOONCAKE_REDIS_HOST="127.0.0.1"
export MOONCAKE_REDIS_PORT="6379"
export MOONCAKE_REDIS_PASSWORD="password"  # optional
export MOONCAKE_REDIS_DB="0"  # optional
```

**Requirements:** hiredis library (compiled with `HAVE_REDIS`)

## C++ Usage

```cpp
#include "connectors/data_connector.h"
#include "connectors/connector_importer.h"

// Create connector
auto connector = mooncake::DataConnector::Create(
    mooncake::ConnectorType::OSS);

// Create importer
auto client = std::make_shared<mooncake::Client>();
mooncake::ConnectorImporter importer(client, std::move(connector));

// Import single object
mooncake::ReplicateConfig config;
config.replica_num = 2;
auto result = importer.ImportObject("models/bert.bin", "bert_model", config);

// Import by prefix
auto count = importer.ImportByPrefix("models/", config);
```

## Python Usage

```python
import mooncake_store as mc

# Create client
client = mc.Client.create(...)

# Create connector
connector = mc.create_connector(mc.ConnectorType.OSS)

# Create importer
importer = mc.ConnectorImporter(client, connector)

# Import single object
config = mc.ReplicateConfig(replica_num=2)
importer.import_object("models/bert.bin", "bert_model", config)

# Import by prefix
count = importer.import_by_prefix("models/", config)
print(f"Imported {count} objects")
```

## Build Configuration

```bash
# Build with OSS connector (requires AWS SDK)
cmake -DHAVE_AWS_SDK=ON ..

# Build with Redis connector (requires hiredis)
cmake .. # Auto-detected if hiredis is installed

# Build without optional dependencies
cmake .. # HuggingFace connector always available
```

## Architecture

- **DataConnector**: Abstract base class
- **OSSConnector**: Wraps S3Helper for OSS access
- **HuggingFaceConnector**: Uses libcurl for HTTP downloads
- **RedisConnector**: Uses hiredis for Redis access
- **ConnectorImporter**: Integration layer using Client API

## Implementation Details

- Connectors handle external operations (list, download)
- Integration via existing `Client::put()` API
- Configuration via environment variables
- Optional dependencies with conditional compilation
- Total implementation: ~660 LOC

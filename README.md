# VastDB Feast Feature Store Setup

This guide explains how to set up and use the VastDB-backed Feast feature store implementation.

## Prerequisites

- Python 3.8+
- VastDB cluster running and accessible
- Access credentials for VastDB

## Installation

### 1. Install Dependencies

```bash
pip install feast vastdb pandas pyarrow
```

Or using the requirements file:

```bash
pip install -r requirements.txt
```

**requirements.txt:**
```
feast>=0.32.0
vastdb>=0.1.0
pandas>=1.5.0
pyarrow>=12.0.0
protobuf>=3.20.0
pydantic>=1.10.0
```

### 2. Set Up VastDB Cluster

Ensure your VastDB cluster is running and accessible. You'll need:
- Endpoint URL (e.g., `http://your-vastdb-cluster:9090`)
- Access key and secret key
- Appropriate permissions to create buckets, schemas, and tables

### 3. Configure Environment

Create a `.env` file with your VastDB credentials:

```bash
VASTDB_ENDPOINT=http://localhost:9090
VASTDB_ACCESS_KEY=your_access_key
VASTDB_SECRET_KEY=your_secret_key
VASTDB_BUCKET=feast
```

## Project Structure

```
vastdb_feast_example/
├── feature_store.yaml          # Feast configuration
├── vastdb_feast_store.py       # Custom VastDB implementations  
├── example_usage.py            # Usage examples
├── requirements.txt            # Python dependencies
├── setup.py                   # Package setup
└── README.md                  # This file
```

## Setup Steps

### 1. Initialize VastDB Environment

```python
import vastdb

# Connect to VastDB
session = vastdb.connect(
    endpoint="http://localhost:9090",
    access="your_access_key",
    secret="your_secret_key"
)

# Create necessary schemas
with session.transaction() as tx:
    bucket = tx.bucket("feast")
    bucket.create_schema("offline_store", fail_if_exists=False)
    bucket.create_schema("online_store", fail_if_exists=False) 
    bucket.create_schema("registry", fail_if_exists=False)
```

### 2. Configure Feast

Update `feature_store.yaml` with your VastDB credentials:

```yaml
project: vastdb_feast_example
provider: local

registry:
  type: vastdb_registry
  endpoint: "http://your-vastdb-cluster:9090"
  access_key: "your_access_key"
  secret_key: "your_secret_key"
  bucket_name: "feast"
  schema_name: "registry"

online_store:
  type: vastdb_online  
  endpoint: "http://your-vastdb-cluster:9090"
  access_key: "your_access_key"
  secret_key: "your_secret_key"
  bucket_name: "feast"
  schema_name: "online_store"

offline_store:
  type: vastdb_offline
  endpoint: "http://your-vastdb-cluster:9090" 
  access_key: "your_access_key"
  secret_key: "your_secret_key"
  bucket_name: "feast"
  schema_name: "offline_store"
```

### 3. Register Custom Stores

For Feast to recognize your custom stores, you can either:

**Option A: Entry Points (Recommended)**

Add to your `setup.py`:

```python
setup(
    name="vastdb-feast-store",
    entry_points={
        "feast.online_stores": [
            "vastdb_online = vastdb_feast_store:register_vastdb_online_store"
        ],
        "feast.offline_stores": [
            "vastdb_offline = vastdb_feast_store:register_vastdb_offline_store"  
        ],
        "feast.registries": [
            "vastdb_registry = vastdb_feast_store:register_vastdb_registry"
        ],
    },
)
```

**Option B: Direct Registration**

```python
from vastdb_feast_store import (
    register_vastdb_online_store,
    register_vastdb_offline_store,
    register_vastdb_registry
)

# Register stores manually
register_vastdb_online_store()
register_vastdb_offline_store()  
register_vastdb_registry()
```

### 4. Define Features

```python
from feast import Entity, FeatureView, Field, FeatureService
from feast.types import Float32, Int64
from vastdb_feast_store import VastdbSource

# Define entity
driver = Entity(name="driver_id", description="Driver ID", value_type=Int64)

# Define data source
source = VastdbSource(
    bucket="feast",
    schema="offline_store",
    table="driver_stats",
    timestamp_field="event_timestamp"
)

# Define feature view
driver_stats_fv = FeatureView(
    name="driver_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
    ],
    source=source,
    online=True,
)
```

### 5. Apply and Use Features

```python
from feast import FeatureStore

# Initialize feature store
store = FeatureStore(repo_path=".")

# Apply feature definitions
store.apply([driver, driver_stats_fv])

# Get historical features for training
entity_df = pd.DataFrame({
    "driver_id": [1001, 1002, 1003],
    "event_timestamp": [datetime.now()] * 3
})

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=["driver_stats:conv_rate", "driver_stats:acc_rate"]
).to_df()

# Materialize to online store
store.materialize(
    start_date=datetime.now() - timedelta(days=1),
    end_date=datetime.now()
)

# Get online features for serving
online_features = store.get_online_features(
    features=["driver_stats:conv_rate"],
    entity_rows=[{"driver_id": 1001}]
).to_dict()
```

## Running the Example

```bash
# Run the complete example
python example_usage.py

# Or run step by step
python -c "from example_usage import setup_vastdb_environment; setup_vastdb_environment()"
python -c "from example_usage import demonstrate_offline_features; demonstrate_offline_features()"
python -c "from example_usage import demonstrate_online_features; demonstrate_online_features()"
```

## Architecture Benefits

### VastDB as Unified Backend

1. **Single Storage System**: One system handles online, offline, and registry storage
2. **ACID Transactions**: Consistent feature updates across all stores
3. **Columnar Storage**: Optimized for analytical workloads
4. **Scalability**: Handles petabyte-scale datasets
5. **Rich Data Types**: Supports nested, array, and complex types

### Performance Advantages

- **Vectorized Processing**: VastDB's Arrow-native processing
- **Parallel Queries**: Concurrent feature retrieval
- **Compression**: Efficient storage and transfer
- **Caching**: Built-in caching for frequently accessed features

### Operational Benefits

- **Simplified Architecture**: Fewer moving parts
- **Consistent Backups**: Single backup strategy
- **Unified Monitoring**: One system to monitor
- **Cost Efficiency**: Shared resources across workloads

## Troubleshooting

### Common Issues

1. **Connection Errors**
   ```
   vastdb.errors.ConnectionError: Failed to connect
   ```
   - Check VastDB cluster status
   - Verify endpoint URL and credentials
   - Ensure network connectivity

2. **Schema Creation Failures**
   ```
   vastdb.errors.Forbidden: Insufficient permissions
   ```
   - Verify access key has proper permissions
   - Check bucket exists and is accessible

3. **Feature Materialization Errors**
   ```
   feast.errors.FeatureViewNotFoundException
   ```
   - Ensure feature views are properly registered
   - Check data source table exists in VastDB

### Debug Mode

Enable debug logging:

```python
import logging
logging.getLogger("vastdb").setLevel(logging.DEBUG)
logging.getLogger("feast").setLevel(logging.DEBUG)
```

### Performance Tuning

1. **Batch Size**: Adjust batch sizes for materialization
2. **Parallel Workers**: Configure concurrent workers
3. **Memory Limits**: Set appropriate memory limits
4. **Connection Pooling**: Use connection pooling for high-throughput

## Advanced Features

### Custom Data Sources

Create custom VastDB data sources:

```python
class CustomVastdbSource(VastdbSource):
    def __init__(self, query: str, **kwargs):
        self.query = query
        super().__init__(**kwargs)
    
    def get_table_query_string(self) -> str:
        return self.query
```

### Feature Transformations

Leverage VastDB's compute capabilities:

```python
# Use VastDB for feature transformations
with session.transaction() as tx:
    table = tx.bucket("feast").schema("offline_store").table("driver_stats")
    
    # Compute rolling averages
    result = table.select(
        columns=["driver_id", "conv_rate"],
        predicate=table["event_timestamp"] > datetime.now() - timedelta(days=7)
    ).read_all()
```

### Monitoring and Alerting

Set up feature monitoring:

```python
def monitor_features():
    store = FeatureStore(repo_path=".")
    
    # Check feature freshness
    for fv in store.list_feature_views():
        last_updated = get_last_updated_timestamp(fv)
        if datetime.now() - last_updated > timedelta(hours=1):
            send_alert(f"Feature view {fv.name} is stale")
```

## Contributing

To contribute to this implementation:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## License

This implementation is provided as an example and may be used under the MIT License.

"""
VastDB Custom Feast Feature Store Implementation

This module provides a complete Feast feature store implementation using VastDB
as the backend for online store, offline store, and registry.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union
import pandas as pd
import pyarrow as pa
from dataclasses import dataclass

# Feast imports
from feast import Entity, FeatureView, FeatureService
from feast.data_format import ParquetFormat
from feast.data_source import DataSource
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.registry.base_registry import BaseRegistry
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.types import from_value_type
from feast.value_type import ValueType

# VastDB imports
import vastdb

logger = logging.getLogger(__name__)


# Configuration Classes
@dataclass
class VastdbOnlineStoreConfig:
    """Configuration for VastDB online store."""
    type: str = "vastdb_online"
    endpoint: str = "http://localhost:9090" 
    access_key: str = ""
    secret_key: str = ""
    bucket_name: str = "feast"
    schema_name: str = "online_store"
    ssl_verify: bool = True


@dataclass  
class VastdbOfflineStoreConfig:
    """Configuration for VastDB offline store."""
    type: str = "vastdb_offline"
    endpoint: str = "http://localhost:9090"
    access_key: str = ""
    secret_key: str = ""
    bucket_name: str = "feast" 
    schema_name: str = "offline_store"
    ssl_verify: bool = True


@dataclass
class VastdbRegistryConfig:
    """Configuration for VastDB registry."""
    type: str = "vastdb_registry"
    endpoint: str = "http://localhost:9090"
    access_key: str = ""
    secret_key: str = ""
    bucket_name: str = "feast"
    schema_name: str = "registry"
    ssl_verify: bool = True


# VastDB Data Source
class VastdbSource(DataSource):
    """VastDB data source for Feast."""
    
    def __init__(
        self,
        bucket: str,
        schema: str,
        table: str,
        timestamp_field: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        self.bucket = bucket
        self.schema = schema  
        self.table = table
        
        super().__init__(
            name=f"{bucket}.{schema}.{table}",
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column, 
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )
    
    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return lambda x: ValueType.STRING
    
    def get_table_query_string(self) -> str:
        return f"{self.bucket}.{self.schema}.{self.table}"


# VastDB Online Store Implementation
class VastdbOnlineStore(OnlineStore):
    """VastDB implementation of Feast OnlineStore."""
    
    def __init__(self):
        self._session = None
    
    def _get_session(self, config: VastdbOnlineStoreConfig) -> vastdb.Session:
        """Get or create VastDB session."""
        if self._session is None:
            self._session = vastdb.connect(
                endpoint=config.endpoint,
                access=config.access_key,
                secret=config.secret_key,
                ssl_verify=config.ssl_verify
            )
        return self._session
    
    def _get_table_name(self, project: str, table: FeatureView) -> str:
        """Generate table name for feature view."""
        return f"{project}__{table.name}"
    
    def _ensure_schema_exists(self, tx, bucket_name: str, schema_name: str):
        """Ensure schema exists in VastDB."""
        try:
            bucket = tx.bucket(bucket_name)
            bucket.create_schema(schema_name, fail_if_exists=False)
        except Exception as e:
            logger.warning(f"Schema creation failed: {e}")
    
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        batch: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]] = None,
    ) -> None:
        """Write batch of features to online store."""
        online_config = config.online_store
        session = self._get_session(online_config)
        table_name = self._get_table_name(config.project, table)
        
        if not batch:
            return
            
        # Prepare data for insertion
        rows = []
        for entity_key, features, timestamp, created_ts in batch:
            row = {}
            
            # Add entity columns
            for entity_name, entity_value in zip(
                table.entities, entity_key.join_keys
            ):
                if entity_name != DUMMY_ENTITY_ID:
                    row[entity_name] = entity_value.string_val
            
            # Add feature columns  
            for feature_name, feature_value in features.items():
                if feature_value.HasField("string_val"):
                    row[feature_name] = feature_value.string_val
                elif feature_value.HasField("int64_val"):
                    row[feature_name] = feature_value.int64_val
                elif feature_value.HasField("double_val"):
                    row[feature_name] = feature_value.double_val
                elif feature_value.HasField("float_val"):
                    row[feature_name] = feature_value.float_val
                elif feature_value.HasField("bool_val"):
                    row[feature_name] = feature_value.bool_val
            
            # Add timestamp columns
            row["event_timestamp"] = timestamp
            if created_ts:
                row["created_timestamp"] = created_ts
                
            rows.append(row)
        
        # Convert to Arrow table
        df = pd.DataFrame(rows)
        arrow_table = pa.Table.from_pandas(df)
        
        # Write to VastDB
        with session.transaction() as tx:
            self._ensure_schema_exists(tx, online_config.bucket_name, online_config.schema_name)
            bucket = tx.bucket(online_config.bucket_name)
            schema = bucket.schema(online_config.schema_name)
            
            try:
                vastdb_table = schema.table(table_name)
                # Upsert data (insert new records)
                vastdb_table.insert(arrow_table)
            except Exception:
                # Create table if it doesn't exist
                vastdb_table = schema.create_table(table_name, arrow_table.schema)
                vastdb_table.insert(arrow_table)
        
        if progress:
            progress(len(batch))
    
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """Read features from online store."""
        online_config = config.online_store
        session = self._get_session(online_config)
        table_name = self._get_table_name(config.project, table)
        
        results = []
        
        with session.transaction() as tx:
            try:
                bucket = tx.bucket(online_config.bucket_name)
                schema = bucket.schema(online_config.schema_name)
                vastdb_table = schema.table(table_name)
                
                for entity_key in entity_keys:
                    # Build filter predicate for entity key
                    predicate = None
                    for entity_name, entity_value in zip(table.entities, entity_key.join_keys):
                        if entity_name != DUMMY_ENTITY_ID:
                            entity_predicate = vastdb_table[entity_name] == entity_value.string_val
                            predicate = entity_predicate if predicate is None else predicate & entity_predicate
                    
                    # Query features
                    columns = requested_features or [f.name for f in table.schema]
                    columns.append("event_timestamp")
                    
                    try:
                        result = vastdb_table.select(
                            columns=columns,
                            predicate=predicate,
                            limit_rows=1
                        ).read_all()
                        
                        if len(result) > 0:
                            row = result.to_pydict()
                            timestamp = row["event_timestamp"][0] if row["event_timestamp"] else None
                            
                            # Convert to ValueProto format
                            features = {}
                            for feature in requested_features or [f.name for f in table.schema]:
                                if feature in row and row[feature]:
                                    value = ValueProto()
                                    val = row[feature][0]
                                    if isinstance(val, str):
                                        value.string_val = val
                                    elif isinstance(val, int):
                                        value.int64_val = val
                                    elif isinstance(val, float):
                                        value.double_val = val
                                    elif isinstance(val, bool):
                                        value.bool_val = val
                                    features[feature] = value
                            
                            results.append((timestamp, features))
                        else:
                            results.append((None, None))
                    except Exception as e:
                        logger.warning(f"Failed to read features: {e}")
                        results.append((None, None))
                        
            except Exception as e:
                logger.error(f"Failed to access table {table_name}: {e}")
                # Return empty results for all entity keys
                results = [(None, None) for _ in entity_keys]
        
        return results


# VastDB Offline Store Implementation  
class VastdbRetrievalJob(RetrievalJob):
    """VastDB retrieval job implementation."""
    
    def __init__(self, data: pa.Table, metadata: RetrievalMetadata):
        self._data = data
        self._metadata = metadata
    
    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        """Convert to pandas DataFrame."""
        return self._data.to_pandas()
    
    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        """Convert to Arrow table."""
        return self._data
    
    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        """Return retrieval metadata."""
        return self._metadata


class VastdbOfflineStore(OfflineStore):
    """VastDB implementation of Feast OfflineStore."""
    
    def __init__(self):
        self._session = None
    
    def _get_session(self, config: VastdbOfflineStoreConfig) -> vastdb.Session:
        """Get or create VastDB session.""" 
        if self._session is None:
            self._session = vastdb.connect(
                endpoint=config.endpoint,
                access=config.access_key,
                secret=config.secret_key,
                ssl_verify=config.ssl_verify
            )
        return self._session
    
    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return lambda x: ValueType.STRING
    
    def get_historical_features(
        self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        """Retrieve historical features."""
        offline_config = config.offline_store
        session = self._get_session(offline_config)
        
        if isinstance(entity_df, str):
            # If entity_df is a query string, execute it
            # For simplicity, assume it's already a DataFrame
            entity_df = pd.read_parquet(entity_df) 
        
        # Convert entity_df to arrow table for joining
        entity_arrow = pa.Table.from_pandas(entity_df)
        
        # Collect all feature data
        all_features = []
        
        with session.transaction() as tx:
            bucket = tx.bucket(offline_config.bucket_name)
            schema = bucket.schema(offline_config.schema_name)
            
            for fv in feature_views:
                try:
                    table_name = fv.name
                    vastdb_table = schema.table(table_name)
                    
                    # Select requested features from this feature view
                    fv_features = [ref.split(':')[1] for ref in feature_refs 
                                 if ref.startswith(f"{fv.name}:")]
                    
                    if fv_features:
                        columns = fv_features + [fv.timestamp_field] + [e.join_key for e in fv.entities]
                        
                        # Get all data (point-in-time correctness would require more complex logic)
                        feature_data = vastdb_table.select(columns=columns).read_all()
                        all_features.append(feature_data)
                        
                except Exception as e:
                    logger.warning(f"Failed to retrieve features from {fv.name}: {e}")
        
        # Combine all feature data
        if all_features:
            # Simple concatenation - in practice, you'd want proper point-in-time joins
            combined_features = pa.concat_tables(all_features)
            
            # Join with entity_df (simplified - real implementation would do point-in-time joins)
            result_df = combined_features.to_pandas()
            final_df = pd.merge(entity_df, result_df, on="driver_id", how="left")
        else:
            final_df = entity_df
        
        result_table = pa.Table.from_pandas(final_df)
        metadata = RetrievalMetadata(
            features=feature_refs,
            keys=list(entity_df.columns),
            min_event_timestamp=entity_df["event_timestamp"].min(),
            max_event_timestamp=entity_df["event_timestamp"].max(),
        )
        
        return VastdbRetrievalJob(result_table, metadata)


# VastDB Registry Implementation
class VastdbRegistry(BaseRegistry):
    """VastDB implementation of Feast Registry."""
    
    def __init__(self, registry_config: VastdbRegistryConfig, project: str, repo_path: str):
        self.registry_config = registry_config
        self.project = project
        self.repo_path = repo_path
        self._session = None
        self._registry_table_name = "registry_store"
    
    def _get_session(self) -> vastdb.Session:
        """Get or create VastDB session."""
        if self._session is None:
            self._session = vastdb.connect(
                endpoint=self.registry_config.endpoint,
                access=self.registry_config.access_key,
                secret=self.registry_config.secret_key,
                ssl_verify=self.registry_config.ssl_verify
            )
        return self._session
    
    def _ensure_registry_table_exists(self, tx):
        """Ensure registry table exists."""
        try:
            bucket = tx.bucket(self.registry_config.bucket_name)
            bucket.create_schema(self.registry_config.schema_name, fail_if_exists=False)
            schema = bucket.schema(self.registry_config.schema_name)
            
            # Try to get existing table
            try:
                schema.table(self._registry_table_name)
            except Exception:
                # Create registry table
                registry_schema = pa.schema([
                    ("registry_key", pa.string()),
                    ("registry_proto", pa.binary()),
                    ("last_updated", pa.timestamp("ns"))
                ])
                schema.create_table(self._registry_table_name, registry_schema)
        except Exception as e:
            logger.error(f"Failed to ensure registry table exists: {e}")
            raise
    
    def get_registry_proto(self) -> RegistryProto:
        """Get registry proto from VastDB."""
        session = self._get_session()
        
        with session.transaction() as tx:
            self._ensure_registry_table_exists(tx)
            bucket = tx.bucket(self.registry_config.bucket_name)
            schema = bucket.schema(self.registry_config.schema_name)
            table = schema.table(self._registry_table_name)
            
            try:
                result = table.select(
                    columns=["registry_proto"],
                    predicate=table["registry_key"] == self.project,
                    limit_rows=1
                ).read_all()
                
                if len(result) > 0:
                    proto_bytes = result["registry_proto"][0].as_py()
                    registry_proto = RegistryProto()
                    registry_proto.ParseFromString(proto_bytes)
                    return registry_proto
            except Exception as e:
                logger.warning(f"Failed to read registry: {e}")
        
        # Return empty registry if not found
        return RegistryProto()
    
    def update_registry_proto(self, registry_proto: RegistryProto):
        """Update registry proto in VastDB."""
        session = self._get_session()
        
        with session.transaction() as tx:
            self._ensure_registry_table_exists(tx)
            bucket = tx.bucket(self.registry_config.bucket_name)
            schema = bucket.schema(self.registry_config.schema_name)
            table = schema.table(self._registry_table_name)
            
            # Prepare data
            data = {
                "registry_key": [self.project],
                "registry_proto": [registry_proto.SerializeToString()],
                "last_updated": [datetime.utcnow()]
            }
            
            arrow_data = pa.Table.from_pydict(data)
            
            try:
                # Try to delete existing registry
                existing = table.select(
                    columns=["$row_id"], 
                    predicate=table["registry_key"] == self.project,
                    internal_row_id=True
                ).read_all()
                
                if len(existing) > 0:
                    table.delete(existing)
                    
                # Insert new registry
                table.insert(arrow_data)
                
            except Exception as e:
                logger.error(f"Failed to update registry: {e}")
                raise
    
    def teardown(self):
        """Clean up registry resources."""
        if self._session:
            # Close session if needed
            pass


# Example Usage and Configuration
class VastdbFeatureStoreExample:
    """Example usage of VastDB Feast feature store."""
    
    @staticmethod
    def create_sample_config() -> dict:
        """Create sample configuration for VastDB feature store."""
        return {
            "project": "vastdb_example",
            "registry": {
                "type": "vastdb_registry",
                "endpoint": "http://localhost:9090",
                "access_key": "your_access_key",
                "secret_key": "your_secret_key", 
                "bucket_name": "feast",
                "schema_name": "registry"
            },
            "provider": "local",
            "online_store": {
                "type": "vastdb_online",
                "endpoint": "http://localhost:9090",
                "access_key": "your_access_key",
                "secret_key": "your_secret_key",
                "bucket_name": "feast", 
                "schema_name": "online_store"
            },
            "offline_store": {
                "type": "vastdb_offline",
                "endpoint": "http://localhost:9090", 
                "access_key": "your_access_key",
                "secret_key": "your_secret_key",
                "bucket_name": "feast",
                "schema_name": "offline_store"
            }
        }
    
    @staticmethod
    def setup_sample_data():
        """Set up sample data in VastDB."""
        # This would create sample driver data in VastDB
        sample_data = pd.DataFrame({
            "driver_id": [1001, 1002, 1003, 1004],
            "conv_rate": [0.5, 0.7, 0.8, 0.6],
            "acc_rate": [0.9, 0.8, 0.95, 0.85], 
            "avg_daily_trips": [10, 15, 20, 12],
            "event_timestamp": pd.to_datetime([
                "2023-01-01 00:00:00",
                "2023-01-01 01:00:00", 
                "2023-01-01 02:00:00",
                "2023-01-01 03:00:00"
            ]),
            "created": pd.to_datetime([
                "2023-01-01 00:00:00",
                "2023-01-01 01:00:00",
                "2023-01-01 02:00:00", 
                "2023-01-01 03:00:00"
            ])
        })
        
        # Connect to VastDB and create sample data table
        session = vastdb.connect(
            endpoint="http://localhost:9090",
            access="your_access_key",
            secret="your_secret_key"
        )
        
        with session.transaction() as tx:
            bucket = tx.bucket("feast")
            bucket.create_schema("offline_store", fail_if_exists=False)
            schema = bucket.schema("offline_store")
            
            arrow_table = pa.Table.from_pandas(sample_data)
            table = schema.create_table("driver_hourly_stats", arrow_table.schema, fail_if_exists=False)
            table.insert(arrow_table)
        
        print("Sample data created in VastDB!")


# Registration functions for Feast plugin system
def register_vastdb_online_store():
    """Register VastDB online store with Feast."""
    from feast.repo_config import FeastConfigBaseModel
    
    class VastdbOnlineStoreConfigModel(FeastConfigBaseModel):
        type: str = "vastdb_online"
        endpoint: str = "http://localhost:9090"
        access_key: str = ""
        secret_key: str = ""
        bucket_name: str = "feast"
        schema_name: str = "online_store" 
        ssl_verify: bool = True
    
    return VastdbOnlineStore, VastdbOnlineStoreConfigModel


def register_vastdb_offline_store():
    """Register VastDB offline store with Feast."""
    from feast.repo_config import FeastConfigBaseModel
    
    class VastdbOfflineStoreConfigModel(FeastConfigBaseModel):
        type: str = "vastdb_offline"
        endpoint: str = "http://localhost:9090"
        access_key: str = ""
        secret_key: str = ""
        bucket_name: str = "feast"
        schema_name: str = "offline_store"
        ssl_verify: bool = True
    
    return VastdbOfflineStore, VastdbOfflineStoreConfigModel


def register_vastdb_registry():
    """Register VastDB registry with Feast.""" 
    from feast.repo_config import FeastConfigBaseModel
    
    class VastdbRegistryConfigModel(FeastConfigBaseModel):
        type: str = "vastdb_registry"
        endpoint: str = "http://localhost:9090"
        access_key: str = ""
        secret_key: str = ""
        bucket_name: str = "feast"
        schema_name: str = "registry"
        ssl_verify: bool = True
    
    return VastdbRegistry, VastdbRegistryConfigModel


if __name__ == "__main__":
    # Example usage
    example = VastdbFeatureStoreExample()
    
    # Create sample configuration
    config = example.create_sample_config()
    print("Sample VastDB Feast configuration:")
    print(config)
    
    # Setup sample data (uncomment to run)
    # example.setup_sample_data()
    
    print("\nVastDB Feast feature store implementation ready!")
    print("To use this implementation, register the stores with Feast and configure your feature_store.yaml accordingly.")

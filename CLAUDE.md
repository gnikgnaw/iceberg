# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Iceberg is a high-performance table format for huge analytic tables. It brings SQL table reliability and simplicity to big data, enabling engines like Spark, Trino, Flink, Presto, Hive, and Impala to safely work with the same tables concurrently.

**Key characteristics:**
- ACID transactions with snapshot isolation using optimistic concurrency control
- Schema evolution and partition evolution without breaking existing readers
- Time travel queries to access historical table versions
- Multi-engine support with versioned integration modules

## Build Commands

Iceberg uses Gradle with Java 8, 11, or 17.

### Essential Commands

```bash
# Full build with tests
./gradlew build

# Build without tests (faster)
./gradlew build -x test -x integrationTest

# Fix code style for default versions
./gradlew spotlessApply

# Fix code style for all Spark/Hive/Flink versions
./gradlew spotlessApply -DallVersions

# Run tests for a specific module
./gradlew :iceberg-core:test

# Run a single test class
./gradlew :iceberg-core:test --tests org.apache.iceberg.TestClass

# Run a specific test method
./gradlew :iceberg-core:test --tests org.apache.iceberg.TestClass.testMethod
```

### Docker Requirement

Tests require Docker. On macOS with Docker Desktop, create a symbolic link:
```bash
sudo ln -s $HOME/.docker/run/docker.sock /var/run/docker.sock
```

### Multi-Version Building

Iceberg supports multiple versions of Spark, Flink, and Hive. Control which versions to build:

```bash
# Build specific Spark versions
./gradlew build -PsparkVersions=3.3,3.4,3.5

# Build specific Flink versions
./gradlew build -PflinkVersions=1.17,1.18

# Build all versions
./gradlew build -DallVersions
```

## Module Architecture

### Core Library Modules

- **`iceberg-common`**: Utility classes used across modules
- **`iceberg-api`**: Public Iceberg API (interfaces and abstractions)
- **`iceberg-core`**: Core implementation of Iceberg API and Avro support
  - **This is what processing engines should depend on**
- **`iceberg-parquet`**: Optional module for Parquet file format support
- **`iceberg-arrow`**: Optional module for reading Parquet into Arrow memory
- **`iceberg-orc`**: Optional module for ORC file format support
- **`iceberg-hive-metastore`**: Hive metastore Thrift client implementation
- **`iceberg-data`**: Optional module for direct JVM table access

### Engine Integration Modules

- **`iceberg-spark`**: Spark Datasource V2 API implementation
  - Submodules: `spark-3.3`, `spark-3.4`, `spark-3.5` (with Scala version suffix)
  - Use runtime JARs for shaded versions
- **`iceberg-flink`**: Apache Flink integration
  - Submodules: `flink-1.16`, `flink-1.17`, `flink-1.18`
  - Use `iceberg-flink-runtime` for shaded versions
- **`iceberg-mr`**: InputFormat and classes for Apache Hive integration
- **`iceberg-pig`**: Pig LoadFunc API implementation

### Cloud Storage Modules

- **`iceberg-aws`**: AWS S3 integration with S3FileIO
- **`iceberg-azure`**: Azure Blob Storage integration
- **`iceberg-gcp`**: Google Cloud Storage integration
- **`iceberg-aliyun`**: Aliyun OSS integration
- **`iceberg-dell`**: Dell ECS integration

## Key Architecture Concepts

### Table Format Structure

```
Table Metadata (TableMetadata)
  ├── Manifest Lists (per snapshot)
  │   ├── Manifest File 1 (lists data files + statistics)
  │   │   ├── Data File 1 (Parquet/Avro/ORC)
  │   │   └── Data File 2
  │   └── Manifest File 2
  │       ├── Data File 3
  │       └── Data File 4
  └── Delete Files (for row-level deletes)
```

### Core Abstractions

**Table Interface** (`api/src/main/java/org/apache/iceberg/Table.java`):
- Provides schema, partition info, and table properties
- Creates scanners for reading data
- Creates transactions for atomic multi-operation commits

**TableOperations** (`core/src/main/java/org/apache/iceberg/TableOperations.java`):
- Handles metadata file read/write operations
- Implements optimistic concurrency control
- Methods: `current()`, `refresh()`, `commit(base, metadata)`

**Transaction** (`api/src/main/java/org/apache/iceberg/Transaction.java`):
- Groups multiple operations for atomic commit
- Supports: append, rewrite, delete, schema updates, partition evolution
- Example operations: `newAppend()`, `updateSchema()`, `commitTransaction()`

### Table Operations

- **AppendFiles**: Add new data files to table
- **RewriteFiles**: Compact or optimize existing data files
- **DeleteFiles**: Remove data files from table
- **OverwriteFiles**: Atomically replace files in specific partitions
- **ExpireSnapshots**: Clean up old snapshots and orphaned files

### Transaction & Concurrency

Iceberg uses **optimistic concurrency control**:
1. Read current table state when starting operation
2. Prepare changes without immediately applying them
3. On commit, check if table state has changed (compare base metadata)
4. If conflict detected, retry; otherwise commit atomically

**Snapshot Isolation**: All reads see a consistent table snapshot, even as writes occur concurrently.

### Schema Evolution

Supported operations (all backward-compatible):
- Add columns (including nested fields)
- Drop columns
- Rename columns
- Reorder columns
- Promote types (int → long, float → double, decimal scale/precision)

Implemented in: `core/src/main/java/org/apache/iceberg/SchemaUpdate.java`

### Partition Evolution

Tables can change partitioning scheme over time without rewriting data:
- Add/remove partition fields
- Change partition transforms (e.g., daily → hourly)
- Old data remains readable with original partitioning

Managed via: `PartitionSpec` and `UpdatePartitionSpec`

## Engine-Specific Notes

### Flink Integration

Key classes:
- `FlinkCatalog`: Catalog implementation for managing Iceberg tables
- `FlinkSource`: Source for reading from Iceberg tables (batch and streaming)
- `FlinkSink`: Sink for writing to Iceberg tables with exactly-once semantics
- `RowDataToRowConverter`: Converts between Flink RowData and Iceberg Row

CDC Support: Flink can ingest CDC streams (INSERT/UPDATE/DELETE) via `RowDataTaskWriterFactory`

Version-specific code: `flink/v1.17/flink/src/main/java/org/apache/iceberg/flink/`

### Spark Integration

Key classes:
- `SparkCatalog`: Catalog implementation using Spark Catalog API
- `SparkTable`: Table implementation with DataSourceV2 API
- `SparkBatchQueryScan`: Batch read implementation with predicate pushdown
- `SparkMicroBatchStream`: Streaming read implementation

SQL Extensions: `IcebergSparkSessionExtensions` adds Iceberg-specific SQL commands:
- `CALL system.rollback_to_snapshot(...)`
- `CALL system.expire_snapshots(...)`
- `ALTER TABLE ... ADD PARTITION FIELD`
- `MERGE INTO` for efficient upserts

Time Travel:
```sql
SELECT * FROM table TIMESTAMP AS OF '2024-01-01 00:00:00'
SELECT * FROM table VERSION AS OF 12345
```

Version-specific code: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/`

## File Format Support

- **Parquet** (default): Columnar format with efficient compression and encoding
  - `parquet/src/main/java/org/apache/iceberg/parquet/`
- **Avro**: Row-based format, used for metadata serialization
  - `core/src/main/java/org/apache/iceberg/avro/`
- **ORC**: Alternative columnar format
  - `orc/src/main/java/org/apache/iceberg/orc/`

## Common Abbreviations in Code

- `txn`: Transaction
- `op`: Operation
- `expr`: Expression
- `impl`: Implementation
- `mgr`: Manager
- `meta`: Metadata
- `serde`: Serialization/Deserialization
- `spec`: Specification (schema, partition, sort order)

## Testing Practices

- Unit tests live alongside source code in `src/test/java/`
- Integration tests use Docker containers (Hive, Spark, Flink environments)
- Test helpers in `core/src/test/java/org/apache/iceberg/TestHelpers.java`
- Use `@TempDir` for test file isolation

## Code Style

- Follow Palantir Baseline Java conventions
- Use Spotless for automatic formatting
- ErrorProne checks for common mistakes
- Always run `./gradlew spotlessApply` before committing

## Important Implementation Details

### Metadata File Versioning

TableMetadata files are versioned and immutable. Each commit creates a new metadata file. Old metadata files are retained for time travel and can be expired.

### Manifest File Optimization

Manifests can be compacted to improve planning performance:
- Use `RewriteManifests` operation
- Controlled by `commit.manifest.target-size-bytes` property

### Delete Files

Iceberg supports position deletes (row-level) and equality deletes (predicate-based):
- Position deletes: `DeleteFile` with file path + row positions
- Equality deletes: `DeleteFile` with equality field values
- Both types tracked in manifest files

### FileIO Abstraction

All file operations go through `FileIO` interface:
- `HadoopFileIO`: HDFS and local filesystem
- `S3FileIO`: AWS S3 with optimized multipart uploads
- `AzureFileIO`: Azure Blob Storage
- `GcsFileIO`: Google Cloud Storage

Custom FileIO implementations can be plugged in via catalog properties.

## Development Workflow

1. **Make changes** in appropriate module (api, core, or engine integration)
2. **Run tests** for affected module: `./gradlew :module-name:test`
3. **Fix style**: `./gradlew spotlessApply`
4. **Run full build** (if changing core): `./gradlew build -x integrationTest`
5. **Commit** with descriptive message following project conventions

## Resources

- Official documentation: https://iceberg.apache.org/docs/latest/
- Format specification: https://iceberg.apache.org/spec
- Community discussions: dev@iceberg.apache.org
- Slack: https://apache-iceberg.slack.com/

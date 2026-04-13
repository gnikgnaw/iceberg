# Apache Iceberg 核心架构源码分析文档

> 分析版本: Apache Iceberg (main branch)
> 核心源码: `api/src/main/java/org/apache/iceberg/` 、`core/src/main/java/org/apache/iceberg/`

---

## 一、Catalog 体系结构

### 1.1 Catalog 接口层次

```
                          ┌─────────────┐
                          │   Catalog   │  (api 模块)
                          │  (顶层接口)  │
                          └──────┬──────┘
                                 │
                ┌────────────────┼────────────────┐
                │                │                │
   ┌────────────▼──────┐  ┌─────▼──────┐  ┌──────▼─────────┐
   │SupportsNamespaces │  │ViewCatalog │  │ SessionCatalog │
   │ (命名空间管理)     │  │ (视图管理)  │  │ (多租户会话)   │
   └───────────────────┘  └────────────┘  └────────────────┘
                │
     ┌──────────┼───────────────────────────────┐
     │          │                               │
┌────▼───────┐ ┌▼────────────┐           ┌─────▼──────────┐
│HadoopCatalog│ │HiveCatalog │           │  RESTCatalog   │
│(文件系统)    │ │(HMS)       │           │  (REST API)    │
└────────────┘ └─────────────┘           └────────────────┘
                                                │
                                         ┌──────▼──────┐
                                         │NessieCatalog │
                                         │(Git-like版控)│
                                         └─────────────┘
```

### 1.2 Catalog 接口定义

**源码路径**: `api/src/main/java/org/apache/iceberg/catalog/Catalog.java`

`Catalog` 是 Iceberg 最核心的入口接口，定义了表的 CRUD 操作：

| 方法 | 功能 | 说明 |
|------|------|------|
| `listTables(Namespace)` | 列出命名空间下的所有表 | 返回 `List<TableIdentifier>` |
| `createTable(identifier, schema, spec, location, properties)` | 创建表 | 指定 schema、分区规则、存储位置 |
| `loadTable(identifier)` | 加载表 | 返回 `Table` 实例，核心入口 |
| `dropTable(identifier, purge)` | 删除表 | `purge=true` 同时删除数据文件 |
| `renameTable(from, to)` | 重命名表 | 原子操作 |
| `tableExists(identifier)` | 检查表是否存在 | 默认通过 `loadTable` + catch 实现 |
| `registerTable(identifier, metadataFileLocation)` | 注册已有表 | 指向已存在的 metadata 文件 |
| `buildTable(identifier, schema)` | 创建 TableBuilder | 用于流式构建表或事务 |
| `initialize(name, properties)` | 初始化 Catalog | 引擎（Spark/Flink）调用，传入配置 |

**TableBuilder 子接口**: 支持流式创建表或启动事务

```java
interface TableBuilder {
    TableBuilder withPartitionSpec(PartitionSpec spec);
    TableBuilder withSortOrder(SortOrder sortOrder);
    TableBuilder withLocation(String location);
    TableBuilder withProperties(Map<String, String> properties);
    Table create();
    Transaction createTransaction();
    Transaction replaceTransaction();
    Transaction createOrReplaceTransaction();
}
```

### 1.3 SupportsNamespaces 接口

**源码路径**: `api/src/main/java/org/apache/iceberg/catalog/SupportsNamespaces.java`

Namespace 是 Iceberg 的命名空间抽象（类似数据库的 database/schema 概念），支持多级嵌套。

| 方法 | 功能 |
|------|------|
| `createNamespace(namespace, metadata)` | 创建命名空间 |
| `listNamespaces(namespace)` | 列出子命名空间 |
| `loadNamespaceMetadata(namespace)` | 加载命名空间属性 |
| `dropNamespace(namespace)` | 删除命名空间（需为空） |
| `setProperties(namespace, properties)` | 设置属性 |
| `removeProperties(namespace, properties)` | 移除属性 |

### 1.4 Catalog 的核心实现

| 实现类 | 元数据存储 | 适用场景 |
|--------|-----------|---------|
| **HadoopCatalog** | 文件系统（HDFS/S3） | 开发测试，简单场景 |
| **HiveCatalog** | Hive Metastore (Thrift) | Hive 生态兼容 |
| **RESTCatalog** | REST API Server | 生产推荐，多引擎统一管理 |
| **NessieCatalog** | Nessie Server (Git-like) | 需要版本控制的场景 |
| **JdbcCatalog** | 关系型数据库 | 轻量级生产部署 |

#### Catalog 的职责边界

Catalog **只负责管理 metadata pointer**（即 metadata.json 文件的位置），不负责管理数据文件。

```
Catalog 的工作:
  catalog.loadTable("db.orders")
    → 查找 "db.orders" → 返回 metadata.json 的路径
    → 解析 metadata.json → 构建 Table 对象

Catalog 不做的事:
  × 不读写数据文件（由 FileIO 负责）
  × 不管理 manifest 文件（由 Table 操作负责）
  × 不执行查询优化（由引擎负责）
```

### 1.5 Table 接口 —— Catalog 返回的核心对象

**源码路径**: `api/src/main/java/org/apache/iceberg/Table.java`

`Table` 是 Iceberg 中最重要的接口，提供了表的元数据访问和所有操作入口:

```java
public interface Table {
    // ========== 元数据访问 ==========
    String name();                          // 表名
    Schema schema();                        // 当前 Schema
    Map<Integer, Schema> schemas();         // 所有历史 Schema
    PartitionSpec spec();                   // 当前分区规则
    Map<Integer, PartitionSpec> specs();    // 所有历史分区规则
    SortOrder sortOrder();                  // 排序规则
    Map<String, String> properties();       // 表属性
    String location();                      // 存储位置
    FileIO io();                            // 文件读写接口

    // ========== 快照访问 ==========
    Snapshot currentSnapshot();             // 当前快照
    Snapshot snapshot(long snapshotId);     // 按 ID 查快照
    Iterable<Snapshot> snapshots();         // 所有快照
    List<HistoryEntry> history();           // 快照历史

    // ========== 读取操作 ==========
    TableScan newScan();                    // 创建全表扫描
    BatchScan newBatchScan();               // 批量扫描
    IncrementalAppendScan newIncrementalAppendScan();   // 增量扫描
    IncrementalChangelogScan newIncrementalChangelogScan(); // CDC 扫描

    // ========== 写入操作 ==========
    AppendFiles newAppend();                // 追加文件
    AppendFiles newFastAppend();            // 快速追加（不合并 manifest）
    RowDelta newRowDelta();                 // 行级变更（data + delete files）
    OverwriteFiles newOverwrite();          // 按表达式覆盖
    ReplacePartitions newReplacePartitions(); // 动态分区覆盖
    RewriteFiles newRewrite();              // 文件重写/合并
    DeleteFiles newDelete();                // 删除文件

    // ========== Schema 演进 ==========
    UpdateSchema updateSchema();            // Schema 变更
    UpdatePartitionSpec updateSpec();       // 分区演进

    // ========== 快照管理 ==========
    ExpireSnapshots expireSnapshots();      // 过期快照
    ManageSnapshots manageSnapshots();      // 快照管理（回滚等）

    // ========== 事务 ==========
    Transaction newTransaction();           // 多操作原子事务
}
```

---

## 二、快照（Snapshot）体系

### 2.1 元数据层次结构

```
┌─────────────────────────────────────────────────────────────────────────┐
│  metadata.json (TableMetadata)                                          │
│                                                                         │
│  ┌─ format-version: 2                                                   │
│  ├─ table-uuid: "xxx"                                                   │
│  ├─ location: "s3://bucket/db/table"                                    │
│  ├─ schemas: [...]              ← 所有历史 Schema                        │
│  ├─ partition-specs: [...]      ← 所有历史分区规则                        │
│  ├─ sort-orders: [...]          ← 排序规则                               │
│  ├─ properties: {...}           ← 表配置                                 │
│  ├─ current-snapshot-id: 123    ← 指向当前快照                            │
│  ├─ refs: {"main": {snapshot-id: 123, type: "branch"}}                  │
│  │                                                                       │
│  └─ snapshots: [                ← 快照列表                               │
│       ┌────────────────────────────────────────────────────────┐         │
│       │  Snapshot (id=123)                                     │         │
│       │  ├─ sequence-number: 5                                 │         │
│       │  ├─ snapshot-id: 123                                   │         │
│       │  ├─ parent-id: 122                                     │         │
│       │  ├─ timestamp-ms: 1700000000000                        │         │
│       │  ├─ operation: "append" / "overwrite" / "delete"       │         │
│       │  ├─ summary: {added-data-files: "3", ...}              │         │
│       │  │                                                     │         │
│       │  └─ manifest-list: "snap-123-xxx.avro"  ────────────┐  │         │
│       └────────────────────────────────────────────────────────┘│         │
│                                                                 │         │
└─────────────────────────────────────────────────────────────────│─────────┘
                                                                  │
                    ┌─────────────────────────────────────────────▼──────┐
                    │  Manifest List (snap-123-xxx.avro)                  │
                    │                                                     │
                    │  ┌─────────────────────────────────────────────┐   │
                    │  │ ManifestFile                                 │   │
                    │  │ ├─ manifest_path: "xxx-m0.avro"             │   │
                    │  │ ├─ manifest_length: 8192                    │   │
                    │  │ ├─ partition_spec_id: 0                     │   │
                    │  │ ├─ content: 0 (DATA) / 1 (DELETES)         │   │
                    │  │ ├─ sequence_number: 5                       │   │
                    │  │ ├─ added_files_count: 3                     │   │
                    │  │ ├─ existing_files_count: 10                 │   │
                    │  │ ├─ deleted_files_count: 0                   │   │
                    │  │ ├─ added_rows_count: 50000                  │   │
                    │  │ └─ partitions: [                            │   │
                    │  │      {contains_null, lower_bound, upper_bound}│   │
                    │  │    ]                   ← 分区级统计           │   │
                    │  └─────────────────────────────────────────────┘   │
                    │  ┌─────────────────────────────────────────────┐   │
                    │  │ ManifestFile (delete manifest)              │   │
                    │  │ ├─ content: 1 (DELETES)                    │   │
                    │  │ └─ ...                                      │   │
                    │  └─────────────────────────────────────────────┘   │
                    └────────────────────────┬──────────────────────────┘
                                             │
                    ┌────────────────────────▼──────────────────────────┐
                    │  Manifest File (xxx-m0.avro)                      │
                    │                                                    │
                    │  ┌───────────────────────────────────────────┐    │
                    │  │ ManifestEntry                              │    │
                    │  │ ├─ status: ADDED / EXISTING / DELETED     │    │
                    │  │ ├─ data_sequence_number: 5                │    │
                    │  │ └─ data_file:                             │    │
                    │  │    ├─ file_path: "data/date=2024-01/...parquet"│
                    │  │    ├─ file_format: PARQUET                │    │
                    │  │    ├─ partition: {date: "2024-01-01"}     │    │
                    │  │    ├─ record_count: 50000                 │    │
                    │  │    ├─ file_size_in_bytes: 12345678        │    │
                    │  │    ├─ column_sizes: {1: 4096, 2: 8192}   │    │
                    │  │    ├─ value_counts: {1: 50000, 2: 50000} │    │
                    │  │    ├─ null_value_counts: {1: 0, 2: 100}  │    │
                    │  │    ├─ lower_bounds: {1: 0, 2: "aaa"}     │    │
                    │  │    └─ upper_bounds: {1: 999, 2: "zzz"}   │    │
                    │  └───────────────────────────────────────────┘    │
                    └──────────────────────────────────────────────────┘
```

### 2.2 Snapshot 接口

**源码路径**: `api/src/main/java/org/apache/iceberg/Snapshot.java`

```java
public interface Snapshot extends Serializable {
    long sequenceNumber();                  // 全局递增序列号（V2+）
    long snapshotId();                      // 快照唯一 ID
    Long parentId();                        // 父快照 ID（形成链表）
    long timestampMillis();                 // 提交时间戳
    String operation();                     // 操作类型: append/overwrite/replace/delete
    Map<String, String> summary();          // 摘要信息（文件数、行数等）
    String manifestListLocation();          // Manifest List 文件路径

    // Manifest 访问
    List<ManifestFile> allManifests(FileIO io);    // 所有 manifest（data + delete）
    List<ManifestFile> dataManifests(FileIO io);   // 仅 data manifest
    List<ManifestFile> deleteManifests(FileIO io); // 仅 delete manifest

    // 增量变化
    Iterable<DataFile> addedDataFiles(FileIO io);      // 本快照新增的 data files
    Iterable<DataFile> removedDataFiles(FileIO io);     // 本快照移除的 data files
    Iterable<DeleteFile> addedDeleteFiles(FileIO io);   // 本快照新增的 delete files
    Iterable<DeleteFile> removedDeleteFiles(FileIO io); // 本快照移除的 delete files
}
```

### 2.3 Sequence Number 的核心作用

Sequence Number 是 Iceberg V2 引入的关键概念，是 **MOR 正确性的基石**:

```
Snapshot 1 (seq=1): 写入 DataFile A
Snapshot 2 (seq=2): 写入 EqualityDeleteFile X (删除 id=100 的行)
Snapshot 3 (seq=3): 写入 DataFile B (也包含 id=100 的行)
```

读取时的规则: **Delete File 只能删除 sequence number <= 自身的 Data File 中的行**

所以 DeleteFile X (seq=2) 能删除 DataFile A (seq=1) 中的 id=100，但 **不会删除** DataFile B (seq=3) 中的 id=100。

这个设计保证了:
1. 后续追加的同 key 数据不会被之前的 delete 误删
2. 流式写入中 checkpoint 间的数据语义正确

### 2.4 快照操作类型（DataOperations）

| 操作 | 说明 | 对应 API |
|------|------|----------|
| `append` | 纯追加数据文件 | `table.newAppend()` |
| `replace` | 替换数据文件（compaction） | `table.newRewrite()` |
| `overwrite` | 按条件覆盖数据 | `table.newOverwrite()` |
| `delete` | 删除数据文件 | `table.newDelete()` |

---

## 三、读取剪枝优化

Iceberg 的读取优化是三级剪枝（Pruning）体系，在打开任何数据文件之前层层过滤:

```
                      查询: SELECT * FROM orders WHERE date = '2024-01-15' AND amount > 100

                      Level 1: Manifest 剪枝
                      ┌──────────────────────────┐
                      │  Manifest List (10个)      │
                      │  根据 partition_summaries  │
                      │  过滤掉不匹配的 manifest   │
                      │  → 剩余 2 个 manifest      │
                      └───────────┬──────────────┘
                                  │
                      Level 2: Data File 剪枝
                      ┌───────────▼──────────────┐
                      │  Manifest File (含100个文件)│
                      │  根据 partition value      │
                      │  + column min/max bounds   │
                      │  过滤掉不匹配的文件         │
                      │  → 剩余 5 个文件            │
                      └───────────┬──────────────┘
                                  │
                      Level 3: Row Group 剪枝
                      ┌───────────▼──────────────┐
                      │  Parquet File              │
                      │  根据 Row Group 的          │
                      │  column statistics         │
                      │  跳过不匹配的 Row Group     │
                      │  → 只读必要的数据块          │
                      └──────────────────────────┘
```

### 3.1 Level 1: Manifest 级别剪枝

**核心类**: `ManifestEvaluator`

**源码路径**: `ManifestGroup.java:250-280`

```java
LoadingCache<Integer, ManifestEvaluator> evalCache =
    Caffeine.newBuilder()
        .build(
            specId -> {
                PartitionSpec spec = specsById.get(specId);
                return ManifestEvaluator.forPartitionFilter(
                    Expressions.and(
                        partitionFilter,
                        Projections.inclusive(spec, caseSensitive).project(dataFilter)),
                    spec,
                    caseSensitive);
            });

// 过滤 manifest：利用 manifest 级别的 partition_summaries（lower_bound/upper_bound）
CloseableIterable<ManifestFile> matchingManifests =
    CloseableIterable.filter(
        scanMetrics.skippedDataManifests(),
        closeableDataManifests,
        manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));
```

**ManifestFile 中用于剪枝的字段**:

| 字段 | 类型 | 剪枝作用 |
|------|------|---------|
| `partition_summaries.lower_bound` | ByteBuffer | 分区值下界 |
| `partition_summaries.upper_bound` | ByteBuffer | 分区值上界 |
| `partition_summaries.contains_null` | boolean | 是否包含 null 分区值 |
| `added_files_count` / `existing_files_count` | int | 快速跳过空 manifest |

**原理**: 将用户的 SQL 谓词（如 `date = '2024-01-15'`）投影到分区字段上，利用 manifest 的分区统计信息判断该 manifest 是否可能包含匹配数据。如果 manifest 的 partition lower_bound > '2024-01-15' 或 upper_bound < '2024-01-15'，整个 manifest 直接跳过。

### 3.2 Level 2: Data File 级别剪枝

**源码路径**: `ManifestGroup.java:307-362`

读取每个 manifest 文件的内容（ManifestEntry），利用每个 DataFile 的统计信息进行过滤:

```java
ManifestReader<DataFile> reader =
    ManifestFiles.read(manifest, io, specsById)
        .filterRows(dataFilter)          // 利用列级 min/max 过滤
        .filterPartitions(partitionFilter) // 分区值精确过滤
        .caseSensitive(caseSensitive)
        .select(columns)                 // 列裁剪
        .scanMetrics(scanMetrics);
```

**DataFile 中用于剪枝的字段**:

| 字段 | 类型 | 剪枝作用 |
|------|------|---------|
| `partition` | StructLike | 精确分区值，用于分区过滤 |
| `lower_bounds` | `Map<Integer, ByteBuffer>` | 每列的最小值 |
| `upper_bounds` | `Map<Integer, ByteBuffer>` | 每列的最大值 |
| `null_value_counts` | `Map<Integer, Long>` | 每列 null 值数量 |
| `value_counts` | `Map<Integer, Long>` | 每列值数量 |
| `column_sizes` | `Map<Integer, Long>` | 每列数据大小 |
| `record_count` | long | 文件行数 |

**剪枝示例**：查询 `amount > 100`
- DataFile A: `upper_bounds.amount = 50` → **跳过**（最大值都不满足条件）
- DataFile B: `lower_bounds.amount = 200` → **保留**
- DataFile C: `lower_bounds.amount = 30, upper_bounds.amount = 500` → **保留**（可能包含匹配行）

### 3.3 Level 3: 列裁剪（Column Projection）

在 `DataTableScan.doPlanFiles()` 中:

```java
manifestGroup.select(scanColumns())  // 只选择查询需要的列
```

Iceberg 利用 Parquet/ORC 的列式存储特性，只读取查询涉及的列，极大减少 I/O：
- **Parquet**: 直接跳过不需要的 Column Chunk
- **ORC**: 跳过不需要的 Stream

### 3.4 Level 4: Residual 过滤

分区剪枝只能过滤掉不匹配的分区，但同一分区内的文件可能仍包含不满足条件的行。Iceberg 计算 **Residual Expression**（剩余表达式）——即分区剪枝之后剩下的条件——传递给引擎做行级过滤:

```java
ResidualEvaluator.of(spec, filter, caseSensitive)
```

例如: `WHERE date = '2024-01-15' AND amount > 100`
- 分区列: `date` → 分区剪枝处理
- Residual: `amount > 100` → 传给引擎在读取后过滤

### 3.5 完整的 Scan 计划流程

**源码路径**: `DataTableScan.java:64-92`

```java
public CloseableIterable<FileScanTask> doPlanFiles() {
    Snapshot snapshot = snapshot();
    FileIO io = table().io();

    List<ManifestFile> dataManifests = snapshot.dataManifests(io);    // 获取 data manifest
    List<ManifestFile> deleteManifests = snapshot.deleteManifests(io); // 获取 delete manifest

    ManifestGroup manifestGroup =
        new ManifestGroup(io, dataManifests, deleteManifests)
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())              // 列裁剪
            .filterData(filter())               // 数据过滤（包含分区 + 列级统计剪枝）
            .schemasById(schemas())
            .specsById(specs())
            .scanMetrics(scanMetrics())
            .ignoreDeleted()                    // 忽略已标记删除的 entry
            .columnsToKeepStats(columnsToKeepStats());

    if (shouldPlanWithExecutor() && (dataManifests.size() > 1 || deleteManifests.size() > 1)) {
        manifestGroup = manifestGroup.planWith(planExecutor());  // 并行规划
    }

    return manifestGroup.planFiles();  // 返回 FileScanTask 列表
}
```

返回的 `FileScanTask` 包含:
- `DataFile`: 要读取的数据文件
- `DeleteFile[]`: 关联的 delete 文件（需要在读取时应用）
- `residual`: 残余表达式（行级过滤条件）

---

## 四、MOR（Merge-On-Read）实现

### 4.1 Delete File 的两种类型

```
┌──────────────────────────────────────────────────────────────────┐
│                     Delete File 类型                              │
│                                                                   │
│  ┌───────────────────────────┐  ┌──────────────────────────────┐ │
│  │  Position Delete          │  │  Equality Delete             │ │
│  │                           │  │                              │ │
│  │  内容: (file_path, pos)   │  │  内容: (col1, col2, ...)     │ │
│  │  含义: 删除指定文件的       │  │  含义: 删除所有匹配该组值     │ │
│  │       指定行号             │  │       的行                   │ │
│  │  复杂度: O(1) 精确定位     │  │  复杂度: O(N) 需扫描比对     │ │
│  │  适用: 同 snapshot 内的    │  │  适用: 跨 snapshot 的删除     │ │
│  │       自我去重             │  │       和 CDC/UPSERT 场景     │ │
│  │                           │  │                              │ │
│  │  例:                      │  │  例:                         │ │
│  │  ("file1.parquet", 42)    │  │  (id=100)                   │ │
│  │  ("file1.parquet", 108)   │  │  (id=200)                   │ │
│  └───────────────────────────┘  └──────────────────────────────┘ │
│                                                                   │
│  ┌───────────────────────────┐                                    │
│  │  Deletion Vector (V3+)    │                                    │
│  │                           │                                    │
│  │  内容: 紧凑位图 (bitmap)   │                                    │
│  │  含义: 每个 data file 一个 │                                    │
│  │       DV，标记删除的行号   │                                    │
│  │  优势: 比 pos-delete 更    │                                    │
│  │       紧凑，减少文件数      │                                    │
│  └───────────────────────────┘                                    │
└──────────────────────────────────────────────────────────────────┘
```

### 4.2 DeleteFileIndex —— MOR 的核心调度器

**源码路径**: `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java`

`DeleteFileIndex` 是 MOR 读取的核心，负责为每个 DataFile 找到需要应用的 DeleteFile 集合。

```java
class DeleteFileIndex {
    private final EqualityDeletes globalDeletes;            // 全局 equality delete
    private final PartitionMap<EqualityDeletes> eqDeletesByPartition;  // 按分区的 eq delete
    private final PartitionMap<PositionDeletes> posDeletesByPartition; // 按分区的 pos delete
    private final Map<String, PositionDeletes> posDeletesByPath;       // 按文件路径的 pos delete
    private final Map<String, DeleteFile> dvByPath;         // 按文件路径的 DV
}
```

#### 查找 Delete File 的匹配逻辑

**源码路径**: `DeleteFileIndex.java:151-168`

```java
DeleteFile[] forDataFile(long sequenceNumber, DataFile file) {
    if (isEmpty) return EMPTY_DELETES;

    DeleteFile[] global = findGlobalDeletes(sequenceNumber, file);       // 全局 eq delete
    DeleteFile[] eqPartition = findEqPartitionDeletes(sequenceNumber, file); // 分区 eq delete
    DeleteFile dv = findDV(sequenceNumber, file);                        // DV
    if (dv != null && global == null && eqPartition == null) {
        return new DeleteFile[] {dv};
    } else if (dv != null) {
        return concat(global, eqPartition, new DeleteFile[] {dv});
    } else {
        DeleteFile[] posPartition = findPosPartitionDeletes(sequenceNumber, file); // 分区 pos delete
        DeleteFile[] posPath = findPathDeletes(sequenceNumber, file);              // 路径 pos delete
        return concat(global, eqPartition, posPartition, posPath);
    }
}
```

**匹配规则总结**:

| Delete 类型 | 匹配维度 | 序列号规则 |
|------------|---------|-----------|
| Global Equality Delete | 全表（未分区的 eq-delete） | delete.seq > dataFile.seq |
| Partition Equality Delete | 同分区 | delete.seq > dataFile.seq |
| Partition Position Delete | 同分区 | delete.seq > dataFile.seq |
| Path Position Delete | 同文件路径 | delete.seq > dataFile.seq |
| Deletion Vector | 同文件路径 | dv.seq >= dataFile.seq |

#### Equality Delete 的列级统计剪枝

**源码路径**: `DeleteFileIndex.java:219-239`

```java
private static boolean canContainEqDeletesForFile(DataFile dataFile, EqualityDeleteFile deleteFile) {
    Map<Integer, ByteBuffer> dataLowers = dataFile.lowerBounds();
    Map<Integer, ByteBuffer> dataUppers = dataFile.upperBounds();

    // 对每个 equality 字段，比较 data file 和 delete file 的值域是否有交集
    for (Types.NestedField field : deleteFile.equalityFields()) {
        // 如果 data file 的某列值域与 delete file 完全不重叠，可以跳过
        // 例: dataFile.amount 范围 [100,200], deleteFile.amount 范围 [300,400] → 无交集，跳过
    }
}
```

这是一个非常精妙的优化：通过比较 DataFile 和 EqualityDeleteFile 的列级统计信息（min/max），在不读取文件内容的情况下判断 delete file 是否可能影响 data file。

### 4.3 MOR 读取的完整流程

```
Step 1: Scan Planning (ManifestGroup.planFiles)
   │
   ├── 读取 data manifests → 获取 DataFile 列表（经过剪枝）
   ├── 读取 delete manifests → 构建 DeleteFileIndex
   └── 为每个 DataFile 匹配对应的 DeleteFile[] → 生成 FileScanTask
           │
Step 2: 引擎执行 FileScanTask
   │
   ├── 读取 DataFile 的数据行
   │
   ├── 应用 Position Delete:
   │   │   读取 pos-delete 文件获取 (file_path, row_position) 列表
   │   │   按 row_position 排序
   │   └── 在读取 data file 时，跳过被标记删除的行号
   │
   ├── 应用 Equality Delete:
   │   │   读取 eq-delete 文件获取要删除的键值集合
   │   │   对 data file 每一行:
   │   └── 如果 row 的 equality 字段匹配 delete 集合中的某个值 → 过滤掉
   │
   └── 应用 Deletion Vector:
       │   读取 DV 位图
       └── 检查每行的行号是否在位图中被标记 → 标记的行跳过
```

### 4.4 FileScanTask 的构建

**源码路径**: `ManifestGroup.java:364-376`

```java
private static CloseableIterable<FileScanTask> createFileScanTasks(
    CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext ctx) {
    return CloseableIterable.transform(
        entries,
        entry -> {
            DataFile dataFile = ContentFileUtil.copy(entry.file(), ...);
            DeleteFile[] deleteFiles = ctx.deletes().forEntry(entry);  // 从 index 查找匹配的 delete files
            return new BaseFileScanTask(
                dataFile, deleteFiles, ctx.schemaAsString(), ctx.specAsString(), ctx.residuals());
        });
}
```

每个 `FileScanTask` 都包含:
- 一个 `DataFile`
- 零到多个 `DeleteFile[]`（MOR 需要应用的 delete 文件）
- Residual 表达式

### 4.5 为什么 MOR 比 COW 更适合流式写入

| 特性 | Copy-On-Write (COW) | Merge-On-Read (MOR) |
|------|---------------------|---------------------|
| 写入操作 | 重写整个受影响的 data file | 只追加 delete file |
| 写入延迟 | 高（需要读取+重写旧文件） | 低（只写小型 delete 文件） |
| 读取代价 | 低（数据已合并） | 中等（需要 merge delete） |
| 适用场景 | 读多写少，批处理 | 写多读少，流式写入 |
| Iceberg 支持 | 通过 RewriteFiles | 通过 RowDelta (append + delete files) |

**Flink Sink 使用 MOR**: 流式场景下 UPSERT 频繁，MOR 的写入代价远低于 COW。读取时通过 DeleteFileIndex 高效匹配，结合列级统计剪枝，读取开销可控。后续可以通过 Compaction 将 data files + delete files 合并成新的 data files。

---

## 五、面试口述回答总结

### 话题一：面试官问"Iceberg 的 Catalog 是做什么的，和 Hive Metastore 有什么区别"

Iceberg 的 Catalog 本质上是一个 **metadata pointer 的注册中心**，它的核心职责就一件事：给定表名，返回当前有效的 metadata.json 文件的位置。

和 Hive Metastore 的核心区别在于 **元数据管理的粒度和位置**:
- Hive Metastore 自己维护 Schema、分区列表、文件列表等全部元信息，元数据的 truth 在 Metastore 里
- Iceberg Catalog 只存一个指针（metadata location），真正的元数据（Schema、快照链、Manifest 列表）全部在自描述的 metadata.json 文件中

这意味着 Iceberg 的表元数据是 **自包含的**——你拿到 metadata.json，不需要任何 Catalog 就能完整理解这张表。这也是为什么 Iceberg 支持 registerTable() 接口，可以把一个已存在的表"注册"到任意 Catalog 中。

HiveCatalog 是 Iceberg Catalog 的一个实现，它底层用 Hive Metastore 存表名到 metadata location 的映射，但真正的 Schema 演进、快照管理都是 Iceberg 自己做的。

---

### 话题二：面试官问"Iceberg 读数据时做了哪些优化，为什么比 Hive 快"

Iceberg 读取的核心优势是 **三级剪枝**，在打开任何数据文件之前就能排除大量无关数据:

**第一级: Manifest 文件级剪枝**。每个 Manifest File 存储了分区值的 lower_bound/upper_bound 统计信息。查询 `date = '2024-01-15'` 时，如果一个 manifest 的 date 上界是 `2024-01-10`，整个 manifest（可能包含数百个文件）直接跳过。Hive 没有这层，它需要列出目录下所有文件。

**第二级: Data File 级剪枝**。每个 Data File 的 manifest entry 记录了每列的 min/max 值、null 计数等统计信息。查询 `amount > 1000` 时，如果文件的 `amount.upper_bound = 500`，这个文件直接跳过。Hive 要到 Parquet Footer 级别才能做类似的事，但 Iceberg 在 manifest 读取阶段就完成了，不需要读取 Parquet 文件本身。

**第三级: 列裁剪 + Residual**。只读取查询涉及的列，并将分区剪枝后的残余条件传给引擎做行级过滤。

另外一个关键优势是 **不需要 list files**。Hive 分区表的 scan planning 需要对每个分区目录做 HDFS listStatus，在万级分区时可能需要数分钟。Iceberg 把所有文件信息直接记录在 manifest 中，scan planning 只需要读几个 Avro 文件。

---

### 话题三：面试官问"Equality Delete 和 Position Delete 有什么区别，什么时候用哪个"

**Position Delete** 记录的是 `(file_path, row_position)` 二元组，意思是"删除 file1.parquet 的第 42 行"。它是精确定位，O(1) 就能应用，但它 **必须知道要删除的行在哪个文件的哪个位置**。

**Equality Delete** 记录的是一组列值，意思是"删除所有 id=100 的行"。它不需要知道目标行的物理位置，但应用时需要 O(N) 扫描 data file，把每行的 equality 字段和 delete 集合比对。

**使用场景**:

Position Delete 适合 **已知位置** 的场景。比如 Flink Sink 在同一个 checkpoint 内先 INSERT 了一行到 file1 的第 10 行，后来对同一个 key 又 INSERT 了一条，此时系统知道旧数据在 file1:10，直接写 pos-delete。

Equality Delete 适合 **不知道位置** 的场景。比如 Flink 收到一条 DELETE 消息要删除 id=100，但这条数据可能在之前的某个 checkpoint 写入的文件中，我不知道在哪，只能写一条 eq-delete(id=100)，让读取时去匹配。

Iceberg 的 `BaseEqualityDeltaWriter` 做了一个很聪明的优化: 它内部维护了一个 `insertedRowMap`，追踪当前 checkpoint 内已插入的行的位置。当需要删除一个 key 时，如果这个 key 在当前 checkpoint 内刚插入过，就用 position delete（更高效）；如果找不到（说明是之前 checkpoint 的数据），才 fallback 到 equality delete。

---

### 话题四：面试官问"Iceberg 的快照和时间旅行是怎么实现的"

Iceberg 的时间旅行本质上是 **快照链 + 不可变文件** 的设计。

每次对表的修改（写入、删除、compaction）都会生成一个新的 Snapshot，包含一个全新的 Manifest List。Snapshot 通过 parentId 形成一条链表，记录了表的完整演进历史。

关键的不可变性保证: 数据文件和 manifest 文件一旦写入就永远不会被修改，只会被新的 snapshot 引用或不引用。一个 Snapshot 的 Manifest List 引用了这个时刻表包含的所有文件，所以你只要拿到任意一个 Snapshot，就能读到那个时间点的完整数据。

时间旅行的实现就是: `SELECT * FROM table VERSION AS OF 12345` → 引擎拿 snapshot-id=12345 去 scan，而不是用 current-snapshot-id。

过期快照（ExpireSnapshots）才会真正删除不再被任何快照引用的数据文件。在此之前所有历史快照都是可读的。

---

### 话题五：面试官问"Iceberg 的 MOR 和 HBase 的 MOR 有什么区别"

核心区别在于 **merge 的时机和粒度**。

HBase 的 MOR 是 **LSM-Tree 模型**: MemTable flush 到 HFile，多个 HFile 通过 Minor/Major Compaction 合并。读取时需要 merge 多个 HFile + MemTable 的数据，按 RowKey 排序合并。这是行级的、实时的、持续进行的 merge。

Iceberg 的 MOR 是 **文件级的 overlay 模型**: 数据写入 DataFile，删除写入 DeleteFile（eq-delete 或 pos-delete），读取时根据 sequence number 判断 delete 是否生效。不需要对数据排序，不需要 LSM 的多层结构。

另一个关键区别: Iceberg 有 **列级统计信息加持**。当 DeleteFileIndex 匹配 eq-delete 和 data file 时，会利用两者的 min/max bounds 判断是否有交集，大量不相关的 delete 文件在 planning 阶段就被跳过了。HBase 的 merge 必须实际读取数据才能判断。

Compaction 也不同: HBase 的 compaction 是必须的，否则读性能会持续劣化。Iceberg 的 compaction 是可选的优化手段，不做 compaction 数据也是正确的，只是读性能可能受 delete file 数量影响。

---

### 话题六：面试官问"和 Kafka Sink 的两阶段提交有什么区别"

这里要区分两个层面的"两阶段提交"。

**Kafka Sink 的两阶段提交** 依赖 Kafka 原生的事务机制——`beginTransaction()`、`commitTransaction()`、`abortTransaction()`。数据在事务提交前对消费者不可见（通过 `isolation.level=read_committed` 控制）。它的"第一阶段"是 pre-commit（把数据写入 Kafka 但标记为 uncommitted），"第二阶段"是 commit/abort。

**Iceberg Sink 的两阶段提交** 没有外部事务系统可用。它利用的是 Iceberg 的 **乐观并发控制 + 原子的 metadata 文件替换**:
- 第一阶段（snapshotState）: 将 WriteResult 序列化为临时 manifest 文件，写入 Flink State Backend。此时数据文件已经在存储上了，但 Iceberg 表的 metadata.json 还没更新，所以对读者不可见。
- 第二阶段（notifyCheckpointComplete）: 调用 Iceberg 的 `table.newAppend().commit()` 或 `table.newRowDelta().commit()`，Iceberg 底层通过 CAS（比较并交换）原子更新 metadata.json 指向新的 snapshot，此时数据对读者可见。

本质区别是: Kafka 的可见性由 Kafka Broker 的事务协调器控制，Iceberg 的可见性由 metadata.json 的原子更新控制。Kafka commit 失败可以 abort（Broker 丢弃未提交数据），Iceberg commit 失败只需重试（数据文件已经在存储上，只是 metadata 还没指向它们）。

所以 Iceberg Sink 故障恢复时不需要"回滚"数据文件，只需要重新提交 metadata 即可。这也是为什么 Flink Committer 的恢复逻辑是: 从 State 中找到未提交的 checkpoint → 查 Iceberg snapshot 确认是否已提交 → 未提交的重新 commit。

---

### 话题七：面试官问"Iceberg Compaction 是怎么工作的，什么时候需要做"

Compaction 在 Iceberg 中有两种主要形式:

**Data File Compaction (RewriteDataFiles)**: 把多个小 data file 合并成大文件，同时将 delete file 中标记删除的行物理移除。触发场景:
1. 流式写入产生大量小文件（每个 checkpoint 一批文件）
2. 存在大量 equality/position delete 文件导致读性能下降
3. 分区演进后旧分区需要按新规则重写

**Manifest Compaction (RewriteManifests)**: 把多个小 manifest 合并成大的。流式写入每次 commit 都产生新 manifest，长期运行后 manifest 数量膨胀会拖慢 scan planning 阶段。

Compaction 的核心是 `table.newRewrite()` 操作，它在一个原子事务中:
1. 添加新的合并后的 data file
2. 删除旧的小 data file 和已应用的 delete file
3. 生成新的 snapshot

**什么时候做**：一般配置定时任务（如 Spark 定时作业），监控指标包括:
- 小文件数量（< target-file-size 的文件比例）
- Delete file 数量（影响读取性能）
- Manifest file 数量（影响 planning 速度）

做完 Compaction 后，旧快照的文件不会立即删除，需要 `ExpireSnapshots` 来清理不再被任何快照引用的孤儿文件。

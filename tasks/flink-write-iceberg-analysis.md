# Flink 写入 Iceberg 源码分析文档

> 分析版本: Flink v1.20 集成模块
> 源码路径: `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/`
> 核心模块: `core/src/main/java/org/apache/iceberg/io/`

---

## 一、整体架构概览

### 1.1 算子拓扑结构

Flink 写入 Iceberg 的流式拓扑由三层算子组成：

```
                          ┌─────────────────────────┐
                          │   Input DataStream       │
                          │   (RowData)              │
                          └────────────┬─────────────┘
                                       │
                          ┌────────────▼─────────────┐
                          │   Distribution Layer      │
                          │   (keyBy / range / none)  │
                          └────────────┬─────────────┘
                                       │
              ┌────────────────────────┼─────────────────────────┐
              │                        │                         │
   ┌──────────▼──────────┐  ┌─────────▼──────────┐  ┌──────────▼──────────┐
   │ IcebergStreamWriter  │  │ IcebergStreamWriter │  │ IcebergStreamWriter  │
   │ (并行度=N)           │  │                     │  │                      │
   └──────────┬──────────┘  └─────────┬──────────┘  └──────────┬──────────┘
              │                        │                         │
              └────────────────────────┼─────────────────────────┘
                                       │ FlinkWriteResult
                          ┌────────────▼─────────────┐
                          │ IcebergFilesCommitter     │
                          │ (并行度=1, 单点提交)       │
                          └────────────┬─────────────┘
                                       │ Void
                          ┌────────────▼─────────────┐
                          │ DiscardingSink            │
                          │ (丢弃输出)                 │
                          └──────────────────────────┘
```

### 1.2 核心类关系

```
FlinkSink.Builder                     -- 构建入口，组装整个拓扑
  ├── IcebergStreamWriter<RowData>    -- 写入算子，每个并行实例持有一个 TaskWriter
  │     └── RowDataTaskWriterFactory  -- 根据配置创建不同类型的 TaskWriter
  │           ├── UnpartitionedWriter       -- Append模式 + 非分区表
  │           ├── RowDataPartitionedFanoutWriter -- Append模式 + 分区表
  │           ├── UnpartitionedDeltaWriter  -- Upsert/CDC模式 + 非分区表
  │           └── PartitionedDeltaWriter    -- Upsert/CDC模式 + 分区表
  └── IcebergFilesCommitter           -- 提交算子，负责将文件元数据提交到 Iceberg Table
```

**关键源码入口**: `FlinkSink.java:427` `chainIcebergOperators()` 方法

---

## 二、写入模式详细分析

### 2.1 APPEND 模式（纯追加）

**触发条件**: `equalityFieldIds` 为空（未设置 equality 字段或 identifier 字段）

**使用的 Writer**:
- 非分区表 → `UnpartitionedWriter` (`core` 模块)
- 分区表 → `RowDataPartitionedFanoutWriter` (Flink 模块内部类)

**源码路径**: `RowDataTaskWriterFactory.java:193-213`

```java
if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
    // Initialize a task writer to write INSERT only.
    if (spec.isUnpartitioned()) {
        return new UnpartitionedWriter<>(...);
    } else {
        return new RowDataPartitionedFanoutWriter(...);
    }
}
```

**处理逻辑**:
Append 模式只处理 **INSERT** 类型的记录。数据直接写入 Data File，不生成 Delete File。

**分区表的路由**: `RowDataPartitionedFanoutWriter` 继承自 `PartitionedFanoutWriter`，通过 `partition()` 方法将 RowData 转为 PartitionKey，内部维护一个 `Map<PartitionKey, RollingFileWriter>`，每个分区一个独立的 writer。

```
[RowData] → RowDataWrapper.wrap() → PartitionKey.partition() → 路由到对应分区的 RollingFileWriter
```

### 2.2 UPSERT 模式

**触发条件**: 设置了 `equalityFieldColumns` 且启用了 `upsert=true`

**使用的 Writer**:
- 非分区表 → `UnpartitionedDeltaWriter`
- 分区表 → `PartitionedDeltaWriter`

两者都继承自 `BaseDeltaTaskWriter`，核心写入逻辑在 `BaseDeltaTaskWriter.write()` 中。

**源码路径**: `BaseDeltaTaskWriter.java:81-111`

#### RowKind 处理规则（UPSERT 模式）

| RowKind | 操作 | 说明 |
|---------|------|------|
| `INSERT` | `deleteKey()` + `write()` | 先写 equality delete（删旧值），再写 data（新值） |
| `UPDATE_AFTER` | `deleteKey()` + `write()` | 同 INSERT，先删后插 |
| `UPDATE_BEFORE` | **忽略** | UPSERT 模式不需要 before 值，避免重复删除 |
| `DELETE` | `deleteKey()` | 只写 equality delete 文件（仅包含主键列值） |

```java
case INSERT:
case UPDATE_AFTER:
    if (upsert) {
        writer.deleteKey(keyProjection.wrap(row));  // 写 equality delete
    }
    writer.write(row);  // 写 data file
    break;

case UPDATE_BEFORE:
    if (upsert) {
        break; // 忽略，防止重复删除
    }
    writer.delete(row);
    break;

case DELETE:
    if (upsert) {
        writer.deleteKey(keyProjection.wrap(row));  // 仅写主键列
    } else {
        writer.delete(row);  // 写整行到 delete file
    }
    break;
```

#### UPSERT vs CDC 模式的关键区别

**UPSERT 模式** (`upsert=true`):
- `deleteKey()`: 只写 **equality 字段列** 到 delete 文件（如只写 `id` 列）
- `equalityDeleteRowSchema` = `TypeUtil.select(schema, equalityFieldIds)` —— 投影出主键列
- `UPDATE_BEFORE` 直接忽略

**CDC 模式** (`upsert=false`, 有 equality fields):
- `delete()`: 写 **整行数据** 到 delete 文件
- `equalityDeleteRowSchema` = 完整 `schema`
- `UPDATE_BEFORE` 正常处理，写入完整行到 delete 文件

**源码路径**: `RowDataTaskWriterFactory.java:134-161`

### 2.3 UPSERT 模式的约束条件

`FlinkSink.Builder.appendWriter()` 中有严格验证 (FlinkSink.java:561-578):

1. UPSERT 不能与 OVERWRITE 模式同时使用
2. Equality 字段不能为空
3. **分区表约束**: 分区字段必须是 equality 字段的子集（否则跨分区的旧记录无法被删除）

---

## 三、文件写入流程

### 3.1 数据文件（Data File）的写入

#### 核心组件：RollingFileWriter

`BaseTaskWriter.RollingFileWriter` 是数据文件写入的核心，实现了**自动滚动**机制。

**源码路径**: `BaseTaskWriter.java:471-499`

```
[RowData] → RollingFileWriter.write()
               │
               ├── 写入当前 DataWriter
               ├── currentRows++
               └── if shouldRollToNewFile()
                     ├── closeCurrent()  → 完成当前文件，加入 completedDataFiles
                     └── openCurrent()   → 创建新文件，新建 DataWriter
```

#### 文件滚动条件

```java
private boolean shouldRollToNewFile() {
    return currentRows % ROWS_DIVISOR == 0 && length(currentWriter) >= targetFileSize;
}
```

- `ROWS_DIVISOR = 1000`: 每写 1000 行检查一次
- `targetFileSize`: 由 `FlinkWriteConf.targetDataFileSize()` 配置，默认 128MB
- **两个条件同时满足**才会滚动：行数是 1000 的倍数 **且** 文件大小超过阈值

#### DataWriter 创建

```java
DataWriter<T> newWriter(EncryptedOutputFile file, StructLike partitionKey) {
    return writerFactory.newDataWriter(file, spec, partitionKey);
}
```

通过 `FlinkFileWriterFactory` 创建，底层调用 Parquet/ORC/Avro 的 appender 实现。

### 3.2 Delete 文件的写入

#### Equality Delete 文件

通过 `RollingEqDeleteWriter` 写入，与 data file 类似的滚动机制。

**源码路径**: `BaseTaskWriter.java:501-529`

```java
EqualityDeleteWriter<T> newWriter(EncryptedOutputFile file, StructLike partitionKey) {
    return writerFactory.newEqualityDeleteWriter(file, spec, partitionKey);
}
```

#### Position Delete 文件

在 `BaseEqualityDeltaWriter` 内部，维护了一个 `insertedRowMap`（`StructLikeMap<PathOffset>`），用于追踪当前 checkpoint 内已经插入的行。

**核心优化**: 如果在同一个 checkpoint 内，先 INSERT 了一行，后又 DELETE 同一个 key，系统会：
1. 从 `insertedRowMap` 中找到之前 INSERT 的 `PathOffset`（文件路径 + 行偏移）
2. 写一个 **position delete**（而非 equality delete）指向该行
3. 这比 equality delete 更高效，因为 position delete 在读取时可以精确定位

**源码路径**: `BaseTaskWriter.java:228-243`

```java
public void write(T row) throws IOException {
    PathOffset pathOffset = PathOffset.of(dataWriter.currentPath(), dataWriter.currentRows());
    StructLike copiedKey = StructLikeUtil.copy(structProjection.wrap(asStructLike(row)));

    // 如果 key 已存在，生成 pos-delete 替换旧记录
    PathOffset previous = insertedRowMap.put(copiedKey, pathOffset);
    if (previous != null) {
        writePosDelete(previous);
    }

    dataWriter.write(row);
}
```

**deleteKey() 方法**: 先尝试 `internalPosDelete`，如果没找到（说明旧数据在之前的 checkpoint 中），则写入 equality delete 文件。

```java
public void deleteKey(T key) throws IOException {
    if (!internalPosDelete(asStructLikeKey(key))) {
        eqDeleteWriter.write(key);  // 跨 checkpoint 的删除，写 eq delete
    }
}
```

### 3.3 DV (Deletion Vector) 支持

当表格式版本 > 2 时 (`formatVersion > 2`)，启用 DV 写入:

**源码路径**: `RowDataTaskWriterFactory.java:175`

```java
this.useDv = TableUtil.formatVersion(table) > 2;
```

`BaseTaskWriter` 会创建 `PartitioningDVWriter`，用于更高效的行级删除。DV 是 Iceberg V3 引入的特性，将 position delete 信息编码到紧凑的位图中，减少 delete 文件数量。

---

## 四、文件生成与管理

### 4.1 文件命名规则

通过 `OutputFileFactory` 生成文件路径:

**源码路径**: `RowDataTaskWriterFactory.java:177-182`

```java
this.outputFileFactory =
    OutputFileFactory.builderFor(table, taskId, attemptId)
        .format(format)
        .ioSupplier(() -> tableSupplier.get().io())
        .defaultSpec(spec)
        .build();
```

文件路径格式: `{table_location}/data/{partition_path}/{taskId}-{attemptId}-{fileCount}.{format}`

例如: `s3://bucket/db/table/data/date=2024-01-01/00001-0-00003.parquet`

### 4.2 分区表 vs 非分区表

| 特性 | 非分区表 | 分区表 |
|------|---------|--------|
| Append Writer | `UnpartitionedWriter` (单一 writer) | `PartitionedFanoutWriter` (每分区一个 writer) |
| Delta Writer | `UnpartitionedDeltaWriter` (单一 DeltaWriter) | `PartitionedDeltaWriter` (每分区维护 DeltaWriter Map) |
| 文件路径 | `{table}/data/` | `{table}/data/{partition_path}/` |
| 并发写 | 单文件串行写入 | 多分区并行写入，每个分区独立滚动 |

### 4.3 文件格式选择

由 `FlinkWriteConf.dataFileFormat()` 决定，可通过 `write.format.default` 配置:

```
Parquet (默认) → 列式存储，最佳压缩比和查询性能
ORC            → 列式存储，Hive 生态常用
Avro           → 行式存储，较少使用
```

压缩配置通过 `SinkUtil.writeProperties()` 传递:

**源码路径**: `SinkUtil.java:116-149`

---

## 五、元数据提交流程

### 5.1 两阶段提交模型

Flink → Iceberg 的提交采用 **两阶段提交** 设计：

```
阶段1: snapshotState()    -- 将 WriteResult 序列化为 Manifest 文件并持久化到 Flink State
阶段2: notifyCheckpointComplete() -- 实际提交到 Iceberg Table
```

### 5.2 IcebergFilesCommitter 核心流程

#### 5.2.1 接收写入结果

**源码路径**: `IcebergFilesCommitter.java:406-412`

```java
public void processElement(StreamRecord<FlinkWriteResult> element) {
    FlinkWriteResult flinkWriteResult = element.getValue();
    List<WriteResult> writeResults =
        writeResultsSinceLastSnapshot.computeIfAbsent(
            flinkWriteResult.checkpointId(), k -> Lists.newArrayList());
    writeResults.add(flinkWriteResult.writeResult());
}
```

- 每个 Writer 并行实例在 checkpoint barrier 到达时 flush 出 `FlinkWriteResult`
- Committer 收集所有 Writer 的结果，按 checkpointId 分组存储在 `writeResultsSinceLastSnapshot` 中

#### 5.2.2 Snapshot 阶段（snapshotState）

**源码路径**: `IcebergFilesCommitter.java:204-225`

```java
public void snapshotState(StateSnapshotContext context) {
    long checkpointId = context.getCheckpointId();
    writeToManifestUptoLatestCheckpoint(checkpointId);  // 写 Manifest 文件
    checkpointsState.clear();
    checkpointsState.add(dataFilesPerCheckpoint);  // 持久化到 Flink State
    jobIdState.clear();
    jobIdState.add(flinkJobId);
}
```

`writeToManifestUptoLatestCheckpoint()` 将每个 checkpoint 的 WriteResult 写成 **DeltaManifests**（包含 data manifest 和 delete manifest），然后序列化为 byte[] 存入 `dataFilesPerCheckpoint`（TreeMap）。

#### 5.2.3 写入 Manifest 文件

**源码路径**: `IcebergFilesCommitter.java:442-454`

```java
private byte[] writeToManifest(long checkpointId, List<WriteResult> writeResults) throws IOException {
    WriteResult result = WriteResult.builder().addAll(writeResults).build();
    DeltaManifests deltaManifests =
        FlinkManifestUtil.writeCompletedFiles(
            result,
            () -> manifestOutputFileFactory.create(checkpointId),
            spec,
            TableUtil.formatVersion(table));
    return SimpleVersionedSerialization.writeVersionAndSerialize(
        DeltaManifestsSerializer.INSTANCE, deltaManifests);
}
```

`DeltaManifests` 是 Flink Sink 特有的概念，包含:
- `dataManifest`: 记录所有新增 Data File 的 manifest 文件
- `deleteManifest`: 记录所有新增 Delete File 的 manifest 文件
- `referencedDataFiles`: 被 position delete 引用的 data file 路径

#### 5.2.4 Commit 阶段（notifyCheckpointComplete）

**源码路径**: `IcebergFilesCommitter.java:228-251`

```java
public void notifyCheckpointComplete(long checkpointId) {
    if (checkpointId > maxCommittedCheckpointId) {
        commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, operatorUniqueId, checkpointId);
        this.maxCommittedCheckpointId = checkpointId;
    }
    this.table = tableLoader.loadTable();  // 刷新表元数据
}
```

#### 5.2.5 实际的 Iceberg 提交逻辑

**源码路径**: `IcebergFilesCommitter.java:329-368`

根据是否有 Delete File，选择不同的 Iceberg API:

```
有 Delete File ?
  ├── 否 → table.newAppend()   (AppendFiles)
  │         遍历所有 WriteResult，appendFile() 所有 DataFile
  │
  └── 是 → table.newRowDelta() (RowDelta) [逐 checkpoint 提交]
            addRows() 添加 DataFile
            addDeletes() 添加 DeleteFile
```

**关键设计决策**: 当存在 delete file 时，**不合并多个 checkpoint 的结果到单个事务**。原因是 equality delete 的语义要求：txn2 的 delete file 必须能应用到 txn1 的 data file。如果合并提交，会破坏这个 delete 语义。

```java
// 有 delete file 时，逐 checkpoint 提交
for (Map.Entry<Long, WriteResult> e : pendingResults.entrySet()) {
    RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
    commitOperation(rowDelta, ...);
}
```

#### 5.2.6 Snapshot 元信息

每次提交时，在 Iceberg Snapshot 的 summary 中记录关键信息:

**源码路径**: `IcebergFilesCommitter.java:384-390`

```java
operation.set(MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
operation.set(FLINK_JOB_ID, newFlinkJobId);
operation.set(OPERATOR_ID, operatorId);
operation.toBranch(branch);
```

这些元信息用于故障恢复时判断哪些 checkpoint 已经成功提交。

### 5.3 Overwrite 模式（ReplacePartitions）

当 `replacePartitions=true` 时，使用 `table.newReplacePartitions()` API:

**源码路径**: `IcebergFilesCommitter.java:304-327`

```java
ReplacePartitions dynamicOverwrite = table.newReplacePartitions().scanManifestsWith(workerPool);
for (WriteResult result : pendingResults.values()) {
    Arrays.stream(result.dataFiles()).forEach(dynamicOverwrite::addFile);
}
commitOperation(dynamicOverwrite, ...);
```

注意：Overwrite 模式不支持 Delete File。

### 5.4 空提交优化

为避免频繁的空提交，引入了 `maxContinuousEmptyCommits` 机制（默认 10）:

```java
continuousEmptyCheckpoints = totalFiles == 0 ? continuousEmptyCheckpoints + 1 : 0;
if (totalFiles != 0 || continuousEmptyCheckpoints % maxContinuousEmptyCommits == 0) {
    // 执行提交
}
```

当连续空 checkpoint 达到阈值时才触发一次空提交（用于更新 `max-committed-checkpoint-id`），避免生成过多空 snapshot。

---

## 六、故障恢复机制

### 6.1 Exactly-Once 语义保障

Flink + Iceberg 的 exactly-once 语义依赖以下机制:

```
┌─────────────────────────────────────────────────────────┐
│                    正常流程                                │
│                                                           │
│  1. Writer 写数据文件到存储                                │
│  2. checkpoint barrier 触发 → Writer flush WriteResult    │
│  3. Committer snapshotState() → 写 manifest 到 Flink State│
│  4. Checkpoint 成功                                       │
│  5. notifyCheckpointComplete() → 提交到 Iceberg           │
│  6. 清理临时 manifest 文件                                  │
└─────────────────────────────────────────────────────────┘
```

### 6.2 Writer 端的状态管理

`IcebergStreamWriter` 是**无状态**的：

**源码路径**: `IcebergStreamWriter.java:66-69`

```java
public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flush(checkpointId);           // 关闭当前所有文件，发出 WriteResult
    this.writer = taskWriterFactory.create();  // 创建全新的 writer
}
```

- 每次 checkpoint barrier 到达时，Writer **完全关闭**当前所有文件
- 将 `WriteResult`（含 DataFile/DeleteFile 列表）发送给下游 Committer
- 然后创建全新的 TaskWriter，从零开始写
- **Writer 本身不保存任何状态到 checkpoint**

### 6.3 Committer 端的状态管理

`IcebergFilesCommitter` 维护两个 Flink State:

| State | 类型 | 内容 |
|-------|------|------|
| `checkpointsState` | `ListState<SortedMap<Long, byte[]>>` | 每个 checkpoint 的 DeltaManifests 序列化字节 |
| `jobIdState` | `ListState<String>` | Flink Job ID |

**源码路径**: `IcebergFilesCommitter.java:115-121`

### 6.4 故障恢复流程

**源码路径**: `IcebergFilesCommitter.java:143-201`

```
故障恢复时 initializeState():
│
├── 1. 从 State 恢复 restoredFlinkJobId
│
├── 2. 遍历 Iceberg 快照历史，找到该 Job 的 maxCommittedCheckpointId
│      └── SinkUtil.getMaxCommittedCheckpointId(table, restoredFlinkJobId, operatorUniqueId, branch)
│
├── 3. 从 State 中取出所有未提交的 checkpoint 数据
│      └── uncommittedDataFiles = checkpointsState.tailMap(maxCommittedCheckpointId, false)
│
└── 4. 将这些未提交的数据重新提交到 Iceberg
       └── commitUpToCheckpoint(uncommittedDataFiles, ...)
```

#### 6.4.1 查找已提交的最大 checkpoint ID

**源码路径**: `SinkUtil.java:83-105`

```java
static long getMaxCommittedCheckpointId(Table table, String flinkJobId, String operatorId, String branch) {
    Snapshot snapshot = table.snapshot(branch);
    while (snapshot != null) {
        Map<String, String> summary = snapshot.summary();
        String snapshotFlinkJobId = summary.get(FLINK_JOB_ID);
        String snapshotOperatorId = summary.get(OPERATOR_ID);
        if (flinkJobId.equals(snapshotFlinkJobId)
            && (snapshotOperatorId == null || snapshotOperatorId.equals(operatorId))) {
            String value = summary.get(MAX_COMMITTED_CHECKPOINT_ID);
            if (value != null) {
                return Long.parseLong(value);
            }
        }
        snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }
    return INITIAL_CHECKPOINT_ID;
}
```

从最新 snapshot 向前遍历，找到同一 Flink Job + Operator ID 的最新已提交 checkpoint ID。

#### 6.4.2 避免重复提交

通过两层保障:

1. **maxCommittedCheckpointId**: `notifyCheckpointComplete()` 中检查 `checkpointId > maxCommittedCheckpointId`，跳过已提交的 checkpoint
2. **Snapshot summary**: 恢复时从 Iceberg snapshot history 中精确找到最后成功提交的 checkpoint

### 6.5 已写未提交文件的处理

故障恢复时，可能存在已写入存储但未提交到 Iceberg 的数据文件:

- **已 snapshot 但未 commit 的文件**: 通过 Flink State 中的 `dataFilesPerCheckpoint` 恢复，重新提交
- **未 snapshot 的文件**（Writer flush 之前故障）: 这些文件已写入存储但 Flink State 中没有记录，成为**孤儿文件**。需要通过 Iceberg 的 `ExpireSnapshots` 或外部清理机制处理

### 6.6 Checkpoint 乱序处理

**源码路径**: `IcebergFilesCommitter.java:230-247`

Flink 可能出现 `notifyCheckpointComplete` 乱序回调的情况:

```
1. snapshotState(ckp1)
2. snapshotState(ckp2)
3. notifyCheckpointComplete(ckp2)  → 提交 ckp1 + ckp2 的所有文件
4. notifyCheckpointComplete(ckp1)  → 跳过（ckp1 < maxCommittedCheckpointId=2）
```

由于 `dataFilesPerCheckpoint` 是 `TreeMap`（有序），`commitUpToCheckpoint` 使用 `headMap(checkpointId, true)` 一次性提交所有 <= checkpointId 的数据。所以即使 ckp1 先完成但 notify 后到达，ckp2 的 notify 会把 ckp1 的数据一起提交。

---

## 七、数据分布模式（Distribution Mode）

### 7.1 三种分布模式

**源码路径**: `FlinkSink.java:607-721`

| 模式 | 说明 | 实现方式 |
|------|------|----------|
| `NONE` | 不做重分布 | 有 equality fields 时用 `keyBy(EqualityFieldKeySelector)` |
| `HASH` | 按分区键哈希 | `keyBy(PartitionKeySelector)` 或 `keyBy(EqualityFieldKeySelector)` |
| `RANGE` | 按排序键范围分区 | `DataStatisticsOperator` + `RangePartitioner` |

### 7.2 HASH 模式详细逻辑

```
有 equality fields?
  ├── 是 + 分区表 → keyBy(PartitionKeySelector)
  │   前提: 分区字段必须包含在 equality fields 中
  │
  ├── 是 + 非分区表 → keyBy(EqualityFieldKeySelector)
  │
  ├── 否 + 分区表 → keyBy(PartitionKeySelector)
  │
  └── 否 + 非分区表 → 退化为 NONE 模式
```

### 7.3 RANGE 模式

Range 模式通过收集数据分布统计信息来实现均衡的范围分区:
- `DataStatisticsOperator`: 收集每个 subtask 的数据分布统计
- `DataStatisticsCoordinator`: 汇总统计信息并下发全局分区方案
- `RangePartitioner`: 根据全局统计信息进行范围分区

---

## 八、IcebergStreamWriter 详细流程

### 8.1 生命周期

**源码路径**: `IcebergStreamWriter.java:32-121`

```
open()
  │ → taskWriterFactory.initialize(subTaskId, attemptId)
  │ → writer = taskWriterFactory.create()
  │
processElement(record)     ←── 循环接收数据
  │ → writer.write(record)
  │
prepareSnapshotPreBarrier(checkpointId)   ←── checkpoint barrier 到达
  │ → flush(checkpointId)
  │     → result = writer.complete()     // 关闭所有文件
  │     → output.collect(FlinkWriteResult(checkpointId, result))
  │ → writer = taskWriterFactory.create() // 创建新 writer
  │
endInput()                 ←── 有界流结束
  │ → flush(END_INPUT_CHECKPOINT_ID)     // checkpointId = Long.MAX_VALUE
  │
close()
  │ → writer.close()
```

### 8.2 关键设计: 防止重复 flush

```java
private void flush(long checkpointId) throws IOException {
    if (writer == null) {
        return;  // 已经 flush 过了
    }
    WriteResult result = writer.complete();
    output.collect(new StreamRecord<>(new FlinkWriteResult(checkpointId, result)));
    writer = null;  // 设为 null 防止重复 flush
}
```

在有界流场景中，`endInput()` 可能在 `prepareSnapshotPreBarrier()` 之前或之后调用，通过 `writer = null` 判断防止重复发送。

---

## 九、完整数据流转图

```
                    ┌──────────────────────────────────────────────┐
                    │           Flink DataStream (RowData)         │
                    └───────────────────┬──────────────────────────┘
                                        │
                    ┌───────────────────▼──────────────────────────┐
                    │        Distribution (keyBy/range/none)        │
                    └───────────────────┬──────────────────────────┘
                                        │
          ┌─────────────────────────────┼──────────────────────────┐
          │                             │                          │
┌─────────▼──────────┐    ┌────────────▼────────────┐   ┌────────▼──────────┐
│ IcebergStreamWriter │    │  IcebergStreamWriter    │   │ IcebergStreamWriter│
│                     │    │                         │   │                    │
│ ┌─────────────────┐ │    │  ┌─────────────────┐   │   │ ┌────────────────┐ │
│ │  TaskWriter      │ │    │  │  TaskWriter      │   │   │ │  TaskWriter    │ │
│ │  ┌────────┐     │ │    │  │  ┌────────┐     │   │   │ │  ┌────────┐   │ │
│ │  │DataFile│     │ │    │  │  │DataFile│     │   │   │ │  │DataFile│   │ │
│ │  ├────────┤     │ │    │  │  ├────────┤     │   │   │ │  ├────────┤   │ │
│ │  │EqDelete│     │ │    │  │  │EqDelete│     │   │   │ │  │EqDelete│   │ │
│ │  ├────────┤     │ │    │  │  ├────────┤     │   │   │ │  ├────────┤   │ │
│ │  │PosDelete│    │ │    │  │  │PosDelete│    │   │   │ │  │PosDelete│  │ │
│ │  └────────┘     │ │    │  │  └────────┘     │   │   │ │  └────────┘   │ │
│ └─────────────────┘ │    │  └─────────────────┘   │   │ └────────────────┘ │
│   ↓ checkpoint      │    │    ↓ checkpoint        │   │   ↓ checkpoint     │
│   WriteResult       │    │    WriteResult         │   │   WriteResult      │
└─────────┬──────────┘    └────────────┬────────────┘   └────────┬──────────┘
          │                             │                          │
          │         FlinkWriteResult    │    FlinkWriteResult      │
          └─────────────────────────────┼──────────────────────────┘
                                        │
                    ┌───────────────────▼──────────────────────────┐
                    │          IcebergFilesCommitter (并行度=1)      │
                    │                                              │
                    │  processElement(): 收集 WriteResult           │
                    │  snapshotState(): 写 Manifest → Flink State  │
                    │  notifyCheckpointComplete():                  │
                    │    ├── 无 delete → AppendFiles.commit()       │
                    │    └── 有 delete → RowDelta.commit() (逐ckp)  │
                    └──────────────────────────────────────────────┘
                                        │
                    ┌───────────────────▼──────────────────────────┐
                    │           Iceberg Table                       │
                    │                                              │
                    │  Snapshot (summary 含 flink.job-id,           │
                    │           flink.max-committed-checkpoint-id)  │
                    │    └── ManifestList                           │
                    │          ├── DataManifest → DataFiles         │
                    │          └── DeleteManifest → DeleteFiles     │
                    └──────────────────────────────────────────────┘
```

---

## 十、关键配置项汇总

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `write.format.default` | `parquet` | 数据文件格式 |
| `write.target-file-size-bytes` | `128MB` | 文件滚动大小阈值 |
| `write.upsert.enabled` | `false` | 是否启用 UPSERT 模式 |
| `write.distribution-mode` | `none` | 数据分布模式: none/hash/range |
| `flink.max-continuous-empty-commits` | `10` | 连续空提交最大次数 |
| `write.parquet.compression-codec` | `gzip` | Parquet 压缩编码 |
| `write-parallelism` | 同输入并行度 | Writer 并行度 |

---

## 十一、总结

### 核心设计要点

1. **Writer 无状态，Committer 有状态**: Writer 每次 checkpoint 完全清空重建，状态全在 Committer 端通过 Flink State 管理
2. **两阶段提交**: snapshotState 写 manifest 到 State，notifyCheckpointComplete 才真正提交到 Iceberg
3. **单点提交**: Committer 并行度固定为 1，避免并发提交冲突
4. **Checkpoint 驱动提交**: 不是每条记录提交，而是每次成功 checkpoint 后批量提交
5. **UPSERT 优化**: 同 checkpoint 内的 insert-then-delete 转为更高效的 position delete
6. **Delete 语义正确性**: 有 delete file 时逐 checkpoint 提交，保证 equality delete 的序列号语义

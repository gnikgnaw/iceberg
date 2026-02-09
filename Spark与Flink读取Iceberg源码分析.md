# Spark与Flink读取Iceberg源码分析

> 本文档深入分析Spark和Flink如何读取Iceberg表数据，重点关注DataFile与DeleteFile的处理机制、流式读取实现以及Delete文件管理策略。

---

## 第一章：Iceberg读取基础架构

### 1.1 Iceberg元数据层次结构

Iceberg采用三层元数据结构来管理海量数据文件，这种设计使得元数据操作高效且支持快照隔离：

```
TableMetadata (表元数据)
    │
    ├─ Snapshot 1 (快照1 - v1版本)
    │   ├─ Manifest List (清单列表)
    │   │   ├─ Data Manifest File 1 (数据清单文件)
    │   │   │   ├─ DataFile 1 (data-001.parquet)
    │   │   │   ├─ DataFile 2 (data-002.parquet)
    │   │   │   └─ DataFile 3 (data-003.parquet)
    │   │   │
    │   │   └─ Delete Manifest File 1 (删除清单文件)
    │   │       ├─ DeleteFile 1 (delete-001.parquet - POSITION_DELETES)
    │   │       └─ DeleteFile 2 (delete-002.parquet - EQUALITY_DELETES)
    │   │
    │   └─ Snapshot Summary (快照统计信息)
    │
    └─ Snapshot 2 (快照2 - v2版本)
        └─ ... (新的Manifest List)
```

**核心概念：**

1. **Snapshot（快照）**：表在某个时间点的完整状态
   - 每次写操作（INSERT/UPDATE/DELETE）创建新快照
   - 通过快照ID实现时间旅行（Time Travel）
   - 包含sequenceNumber用于排序和并发控制

2. **ManifestFile（清单文件）**：元数据文件，记录DataFile或DeleteFile列表
   - 分为两种类型：`DATA`清单和`DELETES`清单
   - 包含分区统计信息，用于快速剪枝
   - 包含文件级别统计（recordCount、columnSizes、lowerBounds/upperBounds）

3. **DataFile（数据文件）**：实际存储表数据的文件（Parquet/Avro/ORC格式）

4. **DeleteFile（删除文件）**：记录被删除的数据，支持Merge-on-Read

---

### 1.2 ContentFile接口体系

#### 1.2.1 接口继承关系

```
ContentFile<F> (通用内容文件接口)
    │
    ├── DataFile (数据文件接口)
    │       └── content() = FileContent.DATA (固定返回DATA类型)
    │
    └── DeleteFile (删除文件接口)
            └── content() = POSITION_DELETES 或 EQUALITY_DELETES
```

**源码位置：**
- `api/src/main/java/org/apache/iceberg/ContentFile.java` - 通用接口
- `api/src/main/java/org/apache/iceberg/DataFile.java` - 数据文件
- `api/src/main/java/org/apache/iceberg/DeleteFile.java` - 删除文件

#### 1.2.2 ContentFile核心属性

```java
public interface ContentFile<F> {
    // 基本信息
    String location();              // 文件完整路径
    FileFormat format();            // 文件格式 (PARQUET/AVRO/ORC)
    FileContent content();          // 文件内容类型
    int specId();                   // 分区规格ID
    StructLike partition();         // 分区值

    // 数据统计
    long recordCount();             // 记录数（顶层记录）
    long fileSizeInBytes();         // 文件大小
    Map<Integer, Long> columnSizes();      // 列ID -> 列大小（字节）
    Map<Integer, Long> valueCounts();      // 列ID -> 值计数
    Map<Integer, Long> nullValueCounts();  // 列ID -> NULL值计数
    Map<Integer, ByteBuffer> lowerBounds(); // 列ID -> 最小值
    Map<Integer, ByteBuffer> upperBounds(); // 列ID -> 最大值

    // 删除相关
    List<Integer> equalityFieldIds(); // Equality Delete的字段ID列表

    // 序列号（用于并发控制）
    Long dataSequenceNumber();      // 数据序列号
    Long fileSequenceNumber();      // 文件序列号
}
```

**关键统计信息的作用：**
- **lowerBounds/upperBounds**：用于谓词下推和数据跳过
- **nullValueCounts**：优化NULL值过滤
- **columnSizes**：用于Split分配和读取优化

---

### 1.3 FileContent：三种文件类型

```java
// 源码：api/src/main/java/org/apache/iceberg/FileContent.java
public enum FileContent {
    DATA(0),              // 数据文件
    POSITION_DELETES(1),  // 位置删除文件
    EQUALITY_DELETES(2);  // 等值删除文件
}
```

#### 1.3.1 DATA - 数据文件

**特征：**
- 存储表的实际数据行
- 使用列式存储格式（Parquet/ORC）或行式格式（Avro）
- 包含完整的列统计信息
- 不可变（Immutable）：一旦写入不可修改

**示例：** `s3://bucket/table/data/part=2024-01-01/data-00001.parquet`

---

#### 1.3.2 POSITION_DELETES - 位置删除文件

**作用：** 精确标记数据文件中被删除的行位置

**存储格式：**
```
Schema:
  file_path: string        (必需) - 被删除的数据文件路径
  pos: long                (必需) - 被删除的行位置（从0开始）
  [其他列...]             (可选) - 用于重建删除前的数据
```

**示例数据：**
```
| file_path                                      | pos  | user_id | name   |
|------------------------------------------------|------|---------|--------|
| s3://bucket/table/data/data-00001.parquet     | 42   | 1001    | Alice  |
| s3://bucket/table/data/data-00001.parquet     | 156  | 1002    | Bob    |
| s3://bucket/table/data/data-00002.parquet     | 7    | 1003    | Carol  |
```

**关键特性：**
1. **引用特定DataFile**：通过`file_path`列关联
2. **DeleteFile.referencedDataFile()**：
   - 返回NULL：删除文件可能影响多个DataFile
   - 返回具体路径：仅影响单个DataFile（优化场景）
3. **高效合并**：读取时只需扫描引用的DataFile

**源码引用：**
```java
// DeleteFile.java:36-44
default String referencedDataFile() {
    return null;  // 大部分Position Delete返回null
}
```

---

#### 1.3.3 EQUALITY_DELETES - 等值删除文件

**作用：** 基于字段值删除满足条件的所有行（类似SQL的DELETE WHERE）

**存储格式：**
```
Schema:
  id: int                (等值字段1)
  email: string          (等值字段2)
  [其他列...]           (可选 - 用于数据重建)
```

**示例数据：**
```
| id   | email              |
|------|--------------------|
| 1001 | alice@example.com |
| 1002 | bob@example.com   |
```

**关键特性：**
1. **equalityFieldIds()**：指定用于等值比较的列ID
   ```java
   // DataFile.java:92-97
   Types.NestedField EQUALITY_IDS = optional(
       135, "equality_ids",
       ListType.ofRequired(136, IntegerType.get()),
       "Equality comparison field IDs"
   );
   ```

2. **作用范围**：基于等值字段匹配，可能作用于多个DataFile
   - 在**非分区表**中常表现为全局范围
   - 在**分区表**中会先按分区裁剪，再做等值匹配

3. **性能考量**：
   - 读取时需要对候选DataFile做额外匹配
   - 需要基于equality字段做集合匹配（Hash查找）
   - 适合批量删除场景（如GDPR数据清理）

**对比Position Delete：**
| 特性             | Position Delete      | Equality Delete       |
|------------------|----------------------|-----------------------|
| 删除精度         | 精确到行位置         | 基于字段值匹配        |
| 影响范围         | 单个或多个DataFile   | 跨文件匹配（分区内/全局） |
| 存储开销         | 小（仅路径+位置）    | 中等（需存储等值字段）|
| 读取性能         | 高（直接跳过行）     | 中（需Join匹配）      |
| 使用场景         | UPDATE/DELETE单行    | 批量DELETE条件删除    |

---

### 1.4 ManifestFile：清单文件结构

#### 1.4.1 ManifestContent类型

```java
// 源码：api/src/main/java/org/apache/iceberg/ManifestContent.java
public enum ManifestContent {
    DATA(0),      // 数据清单：管理DataFile列表
    DELETES(1);   // 删除清单：管理DeleteFile列表
}
```

**分离设计的优势：**
1. **独立演化**：数据文件和删除文件可以独立增长/压缩
2. **快速定位**：读取时可以精准定位到DELETE清单
3. **统计优化**：分别统计数据和删除的行数

#### 1.4.2 ManifestFile核心属性

```java
// 源码：api/src/main/java/org/apache/iceberg/ManifestFile.java
public interface ManifestFile {
    String path();                    // 清单文件路径
    long length();                    // 文件大小
    int partitionSpecId();            // 分区规格ID
    ManifestContent content();        // DATA 或 DELETES

    // 序列号（并发控制）
    long sequenceNumber();            // 清单添加时的序列号
    long minSequenceNumber();         // 清单中最小的数据序列号

    // 文件统计
    Integer addedFilesCount();        // ADDED状态文件数
    Integer existingFilesCount();     // EXISTING状态文件数
    Integer deletedFilesCount();      // DELETED状态文件数

    Long addedRowsCount();            // ADDED文件的总行数
    Long existingRowsCount();         // EXISTING文件的总行数
    Long deletedRowsCount();          // DELETED文件的总行数

    // 分区统计（用于剪枝）
    List<PartitionFieldSummary> partitions();
}
```

#### 1.4.3 Manifest Entry状态

每个Manifest内部存储的Entry有三种状态：

```java
public enum Status {
    EXISTING(0),  // 已存在（从上一个快照继承）
    ADDED(1),     // 新增（当前快照新增）
    DELETED(2);   // 删除（当前快照标记删除）
}
```

**状态转换示例：**
```
Snapshot 1:
  Manifest-1:
    - DataFile A (ADDED)
    - DataFile B (ADDED)

Snapshot 2 (reuse Manifest-1):
  Manifest-1:
    - DataFile A (EXISTING)  ← 状态变更
    - DataFile B (EXISTING)
  Manifest-2:
    - DataFile C (ADDED)
    - DeleteFile D (ADDED)

Snapshot 3:
  Manifest-1:
    - DataFile A (EXISTING)
    - DataFile B (DELETED)   ← 标记删除
  Manifest-2:
    - DataFile C (EXISTING)
    - DeleteFile D (EXISTING)
```

---

### 1.5 读取流程概述

#### 1.5.1 批量读取流程（Snapshot Scan）

```
1. TableScan.planFiles()
   │
   ├─ 2. 获取目标Snapshot
   │   └─ snapshot.dataManifests() / deleteManifests()
   │
   ├─ 3. 读取所有Manifest文件
   │   ├─ 过滤分区（Partition Pruning）
   │   └─ 过滤数据文件（Data File Pruning）
   │
   ├─ 4. 构建FileScanTask
   │   ├─ DataFile列表
   │   └─ 关联的DeleteFile列表
   │
   └─ 5. 执行读取（Merge-on-Read）
       ├─ 读取DataFile
       ├─ 应用Position Deletes（跳过删除的行）
       └─ 应用Equality Deletes（过滤匹配的行）
```

#### 1.5.2 DeleteFile与DataFile的关联

**关联策略：**
1. **Position Delete**：
   - 通过`file_path`列精确关联
   - 一个DeleteFile可以关联多个DataFile
   - 读取时仅加载相关的DeleteFile

2. **Equality Delete**：
   - 基于分区/sequence/统计信息筛选候选文件
   - 读取时加载可能生效的Equality Delete
   - 通过`equalityFieldIds`进行匹配

**示例代码片段：**
```java
// 伪代码：构建ScanTask时关联DeleteFile
for (DataFile dataFile : dataFiles) {
    List<DeleteFile> associatedDeletes = new ArrayList<>();

    // 添加Position Deletes
    for (DeleteFile deleteFile : positionDeletes) {
        if (deleteFile.content() == POSITION_DELETES) {
            if (deleteFile.referencedDataFile() == null
                || deleteFile.referencedDataFile().equals(dataFile.location())) {
                associatedDeletes.add(deleteFile);
            }
        }
    }

    // 添加所有Equality Deletes
    associatedDeletes.addAll(equalityDeletes);

    FileScanTask task = new FileScanTask(dataFile, associatedDeletes, ...);
}
```

---

### 1.6 Sequence Number：并发控制核心

#### 1.6.1 两种序列号

```java
// ContentFile接口
Long dataSequenceNumber();    // 数据所属的序列号
Long fileSequenceNumber();    // 文件添加时的序列号
```

**区别与联系：**

| 属性                 | dataSequenceNumber      | fileSequenceNumber       |
|----------------------|-------------------------|--------------------------|
| **定义**             | 数据逻辑上属于的版本    | 文件物理添加的版本       |
| **变更时机**         | 创建时设置，不变        | 添加到表时设置，不变     |
| **Compaction场景**   | 保持不变                | 设为新的序列号           |
| **用途**             | 判断Delete是否生效      | 文件生命周期追踪         |

**示例场景：**
```
Snapshot 1 (seq=10):
  - DataFile A (dataSeq=10, fileSeq=10)

Snapshot 2 (seq=15):
  - DeleteFile D (dataSeq=10, fileSeq=15)  ← 删除Snapshot 1的数据

Snapshot 3 (seq=20) - Compaction:
  - DataFile B (dataSeq=10, fileSeq=20)    ← 压缩DataFile A
  - DeleteFile D仍然有效，因为B的dataSeq=10
```

**关键规则：**
> DeleteFile的dataSequenceNumber决定它能删除哪些数据。
>
> - **Position Delete**：可作用于 `dataSeq <= deleteFileDataSeq`
> - **Equality Delete**：可作用于 `dataSeq < deleteFileDataSeq`

这确保了：
- 新写入的数据不会被旧的Delete文件误删
- Compaction后Delete仍然正确生效

---

`★ Insight ─────────────────────────────────────`
**1. 三层架构的设计智慧**
Snapshot → ManifestFile → DataFile/DeleteFile的分层，使得元数据操作复杂度从O(N)降到O(logN)。每次commit仅需修改少量Manifest，大部分可复用。

**2. Delete文件分离的权衡**
Position Delete与Equality Delete分开设计，是性能与灵活性的平衡：前者牺牲一定存储换取极致读性能，后者用计算换空间实现灵活删除。

**3. Sequence Number的巧妙性**
通过dataSeq与fileSeq双序列号，Iceberg优雅解决了Compaction后Delete生效性问题，这是Copy-on-Write格式难以实现的。
`─────────────────────────────────────────────────`

---

## 第一章总结

本章介绍了Iceberg读取数据的基础架构：

✅ **元数据结构**：Snapshot → ManifestFile → ContentFile三层体系
✅ **文件类型**：DATA、POSITION_DELETES、EQUALITY_DELETES三种FileContent
✅ **关联机制**：Position Delete按path/分区关联，Equality Delete按分区+sequence+统计裁剪后匹配
✅ **并发控制**：通过dataSequenceNumber确保Delete正确性

**下一章预告：** 我们将深入分析Spark读取机制，重点关注SparkBatchQueryScan如何协调DataFile与DeleteFile的合并读取（Merge-on-Read）。

---

---

## 第二章：Spark读取机制详解

### 2.1 Spark读取流程总览

Spark通过DataSource V2 API与Iceberg集成，读取流程可分为以下几个阶段：

```
1. SparkTable (Catalog层)
   │
   ├─ 2. SparkBatchQueryScan (Scan层)
   │   ├─ 获取Snapshot
   │   ├─ 读取Manifest文件
   │   ├─ 分区过滤 (Partition Pruning)
   │   ├─ 数据文件过滤 (Data File Pruning)
   │   └─ 构建ScanTaskGroup
   │
   ├─ 3. SparkInputPartition (分区任务)
   │   └─ 包含一组FileScanTask
   │
   ├─ 4. RowDataReader (执行层)
   │   ├─ 创建SparkDeleteFilter
   │   ├─ 读取DataFile
   │   └─ 应用Delete过滤
   │
   └─ 5. 返回 InternalRow 流
```

**关键类：**
- `SparkBatchQueryScan`：计划扫描任务，负责文件发现和过滤
- `BaseFileScanTask`：封装单个DataFile及其关联的DeleteFile列表
- `RowDataReader`：实际读取数据并应用删除过滤
- `SparkDeleteFilter`：Merge-on-Read的核心实现

**源码位置：**
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java`
- `core/src/main/java/org/apache/iceberg/BaseFileScanTask.java`
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/RowDataReader.java`

---

### 2.2 FileScanTask：DataFile与DeleteFile的桥梁

#### 2.2.1 FileScanTask接口定义

```java
// 源码：api/src/main/java/org/apache/iceberg/FileScanTask.java:25-57
public interface FileScanTask extends ContentScanTask<DataFile>,
                                      SplittableScanTask<FileScanTask> {
    /**
     * A list of {@link DeleteFile delete files} to apply when reading
     * the task's data file.
     */
    List<DeleteFile> deletes();

    @Override
    default long sizeBytes() {
        return length() + ScanTaskUtil.contentSizeInBytes(deletes());
    }

    @Override
    default int filesCount() {
        return 1 + deletes().size();  // 1个DataFile + N个DeleteFile
    }
}
```

**核心设计：**
1. **一对多关系**：一个DataFile对应多个DeleteFile
2. **大小计算**：任务大小 = DataFile大小 + 所有DeleteFile大小
3. **文件计数**：用于调度器评估IO并行度

---

#### 2.2.2 BaseFileScanTask实现

```java
// 源码：core/src/main/java/org/apache/iceberg/BaseFileScanTask.java:28-89
public class BaseFileScanTask extends BaseContentScanTask<FileScanTask, DataFile>
    implements FileScanTask {

    private final DeleteFile[] deletes;  // DeleteFile数组
    private transient volatile List<DeleteFile> deleteList = null;
    private transient volatile long deletesSizeBytes = 0L;

    public BaseFileScanTask(
        DataFile file,
        DeleteFile[] deletes,  // 构造时传入关联的DeleteFile
        String schemaString,
        String specString,
        ResidualEvaluator residuals) {
        super(file, schemaString, specString, residuals);
        this.deletes = deletes != null ? deletes : new DeleteFile[0];
    }

    @Override
    public List<DeleteFile> deletes() {
        if (deleteList == null) {
            this.deleteList = ImmutableList.copyOf(deletes);
        }
        return deleteList;
    }

    // 懒加载Delete文件大小，避免重复计算
    private long deletesSizeBytes() {
        if (deletesSizeBytes == 0L && deletes.length > 0) {
            long size = 0L;
            for (DeleteFile deleteFile : deletes) {
                size += ScanTaskUtil.contentSizeInBytes(deleteFile);
            }
            this.deletesSizeBytes = size;
        }
        return deletesSizeBytes;
    }
}
```

**关键实现细节：**
1. **懒加载**：deleteList和deletesSizeBytes延迟初始化，优化内存
2. **不可变性**：使用ImmutableList确保线程安全
3. **序列化优化**：transient关键字避免序列化缓存字段

---

### 2.3 DeleteFile关联机制

#### 2.3.1 关联策略总览

Spark在构建FileScanTask时，根据DeleteFile类型采用不同的关联策略：

| DeleteFile类型      | 关联方式                                              | 关联范围                |
|---------------------|-------------------------------------------------------|-------------------------|
| **Position Delete** | 优先用`referencedDataFile()`，否则基于`file_path`统计/分区归类 | 特定DataFile或分区内文件 |
| **Equality Delete** | 按分区+sequence+列统计裁剪后再做等值匹配              | 分区内或跨分区候选文件   |

#### 2.3.2 Position Delete关联逻辑

**场景1：File-Scoped Position Delete（文件级删除）**

```java
// DeleteFile接口
default String referencedDataFile() {
    return null;  // 默认返回null
}
```

当`referencedDataFile() != null`时，表示该DeleteFile仅影响单个DataFile：

```
DeleteFile {
  location: "s3://bucket/deletes/delete-001.parquet"
  content: POSITION_DELETES
  referencedDataFile: "s3://bucket/data/data-001.parquet"  ← 明确指定
}

// 仅关联到 data-001.parquet
```

**好处：**
- 读取`data-002.parquet`时无需加载`delete-001.parquet`
- 减少不必要的IO和内存开销

**场景2：全局Position Delete**

当`referencedDataFile() == null`时，会优先利用`file_path`的上下界统计和分区信息进行归类；
必要时才在读取阶段按`file_path`过滤：

```
DeleteFile {
  location: "s3://bucket/deletes/delete-002.parquet"
  content: POSITION_DELETES
  referencedDataFile: null

  // 内部数据：
  | file_path                              | pos  |
  |----------------------------------------|------|
  | s3://bucket/data/data-001.parquet     | 42   |
  | s3://bucket/data/data-003.parquet     | 156  |
}

// 关联到 data-001.parquet 和 data-003.parquet
```

**代价：**
- 需要依赖统计信息/分区索引进行推断
- 统计不足时，读取阶段的过滤成本会上升

#### 2.3.3 Equality Delete关联逻辑

Equality Delete并非无条件“全局关联”，实际会先经过分区、sequence和统计信息裁剪：

```java
// DeleteFile中的关键字段
List<Integer> equalityFieldIds();  // 返回用于等值比较的列ID

// 示例
DeleteFile {
  location: "s3://bucket/deletes/eq-delete-001.parquet"
  content: EQUALITY_DELETES
  equalityFieldIds: [1, 5]  // 比较 id(fieldId=1) 和 email(fieldId=5)

  // 内部数据：
  | id   | email              |
  |------|--------------------|
  | 1001 | alice@example.com |
  | 1002 | bob@example.com   |
}
```

**生效原因：**
- 等值删除不依赖行位置，依赖数据值
- 在候选文件中，任何`id=1001 AND email='alice@example.com'`的行都会被删除

---

### 2.4 Merge-on-Read：SparkDeleteFilter核心实现

#### 2.4.1 RowDataReader读取流程

```java
// 源码：spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/RowDataReader.java:86-100
@Override
protected CloseableIterator<InternalRow> open(FileScanTask task) {
    String filePath = task.file().location();
    LOG.debug("Opening data file {}", filePath);

    // 1. 创建DeleteFilter
    SparkDeleteFilter deleteFilter =
        new SparkDeleteFilter(filePath, task.deletes(), counter(), true);

    // 2. 计算需要读取的Schema（可能包含额外的删除相关列）
    Schema requiredSchema = deleteFilter.requiredSchema();
    Map<Integer, ?> idToConstant = constantsMap(task, requiredSchema);

    // 3. 读取DataFile并应用Delete过滤
    return deleteFilter.filter(open(task, requiredSchema, idToConstant)).iterator();
}
```

**流程解析：**
1. **创建过滤器**：传入DataFile路径和DeleteFile列表
2. **Schema扩展**：可能添加`_pos`（行位置）和Equality字段
3. **流式过滤**：读取数据流的同时应用删除逻辑

---

#### 2.4.2 DeleteFilter过滤链

```java
// 源码：data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177-179
public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
}
```

**过滤顺序：**
```
原始数据流 (DataFile Records)
    │
    ├─ 1. applyPosDeletes()  ← 先应用Position Delete
    │   └─ 使用PositionDeleteIndex快速判断行位置
    │
    └─ 2. applyEqDeletes()   ← 再应用Equality Delete
        └─ 使用StructLikeSet进行集合匹配
```

**为什么先Position后Equality？**
- Position Delete通常更精确（O(1)位置查找）
- Equality Delete需要Hash匹配（O(1)但常数更大）
- 先过滤Position Delete可以减少Equality比较次数

---

#### 2.4.3 Position Delete应用机制

```java
// 源码：data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:250-258
private CloseableIterable<T> applyPosDeletes(CloseableIterable<T> records) {
    if (posDeletes.isEmpty()) {
        return records;  // 无Position Delete，直接返回
    }

    // 1. 构建Position Delete索引
    PositionDeleteIndex positionIndex = deletedRowPositions();

    // 2. 创建Predicate判断每一行是否被删除
    Predicate<T> isDeleted = record -> positionIndex.isDeleted(pos(record));

    return createDeleteIterable(records, isDeleted);
}
```

**PositionDeleteIndex内部结构：**

```java
// 伪代码：PositionDeleteIndex的实现
class PositionDeleteIndex {
    // 使用Roaring Bitmap存储删除的位置（内存高效）
    private RoaringBitmap deletedPositions;

    boolean isDeleted(long position) {
        return deletedPositions.contains(position);
    }
}
```

**示例执行：**
```
假设DataFile有1000行，Position Delete标记了位置 [42, 156, 789]

读取流程：
Row 0:  positionIndex.isDeleted(0)   → false → 保留
Row 42: positionIndex.isDeleted(42)  → true  → 过滤 ✗
Row 156: positionIndex.isDeleted(156) → true  → 过滤 ✗
Row 789: positionIndex.isDeleted(789) → true  → 过滤 ✗
...
```

**性能优化点：**
1. **Roaring Bitmap**：稀疏位图，内存占用极小
2. **O(1)查找**：直接位运算判断
3. **批量加载**：一次性加载所有Position Delete构建索引

---

#### 2.4.4 Equality Delete应用机制

```java
// 源码：data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:181-214
private List<Predicate<T>> applyEqDeletes() {
    if (isInDeleteSets != null) {
        return isInDeleteSets;  // 缓存结果
    }

    isInDeleteSets = Lists.newArrayList();
    if (eqDeletes.isEmpty()) {
        return isInDeleteSets;
    }

    // 1. 按equalityFieldIds分组DeleteFile
    Multimap<Set<Integer>, DeleteFile> filesByDeleteIds =
        Multimaps.newMultimap(Maps.newHashMap(), Lists::newArrayList);
    for (DeleteFile delete : eqDeletes) {
        filesByDeleteIds.put(
            Sets.newHashSet(delete.equalityFieldIds()), delete);
    }

    // 2. 为每组equalityFieldIds构建StructLikeSet
    for (Map.Entry<Set<Integer>, Collection<DeleteFile>> entry :
            filesByDeleteIds.asMap().entrySet()) {
        Set<Integer> ids = entry.getKey();
        Iterable<DeleteFile> deletes = entry.getValue();

        Schema deleteSchema = TypeUtil.select(requiredSchema, ids);
        StructProjection projectRow =
            StructProjection.create(requiredSchema, deleteSchema);

        // 加载所有EqualityDelete构建HashSet
        StructLikeSet deleteSet =
            deleteLoader().loadEqualityDeletes(deletes, deleteSchema);

        // 3. 创建Predicate：判断行是否在deleteSet中
        Predicate<T> isInDeleteSet = record ->
            deleteSet.contains(projectRow.wrap(asStructLike(record)));

        isInDeleteSets.add(isInDeleteSet);
    }

    return isInDeleteSets;
}
```

**分组优化：**

假设有两个Equality DeleteFile：
```
DeleteFile1: equalityFieldIds = [1, 5]  // id, email
DeleteFile2: equalityFieldIds = [1, 5]  // 同样的字段
DeleteFile3: equalityFieldIds = [3]     // user_name

分组后：
Group1: [DeleteFile1, DeleteFile2] → 合并成一个StructLikeSet
Group2: [DeleteFile3]              → 独立的StructLikeSet
```

**执行示例：**
```
EqualityDelete数据（id, email）：
| id   | email              |
|------|--------------------|
| 1001 | alice@example.com |
| 1002 | bob@example.com   |

读取DataFile时：
Row: {id: 1001, email: alice@example.com, name: "Alice"}
  → projectRow提取 (1001, alice@example.com)
  → deleteSet.contains((1001, alice@example.com)) → true
  → 过滤 ✗

Row: {id: 1003, email: carol@example.com, name: "Carol"}
  → projectRow提取 (1003, carol@example.com)
  → deleteSet.contains((1003, carol@example.com)) → false
  → 保留 ✓
```

**性能考量：**
1. **StructLikeSet使用HashSet**：O(1)平均查找时间
2. **内存开销**：所有Equality Delete需要加载到内存
3. **分组优化**：相同equalityFieldIds共享一个Set

---

### 2.5 Schema投影与Delete协同

#### 2.5.1 requiredSchema计算

```java
// 源码：data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:267-318
private static Schema fileProjection(
    Schema tableSchema,
    Schema requestedSchema,  // 用户查询的列
    List<DeleteFile> posDeletes,
    List<DeleteFile> eqDeletes,
    boolean needRowPosCol) {

    Set<Integer> requiredIds = Sets.newLinkedHashSet();

    // 1. Position Delete需要 _pos 列
    if (needRowPosCol && !posDeletes.isEmpty()) {
        requiredIds.add(MetadataColumns.ROW_POSITION.fieldId());
    }

    // 2. Equality Delete需要其equalityFieldIds列
    for (DeleteFile eqDelete : eqDeletes) {
        requiredIds.addAll(eqDelete.equalityFieldIds());
    }

    // 3. 计算缺失的列（不在requestedSchema中但需要的）
    Set<Integer> missingIds = Sets.difference(
        requiredIds,
        TypeUtil.getProjectedIds(requestedSchema)
    );

    if (missingIds.isEmpty()) {
        return requestedSchema;  // 无需额外列
    }

    // 4. 添加缺失的列到Schema
    List<Types.NestedField> columns =
        Lists.newArrayList(requestedSchema.columns());
    for (int fieldId : missingIds) {
        if (fieldId == MetadataColumns.ROW_POSITION.fieldId()) {
            continue;  // _pos在最后添加
        }
        Types.NestedField field = tableSchema.asStruct().field(fieldId);
        columns.add(field);
    }

    // 5. 添加 _pos 元数据列（如果需要）
    if (missingIds.contains(MetadataColumns.ROW_POSITION.fieldId())) {
        columns.add(MetadataColumns.ROW_POSITION);
    }

    return new Schema(columns);
}
```

**示例场景：**

```sql
-- 用户查询
SELECT name, age FROM users;

-- 实际情况
Table Schema: (id, name, age, email, created_at)
Requested Schema: (name, age)
Equality DeleteFile: equalityFieldIds = [1, 4]  // id, email

-- 计算后的 requiredSchema
Required Schema: (name, age, id, email)  ← 添加了 id 和 email
```

**为什么需要额外列？**
- 用户只查询`name, age`
- 但Equality Delete依赖`id, email`进行匹配
- 必须从DataFile读取这些列才能判断行是否被删除

**性能影响：**
- 增加读取的列数 → 更多IO
- 这是Merge-on-Read的代价

---

#### 2.5.2 _pos元数据列

Iceberg虚拟列`_pos`记录行在文件中的位置：

```java
// 源码：api/src/main/java/org/apache/iceberg/MetadataColumns.java
public class MetadataColumns {
    public static final NestedField ROW_POSITION =
        NestedField.required(
            -1,
            "_pos",
            Types.LongType.get(),
            "Position in the file"
        );
}
```

**生成时机：**
- Parquet/ORC读取器在读取行时自动计算
- 从文件开头开始递增：0, 1, 2, 3, ...

**使用场景：**
```java
// Position Delete匹配逻辑
protected long pos(T record) {
    return (Long) posAccessor.get(asStructLike(record));
}

// 判断删除
positionIndex.isDeleted(pos(record))
```

---

### 2.6 SparkDeleteFilter特有优化

#### 2.6.1 Executor缓存机制

```java
// 源码：spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/BaseReader.java:230-236
@Override
protected DeleteLoader newDeleteLoader() {
    if (cacheDeleteFilesOnExecutors) {
        return new CachingDeleteLoader(this::loadInputFile);
    }
    return new BaseDeleteLoader(this::loadInputFile);
}
```

**CachingDeleteLoader特性：**
1. **Executor级别缓存**：同一Executor上多个Task共享DeleteFile
2. **基于Caffeine的容量/过期淘汰**：受最大权重和访问过期时间控制
3. **适用场景**：
   - 大量小DeleteFile
   - 多个DataFile引用相同DeleteFile
   - Executor内存充足

**配置参数（Spark Session）：**
```scala
spark.conf.set("spark.sql.iceberg.executor-cache.enabled", "true")
spark.conf.set("spark.sql.iceberg.executor-cache.delete-files.enabled", "true")
spark.conf.set("spark.sql.iceberg.executor-cache.max-entry-size", (64L * 1024 * 1024).toString)
spark.conf.set("spark.sql.iceberg.executor-cache.max-total-size", (128L * 1024 * 1024).toString)
```

**示例场景：**
```
Executor上有3个Task：
Task1: 读取 data-001.parquet + delete-001.parquet
Task2: 读取 data-002.parquet + delete-001.parquet  ← 缓存命中！
Task3: 读取 data-003.parquet + delete-001.parquet  ← 缓存命中！

无缓存：读取3次delete-001.parquet
有缓存：读取1次delete-001.parquet，节省2次IO
```

---

#### 2.6.2 Runtime Filter动态过滤

```java
// 源码：spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:127-164
@Override
public void filter(Predicate[] predicates) {
    Expression runtimeFilterExpr = convertRuntimeFilters(predicates);

    if (runtimeFilterExpr != Expressions.alwaysTrue()) {
        // 为每个PartitionSpec创建Evaluator
        Map<Integer, Evaluator> evaluatorsBySpecId = Maps.newHashMap();
        for (PartitionSpec spec : specs()) {
            Expression inclusiveExpr =
                Projections.inclusive(spec, caseSensitive())
                           .project(runtimeFilterExpr);
            Evaluator inclusive = new Evaluator(spec.partitionType(), inclusiveExpr);
            evaluatorsBySpecId.put(spec.specId(), inclusive);
        }

        // 过滤Task
        List<PartitionScanTask> filteredTasks = tasks().stream()
            .filter(task -> {
                Evaluator evaluator = evaluatorsBySpecId.get(task.spec().specId());
                return evaluator.eval(task.partition());
            })
            .collect(Collectors.toList());

        LOG.info("{} of {} task(s) for table {} matched runtime filter",
            filteredTasks.size(), tasks().size(), table().name(),
            ExpressionUtil.toSanitizedString(runtimeFilterExpr));

        if (filteredTasks.size() < tasks().size()) {
            resetTasks(filteredTasks);
        }
    }
}
```

**Runtime Filter工作原理：**

Spark在执行Join时动态生成过滤条件：

```sql
-- 查询
SELECT * FROM large_table l
JOIN small_table s ON l.partition_col = s.id
WHERE s.category = 'A';

-- 执行过程：
1. 先扫描 small_table，发现 category='A' 的 id IN (1, 5, 9)
2. 动态生成 RuntimeFilter: partition_col IN (1, 5, 9)
3. 将此Filter推送到 large_table 的 Scan
4. SparkBatchQueryScan.filter() 过滤掉不匹配的 FileScanTask
```

**优势：**
- 减少需要读取的DataFile数量
- 在Scan阶段就过滤，避免读取无用数据
- 对于分区表效果显著

---

`★ Insight ─────────────────────────────────────`
**1. FileScanTask的精妙设计**
通过封装DataFile和DeleteFile列表，Iceberg将Merge-on-Read的复杂性隐藏在Task内部，上层调度器只需要按Task分配即可，无需理解Delete逻辑。

**2. 两阶段过滤的效率权衡**
先Position后Equality的过滤顺序，本质是用确定性换性能：Position Delete精确到行号（确定性高），Equality Delete基于值匹配（确定性低），先过滤高确定性的能更快缩小数据集。

**3. Schema投影的额外代价**
Equality Delete强制读取额外列是Merge-on-Read的本质代价。这也解释了为什么频繁Update场景下，定期Compaction很重要——将Delete合并回DataFile可以消除这种开销。
`─────────────────────────────────────────────────`

---

## 第二章总结

本章深入分析了Spark读取Iceberg的核心机制：

✅ **FileScanTask结构**：封装DataFile + DeleteFile[]的关联关系
✅ **关联策略**：Position Delete按path/分区匹配，Equality Delete按候选文件匹配
✅ **Merge-on-Read实现**：SparkDeleteFilter的两阶段过滤链
✅ **Schema协同**：requiredSchema自动添加Delete所需的额外列
✅ **Spark优化**：Executor缓存与Runtime Filter动态过滤

**关键性能点：**
- Position Delete查找：O(1) Bitmap查找
- Equality Delete匹配：O(1) HashSet查找
- 额外列读取：Equality Delete带来的IO开销
- 缓存优化：减少重复读取DeleteFile

**下一章预告：** 我们将分析Flink流式读取机制，重点关注Incremental读取如何追踪快照变化，以及流式场景下Delete的处理策略。

---

---

## 第三章：Flink流式读取机制

### 3.1 Flink流式读取概述

Flink与Spark的最大区别在于其**原生流处理能力**，Iceberg提供了两种Flink读取模式：

| 模式           | 特点                                | 使用场景                     |
|----------------|-------------------------------------|------------------------------|
| **Batch模式**  | 读取单个Snapshot的完整数据          | 批处理、历史数据分析         |
| **Streaming模式** | 持续监控新Snapshot，增量读取      | 实时数据管道、CDC场景        |

**核心架构：**

```
┌─────────────────────────────────────────────────────────┐
│              Flink Streaming Job                         │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌─────────────────────────────────────────────────┐   │
│  │   ContinuousIcebergEnumerator (Coordinator)     │   │
│  │   ┌──────────────────────────────────────────┐  │   │
│  │   │ 1. 周期性监控Table (每N秒)              │  │   │
│  │   │ 2. 检测新Snapshot                        │  │   │
│  │   │ 3. Incremental Scan (lastSnapshotId+1)  │  │   │
│  │   │ 4. 生成IcebergSourceSplit               │  │   │
│  │   │ 5. 分配到Reader                          │  │   │
│  │   └──────────────────────────────────────────┘  │   │
│  │                     │                             │   │
│  │                     │ Splits                      │   │
│  │                     ▼                             │   │
│  │   ┌──────────────────────────────────────────┐  │   │
│  │   │   IcebergSourceReader (Parallel)         │  │   │
│  │   │   ┌──────────────────────────────────┐   │  │   │
│  │   │   │ 读取DataFile                     │   │  │   │
│  │   │   │ 应用DeleteFile (同Spark逻辑)     │   │  │   │
│  │   │   │ 输出RowData流                    │   │  │   │
│  │   │   └──────────────────────────────────┘   │  │   │
│  │   └──────────────────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  Checkpoint机制：                                        │
│  - Enumerator State: lastSnapshotId                      │
│  - Reader State: 未完成的Split offset                    │
└─────────────────────────────────────────────────────────┘
```

**新旧API对比：**

| 特性               | 旧API (FlinkSource)           | 新API (IcebergSource)         |
|--------------------|-------------------------------|-------------------------------|
| 实现标准           | SourceFunction (deprecated)   | FLIP-27 Source (推荐)         |
| 监控组件           | StreamingMonitorFunction      | ContinuousIcebergEnumerator   |
| 并行度控制         | Monitor单并行，Reader并行     | Enumerator单并行，Reader并行  |
| Checkpoint支持     | CheckpointedFunction          | SplitEnumerator接口           |
| Flink版本          | 1.11+                         | 1.13+                         |

---

### 3.2 Incremental读取核心：IncrementalAppendScan

#### 3.2.1 IncrementalAppendScan接口

```java
// 源码：api/src/main/java/org/apache/iceberg/IncrementalAppendScan.java
public interface IncrementalAppendScan
    extends IncrementalScan<IncrementalAppendScan, FileScanTask, CombinedScanTask> {}
```

**关键特性：**
1. **Exclusive起点**：`fromSnapshotExclusive`表示从该Snapshot之后开始（不包含）
2. **Inclusive终点**：`toSnapshot`表示读到该Snapshot为止（包含）
3. **仅读取APPEND操作**：忽略DELETE/OVERWRITE等操作的Snapshot

---

#### 3.2.2 appendsBetween实现

```java
// 源码：core/src/main/java/org/apache/iceberg/BaseIncrementalAppendScan.java:104-116
private static List<Snapshot> appendsBetween(
    Table table, Long fromSnapshotIdExclusive, long toSnapshotIdInclusive) {

    List<Snapshot> snapshots = Lists.newArrayList();

    // 遍历从 toSnapshotId 到 fromSnapshotId 之间的所有Snapshot
    for (Snapshot snapshot :
            SnapshotUtil.ancestorsBetween(
                toSnapshotIdInclusive, fromSnapshotIdExclusive, table::snapshot)) {

        // 仅保留APPEND操作的Snapshot
        if (snapshot.operation().equals(DataOperations.APPEND)) {
            snapshots.add(snapshot);
        }
    }

    return snapshots;
}
```

**操作类型过滤：**

```java
// Iceberg操作类型
public class DataOperations {
    public static final String APPEND = "append";       // INSERT
    public static final String REPLACE = "replace";     // INSERT OVERWRITE
    public static final String OVERWRITE = "overwrite"; // DELETE + INSERT
    public static final String DELETE = "delete";       // DELETE
}
```

**为什么只读APPEND？**
- **APPEND**：新增数据文件，增量读取的核心
- **REPLACE/OVERWRITE**：替换或覆盖现有数据，可能导致重复读取
- **DELETE**：仅产生DeleteFile，没有新DataFile

**示例场景：**
```
Snapshot链：
Snapshot 100 (APPEND)    ← 初始状态
    ↓
Snapshot 101 (APPEND)    ← 新增100行
    ↓
Snapshot 102 (DELETE)    ← 删除10行（仅DeleteFile）
    ↓
Snapshot 103 (APPEND)    ← 新增50行
    ↓
Snapshot 104 (OVERWRITE) ← 覆盖分区（危险！）

Incremental Scan (100, 104]:
  ✅ 包含: Snapshot 101 (100行新数据)
  ✗  忽略: Snapshot 102 (DELETE操作)
  ✅ 包含: Snapshot 103 (50行新数据)
  ✗  忽略: Snapshot 104 (OVERWRITE不安全)

结果: 读取150行新增数据
```

---

#### 3.2.3 Manifest过滤策略

```java
// 源码：core/src/main/java/org/apache/iceberg/BaseIncrementalAppendScan.java:68-98
private CloseableIterable<FileScanTask> appendFilesFromSnapshots(
    List<Snapshot> snapshots) {

    // 1. 收集所有相关Snapshot的ID
    Set<Long> snapshotIds = Sets.newHashSet(
        Iterables.transform(snapshots, Snapshot::snapshotId));

    // 2. 收集所有DataManifest，并过滤出属于这些Snapshot的
    Set<ManifestFile> manifests =
        FluentIterable.from(snapshots)
            .transformAndConcat(snapshot -> snapshot.dataManifests(table().io()))
            .filter(manifestFile -> snapshotIds.contains(manifestFile.snapshotId()))
            .toSet();

    // 3. 创建ManifestGroup并过滤ManifestEntry
    ManifestGroup manifestGroup =
        new ManifestGroup(table().io(), manifests)
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())
            .filterManifestEntries(manifestEntry ->
                // 仅读取ADDED状态的Entry
                snapshotIds.contains(manifestEntry.snapshotId()) &&
                manifestEntry.status() == ManifestEntry.Status.ADDED)
            .specsById(table().specs())
            .ignoreDeleted()  // 忽略DELETED Entry
            .columnsToKeepStats(columnsToKeepStats());

    return manifestGroup.planFiles();
}
```

**过滤逻辑解析：**

1. **Manifest级别过滤**：
   ```java
   .filter(manifestFile -> snapshotIds.contains(manifestFile.snapshotId()))
   ```
   - 仅读取属于目标Snapshot的Manifest
   - 避免读取历史Manifest（复用的EXISTING Entry）

2. **Entry级别过滤**：
   ```java
   manifestEntry.status() == ManifestEntry.Status.ADDED
   ```
   - `ADDED`：当前Snapshot新增的文件 → **读取**
   - `EXISTING`：从上一个Snapshot继承 → **忽略**
   - `DELETED`：当前Snapshot删除的文件 → **忽略**

**示例：**
```
Snapshot 101:
  Manifest-A:
    - DataFile-1 (ADDED)     ← 读取 ✅
    - DataFile-2 (ADDED)     ← 读取 ✅

Snapshot 102 (复用Manifest-A):
  Manifest-A:
    - DataFile-1 (EXISTING)  ← 忽略 ✗ (已在101读取)
    - DataFile-2 (EXISTING)  ← 忽略 ✗
  Manifest-B:
    - DataFile-3 (ADDED)     ← 读取 ✅ (新增)

Incremental Scan (100, 102]:
  - 读取Manifest-A的ADDED Entry (在Snapshot 101)
  - 读取Manifest-B的ADDED Entry (在Snapshot 102)
  - 结果：DataFile-1, DataFile-2, DataFile-3
```

---

### 3.3 StreamingMonitorFunction：旧API流式监控

#### 3.3.1 监控循环

```java
// 源码：flink/v1.20/.../StreamingMonitorFunction.java:165-171
@Override
public void run(SourceContext<FlinkInputSplit> ctx) throws Exception {
    this.sourceContext = ctx;
    while (isRunning) {
        monitorAndForwardSplits();  // 监控并发现Split
        Thread.sleep(scanContext.monitorInterval().toMillis());  // 休眠N秒
    }
}
```

**配置参数：**
```sql
-- Flink SQL示例
CREATE TABLE iceberg_table (
  id BIGINT,
  data STRING
) WITH (
  'connector' = 'iceberg',
  'streaming' = 'true',
  'monitor-interval' = '10s'  ← 每10秒检查一次新Snapshot
);
```

---

#### 3.3.2 核心发现逻辑

```java
// 源码：flink/v1.20/.../StreamingMonitorFunction.java:192-239
void monitorAndForwardSplits() {
    // 1. 刷新Table获取最新Snapshot
    table.refresh();

    Snapshot snapshot = scanContext.branch() != null
        ? table.snapshot(scanContext.branch())
        : table.currentSnapshot();

    // 2. 检查是否有新Snapshot
    if (snapshot != null && snapshot.snapshotId() != lastSnapshotId) {
        long snapshotId = snapshot.snapshotId();

        ScanContext newScanContext;
        if (lastSnapshotId == INIT_LAST_SNAPSHOT_ID) {
            // 首次启动：读取当前Snapshot的完整数据
            newScanContext = scanContext.copyWithSnapshotId(snapshotId);
        } else {
            // 增量读取：从lastSnapshotId到当前Snapshot
            snapshotId = toSnapshotIdInclusive(
                lastSnapshotId, snapshotId,
                scanContext.maxPlanningSnapshotCount());
            newScanContext = scanContext.copyWithAppendsBetween(
                lastSnapshotId, snapshotId);
        }

        // 3. 计划Split
        FlinkInputSplit[] splits =
            FlinkSplitPlanner.planInputSplits(table, newScanContext, workerPool);

        // 4. 在Checkpoint锁保护下发送Split并更新状态
        synchronized (sourceContext.getCheckpointLock()) {
            for (FlinkInputSplit split : splits) {
                sourceContext.collect(split);  // 发送到下游Reader
            }
            lastSnapshotId = snapshotId;  // 更新lastSnapshotId
        }
    }
}
```

**关键点解析：**

1. **首次启动逻辑**：
   ```java
   if (lastSnapshotId == INIT_LAST_SNAPSHOT_ID) {
       // 读取当前完整Snapshot
       newScanContext = scanContext.copyWithSnapshotId(snapshotId);
   }
   ```
   - 适用场景：新作业启动
   - 行为：等同于Batch读取当前Snapshot

2. **增量读取逻辑**：
   ```java
   newScanContext = scanContext.copyWithAppendsBetween(
       lastSnapshotId,  // Exclusive
       snapshotId       // Inclusive
   );
   ```
   - 仅读取两个Snapshot之间的APPEND操作

3. **Throttling机制**：
   ```java
   snapshotId = toSnapshotIdInclusive(
       lastSnapshotId, snapshotId,
       scanContext.maxPlanningSnapshotCount());
   ```
   - 限制单次Planning的Snapshot数量
   - 避免积压过多Snapshot导致OOM

---

#### 3.3.3 Checkpoint与状态恢复

```java
// 源码：flink/v1.20/.../StreamingMonitorFunction.java:159-162
@Override
public void snapshotState(FunctionSnapshotContext context) throws Exception {
    lastSnapshotIdState.clear();
    lastSnapshotIdState.add(lastSnapshotId);  // 保存lastSnapshotId到State
}

@Override
public void initializeState(FunctionInitializationContext context) throws Exception {
    // ... 初始化Table ...

    lastSnapshotIdState = context.getOperatorStateStore()
        .getListState(new ListStateDescriptor<>("snapshot-id-state", LongSerializer.INSTANCE));

    // 从State恢复lastSnapshotId
    if (context.isRestored()) {
        LOG.info("Restoring state for the {}.", getClass().getSimpleName());
        lastSnapshotId = lastSnapshotIdState.get().iterator().next();
    }
}
```

**状态恢复示例：**

```
时间线：
T1: Job启动，lastSnapshotId = -1
T2: 发现Snapshot 100，读取完整数据，lastSnapshotId = 100
T3: 发现Snapshot 101，增量读取 (100, 101]，lastSnapshotId = 101
T4: Checkpoint触发，保存 lastSnapshotId = 101
T5: Job失败
T6: 从Checkpoint恢复，lastSnapshotId = 101
T7: 发现Snapshot 103，增量读取 (101, 103]
```

**Exactly-Once语义保证：**
1. **Split发送与状态更新原子性**：
   ```java
   synchronized (sourceContext.getCheckpointLock()) {
       for (FlinkInputSplit split : splits) {
           sourceContext.collect(split);
       }
       lastSnapshotId = snapshotId;  // 状态更新
   }
   ```
   - 在Checkpoint锁保护下，Split发送和状态更新是原子的
   - 确保不会丢失或重复Split

2. **Checkpoint Barrier对齐**：
   - Flink的Barrier机制确保下游Reader也会在相同点Checkpoint
   - 恢复时，Monitor和Reader都回到一致状态

---

### 3.4 ContinuousIcebergEnumerator：新API流式监控

#### 3.4.1 FLIP-27架构优势

```
旧API (SourceFunction):
  Monitor (单并行) → Reader (并行)
  问题：Split发送通过collect()，无法精确控制分配

新API (FLIP-27 Source):
  Enumerator (单并行) → SplitAssigner → Reader (并行)
  优势：Split分配策略可定制，支持动态平衡
```

#### 3.4.2 Split发现异步化

```java
// 源码：flink/v1.20/.../ContinuousIcebergEnumerator.java:92-99
@Override
public void start() {
    super.start();
    enumeratorContext.callAsync(
        this::discoverSplits,          // IO密集任务在线程池执行
        this::processDiscoveredSplits, // 结果在Coordinator线程处理
        0L,                             // 初始延迟0ms
        scanContext.monitorInterval().toMillis()  // 周期N毫秒
    );
}
```

**关键优势：**
- `discoverSplits()`在IO线程池执行，不阻塞Coordinator
- `processDiscoveredSplits()`在单线程执行，无需同步锁

---

#### 3.4.3 Split发现逻辑

```java
// 源码：flink/v1.20/.../ContinuousIcebergEnumerator.java:119-133
private ContinuousEnumerationResult discoverSplits() {
    int pendingSplitCountFromAssigner = assigner.pendingSplitCount();

    // Throttling：如果Assigner积压太多Split，暂停发现
    if (enumerationHistory.shouldPauseSplitDiscovery(pendingSplitCountFromAssigner)) {
        LOG.info(
            "Pause split discovery as the assigner already has too many pending splits: {}",
            pendingSplitCountFromAssigner);
        return new ContinuousEnumerationResult(
            Collections.emptyList(),
            enumeratorPosition.get(),
            enumeratorPosition.get());
    } else {
        return splitPlanner.planSplits(enumeratorPosition.get());
    }
}
```

**Throttling策略：**

```java
class EnumerationHistory {
    // 最近N次发现的Split数量
    private final List<Integer> history;

    boolean shouldPauseSplitDiscovery(int pendingSplitCount) {
        // 如果Assigner积压的Split超过历史平均值的3倍，暂停发现
        int avgSplitsPerCycle = calculateAverage(history);
        return pendingSplitCount > avgSplitsPerCycle * 3;
    }
}
```

**作用：**
- 防止上游数据写入过快，下游消费不及时
- 避免Enumerator State膨胀（包含所有未分配的Split）

---

#### 3.4.4 位置追踪

```java
// IcebergEnumeratorPosition记录当前位置
class IcebergEnumeratorPosition {
    private final Long snapshotId;        // 当前Snapshot ID
    private final Long snapshotTimestamp; // Snapshot时间戳
    // ... 其他元数据 ...
}
```

**处理发现结果：**

```java
// 源码：flink/v1.20/.../ContinuousIcebergEnumerator.java:136-187
private void processDiscoveredSplits(ContinuousEnumerationResult result, Throwable error) {
    if (error == null) {
        consecutiveFailures = 0;

        // CAS检查：仅接受匹配当前位置的结果
        if (!Objects.equals(result.fromPosition(), enumeratorPosition.get())) {
            LOG.info(
                "Skip {} discovered splits because the scan starting position doesn't match",
                result.splits().size());
        } else {
            if (!result.splits().isEmpty()) {
                assigner.onDiscoveredSplits(result.splits());  // 分配Split
                enumerationHistory.add(result.splits().size());
                LOG.info(
                    "Added {} splits discovered between ({}, {}]",
                    result.splits().size(),
                    result.fromPosition(),
                    result.toPosition());
            }

            // 更新位置（即使没有发现Split也更新）
            enumeratorPosition.set(result.toPosition());
        }
    } else {
        consecutiveFailures++;
        if (consecutiveFailures > scanContext.maxAllowedPlanningFailures()) {
            throw new RuntimeException("Failed to discover new splits", error);
        }
    }
}
```

**CAS机制原因：**
- 多个`discoverSplits()`可能并发执行
- 仅接受基于当前位置的发现结果，忽略过时的

---

### 3.5 流式场景下DeleteFile处理边界

#### 核心结论：APPEND快照本身不新增DeleteFile，但Reader仍支持Delete过滤

**源码依据：** `BaseRowDelta.java` 第49-60行

```java
// 源码：core/src/main/java/org/apache/iceberg/BaseRowDelta.java:49-60
@Override
protected String operation() {
    // 只有添加DataFile，没有添加DeleteFile，也没有删除DataFile
    if (addsDataFiles() && !addsDeleteFiles() && !deletesDataFiles()) {
        return DataOperations.APPEND;  // ← 纯新增才是APPEND
    }

    // 只有添加DeleteFile，没有添加DataFile
    if (addsDeleteFiles() && !addsDataFiles()) {
        return DataOperations.DELETE;   // ← 纯删除
    }

    // 其他情况（既有DataFile又有DeleteFile）
    return DataOperations.OVERWRITE;    // ← 混合操作
}
```

**结合 `DataOperations.java` 的注释：**
```java
// 源码：api/src/main/java/org/apache/iceberg/DataOperations.java:31-36
/**
 * New data is appended to the table and no data is removed or deleted.
 *                                    ↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑
 * This operation is implemented by {@link AppendFiles}.
 */
public static final String APPEND = "append";
```

---

#### 快照类型与内容对照表

| 场景 | 包含DataFile | 包含DeleteFile | 快照类型 |
|------|-------------|----------------|---------|
| 纯INSERT | ✅ | ❌ | **APPEND** |
| 纯DELETE | ❌ | ✅ | **DELETE** |
| INSERT + DELETE/UPDATE | ✅ | ✅ | **OVERWRITE** |
| Compaction | 替换 | 可能 | **REPLACE** |

---

#### 流式读取的行为

流式增量规划主要基于`IncrementalAppendScan`，因此只消费APPEND快照中的新增DataFile。

但需要注意两点：

1. **Reader侧总是具备DeleteFilter能力**
   - Flink `RowDataFileScanTaskReader` 会创建并调用 `DeleteFilter`
   - 是否真正生效取决于该`FileScanTask`是否携带`task.deletes()`

2. **流式启动策略会影响是否出现DeleteFile**
   - 纯增量APPEND阶段通常`DeleteFile[]`为空
   - 首次全量扫描（如旧API初次`copyWithSnapshotId`，或`TABLE_SCAN_THEN_INCREMENTAL`的初始scan）可能包含DeleteFile

---

#### 关键理解：SequenceNumber机制

即使读取的是APPEND快照中的新DataFile，是否需要应用**之前快照**的DeleteFile？

**通常不需要**，原因是SequenceNumber机制：
- 新DataFile的`dataSequenceNumber`通常大于历史DeleteFile
- Position Delete仅影响`dataSeq <= deleteSeq`
- Equality Delete仅影响`dataSeq < deleteSeq`
- 因此旧Delete通常不会影响新APPEND数据

```
示例：
Snapshot 100 (OVERWRITE): DeleteFile-1 (dataSeq=100)
Snapshot 101 (APPEND):    DataFile-1 (dataSeq=101)

流式读取Snapshot 101：
  - DataFile-1 的 dataSeq=101 > DeleteFile-1 的 dataSeq=100
  - DeleteFile-1 不影响 DataFile-1
  - 无需加载DeleteFile-1
```

---

#### Flink CDC写入时的快照类型

如果Flink CDC在同一个Checkpoint中既有INSERT又有DELETE/UPDATE：
- 快照类型通常是**OVERWRITE**（取决于提交类型）
- 对`IncrementalAppendScan`而言，这类非APPEND快照不会产出增量append splits

如果Flink CDC只有INSERT（无DELETE/UPDATE）：
- 快照类型才是**APPEND**
- 该快照会被流式读取正常处理

---

`★ Insight ─────────────────────────────────────`
**1. Incremental读取的精妙过滤**
通过`ADDED` Entry过滤 + `APPEND`操作过滤的双重策略，Iceberg确保增量读取不会重复消费数据，这是流式处理Exactly-Once的基础。

**2. APPEND快照的纯净性**
APPEND快照定义为“仅新增数据、不删除数据”。但这不等于“流式Reader永不处理Delete”；
Reader逻辑仍会按`FileScanTask`携带的DeleteFile执行过滤。

**3. 流式读取的局限性**
基于`IncrementalAppendScan`的流式增量主路径只消费APPEND新增；
若需要覆盖DELETE/UPDATE的完整变更语义，需引入专门CDC方案或额外变更建模。
`─────────────────────────────────────────────────`

---

## 第三章总结

本章深入分析了Flink流式读取Iceberg的核心机制：

✅ **Incremental读取**：IncrementalAppendScan仅读取APPEND操作的ADDED Entry
✅ **旧API监控**：StreamingMonitorFunction周期性轮询+Checkpoint保存lastSnapshotId
✅ **新API监控**：ContinuousIcebergEnumerator异步发现+Throttling+CAS位置追踪
✅ **状态恢复**：lastSnapshotId确保Exactly-Once语义
✅ **APPEND快照特性**：APPEND快照不新增DeleteFile；但Reader仍可对携带Delete的任务应用过滤

**性能关键点：**
- 监控间隔：过短增加Planning开销，过长影响实时性
- Throttling：防止Split积压导致State膨胀
- Manifest复用：EXISTING Entry避免重复读取

**下一章预告：** 我们将深入分析DeleteFile的两种类型（Position/Equality）的存储格式、Delete文件过多的性能影响，以及优化策略（Compaction、Rewrite Deletes）。

---

---

## 第四章：DeleteFile深度解析

### 4.1 Position Delete存储格式

#### 4.1.1 Schema定义

```java
// Position Delete文件的Schema
Schema POSITION_DELETE_SCHEMA = new Schema(
    required(2147483546, "file_path", Types.StringType.get()),  // 数据文件路径
    required(2147483545, "pos", Types.LongType.get())           // 行位置（从0开始）
);
```

**字段说明：**
- **file_path**：被删除行所在的DataFile完整路径
  - 示例：`s3://bucket/db/table/data/part=2024-01-01/data-00001.parquet`
  - 作用：关联到特定DataFile

- **pos**：行在DataFile中的位置（基于0的索引）
  - 范围：`[0, recordCount-1]`
  - 作用：精确定位被删除的行

**历史演进（Deprecated）：**

```java
// 旧版本Schema（已废弃）
Schema OLD_POSITION_DELETE_SCHEMA = new Schema(
    required(2147483546, "file_path", Types.StringType.get()),
    required(2147483545, "pos", Types.LongType.get()),
    optional(2147483544, "row", ...)  // ← 已废弃：存储被删除行的完整数据
);
```

**为什么废弃`row`字段？**
- **存储膨胀**：存储完整行数据导致DeleteFile过大
- **性能影响**：读取和反序列化开销增加
- **实用性低**：大部分场景只需要知道"哪些行被删除"，不需要"被删除的行是什么"
- **替代方案**：需要审计可以通过Change Data Feed实现

**源码确认：**
```java
// 源码：core/src/main/java/org/apache/iceberg/deletes/PositionDelete.java:42-52
/**
 * @deprecated This method is deprecated as of version 1.11.0 and will be removed in 1.12.0.
 * Position deletes that include row data are no longer supported.
 */
@Deprecated
public PositionDelete<R> set(CharSequence newPath, long newPos, R newRow) {
    this.path = newPath;
    this.pos = newPos;
    this.row = newRow;  // ← 不再使用
    return this;
}
```

---

#### 4.1.2 存储示例

**场景：删除3行数据**

```
DataFile: s3://bucket/table/data/data-00001.parquet (1000行)
  删除行: 位置 42, 156, 789

Position Delete文件内容 (Parquet格式):
┌──────────────────────────────────────────────┬──────┐
│ file_path                                    │ pos  │
├──────────────────────────────────────────────┼──────┤
│ s3://bucket/table/data/data-00001.parquet   │  42  │
│ s3://bucket/table/data/data-00001.parquet   │ 156  │
│ s3://bucket/table/data/data-00001.parquet   │ 789  │
└──────────────────────────────────────────────┴──────┘

文件大小：~2KB (取决于Parquet压缩)
```

**多DataFile场景：**

```
Position Delete文件内容:
┌──────────────────────────────────────────────┬──────┐
│ file_path                                    │ pos  │
├──────────────────────────────────────────────┼──────┤
│ s3://bucket/table/data/data-00001.parquet   │  42  │
│ s3://bucket/table/data/data-00001.parquet   │ 156  │
│ s3://bucket/table/data/data-00002.parquet   │   7  │
│ s3://bucket/table/data/data-00002.parquet   │  89  │
│ s3://bucket/table/data/data-00003.parquet   │  12  │
└──────────────────────────────────────────────┴──────┘

关联到3个DataFile
```

---

#### 4.1.3 BitmapPositionDeleteIndex内存结构

读取时，Iceberg将Position Delete加载为高效的内存索引：

```java
// 源码：core/src/main/java/org/apache/iceberg/deletes/BitmapPositionDeleteIndex.java:31-44
class BitmapPositionDeleteIndex implements PositionDeleteIndex {
    private final RoaringPositionBitmap bitmap;  // Roaring Bitmap压缩位图
    private final List<DeleteFile> deleteFiles;  // 来源DeleteFile列表

    @Override
    public boolean isDeleted(long position) {
        return bitmap.contains(position);  // O(1)查找
    }

    @Override
    public void delete(long position) {
        bitmap.set(position);  // 标记删除
    }
}
```

**Roaring Bitmap特性：**

1. **压缩存储**：
   ```
   假设删除位置: [42, 43, 44, 45, ..., 99, 156, 789, 1000000]

   朴素Bitmap：需要1000000 bits = 125KB
   Roaring Bitmap：通过Run-Length Encoding，仅需~1KB
   ```

2. **运算高效**：
   ```java
   bitmap1.setAll(bitmap2);  // 合并：O(N)，N为chunk数量
   bitmap.contains(pos);      // 查找：O(1)
   bitmap.setRange(start, end); // 范围设置：O(logN)
   ```

3. **序列化支持**：
   ```java
   // 源码：BitmapPositionDeleteIndex.java:125-137
   public ByteBuffer serialize() {
       bitmap.runLengthEncode();  // Run-Length编码优化
       int bitmapDataLength = computeBitmapDataLength(bitmap);
       byte[] bytes = new byte[LENGTH + bitmapDataLength + CRC];

       // 格式：[长度(4B)] [魔数(4B)] [Bitmap数据] [CRC校验(4B)]
       buffer.putInt(bitmapDataLength);
       serializeBitmapData(bytes, bitmapDataLength, bitmap);
       int crc = computeChecksum(bytes, bitmapDataLength);
       buffer.putInt(crcOffset, crc);

       return buffer;
   }
   ```

**序列化格式：**
```
┌──────────┬─────────────┬────────────────────┬──────────────┐
│  Length  │ Magic Number│   Bitmap Data      │   CRC-32     │
│  (4 Bytes)│  (4 Bytes)  │  (Variable Size)   │  (4 Bytes)   │
└──────────┴─────────────┴────────────────────┴──────────────┘
```

---

### 4.2 Equality Delete存储格式

#### 4.2.1 Schema定义

Equality Delete的Schema**由用户定义**，通过`equalityFieldIds`指定：

```java
// 示例：基于(id, email)的Equality Delete
Schema EQUALITY_DELETE_SCHEMA = new Schema(
    required(1, "id", Types.IntegerType.get()),
    required(5, "email", Types.StringType.get())
);

DeleteFile {
    content: EQUALITY_DELETES
    equalityFieldIds: [1, 5]  // 指定用于等值比较的列
    // ... 其他元数据 ...
}
```

**可选附加列：**

虽然只有`equalityFieldIds`用于匹配，但DeleteFile可以包含额外列：

```java
// 扩展Schema：包含审计信息
Schema EXTENDED_EQUALITY_DELETE_SCHEMA = new Schema(
    required(1, "id", Types.IntegerType.get()),        // Equality字段
    required(5, "email", Types.StringType.get()),      // Equality字段
    optional(10, "deleted_by", Types.StringType.get()), // 附加：操作人
    optional(11, "deleted_at", Types.TimestampType.withZone()) // 附加：删除时间
);

DeleteFile {
    equalityFieldIds: [1, 5]  // 仅id和email用于匹配
}
```

**附加列的作用：**
- **审计日志**：记录删除操作的元数据
- **数据重建**：恢复被删除前的数据状态
- **统计分析**：分析删除模式和趋势

---

#### 4.2.2 存储示例

**场景：GDPR数据删除**

```sql
-- 业务需求：删除特定用户的所有数据
DELETE FROM users WHERE id IN (1001, 1002, 1003);
```

**Equality Delete文件内容：**

```
┌──────┬─────────────────────┐
│  id  │       email         │
├──────┼─────────────────────┤
│ 1001 │ alice@example.com  │
│ 1002 │ bob@example.com    │
│ 1003 │ carol@example.com  │
└──────┴─────────────────────┘

equalityFieldIds: [1, 5]  // id + email
recordCount: 3
fileSizeInBytes: ~5KB
```

**读取时匹配逻辑：**

```java
// 伪代码：Equality Delete匹配
StructLikeSet deleteSet = loadEqualityDeletes(deleteFile);
// deleteSet包含: {(1001, "alice@example.com"), (1002, "bob@example.com"), ...}

for (Row row : dataFile) {
    StructLike projected = projectToEqualityFields(row);
    // projected = (row.id, row.email)

    if (deleteSet.contains(projected)) {
        // 匹配成功，过滤掉该行
        continue;
    }

    emit(row);  // 输出行
}
```

---

### 4.3 DeleteGranularity：删除粒度策略

#### 4.3.1 两种粒度对比

```java
// 源码：core/src/main/java/org/apache/iceberg/deletes/DeleteGranularity.java:45-47
public enum DeleteGranularity {
    FILE,       // 文件粒度：每个DataFile对应独立的DeleteFile
    PARTITION;  // 分区粒度：一个分区的所有DataFile共享DeleteFile
}
```

**配置方式：**
```sql
-- 表属性设置
ALTER TABLE my_table SET TBLPROPERTIES (
  'write.delete.granularity' = 'file'  -- 或 'partition'
);
```

---

#### 4.3.2 FILE粒度

**特征：**
```
Partition: date=2024-01-01
  DataFile-1 → DeleteFile-1 (仅删除DataFile-1的行)
  DataFile-2 → DeleteFile-2 (仅删除DataFile-2的行)
  DataFile-3 → DeleteFile-3 (仅删除DataFile-3的行)

DeleteFile-1.referencedDataFile() = "path/to/DataFile-1"
```

**优点：**
1. **读取精准**：
   ```
   读取DataFile-1时：
     → 仅加载DeleteFile-1
     → 无需加载DeleteFile-2, DeleteFile-3
     → 减少IO和内存
   ```

2. **Planning高效**：
   - `referencedDataFile()`直接指定关联
   - 无需扫描DeleteFile内容确定关联关系

3. **并行写入友好**：
   - 不同Task写入不同DataFile的Delete互不冲突

**缺点：**
1. **DeleteFile数量多**：
   ```
   1000个DataFile → 1000个DeleteFile
   → Manifest膨胀
   → Planning扫描慢
   ```

2. **小文件问题**：
   ```
   单个DataFile仅删除几行 → DeleteFile几KB
   → 大量小文件影响文件系统性能
   ```

---

#### 4.3.3 PARTITION粒度

**特征：**
```
Partition: date=2024-01-01
  DataFile-1 ┐
  DataFile-2 ├─→ DeleteFile-Partition (包含所有3个DataFile的删除)
  DataFile-3 ┘

DeleteFile-Partition.referencedDataFile() = null
```

**DeleteFile内容：**
```
┌─────────────────────┬──────┐
│ file_path           │ pos  │
├─────────────────────┼──────┤
│ .../DataFile-1      │  42  │  ← DataFile-1的删除
│ .../DataFile-1      │ 156  │
│ .../DataFile-2      │   7  │  ← DataFile-2的删除
│ .../DataFile-3      │  89  │  ← DataFile-3的删除
└─────────────────────┴──────┘
```

**优点：**
1. **DeleteFile数量少**：
   ```
   1000个DataFile → 10个分区 → 10个DeleteFile
   → Manifest紧凑
   → Planning快速
   ```

2. **批量写入高效**：
   - 单个Task可以累积多个DataFile的删除
   - 减少Commit次数

**缺点：**
1. **读取冗余**：
   ```
   读取DataFile-1时：
     → 必须加载整个DeleteFile-Partition
     → 包含DataFile-2和DataFile-3的删除（无用）
     → 增加IO和内存
   ```

2. **Planning复杂**：
   - `referencedDataFile() = null`
   - 需要扫描DeleteFile的`file_path`列确定关联

3. **并发写入冲突**：
   - 多个Task写入同一分区时可能产生冲突

---

#### 4.3.4 选择建议

| 场景                     | 推荐粒度   | 理由                                      |
|--------------------------|------------|-------------------------------------------|
| **高频UPDATE/DELETE**    | FILE       | 减少读取时的冗余Delete加载                |
| **大分区表**             | FILE       | 避免单个DeleteFile过大                    |
| **小分区表**             | PARTITION  | 减少DeleteFile数量，简化Planning          |
| **流式写入（CDC）**      | FILE       | 并行写入不冲突                            |
| **批量删除（GDPR）**     | PARTITION  | 删除集中在少数分区，合并高效              |
| **读多写少**             | FILE       | 优化读取性能                              |
| **写多读少**             | PARTITION  | 减少元数据维护开销                        |

**混合策略：**
```sql
-- 写入时使用PARTITION粒度（减少文件数）
SET write.delete.granularity = 'partition';

-- 定期Compaction时转换为FILE粒度（优化读取）
CALL rewrite_position_delete_files(
  granularity => 'file'
);
```

---

### 4.4 Delete文件过多的影响

#### 4.4.1 Planning阶段影响

**问题：Manifest扫描变慢**

```
假设表有10,000个DataFile和5,000个DeleteFile：

Planning流程：
1. 读取Manifest List (假设10个Manifest)
2. 读取10个Data Manifest → 扫描10,000个DataFile Entry
3. 读取10个Delete Manifest → 扫描5,000个DeleteFile Entry  ← 额外开销
4. 构建FileScanTask (关联DataFile和DeleteFile)

时间开销：
  - 无Delete：~2秒
  - 有5000个Delete：~5秒  ← 增加150%
```

**根因分析：**
```java
// Planning时需要为每个DataFile关联DeleteFile
for (DataFile dataFile : dataFiles) {
    List<DeleteFile> associatedDeletes = new ArrayList<>();

    // 遍历所有Position Delete（最坏情况）
    for (DeleteFile delete : positionDeletes) {
        if (appliesToDataFile(delete, dataFile)) {
            associatedDeletes.add(delete);
        }
    }

    // 所有Equality Delete都关联
    associatedDeletes.addAll(equalityDeletes);

    tasks.add(new FileScanTask(dataFile, associatedDeletes));
}
```

**性能瓶颈：**
- 最坏情况下关联判断可能接近 O(M * N)
- 实际实现通过Delete索引（按分区/path/sequence/统计）显著降低了平均开销

---

#### 4.4.2 读取阶段影响

**问题1：内存消耗增加**

```
读取单个DataFile (100MB，100万行):
  关联10个Position DeleteFile (平均50KB)
  关联5个Equality DeleteFile (平均100KB)

内存消耗：
  DataFile Buffer: ~10MB (批量读取)
  Position Delete Index: ~500KB × 10 = 5MB  ← Bitmap索引
  Equality Delete Set: ~500KB × 5 = 2.5MB   ← HashSet
  总计: ~17.5MB

如果100个并发Task: 17.5MB × 100 = 1.75GB
```

**问题2：CPU开销增加**

```java
// 每读取一行数据都要执行的过滤逻辑
for (Row row : dataFileRows) {
    long position = row.getPosition();

    // 1. Position Delete检查 (10次Bitmap.contains)
    for (PositionDeleteIndex index : posIndexes) {
        if (index.isDeleted(position)) {
            continue nextRow;  // 跳过行
        }
    }

    // 2. Equality Delete检查 (5次HashSet.contains)
    for (StructLikeSet deleteSet : eqDeleteSets) {
        StructLike projected = project(row);
        if (deleteSet.contains(projected)) {
            continue nextRow;  // 跳过行
        }
    }

    emit(row);  // 输出行
}
```

**性能下降：**
```
无Delete: 100万行 in 5秒 → 20万行/秒
10个PosDelete + 5个EqDelete: 100万行 in 12秒 → 8.3万行/秒  ← 下降58%
```

---

#### 4.4.3 Checkpoint/元数据影响

**问题：State大小膨胀（Flink流式）**

```
旧API StreamingMonitorFunction Checkpoint State:
  lastSnapshotId: 8 Bytes（核心状态）

新API ContinuousIcebergEnumerator Checkpoint State:
  enumeratorPosition + assigner state（含pending splits）

单个FileScanTask序列化大小：
  DataFile元数据: ~500 Bytes
  DeleteFile元数据: ~500 Bytes × M个DeleteFile
  总计: 500 + 500M Bytes

1000个待消费Split，每个关联20个DeleteFile：
  State大小 = 1000 × (500 + 500×20) = ~10.5MB

Checkpoint影响：
  - Checkpoint时间变长
  - State Backend存储压力
  - 恢复时间增加
```

---

### 4.5 优化策略：Compaction与Rewrite

#### 4.5.1 RewritePositionDeleteFiles Action

**作用：压缩和重组Position Delete文件**

```java
// Spark SQL调用
spark.sql("""
  CALL catalog.system.rewrite_position_delete_files(
    table => 'db.table',
    options => map(
      'rewrite-job-order', 'bytes-desc',  -- 先处理大文件
      'partial-progress.enabled', 'true',
      'max-concurrent-file-group-rewrites', '10'
    )
  )
""");
```

**优化策略：**

1. **合并小文件**：
   ```
   优化前：
     DeleteFile-1: 2KB  (删除DataFile-1的3行)
     DeleteFile-2: 3KB  (删除DataFile-1的5行)
     DeleteFile-3: 1KB  (删除DataFile-1的2行)

   优化后：
     DeleteFile-Merged: 5KB  (删除DataFile-1的10行，去重后)
   ```

2. **按FILE粒度重组**：
   ```
   优化前（PARTITION粒度）：
     DeleteFile-Partition: 500KB
       → 包含1000个DataFile的删除
       → referencedDataFile() = null

   优化后（FILE粒度）：
     DeleteFile-1: 50KB  → referencedDataFile() = "DataFile-1"
     DeleteFile-2: 45KB  → referencedDataFile() = "DataFile-2"
     ...
     DeleteFile-100: 55KB → referencedDataFile() = "DataFile-100"
   ```

3. **清理过时Delete**：
   ```
   DataFile-1已被删除（通过ExpireSnapshots）
     → DeleteFile-1（删除DataFile-1的行）也应该删除
     → Rewrite时自动清理
   ```

**源码接口：**
```java
// 源码：api/src/main/java/org/apache/iceberg/actions/RewritePositionDeleteFiles.java:31-91
public interface RewritePositionDeleteFiles
    extends SnapshotUpdate<RewritePositionDeleteFiles, Result> {

    // 过滤要重写的Delete文件
    RewritePositionDeleteFiles filter(Expression expression);

    // 配置选项
    String MAX_CONCURRENT_FILE_GROUP_REWRITES = "max-concurrent-file-group-rewrites";
    String REWRITE_JOB_ORDER = "rewrite-job-order";  // bytes-asc/desc, files-asc/desc
    String PARTIAL_PROGRESS_ENABLED = "partial-progress.enabled";

    interface Result {
        int rewrittenDeleteFilesCount();  // 重写的DeleteFile数量
        int addedDeleteFilesCount();      // 新增的DeleteFile数量
        long rewrittenBytesCount();       // 重写的字节数
    }
}
```

---

#### 4.5.2 定期Compaction策略

**策略1：基于文件数量触发**

```sql
-- 监控Delete文件数量
SELECT COUNT(*) AS delete_file_count
FROM iceberg_table.files
WHERE content = 'POSITION_DELETES';

-- 超过阈值时触发Compaction
IF delete_file_count > 1000 THEN
  CALL rewrite_position_delete_files('db.table');
END IF;
```

**策略2：基于文件大小触发**

```sql
-- 计算小文件比例
SELECT
  COUNT(CASE WHEN file_size_in_bytes < 10485760 THEN 1 END) AS small_files,
  COUNT(*) AS total_files
FROM iceberg_table.files
WHERE content = 'POSITION_DELETES';

-- 小文件超过50%时触发
IF small_files / total_files > 0.5 THEN
  CALL rewrite_position_delete_files('db.table');
END IF;
```

**策略3：周期性调度**

```python
# Airflow DAG示例
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG('iceberg_delete_compaction', schedule_interval='@daily') as dag:
    compact_deletes = SparkSubmitOperator(
        task_id='compact_delete_files',
        application='rewrite_deletes.py',
        conf={
            'spark.sql.catalog.prod': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.prod.type': 'hive'
        }
    )
```

---

#### 4.5.3 Equality Delete优化

**问题：Equality Delete无法精细Compaction**

Equality Delete按值匹配且跨文件生效，难以像Position Delete那样按DataFile精细重组。

**替代优化策略：**

1. **合并小的Equality DeleteFile**：
   ```
   EqDeleteFile-1: 10KB (删除100个id)
   EqDeleteFile-2: 8KB  (删除80个id)
   EqDeleteFile-3: 12KB (删除120个id)

   合并后：
   EqDeleteFile-Merged: 25KB (删除300个id，去重后)
   ```

2. **转换为Rewrite Data**：
   ```sql
   -- 定期将带有大量Equality Delete的表重写为纯数据
   CALL rewrite_data_files(
     table => 'db.table',
     strategy => 'binpack',
     where => 'partition_col = "2024-01-01"'
   );
   ```

   **效果：**
   ```
   优化前：
     DataFile-1: 1000行
     EqDeleteFile: 删除200行
     实际有效: 800行

   优化后：
     DataFile-1-Rewritten: 800行 (直接排除已删除的行)
     EqDeleteFile: 删除 (不再需要)
   ```

3. **分区级Compaction**：
   ```sql
   -- 仅Compaction Delete较多的分区
   SELECT partition, COUNT(*) AS eq_delete_count
   FROM iceberg_table.files
   WHERE content = 'EQUALITY_DELETES'
   GROUP BY partition
   HAVING eq_delete_count > 10
   ORDER BY eq_delete_count DESC;

   -- 对Top分区执行Rewrite
   CALL rewrite_data_files(
     where => 'partition_col IN ("part-1", "part-2", "part-3")'
   );
   ```

---

`★ Insight ─────────────────────────────────────`
**1. DeleteGranularity的权衡本质**
FILE vs PARTITION粒度的选择，本质是"读时性能"与"元数据管理复杂度"的权衡。FILE粒度让每次读取更精准，但代价是Planning时需要管理更多元数据；PARTITION粒度简化了元数据，但让读取时必须处理冗余的删除信息。

**2. Roaring Bitmap的巧妙应用**
Position Delete使用Roaring Bitmap而非简单的HashSet，是因为行位置本身是稠密的整数序列，Run-Length Encoding能将连续删除压缩到极致。这也解释了为什么Equality Delete不能用Bitmap——业务主键往往是稀疏的字符串或复合键。

**3. Compaction的必要性**
Delete文件过多不是Merge-on-Read的设计缺陷，而是其本质特征——通过延迟合并换取写入性能。Compaction是必然的后续步骤，就像Kafka的Log Compaction或LSM Tree的Compaction一样，是架构的固有部分而非补丁。
`─────────────────────────────────────────────────`

---

## 第四章总结

本章深入分析了DeleteFile的存储格式、性能影响和优化策略：

✅ **Position Delete格式**：file_path + pos两字段，Roaring Bitmap内存索引
✅ **Equality Delete格式**：用户自定义Schema + equalityFieldIds指定匹配字段
✅ **DeleteGranularity**：FILE粒度优化读取，PARTITION粒度减少文件数
✅ **性能影响**：Planning变慢、内存增加、CPU过滤开销
✅ **优化策略**：RewritePositionDeleteFiles合并小文件、定期Compaction、Rewrite Data转换

**关键数据：**
- Position Delete单文件：~2-50KB（取决于删除行数）
- Equality Delete单文件：~5-500KB（取决于主键数量）
- Roaring Bitmap压缩率：~10-100x（稠密场景）
- Delete过多性能下降：50-70%（10+个DeleteFile/DataFile）

**下一章预告：** 我们将对比总结Spark和Flink读取机制的异同，分析Delete文件管理的最佳实践，并提供生产环境的性能调优建议。

---

---

## 第五章：总结与最佳实践

### 5.1 Spark vs Flink读取机制对比

#### 5.1.1 核心差异总览

| 维度                  | Spark批式读取                          | Flink流式读取                              |
|-----------------------|----------------------------------------|--------------------------------------------|
| **读取模式**          | Snapshot Scan（单次完整扫描）         | Incremental Scan（增量扫描）              |
| **任务调度**          | SparkBatchQueryScan → FileScanTask    | ContinuousIcebergEnumerator → IcebergSourceSplit |
| **并行度控制**        | Partition级别（可Split）               | Split级别（动态分配）                      |
| **Delete处理时机**    | 读取时按task.deletes应用（Merge-on-Read） | 读取时按task.deletes应用（若任务携带Delete） |
| **Runtime优化**       | Runtime Filter动态过滤                 | Throttling防止积压                         |
| **Checkpoint支持**    | 不需要（批式无状态）                   | 必需（lastSnapshotId状态）                 |
| **Exactly-Once**      | 天然保证（批式原子性）                 | Checkpoint机制保证                         |
| **缓存策略**          | Executor级缓存DeleteFile               | 无内置缓存（依赖FileSystem缓存）           |

---

#### 5.1.2 DataFile与DeleteFile关联对比

**Spark关联机制：**

```java
// Planning时构建关联
FileScanTask task = new BaseFileScanTask(
    dataFile,
    deleteFiles,  // 预先关联好的DeleteFile数组
    schema,
    spec,
    residual
);

// 读取时直接使用
SparkDeleteFilter filter = new SparkDeleteFilter(
    dataFile.location(),
    task.deletes(),  // 已经关联好
    counter
);
```

**特点：**
- ✅ Planning阶段完成关联，读取时无需再判断
- ✅ 可以精确控制关联策略（FILE粒度优化）
- ❌ Planning开销较大（O(M×N)复杂度）

**Flink关联机制：**

```java
// 同样在Planning时关联
IcebergSourceSplit split = IcebergSourceSplit.fromCombinedScanTask(
    combinedTask  // 内部包含FileScanTask[]
);

// 读取逻辑与Spark同源：FlinkDeleteFilter extends DeleteFilter
FlinkDeleteFilter filter = new FlinkDeleteFilter(
    fileScanTask,
    tableSchema,
    projectedSchema,
    inputFilesDecryptor
);
```

**特点：**
- ✅ 关联逻辑复用Iceberg Core（BaseFileScanTask）
- ✅ Incremental Append路径仅读APPEND新增，通常删除关联量更小
- ⚠️ 启动全量scan或非append消费路径下仍可能携带DeleteFile

---

#### 5.1.3 性能特性对比

**Spark优势场景：**

1. **大规模批处理**：
   ```
   场景：历史数据全量分析
   表大小：10TB，1000万个文件
   优势：
     - Spark调度器成熟，资源利用率高
     - Runtime Filter大幅减少读取量
     - Executor缓存减少重复IO
   ```

2. **复杂查询**：
   ```sql
   SELECT a.*, b.*
   FROM large_table a
   JOIN small_table b ON a.id = b.id
   WHERE a.date BETWEEN '2024-01-01' AND '2024-12-31';
   ```
   - Spark Catalyst优化器强大
   - Runtime Filter自动生成

**Flink优势场景：**

1. **实时增量消费**：
   ```
   场景：CDC数据实时同步
   写入频率：每秒100次Commit
   优势：
     - Incremental Scan仅读新Snapshot
     - 延迟低（秒级）
     - 自动Checkpoint，Exactly-Once
   ```

2. **流式ETL**：
   ```java
   // Flink流式读取 → 转换 → 写入
   env.fromSource(icebergSource, ...)
      .keyBy(...)
      .process(new MyProcessFunction())
      .sinkTo(icebergSink);
   ```
   - 端到端流式处理
   - 背压机制自动调节

---

### 5.2 Delete文件管理最佳实践

#### 5.2.1 写入策略

**策略1：根据UPDATE/DELETE频率选择粒度**

```python
# 决策树
if update_frequency == 'HIGH':  # 每秒>10次UPDATE
    delete_granularity = 'FILE'
    reason = "高频更新，FILE粒度避免读时冗余"

elif partition_count < 100 and files_per_partition < 1000:
    delete_granularity = 'PARTITION'
    reason = "小表，PARTITION粒度减少文件数"

else:
    delete_granularity = 'FILE'
    reason = "默认FILE粒度，配合定期Compaction"
```

**配置示例：**

```sql
-- Spark写入配置（推荐通过表属性）
ALTER TABLE my_table SET TBLPROPERTIES (
  'write.delete.granularity' = 'file',
  'write.delete.mode' = 'merge-on-read'
);

-- Flink写入配置
CREATE TABLE iceberg_table (...)
WITH (
  'write.format.default' = 'parquet',
  'write.delete.granularity' = 'file',
  'write.delete.mode' = 'merge-on-read'
);
```

---

**策略2：控制DeleteFile大小**

```sql
-- 表属性配置
ALTER TABLE my_table SET TBLPROPERTIES (
  'write.target-file-size-bytes' = '536870912',  -- 512MB目标大小
  'write.delete.target-file-size-bytes' = '67108864'  -- 64MB Delete目标
);
```

**建议值：**
- **DataFile目标大小**：256MB - 1GB
- **DeleteFile目标大小**：DataFile的1/10 - 1/20
  - 理由：Delete通常只包含file_path+pos，比完整行小得多

---

#### 5.2.2 监控指标

**关键指标清单：**

```sql
-- 1. DeleteFile数量
SELECT content, COUNT(*) AS file_count
FROM catalog.db.table.files
GROUP BY content;

-- 预期输出：
-- DATA              : 10000
-- POSITION_DELETES  : 500   ← 应<DataFile的10%
-- EQUALITY_DELETES  : 50    ← 应<DataFile的1%

-- 2. DeleteFile与DataFile比例
SELECT
  ROUND(
    COUNT(CASE WHEN content = 'POSITION_DELETES' THEN 1 END) * 100.0 /
    COUNT(CASE WHEN content = 'DATA' THEN 1 END),
    2
  ) AS delete_ratio_pct
FROM catalog.db.table.files;

-- 警戒线：> 15%

-- 3. 小DeleteFile比例
SELECT
  COUNT(CASE WHEN file_size_in_bytes < 10485760 THEN 1 END) AS small_files,
  COUNT(*) AS total_files,
  ROUND(
    COUNT(CASE WHEN file_size_in_bytes < 10485760 THEN 1 END) * 100.0 /
    COUNT(*),
    2
  ) AS small_file_pct
FROM catalog.db.table.files
WHERE content IN ('POSITION_DELETES', 'EQUALITY_DELETES');

-- 警戒线：> 50%

-- 4. 每个DataFile平均关联的DeleteFile数量
WITH data_delete_mapping AS (
  SELECT
    d.file_path AS data_file,
    COUNT(DISTINCT p.file_path) AS delete_count
  FROM catalog.db.table.files d
  LEFT JOIN catalog.db.table.position_deletes p
    ON p.referenced_data_file = d.file_path
  WHERE d.content = 'DATA'
  GROUP BY d.file_path
)
SELECT
  AVG(delete_count) AS avg_deletes_per_data_file,
  MAX(delete_count) AS max_deletes_per_data_file
FROM data_delete_mapping;

-- 警戒线：avg > 10, max > 50
```

---

#### 5.2.3 Compaction调度策略

**策略1：基于阈值的自动触发**

```python
# Airflow Sensor示例
from airflow.sensors.base import BaseSensorOperator

class IcebergDeleteThresholdSensor(BaseSensorOperator):
    def poke(self, context):
        # 查询Delete文件数量
        delete_count = spark.sql("""
            SELECT COUNT(*)
            FROM catalog.db.table.files
            WHERE content = 'POSITION_DELETES'
        """).collect()[0][0]

        data_count = spark.sql("""
            SELECT COUNT(*)
            FROM catalog.db.table.files
            WHERE content = 'DATA'
        """).collect()[0][0]

        ratio = delete_count / data_count if data_count > 0 else 0

        # 超过15%触发
        return ratio > 0.15

# DAG定义
with DAG('iceberg_delete_compaction') as dag:
    check_threshold = IcebergDeleteThresholdSensor(
        task_id='check_delete_threshold',
        poke_interval=3600  # 每小时检查
    )

    run_compaction = SparkSubmitOperator(
        task_id='compact_deletes',
        application='compact_deletes.py'
    )

    check_threshold >> run_compaction
```

---

**策略2：分区级增量Compaction**

```sql
-- 识别需要Compaction的分区
CREATE OR REPLACE TEMP VIEW partitions_to_compact AS
SELECT
  partition_spec_id,
  partition,
  COUNT(*) AS delete_file_count,
  SUM(file_size_in_bytes) / 1024 / 1024 AS total_size_mb
FROM catalog.db.table.files
WHERE content = 'POSITION_DELETES'
GROUP BY partition_spec_id, partition
HAVING delete_file_count > 10
ORDER BY delete_file_count DESC
LIMIT 100;  -- 每次仅处理Top 100分区

-- 执行Compaction
CALL catalog.system.rewrite_position_delete_files(
  table => 'db.table',
  where => 'partition_col IN (
    SELECT partition FROM partitions_to_compact
  )',
  options => map(
    'rewrite-job-order', 'bytes-desc',
    'target-file-size-bytes', '67108864'  -- 64MB
  )
);
```

---

**策略3：夜间全量Compaction**

```bash
#!/bin/bash
# nightly_compact.sh

# 1. Compact Position Deletes
spark-submit \
  --class org.apache.iceberg.spark.actions.RewritePositionDeleteFilesAction \
  --conf spark.sql.catalog.prod=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.prod.type=hive \
  compact_deletes.py \
  --table prod.db.table \
  --parallelism 50

# 2. Compact Data Files (可选，合并Delete到Data)
spark-submit \
  --class org.apache.iceberg.spark.actions.RewriteDataFilesAction \
  --conf spark.sql.catalog.prod=org.apache.iceberg.spark.SparkCatalog \
  rewrite_data.py \
  --table prod.db.table \
  --where "date >= current_date - interval 7 days" \
  --strategy binpack
```

**Cron配置：**
```cron
# 每天凌晨2点执行
0 2 * * * /scripts/nightly_compact.sh >> /logs/compact.log 2>&1
```

---

### 5.3 性能调优建议

#### 5.3.1 Planning阶段优化

**问题：Planning耗时过长**

**优化1：选择合适的Planning模式（Spark）**

```sql
-- Spark配置
spark.conf.set("spark.sql.iceberg.data-planning-mode", "distributed")
spark.conf.set("spark.sql.iceberg.delete-planning-mode", "distributed")

-- 效果：将规划压力从Driver转移到分布式执行
-- 适用场景：Manifest和文件数较大的大表
```

**优化2：并行Planning**

```sql
-- Spark配置
-- Spark并行规划主要由distributed planning + 集群资源决定

-- Flink配置（作业级）
tableConfig.set("table.exec.iceberg.worker-pool-size", "4")

-- Flink流式读取配置（表/connector）
CREATE TABLE iceberg_table (...)
WITH (
  'streaming' = 'true',
  'monitor-interval' = '10s',
  'max-planning-snapshot-count' = '10'
);
```

**优化3：分区过滤下推**

```sql
-- ❌ 不推荐：扫描全表
SELECT * FROM large_table WHERE date = '2024-01-01';

-- ✅ 推荐：分区过滤写在WHERE中
SELECT * FROM large_table
WHERE partition_date = '2024-01-01'  -- 分区列
  AND date = '2024-01-01';           -- 数据列
```

---

#### 5.3.2 读取阶段优化

**问题：DeleteFile加载慢**

**优化1：Executor缓存（Spark）**

```scala
// Spark配置
spark.conf.set("spark.sql.iceberg.executor-cache.enabled", "true")
spark.conf.set("spark.sql.iceberg.executor-cache.delete-files.enabled", "true")
spark.conf.set("spark.sql.iceberg.executor-cache.max-total-size", (512L * 1024 * 1024).toString)

// 效果：多个Task复用DeleteFile，减少IO
// 场景：多个DataFile关联相同DeleteFile（PARTITION粒度）
```

**优化2：预加载DeleteFile**

```java
// 自定义Reader（高级用法）
class PreloadingDeleteFilter extends DeleteFilter<InternalRow> {
    @Override
    protected DeleteLoader newDeleteLoader() {
        return new CachingDeleteLoader(this::loadInputFile) {
            @Override
            public void preload(List<DeleteFile> files) {
                // 批量预加载，利用并行IO
                ExecutorService executor = Executors.newFixedThreadPool(4);
                files.forEach(file ->
                    executor.submit(() -> loadInputFile(file))
                );
                executor.shutdown();
            }
        };
    }
}
```

---

**问题：内存溢出（OOM）**

**优化1：限制并发读取**

```sql
-- Spark：减少Task并发
spark.conf.set("spark.sql.shuffle.partitions", "200")  -- 默认200
spark.conf.set("spark.executor.cores", "4")  -- 每个Executor仅4核

-- Flink：限制Reader并行度
env.fromSource(icebergSource, ...)
   .setParallelism(50);  -- 限制为50并行
```

**优化2：启用Split**

```sql
-- Spark：Split大文件
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  -- 128MB

-- Flink：启用Split
CREATE TABLE iceberg_table (...)
WITH (
  'read.split.target-size' = '134217728',  -- 128MB
  'read.split.planning-lookback' = '10'    -- 回溯10个文件
);
```

---

#### 5.3.3 流式读取优化（Flink）

**问题：Checkpoint时间过长**

**优化1：减少State大小**

```java
// 限制max-planning-snapshot-count
CREATE TABLE iceberg_table (...)
WITH (
  'streaming' = 'true',
  'max-planning-snapshot-count' = '10'  -- 单次最多Planning 10个Snapshot
);

// 效果：减少单次发现的Split数量，降低State大小
```

**优化2：启用Incremental Checkpoint**

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(60000);  // 60秒一次

CheckpointConfig config = env.getCheckpointConfig();
config.enableUnalignedCheckpoints();  // 非对齐Checkpoint（Flink 1.11+）
config.setCheckpointStorage("hdfs:///checkpoints");
```

---

**问题：延迟过高**

**优化1：减少监控间隔**

```sql
-- 默认10秒，可以降低
CREATE TABLE iceberg_table (...)
WITH (
  'monitor-interval' = '5s'  -- 5秒检查一次新Snapshot
);

-- 注意：过短的间隔会增加Planning开销
```

**优化2：启用Watermark**

```java
// 使用事件时间处理乱序数据
env.fromSource(icebergSource, WatermarkStrategy
    .forBoundedOutOfOrderness(Duration.ofMinutes(5))
    .withTimestampAssigner((event, timestamp) -> event.getEventTime())
, "iceberg-source");
```

---

### 5.4 常见问题解答

#### Q1: 为什么读取很慢，即使表只有少量Delete？

**原因分析：**
```
可能原因1：Manifest文件过多
  → 检查：SELECT COUNT(DISTINCT manifest_path) FROM table.manifests
  → 解决：执行 CALL rewrite_manifests('table')

可能原因2：小DataFile过多
  → 检查：SELECT COUNT(*) FROM table.files WHERE file_size_in_bytes < 10MB
  → 解决：执行 CALL rewrite_data_files('table', 'strategy' => 'binpack')

可能原因3：DeleteFile未设置referencedDataFile
  → 检查：Planning日志中"Scanning delete file"
  → 解决：设置 write.delete.granularity = 'file'
```

---

#### Q2: Flink流式读取为什么会重复消费数据？

**原因分析：**
```
可能原因1：Checkpoint失败，从旧状态恢复
  → 检查：Flink Web UI → Checkpoints
  → 解决：排查Checkpoint失败原因（超时、State Backend问题）

可能原因2：Table执行了OVERWRITE操作
  → 检查：Snapshot history中是否有"overwrite"操作
  → 解决：流式读取时避免OVERWRITE，使用APPEND

可能原因3：lastSnapshotId状态丢失
  → 检查：State Backend是否持久化
  → 解决：确保使用RocksDBStateBackend或HDFS
```

---

#### Q3: 如何判断是否需要执行Compaction？

**决策指标：**
```python
# 计算Delete Overhead
delete_overhead = (
    delete_file_count / data_file_count +
    small_delete_file_ratio * 0.5 +
    avg_deletes_per_datafile / 10
)

if delete_overhead > 1.0:
    action = "立即执行Compaction"
elif delete_overhead > 0.5:
    action = "计划Compaction（1周内）"
elif delete_overhead > 0.2:
    action = "监控，考虑Compaction"
else:
    action = "无需Compaction"

print(f"Delete Overhead: {delete_overhead:.2f} → {action}")
```

**示例：**
```
表A：
  - delete_file_count / data_file_count = 500 / 10000 = 0.05
  - small_delete_file_ratio = 0.3
  - avg_deletes_per_datafile = 2

delete_overhead = 0.05 + 0.3*0.5 + 2/10 = 0.4
  → 监控，考虑Compaction

表B：
  - delete_file_count / data_file_count = 2000 / 5000 = 0.4
  - small_delete_file_ratio = 0.8
  - avg_deletes_per_datafile = 15

delete_overhead = 0.4 + 0.8*0.5 + 15/10 = 2.3
  → 立即执行Compaction！
```

---

#### Q4: Equality Delete能否转换为Position Delete？

**答案：不能直接转换，但可以通过Rewrite Data消除**

```sql
-- 错误做法：无法转换
-- Equality Delete是基于值匹配，Position Delete是基于位置
-- 两者语义不同

-- 正确做法：Rewrite Data（推荐）
CALL catalog.system.rewrite_data_files(
  table => 'db.table',
  strategy => 'binpack',
  where => 'partition_date = "2024-01-01"'
);

-- 效果：
-- 1. 读取DataFile，应用Equality Delete过滤
-- 2. 写入新DataFile（仅包含未删除的行）
-- 3. 删除旧DataFile和关联的Equality DeleteFile
```

---

#### Q5: 流式读取如何处理Late Data（迟到数据）？

**场景：**
```
T1: Snapshot 100 (包含数据: event_time=10:00)
T2: Snapshot 101 (包含数据: event_time=09:55)  ← 迟到数据

Flink增量读取：
  读取Snapshot 100 → 输出 event_time=10:00
  读取Snapshot 101 → 输出 event_time=09:55  ← 乱序！
```

**解决方案：**

```java
// 方案1：使用Watermark处理乱序
DataStream<RowData> stream = env.fromSource(
    icebergSource,
    WatermarkStrategy
        .<RowData>forBoundedOutOfOrderness(Duration.ofMinutes(30))
        .withTimestampAssigner((row, ts) ->
            row.getTimestamp(eventTimeFieldIndex, 3).getMillisecond()
        ),
    "iceberg-source"
);

// 方案2：按event_time排序（增加延迟）
stream
    .keyBy(row -> row.getString(partitionFieldIndex))
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .process(new SortedWindowFunction());

// 方案3：容忍乱序，下游处理
// 适用于对顺序不敏感的场景（如聚合统计）
```

---

`★ Insight ─────────────────────────────────────`
**1. Planning是性能瓶颈的根源**
无论Spark还是Flink，Planning阶段的Manifest扫描和FileScanTask构建都是线性复杂度（O(M×N)），这是Iceberg元数据架构的固有成本。优化的核心是减少M（Manifest数）和N（File数），而非优化算法本身。

**2. Delete管理是长期工程**
Delete文件不会自动消失，Compaction是必须的后台任务。生产环境应将Compaction视为与数据写入同等重要的操作，而非临时救火方案。

**3. 流式与批式的选择边界**
Flink流式读取的优势在于"增量"而非"实时"。如果数据更新频率低于1分钟，Spark批式任务配合调度器可能更简单。流式架构的复杂度（Checkpoint、State、反压）应与实时性需求相匹配。
`─────────────────────────────────────────────────`

---

## 全文总结

本文档深入分析了Spark和Flink读取Iceberg表的完整机制，涵盖以下核心内容：

### 📚 章节回顾

**第一章：Iceberg读取基础架构**
- 三层元数据结构：Snapshot → ManifestFile → ContentFile
- 三种文件类型：DATA、POSITION_DELETES、EQUALITY_DELETES
- Sequence Number并发控制机制

**第二章：Spark读取机制详解**
- FileScanTask封装DataFile与DeleteFile关联
- SparkDeleteFilter的两阶段过滤链（Position → Equality）
- Schema投影与_pos元数据列
- Executor缓存与Runtime Filter优化

**第三章：Flink流式读取机制**
- IncrementalAppendScan仅读取APPEND操作的ADDED Entry
- StreamingMonitorFunction周期性监控+Checkpoint状态保存
- ContinuousIcebergEnumerator异步Split发现+Throttling
- Exactly-Once语义通过lastSnapshotId保证

**第四章：DeleteFile深度解析**
- Position Delete：file_path+pos，Roaring Bitmap索引
- Equality Delete：用户定义Schema+equalityFieldIds
- DeleteGranularity：FILE vs PARTITION粒度权衡
- Delete文件过多的性能影响（Planning、内存、CPU）
- Compaction优化策略

**第五章：总结与最佳实践**
- Spark vs Flink读取对比
- Delete文件管理策略（写入、监控、Compaction）
- 性能调优建议（Planning、读取、流式）
- 常见问题解答

---

### 🎯 核心要点

1. **Merge-on-Read的本质**：用写入时的延迟合并换取读取时的过滤开销，适合写多读少场景

2. **Delete文件是双刃剑**：提升写入性能的同时，必须配合定期Compaction才能保持读取性能

3. **Planning是性能关键**：Manifest扫描和FileScanTask构建的复杂度决定了查询响应时间

4. **流式读取的挑战**：Incremental Scan解决了增量消费问题，但Equality Delete的跨文件匹配特性在流式场景需要额外设计

5. **粒度选择无银弹**：FILE粒度优化读取，PARTITION粒度减少文件数，需根据业务特点权衡

---

### 📊 关键性能数据

| 指标                        | 数值                          | 影响                     |
|-----------------------------|-------------------------------|--------------------------|
| Position Delete单文件大小   | 2-50KB                        | 小文件问题               |
| Equality Delete单文件大小   | 5-500KB                       | 内存消耗                 |
| Roaring Bitmap压缩率        | 10-100x                       | 内存效率                 |
| Delete文件推荐比例          | <10% DataFile数量             | Planning性能             |
| Delete过多性能下降          | 50-70%                        | 需立即Compaction         |
| Planning时间增长            | +150% (5000 DeleteFile)       | 用户体验下降             |

---

### 🚀 生产环境检查清单

**部署前检查：**
- [ ] 配置合适的delete.granularity（FILE/PARTITION）
- [ ] 设置target-file-size-bytes（DataFile和DeleteFile）
- [ ] 启用Manifest缓存（交互式场景）
- [ ] 配置合理的并行度（Planning和读取）

**运行时监控：**
- [ ] DeleteFile数量与DataFile比例（目标<10%）
- [ ] 小DeleteFile比例（目标<30%）
- [ ] 每个DataFile平均关联的DeleteFile数量（目标<5）
- [ ] Planning耗时（目标<10秒）
- [ ] Checkpoint时间（Flink，目标<1分钟）

**定期维护：**
- [ ] 每周执行一次RewritePositionDeleteFiles
- [ ] 每月执行一次RewriteDataFiles（合并Delete到Data）
- [ ] 每季度执行一次ExpireSnapshots（清理历史）
- [ ] 每半年Review Delete策略（FILE/PARTITION切换）

---

### 📖 延伸阅读

**官方文档：**
- Iceberg Spec：https://iceberg.apache.org/spec/
- Iceberg Performance：https://iceberg.apache.org/docs/latest/performance/

**社区资源：**
- Iceberg Slack：https://apache-iceberg.slack.com/
- GitHub Issues：https://github.com/apache/iceberg/issues

**相关技术：**
- Apache Paimon（Flink原生表格式）
- Delta Lake（Databricks表格式）
- Apache Hudi（Copy-on-Write与Merge-on-Read混合）

---

## 结语

Iceberg的Merge-on-Read机制通过DeleteFile实现了高效的UPDATE/DELETE操作，但这种设计需要开发者理解其权衡：**写入性能的提升以读取复杂度的增加为代价**。

本文档通过深入源码分析，揭示了Spark和Flink读取Iceberg时DataFile与DeleteFile的协同机制。希望读者能够：

1. **理解原理**：知道为什么Delete文件会影响性能
2. **掌握工具**：能够监控和优化Delete文件
3. **制定策略**：根据业务特点选择合适的粒度和Compaction策略

Iceberg作为新一代数据湖表格式，其设计理念值得深入学习。Merge-on-Read不仅是一种技术实现，更是对**写入性能、读取性能、存储成本、维护复杂度**四维空间的精妙平衡。

---

**文档版本：** v1.0
**最后更新：** 2026-02-03
**适用Iceberg版本：** 基于当前仓库源码（含 Spark 3.5 / Flink 1.20 路径）
**作者：** Claude (claude-4.5-sonnet)

---

**感谢阅读！如有疑问或建议，欢迎反馈。**

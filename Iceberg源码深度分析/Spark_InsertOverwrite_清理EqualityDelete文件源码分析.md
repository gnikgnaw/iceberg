# Spark INSERT OVERWRITE 清理 Equality Delete 文件源码分析

## 问题现象

在使用 Iceberg 表时，经过多次 Flink CDC 或 MERGE INTO 操作后，表中会积累大量 **equality delete 小文件**。当执行 Spark `INSERT OVERWRITE` 后，在新快照中这些 equality delete 文件**突然变为 0**。

本文从 Iceberg 源码层面分析其原因。

---

## 结论

**INSERT OVERWRITE 会同时删除目标分区内的数据文件（data files）和删除文件（delete files）**。这是 Iceberg 的设计意图：既然整个分区的数据都被新数据替换了，那么针对旧数据的 delete files 也就没有存在的意义了。

---

## 源码调用链分析

### 1. Spark 入口层：SparkWrite.java

Spark 执行 `INSERT OVERWRITE` 时，根据覆写模式走不同路径：

#### 1.1 动态分区覆写（DynamicOverwrite）

```
INSERT OVERWRITE table SELECT ...  (动态分区模式)
```

**源码位置**: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java:308-344`

```java
private class DynamicOverwrite extends BaseBatchWrite {
    @Override
    public void commit(WriterCommitMessage[] messages) {
        DataFileSet files = files(messages);
        // 核心：调用 table.newReplacePartitions()
        ReplacePartitions dynamicOverwrite = table.newReplacePartitions();
        for (DataFile file : files) {
            dynamicOverwrite.addFile(file);   // ← 关键入口
        }
        commitOperation(dynamicOverwrite, ...);
    }
}
```

#### 1.2 静态过滤覆写（OverwriteByFilter）

```
INSERT OVERWRITE table PARTITION(dt='2024-01-01') SELECT ...
```

**源码位置**: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java:346-386`

```java
private class OverwriteByFilter extends BaseBatchWrite {
    @Override
    public void commit(WriterCommitMessage[] messages) {
        OverwriteFiles overwriteFiles = table.newOverwrite();
        // 核心：按行过滤条件删除匹配的文件
        overwriteFiles.overwriteByRowFilter(overwriteExpr);
        for (DataFile file : files(messages)) {
            overwriteFiles.addFile(file);
        }
        commitOperation(overwriteFiles, ...);
    }
}
```

---

### 2. Core 层：BaseReplacePartitions.java（动态覆写）

**源码位置**: `core/src/main/java/org/apache/iceberg/BaseReplacePartitions.java`

```java
public class BaseReplacePartitions extends MergingSnapshotProducer<ReplacePartitions>
    implements ReplacePartitions {

    @Override
    public ReplacePartitions addFile(DataFile file) {
        // ★ 关键操作：对该文件所属的分区执行 dropPartition
        dropPartition(file.specId(), file.partition());
        replacedPartitions.add(file.specId(), file.partition());
        add(file);
        return this;
    }

    @Override
    public List<ManifestFile> apply(TableMetadata base, Snapshot snapshot) {
        if (dataSpec().isUnpartitioned()) {
            // 非分区表：删除所有数据
            deleteByRowFilter(Expressions.alwaysTrue());
        }
        return super.apply(base, snapshot);
    }
}
```

**核心逻辑**：每添加一个新数据文件，就会对该文件所属的分区调用 `dropPartition()`。

---

### 3. Core 层：BaseOverwriteFiles.java（静态覆写）

**源码位置**: `core/src/main/java/org/apache/iceberg/BaseOverwriteFiles.java`

```java
public class BaseOverwriteFiles extends MergingSnapshotProducer<OverwriteFiles>
    implements OverwriteFiles {

    @Override
    public OverwriteFiles overwriteByRowFilter(Expression expr) {
        // ★ 关键：按行过滤表达式执行删除
        deleteByRowFilter(expr);
        return this;
    }
}
```

---

### 4. 核心枢纽：MergingSnapshotProducer.java

**源码位置**: `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java`

这里有两个关键的 FilterManager：
- **`filterManager`** (`DataFileFilterManager`)：管理**数据文件**的过滤和删除
- **`deleteFilterManager`** (`DeleteFileFilterManager`)：管理**删除文件**（包括 equality delete 和 position delete）的过滤和删除

```java
// 构造函数中初始化
this.filterManager = new DataFileFilterManager();
this.deleteFilterManager = new DeleteFileFilterManager();
```

#### 4.1 dropPartition — 动态覆写的核心

```java
protected void dropPartition(int specId, StructLike partition) {
    // dropping the data in a partition also drops all deletes in the partition
    //（注释原文：删除分区中的数据，同时也会删除该分区中的所有 delete files）
    filterManager.dropPartition(specId, partition);      // ← 删除数据文件
    deleteFilterManager.dropPartition(specId, partition); // ← 删除 delete 文件
}
```

#### 4.2 deleteByRowFilter — 静态覆写/非分区表的核心

```java
protected void deleteByRowFilter(Expression expr) {
    this.deleteExpression = expr;
    filterManager.deleteByRowFilter(expr);
    // if a delete file matches the row filter, then it can be deleted
    // because the rows will also be deleted
    //（注释原文：如果 delete file 匹配行过滤条件，它也可以被删除，
    //  因为它所删除的行也会被一起删除）
    deleteFilterManager.deleteByRowFilter(expr);
}
```

**这就是 equality delete 文件消失的根本原因！**

无论走哪条路径：
- **动态覆写** → `dropPartition()` → 同时调用 `filterManager` 和 `deleteFilterManager` 的 `dropPartition()`
- **静态覆写** → `deleteByRowFilter()` → 同时调用 `filterManager` 和 `deleteFilterManager` 的 `deleteByRowFilter()`

两个 manager 都会被调用，**数据文件和删除文件被一同清理**。

---

### 5. 底层执行：ManifestFilterManager.java

**源码位置**: `core/src/main/java/org/apache/iceberg/ManifestFilterManager.java`

`ManifestFilterManager` 负责在生成新快照时过滤 manifest 文件的内容：

```java
// 按分区删除
protected void dropPartition(int specId, StructLike partition) {
    invalidateFilteredCache();
    dropPartitions.add(specId, partition);
}

// 按行过滤表达式删除
protected void deleteByRowFilter(Expression expr) {
    invalidateFilteredCache();
    this.deleteExpression = Expressions.or(deleteExpression, expr);
}
```

在生成新的 manifest 文件时，`ManifestFilterManager` 会检查每个 manifest 是否包含需要删除的文件：

```java
private boolean canContainDeletedFiles(ManifestFile manifest, boolean trustManifestReferences) {
    return canContainDroppedFiles(manifest)
        || canContainExpressionDeletes(manifest)
        || canContainDroppedPartitions(manifest);  // ← 检查是否有被 drop 的分区
}

private boolean canContainDroppedPartitions(ManifestFile manifest) {
    if (!dropPartitions.isEmpty()) {
        return ManifestFileUtil.canContainAny(manifest, dropPartitions, specsById);
    }
    return false;
}
```

对于命中的 manifest，其中属于被替换分区的文件条目会被标记为 **DELETED** 状态，不再出现在新快照中。

---

## 完整调用链路图

### 动态分区覆写（INSERT OVERWRITE ... 动态模式）

```
Spark: INSERT OVERWRITE table SELECT ...
  │
  ├─ SparkWrite.DynamicOverwrite.commit()
  │     │
  │     └─ table.newReplacePartitions()
  │           │
  │           └─ BaseReplacePartitions.addFile(dataFile)
  │                 │
  │                 ├─ dropPartition(specId, partition)     ← 对每个新文件的分区执行
  │                 │     │
  │                 │     ├─ filterManager.dropPartition()       → 标记该分区数据文件为删除
  │                 │     └─ deleteFilterManager.dropPartition() → 标记该分区删除文件为删除 ★
  │                 │
  │                 └─ add(dataFile)                        ← 添加新的数据文件
  │
  └─ 新快照生成
        ├─ 新 manifest：包含新写入的数据文件（ADDED）
        └─ 旧 manifest 被过滤：
              ├─ 被替换分区的数据文件 → DELETED（不再可见）
              └─ 被替换分区的删除文件 → DELETED（不再可见）★
```

### 静态分区覆写（INSERT OVERWRITE ... WHERE dt = '2024-01-01'）

```
Spark: INSERT OVERWRITE table PARTITION(dt='2024-01-01') SELECT ...
  │
  ├─ SparkWrite.OverwriteByFilter.commit()
  │     │
  │     └─ table.newOverwrite()
  │           │
  │           ├─ overwriteByRowFilter(expr)
  │           │     │
  │           │     └─ deleteByRowFilter(expr)
  │           │           │
  │           │           ├─ filterManager.deleteByRowFilter()       → 匹配的数据文件被删除
  │           │           └─ deleteFilterManager.deleteByRowFilter() → 匹配的删除文件被删除 ★
  │           │
  │           └─ addFile(newDataFile)                        ← 添加新的数据文件
  │
  └─ 新快照生成（同上）
```

---

## 非分区表的特殊情况

对于非分区表，`BaseReplacePartitions.apply()` 方法会调用：

```java
if (dataSpec().isUnpartitioned()) {
    deleteByRowFilter(Expressions.alwaysTrue());  // 删除所有数据
}
```

`Expressions.alwaysTrue()` 匹配所有文件，因此**所有数据文件和所有删除文件都会被清除**，然后只保留新写入的数据文件。

---

## 设计原理

这个行为是完全合理的，原因如下：

1. **逻辑正确性**：Equality delete 文件的作用是标记"哪些行需要被删除"。当整个分区的旧数据文件都被新数据文件替换后，旧的 delete 文件所指向的旧数据已经不存在了，保留它们没有意义。

2. **避免性能退化**：如果保留这些 delete 文件，读取时引擎仍然需要加载并应用这些 delete 文件做行级过滤。但新写入的数据文件中根本不包含这些"被删除"的行，这些过滤操作纯属浪费。

3. **源码注释佐证**：
   - `MergingSnapshotProducer.java:183-185` 原文注释：
     > if a delete file matches the row filter, then it can be deleted because the rows will also be deleted
   - `MergingSnapshotProducer.java:189-190` 原文注释：
     > dropping the data in a partition also drops all deletes in the partition

---

## 快照 Summary 中的 Delete 文件统计

### 问题：历史快照中 delete 文件还在吗？

**在的。** Iceberg 的快照是**不可变的（immutable）**，INSERT OVERWRITE 只是创建了一个新快照，旧快照的内容不受影响。通过 time travel 回到旧快照，仍然能看到那些 equality delete 文件。

### total-* 指标的计算方式

每个快照的 summary 中包含以下 delete 相关的统计指标：

| 快照 Summary 字段 | 含义 |
|---|---|
| `total-delete-files` | 当前快照可见的 delete 文件总数 |
| `total-equality-deletes` | 当前快照可见的 equality delete **记录**总数（注意是记录数，不是文件数） |
| `total-position-deletes` | 当前快照可见的 position delete 记录总数 |
| `added-delete-files` | 本次操作新增的 delete 文件数 |
| `removed-delete-files` | 本次操作移除的 delete 文件数 |
| `added-equality-delete-files` | 本次操作新增的 equality delete 文件数 |
| `removed-equality-delete-files` | 本次操作移除的 equality delete 文件数 |

### 源码：SnapshotProducer.java 中的 updateTotal 方法

**源码位置**: `core/src/main/java/org/apache/iceberg/SnapshotProducer.java:840-870`

`total-*` 系列指标是**基于上一个快照的 total 值增量计算**的：

```java
private static void updateTotal(
    ImmutableMap.Builder<String, String> summaryBuilder,
    Map<String, String> previousSummary,
    String totalProperty,        // e.g. "total-delete-files"
    Map<String, String> currentSummary,
    String addedProperty,        // e.g. "added-delete-files"
    String deletedProperty) {    // e.g. "removed-delete-files"
  String totalStr = previousSummary.get(totalProperty);
  if (totalStr != null) {
    long newTotal = Long.parseLong(totalStr);

    String addedStr = currentSummary.get(addedProperty);
    if (newTotal >= 0 && addedStr != null) {
      newTotal += Long.parseLong(addedStr);     // ← 加上新增的
    }

    String deletedStr = currentSummary.get(deletedProperty);
    if (newTotal >= 0 && deletedStr != null) {
      newTotal -= Long.parseLong(deletedStr);   // ← 减去移除的
    }

    if (newTotal >= 0) {
      summaryBuilder.put(totalProperty, String.valueOf(newTotal));
    }
  }
}
```

计算公式：

```
新快照的 total = 上一快照的 total + 本次 added - 本次 removed
```

具体调用链（`SnapshotProducer.java:420-440`）：

```java
// total-delete-files = previous.total-delete-files + added-delete-files - removed-delete-files
updateTotal(builder, previousSummary,
    SnapshotSummary.TOTAL_DELETE_FILES_PROP, summary,
    SnapshotSummary.ADDED_DELETE_FILES_PROP,
    SnapshotSummary.REMOVED_DELETE_FILES_PROP);

// total-equality-deletes = previous.total-equality-deletes + added-equality-deletes - removed-equality-deletes
updateTotal(builder, previousSummary,
    SnapshotSummary.TOTAL_EQ_DELETES_PROP, summary,
    SnapshotSummary.ADDED_EQ_DELETES_PROP,
    SnapshotSummary.REMOVED_EQ_DELETES_PROP);

// total-position-deletes 同理
updateTotal(builder, previousSummary,
    SnapshotSummary.TOTAL_POS_DELETES_PROP, summary,
    SnapshotSummary.ADDED_POS_DELETES_PROP,
    SnapshotSummary.REMOVED_POS_DELETES_PROP);
```

### 示例：INSERT OVERWRITE 前后的快照变化

假设 Flink CDC 持续写入后，表状态如下：

```
快照 S1 (Flink checkpoint):
  summary:
    total-delete-files: 80
    total-equality-deletes: 40000
    added-equality-delete-files: 20

快照 S2 (Flink checkpoint):
  summary:
    total-delete-files: 120
    total-equality-deletes: 62000
    added-equality-delete-files: 40

快照 S3 (Spark INSERT OVERWRITE):
  summary:
    total-delete-files: 0           ← 120 + 0 - 120 = 0
    total-equality-deletes: 0       ← 62000 + 0 - 62000 = 0
    removed-delete-files: 120       ← 本次移除了 120 个 delete 文件
    removed-equality-delete-files: 120
```

### 关键结论

1. **每个快照的 `total-*` 是该快照视角下的累计值**，不是全局统计。快照 S2 看到 120 个 delete 文件，快照 S3 看到 0 个，各自都是正确的。

2. **历史快照中的 delete 文件在元数据层仍然存在**。旧快照的 manifest list 仍然引用着那些 delete 文件的 manifest。通过 time travel 回到 S2 仍然能看到那 120 个 delete 文件。

3. **物理文件何时真正删除**：只有执行 `expireSnapshots` 清理旧快照后，且一个 delete 文件不再被任何存活快照引用时，Iceberg 才会删除其物理文件。INSERT OVERWRITE 之后，那些 equality delete 的物理文件仍然占用存储空间。

---

## 实践建议

| 场景 | 说明 |
|------|------|
| **定期重写** | 如果表累积了大量 equality delete 小文件导致读取变慢，可以通过 `INSERT OVERWRITE` 重写受影响的分区来清理 |
| **Rewrite Data Files** | 另一种清理方式是调用 `rewriteDataFiles` 操作（Spark 的 `CALL system.rewrite_data_files`），它也会在重写过程中合并 delete 文件 |
| **Expire Snapshots** | 旧快照中引用的 delete 文件物理文件不会被立即删除，需要通过 `expireSnapshots` 操作清理实际存储空间 |
| **Flink CDC 场景** | Flink CDC 写入会持续产生 equality delete 文件，建议配合定期的 compaction 或 `INSERT OVERWRITE` 来控制小文件数量 |

---

## 关键源码文件索引

| 文件 | 关键类/方法 | 作用 |
|------|-----------|------|
| `SparkWrite.java:308-344` | `DynamicOverwrite.commit()` | Spark 动态覆写入口 |
| `SparkWrite.java:346-386` | `OverwriteByFilter.commit()` | Spark 静态覆写入口 |
| `BaseReplacePartitions.java:51-56` | `addFile()` → `dropPartition()` | 动态覆写：按分区替换 |
| `BaseReplacePartitions.java:113-125` | `apply()` | 非分区表：全量替换 |
| `BaseOverwriteFiles.java:63-66` | `overwriteByRowFilter()` | 静态覆写：按条件删除 |
| `MergingSnapshotProducer.java:180-193` | `deleteByRowFilter()` / `dropPartition()` | **核心枢纽：同时操作两个 FilterManager** |
| `ManifestFilterManager.java:131-144` | `deleteByRowFilter()` / `dropPartition()` | 底层 manifest 过滤逻辑 |
| `SnapshotProducer.java:840-870` | `updateTotal()` | 快照 summary 中 total-* 指标的增量计算 |
| `SnapshotProducer.java:420-440` | `updateTotal()` 调用处 | 计算 total-delete-files / total-equality-deletes 等 |
| `SnapshotSummary.java:35-60` | Summary 常量定义 | 所有快照 summary 字段的 key 定义 |

# Iceberg 行级操作演进深度解析：从 Position Delete 到 Deletion Vector，以及 MOR/COW 两条路径

> 基于 Apache Iceberg 最新源码的全面深度分析
> 分析日期：2026-04-15

---

## 目录

- [第一部分：概述与背景](#第一部分概述与背景)
  - [1.1 行级操作的核心挑战](#11-行级操作的核心挑战)
  - [1.2 Iceberg 表格式的版本演进](#12-iceberg-表格式的版本演进)
  - [1.3 行级操作的分类体系](#13-行级操作的分类体系)
- [第二部分：Equality Delete 深度解析](#第二部分equality-delete-深度解析)
  - [2.1 Equality Delete 的设计思想](#21-equality-delete-的设计思想)
  - [2.2 EqualityDeleteWriter 源码分析](#22-equalitydeletewriter-源码分析)
  - [2.3 Equality Delete 文件的写入流程](#23-equality-delete-文件的写入流程)
  - [2.4 Equality Delete 的读取与应用](#24-equality-delete-的读取与应用)
  - [2.5 Equality Delete 与 Sequence Number 的关系](#25-equality-delete-与-sequence-number-的关系)
  - [2.6 Equality Delete 的局限性](#26-equality-delete-的局限性)
- [第三部分：Position Delete 深度解析](#第三部分position-delete-深度解析)
  - [3.1 Position Delete 的设计思想](#31-position-delete-的设计思想)
  - [3.2 PositionDeleteWriter 源码分析](#32-positiondeletewriter-源码分析)
  - [3.3 SortingPositionOnlyDeleteWriter 源码分析](#33-sortingpositiononlydeletewriter-源码分析)
  - [3.4 Position Delete 文件格式与字段](#34-position-delete-文件格式与字段)
  - [3.5 Delete Granularity：FILE vs PARTITION](#35-delete-granularityfile-vs-partition)
  - [3.6 Position Delete 的性能特征](#36-position-delete-的性能特征)
- [第四部分：Deletion Vector（DV）深度解析](#第四部分deletion-vectordv深度解析)
  - [4.1 DV 的设计动机](#41-dv-的设计动机)
  - [4.2 DeletionVector 接口定义](#42-deletionvector-接口定义)
  - [4.3 基于 Roaring Bitmap 的位图实现](#43-基于-roaring-bitmap-的位图实现)
  - [4.4 RoaringPositionBitmap 的 64 位扩展](#44-roaringpositionbitmap-的-64-位扩展)
  - [4.5 BitmapPositionDeleteIndex 序列化格式](#45-bitmappositiondeleteindex-序列化格式)
  - [4.6 DV 文件格式：Puffin 文件](#46-dv-文件格式puffin-文件)
  - [4.7 BaseDVFileWriter 源码分析](#47-basdvfilewriter-源码分析)
  - [4.8 DVUtil：DV 的读取与合并](#48-dvutildv-的读取与合并)
  - [4.9 V3 规范中的 DV 变更](#49-v3-规范中的-dv-变更)
- [第五部分：RowDelta 操作深度解析](#第五部分rowdelta-操作深度解析)
  - [5.1 RowDelta 接口设计](#51-rowdelta-接口设计)
  - [5.2 BaseRowDelta 实现分析](#52-baserowdelta-实现分析)
  - [5.3 冲突检测与验证机制](#53-冲突检测与验证机制)
  - [5.4 RowDelta 的操作类型判定](#54-rowdelta-的操作类型判定)
  - [5.5 MergingSnapshotProducer 基类分析](#55-mergingsnapshotproducer-基类分析)
- [第六部分：Copy-On-Write（COW）路径深度解析](#第六部分copy-on-writecow路径深度解析)
  - [6.1 COW 的设计思想](#61-cow-的设计思想)
  - [6.2 SparkCopyOnWriteOperation 源码分析](#62-sparkcopyonwriteoperation-源码分析)
  - [6.3 SparkCopyOnWriteScan 源码分析](#63-sparkcopyonwritescan-源码分析)
  - [6.4 SparkWrite 中的 CopyOnWriteOperation](#64-sparkwrite-中的-copyonwriteoperation)
  - [6.5 COW 路径下 DELETE/UPDATE/MERGE 的执行流程](#65-cow-路径下-deleteupdatemerge-的执行流程)
  - [6.6 COW 的写放大问题](#66-cow-的写放大问题)
- [第七部分：Merge-On-Read（MOR）路径深度解析](#第七部分merge-on-readmor路径深度解析)
  - [7.1 MOR 的设计思想](#71-mor-的设计思想)
  - [7.2 SparkPositionDeltaOperation 源码分析](#72-sparkpositiondeltaoperation-源码分析)
  - [7.3 SparkPositionDeltaWrite 源码分析](#73-sparkpositiondeltawrite-源码分析)
  - [7.4 MOR 中 Writer 的选择策略](#74-mor-中-writer-的选择策略)
  - [7.5 MOR 路径下 DELETE/UPDATE/MERGE 的执行流程](#75-mor-路径下-deleteupdatemerge-的执行流程)
  - [7.6 DV 在 MOR 中的集成](#76-dv-在-mor-中的集成)
  - [7.7 Previous Delete Rewrite 机制](#77-previous-delete-rewrite-机制)
- [第八部分：读取时 Delete 合并机制](#第八部分读取时-delete-合并机制)
  - [8.1 DeleteFileIndex 源码分析](#81-deletefileindex-源码分析)
  - [8.2 DeleteFilter 源码分析](#82-deletefilter-源码分析)
  - [8.3 PositionDeleteIndex 接口与实现](#83-positiondeleteindex-接口与实现)
  - [8.4 BaseDeleteLoader 源码分析](#84-basedeleteloader-源码分析)
  - [8.5 Deletes 工具类分析](#85-deletes-工具类分析)
  - [8.6 读取时 Delete 合并的完整流程](#86-读取时-delete-合并的完整流程)
- [第九部分：性能对比与权衡](#第九部分性能对比与权衡)
  - [9.1 COW vs MOR 写入性能](#91-cow-vs-mor-写入性能)
  - [9.2 COW vs MOR 读取性能](#92-cow-vs-mor-读取性能)
  - [9.3 Position Delete vs DV 性能](#93-position-delete-vs-dv-性能)
  - [9.4 Equality Delete 的特殊定位](#94-equality-delete-的特殊定位)
  - [9.5 实际场景的选择建议](#95-实际场景的选择建议)
- [第十部分：演进路线与设计动机](#第十部分演进路线与设计动机)
  - [10.1 从 V1 到 V2：引入行级删除](#101-从-v1-到-v2引入行级删除)
  - [10.2 从 V2 到 V3：引入 Deletion Vector](#102-从-v2-到-v3引入-deletion-vector)
  - [10.3 从 Equality Delete 到 Position Delete 的演进](#103-从-equality-delete-到-position-delete-的演进)
  - [10.4 从 Position Delete 到 DV 的演进](#104-从-position-delete-到-dv-的演进)
  - [10.5 未来展望](#105-未来展望)
- [第十一部分：实际场景分析](#第十一部分实际场景分析)
  - [11.1 DELETE FROM 场景](#111-delete-from-场景)
  - [11.2 UPDATE 场景](#112-update-场景)
  - [11.3 MERGE INTO 场景](#113-merge-into-场景)
- [总结](#总结)

---

## 第一部分：概述与背景

### 1.1 行级操作的核心挑战

在传统的数据湖架构中，数据以不可变的文件（如 Parquet、ORC）存储，这种设计非常适合追加写入（append-only）的场景。然而，当需要对已有数据进行修改（更新或删除单行记录）时，就面临一个根本性的挑战：**不可变文件中的单行记录无法被原地修改**。

Apache Iceberg 作为一个高性能的表格式（Table Format），需要在保持文件不可变性的前提下，提供高效的行级操作支持。这就催生了三种不同的技术方案：

1. **Copy-On-Write（COW）**：读取包含待修改行的整个数据文件，过滤掉需要删除/更新的行，将剩余行写入新文件
2. **Merge-On-Read（MOR）+ Delete Files**：写入时仅记录需要删除的行信息（delete file），读取时将 delete file 与 data file 合并
3. **Deletion Vector（DV）**：MOR 的一种优化形式，使用基于 Bitmap 的紧凑格式记录每个数据文件中被删除的行位置

### 1.2 Iceberg 表格式的版本演进

Iceberg 表格式经历了三个主要版本：

- **V1**（初始版本）：仅支持 append-only 操作，不支持行级删除
- **V2**：引入 Delete File 机制，包括 Equality Delete 和 Position Delete，支持 MOR 路径
- **V3**（最新版本）：引入 Deletion Vector，使用 Puffin 文件格式存储基于 Roaring Bitmap 的位图删除向量

在源码中，Format Version 的定义位于 `TableProperties.java`（第 42 行）：

```java
// core/src/main/java/org/apache/iceberg/TableProperties.java
public static final String FORMAT_VERSION = "format-version";
```

### 1.3 行级操作的分类体系

Iceberg 通过 `FileContent` 枚举类定义了文件内容类型：

```java
// api/src/main/java/org/apache/iceberg/FileContent.java
public enum FileContent {
  DATA(0),
  POSITION_DELETES(1),
  EQUALITY_DELETES(2),
  DATA_MANIFEST(3),
  DELETE_MANIFEST(4);
}
```

其中 `POSITION_DELETES` 和 `EQUALITY_DELETES` 是两种 Delete File 的类型。在 V3 规范中，Deletion Vector 复用了 `POSITION_DELETES` 类型，但使用 Puffin 文件格式（而非 Parquet/Avro），通过 `ContentFileUtil.isDV()` 方法来区分：

```java
// core/src/main/java/org/apache/iceberg/util/ContentFileUtil.java (第 142-144 行)
public static boolean isDV(DeleteFile deleteFile) {
    return deleteFile.format() == FileFormat.PUFFIN;
}
```

Iceberg 提供了两种行级操作模式，通过 `RowLevelOperationMode` 枚举定义：

```java
// core/src/main/java/org/apache/iceberg/RowLevelOperationMode.java (第 40-43 行)
public enum RowLevelOperationMode {
  COPY_ON_WRITE("copy-on-write"),
  MERGE_ON_READ("merge-on-read");
}
```

这两种模式可以通过表属性独立配置：

```java
// core/src/main/java/org/apache/iceberg/TableProperties.java
public static final String DELETE_MODE = "write.delete.mode";         // 第 389 行
public static final String DELETE_MODE_DEFAULT = RowLevelOperationMode.COPY_ON_WRITE.modeName(); // 第 390 行

public static final String UPDATE_MODE = "write.update.mode";         // 第 397 行
public static final String UPDATE_MODE_DEFAULT = RowLevelOperationMode.COPY_ON_WRITE.modeName(); // 第 398 行

public static final String MERGE_MODE = "write.merge.mode";           // 第 405 行
public static final String MERGE_MODE_DEFAULT = RowLevelOperationMode.COPY_ON_WRITE.modeName();  // 第 406 行
```

注意：**默认情况下，DELETE/UPDATE/MERGE 都使用 COW 模式**。用户需要显式设置才能切换到 MOR。

---

## 第二部分：Equality Delete 深度解析

### 2.1 Equality Delete 的设计思想

Equality Delete 是 Iceberg V2 引入的一种基于等值条件的删除机制。其核心思想是：**不需要知道被删除行在数据文件中的具体位置，只需要记录被删除行的关键字段值**。

例如，要删除 `id = 5` 的行，只需要在 equality delete 文件中写入一条记录 `{id: 5}`。在读取时，引擎会将数据文件中的每一行与 equality delete 文件中的记录进行匹配，如果某行的 `id` 字段值等于 5，则该行被标记为已删除。

Equality Delete 特别适合以下场景：
- CDC（Change Data Capture）流摄入，其中上游只提供变更记录的 primary key
- Flink 等流式引擎的行级操作
- 无法高效确定行在文件中确切位置的场景

### 2.2 EqualityDeleteWriter 源码分析

`EqualityDeleteWriter` 是 Equality Delete 文件的核心写入器，位于 `core/src/main/java/org/apache/iceberg/deletes/EqualityDeleteWriter.java`。

```java
// core/src/main/java/org/apache/iceberg/deletes/EqualityDeleteWriter.java (第 36-105 行)
public class EqualityDeleteWriter<T> implements FileWriter<T, DeleteWriteResult> {
  private final FileAppender<T> appender;
  private final FileFormat format;
  private final String location;
  private final PartitionSpec spec;
  private final StructLike partition;
  private final ByteBuffer keyMetadata;
  private final int[] equalityFieldIds;
  private final SortOrder sortOrder;
  private DeleteFile deleteFile = null;
```

关键设计要点：

1. **泛型设计**：`EqualityDeleteWriter<T>` 是泛型的，`T` 代表记录类型。它实现了 `FileWriter<T, DeleteWriteResult>` 接口，可以与不同的记录格式（如 Parquet、Avro）配合使用。

2. **equalityFieldIds**：这是 equality delete 的核心参数，定义了用于判断等值匹配的字段 ID 列表。例如，如果 `equalityFieldIds = [1, 2]`，则只有当数据行的 field 1 和 field 2 同时匹配 delete 记录时，该行才被视为已删除。

3. **sortOrder**：可选的排序顺序，用于对 equality delete 记录进行排序以提高读取效率。

**写入过程**非常简洁：

```java
// 第 67-69 行
@Override
public void write(T row) {
    appender.add(row);
}
```

写入就是简单地通过底层 `FileAppender` 追加记录。Equality Delete 的核心复杂性不在写入端，而在读取端。

**关闭与生成 DeleteFile 元数据**：

```java
// 第 77-94 行
@Override
public void close() throws IOException {
    if (deleteFile == null) {
      appender.close();
      this.deleteFile =
          FileMetadata.deleteFileBuilder(spec)
              .ofEqualityDeletes(equalityFieldIds)  // 标记为 equality delete
              .withFormat(format)
              .withPath(location)
              .withPartition(partition)
              .withEncryptionKeyMetadata(
                  EncryptionUtil.setFileLength(keyMetadata, appender.length()))
              .withFileSizeInBytes(appender.length())
              .withMetrics(appender.metrics())
              .withSplitOffsets(appender.splitOffsets())
              .withSortOrder(sortOrder)
              .build();
    }
}
```

关闭时，通过 `FileMetadata.deleteFileBuilder(spec).ofEqualityDeletes(equalityFieldIds)` 构建 `DeleteFile` 元数据。注意 `ofEqualityDeletes()` 方法接收 `equalityFieldIds` 参数，这些 ID 会被持久化到 manifest 文件中，以便读取时知道哪些字段需要进行等值匹配。

### 2.3 Equality Delete 文件的写入流程

Equality Delete 文件的写入流程可以概括为：

1. **创建 EqualityDeleteWriter**：指定 appender（如 Parquet appender）、文件格式、位置、partition spec 和 equality field IDs
2. **写入删除记录**：对于每条需要删除的记录，将其等值字段的值写入 delete 文件
3. **关闭 Writer**：生成 `DeleteFile` 元数据，包含 equality field IDs、文件统计信息等
4. **通过 RowDelta 提交**：将 `DeleteFile` 添加到 `RowDelta` 操作中并提交

```
用户操作: DELETE FROM table WHERE id = 5
    |
    v
引擎生成等值删除记录: {id: 5}
    |
    v
EqualityDeleteWriter.write({id: 5})
    |
    v
FileAppender 写入 Parquet/Avro 文件
    |
    v
EqualityDeleteWriter.close() -> 生成 DeleteFile 元数据
    |
    v
RowDelta.addDeletes(deleteFile) -> 提交到表
```

### 2.4 Equality Delete 的读取与应用

Equality Delete 的读取发生在 `DeleteFilter` 类中，这是最复杂的部分。

在 `DeleteFilter` 的构造函数中（`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java` 第 69-116 行），delete 文件按类型分为两组：

```java
// 第 90-109 行
ImmutableList.Builder<DeleteFile> posDeleteBuilder = ImmutableList.builder();
ImmutableList.Builder<DeleteFile> eqDeleteBuilder = ImmutableList.builder();
for (DeleteFile delete : deletes) {
    switch (delete.content()) {
        case POSITION_DELETES:
            posDeleteBuilder.add(delete);
            break;
        case EQUALITY_DELETES:
            eqDeleteBuilder.add(delete);
            break;
        default:
            throw new UnsupportedOperationException(
                "Unknown delete file content: " + delete.content());
    }
}
```

Equality Delete 的应用逻辑在 `applyEqDeletes()` 方法中（第 192-225 行）：

```java
// 第 192-225 行
private List<Predicate<T>> applyEqDeletes() {
    if (isInDeleteSets != null) {
        return isInDeleteSets;
    }

    isInDeleteSets = Lists.newArrayList();
    if (eqDeletes.isEmpty()) {
        return isInDeleteSets;
    }

    // 按 equality field IDs 分组，相同字段组合的 delete 文件合并处理
    Multimap<Set<Integer>, DeleteFile> filesByDeleteIds =
        Multimaps.newMultimap(Maps.newHashMap(), Lists::newArrayList);
    for (DeleteFile delete : eqDeletes) {
        filesByDeleteIds.put(Sets.newHashSet(delete.equalityFieldIds()), delete);
    }

    for (Map.Entry<Set<Integer>, Collection<DeleteFile>> entry :
        filesByDeleteIds.asMap().entrySet()) {
        Set<Integer> ids = entry.getKey();
        Iterable<DeleteFile> deletes = entry.getValue();

        Schema deleteSchema = TypeUtil.selectInIdOrder(requiredSchema, ids);
        StructProjection projectRow = StructProjection.create(requiredSchema, deleteSchema);

        // 加载所有等值删除记录到一个 Set 中
        StructLikeSet deleteSet = deleteLoader().loadEqualityDeletes(deletes, deleteSchema);
        // 创建判断谓词：记录是否在删除集合中
        Predicate<T> isInDeleteSet =
            record -> deleteSet.contains(projectRow.wrap(asStructLike(record)));
        isInDeleteSets.add(isInDeleteSet);
    }

    return isInDeleteSets;
}
```

这段代码的核心逻辑是：

1. **按 equality field IDs 分组**：不同的 equality delete 文件可能使用不同的字段组合，需要分别处理
2. **加载到内存 Set**：通过 `deleteLoader().loadEqualityDeletes()` 将所有等值删除记录加载到一个 `StructLikeSet` 中
3. **构建匹配谓词**：对于每条数据记录，投影出 equality 字段后检查是否在删除集合中

最终在 `filter()` 方法中（第 188-189 行），先应用 position delete，再应用 equality delete：

```java
public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
}
```

### 2.5 Equality Delete 与 Sequence Number 的关系

Equality Delete 使用一个特殊的规则来确定其作用范围：**equality delete 文件只作用于 sequence number 严格小于 delete 文件自身 sequence number 的数据文件**。

这在 `DeleteFileIndex.EqualityDeleteFile` 中体现：

```java
// core/src/main/java/org/apache/iceberg/DeleteFileIndex.java (第 843 行)
this.applySequenceNumber = wrapped.dataSequenceNumber() - 1;
```

`applySequenceNumber` 被设置为 `dataSequenceNumber - 1`，这意味着 equality delete 只匹配在它之前写入的数据。这一设计避免了同一 snapshot 中新写入的数据行被同时写入的 equality delete 错误删除的问题。

### 2.6 Equality Delete 的局限性

Equality Delete 存在几个显著的局限性：

1. **读取时开销大**：需要将所有 equality delete 记录加载到内存中，对于大量删除操作可能导致 OOM
2. **全表扫描匹配**：每个 data file 都可能需要与 equality delete 进行匹配（虽然 `canContainEqDeletesForFile` 方法利用文件统计信息进行了优化过滤）
3. **累积效应**：未 compact 的 equality delete 文件会不断累积，随着时间推移读取性能持续下降
4. **字段类型限制**：等值匹配仅适用于基本类型，复杂类型（nested types）的统计信息不可用

由于这些局限性，在 Spark 等批处理引擎中，通常不直接使用 Equality Delete。Spark 的 MOR 路径使用的是 Position Delete 或 DV。Equality Delete 更多用于 Flink 等流式引擎。

---

## 第三部分：Position Delete 深度解析

### 3.1 Position Delete 的设计思想

Position Delete 是一种更精确的删除机制。与 Equality Delete 通过字段值匹配不同，Position Delete 通过 **数据文件路径（file_path）** 和 **行位置（pos）** 精确标识被删除的行。

Position Delete 的核心优势在于：
- 精确定位，不需要全表扫描匹配
- 读取时可以利用行号快速跳过删除行
- 不需要加载大量删除记录到内存

### 3.2 PositionDeleteWriter 源码分析

`PositionDeleteWriter` 位于 `core/src/main/java/org/apache/iceberg/deletes/PositionDeleteWriter.java`：

```java
// 第 50 行
public class PositionDeleteWriter<T> implements FileWriter<PositionDelete<T>, DeleteWriteResult> {
  private static final Set<Integer> FILE_AND_POS_FIELD_IDS =
      ImmutableSet.of(DELETE_FILE_PATH.fieldId(), DELETE_FILE_POS.fieldId());

  private final FileAppender<PositionDelete<T>> appender;
  private final FileFormat format;
  private final String location;
  private final PartitionSpec spec;
  private final StructLike partition;
  private final ByteBuffer keyMetadata;
  private final CharSequenceSet referencedDataFiles;
  private DeleteFile deleteFile = null;
```

关键设计要点：

1. **PositionDelete<T> 类型**：写入的每条记录是一个 `PositionDelete<T>`，包含文件路径和行位置
2. **referencedDataFiles**：跟踪所有被引用的数据文件路径，这是冲突检测的关键信息

**写入逻辑**：

```java
// 第 88-91 行
@Override
public void write(PositionDelete<T> positionDelete) {
    referencedDataFiles.add(positionDelete.path());
    appender.add(positionDelete);
}
```

每次写入时，除了追加记录到底层 appender，还会将被删除行所在的数据文件路径添加到 `referencedDataFiles` 集合中。

**Metrics 处理**：

```java
// 第 131-138 行
private Metrics metrics() {
    Metrics metrics = appender.metrics();
    if (referencedDataFiles.size() > 1) {
      return MetricsUtil.copyWithoutFieldCountsAndBounds(metrics, FILE_AND_POS_FIELD_IDS);
    } else {
      return MetricsUtil.copyWithoutFieldCounts(metrics, FILE_AND_POS_FIELD_IDS);
    }
}
```

这里有一个精妙的优化：当 position delete 文件只引用一个数据文件时（即 `referencedDataFiles.size() == 1`），保留 `file_path` 字段的 lower/upper bounds。这使得 `DeleteFileIndex` 可以通过 bounds 快速判断某个 position delete 文件是否只关联到特定数据文件，从而在规划时只将其分配给对应的数据文件，极大减少不必要的 I/O。

如果引用了多个数据文件（`referencedDataFiles.size() > 1`），则同时移除 bounds 和 field counts，因为跨文件的 bounds 对过滤没有意义。

### 3.3 SortingPositionOnlyDeleteWriter 源码分析

`SortingPositionOnlyDeleteWriter` 是一个更高级的 position delete writer，位于 `core/src/main/java/org/apache/iceberg/deletes/SortingPositionOnlyDeleteWriter.java`。它的核心能力是处理**无序输入**。

Iceberg 规范要求 position delete 文件中的记录必须按 `(file_path, pos)` 排序。当上游无法保证排序时，`SortingPositionOnlyDeleteWriter` 通过内存中的 Bitmap 来缓冲所有删除位置，在关闭时一次性排序输出。

```java
// 第 52-53 行
public class SortingPositionOnlyDeleteWriter<T>
    implements FileWriter<PositionDelete<T>, DeleteWriteResult> {

  private final Supplier<FileWriter<PositionDelete<T>, DeleteWriteResult>> writers;
  private final DeleteGranularity granularity;
  private final CharSequenceMap<PositionDeleteIndex> positionsByPath;
  private final Function<CharSequence, PositionDeleteIndex> loadPreviousDeletes;
```

**写入逻辑**：

```java
// 第 82-88 行
@Override
public void write(PositionDelete<T> positionDelete) {
    CharSequence path = positionDelete.path();
    long position = positionDelete.pos();
    PositionDeleteIndex positions =
        positionsByPath.computeIfAbsent(path, key -> new BitmapPositionDeleteIndex());
    positions.delete(position);
}
```

写入时，按文件路径分组，将每个位置存储在一个 `BitmapPositionDeleteIndex`（基于 Roaring Bitmap）中。这意味着：
1. **自动去重**：相同位置只会记录一次
2. **内存高效**：Roaring Bitmap 具有优秀的压缩性能
3. **排序免费**：Bitmap 天然按顺序输出位置

**关闭时的输出逻辑**取决于 `DeleteGranularity`：

```java
// 第 101-114 行
@Override
public void close() throws IOException {
    if (result == null) {
      switch (granularity) {
        case FILE:
          this.result = writeFileDeletes();  // 每个数据文件一个 delete 文件
          return;
        case PARTITION:
          this.result = writePartitionDeletes();  // 同分区的所有删除写入一个文件
          return;
        default:
          throw new UnsupportedOperationException("Unsupported delete granularity: " + granularity);
      }
    }
}
```

**Previous Deletes 合并**是一个非常重要的特性（第 138-166 行）：

```java
// 第 138-166 行
private DeleteWriteResult writeDeletes(Collection<CharSequence> paths) throws IOException {
    // ...
    try {
      PositionDelete<T> positionDelete = PositionDelete.create();
      for (CharSequence path : sort(paths)) {
        PositionDeleteIndex positions = positionsByPath.get(path);
        PositionDeleteIndex previousPositions = loadPreviousDeletes.apply(path);
        if (previousPositions != null && previousPositions.isNotEmpty()) {
          validatePreviousDeletes(previousPositions);
          positions.merge(previousPositions);
          rewrittenDeleteFiles.addAll(previousPositions.deleteFiles());
        }
        positions.forEach(position -> writer.write(positionDelete.set(path, position)));
      }
    } finally {
      writer.close();
    }
    // ...
}
```

当存在之前的 file-scoped delete 文件时，writer 会加载这些旧的删除位置，将新旧位置合并后输出到一个新文件，同时标记旧文件为可删除（`rewrittenDeleteFiles`）。这实现了**增量 compaction**，避免了一个数据文件关联过多 delete 文件。

### 3.4 Position Delete 文件格式与字段

Position Delete 文件包含两个核心元数据列，定义在 `MetadataColumns` 中：

- `file_path`（field id: 2147483546）：被删除行所在数据文件的路径，类型为 `StringType`
- `pos`（field id: 2147483545）：被删除行在数据文件中的位置（0-based），类型为 `LongType`

```
Position Delete 文件结构（Parquet/Avro）:
+--------------------------------+----------+
| file_path (string)             | pos (long)|
+--------------------------------+----------+
| s3://bucket/data/file1.parquet | 0        |
| s3://bucket/data/file1.parquet | 5        |
| s3://bucket/data/file1.parquet | 12       |
| s3://bucket/data/file2.parquet | 3        |
+--------------------------------+----------+
```

按照 Iceberg 规范，Position Delete 文件中的记录必须按 `(file_path, pos)` 升序排列。

### 3.5 Delete Granularity：FILE vs PARTITION

`DeleteGranularity` 枚举定义了 position delete 的粒度（`core/src/main/java/org/apache/iceberg/deletes/DeleteGranularity.java`）：

```java
// 第 45-47 行
public enum DeleteGranularity {
  FILE,
  PARTITION;
}
```

两种粒度的区别：

**PARTITION 粒度**：
- 同一分区内所有数据文件的删除信息写入同一个 position delete 文件
- 优点：减少 delete 文件数量
- 缺点：扫描单个数据文件时需要读取整个 partition-scoped delete 文件，包含大量无关数据

**FILE 粒度**：
- 每个数据文件对应一个独立的 position delete 文件
- 优点：扫描规划精确，只分配相关的 delete 文件
- 缺点：增加 delete 文件数量，需要更频繁的 compaction

在 `DeleteFileIndex` 的构建过程中（第 502-509 行），这两种粒度的 delete 文件被分别索引：

```java
// core/src/main/java/org/apache/iceberg/DeleteFileIndex.java (第 502-509 行)
for (DeleteFile file : files) {
    switch (file.content()) {
      case POSITION_DELETES:
        if (ContentFileUtil.isDV(file)) {
          add(dvByPath, file);        // DV 按路径索引
        } else {
          add(posDeletesByPath, posDeletesByPartition, file);  // 区分 file-scoped 和 partition-scoped
        }
        break;
      case EQUALITY_DELETES:
        add(globalDeletes, eqDeletesByPartition, file, fieldLookup);
        break;
    }
}
```

`ContentFileUtil.referencedDataFile()` 方法通过检查 `file_path` 字段的 lower/upper bounds 是否相等来判断 position delete 文件是否为 file-scoped：

```java
// core/src/main/java/org/apache/iceberg/util/ContentFileUtil.java (第 63-89 行)
public static CharSequence referencedDataFile(DeleteFile deleteFile) {
    if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
      return null;
    }
    if (deleteFile.referencedDataFile() != null) {
      return deleteFile.referencedDataFile();
    }
    // 检查 file_path 的 lower/upper bounds 是否相等
    Map<Integer, ByteBuffer> lowerBounds = deleteFile.lowerBounds();
    ByteBuffer lowerPathBound = lowerBounds != null ? lowerBounds.get(PATH_ID) : null;
    Map<Integer, ByteBuffer> upperBounds = deleteFile.upperBounds();
    ByteBuffer upperPathBound = upperBounds != null ? upperBounds.get(PATH_ID) : null;
    if (lowerPathBound != null && upperPathBound != null && lowerPathBound.equals(upperPathBound)) {
      return Conversions.fromByteBuffer(PATH_TYPE, lowerPathBound);
    } else {
      return null;  // 无法确定单一引用文件
    }
}
```

### 3.6 Position Delete 的性能特征

Position Delete 相比 Equality Delete 有显著的性能优势：

1. **精确匹配**：通过 `(file_path, pos)` 直接定位，无需全字段比较
2. **Bitmap 加速**：加载到内存后使用 Bitmap，`isDeleted(pos)` 操作为 O(1)
3. **可过滤性**：file-scoped 的 position delete 在规划阶段就能精确分配给对应的数据文件
4. **Sequence Number 过滤**：只有 sequence number >= data file 的 delete 文件才需要应用

但 Position Delete 也有局限性：
- 需要知道行在文件中的精确位置（position），这在流式场景中不总是可用
- 跨文件的 position delete 文件会导致读取放大

---

## 第四部分：Deletion Vector（DV）深度解析

### 4.1 DV 的设计动机

Deletion Vector 是 Position Delete 的自然演进，其设计动机在于解决传统 Position Delete 的几个痛点：

1. **一对一绑定**：每个 DV 严格对应一个数据文件，避免了 partition-scoped delete 的读取放大问题
2. **紧凑存储**：使用 Roaring Bitmap 序列化格式，比 Parquet/Avro 格式的 position delete 文件更紧凑
3. **原子更新**：DV 可以被原子地替换（每个数据文件最多一个 DV），简化了 delete file 的管理
4. **读取高效**：直接反序列化为 Bitmap，无需解析 Parquet/Avro 文件

### 4.2 DeletionVector 接口定义

`DeletionVector` 接口定义在 `core/src/main/java/org/apache/iceberg/DeletionVector.java`：

```java
// 第 29-64 行
interface DeletionVector {
  Types.NestedField LOCATION =
      Types.NestedField.required(
          155, "location", Types.StringType.get(), "Location of the file containing the DV");
  Types.NestedField OFFSET =
      Types.NestedField.required(
          144, "offset", Types.LongType.get(), "Offset in the file where the DV content starts");
  Types.NestedField SIZE_IN_BYTES =
      Types.NestedField.required(
          145, "size_in_bytes", Types.LongType.get(),
          "Length of the referenced DV content stored in the file");
  Types.NestedField CARDINALITY =
      Types.NestedField.required(
          156, "cardinality", Types.LongType.get(),
          "Number of set bits (deleted rows) in the vector");

  String location();
  long offset();
  long sizeInBytes();
  long cardinality();
}
```

DeletionVector 的 schema 由四个字段组成：
- **location**：包含 DV 内容的 Puffin 文件路径
- **offset**：DV 内容在 Puffin 文件中的起始偏移量
- **size_in_bytes**：DV 内容的字节长度
- **cardinality**：被删除的行数（Bitmap 中 set bit 的数量）

这个设计使得多个 DV 可以存储在同一个 Puffin 文件中（通过不同的 offset 和 size 区分），这在批量操作时可以减少小文件数量。

### 4.3 基于 Roaring Bitmap 的位图实现

DV 的核心数据结构是 Roaring Bitmap，其 Iceberg 实现位于 `core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java`。

Roaring Bitmap 是一种高效的压缩位图数据结构，它将整数集合按高 16 位分组为 "容器"（container），每个容器根据元素密度选择不同的存储策略：
- **Array Container**：适合稀疏数据（元素 < 4096 个），使用有序数组
- **Bitmap Container**：适合密集数据（元素 >= 4096 个），使用 65536 位的位图
- **Run Container**：适合连续范围，使用 run-length encoding

Iceberg 的 `RoaringPositionBitmap` 在标准 32 位 Roaring Bitmap 基础上扩展了 64 位支持。

### 4.4 RoaringPositionBitmap 的 64 位扩展

标准 Roaring Bitmap 仅支持 32 位无符号整数。Iceberg 通过分层结构扩展到 64 位：

```java
// core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java (第 51-54 行)
class RoaringPositionBitmap {
  static final long MAX_POSITION = toPosition(Integer.MAX_VALUE - 1, Integer.MIN_VALUE);
  private static final RoaringBitmap[] EMPTY_BITMAP_ARRAY = new RoaringBitmap[0];
  private RoaringBitmap[] bitmaps;
```

**位分割策略**：将 64 位位置分为高 32 位（key）和低 32 位（32-bit position）：

```java
// 第 291-298 行
// 提取高 32 位作为 key
private static int key(long pos) {
    return (int) (pos >> 32);
}

// 提取低 32 位作为 32-bit position
private static int pos32Bits(long pos) {
    return (int) pos;
}
```

内部维护一个 `RoaringBitmap[]` 数组，数组索引就是 key（高 32 位），每个元素是一个标准的 32 位 Roaring Bitmap。

**写入操作**：

```java
// 第 73-79 行
public void set(long pos) {
    validatePosition(pos);
    int key = key(pos);
    int pos32Bits = pos32Bits(pos);
    allocateBitmapsIfNeeded(key + 1);
    bitmaps[key].add(pos32Bits);
}
```

**查询操作**：

```java
// 第 111-116 行
public boolean contains(long pos) {
    validatePosition(pos);
    int key = key(pos);
    int pos32Bits = pos32Bits(pos);
    return key < bitmaps.length && bitmaps[key].contains(pos32Bits);
}
```

对于大多数实际场景，数据文件的行数远小于 2^32（约 42 亿），因此 `bitmaps` 数组通常只有 1 个元素（key = 0），等价于直接使用 32 位 Roaring Bitmap。

**序列化格式**：

```java
// 第 214-221 行
public void serialize(ByteBuffer buffer) {
    validateByteOrder(buffer);
    buffer.putLong(bitmaps.length);  // 8 字节：bitmap 数量
    for (int key = 0; key < bitmaps.length; key++) {
      buffer.putInt(key);             // 4 字节：key
      bitmaps[key].serialize(buffer); // 标准 Roaring 序列化
    }
}
```

序列化格式为小端字节序（little-endian），兼容标准 Roaring Bitmap 序列化规范。

### 4.5 BitmapPositionDeleteIndex 序列化格式

`BitmapPositionDeleteIndex` 包装了 `RoaringPositionBitmap` 并添加了 DV 文件格式的序列化层：

```java
// core/src/main/java/org/apache/iceberg/deletes/BitmapPositionDeleteIndex.java
// 第 112-137 行
/**
 * 序列化格式：
 * 1. 4 字节（big-endian）：magic bytes 和 bitmap 的总长度
 * 2. 4 字节（little-endian）：MAGIC_NUMBER (1681511377)
 * 3. Roaring bitmap（little-endian）：使用 portable Roaring spec 序列化
 * 4. 4 字节（big-endian）：CRC-32 校验和
 */
@Override
public ByteBuffer serialize() {
    bitmap.runLengthEncode(); // 先进行 run-length 编码优化
    int bitmapDataLength = computeBitmapDataLength(bitmap);
    byte[] bytes = new byte[LENGTH_SIZE_BYTES + bitmapDataLength + CRC_SIZE_BYTES];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.putInt(bitmapDataLength);
    serializeBitmapData(bytes, bitmapDataLength, bitmap);
    int crcOffset = LENGTH_SIZE_BYTES + bitmapDataLength;
    int crc = computeChecksum(bytes, bitmapDataLength);
    buffer.putInt(crcOffset, crc);
    buffer.rewind();
    return buffer;
}
```

序列化格式详细说明：
```
+-------------------------------------------+
| Length (4 bytes, big-endian)               |  <- magic bytes + bitmap 的总长度
+-------------------------------------------+
| Magic Number (4 bytes, little-endian)      |  <- 1681511377
+-------------------------------------------+
| Roaring Bitmap (variable, little-endian)   |  <- 标准 Roaring 序列化
+-------------------------------------------+
| CRC-32 Checksum (4 bytes, big-endian)      |  <- magic + bitmap 的 CRC
+-------------------------------------------+
```

这个格式的 bitmap data 部分（magic number + bitmap）与 Delta Lake 的 DV 格式兼容，这是有意为之的设计。

### 4.6 DV 文件格式：Puffin 文件

DV 存储在 Puffin 文件中。Puffin 是 Iceberg 定义的一种通用容器格式，用于存储表级别的统计信息和其他 blob 数据。

DV 在 Puffin 中使用的 blob 类型为 `deletion-vector-v1`：

```java
// core/src/main/java/org/apache/iceberg/puffin/StandardBlobTypes.java
public static final String DV_V1 = "deletion-vector-v1";
```

每个 Puffin 文件可以包含多个 blob，每个 blob 对应一个数据文件的 DV。在 `BaseDVFileWriter` 中创建 blob 的代码：

```java
// core/src/main/java/org/apache/iceberg/deletes/BaseDVFileWriter.java (第 173-186 行)
private Blob toBlob(PositionDeleteIndex positions, String path) {
    return new Blob(
        StandardBlobTypes.DV_V1,                             // blob 类型
        ImmutableList.of(MetadataColumns.ROW_POSITION.fieldId()),  // 关联字段
        -1 /* snapshot ID is inherited */,
        -1 /* sequence number is inherited */,
        positions.serialize(),                                // DV 内容（序列化的 Bitmap）
        null /* uncompressed */,
        ImmutableMap.of(
            REFERENCED_DATA_FILE_KEY, path,                  // 关联的数据文件路径
            CARDINALITY_KEY, String.valueOf(positions.cardinality())));  // 删除行数
}
```

Puffin 文件的整体结构为：
```
Puffin 文件:
  ├── Header (magic bytes)
  ├── Blob 1 (DV for data_file_1)
  │   ├── Type: "deletion-vector-v1"
  │   ├── Properties: {"referenced-data-file": "path/to/file1", "cardinality": "100"}
  │   └── Data: [serialized RoaringBitmap]
  ├── Blob 2 (DV for data_file_2)
  │   ├── Type: "deletion-vector-v1"
  │   └── ...
  ├── Footer (blob metadata, offsets, lengths)
  └── Footer Length + Magic
```

### 4.7 BaseDVFileWriter 源码分析

`BaseDVFileWriter` 是 DV 文件的核心写入器，位于 `core/src/main/java/org/apache/iceberg/deletes/BaseDVFileWriter.java`。

```java
// 第 50-71 行
public class BaseDVFileWriter implements DVFileWriter {
  private static final String REFERENCED_DATA_FILE_KEY = "referenced-data-file";
  private static final String CARDINALITY_KEY = "cardinality";

  private final Supplier<OutputFile> dvOutputFile;
  private final Function<String, PositionDeleteIndex> loadPreviousDeletes;
  private final Map<String, Deletes> deletesByPath = Maps.newHashMap();
  private final Map<String, BlobMetadata> blobsByPath = Maps.newHashMap();
  private DeleteWriteResult result = null;
```

**删除位置记录**：

```java
// 第 74-79 行
@Override
public void delete(String path, long pos, PartitionSpec spec, StructLike partition) {
    Deletes deletes =
        deletesByPath.computeIfAbsent(path, key -> new Deletes(path, spec, partition));
    PositionDeleteIndex positions = deletes.positions();
    positions.delete(pos);
}
```

每次调用 `delete()` 时，按数据文件路径分组，将位置添加到对应的 `BitmapPositionDeleteIndex` 中。

**关闭与写出**（第 99-143 行）是核心逻辑：

```java
@Override
public void close() throws IOException {
    if (result == null) {
      List<DeleteFile> dvs = Lists.newArrayList();
      CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
      List<DeleteFile> rewrittenDeleteFiles = Lists.newArrayList();

      if (deletesByPath.isEmpty()) {
        this.result = new DeleteWriteResult(dvs, referencedDataFiles, rewrittenDeleteFiles);
        return;
      }

      PuffinWriter writer = newWriter();

      try (PuffinWriter closeableWriter = writer) {
        for (Deletes deletes : deletesByPath.values()) {
          String path = deletes.path();
          PositionDeleteIndex positions = deletes.positions();

          // 加载并合并之前的删除
          PositionDeleteIndex previousPositions = loadPreviousDeletes.apply(path);
          if (previousPositions != null) {
            positions.merge(previousPositions);
            for (DeleteFile previousDeleteFile : previousPositions.deleteFiles()) {
              if (ContentFileUtil.isFileScoped(previousDeleteFile)) {
                rewrittenDeleteFiles.add(previousDeleteFile);
              }
            }
          }

          write(closeableWriter, deletes);
          referencedDataFiles.add(path);
        }
      }

      // 所有 DV 共享同一个 Puffin 文件路径和大小
      String puffinPath = writer.location();
      long puffinFileSize = writer.fileSize();

      for (String path : deletesByPath.keySet()) {
        DeleteFile dv = createDV(puffinPath, puffinFileSize, path);
        dvs.add(dv);
      }

      this.result = new DeleteWriteResult(dvs, referencedDataFiles, rewrittenDeleteFiles);
    }
}
```

关键点：
1. **Previous Deletes 合并**：写入新 DV 之前，先加载数据文件之前的 position deletes（可能是旧的 DV 或 file-scoped position delete），将新旧删除位置合并
2. **单文件多 DV**：所有 DV blob 写入同一个 Puffin 文件，但每个 DV 生成独立的 `DeleteFile` 元数据（不同的 offset 和 size）
3. **Rewritten Delete Files**：合并了 previous deletes 后，旧的 delete 文件被标记为可删除

**CreateDV** 方法生成 DeleteFile 元数据：

```java
// 第 145-159 行
private DeleteFile createDV(String path, long size, String referencedDataFile) {
    Deletes deletes = deletesByPath.get(referencedDataFile);
    BlobMetadata blobMetadata = blobsByPath.get(referencedDataFile);
    return FileMetadata.deleteFileBuilder(deletes.spec())
        .ofPositionDeletes()                        // DV 在元数据层面仍是 Position Delete 类型
        .withFormat(FileFormat.PUFFIN)              // 但使用 PUFFIN 格式
        .withPath(path)                             // Puffin 文件路径
        .withPartition(deletes.partition())
        .withFileSizeInBytes(size)                  // 整个 Puffin 文件大小
        .withReferencedDataFile(referencedDataFile) // 关联的数据文件
        .withContentOffset(blobMetadata.offset())   // blob 在 Puffin 中的偏移
        .withContentSizeInBytes(blobMetadata.length()) // blob 的大小
        .withRecordCount(deletes.positions().cardinality()) // 删除行数
        .build();
}
```

### 4.8 DVUtil：DV 的读取与合并

`DVUtil` 位于 `core/src/main/java/org/apache/iceberg/DVUtil.java`，提供了 DV 的读取和合并工具方法。

**读取 DV**：

```java
// 第 50-65 行
static PositionDeleteIndex readDV(DeleteFile deleteFile, FileIO fileIO) {
    Preconditions.checkArgument(
        ContentFileUtil.isDV(deleteFile),
        "Cannot read, not a deletion vector: %s", deleteFile.location());
    InputFile inputFile = fileIO.newInputFile(deleteFile);
    long offset = deleteFile.contentOffset();
    int length = deleteFile.contentSizeInBytes().intValue();
    byte[] bytes = new byte[length];
    try {
      IOUtil.readFully(inputFile, offset, bytes, 0, length);
      return PositionDeleteIndex.deserialize(bytes, deleteFile);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
}
```

DV 的读取非常高效：直接定位到 Puffin 文件中的特定 offset，读取指定长度的字节，然后反序列化为 `BitmapPositionDeleteIndex`。不需要解析完整的 Puffin 文件结构。

**合并重复 DV**（第 78-110 行）：

```java
static List<DeleteFile> mergeAndWriteDVsIfRequired(
    Map<String, List<DeleteFile>> dvsByReferencedFile,
    String mergedOutputLocation,
    FileIO fileIO,
    Map<Integer, PartitionSpec> specs,
    ExecutorService pool) {

  List<DeleteFile> finalDVs = Lists.newArrayList();
  Multimap<String, DeleteFile> duplicates = ...;

  for (Map.Entry<String, List<DeleteFile>> entry : dvsByReferencedFile.entrySet()) {
    if (entry.getValue().size() > 1) {
      duplicates.putAll(entry.getKey(), entry.getValue());  // 有重复 DV
    } else {
      finalDVs.addAll(entry.getValue());  // 无重复，直接保留
    }
  }

  if (duplicates.isEmpty()) {
    return finalDVs;
  }

  // 并行读取重复 DV，合并后写入新文件
  Map<String, PositionDeleteIndex> deletes =
      readAndMergeDVs(duplicates.values().toArray(DeleteFile[]::new), fileIO, pool);
  finalDVs.addAll(writeDVs(deletes, fileIO, mergedOutputLocation, partitions));
  return finalDVs;
}
```

这个方法处理一个数据文件可能关联多个 DV 的情况。在 `MergingSnapshotProducer` 的提交过程中，如果由于并发操作导致同一数据文件出现多个 DV，需要将它们合并为一个。

### 4.9 V3 规范中的 DV 变更

V3 规范对表元数据进行了重要扩展以支持 DV。在 `TrackedFile` 接口中（`core/src/main/java/org/apache/iceberg/TrackedFile.java`），可以看到 V3 新增的字段：

```java
// 第 63-65 行
Types.NestedField DELETION_VECTOR =
    Types.NestedField.optional(
        148, "deletion_vector", DeletionVector.schema(), "Deletion vector for the data file");
```

`deletion_vector` 字段是 V3 中新增的 optional 字段，它直接在 manifest entry 级别存储 DV 信息，包括 location、offset、size_in_bytes 和 cardinality。

V3 规范的关键变更包括：

1. **DV 作为 Position Delete 的特殊形式**：DV 使用 `FileContent.POSITION_DELETES` 类型，但文件格式为 PUFFIN
2. **一对一约束**：每个数据文件最多关联一个 DV（在 `DeleteFileIndex` 中通过 `dvByPath` 维护）
3. **DV 验证**：在 `DeleteFileIndex.findDV()` 中验证 DV 的 sequence number 约束
4. **Sequence Number 约束**：DV 的 `dataSequenceNumber` 必须 >= 关联数据文件的 sequence number

```java
// core/src/main/java/org/apache/iceberg/DeleteFileIndex.java (第 202-216 行)
private DeleteFile findDV(long seq, DataFile dataFile) {
    if (dvByPath == null) {
      return null;
    }
    DeleteFile dv = dvByPath.get(dataFile.location());
    if (dv != null) {
      ValidationException.check(
          dv.dataSequenceNumber() >= seq,
          "DV data sequence number (%s) must be >= data file sequence number (%s)",
          dv.dataSequenceNumber(), seq);
    }
    return dv;
}
```

---

## 第五部分：RowDelta 操作深度解析

### 5.1 RowDelta 接口设计

`RowDelta` 是 Iceberg 中专门为行级操作设计的 API，定义在 `api/src/main/java/org/apache/iceberg/RowDelta.java`：

```java
// 第 32 行
public interface RowDelta extends SnapshotUpdate<RowDelta> {
    RowDelta addRows(DataFile inserts);           // 添加数据文件
    RowDelta addDeletes(DeleteFile deletes);      // 添加删除文件
    RowDelta removeRows(DataFile file);           // 移除数据文件
    RowDelta removeDeletes(DeleteFile deletes);   // 移除删除文件（用于 delete file compaction）
    RowDelta validateFromSnapshot(long snapshotId);
    RowDelta validateDataFilesExist(Iterable<? extends CharSequence> referencedFiles);
    RowDelta validateDeletedFiles();
    RowDelta conflictDetectionFilter(Expression conflictDetectionFilter);
    RowDelta validateNoConflictingDataFiles();
    RowDelta validateNoConflictingDeleteFiles();
}
```

`RowDelta` 的核心能力是**在一个原子操作中同时添加数据文件和删除文件**。这对于 MOR 模式下的 UPDATE/MERGE 操作至关重要：更新一行记录需要同时添加一个 delete file（标记旧行已删除）和一个 data file（包含新行）。

### 5.2 BaseRowDelta 实现分析

`BaseRowDelta` 位于 `core/src/main/java/org/apache/iceberg/BaseRowDelta.java`：

```java
// 第 31-42 行
public class BaseRowDelta extends MergingSnapshotProducer<RowDelta> implements RowDelta {
  private Long startingSnapshotId = null;
  private final CharSequenceSet referencedDataFiles = CharSequenceSet.empty();
  private final DataFileSet removedDataFiles = DataFileSet.create();
  private boolean validateDeletes = false;
  private Expression conflictDetectionFilter = Expressions.alwaysTrue();
  private boolean validateNewDataFiles = false;
  private boolean validateNewDeleteFiles = false;
```

**操作类型自动判定**（第 50-60 行）：

```java
@Override
protected String operation() {
    if (addsDataFiles() && !addsDeleteFiles() && !deletesDataFiles()) {
      return DataOperations.APPEND;      // 只添加数据 -> APPEND
    }
    if (addsDeleteFiles() && !addsDataFiles()) {
      return DataOperations.DELETE;      // 只添加删除 -> DELETE
    }
    return DataOperations.OVERWRITE;     // 同时添加数据和删除 -> OVERWRITE
}
```

这个自动判定逻辑确保 snapshot summary 中的 operation 类型准确反映了实际操作。例如：
- 纯 DELETE FROM 在 MOR 模式下只生成 delete file -> `DataOperations.DELETE`
- UPDATE 在 MOR 模式下同时生成 data file 和 delete file -> `DataOperations.OVERWRITE`

### 5.3 冲突检测与验证机制

`BaseRowDelta` 的 `validate()` 方法（第 132-174 行）包含了丰富的冲突检测逻辑：

```java
@Override
protected void validate(TableMetadata base, Snapshot parent) {
    if (parent != null) {
      // 1. 验证起始快照是祖先
      if (startingSnapshotId != null) {
        Preconditions.checkArgument(
            SnapshotUtil.isAncestorOf(parent.snapshotId(), startingSnapshotId, base::snapshot),
            "Snapshot %s is not an ancestor of %s", startingSnapshotId, parent.snapshotId());
      }

      // 2. 验证被引用的数据文件仍然存在
      if (!referencedDataFiles.isEmpty()) {
        validateDataFilesExist(base, startingSnapshotId, referencedDataFiles,
            !validateDeletes, conflictDetectionFilter, parent);
      }

      // 3. 验证删除文件的完整性
      if (validateDeletes) {
        failMissingDeletePaths();
      }

      // 4. 验证没有冲突的新数据文件
      if (validateNewDataFiles) {
        validateAddedDataFiles(base, startingSnapshotId, conflictDetectionFilter, parent);
      }

      // 5. 验证没有冲突的新删除文件
      if (validateNewDeleteFiles) {
        if (!removedDataFiles.isEmpty()) {
          validateNoNewDeletesForDataFiles(base, startingSnapshotId,
              conflictDetectionFilter, removedDataFiles, parent);
        }
        validateNoNewDeleteFiles(base, startingSnapshotId, conflictDetectionFilter, parent);
      }

      // 6. 验证没有冲突的文件和位置删除
      validateNoConflictingFileAndPositionDeletes();

      // 7. 验证 DV
      validateAddedDVs(base, startingSnapshotId, conflictDetectionFilter, parent);
    }
}
```

**validateNoConflictingFileAndPositionDeletes** 方法（第 181-193 行）确保不能在同一个 commit 中既删除一个数据文件又为其添加新的 delete file：

```java
private void validateNoConflictingFileAndPositionDeletes() {
    List<CharSequence> deletedFileWithNewDVs =
        removedDataFiles.stream()
            .map(DataFile::path)
            .filter(referencedDataFiles::contains)
            .collect(Collectors.toList());

    if (!deletedFileWithNewDVs.isEmpty()) {
      throw new ValidationException(
          "Cannot delete data files %s that are referenced by new delete files",
          deletedFileWithNewDVs);
    }
}
```

### 5.4 RowDelta 的操作类型判定

在 Spark 中，RowDelta 操作的使用场景取决于命令类型和隔离级别。在 `SparkPositionDeltaWrite` 的 commit 方法中（第 242-316 行），可以看到不同命令对冲突验证的配置：

```java
// SparkPositionDeltaWrite.java, PositionDeltaBatchWrite.commit() (第 285-293 行)
if (command == UPDATE || command == MERGE) {
    rowDelta.validateDeletedFiles();
    rowDelta.validateNoConflictingDeleteFiles();
}

if (isolationLevel == SERIALIZABLE) {
    rowDelta.validateNoConflictingDataFiles();
}
```

- **DELETE 命令**：不需要验证冲突的 delete files（删除已删除的行是安全的）
- **UPDATE/MERGE 命令**：需要验证没有冲突的 delete files（因为 update 涉及 read-then-write）
- **SERIALIZABLE 隔离**：额外验证没有冲突的数据文件

### 5.5 MergingSnapshotProducer 基类分析

`BaseRowDelta` 继承自 `MergingSnapshotProducer`，后者提供了 manifest 文件管理的基础设施。

```java
// core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java (第 68-99 行)
abstract class MergingSnapshotProducer<ThisT> extends SnapshotProducer<ThisT> {
  private final ManifestMergeManager<DataFile> mergeManager;
  private final ManifestFilterManager<DataFile> filterManager;
  private final ManifestMergeManager<DeleteFile> deleteMergeManager;
  private final ManifestFilterManager<DeleteFile> deleteFilterManager;
  private final AtomicInteger dvMergeAttempt = new AtomicInteger(0);

  private final Map<Integer, DataFileSet> newDataFilesBySpec = Maps.newHashMap();
  private Long newDataFilesDataSequenceNumber;
  private final List<DeleteFile> v2Deletes = Lists.newArrayList();
  private final Map<String, List<DeleteFile>> dvsByReferencedFile = Maps.newLinkedHashMap();
```

关键设计点：

1. **DV 按引用文件分组**：`dvsByReferencedFile` 将 DV 按关联的数据文件路径分组，用于检测和合并重复 DV
2. **V2 Deletes 与 DV 分离管理**：传统的 position delete（`v2Deletes`）和 DV（`dvsByReferencedFile`）分别管理
3. **DV 合并重试**：`dvMergeAttempt` 计数器用于在提交冲突时进行 DV 合并重试
4. **manifest 合并优化**：`ManifestMergeManager` 和 `ManifestFilterManager` 分别负责 manifest 的合并和过滤

DV 验证操作在 `MergingSnapshotProducer` 中被调用，属于 V3 规范引入的专用验证逻辑（第 83-84 行的 `VALIDATE_ADDED_DVS_OPERATIONS`）。

---

## 第六部分：Copy-On-Write（COW）路径深度解析

### 6.1 COW 的设计思想

COW 的核心思想是**写时复制**：当需要修改（删除或更新）某些行时，读取包含这些行的完整数据文件，过滤掉需要修改的行，将剩余行和新行写入新的数据文件，然后原子性地用新文件替换旧文件。

`RowLevelOperationMode` 类的文档精确描述了 COW 的特征：

```java
// core/src/main/java/org/apache/iceberg/RowLevelOperationMode.java (注释)
// Copy-on-write: changes are materialized immediately and matching data files are replaced
// with new data files that represent the new table state. For example, if there is a record
// that has to be deleted, the data file that contains this record has to be replaced with
// another data file without that record. All unchanged rows have to be copied over to the new
// data file.
```

COW 的优缺点：
- **写入慢**：需要重写整个数据文件，即使只修改一行
- **读取快**：没有 delete file 的开销，数据文件直接反映表的最新状态
- **适用场景**：读多写少、修改比例大、追求读取性能

### 6.2 SparkCopyOnWriteOperation 源码分析

`SparkCopyOnWriteOperation` 实现了 Spark 的 `RowLevelOperation` 接口：

```java
// spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkCopyOnWriteOperation.java
class SparkCopyOnWriteOperation implements RowLevelOperation {
    private final SparkSession spark;
    private final Table table;
    private final Snapshot snapshot;
    private final String branch;
    private final Command command;
    private final IsolationLevel isolationLevel;
```

**Scan 构建**（第 75-87 行）：

```java
@Override
public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    if (lazyScanBuilder == null) {
      lazyScanBuilder =
          new SparkScanBuilder(spark, table, table.schema(), snapshot, branch, options) {
            @Override
            public Scan build() {
              Scan scan = super.buildCopyOnWriteScan();  // 构建 COW 专用扫描
              SparkCopyOnWriteOperation.this.configuredScan = scan;
              return scan;
            }
          };
    }
    return lazyScanBuilder;
}
```

注意这里调用的是 `buildCopyOnWriteScan()` 而非普通的 `build()`。COW 扫描需要读取受影响数据文件的所有行（包括未修改的行），以便重写完整的文件。

**Write 构建**（第 92-99 行）：

```java
@Override
public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    if (lazyWriteBuilder == null) {
      SparkWriteBuilder writeBuilder = new SparkWriteBuilder(spark, table, branch, info);
      lazyWriteBuilder = writeBuilder.overwriteFiles(configuredScan, command, isolationLevel);
    }
    return lazyWriteBuilder;
}
```

通过 `writeBuilder.overwriteFiles()` 创建的 Writer 会使用 `OverwriteFiles` API 而非 `RowDelta` API 来提交变更。

**requiredMetadataAttributes**（第 102-116 行）：

```java
@Override
public NamedReference[] requiredMetadataAttributes() {
    List<NamedReference> metaAttrs = Lists.newArrayList();
    metaAttrs.add(SparkMetadataColumns.FILE_PATH.asRef());  // 始终需要 file_path
    if (command == DELETE || command == UPDATE) {
      metaAttrs.add(SparkMetadataColumns.ROW_POSITION.asRef());  // DELETE/UPDATE 需要 row_position
    }
    // Row Lineage 支持
    if (TableUtil.supportsRowLineage(table)) {
      metaAttrs.add(SparkMetadataColumns.ROW_ID.asRef());
      metaAttrs.add(SparkMetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.asRef());
    }
    return metaAttrs.toArray(NamedReference[]::new);
}
```

### 6.3 SparkCopyOnWriteScan 源码分析

`SparkCopyOnWriteScan` 位于 `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkCopyOnWriteScan.java`：

```java
class SparkCopyOnWriteScan extends SparkPartitioningAwareScan<FileScanTask>
    implements SupportsRuntimeFiltering {
    private Set<String> filteredLocations = null;
```

COW 扫描实现了 `SupportsRuntimeFiltering`，这是一个重要的优化。Spark 在执行 COW 操作时，会先找出包含需要修改行的数据文件，然后通过 `filter(Filter[])` 方法在运行时传递这些文件路径：

```java
// 第 96-128 行
@Override
public void filter(Filter[] filters) {
    for (Filter filter : filters) {
      if (filter instanceof In
          && ((In) filter).attribute().equalsIgnoreCase(MetadataColumns.FILE_PATH.name())) {
        In in = (In) filter;
        Set<String> fileLocations = Sets.newHashSet();
        for (Object value : in.values()) {
          fileLocations.add((String) value);
        }

        // 只保留匹配的文件任务
        if (filteredLocations == null || fileLocations.size() < filteredLocations.size()) {
          this.filteredLocations = fileLocations;
          List<FileScanTask> filteredTasks =
              tasks().stream()
                  .filter(file -> fileLocations.contains(file.file().location()))
                  .collect(Collectors.toList());
          resetTasks(filteredTasks);
        }
      }
    }
}
```

这个运行时过滤确保 COW 只重写确实包含受影响行的文件，而非整个表。

### 6.4 SparkWrite 中的 CopyOnWriteOperation

COW 的实际提交逻辑在 `SparkWrite.CopyOnWriteOperation` 内部类中（第 425-547 行）：

```java
// spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java (第 425 行)
private class CopyOnWriteOperation extends BaseBatchWrite {
    private final SparkCopyOnWriteScan scan;
    private final IsolationLevel isolationLevel;

    // 获取被覆盖的文件
    private DataFileSet overwrittenFiles() {
      if (scan == null) {
        return DataFileSet.create();
      } else {
        return scan.tasks().stream()
            .map(FileScanTask::file)
            .collect(Collectors.toCollection(DataFileSet::create));
      }
    }

    // 获取悬挂的 DV（需要一并清理）
    private DeleteFileSet danglingDVs() {
      if (scan == null) {
        return DeleteFileSet.create();
      } else {
        return scan.tasks().stream()
            .flatMap(task -> task.deletes().stream().filter(ContentFileUtil::isDV))
            .collect(Collectors.toCollection(DeleteFileSet::create));
      }
    }
```

**Commit 方法**（第 460-496 行）：

```java
@Override
public void commit(WriterCommitMessage[] messages, WriteSummary summary) {
    OverwriteFiles overwriteFiles = table.newOverwrite();

    DataFileSet overwrittenFiles = overwrittenFiles();
    int numOverwrittenFiles = overwrittenFiles.size();
    DeleteFileSet danglingDVs = danglingDVs();
    overwriteFiles.deleteFiles(overwrittenFiles, danglingDVs);  // 删除旧文件和悬挂 DV

    int numAddedFiles = 0;
    for (DataFile file : files(messages)) {
      numAddedFiles += 1;
      overwriteFiles.addFile(file);  // 添加新文件
    }

    if (scan != null) {
      switch (isolationLevel) {
        case SERIALIZABLE:
          commitWithSerializableIsolation(overwriteFiles, ...);
          break;
        case SNAPSHOT:
          commitWithSnapshotIsolation(overwriteFiles, ...);
          break;
      }
    }
}
```

COW 操作使用 `OverwriteFiles` API（而非 `RowDelta`），这是与 MOR 的根本区别。`OverwriteFiles` 的语义是原子性地删除旧文件并添加新文件。

**danglingDVs 清理**是一个重要的细节。当 COW 重写了一个带有 DV 的数据文件时，旧数据文件被删除后，其关联的 DV 也变成了"悬挂"的 delete file（不再关联任何有效数据文件），需要一并清理。

### 6.5 COW 路径下 DELETE/UPDATE/MERGE 的执行流程

**DELETE FROM table WHERE condition**（COW 模式）：

```
1. SparkRowLevelOperationBuilder.build()
   -> mode = COPY_ON_WRITE
   -> 创建 SparkCopyOnWriteOperation
2. SparkCopyOnWriteOperation.newScanBuilder()
   -> 构建 SparkCopyOnWriteScan
   -> 扫描所有匹配 condition 的数据文件
3. Spark 运行时过滤
   -> 确定包含待删除行的具体文件
   -> SparkCopyOnWriteScan.filter() 缩小范围
4. 读取受影响的数据文件
   -> 读取每个文件的所有行
   -> 过滤掉满足 condition 的行
   -> 将剩余行写入新数据文件
5. 提交
   -> OverwriteFiles.deleteFiles(旧文件) + addFile(新文件)
   -> 验证冲突
   -> 创建新 Snapshot
```

**UPDATE table SET col = val WHERE condition**（COW 模式）：

```
1-3. 与 DELETE 相同
4. 读取受影响的数据文件
   -> 读取每个文件的所有行
   -> 对满足 condition 的行应用更新
   -> 所有行（包括更新后的行和未修改的行）写入新数据文件
5. 提交（同上）
```

**MERGE INTO target USING source ON condition ...**（COW 模式）：

```
1-3. 与 DELETE 相同
4. 读取受影响的数据文件
   -> 将数据文件与 source 表进行 join
   -> 对匹配的行执行 UPDATE/DELETE/INSERT 操作
   -> 所有行写入新数据文件
5. 提交（同上）
```

### 6.6 COW 的写放大问题

COW 最大的问题是**写放大（Write Amplification）**。假设一个 128MB 的数据文件包含 100 万行，只需要删除其中 1 行，COW 仍需：
1. 读取全部 100 万行
2. 写出 999,999 行到新文件
3. 删除旧文件

这意味着修改 1 行数据需要 I/O 约 256MB（读 128MB + 写 128MB），写放大比接近 256 万倍。

在大规模更新场景中（如批量更新表中 10% 的行），如果更新分布在大量数据文件中，写放大会导致极长的操作时间和大量的 I/O 资源消耗。

---

## 第七部分：Merge-On-Read（MOR）路径深度解析

### 7.1 MOR 的设计思想

MOR 的核心思想是**延迟合并**：写入时只记录变更（delete file + 新增 data file），读取时再将 delete file 与 data file 合并，得到最终结果。

`RowLevelOperationMode` 的文档说明：

```java
// In merge-on-read, changes aren't materialized immediately. Instead, IDs of deleted and
// updated records are written into delete files that are applied during reads and
// updated/inserted records are written into new data files that are committed together
// with the delete files.
```

MOR 的优缺点：
- **写入快**：只需要写入变更记录（delete file 很小）
- **读取慢**：需要在读取时合并 delete file，有额外开销
- **适用场景**：写多读少、修改比例小、追求写入性能

### 7.2 SparkPositionDeltaOperation 源码分析

`SparkPositionDeltaOperation` 实现了 `SupportsDelta` 接口，这是 Spark 3.4+ 引入的 Delta API：

```java
// spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkPositionDeltaOperation.java
class SparkPositionDeltaOperation implements RowLevelOperation, SupportsDelta {
```

**rowId 定义**（第 117-120 行）：

```java
@Override
public NamedReference[] rowId() {
    return new NamedReference[] {
      SparkMetadataColumns.FILE_PATH.asRef(), SparkMetadataColumns.ROW_POSITION.asRef()
    };
}
```

MOR 使用 `(file_path, row_position)` 作为行标识符，这正是 Position Delete 的核心概念。

**Update 表示为 Delete + Insert**（第 124-127 行）：

```java
@Override
public boolean representUpdateAsDeleteAndInsert() {
    return true;
}
```

这意味着在 MOR 路径中，UPDATE 操作被拆解为先删除旧行（写入 delete file），再插入新行（写入 data file）。

**requiredMetadataAttributes**（第 103-114 行）：

```java
@Override
public NamedReference[] requiredMetadataAttributes() {
    List<NamedReference> metaAttrs = Lists.newArrayList();
    metaAttrs.add(SparkMetadataColumns.SPEC_ID.asRef());      // 需要分区 spec ID
    metaAttrs.add(SparkMetadataColumns.partition(table).asRef());  // 需要分区值
    if (TableUtil.supportsRowLineage(table)) {
      metaAttrs.add(SparkMetadataColumns.ROW_ID.asRef());
      metaAttrs.add(SparkMetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.asRef());
    }
    return metaAttrs.toArray(new NamedReference[0]);
}
```

MOR 需要额外的元数据：`spec_id` 和 `partition`，因为 delete file 需要按正确的分区归属进行管理。

### 7.3 SparkPositionDeltaWrite 源码分析

`SparkPositionDeltaWrite` 是 MOR 路径的核心 Write 实现，位于 `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkPositionDeltaWrite.java`：

```java
// 第 106-107 行
class SparkPositionDeltaWrite extends BaseSparkWrite
    implements DeltaWrite, RequiresDistributionAndOrdering {
```

它实现了 `DeltaWrite` 接口，这意味着它接收三种类型的操作：`delete`、`update`（表示为 delete + insert）、`insert`。

**WriterFactory 创建**（第 440-495 行）根据命令类型选择不同的 DeltaWriter：

```java
@Override
public DeltaWriter<InternalRow> createWriter(int partitionId, long taskId) {
    Table table = tableBroadcast.value();
    // ... 创建 file factories 和 writer factory ...

    if (command == DELETE) {
      // 纯删除：只需要 delete writer
      return new DeleteOnlyDeltaWriter(
          table, rewritableDeletes(), writerFactory, deleteFileFactory, context);
    } else if (table.spec().isUnpartitioned()) {
      // 非分区表：使用 UnpartitionedDeltaWriter
      return new UnpartitionedDeltaWriter(
          table, rewritableDeletes(), writerFactory, dataFileFactory, deleteFileFactory, ...);
    } else {
      // 分区表：使用 PartitionedDeltaWriter
      return new PartitionedDeltaWriter(
          table, rewritableDeletes(), writerFactory, dataFileFactory, deleteFileFactory, ...);
    }
}
```

### 7.4 MOR 中 Writer 的选择策略

`BaseDeltaWriter` 的 `newDeleteWriter` 方法（第 536-558 行）是 writer 选择的核心：

```java
protected PartitioningWriter<PositionDelete<InternalRow>, DeleteWriteResult> newDeleteWriter(
    Table table,
    Map<String, DeleteFileSet> rewritableDeletes,
    SparkFileWriterFactory writers,
    OutputFileFactory files,
    Context context) {

  Function<CharSequence, PositionDeleteIndex> previousDeleteLoader =
      PreviousDeleteLoader.create(table, rewritableDeletes);
  FileIO io = table.io();
  boolean inputOrdered = context.inputOrdered();
  long targetFileSize = context.targetDeleteFileSize();
  DeleteGranularity deleteGranularity = context.deleteGranularity();

  if (context.useDVs()) {
    // V3 表：使用 DV Writer
    return new PartitioningDVWriter<>(files, previousDeleteLoader);
  } else if (inputOrdered && rewritableDeletes == null) {
    // 输入有序且无需重写旧 deletes：使用聚集 writer
    return new ClusteredPositionDeleteWriter<>(
        writers, files, io, targetFileSize, deleteGranularity);
  } else {
    // 输入无序或需要重写旧 deletes：使用扇出 writer
    return new FanoutPositionOnlyDeleteWriter<>(
        writers, files, io, targetFileSize, deleteGranularity, previousDeleteLoader);
  }
}
```

三种 delete writer 的选择逻辑：

| 条件 | Writer 类型 | 说明 |
|------|-------------|------|
| `useDVs() == true` | `PartitioningDVWriter` | V3 表，使用 Puffin 文件格式的 DV |
| `inputOrdered && !rewritableDeletes` | `ClusteredPositionDeleteWriter` | 输入按 (file, pos) 排序，高效 |
| 其他 | `FanoutPositionOnlyDeleteWriter` | 处理无序输入，支持旧 delete 合并 |

**useDVs 的判定**（第 949 行）：

```java
boolean useDVs() {
    return deleteFileFormat == FileFormat.PUFFIN;
}
```

当 delete 文件格式配置为 PUFFIN 时，使用 DV。这通常由 V3 表自动设置。

### 7.5 MOR 路径下 DELETE/UPDATE/MERGE 的执行流程

**DELETE FROM table WHERE condition**（MOR 模式）：

```
1. SparkRowLevelOperationBuilder.build()
   -> mode = MERGE_ON_READ
   -> 创建 SparkPositionDeltaOperation
2. SparkPositionDeltaOperation.newScanBuilder()
   -> 构建普通的 SparkBatchQueryScan
   -> 扫描满足 condition 的行
3. Spark 执行扫描
   -> 对于每个匹配的行，提取 (file_path, row_position, spec_id, partition)
4. 写入 Delete File
   -> DeleteOnlyDeltaWriter 接收 delete(metadata, id) 调用
   -> 将 (file_path, pos) 写入 Position Delete 文件或 DV
5. 提交
   -> RowDelta.addDeletes(deleteFile)
   -> 验证冲突
   -> 创建新 Snapshot
```

**UPDATE table SET col = val WHERE condition**（MOR 模式）：

```
1-2. 与 DELETE 相同
3. Spark 执行扫描
   -> 对于每个匹配的行：
      a. 提取 (file_path, row_position) -> 用于 delete
      b. 应用 SET col = val -> 生成更新后的行 -> 用于 insert
4. 写入
   -> delete(metadata, id): 写入 delete file
   -> reinsert(metadata, row): 写入新 data file
5. 提交
   -> RowDelta.addDeletes(deleteFiles) + RowDelta.addRows(dataFiles)
   -> 验证冲突
   -> 创建新 Snapshot
```

**MERGE INTO target USING source ON condition ...**（MOR 模式）：

```
1-2. 与 DELETE 相同
3. Spark 执行 source 和 target 的 join
   -> WHEN MATCHED THEN DELETE: delete(metadata, id)
   -> WHEN MATCHED THEN UPDATE: delete(metadata, id) + reinsert(metadata, row)
   -> WHEN NOT MATCHED THEN INSERT: insert(row)
4. 写入
   -> 同时生成 delete files 和 data files
5. 提交
   -> RowDelta.addDeletes() + RowDelta.addRows()
```

### 7.6 DV 在 MOR 中的集成

`PartitioningDVWriter` 是 DV 在 MOR 路径中的入口点：

```java
// core/src/main/java/org/apache/iceberg/io/PartitioningDVWriter.java (第 35-65 行)
public class PartitioningDVWriter<T>
    implements PartitioningWriter<PositionDelete<T>, DeleteWriteResult> {

  private final DVFileWriter writer;
  private DeleteWriteResult result;

  public PartitioningDVWriter(
      OutputFileFactory fileFactory,
      Function<CharSequence, PositionDeleteIndex> loadPreviousDeletes) {
    this.writer = new BaseDVFileWriter(fileFactory, loadPreviousDeletes::apply);
  }

  @Override
  public void write(PositionDelete<T> row, PartitionSpec spec, StructLike partition) {
    writer.delete(row.path().toString(), row.pos(), spec, partition);
  }
```

每次写入 position delete 时，`PartitioningDVWriter` 将其转化为 DV 写入。底层的 `BaseDVFileWriter` 按数据文件路径分组，在关闭时一次性输出所有 DV 到 Puffin 文件。

### 7.7 Previous Delete Rewrite 机制

当使用 DV 或 file-scoped position deletes 时，新的删除需要与旧的删除合并。这通过 `PreviousDeleteLoader` 实现：

```java
// SparkPositionDeltaWrite.java (第 561-592 行)
private static class PreviousDeleteLoader implements Function<CharSequence, PositionDeleteIndex> {
    private final Map<String, DeleteFileSet> deleteFiles;
    private final DeleteLoader deleteLoader;

    @Override
    public PositionDeleteIndex apply(CharSequence path) {
      DeleteFileSet deleteFileSet = deleteFiles.get(path.toString());
      if (deleteFileSet == null) {
        return null;
      }
      return deleteLoader.loadPositionDeletes(deleteFileSet, path);
    }
}
```

在 `PositionDeltaBatchWrite` 中，通过 `broadcastRewritableDeletes()` 方法广播可重写的 delete 文件：

```java
// 第 215-228 行
private Broadcast<Map<String, DeleteFileSet>> broadcastRewritableDeletes() {
    if (scan != null && shouldRewriteDeletes()) {
      Map<String, DeleteFileSet> rewritableDeletes = scan.rewritableDeletes(context.useDVs());
      if (rewritableDeletes != null && !rewritableDeletes.isEmpty()) {
        return sparkContext.broadcast(rewritableDeletes);
      }
    }
    return null;
}

private boolean shouldRewriteDeletes() {
    return context.useDVs() || context.deleteGranularity() == DeleteGranularity.FILE;
}
```

只有在使用 DV 或 file-scoped granularity 时才需要重写旧 deletes，因为只有这两种情况下，新旧 delete 可以安全地合并（partition-scoped deletes 可能跨多个数据文件，不能被单独替换）。

---

## 第八部分：读取时 Delete 合并机制

### 8.1 DeleteFileIndex 源码分析

`DeleteFileIndex` 是 delete file 管理的核心索引，位于 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java`：

```java
// 第 70-97 行
class DeleteFileIndex {
  private static final DeleteFile[] EMPTY_DELETES = new DeleteFile[0];

  private final EqualityDeletes globalDeletes;           // 全局 equality deletes
  private final PartitionMap<EqualityDeletes> eqDeletesByPartition;  // 按分区的 equality deletes
  private final PartitionMap<PositionDeletes> posDeletesByPartition; // 按分区的 position deletes
  private final Map<String, PositionDeletes> posDeletesByPath;       // 按文件路径的 position deletes
  private final Map<String, DeleteFile> dvByPath;        // 按文件路径的 DV
  private final boolean hasEqDeletes;
  private final boolean hasPosDeletes;
  private final boolean isEmpty;
```

索引将 delete file 分为五个维度：
1. **globalDeletes**：未分区表的 equality deletes
2. **eqDeletesByPartition**：按分区分组的 equality deletes
3. **posDeletesByPartition**：partition-scoped 的 position deletes
4. **posDeletesByPath**：file-scoped 的 position deletes
5. **dvByPath**：按数据文件路径索引的 DV

**查找某个数据文件关联的 delete files**（第 151-168 行）：

```java
DeleteFile[] forDataFile(long sequenceNumber, DataFile file) {
    if (isEmpty) {
      return EMPTY_DELETES;
    }

    DeleteFile[] global = findGlobalDeletes(sequenceNumber, file);
    DeleteFile[] eqPartition = findEqPartitionDeletes(sequenceNumber, file);
    DeleteFile dv = findDV(sequenceNumber, file);

    if (dv != null && global == null && eqPartition == null) {
      return new DeleteFile[] {dv};  // 只有 DV，直接返回
    } else if (dv != null) {
      return concat(global, eqPartition, new DeleteFile[] {dv});  // DV + eq deletes
    } else {
      // 没有 DV 时才查找 position deletes
      DeleteFile[] posPartition = findPosPartitionDeletes(sequenceNumber, file);
      DeleteFile[] posPath = findPathDeletes(sequenceNumber, file);
      return concat(global, eqPartition, posPartition, posPath);
    }
}
```

关键设计点：**DV 和传统 Position Delete 是互斥的**。如果一个数据文件有 DV，则不再查找传统 position deletes。这是因为 DV 在写入时已经合并了之前的 position deletes。

**Equality Delete 的文件级过滤**通过 `canContainEqDeletesForFile()` 方法实现（第 219-281 行）：

```java
private static boolean canContainEqDeletesForFile(
    DataFile dataFile, EqualityDeleteFile deleteFile) {
    // 利用数据文件和 delete 文件的统计信息进行过滤
    // 如果两者的值范围没有交集，可以跳过该 delete 文件
    for (Types.NestedField field : deleteFile.equalityFields()) {
      // 检查 null 计数
      if (allNull(dataNullCounts, dataValueCounts, field) && allNonNull(deleteNullCounts, field)) {
        return false;
      }
      // 检查值范围是否有交集
      if (!rangesOverlap(field, dataLower, dataUpper, deleteLower, deleteUpper)) {
        return false;
      }
    }
    return true;
}
```

这是一个基于列统计信息的优化：如果数据文件的某个 equality 字段的值范围与 delete 文件完全不重叠，则可以确定该 delete 文件不会删除该数据文件中的任何行。

**Sequence Number 过滤**通过 `findStartIndex()` 方法实现（第 647-663 行）：

```java
private static int findStartIndex(long[] seqs, long seq) {
    int pos = Arrays.binarySearch(seqs, seq);
    int start;
    if (pos < 0) {
      start = -(pos + 1);
    } else {
      start = pos;
      while (start > 0 && seqs[start - 1] >= seq) {
        start -= 1;
      }
    }
    return start;
}
```

Position Delete 和 Equality Delete 都使用 sequence number 来确定作用范围。只有 sequence number >= 数据文件 sequence number 的 delete file 才需要应用（更早的 delete file 已经被 compact 到数据文件中了）。

### 8.2 DeleteFilter 源码分析

`DeleteFilter` 是读取时 delete 合并的入口，位于 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java`：

```java
// 第 51-66 行
public abstract class DeleteFilter<T> {
  private final String filePath;            // 当前正在读取的数据文件路径
  private final List<DeleteFile> posDeletes;  // 关联的 position delete files
  private final List<DeleteFile> eqDeletes;   // 关联的 equality delete files
  private final Schema requiredSchema;
  private final Schema expectedSchema;
  private final Accessor<StructLike> posAccessor;
  private final boolean hasIsDeletedColumn;
  private final int isDeletedColumnPosition;
  private final DeleteCounter counter;

  private volatile DeleteLoader deleteLoader = null;
  private PositionDeleteIndex deleteRowPositions = null;
  private List<Predicate<T>> isInDeleteSets = null;
  private Predicate<T> eqDeleteRows = null;
```

**核心过滤流程**（第 188-189 行）：

```java
public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
}
```

过滤按顺序执行：先应用 Position Delete，再应用 Equality Delete。

**Position Delete 应用**（第 261-269 行）：

```java
private CloseableIterable<T> applyPosDeletes(CloseableIterable<T> records) {
    if (posDeletes.isEmpty()) {
      return records;
    }
    PositionDeleteIndex positionIndex = deletedRowPositions();
    Predicate<T> isDeleted = record -> positionIndex.isDeleted(pos(record));
    return createDeleteIterable(records, isDeleted);
}
```

Position Delete 的应用非常高效：
1. 将所有 position delete files 加载为一个 `PositionDeleteIndex`（Bitmap）
2. 对每条记录，通过 `positionIndex.isDeleted(pos)` 检查是否被删除（O(1) 操作）

**Equality Delete 应用**（第 234-238 行）：

```java
private CloseableIterable<T> applyEqDeletes(CloseableIterable<T> records) {
    Predicate<T> isEqDeleted = applyEqDeletes().stream()
        .reduce(Predicate::or).orElse(t -> false);
    return createDeleteIterable(records, isEqDeleted);
}
```

**删除标记 vs 过滤**（第 271-276 行）：

```java
private CloseableIterable<T> createDeleteIterable(
    CloseableIterable<T> records, Predicate<T> isDeleted) {
    return hasIsDeletedColumn
        ? Deletes.markDeleted(records, isDeleted, this::markRowDeleted)
        : Deletes.filterDeleted(records, isDeleted, counter);
}
```

当查询包含 `_deleted` 元数据列时，不过滤已删除的行，而是标记它们。这在 MERGE INTO 操作中很有用，引擎可能需要知道哪些行已被删除来做进一步处理。

**文件投影优化**（第 278-329 行）：

```java
private static Schema fileProjection(
    Function<Integer, Types.NestedField> fieldLookup,
    Schema requestedSchema,
    List<DeleteFile> posDeletes,
    List<DeleteFile> eqDeletes,
    boolean needRowPosCol) {

    Set<Integer> requiredIds = Sets.newLinkedHashSet();

    // Position Delete 需要 _pos 元数据列
    if (needRowPosCol && !posDeletes.isEmpty()) {
      requiredIds.add(MetadataColumns.ROW_POSITION.fieldId());
    }

    // Equality Delete 需要等值字段
    for (DeleteFile eqDelete : eqDeletes) {
      requiredIds.addAll(eqDelete.equalityFieldIds());
    }

    // 将缺少的字段添加到投影中
    Set<Integer> missingIds =
        Sets.difference(requiredIds, TypeUtil.getProjectedIds(requestedSchema));
    // ... 添加缺少的列 ...
}
```

这段代码确保读取器的投影（projection）包含 delete 过滤所需的所有字段。例如，即使用户查询不包含 `_pos` 列，如果有 position deletes，也需要读取行位置；如果有 equality deletes，需要读取等值匹配字段。

### 8.3 PositionDeleteIndex 接口与实现

`PositionDeleteIndex` 是位置删除索引的核心接口（`core/src/main/java/org/apache/iceberg/deletes/PositionDeleteIndex.java`）：

```java
// 第 27 行
public interface PositionDeleteIndex {
  void delete(long position);                    // 标记位置为已删除
  void delete(long posStart, long posEnd);       // 标记范围为已删除
  void merge(PositionDeleteIndex that);          // 合并另一个索引
  boolean isDeleted(long position);              // 检查位置是否已删除
  boolean isEmpty();                             // 是否为空
  void forEach(LongConsumer consumer);           // 遍历所有已删除位置
  Collection<DeleteFile> deleteFiles();          // 关联的 delete files
  long cardinality();                            // 已删除行数
  ByteBuffer serialize();                        // 序列化
}
```

**反序列化入口**：

```java
// 第 112-114 行
static PositionDeleteIndex deserialize(byte[] bytes, DeleteFile deleteFile) {
    return BitmapPositionDeleteIndex.deserialize(bytes, deleteFile);
}
```

**BitmapPositionDeleteIndex** 是唯一的实现（除了 `EmptyPositionDeleteIndex`），基于 `RoaringPositionBitmap`。

**合并操作**（第 77-84 行）：

```java
@Override
public void merge(PositionDeleteIndex that) {
    if (that instanceof BitmapPositionDeleteIndex) {
      merge((BitmapPositionDeleteIndex) that);  // 优化：直接合并 bitmap
    } else {
      that.forEach(this::delete);  // 回退：逐个位置添加
      deleteFiles.addAll(that.deleteFiles());
    }
}
```

**PositionDeleteIndexUtil** 提供了多索引合并工具：

```java
// core/src/main/java/org/apache/iceberg/deletes/PositionDeleteIndexUtil.java
public static PositionDeleteIndex merge(Iterable<? extends PositionDeleteIndex> indexes) {
    BitmapPositionDeleteIndex result = new BitmapPositionDeleteIndex();
    indexes.forEach(result::merge);
    return result;
}
```

### 8.4 BaseDeleteLoader 源码分析

`BaseDeleteLoader` 位于 `data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java`，负责实际的 delete file 加载：

**DV 加载**（第 162-168 行）：

```java
@Override
public PositionDeleteIndex loadPositionDeletes(
    Iterable<DeleteFile> deleteFiles, CharSequence filePath) {
    if (ContentFileUtil.containsSingleDV(deleteFiles)) {
      DeleteFile dv = Iterables.getOnlyElement(deleteFiles);
      validateDV(dv, filePath);
      return readDV(dv);  // 直接读取 DV
    } else {
      return getOrReadPosDeletes(deleteFiles, filePath);  // 读取传统 position deletes
    }
}
```

加载流程根据 delete file 类型自动选择路径：
- 如果只有一个 DV：直接从 Puffin 文件中读取并反序列化
- 如果是传统 position delete files：解析 Parquet/Avro 文件，提取对应数据文件的位置

**DV 读取**（第 171-183 行）：

```java
private PositionDeleteIndex readDV(DeleteFile dv) {
    LOG.trace("Opening DV file {}", dv.location());
    InputFile inputFile = loadInputFile.apply(dv);
    long offset = dv.contentOffset();
    int length = dv.contentSizeInBytes().intValue();
    byte[] bytes = new byte[length];
    try {
      IOUtil.readFully(inputFile, offset, bytes, 0, length);
      return PositionDeleteIndex.deserialize(bytes, dv);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
}
```

DV 的读取只需要一次随机 I/O（读取 Puffin 文件中特定 offset 的字节），然后反序列化为 Bitmap。这比读取整个 Parquet 格式的 position delete 文件高效得多。

**Position Delete 加载（带缓存）**（第 185-213 行）：

```java
private PositionDeleteIndex getOrReadPosDeletes(
    Iterable<DeleteFile> deleteFiles, CharSequence filePath) {
    Iterable<PositionDeleteIndex> deletes =
        execute(deleteFiles, deleteFile -> getOrReadPosDeletes(deleteFile, filePath));
    return PositionDeleteIndexUtil.merge(deletes);
}

private PositionDeleteIndex getOrReadPosDeletes(DeleteFile deleteFile, CharSequence filePath) {
    long estimatedSize = estimatePosDeletesSize(deleteFile);
    if (canCache(estimatedSize)) {
      String cacheKey = deleteFile.location();
      CharSequenceMap<PositionDeleteIndex> indexes =
          getOrLoad(cacheKey, () -> readPosDeletes(deleteFile), estimatedSize);
      return indexes.getOrDefault(filePath, PositionDeleteIndex.empty());
    } else {
      return readPosDeletes(deleteFile, filePath);
    }
}
```

对于传统 position delete 文件，加载器支持缓存机制。由于 partition-scoped 的 position delete 文件包含多个数据文件的删除信息，缓存可以避免重复读取。

**并行执行**：

```java
private <I, O> Iterable<O> execute(Iterable<I> objects, Function<I, O> func) {
    Queue<O> output = new ConcurrentLinkedQueue<>();
    Tasks.foreach(objects)
        .executeWith(workerPool)
        .stopOnFailure()
        .run(object -> output.add(func.apply(object)));
    return output;
}
```

所有 delete file 的加载通过线程池并行执行，以最大化 I/O 吞吐。

### 8.5 Deletes 工具类分析

`Deletes` 工具类提供了 delete 操作的基础设施（`core/src/main/java/org/apache/iceberg/deletes/Deletes.java`）：

**Equality Delete 过滤**：

```java
// 第 55-63 行
public static <T> CloseableIterable<T> filter(
    CloseableIterable<T> rows, Function<T, StructLike> rowToDeleteKey, StructLikeSet deleteSet) {
    if (deleteSet.isEmpty()) {
      return rows;
    }
    EqualitySetDeleteFilter<T> equalityFilter =
        new EqualitySetDeleteFilter<>(rowToDeleteKey, deleteSet);
    return equalityFilter.filter(rows);
}
```

**Position Delete 到 Bitmap 的转换**：

```java
// 第 139-156 行
public static <T extends StructLike> CharSequenceMap<PositionDeleteIndex> toPositionIndexes(
    CloseableIterable<T> posDeletes, DeleteFile file) {
    CharSequenceMap<PositionDeleteIndex> indexes = CharSequenceMap.create();
    try (CloseableIterable<T> deletes = posDeletes) {
      for (T delete : deletes) {
        CharSequence filePath = (CharSequence) FILENAME_ACCESSOR.get(delete);
        long position = (long) POSITION_ACCESSOR.get(delete);
        PositionDeleteIndex index =
            indexes.computeIfAbsent(filePath, key -> new BitmapPositionDeleteIndex(file));
        index.delete(position);
      }
    }
    return indexes;
}
```

这个方法将 position delete 记录转换为按文件路径分组的 Bitmap 索引。每个数据文件对应一个 `BitmapPositionDeleteIndex`。

**标记删除 vs 过滤删除**：

```java
// 标记删除：保留所有行，但标记已删除的行
public static <T> CloseableIterable<T> markDeleted(
    CloseableIterable<T> rows, Predicate<T> isDeleted, Consumer<T> deleteMarker) {
    return CloseableIterable.transform(rows, row -> {
      if (isDeleted.test(row)) {
        deleteMarker.accept(row);
      }
      return row;
    });
}

// 过滤删除：移除已删除的行
public static <T> CloseableIterable<T> filterDeleted(
    CloseableIterable<T> rows, Predicate<T> isDeleted, DeleteCounter counter) {
    Filter<T> remainingRowsFilter = new Filter<T>() {
      @Override
      protected boolean shouldKeep(T item) {
        boolean deleted = isDeleted.test(item);
        if (deleted) { counter.increment(); }
        return !deleted;
      }
    };
    return remainingRowsFilter.filter(rows);
}
```

### 8.6 读取时 Delete 合并的完整流程

综合以上分析，读取时 Delete 合并的完整流程如下：

```
Step 1: 规划阶段 (DeleteFileIndex)
  ├── 从 delete manifest 加载所有 delete files
  ├── 按类型分组：
  │   ├── DV -> dvByPath (Map<String, DeleteFile>)
  │   ├── File-scoped Position Delete -> posDeletesByPath
  │   ├── Partition-scoped Position Delete -> posDeletesByPartition
  │   ├── Partitioned Equality Delete -> eqDeletesByPartition
  │   └── Unpartitioned Equality Delete -> globalDeletes
  └── 对每个数据文件调用 forDataFile()：
      ├── 按 sequence number 过滤 delete files
      ├── DV 优先于传统 position deletes
      └── 返回该数据文件需要应用的 delete files

Step 2: 读取阶段 (DeleteFilter)
  ├── 接收 data file + 关联的 delete files
  ├── 分离为 posDeletes 和 eqDeletes
  ├── 调整文件投影以包含 delete 所需列
  └── filter() 方法链式应用：
      ├── applyPosDeletes()：
      │   ├── BaseDeleteLoader.loadPositionDeletes()
      │   │   ├── 如果是 DV: 读取 Puffin blob → 反序列化 Bitmap
      │   │   └── 如果是 Position Delete: 读取 Parquet → 构建 Bitmap
      │   ├── 创建 PositionDeleteIndex
      │   └── 对每行检查 positionIndex.isDeleted(pos)
      └── applyEqDeletes()：
          ├── BaseDeleteLoader.loadEqualityDeletes()
          │   └── 读取 equality delete 文件 → 构建 StructLikeSet
          └── 对每行检查 deleteSet.contains(projectRow)
```

---

## 第九部分：性能对比与权衡

### 9.1 COW vs MOR 写入性能

| 维度 | COW | MOR |
|------|-----|-----|
| **I/O 量** | 大（读取+重写整个数据文件） | 小（只写 delete file） |
| **写放大** | 高（修改 1 行也要重写整个文件） | 低（只记录变更） |
| **延迟** | 高（等待文件重写完成） | 低（快速写入 delete file） |
| **适合场景** | 大比例更新/删除 | 小比例更新/删除 |

具体分析：

**COW 写入成本**：假设数据文件大小为 `S`，修改的行数为 `M`，文件中总行数为 `N`。
- I/O = S（读取）+ S * (N-M)/N（写入新文件） ≈ 2S（当 M << N 时）
- 如果修改分布在 `K` 个文件中，总 I/O ≈ 2KS

**MOR 写入成本**：
- Position Delete: I/O ≈ 16 * M 字节（每条记录约 16 字节：file_path 引用 + pos）
- DV: I/O ≈ M/8 字节（Roaring Bitmap 约 1 byte/position）

### 9.2 COW vs MOR 读取性能

| 维度 | COW | MOR |
|------|-----|-----|
| **读取额外 I/O** | 无 | 需要读取 delete files |
| **CPU 开销** | 无 | Position Delete: Bitmap 查找 O(1) |
|  | | Equality Delete: Set 查找 O(1) 但内存开销大 |
| **延迟** | 低（直接读取数据文件） | 高（需要先加载 delete index） |
| **适合场景** | 频繁读取的表 | 写入频繁、读取不频繁的表 |

MOR 的读取开销取决于 delete 类型：
- **DV**：最高效，一次 I/O 读取 Puffin blob，反序列化为 Bitmap
- **File-scoped Position Delete**：较高效，只读取与当前数据文件相关的删除
- **Partition-scoped Position Delete**：较低效，可能读取大量不相关的删除
- **Equality Delete**：最低效，需要全量加载到内存进行匹配

### 9.3 Position Delete vs DV 性能

| 维度 | Position Delete (Parquet) | DV (Puffin) |
|------|--------------------------|-------------|
| **写入格式** | Parquet/Avro 行式存储 | Roaring Bitmap 序列化 |
| **文件大小** | 较大（Parquet 开销） | 很小（Bitmap 压缩高效） |
| **读取解析** | 需要 Parquet reader | 直接字节反序列化 |
| **一对一绑定** | 可选（file/partition 粒度） | 强制（每 DV 绑定一个数据文件） |
| **文件数量** | 可能累积很多 | 每个数据文件最多一个 |
| **合并效率** | 需要重新排序写入 | Bitmap OR 操作 |

DV 的内存效率估算：

```java
// BaseDeleteLoader.java (第 245-249 行)
private long estimatePosDeletesSize(DeleteFile deleteFile) {
    // Roaring bitmaps require around 8 bits (1 byte) per value on average
    return deleteFile.recordCount();
}
```

即对于 10 万个被删除位置，估计内存占用约 100KB。实际上由于 Roaring Bitmap 的压缩特性（特别是对连续位置的 run-length encoding），实际内存可能更小。

### 9.4 Equality Delete 的特殊定位

Equality Delete 在性能上是最不利的，但它有独特的优势：

1. **不需要行位置**：流式引擎（如 Flink）在 CDC 场景中可能无法获取行的精确位置
2. **支持任意字段组合**：可以使用任何字段进行等值匹配，不限于 primary key
3. **跨文件删除**：一条 equality delete 记录可以删除所有文件中匹配的行

在实际应用中，Equality Delete 通常用于 Flink CDC 场景的初始摄入，后续通过 compaction 将其转换为 position deletes 或合并到数据文件中。

### 9.5 实际场景的选择建议

| 场景 | 推荐模式 | 推荐 Delete 类型 | 原因 |
|------|---------|-----------------|------|
| OLAP 查询密集，偶尔 DML | COW | N/A | 零读取开销 |
| 高频小批量更新 | MOR + DV | DV | 写入快，DV 开销小 |
| Flink CDC 摄入 | MOR | Equality Delete | 流式场景无法获取行位置 |
| 大批量 DELETE | COW | N/A | 删除比例大，COW 不会比 MOR 差太多 |
| MERGE INTO（小比例匹配） | MOR + DV | DV | 写入快，少量 DV 开销可接受 |
| MERGE INTO（大比例匹配） | COW | N/A | 大量行被修改时 COW 更直接 |

---

## 第十部分：演进路线与设计动机

### 10.1 从 V1 到 V2：引入行级删除

Iceberg V1 是一个纯 append-only 的表格式，不支持任何行级修改。要修改数据只能通过 `OverwriteFiles` API 完全替换数据文件。

V2 引入了 Delete File 机制，包括：
- **Equality Delete**：通过字段值匹配标记删除
- **Position Delete**：通过 (file_path, pos) 精确标记删除
- **RowDelta API**：原子性地添加数据文件和删除文件
- **Sequence Number**：用于确定 delete file 的作用范围

这使得 Iceberg 能够支持真正的 MOR 模式，极大降低了行级操作的写入成本。

### 10.2 从 V2 到 V3：引入 Deletion Vector

V3 引入了 Deletion Vector，主要动机是：

1. **解决 Position Delete 的 Many-to-Many 问题**：V2 中，一个 position delete 文件可以包含多个数据文件的删除信息，一个数据文件也可能关联多个 position delete 文件。这导致了复杂的读取规划和潜在的读取放大。

2. **强制一对一绑定**：DV 与数据文件严格一对一对应，简化了管理：
   - 规划时直接通过路径查找，无需复杂的匹配逻辑
   - 不会出现无关 delete 被分配给数据文件的情况
   - 每个数据文件最多一个 DV，避免了 delete file 堆积

3. **更紧凑的存储**：Roaring Bitmap 序列化格式比 Parquet 格式的 position delete 更紧凑，特别是对于大量连续位置的情况（run-length encoding）。

4. **更快的读取**：DV 的反序列化只需要读取字节流并解析 Bitmap，无需完整的 Parquet/Avro reader。

5. **与 Delta Lake 的互操作性**：DV 的序列化格式（magic number + Roaring Bitmap + CRC-32）与 Delta Lake 兼容，便于跨格式迁移。

V3 还引入了 `TrackedFile` 接口（包含 `deletion_vector` 字段），以及在 manifest entry 中直接嵌入 DV 元数据的能力。

### 10.3 从 Equality Delete 到 Position Delete 的演进

Equality Delete 和 Position Delete 并非替代关系，而是互补的：

- **Equality Delete**：适合无法确定行位置的场景（如流式 CDC）
- **Position Delete**：适合可以确定行位置的场景（如批处理引擎扫描后的修改）

在实际系统中，Equality Delete 通常是临时的：
1. Flink CDC 写入 Equality Delete
2. Compaction 作业将 Equality Delete 转换为 Position Delete（或直接合并到数据文件中）
3. 查询引擎只需要处理 Position Delete

这种分层设计使得不同引擎可以使用最适合自己的 delete 类型，而 compaction 在后台统一管理。

### 10.4 从 Position Delete 到 DV 的演进

DV 是 Position Delete 的精神继承者，保留了按位置删除的核心思想，但在以下方面做了根本性改进：

| 方面 | Position Delete (V2) | DV (V3) |
|------|---------------------|---------|
| 存储格式 | Parquet/Avro | Puffin (Roaring Bitmap) |
| 文件关系 | 多对多 | 一对一 |
| 内容 | file_path + pos 记录 | 纯位图 |
| 读取方式 | 解析 Parquet | 字节流反序列化 |
| 合并方式 | 排序合并写入新文件 | Bitmap OR 操作 |
| 规划复杂度 | O(N * M) | O(1) 路径查找 |

DV 在源码中的关键判定点是 `FileFormat.PUFFIN`：

```java
// ContentFileUtil.isDV()
public static boolean isDV(DeleteFile deleteFile) {
    return deleteFile.format() == FileFormat.PUFFIN;
}

// SparkPositionDeltaWrite.Context.useDVs()
boolean useDVs() {
    return deleteFileFormat == FileFormat.PUFFIN;
}
```

当 delete 文件格式配置为 PUFFIN 时（V3 表的默认行为），整个 delete 路径切换到 DV 模式。

### 10.5 未来展望

基于源码分析，可以观察到几个未来演进的方向：

1. **Row Lineage**：V3 引入了 `ROW_ID` 和 `LAST_UPDATED_SEQUENCE_NUMBER` 元数据列，用于跟踪行级别的版本历史。这为更精确的冲突检测和增量处理打下了基础。

2. **TrackedFile 统一模型**：`TrackedFile` 接口统一了数据文件、Delete 文件和 Manifest 文件的元数据模型，为未来的格式演进提供了扩展性。

3. **DV 合并优化**：`MergingSnapshotProducer` 中的 `dvMergeAttempt` 计数器暗示了在并发场景下 DV 合并重试的优化空间。

4. **Equality Delete 淘汰趋势**：从代码结构看，新版本越来越倾向于 Position Delete/DV，Equality Delete 主要保留为流式引擎的兼容选项。

---

## 第十一部分：实际场景分析

### 11.1 DELETE FROM 场景

假设执行 `DELETE FROM iceberg_table WHERE category = 'obsolete'`，Iceberg 的处理流程取决于配置的模式：

**COW 模式（默认）**：

```
1. SparkRowLevelOperationBuilder 检查 write.delete.mode
   -> "copy-on-write" -> 创建 SparkCopyOnWriteOperation

2. 构建 SparkCopyOnWriteScan
   -> 将 WHERE 条件下推到扫描
   -> 找出所有可能包含 category='obsolete' 的数据文件
   -> 运行时过滤进一步缩小范围

3. 对每个受影响的数据文件：
   -> 读取所有行
   -> 过滤掉 category='obsolete' 的行
   -> 将剩余行写入新数据文件

4. 提交：
   OverwriteFiles overwrite = table.newOverwrite();
   overwrite.deleteFiles(oldFiles, danglingDVs);
   for (DataFile newFile : newFiles) {
     overwrite.addFile(newFile);
   }
   overwrite.commit();

5. 结果：旧数据文件被替换为不含被删除行的新数据文件
```

**MOR + Position Delete 模式**：

```
1. SparkRowLevelOperationBuilder 检查 write.delete.mode
   -> "merge-on-read" -> 创建 SparkPositionDeltaOperation

2. 构建 SparkBatchQueryScan
   -> 扫描满足 category='obsolete' 的行
   -> 对每个匹配行，提取 (file_path, row_position, spec_id, partition)

3. DeleteOnlyDeltaWriter 处理：
   -> 对每个 delete(metadata, id) 调用：
      positionDelete.set(file, position);
      delegate.write(positionDelete, spec, partition);

4. Writer 选择（BaseDeltaWriter.newDeleteWriter）：
   -> V3 表 + PUFFIN 格式: PartitioningDVWriter
      -> 所有删除位置写入 BitmapPositionDeleteIndex
      -> 关闭时序列化为 DV blob，写入 Puffin 文件
   -> V2 表 + 有序输入: ClusteredPositionDeleteWriter
      -> 直接写入 Parquet 格式的 position delete 文件
   -> V2 表 + 无序输入: FanoutPositionOnlyDeleteWriter
      -> 先缓冲到 Bitmap，排序后写入

5. 提交：
   RowDelta rowDelta = table.newRowDelta();
   for (DeleteFile deleteFile : deleteFiles) {
     rowDelta.addDeletes(deleteFile);
   }
   rowDelta.validateDataFilesExist(referencedDataFiles);
   rowDelta.commit();

6. 结果：新增 delete file(s)，原数据文件不变
```

### 11.2 UPDATE 场景

假设执行 `UPDATE iceberg_table SET price = price * 1.1 WHERE category = 'premium'`：

**COW 模式**：

```
1. 创建 SparkCopyOnWriteOperation

2. 扫描 + 运行时过滤
   -> 找出包含 category='premium' 的数据文件

3. 对每个受影响的数据文件：
   -> 读取所有行
   -> 对 category='premium' 的行应用 price = price * 1.1
   -> 所有行（包括更新后的和未修改的）写入新文件

4. 提交：
   OverwriteFiles 原子替换旧文件为新文件
```

**MOR 模式**：

```
1. 创建 SparkPositionDeltaOperation
   -> representUpdateAsDeleteAndInsert() = true
   -> UPDATE 被拆解为 DELETE + INSERT

2. 扫描满足 category='premium' 的行
   -> 对每个匹配行提取：
      a. (file_path, row_position) → 用于 delete
      b. 应用 SET price = price * 1.1 → 生成更新后的新行

3. PartitionedDeltaWriter (或 UnpartitionedDeltaWriter) 处理：
   -> delete(meta, id): 将旧行位置写入 delete writer (DV 或 Position Delete)
   -> reinsert(meta, row): 将更新后的行写入 data writer

4. 内部使用 BasePositionDeltaWriter：
   -> insertWriter: 写入新数据文件
   -> deleteWriter: 写入 delete file / DV

5. 提交：
   RowDelta rowDelta = table.newRowDelta();
   rowDelta.addRows(newDataFiles);
   rowDelta.addDeletes(deleteFiles);
   rowDelta.validateDeletedFiles();
   rowDelta.validateNoConflictingDeleteFiles();
   rowDelta.commit();

6. 验证差异（相比 DELETE 命令）：
   -> UPDATE/MERGE 需要 validateDeletedFiles() 和 validateNoConflictingDeleteFiles()
   -> 因为 UPDATE 是 read-modify-write，需要保证中间没有冲突的删除操作
```

### 11.3 MERGE INTO 场景

假设执行：
```sql
MERGE INTO target USING source
ON target.id = source.id
WHEN MATCHED AND source.action = 'delete' THEN DELETE
WHEN MATCHED AND source.action = 'update' THEN UPDATE SET target.value = source.value
WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)
```

**MOR 模式的执行流程**：

```
1. 创建 SparkPositionDeltaOperation

2. Spark 优化器构建执行计划：
   -> target 扫描：SparkBatchQueryScan
   -> source 扫描
   -> 根据 ON condition 进行 join
   -> 根据 WHEN 条件分流：
      a. MATCHED + DELETE → delete(meta, id)
      b. MATCHED + UPDATE → delete(meta, id) + reinsert(meta, newRow)
      c. NOT MATCHED → insert(newRow)

3. PartitionedDeltaWriter 处理所有三种操作：
   -> delete(): 记录 (file_path, pos) → 写入 DV 或 Position Delete
   -> reinsert(): 将更新后的行写入新数据文件
   -> insert(): 将新插入的行写入新数据文件

4. 注意 reinsert vs insert 的区别：
   // reinsert 可以携带 row lineage 信息
   @Override
   public void reinsert(InternalRow meta, InternalRow row) throws IOException {
     dataPartitionKey.partition(internalRowDataWrapper.wrap(row));
     delegate.insert(decorateWithRowLineage(meta, row), dataSpec, dataPartitionKey);
   }
   // insert 是全新的行，没有 lineage
   @Override
   public void insert(InternalRow row) throws IOException {
     reinsert(null, row);
   }

5. 提交：
   RowDelta rowDelta = table.newRowDelta();
   -> addRows(dataFiles)    // 包含 update 的新行和 insert 的新行
   -> addDeletes(deleteFiles) // 包含 delete 和 update 对应的旧行位置
   -> removeDeletes(rewrittenDeleteFiles) // 合并旧 delete 后可删除的文件
   -> 冲突验证：
      rowDelta.validateDeletedFiles();            // UPDATE/MERGE 需要
      rowDelta.validateNoConflictingDeleteFiles(); // UPDATE/MERGE 需要
      if (isolationLevel == SERIALIZABLE) {
        rowDelta.validateNoConflictingDataFiles(); // SERIALIZABLE 额外需要
      }
   -> commit()

6. Commit message 格式：
   "position delta with X data files, Y delete files and Z rewritten delete files
    (scanSnapshotId: N, conflictDetectionFilter: ..., isolationLevel: ...)"
```

**SparkRowLevelOperationBuilder 中的模式选择**（第 80-97 行）展示了如何根据命令类型选择配置：

```java
private RowLevelOperationMode mode(Map<String, String> properties, Command command) {
    String modeName;
    switch (command) {
      case DELETE:
        modeName = properties.getOrDefault(DELETE_MODE, DELETE_MODE_DEFAULT);
        break;
      case UPDATE:
        modeName = properties.getOrDefault(UPDATE_MODE, UPDATE_MODE_DEFAULT);
        break;
      case MERGE:
        modeName = properties.getOrDefault(MERGE_MODE, MERGE_MODE_DEFAULT);
        break;
    }
    return RowLevelOperationMode.fromName(modeName);
}
```

这意味着可以为不同的命令配置不同的模式。例如：
```sql
ALTER TABLE t SET TBLPROPERTIES (
  'write.delete.mode' = 'merge-on-read',   -- DELETE 使用 MOR
  'write.update.mode' = 'copy-on-write',   -- UPDATE 使用 COW
  'write.merge.mode' = 'merge-on-read'     -- MERGE 使用 MOR
);
```

**隔离级别选择**也是按命令独立配置的（第 100-118 行）：

```java
private IsolationLevel isolationLevel(Map<String, String> properties, Command command) {
    String levelName;
    switch (command) {
      case DELETE:
        levelName = properties.getOrDefault(DELETE_ISOLATION_LEVEL, DELETE_ISOLATION_LEVEL_DEFAULT);
        break;
      case UPDATE:
        levelName = properties.getOrDefault(UPDATE_ISOLATION_LEVEL, UPDATE_ISOLATION_LEVEL_DEFAULT);
        break;
      case MERGE:
        levelName = properties.getOrDefault(MERGE_ISOLATION_LEVEL, MERGE_ISOLATION_LEVEL_DEFAULT);
        break;
    }
    return IsolationLevel.fromName(levelName);
}
```

---

## 总结

### 核心源码文件索引

| 组件 | 源码路径 |
|------|---------|
| **EqualityDeleteWriter** | `core/src/main/java/org/apache/iceberg/deletes/EqualityDeleteWriter.java` |
| **PositionDeleteWriter** | `core/src/main/java/org/apache/iceberg/deletes/PositionDeleteWriter.java` |
| **SortingPositionOnlyDeleteWriter** | `core/src/main/java/org/apache/iceberg/deletes/SortingPositionOnlyDeleteWriter.java` |
| **BitmapPositionDeleteIndex** | `core/src/main/java/org/apache/iceberg/deletes/BitmapPositionDeleteIndex.java` |
| **RoaringPositionBitmap** | `core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java` |
| **PositionDeleteIndex** | `core/src/main/java/org/apache/iceberg/deletes/PositionDeleteIndex.java` |
| **DeletionVector** | `core/src/main/java/org/apache/iceberg/DeletionVector.java` |
| **BaseDVFileWriter** | `core/src/main/java/org/apache/iceberg/deletes/BaseDVFileWriter.java` |
| **DVFileWriter** | `core/src/main/java/org/apache/iceberg/deletes/DVFileWriter.java` |
| **DVUtil** | `core/src/main/java/org/apache/iceberg/DVUtil.java` |
| **PartitioningDVWriter** | `core/src/main/java/org/apache/iceberg/io/PartitioningDVWriter.java` |
| **DeleteGranularity** | `core/src/main/java/org/apache/iceberg/deletes/DeleteGranularity.java` |
| **Deletes** | `core/src/main/java/org/apache/iceberg/deletes/Deletes.java` |
| **DeleteFilter** | `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java` |
| **DeleteFileIndex** | `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java` |
| **BaseDeleteLoader** | `data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java` |
| **BaseRowDelta** | `core/src/main/java/org/apache/iceberg/BaseRowDelta.java` |
| **RowDelta** | `api/src/main/java/org/apache/iceberg/RowDelta.java` |
| **MergingSnapshotProducer** | `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java` |
| **RowLevelOperationMode** | `core/src/main/java/org/apache/iceberg/RowLevelOperationMode.java` |
| **ContentFileUtil** | `core/src/main/java/org/apache/iceberg/util/ContentFileUtil.java` |
| **FileContent** | `api/src/main/java/org/apache/iceberg/FileContent.java` |
| **StandardBlobTypes** | `core/src/main/java/org/apache/iceberg/puffin/StandardBlobTypes.java` |
| **SparkPositionDeltaWrite** | `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkPositionDeltaWrite.java` |
| **SparkPositionDeltaOperation** | `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkPositionDeltaOperation.java` |
| **SparkCopyOnWriteOperation** | `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkCopyOnWriteOperation.java` |
| **SparkCopyOnWriteScan** | `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkCopyOnWriteScan.java` |
| **SparkWrite** | `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java` |
| **SparkRowLevelOperationBuilder** | `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkRowLevelOperationBuilder.java` |
| **BasePositionDeltaWriter** | `core/src/main/java/org/apache/iceberg/io/BasePositionDeltaWriter.java` |
| **FanoutPositionOnlyDeleteWriter** | `core/src/main/java/org/apache/iceberg/io/FanoutPositionOnlyDeleteWriter.java` |
| **TrackedFile** | `core/src/main/java/org/apache/iceberg/TrackedFile.java` |

### 技术演进总结

```
V1 (Append-Only)
  |
  v
V2 (行级删除)
  ├── Equality Delete: 按字段值匹配 → 适合流式 CDC
  ├── Position Delete: 按 (file_path, pos) 精确删除 → 适合批处理
  ├── RowDelta API: 原子添加 data + delete files
  └── COW / MOR 两条路径
  |
  v
V3 (Deletion Vector)
  ├── DV: 基于 Roaring Bitmap + Puffin 文件
  │   ├── 一对一绑定数据文件
  │   ├── 更紧凑的存储
  │   ├── 更快的读取（直接反序列化 Bitmap）
  │   └── 与 Delta Lake 兼容的序列化格式
  ├── TrackedFile 统一模型
  ├── Row Lineage 支持
  └── DV 合并与冲突检测增强
```

Iceberg 的行级操作演进体现了一个清晰的设计哲学：**在不可变文件的约束下，通过不断优化"标记删除"的效率来逼近原地修改的性能**。从 Equality Delete 的全字段匹配，到 Position Delete 的精确定位，再到 DV 的 Bitmap 索引，每一步演进都在减少 delete 操作的存储开销和读取时的合并成本。

同时，COW 和 MOR 两条路径的并存为用户提供了灵活的选择：COW 以写入开销换取读取性能，MOR 以读取开销换取写入性能。用户可以根据工作负载特征，甚至为不同的 SQL 命令（DELETE/UPDATE/MERGE）配置不同的模式，实现最优的性能权衡。

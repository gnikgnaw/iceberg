# Apache Iceberg 索引机制深度源码分析

## 目录

- [1. 概述](#1-概述)
- [2. Manifest 级索引：分区范围摘要](#2-manifest-级索引分区范围摘要)
- [3. 分区索引：Partition Pruning](#3-分区索引partition-pruning)
- [4. 列级统计信息索引](#4-列级统计信息索引)
- [5. Bloom Filter 索引](#5-bloom-filter-索引)
- [6. Puffin 文件与统计信息索引](#6-puffin-文件与统计信息索引)
- [7. 删除文件索引](#7-删除文件索引)
- [8. Multi-dimensional 索引：Z-Order / Hilbert Curve](#8-multi-dimensional-索引z-order--hilbert-curve)
- [9. 查询规划中的索引使用时机与顺序](#9-查询规划中的索引使用时机与顺序)
- [10. 查询场景示例：索引过滤效果对比](#10-查询场景示例索引过滤效果对比)
- [11. 与传统数据库索引的对比](#11-与传统数据库索引的对比)
- [12. 索引相关的性能调优建议](#12-索引相关的性能调优建议)
- [13. 总结](#13-总结)

---

## 1. 概述

Apache Iceberg 没有像传统数据库那样维护独立的 B-Tree 或 Hash Index 结构。相反，Iceberg 采用了一套**多层次的统计信息索引体系**，通过元数据中嵌入的统计信息实现高效的文件裁剪（File Pruning）。这种设计哲学的核心思想是：

- **索引即元数据**：所有索引信息都嵌入在 Manifest File 和 Data File 的元数据中，无需维护独立的索引文件
- **逐层递进过滤**：从 Manifest 级到文件级再到 Row Group 级，逐层缩小扫描范围
- **无副作用的只读索引**：索引信息在写入时一次性生成，读取时只需使用，不会因并发读写而产生锁竞争

Iceberg 的索引体系可以分为以下层次：

```
                     查询规划过程中的索引过滤层次
┌──────────────────────────────────────────────────────────────┐
│  第 1 层: Manifest 级索引 (PartitionFieldSummary)            │
│  ├── 利用 ManifestFile 中的分区范围摘要                        │
│  └── 快速跳过不相关的 Manifest 文件                            │
├──────────────────────────────────────────────────────────────┤
│  第 2 层: 分区索引 (Partition Pruning)                        │
│  ├── 通过分区表达式投影，评估 DataFile 的分区值                   │
│  └── 跳过不匹配分区的数据文件                                   │
├──────────────────────────────────────────────────────────────┤
│  第 3 层: 列级统计索引 (Column-level Min/Max Stats)           │
│  ├── 利用 DataFile 中每列的 lower_bounds / upper_bounds       │
│  └── 通过 InclusiveMetricsEvaluator 排除不匹配文件              │
├──────────────────────────────────────────────────────────────┤
│  第 4 层: Row Group 级索引 (Parquet 内部)                     │
│  ├── Parquet Row Group 统计信息过滤                            │
│  ├── Parquet Dictionary 过滤                                  │
│  └── Parquet Bloom Filter 过滤                                │
└──────────────────────────────────────────────────────────────┘
```

---

## 2. Manifest 级索引：分区范围摘要

### 2.1 核心数据结构

Manifest 级索引是 Iceberg 查询规划的**第一道过滤屏障**。每个 `ManifestFile` 中都包含一个 `PartitionFieldSummary` 列表，记录了该 Manifest 内所有数据文件的分区值范围。

**源码位置**: `api/src/main/java/org/apache/iceberg/ManifestFile.java`（第 68-89 行）

```java
Types.StructType PARTITION_SUMMARY_TYPE =
    Types.StructType.of(
        required(509, "contains_null", Types.BooleanType.get(),
            "True if any file has a null partition value"),
        optional(518, "contains_nan", Types.BooleanType.get(),
            "True if any file has a nan partition value"),
        optional(510, "lower_bound", Types.BinaryType.get(),
            "Partition lower bound for all files"),
        optional(511, "upper_bound", Types.BinaryType.get(),
            "Partition upper bound for all files"));
```

`PartitionFieldSummary` 接口（第 222-255 行）提供了四个关键方法：

| 方法 | 描述 |
|------|------|
| `containsNull()` | 是否有文件在该分区字段包含 null 值 |
| `containsNaN()` | 是否有文件在该分区字段包含 NaN 值 |
| `lowerBound()` | 该 Manifest 内所有文件的分区字段最小值 |
| `upperBound()` | 该 Manifest 内所有文件的分区字段最大值 |

### 2.2 摘要的构建过程

分区摘要在写入 Manifest 文件时由 `PartitionSummary` 类实时计算。

**源码位置**: `core/src/main/java/org/apache/iceberg/PartitionSummary.java`（第 32-102 行）

核心逻辑在 `PartitionFieldStats.update()` 方法中（第 84-99 行）：

```java
void update(T value) {
  if (value == null) {
    this.containsNull = true;
  } else if (NaNUtil.isNaN(value)) {
    this.containsNaN = true;
  } else if (min == null) {
    this.min = value;
    this.max = value;
  } else {
    if (comparator.compare(value, min) < 0) {
      this.min = value;
    }
    if (comparator.compare(max, value) < 0) {
      this.max = value;
    }
  }
}
```

每写入一个数据文件到 Manifest 中，就会用该文件的分区值更新对应分区字段的 `PartitionFieldStats`。最终写入 Manifest List 时，通过 `toSummary()` 将统计信息序列化为 `GenericPartitionFieldSummary`。

### 2.3 Manifest 级评估器

**源码位置**: `api/src/main/java/org/apache/iceberg/expressions/ManifestEvaluator.java`

`ManifestEvaluator` 是 Manifest 级索引的评估器，负责判断一个 Manifest 文件是否**可能**包含匹配查询条件的数据。

```java
public static ManifestEvaluator forRowFilter(
    Expression rowFilter, PartitionSpec spec, boolean caseSensitive) {
  return new ManifestEvaluator(
      spec, Projections.inclusive(spec, caseSensitive).project(rowFilter), caseSensitive);
}
```

关键点：
- 使用 `Projections.inclusive()` 将行级表达式**投影**为分区级表达式
- Inclusive Projection 保证：如果原始表达式匹配某一行，那么投影后的表达式一定匹配该行的分区值
- 评估方式是**包容性的（inclusive）**：返回 `true` 表示"可能匹配"，返回 `false` 表示"一定不匹配"

以 `eq` 操作为例（第 246-266 行）：

```java
public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
  int pos = Accessors.toPosition(ref.accessor());
  PartitionFieldSummary fieldStats = stats.get(pos);
  if (fieldStats.lowerBound() == null) {
    return ROWS_CANNOT_MATCH; // values are all null
  }
  T lower = Conversions.fromByteBuffer(ref.type(), fieldStats.lowerBound());
  int cmp = lit.comparator().compare(lower, lit.value());
  if (cmp > 0) {
    return ROWS_CANNOT_MATCH;
  }
  T upper = Conversions.fromByteBuffer(ref.type(), fieldStats.upperBound());
  cmp = lit.comparator().compare(upper, lit.value());
  if (cmp < 0) {
    return ROWS_CANNOT_MATCH;
  }
  return ROWS_MIGHT_MATCH;
}
```

逻辑：如果查询值在 [lower_bound, upper_bound] 范围之外，则整个 Manifest 可以被跳过。

### 2.4 小结

Manifest 级索引是一种**粗粒度的范围索引**。它的价值在于能够以极低的成本（只需读取 Manifest List 中的摘要信息，而无需打开 Manifest 文件本身）快速排除大量不相关的 Manifest 文件。对于大表来说，一个 Snapshot 可能有成百上千个 Manifest 文件，Manifest 级索引可以在毫秒级完成第一轮过滤。

---

## 3. 分区索引：Partition Pruning

### 3.1 分区裁剪机制

分区裁剪发生在 Manifest 文件内部读取阶段。当 `ManifestReader` 读取 Manifest 文件中的每个 `DataFile` 条目时，会使用 `Evaluator` 对数据文件的分区值进行精确评估。

**源码位置**: `core/src/main/java/org/apache/iceberg/ManifestReader.java`（第 237-264 行）

```java
private CloseableIterable<ManifestEntry<F>> entries(boolean onlyLive) {
  if (hasRowFilter() || hasPartitionFilter() || partitionSet != null) {
    Evaluator evaluator = evaluator();
    InclusiveMetricsEvaluator metricsEvaluator = metricsEvaluator();
    // ...
    return CloseableIterable.filter(
        // ...
        entry ->
            entry != null
                && evaluator.eval(entry.file().partition())  // 分区裁剪
                && metricsEvaluator.eval(entry.file())        // 列统计裁剪
                && inPartitionSet(entry.file()));
  }
  // ...
}
```

### 3.2 Expression Projection（表达式投影）

分区裁剪的核心在于**表达式投影**。Iceberg 通过 `Projections.inclusive()` 将用户的行级查询条件转换为分区级条件。

**源码位置**: `api/src/main/java/org/apache/iceberg/expressions/Projections.java`

表达式投影的过程：
1. 用户写了 `WHERE ts >= '2024-01-01' AND ts < '2024-02-01'`
2. 假设分区规格为 `date(ts)`（按天分区）
3. Inclusive Projection 将条件转换为对分区字段 `ts_day` 的谓词：`ts_day >= 2024-01-01 AND ts_day < 2024-02-01`
4. 该投影后的表达式用于精确评估每个文件的分区值

### 3.3 Evaluator 的工作机制

`Evaluator` 直接对 `StructLike` 形式的分区值进行表达式求值。在 `ManifestReader.evaluator()` 方法中（第 347-354 行）：

```java
private Evaluator evaluator() {
  if (lazyEvaluator == null) {
    Expression projected = Projections.inclusive(spec, caseSensitive).project(rowFilter);
    Expression finalPartFilter = Expressions.and(projected, partFilter);
    this.lazyEvaluator = new Evaluator(spec.partitionType(), finalPartFilter, caseSensitive);
  }
  return lazyEvaluator;
}
```

它将行过滤器的投影和直接传入的分区过滤器做 AND 合并，然后用 `Evaluator` 对每个数据文件的分区值做精确匹配。

### 3.4 小结

分区裁剪是 Iceberg 中最直接、最有效的索引手段。它利用了数据文件的分区元数据（每个 DataFile 都记录了其所属的分区值），可以精确地排除不匹配分区的文件。不同于 Hive 需要实际列出目录下文件来判断分区，Iceberg 的分区信息直接记录在元数据中，效率更高。

---

## 4. 列级统计信息索引

### 4.1 DataFile 中的列统计信息

每个 `DataFile` 都记录了丰富的列级统计信息，这些信息在文件写入时收集并存储在 Manifest 文件的条目中。

**源码位置**: `api/src/main/java/org/apache/iceberg/DataFile.java`（第 51-86 行）

| 统计字段 | Field ID | 类型 | 说明 |
|----------|----------|------|------|
| `column_sizes` | 108 | `Map<Integer, Long>` | 列 ID -> 列在磁盘上的字节数 |
| `value_counts` | 109 | `Map<Integer, Long>` | 列 ID -> 值总数（含 null 和 NaN） |
| `null_value_counts` | 110 | `Map<Integer, Long>` | 列 ID -> null 值数量 |
| `nan_value_counts` | 137 | `Map<Integer, Long>` | 列 ID -> NaN 值数量 |
| `lower_bounds` | 125 | `Map<Integer, ByteBuffer>` | 列 ID -> 列最小值 |
| `upper_bounds` | 128 | `Map<Integer, ByteBuffer>` | 列 ID -> 列最大值 |

`ContentFile` 接口（`api/src/main/java/org/apache/iceberg/ContentFile.java`）定义了访问这些统计信息的方法：

```java
Map<Integer, Long> columnSizes();
Map<Integer, Long> valueCounts();
Map<Integer, Long> nullValueCounts();
Map<Integer, Long> nanValueCounts();
Map<Integer, ByteBuffer> lowerBounds();
Map<Integer, ByteBuffer> upperBounds();
```

### 4.2 InclusiveMetricsEvaluator

`InclusiveMetricsEvaluator` 是列级统计索引的核心评估器。它利用文件级的 min/max、null count 等统计信息来判断文件是否可能包含匹配的行。

**源码位置**: `api/src/main/java/org/apache/iceberg/expressions/InclusiveMetricsEvaluator.java`

评估器初始化时会绑定表达式（第 60-67 行）：

```java
public InclusiveMetricsEvaluator(Schema schema, Expression unbound, boolean caseSensitive) {
  StructType struct = schema.asStruct();
  this.expr = Binder.bind(struct, rewriteNot(unbound), caseSensitive);
}
```

评估入口（第 75-78 行）：

```java
public boolean eval(ContentFile<?> file) {
  return new MetricsEvalVisitor().eval(file);
}
```

#### 4.2.1 MetricsEvalVisitor 内部工作

`MetricsEvalVisitor`（第 83-632 行）是一个 `BoundVisitor<Boolean>`，它遍历绑定后的表达式树，对每个谓词节点利用文件统计信息做判断。

初始化阶段提取文件统计（第 90-109 行）：

```java
private boolean eval(ContentFile<?> file) {
  if (file.recordCount() == 0) {
    return ROWS_CANNOT_MATCH;
  }
  this.valueCounts = file.valueCounts();
  this.nullCounts = file.nullValueCounts();
  this.nanCounts = file.nanValueCounts();
  this.lowerBounds = file.lowerBounds();
  this.upperBounds = file.upperBounds();
  return ExpressionVisitors.visitEvaluator(expr, this);
}
```

**等值查询** `eq`（第 298-324 行）的评估逻辑：

```java
public <T> Boolean eq(Bound<T> term, Literal<T> lit) {
  int id = term.ref().fieldId();
  if (containsNullsOnly(id) || containsNaNsOnly(id)) {
    return ROWS_CANNOT_MATCH;
  }
  T lower = lowerBound(term);
  if (lower != null && !NaNUtil.isNaN(lower)) {
    int cmp = lit.comparator().compare(lower, lit.value());
    if (cmp > 0) { // 查询值 < 文件最小值
      return ROWS_CANNOT_MATCH;
    }
  }
  T upper = upperBound(term);
  if (null == upper) {
    return ROWS_MIGHT_MATCH;
  }
  int cmp = lit.comparator().compare(upper, lit.value());
  if (cmp < 0) { // 查询值 > 文件最大值
    return ROWS_CANNOT_MATCH;
  }
  return ROWS_MIGHT_MATCH;
}
```

**IN 查询**（第 342-386 行）的优化：

```java
public <T> Boolean in(Bound<T> term, Set<T> literalSet) {
  int id = term.ref().fieldId();
  if (containsNullsOnly(id) || containsNaNsOnly(id)) {
    return ROWS_CANNOT_MATCH;
  }
  Collection<T> literals = literalSet;
  if (literals.size() > IN_PREDICATE_LIMIT) { // 200
    return ROWS_MIGHT_MATCH; // 集合太大时跳过评估
  }
  T lower = lowerBound(term);
  // 过滤掉所有 < lower_bound 的值
  literals = literals.stream()
      .filter(v -> comparator.compare(lower, v) <= 0)
      .collect(Collectors.toList());
  if (literals.isEmpty()) {
    return ROWS_CANNOT_MATCH;
  }
  T upper = upperBound(term);
  // 过滤掉所有 > upper_bound 的值
  literals = literals.stream()
      .filter(v -> comparator.compare(upper, v) >= 0)
      .collect(Collectors.toList());
  if (literals.isEmpty()) {
    return ROWS_CANNOT_MATCH;
  }
  return ROWS_MIGHT_MATCH;
}
```

关键设计点：
- IN 谓词的值集合超过 200 个时直接放弃评估，避免性能问题
- 通过双向边界过滤，先用 lower_bound 过滤，再用 upper_bound 过滤

#### 4.2.2 辅助判断方法

```java
// 列是否可能包含 null（第 490-492 行）
private boolean mayContainNull(Integer id) {
  return nullCounts == null || !nullCounts.containsKey(id) || nullCounts.get(id) != 0;
}

// 列是否只包含 null（第 494-500 行）
private boolean containsNullsOnly(Integer id) {
  return valueCounts != null && valueCounts.containsKey(id)
      && nullCounts != null && nullCounts.containsKey(id)
      && valueCounts.get(id) - nullCounts.get(id) == 0;
}

// 列是否只包含 NaN（第 502-507 行）
private boolean containsNaNsOnly(Integer id) {
  return nanCounts != null && nanCounts.containsKey(id)
      && valueCounts != null
      && nanCounts.get(id).equals(valueCounts.get(id));
}
```

#### 4.2.3 Transform 支持

对于涉及 Transform 的表达式（如 `bucket(id, 16) = 5`），`InclusiveMetricsEvaluator` 也能处理。它通过 `BoundTransform` 判断 Transform 是否保序（order preserving），如果保序则可以利用原始列的统计信息（第 579-597 行）：

```java
private <S, T> T transformLowerBound(BoundTransform<S, T> boundTransform) {
  Transform<S, T> transform = boundTransform.transform();
  if (transform.preservesOrder()) {
    S lower = parseLowerBound(boundTransform.ref());
    return boundTransform.transform().bind(boundTransform.ref().type()).apply(lower);
  }
  return null;
}
```

### 4.3 小结

列级统计信息索引是 Iceberg 最核心的文件级过滤机制。它的优势在于：
- 统计信息在写入时自动收集，零额外成本
- 支持所有基础比较操作（eq、lt、gt、in 等）
- 能处理 null/NaN 的边界情况
- 支持保序 Transform 的透明传递

---

## 5. Bloom Filter 索引

### 5.1 概述

Bloom Filter 是一种概率型数据结构，能高效判断一个元素**一定不在**集合中（无假阴性），但可能误判为**在集合中**（有假阳性）。Iceberg 在 Parquet 文件格式层面支持 Bloom Filter，作为列统计信息之后的补充过滤手段。

### 5.2 配置方式

**源码位置**: `core/src/main/java/org/apache/iceberg/TableProperties.java`（第 169-181 行）

```java
// Bloom Filter 最大字节数
public static final String PARQUET_BLOOM_FILTER_MAX_BYTES =
    "write.parquet.bloom-filter-max-bytes";
public static final int PARQUET_BLOOM_FILTER_MAX_BYTES_DEFAULT = 1024 * 1024; // 1MB

// 列级 FPP（False Positive Probability）
public static final String PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX =
    "write.parquet.bloom-filter-fpp.column.";
public static final double PARQUET_BLOOM_FILTER_COLUMN_FPP_DEFAULT = 0.01; // 1%

// 列级 NDV（Number of Distinct Values）
public static final String PARQUET_BLOOM_FILTER_COLUMN_NDV_PREFIX =
    "write.parquet.bloom-filter-ndv.column.";

// 列级启用开关
public static final String PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX =
    "write.parquet.bloom-filter-enabled.column.";
```

配置示例：

```sql
-- 为 user_id 列启用 Bloom Filter
ALTER TABLE my_table SET TBLPROPERTIES (
  'write.parquet.bloom-filter-enabled.column.user_id' = 'true',
  'write.parquet.bloom-filter-fpp.column.user_id' = '0.01',
  'write.parquet.bloom-filter-ndv.column.user_id' = '1000000'
);
```

ORC 格式也有类似的 Bloom Filter 支持（第 202-206 行）：

```java
public static final String ORC_BLOOM_FILTER_COLUMNS = "write.orc.bloom.filter.columns";
public static final String ORC_BLOOM_FILTER_COLUMNS_DEFAULT = "";
public static final String ORC_BLOOM_FILTER_FPP = "write.orc.bloom.filter.fpp";
public static final double ORC_BLOOM_FILTER_FPP_DEFAULT = 0.05;
```

### 5.3 写入时配置传递

**源码位置**: `parquet/src/main/java/org/apache/iceberg/parquet/Parquet.java`

在 Parquet 写入器构建时，Bloom Filter 配置被传递到 Parquet Writer（第 594-623 行相关区域）：

```java
int bloomFilterMaxBytes =
    PropertyUtil.propertyAsInt(
        config, PARQUET_BLOOM_FILTER_MAX_BYTES, PARQUET_BLOOM_FILTER_MAX_BYTES_DEFAULT);
Map<String, String> columnFPPs =
    PropertyUtil.propertiesWithPrefix(config, PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX);
Map<String, String> columnNDVs =
    PropertyUtil.propertiesWithPrefix(config, PARQUET_BLOOM_FILTER_COLUMN_NDV_PREFIX);
Map<String, String> columnEnabledMap =
    PropertyUtil.propertiesWithPrefix(config, PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX);
```

这些配置最终传递给 Parquet 底层的 `ParquetWriter`，由 Parquet 库在 Row Group 级别构建 Bloom Filter。

### 5.4 读取时过滤：ParquetBloomRowGroupFilter

**源码位置**: `parquet/src/main/java/org/apache/iceberg/parquet/ParquetBloomRowGroupFilter.java`

`ParquetBloomRowGroupFilter` 是 Bloom Filter 读取端的过滤器。它在 Parquet Row Group 级别工作，判断一个 Row Group 是否可能包含匹配的记录。

```java
public boolean shouldRead(
    MessageType fileSchema, BlockMetaData rowGroup, BloomFilterReader bloomReader) {
  return new BloomEvalVisitor().eval(fileSchema, rowGroup, bloomReader);
}
```

`BloomEvalVisitor` 只对以下操作有效（第 210-243 行）：

- **`eq`（等值查询）**：核心场景，直接查询值的 hash 是否在 Bloom Filter 中

```java
public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
  int id = ref.fieldId();
  if (!fieldsWithBloomFilter.contains(id)) {
    return ROWS_MIGHT_MATCH;
  }
  BloomFilter bloom = loadBloomFilter(id);
  Type type = types.get(id);
  T value = lit.value();
  return shouldRead(parquetPrimitiveTypes.get(id), value, bloom, type);
}
```

- **`in`（IN 查询）**：对集合中每个值查 Bloom Filter，只要有一个可能匹配就返回 `ROWS_MIGHT_MATCH`

```java
public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
  int id = ref.fieldId();
  if (!fieldsWithBloomFilter.contains(id)) {
    return ROWS_MIGHT_MATCH;
  }
  BloomFilter bloom = loadBloomFilter(id);
  Type type = types.get(id);
  for (T e : literalSet) {
    if (shouldRead(parquetPrimitiveTypes.get(id), e, bloom, type)) {
      return ROWS_MIGHT_MATCH;
    }
  }
  return ROWS_CANNOT_MATCH;
}
```

以下操作对 Bloom Filter **无效**（始终返回 `ROWS_MIGHT_MATCH`）：
- `lt`, `ltEq`, `gt`, `gtEq`（范围查询 -- Bloom Filter 基于 hash，不支持范围）
- `notEq`, `notIn`（不等于查询）
- `isNull`, `notNull`, `isNaN`, `notNaN`
- `startsWith`, `notStartsWith`

### 5.5 在 ReadConf 中的集成

**源码位置**: `parquet/src/main/java/org/apache/iceberg/parquet/ReadConf.java`（第 96-117 行）

Bloom Filter 过滤器与统计过滤器（Stats Filter）、字典过滤器（Dictionary Filter）一起组成了 Parquet 层面的三重过滤：

```java
ParquetMetricsRowGroupFilter statsFilter = null;
ParquetDictionaryRowGroupFilter dictFilter = null;
ParquetBloomRowGroupFilter bloomFilter = null;
if (filter != null) {
  statsFilter = new ParquetMetricsRowGroupFilter(expectedSchema, filter, caseSensitive);
  dictFilter = new ParquetDictionaryRowGroupFilter(expectedSchema, filter, caseSensitive);
  bloomFilter = new ParquetBloomRowGroupFilter(expectedSchema, filter, caseSensitive);
}

for (int i = 0; i < shouldSkip.length; i += 1) {
  BlockMetaData rowGroup = rowGroups.get(i);
  boolean shouldRead =
      filter == null
          || (statsFilter.shouldRead(typeWithIds, rowGroup)
              && dictFilter.shouldRead(typeWithIds, rowGroup, reader.getDictionaryReader(rowGroup))
              && bloomFilter.shouldRead(typeWithIds, rowGroup, reader.getBloomFilterDataReader(rowGroup)));
  this.shouldSkip[i] = !shouldRead;
}
```

三个过滤器用 `&&` 连接，意味着**任何一个**过滤器判断该 Row Group 不需要读取，该 Row Group 就会被跳过。

### 5.6 小结

Bloom Filter 是 Iceberg 在 Parquet 文件格式内部的补充索引机制。它特别适合高基数列的等值查询场景（如 UUID、用户 ID、订单号等）。其局限性在于：不支持范围查询、存在假阳性、需要手动为列配置启用。

---

## 6. Puffin 文件与统计信息索引

### 6.1 Puffin 文件格式

Puffin 是 Iceberg 定义的一种专用文件格式，用于存储无法直接嵌入 Manifest 的索引和统计信息。

**规范文件**: `format/puffin-spec.md`

文件结构：

```
Magic Blob₁ Blob₂ ... Blobₙ Footer
```

- `Magic`: 4 字节标识 `0x50, 0x46, 0x41, 0x31`（Puffin Fratercula arctica, version 1）
- `Blobᵢ`: 第 i 个数据块
- `Footer`: JSON 格式的元数据，描述所有 Blob 的类型、位置和属性

### 6.2 核心接口

**StatisticsFile 接口** (`api/src/main/java/org/apache/iceberg/StatisticsFile.java`):

```java
public interface StatisticsFile {
  long snapshotId();               // 关联的 Snapshot ID
  String path();                   // Puffin 文件路径
  long fileSizeInBytes();          // 文件大小
  long fileFooterSizeInBytes();    // Footer 大小
  List<BlobMetadata> blobMetadata(); // Blob 元数据列表
}
```

**BlobMetadata 接口** (`api/src/main/java/org/apache/iceberg/BlobMetadata.java`):

```java
public interface BlobMetadata {
  String type();                           // Blob 类型
  long sourceSnapshotId();                 // 来源 Snapshot ID
  long sourceSnapshotSequenceNumber();     // 来源序列号
  List<Integer> fields();                  // 关联的字段 ID 列表
  Map<String, String> properties();        // 附加属性
}
```

Puffin 内部的 `BlobMetadata` 实现（`core/src/main/java/org/apache/iceberg/puffin/BlobMetadata.java`）还包括了 `offset`、`length`、`compressionCodec` 等文件内定位信息。

### 6.3 Blob 类型

#### 6.3.1 `apache-datasketches-theta-v1` -- NDV 统计

这是目前 Iceberg 支持的主要 Blob 类型，用于存储列的 **NDV（Number of Distinct Values，去重值数量）** 估算。

实现使用了 Apache DataSketches 库的 Theta Sketch 算法：
- 构建 Alpha 系列 sketch，使用默认种子
- 将各个不同值通过 Iceberg 的单值序列化转为字节后填入 sketch
- Blob 的 properties 中可包含 `ndv` 属性：NDV 的估算值

#### 6.3.2 `deletion-vector-v1` -- 删除向量

从 Iceberg v3 格式开始支持的删除向量 Blob 类型，使用 Roaring Bitmap 存储被删除行的位置。

### 6.4 统计信息的计算与写入

**源码位置**: `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/actions/ComputeTableStatsSparkAction.java`

这是一个 Spark Action，用于计算表的统计信息并以 Puffin 格式存储：

```java
private Result doExecute() {
  List<Blob> blobs = generateNDVBlobs();       // 生成 NDV Blob
  StatisticsFile statisticsFile = writeStatsFile(blobs);  // 写入 Puffin 文件
  table.updateStatistics().setStatistics(statisticsFile).commit();  // 提交到表元数据
  return ImmutableComputeTableStats.Result.builder()
      .statisticsFile(statisticsFile).build();
}
```

写入过程（第 111-120 行）：

```java
private StatisticsFile writeStatsFile(List<Blob> blobs) {
  OutputFile outputFile = table.io().newOutputFile(outputPath());
  try (PuffinWriter writer = Puffin.write(outputFile).createdBy(appIdentifier()).build()) {
    blobs.forEach(writer::add);
    writer.finish();
    return new GenericStatisticsFile(
        snapshotId(), outputFile.location(),
        writer.fileSize(), writer.footerSize(), writer.writtenBlobsMetadata().stream()
            .map(/* ... */).collect(Collectors.toList()));
  }
}
```

### 6.5 NDV 的应用场景

NDV 统计主要用于查询优化器的代价估算（Cost-Based Optimization），而非直接的文件裁剪：
- **Join 顺序优化**：NDV 较小的表适合作为 Build Side
- **聚合策略选择**：NDV 影响是否选择 Hash Aggregation
- **数据倾斜检测**：NDV 与行数的比率可以反映数据倾斜程度

Puffin 统计文件中的 NDV 信息可以在 Spark 的 `SparkScan` 中被读取和利用。

### 6.6 小结

Puffin 文件是 Iceberg 的**扩展统计信息存储机制**。它的设计是面向未来的，可以承载各种类型的 Blob 数据（NDV sketch、Bloom Filter、直方图等）。目前主要应用是 NDV 统计，用于辅助查询优化器做出更好的执行计划决策。

---

## 7. 删除文件索引

### 7.1 删除文件类型

Iceberg 支持三种删除文件：

| 类型 | 描述 | 定位方式 |
|------|------|----------|
| Position Deletes | 按文件路径 + 行位置删除 | 精确匹配数据文件路径 |
| Equality Deletes | 按等值条件删除 | 通过等值字段的值匹配 |
| Deletion Vectors (DVs) | 基于 Puffin 格式的位图 | 通过 `referenced_data_file` 精确关联 |

### 7.2 DeleteFileIndex

**源码位置**: `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java`

`DeleteFileIndex` 是删除文件到数据文件的映射索引。它在查询规划时构建，用于高效地为每个数据文件找到需要应用的删除文件。

核心数据结构（第 70-81 行）：

```java
class DeleteFileIndex {
  private final EqualityDeletes globalDeletes;                    // 全局等值删除
  private final PartitionMap<EqualityDeletes> eqDeletesByPartition; // 按分区的等值删除
  private final PartitionMap<PositionDeletes> posDeletesByPartition; // 按分区的位置删除
  private final Map<String, PositionDeletes> posDeletesByPath;     // 按路径的位置删除
  private final Map<String, DeleteFile> dvByPath;                  // 按路径的 DV
}
```

### 7.3 删除文件查找过程

为一个数据文件查找关联的删除文件（第 151-168 行）：

```java
DeleteFile[] forDataFile(long sequenceNumber, DataFile file) {
  if (isEmpty) {
    return EMPTY_DELETES;
  }
  DeleteFile[] global = findGlobalDeletes(sequenceNumber, file);
  DeleteFile[] eqPartition = findEqPartitionDeletes(sequenceNumber, file);
  DeleteFile dv = findDV(sequenceNumber, file);
  if (dv != null && global == null && eqPartition == null) {
    return new DeleteFile[] {dv};
  } else if (dv != null) {
    return concat(global, eqPartition, new DeleteFile[] {dv});
  } else {
    DeleteFile[] posPartition = findPosPartitionDeletes(sequenceNumber, file);
    DeleteFile[] posPath = findPathDeletes(sequenceNumber, file);
    return concat(global, eqPartition, posPartition, posPath);
  }
}
```

查找逻辑的优先级和分类：
1. **全局等值删除** (`globalDeletes`)：未分区表的等值删除，对所有数据文件都可能生效
2. **分区级等值删除** (`eqDeletesByPartition`)：按分区匹配
3. **DV** (`dvByPath`)：通过 `referenced_data_file` 直接定位到特定数据文件
4. **分区级位置删除** (`posDeletesByPartition`)：按分区匹配
5. **路径级位置删除** (`posDeletesByPath`)：按数据文件路径精确匹配

### 7.4 序列号过滤

删除文件使用序列号（Sequence Number）来确定其适用范围。`PositionDeletes` 类（第 670-742 行）中使用有序数组和二分查找实现高效过滤：

```java
public DeleteFile[] filter(long seq) {
  indexIfNeeded();
  int start = findStartIndex(seqs, seq);
  if (start >= files.length) {
    return EMPTY_DELETES;
  }
  if (start == 0) {
    return files;
  }
  int matchingFilesCount = files.length - start;
  DeleteFile[] matchingFiles = new DeleteFile[matchingFilesCount];
  System.arraycopy(files, start, matchingFiles, 0, matchingFilesCount);
  return matchingFiles;
}
```

核心原则：**只有序列号 >= 数据文件序列号的删除文件才会被应用**。这是 Iceberg MVCC（Multi-Version Concurrency Control）的基础。

### 7.5 等值删除的范围检查优化

对于等值删除，`DeleteFileIndex` 还会通过列统计信息做进一步的范围检查（第 219-281 行）：

```java
private static boolean canContainEqDeletesForFile(
    DataFile dataFile, EqualityDeleteFile deleteFile) {
  // ...
  for (Types.NestedField field : deleteFile.equalityFields()) {
    // 检查 null 值情况
    if (containsNull(dataNullCounts, field) && containsNull(deleteNullCounts, field)) {
      continue; // 数据和删除都有 null，需要应用
    }
    if (allNull(dataNullCounts, dataValueCounts, field) && allNonNull(deleteNullCounts, field)) {
      return false; // 数据全是 null 但删除不含 null，不需要应用
    }
    // 检查值范围是否重叠
    if (!rangesOverlap(field, dataLower, dataUpper, deleteLower, deleteUpper)) {
      return false; // 值范围不重叠，不需要应用
    }
  }
  return true;
}
```

这意味着即使一个等值删除文件与数据文件在同一分区，如果它们在等值字段上的值域不重叠，也可以跳过该删除文件。

### 7.6 删除 Manifest 的过滤

在 `DeleteFileIndex.Builder` 中（第 576-629 行），加载删除 Manifest 时也会应用 Manifest 级过滤：

```java
private Iterable<CloseableIterable<ManifestEntry<DeleteFile>>> deleteManifestReaders() {
  // ...
  LoadingCache<Integer, ManifestEvaluator> evalCache = Caffeine.newBuilder().build(
      specId -> {
        PartitionSpec spec = specsById.get(specId);
        return ManifestEvaluator.forPartitionFilter(
            Expressions.and(partitionFilter, partExprCache.get(specId)),
            spec, caseSensitive);
      });

  CloseableIterable<ManifestFile> matchingManifests =
      CloseableIterable.filter(
          scanMetrics.skippedDeleteManifests(),
          closeableDeleteManifests,
          manifest ->
              manifest.content() == ManifestContent.DELETES
                  && (manifest.hasAddedFiles() || manifest.hasExistingFiles())
                  && evalCache.get(manifest.partitionSpecId()).eval(manifest));
  // ...
}
```

### 7.7 小结

删除文件索引是 Iceberg 中相对复杂的索引机制。它需要在查询规划时为每个数据文件找到所有关联的删除文件。通过分区匹配、路径匹配、序列号过滤和值域范围检查的多重优化，`DeleteFileIndex` 能够高效地缩小每个数据文件需要应用的删除集合。

---

## 8. Multi-dimensional 索引：Z-Order / Hilbert Curve

### 8.1 概念

Z-Order（也称 Morton Code）和 Hilbert Curve 是空间填充曲线（Space-Filling Curve），它们将多维空间的点映射到一维值上，使得在多维空间中相近的点在一维排序中也倾向于相近。

Iceberg 通过将数据按 Z-Order 排序后重写文件，**间接**实现了多维索引效果 -- 多个列的 min/max 范围会更紧凑，从而使列级统计信息过滤更加有效。

### 8.2 Z-Order 字节编码

**源码位置**: `core/src/main/java/org/apache/iceberg/util/ZOrderByteUtils.java`

Z-Order 的核心是将各列的值转换为可比较的字节表示，然后按位交错（bit interleave）。

关键转换方法：

```java
// 整数类型：翻转符号位使负数排在正数前面
public static ByteBuffer wholeNumberOrderedBytes(long val, ByteBuffer reuse) {
  ByteBuffer bytes = ByteBuffers.reuse(reuse, PRIMITIVE_BUFFER_SIZE);
  bytes.putLong(val ^ 0x8000000000000000L);
  return bytes;
}
```

IEEE 754 浮点数也有类似的转换，确保字节序与数值序一致。

### 8.3 Spark Z-Order 重写实现

**源码位置**: `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/actions/SparkZOrderFileRewriteRunner.java`

Z-Order 重写通过 Spark 的 `RewriteDataFiles` Action 触发：

```sql
CALL catalog.system.rewrite_data_files(
  table => 'db.table',
  strategy => 'sort',
  sort_order => 'zorder(col1, col2, col3)'
)
```

关键参数：
- `max-output-size`: 控制 Z-Order 交错输出的最大字节数（默认 `Integer.MAX_VALUE`，即全部交错）
- `var-length-contribution`: 变长类型（String, Binary）取多少字节参与交错（默认 8 字节，与基本类型一致）

### 8.4 Z-Order 如何间接充当索引

Z-Order 排序**不是**一个独立的索引结构。它通过影响数据的物理布局，使得**已有的列统计索引**变得更有效：

1. **排序前**：一个数据文件可能包含 col1 从 1 到 1000、col2 从 A 到 Z 的所有组合，min/max 范围很大
2. **Z-Order 排序后**：相邻文件中 col1 和 col2 的值域都更紧凑
3. **查询 `WHERE col1 = 500 AND col2 = 'M'` 时**：排序后更多文件可以通过 min/max 被跳过

对比：

| 场景 | 排序前 | Z-Order 排序后 |
|------|--------|---------------|
| 单列查询 `WHERE col1 = 500` | 统计索引效果一般 | 统计索引效果较好 |
| 单列查询 `WHERE col2 = 'M'` | 统计索引效果一般 | 统计索引效果较好 |
| 多列查询 `WHERE col1 = 500 AND col2 = 'M'` | 统计索引效果差 | 统计索引效果显著提升 |

### 8.5 Z-Order vs Linear Sort

| 特性 | Linear Sort | Z-Order Sort |
|------|------------|--------------|
| 单列过滤效果 | 排序列极好，其他列差 | 所有参与列都较好 |
| 多列过滤效果 | 差（非排序列无法受益） | 好（所有列均匀受益） |
| 实现复杂度 | 低 | 中等 |
| 适用场景 | 查询主要按单列过滤 | 查询涉及多列联合过滤 |

### 8.6 小结

Z-Order 是一种**数据布局优化**而非传统索引。它通过改善数据文件中列值的聚集性，间接增强了 Iceberg 已有的列统计索引的裁剪效果。它特别适合多维查询（即查询条件涉及多个列）的场景。

---

## 9. 查询规划中的索引使用时机与顺序

### 9.1 完整的 Scan Planning 流程

查询规划的入口在 `ManifestGroup.plan()` 方法中，完整流程如下：

**源码位置**: `core/src/main/java/org/apache/iceberg/ManifestGroup.java`

```
Step 1: ManifestEvaluator 过滤 Manifest 文件
  └── 输入: 所有 Manifest 文件
  └── 使用: PartitionFieldSummary (分区范围摘要)
  └── 输出: 可能包含匹配数据的 Manifest 文件

Step 2: ManifestReader 内部过滤 DataFile 条目
  ├── 2a: Evaluator 分区裁剪
  │   └── 使用: 每个 DataFile 的 partition 值
  ├── 2b: InclusiveMetricsEvaluator 列统计裁剪
  │   └── 使用: DataFile 的 lower_bounds / upper_bounds / null_counts / nan_counts
  └── 2c: PartitionSet 过滤（如果指定）
  └── 输出: 可能包含匹配行的 DataFile 列表

Step 3: DeleteFileIndex 构建删除文件索引
  ├── 3a: ManifestEvaluator 过滤删除 Manifest
  ├── 3b: 加载匹配的 DeleteFile 条目
  └── 3c: 为每个 DataFile 关联适用的 DeleteFile

Step 4: 生成 FileScanTask
  └── 每个 FileScanTask 包含 DataFile + 关联的 DeleteFile[] + ResidualExpression

Step 5: (引擎层) Parquet Row Group 级过滤
  ├── 5a: ParquetMetricsRowGroupFilter (统计信息)
  ├── 5b: ParquetDictionaryRowGroupFilter (字典)
  └── 5c: ParquetBloomRowGroupFilter (Bloom Filter)
```

### 9.2 ManifestGroup 中的关键代码路径

第一层过滤 -- Manifest 级（第 279-309 行）：

```java
private <T> Iterable<CloseableIterable<T>> entries(...) {
  LoadingCache<Integer, ManifestEvaluator> evalCache =
      Caffeine.newBuilder().build(specId -> {
        PartitionSpec spec = specsById.get(specId);
        return ManifestEvaluator.forPartitionFilter(
            Expressions.and(
                partitionFilter,
                Projections.inclusive(spec, caseSensitive).project(dataFilter)),
            spec, caseSensitive);
      });

  CloseableIterable<ManifestFile> matchingManifests =
      CloseableIterable.filter(
          scanMetrics.skippedDataManifests(),
          closeableDataManifests,
          manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));
  // ...
}
```

第二层过滤 -- 在 ManifestReader 内部（通过 `filterRows` 和 `filterPartitions` 传递）：

```java
ManifestReader<DataFile> reader =
    ManifestFiles.read(manifest, io, specsById)
        .filterRows(dataFilter)        // 传递行过滤器
        .filterPartitions(partitionFilter)  // 传递分区过滤器
        .caseSensitive(caseSensitive)
        .select(columns)
        .scanMetrics(scanMetrics);
```

第三层过滤 -- 生成 FileScanTask 时关联删除文件（第 393-405 行）：

```java
private static CloseableIterable<FileScanTask> createFileScanTasks(
    CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext ctx) {
  return CloseableIterable.transform(entries, entry -> {
    DataFile dataFile = ContentFileUtil.copy(entry.file(), ctx.shouldKeepStats(), ctx.columnsToKeepStats());
    DeleteFile[] deleteFiles = ctx.deletes().forEntry(entry);
    return new BaseFileScanTask(
        dataFile, deleteFiles, ctx.schemaAsString(), ctx.specAsString(), ctx.residuals());
  });
}
```

### 9.3 各层过滤的指标度量

Iceberg 通过 `ScanMetrics` 记录各层过滤的效果：

| 指标 | 含义 |
|------|------|
| `scannedDataManifests` | 扫描的数据 Manifest 数量 |
| `skippedDataManifests` | 跳过的数据 Manifest 数量 |
| `scannedDeleteManifests` | 扫描的删除 Manifest 数量 |
| `skippedDeleteManifests` | 跳过的删除 Manifest 数量 |
| `skippedDataFiles` | 跳过的数据文件数量 |
| `skippedDeleteFiles` | 跳过的删除文件数量 |

### 9.4 小结

Iceberg 的索引体系是一个**逐层递进的过滤管线（Filtering Pipeline）**。每一层都尽可能多地排除不相关的数据，使得后续层的工作量最小化。这种设计充分利用了"越早过滤越省钱"的原则。

---

## 10. 查询场景示例：索引过滤效果对比

### 场景设置

假设一张订单表 `orders`：
- 分区方式：`date(order_time)` 按天分区
- 行数：100 亿行
- 数据文件数：10,000 个（每个约 100 万行）
- Manifest 文件数：100 个（每个包含约 100 个数据文件）
- 时间跨度：2023-01-01 至 2024-12-31（730 天）

### 查询 1：分区列等值查询

```sql
SELECT * FROM orders WHERE order_time = '2024-06-15 10:00:00'
```

| 过滤层 | 无索引 | 有索引 |
|--------|--------|--------|
| Manifest 级 | 扫描 100 个 | 扫描 ~1 个（分区摘要排除 99 个） |
| 分区裁剪 | 扫描 10,000 文件 | 扫描 ~14 文件（730 天中的 1 天） |
| 列统计裁剪 | N/A | 可能再排除部分文件 |
| **总计扫描** | **10,000 文件** | **~14 文件** |

### 查询 2：非分区列范围查询

```sql
SELECT * FROM orders WHERE amount > 10000
```

| 过滤层 | 无索引 | 有索引 |
|--------|--------|--------|
| Manifest 级 | 扫描 100 个 | 扫描 100 个（amount 不是分区列） |
| 分区裁剪 | 扫描 10,000 文件 | 扫描 10,000 文件 |
| 列统计裁剪 | 扫描 10,000 文件 | 扫描 ~5,000 文件（假设 50% 文件的 upper_bound > 10000）|
| **总计扫描** | **10,000 文件** | **~5,000 文件** |

### 查询 3：高基数列等值查询 + Bloom Filter

```sql
SELECT * FROM orders WHERE user_id = 'abc-123-def'
```

假设 `user_id` 启用了 Bloom Filter：

| 过滤层 | 无索引 | 有索引（无 Bloom Filter） | 有索引（有 Bloom Filter） |
|--------|--------|--------------------------|--------------------------|
| Manifest 级 | 扫描 100 个 | 扫描 100 个 | 扫描 100 个 |
| 分区裁剪 | 扫描 10,000 文件 | 扫描 10,000 文件 | 扫描 10,000 文件 |
| 列统计裁剪 | 扫描 10,000 文件 | 扫描 ~9,800 文件 | 扫描 ~9,800 文件 |
| Row Group Bloom Filter | N/A | N/A | 扫描 ~50 个 Row Group |
| **总计扫描** | **10,000 文件** | **~9,800 文件** | **实际读取量极小** |

对于高基数列的等值查询，min/max 统计几乎无法过滤（因为 user_id 值随机分布，大多数文件的 [min, max] 范围都会覆盖查询值），但 Bloom Filter 可以在 Row Group 级别做精确过滤。

### 查询 4：多列联合查询 + Z-Order

```sql
SELECT * FROM orders WHERE city = 'Shanghai' AND product_category = 'Electronics'
```

| 过滤层 | 线性排序 | Z-Order 排序 |
|--------|----------|-------------|
| 列统计裁剪（city） | 跳过 ~30% 文件 | 跳过 ~70% 文件 |
| 列统计裁剪（product_category） | 跳过 ~10% 文件 | 跳过 ~60% 文件 |
| 联合效果 | 跳过 ~37% 文件 | 跳过 ~88% 文件 |

Z-Order 排序使得每个文件中 city 和 product_category 的值域更集中，min/max 范围更窄，过滤效果大幅提升。

---

## 11. 与传统数据库索引的对比

### 11.1 设计哲学对比

| 方面 | 传统数据库（B-Tree/Hash Index） | Iceberg 索引体系 |
|------|-------------------------------|-----------------|
| **索引结构** | 独立的数据结构（B+树、哈希表） | 嵌入在元数据中的统计信息 |
| **维护成本** | 每次写入都需更新索引 | 写入时一次性生成，之后不变 |
| **并发控制** | 需要锁或 MVCC 保护索引 | 无锁，索引随元数据一起版本化 |
| **存储开销** | 额外的索引文件，可能占数据大小的 10-30% | 极小，仅为 min/max 等摘要信息 |
| **精确度** | 精确定位到具体行 | 只能定位到文件/Row Group 级别 |
| **查询类型** | 支持精确点查和范围查询 | 主要支持范围裁剪，点查依赖 Bloom Filter |
| **更新影响** | UPDATE 需要更新索引 | Iceberg 不支持原地更新，用 delete + insert 替代 |
| **适用场景** | OLTP，行级操作 | OLAP，大规模扫描 |

### 11.2 核心差异

**B-Tree Index**：
- 是一种指向具体数据行的精确索引
- 支持 O(log n) 的点查和范围查询
- 需要持续维护（插入、删除、分裂/合并节点）
- 在高并发写入场景下会产生索引热点

**Iceberg 的统计索引**：
- 是一种文件级的粗粒度过滤机制
- 只能判断"一定不包含"，不能判断"一定包含"
- 在写入时零成本生成（与数据文件写入同步完成）
- 天然支持并发（不可变元数据 + 乐观并发控制）

### 11.3 为什么 Iceberg 不使用 B-Tree

Iceberg 面向的是数据湖场景，其特点是：
1. **批量写入为主**：每次操作写入大量数据，不适合逐行维护索引
2. **列式存储**：Parquet/ORC 格式天然按列组织，min/max 统计已经内嵌
3. **分布式存储**：数据分布在对象存储（S3/GCS/HDFS）上，维护全局索引代价极高
4. **扫描式查询**：OLAP 查询通常扫描大量数据，文件级裁剪已经足够有效
5. **不可变文件**：Iceberg 的文件一旦写入就不可变，无法在原文件上追加索引

---

## 12. 索引相关的性能调优建议

### 12.1 分区策略优化

- **选择高频查询列作为分区键**：分区裁剪是最有效的过滤手段
- **避免过度分区**：分区数过多导致小文件问题，反而降低性能
- **使用 Hidden Partitioning**：如 `days(ts)` 而非手动 `date_string` 列
- **考虑分区演进**：数据量增长时可从 `month(ts)` 演进为 `day(ts)`

### 12.2 排序策略优化

- **为高频查询列设置 Sort Order**：提升列统计索引的过滤效果
- **多列联合查询场景使用 Z-Order**：均匀提升多列的过滤效果
- **定期运行 `rewrite_data_files`**：保持数据的排序状态

```sql
-- 设置表的默认排序
ALTER TABLE db.table WRITE ORDERED BY city, product_category;

-- 使用 Z-Order 重写
CALL catalog.system.rewrite_data_files(
  table => 'db.table',
  strategy => 'sort',
  sort_order => 'zorder(city, product_category)'
);
```

### 12.3 Bloom Filter 配置

- **为高基数列启用 Bloom Filter**：如 UUID、用户 ID、订单号
- **合理设置 FPP**：默认 0.01（1%），更低的 FPP 需要更多存储空间
- **设置 NDV 提示**：帮助 Bloom Filter 选择最优大小

```sql
ALTER TABLE db.table SET TBLPROPERTIES (
  'write.parquet.bloom-filter-enabled.column.user_id' = 'true',
  'write.parquet.bloom-filter-fpp.column.user_id' = '0.01',
  'write.parquet.bloom-filter-ndv.column.user_id' = '10000000'
);
```

**不适合 Bloom Filter 的场景**：
- 低基数列（min/max 已经足够有效）
- 范围查询为主的列（Bloom Filter 不支持范围）
- 写入频繁但很少查询的列（增加写入开销无收益）

### 12.4 文件大小与 Row Group 大小

- **控制数据文件大小**：推荐 128MB-512MB，太小则过多文件、太大则过滤粒度不足
- **控制 Parquet Row Group 大小**：推荐 128MB，这是 Row Group 级索引（Statistics、Dictionary、Bloom Filter）的粒度

```sql
ALTER TABLE db.table SET TBLPROPERTIES (
  'write.target-file-size-bytes' = '536870912',  -- 512MB
  'write.parquet.row-group-size-bytes' = '134217728'  -- 128MB
);
```

### 12.5 统计信息收集

- **计算 Puffin 统计信息**：帮助查询优化器做出更好的决策

```sql
CALL catalog.system.compute_table_stats(table => 'db.table', columns => ['user_id', 'city']);
```

- **关注 Manifest 合并**：过多的小 Manifest 会增加 Scan Planning 的开销

```sql
-- 合并 Manifest 文件
CALL catalog.system.rewrite_manifests(table => 'db.table');
```

### 12.6 监控索引效果

利用 `ScanMetrics` 监控每次查询的索引过滤效果：

| 指标 | 关注点 |
|------|--------|
| `skippedDataManifests / scannedDataManifests` | Manifest 级过滤率，如果接近 0 说明分区策略可能不合理 |
| `skippedDataFiles` | 文件级过滤数量，越高说明索引越有效 |
| `skippedDeleteManifests` | 删除 Manifest 过滤效果 |

---

## 13. 总结

Apache Iceberg 的索引机制是一套**多层次、自适应、低维护成本**的统计信息过滤体系。与传统数据库的精确索引不同，Iceberg 的索引设计充分考虑了数据湖场景的特点：批量写入、列式存储、分布式部署、不可变文件。

**核心特征总结**：

| 索引层次 | 存储位置 | 构建时机 | 过滤粒度 | 适用查询类型 |
|----------|---------|---------|---------|-------------|
| Manifest 分区摘要 | Manifest List | 写入 Manifest 时 | Manifest 文件级 | 分区列查询 |
| 分区裁剪 | Manifest 条目 | 写入数据文件时 | 数据文件级 | 分区列查询 |
| 列 Min/Max 统计 | Manifest 条目 | 写入数据文件时 | 数据文件级 | 所有列的范围/等值查询 |
| Bloom Filter | Parquet 文件内 | 写入 Parquet 时 | Row Group 级 | 高基数列等值查询 |
| Puffin NDV | Puffin 文件 | 手动触发计算 | 表/列级 | 查询优化器代价估算 |
| 删除文件索引 | 内存（查询规划时构建） | Scan Planning 时 | 数据文件级 | 关联删除文件 |
| Z-Order | 数据文件（布局） | 手动触发重写 | 间接提升 Min/Max 效果 | 多列联合查询 |

**设计哲学**："索引即元数据，过滤即规划"。所有索引信息要么嵌入在不可变的元数据文件中，要么在查询规划时动态构建。这种设计保证了：
- **零维护成本**：索引与数据同生共死，无需单独维护
- **天然并发安全**：不可变文件 + 乐观并发控制
- **水平可扩展**：索引信息分布在各 Manifest 中，天然支持分布式并行评估
- **面向大数据优化**：文件级粗粒度过滤在大规模数据集上的性价比远高于行级精确索引

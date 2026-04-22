# Apache Iceberg Flink HASH 分布模式深度解析

## 1. 概述

在 Apache Iceberg 的 Flink 集成中，当 `DistributionMode` 设置为 `HASH` 时，系统通过 Flink 的 `keyBy` 机制对数据进行哈希分布，确保相同分区的数据被发送到同一个 Flink subtask 中处理。

**与 Spark 的核心区别**：
- **Spark**：使用 `ClusteredDistribution` + `HashPartitioning` 进行 shuffle
- **Flink**：使用 `KeySelector` + `keyBy()` 进行数据流重分区
- **RANGE 模式**：Flink 支持 RANGE 模式，使用 `RangePartitioner` + `DataStatisticsOperator` 实现

---

## 2. Flink HASH 分布的完整流程

### 2.1 触发流程

```
用户设置 write.distribution-mode=hash
    ↓
FlinkSink.Builder.distributionMode(DistributionMode.HASH)
    ↓
FlinkSink.distributeDataStream() 判断分布模式
    ↓
input.keyBy(new PartitionKeySelector(spec, schema, flinkRowType))
    ↓
Flink 根据 KeySelector 返回的 key 进行 hash 分区
    ↓
相同 key 的数据发送到同一个 subtask
    ↓
IcebergStreamWriter 写入文件
```

### 2.2 核心代码分析

**配置读取** (`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/FlinkWriteConf.java:163-173`):

```java
public DistributionMode distributionMode() {
  String modeName =
      confParser
          .stringConf()
          .option(FlinkWriteOptions.DISTRIBUTION_MODE.key())  // 写入选项
          .flinkConfig(FlinkWriteOptions.DISTRIBUTION_MODE)    // Flink配置
          .tableProperty(TableProperties.WRITE_DISTRIBUTION_MODE)  // 表属性
          .defaultValue(TableProperties.WRITE_DISTRIBUTION_MODE_NONE)  // 默认值：NONE
          .parse();
  return DistributionMode.fromName(modeName);
}
```

**分布策略选择** (`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/FlinkSink.java:607-720`):

```java
private DataStream<RowData> distributeDataStream(
    DataStream<RowData> input,
    Set<Integer> equalityFieldIds,
    RowType flinkRowType,
    int writerParallelism) {
  DistributionMode writeMode = flinkWriteConf.distributionMode();
  LOG.info("Write distribution mode is '{}'", writeMode.modeName());

  Schema iSchema = table.schema();
  PartitionSpec partitionSpec = table.spec();
  SortOrder sortOrder = table.sortOrder();

  switch (writeMode) {
    case NONE:
      if (equalityFieldIds.isEmpty()) {
        return input;  // 不进行重分区
      } else {
        LOG.info("Distribute rows by equality fields, because there are equality fields set");
        return input.keyBy(
            new EqualityFieldKeySelector(iSchema, flinkRowType, equalityFieldIds));
      }

    case HASH:
      if (equalityFieldIds.isEmpty()) {
        if (partitionSpec.isUnpartitioned()) {
          // 未分区表 + 无 equality fields -> 退化为 NONE 模式
          LOG.warn(
              "Fallback to use 'none' distribution mode, because there are no equality fields set "
                  + "and table is unpartitioned");
          return input;
        } else {
          // 按分区键进行 keyBy
          return input.keyBy(new PartitionKeySelector(partitionSpec, iSchema, flinkRowType));
        }
      } else {
        if (partitionSpec.isUnpartitioned()) {
          // 未分区表但有 equality fields -> 按 equality fields 分区
          LOG.info(
              "Distribute rows by equality fields, because there are equality fields set "
                  + "and table is unpartitioned");
          return input.keyBy(
              new EqualityFieldKeySelector(iSchema, flinkRowType, equalityFieldIds));
        } else {
          // 有分区且有 equality fields -> 确保分区字段是 equality fields 的子集
          for (PartitionField partitionField : partitionSpec.fields()) {
            Preconditions.checkState(
                equalityFieldIds.contains(partitionField.sourceId()),
                "In 'hash' distribution mode with equality fields set, source column '%s' of partition field '%s' "
                    + "should be included in equality fields: '%s'",
                table.schema().findColumnName(partitionField.sourceId()),
                partitionField,
                equalityFieldColumns);
          }
          return input.keyBy(new PartitionKeySelector(partitionSpec, iSchema, flinkRowType));
        }
      }

    case RANGE:
      // Flink 支持 RANGE 模式，但对于有 equality fields 的场景会退化为 HASH
      if (!equalityFieldIds.isEmpty()) {
        LOG.warn(
            "Hash distribute rows by equality fields, even though {}=range is set. "
                + "Range distribution for primary keys are not always safe in "
                + "Flink streaming writer.",
            WRITE_DISTRIBUTION_MODE);
        return input.keyBy(
            new EqualityFieldKeySelector(iSchema, flinkRowType, equalityFieldIds));
      }

      // 按 sort order 或 partition spec 进行 range 分布
      Preconditions.checkState(
          sortOrder.isSorted() || partitionSpec.isPartitioned(),
          "Invalid write distribution mode: range. Need to define sort order or partition spec.");
      if (sortOrder.isUnsorted()) {
        sortOrder = Partitioning.sortOrderFor(partitionSpec);
        LOG.info("Construct sort order from partition spec");
      }

      LOG.info("Range distribute rows by sort order: {}", sortOrder);
      // 使用 DataStatisticsOperator 收集统计信息
      // 使用 RangePartitioner 进行范围分区
      // ... 省略详细实现
  }
}
```

**关键决策逻辑**：
1. **HASH + 无 equality fields + 有分区** → 按分区键 keyBy（PartitionKeySelector）
2. **HASH + 有 equality fields + 有分区** → 按分区键 keyBy（要求分区字段 ⊆ equality fields）
3. **HASH + 无 equality fields + 无分区** → 不分区（退化为 NONE）
4. **HASH + 有 equality fields + 无分区** → 按 equality fields keyBy（EqualityFieldKeySelector）
5. **RANGE + 无 equality fields** → 使用 RangePartitioner 进行范围分区
6. **RANGE + 有 equality fields** → 退化为按 equality fields keyBy（安全考虑）

---

## 3. PartitionKeySelector 实现机制

### 3.1 核心实现

**PartitionKeySelector 类** (`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/PartitionKeySelector.java:36-66`):

```java
public class PartitionKeySelector implements KeySelector<RowData, String> {

  private final Schema schema;
  private final PartitionKey partitionKey;
  private final RowType flinkSchema;

  private transient RowDataWrapper rowDataWrapper;

  public PartitionKeySelector(PartitionSpec spec, Schema schema, RowType flinkSchema) {
    this.schema = schema;
    this.partitionKey = new PartitionKey(spec, schema);  // 创建分区键计算器
    this.flinkSchema = flinkSchema;
  }

  /**
   * Construct the RowDataWrapper lazily here because few members in it are not
   * serializable. In this way, we don't have to serialize them with forcing.
   */
  private RowDataWrapper lazyRowDataWrapper() {
    if (rowDataWrapper == null) {
      rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    }
    return rowDataWrapper;
  }

  @Override
  public String getKey(RowData row) {
    // 1. 将 Flink RowData 包装为 Iceberg StructLike
    // 2. 计算分区键值
    partitionKey.partition(lazyRowDataWrapper().wrap(row));

    // 3. 将分区键转换为字符串路径作为 Flink 的 key
    return partitionKey.toPath();
  }
}
```

**EqualityFieldKeySelector 类** (`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/EqualityFieldKeySelector.java:37-88`):

```java
public class EqualityFieldKeySelector implements KeySelector<RowData, Integer> {

  private final Schema schema;
  private final RowType flinkSchema;
  private final Schema deleteSchema;

  private transient RowDataWrapper rowDataWrapper;
  private transient StructProjection structProjection;
  private transient StructLikeWrapper structLikeWrapper;

  public EqualityFieldKeySelector(
      Schema schema, RowType flinkSchema, Set<Integer> equalityFieldIds) {
    this.schema = schema;
    this.flinkSchema = flinkSchema;
    this.deleteSchema = TypeUtil.select(schema, equalityFieldIds);
  }

  @Override
  public Integer getKey(RowData row) {
    RowDataWrapper wrappedRowData = lazyRowDataWrapper().wrap(row);
    StructProjection projectedRowData = lazyStructProjection().wrap(wrappedRowData);
    StructLikeWrapper wrapper = lazyStructLikeWrapper().set(projectedRowData);
    return wrapper.hashCode();  // 返回 equality fields 的 hashCode
  }
}
```

**关键区别**：
- **PartitionKeySelector**：返回 `String`（分区路径，如 `"year=2024/region=US"`）
- **EqualityFieldKeySelector**：返回 `Integer`（equality fields 的 hashCode）

### 3.2 工作流程详解

**步骤 1：数据包装**
```java
RowDataWrapper wrappedRow = lazyRowDataWrapper().wrap(row);
```
- 将 Flink 的 `RowData` 适配为 Iceberg 的 `StructLike` 接口
- 提供统一的字段访问方式

**步骤 2：计算分区键**
```java
partitionKey.partition(wrappedRow);
```
- 调用 `PartitionKey.partition()` → `StructTransform.wrap()`
- 根据 `PartitionSpec` 定义的 transform（identity, bucket, day 等）计算分区值
- 将结果存储在 `PartitionKey` 内部的 `transformedTuple` 数组中

**步骤 3：生成分区路径字符串**
```java
return partitionKey.toPath();
```

**PartitionKey.toPath() 实现** (`api/src/main/java/org/apache/iceberg/PartitionKey.java:53-55`):
```java
public String toPath() {
  return spec.partitionToPath(this);
}
```

**PartitionSpec.partitionToPath() 示例输出**：
```java
// 分区定义：PARTITIONED BY (year(ts), region, bucket(10, user_id))
// 数据：ts='2024-01-15 10:30:00', region='US', user_id=12345

// 分区值：year=2024, region='US', bucket=5
// toPath() 返回：'year=2024/region=US/user_id_bucket=5'
```

---

## 4. Flink KeyBy 的 Hash 机制

### 4.1 Flink 如何对 String Key 进行 Hash

当调用 `input.keyBy(new PartitionKeySelector(...))` 时：

1. **KeySelector 提取 key**：
   ```java
   String key = partitionKeySelector.getKey(row);  // 如 "year=2024/region=US/user_id_bucket=5"
   ```

2. **Flink 计算 hash 值**：
   ```java
   int hash = key.hashCode();  // Java String.hashCode()
   ```

3. **确定目标 subtask**：
   ```java
   int subtaskIndex = KeyGroupRangeAssignment.assignKeyToParallelOperator(
       key, maxParallelism, numSubtasks);
   ```

**String.hashCode() 算法**：
```java
public int hashCode() {
    int h = hash;
    if (h == 0 && value.length > 0) {
        char val[] = value;

        for (int i = 0; i < value.length; i++) {
            h = 31 * h + val[i];  // 多项式滚动哈希
        }
        hash = h;
    }
    return h;
}
```

### 4.2 为什么使用字符串作为 Key？

**优点**：
1. **人类可读**：分区路径字符串易于理解和调试（如 `year=2024/region=US`）
2. **避免序列化问题**：`PartitionKey` 对象包含复杂的 Transform 逻辑，不易序列化
3. **唯一性保证**：相同的分区值总是生成相同的路径字符串

**缺点**：
1. **Hash 碰撞**：String.hashCode() 可能产生碰撞
2. **性能开销**：字符串拼接和 hash 计算的开销

**对比 Spark**：
- Spark 直接使用 `PartitionData.hashCode()`（Murmur3 算法）
- Flink 使用 `String.hashCode()`（多项式滚动哈希）

---

## 5. BucketPartitioner 的特殊优化

### 5.1 适用场景

当表的分区定义**仅包含一个 bucket transform** 时，Flink 可以使用 `BucketPartitioner` 进行优化。

**重要说明**：`BucketPartitioner` 不会自动启用，需要手动通过 `partitionCustom()` 配合 `BucketPartitionKeySelector` 使用。

```sql
-- 适合 BucketPartitioner
CREATE TABLE users (
  user_id BIGINT,
  name STRING
) PARTITIONED BY (bucket(100, user_id));

-- 不适合 BucketPartitioner（多个分区字段）
CREATE TABLE sales (
  amount DECIMAL,
  sale_date DATE,
  region STRING
) PARTITIONED BY (days(sale_date), bucket(10, region));
```

### 5.2 手动启用 BucketPartitioner

**示例代码** (参考 `TestBucketPartitionerFlinkIcebergSink.java`):

```java
DataStream<RowData> dataStream = env.addSource(...)
    .partitionCustom(
        new BucketPartitioner(table.spec()),
        new BucketPartitionKeySelector(
            table.spec(),
            table.schema(),
            FlinkSink.toFlinkRowType(table.schema(), flinkSchema)));

FlinkSink.forRowData(dataStream)
    .table(table)
    .tableLoader(tableLoader)
    .writeParallelism(parallelism)
    .distributionMode(DistributionMode.NONE)  // 使用 NONE，因为已经手动分区
    .append();
```

**关键点**：
1. 使用 `partitionCustom()` 手动指定分区器
2. `BucketPartitionKeySelector` 提取 bucket ID（返回 `Integer`）
3. `BucketPartitioner` 根据 bucket ID 分配到对应的 writer
4. 设置 `distributionMode(DistributionMode.NONE)` 避免重复分区

### 5.3 BucketPartitioner 实现

**核心代码** (`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/BucketPartitioner.java:31-103`):

```java
class BucketPartitioner implements Partitioner<Integer> {

  private final int maxNumBuckets;
  private final int[] currentBucketWriterOffset;  // 每个 bucket 的下一个 writer offset

  BucketPartitioner(PartitionSpec partitionSpec) {
    this.maxNumBuckets = BucketPartitionerUtil.getMaxNumBuckets(partitionSpec);
    this.currentBucketWriterOffset = new int[maxNumBuckets];
  }

  @Override
  public int partition(Integer bucketId, int numPartitions) {
    // bucketId: 数据所属的 bucket 编号 (0 ~ maxNumBuckets-1)
    // numPartitions: Flink writer 的并行度

    if (numPartitions <= maxNumBuckets) {
      // 场景1：writer 数量 <= bucket 数量
      // 每个 writer 负责多个 bucket
      return bucketId % numPartitions;
    } else {
      // 场景2：writer 数量 > bucket 数量
      // 多个 writer 负责一个 bucket，使用 round-robin 分配
      return getPartitionWithMoreWritersThanBuckets(bucketId, numPartitions);
    }
  }

  /**
   * 当 writer 数量 > bucket 数量时，使用 round-robin 策略。
   *
   * 示例：numPartitions=5, maxBuckets=2
   * - Bucket 0: Writers 0, 2, 4 (round-robin)
   * - Bucket 1: Writers 1, 3 (round-robin)
   */
  private int getPartitionWithMoreWritersThanBuckets(int bucketId, int numPartitions) {
    int currentOffset = currentBucketWriterOffset[bucketId];

    // 判断这个 bucket 是否需要额外的 writer
    int extraWriter = bucketId < (numPartitions % maxNumBuckets) ? 1 : 0;
    int maxNumWritersPerBucket = (numPartitions / maxNumBuckets) + extraWriter;

    // Round-robin 切换到下一个 writer
    int nextOffset = currentOffset == maxNumWritersPerBucket - 1 ? 0 : currentOffset + 1;
    currentBucketWriterOffset[bucketId] = nextOffset;

    return bucketId + (maxNumBuckets * currentOffset);
  }
}
```

### 5.4 BucketPartitioner 的优势

**传统 PartitionKeySelector 的问题**：
```java
// 假设：bucket(100, user_id), 10 个 writer
// bucketId=5 的数据 -> key="user_id_bucket=5"
// String.hashCode("user_id_bucket=5") % 10 = 可能是任意 writer

// 问题：同一个 bucket 的数据可能分散到多个 writer，产生多个小文件
```

**BucketPartitioner 的优化**：
```java
// bucketId=5, numPartitions=10
// partition(5, 10) = 5 % 10 = 5

// 保证：bucketId=5 的所有数据都发送到 writer 5
// 结果：每个 bucket 只产生一个文件
```

**性能对比**：
| 方案 | 文件数量 | Hash 计算 | 文件大小 |
|------|---------|----------|---------|
| PartitionKeySelector | bucket数 × writer数 | String.hashCode() | 较小，分散 |
| BucketPartitioner | bucket数（最优） | 整数取模 | 较大，聚合 |

---

## 6. 不同场景下的处理策略

### 6.1 场景矩阵

| 场景 | 分区类型 | Equality Fields | 分布模式 | KeySelector | 返回类型 |
|------|---------|----------------|---------|-------------|---------|
| 普通写入 | 有分区 | 无 | HASH | PartitionKeySelector | String |
| 普通写入 | 无分区 | 无 | NONE | 不重分区 | - |
| UPSERT | 有分区 | 有 | HASH | PartitionKeySelector | String |
| UPSERT | 无分区 | 有 | HASH | EqualityFieldKeySelector | Integer |
| CDC | 有分区 | 有 | HASH | PartitionKeySelector | String |
| CDC | 无分区 | 有 | HASH | EqualityFieldKeySelector | Integer |
| NONE 模式 | 有分区 | 有 | NONE | EqualityFieldKeySelector | Integer |
| RANGE 模式 | 有分区 | 无 | RANGE | RangePartitioner | - |

**注意**：
- HASH 模式下，有分区且有 equality fields 时，要求分区字段必须是 equality fields 的子集
- RANGE 模式下，如果有 equality fields，会退化为使用 EqualityFieldKeySelector

### 6.2 UPSERT 模式的特殊处理

**UPSERT 约束** (`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/FlinkSink.java:561-578`):
```java
if (flinkWriteConf.upsertMode()) {
  Preconditions.checkState(
      !flinkWriteConf.overwriteMode(),
      "OVERWRITE mode shouldn't be enable when configuring to use UPSERT data stream.");
  Preconditions.checkState(
      !equalityFieldIds.isEmpty(),
      "Equality field columns shouldn't be empty when configuring to use UPSERT data stream.");
  if (!table.spec().isUnpartitioned()) {
    for (PartitionField partitionField : table.spec().fields()) {
      Preconditions.checkState(
          equalityFieldIds.contains(partitionField.sourceId()),
          "In UPSERT mode, source column '%s' of partition field '%s', should be included in equality fields: '%s'",
          table.schema().findColumnName(partitionField.sourceId()),
          partitionField,
          equalityFieldColumns);
    }
  }
}
```

**为什么要求分区字段 ⊆ equality fields？**

假设违反约束：
```sql
-- 表定义
CREATE TABLE orders (
  order_id BIGINT,
  user_id BIGINT,
  amount DECIMAL,
  order_date DATE
) PARTITIONED BY (days(order_date));

-- 错误配置：equality fields = [order_id]
-- 问题：order_id=123 在 2024-01-01 的记录无法删除 2024-01-02 的旧记录
-- 原因：它们在不同分区，不会被发送到同一个 writer
```

正确配置：
```sql
-- equality fields = [order_id, order_date]
-- 保证：相同 order_id + order_date 的记录在同一分区，可以正确 UPSERT
```

### 6.3 CDC 场景示例

```java
// CDC 数据流
DataStream<RowData> cdcStream = ...;

// 表定义：PARTITIONED BY (days(event_time), region)
// Equality fields: [id, event_time, region]

// Flink 处理：
// 1. 按 PartitionKeySelector keyBy -> 相同 (event_time, region) 发到同一 writer
// 2. Writer 内部按 equality fields [id] 进行 UPSERT
// 3. 保证相同 ID 在同一分区内的记录被正确更新
```

---

## 7. Flink vs Spark 实现对比

### 7.1 核心差异

| 特性 | Flink | Spark |
|------|-------|-------|
| **分布机制** | KeySelector + keyBy() | ClusteredDistribution + HashPartitioning |
| **Key 类型** | String (partition path) | Transform[] (Spark expressions) |
| **Hash 算法** | String.hashCode() | PartitionData.hashCode() (Murmur3) |
| **执行引擎** | 流式 DAG | 批处理 Stage |
| **状态管理** | Stateless KeySelector | Stateless Transform |
| **Bucket 优化** | BucketPartitioner | 无特殊优化 |

### 7.2 Hash 算法对比

**Flink (String.hashCode())**:
```java
// Input: "year=2024/region=US/user_id_bucket=5"
int hash = 0;
for (char c : key.toCharArray()) {
    hash = 31 * hash + c;
}
return hash;
```
- 优点：简单，JVM 优化好
- 缺点：碰撞率较高，分布不够均匀

**Spark (PartitionData.hashCode() with Murmur3)**:
```java
Hasher hasher = Hashing.goodFastHash(32).newHasher();
Stream.of(data).map(Objects::hashCode).forEach(hasher::putInt);
partitionType.fields().stream().map(Objects::hashCode).forEach(hasher::putInt);
return hasher.hash().hashCode();
```
- 优点：均匀分布，碰撞率低
- 缺点：计算开销稍高

### 7.3 数据流向对比

**Flink**:
```
Source → Map → KeyBy (PartitionKeySelector) → IcebergStreamWriter → Committer
                   ↑
                   └─ 按 partition path 的 hashCode 重分区
```

**Spark**:
```
Source → Map → Shuffle (HashPartitioning) → PartitionedDataWriter → Commit
                   ↑
                   └─ 按 PartitionData.hashCode() (Murmur3) 重分区
```

---

## 8. 性能优化建议

### 8.1 选择合适的分布模式

**推荐配置**：
```java
// 场景1：分区表，数据均匀分布
FlinkSink.forRowData(input)
    .table(table)
    .tableLoader(tableLoader)
    .distributionMode(DistributionMode.HASH)  // 推荐
    .append();

// 场景2：未分区表，无 UPSERT
FlinkSink.forRowData(input)
    .distributionMode(DistributionMode.NONE)  // 推荐，避免不必要的重分区
    .append();

// 场景3：UPSERT 模式
FlinkSink.forRowData(input)
    .distributionMode(DistributionMode.HASH)
    .equalityFieldColumns(Arrays.asList("id", "partition_field"))  // 必须包含分区字段
    .upsert(true)
    .append();
```

### 8.2 Writer 并行度调优

**原则**：
- **分区数较少** (< 100)：`writeParallelism` = 分区数 × 2
- **分区数适中** (100-1000)：`writeParallelism` = 分区数
- **分区数很多** (> 1000)：`writeParallelism` = 200-500（避免过多小文件）

**示例**：
```java
int estimatedPartitions = 500;  // 预估的活跃分区数

FlinkSink.forRowData(input)
    .writeParallelism(Math.min(estimatedPartitions, 500))
    .distributionMode(DistributionMode.HASH)
    .append();
```

### 8.3 Bucket 分区优化（手动启用）

**当使用 bucket 分区时，可以手动启用 BucketPartitioner**：
```java
// 表定义（仅使用 bucket 分区）
CREATE TABLE user_events (
  user_id BIGINT,
  event_type STRING,
  event_time TIMESTAMP
) PARTITIONED BY (bucket(100, user_id));

// Flink 写入（手动使用 BucketPartitioner）
DataStream<RowData> eventStream = ...;

DataStream<RowData> partitionedStream = eventStream
    .partitionCustom(
        new BucketPartitioner(table.spec()),
        new BucketPartitionKeySelector(table.spec(), table.schema(), flinkRowType));

FlinkSink.forRowData(partitionedStream)
    .table(table)
    .tableLoader(tableLoader)
    .distributionMode(DistributionMode.NONE)  // 已手动分区，使用 NONE
    .writeParallelism(50)
    .append();
```

**BucketPartitioner 启用条件**：
1. PartitionSpec 只包含一个字段
2. 该字段是 `BucketTransform`
3. 手动使用 `partitionCustom()` 配合 `BucketPartitionKeySelector`

**优化效果**：
- 传统 PartitionKeySelector：100 buckets × 50 writers = 最多 5000 个文件
- BucketPartitioner：100 buckets = 最多 100 个文件（每个 bucket 最多 1 个文件）

### 8.4 避免的反模式

❌ **反模式1：未分区表使用 HASH 模式**
```java
// 错误：未分区表 + HASH 模式 = 无效的重分区
FlinkSink.forRowData(input)
    .distributionMode(DistributionMode.HASH)  // 会退化为 NONE
    .append();
```

✅ **正确做法**：
```java
FlinkSink.forRowData(input)
    .distributionMode(DistributionMode.NONE)  // 明确使用 NONE
    .append();
```

❌ **反模式2：UPSERT 模式下分区字段不在 equality fields 中**
```java
// 错误：会导致跨分区的 UPSERT 失败
PARTITIONED BY (days(event_time))
equalityFieldColumns = ["id"]  // 缺少 event_time

// 结果：旧数据无法被删除
```

✅ **正确做法**：
```java
equalityFieldColumns = ["id", "event_time"]  // 包含分区字段
```

---

## 9. 实战示例

### 示例 1：实时日志写入（按日期+地区分区）

```java
// 表定义
CREATE TABLE access_logs (
  log_id BIGINT,
  user_id BIGINT,
  url STRING,
  log_time TIMESTAMP,
  region STRING
) PARTITIONED BY (days(log_time), region);

// Flink 写入
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<RowData> logStream = ...;  // 原始日志流

FlinkSink.forRowData(logStream)
    .table(table)
    .tableLoader(tableLoader)
    .distributionMode(DistributionMode.HASH)  // 按 (day, region) keyBy
    .writeParallelism(50)  // 假设有 50 个活跃分区
    .set("write.target-file-size-bytes", "134217728")  // 128MB
    .append();

env.execute("Access Log Iceberg Sink");
```

**执行流程**：
1. `PartitionKeySelector.getKey(row)` 计算：
   - `log_time='2024-01-15 10:30:00'` → `day=19737`
   - `region='US'`
   - key = `"log_time_day=19737/region=US"`

2. Flink keyBy 根据 `key.hashCode()` 分配到 subtask

3. 相同 (day, region) 的数据发到同一个 writer，写入同一个文件

### 示例 2：CDC UPSERT 场景（MySQL Binlog）

```java
// 表定义
CREATE TABLE user_profiles (
  user_id BIGINT,
  name STRING,
  email STRING,
  updated_at TIMESTAMP,
  region STRING
) PARTITIONED BY (region);

// Flink CDC
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<RowData> mysqlCDC = MySqlSource.<RowData>builder()
    .hostname("localhost")
    .databaseList("mydb")
    .tableList("mydb.user_profiles")
    .build();

FlinkSink.forRowData(mysqlCDC)
    .table(table)
    .tableLoader(tableLoader)
    .distributionMode(DistributionMode.HASH)  // 按 region keyBy
    .equalityFieldColumns(Arrays.asList("user_id", "region"))  // 包含分区字段
    .upsert(true)  // 启用 UPSERT 模式
    .writeParallelism(10)
    .append();

env.execute("MySQL CDC to Iceberg");
```

**关键点**：
- `equalityFieldColumns` 必须包含 `region`（分区字段）
- 相同 `(user_id, region)` 的 INSERT/UPDATE/DELETE 事件发到同一 writer
- Writer 内部维护 `RowDataDeltaWriter` 处理 UPSERT 逻辑

### 示例 3：手动启用 Bucket 分区优化

```java
// 表定义（仅使用 bucket 分区）
CREATE TABLE user_events (
  user_id BIGINT,
  event_type STRING,
  event_time TIMESTAMP
) PARTITIONED BY (bucket(100, user_id));

// Flink 写入（手动使用 BucketPartitioner）
DataStream<RowData> eventStream = ...;

// 步骤1：手动应用 BucketPartitioner
DataStream<RowData> partitionedStream = eventStream
    .partitionCustom(
        new BucketPartitioner(table.spec()),
        new BucketPartitionKeySelector(
            table.spec(),
            table.schema(),
            FlinkSink.toFlinkRowType(table.schema(), flinkSchema)));

// 步骤2：使用 NONE 模式避免重复分区
FlinkSink.forRowData(partitionedStream)
    .table(table)
    .tableLoader(tableLoader)
    .distributionMode(DistributionMode.NONE)  // 重要：已手动分区
    .writeParallelism(50)  // writer 数量 < bucket 数量，每个 writer 负责 2 个 bucket
    .append();
```

**工作原理**：
1. `BucketPartitionKeySelector.getKey(row)` 提取 bucket ID（Integer）
2. `BucketPartitioner.partition(bucketId, numPartitions)` 计算目标 writer
3. 相同 bucket ID 的数据发到同一个 writer

**优化效果**：
- 不使用 BucketPartitioner：可能产生 100 buckets × 50 writers = 5000 个文件
- 使用 BucketPartitioner：最多 100 个文件（每个 bucket 一个文件）

---

## 10. 总结

### Flink HASH 分布的核心机制

1. **KeySelector 提取分区路径**：
   - `PartitionKeySelector.getKey()` → 计算 `PartitionKey` → 转换为字符串路径

2. **Flink keyBy 重分区**：
   - 使用 `String.hashCode()` 计算 hash 值
   - 根据 hash 值分配到对应的 subtask

3. **Writer 写入文件**：
   - 相同 key 的数据发到同一个 `IcebergStreamWriter`
   - 聚合写入，减少小文件

4. **特殊优化**：
   - **BucketPartitioner**：针对单一 bucket 分区的优化
   - **EqualityFieldKeySelector**：UPSERT 场景的特殊处理

### 适用场景

✅ **推荐使用 HASH 模式**：
- 分区表且数据均匀分布
- UPSERT/CDC 场景（配合 equality fields）
- 需要控制每个分区的文件数量

❌ **不推荐使用 HASH 模式**：
- 未分区表（会自动退化为 NONE）
- 数据严重倾斜（某些分区数据量特别大）
- 实时性要求极高且数据量小（重分区开销大于收益）

### 与 Spark 的对比

| 维度 | Flink | Spark |
|------|-------|-------|
| **适用场景** | 流式实时写入 | 批处理/微批写入 |
| **分布机制** | KeySelector + keyBy | ClusteredDistribution |
| **Hash 算法** | String.hashCode() | Murmur3 |
| **特殊优化** | BucketPartitioner | 无 |
| **UPSERT 支持** | 原生支持 | 需要 MERGE INTO |

---

## 11. 参考资料

- **源码位置**（基于 Flink v1.20 集成）：
  - `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/FlinkWriteConf.java`
  - `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/FlinkSink.java`
  - `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/PartitionKeySelector.java`
  - `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/BucketPartitioner.java`
  - `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/BucketPartitionKeySelector.java`
  - `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/BucketPartitionerUtil.java`
  - `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/EqualityFieldKeySelector.java`
  - `api/src/main/java/org/apache/iceberg/PartitionKey.java`

- **测试代码参考**：
  - `flink/v1.20/flink/src/test/java/org/apache/iceberg/flink/sink/TestBucketPartitionerFlinkIcebergSink.java`

- **相关配置**：
  - `write.distribution-mode`: 分布模式（none/hash/range）
  - `write-parallelism`: Writer 并行度
  - `write.target-file-size-bytes`: 目标文件大小
  - `write.upsert.enabled`: 是否启用 UPSERT 模式

---

## 12. 技术验证修正记录

**验证日期**: 2026-04-20

**验证范围**: 对照 Iceberg 源码（基于 v1.20 Flink 集成）验证文档技术准确性

**发现的错误及修正**：

1. **源码版本路径错误**
   - 原文档引用: `flink/v1.18/flink/src/main/java/...`
   - 实际路径: `flink/v1.20/flink/src/main/java/...`
   - 说明: 当前代码库中 v1.18 目录下无源码文件，实际源码在 v1.20 版本

2. **行号引用更新**
   - `FlinkWriteConf.distributionMode()`: 163-173 行（原文档: 158-168）
   - `FlinkSink.distributeDataStream()`: 607-720 行（原文档: 501-569）
   - `PartitionKeySelector` 类: 36-66 行（原文档: 34-64）
   - `FlinkSink UPSERT 约束`: 561-578 行（原文档: 459-468）

3. **PartitionKeySelector 类访问修饰符错误**
   - 原文档: `class PartitionKeySelector`（包私有）
   - 实际代码: `public class PartitionKeySelector`（公开类）
   - 修正: 已更新为 `public class`

4. **注释内容不准确**
   - 原文档: `"Construct the RowDataWrapper lazily to avoid serialization issues."`
   - 实际代码: `"Construct the RowDataWrapper lazily here because few members in it are not serializable. In this way, we don't have to serialize them with forcing."`
   - 修正: 已更新为实际注释内容

5. **distributeDataStream 方法签名错误**
   - 原文档参数: `List<Integer> equalityFieldIds, PartitionSpec partitionSpec, Schema iSchema, RowType flinkRowType`
   - 实际参数: `Set<Integer> equalityFieldIds, RowType flinkRowType, int writerParallelism`
   - 说明: 方法内部从 `table.schema()` 和 `table.spec()` 获取 schema 和 partitionSpec
   - 修正: 已更新方法签名和实现逻辑

6. **RANGE 模式支持错误（重要）**
   - 原文档: "Flink 不支持 RANGE 模式，退化处理"
   - 实际情况: **Flink 完全支持 RANGE 模式**，使用 `RangePartitioner` + `DataStatisticsOperator` 实现
   - 退化场景: 仅在有 equality fields 时退化为 HASH 模式（安全考虑）
   - 修正: 已更新说明 Flink 支持 RANGE 模式，并补充退化条件

7. **EqualityFieldKeySelector 返回类型遗漏**
   - 原文档: 未明确说明 `EqualityFieldKeySelector` 的返回类型
   - 实际代码: `implements KeySelector<RowData, Integer>`（返回 `Integer` hashCode）
   - 对比: `PartitionKeySelector` 返回 `String`（分区路径）
   - 修正: 已补充 `EqualityFieldKeySelector` 完整实现和类型对比

8. **UPSERT 约束代码不完整**
   - 原文档: 缺少 `overwriteMode` 检查
   - 实际代码: 包含 `!flinkWriteConf.overwriteMode()` 前置检查
   - 错误消息: 实际代码使用 `findColumnName()` 获取列名，更详细
   - 修正: 已更新为完整的 UPSERT 约束代码

9. **日志消息不准确**
   - 原文档 HASH 模式未分区表日志: 简化版本
   - 实际代码: 包含更详细的日志消息（如 "Distribute rows by equality fields, because..."）
   - 修正: 已更新为实际日志消息

10. **决策逻辑补充**
    - 原文档: 仅列出 4 种 HASH 模式场景
    - 实际代码: 还包含 RANGE 模式的 2 种场景
    - 修正: 已补充 RANGE 模式决策逻辑

11. **BucketPartitioner 自动启用错误（重要）**
    - 原文档: "BucketPartitioner 自动启用条件"，暗示会自动使用
    - 实际情况: **BucketPartitioner 需要手动启用**，通过 `partitionCustom()` + `BucketPartitionKeySelector`
    - 证据: `TestBucketPartitionerFlinkIcebergSink.java` 测试代码显示手动调用
    - 使用方式: 
      ```java
      dataStream.partitionCustom(
          new BucketPartitioner(table.spec()),
          new BucketPartitionKeySelector(table.spec(), schema, flinkRowType))
      ```
    - 配合设置: 使用 `distributionMode(DistributionMode.NONE)` 避免重复分区
    - 修正: 已更新为手动启用方式，补充完整示例代码

12. **BucketPartitionKeySelector 遗漏**
    - 原文档: 未提及 `BucketPartitionKeySelector` 类
    - 实际代码: `BucketPartitionKeySelector` 是使用 BucketPartitioner 的必要组件
    - 功能: 从 RowData 中提取 bucket ID（返回 `Integer`）
    - 修正: 已补充 `BucketPartitionKeySelector` 的说明和使用方式

**验证结论**：
- 文档核心技术原理正确（HASH 分布机制、PartitionKeySelector 工作流程）
- 主要问题集中在：
  1. 源码版本路径和行号引用
  2. 方法签名细节和返回类型
  3. **RANGE 模式支持说明（Flink 完全支持 RANGE）**
  4. **BucketPartitioner 启用方式（需手动启用，非自动）**
- 所有发现的错误已修正，文档现在与源码完全一致

**验证方法**：
- 直接读取源码文件验证类名、方法名、字段名
- 对比行号确保引用准确
- 验证方法签名和返回类型
- 检查日志消息和注释内容
- 确认 RANGE 模式实现逻辑
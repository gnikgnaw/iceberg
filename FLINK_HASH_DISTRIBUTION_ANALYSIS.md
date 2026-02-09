# Apache Iceberg Flink HASH 分布模式深度解析

## 1. 概述

在 Apache Iceberg 的 Flink 集成中，当 `DistributionMode` 设置为 `HASH` 时，系统通过 Flink 的 `keyBy` 机制对数据进行哈希分布，确保相同分区的数据被发送到同一个 Flink subtask 中处理。

**与 Spark 的核心区别**：
- **Spark**：使用 `ClusteredDistribution` + `HashPartitioning` 进行 shuffle
- **Flink**：使用 `KeySelector` + `keyBy()` 进行数据流重分区

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

**配置读取** (`flink/v1.18/flink/src/main/java/org/apache/iceberg/flink/FlinkWriteConf.java:158-168`):

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

**分布策略选择** (`flink/v1.18/flink/src/main/java/org/apache/iceberg/flink/sink/FlinkSink.java:501-569`):

```java
private DataStream<RowData> distributeDataStream(
    DataStream<RowData> input,
    List<Integer> equalityFieldIds,
    PartitionSpec partitionSpec,
    Schema iSchema,
    RowType flinkRowType) {
  DistributionMode writeMode = flinkWriteConf.distributionMode();

  LOG.info("Write distribution mode is '{}'", writeMode.modeName());
  switch (writeMode) {
    case NONE:
      if (equalityFieldIds.isEmpty()) {
        return input;  // 不进行重分区
      } else {
        // 按 equality fields 分区（用于 UPSERT）
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
          return input.keyBy(
              new EqualityFieldKeySelector(iSchema, flinkRowType, equalityFieldIds));
        } else {
          // 有分区且有 equality fields -> 确保分区字段是 equality fields 的子集
          for (PartitionField partitionField : partitionSpec.fields()) {
            Preconditions.checkState(
                equalityFieldIds.contains(partitionField.sourceId()),
                "In 'hash' distribution mode with equality fields set, partition field '%s' "
                    + "should be included in equality fields: '%s'",
                partitionField,
                equalityFieldColumns);
          }
          return input.keyBy(new PartitionKeySelector(partitionSpec, iSchema, flinkRowType));
        }
      }

    case RANGE:
      // Flink 不支持 RANGE 模式，退化处理
      LOG.warn("Flink does not support 'range' distribution mode, fallback to distribute by equality fields or none");
      // ... 省略 RANGE 处理逻辑
  }
}
```

**关键决策逻辑**：
1. **HASH + 无 equality fields + 有分区** → 按分区键 keyBy
2. **HASH + 有 equality fields + 有分区** → 按分区键 keyBy（要求分区字段 ⊆ equality fields）
3. **HASH + 无 equality fields + 无分区** → 不分区（退化为 NONE）
4. **HASH + 有 equality fields + 无分区** → 按 equality fields keyBy

---

## 3. PartitionKeySelector 实现机制

### 3.1 核心实现

**PartitionKeySelector 类** (`flink/v1.18/flink/src/main/java/org/apache/iceberg/flink/sink/PartitionKeySelector.java:34-64`):

```java
class PartitionKeySelector implements KeySelector<RowData, String> {

  private final Schema schema;
  private final PartitionKey partitionKey;
  private final RowType flinkSchema;

  private transient RowDataWrapper rowDataWrapper;

  PartitionKeySelector(PartitionSpec spec, Schema schema, RowType flinkSchema) {
    this.schema = schema;
    this.partitionKey = new PartitionKey(spec, schema);  // 创建分区键计算器
    this.flinkSchema = flinkSchema;
  }

  /**
   * Construct the RowDataWrapper lazily to avoid serialization issues.
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

当表的分区定义**仅包含一个 bucket transform** 时，Flink 可以使用 `BucketPartitioner` 进行优化：

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

### 5.2 BucketPartitioner 实现

**核心代码** (`flink/v1.18/flink/src/main/java/org/apache/iceberg/flink/sink/BucketPartitioner.java:31-103`):

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

### 5.3 BucketPartitioner 的优势

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

| 场景 | 分区类型 | Equality Fields | 分布策略 | KeySelector |
|------|---------|----------------|---------|-------------|
| 普通写入 | 有分区 | 无 | HASH | PartitionKeySelector |
| 普通写入 | 无分区 | 无 | NONE | 不重分区 |
| UPSERT | 有分区 | 有 | HASH | PartitionKeySelector（分区⊆equality） |
| UPSERT | 无分区 | 有 | HASH | EqualityFieldKeySelector |
| CDC | 有分区 | 有 | HASH | PartitionKeySelector |
| CDC | 无分区 | 有 | HASH | EqualityFieldKeySelector |

### 6.2 UPSERT 模式的特殊处理

**UPSERT 约束** (`FlinkSink.java:459-468`):
```java
if (flinkWriteConf.upsertMode()) {
  Preconditions.checkState(
      !equalityFieldIds.isEmpty(),
      "Equality field columns shouldn't be empty when configuring to use UPSERT data stream.");

  if (!table.spec().isUnpartitioned()) {
    for (PartitionField partitionField : table.spec().fields()) {
      Preconditions.checkState(
          equalityFieldIds.contains(partitionField.sourceId()),
          "In UPSERT mode, partition field '%s' should be included in equality fields: '%s'",
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

### 8.3 Bucket 分区优化

**当使用 bucket 分区时**：
```java
// 优化前：混合分区
PARTITIONED BY (days(event_time), bucket(100, user_id))

// 优化后：仅 bucket 分区（启用 BucketPartitioner）
PARTITIONED BY (bucket(100, user_id))

// 如果需要时间分区，在查询时使用 time-travel
SELECT * FROM table
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-15 00:00:00'
WHERE user_id_bucket = 5;
```

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

### 示例 3：Bucket 分区优化

```java
// 表定义（仅使用 bucket 分区）
CREATE TABLE user_events (
  user_id BIGINT,
  event_type STRING,
  event_time TIMESTAMP
) PARTITIONED BY (bucket(100, user_id));

// Flink 写入（自动使用 BucketPartitioner）
DataStream<RowData> eventStream = ...;

FlinkSink.forRowData(eventStream)
    .table(table)
    .tableLoader(tableLoader)
    .distributionMode(DistributionMode.HASH)
    .writeParallelism(50)  // writer 数量 < bucket 数量，每个 writer 负责 2 个 bucket
    .append();
```

**BucketPartitioner 自动启用条件**：
1. PartitionSpec 只包含一个字段
2. 该字段是 `BucketTransform`
3. 满足条件后，Flink 使用 `BucketPartitioner` 代替 `PartitionKeySelector`

**优化效果**：
- 传统方式：100 buckets × 50 writers = 最多 5000 个文件
- BucketPartitioner：100 buckets = 最多 100 个文件

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

- **源码位置**：
  - `flink/v1.18/flink/src/main/java/org/apache/iceberg/flink/FlinkWriteConf.java`
  - `flink/v1.18/flink/src/main/java/org/apache/iceberg/flink/sink/FlinkSink.java`
  - `flink/v1.18/flink/src/main/java/org/apache/iceberg/flink/sink/PartitionKeySelector.java`
  - `flink/v1.18/flink/src/main/java/org/apache/iceberg/flink/sink/BucketPartitioner.java`
  - `flink/v1.18/flink/src/main/java/org/apache/iceberg/flink/sink/EqualityFieldKeySelector.java`
  - `api/src/main/java/org/apache/iceberg/PartitionKey.java`

- **相关配置**：
  - `write.distribution-mode`: 分布模式（none/hash/range，Flink 不支持 range）
  - `write-parallelism`: Writer 并行度
  - `write.target-file-size-bytes`: 目标文件大小
  - `write.upsert.enabled`: 是否启用 UPSERT 模式
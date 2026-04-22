# Apache Iceberg HASH 分布模式深度解析

## 1. 概述

当 Apache Iceberg 的 `DistributionMode` 设置为 `HASH` 时，系统会根据表的分区键（Partition Key）对数据进行哈希分布，确保相同分区的数据被发送到同一个 Spark Task 中处理。这种分布模式适合数据在不同分区中均匀分布的场景。

**核心定义** (`api/src/main/java/org/apache/iceberg/DistributionMode.java:39-42`):
```java
public enum DistributionMode {
  NONE("none"),
  HASH("hash"),
  RANGE("range");
```

**注释说明** (`api/src/main/java/org/apache/iceberg/DistributionMode.java:32-33`):
```java
/**
 * 2. hash: hash distribute by partition key, which is suitable for the scenarios where the rows
 * are located into different partitions evenly.
 */
```

---

## 2. HASH 分布的完整流程

### 2.1 触发流程

```
User Query/Write Request
    ↓
SparkWriteBuilder.build() → writeRequirements()
    ↓
SparkWriteConf.writeRequirements()
    ↓
SparkWriteUtil.writeRequirements(table, DistributionMode.HASH)
    ↓
SparkWriteUtil.writeDistribution() → Distributions.clustered(Spark3Util.toTransforms(table.spec()))
    ↓
Spark 执行层创建 HashPartitioning
    ↓
数据根据分区键 hashCode 进行 shuffle
```

### 2.2 核心实现代码

**1) 构建 Distribution 对象** (`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkWriteUtil.java:76-90`):

```java
private static Distribution writeDistribution(Table table, DistributionMode mode) {
  switch (mode) {
    case NONE:
      return Distributions.unspecified();

    case HASH:
      // 将 Iceberg PartitionSpec 转换为 Spark Transform，然后创建 ClusteredDistribution
      return Distributions.clustered(clustering(table));

    case RANGE:
      return Distributions.ordered(ordering(table));

    default:
      throw new IllegalArgumentException("Unsupported distribution mode: " + mode);
  }
}

private static Expression[] clustering(Table table) {
  return Spark3Util.toTransforms(table.spec());
}
```

**关键点**：
- `HASH` 模式返回 `ClusteredDistribution`
- 使用 `Spark3Util.toTransforms()` 将 Iceberg 的 `PartitionSpec` 转换为 Spark 的 `Transform[]`

---

## 3. 分区键的转换机制

### 3.1 PartitionSpec 到 Spark Transform 的转换

**转换入口** (`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/Spark3Util.java:299-303`):

```java
public static Transform[] toTransforms(PartitionSpec spec) {
  SpecTransformToSparkTransform visitor = new SpecTransformToSparkTransform(spec.schema());
  List<Transform> transforms = PartitionSpecVisitor.visit(spec, visitor);
  return transforms.stream().filter(Objects::nonNull).toArray(Transform[]::new);
}
```

### 3.2 支持的 Transform 类型

**Transform 转换器实现** (`Spark3Util.java:305-362`):

```java
private static class SpecTransformToSparkTransform implements PartitionSpecVisitor<Transform> {

  @Override
  public Transform identity(String sourceName, int sourceId) {
    return Expressions.identity(quotedName(sourceId));
  }

  @Override
  public Transform bucket(String sourceName, int sourceId, int numBuckets) {
    return Expressions.bucket(numBuckets, quotedName(sourceId));
  }

  @Override
  public Transform truncate(String sourceName, int sourceId, int width) {
    NamedReference column = Expressions.column(quotedName(sourceId));
    return Expressions.apply("truncate", Expressions.literal(width), column);
  }

  @Override
  public Transform year(String sourceName, int sourceId) {
    return Expressions.years(quotedName(sourceId));
  }

  @Override
  public Transform month(String sourceName, int sourceId) {
    return Expressions.months(quotedName(sourceId));
  }

  @Override
  public Transform day(String sourceName, int sourceId) {
    return Expressions.days(quotedName(sourceId));
  }

  @Override
  public Transform hour(String sourceName, int sourceId) {
    return Expressions.hours(quotedName(sourceId));
  }
}
```

**支持的分区变换类型**：
- `identity`: 直接使用列值作为分区
- `bucket(N, col)`: 对列值进行 bucket 哈希，分成 N 个桶
- `truncate(width, col)`: 截断字符串或数值
- `year/month/day/hour`: 时间分区
- `unknown`: 自定义 transform

---

## 4. Hash 计算机制

### 4.1 PartitionData 的 hashCode 实现

**核心 Hash 计算** (`core/src/main/java/org/apache/iceberg/PartitionData.java:190-195`):

```java
@Override
public int hashCode() {
  Hasher hasher = Hashing.goodFastHash(32).newHasher();
  Stream.of(data).map(Objects::hashCode).forEach(hasher::putInt);
  partitionType.fields().stream().map(Objects::hashCode).forEach(hasher::putInt);
  return hasher.hash().hashCode();
}
```

**Hash 算法详解**：
1. 使用 **Guava 的 `goodFastHash(32)`** 算法（基于 Murmur3）
2. 对分区数据数组 `data[]` 中的每个值计算 `hashCode()`
3. 对分区类型的每个字段也计算 `hashCode()`
4. 将所有 hash 值组合后返回最终的 hashCode

**为什么这样设计？**
- **高性能**：Murmur3 是非加密哈希算法，计算速度快
- **均匀分布**：避免 hash 碰撞，确保数据均匀分布到不同的 Spark partition
- **包含类型信息**：同时考虑数据值和字段类型，避免不同类型但相同值的数据被分到同一分区

### 4.2 PartitionKey 的工作机制

**PartitionKey 类** (`api/src/main/java/org/apache/iceberg/PartitionKey.java:30-74`):

```java
public class PartitionKey extends StructTransform {

  private final PartitionSpec spec;
  private final Schema inputSchema;

  public PartitionKey(PartitionSpec spec, Schema inputSchema) {
    super(inputSchema, fieldTransform(spec));
    this.spec = spec;
    this.inputSchema = inputSchema;
  }

  /**
   * Replace this key's partition values with the partition values for the row.
   */
  public void partition(StructLike row) {
    wrap(row);  // 应用 transform 计算分区值
  }

  private static List<FieldTransform> fieldTransform(PartitionSpec spec) {
    return spec.fields().stream()
        .map(partitionField ->
            new FieldTransform(partitionField.sourceId(), partitionField.transform()))
        .collect(Collectors.toList());
  }
}
```

**工作流程**：
1. 根据 `PartitionSpec` 定义，为每个分区字段创建 `FieldTransform`
2. 调用 `partition(row)` 时，对原始数据行应用 transform 计算分区值
3. 计算出的分区值存储在 `PartitionKey` 中
4. 后续使用 `PartitionKey.hashCode()` 进行哈希分布

---

## 5. Spark 执行层实现

### 5.1 ClusteredDistribution 的作用

当 `buildRequiredDistribution()` 返回 `Distributions.clustered(transforms)` 时：

1. **Spark Planner** 会为写入操作插入 **Shuffle 阶段**
2. 根据 `ClusteredDistribution` 的 `clustering` 表达式创建 **HashPartitioning**
3. 数据按照分区键的 hash 值重新分布到不同的 Task

**Spark 内部实现**（Spark SQL 层）：
```scala
// Spark 内部会根据 ClusteredDistribution 创建类似这样的 HashPartitioning
HashPartitioning(expressions = transforms, numPartitions = taskNum)
```

### 5.2 数据写入时的 Hash 分布

**写入流程** (`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java:788-843`):

```java
private static class PartitionedDataWriter implements DataWriter<InternalRow> {
  private final PartitioningWriter<InternalRow, DataWriteResult> delegate;
  private final FileIO io;
  private final PartitionSpec spec;
  private final PartitionKey partitionKey;
  private final InternalRowWrapper internalRowWrapper;

  private PartitionedDataWriter(..., boolean fanoutEnabled) {
    if (fanoutEnabled) {
      this.delegate = new FanoutDataWriter<>(...);  // 每个分区一个文件
    } else {
      this.delegate = new ClusteredDataWriter<>(...);  // 聚合相同分区的数据
    }
    this.spec = spec;
    this.partitionKey = new PartitionKey(spec, dataSchema);
    this.internalRowWrapper = new InternalRowWrapper(dataSparkType, dataSchema.asStruct());
  }

  @Override
  public void write(InternalRow row) throws IOException {
    // 1. 计算该行数据的分区键
    partitionKey.partition(internalRowWrapper.wrap(row));

    // 2. 将数据和分区键传递给底层 writer
    delegate.write(row, spec, partitionKey);
  }
}
```

**关键步骤**：
1. **计算分区键**：`partitionKey.partition(row)` 根据 PartitionSpec 应用 transform
2. **写入数据**：将行数据和分区键传给 `ClusteredDataWriter` 或 `FanoutDataWriter`
3. **文件生成**：相同分区键的数据写入同一个文件

### 5.3 FanoutDataWriter vs ClusteredDataWriter

| 特性 | FanoutDataWriter | ClusteredDataWriter |
|------|------------------|---------------------|
| **使用场景** | 分区数较少，每个分区数据量大 | 分区数较多，需要聚合相同分区 |
| **内存占用** | 高（为每个分区维护一个 writer） | 低（只维护当前分区的 writer） |
| **写入效率** | 高（并发写入多个文件） | 需要排序或聚合相同分区 |
| **文件数量** | 每个分区至少一个文件 | 可能产生更少的文件 |

**选择逻辑** (`SparkWriteConf.java`):
- 如果配置了 `write.fanout-enabled=true`，使用 `FanoutDataWriter`
- 否则使用 `ClusteredDataWriter`（默认）

---

## 6. 不同场景下的 HASH 分布

### 6.1 普通写入场景（Append/Overwrite）

```java
// SparkWriteUtil.java:76-90
case HASH:
  return Distributions.clustered(clustering(table));

private static Expression[] clustering(Table table) {
  return Spark3Util.toTransforms(table.spec());
}
```

**行为**：
- 按照表的完整 PartitionSpec 进行 hash 分布
- 确保相同分区的数据发送到同一个 Task

**示例**：
```sql
-- 表分区定义：PARTITIONED BY (year(ts), bucket(10, user_id))
INSERT INTO iceberg_table VALUES (...);

-- Spark 会根据 (year(ts), bucket(10, user_id)) 的组合进行 hash shuffle
```

### 6.2 Copy-on-Write DELETE/UPDATE 场景

```java
// SparkWriteUtil.java:109-133
private static Distribution copyOnWriteDeleteUpdateDistribution(
    Table table, DistributionMode mode) {
  switch (mode) {
    case HASH:
      if (table.spec().isPartitioned()) {
        return Distributions.clustered(clustering(table));
      } else {
        // 特殊处理：按文件路径进行 hash，而不是按分区键
        return Distributions.clustered(FILE_CLUSTERING);
      }

    // ...
  }
}
```

**为什么不同？**
- **DELETE/UPDATE 需要读取原文件**，然后重写新文件
- 按 `FILE_PATH` 进行 hash，确保**同一个文件的所有行**被发送到同一个 Task
- 这样可以在一个 Task 中完成文件的读取、修改和重写，避免跨 Task 协调

### 6.3 Position Delete 场景

```java
// SparkWriteUtil.java:154-178
private static Distribution positionDeltaUpdateMergeDistribution(
    Table table, DistributionMode mode) {
  switch (mode) {
    case HASH:
      if (table.spec().isUnpartitioned()) {
        // 未分区表：按 (spec_id, partition, file_path) + 表分区键 hash
        return Distributions.clustered(concat(PARTITION_FILE_CLUSTERING, clustering(table)));
      } else {
        // 分区表：组合 delete 和 data 的 clustering
        return Distributions.clustered(concat(PARTITION_CLUSTERING, clustering(table)));
      }
  }
}
```

**为什么复杂？**
- Position Delete 需要同时处理**删除记录**和**数据记录**
- 需要确保**同一个文件的删除记录和数据记录**在同一个 Task 中
- 组合多个 clustering 键确保正确的 co-location

---

## 7. 实际应用示例

### 示例 1：按日期和地区分区的表

```sql
CREATE TABLE sales (
  id BIGINT,
  amount DECIMAL(10,2),
  sale_date DATE,
  region STRING
) USING iceberg
PARTITIONED BY (days(sale_date), region);

-- 写入配置
SET spark.sql.iceberg.write.distribution-mode=hash;

INSERT INTO sales VALUES (...);
```

**执行流程**：
1. PartitionSpec: `[days(sale_date), identity(region)]`
2. `toTransforms()` 转换为 Spark Transform: `[days("sale_date"), identity("region")]`
3. Spark 创建 `ClusteredDistribution(days(sale_date), region)`
4. 数据根据 `(days(sale_date), region)` 的组合 hash 值 shuffle
5. 相同分区的数据写入同一个文件

**Hash 计算**：
```java
// 假设一行数据：sale_date='2024-01-15', region='US'
// 分区值：day=19737 (2024-01-15), region='US'
// hashCode = hash(19737) ^ hash("US") ^ hash(partitionType)
```

### 示例 2：Bucket 分区优化

```sql
CREATE TABLE users (
  user_id BIGINT,
  name STRING,
  signup_date DATE
) USING iceberg
PARTITIONED BY (bucket(100, user_id), days(signup_date));
```

**优势**：
- `bucket(100, user_id)` 将用户 ID 均匀分布到 100 个桶
- 结合日期分区，避免数据倾斜
- HASH 分布模式确保相同桶的数据在同一个 Task 中处理

---

## 8. 性能优化建议

### 8.1 选择合适的分区策略

| 场景 | 推荐 PartitionSpec | DistributionMode |
|------|-------------------|------------------|
| 时间序列数据 | `day(ts)` 或 `hour(ts)` | HASH |
| 高基数字段 | `bucket(N, col)` | HASH |
| 低基数字段 | `identity(col)` | HASH 或 RANGE |
| 数据倾斜严重 | 组合分区 + bucket | RANGE |

### 8.2 调整 Hash 分布参数

```properties
# 启用 fanout writer（适合分区数少的场景）
spark.sql.iceberg.write.fanout-enabled=true

# 控制 shuffle 分区数
spark.sql.shuffle.partitions=200

# 目标文件大小
spark.sql.iceberg.write.target-file-size-bytes=536870912  # 512MB
```

### 8.3 避免的反模式

❌ **不要在高基数字段上使用 identity 分区**
```sql
-- 错误：user_id 有百万级别的唯一值
PARTITIONED BY (user_id)
```

✅ **使用 bucket 进行降维**
```sql
-- 正确：将用户分到 1000 个桶
PARTITIONED BY (bucket(1000, user_id))
```

---

## 9. 总结

### HASH 分布模式的核心机制

1. **分区键转换**：通过 `Spark3Util.toTransforms()` 将 Iceberg PartitionSpec 转换为 Spark Transform
2. **Hash 计算**：使用 Guava Murmur3 算法计算分区键的 hashCode
3. **数据分布**：Spark 根据 ClusteredDistribution 创建 HashPartitioning 进行 shuffle
4. **文件写入**：相同分区的数据写入同一个文件，减少小文件问题

### 适用场景

✅ **适合**：
- 数据在不同分区间均匀分布
- 分区数适中（几十到几千个）
- 需要控制文件数量

❌ **不适合**：
- 数据严重倾斜（应使用 RANGE 模式）
- 分区数极少或极多
- 需要保证全局排序

### 关键代码路径

```
DistributionMode.HASH
  ↓
SparkWriteConf.writeRequirements()
  ↓
SparkWriteUtil.writeRequirements()
  ↓
SparkWriteUtil.writeDistribution(table, HASH)
  ↓
Spark3Util.toTransforms(PartitionSpec)
  ↓
Distributions.clustered(Transform[])
  ↓
PartitionKey.partition(row) → PartitionData.hashCode()
  ↓
Spark HashPartitioning shuffle
  ↓
PartitionedDataWriter → ClusteredDataWriter/FanoutDataWriter
```

---

## 10. 参考资料

- **源码位置**：
  - `api/src/main/java/org/apache/iceberg/DistributionMode.java`
  - `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkWriteUtil.java`
  - `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/Spark3Util.java`
  - `core/src/main/java/org/apache/iceberg/PartitionData.java`
  - `api/src/main/java/org/apache/iceberg/PartitionKey.java`
  - `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java`

- **相关配置**：
  - `write.distribution-mode`: 设置分布模式（none/hash/range）
  - `write.fanout-enabled`: 是否启用 fanout writer
  - `write.target-file-size-bytes`: 目标文件大小

---

## 11. 文档修正记录

### 修正日期：2026-04-20

#### 修正内容：

1. **DistributionMode.java 行号修正**
   - 原文档：第32-33行包含 HASH 定义
   - 实际情况：第32-33行是注释，HASH 枚举定义在第41行
   - 修正：分离注释说明和枚举定义的行号引用

2. **核心类名修正**
   - 原文档：引用 `SparkDistributionAndOrderingUtil` 类
   - 实际情况：该类不存在，实际逻辑在 `SparkWriteUtil` 类中
   - 修正：所有引用改为 `SparkWriteUtil`

3. **Spark3Util.java 行号修正**
   - 原文档：toTransforms 方法在第291-295行
   - 实际情况：toTransforms 方法在第299-303行
   - 修正：更新为正确的行号

4. **PartitionKey.java 行号修正**
   - 原文档：类定义在第30-65行
   - 实际情况：类定义在第30-74行
   - 修正：更新为正确的行号范围

5. **SparkWrite.java 行号修正**
   - 原文档：PartitionedDataWriter 在第751-806行
   - 实际情况：PartitionedDataWriter 在第788-843行
   - 修正：更新为正确的行号范围

6. **代码细节修正**
   - 原文档：InternalRowWrapper 构造函数只有一个参数
   - 实际情况：InternalRowWrapper 构造函数有两个参数 (dataSparkType, dataSchema.asStruct())
   - 修正：更新构造函数调用代码

7. **方法调用链修正**
   - 原文档：SparkWriteBuilder.buildWriteRequirements()
   - 实际情况：SparkWriteBuilder.build() → writeRequirements() → SparkWriteConf.writeRequirements()
   - 修正：更新完整的调用链路

#### 验证方法：
- 直接读取 Iceberg 源码文件进行逐行对照
- 验证所有类名、方法名、字段名的准确性
- 确认行号引用与实际源码一致

#### 验证范围：
- ✅ DistributionMode.java
- ✅ Spark3Util.java
- ✅ PartitionData.java
- ✅ PartitionKey.java
- ✅ SparkWrite.java
- ✅ SparkWriteUtil.java
- ✅ SparkWriteConf.java
- ✅ SparkWriteRequirements.java
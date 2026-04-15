# Apache Iceberg 排序策略源码深度分析

> 基于 Iceberg 源码的完整排序体系分析，涵盖 SortOrder 核心 API、Z-Order 空间填充曲线、Spark 引擎集成以及 RewriteDataFiles 数据重排策略。

---

## 目录

1. [排序策略概览与核心价值](#1-排序策略概览与核心价值)
2. [SortOrder 核心数据模型](#2-sortorder-核心数据模型)
3. [SortField —— 排序字段定义](#3-sortfield--排序字段定义)
4. [SortOrderBuilder —— 构建器模式](#4-sortorderbuilder--构建器模式)
5. [UnboundSortOrder —— 序列化与反序列化](#5-unboundsortorder--序列化与反序列化)
6. [SortOrderParser —— JSON 持久化](#6-sortorderparser--json-持久化)
7. [SortOrderComparators —— 比较器工厂](#7-sortordercomparators--比较器工厂)
8. [SortKey —— 排序键提取](#8-sortkey--排序键提取)
9. [SortOrderVisitor —— 访问者模式](#9-sortordervisitor--访问者模式)
10. [SortOrderUtil —— 分区与排序的协同](#10-sortorderutil--分区与排序的协同)
11. [Z-Order 空间填充曲线实现](#11-z-order-空间填充曲线实现)
12. [RewriteDataFiles —— 数据重排 Action API](#12-rewritedatafiles--数据重排-action-api)
13. [Spark 集成 —— 排序策略引擎实现](#13-spark-集成--排序策略引擎实现)
14. [Spark Procedure —— SQL 调用入口](#14-spark-procedure--sql-调用入口)
15. [DistributionMode —— 写入分布模式](#15-distributionmode--写入分布模式)
16. [Table API 中的排序管理](#16-table-api-中的排序管理)
17. [使用示例与最佳实践](#17-使用示例与最佳实践)
18. [各排序策略的对比分析](#18-各排序策略的对比分析)
19. [源码文件索引](#19-源码文件索引)

---

## 1. 排序策略概览与核心价值

Apache Iceberg 的排序策略用于控制数据文件中记录的物理存储顺序。合理的排序可以带来以下好处：

- **减少数据扫描量**：排序使得数据文件的 min/max 统计信息更加有效，查询引擎可以跳过更多不相关的文件
- **提高列式压缩率**：相似值在物理上更紧凑，有利于编码和压缩
- **加速范围查询**：针对排序列的范围过滤可以快速定位目标数据
- **多维查询优化**：Z-Order 排序允许同时在多个列上获得近似最优的数据局部性

Iceberg 支持三种排序策略：

| 策略 | 核心类 | 适用场景 |
|------|--------|----------|
| **Linear Sort** | `SortOrder` + `SparkSortFileRewriteRunner` | 单列或多列线性排序，适合固定查询模式 |
| **Z-Order** | `ZOrderByteUtils` + `SparkZOrderFileRewriteRunner` | 多维度等权重查询，适合多列联合过滤 |
| **BinPack** | `SparkBinPackFileRewriteRunner` | 不改变排序，仅合并小文件 |

---

## 2. SortOrder 核心数据模型

**源码位置**: `api/src/main/java/org/apache/iceberg/SortOrder.java`

`SortOrder` 是 Iceberg 排序体系的核心类，定义了一个表的排序规则。

### 2.1 类结构

```java
public class SortOrder implements Serializable {
    private static final SortOrder UNSORTED_ORDER = 
        new SortOrder(new Schema(), 0, Collections.emptyList());
    
    private final Schema schema;       // 关联的 Schema
    private final int orderId;         // 排序 ID (0 保留给 unsorted)
    private final SortField[] fields;  // 排序字段数组
    
    private transient volatile List<SortField> fieldList;  // 延迟初始化的列表视图
}
```

### 2.2 关键设计决策

**orderId 的意义**：
- `orderId = 0` 被保留用于无排序状态 (`UNSORTED_ORDER`)
- 每个有效的排序顺序从 `orderId = 1` 开始
- orderId 在 `TableMetadata` 中唯一标识一个排序顺序，支持排序演进（类似 partition spec 的演进）

**不可变性**：
- `SortOrder` 一旦构建就不可修改，`fields` 是私有数组
- `fieldList` 使用双重检查锁定 (DCL) 的延迟初始化，确保线程安全又避免不必要的对象创建

### 2.3 核心方法

```java
// 判断当前排序是否满足另一个排序的要求
public boolean satisfies(SortOrder anotherSortOrder) {
    // 任何排序都满足无排序
    if (anotherSortOrder.isUnsorted()) return true;
    // 字段数不够则不满足
    if (anotherSortOrder.fields.length > fields.length) return false;
    // 逐前缀比较
    return IntStream.range(0, anotherSortOrder.fields.length)
        .allMatch(index -> fields[index].satisfies(anotherSortOrder.fields[index]));
}
```

**`satisfies` 的语义**：如果数据按 `[a ASC, b DESC, c ASC]` 排序，则它自动满足 `[a ASC, b DESC]` 和 `[a ASC]` 的排序要求。这是前缀匹配逻辑，在数据写入引擎判断是否需要重新排序时非常重要。

### 2.4 静态 Builder 入口

```java
public static Builder builderFor(Schema schema) {
    return new Builder(schema);
}
```

Builder 内部在 `addSortField` 时会将 `UnboundTerm` 绑定到 Schema：
```java
private Builder addSortField(Term term, SortDirection direction, NullOrder nullOrder) {
    BoundTerm<?> boundTerm = ((UnboundTerm<?>) term).bind(schema.asStruct(), caseSensitive);
    int sourceId = boundTerm.ref().fieldId();
    SortField sortField = new SortField(toTransform(boundTerm), sourceId, direction, nullOrder);
    fields.add(sortField);
    return this;
}
```

注意 `toTransform` 方法的逻辑：
- `BoundReference` → `Transforms.identity()` (直接引用列)
- `BoundTransform` → 提取内部 Transform (如 `bucket`、`truncate`、`year` 等)

---

## 3. SortField —— 排序字段定义

**源码位置**: `api/src/main/java/org/apache/iceberg/SortField.java`

每个排序字段包含四个属性：

```java
public class SortField implements Serializable {
    private final Transform<?, ?> transform;  // 变换函数
    private final int sourceId;               // 源列 ID
    private final SortDirection direction;    // 排序方向 (ASC/DESC)
    private final NullOrder nullOrder;        // NULL 排序 (NULLS_FIRST/NULLS_LAST)
}
```

### 3.1 排序方向枚举

**源码位置**: `api/src/main/java/org/apache/iceberg/SortDirection.java`

```java
public enum SortDirection {
    ASC,   // 升序
    DESC;  // 降序
}
```

### 3.2 NULL 排序枚举

**源码位置**: `api/src/main/java/org/apache/iceberg/NullOrder.java`

```java
public enum NullOrder {
    NULLS_FIRST,  // NULL 排在最前
    NULLS_LAST;   // NULL 排在最后
}
```

### 3.3 Transform 在排序中的作用

排序字段中的 Transform 允许对源列的值进行变换后再排序。支持的变换包括：

| Transform | 示例 | 排序效果 |
|-----------|------|----------|
| `identity` | `col ASC` | 按原始值排序 |
| `bucket[N]` | `bucket(16, col) ASC` | 按 bucket hash 值排序 |
| `truncate[W]` | `truncate(10, col) ASC` | 按截断值排序 |
| `year` | `year(ts) ASC` | 按年份排序 |
| `month` | `month(ts) ASC` | 按月份排序 |
| `day` | `day(ts) ASC` | 按天排序 |
| `hour` | `hour(ts) ASC` | 按小时排序 |

### 3.4 satisfies 方法 —— 排序兼容性检查

```java
public boolean satisfies(SortField other) {
    if (Objects.equals(this, other)) return true;
    if (sourceId != other.sourceId || direction != other.direction || nullOrder != other.nullOrder)
        return false;
    return transform.satisfiesOrderOf(other.transform);
}
```

`transform.satisfiesOrderOf` 实现了排序传递性判断。例如 `identity` 排序满足 `year`、`month`、`day` 的排序要求（更细粒度的排序自然满足更粗粒度的排序），而反过来则不成立。

---

## 4. SortOrderBuilder —— 构建器模式

**源码位置**: `api/src/main/java/org/apache/iceberg/SortOrderBuilder.java`

`SortOrderBuilder` 定义了构建排序顺序的流式 API 接口：

```java
public interface SortOrderBuilder<R> {
    // 字段名直接引用
    default R asc(String name) { ... }          // 升序，NULLS_FIRST
    default R asc(String name, NullOrder nullOrder) { ... }
    
    // Term 表达式引用（支持 Transform）
    default R asc(Term term) { ... }            // 升序，NULLS_FIRST
    R asc(Term term, NullOrder nullOrder);       // 升序，自定义 NULL 排序
    
    // 降序方法
    default R desc(String name) { ... }         // 降序，NULLS_LAST
    default R desc(String name, NullOrder nullOrder) { ... }
    default R desc(Term term) { ... }           // 降序，NULLS_LAST
    R desc(Term term, NullOrder nullOrder);      // 降序，自定义 NULL 排序
}
```

**默认 NullOrder 约定**：
- `ASC` 默认 `NULLS_FIRST` (NULL 最小)
- `DESC` 默认 `NULLS_LAST` (NULL 最大，保持 NULL 始终在「末端」)

这与 SQL 标准中 `ASC NULLS FIRST` / `DESC NULLS LAST` 的常见惯例一致。

---

## 5. UnboundSortOrder —— 序列化与反序列化

**源码位置**: `api/src/main/java/org/apache/iceberg/UnboundSortOrder.java`

`UnboundSortOrder` 是 `SortOrder` 的未绑定版本，用于 JSON 序列化/反序列化场景。

### 5.1 Bound vs Unbound 的设计理念

```
SortOrder (Bound)                UnboundSortOrder (Unbound)
├── Schema 引用                  ├── 无 Schema 引用
├── Transform 实例               ├── Transform 字符串表示
├── sourceId                     ├── sourceId
└── 可直接用于排序操作            └── 需要 bind(schema) 后才能使用
```

**为什么需要 Unbound 版本**：
- 排序顺序需要被序列化到 JSON (TableMetadata 文件中)
- 反序列化时 Schema 可能尚未完全加载
- Transform 的字符串表示可以跨版本兼容（前向/后向兼容）

### 5.2 绑定过程

```java
public SortOrder bind(Schema schema) {
    SortOrder.Builder builder = SortOrder.builderFor(schema).withOrderId(orderId);
    for (UnboundSortField field : fields) {
        Type sourceType = schema.findType(field.sourceId);
        Transform<?, ?> transform;
        if (sourceType != null) {
            // 如果 Schema 中存在该列，用具体类型创建 Transform
            transform = Transforms.fromString(sourceType, field.transform.toString());
        } else {
            // 列可能已被删除，保留原始 Transform
            transform = field.transform;
        }
        builder.addSortField(transform, field.sourceId, field.direction, field.nullOrder);
    }
    return builder.build();
}
```

`bindUnchecked` 方法则跳过兼容性验证，用于加载历史（非当前默认）排序顺序，因为它们引用的列可能已经被删除。

---

## 6. SortOrderParser —— JSON 持久化

**源码位置**: `core/src/main/java/org/apache/iceberg/SortOrderParser.java`

`SortOrderParser` 负责排序顺序的 JSON 序列化和反序列化。

### 6.1 JSON 格式

```json
{
  "order-id": 1,
  "fields": [
    {
      "transform": "identity",
      "source-id": 1,
      "direction": "asc",
      "null-order": "nulls-first"
    },
    {
      "transform": "bucket[16]",
      "source-id": 2,
      "direction": "desc",
      "null-order": "nulls-last"
    }
  ]
}
```

### 6.2 解析流程

```
JSON String → JsonNode → UnboundSortOrder → bind(Schema) → SortOrder
```

`fromJson` 提供了多种重载：
- `fromJson(String json)` → 返回 `UnboundSortOrder`
- `fromJson(Schema schema, String json)` → 返回已绑定的 `SortOrder`
- `fromJson(Schema schema, JsonNode json, int defaultSortOrderId)` → 仅对默认排序顺序进行严格验证

---

## 7. SortOrderComparators —— 比较器工厂

**源码位置**: `api/src/main/java/org/apache/iceberg/SortOrderComparators.java`

`SortOrderComparators` 生成可用于 `StructLike` 对象排序的 `Comparator`。

### 7.1 核心实现

```java
private static class SortOrderComparator implements Comparator<StructLike> {
    private final SortKey leftKey;
    private final SortKey rightKey;
    private final int size;
    private final Comparator<Object>[] comparators;
    private final Type[] transformResultTypes;
    
    @Override
    public int compare(StructLike left, StructLike right) {
        leftKey.wrap(left);
        rightKey.wrap(right);
        for (int i = 0; i < size; i += 1) {
            Class<?> valueClass = transformResultTypes[i].typeId().javaClass();
            int cmp = comparators[i].compare(
                leftKey.get(i, valueClass), rightKey.get(i, valueClass));
            if (cmp != 0) return cmp;
        }
        return 0;
    }
}
```

### 7.2 排序方向和 NULL 处理

`sortFieldComparator` 方法负责将基础比较器包装为带方向和 NULL 处理的版本：

```java
private static Comparator<Object> sortFieldComparator(
        Comparator<Object> original, SortField sortField) {
    Comparator<Object> comparator = original;
    if (sortField.direction() == SortDirection.DESC) {
        comparator = comparator.reversed();       // 反转比较方向
    }
    if (sortField.nullOrder() == NullOrder.NULLS_FIRST) {
        comparator = Comparators.nullsFirst().thenComparing(comparator);
    } else if (sortField.nullOrder() == NullOrder.NULLS_LAST) {
        comparator = Comparators.nullsLast().thenComparing(comparator);
    }
    return comparator;
}
```

---

## 8. SortKey —— 排序键提取

**源码位置**: `api/src/main/java/org/apache/iceberg/SortKey.java`

`SortKey` 继承自 `StructTransform`，负责从数据行中提取排序键。

```java
public class SortKey extends StructTransform {
    public SortKey(Schema schema, SortOrder sortOrder) {
        super(schema, fieldTransform(sortOrder));
        ...
    }
    
    private static List<FieldTransform> fieldTransform(SortOrder sortOrder) {
        return sortOrder.fields().stream()
            .map(sortField -> new FieldTransform(
                sortField.sourceId(), sortField.transform()))
            .collect(Collectors.toList());
    }
}
```

**使用模式**：
```java
SortKey key = new SortKey(schema, sortOrder);
key.wrap(row);          // 绑定到一个数据行
key.get(0, String.class); // 获取第一个排序字段的变换结果
```

通过 `wrap` 和 `get` 的组合，`SortKey` 能够从原始行数据中按需提取出经过 Transform 变换后的排序值。

---

## 9. SortOrderVisitor —— 访问者模式

**源码位置**: `api/src/main/java/org/apache/iceberg/transforms/SortOrderVisitor.java`

`SortOrderVisitor` 使用访问者模式遍历排序字段，根据不同的 Transform 类型分发到不同的处理方法。

### 9.1 接口定义

```java
public interface SortOrderVisitor<T> {
    T field(String sourceName, int sourceId, SortDirection direction, NullOrder nullOrder);
    T bucket(String sourceName, int sourceId, int width, ...);
    T truncate(String sourceName, int sourceId, int width, ...);
    T year(String sourceName, int sourceId, ...);
    T month(String sourceName, int sourceId, ...);
    T day(String sourceName, int sourceId, ...);
    T hour(String sourceName, int sourceId, ...);
    
    default T unknown(String sourceName, int sourceId, String transform, ...) {
        throw new UnsupportedOperationException(...);
    }
}
```

### 9.2 分发逻辑 (visit 静态方法)

```java
static <R> List<R> visit(SortOrder sortOrder, SortOrderVisitor<R> visitor) {
    for (SortField field : sortOrder.fields()) {
        Transform<?, ?> transform = field.transform();
        if (transform == null || transform instanceof Identity) {
            visitor.field(...);
        } else if (transform instanceof Bucket) {
            visitor.bucket(..., ((Bucket<?>) transform).numBuckets(), ...);
        } else if (transform instanceof Truncate) {
            visitor.truncate(..., ((Truncate<?>) transform).width(), ...);
        } else if (transform == Dates.YEAR || ...) {
            visitor.year(...);
        }
        // ... 其他时间变换
    }
}
```

**典型应用场景**：
- `SortOrderToSpark`：将 Iceberg 排序顺序转换为 Spark DSv2 的排序表达式
- `CopySortOrderFields`：复制排序字段到新的 Builder（用于 `SortOrderUtil`）

---

## 10. SortOrderUtil —— 分区与排序的协同

**源码位置**: `core/src/main/java/org/apache/iceberg/util/SortOrderUtil.java`

`SortOrderUtil` 解决了一个核心问题：**如何在保证分区聚簇的前提下应用用户指定的排序顺序**。

### 10.1 设计问题

假设表按 `day(ts)` 分区，用户指定排序顺序为 `[name ASC, age DESC]`。那么最终的排序必须是：
```
[day(ts) ASC, name ASC, age DESC]
```

分区字段必须作为排序前缀，确保同一分区的数据物理上连续，才能正确写入各自的分区目录。

### 10.2 核心算法 `buildSortOrder`

```java
public static SortOrder buildSortOrder(Schema schema, PartitionSpec spec, SortOrder sortOrder) {
    // 1. 收集需要聚簇的分区字段
    Map<...> requiredClusteringFields = requiredClusteringFields(spec);
    
    // 2. 去除排序顺序前缀中已满足的分区字段
    for (SortField sortField : sortOrder.fields()) {
        if (requiredClusteringFields.containsKey(sourceAndTransform)) {
            requiredClusteringFields.remove(sourceAndTransform);
            continue;  // 排序前缀已涵盖此分区字段
        }
        // 检查是否通过 satisfiesOrderOf 满足
        for (PartitionField field : spec.fields()) {
            if (sortField.transform().satisfiesOrderOf(field.transform())) {
                requiredClusteringFields.remove(...);
            }
        }
        break;  // 遇到非分区字段就停止检查前缀
    }
    
    // 3. 将剩余的分区字段作为前缀，后接用户排序
    SortOrder.Builder builder = SortOrder.builderFor(schema);
    for (PartitionField field : requiredClusteringFields.values()) {
        builder.asc(Expressions.transform(sourceName, field.transform()));
    }
    SortOrderVisitor.visit(sortOrder, new CopySortOrderFields(builder));
    return builder.build();
}
```

### 10.3 分区字段去重

`requiredClusteringFields` 方法还处理了分区字段间的满足关系。例如如果同时有 `days(ts)` 和 `hours(ts)` 分区字段，`days(ts)` 被 `hours(ts)` 满足，所以只需要在排序中包含 `hours(ts)`。

---

## 11. Z-Order 空间填充曲线实现

### 11.1 Zorder 表达式

**源码位置**: `core/src/main/java/org/apache/iceberg/expressions/Zorder.java`

```java
public class Zorder implements Term {
    private final NamedReference<?>[] refs;  // Z-Order 列引用列表
    
    public Zorder(List<NamedReference<?>> refs) {
        this.refs = refs.toArray(new NamedReference[0]);
    }
    
    public List<NamedReference<?>> refs() {
        return Arrays.asList(refs);
    }
}
```

`Zorder` 是一个特殊的 `Term` 表达式，不同于普通的列引用或变换表达式，它封装了多个列引用，代表一个 Z-Order 排序意图。

### 11.2 ZOrderByteUtils —— 字节编码核心

**源码位置**: `core/src/main/java/org/apache/iceberg/util/ZOrderByteUtils.java`

Z-Order 的核心思想是将多维数据映射到一维字节序列，使得相邻的多维点在一维序列中也尽可能相邻。

#### 11.2.1 设计原则

```
目标：将各种类型的值转换为字典序可比较的字节数组
规则：
  1. 所有基本类型统一为 8 字节 (PRIMITIVE_BUFFER_SIZE = 8)
  2. 字节比较必须是无符号大端序 (unsigned big-endian)
  3. 变长类型（String/Binary）截断或填充到固定长度
```

#### 11.2.2 整数类型编码

```java
public static ByteBuffer wholeNumberOrderedBytes(long val, ByteBuffer reuse) {
    ByteBuffer bytes = ByteBuffers.reuse(reuse, PRIMITIVE_BUFFER_SIZE);
    bytes.putLong(val ^ 0x8000000000000000L);  // 翻转符号位
    return bytes;
}
```

**为什么要翻转符号位**？

有符号整数的二进制补码表示中，负数的最高位为 1，正数为 0。这导致按字节比较时负数反而"更大"。通过 XOR `0x8000000000000000L`（翻转符号位）：
- 原来的负数 (`1xxx...`) → 变成 `0xxx...` (比较时更小)
- 原来的正数 (`0xxx...`) → 变成 `1xxx...` (比较时更大)
- `Long.MIN_VALUE` → `0x0000...` (最小)
- `Long.MAX_VALUE` → `0xFFFF...` (最大)

这保证了字节序与数值序一致。

#### 11.2.3 浮点数编码

```java
public static ByteBuffer floatingPointOrderedBytes(double val, ByteBuffer reuse) {
    ByteBuffer bytes = ByteBuffers.reuse(reuse, PRIMITIVE_BUFFER_SIZE);
    long lval = Double.doubleToLongBits(val);
    lval ^= ((lval >> (Integer.SIZE - 1)) | Long.MIN_VALUE);
    bytes.putLong(lval);
    return bytes;
}
```

IEEE 754 浮点数有一个特性：同符号浮点数的位表示可视为符号-幅值整数，且保序。处理逻辑：
- 正数：翻转符号位 (类似整数处理)
- 负数：翻转所有位 (因为符号-幅值表示中负数的幅值越大数值越小)

`(lval >> 31) | Long.MIN_VALUE` 的效果：
- 正数 (符号位=0)：`>> 31` 得到全0，`| Long.MIN_VALUE` 得到 `0x8000...`，XOR 后只翻转符号位
- 负数 (符号位=1)：`>> 31` 得到全1，`| Long.MIN_VALUE` 仍为全1，XOR 后翻转所有位

#### 11.2.4 字符串编码

```java
public static ByteBuffer stringToOrderedBytes(
        String val, int length, ByteBuffer reuse, CharsetEncoder encoder) {
    ByteBuffer bytes = ByteBuffers.reuse(reuse, length);
    Arrays.fill(bytes.array(), 0, length, (byte) 0x00);  // 清零
    if (val != null) {
        CharBuffer inputBuffer = CharBuffer.wrap(val);
        encoder.encode(inputBuffer, bytes, true);  // UTF-8 编码，截断到 length
    }
    return bytes;
}
```

UTF-8 编码天然保持字典序，但 Z-Order 要求所有列贡献相同数量的字节。因此字符串被截断或填充到统一长度。默认长度为 `PRIMITIVE_BUFFER_SIZE = 8` 字节。

#### 11.2.5 位交织算法 (Bit Interleaving)

```java
public static byte[] interleaveBits(byte[][] columnsBinary, int interleavedSize, ByteBuffer reuse) {
    byte[] interleavedBytes = reuse.array();
    Arrays.fill(interleavedBytes, 0, interleavedSize, (byte) 0x00);
    
    int sourceColumn = 0;   // 当前取位的列
    int sourceByte = 0;     // 当前取位的字节
    int sourceBit = 7;      // 当前取位的 bit 位置 (高位优先)
    int interleaveByte = 0; // 输出字节位置
    int interleaveBit = 7;  // 输出 bit 位置
    
    while (interleaveByte < interleavedSize) {
        // 从源列取 1 个 bit，放到输出对应位置
        interleavedBytes[interleaveByte] |=
            (columnsBinary[sourceColumn][sourceByte] & 1 << sourceBit) 
                >>> sourceBit << interleaveBit;
        --interleaveBit;
        
        // ... 处理输出字节/bit 溢出
        // ... 轮转到下一个源列，如果所有列都取完一个 bit 则移到下一 bit 位置
    }
    return interleavedBytes;
}
```

**位交织示意** (2列，每列2字节)：

```
列 A:  a7 a6 a5 a4 a3 a2 a1 a0 | a15 a14 a13 a12 a11 a10 a9 a8
列 B:  b7 b6 b5 b4 b3 b2 b1 b0 | b15 b14 b13 b12 b11 b10 b9 b8

交织后: a7 b7 a6 b6 a5 b5 a4 b4 | a3 b3 a2 b2 a1 b1 a0 b0 | 
        a15 b15 a14 b14 ...
```

每个列的每一位被交替排列，使得排序时每个列都有相等的权重影响排序位置。这就是 Z-Order 空间填充曲线的本质——将多维坐标按位交织成一维值。

---

## 12. RewriteDataFiles —— 数据重排 Action API

**源码位置**: `api/src/main/java/org/apache/iceberg/actions/RewriteDataFiles.java`

`RewriteDataFiles` 是一个 Action 接口，定义了数据文件重写（compaction）的 API。

### 12.1 三种策略

```java
// BinPack: 仅合并小文件，不改变排序
default RewriteDataFiles binPack() { return this; }

// Sort: 按指定排序顺序重写（使用表默认排序或自定义排序）
default RewriteDataFiles sort() { ... }
default RewriteDataFiles sort(SortOrder sortOrder) { ... }

// Z-Order: 按多列 Z-Order 重写
default RewriteDataFiles zOrder(String... columns) { ... }
```

### 12.2 关键配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `max-file-group-size-bytes` | 100GB | 单个文件组最大大小 |
| `max-concurrent-file-group-rewrites` | 5 | 并发重写文件组数 |
| `target-file-size-bytes` | 表属性 | 目标文件大小 |
| `partial-progress.enabled` | false | 允许部分进度提交 |
| `partial-progress.max-commits` | 10 | 最大提交数 |
| `use-starting-sequence-number` | true | 使用起始序列号避免冲突 |
| `remove-dangling-deletes` | false | 清理悬空删除文件 |
| `rewrite-job-order` | none | 任务执行顺序 |

---

## 13. Spark 集成 —— 排序策略引擎实现

### 13.1 类继承体系

```
SparkDataFileRewriteRunner (抽象基类)
  └── SparkBinPackFileRewriteRunner  (BinPack 策略)
  └── SparkShufflingFileRewriteRunner (抽象, 需要 shuffle)
        ├── SparkSortFileRewriteRunner    (线性排序策略)
        └── SparkZOrderFileRewriteRunner  (Z-Order 策略)
```

### 13.2 SparkShufflingFileRewriteRunner —— 排序基座

**源码位置**: `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/actions/SparkShufflingFileRewriteRunner.java`

这是所有需要 shuffle 的排序策略的基类：

```java
abstract class SparkShufflingFileRewriteRunner extends SparkDataFileRewriteRunner {
    // 子类必须实现的三个方法
    protected abstract SortOrder sortOrder();
    protected abstract Dataset<Row> sortedDF(Dataset<Row> df, 
        Function<Dataset<Row>, Dataset<Row>> sortFunc);
    protected Schema sortSchema() { return table().schema(); }
    
    // 核心重写流程
    public void doRewrite(String groupId, RewriteFileGroup fileGroup) {
        Dataset<Row> scanDF = spark().read().format("iceberg")
            .option(SCAN_TASK_SET_ID, groupId).load(groupId);
        
        Dataset<Row> sortedDF = sortedDF(scanDF, sortFunction(...));
        
        sortedDF.write().format("iceberg")
            .option(REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
            .option(TARGET_FILE_SIZE_BYTES, fileGroup.maxOutputFileSize())
            .option(USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
            .mode("append").save(groupId);
    }
}
```

**`SHUFFLE_PARTITIONS_PER_FILE` 参数**：

当目标文件非常大 (例如 2GB) 时，单个 shuffle 分区可能导致内存不足。通过此参数可以将一个输出文件分成多个 shuffle 分区，然后通过 `OrderAwareCoalesce` 将排好序的分区合并成一个文件。例如设为 4，则 2GB 文件由 4 个 512MB 的 shuffle 分区合并而来。

### 13.3 SparkSortFileRewriteRunner —— 线性排序

**源码位置**: `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/actions/SparkSortFileRewriteRunner.java`

```java
class SparkSortFileRewriteRunner extends SparkShufflingFileRewriteRunner {
    private final SortOrder sortOrder;
    
    SparkSortFileRewriteRunner(SparkSession spark, Table table) {
        super(spark, table);
        this.sortOrder = table.sortOrder();  // 使用表默认排序
    }
    
    SparkSortFileRewriteRunner(SparkSession spark, Table table, SortOrder sortOrder) {
        super(spark, table);
        this.sortOrder = sortOrder;  // 使用自定义排序
    }
    
    @Override
    protected SortOrder sortOrder() { return sortOrder; }
    
    @Override
    protected Dataset<Row> sortedDF(Dataset<Row> df, 
            Function<Dataset<Row>, Dataset<Row>> sortFunc) {
        return sortFunc.apply(df);  // 直接应用排序函数
    }
}
```

### 13.4 SparkZOrderFileRewriteRunner —— Z-Order 排序

**源码位置**: `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/actions/SparkZOrderFileRewriteRunner.java`

```java
class SparkZOrderFileRewriteRunner extends SparkShufflingFileRewriteRunner {
    private static final String Z_COLUMN = "ICEZVALUE";
    
    // Z-Order 实际排序只有一个字段：按 Z 值升序
    private static final SortOrder Z_SORT_ORDER = SortOrder.builderFor(Z_SCHEMA)
        .sortBy(Z_COLUMN, SortDirection.ASC, NullOrder.NULLS_LAST)
        .build();
    
    @Override
    protected Dataset<Row> sortedDF(Dataset<Row> df, 
            Function<Dataset<Row>, Dataset<Row>> sortFunc) {
        // 1. 计算 Z 值并添加为新列
        Dataset<Row> zValueDF = df.withColumn(Z_COLUMN, zValue(df));
        // 2. 按 Z 值排序
        Dataset<Row> sortedDF = sortFunc.apply(zValueDF);
        // 3. 排序后删除 Z 值列（不写入数据文件）
        return sortedDF.drop(Z_COLUMN);
    }
    
    private Column zValue(Dataset<Row> df) {
        SparkZOrderUDF zOrderUDF = new SparkZOrderUDF(
            zOrderColNames.size(), varLengthContribution, maxOutputSize);
        
        // 将每列转换为有序字节数组
        Column[] zOrderCols = zOrderColNames.stream()
            .map(col -> zOrderUDF.sortedLexicographically(df.col(col.name()), col.dataType()))
            .toArray(Column[]::new);
        
        // 交织所有列的字节
        return zOrderUDF.interleaveBytes(array(zOrderCols));
    }
}
```

**Z-Order 的核心流程**：
```
原始数据行 → 各列转有序字节 → 位交织 → 单个二进制 Z 值 → 按 Z 值排序 → 写入数据文件
```

### 13.5 SparkZOrderUDF —— Spark UDF 实现

**源码位置**: `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/actions/SparkZOrderUDF.java`

为每种 Spark 数据类型提供了转换为有序字节数组的 UDF：

```java
Column sortedLexicographically(Column column, DataType type) {
    if (type instanceof ByteType)       return tinyToOrderedBytesUDF().apply(column);
    if (type instanceof ShortType)      return shortToOrderedBytesUDF().apply(column);
    if (type instanceof IntegerType)    return intToOrderedBytesUDF().apply(column);
    if (type instanceof LongType)       return longToOrderedBytesUDF().apply(column);
    if (type instanceof FloatType)      return floatToOrderedBytesUDF().apply(column);
    if (type instanceof DoubleType)     return doubleToOrderedBytesUDF().apply(column);
    if (type instanceof StringType)     return stringToOrderedBytesUDF().apply(column);
    if (type instanceof BinaryType)     return bytesTruncateUDF().apply(column);
    if (type instanceof BooleanType)    return booleanToOrderedBytesUDF().apply(column);
    if (type instanceof TimestampType)  return longToOrderedBytesUDF().apply(column.cast(LongType));
    if (type instanceof DateType)       return longToOrderedBytesUDF().apply(unix_date(column).cast(LongType));
    throw new IllegalArgumentException(...);
}
```

**性能优化**：
- 使用 `ThreadLocal` 缓存 `ByteBuffer` 和 `CharsetEncoder`，避免频繁分配
- `totalOutputBytes` 在 UDF 初始化时计算好，控制输出字节总数
- `maxOutputSize` 参数允许限制交织后的字节数，减少排序开销

### 13.6 SortOrderToSpark —— 排序转换

**源码位置**: `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/SortOrderToSpark.java`

通过 `SortOrderVisitor` 模式，将 Iceberg SortOrder 转换为 Spark DSv2 的 `SortOrder[]`：

```java
class SortOrderToSpark implements SortOrderVisitor<SortOrder> {
    public SortOrder field(String sourceName, int id, SortDirection direction, NullOrder nullOrder) {
        return Expressions.sort(
            Expressions.column(quotedName(id)), toSpark(direction), toSpark(nullOrder));
    }
    
    public SortOrder bucket(String sourceName, int id, int width, ...) {
        return Expressions.sort(
            Expressions.bucket(width, quotedName(id)), toSpark(direction), toSpark(nullOrder));
    }
    
    // year/month/day/hour 类似
}
```

---

## 14. Spark Procedure —— SQL 调用入口

**源码位置**: `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/procedures/RewriteDataFilesProcedure.java`

### 14.1 Procedure 参数

```java
private static final ProcedureParameter[] PARAMETERS = {
    requiredInParameter("table", StringType),       // 表名
    optionalInParameter("strategy", StringType),     // 策略: binpack/sort
    optionalInParameter("sort_order", StringType),   // 排序表达式
    optionalInParameter("options", STRING_MAP),       // 选项 Map
    optionalInParameter("where", StringType),         // 过滤条件
    optionalInParameter("branch", StringType)         // 目标分支
};
```

### 14.2 策略解析逻辑

`checkAndApplyStrategy` 方法处理策略和排序表达式：

```java
if (strategy == null || strategy.equalsIgnoreCase("sort")) {
    if (!zOrderTerms.isEmpty()) {
        // Z-Order 排序
        return action.zOrder(columnNames);
    } else if (!sortOrderFields.isEmpty()) {
        // 自定义排序
        return action.sort(buildSortOrder(sortOrderFields, schema));
    } else {
        // 使用表默认排序
        return action.sort();
    }
} else if (strategy.equalsIgnoreCase("binpack")) {
    return action.binPack();
}
```

**限制**：目前不支持同时混合使用普通排序列和 Z-Order 表达式。

### 14.3 SQL 调用语法

```sql
-- BinPack (默认)
CALL catalog.system.rewrite_data_files(
  table => 'db.table_name'
)

-- 线性排序
CALL catalog.system.rewrite_data_files(
  table => 'db.table_name',
  strategy => 'sort',
  sort_order => 'c1 ASC NULLS FIRST, c2 DESC NULLS LAST'
)

-- 使用 Transform 排序
CALL catalog.system.rewrite_data_files(
  table => 'db.table_name',
  strategy => 'sort',
  sort_order => 'year(ts) ASC, category ASC'
)

-- Z-Order 排序
CALL catalog.system.rewrite_data_files(
  table => 'db.table_name',
  strategy => 'sort',
  sort_order => 'zorder(c1, c2, c3)'
)

-- 带过滤和选项
CALL catalog.system.rewrite_data_files(
  table => 'db.table_name',
  strategy => 'sort',
  sort_order => 'zorder(c1, c2)',
  where => 'ts >= "2024-01-01"',
  options => map('max-concurrent-file-group-rewrites', '10')
)
```

---

## 15. DistributionMode —— 写入分布模式

**源码位置**: `api/src/main/java/org/apache/iceberg/DistributionMode.java`

`DistributionMode` 控制写入时数据的分布方式，与排序策略密切相关：

```java
public enum DistributionMode {
    NONE("none"),    // 不 shuffle，适合数据自然分布在少量分区的场景
    HASH("hash"),    // 按分区键 hash 分布，适合数据均匀分布的场景
    RANGE("range");  // 按分区键(或排序键)范围分布，适合数据倾斜的场景
}
```

**与排序的关系**：

当表配置了 `SortOrder` 时：
- `NONE` 模式下不会自动排序，需要写入引擎自行保证
- `HASH` 模式下数据先按分区键 hash 分区，但不保证分区内排序
- `RANGE` 模式下数据按排序键进行范围分区，保证全局有序

写入引擎（如 Spark）会根据 `write.distribution-mode` 属性和表的 `SortOrder` 自动决定如何 shuffle 和排序数据。

---

## 16. Table API 中的排序管理

**源码位置**: `api/src/main/java/org/apache/iceberg/Table.java`

```java
public interface Table {
    // 获取当前排序顺序
    SortOrder sortOrder();
    
    // 获取所有历史排序顺序（含当前）
    Map<Integer, SortOrder> sortOrders();
    
    // 替换表的排序顺序（排序演进）
    ReplaceSortOrder replaceSortOrder();
}
```

### 16.1 ReplaceSortOrder —— 排序演进

**源码位置**: `api/src/main/java/org/apache/iceberg/ReplaceSortOrder.java`

```java
public interface ReplaceSortOrder 
    extends PendingUpdate<SortOrder>, SortOrderBuilder<ReplaceSortOrder> {}
```

使用示例：
```java
table.replaceSortOrder()
    .asc("col1")
    .desc(Expressions.truncate("col2", 10))
    .asc("col3", NullOrder.NULLS_LAST)
    .commit();
```

替换排序顺序会创建一个新的 orderId，旧的排序顺序保留在 `sortOrders()` 映射中，用于读取历史快照时的元数据兼容。

---

## 17. 使用示例与最佳实践

### 17.1 Java API 构建排序顺序

```java
// 简单排序
SortOrder order = SortOrder.builderFor(schema)
    .asc("name")
    .desc("timestamp")
    .build();

// 带 Transform 的排序
SortOrder order = SortOrder.builderFor(schema)
    .asc(Expressions.year("event_time"))       // 按年排序
    .asc(Expressions.bucket("user_id", 16))    // 按 bucket 排序
    .desc("score", NullOrder.NULLS_LAST)       // 分数降序
    .build();

// 替换表排序（排序演进）
table.replaceSortOrder()
    .asc("region")
    .asc("city")
    .desc("created_at")
    .commit();
```

### 17.2 Spark SQL 数据重排

```sql
-- 使用表默认排序重写数据文件
CALL catalog.system.rewrite_data_files(
  table => 'db.events',
  strategy => 'sort'
);

-- 自定义排序重写
CALL catalog.system.rewrite_data_files(
  table => 'db.events',
  strategy => 'sort',
  sort_order => 'event_date ASC, user_id ASC'
);

-- Z-Order 重写，优化多维查询
CALL catalog.system.rewrite_data_files(
  table => 'db.events',
  strategy => 'sort',
  sort_order => 'zorder(user_id, event_date, region)'
);
```

### 17.3 Spark Java API 数据重排

```java
import org.apache.iceberg.spark.actions.SparkActions;

// BinPack（默认，仅合并小文件）
SparkActions.get()
    .rewriteDataFiles(table)
    .execute();

// 线性排序
SparkActions.get()
    .rewriteDataFiles(table)
    .sort(SortOrder.builderFor(table.schema())
        .sortBy("date", SortDirection.ASC, NullOrder.NULLS_FIRST)
        .sortBy("id", SortDirection.ASC, NullOrder.NULLS_FIRST)
        .build())
    .execute();

// Z-Order 排序
SparkActions.get()
    .rewriteDataFiles(table)
    .zOrder("user_id", "event_date", "region")
    .option("max-concurrent-file-group-rewrites", "10")
    .execute();
```

### 17.4 最佳实践

1. **选择排序列**：优先选择高频查询过滤条件中的列
2. **排序列数量**：线性排序一般不超过 3-4 列（后面的列贡献递减），Z-Order 可以支持更多列
3. **分区与排序配合**：分区处理粗粒度过滤，排序处理细粒度过滤
4. **Z-Order 列选择**：选择查询频率相近的列，如果某列远高于其他列，考虑用线性排序
5. **避免分区列参与 Z-Order**：分区列在每个分区内是常量，参与 Z-Order 无意义（代码会自动跳过）
6. **定期执行重排**：随着数据写入，排序效果会逐渐退化，建议定期执行 compaction

---

## 18. 各排序策略的对比分析

### 18.1 线性排序 (Linear Sort) vs Z-Order

```
线性排序 [A, B, C]:
  
  优先级: A >> B >> C
  
  查询 WHERE A = x         → 极好 (数据完全聚簇)
  查询 WHERE A = x AND B = y → 极好
  查询 WHERE B = y          → 差 (A 未过滤时 B 分散)
  查询 WHERE C = z          → 差 (前面列未过滤时 C 极度分散)
  
  适用: 查询模式固定，总是先按 A 过滤

Z-Order [A, B, C]:
  
  优先级: A ≈ B ≈ C (均等权重)
  
  查询 WHERE A = x         → 好 (部分聚簇)
  查询 WHERE B = y          → 好 (部分聚簇)
  查询 WHERE C = z          → 好 (部分聚簇)
  查询 WHERE A = x AND B = y → 好 (多维聚簇)
  
  适用: 多种查询模式，不确定哪个列最重要
```

### 18.2 Z-Order 的理论基础

Z-Order (Morton Code) 是一种空间填充曲线，将多维空间映射到一维空间。其核心性质是：**多维空间中相近的点在一维映射后也趋于相近**。

```
二维空间 (x, y):          Z-Order 一维映射:
                           
  3 | 10  11  14  15       x: 0 1 2 3 0 1 2 3 0 1 2 3 0 1 2 3
  2 | 08  09  12  13       y: 0 0 0 0 1 1 1 1 2 2 2 2 3 3 3 3
  1 | 02  03  06  07       z: 0 1 4 5 2 3 6 7 8 9 c d a b e f
  0 | 00  01  04  05       
    +--+---+---+---        Z 形遍历路径保持空间局部性
      0   1   2   3
```

### 18.3 性能特征

| 特性 | BinPack | Linear Sort | Z-Order |
|------|---------|-------------|---------|
| 文件合并 | 是 | 是 | 是 |
| Shuffle 开销 | 无 | 中 | 高 |
| 单列范围查询 | 无优化 | 首列极好 | 好 |
| 多列联合查询 | 无优化 | 首列极好，后续列递减 | 所有列均好 |
| 压缩率提升 | 无 | 显著 | 中等 |
| 写入速度 | 最快 | 中 | 最慢 |

---

## 19. 源码文件索引

### 核心 API 层 (api/)

| 文件 | 说明 |
|------|------|
| `api/.../SortOrder.java` | 排序顺序核心类，包含 Builder |
| `api/.../SortField.java` | 单个排序字段定义 |
| `api/.../SortOrderBuilder.java` | 排序构建器接口 (流式 API) |
| `api/.../SortOrderComparators.java` | 排序比较器工厂 |
| `api/.../SortKey.java` | 排序键提取器 |
| `api/.../SortDirection.java` | 排序方向枚举 (ASC/DESC) |
| `api/.../NullOrder.java` | NULL 排序枚举 |
| `api/.../UnboundSortOrder.java` | 未绑定排序顺序 (序列化用) |
| `api/.../ReplaceSortOrder.java` | 排序演进接口 |
| `api/.../DistributionMode.java` | 写入分布模式枚举 |
| `api/.../Table.java` | 表接口 (sortOrder/replaceSortOrder) |
| `api/.../actions/RewriteDataFiles.java` | 数据重写 Action 接口 |
| `api/.../transforms/SortOrderVisitor.java` | 排序访问者模式 |

### 核心实现层 (core/)

| 文件 | 说明 |
|------|------|
| `core/.../SortOrderParser.java` | 排序 JSON 序列化/反序列化 |
| `core/.../util/SortOrderUtil.java` | 分区与排序协同工具 |
| `core/.../util/ZOrderByteUtils.java` | Z-Order 字节编码核心 |
| `core/.../expressions/Zorder.java` | Z-Order 表达式 Term |

### Spark 集成层 (spark/v4.1/)

| 文件 | 说明 |
|------|------|
| `spark/.../actions/RewriteDataFilesSparkAction.java` | Spark 数据重写入口 |
| `spark/.../actions/SparkShufflingFileRewriteRunner.java` | Shuffle 排序基座 |
| `spark/.../actions/SparkSortFileRewriteRunner.java` | 线性排序 Runner |
| `spark/.../actions/SparkZOrderFileRewriteRunner.java` | Z-Order Runner |
| `spark/.../actions/SparkZOrderUDF.java` | Z-Order Spark UDF |
| `spark/.../SortOrderToSpark.java` | Iceberg → Spark 排序转换 |
| `spark/.../procedures/RewriteDataFilesProcedure.java` | SQL Procedure 入口 |

### 测试与基准 (test/jmh/)

| 文件 | 说明 |
|------|------|
| `core/.../util/TestZOrderByteUtil.java` | Z-Order 字节工具测试 |
| `core/.../util/ZOrderByteUtilsBenchmark.java` | Z-Order 性能基准 |
| `spark/.../action/IcebergSortCompactionBenchmark.java` | Sort/Z-Order 压缩基准 |
| `spark/.../extensions/TestRewriteDataFilesProcedure.java` | Procedure 集成测试 |

---

> **总结**：Iceberg 的排序体系从底层的 `SortField`/`SortOrder` 数据模型，到中间的 `SortOrderVisitor` 分发机制和 `SortOrderUtil` 分区协同算法，再到上层 Spark 的 `SparkSortFileRewriteRunner`/`SparkZOrderFileRewriteRunner` 引擎实现，形成了一套完整的多层排序架构。Z-Order 通过 `ZOrderByteUtils` 的符号位翻转、IEEE 754 位转换、UTF-8 截断填充、位交织等算法，将多维排序问题优雅地转化为一维字节排序问题。

# Apache Iceberg 分区演进（Partition Evolution）机制深度源码分析

## 目录

- [1. 概述](#1-概述)
- [2. 核心数据模型](#2-核心数据模型)
  - [2.1 PartitionSpec 分区规范](#21-partitionspec-分区规范)
  - [2.2 PartitionField 分区字段](#22-partitionfield-分区字段)
  - [2.3 Transform 变换体系](#23-transform-变换体系)
- [3. 分区演进的实现机制](#3-分区演进的实现机制)
  - [3.1 UpdatePartitionSpec 接口](#31-updatepartitionspec-接口)
  - [3.2 BaseUpdatePartitionSpec 核心实现](#32-baseupdatepartitionspec-核心实现)
  - [3.3 V1 与 V2 格式的差异处理](#33-v1-与-v2-格式的差异处理)
  - [3.4 Partition Field ID 的回收机制](#34-partition-field-id-的回收机制)
- [4. 分区演进不需要重写数据的原理](#4-分区演进不需要重写数据的原理)
  - [4.1 Manifest 文件与 specId 绑定](#41-manifest-文件与-specid-绑定)
  - [4.2 TableMetadata 中多 spec 共存](#42-tablemetadata-中多-spec-共存)
- [5. 跨 Spec 的查询规划与过滤](#5-跨-spec-的查询规划与过滤)
  - [5.1 ManifestGroup 的 spec 分发机制](#51-manifestgroup-的-spec-分发机制)
  - [5.2 Projections 表达式投影](#52-projections-表达式投影)
  - [5.3 ResidualEvaluator 残余表达式计算](#53-residualevaluator-残余表达式计算)
  - [5.4 ManifestEvaluator 清单级过滤](#54-manifestevaluator-清单级过滤)
- [6. 引擎层实现路径](#6-引擎层实现路径)
  - [6.1 Spark SQL 扩展实现](#61-spark-sql-扩展实现)
  - [6.2 Flink 动态分区演进实现](#62-flink-动态分区演进实现)
- [7. 分区演进对数据写入路径的影响](#7-分区演进对数据写入路径的影响)
- [8. 实际业务场景示例](#8-实际业务场景示例)
- [9. 注意事项与最佳实践](#9-注意事项与最佳实践)
- [10. 总结](#10-总结)

---

## 1. 概述

Apache Iceberg 的分区演进（Partition Evolution）是其最重要的设计特性之一。与传统 Hive 表不同，Iceberg 允许在不重写已有数据的情况下，对表的分区策略进行修改——包括添加新分区字段、删除现有分区字段、以及将分区 transform 从一种类型替换为另一种类型（如从 `day(ts)` 变为 `hour(ts)`）。

这一能力的核心在于：**Iceberg 将分区规范（PartitionSpec）与每个 Manifest 文件绑定，而非与整个表绑定**。每个 Manifest 文件记录了它使用的 `specId`，表的元数据（TableMetadata）中维护了所有历史 spec 的完整列表。在查询规划时，引擎根据每个 Manifest 的 specId 获取对应的 PartitionSpec，从而正确地进行分区过滤和谓词投影。

---

## 2. 核心数据模型

### 2.1 PartitionSpec 分区规范

`PartitionSpec` 是描述一张表如何进行分区的核心数据结构。

**源码位置**: `api/src/main/java/org/apache/iceberg/PartitionSpec.java`

```java
// 第53-75行
public class PartitionSpec implements Serializable {
  // IDs for partition fields start at 1000
  private static final int PARTITION_DATA_ID_START = 1000;

  private final Schema schema;
  private final int specId;
  private final PartitionField[] fields;
  private transient volatile StructType lazyPartitionType = null;
  private final int lastAssignedFieldId;

  private PartitionSpec(
      Schema schema, int specId, List<PartitionField> fields, int lastAssignedFieldId) {
    this.schema = schema;
    this.specId = specId;
    this.fields = fields.toArray(new PartitionField[0]);
    this.lastAssignedFieldId = lastAssignedFieldId;
  }
}
```

关键属性说明：

| 属性 | 类型 | 说明 |
|------|------|------|
| `specId` | `int` | 分区规范的唯一标识符，在 TableMetadata 中全局递增 |
| `fields` | `PartitionField[]` | 有序的分区字段数组，定义了分区数据的结构 |
| `schema` | `Schema` | 此 spec 关联的表 schema |
| `lastAssignedFieldId` | `int` | 此 spec 中最后分配的字段 ID，用于保证 ID 单调递增 |

**`partitionType()` 方法**（第126-150行）是一个重要的派生属性，它根据 fields 中每个字段的 transform 和 source type，生成一个 `StructType`，用于描述分区数据的类型结构：

```java
// 第126-150行
public StructType partitionType() {
  if (lazyPartitionType == null) {
    synchronized (this) {
      if (lazyPartitionType == null) {
        List<Types.NestedField> structFields = Lists.newArrayListWithExpectedSize(fields.length);
        for (PartitionField field : fields) {
          Type sourceType = schema.findType(field.sourceId());
          Type resultType = field.transform().getResultType(sourceType);
          if (sourceType == null) {
            resultType = Types.UnknownType.get();
          }
          structFields.add(Types.NestedField.optional(field.fieldId(), field.name(), resultType));
        }
        this.lazyPartitionType = Types.StructType.of(structFields);
      }
    }
  }
  return lazyPartitionType;
}
```

**`PARTITION_DATA_ID_START = 1000`** 是一个关键常量——分区字段 ID 从 1000 开始，与 schema 列 ID 空间分开，避免冲突。

### 2.2 PartitionField 分区字段

`PartitionField` 是 PartitionSpec 中的基本元素，描述了"如何从源列生成分区值"。

**源码位置**: `api/src/main/java/org/apache/iceberg/PartitionField.java`

```java
// 第26-37行
public class PartitionField implements Serializable {
  private final int sourceId;    // 源 schema 中的列 ID
  private final int fieldId;     // 分区字段的全局唯一 ID（跨所有 spec）
  private final String name;     // 分区字段名称
  private final Transform<?, ?> transform;  // 变换函数

  PartitionField(int sourceId, int fieldId, String name, Transform<?, ?> transform) {
    this.sourceId = sourceId;
    this.fieldId = fieldId;
    this.name = name;
    this.transform = transform;
  }
}
```

关键设计要点：

- **`sourceId`**: 指向表 schema 中的源列。例如对 `day(event_time)` 分区，sourceId 指向 `event_time` 列的 fieldId。
- **`fieldId`**: 分区字段自身的唯一标识符，在整个表的所有 spec 中全局唯一。这是分区演进的关键——即使同一个分区字段出现在不同的 spec 中，它的 fieldId 也保持一致。
- **`transform`**: 定义了如何将源列值转换为分区值。

### 2.3 Transform 变换体系

Transform 是分区的核心抽象，定义了从源数据值到分区值的映射。

**源码位置**: `api/src/main/java/org/apache/iceberg/transforms/Transform.java`

Transform 接口的关键方法：

```java
// 第39行起
public interface Transform<S, T> extends Serializable {
  // 绑定到具体类型
  default SerializableFunction<S, T> bind(Type type) { ... }
  
  // 检查是否能应用于给定类型
  boolean canTransform(Type type);
  
  // 获取变换后的结果类型
  Type getResultType(Type sourceType);
  
  // 是否保持排序（单调性）
  default boolean preservesOrder() { return false; }
  
  // 当前 transform 的排序是否满足另一个 transform 的排序
  default boolean satisfiesOrderOf(Transform<?, ?> other) { return equals(other); }
  
  // 包容性投影：用于分区裁剪
  UnboundPredicate<T> project(String name, BoundPredicate<S> predicate);
  
  // 严格投影：用于分区裁剪
  UnboundPredicate<T> projectStrict(String name, BoundPredicate<S> predicate);
}
```

**Iceberg 内置 Transform 类型**（通过 `Transforms` 工厂类创建）：

| Transform | 说明 | 返回类型 | 示例 |
|-----------|------|----------|------|
| `identity` | 恒等变换，分区值等于源值 | 源类型 | `identity(user_id)` |
| `bucket[N]` | 哈希取模分桶 | Integer | `bucket[16](user_id)` |
| `truncate[W]` | 截断到指定宽度 | 源类型 | `truncate[10](name)` |
| `year` | 提取年份 | Integer | `year(event_time)` |
| `month` | 提取月份（从 epoch 起） | Integer | `month(event_time)` |
| `day` | 提取日期 | DateType | `day(event_time)` |
| `hour` | 提取小时 | Integer | `hour(event_time)` |
| `void` | 总是返回 null（用于删除分区字段的 V1 兼容） | Void | - |

**时间 Transform 的继承体系**（`TimeTransform`）：

`TimeTransform` 是 `year`、`month`、`day`、`hour` 四个时间变换的基类（`api/src/main/java/org/apache/iceberg/transforms/TimeTransform.java`），其中实现了关键的 `satisfiesOrderOf` 方法：

```java
// TimeTransform.java 第60-75行
@Override
public boolean satisfiesOrderOf(Transform<?, ?> other) {
  if (this == other) {
    return true;
  }
  if (other instanceof Dates) {
    return TransformUtil.satisfiesOrderOf(granularity(), ((Dates) other).granularity());
  } else if (other instanceof Timestamps) {
    return TransformUtil.satisfiesOrderOf(granularity(), ((Timestamps) other).granularity());
  } else if (other instanceof TimeTransform) {
    return TransformUtil.satisfiesOrderOf(
        granularity(), ((TimeTransform<?>) other).granularity());
  }
  return false;
}
```

这意味着 `day(ts)` 的排序可以满足 `month(ts)` 和 `year(ts)` 的排序要求，但不能满足 `hour(ts)` 的要求。这在分区演进后的排序优化中非常关键。

**`VoidTransform`**（`api/src/main/java/org/apache/iceberg/transforms/VoidTransform.java`）是一个特殊的 transform：

```java
// VoidTransform.java（关键方法节选，分布在第28-93行）
class VoidTransform<S> implements Transform<S, Void> {
  // 第61行
  @Override
  public Void apply(Object value) { return null; }

  // 第86行
  @Override
  public UnboundPredicate<Void> project(String name, BoundPredicate<S> predicate) {
    return null;  // 总是返回 null，不参与分区过滤
  }

  // 第91行
  @Override
  public boolean isVoid() { return true; }
}
```

VoidTransform 在 V1 格式中用于"删除"分区字段——由于 V1 要求所有 spec 的字段 ID 顺序排列，被删除的字段会被替换为 `void` 而非真正移除。

---

## 3. 分区演进的实现机制

### 3.1 UpdatePartitionSpec 接口

**源码位置**: `api/src/main/java/org/apache/iceberg/UpdatePartitionSpec.java`

这个接口定义了分区演进的 API：

```java
// 第31行起
public interface UpdatePartitionSpec extends PendingUpdate<PartitionSpec> {
  // 大小写敏感设置
  UpdatePartitionSpec caseSensitive(boolean isCaseSensitive);
  
  // 添加 identity 分区字段
  UpdatePartitionSpec addField(String sourceName);
  
  // 通过 Term 添加分区字段（支持 transform）
  UpdatePartitionSpec addField(Term term);
  
  // 通过 Term 添加带名称的分区字段
  UpdatePartitionSpec addField(String name, Term term);
  
  // 按名称移除分区字段
  UpdatePartitionSpec removeField(String name);
  
  // 按 Term 移除分区字段
  UpdatePartitionSpec removeField(Term term);
  
  // 重命名分区字段
  UpdatePartitionSpec renameField(String name, String newName);
}
```

### 3.2 BaseUpdatePartitionSpec 核心实现

**源码位置**: `core/src/main/java/org/apache/iceberg/BaseUpdatePartitionSpec.java`

这是分区演进的核心实现类，采用了**延迟求值（lazy evaluation）** 模式——所有修改操作先记录在内部数据结构中，最终在 `apply()` 或 `commit()` 时一次性生成新的 PartitionSpec。

**构造函数**（第65-85行）：

```java
BaseUpdatePartitionSpec(TableOperations ops) {
  this.ops = ops;
  this.base = ops.current();
  this.formatVersion = base.formatVersion();
  this.spec = base.spec();             // 当前默认 spec
  this.schema = spec.schema();
  this.nameToField = indexSpecByName(spec);
  this.transformToField = indexSpecByTransform(spec);
  this.lastAssignedPartitionId = base.lastAssignedPartitionId();
  
  // 不允许对包含未知 transform 的 spec 进行演进
  spec.fields().stream()
      .filter(field -> field.transform() instanceof UnknownTransform)
      .findAny()
      .ifPresent(field -> {
        throw new IllegalArgumentException(
            "Cannot update partition spec with unknown transform: " + field);
      });
}
```

**内部状态管理**：

```java
// 第53-59行
private final List<PartitionField> adds = Lists.newArrayList();          // 新增的字段
private final Map<Integer, PartitionField> addedTimeFields = Maps.newHashMap(); // 防重复时间字段
private final Map<Pair<Integer, String>, PartitionField> transformToAddedField = Maps.newHashMap();
private final Map<String, PartitionField> nameToAddedField = Maps.newHashMap();
private final Set<Object> deletes = Sets.newHashSet();                   // 待删除的字段 ID
private final Map<String, String> renames = Maps.newHashMap();           // 重命名映射
```

**添加字段的核心逻辑**（`addField` 方法，第178-241行）：

```java
@Override
public BaseUpdatePartitionSpec addField(String name, Term term) {
  // 1. 检查不能重复添加
  PartitionField alreadyAdded = nameToAddedField.get(name);
  Preconditions.checkArgument(alreadyAdded == null, ...);

  // 2. 解析 Term 为 (sourceId, transform) 对
  Pair<Integer, Transform<?, ?>> sourceTransform = resolve(term);
  Pair<Integer, String> validationKey =
      Pair.of(sourceTransform.first(), sourceTransform.second().toString());

  // 3. 检查是否是"先删后加同一个字段"——如果是，则撤销删除
  PartitionField existing = transformToField.get(validationKey);
  if (existing != null && deletes.contains(existing.fieldId())
      && existing.transform().equals(sourceTransform.second())) {
    return rewriteDeleteAndAddField(existing, name);
  }

  // 4. 尝试回收历史 spec 中的字段 ID
  PartitionField newField = recycleOrCreatePartitionField(sourceTransform, name);
  
  // 5. 自动生成分区字段名
  if (newField.name() == null) {
    String partitionName = PartitionSpecVisitor.visit(schema, newField, PartitionNameGenerator.INSTANCE);
    newField = new PartitionField(
        newField.sourceId(), newField.fieldId(), partitionName, newField.transform());
  }

  // 6. 检查时间 transform 的冲突（同一源字段不能有两个时间 transform）
  checkForRedundantAddedPartitions(newField);
  
  adds.add(newField);
  return this;
}
```

**apply() 方法——生成新的 PartitionSpec**（第304-335行）：

```java
@Override
public PartitionSpec apply() {
  PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);

  for (PartitionField field : spec.fields()) {
    if (!deletes.contains(field.fieldId())) {
      // 保留未删除的字段（可能需要重命名）
      String newName = renames.get(field.name());
      if (newName != null) {
        builder.add(field.sourceId(), field.fieldId(), newName, field.transform());
      } else {
        builder.add(field.sourceId(), field.fieldId(), field.name(), field.transform());
      }
    } else if (formatVersion < 2) {
      // V1 格式：被删除的字段替换为 void transform（保持 ID 连续）
      String newName = renames.get(field.name());
      if (newName != null) {
        builder.add(field.sourceId(), field.fieldId(), newName, Transforms.alwaysNull());
      } else {
        builder.add(field.sourceId(), field.fieldId(), field.name(), Transforms.alwaysNull());
      }
    }
    // V2 格式：直接不包含被删除的字段
  }

  // 添加新字段
  for (PartitionField newField : adds) {
    builder.add(newField.sourceId(), newField.fieldId(), newField.name(), newField.transform());
  }

  return builder.build();
}
```

**commit() 方法**（第338-346行）——最终提交到 TableMetadata：

```java
@Override
public void commit() {
  TableMetadata update;
  if (setAsDefault) {
    update = base.updatePartitionSpec(apply());   // 设为默认 spec
  } else {
    update = base.addPartitionSpec(apply());      // 仅添加，不设为默认
  }
  ops.commit(base, update);  // 乐观并发提交
}
```

### 3.3 V1 与 V2 格式的差异处理

分区演进在 V1 和 V2 格式中有本质区别：

**V1 格式**：
- 字段 ID 必须从 1000 开始连续递增
- 删除分区字段不能真正移除，而是替换为 `VoidTransform`（`alwaysNull()`）
- 这是为了保证所有 spec 中字段的位置一致，因为 V1 的 Manifest 文件可能不包含字段 ID 信息

**V2 格式**：
- 字段 ID 是全局唯一的，不要求连续
- 删除的字段直接从新 spec 中移除
- 字段 ID 可以在不同的 spec 间重用（通过 `recycleOrCreatePartitionField` 机制）

在 `apply()` 方法中（第315-323行）可以看到这个关键区别：

```java
} else if (formatVersion < 2) {
  // V1: 替换为 void
  builder.add(field.sourceId(), field.fieldId(), field.name(), Transforms.alwaysNull());
}
// V2: 直接跳过（不添加）
```

### 3.4 Partition Field ID 的回收机制

**源码位置**: `BaseUpdatePartitionSpec.java` 第122-144行

这是一个精妙的设计——当 V2 格式下重新添加一个之前存在过的分区字段时，系统会尝试从历史 spec 中回收其 field ID：

```java
private PartitionField recycleOrCreatePartitionField(
    Pair<Integer, Transform<?, ?>> sourceTransform, String name) {
  if (formatVersion >= 2 && base != null) {
    int sourceId = sourceTransform.first();
    Transform<?, ?> transform = sourceTransform.second();

    // 收集所有历史 spec 中的字段
    Set<PartitionField> allHistoricalFields = Sets.newHashSet();
    for (PartitionSpec partitionSpec : base.specs()) {
      allHistoricalFields.addAll(partitionSpec.fields());
    }

    // 查找匹配的历史字段
    for (PartitionField field : allHistoricalFields) {
      if (field.sourceId() == sourceId && field.transform().equals(transform)) {
        if (name == null || field.name().equals(name)) {
          return field;  // 回收历史字段，保持 fieldId 一致
        }
      }
    }
  }
  // 没找到匹配的历史字段，创建新的
  return new PartitionField(
      sourceTransform.first(), assignFieldId(), name, sourceTransform.second());
}
```

这确保了：如果用户先删除 `day(ts)`、再重新添加 `day(ts)`，新字段会复用旧字段的 ID，使得历史数据的分区字段 ID 与新数据保持一致。

---

## 4. 分区演进不需要重写数据的原理

### 4.1 Manifest 文件与 specId 绑定

这是 Iceberg 分区演进"零成本"的核心机制。每个 Manifest 文件在创建时就记录了它使用的 `partition_spec_id`：

**源码位置**: `api/src/main/java/org/apache/iceberg/ManifestFile.java`

```java
// 第36-37行
Types.NestedField SPEC_ID =
    required(502, "partition_spec_id", Types.IntegerType.get(), "Spec ID used to write");

// 第130行
int partitionSpecId();
```

**ManifestWriter** 在写入时绑定 specId：

**源码位置**: `core/src/main/java/org/apache/iceberg/ManifestWriter.java`

```java
// 第45-46行
private final int specId;
// ...
// 第69行
this.specId = spec.specId();
```

写入 Manifest 文件元数据时记录 specId（第222-227行）：

```java
return new GenericManifestFile(
    file.location(),
    writer.length(),
    specId,    // 关键：specId 写入到 Manifest 文件
    content(),
    UNASSIGNED_SEQ,
    minSeqNumber,
    snapshotId,
    ...);
```

同样，每个 `ContentFile`（DataFile/DeleteFile）也记录了自己的 specId：

```java
// DataFile.java 第100行
Types.NestedField SPEC_ID = optional(141, "spec_id", IntegerType.get(), "Partition spec ID");
```

**这种设计意味着**：
1. 旧数据的 Manifest 文件依然关联着旧的 specId，其分区数据的结构由旧 spec 定义
2. 新数据写入时使用新的默认 spec，生成新的 Manifest 文件
3. 两者共存于同一个 snapshot 中，不需要修改任何已有文件

### 4.2 TableMetadata 中多 spec 共存

**源码位置**: `core/src/main/java/org/apache/iceberg/TableMetadata.java`

TableMetadata 维护了所有历史 spec 的列表：

```java
// 第252-260行
private final int defaultSpecId;
private final List<PartitionSpec> specs;      // 所有 spec 列表
private final Map<Integer, PartitionSpec> specsById;  // specId -> spec 映射
```

查询时通过 `specsById` 映射获取对应的 spec：

```java
// 第448-462行
public PartitionSpec spec() {
  return specsById.get(defaultSpecId);  // 当前默认 spec
}

public PartitionSpec spec(int id) {
  return specsById.get(id);  // 按 ID 查找历史 spec
}

public List<PartitionSpec> specs() {
  return specs;  // 所有 spec
}
```

当添加新的 PartitionSpec 时（`addPartitionSpecInternal`，第1655-1686行），系统会先检查是否已有兼容的 spec 可以复用：

```java
private int addPartitionSpecInternal(PartitionSpec spec) {
  int newSpecId = reuseOrCreateNewSpecId(spec);  // 尝试复用
  if (specsById.containsKey(newSpecId)) {
    return newSpecId;  // 已有兼容 spec，直接复用
  }
  // 检查兼容性并添加新 spec
  PartitionSpec newSpec = freshSpec(newSpecId, schema, spec);
  this.lastAssignedPartitionId =
      Math.max(lastAssignedPartitionId, newSpec.lastAssignedFieldId());
  specs.add(newSpec);
  specsById.put(newSpecId, newSpec);
  return newSpecId;
}

// 第1688-1700行
private int reuseOrCreateNewSpecId(PartitionSpec newSpec) {
  int newSpecId = INITIAL_SPEC_ID;
  for (PartitionSpec spec : specs) {
    if (newSpec.compatibleWith(spec)) {
      return spec.specId();           // 找到兼容 spec，复用其 ID
    } else if (newSpecId <= spec.specId()) {
      newSpecId = spec.specId() + 1;  // 否则递增生成新 ID
    }
  }
  return newSpecId;
}
```

---

## 5. 跨 Spec 的查询规划与过滤

### 5.1 ManifestGroup 的 spec 分发机制

`ManifestGroup` 是查询规划的核心协调器，它负责将用户的过滤条件正确地应用到使用不同 spec 的 Manifest 文件上。

**源码位置**: `core/src/main/java/org/apache/iceberg/ManifestGroup.java`

在 `plan()` 方法（第181-221行）中，ManifestGroup 使用了基于 specId 的缓存来为每个 spec 创建独立的 ResidualEvaluator：

```java
public <T extends ScanTask> CloseableIterable<T> plan(CreateTasksFunction<T> createTasksFunc) {
  // 按 specId 缓存 ResidualEvaluator——每个 spec 有独立的残余表达式计算器
  LoadingCache<Integer, ResidualEvaluator> residualCache =
      Caffeine.newBuilder()
          .build(specId -> {
            PartitionSpec spec = specsById.get(specId);
            Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : dataFilter;
            return ResidualEvaluator.of(spec, filter, caseSensitive);
          });

  // 按 specId 缓存 TaskContext
  LoadingCache<Integer, TaskContext> taskContextCache =
      Caffeine.newBuilder()
          .build(specId -> {
            PartitionSpec spec = specsById.get(specId);
            ResidualEvaluator residuals = residualCache.get(specId);
            return new TaskContext(spec, deleteFiles, residuals, dropStats, ...);
          });

  // 对每个 Manifest，使用其 specId 获取对应的 TaskContext
  Iterable<CloseableIterable<T>> tasks = entries((manifest, entries) -> {
    int specId = manifest.partitionSpecId();
    TaskContext taskContext = taskContextCache.get(specId);
    return createTasksFunc.apply(entries, taskContext);
  });
}
```

在 `entries()` 方法（第276-391行）中，同样为每个 spec 创建了独立的 ManifestEvaluator：

```java
// 第279-292行
LoadingCache<Integer, ManifestEvaluator> evalCache =
    Caffeine.newBuilder()
        .build(specId -> {
          PartitionSpec spec = specsById.get(specId);
          return ManifestEvaluator.forPartitionFilter(
              Expressions.and(
                  partitionFilter,
                  Projections.inclusive(spec, caseSensitive).project(dataFilter)),
              spec,
              caseSensitive);
        });
```

**关键逻辑**：对于同一个用户过滤条件（如 `ts > '2024-01-01'`），不同的 spec 会产生不同的分区过滤表达式。使用 `day(ts)` spec 的 Manifest 会得到 `ts_day >= 2024-01-01` 的过滤条件；而使用 `hour(ts)` spec 的 Manifest 会得到 `ts_hour >= 2024-01-01-00` 的过滤条件。

### 5.2 Projections 表达式投影

**源码位置**: `api/src/main/java/org/apache/iceberg/expressions/Projections.java`

Projections 负责将行级过滤表达式转化为分区级过滤表达式。它有两种模式：

**包容性投影（Inclusive Projection）**（第195-227行）——保证不遗漏任何匹配行所在的分区：

```java
private static class InclusiveProjection extends BaseProjectionEvaluator {
  @Override
  public <T> Expression predicate(BoundPredicate<T> pred) {
    Collection<PartitionField> parts = spec().getFieldsBySourceId(pred.ref().fieldId());
    if (parts == null) {
      return Expressions.alwaysTrue();  // 无分区列，不能裁剪
    }

    Expression result = Expressions.alwaysTrue();
    for (PartitionField part : parts) {
      UnboundPredicate<?> inclusiveProjection =
          ((Transform<T, ?>) part.transform()).project(part.name(), pred);
      if (inclusiveProjection != null) {
        result = Expressions.and(result, inclusiveProjection);
      }
    }
    return result;
  }
}
```

**严格投影（Strict Projection）**（第229-259行）——保证匹配的分区中所有行都满足条件：

```java
private static class StrictProjection extends BaseProjectionEvaluator {
  @Override
  public <T> Expression predicate(BoundPredicate<T> pred) {
    Collection<PartitionField> parts = spec().getFieldsBySourceId(pred.ref().fieldId());
    if (parts == null) {
      return Expressions.alwaysFalse();  // 无分区列，不能断言
    }

    Expression result = Expressions.alwaysFalse();
    for (PartitionField part : parts) {
      UnboundPredicate<?> strictProjection =
          ((Transform<T, ?>) part.transform()).projectStrict(part.name(), pred);
      if (strictProjection != null) {
        result = Expressions.or(result, strictProjection);
      }
    }
    return result;
  }
}
```

注意包容性投影使用 `AND` 组合多个分区字段的投影结果（更严格地过滤），而严格投影使用 `OR`（任一字段满足即可断言）。

### 5.3 ResidualEvaluator 残余表达式计算

**源码位置**: `api/src/main/java/org/apache/iceberg/expressions/ResidualEvaluator.java`

在分区裁剪后，一些数据文件虽然被保留（因为其分区值可能匹配），但并非所有行都满足条件。ResidualEvaluator 计算的"残余表达式"就是这些需要在行级继续应用的过滤条件。

其核心 `predicate()` 方法（第227-288行）结合了严格投影和包容性投影：

```java
@Override
public <T> Expression predicate(BoundPredicate<T> pred) {
  List<PartitionField> parts = spec.getFieldsBySourceId(pred.ref().fieldId());
  if (parts == null) {
    return pred;  // 非分区列，保留原始谓词
  }

  for (PartitionField part : parts) {
    // 严格投影：如果严格投影为 true，原始谓词一定为 true，可以消除
    UnboundPredicate<?> strictProjection =
        ((Transform<T, ?>) part.transform()).projectStrict(part.name(), pred);
    // ... 绑定并求值 ...
    if (strictResult != null && strictResult.op() == Expression.Operation.TRUE) {
      return Expressions.alwaysTrue();
    }

    // 包容性投影：如果包容性投影为 false，原始谓词一定为 false，可以消除
    UnboundPredicate<?> inclusiveProjection =
        ((Transform<T, ?>) part.transform()).project(part.name(), pred);
    // ... 绑定并求值 ...
    if (inclusiveResult != null && inclusiveResult.op() == Expression.Operation.FALSE) {
      return Expressions.alwaysFalse();
    }
  }

  return pred;  // 无法确定，保留原始谓词
}
```

**跨 spec 的残余计算**：正如 ManifestGroup 中所展示的，每个 specId 都有独立的 ResidualEvaluator。这意味着对于使用 `hour(ts)` spec 的分区，残余表达式可能比 `day(ts)` spec 更简单（因为更细粒度的分区能更精确地过滤），反之亦然。

### 5.4 ManifestEvaluator 清单级过滤

**源码位置**: `api/src/main/java/org/apache/iceberg/expressions/ManifestEvaluator.java`

ManifestEvaluator 利用 Manifest 文件中记录的分区统计信息（上下界、null 计数等）来决定是否需要读取该 Manifest。它接收一个已经按照特定 spec 投影过的分区过滤表达式：

```java
// 第55-58行
public static ManifestEvaluator forRowFilter(
    Expression rowFilter, PartitionSpec spec, boolean caseSensitive) {
  return new ManifestEvaluator(
      spec, Projections.inclusive(spec, caseSensitive).project(rowFilter), caseSensitive);
}
```

在 ManifestGroup 的 `entries()` 方法中（第279-292行），每个 spec 有独立的 ManifestEvaluator 实例。这确保了即使表经历了分区演进，使用旧 spec 的 Manifest 也能被正确过滤。

---

## 6. 引擎层实现路径

### 6.1 Spark SQL 扩展实现

Spark 通过 Iceberg SQL 扩展支持分区演进操作。完整路径如下：

**第一步：语法定义**

**源码位置**: `spark/v3.5/spark-extensions/src/main/antlr/org.apache.spark.sql.catalyst.parser.extensions/IcebergSqlExtensions.g4`

```sql
-- 第70-72行
ALTER TABLE multipartIdentifier ADD PARTITION FIELD transform (AS name=identifier)?
ALTER TABLE multipartIdentifier DROP PARTITION FIELD transform
ALTER TABLE multipartIdentifier REPLACE PARTITION FIELD transform WITH transform (AS name=identifier)?
```

支持的语法示例：
```sql
ALTER TABLE db.events ADD PARTITION FIELD hour(event_time)
ALTER TABLE db.events DROP PARTITION FIELD day(event_time)
ALTER TABLE db.events REPLACE PARTITION FIELD day(event_time) WITH hour(event_time) AS ts_hour
ALTER TABLE db.events ADD PARTITION FIELD bucket(16, user_id) AS user_bucket
ALTER TABLE db.events ADD PARTITION FIELD truncate(10, city) AS city_trunc
```

**第二步：AST 构建**

**源码位置**: `spark/v3.5/spark-extensions/src/main/scala/org/apache/spark/sql/catalyst/parser/extensions/IcebergSqlExtensionsAstBuilder.scala`

```scala
// 第84-90行
override def visitAddPartitionField(ctx: AddPartitionFieldContext): AddPartitionField =
  withOrigin(ctx) {
    AddPartitionField(
      typedVisit[Seq[String]](ctx.multipartIdentifier),
      typedVisit[Transform](ctx.transform),
      Option(ctx.name).map(_.getText))
  }

// 第95-100行
override def visitDropPartitionField(ctx: DropPartitionFieldContext): DropPartitionField =
  withOrigin(ctx) {
    DropPartitionField(
      typedVisit[Seq[String]](ctx.multipartIdentifier),
      typedVisit[Transform](ctx.transform))
  }

// 第203-210行
override def visitReplacePartitionField(
    ctx: ReplacePartitionFieldContext): ReplacePartitionField = withOrigin(ctx) {
  ReplacePartitionField(
    typedVisit[Seq[String]](ctx.multipartIdentifier),
    typedVisit[Transform](ctx.transform(0)),
    typedVisit[Transform](ctx.transform(1)),
    Option(ctx.name).map(_.getText))
}
```

**第三步：执行计划**

**源码位置**: `spark/v3.5/spark-extensions/src/main/scala/org/apache/spark/sql/execution/datasources/v2/AddPartitionFieldExec.scala`

```scala
// 第29-58行
case class AddPartitionFieldExec(
    catalog: TableCatalog,
    ident: Identifier,
    transform: Transform,
    name: Option[String]) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case iceberg: SparkTable =>
        iceberg.table
          .updateSpec()                                    // 获取 UpdatePartitionSpec
          .addField(name.orNull, Spark3Util.toIcebergTerm(transform))  // 转换并添加
          .commit()                                        // 提交
      case table =>
        throw new UnsupportedOperationException(...)
    }
    Nil
  }
}
```

**执行链路总结**：
```
SQL 语句 (ALTER TABLE ... ADD PARTITION FIELD hour(ts))
  -> ANTLR 解析 (IcebergSqlExtensions.g4)
  -> AST 构建 (IcebergSqlExtensionsAstBuilder.scala)
  -> 逻辑计划 (AddPartitionField.scala)
  -> 物理执行 (AddPartitionFieldExec.scala)
  -> Iceberg API (Table.updateSpec().addField(term).commit())
  -> BaseUpdatePartitionSpec.apply() + commit()
  -> TableMetadata.updatePartitionSpec(newSpec)
  -> TableOperations.commit(base, update)
```

### 6.2 Flink 动态分区演进实现

Flink 的分区演进通过动态 sink 场景下的 `TableUpdater` + `PartitionSpecEvolution` 实现。

**PartitionSpecEvolution**

**源码位置**: `flink/v2.0/flink/src/main/java/org/apache/iceberg/flink/sink/dynamic/PartitionSpecEvolution.java`

该类负责计算两个 PartitionSpec 之间的差异：

```java
// 第61-87行
static PartitionSpecChanges evolve(PartitionSpec currentSpec, PartitionSpec targetSpec) {
  if (currentSpec.compatibleWith(targetSpec)) {
    return new PartitionSpecChanges();  // 兼容，无需变更
  }

  PartitionSpecChanges result = new PartitionSpecChanges();
  int maxNumFields = Math.max(currentSpec.fields().size(), targetSpec.fields().size());
  
  for (int i = 0; i < maxNumFields; i++) {
    PartitionField currentField = Iterables.get(currentSpec.fields(), i, null);
    PartitionField targetField = Iterables.get(targetSpec.fields(), i, null);

    if (!specFieldsAreCompatible(
        currentField, currentSpec.schema(), targetField, targetSpec.schema())) {
      if (currentField != null) {
        result.remove(toTerm(currentField, currentSpec.schema()));  // 删除旧字段
      }
      if (targetField != null) {
        result.add(toTerm(targetField, targetSpec.schema()));  // 添加新字段
      }
    }
  }
  return result;
}
```

**TableUpdater.findOrCreateSpec()**

**源码位置**: `flink/v2.0/flink/src/main/java/org/apache/iceberg/flink/sink/dynamic/TableUpdater.java`（第179-225行）

```java
private PartitionSpec findOrCreateSpec(TableIdentifier identifier, PartitionSpec targetSpec) {
  PartitionSpec currentSpec = cache.spec(identifier, targetSpec);
  if (currentSpec != null) return currentSpec;

  Table table = catalog.loadTable(identifier);
  currentSpec = table.spec();

  PartitionSpecEvolution.PartitionSpecChanges result =
      PartitionSpecEvolution.evolve(currentSpec, targetSpec);
  if (result.isEmpty()) return currentSpec;

  // 应用变更
  UpdatePartitionSpec updater = table.updateSpec();
  result.termsToRemove().forEach(updater::removeField);
  result.termsToAdd().forEach(updater::addField);

  try {
    updater.commit();  // 提交分区演进
    cache.update(identifier, table);
  } catch (CommitFailedException e) {
    // 处理并发冲突
    ...
  }
  return cache.spec(identifier, targetSpec);
}
```

需要注意的是，**Flink 的 FlinkCatalog 目前不支持通过 Flink SQL `ALTER TABLE` 语句直接修改分区**。在 `FlinkCatalog.alterTable()` 方法中（`flink/v2.0/flink/src/main/java/org/apache/iceberg/flink/FlinkCatalog.java`），分区变更的校验会直接抛出异常：

```java
// FlinkCatalog.java 第480-482行
private static void validateTablePartition(CatalogTable ct1, CatalogTable ct2) {
  if (!ct1.getPartitionKeys().equals(ct2.getPartitionKeys())) {
    throw new UnsupportedOperationException("Altering partition keys is not supported yet.");
  }
}
```

因此 Flink 的分区演进主要通过编程式 API（如 `TableUpdater` 的动态 sink 场景）或直接使用 Iceberg Java API 来实现。

---

## 7. 分区演进对数据写入路径的影响

写入数据时，Writer 使用**当前默认的 PartitionSpec**（由 `TableMetadata.spec()` 返回，即 `specsById.get(defaultSpecId)`）。

关键影响点：

1. **分区值计算**：Writer 根据当前 spec 中的 PartitionField 和 Transform，从源数据行计算分区值。演进后新写入的数据使用新 spec 的 transform。

2. **Manifest 文件绑定**：`ManifestWriter` 在构造时绑定当前 spec 的 specId，新写入的 Manifest 文件携带新的 specId。

3. **数据文件的物理布局**：
   - 分区演进前后的数据文件存储在不同的分区目录下
   - 例如，从 `day(ts)` 演进到 `hour(ts)` 后，旧数据在 `ts_day=2024-01-15/` 目录下，新数据在 `ts_hour=2024-01-15-08/` 目录下
   - 两者在同一张表中和谐共存

4. **数据文件中的 spec_id**：每个 DataFile 记录了自己的 `spec_id`，确保读取时能找到正确的分区类型定义。

---

## 8. 实际业务场景示例

### 场景：事件日志表从按天分区演进到按小时分区

**初始状态**：一张事件日志表，按天分区。

```sql
CREATE TABLE events (
    event_id BIGINT,
    event_time TIMESTAMP,
    user_id STRING,
    event_type STRING,
    payload STRING
) PARTITIONED BY (day(event_time));
```

此时 TableMetadata 中有一个 spec：
```
spec_id=0: [1000: event_time_day: day(1)]
```

**随着数据量增长**，每天的分区数据量过大（数百 GB），查询按小时过滤的场景非常频繁但无法高效裁剪。

**执行分区演进**：

```sql
-- 方式1：先删后加
ALTER TABLE events DROP PARTITION FIELD day(event_time);
ALTER TABLE events ADD PARTITION FIELD hour(event_time);

-- 方式2：替换（原子操作）
ALTER TABLE events REPLACE PARTITION FIELD day(event_time) 
  WITH hour(event_time) AS event_time_hour;
```

**演进后的 TableMetadata**：
```
specs: [
  spec_id=0: [1000: event_time_day: day(1)]           -- 旧 spec，仍然保留
  spec_id=1: [1001: event_time_hour: hour(1)]          -- 新 spec，成为默认
]
default_spec_id: 1
```

**查询 `SELECT * FROM events WHERE event_time > '2024-06-01 08:00:00'` 的执行过程**：

1. **ManifestGroup** 获取所有 Manifest 文件
2. 对于 specId=0（旧 spec）的 Manifest：
   - 使用 `Projections.inclusive(spec0).project(filter)` 生成 `event_time_day >= '2024-06-01'`
   - 使用 ManifestEvaluator 判断 Manifest 的分区统计信息是否匹配
   - 匹配的 Manifest 中的文件使用 `ResidualEvaluator(spec0)` 计算残余表达式
3. 对于 specId=1（新 spec）的 Manifest：
   - 使用 `Projections.inclusive(spec1).project(filter)` 生成 `event_time_hour >= '2024-06-01-08'`
   - 使用 ManifestEvaluator 判断匹配
   - 匹配的文件使用 `ResidualEvaluator(spec1)` 计算残余表达式

**关键点**：新 spec 能更精确地裁剪数据（按小时而非按天），从而提升查询性能。而旧数据仍然可以按天级别进行分区裁剪，不需要重新写入。

---

## 9. 注意事项与最佳实践

### 9.1 注意事项

1. **分区演进不会重写数据，但旧数据的查询效率不会提升**
   - 演进只影响新写入的数据。旧数据依然使用旧的分区策略
   - 如果需要旧数据也使用新分区，必须通过 `RewriteDataFiles` 操作重写

2. **V1 格式的限制**
   - V1 格式下删除分区字段会替换为 `void` transform，仍占用一个字段位置
   - V1 的字段 ID 必须连续（从 1000 开始），这限制了演进的灵活性
   - 建议使用 V2 格式以获得完整的分区演进支持

3. **时间类 Transform 的冗余检测**
   - `BaseUpdatePartitionSpec` 中的 `checkForRedundantAddedPartitions()` 方法会阻止对同一个源字段添加多个时间类 transform
   - 例如，不能同时有 `day(ts)` 和 `hour(ts)`（在同一个 spec 中）
   - 但可以有 `day(ts)` 和 `bucket(16, user_id)` 共存

4. **乐观并发控制**
   - `commit()` 使用乐观并发控制，如果在修改期间表元数据被其他操作修改，会抛出 `CommitFailedException`
   - 需要在应用层处理重试逻辑

5. **Flink SQL 暂不支持 ALTER TABLE 分区操作**
   - Flink 的分区演进只能通过编程 API 或 Iceberg Java API 完成
   - 动态 sink（`TableUpdater`）可以自动进行分区演进

6. **分区字段名称冲突处理**
   - 当新添加的分区字段名与已有的 void 字段名冲突时，系统会自动给旧字段加上 `_<fieldId>` 后缀
   - 见 `addField()` 方法中的 `renameField(existingField.name(), existingField.name() + "_" + existingField.fieldId())`

### 9.2 最佳实践

1. **规划好分区策略后再创建表**
   - 虽然可以演进，但频繁变更分区策略会导致多个 spec 共存，增加查询规划复杂度

2. **使用 `REPLACE PARTITION FIELD` 而非 `DROP` + `ADD`**
   - `REPLACE` 是原子操作，语义更清晰
   - 直接替换可以在 BaseUpdatePartitionSpec 内部优化为"撤销删除"（如果是同一个 transform）

3. **演进后考虑重写旧数据**
   - 使用 Spark 的 `rewrite_data_files` 存储过程
   - `CALL system.rewrite_data_files('db.events')` 将使用新的默认 spec 重写数据文件

4. **监控分区 spec 数量**
   - 通过 `table.specs()` 查看历史 spec 数量
   - 过多的 spec 会增加查询规划中的缓存压力（每个 specId 需要独立的 ManifestEvaluator 和 ResidualEvaluator）

5. **使用 V2 格式**
   - V2 格式提供了更好的分区演进支持，建议新表统一使用 V2

6. **先删除旧分区字段，再添加新分区字段**
   - 对于时间类 transform 的替换，Iceberg 不允许同一源字段同时存在两个时间 transform
   - 使用 `REPLACE` 可以避免这个限制

---

## 10. 总结

Iceberg 的分区演进机制是一个精巧的多层设计：

| 层次 | 组件 | 职责 |
|------|------|------|
| **数据模型层** | `PartitionSpec` + `PartitionField` + `Transform` | 定义分区规范的数据结构 |
| **演进 API 层** | `UpdatePartitionSpec` / `BaseUpdatePartitionSpec` | 提供添加/删除/替换/重命名分区字段的操作接口 |
| **元数据管理层** | `TableMetadata`（specs, specsById, defaultSpecId） | 维护所有历史 spec，支持多 spec 共存 |
| **存储绑定层** | `ManifestFile.partitionSpecId()` / `ContentFile.specId()` | 每个 Manifest/数据文件绑定到特定 spec |
| **查询规划层** | `ManifestGroup` + `ManifestEvaluator` + `Projections` + `ResidualEvaluator` | 按 specId 分别进行分区过滤和残余计算 |
| **引擎集成层** | Spark SQL Extensions / Flink TableUpdater | 提供 SQL 和编程式的分区演进接口 |

其核心设计理念可以概括为：**将分区规范视为表的"代际"属性，而非全局不变的属性**。每一代数据（由 Manifest 文件标识）携带自己的分区规范 ID，在读取时根据该 ID 获取正确的规范定义。这使得分区演进成为一个纯元数据操作——只需修改 TableMetadata，无需触及任何数据文件。

这种设计在保证向后兼容的同时，给了用户极大的灵活性来适应业务变化。无论是数据量增长需要更细粒度的分区，还是业务需求变化需要调整分区策略，Iceberg 都能以极低的成本完成切换。

# Apache Iceberg 面试题库

> 本文档从面试官视角整理了 Apache Iceberg 核心知识点，涵盖 Schema 演进、事务机制、流式集成、文件管理和查询优化等关键领域。
> 
> **难度标记**: 🟢 基础 | 🟡 中级 | 🟠 高级 | 🔴 专家

---

## 目录（按题目导航）

### 一、Schema 演进与元数据管理
- [请说明 Iceberg 默认值机制在读写路径中的工作原理及其优势。](#q-1-1) 🟢
- [Iceberg 如何实现零成本列重命名？核心机制是什么？](#q-1-2) 🟢
- [Parquet 是基于名称的格式，Iceberg 如何将其改造为支持基于 ID 的 Schema 演进？](#q-1-3) 🟡
- [从源码层面说明 Iceberg 如何将 `fieldId` 写入 Parquet 文件？](#q-1-4) 🟡
- [Iceberg 如何处理没有 `fieldId` 的 Parquet 文件(如从 Hive 导入的文件)？](#q-1-5) 🔴
- [`initial-default` 和 `write-default` 有什么区别？](#q-1-6) 🟢
- [Iceberg 如何在不进行 I/O 的情况下实现读路径默认值填充？](#q-1-7) 🟡
- [为什么默认值填充由引擎层(Spark/Flink)完成，而非 Iceberg 核心库？](#q-1-8) 🟠
- [迁移 Hive 表到 Iceberg 时，如何配合 Name Mapping 处理默认值？](#q-1-9) 🔴

### 二、事务与并发控制
- [Iceberg 如何在高并发场景下保证 ACID 特性中的原子性和隔离性？](#q-2-1) 🟠
- [两个 Flink 作业同时向一个 Iceberg 表写入时会发生什么？](#q-2-2) 🟡
- [Flink 作业重启后，Iceberg Sink 如何保证不重复提交数据？](#q-2-3) 🔴

### 三、Flink 流式集成
- [简述 Flink 写入 Iceberg 时如何实现端到端 Exactly-Once 语义？](#q-3-1) 🟠
- [详细说明 Flink-Iceberg 的两阶段提交流程。](#q-3-2) 🟠
- [`prepareSnapshotPreBarrier` 为什么在 Barrier 到达前调用？](#q-3-3) 🟡
- [为什么 `IcebergFilesCommitter` 并行度必须为 1？](#q-3-4) 🟠
- [详细解释 Position Delete 和 Equality Delete 的原理、结构和适用场景。](#q-3-5) 🟡
- [什么是 Merge-on-Read？Flink CDC 场景下如何智能结合两种删除方式优化性能？](#q-3-6) 🔴
- [`write.upsert.enabled=true` 的作用及其与 `RowKind` 的关系？](#q-3-7) 🟡
- [Position Delete 和 Equality Delete 文件分别是如何产生的？什么情况下二者会相互转化？](#q-3-8) 🟠
- [小文件优化(Compaction)是否会优化删除文件？如何优化？](#q-3-9) 🟠
- [Iceberg 打 Tag 时与快照的关系是什么？Tag 的底层实现机制是什么？](#q-3-10) 🟡
- [小文件合并(Compaction)重写了新文件,历史文件会删除吗？在什么时候删除？](#q-3-11) 🟠

### 四、文件管理与优化
- [对比分析 `RewriteDataFiles` 的 BinPack、Sort 和 Z-Order 策略。](#q-4-1) 🟠
- [`ExpireSnapshots` 和 `DeleteOrphanFiles` 的区别和联系？](#q-4-2) 🟡
- [`RewriteManifests` 如何提升查询性能，尤其是分区聚合？](#q-4-3) 🟠
- [`DeleteOrphanFiles` 有哪些安全保护机制防止误删？](#q-4-4) 🟡

### 五、查询与读取优化
- [简述 Iceberg 读取流程中的关键剪枝步骤。](#q-5-1) 🟡
- [解释 Iceberg 如何利用元数据进行 Filter Pushdown。](#q-5-2) 🟡
- [Iceberg 如何实现时间旅行查询？](#q-5-3) 🟢
- [对比 Spark 和 Flink 读取 Iceberg 的实现差异。](#q-5-4) 🟠

### 六、Spark 与 Flink 读取机制深度解析
- [请从源码角度说明 `FileScanTask` 如何封装 DataFile 与 DeleteFile 的关联关系？](#q-6-1) 🟡
- [Spark/Flink 在构建 `FileScanTask` 时,如何判断哪些 `DeleteFile` 需要关联到某个 `DataFile`？](#q-6-2) 🟠
- [请详细说明 Spark 读取 Iceberg 时,DeleteFilter 如何实现 Merge-on-Read？](#q-6-3) 🟠
- [Position Delete 如何实现 O(1) 的删除行判断？请结合源码说明。](#q-6-4) 🟡
- [Equality Delete 如何通过 StructLikeSet 实现高效的等值匹配？](#q-6-5) 🟡
- [为什么应用 Equality Delete 时需要读取额外的列？请从源码角度说明 requiredSchema 的计算逻辑。](#q-6-6) 🟠
- [`dataSequenceNumber` 和 `fileSequenceNumber` 有什么区别？Delete 的生效规则到底是不是统一 `<=`？](#q-6-7) 🔴
- [Flink 增量读取 Iceberg 的关键路径是什么？`IncrementalAppendScan` 接口在源码里到底长什么样？](#q-6-8) 🟠
- [Spark 侧对 DeleteFilter 有哪些真实可用的优化？哪些常见说法在源码层面不准确？](#q-6-9) 🟡

### 七、Catalog 与元数据管理架构
- [请解释 Iceberg Catalog 的 SPI 设计模式，以及如何通过 CatalogUtil 实现 Catalog 的可插拔加载？](#q-7-1) 🟠
- [TableOperations.commit() 是 Iceberg 事务的核心方法。请详细说明其原子性保证机制,以及 CommitStateUnknownException 的处理策略。](#q-7-2) 🔴
- [REST Catalog 相比 HiveCatalog 有哪些架构优势？请从 commit 实现角度分析差异。](#q-7-3) 🟡

### 八、SnapshotProducer 提交核心流程
- [SnapshotProducer 是 Iceberg 所有写操作的基类。请详细说明其模板方法模式设计,以及 commit() 方法的重试机制。](#q-8-1) 🔴
- [Iceberg 如何自动管理 Manifest 文件数量？请从源码角度说明 ManifestMergeManager 的合并策略。](#q-8-2) 🟠
- [Iceberg 快照的 summary 字段包含丰富的统计信息。请说明 SnapshotSummary 的累加器设计及其在增量计算中的应用。](#q-8-3) 🟡

### 九、DeleteFileIndex 与删除文件匹配优化
- [DeleteFileIndex 是 Iceberg 读取时关联删除文件的核心组件。请详细说明其多维索引结构及查找算法。](#q-9-1) 🔴
- [Equality Delete 的匹配比 Position Delete 更复杂。请说明 canContainEqDeletesForFile() 方法的统计信息剪枝逻辑。](#q-9-2) 🟠
- [Iceberg V3 引入了 Deletion Vector (DV) 作为更高效的行级删除方案。请说明 DV 的 Roaring Bitmap 实现原理。](#q-9-3) 🟡

### 十、面试必杀技 - 能让你脱颖而出的源码细节
- [在 Flink 写入 Iceberg 时,如果存在 Delete 文件,IcebergFilesCommitter 会逐 checkpoint 提交而不是合并成一个事务。这是为什么？](#q-10-1) 🔴
- [IcebergFilesCommitter 使用 NavigableMap 来管理待提交的 checkpoint 数据。这个数据结构选择有什么特殊考量？](#q-10-2) 🟠
- [Iceberg 在多处使用 Roaring Bitmap,包括 Deletion Vector 和 Position Delete 缓存。相比其他位图实现,Roaring Bitmap 有什么优势？](#q-10-3) 🟡
- [Iceberg 表格式从 V1 演进到 V3,每个版本引入了哪些核心特性？如何理解版本兼容性？](#q-10-4) 🟠

### 十一、源码级高阶追问（新增12题）
- [`DeleteFilter` 为什么先应用 position delete，再应用 equality delete？顺序可交换吗？](#q-11-1) 🔴
- [用户只查少量列，为什么读取时 Iceberg 可能补读更多列？](#q-11-2) 🟠
- [为什么 delete 匹配不是“每个 DataFile 扫所有 DeleteFile”？](#q-11-3) 🔴
- [这个 `-1` 到底在保护什么语义？](#q-11-4) 🔴
- [`referencedDataFile` 为空时，Iceberg 还能否只靠元数据判断作用范围？](#q-11-5) 🟠
- [Spark 的 delete 缓存到底怎么开启、如何淘汰、有什么边界？](#q-11-6) 🟠
- [旧 SourceFunction 与 FLIP-27 在“首次启动”阶段有什么关键差异？](#q-11-7) 🔴
- [这个参数只影响 planner CPU，还是也影响消费节奏？](#q-11-8) 🟠
- [DV 与 position delete 在读取与缓存上有什么本质区别？](#q-11-9) 🟠
- [为什么有些查询 manifest 规划时会“多读统计列”？](#q-11-10) 🟠
- [Spark Join 产生的 runtime filter 如何影响 Iceberg 扫描任务？](#q-11-11) 🟡
- [V2 与 V3+ 在 position delete 介质上的硬约束是什么？](#q-11-12) 🔴

---

## 一、Schema 演进与元数据管理

<a id="q-1-1"></a>
### 1.1 默认值机制

**🟢 问题: 请说明 Iceberg 默认值机制在读写路径中的工作原理及其优势。**

**答案:**

Iceberg v3 通过 `initial-default` 和 `write-default` 实现了读写分离的默认值策略。

#### 读路径 - 惰性虚拟策略

当查询新增的带默认值列，但读取的是该列添加前的旧数据文件时:

1. **Schema 对比**: Reader 对比表 Schema 与文件 Schema，识别缺失列
2. **虚拟列生成**: 启用 `ConstantReader`，无需磁盘 I/O
3. **默认值填充**: 直接返回 `initial-default` 值

**核心优势**: 零 I/O 开销，实现真正的 Schema-on-Read

#### 写路径 - 主动物理化策略

当 `INSERT` 操作未指定带默认值列的值时:

1. **引擎层处理**: 计算引擎(Spark/Flink)的分析器负责
2. **逻辑计划重写**: 将 `write-default` 作为常量注入
3. **物理写入**: 默认值被实际写入数据文件

**核心优势**: 保证数据完整性，利用引擎向量化执行优化性能

#### 30 秒速记（简版）

- 结论先行：Iceberg v3 通过 initial-default 和 write-default 实现了读写分离的默认值策略。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/Schema.java:45`，再联动 `api/src/main/java/org/apache/iceberg/types/TypeUtil.java:56`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `api/src/main/java/org/apache/iceberg/Schema.java:45`
- `api/src/main/java/org/apache/iceberg/types/TypeUtil.java:56`




#### 源码关键片段

**片段 1：`api/src/main/java/org/apache/iceberg/Schema.java:42-48`**
```java
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

/**
```

**片段 2：`api/src/main/java/org/apache/iceberg/types/TypeUtil.java:53-59`**
```java
   *
   * @param schema to project fields from
   * @param fieldIds list of explicit fields to extract
   * @return the schema with all fields fields not selected removed
   */
  public static Schema project(Schema schema, Set<Integer> fieldIds) {
    Preconditions.checkNotNull(schema, "Schema cannot be null");
```

---
<a id="q-1-2"></a>
### 1.2 零成本列重命名

**🟢 问题: Iceberg 如何实现零成本列重命名？核心机制是什么？**

**答案:**

Iceberg 通过 **`fieldId` 唯一标识机制**实现零成本列重命名，而非依赖列名。

#### 核心机制

- **`fieldId` 作用**: 列的"身份证号"，终身不变；`fieldName` 只是"姓名"，可随时更改
- **ID 生成**: `metadata.json` 维护原子计数器 `next-column-id`
- **重命名操作**: 仅修改元数据中 `fieldId` 对应的 `name` 属性
- **数据读取**: 忽略文件中的旧列名，仅根据 `fieldId` 匹配数据

#### Schema 源码实现

```java
// 源码: api/src/main/java/org/apache/iceberg/Schema.java
public class Schema implements Serializable {
    private final StructType struct;  // 底层类型结构
    private final int schemaId;       // Schema 版本 ID
    private final int[] identifierFieldIds;  // 主键字段 ID 数组
    private final int highestFieldId; // 当前最大 fieldId
    
    // 懒加载的索引映射(设计模式: 延迟初始化)
    private transient Map<Integer, NestedField> idToField = null;  // ID → Field
    private transient Map<String, Integer> nameToId = null;        // Name → ID
    private transient Map<Integer, String> idToName = null;        // ID → Name
    
    // 根据 fieldId 查找字段(核心方法)
    public NestedField findField(int id) {
        return lazyIdToField().get(id);  // 仅依赖 ID,与 name 无关
    }
    
    // 根据列名查找字段(内部转换为 ID 查找)
    public NestedField findField(String name) {
        Integer id = lazyNameToId().get(name);  // 先通过 name 找到 ID
        if (id != null) {
            return lazyIdToField().get(id);     // 再通过 ID 找到 Field
        }
        return null;
    }
    
    // 懒加载 ID 到 Field 的映射
    private Map<Integer, NestedField> lazyIdToField() {
        if (idToField == null) {
            this.idToField = TypeUtil.indexById(struct);  // 仅构建一次
        }
        return idToField;
    }
}
```

**关键设计**: `idToField` 是核心索引,`nameToId` 只是辅助查找层,重命名只需更新 `nameToId` 映射。

#### 设计模式分析

**1. 不可变对象模式(Immutable Object)**

```java
public class Schema implements Serializable {
    private final StructType struct;        // final 修饰,不可变
    private final int schemaId;             // final 修饰,不可变
    private final int[] identifierFieldIds; // final 修饰,数组引用不变
    
    // 所有修改操作都返回新的 Schema 实例
    public Schema select(Collection<String> names) {
        // ...
        return TypeUtil.select(this, selected);  // 返回新实例
    }
}
```

**优势**:
- **线程安全**: 多线程并发读取无需加锁
- **快照隔离**: 每个查询使用固定版本的 Schema,不受并发修改影响
- **简化推理**: 一旦创建,状态永不改变

**2. 懒加载模式(Lazy Initialization)**

```java
private transient Map<Integer, NestedField> idToField = null;

private Map<Integer, NestedField> lazyIdToField() {
    if (idToField == null) {  // Double-Check 模式
        this.idToField = TypeUtil.indexById(struct);
    }
    return idToField;
}
```

**优势**:
- **节省内存**: 仅在需要时构建索引
- **加速序列化**: `transient` 关键字避免序列化临时索引
- **按需计算**: 不同查询可能只需要部分索引

#### 与 Hive 的对比

| 特性 | Iceberg | Hive |
|------|---------|------|
| 标识机制 | `fieldId`(不变) | 列名(可变) |
| 重命名成本 | 零成本(仅元数据) | 元数据更新 |
| 外部工具兼容 | 保证数据关联 | 可能读取错误 |
| 并发安全 | 不可变对象,天然线程安全 | 需要额外同步机制 |

**深入理解**: Hive 重命名列后，外部工具直接读取 Parquet 文件可能因列名不匹配读到 `null`，而 Iceberg 的 `fieldId` 从根本上解决了这个问题。不可变对象模式确保了在高并发场景下的数据一致性。

#### 30 秒速记（简版）

- 结论先行：Iceberg 通过 **fieldId 唯一标识机制**实现零成本列重命名，而非依赖列名。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/Schema.java:1`，再联动 `api/src/main/java/org/apache/iceberg/Schema.java:45`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `api/src/main/java/org/apache/iceberg/Schema.java:1`
- `api/src/main/java/org/apache/iceberg/Schema.java:45`




#### 源码关键片段

**片段 1：`api/src/main/java/org/apache/iceberg/Schema.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`api/src/main/java/org/apache/iceberg/Schema.java:42-48`**
```java
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

/**
```

---
<a id="q-1-3"></a>
### 1.3 Parquet 文件的 ID 映射

**🟡 问题: Parquet 是基于名称的格式，Iceberg 如何将其改造为支持基于 ID 的 Schema 演进？**

**答案:**

Iceberg 并未改造 Parquet 格式本身，而是利用 Parquet 规范的**可选 ID 属性**构建了元数据层。

#### 实现机制

1. **利用规范**: Parquet 允许为字段附加可选的整数 `ID` 属性
2. **写入注入**: 将 Iceberg `fieldId` 写入 Parquet 字段的 `ID` 属性
3. **读取处理**: Iceberg-aware 引擎忽略 `fieldName`，仅使用 `ID` 匹配

#### 设计哲学

**不重新发明文件格式，而是赋能现有格式**

- **优点**: 充分利用 Parquet/Avro/ORC 的性能优势和生态
- **权衡**: 需要使用理解 Iceberg 约定的计算引擎才能完全享受便利

**深入理解**: 通用 Parquet 读取工具会退回到基于列名的模式，失去 Iceberg 的部分优势。

#### 30 秒速记（简版）

- 结论先行：Iceberg 并未改造 Parquet 格式本身，而是利用 Parquet 规范的**可选 ID 属性**构建了元数据层。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/Schema.java:45`，再联动 `api/src/main/java/org/apache/iceberg/types/TypeUtil.java:56`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `api/src/main/java/org/apache/iceberg/Schema.java:45`
- `api/src/main/java/org/apache/iceberg/types/TypeUtil.java:56`




#### 源码关键片段

**片段 1：`api/src/main/java/org/apache/iceberg/Schema.java:42-48`**
```java
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

/**
```

**片段 2：`api/src/main/java/org/apache/iceberg/types/TypeUtil.java:53-59`**
```java
   *
   * @param schema to project fields from
   * @param fieldIds list of explicit fields to extract
   * @return the schema with all fields fields not selected removed
   */
  public static Schema project(Schema schema, Set<Integer> fieldIds) {
    Preconditions.checkNotNull(schema, "Schema cannot be null");
```

---
<a id="q-1-4"></a>
### 1.4 fieldId 写入机制

**🟡 问题: 从源码层面说明 Iceberg 如何将 `fieldId` 写入 Parquet 文件？**

**答案:**

核心逻辑位于 `org.apache.iceberg.parquet.ParquetSchemaUtil` 工具类和 `TypeToMessageType` 转换器。

#### 源码流程

**1. 入口方法**

```java
// 源码: parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java
public class ParquetSchemaUtil {
    /**
     * 将 Iceberg Schema 转换为 Parquet MessageType
     */
    public static MessageType convert(Schema schema, String name) {
        return new TypeToMessageType().convert(schema, name);
    }
}
```

**2. TypeToMessageType 转换器(访问者模式)**

```java
// 源码: parquet/src/main/java/org/apache/iceberg/parquet/TypeToMessageType.java
class TypeToMessageType extends TypeUtil.SchemaVisitor<Type> {
    
    @Override
    public Type field(Types.NestedField field, Type fieldType) {
        // 核心: 将 Iceberg fieldId 注入到 Parquet Type
        Type.Repetition repetition = field.isOptional() 
            ? Type.Repetition.OPTIONAL 
            : Type.Repetition.REQUIRED;
        
        // 关键代码: 通过 .id() 方法设置 fieldId
        if (fieldType.isPrimitive()) {
            return Types.primitive(fieldType.asPrimitiveType().getPrimitiveTypeName(), repetition)
                .id(field.fieldId())  // ← 注入 fieldId
                .named(field.name());
        } else {
            return fieldType.withId(field.fieldId());  // ← 注入 fieldId
        }
    }
    
    @Override
    public Type struct(Types.StructType struct, List<Type> fieldTypes) {
        return Types.buildGroup(Type.Repetition.REQUIRED)
            .addFields(fieldTypes.toArray(new Type[0]))
            .named("struct");
    }
}
```

**3. Parquet Type Builder 机制**

```java
// Parquet 库提供的 Builder API
Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
    .id(42)              // ← 设置 field ID
    .named("user_id");   // ← 设置 field name
```

#### 设计模式分析: 访问者模式(Visitor Pattern)

**访问者模式结构**:

```java
// 抽象访问者
abstract class SchemaVisitor<T> {
    abstract T schema(Schema schema, T structResult);
    abstract T struct(StructType struct, List<T> fieldResults);
    abstract T field(NestedField field, T fieldResult);
    abstract T list(ListType list, T elementResult);
    abstract T map(MapType map, T keyResult, T valueResult);
    abstract T primitive(PrimitiveType primitive);
}

// 具体访问者: TypeToMessageType
class TypeToMessageType extends SchemaVisitor<Type> {
    // 实现各个 visit 方法,将 Iceberg Type 转换为 Parquet Type
}
```

**为什么使用访问者模式？**

1. **类型系统复杂**: Iceberg 支持 Struct、List、Map、Primitive 等多种嵌套类型
2. **递归遍历**: 需要深度优先遍历整个 Schema 树
3. **操作解耦**: 转换逻辑与类型定义分离,易于扩展新的转换器

**访问者模式优势**:

| 优势 | 说明 |
|------|------|
| **开闭原则** | 新增转换逻辑(如 Avro、ORC)无需修改 Schema 类 |
| **单一职责** | Schema 类专注于类型定义,转换逻辑由 Visitor 负责 |
| **类型安全** | 编译期检查所有类型分支是否都有处理 |

#### 完整转换示例

```
Iceberg Schema:
  NestedField(id=1, name="user_id", type=IntegerType, required=true)
  NestedField(id=2, name="user_name", type=StringType, required=false)

↓ TypeToMessageType.visit()

Parquet MessageType:
  message table {
    required int32 user_id = 1;      ← fieldId 已注入
    optional binary user_name = 2;   ← fieldId 已注入
  }
```

**深入理解**: 访问者模式是处理复杂类型系统的经典方案,Iceberg 在 Schema 转换、谓词下推、统计信息收集等多处使用了这一模式,体现了良好的架构设计。

#### 30 秒速记（简版）

- 结论先行：核心逻辑位于 org.apache.iceberg.parquet.ParquetSchemaUtil 工具类和 TypeToMessageType 转换器。
- 主链路：先定位 `parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java:1`，再联动 `parquet/src/main/java/org/apache/iceberg/parquet/TypeToMessageType.java:1`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java:1`
- `parquet/src/main/java/org/apache/iceberg/parquet/TypeToMessageType.java:1`




#### 源码关键片段

**片段 1：`parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`parquet/src/main/java/org/apache/iceberg/parquet/TypeToMessageType.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

---
<a id="q-1-5"></a>
### 1.5 Name Mapping 机制

**🔴 问题: Iceberg 如何处理没有 `fieldId` 的 Parquet 文件(如从 Hive 导入的文件)？**

**答案:**

通过 **Name Mapping(名称映射)** 机制桥接列名与 `fieldId` 的关联。

#### 工作流程

1. **查找映射**: Reader 发现文件无 ID 时，查看表属性 `schema.name-mapping.default`
2. **构建映射**: 该属性包含 JSON 格式的"列名→ID"映射规则(支持别名)
3. **应用映射**: 根据列名动态赋予 `fieldId`
4. **生成 Schema**: 内存中补全 ID，后续逻辑基于 ID 处理

#### NameMapping 源码实现

```java
// 源码: core/src/main/java/org/apache/iceberg/mapping/NameMapping.java
public class NameMapping implements Serializable {
    private final MappedFields mapping;  // 映射字段列表
    
    // 懒加载的索引
    private transient Map<Integer, MappedField> fieldsById;    // ID → MappedField
    private transient Map<String, MappedField> fieldsByName;   // Name → MappedField
    
    /**
     * 根据 fieldId 查找映射
     */
    public MappedField find(int id) {
        return lazyFieldsById().get(id);
    }
    
    /**
     * 根据列名查找映射(支持嵌套路径)
     */
    public MappedField find(String... names) {
        return lazyFieldsByName().get(DOT.join(names));
    }
    
    // 懒加载 ID 索引
    private Map<Integer, MappedField> lazyFieldsById() {
        if (fieldsById == null) {
            this.fieldsById = MappingUtil.indexById(mapping);
        }
        return fieldsById;
    }
    
    // 懒加载 Name 索引
    private Map<String, MappedField> lazyFieldsByName() {
        if (fieldsByName == null) {
            this.fieldsByName = MappingUtil.indexByName(mapping);
        }
        return fieldsByName;
    }
}
```

#### ApplyNameMapping 应用逻辑

```java
// 源码: parquet/src/main/java/org/apache/iceberg/parquet/ApplyNameMapping.java
public class ApplyNameMapping extends ParquetTypeVisitor<Type> {
    private final NameMapping nameMapping;
    
    @Override
    public Type primitive(PrimitiveType primitive) {
        // 根据列名查找映射
        MappedField mapped = nameMapping.find(currentPath());
        if (mapped != null && mapped.id() != null) {
            // 将映射的 ID 设置到 Parquet Type
            return primitive.withId(mapped.id());
        }
        return primitive;  // 无映射则保持原样
    }
}
```

#### 设计模式分析: 适配器模式(Adapter Pattern)

**问题**: Hive 表的 Parquet 文件不包含 `fieldId`,但 Iceberg 依赖 `fieldId` 进行 Schema 演进

**解决方案**: Name Mapping 作为适配器,将基于名称的旧系统适配到基于 ID 的新系统

```
旧系统(Hive)          适配器(NameMapping)      新系统(Iceberg)
┌───────────┐       ┌──────────────────┐     ┌─────────────┐
│ Parquet    │       │ Name → ID       │     │ Schema      │
│ (no ID)    │ ────▶ │ Mapping         │ ───▶ │ (with ID)   │
│ user_name  │       │ user_name → 1   │     │ fieldId=1   │
│ user_id    │       │ user_id → 2     │     │ fieldId=2   │
└───────────┘       └──────────────────┘     └─────────────┘
```

**适配器模式优势**:
- **兼容性**: 无需重写文件,直接读取 Hive 表
- **灵活性**: 支持别名和重命名场景
- **透明性**: 上层代码无需感知映射存在

#### Name Mapping 示例

```json
[
  {"field-id": 1, "names": ["user_name", "username"]},  // 支持别名
  {"field-id": 2, "names": ["user_id"]},
  {
    "field-id": 3, 
    "names": ["address"],
    "fields": [  // 嵌套结构
      {"field-id": 4, "names": ["city"]},
      {"field-id": 5, "names": ["street"]}
    ]
  }
]
```

#### 风险与最佳实践

**风险**: 错误的 `NameMapping` 可能导致数据静默错位或置为 `null`

**错误示例**:
```
旧列 user_name → 错误映射到 → 新列 account_id 的 fieldId
结果: 数据静默错位，引发数据质量问题
```

**最佳实践**:
- 使用 Spark `migrate` 或 `add_files` 自动生成映射
- 大规模迁移时进行充分的数据抽样验证
- 避免手动编写复杂映射规则

**深入理解**: Name Mapping 是 Iceberg 与传统数仓系统集成的关键机制,通过适配器模式优雅地解决了兼容性问题。但必须谨慎使用,错误的映射可能导致严重的数据质量问题。

#### 💡 面试加分话术

> "Name Mapping 是 Iceberg **零拷贝迁移**的核心技术:
> 1. **原理**: 通过 JSON 格式的名称-ID 映射表，让无 ID 的 Parquet 文件也能被 Iceberg 读取
> 2. **迁移优势**: 无需重写数据文件，只需在元数据层建立映射关系
> 3. **别名支持**: 同一个 fieldId 可映射多个名称，支持列名变更历史
>
> 这体现了 Iceberg 的核心设计哲学: **元数据驱动**。通过丰富的元数据层解决兼容性问题，而非要求数据文件重写。"

#### 🔥 高频追问

**Q: 如果 Name Mapping 配置错误导致数据错位，有什么恢复手段？**

**A**: Iceberg 提供了安全机制:
1. **不可变数据**: 原始文件未被修改，修正 Name Mapping 后重新读取即可
2. **Schema 校验**: 类型不匹配时会抛出异常（如 STRING 映射到 INT）
3. **回滚能力**: 通过快照可以回滚到正确的元数据版本

**最佳实践**: 迁移前使用 `spark.read.format("iceberg").option("snapshot-id", xxx)` 进行数据抽样验证。

#### 30 秒速记（简版）

- 结论先行：通过 **Name Mapping(名称映射)** 机制桥接列名与 fieldId 的关联。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/mapping/NameMapping.java:1`，再联动 `parquet/src/main/java/org/apache/iceberg/parquet/ApplyNameMapping.java:1`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/mapping/NameMapping.java:1`
- `parquet/src/main/java/org/apache/iceberg/parquet/ApplyNameMapping.java:1`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/mapping/NameMapping.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`parquet/src/main/java/org/apache/iceberg/parquet/ApplyNameMapping.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

---
<a id="q-1-6"></a>
### 1.6 默认值类型详解

**🟢 问题: `initial-default` 和 `write-default` 有什么区别？**

**答案:**

两者分别处理不同的数据生命周期场景。

| 类型 | 作用时机 | 应用场景 | 核心目标 |
|------|----------|----------|----------|
| `initial-default` | 读取时 | Schema 演进，读取旧数据 | 解决历史数据不完整性 |
| `write-default` | 写入时 | 新数据插入 | 保证未来数据一致性 |

**示例**:
```sql
-- 新增列时设置默认值
ALTER TABLE users ADD COLUMN status STRING 
  DEFAULT 'active' -- write-default
  INITIAL DEFAULT 'unknown'; -- initial-default
```

- 查询旧数据: 返回 `'unknown'`
- 新插入数据未指定 `status`: 写入 `'active'`

#### 30 秒速记（简版）

- 结论先行：两者分别处理不同的数据生命周期场景。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/Schema.java:45`，再联动 `api/src/main/java/org/apache/iceberg/types/TypeUtil.java:56`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `api/src/main/java/org/apache/iceberg/Schema.java:45`
- `api/src/main/java/org/apache/iceberg/types/TypeUtil.java:56`




#### 源码关键片段

**片段 1：`api/src/main/java/org/apache/iceberg/Schema.java:42-48`**
```java
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

/**
```

**片段 2：`api/src/main/java/org/apache/iceberg/types/TypeUtil.java:53-59`**
```java
   *
   * @param schema to project fields from
   * @param fieldIds list of explicit fields to extract
   * @return the schema with all fields fields not selected removed
   */
  public static Schema project(Schema schema, Set<Integer> fieldIds) {
    Preconditions.checkNotNull(schema, "Schema cannot be null");
```

---
<a id="q-1-7"></a>
### 1.7 ConstantReader 实现

**🟡 问题: Iceberg 如何在不进行 I/O 的情况下实现读路径默认值填充？**

**答案:**

通过 **ConstantReader(常量读取器)** 机制实现零 I/O 填充。

#### 实现流程

1. **Schema 对齐**: Reader 对比 Table Schema 与 File Schema
2. **识别缺失列**: 发现列在文件中不存在但有 `initial-default`
3. **构造 ConstantReader**: 不创建物理 `ColumnReader`，而是实例化虚拟列读取器
4. **零 I/O 填充**: 直接在内存中为每行返回预定义的 Literal 值

**核心优势**: 将"虚拟列"的概念引入查询引擎层，避免了数据文件的物理修改。

#### 30 秒速记（简版）

- 结论先行：通过 **ConstantReader(常量读取器)** 机制实现零 I/O 填充。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/Schema.java:45`，再联动 `api/src/main/java/org/apache/iceberg/types/TypeUtil.java:56`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `api/src/main/java/org/apache/iceberg/Schema.java:45`
- `api/src/main/java/org/apache/iceberg/types/TypeUtil.java:56`




#### 源码关键片段

**片段 1：`api/src/main/java/org/apache/iceberg/Schema.java:42-48`**
```java
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

/**
```

**片段 2：`api/src/main/java/org/apache/iceberg/types/TypeUtil.java:53-59`**
```java
   *
   * @param schema to project fields from
   * @param fieldIds list of explicit fields to extract
   * @return the schema with all fields fields not selected removed
   */
  public static Schema project(Schema schema, Set<Integer> fieldIds) {
    Preconditions.checkNotNull(schema, "Schema cannot be null");
```

---
<a id="q-1-8"></a>
### 1.8 默认值职责分离

**🟠 问题: 为什么默认值填充由引擎层(Spark/Flink)完成，而非 Iceberg 核心库？**

**答案:**

基于**职责解耦**和**计算下推**的设计原则。

#### 设计优势

1. **职责解耦**
   - Iceberg Core: 元数据规范、事务管理、文件格式
   - 计算引擎: SQL 解析、缺失列处理等计算逻辑

2. **性能优化**
   - 引擎层可将默认值填充转化为执行计划中的表达式
   - 利用向量化执行或代码生成(Codegen)技术
   - 比在 `FileAppender` 中逐行判断效率高得多

3. **引擎无关性**
   - Iceberg Core 保持通用性
   - 各引擎用最原生的方式实现，无需修改底层存储代码

#### 30 秒速记（简版）

- 结论先行：基于**职责解耦**和**计算下推**的设计原则。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/Schema.java:45`，再联动 `api/src/main/java/org/apache/iceberg/types/TypeUtil.java:56`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `api/src/main/java/org/apache/iceberg/Schema.java:45`
- `api/src/main/java/org/apache/iceberg/types/TypeUtil.java:56`




#### 源码关键片段

**片段 1：`api/src/main/java/org/apache/iceberg/Schema.java:42-48`**
```java
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

/**
```

**片段 2：`api/src/main/java/org/apache/iceberg/types/TypeUtil.java:53-59`**
```java
   *
   * @param schema to project fields from
   * @param fieldIds list of explicit fields to extract
   * @return the schema with all fields fields not selected removed
   */
  public static Schema project(Schema schema, Set<Integer> fieldIds) {
    Preconditions.checkNotNull(schema, "Schema cannot be null");
```

---
<a id="q-1-9"></a>
### 1.9 Hive 迁移与默认值

**🔴 问题: 迁移 Hive 表到 Iceberg 时，如何配合 Name Mapping 处理默认值？**

**答案:**

Name Mapping 与默认值机制协同工作，处理历史数据的列缺失问题。

#### 协同流程

1. **建立 Name Mapping**: 配置 `schema.name-mapping.default`，映射列名到 `fieldId`
2. **识别缺失列**: 应用映射后，若文件中仍缺少某列(新增列)
3. **触发默认值填充**: 回退到 `initial-default` 机制，通过 `ConstantReader` 填充

#### 关键风险

**强依赖 Name Mapping 的正确性**

错误示例:
```
旧列 user_name → 错误映射到 → 新列 account_id 的 fieldId
结果: 数据静默错位，引发数据质量问题
```

**最佳实践**:
- 不仅依赖工具自动生成
- 进行严格的数据抽样对比验证
- 小批量测试后再全量迁移

#### 30 秒速记（简版）

- 结论先行：Name Mapping 与默认值机制协同工作，处理历史数据的列缺失问题。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/Schema.java:45`，再联动 `api/src/main/java/org/apache/iceberg/types/TypeUtil.java:56`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `api/src/main/java/org/apache/iceberg/Schema.java:45`
- `api/src/main/java/org/apache/iceberg/types/TypeUtil.java:56`




#### 源码关键片段

**片段 1：`api/src/main/java/org/apache/iceberg/Schema.java:42-48`**
```java
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

/**
```

**片段 2：`api/src/main/java/org/apache/iceberg/types/TypeUtil.java:53-59`**
```java
   *
   * @param schema to project fields from
   * @param fieldIds list of explicit fields to extract
   * @return the schema with all fields fields not selected removed
   */
  public static Schema project(Schema schema, Set<Integer> fieldIds) {
    Preconditions.checkNotNull(schema, "Schema cannot be null");
```

---

## 二、事务与并发控制
<a id="q-2-1"></a>
### 2.1 ACID 特性实现

**🟠 问题: Iceberg 如何在高并发场景下保证 ACID 特性中的原子性和隔离性？**

**答案:**

通过**乐观并发控制(OCC)** 和**快照机制**实现。

#### 原子性(Atomicity)

**核心机制**: 元数据文件的原子替换

1. 不直接修改现有元数据文件
2. 创建新的元数据文件
3. 通过文件系统原子操作(如 `rename`)更新指针

**结果**: 提交要么完全成功，要么完全失败

#### BaseTransaction 源码实现

```java
// 源码: core/src/main/java/org/apache/iceberg/BaseTransaction.java
public class BaseTransaction implements Transaction {
    private final TableOperations ops;           // 底层表操作
    private final List<PendingUpdate> updates;   // 待提交的更新列表
    private TableMetadata base;                  // 基线元数据
    private TableMetadata current;               // 当前元数据
    
    /**
     * 提交事务(乐观并发控制)
     */
    @Override
    public void commitTransaction() {
        switch (type) {
            case SIMPLE:
                commitSimpleTransaction();
                break;
            // ...
        }
    }
    
    /**
     * 简单事务提交(带重试)
     */
    private void commitSimpleTransaction() {
        try {
            Tasks.foreach(ops)
                .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
                .exponentialBackoff(...)  // 指数退避
                .onlyRetryOn(CommitFailedException.class)
                .run(underlyingOps -> {
                    // 关键: 应用所有更新
                    applyUpdates(underlyingOps);
                    
                    // 关键: 乐观锁 - 比较 base 与 current
                    underlyingOps.commit(base, current);
                });
        } catch (CommitFailedException e) {
            // 冲突检测,清理并重试
            cleanUpOnCommitFailure();
            throw e;
        }
    }
    
    /**
     * 应用更新(冲突检测与重试)
     */
    private void applyUpdates(TableOperations underlyingOps) {
        if (base != underlyingOps.refresh()) {
            // 检测到元数据变化,重新应用更新
            this.base = underlyingOps.current();
            this.current = underlyingOps.current();
            
            for (PendingUpdate update : updates) {
                // 重新执行每个更新
                update.commit();
            }
        }
    }
}
```

#### 乐观并发控制(OCC)流程

```
线程 A                          线程 B
│                              │
├─ 1. 读取 base (v1)           ├─ 1. 读取 base (v1)
│                              │
├─ 2. 应用更新 A              ├─ 2. 应用更新 B
│   current = v2              │   current = v2'
│                              │
├─ 3. commit(v1, v2)          │
│   ✅ 成功,表版本变为 v2    │
│                              │
│                              ├─ 3. commit(v1, v2')
│                              │   ❌ 失败! base 已是 v2
│                              │
│                              ├─ 4. 重试: refresh()
│                              │   base = v2
│                              │
│                              ├─ 5. 重新应用更新 B
│                              │   current = v3
│                              │
│                              ├─ 6. commit(v2, v3)
│                              │   ✅ 成功
```

**关键设计**:
- **无锁读取**: 读操作不需要锁,直接读取快照
- **提交时验证**: 仅在 commit 时检查冲突
- **自动重试**: 冲突时自动重新应用更新

#### 隔离性(Isolation)

**核心机制**: 快照隔离(Snapshot Isolation)

1. 每个事务基于特定快照操作
2. 读事务确定快照版本后，新快照对其不可见
3. 读操作永不被写操作阻塞

#### 设计模式分析: 命令模式(Command Pattern)

**PendingUpdate 命令接口**:

```java
public interface PendingUpdate<T> {
    T commit();  // 执行命令
}

// 具体命令实现
public class SchemaUpdate implements PendingUpdate<Schema> { ... }
public class FastAppend implements PendingUpdate<Snapshot> { ... }
public class BaseRewriteFiles implements PendingUpdate<Snapshot> { ... }
```

**命令模式优势**:

| 优势 | 说明 |
|------|------|
| **解耦请求与执行** | Transaction 不需知道具体操作类型 |
| **支持撤销/重做** | 冲突时可重新执行命令列表 |
| **组合操作** | 多个命令可组合成事务 |
| **日志记录** | 命令列表即为操作历史 |

**命令模式应用**:

```java
// 事务中的命令队列
List<PendingUpdate> updates = [
    new SchemaUpdate(...),      // 命令 1: 更新 Schema
    new FastAppend(...),        // 命令 2: 追加数据
    new BaseRewriteFiles(...)   // 命令 3: 重写文件
];

// 执行所有命令
for (PendingUpdate update : updates) {
    update.commit();  // 多态调用
}
```

#### 隔离级别的边界

**快照隔离(SI) ≠ 可串行化(Serializable)**

存在**写偏斜(Write Skew)** 异常:

```
规则: A + B ≥ 0
T1 读取 A=50, B=50 → 执行 A=-40
T2 读取 A=50, B=50 → 执行 B=-40
结果: A=-40, B=-40 (违反约束)
```

**深入理解**: SI 适用于绝大多数场景，但需要了解其边界，在关键业务逻辑中额外验证约束。命令模式使得事务系统具有良好的可扩展性和可维护性。

#### 面试加分话术

> "Iceberg 通过 OCC 乐观并发控制实现 ACID: 原子性依靠元数据文件的原子替换(文件系统 rename),隔离性通过快照机制实现(每次读取看到一致的快照)。事务内部使用命令模式,将 SchemaUpdate、AppendFiles 等操作封装为 PendingUpdate,最后原子提交。需要注意的是 Iceberg 提供快照隔离而非可串行化,存在写偏斜可能。"

#### 高频追问

**Q: Iceberg 如何处理事务提交失败后的清理？**

**A**: Iceberg 区分两类异常: (1) `CleanableFailure` 可安全清理已写入的数据文件 (2) `CommitStateUnknownException` 状态未知,不清理以避免数据丢失,需人工介入。SnapshotProducer 的 `strictCleanup` 标志控制清理策略。

#### 30 秒速记（简版）

- 结论先行：通过**乐观并发控制(OCC)** 和**快照机制**实现。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/BaseTransaction.java:1`，再联动 `core/src/main/java/org/apache/iceberg/BaseTransaction.java:58`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/BaseTransaction.java:1`
- `core/src/main/java/org/apache/iceberg/BaseTransaction.java:58`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/BaseTransaction.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`core/src/main/java/org/apache/iceberg/BaseTransaction.java:55-61`**
```java
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseTransaction implements Transaction {
  private static final Logger LOG = LoggerFactory.getLogger(BaseTransaction.class);

```

---
<a id="q-2-2"></a>
### 2.2 并发写入冲突处理

**🟡 问题: 两个 Flink 作业同时向一个 Iceberg 表写入时会发生什么？**

**答案:**

结果取决于操作类型和分区冲突情况。

#### 场景分析

| 场景 | 操作类型 | 分区关系 | 结果 |
|------|----------|----------|------|
| 1 | Append | 不同分区 | ✅ 自动合并，都成功 |
| 2 | Append | 同一分区 | ✅ 通常成功(冲突验证宽松) |
| 3 | Overwrite | 同一分区 | ❌ 后者失败(`CommitFailedException`) |

#### 冲突检测机制

- **Append**: 只要 Schema、Spec 不变即可合并
- **Overwrite**: 严格验证相关分区是否被修改

**最佳实践**: 
- 按分区并行写入，避免同一分区的并发 Overwrite
- 监控 `CommitFailedException`，实现重试机制

#### 冲突检测的源码实现

**核心方法**: `MergingSnapshotProducer.validateAddedDataFiles()`

```java
// 源码: core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java
protected void validateAddedDataFiles(
    TableMetadata base,
    Long startingSnapshotId,
    Expression conflictDetectionFilter,
    Snapshot parent) {
    
    // 1. 获取从 startingSnapshotId 到 parent 之间新增的数据文件
    CloseableIterable<ManifestEntry<DataFile>> conflictEntries =
        addedDataFiles(base, startingSnapshotId, conflictDetectionFilter, null, parent);
    
    // 2. 检查是否有冲突文件
    try (CloseableIterator<ManifestEntry<DataFile>> conflicts = conflictEntries.iterator()) {
        if (conflicts.hasNext()) {
            // 3. 抛出 ValidationException,导致提交失败
            throw new ValidationException(
                "Found conflicting files that can contain records matching %s: %s",
                conflictDetectionFilter,
                Iterators.toString(
                    Iterators.transform(conflicts, entry -> entry.file().location().toString())));
        }
    }
}
```

**关键步骤**:

1. **确定验证范围**:
   ```java
   // 从 startingSnapshotId 到 parent 之间的所有快照
   Pair<List<ManifestFile>, Set<Long>> history =
       validationHistory(
           base,
           startingSnapshotId,
           VALIDATE_ADDED_FILES_OPERATIONS,  // 仅检查 APPEND 和 OVERWRITE 操作
           ManifestContent.DATA,
           parent);
   ```

2. **过滤新增文件**:
   ```java
   ManifestGroup manifestGroup =
       new ManifestGroup(ops().io(), manifests, ImmutableList.of())
           .filterManifestEntries(entry -> newSnapshots.contains(entry.snapshotId()))
           .ignoreDeleted()    // 忽略已删除的文件
           .ignoreExisting();  // 忽略已存在的文件
   
   // 如果提供了冲突检测过滤器,进一步过滤
   if (conflictDetectionFilter != null) {
       manifestGroup = manifestGroup.filterData(conflictDetectionFilter);
   }
   ```

3. **检查冲突**:
   - 如果找到任何匹配的文件,抛出 `ValidationException`
   - 导致提交失败,触发重试机制

#### 不同操作的冲突检测策略

**Append 操作**:

```java
// 源码: core/src/main/java/org/apache/iceberg/FastAppend.java
@Override
protected void validate(TableMetadata base, Snapshot parent) {
    // Append 操作通常不进行严格的冲突检测
    // 只要 Schema 和 PartitionSpec 兼容即可
    // 因此两个 Append 操作几乎总是可以自动合并
}
```

**Overwrite 操作**:

```java
// 源码: core/src/main/java/org/apache/iceberg/BaseOverwriteFiles.java
@Override
protected void validate(TableMetadata base, Snapshot parent) {
    // 1. 验证被覆盖的分区没有新增文件
    validateAddedDataFiles(base, startingSnapshotId, dataConflictDetectionFilter(), parent);
    
    // 2. 验证被覆盖的数据文件没有被删除
    validateDeletedDataFiles(base, startingSnapshotId, dataConflictDetectionFilter(), parent);
    
    // 3. 验证被覆盖的数据文件没有新的删除文件
    validateNoNewDeleteFiles(base, startingSnapshotId, dataConflictDetectionFilter(), parent);
}
```

**RowDelta 操作** (UPDATE/DELETE):

```java
// 源码: core/src/main/java/org/apache/iceberg/BaseRowDelta.java
@Override
protected void validate(TableMetadata base, Snapshot parent) {
    if (validateConflictingData) {
        // 验证没有并发添加冲突的数据文件
        validateAddedDataFiles(base, startingSnapshotId, conflictDetectionFilter, parent);
    }
    
    if (validateConflictingDeletes) {
        // 验证没有并发添加冲突的删除文件
        validateNoNewDeleteFiles(base, startingSnapshotId, conflictDetectionFilter, parent);
    }
    
    if (!referencedDataFiles.isEmpty()) {
        // 验证引用的数据文件仍然存在
        validateDataFilesExist(base, startingSnapshotId, referencedDataFiles, 
                               !validateDeletedFiles, conflictDetectionFilter, parent);
    }
}
```

#### 冲突检测的粒度控制

**分区级别冲突检测**:

```java
// 使用 PartitionSet 进行分区级别的冲突检测
PartitionSet replacedPartitions = PartitionSet.create();
for (DataFile file : filesToReplace) {
    replacedPartitions.add(file.specId(), file.partition());
}

// 仅检查这些分区是否有新增文件
validateAddedDataFiles(base, startingSnapshotId, replacedPartitions, parent);
```

**行级别冲突检测**:

```java
// 使用 Expression 进行行级别的冲突检测
Expression conflictFilter = Expressions.and(
    Expressions.equal("user_id", 123),
    Expressions.greaterThan("timestamp", "2024-01-01")
);

// 仅检查匹配此条件的新增文件
validateAddedDataFiles(base, startingSnapshotId, conflictFilter, parent);
```

#### 重试机制

```java
// 源码: core/src/main/java/org/apache/iceberg/BaseTransaction.java
Tasks.foreach(ops)
    .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))  // 默认 4 次
    .exponentialBackoff(
        base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),  // 100ms
        base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),  // 60s
        base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),  // 30min
        2.0  // 指数退避因子
    )
    .onlyRetryOn(CommitFailedException.class)
    .run(underlyingOps -> {
        // 1. 刷新元数据
        underlyingOps.refresh();
        
        // 2. 重新应用所有更新
        applyUpdates(underlyingOps);
        
        // 3. 重新验证(可能再次失败)
        // 4. 提交
        underlyingOps.commit(base, current);
    });
```

**重试流程**:

```
Attempt 1: 提交失败 (CommitFailedException)
           ↓ 等待 100ms
Attempt 2: 刷新元数据 → 重新应用更新 → 重新验证 → 提交失败
           ↓ 等待 200ms
Attempt 3: 刷新元数据 → 重新应用更新 → 重新验证 → 提交失败
           ↓ 等待 400ms
Attempt 4: 刷新元数据 → 重新应用更新 → 重新验证 → 提交成功/失败
```

#### 并发场景分析

**场景 1: 两个 Append 操作(不同分区)**

```
Job A: Append to partition dt=2024-01-01
Job B: Append to partition dt=2024-01-02

时间线:
T1: Job A 读取 Snapshot 100
T2: Job B 读取 Snapshot 100
T3: Job A 提交 → Snapshot 101 (成功)
T4: Job B 提交 → 验证通过 → Snapshot 102 (成功)

原因: Append 不进行严格的冲突检测,两个操作可以自动合并
```

**场景 2: 两个 Overwrite 操作(同一分区)**

```
Job A: Overwrite partition dt=2024-01-01
Job B: Overwrite partition dt=2024-01-01

时间线:
T1: Job A 读取 Snapshot 100
T2: Job B 读取 Snapshot 100
T3: Job A 提交 → Snapshot 101 (成功)
T4: Job B 提交 → validateAddedDataFiles() 检测到 Job A 添加的文件
    → 抛出 ValidationException
    → 重试: 刷新元数据 → 重新应用更新 → 重新验证
    → 如果 Job A 的文件仍在,继续失败
    → 最终失败 (CommitFailedException)

原因: Overwrite 进行严格的分区级别冲突检测
```

**场景 3: Append + Overwrite(同一分区)**

```
Job A: Append to partition dt=2024-01-01
Job B: Overwrite partition dt=2024-01-01

时间线:
T1: Job A 读取 Snapshot 100
T2: Job B 读取 Snapshot 100
T3: Job A 提交 → Snapshot 101 (成功)
T4: Job B 提交 → validateAddedDataFiles() 检测到 Job A 添加的文件
    → 抛出 ValidationException
    → 失败 (CommitFailedException)

原因: Overwrite 会检测到 Append 添加的文件,视为冲突
```

**深入理解**: Iceberg 的冲突检测是可配置的,不同操作类型有不同的验证策略。Append 操作最宽松(几乎总是成功),Overwrite 和 RowDelta 操作最严格(需要验证分区或行级别的冲突)。理解这些机制对于设计高并发的数据管道至关重要。

#### 面试加分话术

> "Iceberg 并发冲突检测分三层: (1) 隔离级别检测(SERIALIZABLE 最严格) (2) 操作类型验证(Append 宽松,Overwrite 严格) (3) 分区/文件级别粒度控制。核心是 validateAddedDataFiles() 和 validateDeletedDataFiles(),它们检查是否有并发操作影响了同一分区或文件。冲突时自动重试,重试参数可通过 commit.retry.* 配置。"

#### 高频追问

**Q: 如何避免生产环境中的频繁冲突重试？**

**A**: (1) 按分区隔离不同作业的写入范围 (2) 使用 Append 而非 Overwrite (3) 调整 `commit.retry.num-retries` 和 `commit.retry.min-wait-ms` (4) 对于 CDC 场景使用单写入者模式避免竞争。

#### 30 秒速记（简版）

- 结论先行：结果取决于操作类型和分区冲突情况。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:1`，再联动 `core/src/main/java/org/apache/iceberg/FastAppend.java:1`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:1`
- `core/src/main/java/org/apache/iceberg/FastAppend.java:1`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`core/src/main/java/org/apache/iceberg/FastAppend.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

---
<a id="q-2-3"></a>
### 2.3 Flink 幂等性保证

**🔴 问题: Flink 作业重启后，Iceberg Sink 如何保证不重复提交数据？**

**答案:**

通过 **Flink State** 和 **`max-committed-checkpoint-id`** 双重机制保证幂等性。

#### 幂等性机制

1. **State 恢复**: `IcebergFilesCommitter` 将文件列表保存在 Flink State
2. **Checkpoint ID 检查**: 
   - 读取 Iceberg 快照元数据中的 `max-committed-checkpoint-id`
   - 提交前检查当前 Checkpoint ID 是否已提交
   - 若已提交则跳过

#### IcebergFilesCommitter 源码实现

```java
// 源码: flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java
public class IcebergFilesCommitter extends RichSinkFunction<WriteResult> 
        implements CheckpointedFunction {
    
    // Flink State: 存储待提交的文件
    private transient ListState<WriteResult> filesState;
    
    // 当前作业的 jobId
    private final String jobId;
    
    /**
     * 初始化状态(从 Checkpoint 恢复)
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.filesState = context.getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("iceberg-files-committer-state", ...));
        
        // 从 State 恢复待提交的文件
        if (context.isRestored()) {
            for (WriteResult result : filesState.get()) {
                pendingFiles.add(result);
            }
        }
    }
    
    /**
     * Checkpoint 完成回调 - 提交数据
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // 1. 检查是否已经提交过(幂等性检查)
        long maxCommittedCheckpointId = getMaxCommittedCheckpointId();
        if (checkpointId <= maxCommittedCheckpointId) {
            LOG.info("Checkpoint {} already committed, skipping", checkpointId);
            return;  // 已提交,跳过
        }
        
        // 2. 获取当前 Checkpoint 的文件
        List<WriteResult> filesToCommit = pendingFiles.get(checkpointId);
        if (filesToCommit == null || filesToCommit.isEmpty()) {
            return;
        }
        
        // 3. 提交到 Iceberg
        AppendFiles append = table.newAppend();
        for (WriteResult result : filesToCommit) {
            for (DataFile file : result.dataFiles()) {
                append.appendFile(file);
            }
        }
        
        // 4. 设置元数据(关键: 记录 checkpoint ID)
        append.set("flink.job-id", jobId);
        append.set("flink.max-committed-checkpoint-id", String.valueOf(checkpointId));
        
        // 5. 原子提交
        append.commit();
        
        // 6. 清理已提交的文件
        pendingFiles.remove(checkpointId);
    }
    
    /**
     * 从 Iceberg 快照读取最大已提交的 Checkpoint ID
     */
    private long getMaxCommittedCheckpointId() {
        Snapshot currentSnapshot = table.currentSnapshot();
        if (currentSnapshot == null) {
            return -1;
        }
        
        String maxCommittedId = currentSnapshot.summary()
            .get("flink.max-committed-checkpoint-id");
        
        return maxCommittedId != null ? Long.parseLong(maxCommittedId) : -1;
    }
}
```

#### 幂等性保证流程

```
正常场景:
┌─────────────────────────────────────────────┐
│ Checkpoint 1 完成                                │
│   → notifyCheckpointComplete(1)                  │
│   → 检查: maxCommittedCheckpointId = -1          │
│   → 1 > -1, 提交数据                          │
│   → 设置: max-committed-checkpoint-id = 1       │
└─────────────────────────────────────────────┘

重启场景(从 Checkpoint 1 恢复):
┌─────────────────────────────────────────────┐
│ initializeState()                              │
│   → 从 State 恢复 Checkpoint 1 的文件           │
│                                                 │
│ notifyCheckpointComplete(1) 再次触发         │
│   → 检查: maxCommittedCheckpointId = 1          │
│   → 1 <= 1, 跳过提交 ✅                      │
│   → 避免重复提交!                            │
└─────────────────────────────────────────────┘
```

#### 设计模式分析: 状态机模式(State Machine Pattern)

**Checkpoint 状态转换**:

```
                  prepareSnapshotPreBarrier()
    RUNNING ────────────────────────────▶ PRE_COMMIT
       │                                           │
       │                                           │ snapshotState()
       │                                           │
       │                                           ▼
       │                                      CHECKPOINTED
       │                                           │
       │                                           │ notifyCheckpointComplete()
       │                                           │
       │                                           ▼
       │                                       COMMITTED
       │                                           │
       └─────────────────────────────────────────────────┘
                      继续处理新数据
```

**状态机优势**:
- **清晰的状态转换**: 每个阶段的职责明确
- **容错处理**: 任何阶段失败都可从 Checkpoint 恢复
- **幂等性保证**: 状态转换可重复执行

#### 关键元数据

```json
{
  "snapshot-id": 123456,
  "summary": {
    "flink.job-id": "abc-def-ghi",
    "flink.max-committed-checkpoint-id": "42"  // 关键字段
  }
}
```

**元数据作用**:
- `flink.job-id`: 标识哪个 Flink 作业提交的数据
- `flink.max-committed-checkpoint-id`: 记录最大已提交的 Checkpoint ID

#### 运维风险

**非标准恢复流程的风险**:

❌ 错误做法:
```
停止旧作业 → 启动全新作业(新 job-id)
结果: 无法找到 max-committed-checkpoint-id，可能重复提交
```

✅ 正确做法:
```
从 Savepoint 恢复 → 保持 job-id 连续性
```

**深入理解**: 生产环境必须遵循标准恢复流程，否则 Exactly-Once 语义会被破坏。状态机模式使得 Checkpoint 生命周期管理更加清晰和可靠。

#### 30 秒速记（简版）

- 结论先行：通过 **Flink State** 和 **max-committed-checkpoint-id** 双重机制保证幂等性。
- 主链路：先定位 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:1`，再联动 `core/src/main/java/org/apache/iceberg/BaseTransaction.java:58`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:1`
- `core/src/main/java/org/apache/iceberg/BaseTransaction.java:58`




#### 源码关键片段

**片段 1：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`core/src/main/java/org/apache/iceberg/BaseTransaction.java:55-61`**
```java
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseTransaction implements Transaction {
  private static final Logger LOG = LoggerFactory.getLogger(BaseTransaction.class);

```

---

## 三、Flink 流式集成
<a id="q-3-1"></a>
### 3.1 Exactly-Once 语义

**🟠 问题: 简述 Flink 写入 Iceberg 时如何实现端到端 Exactly-Once 语义？**

**答案:**

通过**两阶段提交(2PC)** 与 **Flink Checkpoint** 紧密结合实现。

#### 核心组件

- `IcebergStreamWriter`: 并行算子，负责写入数据文件
- `IcebergFilesCommitter`: 单并行度算子，负责提交事务

#### 简化流程

1. **预提交**: Checkpoint Barrier 到达前，Writer 关闭文件并发送元数据给 Committer
2. **提交**: Checkpoint 全局成功后，Committer 原子性提交到 Iceberg

**容错保证**: 失败时从上一个成功 Checkpoint 恢复，不丢失不重复

#### 30 秒速记（简版）

- 结论先行：通过**两阶段提交(2PC)** 与 **Flink Checkpoint** 紧密结合实现。
- 主链路：先定位 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`，再联动 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`




#### 源码关键片段

**片段 1：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:92-98`**
```java
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot = Maps.newHashMap();
```

**片段 2：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:170-176`**
```java
    }
  }

  private long toSnapshotIdInclusive(
      long lastConsumedSnapshotId, long currentSnapshotId, int maxPlanningSnapshotCount) {
    List<Long> snapshotIds =
        SnapshotUtil.snapshotIdsBetween(table, lastConsumedSnapshotId, currentSnapshotId);
```

---
<a id="q-3-2"></a>
### 3.2 两阶段提交机制

**🟠 问题: 详细说明 Flink-Iceberg 的两阶段提交流程。**

**答案:**

#### 第一阶段: 预提交(Pre-Commit)

**触发**: `prepareSnapshotPreBarrier()` 回调(Barrier 到达**前**)

**动作**:
1. Writer 停止并关闭当前数据文件
2. 生成 `WriteResult` 对象(包含文件元数据)
3. 发送 `WriteResult` 和 `checkpointId` 给 Committer
4. Committer 将其序列化保存到 **Flink State Backend**
5. Writer 创建新文件，继续接收数据

#### 第二阶段: 正式提交(Commit)

**触发**: `notifyCheckpointComplete(checkpointId)` 回调(Checkpoint 全局成功)

**动作**:
1. Committer 从 State 读取待提交文件元数据
2. 调用 Iceberg API(`table.newAppend()`)
3. 原子性提交事务，生成新快照
4. 更新 `maxCommittedCheckpointId`，保证幂等性

#### 容错场景

| 失败时机 | 状态 | 恢复策略 |
|----------|------|----------|
| 阶段一失败 | 文件已写，未提交 | 从上一个 Checkpoint 恢复，重新写入 |
| 阶段二失败 | 部分提交 | 幂等性检查，跳过已提交的 Checkpoint |

#### 面试加分话术

> "Flink-Iceberg 两阶段提交的精髓在于: 第一阶段 prepareSnapshotPreBarrier 在 Barrier 到达**前**触发,确保当前文件已关闭并将元数据保存到 State;第二阶段 notifyCheckpointComplete 在全局成功后才真正提交到 Iceberg。关键是 maxCommittedCheckpointId 保证幂等 — 重启后通过比对表快照中记录的 checkpoint ID 避免重复提交。"

#### 30 秒速记（简版）

- 结论先行：本题核心是围绕“详细说明 Flink-Iceberg 的两阶段提交流程。”解释实现机制、设计动机与边界条件。
- 主链路：先定位 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`，再联动 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`




#### 源码关键片段

**片段 1：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:92-98`**
```java
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot = Maps.newHashMap();
```

**片段 2：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:170-176`**
```java
    }
  }

  private long toSnapshotIdInclusive(
      long lastConsumedSnapshotId, long currentSnapshotId, int maxPlanningSnapshotCount) {
    List<Long> snapshotIds =
        SnapshotUtil.snapshotIdsBetween(table, lastConsumedSnapshotId, currentSnapshotId);
```

---
<a id="q-3-3"></a>
### 3.3 prepareSnapshotPreBarrier 作用

**🟡 问题: `prepareSnapshotPreBarrier` 为什么在 Barrier 到达前调用？**

**答案:**

这是实现 2PC 的关键设计，有三个核心目的。

#### 核心目的

1. **确保数据完整性**
   - 保证 Barrier 前的所有数据完整刷入文件
   - 为 Checkpoint 划分清晰的文件边界

2. **实现真正的预提交**
   - Barrier 到达前完成文件关闭和元数据生成
   - 符合 2PC "Pre-Commit" 阶段语义

3. **避免阻塞，提高吞吐**
   - Flush 后立即创建新 Writer 接收后续数据
   - 数据流不会因等待 Barrier 而阻塞

**深入理解**: 如果在 Barrier 到达后才 flush，会导致数据流阻塞，严重影响吞吐量。

#### 30 秒速记（简版）

- 结论先行：这是实现 2PC 的关键设计，有三个核心目的。
- 主链路：先定位 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`，再联动 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`




#### 源码关键片段

**片段 1：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:92-98`**
```java
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot = Maps.newHashMap();
```

**片段 2：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:170-176`**
```java
    }
  }

  private long toSnapshotIdInclusive(
      long lastConsumedSnapshotId, long currentSnapshotId, int maxPlanningSnapshotCount) {
    List<Long> snapshotIds =
        SnapshotUtil.snapshotIdsBetween(table, lastConsumedSnapshotId, currentSnapshotId);
```

---
<a id="q-3-4"></a>
### 3.4 Committer 单并行度设计

**🟠 问题: 为什么 `IcebergFilesCommitter` 并行度必须为 1？**

**答案:**

为了**避免版本冲突**和**保证提交顺序性**。

#### 设计原因

1. **避免版本冲突**
   - Iceberg 采用乐观并发控制(OCC)
   - 多个 Committer 实例同时提交会导致 `CommitFailedException`
   - 单实例串行提交避免冲突

2. **维护全局顺序**
   - 需要维护 `maxCommittedCheckpointId` 状态
   - 只有单实例才能可靠地串行更新

#### 架构权衡

**优点**:
- 无需外部分布式锁(如 ZooKeeper)
- 架构简洁，易于部署

**潜在瓶颈**:
- Checkpoint 间隔极短且数据量极大时，单点可能成为瓶颈
- 需要处理所有子任务的 `WriteResult` 并与 Catalog 交互

**深入理解**: 对绝大多数场景，单点 Committer 性能足够；极端场景需监控 Checkpoint 耗时。

#### 30 秒速记（简版）

- 结论先行：为了**避免版本冲突**和**保证提交顺序性**。
- 主链路：先定位 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`，再联动 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`




#### 源码关键片段

**片段 1：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:92-98`**
```java
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot = Maps.newHashMap();
```

**片段 2：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:170-176`**
```java
    }
  }

  private long toSnapshotIdInclusive(
      long lastConsumedSnapshotId, long currentSnapshotId, int maxPlanningSnapshotCount) {
    List<Long> snapshotIds =
        SnapshotUtil.snapshotIdsBetween(table, lastConsumedSnapshotId, currentSnapshotId);
```

---
<a id="q-3-5"></a>
### 3.5 Position Delete vs Equality Delete

**🟡 问题: 详细解释 Position Delete 和 Equality Delete 的原理、结构和适用场景。**

**答案:**

Iceberg v2 通过两种删除文件实现行级删除能力。

#### Position Delete(位置删除)

**原理**: 通过精确的文件路径和行号标记删除

**文件结构**:
```
Schema: {file_path: string, pos: long}
示例: {"file_path": "data/file_A.parquet", "pos": 100}
```

**优点**:
- 读取效率极高(构建位图，直接跳过)
- 存储开销小(仅路径+行号)

**缺点**:
- 必须预知行的物理位置

**适用场景**:
- Copy-on-Write 或 Compaction
- 同批次内的删除优化
- Spark `DELETE FROM ... WHERE ...`(先扫描定位)

#### Equality Delete(等值删除)

**原理**: 通过一个或多个列的值标记删除

**文件结构**:
```
Schema: {user_id: long} (equality fields)
示例: {"user_id": 123}
```

**优点**:
- 灵活性高，无需知道物理位置

**缺点**:
- 读取开销大(需构建哈希表，逐行比较)
- 存储开销相对较大

**适用场景**:
- CDC(Change Data Capture)
- 流式 Upsert
- 外部系统的删除请求(仅有主键)

#### 对比总结

| 维度 | Position Delete | Equality Delete |
|------|-----------------|-----------------|
| 标识方式 | 文件路径+行号 | 列值 |
| 读取性能 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| 存储开销 | 小 | 中 |
| 灵活性 | 低(需预知位置) | 高 |
| 典型场景 | Compaction | CDC/Upsert |

#### 面试加分话术

> "Position Delete 用 (file_path, pos) 精确定位,读取时构建 bitmap O(1) 跳过,适合 Compaction;Equality Delete 用列值标识,需要构建哈希表逐行比较,但无需预知位置,适合 CDC。V3 引入的 Deletion Vector 本质是 Position Delete 的优化 — 用 Roaring Bitmap 存储,比 Parquet 文件更紧凑。选择时的黄金法则: 知道位置用 Position,只有主键用 Equality。"

#### 高频追问

**Q: 为什么 Equality Delete 读取开销大？**

**A**: Equality Delete 需要: (1) 加载所有相关 DeleteFile 构建 HashSet (2) 读取每条数据记录时提取 equality 字段并查找 HashSet。而 Position Delete 只需构建一次 bitmap,然后通过行号 O(1) 判断,无需访问数据内容。

#### 30 秒速记（简版）

- 结论先行：Iceberg v2 通过两种删除文件实现行级删除能力。
- 主链路：先定位 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`，再联动 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`




#### 源码关键片段

**片段 1：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:92-98`**
```java
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot = Maps.newHashMap();
```

**片段 2：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:170-176`**
```java
    }
  }

  private long toSnapshotIdInclusive(
      long lastConsumedSnapshotId, long currentSnapshotId, int maxPlanningSnapshotCount) {
    List<Long> snapshotIds =
        SnapshotUtil.snapshotIdsBetween(table, lastConsumedSnapshotId, currentSnapshotId);
```

---
<a id="q-3-6"></a>
### 3.6 Merge-on-Read 优化

**🔴 问题: 什么是 Merge-on-Read？Flink CDC 场景下如何智能结合两种删除方式优化性能？**

**答案:**

#### Merge-on-Read 概念

**核心思想**: 写入时不修改旧文件，而是记录变更；查询时合并数据

- 写入: 分别记录 INSERT/UPDATE/DELETE
- 查询: 引擎合并文件，过滤已删除/更新的行

#### Flink CDC 混合删除策略

在 `BaseEqualityDeltaWriter` 中实现的智能优化:

**核心机制**: 维护内存 `insertedRowMap`

```
Map<Key, Pair<file_path, pos>>
```

**智能决策逻辑**:

1. **处理 INSERT/UPDATE_AFTER**:
   ```
   检查 insertedRowMap
   ├─ 存在 → 生成 Position Delete(删除旧版本)
   └─ 不存在 → 直接插入
   更新 insertedRowMap
   ```

2. **处理 DELETE**:
   ```
   检查 insertedRowMap
   ├─ 存在 → 生成 Position Delete(撤销本批次插入)
   └─ 不存在 → 生成 Equality Delete(删除历史数据)
   ```

#### 优化效果

**批次内操作**: 全部降级为高效的 Position Delete  
**历史数据操作**: 才使用 Equality Delete

**性能提升**:
- 减少 Equality Delete 文件数量
- 降低查询时的合并开销
- 提升 CDC 场景写入性能

#### 30 秒速记（简版）

- 结论先行：在 BaseEqualityDeltaWriter 中实现的智能优化:。
- 主链路：先定位 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`，再联动 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`




#### 源码关键片段

**片段 1：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:92-98`**
```java
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot = Maps.newHashMap();
```

**片段 2：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:170-176`**
```java
    }
  }

  private long toSnapshotIdInclusive(
      long lastConsumedSnapshotId, long currentSnapshotId, int maxPlanningSnapshotCount) {
    List<Long> snapshotIds =
        SnapshotUtil.snapshotIdsBetween(table, lastConsumedSnapshotId, currentSnapshotId);
```

---
<a id="q-3-7"></a>
### 3.7 Upsert 模式优化

**🟡 问题: `write.upsert.enabled=true` 的作用及其与 `RowKind` 的关系？**

**答案:**

这是 CDC 场景的关键性能优化参数。

#### 默认行为(`write.upsert.enabled=false`)

CDC `UPDATE` 操作生成两条消息:

```
UPDATE_BEFORE(旧值) → 生成 Equality Delete(包含完整行)
UPDATE_AFTER(新值)  → 生成 Data File
```

**缺点**: Equality Delete 文件大，处理消息多

#### Upsert 模式(`write.upsert.enabled=true`)

```
UPDATE_BEFORE → 完全忽略
UPDATE_AFTER  → 生成 Equality Delete(仅主键) + Data File
```

#### 核心优势

1. **性能提升**: 处理消息数减半
2. **存储优化**: Equality Delete 仅包含主键，体积大幅减小
3. **读取优化**: 减少查询时的加载开销

#### 源码逻辑

```java
if (upsert) {
    switch (kind) {
        case UPDATE_BEFORE:
            break; // 关键: 直接跳过
        case UPDATE_AFTER:
            deleteKey(keyProjection.wrap(row)); // 仅删除key
            writeInsert(row);
            break;
    }
}
```

**最佳实践**: CDC 接入 Iceberg 时必须启用此参数。

---

#### 30 秒速记（简版）

- 结论先行：这是 CDC 场景的关键性能优化参数。
- 主链路：先定位 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`，再联动 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`




#### 源码关键片段

**片段 1：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:92-98`**
```java
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot = Maps.newHashMap();
```

**片段 2：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:170-176`**
```java
    }
  }

  private long toSnapshotIdInclusive(
      long lastConsumedSnapshotId, long currentSnapshotId, int maxPlanningSnapshotCount) {
    List<Long> snapshotIds =
        SnapshotUtil.snapshotIdsBetween(table, lastConsumedSnapshotId, currentSnapshotId);
```

---
<a id="q-3-8"></a>
### 3.8 删除文件的产生机制

**🟠 问题: Position Delete 和 Equality Delete 文件分别是如何产生的？什么情况下二者会相互转化？**

**答案:**

#### Position Delete 的产生机制

**产生场景**:

1. **DELETE 操作(先扫描后删除)**
   ```sql
   DELETE FROM table WHERE user_id = 123
   ```
   - Spark/Flink 先扫描数据文件,定位符合条件的行
   - 记录每一行的 `(file_path, pos)`
   - 生成 Position Delete 文件

2. **UPDATE 操作(Copy-on-Write 模式)**
   - 读取原数据文件
   - 对于被更新的行,生成 Position Delete
   - 新数据写入新的 Data File

3. **Compaction 过程**
   - 合并小文件时,对于需要删除的行生成 Position Delete
   - 避免重写整个文件

**源码实现**:

```java
// 源码: data/src/main/java/org/apache/iceberg/data/BaseDeleteWriter.java
public class PositionDeleteWriter<T> implements DeleteWriter<T> {
    
    /**
     * 写入 Position Delete
     */
    public void delete(CharSequence path, long pos) {
        PositionDelete<T> positionDelete = PositionDelete.create();
        positionDelete.set(path, pos, null);  // 仅记录路径和位置
        writer.write(positionDelete);
    }
}
```

#### Equality Delete 的产生机制

**产生场景**:

1. **CDC 流式写入(Flink)**
   ```java
   // Flink CDC 接收到 DELETE 消息
   RowKind.DELETE → 生成 Equality Delete
   ```
   - 仅包含主键列的值
   - 无需知道数据的物理位置

2. **MERGE INTO 操作**
   ```sql
   MERGE INTO target USING source
   ON target.id = source.id
   WHEN MATCHED THEN DELETE
   ```
   - 根据 JOIN 条件生成 Equality Delete

3. **外部系统的删除请求**
   - 仅提供主键,无法定位物理位置
   - 必须使用 Equality Delete

**源码实现**:

```java
// 源码: data/src/main/java/org/apache/iceberg/data/BaseDeleteWriter.java
public class EqualityDeleteWriter<T> implements DeleteWriter<T> {
    private final int[] equalityFieldIds;  // 等值字段 ID 列表
    
    /**
     * 写入 Equality Delete
     */
    public void delete(T row) {
        // 仅投影出等值字段
        T projectedRow = projectRow(row, equalityFieldIds);
        writer.write(projectedRow);  // 写入等值字段的值
    }
}
```

#### 二者是否会相互转化？

**关键结论**: **不会自动转化,但可以通过 Action 手动转换**

**原因**:
1. **文件格式不同**:
   - Position Delete: `Schema{file_path: string, pos: long}`
   - Equality Delete: `Schema{<equality_fields>}`
   - 两者的 Schema 完全不同,无法直接转换

2. **语义不同**:
   - Position Delete: 精确定位到某个文件的某一行
   - Equality Delete: 根据列值匹配,可能影响多个文件

**手动转换场景**:

Iceberg 提供了 `ConvertEqualityDeleteFiles` Action:

```java
// 源码: api/src/main/java/org/apache/iceberg/actions/ConvertEqualityDeleteFiles.java
public interface ConvertEqualityDeleteFiles extends Action<ConvertEqualityDeleteFiles, Result> {
    /**
     * 将 Equality Delete 转换为 Position Delete
     * 前提: 必须能够定位到具体的数据文件和行号
     */
    ConvertEqualityDeleteFiles filter(Expression expr);
}
```

**转换条件**:
```
Equality Delete → Position Delete (可能)
条件:
1. Equality Delete 的等值字段包含主键
2. 能够通过扫描数据文件定位到具体行号
3. 转换后的 Position Delete 文件更小

Position Delete → Equality Delete (不可能)
原因: Position Delete 不包含列值,无法反向推导
```

**转换示例**:

```java
// 场景: Equality Delete 文件过多,影响查询性能
Table table = ...;
table.newRewrite()
    .rewriteFiles(
        equalityDeleteFiles,  // 待转换的 Equality Delete 文件
        ImmutableSet.of(),
        positionDeleteFiles,  // 转换后的 Position Delete 文件
        ImmutableSet.of()
    )
    .commit();
```

**深入理解**: Equality Delete 转 Position Delete 是一种优化手段,通过扫描数据文件定位行号,将灵活但低效的 Equality Delete 转换为高效的 Position Delete。但这需要额外的扫描成本,只有在 Equality Delete 文件过多时才值得执行。

#### 30 秒速记（简版）

- 结论先行：DELETE FROM table WHERE user_id = 123。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/actions/ConvertEqualityDeleteFiles.java:1`，再联动 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `api/src/main/java/org/apache/iceberg/actions/ConvertEqualityDeleteFiles.java:1`
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`




#### 源码关键片段

**片段 1：`api/src/main/java/org/apache/iceberg/actions/ConvertEqualityDeleteFiles.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:92-98`**
```java
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot = Maps.newHashMap();
```

---
<a id="q-3-9"></a>
### 3.9 删除文件的小文件优化

**🟠 问题: 小文件优化(Compaction)是否会优化删除文件？如何优化？**

**答案:**

**是的,Iceberg 提供了专门的删除文件优化机制。**

#### 删除文件的小文件问题

**产生原因**:
- CDC 流式写入:每个 Checkpoint 生成一个小的 Equality Delete 文件
- 高频 DELETE 操作:每次删除生成一个小的 Position Delete 文件
- 累积效应:数千个小删除文件严重影响查询性能

**影响**:
```
查询时需要:
1. 读取所有删除文件的 Manifest
2. 加载所有删除文件到内存
3. 构建过滤器(Position Delete → Bitmap, Equality Delete → HashSet)
4. 逐行应用过滤

小文件越多,步骤 1-3 的开销越大
```

#### RewritePositionDeleteFiles Action

**源码实现**:

```java
// 源码: api/src/main/java/org/apache/iceberg/actions/RewritePositionDeleteFiles.java
public interface RewritePositionDeleteFiles extends Action<RewritePositionDeleteFiles, Result> {
    /**
     * 合并 Position Delete 文件
     */
    RewritePositionDeleteFiles filter(Expression filter);
    
    /**
     * 设置目标文件大小
     */
    RewritePositionDeleteFiles option(String name, String value);
}
```

**合并策略**:

```java
// 核心逻辑: 按照 referencedDataFile 分组合并
Map<String, List<DeleteFile>> groupedDeletes = 
    positionDeleteFiles.stream()
        .collect(Collectors.groupingBy(DeleteFile::referencedDataFile));

// 每组内的删除文件合并为一个大文件
for (Map.Entry<String, List<DeleteFile>> entry : groupedDeletes.entrySet()) {
    String dataFile = entry.getKey();
    List<DeleteFile> deletes = entry.getValue();
    
    if (deletes.size() > 1 && totalSize(deletes) < targetFileSize) {
        // 合并成一个文件
        DeleteFile mergedDelete = mergeDeletes(deletes);
        rewriteFiles(deletes, Collections.singleton(mergedDelete));
    }
}
```

**关键设计**:
- **按 referencedDataFile 分组**: 同一个数据文件的删除记录合并在一起
- **排序优化**: 合并后的文件按 `pos` 排序,读取时可以二分查找
- **大小控制**: 避免生成过大的删除文件

#### Equality Delete 文件优化

**挑战**: Equality Delete 文件无法简单合并

**原因**:
```
Equality Delete 文件 A: {user_id: 1, user_id: 2}
Equality Delete 文件 B: {user_id: 2, user_id: 3}

简单合并 → {user_id: 1, user_id: 2, user_id: 2, user_id: 3}
问题: user_id=2 重复,但无法去重(可能是不同时间的删除)
```

**解决方案**: 转换为 Position Delete

```java
// 1. 扫描数据文件,定位 Equality Delete 对应的行号
// 2. 生成 Position Delete 文件
// 3. 删除原 Equality Delete 文件

table.newRewrite()
    .rewriteFiles(
        ImmutableSet.of(),
        equalityDeleteFiles,      // 删除 Equality Delete
        ImmutableSet.of(),
        positionDeleteFiles       // 添加 Position Delete
    )
    .commit();
```

#### 配置参数

```properties
# Position Delete 文件目标大小
write.delete.target-file-size-bytes=67108864  # 64MB

# 触发合并的最小文件数
write.delete.min-file-count-to-compact=5

# 删除文件的最大数量(超过后触发合并)
write.delete.max-file-count=100
```

#### 执行示例

```java
// Spark Action
SparkActions.get()
    .rewritePositionDeletes(table)
    .option("target-file-size-bytes", "134217728")  // 128MB
    .option("min-file-size-bytes", "8388608")       // 8MB
    .execute();
```

**深入理解**: 删除文件的小文件优化与数据文件优化同样重要。在 CDC 场景下,删除文件的累积速度可能比数据文件更快,必须定期执行 `RewritePositionDeleteFiles` Action 以保持查询性能。

#### 30 秒速记（简版）

- 结论先行：小文件越多,步骤 1-3 的开销越大。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/actions/RewritePositionDeleteFiles.java:1`，再联动 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `api/src/main/java/org/apache/iceberg/actions/RewritePositionDeleteFiles.java:1`
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`




#### 源码关键片段

**片段 1：`api/src/main/java/org/apache/iceberg/actions/RewritePositionDeleteFiles.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:92-98`**
```java
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot = Maps.newHashMap();
```

---
<a id="q-3-10"></a>
### 3.10 Tag 与快照的关系

**🟡 问题: Iceberg 打 Tag 时与快照的关系是什么？Tag 的底层实现机制是什么？**

**答案:**

#### Tag 的本质

**核心概念**: Tag 是指向特定快照的**命名引用(Named Reference)**

```
Tag 不是快照的副本,而是快照的"别名"或"书签"
```

**源码定义**:

```java
// 源码: api/src/main/java/org/apache/iceberg/SnapshotRef.java
public class SnapshotRef implements Serializable {
    private final long snapshotId;           // 指向的快照 ID
    private final SnapshotRefType type;      // 类型: TAG 或 BRANCH
    private final Integer minSnapshotsToKeep;  // 保留策略(仅 BRANCH)
    private final Long maxSnapshotAgeMs;        // 快照最大年龄(仅 BRANCH)
    private final Long maxRefAgeMs;             // 引用最大年龄
    
    public boolean isTag() {
        return type == SnapshotRefType.TAG;
    }
    
    public boolean isBranch() {
        return type == SnapshotRefType.BRANCH;
    }
}
```

#### Tag vs Branch

| 特性 | Tag | Branch |
|------|-----|--------|
| 类型 | `SnapshotRefType.TAG` | `SnapshotRefType.BRANCH` |
| 可变性 | 不可变(指向固定快照) | 可变(随提交移动) |
| 保留策略 | 仅 `maxRefAgeMs` | `minSnapshotsToKeep` + `maxSnapshotAgeMs` + `maxRefAgeMs` |
| 典型用途 | 版本标记(v1.0, release-2024) | 开发分支(main, dev) |

#### 创建 Tag 的源码流程

```java
// 源码: api/src/main/java/org/apache/iceberg/ManageSnapshots.java
public interface ManageSnapshots extends PendingUpdate<Snapshot> {
    /**
     * 创建 Tag
     */
    ManageSnapshots createTag(String name, long snapshotId);
    
    /**
     * 删除 Tag
     */
    ManageSnapshots removeTag(String name);
    
    /**
     * 替换 Tag(指向新快照)
     */
    ManageSnapshots replaceTag(String name, long snapshotId);
}
```

**实际执行**:

```java
// 创建 Tag
table.manageSnapshots()
    .createTag("v1.0.0", 123456789L)  // 将 Tag "v1.0.0" 指向快照 123456789
    .commit();

// 元数据变化
{
  "refs": {
    "main": {
      "snapshot-id": 987654321,
      "type": "branch"
    },
    "v1.0.0": {
      "snapshot-id": 123456789,  // Tag 指向的快照
      "type": "tag",
      "max-ref-age-ms": 31536000000  // 1年
    }
  }
}
```

#### Tag 的生命周期管理

**保留策略**:

```java
// Tag 的保留策略仅受 maxRefAgeMs 控制
SnapshotRef tag = SnapshotRef.tagBuilder(snapshotId)
    .maxRefAgeMs(TimeUnit.DAYS.toMillis(365))  // Tag 保留 1 年
    .build();
```

**过期清理**:

```java
// ExpireSnapshots 会检查 Tag 的 maxRefAgeMs
table.expireSnapshots()
    .expireOlderThan(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30))
    .commit();

// 清理逻辑:
// 1. 检查每个 Tag 的 maxRefAgeMs
// 2. 超过年龄的 Tag 被删除
// 3. 如果快照没有其他引用(Tag/Branch),则快照也被删除
```

#### Tag 的查询用途

**时间旅行查询**:

```sql
-- Spark SQL
SELECT * FROM table VERSION AS OF 'v1.0.0';

-- Flink SQL
SELECT * FROM table /*+ OPTIONS('scan.tag-name'='v1.0.0') */;
```

**底层实现**:

```java
// 读取 Tag 指向的快照
SnapshotRef tagRef = table.refs().get("v1.0.0");
long snapshotId = tagRef.snapshotId();
Snapshot snapshot = table.snapshot(snapshotId);

// 基于该快照创建 TableScan
TableScan scan = table.newScan().useSnapshot(snapshotId);
```

#### Tag 与快照的依赖关系

**关键规则**:

```
1. Tag 依赖快照: Tag 必须指向一个存在的快照
2. 快照可以被多个 Tag 引用
3. 删除 Tag 不会删除快照(除非快照没有其他引用)
4. 删除快照会导致指向它的 Tag 失效
```

**示例**:

```
快照 A (id=100) ← Tag "v1.0" + Tag "stable"
快照 B (id=200) ← Branch "main"
快照 C (id=300) ← (无引用)

ExpireSnapshots 执行:
- 快照 A: 保留(被 2 个 Tag 引用)
- 快照 B: 保留(被 Branch 引用)
- 快照 C: 删除(无引用)
```

**深入理解**: Tag 是 Iceberg 版本管理的核心机制,通过轻量级的引用实现快照的命名和保留。与 Git Tag 类似,Iceberg Tag 提供了不可变的版本标记,非常适合用于发布版本管理、审计和回滚场景。

#### 30 秒速记（简版）

- 结论先行：Tag 不是快照的副本,而是快照的"别名"或"书签"。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/SnapshotRef.java:1`，再联动 `api/src/main/java/org/apache/iceberg/ManageSnapshots.java:1`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `api/src/main/java/org/apache/iceberg/SnapshotRef.java:1`
- `api/src/main/java/org/apache/iceberg/ManageSnapshots.java:1`




#### 源码关键片段

**片段 1：`api/src/main/java/org/apache/iceberg/SnapshotRef.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`api/src/main/java/org/apache/iceberg/ManageSnapshots.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

---
<a id="q-3-11"></a>
### 3.11 小文件合并后的历史文件删除

**🟠 问题: 小文件合并(Compaction)重写了新文件,历史文件会删除吗？在什么时候删除？**

**答案:**

**关键结论**: **历史文件不会立即删除,而是在快照过期时删除**

#### 小文件合并的完整流程

**1. Rewrite 阶段**:

```java
// 源码: core/src/main/java/org/apache/iceberg/BaseRewriteFiles.java
public class BaseRewriteFiles extends MergingSnapshotProducer<RewriteFiles> {
    
    @Override
    public RewriteFiles rewriteFiles(
        Set<DataFile> filesToDelete,  // 旧的小文件
        Set<DataFile> filesToAdd,     // 新的大文件
        long sequenceNumber) {
        
        // 1. 标记旧文件为"已删除"
        for (DataFile file : filesToDelete) {
            delete(file);  // 添加到 deletedFiles 集合
        }
        
        // 2. 添加新文件
        for (DataFile file : filesToAdd) {
            add(file);  // 添加到 addedFiles 集合
        }
        
        return this;
    }
}
```

**2. Commit 阶段**:

```java
// 提交时创建新快照
Snapshot newSnapshot = commit();

// 新快照的 Manifest 文件:
{
  "added-files": [new-file-1.parquet, new-file-2.parquet],
  "deleted-files": [old-file-1.parquet, old-file-2.parquet, ...]
}
```

**3. 文件状态变化**:

```
合并前:
Snapshot 100 → Manifest A → [old-file-1.parquet, old-file-2.parquet, ...]

合并后:
Snapshot 100 → Manifest A → [old-file-1.parquet, old-file-2.parquet, ...]  (仍存在)
Snapshot 101 → Manifest B → [new-file-1.parquet, new-file-2.parquet]       (新快照)
                           → deleted: [old-file-1.parquet, old-file-2.parquet]
```

**关键点**: 旧文件仍然存在于文件系统中,因为 Snapshot 100 仍然引用它们

#### 历史文件的删除时机

**触发条件**: 执行 `ExpireSnapshots` 操作

```java
// 源码: api/src/main/java/org/apache/iceberg/ExpireSnapshots.java
public interface ExpireSnapshots extends PendingUpdate<Snapshot> {
    /**
     * 过期指定时间之前的快照
     */
    ExpireSnapshots expireOlderThan(long timestampMillis);
    
    /**
     * 保留最近 N 个快照
     */
    ExpireSnapshots retainLast(int numSnapshots);
    
    /**
     * 删除孤儿文件
     */
    ExpireSnapshots deleteWith(Consumer<String> deleteFunc);
}
```

**执行示例**:

```java
// 过期 7 天前的快照
table.expireSnapshots()
    .expireOlderThan(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7))
    .retainLast(5)  // 至少保留最近 5 个快照
    .commit();
```

**删除逻辑**:

```java
// 伪代码: ExpireSnapshots 的核心逻辑
Set<String> filesToDelete = new HashSet<>();

// 1. 确定要删除的快照
List<Snapshot> snapshotsToExpire = determineSnapshotsToExpire();

// 2. 收集这些快照中的文件
for (Snapshot snapshot : snapshotsToExpire) {
    for (ManifestFile manifest : snapshot.allManifests()) {
        for (DataFile file : readManifest(manifest)) {
            filesToDelete.add(file.path());
        }
    }
}

// 3. 排除仍被引用的文件
Set<String> referencedFiles = collectReferencedFiles(remainingSnapshots);
filesToDelete.removeAll(referencedFiles);

// 4. 删除文件
for (String file : filesToDelete) {
    fileIO.deleteFile(file);  // 物理删除
}

// 5. 删除 Manifest 文件
for (Snapshot snapshot : snapshotsToExpire) {
    for (ManifestFile manifest : snapshot.allManifests()) {
        fileIO.deleteFile(manifest.path());
    }
}

// 6. 删除元数据文件
for (Snapshot snapshot : snapshotsToExpire) {
    fileIO.deleteFile(snapshot.manifestListLocation());
}
```

#### 完整的文件生命周期

```
Day 0: 创建小文件
├─ Snapshot 100 → [file-1.parquet (10MB), file-2.parquet (8MB), ...]
├─ 文件状态: ACTIVE

Day 1: 执行 Compaction
├─ Snapshot 101 → [merged-file.parquet (512MB)]
│                 deleted: [file-1.parquet, file-2.parquet, ...]
├─ 文件状态: 
│   ├─ file-1.parquet: ACTIVE (仍被 Snapshot 100 引用)
│   └─ merged-file.parquet: ACTIVE

Day 8: 执行 ExpireSnapshots (过期 7 天前的快照)
├─ 删除 Snapshot 100
├─ 检查 file-1.parquet:
│   └─ 不再被任何快照引用 → 物理删除
├─ 文件状态:
│   ├─ file-1.parquet: DELETED (物理删除)
│   └─ merged-file.parquet: ACTIVE
```

#### 配置参数

```properties
# 快照保留时间(默认 5 天)
write.metadata.delete-after-commit.enabled=true
write.metadata.previous-versions-max=100

# 最小保留快照数
history.expire.min-snapshots-to-keep=5

# 快照最大年龄
history.expire.max-snapshot-age-ms=604800000  # 7 天
```

#### 孤儿文件清理

**场景**: 如果 Compaction 失败,新文件已写入但未提交

```java
// 使用 RemoveOrphanFiles Action
SparkActions.get()
    .removeOrphanFiles(table)
    .olderThan(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3))
    .execute();

// 清理逻辑:
// 1. 列出表目录下的所有文件
// 2. 收集所有快照引用的文件
// 3. 找出未被引用且超过 3 天的文件
// 4. 删除这些孤儿文件
```

**深入理解**: Iceberg 的 MVCC 机制保证了时间旅行查询的能力,但也意味着历史文件不会立即删除。必须定期执行 `ExpireSnapshots` 来回收存储空间。在生产环境中,建议配置自动化的快照过期策略,平衡时间旅行能力和存储成本。

#### 30 秒速记（简版）

- 结论先行：// 源码: core/src/main/java/org/apache/iceberg/BaseRewriteFiles.java。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/BaseRewriteFiles.java:1`，再联动 `api/src/main/java/org/apache/iceberg/ExpireSnapshots.java:1`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/BaseRewriteFiles.java:1`
- `api/src/main/java/org/apache/iceberg/ExpireSnapshots.java:1`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/BaseRewriteFiles.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`api/src/main/java/org/apache/iceberg/ExpireSnapshots.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

---

## 四、文件管理与优化
<a id="q-4-1"></a>
### 4.1 小文件合并策略

**🟠 问题: 对比分析 `RewriteDataFiles` 的 BinPack、Sort 和 Z-Order 策略。**

**答案:**

Iceberg 通过 `RewriteDataFiles` action 解决小文件问题，提供三种策略。

#### 1. BinPack(装箱策略)

**原理**: 简单合并文件至目标大小(默认 512MB)，不关心数据内容

**特点**:
- 无 Shuffle 开销
- 速度最快
- 不优化数据局部性

**适用场景**:
- 主要矛盾是文件数量过多
- 对查询裁剪要求不高
- 快速完成合并，减少资源消耗

#### 2. Sort(排序策略)

**原理**: 根据排序键进行全局排序后合并

**优势**:
- **查询性能**: 高区分度的 min/max 统计，高效跳过无关文件
- **压缩率**: 相似数据连续存储，提升压缩效率

**适用场景**:
- 查询模式固定，经常按特定列范围过滤
- 对查询性能有极致要求
- 例: 按 `event_time` 查询最近数据

#### 3. Z-Order(Z阶曲线排序)

**原理**: 多维数据聚类，将多列值映射为一维 Z-Value 后排序

**优势**:
- 多维组合过滤查询高效
- 实现多维数据裁剪

**适用场景**:
- 查询涉及多个高基数列的组合过滤
- 无主要线性排序维度
- 例: `WHERE col_A='a' AND col_B='b' AND col_C>100`

#### 决策树

```
只需合并文件? → BinPack
  ↓
单列/少数列范围过滤? → Sort
  ↓
多列组合过滤? → Z-Order
```

#### 💡 面试加分话术

> "小文件合并策略选择的本质是**I/O 开销与查询性能的权衡**:
> 1. **BinPack 零 Shuffle**: 只做文件拼接，适合主要矛盾是文件数量的场景
> 2. **Sort 全局有序**: 牺牲写入时的 Shuffle 开销，换取查询时的高效裁剪
> 3. **Z-Order 多维聚类**: 通过空间填充曲线将多维问题降维，适合多列组合过滤
>
> 实践中我们通常采用**分层策略**: 每日 BinPack 快速合并，每周 Sort/Z-Order 深度优化。"

#### 🔥 高频追问

**Q: Z-Order 为什么不是万能的？什么场景下反而会降低性能？**

**A**: Z-Order 的局限性:
1. **单列高选择性查询退化**: 如果查询只过滤一列，Z-Order 的多维交织反而打散了数据
2. **高基数列效果差**: 当列的 cardinality 过高时，Z-Value 的聚类效果下降
3. **列数限制**: 通常只对 3-4 列有效，超过后维度灾难导致效果骤降

#### 30 秒速记（简版）

- 结论先行：Iceberg 通过 RewriteDataFiles action 解决小文件问题，提供三种策略。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/RemoveSnapshots.java:63`，再联动 `core/src/main/java/org/apache/iceberg/ManifestFiles.java:58`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/RemoveSnapshots.java:63`
- `core/src/main/java/org/apache/iceberg/ManifestFiles.java:58`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/RemoveSnapshots.java:60-66`**
```java
@SuppressWarnings("UnnecessaryAnonymousClass")
class RemoveSnapshots implements ExpireSnapshots {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveSnapshots.class);

  // Creates an executor service that runs each task in the thread that invokes execute/submit.
  private static final ExecutorService DEFAULT_DELETE_EXECUTOR_SERVICE =
      MoreExecutors.newDirectExecutorService();
```

**片段 2：`core/src/main/java/org/apache/iceberg/ManifestFiles.java:55-61`**
```java
              GenericManifestFile.class.getName(),
              ManifestFile.PARTITION_SUMMARY_TYPE,
              GenericPartitionFieldSummary.class.getName()));

  @VisibleForTesting
  static Caffeine<Object, Object> newManifestCacheBuilder() {
    int maxSize = SystemConfigs.IO_MANIFEST_CACHE_MAX_FILEIO.value();
```

---
<a id="q-4-2"></a>
### 4.2 垃圾回收机制

**🟡 问题: `ExpireSnapshots` 和 `DeleteOrphanFiles` 的区别和联系？**

**答案:**

两者是相辅相成的垃圾回收工具，职责不同。

#### ExpireSnapshots(快照过期)

**职责**: 清理元数据中过期的快照记录

**工作原理**:
1. 根据保留策略(如保留 7 天)计算过期快照
2. 从元数据移除过期快照引用
3. 删除仅被过期快照引用的数据文件

**特点**:
- ✅ 确定性高，安全性高
- ✅ 速度快(元数据层面)
- ❌ 无法处理从未被引用的孤儿文件

#### DeleteOrphanFiles(孤儿文件清理)

**职责**: 清理存储中存在但不被任何快照引用的文件

**工作原理**:
1. 全量扫描物理存储中的所有文件
2. 遍历元数据收集被引用的文件
3. 计算差集得到孤儿文件
4. 使用时间阈值(`olderThan`，默认 3 天)安全删除

**特点**:
- ✅ 大而全，清理所有类型孤儿文件
- ❌ 开销大，速度慢
- ⚠️ 依赖时间戳保护

#### 完整 GC 策略

```
日常(每天): ExpireSnapshots
  ↓ 快速清理大部分过期文件
定期(每周): DeleteOrphanFiles
  ↓ 兜底清理异常产生的孤儿文件
```

**最佳实践**: 两者结合，构成健壮的文件生命周期管理。

#### 30 秒速记（简版）

- 结论先行：两者是相辅相成的垃圾回收工具，职责不同。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/RemoveSnapshots.java:63`，再联动 `core/src/main/java/org/apache/iceberg/ManifestFiles.java:58`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/RemoveSnapshots.java:63`
- `core/src/main/java/org/apache/iceberg/ManifestFiles.java:58`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/RemoveSnapshots.java:60-66`**
```java
@SuppressWarnings("UnnecessaryAnonymousClass")
class RemoveSnapshots implements ExpireSnapshots {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveSnapshots.class);

  // Creates an executor service that runs each task in the thread that invokes execute/submit.
  private static final ExecutorService DEFAULT_DELETE_EXECUTOR_SERVICE =
      MoreExecutors.newDirectExecutorService();
```

**片段 2：`core/src/main/java/org/apache/iceberg/ManifestFiles.java:55-61`**
```java
              GenericManifestFile.class.getName(),
              ManifestFile.PARTITION_SUMMARY_TYPE,
              GenericPartitionFieldSummary.class.getName()));

  @VisibleForTesting
  static Caffeine<Object, Object> newManifestCacheBuilder() {
    int maxSize = SystemConfigs.IO_MANIFEST_CACHE_MAX_FILEIO.value();
```

---
<a id="q-4-3"></a>
### 4.3 Manifest 优化

**🟠 问题: `RewriteManifests` 如何提升查询性能，尤其是分区聚合？**

**答案:**

`RewriteManifests` 通过优化 Manifest 文件解决"Manifest 膨胀"问题。

#### Manifest 膨胀的影响

高频小批量写入导致数千个 Manifest 文件:
- 大量 I/O 操作(特别是对象存储)
- 大量元数据反序列化开销
- 查询规划时间极长

#### 优化机制

**1. 合并 Manifest**

将多个小 Manifest 合并为少数大文件(默认 8MB)

**效果**: 1000 个小文件 → 10 个大文件，I/O 开销显著降低

**2. 分区聚合(核心优化)**

将同一分区的数据文件条目聚集到同一 Manifest

**未优化前**:
```
分区 date='2026-02-03' 的文件信息散落在 100 个 Manifest
查询 WHERE date='2026-02-03' → 需读取 100 个 Manifest
```

**优化后**:
```
分区 date='2026-02-03' 的文件信息聚合到 1 个 Manifest
查询 WHERE date='2026-02-03' → 仅读取 1 个 Manifest
```

#### 实现机制

通过 Spark 的 `repartitionByRange` 和 `sortWithinPartitions`:

```scala
manifest_entries
  .repartitionByRange(partitionCols)
  .sortWithinPartitions(partitionCols)
  .write(newManifests)
```

**效果**: 元数据层面的高效分区剪枝

#### 30 秒速记（简版）

- 结论先行：RewriteManifests 通过优化 Manifest 文件解决"Manifest 膨胀"问题。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/RemoveSnapshots.java:63`，再联动 `core/src/main/java/org/apache/iceberg/ManifestFiles.java:58`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/RemoveSnapshots.java:63`
- `core/src/main/java/org/apache/iceberg/ManifestFiles.java:58`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/RemoveSnapshots.java:60-66`**
```java
@SuppressWarnings("UnnecessaryAnonymousClass")
class RemoveSnapshots implements ExpireSnapshots {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveSnapshots.class);

  // Creates an executor service that runs each task in the thread that invokes execute/submit.
  private static final ExecutorService DEFAULT_DELETE_EXECUTOR_SERVICE =
      MoreExecutors.newDirectExecutorService();
```

**片段 2：`core/src/main/java/org/apache/iceberg/ManifestFiles.java:55-61`**
```java
              GenericManifestFile.class.getName(),
              ManifestFile.PARTITION_SUMMARY_TYPE,
              GenericPartitionFieldSummary.class.getName()));

  @VisibleForTesting
  static Caffeine<Object, Object> newManifestCacheBuilder() {
    int maxSize = SystemConfigs.IO_MANIFEST_CACHE_MAX_FILEIO.value();
```

---
<a id="q-4-4"></a>
### 4.4 孤儿文件清理安全机制

**🟡 问题: `DeleteOrphanFiles` 有哪些安全保护机制防止误删？**

**答案:**

Iceberg 内置多重保护机制确保数据安全。

#### 1. 时间阈值保护

**机制**: 必须提供 `olderThan` 时间戳，仅删除修改时间早于该时间的文件

**目的**: 为正在写入的文件提供安全缓冲期

**示例**:
```java
deleteOrphanFiles()
  .olderThan(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3))
  .execute();
```

#### 2. 垃圾回收总开关

**机制**: 检查表属性 `gc.enabled`(默认 `true`)

**目的**: 全局保护锁，防止意外清理

**使用场景**:
- 多团队共享数据
- 数据迁移期间
- 怀疑配置问题时

#### 3. 路径前缀检查

**机制**: 处理不同 URI scheme(`s3://`, `s3a://`, `s3n://`)

**策略**(`prefix-mismatch-mode`):
- `ERROR`(默认): 立即报错，最安全
- `IGNORE`: 跳过不匹配文件
- `DELETE`: 删除不匹配文件(危险)

**目的**: 防止因 URI 不一致误删有效文件

#### 4. Dry-Run 模式

**机制**: 提供自定义删除函数，仅打印不执行

**目的**: 演习模式，确认后再执行

**示例**:
```java
deleteOrphanFiles()
  .deleteWith(file -> System.out.println("Would delete: " + file))
  .execute();
```

**最佳实践**: 生产环境先 Dry-Run，确认无误后再执行真正删除。

#### 💡 面试加分话术

> "孤儿文件清理是高风险操作，Iceberg 通过**四层防护**确保安全:
> 1. **时间阈值**: `olderThan` 参数保护正在写入的文件
> 2. **全局开关**: `gc.enabled` 作为最后一道防线
> 3. **URI 校验**: 防止因 scheme 不一致导致的误删
> 4. **Dry-Run**: 先演练再执行，可逆操作
>
> 实践中最重要的是**时间阈值设置**: 必须大于最长可能的事务持续时间 + 文件系统时钟偏差。
> 我们通常设置 3 天，确保即使有长事务也不会误删。"

#### 🔥 高频追问

**Q: 为什么会产生孤儿文件？常见原因有哪些？**

**A**: 孤儿文件的产生场景:
1. **写入失败**: 文件写入成功但 commit 失败，文件未被引用
2. **Compaction 中断**: 重写文件时任务失败，新文件成为孤儿
3. **Spark 任务重试**: 推测执行产生的重复文件
4. **手动干预**: 人为删除元数据但未删除数据文件

**预防措施**: 定期执行 `DeleteOrphanFiles`，配合 `ExpireSnapshots` 形成完整的 GC 策略。

#### 30 秒速记（简版）

- 结论先行：Iceberg 内置多重保护机制确保数据安全。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/RemoveSnapshots.java:63`，再联动 `core/src/main/java/org/apache/iceberg/ManifestFiles.java:58`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/RemoveSnapshots.java:63`
- `core/src/main/java/org/apache/iceberg/ManifestFiles.java:58`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/RemoveSnapshots.java:60-66`**
```java
@SuppressWarnings("UnnecessaryAnonymousClass")
class RemoveSnapshots implements ExpireSnapshots {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveSnapshots.class);

  // Creates an executor service that runs each task in the thread that invokes execute/submit.
  private static final ExecutorService DEFAULT_DELETE_EXECUTOR_SERVICE =
      MoreExecutors.newDirectExecutorService();
```

**片段 2：`core/src/main/java/org/apache/iceberg/ManifestFiles.java:55-61`**
```java
              GenericManifestFile.class.getName(),
              ManifestFile.PARTITION_SUMMARY_TYPE,
              GenericPartitionFieldSummary.class.getName()));

  @VisibleForTesting
  static Caffeine<Object, Object> newManifestCacheBuilder() {
    int maxSize = SystemConfigs.IO_MANIFEST_CACHE_MAX_FILEIO.value();
```

---

## 五、查询与读取优化
<a id="q-5-1"></a>
### 5.1 多层剪枝机制

**🟡 问题: 简述 Iceberg 读取流程中的关键剪枝步骤。**

**答案:**

Iceberg 通过多层次、由粗到细的剪枝机制避免不必要的 I/O。

#### 完整读取流程

**1. 确定快照**
```
SQL 查询 → 定位当前快照(或时间旅行指定快照)
```

**2. 分区剪枝(第一层)**

- **输入**: `WHERE` 条件(如 `event_date='2026-02-03'`)
- **动作**: 读取 Manifest List，检查每个 Manifest File 的分区范围统计
- **剪枝**: 过滤不包含目标分区的 Manifest File

**示例**:
```
Manifest File A: event_date ∈ ['2026-01-01', '2026-01-31'] → 跳过
Manifest File B: event_date ∈ ['2026-02-01', '2026-02-28'] → 保留
```

**3. 数据文件剪枝(第二层)**

- **输入**: `WHERE` 条件和幸存的 Manifest File
- **动作**: 读取 Manifest File，检查每个数据文件的列级统计(min/max/null_count)
- **剪枝**: 
  - Min/Max 剪枝: `WHERE price>500` 且 `max(price)=450` → 跳过
  - Null 剪枝: `WHERE user_id IS NOT NULL` 且 `null_count=record_count` → 跳过

**4. 生成扫描任务**
```
剩余数据文件 → FileScanTask → 分发到 Executor
```

**5. 读取数据**
```
Executor → 打开文件 → 应用删除文件 → 返回数据
```

#### 核心优势

通过两层元数据索引(Manifest List 分区统计 + Manifest File 列统计)，避免传统数据湖的"List + 暴力扫描"模式。

# 5.1 多层剪枝机制 - 源码深度扩展

#### 剪枝的源码实现

**核心类**: `ManifestGroup`

```java
// 源码: core/src/main/java/org/apache/iceberg/ManifestGroup.java
public class ManifestGroup {
    private final FileIO io;
    private final Set<ManifestFile> dataManifests;      // 所有 Manifest 文件
    private final Set<ManifestFile> deleteManifests;    // 删除文件的 Manifest
    
    private Expression dataFilter = Expressions.alwaysTrue();      // 数据过滤器
    private Expression partitionFilter = Expressions.alwaysTrue(); // 分区过滤器
    private Expression fileFilter = Expressions.alwaysTrue();      // 文件过滤器
    
    /**
     * 设置数据过滤器(用于列级别剪枝)
     */
    public ManifestGroup filterData(Expression newDataFilter) {
        this.dataFilter = Expressions.and(dataFilter, newDataFilter);
        return this;
    }
    
    /**
     * 设置分区过滤器(用于分区级别剪枝)
     */
    public ManifestGroup filterPartitions(Expression newPartitionFilter) {
        this.partitionFilter = Expressions.and(partitionFilter, newPartitionFilter);
        return this;
    }
}
```

#### 第一层剪枝: Manifest 文件级别

**使用 ManifestEvaluator 进行分区剪枝**:

```java
// 源码: api/src/main/java/org/apache/iceberg/expressions/ManifestEvaluator.java
public class ManifestEvaluator {
    private final PartitionSpec spec;
    private final Expression expr;
    
    /**
     * 评估 Manifest 文件是否可能包含匹配的数据
     */
    public boolean eval(ManifestFile manifest) {
        // 1. 检查分区统计信息
        if (manifest.partitions() == null || manifest.partitions().isEmpty()) {
            return true;  // 无统计信息,保守地保留
        }
        
        // 2. 使用分区边界进行剪枝
        for (FieldSummary summary : manifest.partitions()) {
            if (!canContainMatches(summary)) {
                return false;  // 该 Manifest 不可能包含匹配数据
            }
        }
        
        return true;
    }
    
    private boolean canContainMatches(FieldSummary summary) {
        // 检查 lower_bound 和 upper_bound
        // 例如: WHERE event_date='2026-02-10'
        // 如果 upper_bound < '2026-02-10' 或 lower_bound > '2026-02-10'
        // 则返回 false
        return evaluateAgainstBounds(summary.lowerBound(), summary.upperBound());
    }
}
```

**ManifestGroup 中的应用**:

```java
// 源码: core/src/main/java/org/apache/iceberg/ManifestGroup.java
private CloseableIterable<ManifestFile> matchingManifests() {
    // 1. 创建 ManifestEvaluator
    ManifestEvaluator evaluator = ManifestEvaluator.forRowFilter(
        partitionFilter,  // 用户的 WHERE 条件
        spec,
        caseSensitive
    );
    
    // 2. 过滤 Manifest 文件
    Iterable<ManifestFile> filtered = Iterables.filter(
        dataManifests,
        manifest -> evaluator.eval(manifest)  // 第一层剪枝
    );
    
    return CloseableIterable.withNoopClose(filtered);
}
```

**剪枝示例**:

```
查询: SELECT * FROM table WHERE event_date='2026-02-10'

Manifest List 包含 100 个 Manifest 文件:
- Manifest 1: partitions=[{lower:'2026-01-01', upper:'2026-01-31'}]
  → evaluator.eval() = false (2026-02-10 不在范围内)
  → 跳过,无需读取

- Manifest 2: partitions=[{lower:'2026-02-01', upper:'2026-02-28'}]
  → evaluator.eval() = true (2026-02-10 在范围内)
  → 保留,继续处理

结果: 100 个 Manifest 文件 → 剪枝后剩余 3 个
```

#### 第二层剪枝: 数据文件级别

**使用 ManifestEntry 的统计信息**:

```java
// 源码: core/src/main/java/org/apache/iceberg/ManifestGroup.java
private CloseableIterable<ManifestEntry<DataFile>> matchingEntries() {
    return CloseableIterable.transform(
        matchingManifests(),  // 第一层剪枝后的 Manifest 文件
        manifest -> {
            // 1. 读取 Manifest 文件的所有条目
            CloseableIterable<ManifestEntry<DataFile>> entries = 
                ManifestFiles.read(manifest, io, specsById);
            
            // 2. 应用数据过滤器(第二层剪枝)
            return CloseableIterable.filter(
                entries,
                entry -> {
                    // 2.1 检查文件级别的统计信息
                    DataFile file = entry.file();
                    
                    // 2.2 Min/Max 剪枝
                    if (!canContainMatches(file, dataFilter)) {
                        return false;  // 跳过该文件
                    }
                    
                    // 2.3 Null 剪枝
                    if (!passesNullCheck(file, dataFilter)) {
                        return false;  // 跳过该文件
                    }
                    
                    return true;  // 保留该文件
                }
            );
        }
    );
}
```

**Min/Max 剪枝实现**:

```java
private boolean canContainMatches(DataFile file, Expression filter) {
    // 使用 ResidualEvaluator 评估文件是否可能包含匹配数据
    ResidualEvaluator residualEvaluator = ResidualEvaluator.of(
        spec,
        filter,
        caseSensitive
    );
    
    // 检查文件的 lower_bounds 和 upper_bounds
    Expression residual = residualEvaluator.residualFor(file.partition());
    
    if (residual == Expressions.alwaysFalse()) {
        return false;  // 该文件不可能包含匹配数据
    }
    
    // 进一步检查列级别统计
    Map<Integer, ByteBuffer> lowerBounds = file.lowerBounds();
    Map<Integer, ByteBuffer> upperBounds = file.upperBounds();
    
    // 例如: WHERE price > 1000
    // 如果 upperBounds.get(priceFieldId) <= 1000
    // 则返回 false
    
    return evaluateColumnBounds(filter, lowerBounds, upperBounds);
}
```

**Null 剪枝实现**:

```java
private boolean passesNullCheck(DataFile file, Expression filter) {
    Map<Integer, Long> nullValueCounts = file.nullValueCounts();
    Long recordCount = file.recordCount();
    
    // 例如: WHERE user_id IS NOT NULL
    // 如果 nullValueCounts.get(userIdFieldId) == recordCount
    // 说明所有行的 user_id 都是 NULL,可以跳过该文件
    
    for (Integer fieldId : extractNotNullFields(filter)) {
        Long nullCount = nullValueCounts.get(fieldId);
        if (nullCount != null && nullCount.equals(recordCount)) {
            return false;  // 该文件所有行都是 NULL,跳过
        }
    }
    
    return true;
}
```

**剪枝示例**:

```
查询: SELECT * FROM table WHERE price > 1000

Manifest 文件包含 1000 个数据文件:
- file_1.parquet: 
    lowerBounds={price: 100}, upperBounds={price: 500}
  → upperBounds[price]=500 < 1000
  → 跳过,无需读取

- file_2.parquet:
    lowerBounds={price: 800}, upperBounds={price: 1200}
  → upperBounds[price]=1200 >= 1000
  → 保留,需要读取

- file_3.parquet:
    nullValueCounts={price: 10000}, recordCount=10000
  → 所有行的 price 都是 NULL
  → 跳过,无需读取

结果: 1000 个数据文件 → 剪枝后剩余 50 个
```

#### 完整的剪枝流程

```java
// 源码: core/src/main/java/org/apache/iceberg/ManifestGroup.java
public CloseableIterable<FileScanTask> planFiles() {
    // 1. 第一层剪枝: Manifest 文件级别(分区剪枝)
    CloseableIterable<ManifestFile> matchingManifests = matchingManifests();
    
    // 2. 第二层剪枝: 数据文件级别(列剪枝)
    CloseableIterable<ManifestEntry<DataFile>> matchingEntries = 
        CloseableIterable.transform(
            matchingManifests,
            manifest -> filterEntries(manifest)  // 应用 Min/Max 和 Null 剪枝
        );
    
    // 3. 生成 FileScanTask
    return CloseableIterable.transform(
        matchingEntries,
        entry -> new BaseFileScanTask(
            entry.file(),
            deleteFiles,  // 关联的删除文件
            schemaAsString,
            specAsString,
            residuals     // 残留的过滤条件(需要在读取时应用)
        )
    );
}
```

**性能优势**:

```
原始场景:
- 100 个 Manifest 文件
- 每个 Manifest 包含 1000 个数据文件
- 总共 100,000 个数据文件

经过两层剪枝:
- 第一层: 100 个 Manifest → 3 个 Manifest (减少 97%)
- 第二层: 3,000 个数据文件 → 50 个数据文件 (减少 98.3%)

最终: 仅需读取 50 个数据文件,而非 100,000 个
I/O 减少: 99.95%
```

**深入理解**: Iceberg 的多层剪枝机制是其查询性能的核心优势。通过 Manifest List 的分区统计和 Manifest File 的列统计,在查询规划阶段就能大幅减少需要读取的文件数量。这种设计避免了传统数据湖的"List + 暴力扫描"模式,使得 Iceberg 能够在 PB 级数据规模下保持秒级查询响应。

#### 💡 面试加分话术

> "Iceberg 的多层剪枝是**元数据驱动查询优化**的典范:
> 1. **第一层 Manifest List 剪枝**: O(1) 复杂度，通过分区范围统计跳过整个 Manifest 文件
> 2. **第二层 Manifest Entry 剪枝**: O(n) 复杂度（n=Manifest 条目数），通过 min/max/null_count 跳过数据文件
> 3. **残差过滤器(Residual)**: 无法在元数据层剪枝的条件，生成 Residual 表达式下推到文件读取层
>
> 这种**层层递进的剪枝策略**，使得 PB 级数据的查询规划能在秒级完成，这是 Iceberg 相比传统 Hive 的核心优势。"

#### 🔥 高频追问

**Q: 如果数据文件没有 min/max 统计信息，Iceberg 会怎么处理？**

**A**: Iceberg 采用保守策略:
```java
// 源码: ManifestGroup.java
if (file.lowerBounds() == null || file.upperBounds() == null) {
    return true;  // 无统计信息时,保守地保留文件
}
```
这意味着:
1. 老版本写入的文件（无统计）会退化为全量扫描
2. 某些文件格式不支持统计时同样退化
3. **最佳实践**: 使用 RewriteDataFiles 重写老文件以补全统计信息

---

# 6.4 Position Delete 应用机制 - 源码深度扩展

#### Position Delete 的核心数据结构

**PositionDeleteIndex 接口**:

```java
// 源码: core/src/main/java/org/apache/iceberg/deletes/PositionDeleteIndex.java
public interface PositionDeleteIndex {
    /**
     * 标记一个位置为已删除
     */
    void delete(long position);
    
    /**
     * 标记一个位置范围为已删除
     */
    void delete(long posStart, long posEnd);
    
    /**
     * 检查某个位置是否已删除
     */
    boolean isDeleted(long position);
    
    /**
     * 合并另一个索引
     */
    void merge(PositionDeleteIndex that);
    
    /**
     * 返回已删除位置的数量
     */
    long cardinality();
}
```

#### BitmapPositionDeleteIndex 实现

**核心设计**: 使用 **Roaring Bitmap** 存储删除位置

```java
// 源码: core/src/main/java/org/apache/iceberg/deletes/BitmapPositionDeleteIndex.java
class BitmapPositionDeleteIndex implements PositionDeleteIndex {
    private final RoaringPositionBitmap bitmap;  // 核心: Roaring Bitmap
    private final List<DeleteFile> deleteFiles;  // 关联的删除文件
    
    @Override
    public void delete(long position) {
        bitmap.set(position);  // 在位图中设置该位置
    }
    
    @Override
    public boolean isDeleted(long position) {
        return bitmap.contains(position);  // O(1) 时间复杂度
    }
    
    @Override
    public void merge(PositionDeleteIndex that) {
        if (that instanceof BitmapPositionDeleteIndex) {
            // 位图合并: 使用 OR 操作
            bitmap.setAll(((BitmapPositionDeleteIndex) that).bitmap);
            deleteFiles.addAll(that.deleteFiles());
        }
    }
}
```

**Roaring Bitmap 的优势**:

```
传统 Bitmap:
- 存储 1,000,000 个位置需要 125KB (1,000,000 / 8)
- 即使只有 10 个删除位置,也需要 125KB

Roaring Bitmap:
- 压缩存储,仅存储实际的删除位置
- 10 个删除位置仅需 ~100 字节
- 压缩率: 99.92%
```

#### Position Delete 的构建流程

**1. 读取 Position Delete 文件**:

```java
// 源码: core/src/main/java/org/apache/iceberg/deletes/PositionDeleteIndexUtil.java
public static PositionDeleteIndex buildIndex(DeleteFile deleteFile, FileIO io) {
    // 1. 创建空索引
    BitmapPositionDeleteIndex index = new BitmapPositionDeleteIndex(deleteFile);
    
    // 2. 读取 Position Delete 文件
    try (CloseableIterable<Record> deletes = readDeletes(deleteFile, io)) {
        for (Record delete : deletes) {
            // 3. 提取位置信息
            String filePath = delete.getField("file_path");
            Long position = delete.getField("pos");
            
            // 4. 添加到索引
            if (filePath.equals(targetDataFile)) {
                index.delete(position);
            }
        }
    }
    
    return index;
}
```

**2. 合并多个 Position Delete 文件**:

```java
public static PositionDeleteIndex mergeIndexes(
    List<DeleteFile> deleteFiles,
    String dataFilePath,
    FileIO io) {
    
    BitmapPositionDeleteIndex mergedIndex = new BitmapPositionDeleteIndex();
    
    for (DeleteFile deleteFile : deleteFiles) {
        // 1. 为每个删除文件构建索引
        PositionDeleteIndex index = buildIndex(deleteFile, io);
        
        // 2. 合并到总索引
        mergedIndex.merge(index);
    }
    
    return mergedIndex;
}
```

#### Position Delete 的应用流程

**在读取数据文件时应用删除**:

```java
// 源码: data/src/main/java/org/apache/iceberg/data/DeleteFilter.java
public class DeleteFilter<T> {
    private final PositionDeleteIndex positionDeletes;  // 位置删除索引
    
    /**
     * 过滤已删除的行
     */
    public CloseableIterable<T> filter(CloseableIterable<T> rows) {
        return new CloseableIterable<T>() {
            private long currentPosition = 0;
            
            @Override
            public CloseableIterator<T> iterator() {
                CloseableIterator<T> rowIterator = rows.iterator();
                
                return new CloseableIterator<T>() {
                    @Override
                    public boolean hasNext() {
                        // 跳过已删除的行
                        while (rowIterator.hasNext()) {
                            if (!positionDeletes.isDeleted(currentPosition)) {
                                return true;  // 找到未删除的行
                            }
                            rowIterator.next();  // 跳过已删除的行
                            currentPosition++;
                        }
                        return false;
                    }
                    
                    @Override
                    public T next() {
                        T row = rowIterator.next();
                        currentPosition++;
                        return row;
                    }
                };
            }
        };
    }
}
```

**完整的读取流程**:

```java
// 1. 打开数据文件
ParquetReader<Record> dataReader = openDataFile("data/file_A.parquet");

// 2. 构建 Position Delete 索引
List<DeleteFile> deleteFiles = getDeleteFilesFor("data/file_A.parquet");
PositionDeleteIndex deleteIndex = mergeIndexes(deleteFiles, "data/file_A.parquet", io);

// 3. 应用删除过滤
CloseableIterable<Record> rows = dataReader.read();
CloseableIterable<Record> filteredRows = new DeleteFilter(deleteIndex).filter(rows);

// 4. 读取数据
for (Record row : filteredRows) {
    // 仅返回未删除的行
    process(row);
}
```

#### 优化: 向量化应用

**批量检查删除位置**:

```java
// 源码: parquet/src/main/java/org/apache/iceberg/parquet/VectorizedDeleteFilter.java
public class VectorizedDeleteFilter {
    private final PositionDeleteIndex deleteIndex;
    
    /**
     * 向量化过滤: 批量检查 1024 行
     */
    public int[] filterBatch(int batchSize, long startPosition) {
        int[] validRows = new int[batchSize];
        int validCount = 0;
        
        for (int i = 0; i < batchSize; i++) {
            long position = startPosition + i;
            if (!deleteIndex.isDeleted(position)) {
                validRows[validCount++] = i;  // 记录有效行的索引
            }
        }
        
        return Arrays.copyOf(validRows, validCount);
    }
}
```

**性能对比**:

```
逐行检查:
- 1,000,000 行数据
- 每行调用 isDeleted(): 1,000,000 次函数调用
- 耗时: ~100ms

向量化批量检查:
- 1,000,000 行数据,分 1000 个批次
- 每批次处理 1000 行: 1,000 次函数调用
- 耗时: ~10ms
- 性能提升: 10x
```

#### Deletion Vector (V3 特性)

**存储格式优化**:

```java
// V2: Position Delete 文件
// 文件格式: Parquet
// Schema: {file_path: string, pos: long}
// 大小: 每个删除位置 ~50 字节

// V3: Deletion Vector (DV)
// 文件格式: Puffin (二进制 Blob)
// 内容: 序列化的 Roaring Bitmap
// 大小: 每个删除位置 ~0.1 字节 (压缩后)
```

**序列化和反序列化**:

```java
// 源码: core/src/main/java/org/apache/iceberg/deletes/BitmapPositionDeleteIndex.java
@Override
public ByteBuffer serialize() {
    // 1. Run-length 编码压缩
    bitmap.runLengthEncode();
    
    // 2. 计算大小
    int bitmapDataLength = MAGIC_NUMBER_SIZE + bitmap.serializedSizeInBytes();
    byte[] bytes = new byte[LENGTH_SIZE + bitmapDataLength + CRC_SIZE];
    
    // 3. 写入 Magic Number
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.putInt(bitmapDataLength);
    buffer.putInt(MAGIC_NUMBER);  // 1681511377
    
    // 4. 序列化 Bitmap
    bitmap.serialize(buffer);
    
    // 5. 计算 CRC 校验和
    int crc = computeChecksum(bytes, bitmapDataLength);
    buffer.putInt(crc);
    
    return buffer;
}

public static PositionDeleteIndex deserialize(byte[] bytes, DeleteFile deleteFile) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    
    // 1. 读取并验证长度
    int length = buffer.getInt();
    
    // 2. 验证 Magic Number
    int magicNumber = buffer.getInt();
    Preconditions.checkArgument(magicNumber == MAGIC_NUMBER, "Invalid magic number");
    
    // 3. 反序列化 Bitmap
    RoaringPositionBitmap bitmap = RoaringPositionBitmap.deserialize(buffer);
    
    // 4. 验证 CRC
    int crc = computeChecksum(bytes, length);
    int expectedCrc = buffer.getInt();
    Preconditions.checkArgument(crc == expectedCrc, "Invalid CRC");
    
    return new BitmapPositionDeleteIndex(bitmap, deleteFile);
}
```

**存储效率对比**:

```
场景: 1,000,000 行数据,删除 10,000 行

V2 Position Delete 文件:
- 10,000 行 × 50 字节 = 500KB

V3 Deletion Vector:
- Roaring Bitmap 压缩后: ~10KB
- 压缩率: 98%
```

**深入理解**: Position Delete 通过 Roaring Bitmap 实现了高效的删除位置索引。Bitmap 的 O(1) 查询复杂度和极高的压缩率,使得 Iceberg 能够在 Merge-on-Read 模式下保持高性能。V3 的 Deletion Vector 进一步优化了存储效率,将删除文件的大小减少了 98%,这对于高频更新的场景至关重要。

---

#### 30 秒速记（简版）

- 结论先行：Iceberg 通过多层次、由粗到细的剪枝机制避免不必要的 I/O。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/ManifestGroup.java:1`，再联动 `api/src/main/java/org/apache/iceberg/expressions/ManifestEvaluator.java:1`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/ManifestGroup.java:1`
- `api/src/main/java/org/apache/iceberg/expressions/ManifestEvaluator.java:1`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/ManifestGroup.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`api/src/main/java/org/apache/iceberg/expressions/ManifestEvaluator.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

---
<a id="q-5-2"></a>
### 5.2 谓词下推优化

**🟡 问题: 解释 Iceberg 如何利用元数据进行 Filter Pushdown。**

**答案:**

Iceberg 的谓词下推通过元数据统计信息在查询规划阶段过滤文件。

#### 分区级别剪枝(Manifest List)

**元数据**: 每个 Manifest File 的分区范围

**示例**:
```json
{
  "manifest_path": "...",
  "partitions": [
    {"lower_bound": "2026-01-01", "upper_bound": "2026-01-15"},
    {"lower_bound": "A", "upper_bound": "C"}
  ]
}
```

**工作原理**:
```sql
WHERE event_date='2026-02-10'
→ '2026-02-10' ∉ ['2026-01-01', '2026-01-15']
→ 跳过该 Manifest File 及其所有数据文件
```

#### 列级别剪枝(Manifest File)

**元数据**: 每个数据文件的列统计(`lower_bounds`, `upper_bounds`, `null_value_counts`)

**Min/Max 剪枝**:
```sql
WHERE price > 1000
→ 检查 max(price)
→ max(price)=800 → 跳过该文件
```

**Null 剪枝**:
```sql
WHERE user_id IS NOT NULL
→ 检查 null_value_counts(user_id)
→ null_count = record_count → 跳过该文件
```

#### 两阶段过滤

```
第一阶段: Manifest List → 粗粒度分区剪枝
  ↓
第二阶段: Manifest File → 精细化列剪枝
  ↓
最终: 最小化需要读取的数据文件
```

#### 30 秒速记（简版）

- 结论先行：Iceberg 的谓词下推通过元数据统计信息在查询规划阶段过滤文件。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/BaseTableScan.java:45`，再联动 `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:127`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/BaseTableScan.java:45`
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:127`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/BaseTableScan.java:42-48`**
```java
  @Override
  public CloseableIterable<CombinedScanTask> planTasks() {
    CloseableIterable<FileScanTask> fileScanTasks = planFiles();
    CloseableIterable<FileScanTask> splitFiles =
        TableScanUtil.splitFiles(fileScanTasks, targetSplitSize());
    return TableScanUtil.planTasks(
        splitFiles, targetSplitSize(), splitLookback(), splitOpenFileCost());
```

**片段 2：`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:124-130`**
```java
  }

  @Override
  public void filter(Predicate[] predicates) {
    Expression runtimeFilterExpr = convertRuntimeFilters(predicates);

    if (runtimeFilterExpr != Expressions.alwaysTrue()) {
```

---
<a id="q-5-3"></a>
### 5.3 时间旅行实现

**🟢 问题: Iceberg 如何实现时间旅行查询？**

**答案:**

基于快照的不可变元数据历史实现。

#### 实现原理

**1. 不可变元数据历史**

`metadata.json` 包含所有历史快照列表:
```json
{
  "snapshots": [
    {"snapshot-id": 1, "timestamp-ms": 1706000000000},
    {"snapshot-id": 2, "timestamp-ms": 1706100000000},
    {"snapshot-id": 3, "timestamp-ms": 1706200000000}
  ]
}
```

**2. 定位目标快照**

- **按 ID**: `FOR VERSION AS OF 2` → 直接查找 `snapshot-id=2`
- **按时间**: `FOR SYSTEM_TIME AS OF '2026-02-03 10:07:00'`
  ```
  快照历史: [10:00, 10:05, 10:10]
  查询 10:07 → 定位到 10:05 的快照
  ```

**3. 从目标快照读取**

后续流程与普通查询完全一致:
```
目标快照 → Manifest List → Manifest File → 数据文件
```

#### 关键点

- **元数据层面**: 仅改变查询入口点(快照 ID)
- **依赖保留策略**: 时间旅行窗口取决于 `expireSnapshots` 配置
- **性能开销**: 与查询最新数据几乎无差异

**示例**:
```sql
-- 查询昨天的数据
SELECT * FROM orders 
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-02-03 00:00:00'
WHERE order_date = '2026-02-02';
```

#### 30 秒速记（简版）

- 结论先行：基于快照的不可变元数据历史实现。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/BaseTableScan.java:45`，再联动 `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:127`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/BaseTableScan.java:45`
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:127`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/BaseTableScan.java:42-48`**
```java
  @Override
  public CloseableIterable<CombinedScanTask> planTasks() {
    CloseableIterable<FileScanTask> fileScanTasks = planFiles();
    CloseableIterable<FileScanTask> splitFiles =
        TableScanUtil.splitFiles(fileScanTasks, targetSplitSize());
    return TableScanUtil.planTasks(
        splitFiles, targetSplitSize(), splitLookback(), splitOpenFileCost());
```

**片段 2：`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:124-130`**
```java
  }

  @Override
  public void filter(Predicate[] predicates) {
    Expression runtimeFilterExpr = convertRuntimeFilters(predicates);

    if (runtimeFilterExpr != Expressions.alwaysTrue()) {
```

---
<a id="q-5-4"></a>
### 5.4 Spark vs Flink 读取对比

**🟠 问题: 对比 Spark 和 Flink 读取 Iceberg 的实现差异。**

**答案:**

两者基于不同 API 实现，但核心逻辑相似。

#### Spark 读取(DataSource V2)

**API 集成**: `DataSource V2 API`

**任务规划**:
1. **Driver 端**: `SparkBatchQueryScan` 完成所有查询规划
2. **生成任务**: `FileScanTask` → `SparkInputPartition`
3. **分发**: 一次性分发到 Executors

**数据读取**:
- Executor 创建 `RowDataReader`
- 打开文件并应用 Merge-on-Read

**向量化读取**:
- ✅ 原生支持 `ParquetVectorizedReader`
- 数据以 `ColumnarBatch` 形式处理

#### Flink 读取(FLIP-27 Source)

**API 集成**: `FLIP-27 Source API`

**组件**:
- `SplitEnumerator`: 运行在 JobManager
- `SourceReader`: 运行在 TaskManager

**任务规划(流式)**:
1. **JobManager**: `ContinuousIcebergEnumerator` 周期性扫描新快照
2. **发现增量**: `IncrementalAppendScan` 发现新数据文件
3. **动态分配**: 按需分配 `IcebergSourceSplit` 给 SourceReader

**向量化读取**:
- ✅ 原生支持 `VectorizedRowDataParquetInputFormat`
- 数据以 `ColumnarRowData` 形式处理

#### 核心差异

| 维度 | Spark(批处理) | Flink(流式) |
|------|---------------|-------------|
| **规划协调者** | Driver | JobManager(`SplitEnumerator`) |
| **任务单元** | `SparkInputPartition`(静态) | `IcebergSourceSplit`(动态) |
| **任务分发** | 一次性分发 | 持续按需分发 |
| **核心场景** | 读取特定快照 | 持续消费新快照 |
| **向量化** | ✅ `ColumnarBatch` | ✅ `ColumnarRowData` |

#### 共性

- 都支持向量化读取(关键性能优化)
- Executor/TaskManager 层面的读取逻辑相似
- 都应用 Merge-on-Read 处理删除文件

---

## 总结

本文档从面试官视角系统梳理了 Apache Iceberg 的核心知识点，涵盖:

1. **Schema 演进**: fieldId 机制、默认值、Name Mapping
2. **事务控制**: ACID 实现、并发冲突、幂等性保证
3. **流式集成**: Flink 2PC、删除机制、CDC 优化
4. **文件管理**: 小文件合并、垃圾回收、Manifest 优化
5. **查询优化**: 多层剪枝、谓词下推、时间旅行

**面试建议**:
- 🟢 基础问题: 理解概念和基本原理
- 🟡 中级问题: 掌握实现机制和源码逻辑
- 🟠 高级问题: 理解架构设计和性能优化
- 🔴 专家问题: 深入边界场景和生产实践

**持续学习**: Iceberg 快速演进，建议关注官方文档和社区动态，及时更新知识体系。

#### 30 秒速记（简版）

- 结论先行：两者基于不同 API 实现，但核心逻辑相似。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/BaseTableScan.java:45`，再联动 `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:127`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/BaseTableScan.java:45`
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:127`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/BaseTableScan.java:42-48`**
```java
  @Override
  public CloseableIterable<CombinedScanTask> planTasks() {
    CloseableIterable<FileScanTask> fileScanTasks = planFiles();
    CloseableIterable<FileScanTask> splitFiles =
        TableScanUtil.splitFiles(fileScanTasks, targetSplitSize());
    return TableScanUtil.planTasks(
        splitFiles, targetSplitSize(), splitLookback(), splitOpenFileCost());
```

**片段 2：`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:124-130`**
```java
  }

  @Override
  public void filter(Predicate[] predicates) {
    Expression runtimeFilterExpr = convertRuntimeFilters(predicates);

    if (runtimeFilterExpr != Expressions.alwaysTrue()) {
```

---

## 六、Spark 与 Flink 读取机制深度解析
<a id="q-6-1"></a>
### 6.1 FileScanTask 结构与设计

**🟡 问题: 请从源码角度说明 `FileScanTask` 如何封装 DataFile 与 DeleteFile 的关联关系？**

**答案:**

`FileScanTask` 是 Iceberg 读取机制的核心抽象,它将一个 DataFile 与其关联的 DeleteFile 列表封装为一个扫描任务单元。

#### 接口定义

```java
// 源码: api/src/main/java/org/apache/iceberg/FileScanTask.java
public interface FileScanTask extends ContentScanTask<DataFile>,
                                       SplittableScanTask<FileScanTask> {
    /**
     * 返回需要应用的 DeleteFile 列表
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

#### BaseFileScanTask 实现

```java
// 源码: core/src/main/java/org/apache/iceberg/BaseFileScanTask.java
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

    // 懒加载Delete文件大小,避免重复计算
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

#### 核心设计要点

1. **一对多关系**: 一个 DataFile 对应多个 DeleteFile
2. **懒加载优化**: `deleteList` 和 `deletesSizeBytes` 延迟初始化,优化内存
3. **不可变性**: 使用 `ImmutableList` 确保线程安全
4. **大小计算**: 任务大小 = DataFile 大小 + 所有 DeleteFile 大小
5. **序列化优化**: `transient` 关键字避免序列化缓存字段

**深入理解**: 通过封装 DataFile 和 DeleteFile 列表,Iceberg 将 Merge-on-Read 的复杂性隐藏在 Task 内部,上层调度器只需要按 Task 分配即可,无需理解 Delete 逻辑。

#### 30 秒速记（简版）

- 结论先行：FileScanTask 是 Iceberg 读取机制的核心抽象,它将一个 DataFile 与其关联的 DeleteFile 列表封装为一个扫描任务单元。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/FileScanTask.java:1`，再联动 `core/src/main/java/org/apache/iceberg/BaseFileScanTask.java:1`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `api/src/main/java/org/apache/iceberg/FileScanTask.java:1`
- `core/src/main/java/org/apache/iceberg/BaseFileScanTask.java:1`




#### 源码关键片段

**片段 1：`api/src/main/java/org/apache/iceberg/FileScanTask.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`core/src/main/java/org/apache/iceberg/BaseFileScanTask.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

---
<a id="q-6-2"></a>
### 6.2 DeleteFile 关联机制

**🟠 问题: Spark/Flink 在构建 `FileScanTask` 时,如何判断哪些 `DeleteFile` 需要关联到某个 `DataFile`？**

**答案（总）:**

不是“所有 Delete 都全局广播”。当前源码是**分层索引匹配**: 先按 delete 类型拆分,再按 `global / partition / path / dv` 四个维度命中候选集,最后再做 sequence 与统计过滤。

**答案（分）:**

1. `ManifestGroup` 先构建 `DeleteFileIndex`,任务生成时按 entry 调 `ctx.deletes().forEntry(entry)`,不是边扫边全量比对。  
   - 源码: `core/src/main/java/org/apache/iceberg/ManifestGroup.java:185`, `core/src/main/java/org/apache/iceberg/ManifestGroup.java:366`
2. Position delete 优先走 file-scoped 路径索引,否则落到分区索引。  
   - 源码: `DeleteFileIndex#add(...PositionDeletes...)` 里先取 `ContentFileUtil.referencedDataFileLocation(file)` 决定进 `deletesByPath` 还是 `deletesByPartition`  
   - 路径: `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:520-535`
3. Equality delete 并非“总是全局”: unpartitioned 表进入 `globalDeletes`,partitioned 表进入 `eqDeletesByPartition`。  
   - 源码: `DeleteFileIndex#add(...EqualityDeletes...)`  
   - 路径: `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:541-549`
4. `referencedDataFile == null` 也不等于“必须读内容文件”: 会先看 `file_path` 的 lower/upper bound 是否相等来推断 file-scoped。  
   - 源码: `ContentFileUtil.referencedDataFile`  
   - 路径: `core/src/main/java/org/apache/iceberg/util/ContentFileUtil.java:63-86`

**答案（总）:**

你在面试里可以一句话收口: **Iceberg 用“类型拆分 + 多维索引 + 条件过滤”做 delete 关联,不是粗暴的全局 join。**

**追问与反问:**
- 高频追问: “那 equality delete 到底什么时候是全局的？”
- 你的反问: “我们线上表主要是 unpartitioned 还是 partitioned？这会直接决定 equality delete 的匹配放大倍数。”

#### 30 秒速记（简版）

- 结论先行：不是“所有 Delete 都全局广播”。当前源码是**分层索引匹配**: 先按 delete 类型拆分,再按 global / partition / pa。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/ManifestGroup.java:185`，再联动 `core/src/main/java/org/apache/iceberg/ManifestGroup.java:366`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/ManifestGroup.java:185`
- `core/src/main/java/org/apache/iceberg/ManifestGroup.java:366`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/ManifestGroup.java:182-188`**
```java
                  return ResidualEvaluator.of(spec, filter, caseSensitive);
                });

    DeleteFileIndex deleteFiles = deleteIndexBuilder.scanMetrics(scanMetrics).build();

    boolean dropStats = ManifestReader.dropStats(columns);
    if (deleteFiles.hasEqualityDeletes()) {
```

**片段 2：`core/src/main/java/org/apache/iceberg/ManifestGroup.java:363-369`**
```java
        entry -> {
          DataFile dataFile =
              ContentFileUtil.copy(entry.file(), ctx.shouldKeepStats(), ctx.columnsToKeepStats());
          DeleteFile[] deleteFiles = ctx.deletes().forEntry(entry);
          ScanMetricsUtil.fileTask(ctx.scanMetrics(), dataFile, deleteFiles);
          return new BaseFileScanTask(
              dataFile, deleteFiles, ctx.schemaAsString(), ctx.specAsString(), ctx.residuals());
```

---
<a id="q-6-3"></a>
### 6.3 Merge-on-Read 实现原理

**🟠 问题: 请详细说明 Spark 读取 Iceberg 时,DeleteFilter 如何实现 Merge-on-Read？**

**答案:**

Merge-on-Read 是 Iceberg 的核心特性,通过在读取时合并 DataFile 和 DeleteFile 来实现行级删除。

#### RowDataReader 读取流程

```java
// 源码: spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/RowDataReader.java
@Override
protected CloseableIterator<InternalRow> open(FileScanTask task) {
    String filePath = task.file().location();
    LOG.debug("Opening data file {}", filePath);

    // 1. 创建 DeleteFilter
    SparkDeleteFilter deleteFilter =
        new SparkDeleteFilter(filePath, task.deletes(), counter(), true);

    // 2. 计算需要读取的 Schema(可能包含额外的删除相关列)
    Schema requiredSchema = deleteFilter.requiredSchema();
    Map<Integer, ?> idToConstant = constantsMap(task, requiredSchema);

    // 3. 读取 DataFile 并应用 Delete 过滤
    return deleteFilter.filter(open(task, requiredSchema, idToConstant)).iterator();
}
```

#### DeleteFilter 过滤链

```java
// 源码: data/src/main/java/org/apache/iceberg/data/DeleteFilter.java
public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
}
```

**过滤顺序**:

```
原始数据流 (DataFile Records)
    │
    ├─ 1. applyPosDeletes()  ← 先应用 Position Delete
    │   └─ 使用 PositionDeleteIndex 快速判断行位置
    │
    └─ 2. applyEqDeletes()   ← 再应用 Equality Delete
        └─ 使用 StructLikeSet 进行集合匹配
```

**为什么先 Position 后 Equality？**
- Position Delete 通常更精确(O(1) 位置查找)
- Equality Delete 需要 Hash 匹配(O(1) 但常数更大)
- 先过滤 Position Delete 可以减少 Equality 比较次数

**深入理解**: 两阶段过滤的效率权衡,本质是用确定性换性能:Position Delete 精确到行号(确定性高),Equality Delete 基于值匹配(确定性低),先过滤高确定性的能更快缩小数据集。

#### 💡 面试加分话术

> "Merge-on-Read 的核心是**延迟物化删除**:
> 1. **写入时**: 只写 Delete File，不修改原数据文件，避免写放大
> 2. **读取时**: 通过 DeleteFilter 两阶段过滤合并数据
> 3. **过滤顺序**: Position → Equality，从高确定性到低确定性
>
> 这是 Iceberg 实现**高吞吐写入 + 正确性保证**的关键设计。
> 代价是读取时有额外开销，但通过 Compaction 可以定期物化删除，消除读放大。"

#### 🔥 高频追问

**Q: Merge-on-Read 的读放大如何量化？什么时候需要触发 Compaction？**

**A**: 读放大指标:
```
读放大系数 = (读取的数据量 + 读取的删除文件量) / 实际返回的数据量
```
触发 Compaction 的阈值:
1. **删除比例**: 当分区的删除行数 > 总行数的 10-20%
2. **Delete 文件数**: 当单个 DataFile 关联的 Delete 文件 > 3-5 个
3. **定期策略**: 每日/每周定时 Compaction

**监控方法**: 通过 `iceberg.tables.history` 表监控 Delete 文件增长趋势。

#### 30 秒速记（简版）

- 结论先行：Merge-on-Read 是 Iceberg 的核心特性,通过在读取时合并 DataFile 和 DeleteFile 来实现行级删除。
- 主链路：先定位 `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/RowDataReader.java:1`，再联动 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:1`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/RowDataReader.java:1`
- `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:1`




#### 源码关键片段

**片段 1：`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/RowDataReader.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

---
<a id="q-6-4"></a>
### 6.4 Position Delete 应用机制

**🟡 问题: Position Delete 如何实现 O(1) 的删除行判断？请结合源码说明。**

**答案:**

Position Delete 通过 `PositionDeleteIndex` 实现高效的行位置查找。

#### Position Delete 应用逻辑

```java
// 源码: data/src/main/java/org/apache/iceberg/data/DeleteFilter.java
private CloseableIterable<T> applyPosDeletes(CloseableIterable<T> records) {
    if (posDeletes.isEmpty()) {
        return records;  // 无 Position Delete,直接返回
    }

    // 1. 构建 Position Delete 索引
    PositionDeleteIndex positionIndex = deletedRowPositions();

    // 2. 创建 Predicate 判断每一行是否被删除
    Predicate<T> isDeleted = record -> positionIndex.isDeleted(pos(record));

    return createDeleteIterable(records, isDeleted);
}

public PositionDeleteIndex deletedRowPositions() {
    if (deleteRowPositions == null && !posDeletes.isEmpty()) {
        this.deleteRowPositions = deleteLoader().loadPositionDeletes(posDeletes, filePath);
    }
    return deleteRowPositions;
}
```

#### PositionDeleteIndex 内部结构

```java
// 伪代码: PositionDeleteIndex 的实现
class PositionDeleteIndex {
    // 使用 Roaring Bitmap 存储删除的位置(内存高效)
    private RoaringBitmap deletedPositions;

    boolean isDeleted(long position) {
        return deletedPositions.contains(position);
    }
}
```

#### _pos 元数据列

Iceberg 虚拟列 `_pos` 记录行在文件中的位置:

```java
// 源码: api/src/main/java/org/apache/iceberg/MetadataColumns.java
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

**生成时机**:
- Parquet/ORC 读取器在读取行时自动计算
- 从文件开头开始递增: 0, 1, 2, 3, ...

#### 执行示例

```
假设 DataFile 有 1000 行,Position Delete 标记了位置 [42, 156, 789]

读取流程:
Row 0:  positionIndex.isDeleted(0)   → false → 保留
Row 42: positionIndex.isDeleted(42)  → true  → 过滤 ✗
Row 156: positionIndex.isDeleted(156) → true  → 过滤 ✗
Row 789: positionIndex.isDeleted(789) → true  → 过滤 ✗
...
```

#### 性能优化点

1. **Roaring Bitmap**: 稀疏位图,内存占用极小
2. **O(1) 查找**: 直接位运算判断
3. **批量加载**: 一次性加载所有 Position Delete 构建索引

#### 30 秒速记（简版）

- 结论先行：Position Delete 通过 PositionDeleteIndex 实现高效的行位置查找。
- 主链路：先定位 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:1`，再联动 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:140`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:1`
- `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:140`




#### 源码关键片段

**片段 1：`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:137-143`**
```java
    return deleteFiles;
  }

  DeleteFile[] forEntry(ManifestEntry<DataFile> entry) {
    return forDataFile(entry.dataSequenceNumber(), entry.file());
  }

```

---
<a id="q-6-5"></a>
### 6.5 Equality Delete 应用机制

**🟡 问题: Equality Delete 如何通过 StructLikeSet 实现高效的等值匹配？**

**答案:**

Equality Delete 通过将删除数据加载到内存 HashSet,实现 O(1) 的等值查找。

#### Equality Delete 应用逻辑

```java
// 源码: data/src/main/java/org/apache/iceberg/data/DeleteFilter.java
private List<Predicate<T>> applyEqDeletes() {
    if (isInDeleteSets != null) {
        return isInDeleteSets;  // 缓存结果
    }

    isInDeleteSets = Lists.newArrayList();
    if (eqDeletes.isEmpty()) {
        return isInDeleteSets;
    }

    // 1. 按 equalityFieldIds 分组 DeleteFile
    Multimap<Set<Integer>, DeleteFile> filesByDeleteIds =
        Multimaps.newMultimap(Maps.newHashMap(), Lists::newArrayList);
    for (DeleteFile delete : eqDeletes) {
        filesByDeleteIds.put(Sets.newHashSet(delete.equalityFieldIds()), delete);
    }

    // 2. 为每组 equalityFieldIds 构建 StructLikeSet
    for (Map.Entry<Set<Integer>, Collection<DeleteFile>> entry :
            filesByDeleteIds.asMap().entrySet()) {
        Set<Integer> ids = entry.getKey();
        Iterable<DeleteFile> deletes = entry.getValue();

        Schema deleteSchema = TypeUtil.select(requiredSchema, ids);
        StructProjection projectRow = StructProjection.create(requiredSchema, deleteSchema);

        // 加载所有 EqualityDelete 构建 HashSet
        StructLikeSet deleteSet =
            deleteLoader().loadEqualityDeletes(deletes, deleteSchema);

        // 3. 创建 Predicate: 判断行是否在 deleteSet 中
        Predicate<T> isInDeleteSet = record ->
            deleteSet.contains(projectRow.wrap(asStructLike(record)));

        isInDeleteSets.add(isInDeleteSet);
    }

    return isInDeleteSets;
}
```

#### 分组优化

假设有两个 Equality DeleteFile:

```
DeleteFile1: equalityFieldIds = [1, 5]  // id, email
DeleteFile2: equalityFieldIds = [1, 5]  // 同样的字段
DeleteFile3: equalityFieldIds = [3]     // user_name

分组后:
Group1: [DeleteFile1, DeleteFile2] → 合并成一个 StructLikeSet
Group2: [DeleteFile3]              → 独立的 StructLikeSet
```

#### 执行示例

```
EqualityDelete 数据(id, email):
| id   | email              |
|------|--------------------| 
| 1001 | alice@example.com |
| 1002 | bob@example.com   |

读取 DataFile 时:
Row: {id: 1001, email: alice@example.com, name: "Alice"}
  → projectRow 提取 (1001, alice@example.com)
  → deleteSet.contains((1001, alice@example.com)) → true
  → 过滤 ✗

Row: {id: 1003, email: carol@example.com, name: "Carol"}
  → projectRow 提取 (1003, carol@example.com)
  → deleteSet.contains((1003, carol@example.com)) → false
  → 保留 ✓
```

#### 性能考量

1. **StructLikeSet 使用 HashSet**: O(1) 平均查找时间
2. **内存开销**: 所有 Equality Delete 需要加载到内存
3. **分组优化**: 相同 equalityFieldIds 共享一个 Set

#### 30 秒速记（简版）

- 结论先行：Equality Delete 通过将删除数据加载到内存 HashSet,实现 O(1) 的等值查找。
- 主链路：先定位 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:1`，再联动 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:140`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:1`
- `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:140`




#### 源码关键片段

**片段 1：`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:137-143`**
```java
    return deleteFiles;
  }

  DeleteFile[] forEntry(ManifestEntry<DataFile> entry) {
    return forDataFile(entry.dataSequenceNumber(), entry.file());
  }

```

---
<a id="q-6-6"></a>
### 6.6 Schema 投影与 Delete 协同

**🟠 问题: 为什么应用 Equality Delete 时需要读取额外的列？请从源码角度说明 requiredSchema 的计算逻辑。**

**答案:**

Equality Delete 依赖特定列进行等值匹配,即使用户查询不包含这些列,也必须从 DataFile 读取。

#### requiredSchema 计算

```java
// 源码: data/src/main/java/org/apache/iceberg/data/DeleteFilter.java
private static Schema fileProjection(
    Schema tableSchema,
    Schema requestedSchema,  // 用户查询的列
    List<DeleteFile> posDeletes,
    List<DeleteFile> eqDeletes,
    boolean needRowPosCol) {

    Set<Integer> requiredIds = Sets.newLinkedHashSet();

    // 1. Position Delete 需要 _pos 列
    if (needRowPosCol && !posDeletes.isEmpty()) {
        requiredIds.add(MetadataColumns.ROW_POSITION.fieldId());
    }

    // 2. Equality Delete 需要其 equalityFieldIds 列
    for (DeleteFile eqDelete : eqDeletes) {
        requiredIds.addAll(eqDelete.equalityFieldIds());
    }

    // 3. 计算缺失的列(不在 requestedSchema 中但需要的)
    Set<Integer> missingIds = Sets.difference(
        requiredIds,
        TypeUtil.getProjectedIds(requestedSchema)
    );

    if (missingIds.isEmpty()) {
        return requestedSchema;  // 无需额外列
    }

    // 4. 添加缺失的列到 Schema
    List<Types.NestedField> columns =
        Lists.newArrayList(requestedSchema.columns());
    for (int fieldId : missingIds) {
        if (fieldId == MetadataColumns.ROW_POSITION.fieldId()) {
            continue;  // _pos 在最后添加
        }
        Types.NestedField field = tableSchema.asStruct().field(fieldId);
        columns.add(field);
    }

    // 5. 添加 _pos 元数据列(如果需要)
    if (missingIds.contains(MetadataColumns.ROW_POSITION.fieldId())) {
        columns.add(MetadataColumns.ROW_POSITION);
    }

    return new Schema(columns);
}
```

#### 示例场景

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
- 用户只查询 `name, age`
- 但 Equality Delete 依赖 `id, email` 进行匹配
- 必须从 DataFile 读取这些列才能判断行是否被删除

**性能影响**:
- 增加读取的列数 → 更多 IO
- 这是 Merge-on-Read 的代价

**深入理解**: Equality Delete 强制读取额外列是 Merge-on-Read 的本质代价。这也解释了为什么频繁 Update 场景下,定期 Compaction 很重要——将 Delete 合并回 DataFile 可以消除这种开销。

#### 30 秒速记（简版）

- 结论先行：Equality Delete 依赖特定列进行等值匹配,即使用户查询不包含这些列,也必须从 DataFile 读取。
- 主链路：先定位 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:1`，再联动 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:140`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:1`
- `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:140`




#### 源码关键片段

**片段 1：`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:137-143`**
```java
    return deleteFiles;
  }

  DeleteFile[] forEntry(ManifestEntry<DataFile> entry) {
    return forDataFile(entry.dataSequenceNumber(), entry.file());
  }

```

---
<a id="q-6-7"></a>
### 6.7 Sequence Number 并发控制

**🔴 问题: `dataSequenceNumber` 和 `fileSequenceNumber` 有什么区别？Delete 的生效规则到底是不是统一 `<=`？**

**答案（总）:**

两类 sequence 要分开看: `fileSequenceNumber` 更偏“文件何时被加入表”,`dataSequenceNumber` 更偏“这批数据逻辑上属于哪个序列”。Delete 生效规则也**不是一条统一公式**,position 与 equality 在源码中是两套语义。

**答案（分）:**

1. Position delete 以 `DeleteFile::dataSequenceNumber` 排序与筛选。  
   - `PositionDeletes` 的 comparator 直接按 `DeleteFile::dataSequenceNumber` 排序。  
   - `filter(seq)` 通过 `findStartIndex` 返回 `deleteSeq >= dataSeq` 的候选。  
   - 源码: `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:650-668`
2. Equality delete 使用的是 `applySequenceNumber` 而不是 delete 原始 dataSeq。  
   - `EqualityDeleteFile` 构造时: `applySequenceNumber = wrapped.dataSequenceNumber() - 1`。  
   - 源码: `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:811-815`
3. 这意味着 equality delete 的判定是“严格小于”:  
   - 因为过滤比较的是 `applySequenceNumber >= dataSeq`,等价为 `deleteDataSeq > dataSeq`。  
   - 源码: `EqualityDeletes#filter` + `findStartIndex`  
   - 路径: `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:741-745`
4. `fileSequenceNumber` 仍有价值,但不用于这里的 delete 行匹配主判定。它更多体现文件提交生命周期与快照演进语义。

**答案（总）:**

面试收口句: **Position delete 是“同序列可见(>=)”,equality delete 是“严格前序可见(>)”,差别来自 `dataSeq - 1` 的设计。**

**追问与反问:**
- 高频追问: “为什么 equality 要故意减 1？”
- 你的反问: “我们是否验证过跨引擎（Spark/Flink）在同事务写删场景下的序列一致性？”

#### 30 秒速记（简版）

- 结论先行：两类 sequence 要分开看: fileSequenceNumber 更偏“文件何时被加入表”,dataSequenceNumber 更偏“这批数据逻辑。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:650`，再联动 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:811`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:650`
- `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:811`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:647-653`**
```java

  // a group of position delete files sorted by the sequence number they apply to
  static class PositionDeletes {
    private static final Comparator<DeleteFile> SEQ_COMPARATOR =
        Comparator.comparingLong(DeleteFile::dataSequenceNumber);

    // indexed state
```

**片段 2：`core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:808-814`**
```java
    private volatile Map<Integer, Object> convertedLowerBounds = null;
    private volatile Map<Integer, Object> convertedUpperBounds = null;

    EqualityDeleteFile(PartitionSpec spec, DeleteFile file) {
      this.spec = spec;
      this.wrapped = file;
      this.applySequenceNumber = wrapped.dataSequenceNumber() - 1;
```

---
<a id="q-6-8"></a>
### 6.8 Flink Incremental Scan 实现

**🟠 问题: Flink 增量读取 Iceberg 的关键路径是什么？`IncrementalAppendScan` 接口在源码里到底长什么样？**

**答案（总）:**

`IncrementalAppendScan` 本体非常薄,只是继承 `IncrementalScan`。真正的行为定义在父接口和实现类里: 起止快照边界、只消费 APPEND 快照、以及流式启动策略（旧 API/FLIP-27）的差异。

**答案（分）:**

1. `IncrementalAppendScan` 接口没有显式声明 `from/to` 方法,只做 extends。  
   - 源码: `api/src/main/java/org/apache/iceberg/IncrementalAppendScan.java:22`
2. `fromSnapshotExclusive`、`toSnapshot` 等方法定义在父接口 `IncrementalScan`。  
   - 源码: `api/src/main/java/org/apache/iceberg/IncrementalScan.java:61`, `api/src/main/java/org/apache/iceberg/IncrementalScan.java:87`
3. append-only 语义在实现层体现: `BaseIncrementalAppendScan#appendsBetween` 只保留 `DataOperations.APPEND`。  
   - 源码: `core/src/main/java/org/apache/iceberg/BaseIncrementalAppendScan.java:104-111`
4. Flink 旧 SourceFunction 首次运行可走 `copyWithSnapshotId`；FLIP-27 在 `TABLE_SCAN_THEN_INCREMENTAL` 策略下会先做一次快照扫描。  
   - 源码: `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:204-210`  
   - 源码: `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/enumerator/ContinuousSplitPlannerImpl.java:170-176`
5. Flink 文件读取阶段仍会构造 `DeleteFilter`,增量并不意味着“忽略 delete 过滤”。  
   - 源码: `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/RowDataFileScanTaskReader.java:87-91`, `...:216`

**答案（总）:**

可复述总结: **接口很薄,语义在父接口与实现；增量是快照边界语义,不是“天然不处理 delete”。**

**追问与反问:**
- 高频追问: “为什么我设置了增量,首次还是读了很多历史文件？”
- 你的反问: “当前启动策略是 `TABLE_SCAN_THEN_INCREMENTAL` 还是 latest-exclusive？两者首轮成本完全不同。”

#### 30 秒速记（简版）

- 结论先行：IncrementalAppendScan 本体非常薄,只是继承 IncrementalScan。真正的行为定义在父接口和实现类里: 起止快照边界、只消费 。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/IncrementalAppendScan.java:22`，再联动 `api/src/main/java/org/apache/iceberg/IncrementalScan.java:61`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `api/src/main/java/org/apache/iceberg/IncrementalAppendScan.java:22`
- `api/src/main/java/org/apache/iceberg/IncrementalScan.java:61`




#### 源码关键片段

**片段 1：`api/src/main/java/org/apache/iceberg/IncrementalAppendScan.java:19-23`**
```java
package org.apache.iceberg;

/** API for configuring an incremental table scan for appends only snapshots */
public interface IncrementalAppendScan
    extends IncrementalScan<IncrementalAppendScan, FileScanTask, CombinedScanTask> {}
```

**片段 2：`api/src/main/java/org/apache/iceberg/IncrementalScan.java:58-64`**
```java
   * @return this for method chaining
   * @throws IllegalArgumentException if the start snapshot is not an ancestor of the end snapshot
   */
  ThisT fromSnapshotExclusive(long fromSnapshotId);

  /**
   * Instructs this scan to look for changes starting from a particular snapshot (exclusive).
```

---
<a id="q-6-9"></a>
### 6.9 Spark DeleteFilter 优化

**🟡 问题: Spark 侧对 DeleteFilter 有哪些真实可用的优化？哪些常见说法在源码层面不准确？**

**答案（总）:**

最容易答错的两点:  
1) delete 缓存开关不是 DataFrame read option；  
2) 缓存策略也不能笼统说“纯 LRU”。  
源码里是 session conf + Caffeine（访问过期 + 权重上限）。

**答案（分）:**

1. 读取开关入口在 `SparkReadConf#cacheDeleteFilesOnExecutors`。  
   - 源码: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkReadConf.java:335-344`
2. 配置 key 是 Spark Session Conf:  
   - `spark.sql.iceberg.executor-cache.enabled`  
   - `spark.sql.iceberg.executor-cache.delete-files.enabled`  
   - 源码: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkSQLProperties.java:79-96`
3. Executor 缓存实现基于 Caffeine: `expireAfterAccess(timeout)` + `maximumWeight(maxTotalSize)` + `weigher`。  
   - 源码: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkExecutorCache.java:163-167`
4. Spark Runtime Filter 可以在 scan 侧减少任务: `SparkBatchQueryScan#filter(Predicate[])` 命中后 `resetTasks(filteredTasks)`。  
   - 源码: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:127-159`

**答案（总）:**

面试一锤定音: **Spark delete 优化的主抓手是“会话级缓存策略 + 运行时任务裁剪”,不是 DataFrame 单次 read option。**

**追问与反问:**
- 高频追问: “为什么开了缓存命中率依然低？”
- 你的反问: “delete 文件大小分布是否超过 `max-entry-size`？如果大量超限,命中率天然上不去。”

#### 30 秒速记（简版）

- 结论先行：1) delete 缓存开关不是 DataFrame read option；。
- 主链路：先定位 `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkReadConf.java:335`，再联动 `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkSQLProperties.java:79`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkReadConf.java:335`
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkSQLProperties.java:79`




#### 源码关键片段

**片段 1：`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkReadConf.java:332-338`**
```java
    return executorCacheEnabled() && executorCacheLocalityEnabledInternal();
  }

  public boolean cacheDeleteFilesOnExecutors() {
    return executorCacheEnabled() && cacheDeleteFilesOnExecutorsInternal();
  }

```

**片段 2：`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkSQLProperties.java:76-82`**
```java
  // Controls whether to report locality information to Spark while allocating input partitions
  public static final String LOCALITY = "spark.sql.iceberg.locality.enabled";

  public static final String EXECUTOR_CACHE_ENABLED = "spark.sql.iceberg.executor-cache.enabled";
  public static final boolean EXECUTOR_CACHE_ENABLED_DEFAULT = true;

  public static final String EXECUTOR_CACHE_TIMEOUT = "spark.sql.iceberg.executor-cache.timeout";
```

---

## 七、Catalog 与元数据管理架构
<a id="q-7-1"></a>
### 7.1 Catalog 抽象架构与 SPI 机制

**🟠 问题: 请解释 Iceberg Catalog 的 SPI 设计模式，以及如何通过 CatalogUtil 实现 Catalog 的可插拔加载？**

**答案:**

Iceberg 通过 **SPI (Service Provider Interface)** 设计模式实现了 Catalog 的可插拔架构，使得 HiveCatalog、RESTCatalog、JdbcCatalog 等可以无缝替换。

#### Catalog 接口定义

```java
// 源码: api/src/main/java/org/apache/iceberg/catalog/Catalog.java
public interface Catalog {

    /**
     * 返回 Catalog 名称
     */
    default String name() {
        return toString();
    }

    /**
     * 列出命名空间下的所有表
     */
    List<TableIdentifier> listTables(Namespace namespace);

    /**
     * 创建表(委托给 TableBuilder)
     */
    default Table createTable(
            TableIdentifier identifier,
            Schema schema,
            PartitionSpec spec,
            String location,
            Map<String, String> properties) {
        return buildTable(identifier, schema)
            .withPartitionSpec(spec)
            .withLocation(location)
            .withProperties(properties)
            .create();
    }

    /**
     * 删除表
     */
    boolean dropTable(TableIdentifier identifier, boolean purge);

    /**
     * 重命名表
     */
    void renameTable(TableIdentifier from, TableIdentifier to);

    /**
     * 加载表(核心方法)
     */
    Table loadTable(TableIdentifier identifier);

    /**
     * 构建表(Builder 模式)
     */
    default TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
        throw new UnsupportedOperationException(
            this.getClass().getName() + " does not implement buildTable");
    }

    /**
     * SPI 初始化方法
     */
    default void initialize(String name, Map<String, String> properties) {}
}
```

#### SPI 加载机制

Iceberg 通过 `CatalogUtil` 实现 Catalog 的动态加载:

```java
// 源码: core/src/main/java/org/apache/iceberg/CatalogUtil.java
public class CatalogUtil {
    public static final String ICEBERG_CATALOG_TYPE = "type";

    // 支持的 Catalog 类型
    public static final String ICEBERG_CATALOG_TYPE_HIVE = "hive";
    public static final String ICEBERG_CATALOG_TYPE_HADOOP = "hadoop";
    public static final String ICEBERG_CATALOG_TYPE_REST = "rest";
    public static final String ICEBERG_CATALOG_TYPE_GLUE = "glue";
    public static final String ICEBERG_CATALOG_TYPE_NESSIE = "nessie";
    public static final String ICEBERG_CATALOG_TYPE_JDBC = "jdbc";

    /**
     * 根据配置动态创建 Catalog 实例
     */
    public static Catalog buildIcebergCatalog(
            String name,
            Map<String, String> options,
            Object hadoopConf) {

        String catalogImpl = options.get(CatalogProperties.CATALOG_IMPL);

        if (catalogImpl != null) {
            // 方式1: 显式指定 Catalog 实现类
            return CatalogUtil.loadCatalog(catalogImpl, name, options, hadoopConf);
        }

        // 方式2: 根据 type 属性选择内置实现
        String catalogType = options.getOrDefault(ICEBERG_CATALOG_TYPE, "hive");
        switch (catalogType.toLowerCase()) {
            case ICEBERG_CATALOG_TYPE_HIVE:
                catalogImpl = "org.apache.iceberg.hive.HiveCatalog";
                break;
            case ICEBERG_CATALOG_TYPE_HADOOP:
                catalogImpl = "org.apache.iceberg.hadoop.HadoopCatalog";
                break;
            case ICEBERG_CATALOG_TYPE_REST:
                catalogImpl = "org.apache.iceberg.rest.RESTCatalog";
                break;
            // ... 其他类型
        }

        return loadCatalog(catalogImpl, name, options, hadoopConf);
    }

    /**
     * 反射加载 Catalog 实例
     */
    public static Catalog loadCatalog(
            String impl,
            String catalogName,
            Map<String, String> properties,
            Object hadoopConf) {

        // 1. 反射创建实例(必须有无参构造器)
        Catalog catalog = (Catalog) DynConstructors.builder()
            .impl(impl)
            .buildChecked()
            .newInstance();

        // 2. 如果支持 Hadoop 配置,注入配置
        if (catalog instanceof Configurable) {
            ((Configurable) catalog).setConf(hadoopConf);
        }

        // 3. 调用 initialize 完成初始化
        catalog.initialize(catalogName, properties);

        return catalog;
    }
}
```

#### SPI 设计模式分析

```
┌─────────────────────────────────────────────────────────────┐
│                      Catalog SPI 架构                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────┐    loadCatalog()    ┌──────────────────┐   │
│  │ CatalogUtil │ ─────────────────▶ │  Catalog 接口     │   │
│  │ (工厂类)     │                    │  (SPI 规范)       │   │
│  └─────────────┘                    └────────┬─────────┘   │
│                                               │              │
│                    ┌──────────────────────────┼─────────────┤
│                    │                          │             │
│            ┌───────▼──────┐          ┌───────▼──────┐      │
│            │ HiveCatalog   │          │ RESTCatalog  │      │
│            │ (HMS 实现)    │          │ (HTTP 实现)   │      │
│            └───────────────┘          └──────────────┘      │
│                    │                          │             │
│            ┌───────▼──────┐          ┌───────▼──────┐      │
│            │ HadoopCatalog │          │ GlueCatalog  │      │
│            │ (HDFS 实现)   │          │ (AWS 实现)   │      │
│            └───────────────┘          └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

**设计模式应用**:

| 模式 | 应用点 | 优势 |
|------|--------|------|
| **工厂模式** | `CatalogUtil.buildIcebergCatalog()` | 封装创建逻辑,统一入口 |
| **策略模式** | 根据 `type` 选择实现 | 运行时可切换策略 |
| **模板方法** | `initialize()` 钩子 | 子类可定制初始化逻辑 |
| **Builder 模式** | `buildTable()` | 链式调用,流畅 API |

#### Spark/Flink 集成示例

```scala
// Spark SQL 配置
spark.sql.catalog.my_catalog = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.my_catalog.type = hive
spark.sql.catalog.my_catalog.uri = thrift://hive-metastore:9083

// 或使用 REST Catalog
spark.sql.catalog.rest_catalog = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.rest_catalog.type = rest
spark.sql.catalog.rest_catalog.uri = https://iceberg-rest:8181
```

```java
// Flink Table API 配置
tableEnv.executeSql(
    "CREATE CATALOG iceberg_catalog WITH (" +
    "  'type' = 'iceberg'," +
    "  'catalog-type' = 'hive'," +
    "  'uri' = 'thrift://hive-metastore:9083'" +
    ")");
```

**深入理解**: SPI 模式是 Iceberg 可扩展性的基石。通过定义清晰的 Catalog 接口契约,任何满足接口的实现都可以无缝集成。这也是 Iceberg 能够快速支持 AWS Glue、Nessie、JDBC 等多种后端的关键原因。

#### 面试加分话术

> "Iceberg Catalog 采用 SPI 设计,将表操作抽象为统一接口。核心亮点是 `initialize()` 方法作为延迟初始化钩子,使得 Spark/Flink 可以先创建 Catalog 实例,再通过配置完成初始化。这种设计完美适配了计算引擎的配置加载流程。"

#### 30 秒速记（简版）

- 结论先行：Iceberg 通过 **SPI (Service Provider Interface)** 设计模式实现了 Catalog 的可插拔架构，使得 Hive。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/catalog/Catalog.java:1`，再联动 `core/src/main/java/org/apache/iceberg/CatalogUtil.java:1`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `api/src/main/java/org/apache/iceberg/catalog/Catalog.java:1`
- `core/src/main/java/org/apache/iceberg/CatalogUtil.java:1`




#### 源码关键片段

**片段 1：`api/src/main/java/org/apache/iceberg/catalog/Catalog.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`core/src/main/java/org/apache/iceberg/CatalogUtil.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

---
<a id="q-7-2"></a>
### 7.2 TableOperations 的 commit 原子性保证

**🔴 问题: TableOperations.commit() 是 Iceberg 事务的核心方法。请详细说明其原子性保证机制,以及 CommitStateUnknownException 的处理策略。**

**答案:**

`TableOperations` 是 Iceberg 元数据操作的 **SPI 核心接口**,其 `commit()` 方法实现了乐观并发控制(OCC)的原子提交。

#### TableOperations 接口定义

```java
// 源码: core/src/main/java/org/apache/iceberg/TableOperations.java
public interface TableOperations {

    /**
     * 返回当前表元数据(不检查更新)
     */
    TableMetadata current();

    /**
     * 刷新并返回最新表元数据
     */
    TableMetadata refresh();

    /**
     * 原子性替换表元数据
     *
     * 关键约束:
     * 1. 必须检查 base 是否为当前版本,避免覆盖其他更新
     * 2. 原子操作成功后,不允许执行任何可能失败的操作
     * 3. 无法确定提交状态时,必须抛出 CommitStateUnknownException
     *
     * @param base 基于此元数据进行修改
     * @param metadata 新的表元数据
     */
    void commit(TableMetadata base, TableMetadata metadata);

    /**
     * 返回 FileIO 用于读写数据/元数据文件
     */
    FileIO io();

    /**
     * 是否要求严格清理(仅在 CleanableFailure 时清理)
     */
    default boolean requireStrictCleanup() {
        return true;
    }
}
```

#### HiveTableOperations commit 实现

HiveCatalog 的 commit 实现展示了完整的原子性保证机制:

```java
// 源码: hive-metastore/src/main/java/org/apache/iceberg/hive/HiveTableOperations.java
@Override
protected void doCommit(TableMetadata base, TableMetadata metadata) {
    boolean newTable = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(newTable, metadata);

    CommitStatus commitStatus = CommitStatus.FAILURE;
    boolean updateHiveTable = false;

    // 1. 获取分布式锁(可选)
    HiveLock lock = lockObject(base != null ? base : metadata);
    try {
        lock.lock();  // 悲观锁保护

        Table tbl = loadHmsTable();

        // 2. 检查并发冲突
        String metadataLocation = tbl.getParameters().get(METADATA_LOCATION_PROP);
        String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;

        if (!Objects.equals(baseMetadataLocation, metadataLocation)) {
            // 检测到并发修改!
            throw new CommitFailedException(
                "Cannot commit: Base metadata location '%s' is not same as " +
                "the current table metadata location '%s' for %s.%s",
                baseMetadataLocation, metadataLocation, database, tableName);
        }

        // 3. 更新 HMS 表属性
        HMSTablePropertyHelper.updateHmsTableForIcebergTable(
            newMetadataLocation, tbl, metadata, ...);

        // 4. 确保锁仍然有效
        lock.ensureActive();

        try {
            // 5. 持久化到 HMS
            persistTable(tbl, updateHiveTable, baseMetadataLocation);
            lock.ensureActive();

            commitStatus = CommitStatus.SUCCESS;

        } catch (LockException le) {
            // 锁心跳失败,状态未知!
            commitStatus = CommitStatus.UNKNOWN;
            throw new CommitStateUnknownException(
                "Failed to heartbeat for hive lock while committing changes. " +
                "This can lead to a concurrent commit attempt be able to " +
                "overwrite this commit. Please check the commit history.", le);

        } catch (Throwable e) {
            // 其他异常,尝试检测实际状态
            if (e.getMessage().contains("The table has been modified")) {
                // HMS 返回并发修改错误,但可能是网络重试导致
                commitStatus = checkCommitStatusStrict(newMetadataLocation, metadata);
                if (commitStatus == CommitStatus.FAILURE) {
                    throw new CommitFailedException(e,
                        "The table %s.%s has been modified concurrently",
                        database, tableName);
                }
            } else {
                // 未知错误,尝试重连检测
                commitStatus = checkCommitStatus(newMetadataLocation, metadata);
            }

            switch (commitStatus) {
                case SUCCESS:
                    break;  // 实际上成功了
                case FAILURE:
                    throw e;
                case UNKNOWN:
                    throw new CommitStateUnknownException(e);
            }
        }
    } finally {
        // 6. 清理与解锁
        HiveOperationsBase.cleanupMetadataAndUnlock(io(), commitStatus, newMetadataLocation, lock);
    }
}
```

#### CommitStateUnknownException 设计哲学

```java
// 源码: api/src/main/java/org/apache/iceberg/exceptions/CommitStateUnknownException.java
public class CommitStateUnknownException extends RuntimeException {

    private static final String COMMON_INFO =
        "Cannot determine whether the commit was successful or not, " +
        "the underlying data files may or may not be needed. " +
        "Manual intervention via the Remove Orphan Files Action can remove " +
        "these files when a connection to the Catalog can be re-established " +
        "if the commit was actually unsuccessful.\n" +
        "Please check to see whether or not your commit was successful " +
        "before retrying this commit. Retrying an already successful " +
        "operation will result in duplicate records or unintentional modifications.\n" +
        "At this time no files will be deleted including possibly unused manifest lists.";

    public CommitStateUnknownException(Throwable cause) {
        super(cause.getMessage() + "\n" + COMMON_INFO, cause);
    }
}
```

**为什么需要 CommitStateUnknownException？**

```
场景: 网络分区
┌─────────┐     commit()     ┌─────────┐     alterTable()    ┌─────────┐
│ Client  │ ────────────────▶│  Proxy  │ ──────────────────▶│  HMS    │
│         │                  │         │                     │         │
│         │ ← ─ ─ 网络超时 ─ ─ │         │ ← ─ SUCCESS ─ ─ ─ ─ │         │
└─────────┘                  └─────────┘                     └─────────┘

Client 无法判断:
- 提交成功: 表已更新,数据文件有效
- 提交失败: 表未更新,数据文件需清理

错误处理:
- 如果假设失败并清理 → 可能删除已提交的数据(数据丢失!)
- 如果假设成功并重试 → 可能产生重复数据
- 正确做法: 抛出 CommitStateUnknownException,由上层决定
```

#### 提交状态检测机制

```java
// HiveTableOperations 的状态检测
private CommitStatus checkCommitStatus(String newMetadataLocation, TableMetadata metadata) {
    try {
        // 尝试刷新元数据
        refresh();

        // 检查当前元数据位置是否为我们提交的
        if (Objects.equals(currentMetadataLocation(), newMetadataLocation)) {
            return CommitStatus.SUCCESS;  // 提交成功!
        }

        // 元数据位置不同,提交失败
        return CommitStatus.FAILURE;

    } catch (Exception e) {
        // 无法确定状态
        return CommitStatus.UNKNOWN;
    }
}
```

#### 设计模式分析: 状态机模式

```
            ┌─────────────────────────────────────────────┐
            │            Commit 状态机                     │
            ├─────────────────────────────────────────────┤
            │                                             │
            │  ┌───────────┐                              │
            │  │  PENDING  │ ←─── 初始状态                │
            │  └─────┬─────┘                              │
            │        │ commit()                           │
            │        ▼                                    │
            │  ┌───────────┐    成功    ┌───────────┐    │
            │  │ COMMITTING│ ─────────▶│  SUCCESS  │    │
            │  └─────┬─────┘           └───────────┘    │
            │        │                                   │
            │        │ 异常                              │
            │        ▼                                   │
            │  ┌───────────┐   可检测   ┌───────────┐    │
            │  │ CHECKING  │ ─────────▶│  FAILURE  │    │
            │  └─────┬─────┘           └───────────┘    │
            │        │                                   │
            │        │ 无法确定                          │
            │        ▼                                   │
            │  ┌───────────┐                              │
            │  │  UNKNOWN  │ ─── 抛出 CommitStateUnknown │
            │  └───────────┘                              │
            └─────────────────────────────────────────────┘
```

#### 不同 Catalog 的原子性保证对比

| Catalog | 原子性机制 | 锁机制 | 状态检测 |
|---------|-----------|--------|---------|
| **HiveCatalog** | HMS alterTable | HIVE_LOCKS 表锁 | metadata 位置比对 |
| **HadoopCatalog** | 文件系统 rename | 无锁(依赖 FS 原子 rename) | 文件存在性检查 |
| **RESTCatalog** | 服务端事务 | 服务端处理 | 快照 ID 验证 |
| **GlueCatalog** | AWS API 乐观锁 | 无锁 | version 字段比对 |
| **JdbcCatalog** | 数据库事务 | 行级锁 | commit 后查询 |

**深入理解**: `CommitStateUnknownException` 体现了 Iceberg 对数据一致性的极致追求。宁可让操作处于"未知"状态需要人工介入,也不冒险做出可能导致数据丢失或重复的决定。这在分布式系统中是非常负责任的设计。

#### 面试加分话术

> "TableOperations.commit() 采用 OCC 乐观并发控制,核心是 base metadata 比对。关键设计亮点是 CommitStateUnknownException — 当网络分区等场景无法确定提交状态时,不做假设,而是显式抛出异常让上层处理。这避免了'假设失败清理数据'导致的数据丢失,也避免了'假设成功重试'导致的数据重复。"

#### 高频追问

**Q: 如果收到 CommitStateUnknownException,应该如何处理？**

**A**:
1. **不要立即重试**: 重试可能导致数据重复
2. **检查表状态**: 手动查询表的 snapshot 历史,确认是否提交成功
3. **孤儿文件清理**: 如果确认失败,使用 `removeOrphanFiles` Action 清理
4. **告警通知**: 在生产环境应触发告警,由 DBA 介入

#### 30 秒速记（简版）

- 结论先行：TableOperations 是 Iceberg 元数据操作的 **SPI 核心接口**,其 commit() 方法实现了乐观并发控制(OCC)的原子提交。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/TableOperations.java:1`，再联动 `hive-metastore/src/main/java/org/apache/iceberg/hive/HiveTableOperations.java:1`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/TableOperations.java:1`
- `hive-metastore/src/main/java/org/apache/iceberg/hive/HiveTableOperations.java:1`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/TableOperations.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`hive-metastore/src/main/java/org/apache/iceberg/hive/HiveTableOperations.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

---
<a id="q-7-3"></a>
### 7.3 REST Catalog 与传统 Catalog 的对比

**🟡 问题: REST Catalog 相比 HiveCatalog 有哪些架构优势？请从 commit 实现角度分析差异。**

**答案:**

REST Catalog 是 Iceberg 社区推荐的**下一代 Catalog 架构**,通过 HTTP API 实现了更好的解耦、扩展性和安全性。

#### 架构对比

```
┌────────────────────────────────────────────────────────────────────────┐
│                      传统 Catalog 架构 (HiveCatalog)                    │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────┐   Thrift    ┌─────────┐   JDBC    ┌─────────────┐         │
│  │ Spark   │ ──────────▶│ HMS     │ ────────▶│  MySQL       │         │
│  └─────────┘             └─────────┘           └─────────────┘         │
│                               ▲                                         │
│  ┌─────────┐   Thrift        │                                         │
│  │ Flink   │ ────────────────┘                                         │
│  └─────────┘                                                            │
│                                                                         │
│  问题:                                                                  │
│  1. 需要每个客户端配置 HMS 连接                                         │
│  2. 元数据 JSON 由客户端写入存储(S3/HDFS)                               │
│  3. 锁管理分散在各客户端                                                │
│  4. 难以实现统一的权限控制                                              │
└────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│                        REST Catalog 架构                                │
├────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────┐   HTTP/REST  ┌─────────────┐      ┌─────────────┐         │
│  │ Spark   │ ───────────▶│ REST Server │ ───▶│  Backend    │         │
│  └─────────┘              │  (集中式)    │      │  (任意存储)  │         │
│                           └─────────────┘      └─────────────┘         │
│  ┌─────────┐   HTTP/REST       ▲                                       │
│  │ Flink   │ ──────────────────┘                                       │
│  └─────────┘                                                            │
│                                                                         │
│  优势:                                                                  │
│  1. 客户端仅需配置 REST endpoint                                        │
│  2. 元数据由服务端管理,客户端无需直接访问存储                            │
│  3. 服务端统一处理锁和并发                                              │
│  4. 可实现细粒度权限控制(行级/列级)                                     │
└────────────────────────────────────────────────────────────────────────┘
```

#### RESTTableOperations commit 实现

```java
// 源码: core/src/main/java/org/apache/iceberg/rest/RESTTableOperations.java
@Override
public void commit(TableMetadata base, TableMetadata metadata) {
    Endpoint.check(endpoints, Endpoint.V1_UPDATE_TABLE);

    List<UpdateRequirement> requirements;
    List<MetadataUpdate> updates;

    switch (updateType) {
        case CREATE:
            // 创建表: 合并 createChanges 和当前变更
            updates = ImmutableList.<MetadataUpdate>builder()
                .addAll(createChanges)
                .addAll(metadata.changes())
                .build();
            requirements = UpdateRequirements.forCreateTable(updates);
            break;

        case REPLACE:
            // 替换表: 使用原始 base 计算 requirements
            updates = ImmutableList.<MetadataUpdate>builder()
                .addAll(createChanges)
                .addAll(metadata.changes())
                .build();
            requirements = UpdateRequirements.forReplaceTable(replaceBase, updates);
            break;

        case SIMPLE:
            // 普通更新: 直接使用变更列表
            updates = metadata.changes();
            requirements = UpdateRequirements.forUpdateTable(base, updates);
            break;
    }

    // 构建请求
    UpdateTableRequest request = new UpdateTableRequest(requirements, updates);

    try {
        // 发送 POST 请求到服务端
        LoadTableResponse response = client.post(
            path,
            request,
            LoadTableResponse.class,
            mutationHeaders,
            errorHandler
        );

        // 更新本地元数据
        updateCurrentMetadata(response);

    } catch (CommitStateUnknownException e) {
        // 尝试轻量级协调(仅限快照追加场景)
        if (updateType == UpdateType.SIMPLE && reconcileOnSimpleUpdate(updates, e)) {
            return;  // 协调成功,提交实际上成功了
        }
        throw e;
    }

    // 后续都是 SIMPLE 提交
    this.updateType = UpdateType.SIMPLE;
}
```

#### REST Catalog 轻量级协调机制

REST Catalog 独有的优化 — 对于 **仅追加快照** 的场景,可以尝试协调:

```java
// 源码: RESTTableOperations.java
private boolean reconcileOnSimpleUpdate(
        List<MetadataUpdate> updates,
        CommitStateUnknownException original) {

    // 1. 检查是否为"仅追加快照"更新
    Long expectedSnapshotId = expectedSnapshotIdIfSnapshotAddOnly(updates);
    if (expectedSnapshotId == null) {
        return false;  // 不是纯快照追加,无法协调
    }

    try {
        // 2. 刷新表状态
        TableMetadata refreshed = refresh();

        // 3. 检查预期的快照是否存在
        return refreshed != null &&
               refreshed.snapshot(expectedSnapshotId) != null;

    } catch (RuntimeException reconEx) {
        original.addSuppressed(reconEx);
        return false;
    }
}

private static Long expectedSnapshotIdIfSnapshotAddOnly(List<MetadataUpdate> updates) {
    Long addedSnapshotId = null;
    Long mainRefSnapshotId = null;

    for (MetadataUpdate update : updates) {
        if (update instanceof MetadataUpdate.AddSnapshot) {
            if (addedSnapshotId != null) {
                return null;  // 多个快照追加,不安全
            }
            addedSnapshotId = ((MetadataUpdate.AddSnapshot) update)
                .snapshot().snapshotId();

        } else if (update instanceof MetadataUpdate.SetSnapshotRef) {
            MetadataUpdate.SetSnapshotRef setRef = (MetadataUpdate.SetSnapshotRef) update;
            if (!SnapshotRef.MAIN_BRANCH.equals(setRef.name())) {
                return null;  // 非 main 分支更新,不安全
            }
            mainRefSnapshotId = setRef.snapshotId();

        } else {
            return null;  // 其他类型更新,不安全
        }
    }

    // 确保是"追加到 main"场景
    if (mainRefSnapshotId != null && !addedSnapshotId.equals(mainRefSnapshotId)) {
        return null;
    }

    return addedSnapshotId;
}
```

**协调机制的安全性分析**:

| 更新类型 | 可协调 | 原因 |
|---------|--------|------|
| 追加快照到 main | ✅ | 快照 ID 唯一,存在即成功 |
| 多个快照追加 | ❌ | 部分成功状态无法判断 |
| 分支操作 | ❌ | 分支指针可能被覆盖 |
| Schema 变更 | ❌ | 无法通过快照判断 |
| 属性变更 | ❌ | 无法通过快照判断 |

#### UpdateRequirements 机制

REST Catalog 使用 `UpdateRequirements` 在服务端验证前置条件:

```java
// 源码: api/src/main/java/org/apache/iceberg/UpdateRequirements.java
public class UpdateRequirements {

    public static List<UpdateRequirement> forUpdateTable(
            TableMetadata base,
            List<MetadataUpdate> updates) {

        ImmutableList.Builder<UpdateRequirement> requirements = ImmutableList.builder();

        // 1. 要求表必须存在
        requirements.add(new AssertTableExists());

        // 2. 要求 UUID 匹配
        if (base.uuid() != null) {
            requirements.add(new AssertTableUUID(base.uuid()));
        }

        // 3. 根据更新类型添加额外要求
        for (MetadataUpdate update : updates) {
            if (update instanceof MetadataUpdate.AddSnapshot) {
                // 追加快照: 要求当前快照 ID 匹配
                requirements.add(new AssertRefSnapshotId(
                    SnapshotRef.MAIN_BRANCH,
                    base.currentSnapshot().snapshotId()
                ));
            }
            // ... 其他更新类型
        }

        return requirements.build();
    }
}
```

**服务端验证流程**:

```
1. Client 发送: UpdateTableRequest {
     requirements: [AssertTableExists, AssertTableUUID("xxx"), ...],
     updates: [AddSnapshot(...), SetSnapshotRef(...)]
   }

2. Server 验证:
   for (requirement : requirements) {
       if (!requirement.validate(currentMetadata)) {
           return 409 Conflict;  // 前置条件不满足
       }
   }

3. Server 应用更新:
   newMetadata = apply(currentMetadata, updates);
   persist(newMetadata);
   return 200 OK with LoadTableResponse;
```

#### 特性对比总结

| 特性 | HiveCatalog | RESTCatalog |
|------|-------------|-------------|
| **协议** | Thrift | HTTP/REST |
| **元数据存储** | HMS 数据库 | 服务端决定 |
| **元数据文件** | 客户端写 S3/HDFS | 可服务端托管 |
| **锁机制** | HIVE_LOCKS 表 | 服务端实现 |
| **并发控制** | 元数据位置比对 | UpdateRequirements |
| **状态协调** | 无 | 快照追加场景可协调 |
| **权限控制** | HMS 级别 | 可细粒度控制 |
| **多租户** | 困难 | 原生支持 |
| **版本兼容** | 需匹配 HMS 版本 | API 版本化 |

**深入理解**: REST Catalog 代表了 Iceberg 的架构演进方向。通过将元数据操作集中到服务端,不仅简化了客户端配置,还为实现高级特性(如行级权限、数据审计、多租户)提供了基础。Netflix、Apple 等大厂已在生产环境大规模使用 REST Catalog。

#### 面试加分话术

> "REST Catalog 的核心优势是**关注点分离**: 客户端只需通过 HTTP 调用 REST API,无需关心底层存储细节。服务端可以灵活选择 HMS、PostgreSQL 甚至 DynamoDB 作为后端,客户端代码无需任何修改。此外,REST Catalog 独有的轻量级协调机制可以在网络超时场景下自动恢复快照追加操作,提高了系统容错性。"

#### 30 秒速记（简版）

- 结论先行：REST Catalog 是 Iceberg 社区推荐的**下一代 Catalog 架构**,通过 HTTP API 实现了更好的解耦、扩展性和安全性。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/rest/RESTTableOperations.java:1`，再联动 `core/src/main/java/org/apache/iceberg/CatalogUtil.java:66`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/rest/RESTTableOperations.java:1`
- `core/src/main/java/org/apache/iceberg/CatalogUtil.java:66`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/rest/RESTTableOperations.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`core/src/main/java/org/apache/iceberg/CatalogUtil.java:63-69`**
```java
   *
   * <ul>
   *   <li>hive: org.apache.iceberg.hive.HiveCatalog
   *   <li>hadoop: org.apache.iceberg.hadoop.HadoopCatalog
   * </ul>
   */
  public static final String ICEBERG_CATALOG_TYPE = "type";
```

---

## 八、SnapshotProducer 提交核心流程
<a id="q-8-1"></a>
### 8.1 SnapshotProducer 模板方法模式

**🔴 问题: SnapshotProducer 是 Iceberg 所有写操作的基类。请详细说明其模板方法模式设计,以及 commit() 方法的重试机制。**

**答案:**

`SnapshotProducer` 是 Iceberg 快照生产的**核心抽象类**,采用**模板方法模式**定义了快照创建的骨架流程,子类只需实现特定的抽象方法。

#### 类继承体系

```
SnapshotProducer<ThisT> (抽象基类)
    │
    ├── MergingSnapshotProducer<ThisT> (合并快照生产者)
    │       │
    │       ├── BaseAppendFiles (追加文件)
    │       ├── BaseOverwriteFiles (覆盖文件)
    │       ├── BaseReplacePartitions (替换分区)
    │       ├── BaseDeleteFiles (删除文件)
    │       └── BaseRowDelta (行级变更)
    │
    ├── ManageSnapshots (快照管理)
    ├── SetSnapshotOperation (设置快照)
    └── RewriteManifestsAction (重写 Manifest)
```

#### 模板方法核心结构

```java
// 源码: core/src/main/java/org/apache/iceberg/SnapshotProducer.java
abstract class SnapshotProducer<ThisT> implements SnapshotUpdate<ThisT> {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotProducer.class);

    // Caffeine 缓存: 用于缓存 Manifest 元数据
    private final LoadingCache<ManifestFile, ManifestFile> manifestsWithMetadata;

    private final TableOperations ops;
    private final String commitUUID = UUID.randomUUID().toString();
    private final AtomicInteger attempt = new AtomicInteger(0);
    private final List<String> manifestLists = Lists.newArrayList();
    private final long targetManifestSizeBytes;

    private volatile Long snapshotId = null;
    private TableMetadata base;

    protected SnapshotProducer(TableOperations ops) {
        this.ops = ops;
        this.base = ops.current();

        // Caffeine 缓存初始化
        this.manifestsWithMetadata = Caffeine.newBuilder()
            .build(file -> {
                if (file.snapshotId() != null) {
                    return file;
                }
                return addMetadata(ops, file);  // 补充 Manifest 元数据
            });

        this.targetManifestSizeBytes = ops.current()
            .propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
    }

    // ============ 模板方法 ============

    /**
     * 抽象方法: 返回操作类型(如 "append", "overwrite", "delete")
     */
    protected abstract String operation();

    /**
     * 抽象方法: 应用变更,返回新的 Manifest 列表
     */
    protected abstract List<ManifestFile> apply(TableMetadata metadataToUpdate, Snapshot snapshot);

    /**
     * 抽象方法: 返回快照摘要信息
     */
    protected abstract Map<String, String> summary();

    /**
     * 抽象方法: 清理未提交的 Manifest
     */
    protected abstract void cleanUncommitted(Set<ManifestFile> committed);

    /**
     * 钩子方法: 子类可覆盖以添加自定义验证
     */
    protected void validate(TableMetadata currentMetadata, Snapshot snapshot) {}
}
```

#### commit() 方法核心实现

```java
// 源码: SnapshotProducer.java
@Override
public void commit() {
    // 用于跨重试保持最新快照引用
    AtomicReference<Snapshot> stagedSnapshot = new AtomicReference<>();

    try (Timed ignore = commitMetrics().totalDuration().start()) {
        try {
            // 核心: Tasks 框架实现重试机制
            Tasks.foreach(ops)
                .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
                .exponentialBackoff(
                    base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
                    base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
                    base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
                    2.0 /* exponential factor */)
                .onlyRetryOn(CommitFailedException.class)
                .countAttempts(commitMetrics().attempts())
                .run(taskOps -> {
                    // 1. 调用 apply() 生成新快照
                    Snapshot newSnapshot = apply();
                    stagedSnapshot.set(newSnapshot);

                    // 2. 构建新的 TableMetadata
                    TableMetadata.Builder update = TableMetadata.buildFrom(base);

                    if (base.snapshot(newSnapshot.snapshotId()) != null) {
                        // 回滚操作: 快照已存在
                        update.setBranchSnapshot(newSnapshot.snapshotId(), targetBranch);
                    } else if (stageOnly) {
                        // 仅暂存,不设为当前快照
                        update.addSnapshot(newSnapshot);
                    } else {
                        // 正常提交: 添加快照并设为当前
                        update.setBranchSnapshot(newSnapshot, targetBranch);
                    }

                    TableMetadata updated = update.build();

                    // 3. 检查是否有实际变更
                    if (updated.changes().isEmpty()) {
                        return;  // 无变更,跳过提交
                    }

                    // 4. 调用 TableOperations.commit() 原子提交
                    taskOps.commit(base, updated.withUUID());
                });

        } catch (CommitStateUnknownException e) {
            // 状态未知异常直接抛出,不清理
            throw e;

        } catch (RuntimeException e) {
            // 其他异常: 根据 strictCleanup 决定是否清理
            if (!strictCleanup || e instanceof CleanableFailure) {
                Exceptions.suppressAndThrow(e, this::cleanAll);
            }
            throw e;
        }

        // 5. 提交成功后清理未使用的 Manifest
        Snapshot committedSnapshot = stagedSnapshot.get();
        try {
            LOG.info("Committed snapshot {} ({})",
                committedSnapshot.snapshotId(), getClass().getSimpleName());

            if (cleanupAfterCommit()) {
                cleanUncommitted(Sets.newHashSet(committedSnapshot.allManifests(ops.io())));
            }

            // 清理多次重试产生的未使用 ManifestList
            for (String manifestList : manifestLists) {
                if (!committedSnapshot.manifestListLocation().equals(manifestList)) {
                    deleteFile(manifestList);
                }
            }
        } catch (Throwable e) {
            LOG.warn("Failed to cleanup after commit", e);
        }
    }

    // 6. 通知事件监听器
    notifyListeners();
}
```

#### apply() 方法实现

```java
// 源码: SnapshotProducer.java
@Override
public Snapshot apply() {
    // 1. 刷新 base 元数据
    refresh();

    // 2. 获取父快照
    Snapshot parentSnapshot = SnapshotUtil.latestSnapshot(base, targetBranch);
    long sequenceNumber = base.nextSequenceNumber();
    Long parentSnapshotId = parentSnapshot == null ? null : parentSnapshot.snapshotId();

    // 3. 运行验证
    runValidations(parentSnapshot);

    // 4. 调用子类 apply() 获取 Manifest 列表
    List<ManifestFile> manifests = apply(base, parentSnapshot);

    // 5. 创建 ManifestList 文件
    OutputFile manifestList = manifestListPath();
    ManifestListWriter writer = ManifestLists.write(
        ops.current().formatVersion(),
        manifestList,
        ops.encryption(),
        snapshotId(),
        parentSnapshotId,
        sequenceNumber,
        base.nextRowId());

    try (writer) {
        manifestLists.add(manifestList.location());

        // 并行补充 Manifest 元数据(使用 Caffeine 缓存)
        ManifestFile[] manifestFiles = new ManifestFile[manifests.size()];
        Tasks.range(manifestFiles.length)
            .stopOnFailure()
            .throwFailureWhenFinished()
            .executeWith(workerPool())
            .run(index -> manifestFiles[index] = manifestsWithMetadata.get(manifests.get(index)));

        writer.addAll(Arrays.asList(manifestFiles));
    }

    // 6. 构建并返回新快照
    return new BaseSnapshot(
        sequenceNumber,
        snapshotId(),
        parentSnapshotId,
        System.currentTimeMillis(),
        operation(),        // 子类实现
        summary(base),      // 子类实现
        base.currentSchemaId(),
        manifestList.location(),
        nextRowId,
        assignedRows,
        writer.toManifestListFile().encryptionKeyID());
}
```

#### 重试机制配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `commit.retry.num-retries` | 4 | 最大重试次数 |
| `commit.retry.min-wait-ms` | 100 | 最小重试等待时间(ms) |
| `commit.retry.max-wait-ms` | 60000 | 最大重试等待时间(ms) |
| `commit.retry.total-timeout-ms` | 1800000 | 总重试超时时间(30分钟) |

**指数退避示例**:
```
重试1: 等待 100ms
重试2: 等待 200ms (100 * 2^1)
重试3: 等待 400ms (100 * 2^2)
重试4: 等待 800ms (100 * 2^3)
... 直到达到 max-wait-ms 或 total-timeout-ms
```

#### Caffeine 缓存设计

```java
// Caffeine 缓存用于避免重复读取 Manifest 文件
private final LoadingCache<ManifestFile, ManifestFile> manifestsWithMetadata;

this.manifestsWithMetadata = Caffeine.newBuilder()
    .build(file -> {
        if (file.snapshotId() != null) {
            return file;  // 已有元数据,直接返回
        }
        return addMetadata(ops, file);  // 读取文件补充元数据
    });
```

**缓存的元数据包括**:
- `snapshotId`: 快照 ID
- `addedFilesCount`, `addedRowsCount`: 新增文件/行数
- `existingFilesCount`, `existingRowsCount`: 已存在文件/行数
- `deletedFilesCount`, `deletedRowsCount`: 删除文件/行数
- `partitionSummaries`: 分区统计信息

**设计优势**:
1. **避免重复 IO**: 同一 Manifest 只读取一次
2. **支持重试**: 重试时复用缓存的元数据
3. **并行加速**: Tasks 框架并行填充缓存

**深入理解**: SnapshotProducer 的模板方法模式将通用逻辑(重试、元数据构建、清理)与特定操作(追加、覆盖、删除)解耦。Caffeine 缓存是关键优化点,在高并发写入场景下可显著减少 IO 开销。

#### 面试加分话术

> "SnapshotProducer 采用经典的模板方法模式: `commit()` 定义骨架流程,`apply()`、`operation()`、`summary()` 由子类实现。核心亮点是 Caffeine LoadingCache 用于缓存 Manifest 元数据 — 在 OCC 重试场景下,避免重复读取已处理的 Manifest 文件,同时 Tasks 框架提供了指数退避的重试机制,确保在高并发冲突时能够优雅恢复。"

#### 30 秒速记（简版）

- 结论先行：SnapshotProducer 是 Iceberg 快照生产的**核心抽象类**,采用**模板方法模式**定义了快照创建的骨架流程,子类只需实现特定的抽象。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/SnapshotProducer.java:1`，再联动 `core/src/main/java/org/apache/iceberg/SnapshotProducer.java:83`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/SnapshotProducer.java:1`
- `core/src/main/java/org/apache/iceberg/SnapshotProducer.java:83`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/SnapshotProducer.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`core/src/main/java/org/apache/iceberg/SnapshotProducer.java:80-86`**
```java
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps common functionality to create a new snapshot.
 *
```

---
<a id="q-8-2"></a>
### 8.2 Manifest 自动合并与分裂机制

**🟠 问题: Iceberg 如何自动管理 Manifest 文件数量？请从源码角度说明 ManifestMergeManager 的合并策略。**

**答案:**

Iceberg 通过 `ManifestMergeManager` 实现 Manifest 文件的**自动合并与分裂**,在查询性能和写入效率之间取得平衡。

#### 为什么需要 Manifest 合并？

```
场景: 流式写入产生大量小 Manifest

每个 Checkpoint 产生一个 Manifest:
Checkpoint 1 → manifest-001.avro (100 文件)
Checkpoint 2 → manifest-002.avro (100 文件)
Checkpoint 3 → manifest-003.avro (100 文件)
...
Checkpoint 1000 → manifest-1000.avro (100 文件)

问题:
- 查询规划需要读取 1000 个 Manifest 文件
- 每个 Manifest 只有 ~10KB,大量小文件 IO
- 元数据膨胀,影响性能

解决方案: 自动合并成少量大 Manifest
合并后: manifest-merged.avro (100000 文件, ~10MB)
```

#### ManifestMergeManager 核心实现

```java
// 源码: core/src/main/java/org/apache/iceberg/ManifestMergeManager.java
abstract class ManifestMergeManager<F extends ContentFile<F>> {
    private final long targetSizeBytes;      // 目标 Manifest 大小
    private final int minCountToMerge;       // 最小合并数量
    private final boolean mergeEnabled;      // 是否启用合并

    // 缓存已合并的 Manifest,支持重试复用
    private final Map<List<ManifestFile>, ManifestFile> mergedManifests = Maps.newConcurrentMap();

    /**
     * 合并 Manifest 文件
     */
    Iterable<ManifestFile> mergeManifests(Iterable<ManifestFile> manifests) {
        Iterator<ManifestFile> manifestIter = manifests.iterator();

        // 如果合并未启用或为空,直接返回
        if (!mergeEnabled || !manifestIter.hasNext()) {
            return manifests;
        }

        ManifestFile first = manifestIter.next();
        List<ManifestFile> merged = Lists.newArrayList();

        // 1. 按 PartitionSpec 分组
        ListMultimap<Integer, ManifestFile> groups = groupBySpec(first, manifestIter);

        // 2. 对每个 Spec 分组进行合并
        for (Integer specId : groups.keySet()) {
            Iterables.addAll(merged, mergeGroup(first, specId, groups.get(specId)));
        }

        return merged;
    }
}
```

#### BinPacking 装箱算法

```java
// 源码: ManifestMergeManager.java
private Iterable<ManifestFile> mergeGroup(
        ManifestFile first,
        int specId,
        List<ManifestFile> group) {

    // 使用 BinPacking 算法将 Manifest 分组
    // lookback=1: 避免重排序,从尾部开始装箱
    ListPacker<ManifestFile> packer = new ListPacker<>(targetSizeBytes, 1, false);
    List<List<ManifestFile>> bins = packer.packEnd(group, ManifestFile::length);

    // 并行处理每个 bin
    List<ManifestFile>[] binResults = new List[bins.size()];

    Tasks.range(bins.size())
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(workerPoolSupplier.get())
        .run(index -> {
            List<ManifestFile> bin = bins.get(index);
            List<ManifestFile> outputManifests = Lists.newArrayList();
            binResults[index] = outputManifests;

            if (bin.size() == 1) {
                // 单个 Manifest,无需合并
                outputManifests.add(bin.get(0));
                return;
            }

            // 如果 bin 包含新数据的 Manifest 且数量不足最小值,不合并
            if (bin.contains(first) && bin.size() < minCountToMerge) {
                outputManifests.addAll(bin);
            } else {
                // 执行合并
                outputManifests.add(createManifest(specId, bin));
            }
        });

    return Iterables.concat(binResults);
}
```

#### 合并逻辑详解

```java
// 源码: ManifestMergeManager.java
private ManifestFile createManifest(int specId, List<ManifestFile> bin) {
    // 检查缓存,避免重复合并
    if (mergedManifests.containsKey(bin)) {
        return mergedManifests.get(bin);
    }

    ManifestWriter<F> writer = newManifestWriter(spec(specId));
    try {
        for (ManifestFile manifest : bin) {
            try (ManifestReader<F> reader = newManifestReader(manifest)) {
                for (ManifestEntry<F> entry : reader.entries()) {

                    if (entry.status() == Status.DELETED) {
                        // DELETED 状态: 仅保留当前快照的删除
                        if (entry.snapshotId() == snapshotId()) {
                            writer.delete(entry);
                        }
                        // 其他快照的 DELETED 被抑制(已在之前快照处理)

                    } else if (entry.status() == Status.ADDED &&
                               entry.snapshotId() == snapshotId()) {
                        // ADDED 状态且属于当前快照: 保持 ADDED
                        writer.add(entry);

                    } else {
                        // 其他情况: 转为 EXISTING 状态
                        writer.existing(entry);
                    }
                }
            }
        }
    } finally {
        writer.close();
    }

    ManifestFile manifest = writer.toManifestFile();

    // 更新缓存
    mergedManifests.put(bin, manifest);

    return manifest;
}
```

#### 状态转换规则

```
合并时 ManifestEntry 状态转换:

┌─────────────────────────────────────────────────────────────┐
│  原状态         条件                      新状态             │
├─────────────────────────────────────────────────────────────┤
│  ADDED         snapshotId == 当前快照      ADDED            │
│  ADDED         snapshotId != 当前快照      EXISTING         │
│  EXISTING      -                          EXISTING         │
│  DELETED       snapshotId == 当前快照      DELETED          │
│  DELETED       snapshotId != 当前快照      (抑制,不写入)     │
└─────────────────────────────────────────────────────────────┘

原因:
- 旧快照的 DELETED 已经在该快照的元数据中记录,无需重复
- 旧快照的 ADDED 对当前快照来说是已存在的文件
- 只有当前快照的 ADDED/DELETED 需要保持原状态
```

#### 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `commit.manifest.target-size-bytes` | 8MB | 目标 Manifest 大小 |
| `commit.manifest.min-count-to-merge` | 100 | 最小合并数量 |
| `commit.manifest-merge.enabled` | true | 是否启用自动合并 |

#### 合并示例

```
输入 Manifests:
┌──────────────────┬─────────┬────────────────┐
│ Manifest         │ Size    │ PartitionSpec  │
├──────────────────┼─────────┼────────────────┤
│ manifest-001.avro│ 1MB     │ spec-0         │
│ manifest-002.avro│ 2MB     │ spec-0         │
│ manifest-003.avro│ 3MB     │ spec-0         │
│ manifest-004.avro│ 1MB     │ spec-0         │
│ manifest-005.avro│ 2MB     │ spec-0         │
└──────────────────┴─────────┴────────────────┘

BinPacking (targetSize=8MB):
Bin 1: [manifest-005, manifest-004, manifest-003] → 6MB (不足8MB,继续装)
       [manifest-005, manifest-004, manifest-003, manifest-002] → 8MB ✓
Bin 2: [manifest-001] → 1MB (剩余单个)

输出 Manifests:
┌──────────────────────┬─────────┐
│ Manifest             │ Size    │
├──────────────────────┼─────────┤
│ merged-001.avro      │ 8MB     │ ← 合并了 4 个
│ manifest-001.avro    │ 1MB     │ ← 保持原样
└──────────────────────┴─────────┘
```

**深入理解**: ManifestMergeManager 的设计体现了"空间换时间"的思想。通过缓存已合并的 Manifest,在 OCC 重试场景下避免重复合并。BinPacking 算法确保合并后的 Manifest 接近目标大小,既不会产生过多小文件,也不会生成难以处理的超大文件。

#### 面试加分话术

> "ManifestMergeManager 使用 BinPacking 装箱算法,将多个小 Manifest 合并成接近 target-size-bytes 的大 Manifest。关键设计是 `mergedManifests` 缓存 — 在 OCC 重试时复用已合并的结果,避免重复 IO。另外,状态转换规则确保旧快照的 DELETED 条目被抑制,避免元数据无限膨胀。"

#### 30 秒速记（简版）

- 结论先行：Iceberg 通过 ManifestMergeManager 实现 Manifest 文件的**自动合并与分裂**,在查询性能和写入效率之间取得平衡。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/ManifestMergeManager.java:1`，再联动 `core/src/main/java/org/apache/iceberg/SnapshotProducer.java:83`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/ManifestMergeManager.java:1`
- `core/src/main/java/org/apache/iceberg/SnapshotProducer.java:83`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/ManifestMergeManager.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`core/src/main/java/org/apache/iceberg/SnapshotProducer.java:80-86`**
```java
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps common functionality to create a new snapshot.
 *
```

---
<a id="q-8-3"></a>
### 8.3 Snapshot Summary 累加器设计

**🟡 问题: Iceberg 快照的 summary 字段包含丰富的统计信息。请说明 SnapshotSummary 的累加器设计及其在增量计算中的应用。**

**答案:**

`SnapshotSummary` 采用**累加器模式**,通过增量计算维护表的全局统计信息,避免每次提交都需要全表扫描。

#### Summary 字段含义

```java
// 源码: core/src/main/java/org/apache/iceberg/SnapshotSummary.java
public class SnapshotSummary {
    // 数据文件统计
    public static final String ADDED_FILES_PROP = "added-data-files";
    public static final String DELETED_FILES_PROP = "deleted-data-files";
    public static final String TOTAL_DATA_FILES_PROP = "total-data-files";

    // 删除文件统计
    public static final String ADDED_DELETE_FILES_PROP = "added-delete-files";
    public static final String REMOVED_DELETE_FILES_PROP = "removed-delete-files";
    public static final String TOTAL_DELETE_FILES_PROP = "total-delete-files";

    // 记录数统计
    public static final String ADDED_RECORDS_PROP = "added-records";
    public static final String DELETED_RECORDS_PROP = "deleted-records";
    public static final String TOTAL_RECORDS_PROP = "total-records";

    // 文件大小统计
    public static final String ADDED_FILE_SIZE_PROP = "added-files-size";
    public static final String REMOVED_FILE_SIZE_PROP = "removed-files-size";
    public static final String TOTAL_FILE_SIZE_PROP = "total-files-size";

    // Position Delete 统计
    public static final String ADDED_POS_DELETES_PROP = "added-position-deletes";
    public static final String REMOVED_POS_DELETES_PROP = "removed-position-deletes";
    public static final String TOTAL_POS_DELETES_PROP = "total-position-deletes";

    // Equality Delete 统计
    public static final String ADDED_EQ_DELETES_PROP = "added-equality-deletes";
    public static final String REMOVED_EQ_DELETES_PROP = "removed-equality-deletes";
    public static final String TOTAL_EQ_DELETES_PROP = "total-equality-deletes";

    // Manifest 统计
    public static final String CREATED_MANIFESTS_COUNT = "manifests-created";
    public static final String REPLACED_MANIFESTS_COUNT = "manifests-replaced";
    public static final String KEPT_MANIFESTS_COUNT = "manifests-kept";

    // 分区变更统计
    public static final String CHANGED_PARTITION_COUNT_PROP = "changed-partition-count";
    public static final String PARTITION_SUMMARY_PROP = "partition-summaries-included";
}
```

#### SnapshotSummary.Builder 累加器实现

```java
// 源码: SnapshotSummary.java
public static class Builder {
    private final Map<String, String> properties = Maps.newHashMap();
    private final Map<String, UpdateMetrics> partitionMetrics = Maps.newHashMap();
    private final UpdateMetrics metrics = new UpdateMetrics();

    /**
     * 添加数据文件时累加统计
     */
    public void addedFile(PartitionSpec spec, DataFile file) {
        metrics.addedFile(file);
        updatePartitions(spec, file, true);
    }

    /**
     * 添加删除文件时累加统计
     */
    public void addedFile(PartitionSpec spec, DeleteFile file) {
        metrics.addedFile(file);
        updatePartitions(spec, file, true);
    }

    /**
     * 删除数据文件时累加统计
     */
    public void deletedFile(PartitionSpec spec, DataFile file) {
        metrics.removedFile(file);
        updatePartitions(spec, file, false);
    }

    /**
     * 删除 Delete 文件时累加统计
     */
    public void deletedFile(PartitionSpec spec, DeleteFile file) {
        metrics.removedFile(file);
        updatePartitions(spec, file, false);
    }
}
```

#### UpdateMetrics 内部累加器

```java
// UpdateMetrics 负责累加文件级别统计
class UpdateMetrics {
    private long addedFiles = 0;
    private long removedFiles = 0;
    private long addedRecords = 0;
    private long removedRecords = 0;
    private long addedFileSize = 0;
    private long removedFileSize = 0;
    private long addedDeleteFiles = 0;
    private long removedDeleteFiles = 0;
    private long addedPosDeletes = 0;
    private long removedPosDeletes = 0;
    private long addedEqDeletes = 0;
    private long removedEqDeletes = 0;

    void addedFile(DataFile file) {
        addedFiles++;
        addedRecords += file.recordCount();
        addedFileSize += file.fileSizeInBytes();
    }

    void addedFile(DeleteFile file) {
        addedDeleteFiles++;
        addedFileSize += file.fileSizeInBytes();
        if (file.content() == FileContent.POSITION_DELETES) {
            addedPosDeletes += file.recordCount();
        } else {
            addedEqDeletes += file.recordCount();
        }
    }

    void removedFile(DataFile file) {
        removedFiles++;
        removedRecords += file.recordCount();
        removedFileSize += file.fileSizeInBytes();
    }

    void removedFile(DeleteFile file) {
        removedDeleteFiles++;
        removedFileSize += file.fileSizeInBytes();
        if (file.content() == FileContent.POSITION_DELETES) {
            removedPosDeletes += file.recordCount();
        } else {
            removedEqDeletes += file.recordCount();
        }
    }
}
```

#### Total 值增量计算

```java
// 源码: SnapshotProducer.java
private Map<String, String> summary(TableMetadata previous) {
    Map<String, String> summary = summary();  // 子类提供的增量统计

    // 获取前一个快照的 summary
    Map<String, String> previousSummary;
    SnapshotRef previousBranchHead = previous.ref(targetBranch);
    if (previousBranchHead != null) {
        previousSummary = previous.snapshot(previousBranchHead.snapshotId()).summary();
    } else {
        // 首个快照,初始化为 0
        previousSummary = ImmutableMap.of(
            TOTAL_RECORDS_PROP, "0",
            TOTAL_FILE_SIZE_PROP, "0",
            TOTAL_DATA_FILES_PROP, "0",
            TOTAL_DELETE_FILES_PROP, "0",
            TOTAL_POS_DELETES_PROP, "0",
            TOTAL_EQ_DELETES_PROP, "0"
        );
    }

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(summary);

    // 增量计算: total = previous_total + added - deleted
    updateTotal(builder, previousSummary,
        TOTAL_RECORDS_PROP, summary, ADDED_RECORDS_PROP, DELETED_RECORDS_PROP);
    updateTotal(builder, previousSummary,
        TOTAL_FILE_SIZE_PROP, summary, ADDED_FILE_SIZE_PROP, REMOVED_FILE_SIZE_PROP);
    updateTotal(builder, previousSummary,
        TOTAL_DATA_FILES_PROP, summary, ADDED_FILES_PROP, DELETED_FILES_PROP);
    updateTotal(builder, previousSummary,
        TOTAL_DELETE_FILES_PROP, summary, ADDED_DELETE_FILES_PROP, REMOVED_DELETE_FILES_PROP);
    updateTotal(builder, previousSummary,
        TOTAL_POS_DELETES_PROP, summary, ADDED_POS_DELETES_PROP, REMOVED_POS_DELETES_PROP);
    updateTotal(builder, previousSummary,
        TOTAL_EQ_DELETES_PROP, summary, ADDED_EQ_DELETES_PROP, REMOVED_EQ_DELETES_PROP);

    return builder.build();
}

private static void updateTotal(
        ImmutableMap.Builder<String, String> builder,
        Map<String, String> previousSummary,
        String totalProperty,
        Map<String, String> currentSummary,
        String addedProperty,
        String deletedProperty) {

    String totalStr = previousSummary.get(totalProperty);
    if (totalStr != null) {
        long newTotal = Long.parseLong(totalStr);

        String addedStr = currentSummary.get(addedProperty);
        if (addedStr != null) {
            newTotal += Long.parseLong(addedStr);  // total += added
        }

        String deletedStr = currentSummary.get(deletedProperty);
        if (deletedStr != null) {
            newTotal -= Long.parseLong(deletedStr);  // total -= deleted
        }

        if (newTotal >= 0) {
            builder.put(totalProperty, String.valueOf(newTotal));
        }
    }
}
```

#### 增量计算示例

```
Snapshot 1 (初始):
  added-data-files: 10
  added-records: 1000
  total-data-files: 10
  total-records: 1000

Snapshot 2 (追加):
  added-data-files: 5
  added-records: 500
  total-data-files: 10 + 5 = 15
  total-records: 1000 + 500 = 1500

Snapshot 3 (删除+追加):
  added-data-files: 3
  deleted-data-files: 2
  added-records: 300
  deleted-records: 200
  total-data-files: 15 + 3 - 2 = 16
  total-records: 1500 + 300 - 200 = 1600
```

#### 分区级别统计

```java
// 源码: SnapshotSummary.java
private void updatePartitions(PartitionSpec spec, ContentFile<?> file, boolean isAdd) {
    if (spec.isUnpartitioned()) {
        return;  // 非分区表跳过
    }

    // 构建分区键
    String partitionKey = spec.partitionToPath(file.partition());

    // 获取或创建分区指标
    UpdateMetrics partMetrics = partitionMetrics
        .computeIfAbsent(partitionKey, k -> new UpdateMetrics());

    // 累加分区级别统计
    if (isAdd) {
        if (file instanceof DataFile) {
            partMetrics.addedFile((DataFile) file);
        } else {
            partMetrics.addedFile((DeleteFile) file);
        }
    } else {
        if (file instanceof DataFile) {
            partMetrics.removedFile((DataFile) file);
        } else {
            partMetrics.removedFile((DeleteFile) file);
        }
    }
}
```

**分区 Summary 示例**:
```json
{
  "changed-partition-count": "3",
  "partition-summaries-included": "true",
  "partitions.date=2024-01-01": "added-data-files=5,added-records=1000",
  "partitions.date=2024-01-02": "added-data-files=3,added-records=600",
  "partitions.date=2024-01-03": "deleted-data-files=2,deleted-records=400"
}
```

#### 设计优势

| 特性 | 传统方式 | Iceberg 累加器 |
|------|---------|---------------|
| **计算方式** | 全表扫描统计 | 增量计算 |
| **时间复杂度** | O(n) - n为文件数 | O(1) - 仅访问当前变更 |
| **支持分区统计** | 需额外扫描 | 原生支持 |
| **历史追溯** | 不支持 | 每个快照独立 summary |

**深入理解**: SnapshotSummary 的累加器模式是 Iceberg 高性能的关键设计之一。通过维护 `added` 和 `deleted` 的增量值,结合前一个快照的 `total` 值,可以在 O(1) 时间内计算出新快照的全局统计,这对于流式写入场景尤为重要。

#### 面试加分话术

> "SnapshotSummary 采用累加器模式实现 O(1) 时间复杂度的统计更新: `new_total = old_total + added - deleted`。每个快照只记录增量变化,通过链式累加得到全局统计。这种设计不仅避免了全表扫描,还支持分区级别的细粒度统计,对于监控和数据治理非常有价值。"

#### 高频追问

**Q: 如果 Summary 统计值与实际不一致怎么办？**

**A**:
1. **Compaction 可修复**: `rewriteDataFiles` 会重新计算统计
2. **ExpireSnapshots**: 可清理历史数据,重置统计基线
3. **手动修复**: 可通过 `RewriteTableMetadata` 修复元数据
4. **监控告警**: 建议定期对比 Summary 与实际文件数

#### 30 秒速记（简版）

- 结论先行：SnapshotSummary 采用**累加器模式**,通过增量计算维护表的全局统计信息,避免每次提交都需要全表扫描。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/SnapshotSummary.java:1`，再联动 `core/src/main/java/org/apache/iceberg/SnapshotProducer.java:83`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/SnapshotSummary.java:1`
- `core/src/main/java/org/apache/iceberg/SnapshotProducer.java:83`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/SnapshotSummary.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`core/src/main/java/org/apache/iceberg/SnapshotProducer.java:80-86`**
```java
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps common functionality to create a new snapshot.
 *
```

---

## 九、DeleteFileIndex 与删除文件匹配优化
<a id="q-9-1"></a>
### 9.1 DeleteFileIndex 多维索引结构

**🔴 问题: DeleteFileIndex 是 Iceberg 读取时关联删除文件的核心组件。请详细说明其多维索引结构及查找算法。**

**答案:**

`DeleteFileIndex` 是 Iceberg 在读取时高效匹配删除文件的**核心数据结构**,它通过多维索引实现 O(log n) 时间复杂度的删除文件查找。

#### 索引结构概览

```java
// 源码: core/src/main/java/org/apache/iceberg/DeleteFileIndex.java
class DeleteFileIndex {
    // Equality Delete 索引
    private final EqualityDeletes globalDeletes;             // 全局(非分区表)
    private final PartitionMap<EqualityDeletes> eqDeletesByPartition;  // 按分区索引

    // Position Delete 索引
    private final PartitionMap<PositionDeletes> posDeletesByPartition; // 按分区索引
    private final Map<String, PositionDeletes> posDeletesByPath;       // 按文件路径索引

    // Deletion Vector 索引 (V3+)
    private final Map<String, DeleteFile> dvByPath;          // DV 按文件路径索引

    // 快速判断标志
    private final boolean hasEqDeletes;
    private final boolean hasPosDeletes;
    private final boolean isEmpty;
}
```

#### 多维索引架构图

```
                    DeleteFileIndex
                          │
    ┌─────────────────────┼─────────────────────┐
    │                     │                     │
    ▼                     ▼                     ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ EqualityDeletes│  │PositionDeletes│  │ DeletionVector│
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                 │
       ├──globalDeletes  ├──byPartition    └──byPath (V3+)
       │  (非分区表)      │  (分区过滤)         (直接映射)
       │                 │
       └──byPartition    └──byPath
          (分区过滤)        (精确匹配)

索引查找优先级:
1. DV (V3) > Position Delete (V2) > Equality Delete
2. 精确路径匹配 > 分区匹配 > 全局匹配
```

#### forDataFile() 查找算法

```java
// 源码: DeleteFileIndex.java
DeleteFile[] forDataFile(long sequenceNumber, DataFile file) {
    if (isEmpty) {
        return EMPTY_DELETES;  // 快速返回
    }

    // 1. 查找全局 Equality Delete
    DeleteFile[] global = findGlobalDeletes(sequenceNumber, file);

    // 2. 查找分区 Equality Delete
    DeleteFile[] eqPartition = findEqPartitionDeletes(sequenceNumber, file);

    // 3. 查找 Deletion Vector (V3 优先)
    DeleteFile dv = findDV(sequenceNumber, file);
    if (dv != null && global == null && eqPartition == null) {
        return new DeleteFile[] {dv};  // 仅 DV,快速返回
    } else if (dv != null) {
        return concat(global, eqPartition, new DeleteFile[] {dv});
    }

    // 4. 查找 Position Delete (按分区)
    DeleteFile[] posPartition = findPosPartitionDeletes(sequenceNumber, file);

    // 5. 查找 Position Delete (按路径)
    DeleteFile[] posPath = findPathDeletes(sequenceNumber, file);

    return concat(global, eqPartition, posPartition, posPath);
}
```

#### Sequence Number 过滤机制

```java
// PositionDeletes 内部实现: 按 sequence number 排序的数组
static class PositionDeletes {
    private long[] seqs = null;        // 排序的 sequence number 数组
    private DeleteFile[] files = null;  // 对应的删除文件数组

    public DeleteFile[] filter(long seq) {
        indexIfNeeded();  // 延迟索引

        // 二分查找起始位置
        int start = findStartIndex(seqs, seq);

        if (start >= files.length) {
            return EMPTY_DELETES;
        }

        if (start == 0) {
            return files;  // 所有删除文件都适用
        }

        // 返回 seq 之后的所有删除文件
        int matchingFilesCount = files.length - start;
        DeleteFile[] matchingFiles = new DeleteFile[matchingFilesCount];
        System.arraycopy(files, start, matchingFiles, 0, matchingFilesCount);
        return matchingFiles;
    }
}
```

**二分查找实现**:
```java
// 源码: DeleteFileIndex.java
private static int findStartIndex(long[] seqs, long seq) {
    int pos = Arrays.binarySearch(seqs, seq);
    int start;

    if (pos < 0) {
        // 未找到,返回插入点
        start = -(pos + 1);
    } else {
        // 找到,但需要找到第一个匹配
        start = pos;
        while (start > 0 && seqs[start - 1] >= seq) {
            start -= 1;
        }
    }

    return start;
}
```

#### 延迟索引模式

```java
// EqualityDeletes 使用 Double-Check Locking 延迟构建索引
static class EqualityDeletes {
    private volatile List<EqualityDeleteFile> buffer = Lists.newArrayList();
    private long[] seqs = null;
    private EqualityDeleteFile[] files = null;

    public void add(PartitionSpec spec, DeleteFile file) {
        Preconditions.checkState(buffer != null, "Can't add files upon indexing");
        buffer.add(new EqualityDeleteFile(spec, file));
    }

    private void indexIfNeeded() {
        if (buffer != null) {
            synchronized (this) {
                if (buffer != null) {
                    // 排序并构建索引
                    this.files = indexFiles(buffer);
                    this.seqs = indexSeqs(files);
                    this.buffer = null;  // 释放 buffer
                }
            }
        }
    }
}
```

#### 索引构建流程

```java
// 源码: DeleteFileIndex.Builder
DeleteFileIndex build() {
    Iterable<DeleteFile> files = deleteFiles != null
        ? filterDeleteFiles()
        : loadDeleteFiles();  // 并行加载

    EqualityDeletes globalDeletes = new EqualityDeletes();
    PartitionMap<EqualityDeletes> eqDeletesByPartition = PartitionMap.create(specsById);
    PartitionMap<PositionDeletes> posDeletesByPartition = PartitionMap.create(specsById);
    Map<String, PositionDeletes> posDeletesByPath = Maps.newHashMap();
    Map<String, DeleteFile> dvByPath = Maps.newHashMap();

    for (DeleteFile file : files) {
        switch (file.content()) {
            case POSITION_DELETES:
                if (ContentFileUtil.isDV(file)) {
                    // V3: Deletion Vector
                    add(dvByPath, file);
                } else {
                    // V2: Position Delete
                    add(posDeletesByPath, posDeletesByPartition, file);
                }
                break;

            case EQUALITY_DELETES:
                add(globalDeletes, eqDeletesByPartition, file);
                break;
        }
    }

    return new DeleteFileIndex(
        globalDeletes.isEmpty() ? null : globalDeletes,
        eqDeletesByPartition.isEmpty() ? null : eqDeletesByPartition,
        posDeletesByPartition.isEmpty() ? null : posDeletesByPartition,
        posDeletesByPath.isEmpty() ? null : posDeletesByPath,
        dvByPath.isEmpty() ? null : dvByPath);
}
```

#### 时间复杂度分析

| 操作 | 复杂度 | 说明 |
|------|--------|------|
| 索引构建 | O(n log n) | 排序 + 并行加载 |
| DV 查找 | O(1) | HashMap 直接查找 |
| 路径 Position Delete | O(1) + O(log m) | HashMap + 二分查找 |
| 分区 Position Delete | O(1) + O(log m) | PartitionMap + 二分查找 |
| Equality Delete | O(1) + O(log m) * k | 分区查找 + 范围过滤(k=字段数) |

**深入理解**: DeleteFileIndex 的多维索引设计体现了"分治"思想。通过将删除文件按类型(EQ/POS/DV)、范围(全局/分区/路径)分类索引,实现了高效的查找。延迟索引模式避免了不必要的排序开销,适合流式读取场景。

#### 面试加分话术

> "DeleteFileIndex 采用多维索引结构: DV 按路径 HashMap O(1) 查找,Position Delete 按路径/分区双索引,Equality Delete 支持全局和分区级别。核心是延迟索引模式 — buffer 收集文件,首次查询时才排序构建 seqs 数组,通过二分查找实现 O(log n) 的 sequence number 过滤。"

#### 30 秒速记（简版）

- 结论先行：DeleteFileIndex 是 Iceberg 在读取时高效匹配删除文件的**核心数据结构**,它通过多维索引实现 O(log n) 时间复杂度的删除文。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:1`，再联动 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:140`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:1`
- `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:140`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:137-143`**
```java
    return deleteFiles;
  }

  DeleteFile[] forEntry(ManifestEntry<DataFile> entry) {
    return forDataFile(entry.dataSequenceNumber(), entry.file());
  }

```

---
<a id="q-9-2"></a>
### 9.2 Equality Delete 的 Range Overlap 剪枝

**🟠 问题: Equality Delete 的匹配比 Position Delete 更复杂。请说明 canContainEqDeletesForFile() 方法的统计信息剪枝逻辑。**

**答案:**

Equality Delete 需要在读取时与每条记录的主键进行匹配,开销较大。Iceberg 通过**统计信息剪枝**,在文件级别快速判断是否可能包含匹配的删除记录,避免不必要的 IO。

#### 剪枝判断流程

```java
// 源码: DeleteFileIndex.java
private static boolean canContainEqDeletesForFile(
        DataFile dataFile,
        EqualityDeleteFile deleteFile) {

    Map<Integer, ByteBuffer> dataLowers = dataFile.lowerBounds();
    Map<Integer, ByteBuffer> dataUppers = dataFile.upperBounds();

    // 是否检查范围重叠(需要有 lower/upper bounds)
    boolean checkRanges = dataLowers != null && dataUppers != null
                          && deleteFile.hasLowerAndUpperBounds();

    Map<Integer, Long> dataNullCounts = dataFile.nullValueCounts();
    Map<Integer, Long> dataValueCounts = dataFile.valueCounts();
    Map<Integer, Long> deleteNullCounts = deleteFile.nullValueCounts();
    Map<Integer, Long> deleteValueCounts = deleteFile.valueCounts();

    // 遍历 Equality Delete 的每个字段
    for (Types.NestedField field : deleteFile.equalityFields()) {
        if (!field.type().isPrimitiveType()) {
            continue;  // 嵌套类型无统计信息,假设可能匹配
        }

        // ===== 剪枝规则 1: NULL 值检查 =====
        if (containsNull(dataNullCounts, field) &&
            containsNull(deleteNullCounts, field)) {
            // 数据和删除都有 NULL,必须应用
            continue;
        }

        // ===== 剪枝规则 2: 全 NULL 数据 vs 非 NULL 删除 =====
        if (allNull(dataNullCounts, dataValueCounts, field) &&
            allNonNull(deleteNullCounts, field)) {
            // 数据全是 NULL,删除全是非 NULL → 不可能匹配
            return false;
        }

        // ===== 剪枝规则 3: 全 NULL 删除 vs 非 NULL 数据 =====
        if (allNull(deleteNullCounts, deleteValueCounts, field) &&
            allNonNull(dataNullCounts, field)) {
            // 删除全是 NULL,数据全是非 NULL → 不可能匹配
            return false;
        }

        if (!checkRanges) {
            continue;  // 无法检查范围,假设可能匹配
        }

        // ===== 剪枝规则 4: 范围重叠检查 =====
        int id = field.fieldId();
        ByteBuffer dataLower = dataLowers.get(id);
        ByteBuffer dataUpper = dataUppers.get(id);
        Object deleteLower = deleteFile.lowerBound(id);
        Object deleteUpper = deleteFile.upperBound(id);

        if (dataLower == null || dataUpper == null ||
            deleteLower == null || deleteUpper == null) {
            continue;  // 边界不完整,假设可能匹配
        }

        if (!rangesOverlap(field, dataLower, dataUpper, deleteLower, deleteUpper)) {
            // 范围不重叠 → 不可能匹配
            return false;
        }
    }

    return true;  // 所有字段都可能匹配
}
```

#### 范围重叠判断

```java
// 源码: DeleteFileIndex.java
private static <T> boolean rangesOverlap(
        Types.NestedField field,
        ByteBuffer dataLowerBuf,
        ByteBuffer dataUpperBuf,
        T deleteLower,
        T deleteUpper) {

    Type.PrimitiveType type = field.type().asPrimitiveType();
    Comparator<T> comparator = Comparators.forType(type);

    // 转换数据文件的边界
    T dataLower = Conversions.fromByteBuffer(type, dataLowerBuf);
    T dataUpper = Conversions.fromByteBuffer(type, dataUpperBuf);

    // 范围不重叠条件:
    // dataLower > deleteUpper 或 deleteLower > dataUpper
    if (comparator.compare(dataLower, deleteUpper) > 0) {
        return false;  // 数据完全在删除范围之后
    }

    if (comparator.compare(deleteLower, dataUpper) > 0) {
        return false;  // 删除完全在数据范围之后
    }

    return true;  // 范围重叠
}
```

**范围重叠图示**:
```
情况1: 不重叠 (可剪枝)
Data:    [=====]
Delete:              [=====]
         dataUpper < deleteLower → 不重叠 ✓

情况2: 不重叠 (可剪枝)
Data:                [=====]
Delete:  [=====]
         deleteUpper < dataLower → 不重叠 ✓

情况3: 重叠 (需要检查)
Data:    [=====]
Delete:      [=====]
         dataLower <= deleteUpper && deleteLower <= dataUpper → 重叠

情况4: 包含 (需要检查)
Data:    [===========]
Delete:     [=====]
         deleteLower >= dataLower && deleteUpper <= dataUpper → 包含
```

#### 辅助判断函数

```java
// 判断是否包含 NULL 值
private static boolean containsNull(Map<Integer, Long> nullValueCounts, Types.NestedField field) {
    if (field.isRequired()) {
        return false;  // 必填字段不可能有 NULL
    }

    if (nullValueCounts == null) {
        return true;  // 无统计信息,假设有 NULL
    }

    Long nullValueCount = nullValueCounts.get(field.fieldId());
    return nullValueCount == null || nullValueCount > 0;
}

// 判断是否全部为 NULL
private static boolean allNull(
        Map<Integer, Long> nullValueCounts,
        Map<Integer, Long> valueCounts,
        Types.NestedField field) {

    if (field.isRequired()) {
        return false;  // 必填字段不可能全 NULL
    }

    if (nullValueCounts == null || valueCounts == null) {
        return false;  // 无法确定
    }

    Long nullValueCount = nullValueCounts.get(field.fieldId());
    Long valueCount = valueCounts.get(field.fieldId());

    if (nullValueCount == null || valueCount == null) {
        return false;
    }

    return nullValueCount.equals(valueCount);  // NULL 数 = 总数
}

// 判断是否全部非 NULL
private static boolean allNonNull(Map<Integer, Long> nullValueCounts, Types.NestedField field) {
    if (field.isRequired()) {
        return true;  // 必填字段全是非 NULL
    }

    if (nullValueCounts == null) {
        return false;  // 无法确定
    }

    Long nullValueCount = nullValueCounts.get(field.fieldId());
    return nullValueCount != null && nullValueCount <= 0;
}
```

#### EqualityDeleteFile 缓存优化

```java
// 源码: DeleteFileIndex.java
private static class EqualityDeleteFile {
    private final PartitionSpec spec;
    private final DeleteFile wrapped;
    private final long applySequenceNumber;

    // 延迟计算并缓存的字段
    private volatile List<Types.NestedField> equalityFields = null;
    private volatile Map<Integer, Object> convertedLowerBounds = null;
    private volatile Map<Integer, Object> convertedUpperBounds = null;

    EqualityDeleteFile(PartitionSpec spec, DeleteFile file) {
        this.spec = spec;
        this.wrapped = file;
        // Equality Delete 应用于 seq - 1 之前的数据
        this.applySequenceNumber = wrapped.dataSequenceNumber() - 1;
    }

    public List<Types.NestedField> equalityFields() {
        if (equalityFields == null) {
            synchronized (this) {
                if (equalityFields == null) {
                    List<Types.NestedField> fields = Lists.newArrayList();
                    for (int id : wrapped.equalityFieldIds()) {
                        Types.NestedField field = spec.schema().findField(id);
                        fields.add(field);
                    }
                    this.equalityFields = fields;
                }
            }
        }
        return equalityFields;
    }

    // 边界值转换缓存
    private Map<Integer, Object> lowerBounds() {
        if (convertedLowerBounds == null) {
            synchronized (this) {
                if (convertedLowerBounds == null) {
                    this.convertedLowerBounds = convertBounds(wrapped.lowerBounds());
                }
            }
        }
        return convertedLowerBounds;
    }
}
```

#### 剪枝效果示例

```
场景: 删除 user_id IN (100, 200, 300)

EqualityDeleteFile 统计:
  - equalityFieldIds: [1]  (user_id)
  - lowerBounds: {1: 100}
  - upperBounds: {1: 300}
  - nullValueCounts: {1: 0}

DataFile 1 统计:
  - lowerBounds: {1: 1}
  - upperBounds: {1: 50}
  → canContainEqDeletesForFile() = false (范围不重叠) ✓ 跳过!

DataFile 2 统计:
  - lowerBounds: {1: 150}
  - upperBounds: {1: 250}
  → canContainEqDeletesForFile() = true (范围重叠) → 需要检查

DataFile 3 统计:
  - nullValueCounts: {1: 1000}
  - valueCounts: {1: 1000}  (全 NULL)
  → canContainEqDeletesForFile() = false (全 NULL vs 非 NULL) ✓ 跳过!
```

**深入理解**: Range Overlap 剪枝的效果取决于数据分布。如果数据按主键排序写入,则每个 DataFile 的范围较窄,剪枝效果显著。反之,如果数据随机分布,大多数文件都会与删除范围重叠,剪枝效果有限。

#### 面试加分话术

> "canContainEqDeletesForFile() 实现了四层剪枝: (1) NULL 值相交检测 (2) 全 NULL vs 全非 NULL 互斥 (3) 范围不重叠检测 (4) 嵌套类型保守处理。关键是 EqualityDeleteFile 会缓存转换后的边界值,避免每次比较都做 ByteBuffer 反序列化。在数据按主键排序的场景下,这个剪枝可以跳过 90%+ 的 DataFile。"

#### 30 秒速记（简版）

- 结论先行：Equality Delete 需要在读取时与每条记录的主键进行匹配,开销较大。Iceberg 通过**统计信息剪枝**,在文件级别快速判断是否可能包含匹配。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:140`，再联动 `data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java:170`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:140`
- `data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java:170`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:137-143`**
```java
    return deleteFiles;
  }

  DeleteFile[] forEntry(ManifestEntry<DataFile> entry) {
    return forDataFile(entry.dataSequenceNumber(), entry.file());
  }

```

**片段 2：`data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java:167-173`**
```java
   * @return a position delete index for the provided data file path
   */
  @Override
  public PositionDeleteIndex loadPositionDeletes(
      Iterable<DeleteFile> deleteFiles, CharSequence filePath) {
    if (ContentFileUtil.containsSingleDV(deleteFiles)) {
      DeleteFile dv = Iterables.getOnlyElement(deleteFiles);
```

---
<a id="q-9-3"></a>
### 9.3 Deletion Vector 与 Roaring Bitmap 实现

**🟡 问题: Iceberg V3 引入了 Deletion Vector (DV) 作为更高效的行级删除方案。请说明 DV 的 Roaring Bitmap 实现原理。**

**答案:**

**Deletion Vector (DV)** 是 Iceberg V3 引入的新特性,使用 **Roaring Bitmap** 高效存储被删除行的位置,相比 Position Delete 文件大幅减少存储和读取开销。

#### DV vs Position Delete 对比

| 特性 | Position Delete | Deletion Vector |
|------|-----------------|-----------------|
| **存储格式** | Parquet/Avro 文件 | Puffin 文件内嵌 |
| **内容** | (file_path, pos) 对 | Roaring Bitmap |
| **关联方式** | 分区/路径索引 | 直接引用 DataFile |
| **压缩效率** | 中等 | 极高(位图压缩) |
| **查找复杂度** | O(n) 遍历 | O(1) 位图查找 |
| **合并开销** | 高(文件合并) | 低(位图 OR) |
| **支持版本** | V2+ | V3+ |

#### Roaring Bitmap 原理

Roaring Bitmap 是一种**混合压缩位图**,根据数据密度自动选择最优存储方式:

```
64位位置 → 32位 key (高4字节) + 32位 position (低4字节)

┌─────────────────────────────────────────────────────────────┐
│                   RoaringPositionBitmap                     │
├─────────────────────────────────────────────────────────────┤
│  bitmaps[]: RoaringBitmap[]                                 │
│                                                             │
│  key=0: RoaringBitmap for positions 0 ~ 4,294,967,295       │
│  key=1: RoaringBitmap for positions 4,294,967,296 ~ ...     │
│  ...                                                        │
└─────────────────────────────────────────────────────────────┘

每个 RoaringBitmap 内部:
┌─────────────────────────────────────────────────────────────┐
│  Container 类型根据密度自动选择:                             │
│                                                             │
│  1. Array Container (稀疏): 存储 < 4096 个位置              │
│     [100, 200, 300, 500, 1000, ...]                         │
│     空间: 2 * count 字节                                    │
│                                                             │
│  2. Bitmap Container (密集): 存储 >= 4096 个位置            │
│     [0,1,1,0,1,1,1,0,0,1,...]  (65536 位)                  │
│     空间: 固定 8KB                                          │
│                                                             │
│  3. Run Container (连续): 存储连续范围                       │
│     [(start1, length1), (start2, length2), ...]             │
│     空间: 4 * runs 字节                                     │
└─────────────────────────────────────────────────────────────┘
```

#### RoaringPositionBitmap 实现

```java
// 源码: core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java
class RoaringPositionBitmap {
    static final long MAX_POSITION = toPosition(Integer.MAX_VALUE - 1, Integer.MIN_VALUE);

    private RoaringBitmap[] bitmaps;

    /**
     * 设置一个位置为已删除
     */
    public void set(long pos) {
        validatePosition(pos);
        int key = key(pos);           // 高32位作为 key
        int pos32Bits = pos32Bits(pos); // 低32位作为位置
        allocateBitmapsIfNeeded(key + 1);
        bitmaps[key].add(pos32Bits);
    }

    /**
     * 检查位置是否已删除
     */
    public boolean contains(long pos) {
        validatePosition(pos);
        int key = key(pos);
        int pos32Bits = pos32Bits(pos);
        return key < bitmaps.length && bitmaps[key].contains(pos32Bits);
    }

    /**
     * 返回已删除行数
     */
    public long cardinality() {
        long cardinality = 0L;
        for (RoaringBitmap bitmap : bitmaps) {
            cardinality += bitmap.getLongCardinality();
        }
        return cardinality;
    }

    /**
     * 应用 Run-Length Encoding 优化
     */
    public boolean runLengthEncode() {
        boolean changed = false;
        for (RoaringBitmap bitmap : bitmaps) {
            changed |= bitmap.runOptimize();
        }
        return changed;
    }
}
```

#### 64位位置到32位的映射

```java
// 提取高32位作为 key
private static int key(long pos) {
    return (int) (pos >> 32);
}

// 提取低32位作为32位位置
private static int pos32Bits(long pos) {
    return (int) pos;
}

// 合并为64位位置
private static long toPosition(int key, int pos32Bits) {
    return (((long) key) << 32) | (((long) pos32Bits) & 0xFFFFFFFFL);
}
```

**示例**:
```
64位位置: 4,294,967,396 (即 2^32 + 100)

key = 4,294,967,396 >> 32 = 1
pos32Bits = 4,294,967,396 & 0xFFFFFFFF = 100

→ 存储在 bitmaps[1] 中的位置 100
```

#### 序列化格式

```java
// 源码: RoaringPositionBitmap.java
public void serialize(ByteBuffer buffer) {
    validateByteOrder(buffer);  // 必须 Little-Endian

    // 1. 写入 bitmap 数量 (8字节)
    buffer.putLong(bitmaps.length);

    // 2. 写入每个 bitmap
    for (int key = 0; key < bitmaps.length; key++) {
        buffer.putInt(key);              // key (4字节)
        bitmaps[key].serialize(buffer);  // Roaring 标准格式
    }
}

public static RoaringPositionBitmap deserialize(ByteBuffer buffer) {
    validateByteOrder(buffer);

    int remainingBitmapCount = readBitmapCount(buffer);
    List<RoaringBitmap> bitmaps = Lists.newArrayListWithExpectedSize(remainingBitmapCount);

    int lastKey = -1;
    while (remainingBitmapCount > 0) {
        int key = readKey(buffer, lastKey);

        // 填充稀疏间隙
        while (lastKey < key - 1) {
            bitmaps.add(new RoaringBitmap());
            lastKey++;
        }

        RoaringBitmap bitmap = readBitmap(buffer);
        bitmaps.add(bitmap);

        lastKey = key;
        remainingBitmapCount--;
    }

    return new RoaringPositionBitmap(bitmaps.toArray(EMPTY_BITMAP_ARRAY));
}
```

#### DV 在 DeleteFileIndex 中的使用

```java
// 源码: DeleteFileIndex.java
private DeleteFile findDV(long seq, DataFile dataFile) {
    if (dvByPath == null) {
        return null;
    }

    // 直接通过路径查找 DV
    DeleteFile dv = dvByPath.get(dataFile.location());

    if (dv != null) {
        // 验证 sequence number 语义
        ValidationException.check(
            dv.dataSequenceNumber() >= seq,
            "DV data sequence number (%s) must be >= data file sequence number (%s)",
            dv.dataSequenceNumber(), seq);
    }

    return dv;
}
```

#### 压缩效率对比

```
场景: 删除 DataFile 中的 10,000 行

Position Delete 文件:
  - 10,000 条 (file_path, pos) 记录
  - file_path 平均 100 字节
  - 预估大小: 10,000 * (100 + 8) ≈ 1MB

Deletion Vector (稀疏):
  - 10,000 个位置使用 Array Container
  - 预估大小: 8 + 4 + 10,000 * 2 ≈ 20KB (50x 压缩)

Deletion Vector (连续范围):
  - 假设删除 [0, 10000) 连续范围
  - 使用 Run Container: (0, 10000) 单个 run
  - 预估大小: 8 + 4 + 4 = 16 字节 (62,500x 压缩!)
```

#### 为何选择 Roaring Bitmap

| 方案 | 优点 | 缺点 |
|------|------|------|
| **BitSet** | 简单,O(1)查找 | 固定空间,稀疏场景浪费 |
| **HashSet** | 灵活 | 内存开销大,序列化慢 |
| **Sorted Array** | 紧凑 | 查找 O(log n),合并慢 |
| **Roaring Bitmap** | 自适应压缩,O(1)查找,快速合并 | 实现复杂 |

**深入理解**: Roaring Bitmap 的核心创新是**自适应 Container 选择**。对于删除操作的典型模式(批量删除、范围删除),Run Container 可以实现极致压缩。这使得 DV 在大规模删除场景下比 Position Delete 高效数个数量级。

#### 面试加分话术

> "Iceberg V3 的 Deletion Vector 使用 Roaring Bitmap 存储被删除行位置。核心设计是64位位置分解为32位 key + 32位 position,每个 key 对应一个独立的 Roaring Bitmap。Roaring 的三种 Container(Array/Bitmap/Run)根据密度自适应选择:稀疏用 Array 省空间,密集用 Bitmap 保证 O(1) 查找,连续范围用 Run 实现极致压缩。在批量删除场景下,DV 比 Position Delete 小 50-60000 倍。"

#### 高频追问

**Q: DV 和 Position Delete 可以共存吗？**

**A**:
- V2 表: 只能使用 Position Delete
- V3 表: 新写入使用 DV,可以读取旧的 Position Delete
- 升级路径: 通过 Compaction 将 Position Delete 转换为 DV
- DeleteFileIndex 会同时索引两者,forDataFile() 优先返回 DV

#### 30 秒速记（简版）

- 结论先行：Roaring Bitmap 是一种**混合压缩位图**,根据数据密度自动选择最优存储方式:。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java:1`，再联动 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:140`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java:1`
- `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:140`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:137-143`**
```java
    return deleteFiles;
  }

  DeleteFile[] forEntry(ManifestEntry<DataFile> entry) {
    return forDataFile(entry.dataSequenceNumber(), entry.file());
  }

```

---

## 十、面试必杀技 - 能让你脱颖而出的源码细节

> 本章收集了一些**不太容易被发现但非常有深度**的源码细节。在面试中提及这些内容,可以展示你对 Iceberg 的深入理解,让面试官眼前一亮。
<a id="q-10-1"></a>
### 10.1 Flink CommitDeltaTxn 为何不合并事务

**🔴 问题: 在 Flink 写入 Iceberg 时,如果存在 Delete 文件,IcebergFilesCommitter 会逐 checkpoint 提交而不是合并成一个事务。这是为什么？**

**答案:**

这是 Iceberg Flink Sink 中一个非常精妙的设计,直接关系到 **Equality Delete 的 Sequence Number 语义正确性**。

#### 源码分析

```java
// 源码: flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java
private void commitDeltaTxn(
        NavigableMap<Long, WriteResult> pendingResults,
        CommitSummary summary,
        String newFlinkJobId,
        String operatorId,
        long checkpointId) {

    if (summary.deleteFilesCount() == 0) {
        // 无 Delete 文件: 可以合并成单个 AppendFiles 事务
        AppendFiles appendFiles = table.newAppend().scanManifestsWith(workerPool);
        for (WriteResult result : pendingResults.values()) {
            Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
        }
        commitOperation(appendFiles, summary, "append", newFlinkJobId, operatorId, checkpointId);

    } else {
        // 有 Delete 文件: 必须逐 checkpoint 提交!
        for (Map.Entry<Long, WriteResult> e : pendingResults.entrySet()) {
            // ⚠️ 关键注释: 不合并的原因
            // We don't commit the merged result into a single transaction because
            // for the sequential transaction txn1 and txn2, the equality-delete
            // files of txn2 are required to be applied to data files from txn1.
            // Committing the merged one will lead to the incorrect delete semantic.

            WriteResult result = e.getValue();
            RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);
            Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
            Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
            commitOperation(rowDelta, summary, "rowDelta", newFlinkJobId, operatorId, e.getKey());
        }
    }
}
```

#### Sequence Number 语义深度解析

```
场景: Upsert 模式下,同一主键先 INSERT 后 DELETE

Checkpoint 1 (seq=100):
  - DataFile: {id=1, name="Alice"}
  - DeleteFile: (无)

Checkpoint 2 (seq=101):
  - DataFile: {id=1, name="Bob"}  (更新后的值)
  - DeleteFile: id=1 的 Equality Delete (删除旧值)

如果合并成单个事务提交:
┌─────────────────────────────────────────────────────────────┐
│ 单个事务: seq=101                                           │
│                                                             │
│ DataFile 1: {id=1, name="Alice"}  seq=101                  │
│ DataFile 2: {id=1, name="Bob"}    seq=101                  │
│ DeleteFile: id=1                  seq=101                  │
│                                                             │
│ 问题: Equality Delete 的 seq=101                           │
│       只删除 seq < 101 的数据                               │
│       但 DataFile 1 的 seq 也是 101!                        │
│       → Equality Delete 无法删除 DataFile 1 中的 Alice     │
│       → 结果: 同时存在 Alice 和 Bob (数据重复!)             │
└─────────────────────────────────────────────────────────────┘

正确做法: 逐 checkpoint 提交
┌─────────────────────────────────────────────────────────────┐
│ 事务1: seq=100                                              │
│ DataFile 1: {id=1, name="Alice"}  seq=100                  │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ 事务2: seq=101                                              │
│ DataFile 2: {id=1, name="Bob"}    seq=101                  │
│ DeleteFile: id=1                  seq=101                  │
│                                                             │
│ Equality Delete seq=101 删除 seq < 101 的数据              │
│ → DataFile 1 (seq=100) 中的 Alice 被删除 ✓                 │
│ → DataFile 2 (seq=101) 中的 Bob 保留 ✓                     │
│ → 结果正确: 只有 Bob                                        │
└─────────────────────────────────────────────────────────────┘
```

#### Equality Delete 的 Apply Sequence Number

```java
// 源码: DeleteFileIndex.java (EqualityDeleteFile 内部类)
EqualityDeleteFile(PartitionSpec spec, DeleteFile file) {
    this.spec = spec;
    this.wrapped = file;
    // 关键: Equality Delete 的应用序列号 = 数据序列号 - 1
    this.applySequenceNumber = wrapped.dataSequenceNumber() - 1;
}
```

**语义**: Equality Delete 文件的 `dataSequenceNumber` 为 N,则它**只删除** `sequenceNumber < N` 的数据文件中匹配的行。

```
Equality Delete (dataSeq=101)
  ↓ 应用于
DataFile (seq=100) → 被删除 ✓
DataFile (seq=101) → 不受影响 (同一事务的数据)
DataFile (seq=102) → 不受影响 (更新的数据)
```

#### 为什么 Position Delete 不需要分开提交？

Position Delete 通过 `(file_path, pos)` 精确指定被删除的行,与 Sequence Number 无关:

```java
// Position Delete 直接引用文件路径和行号
// 不依赖 Sequence Number 语义
positionDelete = {
    file_path: "s3://bucket/data/file1.parquet",
    pos: 42
}
```

因此 Position Delete 可以在同一事务中与数据文件一起提交。

#### 设计权衡

| 方案 | 事务数 | 正确性 | 性能 |
|------|--------|--------|------|
| 合并提交 | 1 | ❌ 错误 | 高 |
| 逐 checkpoint 提交 | N | ✅ 正确 | 中 |
| 逐文件提交 | M (M >> N) | ✅ 正确 | 低 |

Iceberg Flink Sink 选择了**逐 checkpoint 提交**作为折中方案。

**深入理解**: 这个设计细节揭示了 Iceberg Sequence Number 机制的精髓。Sequence Number 不仅用于快照隔离,还用于 Equality Delete 的语义控制。理解这一点,才能真正理解 Iceberg 的 MVCC 实现。

#### 面试加分话术

> "Flink IcebergFilesCommitter 在有 Delete 文件时必须逐 checkpoint 提交,原因是 Equality Delete 的 Sequence Number 语义: Delete 文件只删除 seq < N 的数据。如果合并提交,所有文件的 seq 相同,Delete 无法删除同一事务中的旧数据,导致 Upsert 语义错误。这是我在阅读源码时发现的一个非常精妙的设计。"

#### 🔥 高频追问

**Q: 如果不使用 Equality Delete（只用 Position Delete），可以合并事务吗？**

**A**: **可以合并**，这也正是源码中的判断逻辑:
```java
// IcebergFilesCommitter.java
private boolean shouldMerge(NavigableMap<Long, DeltaManifests> pendingResults) {
    // 只有纯数据追加(无 Delete 文件)才能合并
    return pendingResults.values().stream()
        .allMatch(delta -> delta.deleteFiles().isEmpty());
}
```
原因:
1. **Position Delete 依赖文件路径+行号**，不依赖 Sequence Number 比较
2. **纯追加场景**没有语义冲突问题
3. 合并提交可以减少 snapshot 数量，提升元数据操作效率

这也说明 Flink Upsert 模式性能略低于纯 Append 模式的原因之一。

#### 30 秒速记（简版）

- 结论先行：这是 Iceberg Flink Sink 中一个非常精妙的设计,直接关系到 **Equality Delete 的 Sequence Number 语义正。
- 主链路：先定位 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:1`，再联动 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:1`
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`




#### 源码关键片段

**片段 1：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:1-4`**
```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
```

**片段 2：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:92-98`**
```java
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot = Maps.newHashMap();
```

---
<a id="q-10-2"></a>
### 10.2 IcebergFilesCommitter 的 NavigableMap 设计

**🟠 问题: IcebergFilesCommitter 使用 NavigableMap 来管理待提交的 checkpoint 数据。这个数据结构选择有什么特殊考量？**

**答案:**

`NavigableMap<Long, byte[]>` 的选择是为了处理 **Flink Checkpoint 乱序完成** 和 **故障恢复** 场景。

#### 数据结构定义

```java
// 源码: IcebergFilesCommitter.java
// A sorted map to maintain the completed data files for each pending checkpointId
// (which have not been committed to iceberg table).
private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();
```

**为什么是 NavigableMap？**

```
NavigableMap 提供的关键方法:
1. headMap(checkpointId, true)  → 获取 <= checkpointId 的所有条目
2. tailMap(checkpointId, false) → 获取 > checkpointId 的所有条目
3. 自动按 key 排序 (TreeMap 实现)
```

#### 场景1: Checkpoint 乱序完成

```
Flink 可能出现以下事件序列:
1. snapshotState(ckpId=1)    → dataFilesPerCheckpoint = {1: [file0, file1]}
2. snapshotState(ckpId=2)    → dataFilesPerCheckpoint = {1: [...], 2: [file2]}
3. notifyCheckpointComplete(ckpId=2)  → 先收到 ckp2 完成!
4. notifyCheckpointComplete(ckpId=1)  → 后收到 ckp1 完成

处理逻辑:
```

```java
// 源码: IcebergFilesCommitter.java
@Override
public void notifyCheckpointComplete(long checkpointId) throws Exception {
    // 关键: 只处理比已提交 checkpoint 更新的
    if (checkpointId > maxCommittedCheckpointId) {
        LOG.info("Checkpoint {} completed. Attempting commit.", checkpointId);

        // 使用 headMap 获取所有 <= checkpointId 的数据
        commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, operatorUniqueId, checkpointId);

        this.maxCommittedCheckpointId = checkpointId;
    } else {
        // 跳过旧的 checkpoint 完成通知
        LOG.info("Skipping committing checkpoint {}. {} is already committed.",
                 checkpointId, maxCommittedCheckpointId);
    }
}
```

```
步骤3执行时:
  - checkpointId=2 > maxCommittedCheckpointId=-1 ✓
  - headMap(2, true) = {1: [...], 2: [...]}
  - 同时提交 ckp1 和 ckp2 的数据
  - maxCommittedCheckpointId = 2

步骤4执行时:
  - checkpointId=1 < maxCommittedCheckpointId=2
  - 跳过! (数据已在步骤3提交)
```

#### 场景2: 故障恢复

```java
// 源码: IcebergFilesCommitter.java
@Override
public void initializeState(StateInitializationContext context) throws Exception {
    if (context.isRestored()) {
        // 从 state 恢复 checkpointsState
        String restoredFlinkJobId = jobIdIterable.iterator().next();

        // 获取已提交的最大 checkpoint id
        this.maxCommittedCheckpointId = SinkUtil.getMaxCommittedCheckpointId(
            table, restoredFlinkJobId, operatorUniqueId, branch);

        // 关键: 使用 tailMap 获取未提交的数据
        NavigableMap<Long, byte[]> uncommittedDataFiles =
            Maps.newTreeMap(checkpointsState.get().iterator().next())
                .tailMap(maxCommittedCheckpointId, false);  // > maxCommittedCheckpointId

        if (!uncommittedDataFiles.isEmpty()) {
            // 提交所有未提交的数据
            long maxUncommittedCheckpointId = uncommittedDataFiles.lastKey();
            commitUpToCheckpoint(uncommittedDataFiles, restoredFlinkJobId,
                                 operatorUniqueId, maxUncommittedCheckpointId);
        }
    }
}
```

```
故障恢复场景:
恢复前 state: {1: [file0], 2: [file1], 3: [file2], 4: [file3]}
表中已提交: checkpoint 2 (通过快照 summary 中的 max-committed-checkpoint-id 判断)

恢复逻辑:
1. maxCommittedCheckpointId = 2
2. tailMap(2, false) = {3: [file2], 4: [file3]}  (未提交的)
3. commitUpToCheckpoint({3: [...], 4: [...]}, ..., 4)
4. 数据恢复完成,无丢失!
```

#### commitUpToCheckpoint 的 headMap 使用

```java
// 源码: IcebergFilesCommitter.java
private void commitUpToCheckpoint(
        NavigableMap<Long, byte[]> deltaManifestsMap,
        String newFlinkJobId,
        String operatorId,
        long checkpointId) throws IOException {

    // 获取 <= checkpointId 的所有待提交数据
    NavigableMap<Long, byte[]> pendingMap = deltaManifestsMap.headMap(checkpointId, true);

    // 遍历并提交
    for (Map.Entry<Long, byte[]> e : pendingMap.entrySet()) {
        // ... 处理每个 checkpoint 的数据
    }

    // 提交后清理
    pendingMap.clear();  // 自动从 deltaManifestsMap 中移除
}
```

#### NavigableMap vs HashMap 对比

| 特性 | NavigableMap (TreeMap) | HashMap |
|------|------------------------|---------|
| **范围查询** | O(log n) headMap/tailMap | 需遍历 O(n) |
| **有序遍历** | 按 key 排序 | 无序 |
| **插入/查找** | O(log n) | O(1) |
| **适用场景** | checkpoint 管理 | 简单 key-value |

**深入理解**: NavigableMap 的选择体现了对 Flink Checkpoint 机制的深刻理解。Checkpoint ID 单调递增的特性,配合 NavigableMap 的范围操作,实现了优雅的乱序处理和故障恢复。

#### 面试加分话术

> "IcebergFilesCommitter 使用 NavigableMap 管理 checkpoint 数据,核心是利用 headMap/tailMap 处理两个场景: (1) Checkpoint 乱序完成时,headMap(ckpId) 可以一次性提交所有 <= ckpId 的数据 (2) 故障恢复时,tailMap(maxCommitted) 可以精确找出未提交的 checkpoint 并重新提交。这比 HashMap 遍历过滤高效得多。"

#### 30 秒速记（简版）

- 结论先行：NavigableMap<Long, byte[]> 的选择是为了处理 **Flink Checkpoint 乱序完成** 和 **故障恢复** 场景。
- 主链路：先定位 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`，再联动 `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:280`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`
- `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:280`




#### 源码关键片段

**片段 1：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:92-98`**
```java
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot = Maps.newHashMap();
```

**片段 2：`core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:277-283`**
```java
    }
  }

  protected void validateNewDeleteFile(DeleteFile file) {
    Preconditions.checkNotNull(file, "Invalid delete file: null");
    switch (formatVersion()) {
      case 1:
```

---
<a id="q-10-3"></a>
### 10.3 Iceberg 为何选择 Roaring Bitmap

**🟡 问题: Iceberg 在多处使用 Roaring Bitmap,包括 Deletion Vector 和 Position Delete 缓存。相比其他位图实现,Roaring Bitmap 有什么优势？**

**答案:**

Roaring Bitmap 是 Iceberg 选择的**核心位图数据结构**,在存储效率和查询性能之间取得了最佳平衡。

#### Roaring Bitmap 三种 Container

```
┌─────────────────────────────────────────────────────────────┐
│                  Roaring Bitmap Container 选择              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  每个 Container 管理 65536 (2^16) 个位置                    │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ Array Container (稀疏,< 4096 元素)                    │ │
│  │                                                       │ │
│  │ 存储: sorted short[] 数组                            │ │
│  │ 空间: 2 * cardinality 字节                           │ │
│  │ 查找: 二分查找 O(log n)                              │ │
│  │                                                       │ │
│  │ 示例: [100, 200, 500, 1000]                          │ │
│  │ 空间: 4 * 2 = 8 字节                                 │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ Bitmap Container (密集,>= 4096 元素)                  │ │
│  │                                                       │ │
│  │ 存储: 固定 1024 个 long (65536 位)                   │ │
│  │ 空间: 固定 8KB                                       │ │
│  │ 查找: O(1) 位运算                                    │ │
│  │                                                       │ │
│  │ 当 cardinality >= 4096 时:                           │ │
│  │ Array 空间 = 4096 * 2 = 8KB = Bitmap 空间           │ │
│  │ → 切换到 Bitmap 更划算                               │ │
│  └───────────────────────────────────────────────────────┘ │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ Run Container (连续范围)                              │ │
│  │                                                       │ │
│  │ 存储: (start, length) 对的数组                       │ │
│  │ 空间: 4 * runs 字节                                  │ │
│  │ 查找: 二分查找 O(log runs)                           │ │
│  │                                                       │ │
│  │ 示例: [0-999, 5000-5999]                             │ │
│  │ 表示: [(0, 1000), (5000, 1000)]                      │ │
│  │ 空间: 2 * 4 = 8 字节 (vs 2000 * 2 = 4KB Array)      │ │
│  └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

#### 自适应选择算法

```
                 cardinality
                     │
         ┌───────────┴───────────┐
         │                       │
    < 4096                  >= 4096
         │                       │
         ▼                       ▼
  ┌──────────────┐       ┌──────────────┐
  │    Array     │       │   Bitmap     │
  │  Container   │       │  Container   │
  └──────────────┘       └──────────────┘
         │                       │
         └───────────┬───────────┘
                     │
                runOptimize()
                     │
                     ▼
              连续范围多?
            ┌────┴────┐
            │         │
           是        否
            │         │
            ▼         ▼
     ┌──────────┐  保持原样
     │   Run    │
     │Container │
     └──────────┘
```

#### 与其他位图方案对比

| 方案 | 稀疏场景 | 密集场景 | 连续范围 | 合并效率 |
|------|---------|---------|---------|---------|
| **Java BitSet** | 差(固定空间) | 好 | 差 | 中 |
| **HashSet<Long>** | 好 | 差(内存大) | 差 | 差 |
| **Sorted Array** | 好 | 差 | 差 | 差(需合并排序) |
| **Roaring Bitmap** | 好(Array) | 好(Bitmap) | 极好(Run) | 极好(位运算) |

#### Iceberg 中的 Roaring 使用场景

**1. Deletion Vector (V3)**
```java
// 源码: RoaringPositionBitmap.java
class RoaringPositionBitmap {
    private RoaringBitmap[] bitmaps;  // 64位 → 32位 key + 32位 Roaring

    public void set(long pos) {
        int key = key(pos);
        int pos32Bits = pos32Bits(pos);
        bitmaps[key].add(pos32Bits);
    }

    public boolean contains(long pos) {
        int key = key(pos);
        int pos32Bits = pos32Bits(pos);
        return key < bitmaps.length && bitmaps[key].contains(pos32Bits);
    }
}
```

**2. Position Delete 内存缓存**
```java
// 读取时缓存被删除的位置
RoaringBitmap deletedPositions = new RoaringBitmap();
for (PositionDeleteRecord record : positionDeletes) {
    deletedPositions.add((int) record.pos());
}

// O(1) 检查是否被删除
boolean isDeleted = deletedPositions.contains(rowPosition);
```

**3. 文件级别位图过滤**
```java
// 跳过已处理的文件
RoaringBitmap processedFiles = new RoaringBitmap();
for (DataFile file : files) {
    int fileId = hash(file.path());
    if (!processedFiles.contains(fileId)) {
        process(file);
        processedFiles.add(fileId);
    }
}
```

#### 性能基准测试

```
场景: 100 万个被删除的行位置

存储空间:
- Java BitSet: 125 KB (固定 1M 位)
- HashSet<Long>: ~40 MB (对象开销)
- Roaring (稀疏): ~2 MB
- Roaring (连续范围 [0, 1000000)): ~16 字节!

查找性能 (单次):
- Java BitSet: ~5 ns (位运算)
- HashSet<Long>: ~50 ns (哈希+equals)
- Roaring Array: ~100 ns (二分查找)
- Roaring Bitmap: ~10 ns (位运算)

合并性能 (两个 100 万位图 OR):
- Java BitSet: ~1 ms
- HashSet<Long>: ~500 ms
- Roaring: ~5 ms (SIMD 优化)
```

**深入理解**: Roaring Bitmap 的核心优势是**自适应**。Iceberg 的删除模式多样: 随机删除(Array 最优)、批量删除(Bitmap 最优)、范围删除(Run 最优)。Roaring 自动选择最佳存储,适应各种场景。

#### 面试加分话术

> "Roaring Bitmap 是 Iceberg 的核心位图选择,优势在于三种 Container 自适应: Array Container 用于稀疏场景(< 4096),Bitmap Container 用于密集场景(>= 4096),Run Container 用于连续范围(批量删除)。临界点 4096 是精心计算的: Array 空间 = 4096 * 2 字节 = Bitmap 固定 8KB。这种设计使 Roaring 在各种删除模式下都能保持最优性能。"

#### 30 秒速记（简版）

- 结论先行：Roaring Bitmap 是 Iceberg 选择的**核心位图数据结构**,在存储效率和查询性能之间取得了最佳平衡。
- 主链路：先定位 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`，再联动 `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:280`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`
- `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:280`




#### 源码关键片段

**片段 1：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:92-98`**
```java
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot = Maps.newHashMap();
```

**片段 2：`core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:277-283`**
```java
    }
  }

  protected void validateNewDeleteFile(DeleteFile file) {
    Preconditions.checkNotNull(file, "Invalid delete file: null");
    switch (formatVersion()) {
      case 1:
```

---
<a id="q-10-4"></a>
### 10.4 表格式版本演进 V1→V2→V3 核心变化

**🟠 问题: Iceberg 表格式从 V1 演进到 V3,每个版本引入了哪些核心特性？如何理解版本兼容性？**

**答案:**

Iceberg 表格式版本演进遵循**向前兼容**原则: 新版本读取器可以读取旧版本数据,但旧版本读取器无法理解新版本特性。

#### 版本演进时间线

```
┌─────────────────────────────────────────────────────────────┐
│                   Iceberg 表格式版本演进                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  V1 (2019)              V2 (2021)              V3 (2024)   │
│  ──────────────────────────────────────────────────────────│
│  │                      │                      │           │
│  │ 分析表基础格式        │ 行级删除             │ 扩展类型   │
│  │ - Parquet/Avro/ORC   │ - Position Delete   │ - 纳秒时间戳│
│  │ - 快照隔离           │ - Equality Delete   │ - Variant  │
│  │ - Schema 演进        │ - Sequence Number   │ - Geometry │
│  │ - 分区演进           │ - 写入器要求更严格   │ - DV (V3)  │
│  │                      │                      │           │
│  └──────────────────────┴──────────────────────┴───────────│
│                                                             │
│  V4 (开发中): First Row ID, 更多类型扩展                    │
└─────────────────────────────────────────────────────────────┘
```

#### V1: 分析表基础格式

```
核心特性:
- 不可变文件格式 (Parquet, Avro, ORC)
- 快照隔离 (Snapshot Isolation)
- Schema 演进 (列重命名、添加、删除、重排序、类型提升)
- 分区演进 (无需重写数据)
- 时间旅行查询

限制:
- 无法高效删除单行 (必须重写整个文件)
- 无 Sequence Number (无法支持 Equality Delete)
```

#### V2: 行级删除

```
新增特性:
1. Position Delete
   - 格式: (file_path, pos)
   - 精确删除指定文件的指定行

2. Equality Delete
   - 格式: 包含删除列值的记录
   - 删除所有匹配指定值的行
   - 依赖 Sequence Number 确定应用范围

3. Sequence Number
   - 每个快照分配唯一递增序号
   - Equality Delete 仅应用于 seq < N 的数据

4. 更严格的写入器要求
   - 必须写入列统计信息
   - 必须设置正确的 Sequence Number
```

```java
// V2 特性: Sequence Number 控制 Equality Delete 应用范围
// 源码: DeleteFileIndex.java
EqualityDeleteFile(PartitionSpec spec, DeleteFile file) {
    this.applySequenceNumber = wrapped.dataSequenceNumber() - 1;
    // Equality Delete (seq=N) 只删除 seq < N 的数据
}
```

#### V3: 扩展类型和能力

```
新增特性:
1. 新数据类型
   - nanosecond timestamp(tz): 纳秒精度时间戳
   - unknown: 未知类型占位符
   - variant: 半结构化数据 (类似 JSON)
   - geometry/geography: 地理空间类型

2. Deletion Vector (DV)
   - 使用 Roaring Bitmap 存储被删除行位置
   - 存储在 Puffin 文件中
   - 相比 Position Delete 大幅减少存储

3. 行级特性增强
   - First Row ID (V4 预览)
   - 更精确的行级追踪

4. 元数据增强
   - 更丰富的统计信息
   - 改进的元数据编码
```

```java
// V3 特性: 根据版本选择 Position Delete 或 DV
// 源码: MergingSnapshotProducer.java
protected void validateNewDeleteFile(DeleteFile file) {
    switch (formatVersion()) {
        case 1:
            throw new IllegalArgumentException("Deletes are supported in V2 and above");
        case 2:
            // V2: 必须使用传统 Position Delete 文件
            Preconditions.checkArgument(
                file.content() == FileContent.EQUALITY_DELETES || !ContentFileUtil.isDV(file),
                "Must not use DVs for position deletes in V2: %s", ...);
            break;
        case 3:
        case 4:
            // V3+: Position Delete 必须使用 DV
            Preconditions.checkArgument(
                file.content() == FileContent.EQUALITY_DELETES || ContentFileUtil.isDV(file),
                "Must use DVs for position deletes in V%s: %s", formatVersion(), ...);
            break;
    }
}
```

#### 版本兼容性矩阵

| 写入版本 | V1 读取器 | V2 读取器 | V3 读取器 |
|---------|----------|----------|----------|
| V1 | ✅ | ✅ | ✅ |
| V2 | ❌ (无法理解 Delete) | ✅ | ✅ |
| V3 | ❌ | ❌ (无法理解 DV/新类型) | ✅ |

#### 升级路径

```sql
-- 查看当前版本
SELECT * FROM prod.db.table.metadata_log_entries;

-- 升级到 V2
ALTER TABLE prod.db.table SET TBLPROPERTIES ('format-version' = '2');

-- 升级到 V3 (需要支持的引擎版本)
ALTER TABLE prod.db.table SET TBLPROPERTIES ('format-version' = '3');
```

**注意**: 版本升级是**单向**的,无法降级!

#### 版本选择建议

| 场景 | 推荐版本 | 原因 |
|------|---------|------|
| 只追加数据 | V1 | 简单,兼容性最好 |
| 需要 UPDATE/DELETE | V2 | 支持行级删除 |
| 大量删除操作 | V3 | DV 压缩效率高 |
| 需要新数据类型 | V3 | Variant, 纳秒时间戳 |
| 最大兼容性 | V2 | 主流引擎都支持 |

**深入理解**: Iceberg 的版本演进非常保守 — 只有在社区广泛认可且实现成熟后才会正式发布新版本。V3 在 2024 年才正式发布,距离 V2 已过去 3 年。这种谨慎态度确保了生产环境的稳定性。

#### 面试加分话术

> "Iceberg 表格式版本演进遵循向前兼容原则: V1 提供分析表基础,V2 引入行级删除和 Sequence Number,V3 添加 Deletion Vector 和新数据类型。关键设计是 V2 的 Sequence Number — 它不仅用于快照隔离,还控制 Equality Delete 的应用范围(只删除 seq < N 的数据)。V3 的 DV 强制要求 Position Delete 使用 Roaring Bitmap,相比 V2 可减少 50-60000 倍存储。"

#### 30 秒速记（简版）

- 结论先行：Iceberg 表格式版本演进遵循**向前兼容**原则: 新版本读取器可以读取旧版本数据,但旧版本读取器无法理解新版本特性。
- 主链路：先定位 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`，再联动 `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:280`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:95`
- `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:280`




#### 源码关键片段

**片段 1：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java:92-98`**
```java
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot = Maps.newHashMap();
```

**片段 2：`core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:277-283`**
```java
    }
  }

  protected void validateNewDeleteFile(DeleteFile file) {
    Preconditions.checkNotNull(file, "Invalid delete file: null");
    switch (formatVersion()) {
      case 1:
```

---

## 十一、源码级高阶追问（新增12题）

> 本章面向“二面/三面追问场景”。每题统一为 **总-分-总 + 追问与反问**，并附源码锚点，帮助你把答案从“知道”升级到“能主导面试节奏”。
<a id="q-11-1"></a>
### 11.1 DeleteFilter 为什么先 Position 再 Equality

**🔴 问题: `DeleteFilter` 为什么先应用 position delete，再应用 equality delete？顺序可交换吗？**

**答案（总）:**

当前实现先按位置过滤，再按值过滤。语义上通常可交换，但性能上先做位置过滤更划算，因为 position 判定更轻量。

**答案（分）:**

1. 调用顺序在代码里是写死的: `applyEqDeletes(applyPosDeletes(records))`。  
   - 源码: `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177-179`
2. position 判定依赖 `PositionDeleteIndex#isDeleted(pos)`，是 O(1) 风格位点检查。  
   - 源码: `DeleteFilter.java:255-257`
3. equality 判定需要构建 `StructLikeSet` 和行投影，常数开销更高。  
   - 源码: `DeleteFilter.java:202-210`

**答案（总）:**

30 秒话术: **先用便宜条件快速减行，再用昂贵条件精筛，这是典型分层过滤设计。**

**追问与反问:**
- 追问: “如果 equality delete 远多于 position delete，还应该这个顺序吗？”
- 反问: “我们是否做过 delete 类型占比统计，来验证顺序对真实负载的收益？”

#### 30 秒速记（简版）

- 结论先行：当前实现先按位置过滤，再按值过滤。语义上通常可交换，但性能上先做位置过滤更划算，因为 position 判定更轻量。
- 主链路：先定位 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`，再联动 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:140`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`
- `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:140`




#### 源码关键片段

**片段 1：`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:174-180`**
```java
    return deleteLoader;
  }

  public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
  }

```

**片段 2：`core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:137-143`**
```java
    return deleteFiles;
  }

  DeleteFile[] forEntry(ManifestEntry<DataFile> entry) {
    return forDataFile(entry.dataSequenceNumber(), entry.file());
  }

```

---
<a id="q-11-2"></a>
### 11.2 `requiredSchema` 如何动态扩展以支持 Delete

**🟠 问题: 用户只查少量列，为什么读取时 Iceberg 可能补读更多列？**

**答案（总）:**

因为 delete 过滤需要额外字段。`fileProjection` 会在请求列之外补齐 `_pos` 和 equality 字段，保证删除语义正确。

**答案（分）:**

1. 没有 deletes 时直接返回 `requestedSchema`。  
   - 源码: `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:273-275`
2. 存在 position delete 且需要位置信息时补 `ROW_POSITION`。  
   - 源码: `DeleteFilter.java:277-280`
3. 对每个 equality delete 收集 `equalityFieldIds` 并补进投影。  
   - 源码: `DeleteFilter.java:282-307`
4. 只补“缺失字段”，不是无脑全量扩列。  
   - 源码: `DeleteFilter.java:286-292`

**答案（总）:**

一句话: **逻辑查询列不等于物理读取列，Iceberg 会做最小必要扩列来换语义正确。**

**追问与反问:**
- 追问: “这会不会抵消列裁剪收益？”
- 反问: “我们的 equality delete 是否集中在宽表高基数字段？这是 IO 放大的关键。”

#### 30 秒速记（简版）

- 结论先行：因为 delete 过滤需要额外字段。fileProjection 会在请求列之外补齐 _pos 和 equality 字段，保证删除语义正确。
- 主链路：先定位 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:273`，再联动 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:273`
- `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`




#### 源码关键片段

**片段 1：`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:270-276`**
```java
      List<DeleteFile> posDeletes,
      List<DeleteFile> eqDeletes,
      boolean needRowPosCol) {
    if (posDeletes.isEmpty() && eqDeletes.isEmpty()) {
      return requestedSchema;
    }

```

**片段 2：`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:174-180`**
```java
    return deleteLoader;
  }

  public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
  }

```

---
<a id="q-11-3"></a>
### 11.3 `DeleteFileIndex` 如何避免朴素 O(M*N)

**🔴 问题: 为什么 delete 匹配不是“每个 DataFile 扫所有 DeleteFile”？**

**答案（总）:**

Iceberg 先建索引再匹配。`ManifestGroup` 先构建 `DeleteFileIndex`，随后每个 entry 只查候选桶，不做全量笛卡尔遍历。

**答案（分）:**

1. 规划阶段先执行 `deleteIndexBuilder.build()`。  
   - 源码: `core/src/main/java/org/apache/iceberg/ManifestGroup.java:185`
2. 任务生成时按 entry 调 `ctx.deletes().forEntry(entry)`。  
   - 源码: `ManifestGroup.java:366`
3. `DeleteFileIndex` 内部按 `global / partition / path / dv` 分桶查找。  
   - 源码: `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:153-164`
4. equality 还会叠加统计边界判断，进一步缩小候选。  
   - 源码: `DeleteFileIndex.java:216-245`

**答案（总）:**

面试收口: **这是“候选集匹配”而不是“全集匹配”，复杂度下降来自索引分层。**

**追问与反问:**
- 追问: “那为什么有些表计划阶段依旧很慢？”
- 反问: “瓶颈是在 delete 文件数量，还是在统计缺失导致候选集无法收敛？”

#### 30 秒速记（简版）

- 结论先行：Iceberg 先建索引再匹配。ManifestGroup 先构建 DeleteFileIndex，随后每个 entry 只查候选桶，不做全量笛卡尔遍历。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/ManifestGroup.java:185`，再联动 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:153`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/ManifestGroup.java:185`
- `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:153`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/ManifestGroup.java:182-188`**
```java
                  return ResidualEvaluator.of(spec, filter, caseSensitive);
                });

    DeleteFileIndex deleteFiles = deleteIndexBuilder.scanMetrics(scanMetrics).build();

    boolean dropStats = ManifestReader.dropStats(columns);
    if (deleteFiles.hasEqualityDeletes()) {
```

**片段 2：`core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:150-156`**
```java
      return EMPTY_DELETES;
    }

    DeleteFile[] global = findGlobalDeletes(sequenceNumber, file);
    DeleteFile[] eqPartition = findEqPartitionDeletes(sequenceNumber, file);
    DeleteFile dv = findDV(sequenceNumber, file);
    if (dv != null && global == null && eqPartition == null) {
```

---
<a id="q-11-4"></a>
### 11.4 为什么 Equality Delete 要 `applySequenceNumber = dataSeq - 1`

**🔴 问题: 这个 `-1` 到底在保护什么语义？**

**答案（总）:**

它把 equality delete 的生效条件从“<=”改成了严格“<”，避免删到同序列的新写入数据，是并发正确性的关键细节。

**答案（分）:**

1. 构造 `EqualityDeleteFile` 时明确执行 `wrapped.dataSequenceNumber() - 1`。  
   - 源码: `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:814`
2. equality 过滤按 `applySequenceNumber` 排序比较。  
   - 源码: `DeleteFileIndex.java:725-727`, `741-745`
3. 推导后等价于 `deleteDataSeq > dataDataSeq`，自然规避同序列误删。

**答案（总）:**

一锤定音: **`-1` 不是实现小技巧，而是“严格前序可见”语义编码。**

**追问与反问:**
- 追问: “position delete 为什么不做 `-1`？”
- 反问: “我们是否对同批次写删混合场景做过回归用例？”

#### 30 秒速记（简版）

- 结论先行：它把 equality delete 的生效条件从“<=”改成了严格“<”，避免删到同序列的新写入数据，是并发正确性的关键细节。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:814`，再联动 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:814`
- `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/DeleteFileIndex.java:811-817`**
```java
    EqualityDeleteFile(PartitionSpec spec, DeleteFile file) {
      this.spec = spec;
      this.wrapped = file;
      this.applySequenceNumber = wrapped.dataSequenceNumber() - 1;
    }

    public DeleteFile wrapped() {
```

**片段 2：`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:174-180`**
```java
    return deleteLoader;
  }

  public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
  }

```

---
<a id="q-11-5"></a>
### 11.5 Position Delete 如何利用 metrics 推断 file-scoped

**🟠 问题: `referencedDataFile` 为空时，Iceberg 还能否只靠元数据判断作用范围？**

**答案（总）:**

可以。若 `file_path` 的 lower/upper bound 相等，源码直接把它判成 file-scoped，无需先读 delete 内容。

**答案（分）:**

1. equality delete 直接返回 null，不参与这条推断。  
   - 源码: `core/src/main/java/org/apache/iceberg/util/ContentFileUtil.java:63-66`
2. position delete 先看显式 `referencedDataFile()`。  
   - 源码: `ContentFileUtil.java:68-70`
3. 若未显式给出，则比较 `file_path` 的 lower/upper bound。  
   - 源码: `ContentFileUtil.java:72-84`
4. 上下界相等即返回单文件路径，否则保持“多文件未知”。  
   - 源码: `ContentFileUtil.java:84-87`

**答案（总）:**

面试收口: **Iceberg 先用统计推断，再决定是否读内容，这是典型“元数据先行”的成本优化。**

**追问与反问:**
- 追问: “如果写入器没写这类 bounds 怎么办？”
- 反问: “我们是否在数据质量巡检里覆盖了 delete 文件的关键统计完整性？”

#### 30 秒速记（简版）

- 结论先行：可以。若 file_path 的 lower/upper bound 相等，源码直接把它判成 file-scoped，无需先读 delete 内容。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/util/ContentFileUtil.java:63`，再联动 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/util/ContentFileUtil.java:63`
- `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/util/ContentFileUtil.java:60-66`**
```java
    }
  }

  public static CharSequence referencedDataFile(DeleteFile deleteFile) {
    if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
      return null;
    }
```

**片段 2：`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:174-180`**
```java
    return deleteLoader;
  }

  public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
  }

```

---
<a id="q-11-6"></a>
### 11.6 Spark Executor Delete 缓存的真实行为与边界

**🟠 问题: Spark 的 delete 缓存到底怎么开启、如何淘汰、有什么边界？**

**答案（总）:**

这是会话级配置驱动的 executor 缓存，不是 read option；淘汰机制是访问过期 + 权重上限，不是单一 LRU。

**答案（分）:**

1. 开关入口: `SparkReadConf#cacheDeleteFilesOnExecutors`。  
   - 源码: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkReadConf.java:335-344`
2. 配置键来自 `SparkSQLProperties` 会话参数。  
   - 源码: `SparkSQLProperties.java:79-96`
3. 缓存实现: `expireAfterAccess(timeout) + maximumWeight(maxTotalSize)`。  
   - 源码: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkExecutorCache.java:163-167`
4. 权重由 entry size 决定，超大对象可能导致低命中或不经济缓存。  
   - 源码: `SparkExecutorCache.java:187-188`

**答案（总）:**

一句话: **delete 缓存是“容量与时效受约束的机会主义优化”，不是万能开关。**

**追问与反问:**
- 追问: “为何缓存开了还是慢？”
- 反问: “我们的 delete 文件大小分布和重复访问度是否匹配缓存模型？”

#### 30 秒速记（简版）

- 结论先行：这是会话级配置驱动的 executor 缓存，不是 read option；淘汰机制是访问过期 + 权重上限，不是单一 LRU。
- 主链路：先定位 `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkReadConf.java:335`，再联动 `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkExecutorCache.java:163`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkReadConf.java:335`
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkExecutorCache.java:163`




#### 源码关键片段

**片段 1：`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkReadConf.java:332-338`**
```java
    return executorCacheEnabled() && executorCacheLocalityEnabledInternal();
  }

  public boolean cacheDeleteFilesOnExecutors() {
    return executorCacheEnabled() && cacheDeleteFilesOnExecutorsInternal();
  }

```

**片段 2：`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkExecutorCache.java:160-166`**
```java
  }

  private Cache<String, CacheValue> initState() {
    return Caffeine.newBuilder()
        .expireAfterAccess(timeout)
        .maximumWeight(maxTotalSize)
        .weigher((key, value) -> ((CacheValue) value).weight())
```

---
<a id="q-11-7"></a>
### 11.7 Flink 流式增量启动策略差异（旧 API vs FLIP-27）

**🔴 问题: 旧 SourceFunction 与 FLIP-27 在“首次启动”阶段有什么关键差异？**

**答案（总）:**

两条链路都支持增量，但首轮行为不同。旧 API 在 monitor 中首次 `copyWithSnapshotId`；FLIP-27 的 `TABLE_SCAN_THEN_INCREMENTAL` 会先做一次批量快照扫描再切增量。

**答案（分）:**

1. 旧 API 首轮分支: `lastSnapshotId == INIT` 时 `copyWithSnapshotId(snapshotId)`。  
   - 源码: `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:204-206`
2. 后续轮次再用 `copyWithAppendsBetween`。  
   - 源码: `StreamingMonitorFunction.java:207-210`
3. FLIP-27 在 `TABLE_SCAN_THEN_INCREMENTAL` 下显式先 `planIcebergSourceSplits(...copyWithSnapshotId...)`。  
   - 源码: `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/enumerator/ContinuousSplitPlannerImpl.java:170-176`
4. 其他策略（如 latest-exclusive）直接从定位点进入增量。  
   - 源码: `ContinuousSplitPlannerImpl.java:183-190`

**答案（总）:**

面试收口: **“增量读取”描述的是持续阶段，不代表首次冷启动一定轻量。**

**追问与反问:**
- 追问: “为何我设置流式后第一次运行仍读很多历史数据？”
- 反问: “我们明确过 starting strategy 吗？这是首轮资源画像的决定因素。”

#### 30 秒速记（简版）

- 结论先行：两条链路都支持增量，但首轮行为不同。旧 API 在 monitor 中首次 copyWithSnapshotId；FLIP-27 的 TABLE_SCAN_。
- 主链路：先定位 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:204`，再联动 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/enumerator/ContinuousSplitPlannerImpl.java:170`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:204`
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/enumerator/ContinuousSplitPlannerImpl.java:170`




#### 源码关键片段

**片段 1：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:201-207`**
```java
      long snapshotId = snapshot.snapshotId();

      ScanContext newScanContext;
      if (lastSnapshotId == INIT_LAST_SNAPSHOT_ID) {
        newScanContext = scanContext.copyWithSnapshotId(snapshotId);
      } else {
        snapshotId =
```

**片段 2：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/enumerator/ContinuousSplitPlannerImpl.java:167-173`**
```java
        scanContext.streamingStartingStrategy());
    List<IcebergSourceSplit> splits = Collections.emptyList();
    IcebergEnumeratorPosition toPosition;
    if (scanContext.streamingStartingStrategy()
        == StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL) {
      // do a batch table scan first
      splits =
```

---
<a id="q-11-8"></a>
### 11.8 `max-planning-snapshot-count` 如何节流并影响吞吐

**🟠 问题: 这个参数只影响 planner CPU，还是也影响消费节奏？**

**答案（总）:**

它不仅影响规划成本，还会改变每轮推进窗口，从而影响 split 发现批量、追赶速度和端到端延迟。

**答案（分）:**

1. 旧 API 用 `toSnapshotIdInclusive` 截断本轮终点。  
   - 源码: `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173-183`
2. FLIP-27 用 `toSnapshotInclusive` 做同类截断。  
   - 源码: `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/enumerator/ContinuousSplitPlannerImpl.java:89-102`
3. 截断后通过 position 继续推进，语义连续但节奏改变。  
   - 源码: `ContinuousSplitPlannerImpl.java:125-133`

**答案（总）:**

一句话: **这是“单轮批量度阀门”，本质是在实时性和稳态吞吐间做权衡。**

**追问与反问:**
- 追问: “为什么调小后系统更稳但追赶变慢？”
- 反问: “当前业务优先级是低延迟还是大积压快速回补？”

#### 30 秒速记（简版）

- 结论先行：它不仅影响规划成本，还会改变每轮推进窗口，从而影响 split 发现批量、追赶速度和端到端延迟。
- 主链路：先定位 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`，再联动 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/enumerator/ContinuousSplitPlannerImpl.java:89`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:173`
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/enumerator/ContinuousSplitPlannerImpl.java:89`




#### 源码关键片段

**片段 1：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java:170-176`**
```java
    }
  }

  private long toSnapshotIdInclusive(
      long lastConsumedSnapshotId, long currentSnapshotId, int maxPlanningSnapshotCount) {
    List<Long> snapshotIds =
        SnapshotUtil.snapshotIdsBetween(table, lastConsumedSnapshotId, currentSnapshotId);
```

**片段 2：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/enumerator/ContinuousSplitPlannerImpl.java:86-92`**
```java
    }
  }

  private Snapshot toSnapshotInclusive(
      Long lastConsumedSnapshotId, Snapshot currentSnapshot, int maxPlanningSnapshotCount) {
    // snapshots are in reverse order (latest snapshot first)
    List<Snapshot> snapshots =
```

---
<a id="q-11-9"></a>
### 11.9 Deletion Vector 读取路径与缓存策略为何不同于 position deletes

**🟠 问题: DV 与 position delete 在读取与缓存上有什么本质区别？**

**答案（总）:**

DV 走 Puffin 偏移读取、直接反序列化位图；position delete 则按 delete 文件生成可复用位置索引。两者 IO 形态不同，所以缓存策略也不同。

**答案（分）:**

1. `loadPositionDeletes` 检测到单个 DV 时走 `readDV` 分支。  
   - 源码: `data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java:170-176`
2. `readDV` 依赖 `contentOffset/contentSizeInBytes` 精确读取 blob。  
   - 源码: `BaseDeleteLoader.java:181-187`
3. 注释明确 DV 当前不走同类缓存路径，position delete 更适合缓存。  
   - 源码: `BaseDeleteLoader.java:154-163`
4. position delete 可缓存 `CharSequenceMap<PositionDeleteIndex>` 并按 data file 取索引。  
   - 源码: `BaseDeleteLoader.java:200-204`

**答案（总）:**

面试收口: **DV 是“定点位图读取”，position delete 是“可复用索引构建”，缓存收益模型不同。**

**追问与反问:**
- 追问: “既然 DV 更轻，为什么不统一缓存？”
- 反问: “我们 workload 的 split locality 如何？若本地复用低，缓存价值本来就有限。”

#### 30 秒速记（简版）

- 结论先行：DV 走 Puffin 偏移读取、直接反序列化位图；position delete 则按 delete 文件生成可复用位置索引。两者 IO 形态不同，所以缓。
- 主链路：先定位 `data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java:170`，再联动 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java:170`
- `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`




#### 源码关键片段

**片段 1：`data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java:167-173`**
```java
   * @return a position delete index for the provided data file path
   */
  @Override
  public PositionDeleteIndex loadPositionDeletes(
      Iterable<DeleteFile> deleteFiles, CharSequence filePath) {
    if (ContentFileUtil.containsSingleDV(deleteFiles)) {
      DeleteFile dv = Iterables.getOnlyElement(deleteFiles);
```

**片段 2：`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:174-180`**
```java
    return deleteLoader;
  }

  public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
  }

```

---
<a id="q-11-10"></a>
### 11.10 何时会强制读取更多统计列（Equality Delete 场景）

**🟠 问题: 为什么有些查询 manifest 规划时会“多读统计列”？**

**答案（总）:**

当存在 equality deletes 时，`ManifestGroup` 会主动保留 stats 列，用于后续候选裁剪，减少无效 delete 应用。

**答案（分）:**

1. 先构建 delete index，再判断 `hasEqualityDeletes()`。  
   - 源码: `core/src/main/java/org/apache/iceberg/ManifestGroup.java:185-188`
2. 命中后执行 `select(ManifestReader.withStatsColumns(columns))`。  
   - 源码: `ManifestGroup.java:188-190`
3. 这是“规划期多读元数据，执行期少做无效工作”的典型取舍。

**答案（总）:**

一句话: **多读统计是为了少做错事，不是 planner 失控。**

**追问与反问:**
- 追问: “这会不会让 planning 变慢？”
- 反问: “我们是否评估过 planner 增量成本与执行期节省成本的净收益？”

#### 30 秒速记（简版）

- 结论先行：当存在 equality deletes 时，ManifestGroup 会主动保留 stats 列，用于后续候选裁剪，减少无效 delete 应用。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/ManifestGroup.java:185`，再联动 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/ManifestGroup.java:185`
- `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/ManifestGroup.java:182-188`**
```java
                  return ResidualEvaluator.of(spec, filter, caseSensitive);
                });

    DeleteFileIndex deleteFiles = deleteIndexBuilder.scanMetrics(scanMetrics).build();

    boolean dropStats = ManifestReader.dropStats(columns);
    if (deleteFiles.hasEqualityDeletes()) {
```

**片段 2：`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:174-180`**
```java
    return deleteLoader;
  }

  public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
  }

```

---
<a id="q-11-11"></a>
### 11.11 Runtime Filter 在 Spark Iceberg Scan 中如何生效

**🟡 问题: Spark Join 产生的 runtime filter 如何影响 Iceberg 扫描任务？**

**答案（总）:**

它不是“读后过滤”，而是在 scan 任务集合上做前置裁剪。命中后会重置任务列表，直接减少后续 split 与 IO。

**答案（分）:**

1. 入口是 `SparkBatchQueryScan#filter(Predicate[])`。  
   - 源码: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:127`
2. 先把 Spark predicate 转成 Iceberg expression（`convertRuntimeFilters`）。  
   - 源码: `SparkBatchQueryScan.java:195-213`
3. 按 `PartitionSpec` 生成 evaluator，过滤 `tasks()`。  
   - 源码: `SparkBatchQueryScan.java:133-147`
4. 仅当任务数减少时才 `resetTasks(filteredTasks)`，避免无效重规划。  
   - 源码: `SparkBatchQueryScan.java:156-159`

**答案（总）:**

可背句: **Runtime Filter 在 Iceberg 中的价值，是“计划前移裁剪”，不是“执行后补救”。**

**追问与反问:**
- 追问: “为什么有时 runtime filter 没效果？”
- 反问: “过滤字段能否映射到分区 source 字段？不能映射时 scan 层收益会受限。”

#### 30 秒速记（简版）

- 结论先行：它不是“读后过滤”，而是在 scan 任务集合上做前置裁剪。命中后会重置任务列表，直接减少后续 split 与 IO。
- 主链路：先定位 `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:127`，再联动 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:127`
- `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`




#### 源码关键片段

**片段 1：`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java:124-130`**
```java
  }

  @Override
  public void filter(Predicate[] predicates) {
    Expression runtimeFilterExpr = convertRuntimeFilters(predicates);

    if (runtimeFilterExpr != Expressions.alwaysTrue()) {
```

**片段 2：`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:174-180`**
```java
    return deleteLoader;
  }

  public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
  }

```

---
<a id="q-11-12"></a>
### 11.12 Row-level delete 在格式版本上的约束差异（V2/V3+）

**🔴 问题: V2 与 V3+ 在 position delete 介质上的硬约束是什么？**

**答案（总）:**

这是写入时的硬校验，不是文档建议。V1 不支持 delete；V2 禁止 position delete 使用 DV；V3/V4 要求 position delete 必须使用 DV（equality delete 除外）。

**答案（分）:**

1. V1 直接抛错: “Deletes are supported in V2 and above”。  
   - 源码: `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:283-285`
2. V2 约束: position delete 不能是 DV。  
   - 源码: `MergingSnapshotProducer.java:286-289`
3. V3/V4 约束: position delete 必须是 DV。  
   - 源码: `MergingSnapshotProducer.java:291-297`
4. 这在 `validateNewDeleteFile` 中执行，提交前即失败。  
   - 源码: `MergingSnapshotProducer.java:280-301`

**答案（总）:**

一锤定音: **跨版本迁移最危险的点不是语法，而是 row-level delete 介质约束不兼容。**

**追问与反问:**
- 追问: “为什么 V3 要强制 DV？”
- 反问: “我们线上表 format-version 与计算引擎能力矩阵，是否有准入校验清单？”

#### 30 秒速记（简版）

- 结论先行：这是写入时的硬校验，不是文档建议。V1 不支持 delete；V2 禁止 position delete 使用 DV；V3/V4 要求 position d。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:283`，再联动 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`，把“定义-实现-结果”串成一条线。
- 面试表达：先回答“是什么”，再补“为什么这样设计”，最后给“边界条件/取舍”。

#### 源码深挖（详版）

- 执行链路：从入口实现到关键方法再到输出行为，重点核对调用顺序、状态变化和异常分支。
- 关键取舍：这一题通常体现“正确性优先，性能优化随后”的实现原则；作答时要同时说清收益与代价。
- 边界与误区：明确区分接口声明与具体实现、规划阶段与执行阶段、语义保证与性能优化。
- 追问与反问：可追问并发/大数据量下的退化路径，并反问线上如何做指标观测与回归验证。

#### 源码定位

- `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:283`
- `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177`




#### 源码关键片段

**片段 1：`core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:280-286`**
```java
  protected void validateNewDeleteFile(DeleteFile file) {
    Preconditions.checkNotNull(file, "Invalid delete file: null");
    switch (formatVersion()) {
      case 1:
        throw new IllegalArgumentException("Deletes are supported in V2 and above");
      case 2:
        Preconditions.checkArgument(
```

**片段 2：`data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:174-180`**
```java
    return deleteLoader;
  }

  public CloseableIterable<T> filter(CloseableIterable<T> records) {
    return applyEqDeletes(applyPosDeletes(records));
  }

```

---

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
- [全新启动的 Flink 作业写 Iceberg，checkpoint 从 1 开始，会被 Iceberg 中已有的 maxCheckpointId 拦截吗？](#q-3-12) 🟠
- [Flink 的 JobID 是如何生成的？为什么全新启动和恢复启动不会冲突？](#q-3-13) 🟡
- [Flink 写入 Iceberg 时 `insertedRowMap` 会定时清空吗？它的生命周期是什么？](#q-3-14) 🟠
- [Flink CDC upsert 模式下的删除一定是 Equality Delete 文件吗？`ConvertEqualityDeleteFiles` 能否将其转换？](#q-3-15) 🔴

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
- [Iceberg 元数据的列统计指标存储在哪一层？每层分别记录什么？如何控制统计收集？](#q-5-5) 🟡

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

### 十二、Iceberg 元数据结构体系深度解析
- [元数据整体架构 — 多级索引树](#12-1) 🟢
- [TableMetadata — 元数据根节点（完整字段、常量、版本约束）](#12-2) 🟡
- [Schema — 表结构定义（NestedField、V3 新增类型）](#12-3) 🟢
- [PartitionSpec — 分区规范（Transform 类型、ID 空间隔离）](#12-4) 🟢
- [SortOrder — 排序规范](#12-5) 🟢
- [Snapshot — 快照（操作类型、summary 统计、惰性加载）](#12-6) 🟡
- [SnapshotRef — 快照引用（BRANCH / TAG / 保留策略）](#12-7) 🟡
- [ManifestFile — 清单文件元数据（Avro Field ID、PartitionFieldSummary 第一层剪枝）](#12-8) 🟠
- [ManifestEntry — 清单条目（data sequence number vs file sequence number）](#12-9) 🟠
- [ContentFile / DataFile / DeleteFile — 数据文件元数据（统计信息详解、第二层剪枝）](#12-10) 🟠
- [StatisticsFile 与 PartitionStatisticsFile — Puffin 格式统计](#12-11) 🟡
- [版本差异对比矩阵 (V1/V2/V3/V4)](#12-12) 🟡
- [元数据在读写场景中的作用（写入流程、查询多层剪枝、Compaction）](#12-13) 🟠
- [完整 TableMetadata JSON 示例 (V2 + V3 差异)](#12-14) 🟡
- [五大用户特性的元数据架构实现原理（Schema Evolution / Hidden Partitioning / Partition Evolution / Time Travel / Version Rollback）](#12-15) 🔴
- [元数据层级关系总图](#12-16) 🟡

---
## 一、Schema 演进与元数据管理

<a id="q-1-1"></a>
### 1.1 默认值机制

**[绿] 问题: 请说明 Iceberg 默认值机制在读写路径中的工作原理及其优势。**

**答案:**

Iceberg v3 通过 `initial-default` 和 `write-default` 实现了读写分离的默认值策略。

#### 读路径 - 惰性虚拟策略

当查询新增的带默认值列，但读取的是该列添加前的旧数据文件时:

1. **Schema 对比**: Reader 对比表 Schema 与文件 Schema，识别缺失列
2. **虚拟列生成**: 启用 `ConstantReader`，无需磁盘 I/O
3. **默认值填充**: 直接返回 `initial-default` 值

**核心优势**: 零 I/O 开销，实现真正的 Schema-on-Read

#### 写路径 - 主动物理化策略（Spark 4.x 专属，有源码支撑）

当 `INSERT` 操作未指定带默认值列的值时（如 `INSERT INTO t VALUES (1, DEFAULT)`）:

1. **Schema 转换阶段**: Iceberg 将 `write-default` 注册到 Spark Schema 元数据中
2. **Spark Analyzer 逻辑计划重写**: Spark 自身的分析器将 SQL 中的 `DEFAULT` 关键字替换为具体的常量 Literal 节点
3. **物理写入**: 默认值作为普通常量被实际写入数据文件

**源码证据链（完整调用路径）**:

**Step 1** — Iceberg Schema → Spark Schema 转换时注入默认值:

```java
// 源码: spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/TypeToSparkType.java:76-79
// 注意: 此代码仅存在于 Spark 4.0+ 模块，Spark 3.5 的 TypeToSparkType 中没有此逻辑
if (field.writeDefault() != null) {
    Object writeDefault = SparkUtil.internalToSpark(field.type(), field.writeDefault());
    sparkField =
        sparkField.withCurrentDefaultValue(Literal$.MODULE$.create(writeDefault, type).sql());
        // ↑ 将 Iceberg 的 write-default 转换为 Spark StructField 的 currentDefaultValue
}

if (field.initialDefault() != null) {
    Object initialDefault = SparkUtil.internalToSpark(field.type(), field.initialDefault());
    sparkField =
        sparkField.withExistenceDefaultValue(Literal$.MODULE$.create(initialDefault, type).sql());
        // ↑ 将 Iceberg 的 initial-default 转换为 Spark StructField 的 existenceDefaultValue
}
```

调用入口: `SparkSchemaUtil.convert(schema)` → `TypeUtil.visit(schema, new TypeToSparkType())`

**Step 2** — Spark Analyzer 自动处理 `DEFAULT` 关键字:

当用户写 `INSERT INTO t VALUES (1, DEFAULT)` 时，Spark 4.x 的 Analyzer 看到 `DEFAULT` 后，
查找对应列的 `currentDefaultValue`，将其替换为具体的 `Literal(42)` 等常量节点。
**这一步完全由 Spark 引擎完成，Iceberg 不参与**。

**Step 3** — 常量值随正常写入流程写入 Parquet/ORC 文件。

**测试验证**（来自实际测试用例）:
```java
// 源码: spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/sql/TestSparkDefaultValues.java
// 建表时通过 API 设置 write-default（必须 format-version=3）
Types.NestedField.optional("int_col").withId(3)
    .ofType(Types.IntegerType.get())
    .withWriteDefault(Literal.of(42))  // ← write-default = 42
    .build();

// INSERT 时使用 DEFAULT 关键字
sql("INSERT INTO %s VALUES (1, DEFAULT, DEFAULT, DEFAULT)", commitTarget());
// 结果: int_col 的值为 42，被实际写入数据文件
```

**重要限制**:
- **Spark DDL 不支持 DEFAULT 子句**: `CREATE TABLE ... (col INT DEFAULT 42)` 会抛出 `AnalysisException("does not support column default value")`
- **ALTER TABLE 不支持**: `ALTER TABLE ADD COLUMN col STRING DEFAULT 'x'` 会抛出 `UnsupportedOperationException("default values in Spark is currently unsupported")`
- 默认值只能通过 Iceberg Java API（`withWriteDefault()`）或 REST API 设置，然后在 `INSERT ... VALUES (..., DEFAULT)` 中使用
- **仅 Spark 4.0+ 支持**: Spark 3.5 的 `TypeToSparkType` 中完全没有 `withCurrentDefaultValue` 调用
- **Flink 目前不支持**: Flink 模块中没有任何 `writeDefault` 相关代码

**核心优势**: 保证数据完整性，利用引擎向量化执行优化性能

#### NestedField 中的默认值定义

默认值在 `Types.NestedField` 内部以 `Literal<?>` 类型存储，并在构造时进行类型安全转换:

```java
// 源码: api/src/main/java/org/apache/iceberg/types/Types.java
public static class NestedField implements Serializable {
    private final Literal<?> initialDefault;  // 读路径默认值
    private final Literal<?> writeDefault;    // 写路径默认值

    private NestedField(
        boolean isOptional, int id, String name,
        org.apache.iceberg.types.Type type, String doc,
        Literal<?> initialDefault, Literal<?> writeDefault) {
      // ...
      this.initialDefault = castDefault(initialDefault, type);  // 类型安全转换
      this.writeDefault = castDefault(writeDefault, type);
    }

    public Literal<?> initialDefaultLiteral() {
      return initialDefault;
    }

    public Object initialDefault() {
      return initialDefault != null ? initialDefault.value() : null;
    }

    public Literal<?> writeDefaultLiteral() {
      return writeDefault;
    }

    public Object writeDefault() {
      return writeDefault != null ? writeDefault.value() : null;
    }
}
```

**注意**: 默认值需要 format version >= 3 才支持。`Schema.checkCompatibility()` 方法中会校验:

```java
// 源码: api/src/main/java/org/apache/iceberg/Schema.java
@VisibleForTesting static final int DEFAULT_VALUES_MIN_FORMAT_VERSION = 3;

// 在 checkCompatibility 中:
if (field.initialDefault() != null && formatVersion < DEFAULT_VALUES_MIN_FORMAT_VERSION) {
    // 抛出 IllegalStateException
}
```

#### 30 秒速记（简版）

- 结论先行：Iceberg v3 通过 initial-default 和 write-default 实现了读写分离的默认值策略。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/types/Types.java`（NestedField 中的 `initialDefault` 和 `writeDefault` 字段），再联动 `api/src/main/java/org/apache/iceberg/Schema.java`（`checkCompatibility()` 做版本校验），把"定义-校验-应用"串成一条线。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-1-2"></a>
### 1.2 零成本列重命名

**[绿] 问题: Iceberg 如何实现零成本列重命名？核心机制是什么？**

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
    private final StructType struct;        // 底层类型结构
    private final int schemaId;             // Schema 版本 ID
    private final int[] identifierFieldIds; // 主键字段 ID 数组
    private final int highestFieldId;       // 当前最大 fieldId

    // 懒加载的索引映射(设计模式: 延迟初始化)
    private transient BiMap<String, Integer> aliasToId = null;          // 别名 → ID（双向映射）
    private transient Map<Integer, NestedField> idToField = null;      // ID → Field（核心索引）
    private transient Map<String, Integer> nameToId = null;            // Name → ID
    private transient Map<String, Integer> lowerCaseNameToId = null;   // 小写 Name → ID（大小写不敏感查询）
    private transient Map<Integer, Accessor<StructLike>> idToAccessor = null; // ID → 数据访问器
    private transient Map<Integer, String> idToName = null;            // ID → Name
    private transient Set<Integer> identifierFieldIdSet = null;        // 标识字段 ID 集合
    private final transient Map<Integer, Integer> idsToReassigned;     // 原始ID → 重分配ID
    private final transient Map<Integer, Integer> idsToOriginal;       // 重分配ID → 原始ID

    // 根据 fieldId 查找字段(核心方法)
    public NestedField findField(int id) {
        return lazyIdToField().get(id);  // 仅依赖 ID,与 name 无关
    }

    // 根据列名查找字段(内部先转为 ID 再查找)
    public NestedField findField(String name) {
        Preconditions.checkArgument(!name.isEmpty(), "Invalid column name: (empty)");
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

    // 懒加载 Name 到 ID 的映射（注意使用 ImmutableMap.copyOf 保证不可变性）
    private Map<String, Integer> lazyNameToId() {
        if (nameToId == null) {
            this.nameToId = ImmutableMap.copyOf(TypeUtil.indexByName(struct));
        }
        return nameToId;
    }

    // 懒加载 ID 到 Name 的映射
    private Map<Integer, String> lazyIdToName() {
        if (idToName == null) {
            this.idToName = ImmutableMap.copyOf(TypeUtil.indexNameById(struct));
        }
        return idToName;
    }
}
```

**关键设计**: `idToField` 是核心索引，`nameToId` 只是辅助查找层，重命名只需更新 `nameToId` 映射。`BiMap<String, Integer> aliasToId` 还提供了列别名到 ID 的双向映射，用于支持 Parquet/Avro 格式转换时的列名映射。

#### 设计模式分析

**1. 不可变对象模式(Immutable Object)**

```java
public class Schema implements Serializable {
    private final StructType struct;        // final 修饰,不可变
    private final int schemaId;             // final 修饰,不可变
    private final int[] identifierFieldIds; // final 修饰,数组引用不变

    // 投影操作返回新的 Schema 实例（注意: 选择 "*" 时返回 this 本身）
    public Schema select(Collection<String> names) {
        return internalSelect(names, true);
    }

    private Schema internalSelect(Collection<String> names, boolean caseSensitive) {
        if (names.contains("*")) {
            return this;  // 短路: 选择所有列时直接返回自身
        }
        // ... 收集选中列的 fieldId
        return TypeUtil.select(this, selected);  // 其他情况返回新实例
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
    if (idToField == null) {  // 单检查模式（非 Double-Check Locking）
        this.idToField = TypeUtil.indexById(struct);
    }
    return idToField;
}
```

**注意**: 这里并未使用 Double-Check Locking 或 `synchronized`，因为 Schema 本身是不可变对象（所有核心字段均为 `final`），即使多线程同时触发懒加载，也只是重复构建相同的索引，不会导致数据不一致。这是一种安全的"racy single-check"惯用法。

**优势**:
- **节省内存**: 仅在需要时构建索引
- **加速序列化**: `transient` 关键字避免序列化临时索引
- **按需计算**: 不同查询可能只需要部分索引

#### 与 Hive 的对比

| 特性 | Iceberg | Hive |
|------|---------|------|
| 标识机制 | `fieldId`(不变) | 列名(可变) |
| 重命名成本 | 零成本(仅改 metadata.json 中的 name) | 仅改 Metastore 元数据，但已有文件中列名不变 |
| 外部工具兼容 | 通过 `fieldId` 匹配，重命名后仍能正确读取 | 基于列名匹配，重命名后外部工具可能读到 `null` |
| 并发安全 | Schema 不可变对象,天然线程安全 | 需要 Metastore 锁机制 |

**深入理解**: Hive 重命名列后，外部工具直接读取 Parquet 文件可能因列名不匹配读到 `null`，而 Iceberg 的 `fieldId` 从根本上解决了这个问题。不可变对象模式确保了在高并发场景下的数据一致性。

#### 30 秒速记（简版）

- 结论先行：Iceberg 通过 **fieldId 唯一标识机制**实现零成本列重命名，而非依赖列名。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/Schema.java`（核心字段定义与懒加载索引），再联动 `api/src/main/java/org/apache/iceberg/types/TypeUtil.java`（`indexById()` 等索引构建方法），把"定义-实现-结果"串成一条线。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-1-3"></a>
### 1.3 Parquet 文件的 ID 映射

**[黄] 问题: Parquet 是基于名称的格式，Iceberg 如何将其改造为支持基于 ID 的 Schema 演进？**

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

**深入理解 — 通用工具读取 Iceberg Parquet 文件会失去什么？**

通用 Parquet 工具（如 `parquet-tools`、`pyarrow.parquet`、Pandas）不理解 Iceberg 的 `field_id` 约定，只能**基于列名或列位置**匹配数据。这会导致以下 Schema 演进能力失效：

| 失效的能力 | 原因 | 具体后果 |
|-----------|------|---------|
| **列重命名** | 工具按列名匹配，重命名后旧文件中的列名对不上 | 重命名后的列读出 `null`，旧名称列变成"多余列" |
| **列删除后重新添加** | 工具按列名匹配，无法区分"旧的已删除列"和"新添加的同名列" | 新列错误地读到旧数据 |
| **列重排序** | 工具按列位置匹配时，重排序后位置错乱 | 列数据张冠李戴 |
| **嵌套字段演进** | 工具不解析嵌套结构中的 `field_id` | struct 内部字段的增删改名全部失效 |

**源码证据** — Iceberg 自身读取时也有三条路径，退化效果一目了然:

```java
// 源码: parquet/src/main/java/org/apache/iceberg/parquet/ParquetReadSupport.java:66-72
if (ParquetSchemaUtil.hasIds(fileSchema)) {
    // 路径1: 有 field_id → 基于 ID 精确匹配（完整 Schema 演进能力）
    projection = ParquetSchemaUtil.pruneColumns(fileSchema, expectedSchema);
} else if (nameMapping != null) {
    // 路径2: 无 ID 但有 NameMapping → 通过名称映射恢复 ID（兼容 Hive 迁移文件）
    MessageType typeWithIds = ParquetSchemaUtil.applyNameMapping(fileSchema, nameMapping);
    projection = ParquetSchemaUtil.pruneColumns(typeWithIds, expectedSchema);
} else {
    // 路径3: 无 ID 无映射 → 退化为按列位置匹配（假设列顺序不变且未删列）
    projection = ParquetSchemaUtil.pruneColumnsFallback(fileSchema, expectedSchema);
}
```

路径3 的 `pruneColumnsFallback` Javadoc 明确写道:
> *"Files that were written without field ids are read assuming that schema evolution preserved column order. Deleting columns was not allowed."*
> （没有 field_id 的文件，读取时假设列顺序不变且未发生过删列操作。）

**通用工具的情况比路径3 更差** — 它们连 Iceberg 的 NameMapping 都不会使用，完全靠列名字面匹配。

**一句话总结**: `fieldId` 是 Iceberg Schema 演进的基石，通用工具忽略它就等于放弃了列重命名、列删除、列重排序等所有非追加式的 Schema 演进能力。

#### ID 检测机制

Iceberg 在 `ParquetSchemaUtil` 中提供了检测 Parquet 文件是否包含 field ID 的能力:

```java
// 源码: parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java
public static boolean hasIds(MessageType fileSchema) {
    return ParquetTypeVisitor.visit(fileSchema, new HasIds());
}

// HasIds 是 ParquetSchemaUtil 的内部类，遍历所有字段检查 ID
public static class HasIds extends ParquetTypeVisitor<Boolean> {
    @Override
    public Boolean primitive(PrimitiveType primitive) {
        return primitive.getId() != null;  // 检查原始类型是否有 ID
    }

    @Override
    public Boolean struct(GroupType struct, List<Boolean> hasIds) {
        for (Boolean hasId : hasIds) {
            if (hasId) return true;  // 任一子字段有 ID 即返回 true
        }
        return struct.getId() != null;
    }
    // ... map, list, variant 类似处理
}
```

对于没有 ID 的文件，Iceberg 会分配回退 ID:

```java
// 源码: parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java
public static MessageType addFallbackIds(MessageType fileSchema) {
    MessageTypeBuilder builder = org.apache.parquet.schema.Types.buildMessage();
    int ordinal = 1; // ids are assigned starting at 1
    for (Type type : fileSchema.getFields()) {
        builder.addField(type.withId(ordinal));
        ordinal += 1;
    }
    return builder.named(fileSchema.getName());
}
```

#### NameMapping 机制详解

当 Parquet 文件没有 `field_id`（如 Hive 迁移文件），`addFallbackIds` 只能按位置分配 ordinal ID，无法处理嵌套字段。更完善的方案是 **NameMapping**——一份"列名 → fieldId"的映射表，存储在 `metadata.json` 的 table properties 中。

**存储位置与格式:**

```
metadata.json
  └── "properties": {
        "schema.name-mapping.default": "[{\"field-id\":1,\"names\":[\"id\"]}, ...]"
      }
```

property key 定义在 `TableProperties.java:314`:
```java
public static final String DEFAULT_NAME_MAPPING = "schema.name-mapping.default";
```

value 是一段 JSON 数组（`NameMappingParser.java` Javadoc 示例）:
```json
[
  { "field-id": 1, "names": ["id", "record_id"] },
  { "field-id": 2, "names": ["data"] },
  { "field-id": 3, "names": ["location"], "fields": [
      { "field-id": 4, "names": ["latitude", "lat"] },
      { "field-id": 5, "names": ["longitude", "long"] }
  ]}
]
```

每个条目:
- `field-id`: Iceberg fieldId
- `names`: 该字段的**所有已知名称**（支持多别名，如 `["latitude", "lat"]`，字段 rename 后新旧名都会保留）
- `fields`: 嵌套字段的映射（支持 struct/map/list 递归）

**完整生命周期:**

| 阶段 | 操作 | 源码位置 |
|------|------|---------|
| **创建** | `MappingUtil.create(schema)` 从 Schema 自动生成 | `core/.../mapping/MappingUtil.java:52` |
| **存储** | 序列化为 JSON 写入 table property | `NameMappingParser.toJson()` |
| **自动更新** | `SchemaUpdate` 每次 Schema 变更时同步更新 NameMapping | `SchemaUpdate.java:484-496` |
| **读取使用** | `ParquetReadSupport.init()` 判断文件无 ID 时，用 NameMapping 恢复 ID | `ParquetReadSupport.java:68-70` |

**自动更新源码:**

```java
// 源码: core/src/main/java/org/apache/iceberg/SchemaUpdate.java:484-502
private TableMetadata applyChangesToMetadata(TableMetadata metadata) {
    String mappingJson = metadata.property(TableProperties.DEFAULT_NAME_MAPPING, null);
    TableMetadata newMetadata = metadata;
    if (mappingJson != null) {
        try {
            NameMapping mapping = NameMappingParser.fromJson(mappingJson);
            // 根据本次 Schema 变更（rename/add/delete）同步更新 NameMapping
            NameMapping updated = MappingUtil.update(mapping, updates, parentToAddedIds);
            Map<String, String> updatedProperties = Maps.newHashMap();
            updatedProperties.putAll(metadata.properties());
            updatedProperties.put(
                TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(updated));
            newMetadata = metadata.replaceProperties(updatedProperties);
        } catch (RuntimeException e) {
            LOG.warn("Failed to update external schema mapping: {}", mappingJson, e);
            // 更新失败不阻断 Schema 变更，只是 warn
        }
    }
    // ...
}
```

**关键设计: `names` 数组支持多别名**。当 rename 列时，`MappingUtil.update()` 不会删除旧名称，而是追加新名称。例如 `latitude` → `lat` 后，NameMapping 变为 `["latitude", "lat"]`，这样无论 Parquet 文件中用哪个名字都能匹配到同一个 fieldId。

**面试追问: NameMapping 和 `addFallbackIds` 的区别?**
- `addFallbackIds`: 只按顶层列位置分配 ordinal ID，不处理嵌套字段，不支持列名变化
- `NameMapping`: 通过名称匹配恢复 ID，支持嵌套字段、多别名、列 rename，是 Hive 迁移的正确方案

#### 无 ID Parquet 文件的来源 — 表迁移而非手动复制

"无 ID 的 Parquet 文件"出现在 Iceberg 中，**不是手动复制文件到目录里**，而是通过正规的表迁移操作将已有文件**注册**到 Iceberg 元数据中：

**场景一: Migrate Table（就地转换，最常见）**
```sql
CALL catalog.system.migrate('db.hive_table');
```

`MigrateTableSparkAction` 的 Javadoc 明确说明:
> *"Takes a Spark table in the source catalog and attempts to transform it into an Iceberg table **in the same location** with the same identifier."*

它的核心步骤:
1. 不移动/复制任何数据文件 — 原始 Parquet 文件留在原地
2. 创建 Iceberg 的 `metadata.json`、manifest list、manifest file，将已有 Parquet 文件**注册**进去
3. **自动调用 `ensureNameMappingPresent()` 生成 NameMapping**

```java
// 源码: spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/actions/MigrateTableSparkAction.java:141-142
LOG.info("Ensuring {} has a valid name mapping", destTableIdent());
ensureNameMappingPresent(icebergTable);

// 源码: spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/actions/BaseTableCreationSparkAction.java:168-173
protected void ensureNameMappingPresent(Table table) {
    if (!table.properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING)) {
        NameMapping nameMapping = MappingUtil.create(table.schema());
        String nameMappingJson = NameMappingParser.toJson(nameMapping);
        table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, nameMappingJson).commit();
    }
}
```

**场景二: Snapshot Table（创建副本表，原表不变）**
```sql
CALL catalog.system.snapshot('db.hive_table', 'db.iceberg_table');
```
创建一个新的 Iceberg 表指向同一批数据文件，原 Hive 表保持不变。同样自动创建 NameMapping。

**场景三: Add Files（手动注册外部文件到已有 Iceberg 表）**
```sql
CALL catalog.system.add_files('db.iceberg_table', 'db.source_hive_table');
```

**关键理解**: 所有迁移操作都是"元数据级别"的 — 只创建 metadata.json 和 manifest 文件来注册已有数据文件，**零数据拷贝**。迁移后读取旧文件时走 `ParquetReadSupport` 的路径2（NameMapping），新写入的文件则带有 `field_id` 走路径1。

#### 面试追问: Hive 是 ORC 格式，迁移到 Iceberg（默认 Parquet）会有问题吗？

**答案: 完全没有问题。** Iceberg 原生支持 Parquet、ORC、Avro 三种格式**混合存储**。

**源码证据 1 — 迁移白名单支持 ORC**

`BaseTableCreationSparkAction.java:53-54`:
```java
private static final Set<String> ALLOWED_SOURCES =
    ImmutableSet.of("parquet", "avro", "orc", "hive");
```

`validateSourceTable()` 检查源表 provider 是否在白名单中，ORC 明确在列表内。

**源码证据 2 — 迁移时按格式分路径读取 metrics**

`TableMigrationUtil.listPartition()` (第 178-203 行) 根据 `format` 分三条路径：
```java
if (format.contains("avro")) {
    // Avro: 读取行数，其余 metrics 为 null
} else if (format.contains("parquet")) {
    // Parquet: ParquetUtil.fileMetrics() 从 footer 读取完整 metrics
} else if (format.contains("orc")) {
    // ORC: OrcMetrics.fromInputFile() 从 footer 读取完整 metrics
}
```

注意：迁移**不转换文件格式**，只从 footer 读取统计信息，然后用 `DataFiles.builder()` 构建元数据：
```java
// TableMigrationUtil.buildDataFile()
return DataFiles.builder(spec)
    .withPath(stat.getPath().toString())
    .withFormat(format)      // ← 记录为 "orc"，不做格式转换
    .withFileSizeInBytes(stat.getLen())
    .withMetrics(metrics)
    .withPartitionValues(partitionValues)
    .build();
```

**迁移后的数据读写行为**

| 方面 | 说明 |
|------|------|
| **已有数据** | 保持 ORC 格式不变，Iceberg 根据 manifest 中的 `file_format` 字段选择 ORC reader |
| **新写入数据** | 默认用 Parquet（`write.format.default=parquet`），可配置为 ORC |
| **同一张表** | 可以同时存在 ORC 和 Parquet 文件，按文件粒度选择 reader |
| **查询性能** | 无影响，Iceberg 对每种格式都有完整的原生读写支持 |

**如需统一格式**，可用 `RewriteDataFiles` 将旧 ORC 文件重写为 Parquet（可选，非必须）：
```sql
CALL catalog.system.rewrite_data_files('db.table');
```

#### 面试追问: `rewrite_data_files` 会将 ORC 格式转换为 Parquet 吗？

**答案: 是的，默认配置下会将 ORC 重写为 Parquet。**

rewrite 的读和写是独立的 — 读按原文件格式（ORC）读取，写按 table property `write.format.default` 决定输出格式。

**源码证据 — 写入格式的优先级链**

`SparkWriteConf.dataFileFormat()` (SparkWriteConf.java:167-176):
```java
public FileFormat dataFileFormat() {
    String valueAsString =
        confParser.stringConf()
            .option(SparkWriteOptions.WRITE_FORMAT)              // 优先级1: Spark write option
            .tableProperty(TableProperties.DEFAULT_FILE_FORMAT)  // 优先级2: table property
            .defaultValue(TableProperties.DEFAULT_FILE_FORMAT_DEFAULT) // 优先级3: 硬编码默认值
            .parse();
    return FileFormat.fromString(valueAsString);
}
```

`TableProperties.java:121-123` — 默认值硬编码为 `"parquet"`:
```java
public static final String DEFAULT_FILE_FORMAT = "write.format.default";
public static final String DEFAULT_FILE_FORMAT_DEFAULT = "parquet";
```

**执行流程**:
```
rewrite_data_files 读取 ORC 文件 → 反序列化为内存行 → 按 write.format.default(parquet) 序列化写出新文件
```

旧 ORC 文件被新 Parquet 文件替换（通过原子 commit 完成）。

**如果想保持 ORC 格式不变**，需要在 rewrite 之前设置表属性：
```sql
ALTER TABLE db.table SET TBLPROPERTIES ('write.format.default' = 'orc');
CALL catalog.system.rewrite_data_files('db.table');
```

这样重写后的文件仍然是 ORC 格式。

#### 30 秒速记（简版）

- 结论先行：Iceberg 并未改造 Parquet 格式本身，而是利用 Parquet 规范的**可选 ID 属性**构建了元数据层。
- 主链路：`ParquetSchemaUtil.hasIds()` 检测 → 有 ID 时 `pruneColumns()` 按 ID 精确匹配 → 无 ID 时优先用 `NameMapping`（存储在 `schema.name-mapping.default` table property）通过名称恢复 ID → 都没有则 `pruneColumnsFallback()` 按列位置退化匹配。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-1-4"></a>
### 1.4 fieldId 写入机制

**[黄] 问题: 从源码层面说明 Iceberg 如何将 `fieldId` 写入 Parquet 文件？**

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

    /**
     * 支持 Variant 类型 shredding 的重载版本
     */
    public static MessageType convert(
        Schema schema, String name, VariantShreddingFunction variantShreddingFunc) {
        return new TypeToMessageType(variantShreddingFunc).convert(schema, name);
    }
}
```

**2. TypeToMessageType 转换器（独立工具类，非 Visitor 模式）**

```java
// 源码: parquet/src/main/java/org/apache/iceberg/parquet/TypeToMessageType.java
public class TypeToMessageType {  // 独立类，不继承任何基类

    private final BiFunction<Integer, String, Type> variantShreddingFunc;

    public TypeToMessageType() {
        this.variantShreddingFunc = null;
    }

    public MessageType convert(Schema schema, String name) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (NestedField field : schema.columns()) {
            // unknown 类型不写入数据文件
            Type fieldType = field(field);
            if (fieldType != null) {
                builder.addField(fieldType);
            }
        }
        return builder.named(AvroSchemaUtil.makeCompatibleName(name));
    }

    // 核心方法: 将 Iceberg NestedField 转换为 Parquet Type，注入 fieldId
    public Type field(NestedField field) {
        Type.Repetition repetition =
            field.isOptional() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
        int id = field.fieldId();    // <-- 获取 fieldId
        String name = field.name();

        if (field.type().typeId() == TypeID.UNKNOWN) {
            return null;                  // UNKNOWN 类型跳过，不写入文件

        } else if (field.type().isPrimitiveType()) {
            return primitive(field.type().asPrimitiveType(), repetition, id, name);

        } else if (field.type().isVariantType()) {
            return variant(repetition, id, name);  // Variant 类型特殊处理

        } else {
            NestedType nested = field.type().asNestedType();
            if (nested.isStructType()) {
                return struct(nested.asStructType(), repetition, id, name);
            } else if (nested.isMapType()) {
                return map(nested.asMapType(), repetition, id, name);
            } else if (nested.isListType()) {
                return list(nested.asListType(), repetition, id, name);
            }
            throw new UnsupportedOperationException("Can't convert unknown type: " + nested);
        }
    }

    // struct 方法通过 .id(id) 注入 fieldId
    public GroupType struct(StructType struct, Type.Repetition repetition, int id, String name) {
        Types.GroupBuilder<GroupType> builder = Types.buildGroup(repetition);
        for (NestedField field : struct.fields()) {
            Type fieldType = field(field);  // 递归调用 field()
            if (fieldType != null) {
                builder.addField(fieldType);
            }
        }
        return builder.id(id).named(AvroSchemaUtil.makeCompatibleName(name));  // <-- 注入 fieldId
    }
}
```

**3. Parquet Type Builder 机制**

```java
// Parquet 库提供的 Builder API
Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
    .id(42)              // <-- 设置 field ID
    .named("user_id");   // <-- 设置 field name
```

#### 设计模式分析: 直接递归转换器模式

**TypeToMessageType 的设计特点**:

`TypeToMessageType` 是一个**独立工具类**，不继承 `SchemaVisitor`，采用**直接递归调用**的方式转换类型：

```
convert(Schema) --> 遍历 schema.columns()
  +-- field(NestedField) --> 根据类型分派
       |-- UNKNOWN                                       --> 返回 null（跳过）
       |-- primitive(PrimitiveType, repetition, id, name) --> 返回 PrimitiveType with fieldId
       |-- variant(repetition, id, name)                  --> 返回 Variant GroupType with fieldId
       |-- struct(StructType, repetition, id, name)       --> 递归调用 field()
       |-- list(ListType, repetition, id, name)           --> 递归调用 field()
       +-- map(MapType, repetition, id, name)             --> 递归调用 field()
```

**为什么不使用 Visitor 模式？**

1. **职责单一**: TypeToMessageType 仅负责 Iceberg-->Parquet 的类型转换，不需要通用遍历框架
2. **直接控制**: 直接递归调用提供了更精确的参数传递（如 `repetition`, `id`, `name` 需要逐层传递）
3. **与 Parquet Builder API 匹配**: Parquet 的 `Types.buildGroup()` / `Types.primitive()` Builder 模式天然适合递归构造

> **注意**: Iceberg 中真正使用 Visitor 模式的是 `TypeUtil.SchemaVisitor<T>`，用于 Schema 投影、字段收集等通用遍历场景，与 TypeToMessageType 的专用转换器是不同的设计选择。

| 模式 | 代表类 | 适用场景 |
|------|--------|---------|
| **直接递归转换器** | `TypeToMessageType` | 专用的 Iceberg-->Parquet 转换 |
| **Visitor 模式** | `TypeUtil.SchemaVisitor` | 通用 Schema 遍历（投影、裁剪等） |

#### 完整转换示例

```
Iceberg Schema:
  NestedField(id=1, name="user_id", type=IntegerType, required=true)
  NestedField(id=2, name="user_name", type=StringType, required=false)

--- TypeToMessageType.convert() --> field() 递归 --->

Parquet MessageType:
  message table {
    required int32 user_id = 1;      <-- fieldId 已注入
    optional binary user_name = 2;   <-- fieldId 已注入（StringType 映射为 binary + STRING 注解）
  }
```

**补充说明**: StringType 在 Parquet 中映射为 `BINARY` 原始类型 + `STRING` LogicalTypeAnnotation，这在 `primitive()` 方法中实现:

```java
// 源码: parquet/src/main/java/org/apache/iceberg/parquet/TypeToMessageType.java
case STRING:
    return Types.primitive(BINARY, repetition).as(STRING).id(id).named(name);
```

**深入理解**: TypeToMessageType 采用直接递归转换器模式，通过 `convert()` --> `field()` --> `struct()/list()/map()/primitive()/variant()` 的递归调用链，将 fieldId 逐层注入到 Parquet 类型树中。`field()` 方法中还处理了 UNKNOWN 类型（跳过不写入）和 Variant 类型（特殊的 Group 结构），体现了对完整类型系统的覆盖。

#### 30 秒速记（简版）

- 结论先行：核心逻辑位于 `ParquetSchemaUtil` 工具类和 `TypeToMessageType` 转换器。
- 主链路：先定位 `parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java`（入口 `convert()` 方法），再联动 `parquet/src/main/java/org/apache/iceberg/parquet/TypeToMessageType.java`（`field()` 方法注入 ID），把"入口-分派-注入"串成一条线。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-1-5"></a>
### 1.5 Name Mapping 机制

**[红] 问题: Iceberg 如何处理没有 `fieldId` 的 Parquet 文件(如从 Hive 导入的文件)？**

**答案:**

通过 **Name Mapping(名称映射)** 机制桥接列名与 `fieldId` 的关联。

#### 工作流程

1. **查找映射**: Reader 发现文件无 ID 时，查看表属性 `schema.name-mapping.default`
2. **构建映射**: 该属性包含 JSON 格式的"列名-->ID"映射规则(支持别名)
3. **应用映射**: 根据列名动态赋予 `fieldId`
4. **生成 Schema**: 内存中补全 ID，后续逻辑基于 ID 处理

#### NameMapping 源码实现

```java
// 源码: core/src/main/java/org/apache/iceberg/mapping/NameMapping.java
public class NameMapping implements Serializable {
    private final MappedFields mapping;  // 映射字段列表

    // 索引（注意：虽然方法名带 lazy，但构造器中会立即调用）
    private transient Map<Integer, MappedField> fieldsById;    // ID --> MappedField
    private transient Map<String, MappedField> fieldsByName;   // Name --> MappedField

    // 构造器：立即初始化两个索引（非惰性加载！）
    NameMapping(MappedFields mapping) {
        this.mapping = mapping;
        lazyFieldsById();    // 构造时立即调用
        lazyFieldsByName();  // 构造时立即调用
    }

    /**
     * 根据 fieldId 查找映射
     */
    public MappedField find(int id) {
        return lazyFieldsById().get(id);
    }

    /**
     * 根据列名查找映射(支持嵌套路径，用 "." 连接)
     */
    public MappedField find(String... names) {
        return lazyFieldsByName().get(DOT.join(names));
    }

    /**
     * 根据列名列表查找映射
     */
    public MappedField find(List<String> names) {
        return lazyFieldsByName().get(DOT.join(names));
    }

    /**
     * 根据父路径和字段名查找映射
     */
    public MappedField find(Iterable<String> names, String name) {
        return lazyFieldsByName().get(DOT.join(Iterables.concat(names, ImmutableList.of(name))));
    }

    // 索引构建方法（带缓存，但首次在构造器中即被触发）
    private Map<Integer, MappedField> lazyFieldsById() {
        if (fieldsById == null) {
            this.fieldsById = MappingUtil.indexById(mapping);
        }
        return fieldsById;
    }

    private Map<String, MappedField> lazyFieldsByName() {
        if (fieldsByName == null) {
            this.fieldsByName = MappingUtil.indexByName(mapping);
        }
        return fieldsByName;
    }
}
```

**重要细节**: 虽然 `lazyFieldsById()` 和 `lazyFieldsByName()` 方法名带有 "lazy" 前缀，但 **NameMapping 的构造器会立即调用这两个方法**，实际上是**饥饿初始化(Eager Initialization)**。这与 `Schema` 类的真正懒加载不同。`lazy` 前缀保留的意义在于反序列化后（`transient` 字段会被置为 null），后续调用时能够重新构建索引。

#### ApplyNameMapping 应用逻辑

```java
// 源码: parquet/src/main/java/org/apache/iceberg/parquet/ApplyNameMapping.java
class ApplyNameMapping extends ParquetTypeVisitor<Type> {  // 注意：包级可见性，非 public
    private final NameMapping nameMapping;
    private final Deque<String> fieldNames = Lists.newLinkedList();  // 路径栈

    ApplyNameMapping(NameMapping nameMapping) {
        this.nameMapping = nameMapping;
    }

    @Override
    public Type primitive(PrimitiveType primitive) {
        MappedField field = nameMapping.find(currentPath());
        // 找到映射则注入 ID，否则保持原样
        return field == null ? primitive : primitive.withId(field.id());
    }

    @Override
    public Type struct(GroupType struct, List<Type> types) {
        MappedField field = nameMapping.find(currentPath());
        List<Type> actualTypes = types.stream()
            .filter(Objects::nonNull).collect(Collectors.toList());
        Type structType = struct.withNewFields(actualTypes);
        return field == null ? structType : structType.withId(field.id());
    }

    // 路径管理：通过 beforeField/afterField 维护当前字段路径
    @Override
    public void beforeField(Type type) {
        fieldNames.push(type.getName());  // 进入字段时压栈
    }

    @Override
    public void afterField(Type type) {
        fieldNames.pop();  // 离开字段时弹栈
    }

    // list/map 的 element/key/value 使用标准化名称
    @Override
    public void beforeElementField(Type element) {
        fieldNames.push("element");  // 标准化为 "element"
    }

    @Override
    public void beforeKeyField(Type key) {
        fieldNames.push("key");  // 标准化为 "key"
    }

    @Override
    public void beforeValueField(Type key) {
        fieldNames.push("value");  // 标准化为 "value"
    }

    @Override
    protected String[] currentPath() {
        return Lists.newArrayList(fieldNames.descendingIterator()).toArray(new String[0]);
    }
}
```

`ApplyNameMapping` 的调用入口在 `ParquetSchemaUtil` 中:

```java
// 源码: parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java
public static MessageType applyNameMapping(MessageType fileSchema, NameMapping nameMapping) {
    return (MessageType) ParquetTypeVisitor.visit(fileSchema, new ApplyNameMapping(nameMapping));
}
```

#### 设计模式分析: 适配器模式(Adapter Pattern)

**问题**: Hive 表的 Parquet 文件不包含 `fieldId`,但 Iceberg 依赖 `fieldId` 进行 Schema 演进

**解决方案**: Name Mapping 作为适配器,将基于名称的旧系统适配到基于 ID 的新系统

```
旧系统(Hive)          适配器(NameMapping)      新系统(Iceberg)
+-------------+       +------------------+     +---------------+
| Parquet      |       | Name --> ID      |     | Schema        |
| (no ID)      | ----> | Mapping          | --->| (with ID)     |
| user_name    |       | user_name --> 1  |     | fieldId=1     |
| user_id      |       | user_id --> 2    |     | fieldId=2     |
+-------------+       +------------------+     +---------------+
```

**适配器模式优势**:
- **兼容性**: 无需重写文件,直接读取 Hive 表
- **灵活性**: 支持别名和重命名场景
- **透明性**: 上层代码无需感知映射存在

#### MappedField 数据结构

```java
// 源码: core/src/main/java/org/apache/iceberg/mapping/MappedField.java
public class MappedField implements Serializable {
    private final Set<String> names;              // 支持多个别名
    private Integer id;                           // 对应的 fieldId
    private MappedFields nestedMapping;           // 嵌套字段的映射

    // 工厂方法
    public static MappedField of(Integer id, String name) { ... }
    public static MappedField of(Integer id, Iterable<String> names) { ... }
    public static MappedField of(Integer id, String name, MappedFields nestedMapping) { ... }
    public static MappedField of(Integer id, Iterable<String> names, MappedFields nestedMapping) { ... }
}
```

#### Name Mapping 示例

```json
[
  {"field-id": 1, "names": ["user_name", "username"]},
  {"field-id": 2, "names": ["user_id"]},
  {
    "field-id": 3,
    "names": ["address"],
    "fields": [
      {"field-id": 4, "names": ["city"]},
      {"field-id": 5, "names": ["street"]}
    ]
  }
]
```

#### 自动生成 Name Mapping

Iceberg 提供了从现有 Schema 自动生成 Name Mapping 的能力:

```java
// 源码: core/src/main/java/org/apache/iceberg/mapping/MappingUtil.java
public static NameMapping create(Schema schema) {
    return new NameMapping(TypeUtil.visit(schema, CreateMapping.INSTANCE));
}
```

`CreateMapping` 使用 `TypeUtil.SchemaVisitor` 遍历 Schema，为每个字段创建 `MappedField`（使用字段当前名称和 ID）。

#### 风险与最佳实践

**风险**: 错误的 `NameMapping` 可能导致数据静默错位或置为 `null`

**错误示例**:
```
旧列 user_name --> 错误映射到 --> 新列 account_id 的 fieldId
结果: 数据静默错位，引发数据质量问题
```

**最佳实践**:
- 使用 Spark `migrate` 或 `add_files` 自动生成映射
- 大规模迁移时进行充分的数据抽样验证
- 避免手动编写复杂映射规则

**深入理解**: Name Mapping 是 Iceberg 与传统数仓系统集成的关键机制,通过适配器模式优雅地解决了兼容性问题。但必须谨慎使用,错误的映射可能导致严重的数据质量问题。

#### 面试加分话术

> "Name Mapping 是 Iceberg **零拷贝迁移**的核心技术:
> 1. **原理**: 通过 JSON 格式的名称-ID 映射表，让无 ID 的 Parquet 文件也能被 Iceberg 读取
> 2. **迁移优势**: 无需重写数据文件，只需在元数据层建立映射关系
> 3. **别名支持**: 同一个 fieldId 可映射多个名称，支持列名变更历史
> 4. **路径标准化**: `ApplyNameMapping` 将 list element / map key / map value 标准化为固定名称（element/key/value），确保不同命名约定的文件都能正确映射
>
> 这体现了 Iceberg 的核心设计哲学: **元数据驱动**。通过丰富的元数据层解决兼容性问题，而非要求数据文件重写。"

#### 高频追问

**Q: 如果 Name Mapping 配置错误导致数据错位，有什么恢复手段？**

**A**: Iceberg 提供了安全机制:
1. **不可变数据**: 原始文件未被修改，修正 Name Mapping 后重新读取即可
2. **Schema 校验**: 类型不匹配时会抛出异常（如 STRING 映射到 INT）
3. **回滚能力**: 通过快照可以回滚到正确的元数据版本

**最佳实践**: 迁移前使用 `spark.read.format("iceberg").option("snapshot-id", xxx)` 进行数据抽样验证。

#### 30 秒速记（简版）

- 结论先行：通过 **Name Mapping(名称映射)** 机制桥接列名与 fieldId 的关联。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/mapping/NameMapping.java`（映射定义与查找），再联动 `parquet/src/main/java/org/apache/iceberg/parquet/ApplyNameMapping.java`（Parquet 文件级 ID 注入），最后看 `core/src/main/java/org/apache/iceberg/mapping/MappingUtil.java`（自动生成映射），把"定义-注入-生成"串成一条线。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-1-6"></a>
### 1.6 默认值类型详解

**[绿] 问题: `initial-default` 和 `write-default` 有什么区别？**

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

#### 源码对应

在 `Types.NestedField` 中，两者均以 `Literal<?>` 类型存储:

```java
// 源码: api/src/main/java/org/apache/iceberg/types/Types.java
// NestedField.Builder 中的设置方法
public static class NestedFieldBuilder {
    private Literal<?> initialDefault = null;
    private Literal<?> writeDefault = null;

    public NestedFieldBuilder withInitialDefault(Literal<?> fieldInitialDefault) {
        initialDefault = fieldInitialDefault;
        return this;
    }

    public NestedFieldBuilder withWriteDefault(Literal<?> fieldWriteDefault) {
        writeDefault = fieldWriteDefault;
        return this;
    }

    public NestedField build() {
        return new NestedField(isOptional, id, name, type, doc, initialDefault, writeDefault);
    }
}
```

#### 30 秒速记（简版）

- 结论先行：两者分别处理不同的数据生命周期场景。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/types/Types.java`（NestedField 内部的 `initialDefault` 和 `writeDefault` 字段），理解它们分别在读路径（ConstantReader）和写路径（引擎注入）中被使用。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-1-7"></a>
### 1.7 ConstantReader 实现

**[黄] 问题: Iceberg 如何在不进行 I/O 的情况下实现读路径默认值填充？**

**答案:**

通过 **ConstantReader(常量读取器)** 机制实现零 I/O 填充。

#### 实现流程

1. **Schema 对齐**: Reader 对比 Table Schema 与 File Schema
2. **识别缺失列**: 发现列在文件中不存在但有 `initial-default`
3. **构造 ConstantReader**: 不创建物理 `ColumnReader`，而是实例化虚拟列读取器
4. **零 I/O 填充**: 直接在内存中为每行返回预定义的常量值

#### Parquet 中的 ConstantReader 源码

```java
// 源码: parquet/src/main/java/org/apache/iceberg/parquet/ParquetValueReaders.java
public static <C> ParquetValueReader<C> constant(C value) {
    return new ConstantReader<>(value);
}

public static <C> ParquetValueReader<C> constant(C value, int definitionLevel) {
    return new ConstantReader<>(value, definitionLevel);
}

private static class ConstantReader<C> implements ParquetValueReader<C> {
    private final C constantValue;
    private final TripleIterator<?> column;
    private final List<TripleIterator<?>> children;

    ConstantReader(C constantValue) {
        this.constantValue = constantValue;
        this.column = NullReader.NULL_COLUMN;      // 不关联任何物理列
        this.children = NullReader.COLUMNS;
    }

    ConstantReader(C constantValue, int parentDl) {
        this.constantValue = constantValue;
        this.column = new ConstantDLColumn<>(parentDl);  // 支持 definition level
        this.children = ImmutableList.of(column);
    }

    @Override
    public C read(C reuse) {
        return constantValue;  // 直接返回常量，零 I/O
    }
    // ...
}
```

类似的 ConstantReader 实现也存在于其他格式模块中:
- `core/src/main/java/org/apache/iceberg/avro/ValueReaders.java` (Avro 格式)
- `orc/src/main/java/org/apache/iceberg/orc/OrcValueReaders.java` (ORC 格式)
- `arrow/src/main/java/org/apache/iceberg/arrow/vectorized/VectorizedArrowReader.java` (Arrow 向量化)

**核心优势**: 将"虚拟列"的概念引入查询引擎层，避免了数据文件的物理修改。不同文件格式各自实现了对应的 ConstantReader，但核心思想一致：直接返回常量值，不进行任何磁盘 I/O。

#### 30 秒速记（简版）

- 结论先行：通过 **ConstantReader(常量读取器)** 机制实现零 I/O 填充。
- 主链路：先定位 `parquet/src/main/java/org/apache/iceberg/parquet/ParquetValueReaders.java`（`ConstantReader` 内部类），理解其 `read()` 方法直接返回常量值，不关联任何物理列读取器。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-1-8"></a>
### 1.8 默认值职责分离

**[橙] 问题: 为什么默认值填充由引擎层(Spark/Flink)完成，而非 Iceberg 核心库？**

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

#### 读路径 vs 写路径的职责划分

| 路径 | 负责方 | 机制 |
|------|--------|------|
| **读路径** (`initial-default`) | Iceberg Reader 层 | `ConstantReader`，在各文件格式模块中实现 |
| **写路径** (`write-default`) | 计算引擎(Spark/Flink) | 在逻辑计划/分析器中注入默认值表达式 |

**补充说明**: 读路径的 `ConstantReader` 虽然在 Iceberg 的文件格式模块中实现，但它是作为 Reader 构建过程的一部分，而非在 Core 层处理。写路径的默认值填充则完全由引擎层的分析器/优化器负责，Iceberg Core 只负责定义和存储默认值元数据。

#### 30 秒速记（简版）

- 结论先行：基于**职责解耦**和**计算下推**的设计原则。
- 主链路：Iceberg Core（`Types.NestedField`）只负责存储默认值定义，读路径由格式模块的 `ConstantReader` 处理，写路径由引擎层的分析器处理。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-1-9"></a>
### 1.9 Hive 迁移与默认值

**[红] 问题: 迁移 Hive 表到 Iceberg 时，如何配合 Name Mapping 处理默认值？**

**答案:**

Name Mapping 与默认值机制协同工作，处理历史数据的列缺失问题。

#### 协同流程

1. **建立 Name Mapping**: 配置 `schema.name-mapping.default`，映射列名到 `fieldId`（可通过 `MappingUtil.create(schema)` 自动生成）
2. **识别缺失列**: 应用映射后，若文件中仍缺少某列(新增列)
3. **触发默认值填充**: 回退到 `initial-default` 机制，通过 `ConstantReader` 填充

#### 详细流程链

```
Hive Parquet 文件（无 field ID）
       |
       v
ParquetSchemaUtil.hasIds() --> false
       |
       v
ParquetSchemaUtil.applyNameMapping(fileSchema, nameMapping)
       |
       v
ApplyNameMapping 遍历文件 Schema，根据列名注入 field ID
       |
       v
Reader 对比 Table Schema 与 File Schema
       |
       +-- 列已映射且存在 --> 正常读取
       |
       +-- 列在文件中不存在（新增列）
              |
              v
           检查 initial-default
              |
              +-- 有默认值 --> ConstantReader 填充
              |
              +-- 无默认值 --> 返回 null
```

#### 关键风险

**强依赖 Name Mapping 的正确性**

错误示例:
```
旧列 user_name --> 错误映射到 --> 新列 account_id 的 fieldId
结果: 数据静默错位，引发数据质量问题
```

**最佳实践**:
- 优先使用 `MappingUtil.create(schema)` 或 Spark `migrate`/`add_files` 自动生成映射
- 进行严格的数据抽样对比验证
- 小批量测试后再全量迁移
- 注意 `ApplyNameMapping` 会标准化 list/map 子字段名称（element/key/value），确保映射规则与此一致

#### 30 秒速记（简版）

- 结论先行：Name Mapping 与默认值机制协同工作，处理历史数据的列缺失问题。
- 主链路：先 `ParquetSchemaUtil.applyNameMapping()` 注入 field ID，再由 Reader 对比 Schema 识别缺失列，最后 `ConstantReader` 使用 `initial-default` 填充。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
## 二、事务与并发控制
<a id="q-2-1"></a>
### 2.1 ACID 特性实现

**问题: Iceberg 如何在高并发场景下保证 ACID 特性中的原子性和隔离性？**

**答案:**

通过**乐观并发控制(OCC)** 和**快照机制**实现。

#### 原子性(Atomicity)

**核心机制**: 元数据文件的原子替换

1. 不直接修改现有元数据文件
2. 创建新的元数据文件(包含完整的新快照信息)
3. 通过底层文件系统/Catalog 的原子操作(如 HDFS 的 `rename`、Hive Metastore 的表级锁、Nessie 的 CAS 操作)更新指针

**结果**: 提交要么完全成功，要么完全失败

#### BaseTransaction 源码实现

```java
// 源码: core/src/main/java/org/apache/iceberg/BaseTransaction.java
public class BaseTransaction implements Transaction {
    // 事务类型枚举
    enum TransactionType {
        CREATE_TABLE,
        REPLACE_TABLE,
        CREATE_OR_REPLACE_TABLE,
        SIMPLE
    }

    private final String tableName;
    private final TableOperations ops;           // 底层表操作(与实际 Catalog 交互)
    private final TransactionTable transactionTable;
    private final TableOperations transactionOps; // 事务内部的虚拟 TableOperations
    private final List<PendingUpdate> updates;   // 待提交的更新列表
    private final TransactionType type;
    private TableMetadata base;                  // 基线元数据(事务开始时的元数据)
    private TableMetadata current;               // 当前元数据(应用更新后的元数据)
    private boolean hasLastOpCommitted;

    /**
     * 提交事务(根据事务类型分派)
     */
    @Override
    public void commitTransaction() {
        Preconditions.checkState(
            hasLastOpCommitted, "Cannot commit transaction: last operation has not committed");

        switch (type) {
            case CREATE_TABLE:
                commitCreateTransaction();
                break;
            case REPLACE_TABLE:
                commitReplaceTransaction(false);
                break;
            case CREATE_OR_REPLACE_TABLE:
                commitReplaceTransaction(true);
                break;
            case SIMPLE:
                commitSimpleTransaction();
                break;
        }
    }

    /**
     * 简单事务提交(带重试的乐观并发控制)
     */
    private void commitSimpleTransaction() {
        // 如果没有变更，不尝试提交
        if (base == current) {
            return;
        }

        try {
            Tasks.foreach(ops)
                .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
                .exponentialBackoff(...)  // 指数退避
                .onlyRetryOn(CommitFailedException.class)
                .run(underlyingOps -> {
                    // 关键: 检测并发变更，重新应用所有更新
                    applyUpdates(underlyingOps);

                    // 关键: 乐观锁 - 提交 base 与 current
                    underlyingOps.commit(base, current);
                });
        } catch (CommitStateUnknownException e) {
            // 提交状态未知，不做清理，直接抛出
            throw e;
        } catch (PendingUpdateFailedException e) {
            // PendingUpdate 重新应用失败(如冲突无法解决)
            cleanUpOnCommitFailure();
            throw e.wrapped();
        } catch (RuntimeException e) {
            if (!ops.requireStrictCleanup() || e instanceof CleanableFailure) {
                cleanUpOnCommitFailure();
            }
            throw e;
        }
    }

    /**
     * 应用更新(冲突检测与重新应用)
     * 注意: 如果 base 元数据已被并发修改，则刷新 base 并重新应用所有 PendingUpdate
     */
    private void applyUpdates(TableOperations underlyingOps) {
        if (base != underlyingOps.refresh()) {
            // 检测到元数据变化，需要在新的 base 上重新应用更新
            this.base = underlyingOps.current();
            this.current = underlyingOps.current();

            for (PendingUpdate update : updates) {
                // 重新执行每个更新(可能因冲突无法重新应用而抛出异常)
                try {
                    update.commit();
                } catch (CommitFailedException e) {
                    throw new PendingUpdateFailedException(e);
                }
            }
        }
    }
}
```

**源码验证要点**:
- `BaseTransaction` 有四种事务类型: `CREATE_TABLE`、`REPLACE_TABLE`、`CREATE_OR_REPLACE_TABLE`、`SIMPLE`
- `commitSimpleTransaction()` 中有 `base == current` 的短路判断，无变更则不提交
- `applyUpdates()` 中对 `PendingUpdate.commit()` 的 `CommitFailedException` 会被包装为 `PendingUpdateFailedException`，用于中断重试循环(因为即使重试也无法解决的冲突)
- 异常处理区分 `CommitStateUnknownException`(不清理)、`PendingUpdateFailedException`(清理并抛出内部异常)、一般 `RuntimeException`(根据 `requireStrictCleanup` 决定是否清理)

#### 乐观并发控制(OCC)流程

```
线程 A                          线程 B
|                              |
+-- 1. 读取 base (v1)          +-- 1. 读取 base (v1)
|                              |
+-- 2. 应用更新 A              +-- 2. 应用更新 B
|   current = v2              |   current = v2'
|                              |
+-- 3. commit(v1, v2)          |
|   成功,表版本变为 v2         |
|                              |
|                              +-- 3. commit(v1, v2')
|                              |   失败! base 已是 v2
|                              |
|                              +-- 4. 重试: refresh()
|                              |   base = v2
|                              |
|                              +-- 5. 重新应用更新 B
|                              |   current = v3
|                              |
|                              +-- 6. commit(v2, v3)
|                              |   成功
```

**关键设计**:
- **无锁读取**: 读操作不需要锁，直接读取快照
- **提交时验证**: 仅在 commit 时检查冲突
- **自动重试**: 冲突时自动 `refresh()` 并重新应用更新

#### 隔离性(Isolation)

**核心机制**: 快照隔离(Snapshot Isolation)

1. 每个事务基于特定快照操作
2. 读事务确定快照版本后，新快照对其不可见
3. 读操作永不被写操作阻塞

#### 设计模式分析: 命令模式(Command Pattern)

**PendingUpdate 命令接口**:

```java
// 源码: api/src/main/java/org/apache/iceberg/PendingUpdate.java
public interface PendingUpdate<T> {
    T apply();     // 应用变更并返回未提交的结果(用于验证)
    void commit(); // 应用变更并提交
}

// 具体命令实现(注意: 它们实现的是 PendingUpdate 的子接口)
class SchemaUpdate implements UpdateSchema { ... }
// UpdateSchema extends PendingUpdate<Schema>

class FastAppend extends SnapshotProducer<AppendFiles> implements AppendFiles { ... }
// AppendFiles extends SnapshotUpdate<AppendFiles>
// SnapshotUpdate<ThisT> extends PendingUpdate<Snapshot>

class BaseRewriteFiles extends MergingSnapshotProducer<RewriteFiles> implements RewriteFiles { ... }
// RewriteFiles extends SnapshotUpdate<RewriteFiles>
```

**命令模式优势**:

| 优势 | 说明 |
|------|------|
| **解耦请求与执行** | Transaction 不需知道具体操作类型 |
| **支持撤销/重做** | 冲突时可重新执行命令列表 |
| **组合操作** | 多个命令可组合成事务 |
| **延迟执行** | 在事务内 `commit()` 只更新本地 `current`，真正提交在 `commitTransaction()` |

**命令模式应用**:

```java
// 事务中的命令队列(BaseTransaction.updates)
List<PendingUpdate> updates = [
    new SchemaUpdate(transactionOps),   // 命令 1: 更新 Schema
    new FastAppend(tableName, transactionOps),  // 命令 2: 追加数据
    new BaseRewriteFiles(tableName, transactionOps) // 命令 3: 重写文件
];

// 重新应用所有命令(在 applyUpdates 中)
for (PendingUpdate update : updates) {
    update.commit();  // 多态调用，每个命令更新 transactionOps 中的 current
}
```

**注意**: 事务内的 `commit()` 调用的是 `TransactionTableOperations.commit()`，它只是更新内存中的 `current` 元数据，并不会真正写入 Catalog。真正的原子提交发生在 `commitSimpleTransaction()` 中的 `underlyingOps.commit(base, current)`。

#### 隔离级别的边界

**快照隔离(SI) 不等于 可串行化(Serializable)**

存在**写偏斜(Write Skew)** 异常:

```
规则: A + B >= 0
T1 读取 A=50, B=50 -> 执行 A=-40
T2 读取 A=50, B=50 -> 执行 B=-40
结果: A=-40, B=-40 (违反约束)
```

**深入理解**: SI 适用于绝大多数场景，但需要了解其边界，在关键业务逻辑中额外验证约束。命令模式使得事务系统具有良好的可扩展性和可维护性。

#### 面试加分话术

> "Iceberg 通过 OCC 乐观并发控制实现 ACID: 原子性依靠元数据文件的原子替换(具体机制取决于 Catalog 实现，如 HDFS rename、Hive Metastore 锁等)，隔离性通过快照机制实现(每次读取看到一致的快照)。事务内部使用命令模式，将 SchemaUpdate、AppendFiles 等操作封装为 PendingUpdate，在事务内仅更新本地元数据，最后通过 commitSimpleTransaction 原子提交。需要注意 Iceberg 提供快照隔离而非可串行化，存在写偏斜可能。另外，applyUpdates 中对 PendingUpdate 重新应用失败会包装为 PendingUpdateFailedException 以中断重试循环，这是一个精妙的设计。"

#### 高频追问

**Q: Iceberg 如何处理事务提交失败后的清理？**

**A**: Iceberg 区分三类异常: (1) `CommitStateUnknownException` 状态未知，不做任何清理以避免数据丢失，需人工介入 (2) `CleanableFailure` 可安全清理已写入的数据文件 (3) 普通 `RuntimeException` 则根据 `TableOperations.requireStrictCleanup()` 标志决定是否清理。在 `SnapshotProducer` 的 `commit()` 方法中有对应的 `strictCleanup` 字段控制清理策略。

#### 30 秒速记（简版）

- 结论先行：通过**乐观并发控制(OCC)** 和**快照机制**实现。
- 主链路：`BaseTransaction.commitSimpleTransaction()` -> `Tasks.foreach(ops).run()` -> `applyUpdates()` 刷新 base 并重新应用 PendingUpdate -> `underlyingOps.commit(base, current)` 原子提交。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-2-2"></a>
### 2.2 并发写入冲突处理

**问题: 两个 Flink 作业同时向一个 Iceberg 表写入时会发生什么？**

**答案:**

结果取决于操作类型和分区冲突情况。

#### 场景分析

| 场景 | 操作类型 | 分区关系 | 结果 |
|------|----------|----------|------|
| 1 | Append | 不同分区 | 自动合并，都成功 |
| 2 | Append | 同一分区 | 通常成功(FastAppend 无冲突验证) |
| 3 | Overwrite | 同一分区 | 后者可能失败(需启用 `validateNoConflictingData`) |

**注意**: Overwrite 操作的严格冲突检测并非默认行为，需要调用方主动调用 `validateNoConflictingData()` 或 `validateNoConflictingDeletes()` 启用验证。如果未启用验证，Overwrite 也可能成功合并。

#### 冲突检测机制

- **Append (FastAppend)**: 不覆盖 `validate()` 方法，继承 `SnapshotProducer` 的空实现，几乎不做任何冲突检测
- **Overwrite**: 仅在调用方显式启用 `validateNoConflictingData()` / `validateNoConflictingDeletes()` 后才进行严格验证
- **RowDelta**: 同样需要显式启用 `validateNoConflictingDataFiles()` / `validateNoConflictingDeleteFiles()`

**最佳实践**:
- 按分区并行写入，避免同一分区的并发 Overwrite
- 监控 `CommitFailedException`，Iceberg 内置重试机制会自动处理
- 对于需要严格冲突检测的场景，确保调用 `validateNoConflictingData()` 等方法

#### 冲突检测的源码实现

**核心方法**: `MergingSnapshotProducer.validateAddedDataFiles()`

```java
// 源码: core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java
// 该方法有两个重载版本: 基于 Expression 和基于 PartitionSet

// 版本 1: 基于 Expression 过滤器
protected void validateAddedDataFiles(
    TableMetadata base,
    Long startingSnapshotId,
    Expression conflictDetectionFilter,
    Snapshot parent) {

    CloseableIterable<ManifestEntry<DataFile>> conflictEntries =
        addedDataFiles(base, startingSnapshotId, conflictDetectionFilter, null, parent);

    try (CloseableIterator<ManifestEntry<DataFile>> conflicts = conflictEntries.iterator()) {
        if (conflicts.hasNext()) {
            throw new ValidationException(
                "Found conflicting files that can contain records matching %s: %s",
                conflictDetectionFilter,
                Iterators.toString(
                    Iterators.transform(conflicts, entry -> entry.file().location().toString())));
        }
    } catch (IOException e) {
        throw new UncheckedIOException(...);
    }
}

// 版本 2: 基于 PartitionSet
protected void validateAddedDataFiles(
    TableMetadata base, Long startingSnapshotId, PartitionSet partitionSet, Snapshot parent) {
    // 类似逻辑，使用 PartitionSet 进行分区级别过滤
}
```

**注意**: 冲突检测抛出的是 `ValidationException` 而非 `CommitFailedException`。`ValidationException` 会在 `SnapshotProducer.commit()` 的重试框架中被捕获，由于它不是 `CommitFailedException` 的子类，因此**不会触发重试**，会直接导致提交失败。

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
           .caseSensitive(caseSensitive)
           .filterManifestEntries(entry -> newSnapshots.contains(entry.snapshotId()))
           .specsById(base.specsById())
           .ignoreDeleted()    // 忽略已删除的文件
           .ignoreExisting();  // 忽略已存在的文件

   // 如果提供了冲突检测过滤器，进一步过滤
   if (dataFilter != null) {
       manifestGroup = manifestGroup.filterData(dataFilter);
   }

   // 如果提供了分区集合，进一步过滤
   if (partitionSet != null) {
       manifestGroup = manifestGroup.filterManifestEntries(
           entry -> partitionSet.contains(entry.file().specId(), entry.file().partition()));
   }
   ```

3. **检查冲突**:
   - 如果找到任何匹配的文件，抛出 `ValidationException`
   - 注意: 这里抛出的是 `ValidationException`，不是 `CommitFailedException`

#### 不同操作的冲突检测策略

**Append 操作 (FastAppend)**:

```java
// 源码: core/src/main/java/org/apache/iceberg/FastAppend.java
// FastAppend 继承自 SnapshotProducer，没有覆盖 validate() 方法
// SnapshotProducer 的默认 validate() 实现为空方法:
//   protected void validate(TableMetadata currentMetadata, Snapshot snapshot) {}
// 因此 FastAppend 不做任何冲突检测，两个 Append 操作几乎总是可以自动合并
```

**注意**: `MergeAppend` (通过 `table.newAppend()` 创建) 也继承自 `MergingSnapshotProducer`，但同样没有在 `validate()` 中添加冲突检测逻辑。Append 操作的设计哲学是: 添加新文件不会与其他操作冲突。

**Overwrite 操作**:

```java
// 源码: core/src/main/java/org/apache/iceberg/BaseOverwriteFiles.java
@Override
protected void validate(TableMetadata base, Snapshot parent) {
    // 1. 可选: 验证新增文件匹配覆盖过滤器
    if (validateAddedFilesMatchOverwriteFilter) {
        // 验证所有新增文件都匹配 overwrite 的行过滤器
    }

    // 2. 仅在调用了 validateNoConflictingData() 后才验证
    if (validateNewDataFiles) {
        validateAddedDataFiles(base, startingSnapshotId, dataConflictDetectionFilter(), parent);
    }

    // 3. 仅在调用了 validateNoConflictingDeletes() 后才验证
    if (validateNewDeletes) {
        if (rowFilter() != Expressions.alwaysFalse()) {
            Expression filter = conflictDetectionFilter != null ? conflictDetectionFilter : rowFilter();
            validateNoNewDeleteFiles(base, startingSnapshotId, filter, parent);
            validateDeletedDataFiles(base, startingSnapshotId, filter, parent);
        }

        if (!deletedDataFiles.isEmpty()) {
            validateNoNewDeletesForDataFiles(
                base, startingSnapshotId, conflictDetectionFilter, deletedDataFiles, parent);
        }
    }
}
```

**重要**: `validateNewDataFiles` 和 `validateNewDeletes` 默认为 `false`。只有当调用方显式调用 `validateNoConflictingData()` 和 `validateNoConflictingDeletes()` 时才会设为 `true`。这意味着 Overwrite 的严格冲突检测是**可选的、需要调用方主动启用的**。

**RowDelta 操作** (UPDATE/DELETE):

```java
// 源码: core/src/main/java/org/apache/iceberg/BaseRowDelta.java
@Override
protected void validate(TableMetadata base, Snapshot parent) {
    if (parent != null) {
        // 验证 startingSnapshotId 的祖先关系
        if (startingSnapshotId != null) {
            Preconditions.checkArgument(
                SnapshotUtil.isAncestorOf(parent.snapshotId(), startingSnapshotId, base::snapshot),
                "Snapshot %s is not an ancestor of %s", startingSnapshotId, parent.snapshotId());
        }

        // 验证引用的数据文件仍然存在
        if (!referencedDataFiles.isEmpty()) {
            validateDataFilesExist(
                base, startingSnapshotId, referencedDataFiles,
                !validateDeletes, conflictDetectionFilter, parent);
        }

        // 仅在调用了 validateNoConflictingDataFiles() 后才验证
        if (validateNewDataFiles) {
            validateAddedDataFiles(base, startingSnapshotId, conflictDetectionFilter, parent);
        }

        // 仅在调用了 validateNoConflictingDeleteFiles() 后才验证
        if (validateNewDeleteFiles) {
            if (!removedDataFiles.isEmpty()) {
                validateNoNewDeletesForDataFiles(
                    base, startingSnapshotId, conflictDetectionFilter, removedDataFiles, parent);
            }
            validateNoNewDeleteFiles(base, startingSnapshotId, conflictDetectionFilter, parent);
        }

        // 验证没有冲突的文件删除与位置删除
        validateNoConflictingFileAndPositionDeletes();

        // 验证没有并发添加的 DV(Deletion Vector)
        validateAddedDVs(base, startingSnapshotId, conflictDetectionFilter, parent);
    }
}
```

**注意**: RowDelta 中 `referencedDataFiles` 的验证是通过 `validateDataFilesExist()` 实现的，它检查的是文件是否被并发删除(而非并发新增)。`validateAddedDVs()` 是一个无条件执行的验证，防止对同一数据文件并发添加 DV。

#### 冲突检测的粒度控制

**分区级别冲突检测**:

```java
// 使用 PartitionSet 进行分区级别的冲突检测
// 在 MergingSnapshotProducer 中通过 validateAddedDataFiles 的 PartitionSet 重载版本实现
PartitionSet partitionSet = PartitionSet.create(specsById);
for (DataFile file : filesToReplace) {
    partitionSet.add(file.specId(), file.partition());
}

// 仅检查这些分区是否有新增文件
validateAddedDataFiles(base, startingSnapshotId, partitionSet, parent);
```

**行级别冲突检测**:

```java
// 使用 Expression 进行行级别的冲突检测
// 通过 conflictDetectionFilter() 方法设置
Expression conflictFilter = Expressions.and(
    Expressions.equal("user_id", 123),
    Expressions.greaterThan("timestamp", "2024-01-01")
);

// 仅检查匹配此条件的新增文件(基于文件的统计信息进行过滤)
validateAddedDataFiles(base, startingSnapshotId, conflictFilter, parent);
```

#### 重试机制

重试机制存在于两个层面:

**层面 1: 单个操作的重试 (SnapshotProducer.commit())**

```java
// 源码: core/src/main/java/org/apache/iceberg/SnapshotProducer.java
Tasks.foreach(ops)
    .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))  // 默认 4 次
    .exponentialBackoff(
        base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),  // 100ms
        base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),  // 60000ms(1分钟)
        base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),  // 1800000ms(30分钟)
        2.0  // 指数退避因子
    )
    .onlyRetryOn(CommitFailedException.class)
    .run(taskOps -> {
        Snapshot newSnapshot = apply();  // 调用子类的 apply() 生成新快照
        // ...构建新的 TableMetadata...
        taskOps.commit(base, updated.withUUID());
    });
```

**层面 2: 事务的重试 (BaseTransaction.commitSimpleTransaction())**

```java
// 源码: core/src/main/java/org/apache/iceberg/BaseTransaction.java
Tasks.foreach(ops)
    .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
    .exponentialBackoff(...)
    .onlyRetryOn(CommitFailedException.class)
    .run(underlyingOps -> {
        // 1. 刷新元数据并重新应用所有 PendingUpdate
        applyUpdates(underlyingOps);

        // 2. 提交(注意: 这里直接调用 underlyingOps.commit，不经过 SnapshotProducer 的重试)
        underlyingOps.commit(base, current);
    });
```

**重试流程**:

```
Attempt 1: 提交失败 (CommitFailedException)
           | 等待 100ms
Attempt 2: 刷新元数据 -> 重新应用更新 -> 提交失败
           | 等待 200ms
Attempt 3: 刷新元数据 -> 重新应用更新 -> 提交失败
           | 等待 400ms
Attempt 4: 刷新元数据 -> 重新应用更新 -> 提交成功/失败
```

**关键区别**:
- `SnapshotProducer.commit()` 中的重试只重新调用 `apply()`(重新计算 Manifest 列表)，不重新执行 `validate()`(validate 在 apply 内部被调用)
- `BaseTransaction.commitSimpleTransaction()` 中的重试会重新应用整个 PendingUpdate 链

#### 并发场景分析

**场景 1: 两个 Append 操作(不同分区)**

```
Job A: Append to partition dt=2024-01-01
Job B: Append to partition dt=2024-01-02

时间线:
T1: Job A 读取 Snapshot 100
T2: Job B 读取 Snapshot 100
T3: Job A 提交 -> Snapshot 101 (成功)
T4: Job B 提交 -> CommitFailedException (base 已变)
    -> 重试: refresh() 获取新 base -> 重新 apply() -> 提交 -> Snapshot 102 (成功)

原因: FastAppend 不做冲突验证，重试后 apply() 只是在新的 Manifest 列表上追加新文件
```

**场景 2: 两个 Overwrite 操作(同一分区，启用了 validateNoConflictingData)**

```
Job A: Overwrite partition dt=2024-01-01 (validateNoConflictingData)
Job B: Overwrite partition dt=2024-01-01 (validateNoConflictingData)

时间线:
T1: Job A 读取 Snapshot 100
T2: Job B 读取 Snapshot 100
T3: Job A 提交 -> Snapshot 101 (成功)
T4: Job B 提交 -> CommitFailedException -> 重试
    -> 重试: refresh() 获取新 base
    -> apply() 内部调用 validate()
    -> validateAddedDataFiles() 检测到 Job A 添加的文件
    -> 抛出 ValidationException (不是 CommitFailedException，不会继续重试)
    -> 最终失败

原因: Overwrite 在 validate() 中检测到并发新增的文件，抛出 ValidationException
```

**场景 3: 两个 Overwrite 操作(同一分区，未启用验证)**

```
Job A: Overwrite partition dt=2024-01-01 (未调用 validateNoConflictingData)
Job B: Overwrite partition dt=2024-01-01 (未调用 validateNoConflictingData)

时间线:
T1: Job A 读取 Snapshot 100
T2: Job B 读取 Snapshot 100
T3: Job A 提交 -> Snapshot 101 (成功)
T4: Job B 提交 -> CommitFailedException -> 重试
    -> refresh() -> apply() -> validate() 为空 -> 提交 -> Snapshot 102 (成功)

原因: 未启用验证时，Overwrite 行为类似 Append，不检查并发冲突
注意: 这可能导致数据不一致(Job A 删除的文件可能在 Job B 的视图中仍然存在)
```

**深入理解**: Iceberg 的冲突检测是**可选且可配置的**，不同操作类型的默认验证策略不同。FastAppend 最宽松(无验证)，Overwrite 和 RowDelta 需要调用方显式启用验证。理解"验证默认未启用"这一点对于正确使用 Iceberg 的并发写入至关重要。

#### 面试加分话术

> "Iceberg 的并发冲突检测是分层且可选的: (1) 底层依赖 `TableOperations.commit()` 的乐观锁(base metadata 比较) (2) 上层通过 `validate()` 方法做业务级验证。重要的是，冲突验证需要调用方主动启用，例如 Overwrite 需要调用 `validateNoConflictingData()`，RowDelta 需要调用 `validateNoConflictingDataFiles()`。核心验证方法是 `validateAddedDataFiles()` 和 `validateNoNewDeleteFiles()`，它们检查从 startingSnapshotId 到当前快照之间是否有冲突的文件变更。另外需要注意，验证失败抛出的是 `ValidationException`，它不属于 `CommitFailedException`，因此不会触发重试。"

#### 高频追问

**Q: 如何避免生产环境中的频繁冲突重试？**

**A**: (1) 按分区隔离不同作业的写入范围，避免写入同一分区 (2) 使用 Append 而非 Overwrite，因为 Append 几乎不会冲突 (3) 调整 `commit.retry.num-retries`(默认4次)和 `commit.retry.min-wait-ms`(默认100ms) (4) 对于 CDC 场景使用单写入者模式避免竞争 (5) 了解 `ValidationException` 和 `CommitFailedException` 的区别，前者不会触发重试。

#### 30 秒速记（简版）

- 结论先行：结果取决于操作类型和是否启用冲突验证。
- 主链路：`SnapshotProducer.commit()` -> `apply()` 内部调用 `validate()` -> 子类覆盖 `validate()` 进行特定验证 -> `MergingSnapshotProducer.validateAddedDataFiles()` 检查并发新增文件。
- 关键点：冲突验证是可选的(`validateNewDataFiles` 默认 `false`)，`ValidationException` 不会触发重试。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-2-3"></a>
### 2.3 Flink 幂等性保证

**问题: Flink 作业重启后，Iceberg Sink 如何保证不重复提交数据？**

**答案:**

通过 **Flink Checkpoint State** 和 **`flink.max-committed-checkpoint-id`** 双重机制保证幂等性。

#### 幂等性机制

1. **State 恢复**: `IcebergFilesCommitter` 将待提交的文件 Manifest 序列化数据保存在 Flink 的 Operator State 中
2. **Checkpoint ID 检查**:
   - 从 Iceberg 快照的 summary 中读取 `flink.max-committed-checkpoint-id`
   - 提交前检查当前 Checkpoint ID 是否大于已提交的最大 ID
   - 若不大于则跳过

#### IcebergFilesCommitter 源码实现

```java
// 源码: flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java
// 注意: 该类不是 RichSinkFunction，而是 AbstractStreamOperator + OneInputStreamOperator
class IcebergFilesCommitter extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<FlinkWriteResult, Void>, BoundedOneInput {

    private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";
    private static final String FLINK_JOB_ID = "flink.job-id";
    private static final String OPERATOR_ID = "flink.operator-id";

    // 核心数据结构: 按 checkpointId 排序的待提交文件(Manifest 序列化字节)
    private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

    // 当前 checkpoint 的写入结果缓存(尚未序列化为 Manifest)
    private final Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot = Maps.newHashMap();

    // Flink State: 存储待提交的 checkpoint -> manifest 映射
    private transient ListState<SortedMap<Long, byte[]>> checkpointsState;
    // Flink State: 存储 Flink job ID(用于跨作业恢复)
    private transient ListState<String> jobIdState;

    private transient long maxCommittedCheckpointId;

    /**
     * 初始化状态(从 Checkpoint 恢复)
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
        this.operatorUniqueId = getRuntimeContext().getOperatorUniqueID();

        this.tableLoader.open();
        this.table = tableLoader.loadTable();

        this.maxCommittedCheckpointId = INITIAL_CHECKPOINT_ID; // -1L

        this.checkpointsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
        this.jobIdState = context.getOperatorStateStore().getListState(JOB_ID_DESCRIPTOR);

        if (context.isRestored()) {
            // 从 State 恢复 Flink job ID
            String restoredFlinkJobId = jobIdState.get().iterator().next();

            // 从 Iceberg 快照中查找该 job ID 对应的最大已提交 checkpoint ID
            this.maxCommittedCheckpointId =
                SinkUtil.getMaxCommittedCheckpointId(
                    table, restoredFlinkJobId, operatorUniqueId, branch);

            // 恢复未提交的 checkpoint 数据(仅保留大于 maxCommittedCheckpointId 的)
            NavigableMap<Long, byte[]> uncommittedDataFiles =
                Maps.newTreeMap(checkpointsState.get().iterator().next())
                    .tailMap(maxCommittedCheckpointId, false);

            if (!uncommittedDataFiles.isEmpty()) {
                // 立即提交所有未提交的数据
                long maxUncommittedCheckpointId = uncommittedDataFiles.lastKey();
                commitUpToCheckpoint(
                    uncommittedDataFiles, restoredFlinkJobId,
                    operatorUniqueId, maxUncommittedCheckpointId);
            }
        }
    }

    /**
     * Checkpoint 快照回调 - 将 WriteResult 序列化为 Manifest 并保存到 State
     */
    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        long checkpointId = context.getCheckpointId();

        // 将当前 checkpoint 的 WriteResult 序列化为 DeltaManifests(字节数组)
        writeToManifestUptoLatestCheckpoint(checkpointId);

        // 保存到 Flink State
        checkpointsState.clear();
        checkpointsState.add(dataFilesPerCheckpoint);

        jobIdState.clear();
        jobIdState.add(flinkJobId);
    }

    /**
     * Checkpoint 完成回调 - 提交数据到 Iceberg
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        // 关键: 仅在 checkpointId > maxCommittedCheckpointId 时才提交(幂等性保证)
        if (checkpointId > maxCommittedCheckpointId) {
            commitUpToCheckpoint(
                dataFilesPerCheckpoint, flinkJobId, operatorUniqueId, checkpointId);
            this.maxCommittedCheckpointId = checkpointId;
        } else {
            LOG.info("Skipping committing checkpoint {}. {} is already committed.",
                     checkpointId, maxCommittedCheckpointId);
        }

        // 重新加载表(可能有新配置)
        this.table = tableLoader.loadTable();
    }

    /**
     * 实际提交逻辑: 将所有 <= checkpointId 的未提交数据一次性提交
     */
    private void commitUpToCheckpoint(
        NavigableMap<Long, byte[]> deltaManifestsMap,
        String newFlinkJobId, String operatorId, long checkpointId) throws IOException {

        NavigableMap<Long, byte[]> pendingMap = deltaManifestsMap.headMap(checkpointId, true);
        // 反序列化 DeltaManifests -> WriteResult
        NavigableMap<Long, WriteResult> pendingResults = Maps.newTreeMap();
        for (Map.Entry<Long, byte[]> e : pendingMap.entrySet()) {
            if (Arrays.equals(EMPTY_MANIFEST_DATA, e.getValue())) {
                continue; // 跳过空数据
            }
            DeltaManifests deltaManifests = /* 反序列化 */ ;
            pendingResults.put(e.getKey(),
                FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io(), table.specs()));
        }

        // 根据是否有 delete 文件选择不同的提交方式
        commitPendingResult(pendingResults, summary, newFlinkJobId, operatorId, checkpointId);
        pendingMap.clear();
    }

    /**
     * 根据数据类型选择提交方式
     */
    private void commitPendingResult(...) {
        if (replacePartitions) {
            // 使用 ReplacePartitions
            replacePartitions(pendingResults, summary, newFlinkJobId, operatorId, checkpointId);
        } else {
            // 使用 commitDeltaTxn
            commitDeltaTxn(pendingResults, summary, newFlinkJobId, operatorId, checkpointId);
        }
    }

    /**
     * Delta 提交(区分纯 Append 和含 Delete 的场景)
     */
    private void commitDeltaTxn(...) {
        if (summary.deleteFilesCount() == 0) {
            // 纯 Append: 使用 AppendFiles，所有 checkpoint 的数据合并为一次提交
            AppendFiles appendFiles = table.newAppend().scanManifestsWith(workerPool);
            for (WriteResult result : pendingResults.values()) {
                Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
            }
            commitOperation(appendFiles, ...);
        } else {
            // 含 Delete: 使用 RowDelta，每个 checkpoint 单独提交
            // 原因: equality-delete 需要正确的序列号语义
            for (Map.Entry<Long, WriteResult> e : pendingResults.entrySet()) {
                RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);
                Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
                Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
                commitOperation(rowDelta, ...);
            }
        }
    }

    /**
     * 统一的提交方法: 设置元数据属性并执行提交
     */
    private void commitOperation(
        SnapshotUpdate<?> operation, CommitSummary summary,
        String description, String newFlinkJobId, String operatorId, long checkpointId) {
        snapshotProperties.forEach(operation::set);
        // 关键: 在 snapshot summary 中记录 checkpoint 信息
        operation.set(MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
        operation.set(FLINK_JOB_ID, newFlinkJobId);
        operation.set(OPERATOR_ID, operatorId);
        operation.toBranch(branch);

        operation.commit();  // 原子提交
    }
}
```

**源码验证要点**:
- `IcebergFilesCommitter` **不是** `RichSinkFunction`，而是 `AbstractStreamOperator<Void>`，实现 `OneInputStreamOperator<FlinkWriteResult, Void>` 和 `BoundedOneInput`
- State 不是 `ListState<WriteResult>`，而是 `ListState<SortedMap<Long, byte[]>>`(checkpoint ID -> DeltaManifests 序列化字节)
- 额外维护了 `jobIdState`(ListState<String>)用于跨作业恢复
- `notifyCheckpointComplete()` 中的判断条件是 `checkpointId > maxCommittedCheckpointId`(严格大于，非小于等于)
- 提交时区分纯 Append(合并为一次 AppendFiles)和含 Delete(逐 checkpoint RowDelta 提交)两种模式
- 除了 `FLINK_JOB_ID` 和 `MAX_COMMITTED_CHECKPOINT_ID`，还记录了 `OPERATOR_ID`

#### 幂等性保证流程

```
正常场景:
+-----------------------------------------------+
| Checkpoint 1 完成                               |
|   -> notifyCheckpointComplete(1)                |
|   -> 检查: 1 > maxCommittedCheckpointId(-1)     |
|   -> 提交数据                                   |
|   -> 在 snapshot summary 设置:                   |
|      flink.max-committed-checkpoint-id = 1      |
|      flink.job-id = xxx                         |
|      flink.operator-id = yyy                    |
|   -> 更新 maxCommittedCheckpointId = 1          |
+-----------------------------------------------+

重启场景(从 Checkpoint 1 恢复):
+-----------------------------------------------+
| initializeState()                               |
|   -> 从 jobIdState 恢复 restoredFlinkJobId      |
|   -> SinkUtil.getMaxCommittedCheckpointId()     |
|      遍历 Iceberg 快照链,找到该 job 的最大 ID    |
|      -> maxCommittedCheckpointId = 1            |
|   -> 从 checkpointsState 恢复未提交数据          |
|   -> tailMap(1, false) = 空 -> 无需补提交       |
|                                                 |
| notifyCheckpointComplete(2)                     |
|   -> 2 > 1, 提交 checkpoint 2 的数据            |
+-----------------------------------------------+

Checkpoint 乱序场景:
+-----------------------------------------------+
| snapshotState(ckpId=1)                          |
| snapshotState(ckpId=2)                          |
| notifyCheckpointComplete(2)                     |
|   -> 2 > -1, 提交 <= 2 的所有数据(包含 ckp 1) |
| notifyCheckpointComplete(1)                     |
|   -> 1 <= 2(maxCommittedCheckpointId), 跳过    |
+-----------------------------------------------+
```

#### Flink Sink 算子链架构

```
IcebergStreamWriter                    IcebergFilesCommitter
(每个 subtask 一个)                   (parallelism=1 或多个)
+---------------------------+          +---------------------------+
| processElement(record)    |          | processElement(FlinkWR)   |
|   -> writer.write(record) |          |   -> 缓存到              |
|                           |          |      writeResultsSince... |
| prepareSnapshotPreBarrier |          |                           |
|   -> writer.complete()    |   emit   | snapshotState()           |
|   -> emit FlinkWriteResult|--------->|   -> 序列化为 Manifest    |
|   -> 创建新 writer        |          |   -> 保存到 State         |
|                           |          |                           |
|                           |          | notifyCheckpointComplete()|
|                           |          |   -> commitUpToCheckpoint |
|                           |          |   -> AppendFiles.commit() |
+---------------------------+          +---------------------------+
```

**关键流程**:
1. `IcebergStreamWriter.prepareSnapshotPreBarrier()` 在 Barrier 到达时刷新数据，发送 `FlinkWriteResult` 下游
2. `IcebergFilesCommitter.snapshotState()` 将 WriteResult 序列化为 DeltaManifests 并保存到 State
3. `IcebergFilesCommitter.notifyCheckpointComplete()` 在 Checkpoint 完成后执行真正的 Iceberg 提交

#### getMaxCommittedCheckpointId 的实现

```java
// 源码: flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/SinkUtil.java
static long getMaxCommittedCheckpointId(
    Table table, String flinkJobId, String operatorId, String branch) {
    Snapshot snapshot = table.snapshot(branch);
    long lastCommittedCheckpointId = INITIAL_CHECKPOINT_ID; // -1L

    while (snapshot != null) {
        Map<String, String> summary = snapshot.summary();
        String snapshotFlinkJobId = summary.get(FLINK_JOB_ID);
        String snapshotOperatorId = summary.get(OPERATOR_ID);
        // 匹配 job ID 和 operator ID
        if (flinkJobId.equals(snapshotFlinkJobId)
            && (snapshotOperatorId == null || snapshotOperatorId.equals(operatorId))) {
            String value = summary.get(MAX_COMMITTED_CHECKPOINT_ID);
            if (value != null) {
                lastCommittedCheckpointId = Long.parseLong(value);
                break;
            }
        }
        // 沿快照链向前遍历
        Long parentSnapshotId = snapshot.parentId();
        snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }

    return lastCommittedCheckpointId;
}
```

**注意**: 该方法不只是读取当前快照的 summary，而是**沿快照链向前遍历**，找到匹配 job ID 和 operator ID 的最近一个快照。这保证了即使有其他作业在中间提交了快照，也能正确找到当前作业的最大已提交 checkpoint ID。

#### 关键元数据

```json
{
  "snapshot-id": 123456,
  "summary": {
    "flink.job-id": "abc-def-ghi",
    "flink.operator-id": "operator-xyz",
    "flink.max-committed-checkpoint-id": "42"
  }
}
```

**元数据作用**:
- `flink.job-id`: 标识哪个 Flink 作业提交的数据
- `flink.operator-id`: 标识哪个 Committer 算子实例(支持多 Sink 场景)
- `flink.max-committed-checkpoint-id`: 记录最大已提交的 Checkpoint ID

#### 运维风险

**非标准恢复流程的风险**:

错误做法:
```
停止旧作业 -> 启动全新作业(新 job-id，不从 checkpoint/savepoint 恢复)
结果: maxCommittedCheckpointId 重置为 -1，但旧数据可能已提交
```

正确做法:
```
从 Savepoint/Checkpoint 恢复 -> jobIdState 保存了旧 job-id
-> SinkUtil.getMaxCommittedCheckpointId() 用旧 job-id 遍历快照链找到正确的 checkpoint ID
-> 正确跳过已提交的数据
```

**注意**: 即使使用新的 job-id 从 savepoint 恢复，`IcebergFilesCommitter` 也能正确工作，因为它会从 `jobIdState` 中恢复旧的 job-id，并用旧 job-id 去查找 `maxCommittedCheckpointId`。真正危险的是**不从 savepoint 恢复而是全新启动**，这时 State 丢失，无法确定哪些数据已提交。

**深入理解**: 生产环境必须遵循标准恢复流程，否则 Exactly-Once 语义会被破坏。`IcebergFilesCommitter` 的幂等性依赖两个条件同时满足: (1) Flink State 中保存了 job-id (2) Iceberg 快照 summary 中记录了 checkpoint-id。两者缺一不可。

#### 面试加分话术

> "Flink 写入 Iceberg 的幂等性通过双重机制保证: (1) IcebergFilesCommitter 在 Flink State 中保存待提交文件的 Manifest 序列化数据和 job-id (2) 每次提交时在 Iceberg 快照 summary 中记录 flink.max-committed-checkpoint-id。恢复时，从 State 恢复旧 job-id，用 SinkUtil.getMaxCommittedCheckpointId() 沿快照链遍历找到已提交的最大 checkpoint ID，跳过已提交数据。需要注意的是，IcebergFilesCommitter 实际上是 AbstractStreamOperator，不是 RichSinkFunction，它接收 IcebergStreamWriter 发送的 FlinkWriteResult。另外，含 delete 文件时会逐 checkpoint 使用 RowDelta 单独提交，而纯 append 会合并为一次 AppendFiles 提交。"

#### 高频追问

**Q: 如果 Flink 作业的 operator-id 变了(比如修改了 DAG 拓扑)，会发生什么？**

**A**: 如果 `jobIdState` 无法恢复(operator uid 变更导致 state 无法映射)，`IcebergFilesCommitter.initializeState()` 中会打印警告日志并直接返回，`maxCommittedCheckpointId` 保持为 -1。这意味着如果之前的数据已经提交到 Iceberg，可能会导致重复提交。最佳实践是通过 `FlinkSink.Builder.uidPrefix()` 显式设置 operator uid，避免因拓扑变更导致的 state 恢复失败。

**Q: 多个 IcebergFilesCommitter 并行实例会冲突吗？**

**A**: 每个 Committer 实例独立提交，依赖 Iceberg 的乐观并发控制解决冲突。由于使用的是 `AppendFiles`(无冲突验证)，多个实例可以安全并行提交。冲突时通过 `SnapshotProducer` 的重试机制自动解决。

#### 30 秒速记（简版）

- 结论先行：通过 **Flink Operator State** 和 **flink.max-committed-checkpoint-id** 双重机制保证幂等性。
- 主链路：`IcebergStreamWriter.prepareSnapshotPreBarrier()` 刷新数据 -> `IcebergFilesCommitter.snapshotState()` 序列化到 State -> `notifyCheckpointComplete()` 提交到 Iceberg 并记录 checkpoint ID。
- 恢复链路：`initializeState()` -> 恢复 job-id -> `SinkUtil.getMaxCommittedCheckpointId()` 遍历快照链 -> 提交未完成数据 -> 后续 `notifyCheckpointComplete()` 跳过已提交。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
## 三、Flink 流式集成
<a id="q-3-1"></a>
### 3.1 Exactly-Once 语义

**难度: 中等**

**问题: 简述 Flink 写入 Iceberg 时如何实现端到端 Exactly-Once 语义？**

**答案:**

通过**两阶段提交(2PC)** 与 **Flink Checkpoint** 紧密结合实现。

#### 核心组件

Iceberg 的 Flink Sink 存在两套 API 架构:

**传统 API (FlinkSink)**:
- `IcebergStreamWriter`: 并行算子，负责写入数据文件，输出 `FlinkWriteResult`
- `IcebergFilesCommitter`: 单并行度算子，负责提交事务

**新版 SinkV2 API (IcebergSink)**:
- `IcebergSinkWriter`: 实现 `CommittingSinkWriter` 接口，负责写入数据文件
- `IcebergWriteAggregator`: 将多个 `WriteResult` 聚合为 `IcebergCommittable`
- `IcebergCommitter`: 实现 Flink SinkV2 的 `Committer` 接口，负责提交事务

两套 API 的核心提交逻辑相同，区别在于与 Flink 框架的集成方式不同。

#### 简化流程

1. **预提交**: Checkpoint Barrier 到达前，Writer 关闭当前文件并将 `WriteResult`（包含文件元数据）通过 `output.collect()` 发送给下游 Committer
2. **提交**: Checkpoint 全局成功后，Committer 原子性提交到 Iceberg 表

**容错保证**: 失败时从上一个成功 Checkpoint 恢复，不丢失不重复

#### 源码定位

关键源码文件:
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergStreamWriter.java` -- Writer 算子，`prepareSnapshotPreBarrier()` 实现预提交
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java` -- Committer 算子，`notifyCheckpointComplete()` 实现正式提交
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/FlinkSink.java` -- 传统 API 入口，组装 Writer -> Committer 拓扑
- `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergSink.java` -- SinkV2 API 入口

#### 30 秒速记

- 结论先行：通过**两阶段提交(2PC)** 与 **Flink Checkpoint** 紧密结合实现
- 关键类：`IcebergStreamWriter`（并行写入）+ `IcebergFilesCommitter`（单并行度提交）
- 关键方法：`prepareSnapshotPreBarrier()` 预提交 + `notifyCheckpointComplete()` 正式提交

---
<a id="q-3-2"></a>
### 3.2 两阶段提交机制

**难度: 中等**

**问题: 详细说明 Flink-Iceberg 的两阶段提交流程。**

**答案:**

#### 第一阶段: 预提交(Pre-Commit)

**触发**: `IcebergStreamWriter.prepareSnapshotPreBarrier(checkpointId)` 回调(Barrier 到达**前**)

**源码** (`IcebergStreamWriter.java`):
```java
@Override
public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flush(checkpointId);
    this.writer = taskWriterFactory.create();
}

private void flush(long checkpointId) throws IOException {
    if (writer == null) {
        return;
    }
    long startNano = System.nanoTime();
    WriteResult result = writer.complete();
    writerMetrics.updateFlushResult(result);
    output.collect(new StreamRecord<>(new FlinkWriteResult(checkpointId, result)));
    writerMetrics.flushDuration(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));
    writer = null;  // 防止重复 flush
}
```

**动作**:
1. Writer 调用 `writer.complete()` 关闭当前数据文件，生成 `WriteResult`（包含 DataFile 和 DeleteFile 元数据）
2. 将 `FlinkWriteResult(checkpointId, result)` 通过 `output.collect()` 发送给下游 Committer
3. 将 `writer` 设为 null 防止重复 flush
4. 创建新的 TaskWriter，继续接收后续数据

**Committer 的 State 保存** (`IcebergFilesCommitter.snapshotState()`):

Committer 在 `processElement()` 中接收 `FlinkWriteResult`，缓存到 `writeResultsSinceLastSnapshot` Map 中。在 `snapshotState()` 被调用时，将 WriteResult 序列化为 Manifest 文件并存入 `dataFilesPerCheckpoint` (NavigableMap<Long, byte[]>)，再持久化到 Flink State Backend。

```java
// IcebergFilesCommitter.processElement()
public void processElement(StreamRecord<FlinkWriteResult> element) {
    FlinkWriteResult flinkWriteResult = element.getValue();
    List<WriteResult> writeResults =
        writeResultsSinceLastSnapshot.computeIfAbsent(
            flinkWriteResult.checkpointId(), k -> Lists.newArrayList());
    writeResults.add(flinkWriteResult.writeResult());
}

// IcebergFilesCommitter.snapshotState()
public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    long checkpointId = context.getCheckpointId();
    // 将 WriteResult 写入 Manifest 文件并序列化
    writeToManifestUptoLatestCheckpoint(checkpointId);
    // 更新 State
    checkpointsState.clear();
    checkpointsState.add(dataFilesPerCheckpoint);
    jobIdState.clear();
    jobIdState.add(flinkJobId);
}
```

#### 第二阶段: 正式提交(Commit)

**触发**: `IcebergFilesCommitter.notifyCheckpointComplete(checkpointId)` 回调(Checkpoint 全局成功)

**源码** (`IcebergFilesCommitter.java`):
```java
@Override
public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    if (checkpointId > maxCommittedCheckpointId) {
        commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, operatorUniqueId, checkpointId);
        this.maxCommittedCheckpointId = checkpointId;
    } else {
        LOG.info("Skipping committing checkpoint {}. {} is already committed.",
            checkpointId, maxCommittedCheckpointId);
    }
    // 重新加载表元数据以获取最新配置
    this.table = tableLoader.loadTable();
}
```

**提交逻辑** (`commitDeltaTxn`):

Committer 区分两种情况:
1. **仅有数据文件（无删除文件）**: 使用 `table.newAppend()` 提交（兼容 Iceberg V1 格式）
2. **包含删除文件**: 使用 `table.newRowDelta()` 提交（Iceberg V2 格式），且**每个 checkpoint 单独提交**而非合并提交

```java
private void commitDeltaTxn(...) {
    if (summary.deleteFilesCount() == 0) {
        // V1 兼容: 使用 AppendFiles
        AppendFiles appendFiles = table.newAppend().scanManifestsWith(workerPool);
        for (WriteResult result : pendingResults.values()) {
            Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
        }
        commitOperation(appendFiles, summary, "append", ...);
    } else {
        // V2: 使用 RowDelta，每个 checkpoint 独立提交
        // 原因: txn2 的 equality-delete 需要应用到 txn1 的数据文件
        for (Map.Entry<Long, WriteResult> e : pendingResults.entrySet()) {
            RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);
            Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
            Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
            commitOperation(rowDelta, summary, "rowDelta", ...);
        }
    }
}
```

**关键设计**: 当存在删除文件时，不会将多个 checkpoint 的结果合并为一次提交。这是因为对于顺序的 txn1 和 txn2，txn2 的 equality-delete 文件需要应用到 txn1 的数据文件上。如果合并提交，会导致删除语义错误。

**提交元数据**: 每次提交都会在快照的 Summary 中写入:
- `flink.max-committed-checkpoint-id`: 当前 checkpoint ID
- `flink.job-id`: Flink 作业 ID
- `flink.operator-id`: 算子唯一标识

#### 容错场景

| 失败时机 | 状态 | 恢复策略 |
|----------|------|----------|
| 阶段一失败(prepareSnapshotPreBarrier 或 snapshotState 失败) | 文件已写入存储，但 State 未持久化 | 从上一个成功 Checkpoint 恢复，已写文件成为孤儿文件，需后续 `RemoveOrphanFiles` 清理 |
| 阶段二失败(notifyCheckpointComplete 失败) | State 已持久化，但 Iceberg 事务未提交 | 恢复后重新触发提交；通过 `maxCommittedCheckpointId` 比对跳过已提交的 checkpoint |

#### 面试加分话术

> "Flink-Iceberg 两阶段提交的精髓在于: 第一阶段 `prepareSnapshotPreBarrier` 在 Barrier 到达**前**触发,确保当前文件已关闭并将 `WriteResult` 通过 `output.collect()` 发送给 Committer;Committer 在 `snapshotState()` 中将结果序列化为 Manifest 文件并持久化到 State;第二阶段 `notifyCheckpointComplete` 在全局成功后才真正调用 `table.newAppend()` 或 `table.newRowDelta()` 提交到 Iceberg。关键是 `maxCommittedCheckpointId` 保证幂等 -- 重启后通过遍历表快照的 Summary 找到已提交的最大 checkpoint ID，避免重复提交。"

#### 30 秒速记

- 结论先行：两阶段提交 = `prepareSnapshotPreBarrier` (预提交) + `notifyCheckpointComplete` (正式提交)
- 关键数据结构: `dataFilesPerCheckpoint` (NavigableMap<Long, byte[]>)，按 checkpointId 有序维护待提交的 Manifest 序列化数据
- 幂等机制: `maxCommittedCheckpointId` 写入快照 Summary，恢复时通过 `SinkUtil.getMaxCommittedCheckpointId()` 遍历快照历史获取

---
<a id="q-3-3"></a>
### 3.3 prepareSnapshotPreBarrier 作用

**难度: 较易**

**问题: `prepareSnapshotPreBarrier` 为什么在 Barrier 到达前调用？**

**答案:**

这是实现 2PC 的关键设计，有三个核心目的。

#### 核心目的

1. **确保数据完整性**
   - 保证 Barrier 前的所有数据完整刷入文件
   - 为 Checkpoint 划分清晰的文件边界

2. **实现真正的预提交**
   - Barrier 到达前完成文件关闭（`writer.complete()`）和元数据生成
   - 符合 2PC "Pre-Commit" 阶段语义

3. **避免阻塞，提高吞吐**
   - Flush 后立即创建新 Writer (`this.writer = taskWriterFactory.create()`) 接收后续数据
   - 数据流不会因等待 Barrier 而阻塞

#### 源码验证

```java
// 源码: flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergStreamWriter.java
@Override
public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flush(checkpointId);                      // 1. 关闭文件，发送 WriteResult
    this.writer = taskWriterFactory.create();  // 2. 立即创建新 Writer
}
```

注意: `flush()` 方法在发送 WriteResult 后会将 `writer` 设为 null。这是一个防护设计，防止 `prepareSnapshotPreBarrier` 在 `endInput` 之后被调用时出现重复 flush 的问题。

**深入理解**: 如果在 Barrier 到达后才 flush，会导致数据流阻塞，严重影响吞吐量。

#### 30 秒速记

- 结论先行：这是实现 2PC 的关键设计，有三个核心目的
- 关键: Barrier 前关闭文件 + 发送元数据 + 创建新 Writer，三步连续完成
- 防护: flush 后将 writer 设为 null，防止重复 flush

---
<a id="q-3-4"></a>
### 3.4 Committer 单并行度设计

**难度: 中等**

**问题: 为什么 `IcebergFilesCommitter` 并行度必须为 1？**

**答案:**

为了**避免版本冲突**和**保证提交顺序性**。

#### 源码验证

在 `FlinkSink.java` 中可以看到 Committer 的并行度被强制设为 1:

```java
// 源码: flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/FlinkSink.java
private SingleOutputStreamOperator<Void> appendCommitter(
        SingleOutputStreamOperator<FlinkWriteResult> writerStream) {
    IcebergFilesCommitter filesCommitter = new IcebergFilesCommitter(...);
    SingleOutputStreamOperator<Void> committerStream =
        writerStream
            .transform(operatorName(ICEBERG_FILES_COMMITTER_NAME), Types.VOID, filesCommitter)
            .setParallelism(1)       // 强制并行度为 1
            .setMaxParallelism(1);   // 最大并行度也为 1
    ...
}
```

#### 设计原因

1. **避免版本冲突**
   - Iceberg 采用乐观并发控制(OCC)
   - 多个 Committer 实例同时提交会导致 `CommitFailedException`
   - 单实例串行提交避免冲突

2. **维护全局顺序**
   - 需要维护 `maxCommittedCheckpointId` 状态
   - 只有单实例才能可靠地串行更新
   - 保证 checkpoint ID 单调递增的提交语义

3. **保证 Equality Delete 语义正确**
   - 当存在删除文件时，每个 checkpoint 按顺序独立提交
   - 后续 checkpoint 的 equality-delete 需要正确应用到前序 checkpoint 的数据文件

#### 架构权衡

**优点**:
- 无需外部分布式锁(如 ZooKeeper)
- 架构简洁，易于部署
- `maxCommittedCheckpointId` 的维护无并发问题

**潜在瓶颈**:
- Checkpoint 间隔极短且数据量极大时，单点可能成为瓶颈
- 需要处理所有子任务的 `FlinkWriteResult` 并与 Catalog 交互

**深入理解**: 对绝大多数场景，单点 Committer 性能足够。Committer 的主要工作是将 Manifest 文件引用提交到 Iceberg 元数据（一次 commit 操作），而非处理数据本身。极端场景需监控 Checkpoint 耗时。

#### 30 秒速记

- 结论先行：为了避免 OCC 冲突和保证 checkpoint 提交顺序性
- 源码证据：`FlinkSink.java` 中 `setParallelism(1).setMaxParallelism(1)`
- 本质原因：Iceberg 乐观并发 + equality-delete 顺序语义

---
<a id="q-3-5"></a>
### 3.5 Position Delete vs Equality Delete

**难度: 较易**

**问题: 详细解释 Position Delete 和 Equality Delete 的原理、结构和适用场景。**

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
- 同批次内的删除优化（BaseEqualityDeltaWriter 中的 insertedRowMap 机制）
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
| 读取性能 | 极高(bitmap O(1) 跳过) | 较低(需构建 HashSet 逐行比较) |
| 存储开销 | 小 | 中 |
| 灵活性 | 低(需预知位置) | 高 |
| 典型场景 | Compaction / 批次内优化 | CDC / Upsert |

#### 面试加分话术

> "Position Delete 用 (file_path, pos) 精确定位,读取时构建 bitmap O(1) 跳过,适合 Compaction;Equality Delete 用列值标识,需要构建哈希表逐行比较,但无需预知位置,适合 CDC。V3 引入的 Deletion Vector 本质是 Position Delete 的优化 -- 用 Roaring Bitmap 存储,比 Parquet 文件更紧凑。选择时的黄金法则: 知道位置用 Position,只有主键用 Equality。"

#### 高频追问

**Q: 为什么 Equality Delete 读取开销大？**

**A**: Equality Delete 需要: (1) 加载所有相关 DeleteFile 构建 HashSet (2) 读取每条数据记录时提取 equality 字段并查找 HashSet。而 Position Delete 只需构建一次 bitmap,然后通过行号 O(1) 判断,无需访问数据内容。

#### 30 秒速记

- 结论先行：Iceberg v2 通过两种删除文件实现行级删除能力
- Position Delete: `(file_path, pos)` 精确定位，高效但需要知道位置
- Equality Delete: 按列值匹配，灵活但读取开销大

---
<a id="q-3-6"></a>
### 3.6 Merge-on-Read 优化

**难度: 困难**

**问题: 什么是 Merge-on-Read？Flink CDC 场景下如何智能结合两种删除方式优化性能？**

**答案:**

#### Merge-on-Read 概念

**核心思想**: 写入时不修改旧文件，而是记录变更；查询时合并数据

- 写入: 分别记录 INSERT/UPDATE/DELETE
- 查询: 引擎合并数据文件和删除文件，过滤已删除/更新的行

#### Flink CDC 混合删除策略

在 `BaseTaskWriter.BaseEqualityDeltaWriter`（`core/src/main/java/org/apache/iceberg/io/BaseTaskWriter.java` 中的受保护抽象内部类）中实现的智能优化:

**核心机制**: 维护内存 `insertedRowMap`

```java
// 源码: core/src/main/java/org/apache/iceberg/io/BaseTaskWriter.java
// 实际类型: StructLikeMap<PathOffset>
// PathOffset 包含 CharSequence path 和 long rowOffset
private Map<StructLike, PathOffset> insertedRowMap;

// 在 BaseEqualityDeltaWriter 构造函数中初始化:
this.insertedRowMap = StructLikeMap.create(deleteSchema.asStruct());
```

**智能决策逻辑**:

1. **处理 write(row)（对应 INSERT/UPDATE_AFTER）**:
   ```java
   // 源码: BaseTaskWriter.BaseEqualityDeltaWriter.write()
   public void write(T row) throws IOException {
       PathOffset pathOffset = PathOffset.of(dataWriter.currentPath(), dataWriter.currentRows());
       // 投影出 equality key 并复制
       StructLike copiedKey = StructLikeUtil.copy(structProjection.wrap(asStructLike(row)));
       // 将 key -> PathOffset 写入 insertedRowMap
       PathOffset previous = insertedRowMap.put(copiedKey, pathOffset);
       if (previous != null) {
           // 同批次内已有相同 key 的记录 -> 生成 Position Delete 删除旧版本
           writePosDelete(previous);
       }
       dataWriter.write(row);
   }
   ```

2. **处理 delete(row)（对应 DELETE / UPDATE_BEFORE）**:
   ```java
   // 源码: BaseTaskWriter.BaseEqualityDeltaWriter.delete()
   public void delete(T row) throws IOException {
       if (!internalPosDelete(structProjection.wrap(asStructLike(row)))) {
           // insertedRowMap 中不存在该 key -> 写入 Equality Delete（删除历史数据）
           eqDeleteWriter.write(row);
       }
       // 如果 internalPosDelete 返回 true，说明在 insertedRowMap 中找到了该 key，
       // 已经生成了 Position Delete 来撤销本批次的插入
   }
   ```

3. **处理 deleteKey(key)（upsert 模式使用）**:
   ```java
   // 源码: BaseTaskWriter.BaseEqualityDeltaWriter.deleteKey()
   public void deleteKey(T key) throws IOException {
       if (!internalPosDelete(asStructLikeKey(key))) {
           eqDeleteWriter.write(key);  // 仅写入主键列的值
       }
   }
   ```

#### 优化效果

**批次内操作**: 全部降级为高效的 Position Delete
**历史数据操作**: 才使用 Equality Delete

**性能提升**:
- 减少 Equality Delete 文件数量
- 降低查询时的合并开销
- 提升 CDC 场景写入性能

#### 30 秒速记

- 结论先行：`BaseEqualityDeltaWriter` 通过 `insertedRowMap` 智能选择删除方式
- 核心逻辑：同批次内的重复 key -> Position Delete（高效）；跨批次历史数据 -> Equality Delete
- 关键方法：`write()` 写入时检查重复 key，`delete()` / `deleteKey()` 通过 `internalPosDelete()` 优先尝试 Position Delete

---
<a id="q-3-7"></a>
### 3.7 Upsert 模式优化

**难度: 较易**

**问题: `write.upsert.enabled=true` 的作用及其与 `RowKind` 的关系？**

**答案:**

这是 CDC 场景的关键性能优化参数。

#### 默认行为(`write.upsert.enabled=false`)

CDC `UPDATE` 操作生成两条消息:

```
UPDATE_BEFORE(旧值) → 调用 writer.delete(row)，可能生成 Equality Delete(包含完整行)
UPDATE_AFTER(新值)  → 调用 writer.write(row)，生成 Data File
```

**缺点**: Equality Delete 文件大（包含完整行），处理消息多

#### Upsert 模式(`write.upsert.enabled=true`)

```
UPDATE_BEFORE → 完全忽略(直接 break)
UPDATE_AFTER  → 先调用 writer.deleteKey(仅主键投影) + 再调用 writer.write(row)
```

#### 源码逻辑

```java
// 源码: flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/BaseDeltaTaskWriter.java
// BaseDeltaTaskWriter.write() 方法
@Override
public void write(RowData row) throws IOException {
    RowDataDeltaWriter writer = route(row);

    switch (row.getRowKind()) {
        case INSERT:
        case UPDATE_AFTER:
            if (upsert) {
                writer.deleteKey(keyProjection.wrap(row)); // 先删旧 key（可能生成 Equality Delete）
            }
            writer.write(row); // 写入新数据
            break;

        case UPDATE_BEFORE:
            if (upsert) {
                break; // 关键: 直接跳过，不生成任何删除
            }
            writer.delete(row); // 非 upsert 模式才处理
            break;

        case DELETE:
            if (upsert) {
                writer.deleteKey(keyProjection.wrap(row)); // upsert 模式: 仅投影主键列
            } else {
                writer.delete(row); // 非 upsert 模式: 写入完整行
            }
            break;

        default:
            throw new UnsupportedOperationException("Unknown row kind: " + row.getRowKind());
    }
}
```

#### 核心优势

1. **性能提升**: 跳过 UPDATE_BEFORE 消息，处理消息数减半
2. **存储优化**: `deleteKey()` 仅写入主键列（通过 `keyProjection` 投影），Equality Delete 体积大幅减小
3. **读取优化**: 减少查询时的 Equality Delete 加载开销

#### delete() vs deleteKey() 的区别

- `delete(row)`: 写入完整行数据到 Equality Delete 文件
- `deleteKey(key)`: 仅写入 equality fields（主键列）到 Equality Delete 文件

这两个方法都定义在 `BaseTaskWriter.BaseEqualityDeltaWriter` 中，都会先尝试 `internalPosDelete`（检查 insertedRowMap），失败后才写入 Equality Delete。区别在于写入的数据量不同。

**最佳实践**: CDC 接入 Iceberg 时建议启用此参数。

---

#### 30 秒速记

- 结论先行：Upsert 模式跳过 UPDATE_BEFORE，使用 `deleteKey()` 代替 `delete()`
- 关键区别：`deleteKey()` 仅写主键列，`delete()` 写完整行
- 源码位置：`BaseDeltaTaskWriter.write()` 方法中的 switch-case 逻辑

---
<a id="q-3-8"></a>
### 3.8 删除文件的产生机制

**难度: 中等**

**问题: Position Delete 和 Equality Delete 文件分别是如何产生的？什么情况下二者会相互转化？**

**答案:**

#### Position Delete 的产生机制

**产生场景**:

1. **批次内重复 key 优化（BaseEqualityDeltaWriter.write()）**
   - 当 `insertedRowMap` 中已存在相同 key 时，对旧版本生成 Position Delete
   - 这是 CDC 场景中最常见的 Position Delete 产生方式

2. **批次内删除优化（BaseEqualityDeltaWriter.delete() / deleteKey()）**
   - 当 `insertedRowMap` 中找到对应 key 时，生成 Position Delete 撤销本批次的插入

3. **Spark DELETE/UPDATE 操作（Copy-on-Write 或 先扫描后删除）**
   ```sql
   DELETE FROM table WHERE user_id = 123
   ```
   - 引擎先扫描数据文件,定位符合条件的行
   - 记录每一行的 `(file_path, pos)`
   - 生成 Position Delete 文件

4. **Compaction 过程**
   - 合并小文件时,已有的 Equality Delete 可以转换为 Position Delete

**源码实现**:

```java
// 源码: core/src/main/java/org/apache/iceberg/io/BaseTaskWriter.java
// BaseEqualityDeltaWriter 内部的 writePosDelete 方法
private void writePosDelete(PathOffset pathOffset) {
    positionDelete.set(pathOffset.path, pathOffset.rowOffset, null);
    posDeleteWriter.write(positionDelete, spec, partitionKey);
}

// 在 write(row) 方法中触发:
PathOffset previous = insertedRowMap.put(copiedKey, pathOffset);
if (previous != null) {
    writePosDelete(previous);  // 对同批次内的旧版本生成 Position Delete
}
```

#### Equality Delete 的产生机制

**产生场景**:

1. **CDC 流式写入（Flink）**
   ```java
   // Flink CDC 接收到 DELETE 消息，且 insertedRowMap 中不存在该 key
   // BaseDeltaTaskWriter.write() -> writer.delete(row) / writer.deleteKey(key)
   //   -> internalPosDelete() 返回 false
   //   -> eqDeleteWriter.write(row) 或 eqDeleteWriter.write(key)
   ```

2. **MERGE INTO 操作**
   ```sql
   MERGE INTO target USING source
   ON target.id = source.id
   WHEN MATCHED THEN DELETE
   ```

3. **外部系统的删除请求**
   - 仅提供主键,无法定位物理位置
   - 必须使用 Equality Delete

**源码实现**:

```java
// 源码: core/src/main/java/org/apache/iceberg/io/BaseTaskWriter.java
// BaseEqualityDeltaWriter.delete() 方法 -- 写入完整行
public void delete(T row) throws IOException {
    if (!internalPosDelete(structProjection.wrap(asStructLike(row)))) {
        eqDeleteWriter.write(row);  // 写入完整行到 Equality Delete 文件
    }
}

// BaseEqualityDeltaWriter.deleteKey() 方法 -- 仅写入主键列（upsert 模式使用）
public void deleteKey(T key) throws IOException {
    if (!internalPosDelete(asStructLikeKey(key))) {
        eqDeleteWriter.write(key);  // 仅写入主键列的值
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

Iceberg 定义了 `ConvertEqualityDeleteFiles` Action 接口，**但截至当前版本（1.x）尚无任何引擎实现**:

```java
// 源码: api/src/main/java/org/apache/iceberg/actions/ConvertEqualityDeleteFiles.java
/** An action for converting the equality delete files to position delete files. */
public interface ConvertEqualityDeleteFiles
    extends SnapshotUpdate<ConvertEqualityDeleteFiles, ConvertEqualityDeleteFiles.Result> {
    ConvertEqualityDeleteFiles filter(Expression expression);
    interface Result {
        int convertedEqualityDeleteFilesCount();
        int addedPositionDeleteFilesCount();
    }
}
// ⚠️ 注意: ActionsProvider 中未注册此方法，SparkActions/FlinkActions 中均无实现类
// 全仓库搜索 "implements ConvertEqualityDeleteFiles" 返回 0 结果
```

**转换方向（理论上）**:
```
Equality Delete -> Position Delete (理论可行，但当前无现成实现)
原理:
1. 通过扫描数据文件，根据 equality fields 定位到具体行号
2. 生成新的 Position Delete 文件
3. 原子性替换旧的 Equality Delete 文件

Position Delete -> Equality Delete (不可能)
原因: Position Delete 不包含列值,无法反向推导
```

**当前可行的替代方案**: 使用 `rewriteDataFiles` 重写数据文件时会自动 apply equality delete，重写后的数据文件不再需要旧的 eq-delete 文件，后续通过 `expireSnapshots` + `deleteOrphanFiles` 清理。

#### 30 秒速记

- 结论先行：Position Delete 来自批次内优化（insertedRowMap），Equality Delete 来自跨批次历史删除
- 关键方法：`internalPosDelete()` 是分流的核心 -- 优先 Position Delete，失败才 Equality Delete
- 转换：`ConvertEqualityDeleteFiles` 接口已定义但**未实现**，实际通过 `rewriteDataFiles` 间接消除 Equality Delete

---
<a id="q-3-9"></a>
### 3.9 删除文件的小文件优化

**难度: 中等**

**问题: 小文件优化(Compaction)是否会优化删除文件？如何优化？**

**答案:**

**是的,Iceberg 提供了专门的删除文件优化机制。**

#### 删除文件的小文件问题

**产生原因**:
- CDC 流式写入:每个 Checkpoint 生成一个或多个小的 Equality Delete 文件
- 高频 DELETE 操作:每次删除生成一个小的 Position Delete 文件
- 累积效应:数千个小删除文件严重影响查询性能

**影响**:
```
查询时需要:
1. 读取所有删除文件的 Manifest
2. 加载所有删除文件到内存
3. 构建过滤器(Position Delete -> Bitmap, Equality Delete -> HashSet)
4. 逐行应用过滤

小文件越多,步骤 1-3 的开销越大
```

#### RewritePositionDeleteFiles Action

**源码实现**:

```java
// 源码: api/src/main/java/org/apache/iceberg/actions/RewritePositionDeleteFiles.java
/** An action for rewriting position delete files. */
public interface RewritePositionDeleteFiles
    extends SnapshotUpdate<RewritePositionDeleteFiles, RewritePositionDeleteFiles.Result> {

    /** A filter for finding the deletes to rewrite. */
    RewritePositionDeleteFiles filter(Expression expression);

    interface Result {
        List<FileGroupRewriteResult> rewriteResults();
        int rewrittenDeleteFilesCount();
        int addedDeleteFilesCount();
        long rewrittenBytesCount();
        long addedBytesCount();
    }
}
```

Spark 实现类: `RewritePositionDeleteFilesSparkAction`，使用 `BinPackRewritePositionDeletePlanner` 进行分组规划，`SparkRewritePositionDeleteRunner` 执行实际重写。

**合并策略（源码验证后的完整 5 步流程）**:

`SparkRewritePositionDeleteRunner` 是实际执行重写的核心类，其 `doRewrite()` 方法（`SparkRewritePositionDeleteRunner.java:96-127`）揭示了完整流程：

```
1. 扫描表中的 Position Delete 文件，按分区分组（BinPackRewritePositionDeletePlanner）
2. 将同一分区内的多个小 Position Delete 文件读取
3. ⭐ 与 DATA_FILES 元数据表做 leftsemi join，过滤掉指向已不存在数据文件的无效删除记录
4. 按 (file_path, pos) 排序后写入新的、更大的 Position Delete 文件
5. 通过 RewriteFiles 操作原子性地替换旧文件
```

**步骤 3 是文档中容易遗漏的关键点**。源码证据：

```java
// 源码: SparkRewritePositionDeleteRunner.java:113-116
// keep only valid position deletes
Dataset<Row> dataFiles = dataFiles(partitionType, partition);
Column joinCond = posDeletes.col("file_path").equalTo(dataFiles.col("file_path"));
Dataset<Row> validDeletes = posDeletes.join(dataFiles, joinCond, "leftsemi");
```

这意味着 `RewritePositionDeleteFiles` 不仅仅是"合并小文件"，还会**清理过时的删除记录**——那些指向已被 compaction 或 rewrite 删除的数据文件的 position delete 条目会被自动丢弃。

**v3 Deletion Vector（DV）升级能力**:

对于 format-version 3 的表，此 Action 还承担将 v2 Position Delete 文件重写为 v3 Deletion Vector（Puffin 格式）的职责：

```java
// 源码: RewritePositionDeleteFilesSparkAction.java:122-125
if (TableUtil.formatVersion(table) >= 3 && !requiresRewriteToDVs()) {
    LOG.info("v2 deletes in {} have already been rewritten to v3 DVs", table.name());
    return EMPTY_RESULT;
}
```

`requiresRewriteToDVs()` 检查是否还有非 Puffin 格式的 position delete 文件。如果所有 delete 都已经是 DV 格式，则直接返回空结果。

**使用方式**:

```java
// 通过 Spark Actions 调用
SparkActions.get()
    .rewritePositionDeletes(table)
    .filter(Expressions.greaterThan("date", "2024-01-01"))
    .option("target-file-size-bytes", "134217728")  // 128MB，控制合并后的 delete 文件大小
    .execute();
```

可用的 option 参数（来自 `SizeBasedFileRewritePlanner`）：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `target-file-size-bytes` | 继承 `write.delete.target-file-size-bytes` | 合并后的目标 delete 文件大小 |
| `min-file-size-bytes` | target × 0.75 | 小于此值的文件被视为需要合并 |
| `max-file-size-bytes` | target × 1.80 | 大于此值的文件被视为需要拆分 |
| `min-input-files` | 5 | 一个分组内最少文件数才触发合并 |

**关键设计**:
- **分区级别合并**: 同一分区内的 Position Delete 文件合并在一起
- **排序优化**: 合并后的文件按 `(file_path, pos)` 排序（`SparkRewritePositionDeleteRunner.java:120`），读取时可高效二分查找
- **无效删除清理**: leftsemi join 过滤指向已不存在数据文件的 position delete 条目
- **v3 DV 升级**: format-version 3 的表会将 v2 position delete 重写为 Deletion Vector
- **大小控制**: 通过 `target-file-size-bytes` 控制输出文件大小，避免生成过大的删除文件

#### Equality Delete 文件优化

**挑战**: Equality Delete 文件无法简单合并

**原因**:
```
Equality Delete 文件 A: {user_id: 1, user_id: 2}
Equality Delete 文件 B: {user_id: 2, user_id: 3}

简单合并 -> {user_id: 1, user_id: 2, user_id: 2, user_id: 3}
问题: user_id=2 重复,但无法去重(可能是不同时间的删除)
```

**解决方案**: `ConvertEqualityDeleteFiles` 接口已定义但**尚无引擎实现**。当前最有效的方式是通过 `RewriteDataFiles` 重写数据文件，重写过程中会自动 apply equality delete，之后通过 `ExpireSnapshots` + `DeleteOrphanFiles` 清理旧的 eq-delete 文件。

```java
// 当前可行方案: 通过 RewriteDataFiles 间接消除 Equality Delete
// 1. rewriteDataFiles 读取数据时自动 apply equality delete
// 2. 重写后的数据文件已经是"干净"的，不再需要旧的 eq-delete
// 3. expireSnapshots + deleteOrphanFiles 清理旧文件
SparkActions.get(spark).rewriteDataFiles(table).execute();
SparkActions.get(spark).expireSnapshots(table).execute();
SparkActions.get(spark).deleteOrphanFiles(table).execute();
```

也可以通过 `RewriteFiles` 手动转换（需要自行实现扫描逻辑）:

```java
// 源码: core/src/main/java/org/apache/iceberg/BaseRewriteFiles.java
// rewriteFiles 方法支持同时替换数据文件和删除文件
table.newRewrite()
    .rewriteFiles(
        ImmutableSet.of(),         // dataFilesToReplace (无)
        equalityDeleteFiles,       // deleteFilesToReplace (旧的 Equality Delete)
        ImmutableSet.of(),         // dataFilesToAdd (无)
        positionDeleteFiles        // deleteFilesToAdd (新的 Position Delete)
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

**深入理解**: 删除文件的小文件优化与数据文件优化同样重要。在 CDC 场景下,删除文件的累积速度可能比数据文件更快,必须定期执行 `RewritePositionDeleteFiles` 合并 Position Delete 小文件,以及通过 `RewriteDataFiles` 间接消除 Equality Delete（`ConvertEqualityDeleteFiles` 接口已定义但尚未实现）。

#### 30 秒速记

- 结论先行：Iceberg 提供 `RewritePositionDeleteFiles` 合并 Position Delete；Equality Delete 通过 `RewriteDataFiles` 间接消除
- Position Delete 优化：合并小文件，按 (file_path, pos) 排序
- Equality Delete 优化：`ConvertEqualityDeleteFiles` 接口未实现，当前通过重写数据文件间接消除

---
<a id="q-3-10"></a>
### 3.10 Tag 与快照的关系

**难度: 较易**

**问题: Iceberg 打 Tag 时与快照的关系是什么？Tag 的底层实现机制是什么？**

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
    public static final String MAIN_BRANCH = "main";

    private final long snapshotId;             // 指向的快照 ID
    private final SnapshotRefType type;        // 类型: TAG 或 BRANCH
    private final Integer minSnapshotsToKeep;  // 保留策略(仅 BRANCH 支持)
    private final Long maxSnapshotAgeMs;       // 快照最大年龄(仅 BRANCH 支持)
    private final Long maxRefAgeMs;            // 引用最大年龄(TAG 和 BRANCH 都支持)

    public boolean isTag() {
        return type == SnapshotRefType.TAG;
    }

    public boolean isBranch() {
        return type == SnapshotRefType.BRANCH;
    }
}
```

**关键约束** (来自 `SnapshotRef.Builder`):
- `minSnapshotsToKeep`: **Tags 不支持设置**（设置时会抛出 IllegalArgumentException: "Tags do not support setting minSnapshotsToKeep"）
- `maxSnapshotAgeMs`: **Tags 不支持设置**（设置时会抛出 IllegalArgumentException: "Tags do not support setting maxSnapshotAgeMs"）
- `maxRefAgeMs`: Tags 和 Branches 都支持

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
    /** 创建 Tag，指向指定快照 */
    ManageSnapshots createTag(String name, long snapshotId);

    /** 删除 Tag */
    ManageSnapshots removeTag(String name);

    /** 替换 Tag，指向新快照 */
    ManageSnapshots replaceTag(String name, long snapshotId);

    /** 设置引用的最大年龄（Tag 和 Branch 都支持） */
    ManageSnapshots setMaxRefAgeMs(String name, long maxRefAgeMs);
}
```

**实际执行**:

```java
// 创建 Tag
table.manageSnapshots()
    .createTag("v1.0.0", 123456789L)  // 将 Tag "v1.0.0" 指向快照 123456789
    .commit();

// 元数据变化(存储在 metadata.json 的 refs 字段中)
{
  "refs": {
    "main": {
      "snapshot-id": 987654321,
      "type": "branch"
    },
    "v1.0.0": {
      "snapshot-id": 123456789,  // Tag 指向的快照
      "type": "tag",
      "max-ref-age-ms": 31536000000  // 1年(如果设置了的话)
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
3. 删除 Tag 不会删除快照(除非快照没有其他引用且已过期)
4. 有 Tag 引用的快照不会被 ExpireSnapshots 删除
```

**示例**:

```
快照 A (id=100) <- Tag "v1.0" + Tag "stable"
快照 B (id=200) <- Branch "main"
快照 C (id=300) <- (无引用)

ExpireSnapshots 执行:
- 快照 A: 保留(被 2 个 Tag 引用)
- 快照 B: 保留(被 Branch 引用)
- 快照 C: 删除(无引用且已过期)
```

**深入理解**: Tag 是 Iceberg 版本管理的核心机制,通过轻量级的引用实现快照的命名和保留。与 Git Tag 类似,Iceberg Tag 提供了不可变的版本标记,非常适合用于发布版本管理、审计和回滚场景。

#### 30 秒速记

- 结论先行：Tag 是指向快照的命名引用,不是副本
- 关键区别：Tag 不可变且仅支持 `maxRefAgeMs`；Branch 可变且支持完整保留策略
- 源码位置：`SnapshotRef.java` 定义结构，`ManageSnapshots.java` 定义操作接口

---
<a id="q-3-11"></a>
### 3.11 小文件合并后的历史文件删除

**难度: 中等**

**问题: 小文件合并(Compaction)重写了新文件,历史文件会删除吗？在什么时候删除？**

**答案:**

**关键结论**: **历史文件不会立即删除,而是在快照过期时删除**

#### 小文件合并的完整流程

**1. Rewrite 阶段**:

```java
// 源码: core/src/main/java/org/apache/iceberg/BaseRewriteFiles.java
class BaseRewriteFiles extends MergingSnapshotProducer<RewriteFiles> implements RewriteFiles {

    @Override
    public RewriteFiles rewriteFiles(
        Set<DataFile> dataFilesToReplace,     // 旧的小文件
        Set<DeleteFile> deleteFilesToReplace,  // 旧的删除文件
        Set<DataFile> dataFilesToAdd,          // 新的大文件
        Set<DeleteFile> deleteFilesToAdd) {    // 新的删除文件

        // 1. 标记旧数据文件为"已删除"
        for (DataFile dataFile : dataFilesToReplace) {
            deleteFile(dataFile);  // 内部调用 delete(dataFile)
        }

        // 2. 标记旧删除文件为"已删除"
        for (DeleteFile deleteFile : deleteFilesToReplace) {
            deleteFile(deleteFile);
        }

        // 3. 添加新数据文件
        for (DataFile dataFile : dataFilesToAdd) {
            addFile(dataFile);
        }

        // 4. 添加新删除文件
        for (DeleteFile deleteFile : deleteFilesToAdd) {
            addFile(deleteFile);
        }

        return this;
    }
}
```

**关键验证**: `BaseRewriteFiles` 还包含校验逻辑 -- 如果没有要删除的数据文件,则不允许添加新数据文件;如果没有要删除的删除文件,则不允许添加新删除文件。

**2. Commit 阶段**:

提交时创建新快照,新快照的 Manifest 文件记录了文件的变更:
```
新快照的 Manifest 记录:
- added-files: [new-file-1.parquet, new-file-2.parquet]    (status=ADDED)
- deleted-files: [old-file-1.parquet, old-file-2.parquet]  (status=DELETED)
```

**3. 文件状态变化**:

```
合并前:
Snapshot 100 -> Manifest A -> [old-file-1.parquet, old-file-2.parquet, ...]

合并后:
Snapshot 100 -> Manifest A -> [old-file-1.parquet, old-file-2.parquet, ...]  (仍存在)
Snapshot 101 -> Manifest B -> [new-file-1.parquet, new-file-2.parquet]       (新快照)
                             deleted: [old-file-1.parquet, old-file-2.parquet]
```

**关键点**: 旧文件仍然存在于文件系统中,因为 Snapshot 100 仍然引用它们。这保证了时间旅行查询的正确性。

#### 历史文件的删除时机

**触发条件**: 执行 `ExpireSnapshots` 操作

```java
// 源码: api/src/main/java/org/apache/iceberg/ExpireSnapshots.java
public interface ExpireSnapshots extends PendingUpdate<List<Snapshot>> {
    /** 过期指定时间之前的快照 */
    ExpireSnapshots expireOlderThan(long timestampMillis);

    /** 保留最近 N 个快照 */
    ExpireSnapshots retainLast(int numSnapshots);

    /** 传入自定义删除函数 */
    ExpireSnapshots deleteWith(Consumer<String> deleteFunc);

    /** 传入并行删除执行器 */
    ExpireSnapshots executeDeleteWith(ExecutorService executorService);

    /** 配置清理级别: NONE / METADATA_ONLY / ALL */
    ExpireSnapshots cleanupLevel(CleanupLevel level);
}
```

注意: `ExpireSnapshots` 的 `apply()` 方法返回 `List<Snapshot>`（将被删除的快照列表），而非 `Snapshot`。

**执行示例**:

```java
// 过期 7 天前的快照
table.expireSnapshots()
    .expireOlderThan(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7))
    .retainLast(5)  // 至少保留最近 5 个快照
    .commit();
```

**删除逻辑**（伪代码）:

```java
// ExpireSnapshots 的核心逻辑
Set<String> filesToDelete = new HashSet<>();

// 1. 确定要删除的快照（考虑 Tag/Branch 引用保护）
List<Snapshot> snapshotsToExpire = determineSnapshotsToExpire();

// 2. 收集这些快照中的文件
for (Snapshot snapshot : snapshotsToExpire) {
    for (ManifestFile manifest : snapshot.allManifests()) {
        for (DataFile file : readManifest(manifest)) {
            filesToDelete.add(file.path());
        }
    }
}

// 3. 排除仍被存活快照引用的文件
Set<String> referencedFiles = collectReferencedFiles(remainingSnapshots);
filesToDelete.removeAll(referencedFiles);

// 4. 删除数据文件
for (String file : filesToDelete) {
    fileIO.deleteFile(file);  // 物理删除
}

// 5. 删除不再需要的 Manifest 文件
// 6. 删除不再需要的 Manifest List 文件
```

#### 完整的文件生命周期

```
Day 0: 创建小文件
|- Snapshot 100 -> [file-1.parquet (10MB), file-2.parquet (8MB), ...]
|- 文件状态: ACTIVE

Day 1: 执行 Compaction
|- Snapshot 101 -> [merged-file.parquet (512MB)]
|                 deleted: [file-1.parquet, file-2.parquet, ...]
|- 文件状态:
|   |- file-1.parquet: ACTIVE (仍被 Snapshot 100 引用)
|   |- merged-file.parquet: ACTIVE

Day 8: 执行 ExpireSnapshots (过期 7 天前的快照)
|- 删除 Snapshot 100
|- 检查 file-1.parquet:
|   |- 不再被任何存活快照引用 -> 物理删除
|- 文件状态:
|   |- file-1.parquet: DELETED (物理删除)
|   |- merged-file.parquet: ACTIVE
```

#### 配置参数

```properties
# 自动删除旧版本元数据文件
write.metadata.delete-after-commit.enabled=true
write.metadata.previous-versions-max=100

# 快照保留策略（在 refs 中为 main branch 配置）
# 这些参数影响 ExpireSnapshots 的行为
history.expire.min-snapshots-to-keep=5
history.expire.max-snapshot-age-ms=604800000  # 7 天
```

#### 孤儿文件清理

**场景**: 如果 Compaction 失败,新文件已写入存储但未提交到 Iceberg 元数据

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

#### 30 秒速记

- 结论先行：历史文件不会立即删除,在 `ExpireSnapshots` 时才物理删除
- 关键机制：快照引用保护 -- 只要有快照（包括 Tag/Branch 引用的）指向文件，文件就不会被删除
- 生产实践：定期执行 `ExpireSnapshots` + `RemoveOrphanFiles` 回收空间

---

### 补充: Flink SinkV2 API 架构说明

**说明**: 当前 Iceberg 的 Flink 集成同时支持两套 Sink API。理解它们的区别有助于面试中展示全面的知识。

#### 传统 API (FlinkSink)

使用 Flink 的底层 Operator API 构建:

```
DataStream<RowData>
    -> IcebergStreamWriter (OneInputStreamOperator, 并行)
        -> FlinkWriteResult (checkpointId + WriteResult)
    -> IcebergFilesCommitter (OneInputStreamOperator, 并行度=1)
        -> Void
```

**关键特点**:
- `IcebergStreamWriter` 继承 `AbstractStreamOperator`，实现 `prepareSnapshotPreBarrier` 和 `endInput`
- `IcebergFilesCommitter` 继承 `AbstractStreamOperator`，实现 `snapshotState` 和 `notifyCheckpointComplete`
- 状态管理由 `IcebergFilesCommitter` 自行维护（`NavigableMap<Long, byte[]> dataFilesPerCheckpoint`）

#### 新版 SinkV2 API (IcebergSink)

使用 Flink 的 Sink2 接口:

```
DataStream<RowData>
    -> IcebergSinkWriter (CommittingSinkWriter, 并行)
        -> WriteResult
    -> IcebergWriteAggregator (聚合多个 WriteResult 为一个 IcebergCommittable)
        -> IcebergCommittable (manifest bytes + jobId + operatorId + checkpointId)
    -> IcebergCommitter (Committer<IcebergCommittable>, 由框架管理)
```

**关键特点**:
- `IcebergSinkWriter` 实现 `CommittingSinkWriter<RowData, WriteResult>` 接口
- `IcebergWriteAggregator` 负责将 WriteResult 序列化为 DeltaManifests 并聚合为 IcebergCommittable
- `IcebergCommitter` 实现 Flink SinkV2 的 `Committer<IcebergCommittable>` 接口，框架自动处理 checkpoint 和恢复
- 幂等性通过 `SinkUtil.getMaxCommittedCheckpointId()` 保证，与传统 API 共享同一机制

两套 API 的 Iceberg 提交逻辑（`commitDeltaTxn`、`replacePartitions`、`commitOperation` 等）几乎完全一致。

---
<a id="q-3-12"></a>
### 3.12 全新启动 Flink 作业的 checkpointId 会被拦截吗

**🟠 问题: 全新启动的 Flink 作业写 Iceberg，checkpoint 从 1 开始，而 Iceberg 中已有其他作业写入的 maxCheckpointId 可能是 1000+。新作业的提交会被跳过吗？**

**答案:**

**不会。** checkpointId 的过滤是按 `(flink.job-id, flink.operator-id)` 绑定的，不是全局的。

#### 核心源码: `SinkUtil.getMaxCommittedCheckpointId()`

`SinkUtil.java:83-105`:
```java
static long getMaxCommittedCheckpointId(
    Table table, String flinkJobId, String operatorId, String branch) {
  Snapshot snapshot = table.snapshot(branch);
  long lastCommittedCheckpointId = INITIAL_CHECKPOINT_ID; // -1

  while (snapshot != null) {
    Map<String, String> summary = snapshot.summary();
    String snapshotFlinkJobId = summary.get(FLINK_JOB_ID);
    String snapshotOperatorId = summary.get(OPERATOR_ID);
    // ★ 关键：必须 jobId 和 operatorId 都匹配才会读取 checkpointId
    if (flinkJobId.equals(snapshotFlinkJobId)
        && (snapshotOperatorId == null || snapshotOperatorId.equals(operatorId))) {
      String value = summary.get(MAX_COMMITTED_CHECKPOINT_ID);
      if (value != null) {
        lastCommittedCheckpointId = Long.parseLong(value);
        break;
      }
    }
    Long parentSnapshotId = snapshot.parentId();
    snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
  }

  return lastCommittedCheckpointId;
}
```

#### 每次 Iceberg commit 写入的 snapshot summary

`IcebergCommitter.commitOperation()` (IcebergCommitter.java:292-294):
```java
operation.set(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
operation.set(SinkUtil.FLINK_JOB_ID, newFlinkJobId);
operation.set(SinkUtil.OPERATOR_ID, operatorId);
```

每个 snapshot 都绑定了产生它的 `(jobId, operatorId, checkpointId)` 三元组。

#### 两种场景对比

| 场景 | jobId | getMaxCommittedCheckpointId 返回值 | 结果 |
|------|-------|-----------------------------------|------|
| **全新启动** | 全新 UUID，Iceberg 中没有匹配的 snapshot | `-1` (INITIAL_CHECKPOINT_ID) | 所有 checkpoint 正常提交 |
| **从 checkpoint/savepoint 恢复** | 新 UUID，但从 state 恢复旧 jobId，用旧 jobId 查询 | 旧作业已提交的最大 checkpointId | 跳过已提交的，只提交剩余的 |

#### 全新启动的完整流程

1. Flink 生成全新的 `jobId`（128-bit 随机数）
2. `IcebergCommitter.commit()` 调用 `SinkUtil.getMaxCommittedCheckpointId(table, newJobId, operatorId, branch)`
3. 遍历所有 snapshot，**没有任何 snapshot 的 `flink.job-id` 匹配这个新 UUID**
4. 返回 `-1`
5. `commitRequestMap.headMap(-1, true)` → 空集合，没有被跳过
6. `commitRequestMap.tailMap(-1, false)` → 所有 checkpoint 都进入 uncommitted，**正常提交**

#### 深入理解

这个设计意味着**多个不同的 Flink 作业可以同时写同一张 Iceberg 表而互不干扰**。每个作业有独立的 jobId，各自的 checkpoint 过滤逻辑完全隔离。唯一需要处理的是 Iceberg 层面的乐观并发冲突（OCC），这由 `TableOperations.commit()` 的重试机制解决。

#### 30 秒速记

- 结论先行：**不会被拦截**。checkpointId 过滤按 `(jobId, operatorId)` 隔离，全新作业的 jobId 不匹配任何历史 snapshot，返回 -1，所有 checkpoint 正常提交。
- 关键方法：`SinkUtil.getMaxCommittedCheckpointId()` — 遍历 snapshot 链，只匹配同一 jobId 的记录。
- 面试表达：先说结论（不会），再说原因（jobId 隔离），最后补充（多作业并发写表的安全性）。

---
<a id="q-3-13"></a>
### 3.13 Flink JobID 生成机制

**🟡 问题: Flink 的 JobID 是如何生成的？为什么全新启动和恢复启动不会冲突？**

**答案:**

Flink 的 `JobID` 底层是 128-bit 随机数，等价于 UUID。

#### JobID 的数据结构

```java
// org.apache.flink.api.common.JobID
public class JobID extends AbstractID {
    public JobID() {
        super();  // 调用 AbstractID 的无参构造
    }
}

// org.apache.flink.util.AbstractID
public class AbstractID {
    private final long lowerPart;
    private final long upperPart;

    public AbstractID() {
        this.lowerPart = ThreadLocalRandom.current().nextLong();
        this.upperPart = ThreadLocalRandom.current().nextLong();
    }
}
```

两个 `long` 组成 128-bit 随机数，碰撞概率极低。

#### 不同场景下的 JobID 行为

| 场景 | JobID 行为 | 说明 |
|------|-----------|------|
| **全新提交作业** | `new JobID()` → 全新随机 ID | 与历史 snapshot 无匹配 |
| **从 savepoint 恢复** | 生成**全新**的 JobID | Flink 框架行为，恢复 ≠ 延续 |
| **从 checkpoint 恢复** | 生成**全新**的 JobID | 同上 |
| **手动指定** | `-D execution.job-id=xxx` | 极少使用，测试场景 |

#### 恢复时为什么不会冲突

关键在于 `IcebergFilesCommitter.initializeState()` (第 170-192 行):

```java
if (context.isRestored()) {
    // 从 Flink State 中恢复旧作业的 jobId
    String restoredFlinkJobId = jobIdIterable.iterator().next();
    // 用旧 jobId 去 Iceberg snapshot 中查找已提交的最大 checkpointId
    this.maxCommittedCheckpointId =
        SinkUtil.getMaxCommittedCheckpointId(table, restoredFlinkJobId, operatorUniqueId, branch);
    // 提交旧作业遗留的未提交数据
    NavigableMap<Long, byte[]> uncommittedDataFiles =
        Maps.newTreeMap(checkpointsState.get().iterator().next())
            .tailMap(maxCommittedCheckpointId, false);
    if (!uncommittedDataFiles.isEmpty()) {
        commitUpToCheckpoint(uncommittedDataFiles, restoredFlinkJobId, operatorUniqueId, ...);
    }
}
```

恢复流程的精妙之处:
1. Flink 分配了新 jobId，但 Committer 从 State 中读出旧 jobId
2. 用旧 jobId 查 Iceberg snapshot，找到已提交的最大 checkpointId
3. 将旧作业遗留的未提交数据（从 State 恢复）用旧 jobId 提交
4. 之后的新 checkpoint 使用新 jobId 提交，开始新的提交序列

> Flink 注释原文 (IcebergFilesCommitter.java:187-190): "Since flink's checkpoint id will start from the max-committed-checkpoint-id + 1 in the new flink job even if it's restored from a snapshot created by another different flink job, so it's safe to assign the max committed checkpoint id from restored flink job to the current flink job."

#### 30 秒速记

- 结论先行：JobID 是 128-bit 随机数（`ThreadLocalRandom` 生成），每次启动/恢复都是全新的。
- 恢复时的关键：从 Flink State 中恢复旧 jobId，用旧 jobId 查 Iceberg 已提交记录，然后切换到新 jobId 继续。
- 面试表达：先说 JobID 生成方式，再说恢复时的 State 传递机制，最后总结这保证了多作业并发安全。

---
<a id="q-3-14"></a>
### 3.14 insertedRowMap 的生命周期

**🟠 问题: Flink 流式写入 Iceberg 时维护的 `insertedRowMap` 会定时清空吗？它的生命周期是什么？**

**答案:**

**`insertedRowMap` 不会"定时"清空，而是在每次 Checkpoint 时随 writer 整体销毁并重建。每个 Checkpoint 周期就是 insertedRowMap 的完整生命周期。**

#### insertedRowMap 的作用

`insertedRowMap` 定义在 `BaseTaskWriter.BaseEqualityDeltaWriter` 中（`BaseTaskWriter.java:186`），用于记录当前批次内已写入的行的 equality key 到文件路径+行偏移的映射：

```java
// 源码: core/src/main/java/org/apache/iceberg/io/BaseTaskWriter.java:186,218
private Map<StructLike, PathOffset> insertedRowMap;
this.insertedRowMap = StructLikeMap.create(deleteSchema.asStruct());
```

每次 `write()` 时将 `(equalityKey → filePath+rowOffset)` 写入 map；
每次 `deleteKey()`/`delete()` 时先在 map 中查找，命中则用 Position Delete（高效），未命中则退化为 Equality Delete。

#### 清空时机：Checkpoint 触发时整体销毁

**SinkV1（`IcebergStreamWriter`）路径**:

```java
// 源码: IcebergStreamWriter.java:64-67
public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flush(checkpointId);                    // → writer.complete() → writer.close()
    this.writer = taskWriterFactory.create(); // 创建全新 writer（含空的 insertedRowMap）
}
```

**SinkV2（`IcebergSinkWriter`）路径**:

```java
// 源码: IcebergSinkWriter.java:99-102
public Collection<WriteResult> prepareCommit() throws IOException {
    WriteResult result = writer.complete();   // → writer.close()
    this.writer = taskWriterFactory.create(); // 创建全新 writer
    // ...
}
```

**`close()` 内部显式清空 insertedRowMap**:

```java
// 源码: BaseTaskWriter.java:330-333
if (insertedRowMap != null) {
    insertedRowMap.clear();
    insertedRowMap = null;
}
```

对于分区表，`PartitionedDeltaWriter` 维护 `Map<PartitionKey, RowDataDeltaWriter>`，每个分区一个 DeltaWriter（各自有独立的 insertedRowMap），在 `close()` 时全部关闭并清空：

```java
// 源码: PartitionedDeltaWriter.java:87-99
public void close() {
    super.close();
    Tasks.foreach(writers.values())
        .throwFailureWhenFinished()
        .noRetry()
        .run(RowDataDeltaWriter::close, IOException.class);
    writers.clear();  // 所有分区的 writer（含 insertedRowMap）全部销毁
}
```

#### 生命周期总结

| 阶段 | insertedRowMap 状态 |
|---|---|
| Checkpoint N 开始 | **空的** — 全新 writer 创建 |
| 两次 Checkpoint 之间 | **持续增长** — 每次 INSERT/UPDATE_AFTER 都 put 一条 |
| 同 key 再次写入时 | 旧条目被覆盖，对旧行生成 Position Delete |
| Checkpoint N+1 触发 | **整体销毁** — writer.close() → insertedRowMap.clear() + null |
| Checkpoint N+1 之后 | **全新空 map** — 新 writer 从零开始 |

#### OOM 风险

由于 insertedRowMap 在两次 Checkpoint 之间持续增长，如果满足以下条件可能导致 OOM：
- Checkpoint 间隔设置过长（如 30 分钟）
- 数据量极大且 equality field 基数很高（每行 key 都不同）
- 多分区写入（每个分区维护独立的 insertedRowMap）

**建议**: CDC 场景下 Checkpoint 间隔通常设置为 1-5 分钟，兼顾 insertedRowMap 内存压力和提交频率。

#### 30 秒速记

- 结论先行：insertedRowMap 随 Checkpoint 周期创建和销毁，不存在"定时清空"机制
- 关键路径：`prepareSnapshotPreBarrier`/`prepareCommit` → `writer.complete()` → `close()` → `insertedRowMap.clear()`，然后 `taskWriterFactory.create()` 创建全新 writer
- OOM 风险：Checkpoint 间隔过长 + 高基数 key = insertedRowMap 膨胀，建议 CDC 场景 1-5 分钟

---
<a id="q-3-15"></a>
### 3.15 Flink CDC upsert 删除类型与 ConvertEqualityDeleteFiles 现状

**🔴 问题: Flink CDC upsert 模式下的删除一定产生 Equality Delete 文件吗？`ConvertEqualityDeleteFiles` Action 能否将 Equality Delete 转换为 Position Delete？**

**答案:**

**两个关键结论：(1) upsert 删除不一定是 Equality Delete，而是一个两级判断机制；(2) `ConvertEqualityDeleteFiles` 只有接口定义，截至当前版本没有任何引擎实现。**

#### 结论一：upsert 删除优先尝试 Position Delete

在 `BaseDeltaTaskWriter.write()` 中，upsert 模式的 DELETE 和 UPDATE_AFTER 都调用 `deleteKey()`：

```java
// 源码: BaseDeltaTaskWriter.java:84-111
switch (row.getRowKind()) {
    case INSERT:
    case UPDATE_AFTER:
        if (upsert) {
            writer.deleteKey(keyProjection.wrap(row)); // 先尝试删旧
        }
        writer.write(row);
        break;
    case DELETE:
        if (upsert) {
            writer.deleteKey(keyProjection.wrap(row)); // upsert 用 deleteKey
        } else {
            writer.delete(row);
        }
        break;
}
```

`deleteKey()` 内部的两级判断：

```java
// 源码: BaseTaskWriter.java:273-307
private boolean internalPosDelete(StructLike key) {
    PathOffset previous = insertedRowMap.remove(key);
    if (previous != null) {
        writePosDelete(previous);  // ✅ 命中 → Position Delete（精确高效）
        return true;
    }
    return false;                  // ❌ 未命中
}

public void deleteKey(T key) throws IOException {
    if (!internalPosDelete(asStructLikeKey(key))) {
        eqDeleteWriter.write(key); // 未命中 → 退化为 Equality Delete
    }
}
```

#### 两种场景对比

| 场景 | insertedRowMap 命中？ | 产生的 Delete 类型 |
|---|---|---|
| 同一 Checkpoint 周期内先 INSERT `id=1`，再 DELETE `id=1` | ✅ 命中 | **Position Delete** |
| DELETE 的 `id=1` 是之前 Checkpoint 或历史批次写入的 | ❌ 未命中 | **Equality Delete** |
| 同一周期内 INSERT `id=1`，再 UPDATE（deleteKey+write） | ✅ 命中 | **Position Delete**（删旧）+ Data File（写新） |

#### 结论二：ConvertEqualityDeleteFiles 只有接口没有实现

**源码证据**:

```java
// 源码: api/src/main/java/org/apache/iceberg/actions/ConvertEqualityDeleteFiles.java
public interface ConvertEqualityDeleteFiles
    extends SnapshotUpdate<ConvertEqualityDeleteFiles, ConvertEqualityDeleteFiles.Result> {
    ConvertEqualityDeleteFiles filter(Expression expression);
}

// 源码: core/src/main/java/org/apache/iceberg/actions/ConvertEqualityDeleteStrategy.java
public interface ConvertEqualityDeleteStrategy { ... }
```

**三个关键缺失**:

| 缺失项 | 对比已实现的 rewritePositionDeletes |
|---|---|
| `ActionsProvider` 中无注册 | ✅ `rewritePositionDeletes(Table)` 已注册 |
| `SparkActions` 中无实现 | ✅ `RewritePositionDeleteFilesSparkAction` 已实现 |
| 全仓库 `implements ConvertEqualityDeleteFiles` 返回 0 结果 | ✅ `implements RewritePositionDeleteFiles` 有实现 |

#### 当前消除 Equality Delete 的可行方案

```
方案: RewriteDataFiles（间接消除）
原理:
1. rewriteDataFiles 读取数据时自动 apply equality delete
2. 重写后的数据文件已经是"干净"的
3. 旧的 eq-delete 文件在 expireSnapshots 后变成孤儿文件
4. deleteOrphanFiles 最终物理删除

执行顺序:
rewriteDataFiles → expireSnapshots → deleteOrphanFiles
```

#### 30 秒速记

- 结论先行：upsert 删除不一定是 Equality Delete，`insertedRowMap` 命中时优先 Position Delete；`ConvertEqualityDeleteFiles` 只有接口无实现
- 两级判断：`internalPosDelete()` 先查 insertedRowMap，命中 → pos-delete，未命中 → eq-delete
- 替代方案：通过 `rewriteDataFiles` 间接消除 Equality Delete（重写数据时自动 apply），这是目前生产环境的主要手段

---

## 四、文件管理与优化
<a id="q-4-1"></a>
### 4.1 小文件合并策略

**🟠 问题: 对比分析 `RewriteDataFiles` 的 BinPack、Sort 和 Z-Order 策略。**

**答案:**

Iceberg 通过 `RewriteDataFiles` action 解决小文件问题，提供三种策略。

#### 1. BinPack(装箱策略)

**原理**: 简单合并文件至目标大小(默认使用表属性 `write.target-file-size-bytes`，即 512MB)，不关心数据内容

**实现类**: `SparkBinPackFileRewriteRunner`

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

**实现类**: `SparkSortFileRewriteRunner`

**优势**:
- **查询性能**: 高区分度的 min/max 统计，高效跳过无关文件
- **压缩率**: 相似数据连续存储，提升压缩效率

**适用场景**:
- 查询模式固定，经常按特定列范围过滤
- 对查询性能有极致要求
- 例: 按 `event_time` 查询最近数据

#### 3. Z-Order(Z阶曲线排序)

**原理**: 多维数据聚类，将多列值映射为一维 Z-Value 后排序

**实现类**: `SparkZOrderFileRewriteRunner`

**优势**:
- 多维组合过滤查询高效
- 实现多维数据裁剪

**适用场景**:
- 查询涉及多个高基数列的组合过滤
- 无主要线性排序维度
- 例: `WHERE col_A='a' AND col_B='b' AND col_C>100`

#### 策略的选择机制

```java
// 源码: spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java
// 策略通过不同的 runner 实现:

public RewriteDataFilesSparkAction binPack() {
    this.runner = new SparkBinPackFileRewriteRunner(spark(), table);
    return this;
}

public RewriteDataFilesSparkAction sort(SortOrder sortOrder) {
    this.runner = new SparkSortFileRewriteRunner(spark(), table, sortOrder);
    return this;
}

public RewriteDataFilesSparkAction zOrder(String... columnNames) {
    this.runner = new SparkZOrderFileRewriteRunner(spark(), table, Arrays.asList(columnNames));
    return this;
}

// 默认策略: 如果未指定，init() 方法中默认使用 BinPack
private void init(long startingSnapshotId) {
    if (this.runner == null) {
        this.runner = new SparkBinPackFileRewriteRunner(spark(), table);
    }
}
```

**Planner 的选择**: Sort 和 Z-Order 策略(继承自 `SparkShufflingFileRewriteRunner`)使用 `SparkShufflingDataRewritePlanner`，BinPack 使用 `BinPackRewriteFilePlanner`。

#### 决策树

```
只需合并文件? --> BinPack
  |
单列/少数列范围过滤? --> Sort
  |
多列组合过滤? --> Z-Order
```

#### 面试加分话术

> "小文件合并策略选择的本质是**I/O 开销与查询性能的权衡**:
> 1. **BinPack 零 Shuffle**: 只做文件拼接，适合主要矛盾是文件数量的场景
> 2. **Sort 全局有序**: 牺牲写入时的 Shuffle 开销，换取查询时的高效裁剪
> 3. **Z-Order 多维聚类**: 通过空间填充曲线将多维问题降维，适合多列组合过滤
>
> 实践中我们通常采用**分层策略**: 每日 BinPack 快速合并，每周 Sort/Z-Order 深度优化。"

#### 高频追问

**Q: Z-Order 为什么不是万能的？什么场景下反而会降低性能？**

**A**: Z-Order 的局限性:
1. **单列高选择性查询退化**: 如果查询只过滤一列，Z-Order 的多维交织反而打散了数据
2. **高基数列效果差**: 当列的 cardinality 过高时，Z-Value 的聚类效果下降
3. **列数限制**: 通常只对 3-4 列有效，超过后维度灾难导致效果骤降

#### 30 秒速记（简版）

- 结论先行：Iceberg 通过 RewriteDataFiles action 解决小文件问题，提供三种策略（BinPack/Sort/Z-Order）。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/actions/RewriteDataFiles.java`（策略接口），再到 `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteDataFilesSparkAction.java`（Spark 实现），把"接口-策略选择-执行"串成一条线。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-4-2"></a>
### 4.2 垃圾回收机制

**🟡 问题: `ExpireSnapshots` 和 `DeleteOrphanFiles` 的区别和联系？**

**答案:**

两者是相辅相成的垃圾回收工具，职责不同。

#### ExpireSnapshots(快照过期)

**职责**: 清理元数据中过期的快照记录

**实现类**: `RemoveSnapshots`（`core/src/main/java/org/apache/iceberg/RemoveSnapshots.java`）

**工作原理**:
1. 检查 `gc.enabled` 表属性(默认 `true`)，若为 `false` 则抛出 `ValidationException`
2. 根据保留策略计算过期快照：
   - `max-snapshot-age-ms`(默认 5 天): 快照保留时间
   - `min-snapshots-to-keep`(默认 1): 最少保留快照数
   - `max-ref-age-ms`(默认 Long.MAX_VALUE): 引用最大保留时间
3. 对每个 branch ref，按 `maxSnapshotAgeMs` 和 `minSnapshotsToKeep` 计算要保留的祖先快照
4. 未被任何 ref 引用但尚未到期的快照也会被保留
5. 从元数据移除过期快照引用
6. 根据 `CleanupLevel` 决定是否删除文件:
   - 优先使用增量清理(`IncrementalFileCleanup`)：对比新旧元数据差异
   - 回退到可达性清理(`ReachableFileCleanup`)：扫描所有保留快照的文件

**源码关键逻辑**:

```java
// 源码: core/src/main/java/org/apache/iceberg/RemoveSnapshots.java
class RemoveSnapshots implements ExpireSnapshots {
    RemoveSnapshots(TableOperations ops) {
        this.ops = ops;
        this.base = ops.current();
        // gc.enabled 检查
        ValidationException.check(
            PropertyUtil.propertyAsBoolean(base.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
            "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)");

        long defaultMaxSnapshotAgeMs =
            PropertyUtil.propertyAsLong(base.properties(), MAX_SNAPSHOT_AGE_MS, MAX_SNAPSHOT_AGE_MS_DEFAULT);
        this.now = System.currentTimeMillis();
        this.defaultExpireOlderThan = now - defaultMaxSnapshotAgeMs;
        this.defaultMinNumSnapshots =
            PropertyUtil.propertyAsInt(base.properties(), MIN_SNAPSHOTS_TO_KEEP, MIN_SNAPSHOTS_TO_KEEP_DEFAULT);
    }

    // 提交后清理过期文件
    public void commit() {
        // ... 提交元数据更新 ...
        if (CleanupLevel.NONE != cleanupLevel && !base.snapshots().isEmpty()) {
            cleanExpiredSnapshots();
        }
    }

    // 选择增量清理或可达性清理策略
    private void cleanExpiredSnapshots() {
        FileCleanupStrategy cleanupStrategy = incrementalCleanup
            ? new IncrementalFileCleanup(ops.io(), deleteExecutorService, planExecutorService(), deleteFunc)
            : new ReachableFileCleanup(ops.io(), deleteExecutorService, planExecutorService(), deleteFunc);
        cleanupStrategy.cleanFiles(base, current, cleanupLevel);
    }
}
```

**特点**:
- 确定性高，安全性高
- 速度快(元数据层面)
- 无法处理从未被引用的孤儿文件
- 支持 `CleanupLevel`: ALL(清理数据+元数据), METADATA_ONLY(仅清理元数据文件), NONE(不清理)

#### DeleteOrphanFiles(孤儿文件清理)

**职责**: 清理存储中存在但不被任何快照引用的文件

**接口定义**: `api/src/main/java/org/apache/iceberg/actions/DeleteOrphanFiles.java`

**工作原理**:
1. 全量扫描物理存储中的所有文件
2. 遍历所有有效快照的元数据收集被引用的文件
3. 计算差集得到孤儿文件
4. 使用时间阈值(`olderThan`，默认 3 天)安全删除

**特点**:
- 大而全，清理所有类型孤儿文件
- 开销大，速度慢(需要 list 存储)
- 依赖时间戳保护
- 支持 URI 前缀不匹配处理(`PrefixMismatchMode`)

#### 完整 GC 策略

```
日常(每天): ExpireSnapshots
  | 快速清理大部分过期文件
定期(每周): DeleteOrphanFiles
  | 兜底清理异常产生的孤儿文件
```

**最佳实践**: 两者结合，构成健壮的文件生命周期管理。

#### 30 秒速记（简版）

- 结论先行：两者是相辅相成的垃圾回收工具，职责不同。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/RemoveSnapshots.java`（ExpireSnapshots 实现），再联动 `api/src/main/java/org/apache/iceberg/actions/DeleteOrphanFiles.java`（孤儿文件清理接口），把"定义-实现-结果"串成一条线。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

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

将多个小 Manifest 合并为少数大文件(默认 8MB，由 `commit.manifest.target-size-bytes` 控制)

**效果**: 1000 个小文件 -> 10 个大文件，I/O 开销显著降低

**2. 分区聚合(核心优化)**

将同一分区的数据文件条目聚集到同一 Manifest

**未优化前**:
```
分区 date='2026-02-03' 的文件信息散落在 100 个 Manifest
查询 WHERE date='2026-02-03' --> 需读取 100 个 Manifest
```

**优化后**:
```
分区 date='2026-02-03' 的文件信息聚合到 1 个 Manifest
查询 WHERE date='2026-02-03' --> 仅读取 1 个 Manifest
```

#### 源码实现

**实现类**: `RewriteManifestsSparkAction`（`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteManifestsSparkAction.java`）

该 action 同时处理数据 Manifest 和删除文件 Manifest:

```java
// 源码: RewriteManifestsSparkAction.java
private RewriteManifests.Result doExecute() {
    List<ManifestFile> rewrittenManifests = Lists.newArrayList();
    List<ManifestFile> addedManifests = Lists.newArrayList();

    // 分别重写数据 Manifest 和删除文件 Manifest
    RewriteManifests.Result dataResult = rewriteManifests(ManifestContent.DATA);
    Iterables.addAll(rewrittenManifests, dataResult.rewrittenManifests());
    Iterables.addAll(addedManifests, dataResult.addedManifests());

    RewriteManifests.Result deletesResult = rewriteManifests(ManifestContent.DELETES);
    Iterables.addAll(rewrittenManifests, deletesResult.rewrittenManifests());
    Iterables.addAll(addedManifests, deletesResult.addedManifests());

    // ... 提交替换 ...
}
```

**分区聚合的核心**: 通过 Spark 的 `repartitionByRange` 和 `sortWithinPartitions` 实现:

```java
// 源码: RewriteManifestsSparkAction.java
private List<ManifestFile> writePartitionedManifests(
    ManifestContent content, Dataset<Row> manifestEntryDF, int numManifests) {
    return withReusableDS(
        manifestEntryDF,
        df -> {
            WriteManifests<?> writeFunc = newWriteManifestsFunc(content, df.schema());
            Dataset<Row> transformedDF = repartitionAndSort(df, sortColumn(), numManifests);
            return writeFunc.apply(transformedDF).collectAsList();
        });
}

// 核心: 按分区列重分区并排序
private Dataset<Row> repartitionAndSort(Dataset<Row> df, Column col, int numPartitions) {
    return df.repartitionByRange(numPartitions, col).sortWithinPartitions(col);
}

// 排序列选择: 默认按 data_file.partition 排序，也支持自定义分区字段
private Column sortColumn() {
    if (partitionFieldClustering != null) {
        Column[] partitionColumns =
            partitionFieldClustering.stream()
                .map(p -> col("data_file.partition." + p))
                .toArray(Column[]::new);
        return functions.struct(partitionColumns);
    } else {
        return new Column("data_file.partition");
    }
}
```

**Manifest 入口数据构建**: 从 `ENTRIES` 元数据表读取 live entries，然后与待重写 Manifest 做 left semi join:

```java
// 源码: RewriteManifestsSparkAction.java
private Dataset<Row> buildManifestEntryDF(List<ManifestFile> manifests) {
    Dataset<Row> manifestDF =
        spark().createDataset(Lists.transform(manifests, ManifestFile::path), Encoders.STRING())
            .toDF("manifest");

    Dataset<Row> manifestEntryDF =
        loadMetadataTable(table, ENTRIES)
            .filter("status < 2") // 仅选择 live entries (EXISTING=0, ADDED=1)
            .selectExpr(
                "input_file_name() as manifest",
                "snapshot_id", "sequence_number", "file_sequence_number", "data_file");

    Column joinCond = manifestDF.col("manifest").equalTo(manifestEntryDF.col("manifest"));
    return manifestEntryDF
        .join(manifestDF, joinCond, "left_semi")
        .select("snapshot_id", "sequence_number", "file_sequence_number", "data_file");
}
```

**效果**: 元数据层面的高效分区剪枝

#### 30 秒速记（简版）

- 结论先行：RewriteManifests 通过优化 Manifest 文件解决"Manifest 膨胀"问题。
- 主链路：先定位 `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/RewriteManifestsSparkAction.java`（Spark 实现），核心方法 `repartitionAndSort` 使用 `repartitionByRange` + `sortWithinPartitions` 实现分区聚合。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-4-4"></a>
### 4.4 孤儿文件清理安全机制

**🟡 问题: `DeleteOrphanFiles` 有哪些安全保护机制防止误删？**

**答案:**

Iceberg 内置多重保护机制确保数据安全。

#### 1. 时间阈值保护

**机制**: 必须提供 `olderThan` 时间戳，仅删除修改时间早于该时间的文件

**目的**: 为正在写入的文件提供安全缓冲期

**默认值**: 3 天前（见 `DeleteOrphanFiles` 接口 Javadoc: "If not set, defaults to a timestamp 3 days ago"）

**示例**:
```java
deleteOrphanFiles()
  .olderThan(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3))
  .execute();
```

#### 2. 垃圾回收总开关

**机制**: `ExpireSnapshots` 检查表属性 `gc.enabled`(默认 `true`)

**源码确认**:
```java
// 源码: core/src/main/java/org/apache/iceberg/RemoveSnapshots.java
RemoveSnapshots(TableOperations ops) {
    this.base = ops.current();
    ValidationException.check(
        PropertyUtil.propertyAsBoolean(base.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
        "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)");
}
```

**注意**: 此保护在 `ExpireSnapshots`(`RemoveSnapshots`) 构造函数中检查。`DeleteOrphanFiles` 的具体实现中是否检查 `gc.enabled` 取决于引擎端实现（如 Spark 的 `DeleteOrphanFilesSparkAction`）。

**使用场景**:
- 多团队共享数据
- 数据迁移期间
- 怀疑配置问题时

#### 3. URI 前缀不匹配处理

**机制**: 处理不同 URI scheme 导致的路径不一致问题

**策略**(`PrefixMismatchMode`):
- `ERROR`(默认): 立即报错，最安全
- `IGNORE`: 跳过不匹配文件
- `DELETE`: 删除不匹配文件(危险)

**辅助方法**: 还提供了 `equalSchemes(Map)` 和 `equalAuthorities(Map)` 来声明等价的 scheme/authority:

```java
// 源码: api/src/main/java/org/apache/iceberg/actions/DeleteOrphanFiles.java
default DeleteOrphanFiles equalSchemes(Map<String, String> newEqualSchemes) { ... }
default DeleteOrphanFiles equalAuthorities(Map<String, String> newEqualAuthorities) { ... }
```

**目的**: 防止因 URI 不一致（如 `s3://` vs `s3a://`）误删有效文件

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

#### 面试加分话术

> "孤儿文件清理是高风险操作，Iceberg 通过**四层防护**确保安全:
> 1. **时间阈值**: `olderThan` 参数保护正在写入的文件
> 2. **全局开关**: `gc.enabled` 作为最后一道防线
> 3. **URI 校验**: `PrefixMismatchMode` + `equalSchemes/equalAuthorities` 防止因 scheme 不一致导致的误删
> 4. **Dry-Run**: 先演练再执行，可逆操作
>
> 实践中最重要的是**时间阈值设置**: 必须大于最长可能的事务持续时间 + 文件系统时钟偏差。
> 我们通常设置 3 天，确保即使有长事务也不会误删。"

#### 高频追问

**Q: 为什么会产生孤儿文件？常见原因有哪些？**

**A**: 孤儿文件的产生场景:
1. **写入失败**: 文件写入成功但 commit 失败，文件未被引用
2. **Compaction 中断**: 重写文件时任务失败，新文件成为孤儿
3. **Spark 任务重试**: 推测执行产生的重复文件
4. **手动干预**: 人为删除元数据但未删除数据文件

**预防措施**: 定期执行 `DeleteOrphanFiles`，配合 `ExpireSnapshots` 形成完整的 GC 策略。

#### 30 秒速记（简版）

- 结论先行：Iceberg 内置多重保护机制确保数据安全。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/actions/DeleteOrphanFiles.java`（接口定义，含 `PrefixMismatchMode` 枚举），再联动 `core/src/main/java/org/apache/iceberg/RemoveSnapshots.java`（`gc.enabled` 检查），把安全机制串成一条线。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

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
SQL 查询 --> 定位当前快照(或时间旅行指定快照)
```

**2. 分区剪枝(第一层: Manifest 文件级别)**

- **输入**: `WHERE` 条件(如 `event_date='2026-02-03'`)
- **动作**: 读取 Manifest List，检查每个 Manifest File 的分区范围统计（`PartitionFieldSummary`）
- **剪枝**: 过滤不包含目标分区的 Manifest File

**示例**:
```
Manifest File A: event_date in ['2026-01-01', '2026-01-31'] --> 跳过
Manifest File B: event_date in ['2026-02-01', '2026-02-28'] --> 保留
```

**3. 数据文件剪枝(第二层: 文件级别)**

- **输入**: `WHERE` 条件和幸存的 Manifest File
- **动作**: 读取 Manifest File，检查每个数据文件的分区值（通过 `Evaluator`）和列级统计（通过 `InclusiveMetricsEvaluator`，含 min/max/null_count/nan_count）
- **剪枝**:
  - 分区剪枝: 分区值不满足过滤条件 -> 跳过
  - Min/Max 剪枝: `WHERE price>500` 且 `max(price)=450` -> 跳过
  - Null 剪枝: `WHERE user_id IS NOT NULL` 且 `null_count=record_count` -> 跳过

**4. 生成扫描任务**
```
剩余数据文件 --> FileScanTask --> 分发到 Executor
```

**5. 读取数据**
```
Executor --> 打开文件 --> 应用删除文件 --> 返回数据
```

#### 核心优势

通过两层元数据索引(Manifest List 分区统计 + Manifest File 列统计)，避免传统数据湖的"List + 暴力扫描"模式。

# 5.1 多层剪枝机制 - 源码深度扩展

#### 剪枝的源码实现

**核心类**: `ManifestGroup`（包级私有）

```java
// 源码: core/src/main/java/org/apache/iceberg/ManifestGroup.java
class ManifestGroup {
    private final FileIO io;
    private final Set<ManifestFile> dataManifests;                  // 数据文件的 Manifest
    private final DeleteFileIndex.Builder deleteIndexBuilder;       // 删除文件索引构建器

    private Expression dataFilter;       // 数据过滤器，默认 alwaysTrue()
    private Expression fileFilter;       // 文件过滤器，默认 alwaysTrue()
    private Expression partitionFilter;  // 分区过滤器，默认 alwaysTrue()

    /**
     * 设置数据过滤器(用于列级别剪枝)
     */
    ManifestGroup filterData(Expression newDataFilter) {
        this.dataFilter = Expressions.and(dataFilter, newDataFilter);
        deleteIndexBuilder.filterData(newDataFilter);
        return this;
    }

    /**
     * 设置分区过滤器(用于分区级别剪枝)
     */
    ManifestGroup filterPartitions(Expression newPartitionFilter) {
        this.partitionFilter = Expressions.and(partitionFilter, newPartitionFilter);
        deleteIndexBuilder.filterPartitions(newPartitionFilter);
        return this;
    }
}
```

#### 第一层剪枝: Manifest 文件级别

**使用 ManifestEvaluator 进行分区剪枝**:

```java
// 源码: api/src/main/java/org/apache/iceberg/expressions/ManifestEvaluator.java
public class ManifestEvaluator {
    // 注意: 只持有一个绑定后的表达式字段，不持有 PartitionSpec
    private final Expression expr;

    // 工厂方法1: 基于行过滤器，自动通过 Projections.inclusive 投影到分区表达式
    public static ManifestEvaluator forRowFilter(
        Expression rowFilter, PartitionSpec spec, boolean caseSensitive) {
        return new ManifestEvaluator(
            spec, Projections.inclusive(spec, caseSensitive).project(rowFilter), caseSensitive);
    }

    // 工厂方法2: 直接使用已有的分区过滤器（ManifestGroup 内部使用此方法）
    public static ManifestEvaluator forPartitionFilter(
        Expression partitionFilter, PartitionSpec spec, boolean caseSensitive) {
        return new ManifestEvaluator(spec, partitionFilter, caseSensitive);
    }

    private ManifestEvaluator(PartitionSpec spec, Expression partitionFilter, boolean caseSensitive) {
        // 将分区过滤表达式绑定到分区类型，并重写 NOT 操作
        this.expr = Binder.bind(spec.partitionType(), rewriteNot(partitionFilter), caseSensitive);
    }

    /**
     * 评估 Manifest 文件是否可能包含匹配的数据。
     * 内部通过 ManifestEvalVisitor（继承 BoundExpressionVisitor<Boolean>）遍历表达式树。
     */
    public boolean eval(ManifestFile manifest) {
        return new ManifestEvalVisitor().eval(manifest);
    }

    // 内部类 ManifestEvalVisitor 核心逻辑:
    // 1. 读取 manifest.partitions() 获取 PartitionFieldSummary 列表
    // 2. 若 partitions() == null，保守返回 ROWS_MIGHT_MATCH (true)
    // 3. 对不同操作逐一评估 lowerBound/upperBound:
    //    - eq(ref, lit): lower > lit || upper < lit --> ROWS_CANNOT_MATCH
    //    - gt(ref, lit): upper <= lit --> ROWS_CANNOT_MATCH
    //    - lt(ref, lit): lower >= lit --> ROWS_CANNOT_MATCH
    //    - isNull(ref): !containsNull() --> ROWS_CANNOT_MATCH
    //    - notNull(ref): allValuesAreNull(summary) --> ROWS_CANNOT_MATCH
    //    - in(ref, set): 过滤掉 < lower 或 > upper 的值，若为空 --> ROWS_CANNOT_MATCH
    //    - startsWith(ref, prefix): 截断 lower/upper 与 prefix 比较
}
```

**ManifestGroup 中的应用**:

```java
// 源码: core/src/main/java/org/apache/iceberg/ManifestGroup.java
// entries() 方法内部，第一层剪枝的核心逻辑:

private <T> Iterable<CloseableIterable<T>> entries(...) {
    // 1. 为每个 PartitionSpec 创建缓存的 ManifestEvaluator
    LoadingCache<Integer, ManifestEvaluator> evalCache =
        specsById == null ? null :
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

    // 2. 过滤 Manifest 文件（第一层剪枝）
    CloseableIterable<ManifestFile> matchingManifests =
        evalCache == null
            ? closeableDataManifests
            : CloseableIterable.filter(
                scanMetrics.skippedDataManifests(),
                closeableDataManifests,
                manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));
    // 注意: 使用 forPartitionFilter（非 forRowFilter），因为 dataFilter 已手动投影

    // 3. 对每个幸存的 Manifest，创建 ManifestReader 进行第二层剪枝
    return Iterables.transform(matchingManifests, manifest -> {
        ManifestReader<DataFile> reader =
            ManifestFiles.read(manifest, io, specsById)
                .filterRows(dataFilter)          // 传入行级过滤器
                .filterPartitions(partitionFilter) // 传入分区过滤器
                .caseSensitive(caseSensitive)
                .select(columns)
                .scanMetrics(scanMetrics);
        // ...
    });
}
```

**剪枝示例**:

```
查询: SELECT * FROM table WHERE event_date='2026-02-10'

Manifest List 包含 100 个 Manifest 文件:
- Manifest 1: partitions=[{lower:'2026-01-01', upper:'2026-01-31'}]
  --> evaluator.eval() = false (2026-02-10 不在范围内)
  --> 跳过,无需读取

- Manifest 2: partitions=[{lower:'2026-02-01', upper:'2026-02-28'}]
  --> evaluator.eval() = true (2026-02-10 在范围内)
  --> 保留,继续处理

结果: 100 个 Manifest 文件 --> 剪枝后剩余 3 个
```

#### 第二层剪枝: 数据文件级别

**由 ManifestReader 内部执行**，使用两个评估器:

```java
// 源码: core/src/main/java/org/apache/iceberg/ManifestReader.java
private CloseableIterable<ManifestEntry<F>> entries(boolean onlyLive) {
    if (hasRowFilter() || hasPartitionFilter() || partitionSet != null) {
        Evaluator evaluator = evaluator();                       // 分区评估器
        InclusiveMetricsEvaluator metricsEvaluator = metricsEvaluator(); // 列统计评估器

        // 确保统计列存在以支持 metrics 评估
        boolean requireStatsProjection = requireStatsProjection(rowFilter, columns);
        Collection<String> projectColumns =
            requireStatsProjection ? withStatsColumns(columns) : columns;

        CloseableIterable<ManifestEntry<F>> entries = open(projection(...));

        // 核心: 对每个 entry 同时应用分区剪枝和列统计剪枝
        return CloseableIterable.filter(
            scanMetrics.skippedDataFiles(),
            onlyLive ? filterLiveEntries(entries) : entries,
            entry -> entry != null
                && evaluator.eval(entry.file().partition())      // 分区值评估
                && metricsEvaluator.eval(entry.file())           // 列统计评估(min/max/null)
                && inPartitionSet(entry.file()));                // 分区集合检查
    }
    // ...
}
```

**InclusiveMetricsEvaluator 核心逻辑**:

```java
// 源码: api/src/main/java/org/apache/iceberg/expressions/InclusiveMetricsEvaluator.java
public class InclusiveMetricsEvaluator {
    // 评估一个数据文件是否可能包含匹配的行
    public boolean eval(ContentFile<?> file) {
        return new MetricsEvalVisitor().eval(file);
    }

    private class MetricsEvalVisitor extends BoundVisitor<Boolean> {
        private boolean eval(ContentFile<?> file) {
            if (file.recordCount() == 0) {
                return ROWS_CANNOT_MATCH;   // 空文件直接跳过
            }
            // 读取文件的统计信息
            this.valueCounts = file.valueCounts();
            this.nullCounts = file.nullValueCounts();
            this.nanCounts = file.nanValueCounts();
            this.lowerBounds = file.lowerBounds();
            this.upperBounds = file.upperBounds();
            return ExpressionVisitors.visitEvaluator(expr, this);
        }

        // 示例: gt(ref, lit) -- WHERE price > 1000
        public <T> Boolean gt(Bound<T> term, Literal<T> lit) {
            int id = term.ref().fieldId();
            if (containsNullsOnly(id) || containsNaNsOnly(id)) {
                return ROWS_CANNOT_MATCH;
            }
            T upper = upperBound(term);     // 获取文件的 upper bound
            if (null == upper || NaNUtil.isNaN(upper)) {
                return ROWS_MIGHT_MATCH;    // 无统计信息时保守返回 true
            }
            int cmp = lit.comparator().compare(upper, lit.value());
            if (cmp <= 0) {
                return ROWS_CANNOT_MATCH;   // upper <= 1000, 不可能有 > 1000 的值
            }
            return ROWS_MIGHT_MATCH;
        }

        // 示例: notNull(ref) -- WHERE user_id IS NOT NULL
        public <T> Boolean notNull(Bound<T> term) {
            int id = term.ref().fieldId();
            if (containsNullsOnly(id)) {   // 所有值都是 null
                return ROWS_CANNOT_MATCH;
            }
            return ROWS_MIGHT_MATCH;
        }

        // 辅助方法: 检查文件中某列是否全部为 null
        private boolean containsNullsOnly(int id) {
            if (nullCounts != null && nullCounts.containsKey(id) && valueCounts != null) {
                Long nullCount = nullCounts.get(id);
                Long valueCount = valueCounts.get(id);
                return nullCount != null && valueCount != null
                    && nullCount.equals(valueCount);
            }
            return false;
        }
    }
}
```

**剪枝示例**:

```
查询: SELECT * FROM table WHERE price > 1000

Manifest 文件包含 1000 个数据文件:
- file_1.parquet:
    lowerBounds={price: 100}, upperBounds={price: 500}
  --> upperBounds[price]=500 <= 1000
  --> ROWS_CANNOT_MATCH, 跳过

- file_2.parquet:
    lowerBounds={price: 800}, upperBounds={price: 1200}
  --> upperBounds[price]=1200 > 1000
  --> ROWS_MIGHT_MATCH, 保留

- file_3.parquet:
    nullValueCounts={price: 10000}, valueCounts={price: 10000}
  --> containsNullsOnly(priceFieldId) = true
  --> ROWS_CANNOT_MATCH, 跳过

结果: 1000 个数据文件 --> 剪枝后剩余 50 个
```

#### 完整的剪枝流程

```java
// 源码: core/src/main/java/org/apache/iceberg/ManifestGroup.java
public CloseableIterable<FileScanTask> planFiles() {
    return plan(ManifestGroup::createFileScanTasks);
}

public <T extends ScanTask> CloseableIterable<T> plan(CreateTasksFunction<T> createTasksFunc) {
    // 1. 构建 ResidualEvaluator 缓存 (残差过滤器)
    LoadingCache<Integer, ResidualEvaluator> residualCache =
        Caffeine.newBuilder().build(specId -> {
            PartitionSpec spec = specsById.get(specId);
            Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : dataFilter;
            return ResidualEvaluator.of(spec, filter, caseSensitive);
        });

    // 2. 构建删除文件索引
    DeleteFileIndex deleteFiles = deleteIndexBuilder.scanMetrics(scanMetrics).build();

    // 3. 构建 TaskContext 缓存
    LoadingCache<Integer, TaskContext> taskContextCache =
        Caffeine.newBuilder().build(specId -> {
            PartitionSpec spec = specsById.get(specId);
            ResidualEvaluator residuals = residualCache.get(specId);
            return new TaskContext(spec, deleteFiles, residuals, dropStats, columnsToKeepStats, scanMetrics);
        });

    // 4. 遍历 entries (内部执行两层剪枝) 并创建 FileScanTask
    Iterable<CloseableIterable<T>> tasks =
        entries((manifest, entries) -> {
            int specId = manifest.partitionSpecId();
            TaskContext taskContext = taskContextCache.get(specId);
            return createTasksFunc.apply(entries, taskContext);
        });

    // 5. 并行或串行返回
    return executorService != null
        ? new ParallelIterable<>(tasks, executorService)
        : CloseableIterable.concat(tasks);
}

// createFileScanTasks: 将 ManifestEntry 转为 FileScanTask
private static CloseableIterable<FileScanTask> createFileScanTasks(
    CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext ctx) {
    return CloseableIterable.transform(entries, entry -> {
        DataFile dataFile = ContentFileUtil.copy(entry.file(), ctx.shouldKeepStats(), ctx.columnsToKeepStats());
        DeleteFile[] deleteFiles = ctx.deletes().forEntry(entry);   // 关联删除文件
        return new BaseFileScanTask(
            dataFile, deleteFiles, ctx.schemaAsString(), ctx.specAsString(), ctx.residuals());
    });
}
```

**性能优势**:

```
原始场景:
- 100 个 Manifest 文件
- 每个 Manifest 包含 1000 个数据文件
- 总共 100,000 个数据文件

经过两层剪枝:
- 第一层: 100 个 Manifest --> 3 个 Manifest (减少 97%)
- 第二层: 3,000 个数据文件 --> 50 个数据文件 (减少 98.3%)

最终: 仅需读取 50 个数据文件,而非 100,000 个
I/O 减少: 99.95%
```

**深入理解**: Iceberg 的多层剪枝机制是其查询性能的核心优势。通过 Manifest List 的分区统计和 Manifest File 的列统计,在查询规划阶段就能大幅减少需要读取的文件数量。这种设计避免了传统数据湖的"List + 暴力扫描"模式,使得 Iceberg 能够在 PB 级数据规模下保持秒级查询响应。

#### 面试加分话术

> "Iceberg 的多层剪枝是**元数据驱动查询优化**的典范:
> 1. **第一层 ManifestEvaluator 剪枝**: 在 `ManifestGroup.entries()` 中，基于 `PartitionFieldSummary` 的 lowerBound/upperBound 跳过整个 Manifest 文件
> 2. **第二层 ManifestReader 内部剪枝**: 通过 `Evaluator`(分区值评估) + `InclusiveMetricsEvaluator`(min/max/null_count/nan_count 评估) 双重过滤跳过数据文件
> 3. **残差过滤器(Residual)**: 无法在元数据层剪枝的条件，生成 Residual 表达式下推到文件读取层
>
> 这种**层层递进的剪枝策略**，使得 PB 级数据的查询规划能在秒级完成，这是 Iceberg 相比传统 Hive 的核心优势。"

#### 高频追问

**Q: 如果数据文件没有 min/max 统计信息，Iceberg 会怎么处理？**

**A**: Iceberg 采用保守策略:
```java
// 源码: api/src/main/java/org/apache/iceberg/expressions/InclusiveMetricsEvaluator.java
// MetricsEvalVisitor 中，当统计信息缺失时:
public <T> Boolean gt(Bound<T> term, Literal<T> lit) {
    T upper = upperBound(term);
    if (null == upper || NaNUtil.isNaN(upper)) {
        return ROWS_MIGHT_MATCH;  // 无统计信息时，保守地保留文件
    }
    // ...
}
```
这意味着:
1. 老版本写入的文件（无统计）会退化为全量扫描
2. 某些文件格式不支持统计时同样退化
3. **最佳实践**: 使用 RewriteDataFiles 重写老文件以补全统计信息

---

# 5.1 附: Position Delete 应用机制 - 源码深度扩展

#### Position Delete 的核心数据结构

**PositionDeleteIndex 接口**:

```java
// 源码: core/src/main/java/org/apache/iceberg/deletes/PositionDeleteIndex.java
public interface PositionDeleteIndex {
    /** 标记一个位置为已删除 */
    void delete(long position);

    /** 标记一个位置范围为已删除 [posStart, posEnd) */
    void delete(long posStart, long posEnd);

    /** 合并另一个索引（默认实现通过 forEach + delete 逐个合并） */
    default void merge(PositionDeleteIndex that) { ... }

    /** 检查某个位置是否已删除 */
    boolean isDeleted(long position);

    /** 返回索引是否为空 */
    boolean isEmpty();

    /** 遍历所有已删除位置 */
    default void forEach(LongConsumer consumer) { ... }

    /** 返回关联的删除文件列表 */
    default Collection<DeleteFile> deleteFiles() { ... }

    /** 返回已删除位置的数量 */
    default long cardinality() { ... }

    /** 序列化索引 */
    default ByteBuffer serialize() { ... }

    /** 反序列化索引 */
    static PositionDeleteIndex deserialize(byte[] bytes, DeleteFile deleteFile) {
        return BitmapPositionDeleteIndex.deserialize(bytes, deleteFile);
    }

    /** 返回空的不可变索引 */
    static PositionDeleteIndex empty() {
        return EmptyPositionDeleteIndex.get();
    }
}
```

#### BitmapPositionDeleteIndex 实现

**核心设计**: 使用 **RoaringPositionBitmap** 存储删除位置。RoaringPositionBitmap 是 Iceberg 自实现的 64 位扩展，内部使用 32 位 RoaringBitmap 数组。

```java
// 源码: core/src/main/java/org/apache/iceberg/deletes/BitmapPositionDeleteIndex.java
class BitmapPositionDeleteIndex implements PositionDeleteIndex {
    private final RoaringPositionBitmap bitmap;  // 核心: 64 位 Roaring Bitmap
    private final List<DeleteFile> deleteFiles;  // 关联的删除文件

    @Override
    public void delete(long position) {
        bitmap.set(position);  // 在位图中设置该位置
    }

    @Override
    public void delete(long posStart, long posEnd) {
        bitmap.setRange(posStart, posEnd);  // 范围设置
    }

    @Override
    public boolean isDeleted(long position) {
        return bitmap.contains(position);  // O(1) 时间复杂度
    }

    // 类型化合并: 直接使用 bitmap.setAll 进行高效 OR 操作
    void merge(BitmapPositionDeleteIndex that) {
        bitmap.setAll(that.bitmap);
        deleteFiles.addAll(that.deleteFiles);
    }

    // 通用合并: 对非 BitmapPositionDeleteIndex 类型，逐个 delete
    @Override
    public void merge(PositionDeleteIndex that) {
        if (that instanceof BitmapPositionDeleteIndex) {
            merge((BitmapPositionDeleteIndex) that);
        } else {
            that.forEach(this::delete);
            deleteFiles.addAll(that.deleteFiles());
        }
    }
}
```

**RoaringPositionBitmap 的设计**:

```java
// 源码: core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java
// 支持 64 位正整数位置，但针对 32 位场景优化:
// - 64 位位置拆为高 32 位 (key) 和低 32 位 (position)
// - 每个 key 对应一个 32 位 RoaringBitmap
// - MAX_POSITION = toPosition(Integer.MAX_VALUE - 1, Integer.MIN_VALUE)
```

**Roaring Bitmap 的优势**:

```
传统 Bitmap:
- 存储 1,000,000 个位置需要 125KB (1,000,000 / 8)
- 即使只有 10 个删除位置,也需要 125KB

Roaring Bitmap:
- 压缩存储,仅存储实际的删除位置
- 10 个删除位置仅需约 100 字节
- 查询复杂度: O(1)
```

#### Position Delete 的加载流程

**通过 BaseDeleteLoader 加载**:

```java
// 源码: data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java
@Override
public PositionDeleteIndex loadPositionDeletes(
    Iterable<DeleteFile> deleteFiles, CharSequence filePath) {
    // 优先处理 Deletion Vector (V3)
    if (ContentFileUtil.containsSingleDV(deleteFiles)) {
        DeleteFile dv = Iterables.getOnlyElement(deleteFiles);
        validateDV(dv, filePath);
        return readDV(dv);                   // 直接读取 DV 二进制数据并反序列化
    } else {
        return getOrReadPosDeletes(deleteFiles, filePath);  // 读取 V2 Position Delete 文件
    }
}

// V3 Deletion Vector 读取: 直接读取 Puffin 文件中的二进制 blob
private PositionDeleteIndex readDV(DeleteFile dv) {
    InputFile inputFile = loadInputFile.apply(dv);
    long offset = dv.contentOffset();
    int length = dv.contentSizeInBytes().intValue();
    byte[] bytes = readBytes(inputFile, offset, length);
    return PositionDeleteIndex.deserialize(bytes, dv);  // 反序列化为 BitmapPositionDeleteIndex
}

// V2 Position Delete 文件读取: 支持缓存
private PositionDeleteIndex getOrReadPosDeletes(
    Iterable<DeleteFile> deleteFiles, CharSequence filePath) {
    Iterable<PositionDeleteIndex> deletes =
        execute(deleteFiles, deleteFile -> getOrReadPosDeletes(deleteFile, filePath));
    return PositionDeleteIndexUtil.merge(deletes);   // 合并多个索引
}

// 读取单个 Position Delete 文件
private PositionDeleteIndex readPosDeletes(DeleteFile deleteFile, CharSequence filePath) {
    // 通过 file_path 过滤，只读取与目标数据文件相关的删除记录
    Expression filter = Expressions.equal(MetadataColumns.DELETE_FILE_PATH.name(), filePath);
    CloseableIterable<Record> deletes = openDeletes(deleteFile, POS_DELETE_SCHEMA, filter);
    return Deletes.toPositionIndex(filePath, deletes, deleteFile);
}

// 合并多个索引
// 源码: core/src/main/java/org/apache/iceberg/deletes/PositionDeleteIndexUtil.java
public static PositionDeleteIndex merge(Iterable<? extends PositionDeleteIndex> indexes) {
    BitmapPositionDeleteIndex result = new BitmapPositionDeleteIndex();
    indexes.forEach(result::merge);
    return result;
}
```

#### Position Delete 的应用流程

**在 DeleteFilter 中应用删除**:

```java
// 源码: data/src/main/java/org/apache/iceberg/data/DeleteFilter.java
public abstract class DeleteFilter<T> {
    private final String filePath;
    private final List<DeleteFile> posDeletes;     // 位置删除文件
    private final List<DeleteFile> eqDeletes;      // 等值删除文件
    private final Accessor<StructLike> posAccessor; // 行位置访问器

    // 核心过滤方法: 先应用 Position Delete，再应用 Equality Delete
    public CloseableIterable<T> filter(CloseableIterable<T> records) {
        return applyEqDeletes(applyPosDeletes(records));
    }

    // 应用 Position Delete
    private CloseableIterable<T> applyPosDeletes(CloseableIterable<T> records) {
        if (posDeletes.isEmpty()) {
            return records;
        }
        // 1. 加载 Position Delete 索引
        PositionDeleteIndex positionIndex = deletedRowPositions();
        // 2. 构建过滤谓词: 通过 posAccessor 获取行位置，检查是否在删除索引中
        Predicate<T> isDeleted = record -> positionIndex.isDeleted(pos(record));
        return createDeleteIterable(records, isDeleted);
    }

    // 加载删除索引(延迟加载)
    public PositionDeleteIndex deletedRowPositions() {
        if (deleteRowPositions == null && !posDeletes.isEmpty()) {
            this.deleteRowPositions = deleteLoader().loadPositionDeletes(posDeletes, filePath);
        }
        return deleteRowPositions;
    }

    // 获取行位置
    protected long pos(T record) {
        return (Long) posAccessor.get(asStructLike(record));
    }
}
```

**关键设计点**: DeleteFilter 通过 `posAccessor`（从 `_pos` 元数据列获取行位置）结合 `PositionDeleteIndex.isDeleted()` 来判断行是否被删除。读取数据文件时会自动包含 `_pos` 列。

#### Deletion Vector (V3 特性)

**存储格式优化**:

```
V2: Position Delete 文件
- 文件格式: Parquet/Avro
- Schema: {file_path: string, pos: long}
- 每个删除位置约 50 字节
- 一个 Position Delete 文件可引用多个数据文件

V3: Deletion Vector (DV)
- 文件格式: Puffin (二进制 Blob)
- 内容: 序列化的 Roaring Bitmap
- 每个 DV 与一个数据文件一一对应
- 每个删除位置约 0.1 字节 (压缩后)
```

**序列化格式**:

```java
// 源码: core/src/main/java/org/apache/iceberg/deletes/BitmapPositionDeleteIndex.java
// 序列化格式:
// [4 bytes: bitmapDataLength (big-endian)]
// [4 bytes: MAGIC_NUMBER=1681511377 (little-endian)]
// [N bytes: Roaring Bitmap 数据 (little-endian, portable Roaring spec)]
// [4 bytes: CRC-32 checksum (big-endian)]

@Override
public ByteBuffer serialize() {
    bitmap.runLengthEncode();  // 先进行 run-length 编码压缩
    int bitmapDataLength = computeBitmapDataLength(bitmap);
    byte[] bytes = new byte[LENGTH_SIZE_BYTES + bitmapDataLength + CRC_SIZE_BYTES];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.putInt(bitmapDataLength);
    serializeBitmapData(bytes, bitmapDataLength, bitmap); // little-endian 写入
    int crc = computeChecksum(bytes, bitmapDataLength);
    buffer.putInt(LENGTH_SIZE_BYTES + bitmapDataLength, crc);
    buffer.rewind();
    return buffer;
}

public static PositionDeleteIndex deserialize(byte[] bytes, DeleteFile deleteFile) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int bitmapDataLength = readBitmapDataLength(buffer, deleteFile);
    RoaringPositionBitmap bitmap = deserializeBitmap(bytes, bitmapDataLength, deleteFile);
    // 验证 CRC
    int crc = computeChecksum(bytes, bitmapDataLength);
    int expectedCrc = buffer.getInt(LENGTH_SIZE_BYTES + bitmapDataLength);
    Preconditions.checkArgument(crc == expectedCrc, "Invalid CRC");
    return new BitmapPositionDeleteIndex(bitmap, deleteFile);
}
```

**存储效率对比**:

```
场景: 1,000,000 行数据,删除 10,000 行

V2 Position Delete 文件:
- 10,000 行 x ~50 字节 = ~500KB

V3 Deletion Vector:
- Roaring Bitmap 压缩后: ~10KB
- 压缩率: ~98%
```

**深入理解**: Position Delete 通过 Roaring Bitmap 实现了高效的删除位置索引。Bitmap 的 O(1) 查询复杂度和极高的压缩率,使得 Iceberg 能够在 Merge-on-Read 模式下保持高性能。V3 的 Deletion Vector 进一步优化了存储效率并建立了数据文件与删除之间的一一对应关系,消除了 V2 中 Position Delete 文件可能引用多个数据文件带来的扫描开销。

---

#### 30 秒速记（简版）

- 结论先行：Iceberg 通过多层次、由粗到细的剪枝机制避免不必要的 I/O。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/ManifestGroup.java`（第一层: ManifestEvaluator 过滤 Manifest），再联动 `core/src/main/java/org/apache/iceberg/ManifestReader.java`（第二层: Evaluator + InclusiveMetricsEvaluator 过滤 DataFile），以及 `api/src/main/java/org/apache/iceberg/expressions/InclusiveMetricsEvaluator.java`（列统计评估器），把"定义-实现-结果"串成一条线。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-5-2"></a>
### 5.2 谓词下推优化

**🟡 问题: 解释 Iceberg 如何利用元数据进行 Filter Pushdown。**

**答案:**

Iceberg 的谓词下推通过元数据统计信息在查询规划阶段过滤文件。

#### 分区级别剪枝(Manifest List)

**元数据**: 每个 Manifest File 的分区范围（`PartitionFieldSummary` 包含 `lowerBound`、`upperBound`、`containsNull`、`containsNaN`）

**实现类**: `ManifestEvaluator`（`api/src/main/java/org/apache/iceberg/expressions/ManifestEvaluator.java`）

**示例**:
```json
{
  "manifest_path": "...",
  "partitions": [
    {"lower_bound": "2026-01-01", "upper_bound": "2026-01-15", "contains_null": false},
    {"lower_bound": "A", "upper_bound": "C", "contains_null": false}
  ]
}
```

**工作原理**:
```sql
WHERE event_date='2026-02-10'
--> '2026-02-10' 不在 ['2026-01-01', '2026-01-15'] 范围内
--> 跳过该 Manifest File 及其所有数据文件
```

#### 列级别剪枝(Manifest Entry)

**元数据**: 每个数据文件的列统计(`lower_bounds`, `upper_bounds`, `null_value_counts`, `nan_value_counts`, `value_counts`)

**实现类**: `InclusiveMetricsEvaluator`（`api/src/main/java/org/apache/iceberg/expressions/InclusiveMetricsEvaluator.java`）

**Min/Max 剪枝**:
```sql
WHERE price > 1000
--> 检查 upper_bounds[price]
--> upper_bounds[price]=800 --> 跳过该文件
```

**Null 剪枝**:
```sql
WHERE user_id IS NOT NULL
--> 检查 null_value_counts[user_id] 和 value_counts[user_id]
--> null_count = value_count --> 该列全部为 null --> 跳过该文件
```

**NaN 剪枝**:
```sql
WHERE temperature > 0
--> 检查 nan_value_counts[temperature]
--> 若全部为 NaN --> 跳过该文件
```

#### 谓词投影(Projection)

Iceberg 使用 `Projections.inclusive()` 将行级过滤表达式投影到分区表达式:

```java
// 源码: ManifestGroup.java
// 将 dataFilter (行级别) 通过 inclusive projection 转为分区级别
ManifestEvaluator.forPartitionFilter(
    Expressions.and(
        partitionFilter,
        Projections.inclusive(spec, caseSensitive).project(dataFilter)),
    spec, caseSensitive);
```

**inclusive 投影的含义**: 投影后的表达式是原始表达式的"宽松"版本，即: 如果投影后的表达式为 false，那么原始表达式一定为 false（但反过来不一定）。这保证了剪枝的安全性。

#### 残差过滤器(Residual)

无法在元数据层完全处理的条件会生成 Residual 表达式，下推到文件读取层:

```java
// 源码: ManifestGroup.java
LoadingCache<Integer, ResidualEvaluator> residualCache =
    Caffeine.newBuilder().build(specId -> {
        PartitionSpec spec = specsById.get(specId);
        Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : dataFilter;
        return ResidualEvaluator.of(spec, filter, caseSensitive);
    });
```

#### 两阶段过滤

```
第一阶段: ManifestEvaluator --> 粗粒度分区剪枝(Manifest 级别)
  |
第二阶段: Evaluator + InclusiveMetricsEvaluator --> 精细化文件剪枝(DataFile 级别)
  |
残差阶段: ResidualEvaluator --> 文件内行级过滤(由引擎执行)
  |
最终: 最小化需要读取和处理的数据量
```

#### 30 秒速记（简版）

- 结论先行：Iceberg 的谓词下推通过元数据统计信息在查询规划阶段过滤文件。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/expressions/ManifestEvaluator.java`（Manifest 级剪枝），再联动 `api/src/main/java/org/apache/iceberg/expressions/InclusiveMetricsEvaluator.java`（文件级剪枝），最后 `api/src/main/java/org/apache/iceberg/expressions/ResidualEvaluator.java`（残差过滤），把"定义-实现-结果"串成一条线。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

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

- **按 ID**: `FOR VERSION AS OF 2` --> 直接查找 `snapshot-id=2`
- **按时间**: `FOR SYSTEM_TIME AS OF '2026-02-03 10:07:00'`

**时间定位的核心逻辑**:

```java
// 源码: core/src/main/java/org/apache/iceberg/util/SnapshotUtil.java
public static Long nullableSnapshotIdAsOfTime(Table table, long timestampMillis) {
    Long snapshotId = null;
    // 遍历 table.history()，找到 timestamp <= 目标时间 的最后一个快照
    for (HistoryEntry logEntry : table.history()) {
        if (logEntry.timestampMillis() <= timestampMillis) {
            snapshotId = logEntry.snapshotId();
        }
    }
    return snapshotId;
}

public static long snapshotIdAsOfTime(Table table, long timestampMillis) {
    Long snapshotId = nullableSnapshotIdAsOfTime(table, timestampMillis);
    Preconditions.checkArgument(snapshotId != null,
        "Cannot find a snapshot older than %s",
        DateTimeUtil.formatTimestampMillis(timestampMillis));
    return snapshotId;
}
```

**注意**: 时间旅行遍历的是 `table.history()`（即 `snapshot-log` 列表），而非 `snapshots` 列表。`history()` 记录了快照的提交顺序，`<=` 比较确保返回目标时间点或之前最近的快照。

**3. 从目标快照读取**

后续流程与普通查询完全一致:
```
目标快照 --> Manifest List --> Manifest File --> 数据文件
```

通过 `TableScan.useSnapshot(snapshotId)` 或 `BatchScan.useSnapshot(snapshotId)` API 指定快照。

#### 关键点

- **元数据层面**: 仅改变查询入口点(快照 ID)
- **依赖保留策略**: 时间旅行窗口取决于 `ExpireSnapshots` 配置
  - `history.expire.max-snapshot-age-ms`(默认 5 天)
  - `history.expire.min-snapshots-to-keep`(默认 1)
- **性能开销**: 与查询最新数据几乎无差异（只是入口快照不同）
- **数据文件保留**: 过期的快照被清理后，其引用的数据文件如果不被其他快照引用也会被删除

**示例**:
```sql
-- 查询昨天的数据
SELECT * FROM orders
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-02-03 00:00:00'
WHERE order_date = '2026-02-02';
```

#### 30 秒速记（简版）

- 结论先行：基于快照的不可变元数据历史实现。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/util/SnapshotUtil.java`（`snapshotIdAsOfTime` 方法遍历 `table.history()` 定位快照），再联动 `api/src/main/java/org/apache/iceberg/TableScan.java`（`useSnapshot(long)` 指定快照），后续流程与普通查询一致。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

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
2. **生成任务**: `FileScanTask` -> `SparkInputPartition`
3. **分发**: 一次性分发到 Executors

**数据读取**:
- Executor 创建 `RowDataReader` / `BaseBatchReader`
- 打开文件并应用 Merge-on-Read

**向量化读取**:
- 原生支持 `VectorizedSparkParquetReaders` 和 `ColumnarBatchReader`
- 数据以 `ColumnarBatch` 形式处理
- 也支持 ORC 向量化读取 (`VectorizedSparkOrcReaders`)
- 支持 Apache Comet 向量化 (`CometVectorizedReaderBuilder`)

**核心实现类**:
```
spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java
spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/BaseBatchReader.java
spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/data/vectorized/VectorizedSparkParquetReaders.java
spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/data/vectorized/ColumnarBatchReader.java
```

#### Flink 读取(FLIP-27 Source)

**API 集成**: `FLIP-27 Source API`

**组件**:
- `SplitEnumerator`: 运行在 JobManager
- `SourceReader`: 运行在 TaskManager

**任务规划(流式)**:
1. **JobManager**: `ContinuousIcebergEnumerator` 周期性扫描新快照
2. **发现增量**: 通过 `ContinuousSplitPlanner` 发现新数据文件
3. **动态分配**: 通过 `SplitAssigner` 按需分配 `IcebergSourceSplit` 给 SourceReader

**任务规划(批式)**:
1. **JobManager**: `StaticIcebergEnumerator` 一次性扫描所有文件
2. **分配**: 通过 `SplitAssigner` 分配 `IcebergSourceSplit`

**向量化读取**:
- **不支持** Parquet 向量化读取（Iceberg Flink 集成使用行式读取 `FlinkParquetReaders`）
- ORC 向量化读取通过 `FlinkOrcReader` 支持

**核心实现类**:
```
flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/enumerator/ContinuousIcebergEnumerator.java
flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/enumerator/StaticIcebergEnumerator.java
flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/split/IcebergSourceSplit.java
flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/RowDataFileScanTaskReader.java
flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/data/FlinkParquetReaders.java
```

#### 核心差异

| 维度 | Spark(批处理) | Flink(流式/批式) |
|------|---------------|-------------|
| **规划协调者** | Driver (`SparkBatchQueryScan`) | JobManager (`SplitEnumerator`) |
| **任务单元** | `SparkInputPartition`(静态) | `IcebergSourceSplit`(动态分配) |
| **任务分发** | 一次性分发 | 持续按需分发(流式) / 一次性分发(批式) |
| **核心场景** | 读取特定快照 | 持续消费新快照(流式) / 特定快照(批式) |
| **Parquet 向量化** | 支持 (`VectorizedSparkParquetReaders`) | **不支持**（使用行式 `FlinkParquetReaders`） |
| **ORC 向量化** | 支持 (`VectorizedSparkOrcReaders`) | 部分支持 (`FlinkOrcReader`) |
| **数据类型** | `ColumnarBatch` / `InternalRow` | `RowData` |

#### 共性

- Executor/TaskManager 层面的读取逻辑相似（打开文件，应用过滤，应用删除）
- 都应用 Merge-on-Read 处理删除文件
- 都使用 Iceberg 核心 API (`ManifestGroup`, `FileScanTask`) 进行查询规划

#### 面试加分话术

> "Spark 和 Flink 读取 Iceberg 的核心差异在于**任务调度模式**:
> 1. Spark 采用**静态规划**: Driver 一次性完成所有查询规划，适合大规模批处理
> 2. Flink 采用**动态规划**: `ContinuousIcebergEnumerator` 周期性发现新快照，`SplitAssigner` 按需分配 split，天然适合流式场景
>
> 性能层面的一个关键差异是 **Parquet 向量化读取**: Spark 通过 `VectorizedSparkParquetReaders` 以 `ColumnarBatch` 形式批量处理数据，而 Flink 当前使用行式 `FlinkParquetReaders`，这使得 Spark 在纯批处理场景下的 Parquet 读取性能通常优于 Flink。"

---

<a id="q-5-5"></a>
### 5.5 元数据列统计指标的存储层级与控制

**问题: Iceberg 的元数据文件会统计每个列的最大值和最小值吗？统计信息存储在哪一层？如何控制统计收集策略？** 🟡

**答案:**

Iceberg 采用**两层统计架构**，不同层级记录不同粒度的列统计信息：

#### 第一层：DataFile 级别（所有列）

每个 DataFile 的元数据中记录**所有列**的统计信息，定义在 `api/src/main/java/org/apache/iceberg/DataFile.java`：

```java
// DataFile.java 核心统计字段
Types.MapType COLUMN_SIZES       = /* field 108 */ Map<Integer, Long>    // 每列的字节大小
Types.MapType VALUE_COUNTS       = /* field 109 */ Map<Integer, Long>    // 每列的值数量
Types.MapType NULL_VALUE_COUNTS  = /* field 110 */ Map<Integer, Long>    // 每列的 null 值数量
Types.MapType NAN_VALUE_COUNTS   = /* field 137 */ Map<Integer, Long>    // 每列的 NaN 值数量
Types.MapType LOWER_BOUNDS       = /* field 125 */ Map<Integer, ByteBuffer> // 每列的最小值
Types.MapType UPPER_BOUNDS       = /* field 128 */ Map<Integer, ByteBuffer> // 每列的最大值
```

**关键点**: `LOWER_BOUNDS` 和 `UPPER_BOUNDS` 的 key 是 **column_id**（而非列名），value 是序列化后的边界值。这些统计信息被 `InclusiveMetricsEvaluator` 用于**文件级剪枝**。

#### 第二层：ManifestFile 级别（仅分区列）

ManifestFile 的 `partitions` 字段记录**仅分区列**的汇总统计，定义在 `api/src/main/java/org/apache/iceberg/ManifestFile.java`：

```java
// ManifestFile.java - PartitionFieldSummary
Types.StructType PARTITION_SUMMARY_TYPE = Types.StructType.of(
    required(509, "contains_null", BooleanType.get()),   // 是否包含 null 分区值
    optional(518, "contains_nan",  BooleanType.get()),   // 是否包含 NaN 分区值
    optional(510, "lower_bound",   BinaryType.get()),    // 分区字段最小值
    optional(511, "upper_bound",   BinaryType.get())     // 分区字段最大值
);
```

**关键点**: 这一层**仅针对分区字段**，不包含普通列。被 `ManifestEvaluator` 用于**Manifest 级剪枝**——在打开 Manifest 文件之前就跳过不匹配的 Manifest。

#### 两层统计架构对比

| 维度 | DataFile 级别 | ManifestFile 级别 |
|------|--------------|-------------------|
| **统计范围** | 所有列（受 metrics mode 控制） | 仅分区列 |
| **存储位置** | Manifest 文件内的 DataFile 条目 | Manifest 文件头部的 `partitions` 字段 |
| **统计内容** | lower_bounds, upper_bounds, null_counts, nan_counts, value_counts, column_sizes | lower_bound, upper_bound, contains_null, contains_nan |
| **用途** | `InclusiveMetricsEvaluator` 文件级剪枝 | `ManifestEvaluator` Manifest 级剪枝 |
| **剪枝层级** | 第二层（跳过不匹配的 DataFile） | 第一层（跳过整个 Manifest 文件） |

#### MetricsModes：统计收集的 4 种模式

Iceberg 通过 `MetricsModes`（`core/src/main/java/org/apache/iceberg/MetricsModes.java`）提供 4 种收集模式来控制 DataFile 级别的统计信息：

| 模式 | 说明 | 记录内容 |
|------|------|---------|
| `none` | 不收集任何统计信息 | 无 |
| `counts` | 仅收集计数指标 | value_counts, null_value_counts, nan_value_counts |
| `truncate(N)` | 收集计数 + 截断的边界值（**默认: truncate(16)**） | counts + lower_bounds/upper_bounds（截断到 N 字节） |
| `full` | 收集计数 + 完整边界值 | counts + lower_bounds/upper_bounds（完整值） |

#### 配置方式

通过 `TableProperties`（`core/src/main/java/org/apache/iceberg/TableProperties.java`）设置：

```sql
-- 全局默认模式（默认 truncate(16)）
ALTER TABLE t SET TBLPROPERTIES ('write.metadata.metrics.default' = 'truncate(32)');

-- 针对特定列的模式（覆盖全局默认）
ALTER TABLE t SET TBLPROPERTIES ('write.metadata.metrics.column.sensitive_col' = 'none');
ALTER TABLE t SET TBLPROPERTIES ('write.metadata.metrics.column.id' = 'full');

-- 最大自动推断列数（默认 100，超过此数的列不自动收集统计）
ALTER TABLE t SET TBLPROPERTIES ('write.metadata.metrics.max-inferred-column-defaults' = '200');
```

#### 多层剪枝的连接关系

```
查询: SELECT * FROM t WHERE date = '2024-01-01' AND amount > 1000

第1层: ManifestEvaluator（ManifestFile 的 partition summary）
  → date 分区字段的 lower_bound/upper_bound 不匹配 → 跳过整个 Manifest

第2层: InclusiveMetricsEvaluator（DataFile 的 column bounds）
  → amount 列的 UPPER_BOUNDS < 1000 → 跳过该 DataFile

第3层: Parquet/ORC RowGroup 级别的 min/max
  → 引擎利用文件内部统计信息进一步跳过 RowGroup

第4层: 残差过滤器（ResidualEvaluator）
  → 对剩余行逐行过滤
```

#### 30 秒速记

- **两层统计**: DataFile 级别记录**所有列**的 min/max（被 InclusiveMetricsEvaluator 使用），ManifestFile 级别**仅记录分区列**的 min/max（被 ManifestEvaluator 使用）
- **默认 truncate(16)**: 边界值截断到 16 字节，平衡存储开销与剪枝精度
- **4 种模式**: none → counts → truncate(N) → full，粒度递增
- **可逐列配置**: `write.metadata.metrics.column.<col>` 覆盖全局默认，敏感列可设为 `none`
- **面试表达**: "Iceberg 的列统计采用两层架构——ManifestFile 汇总分区列做粗粒度剪枝，DataFile 记录所有列做细粒度剪枝，默认用 truncate(16) 平衡存储与精度"

---

## 总结

本文档从面试官视角系统梳理了 Apache Iceberg 的核心知识点，涵盖:

1. **文件管理**: 小文件合并(BinPack/Sort/Z-Order)、垃圾回收(ExpireSnapshots/DeleteOrphanFiles)、Manifest 优化
2. **查询优化**: 多层剪枝(ManifestEvaluator + InclusiveMetricsEvaluator)、谓词下推、残差过滤器、时间旅行
3. **引擎对比**: Spark vs Flink 读取实现差异
4. **删除机制**: Position Delete 应用(Roaring Bitmap)、Deletion Vector(V3)

**面试建议**:
- 🟢 基础问题: 理解概念和基本原理
- 🟡 中级问题: 掌握实现机制和源码逻辑
- 🟠 高级问题: 理解架构设计和性能优化
- 🔴 专家问题: 深入边界场景和生产实践

**持续学习**: Iceberg 快速演进，建议关注官方文档和社区动态，及时更新知识体系。

#### 30 秒速记（简版）

- 结论先行：两者基于不同 API 实现，但核心逻辑相似。
- 主链路：先定位 `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java`（Spark 读取入口），再对比 `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/enumerator/ContinuousIcebergEnumerator.java`（Flink 流式读取入口），理解调度差异。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。
## 六、Spark 与 Flink 读取机制深度解析
<a id="q-6-1"></a>
### 6.1 FileScanTask 结构与设计

**问题: 请从源码角度说明 `FileScanTask` 如何封装 DataFile 与 DeleteFile 的关联关系？**

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

    /**
     * 返回此 FileScanTask 的 schema
     * 默认抛出 UnsupportedOperationException, 由子类覆写
     */
    default Schema schema() {
        throw new UnsupportedOperationException("Does not support schema getter");
    }

    @Override
    default long sizeBytes() {
        return length() + ScanTaskUtil.contentSizeInBytes(deletes());
    }

    @Override
    default int filesCount() {
        return 1 + deletes().size();  // 1个DataFile + N个DeleteFile
    }

    @Override
    default boolean isFileScanTask() {
        return true;
    }

    @Override
    default FileScanTask asFileScanTask() {
        return this;
    }
}
```

#### BaseFileScanTask 实现

```java
// 源码: core/src/main/java/org/apache/iceberg/BaseFileScanTask.java
public class BaseFileScanTask extends BaseContentScanTask<FileScanTask, DataFile>
    implements FileScanTask {
    private final DeleteFile[] deletes;  // DeleteFile数组(原始存储)
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

    // 注意: BaseFileScanTask 覆写了接口默认方法, 直接使用数组长度
    @Override
    public int filesCount() {
        return 1 + deletes.length;  // 直接用数组长度, 避免触发 ImmutableList 创建
    }

    // 同样覆写 sizeBytes, 使用内部的 deletesSizeBytes() 方法
    @Override
    public long sizeBytes() {
        return length() + deletesSizeBytes();
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
2. **懒加载优化**: `deleteList` 和 `deletesSizeBytes` 延迟初始化,优化内存和计算
3. **不可变性**: 使用 `ImmutableList` 确保线程安全,接口返回不可变视图
4. **大小计算**: 任务大小 = DataFile 大小 + 所有 DeleteFile 大小
5. **序列化优化**: `transient` 关键字避免序列化缓存字段
6. **覆写优化**: `BaseFileScanTask` 覆写了 `filesCount()` 和 `sizeBytes()`,直接使用数组操作避免触发 `ImmutableList` 的创建
7. **Split 支持**: 内部类 `SplitScanTask` 实现了 `MergeableScanTask`,支持 Split 合并,且复用父任务的 deletesSizeBytes 缓存

**深入理解**: 通过封装 DataFile 和 DeleteFile 列表,Iceberg 将 Merge-on-Read 的复杂性隐藏在 Task 内部,上层调度器只需要按 Task 分配即可,无需理解 Delete 逻辑。接口定义了通用的默认实现,而 `BaseFileScanTask` 通过覆写进一步优化了性能敏感路径。

#### 30 秒速记（简版）

- 结论先行：FileScanTask 是 Iceberg 读取机制的核心抽象,它将一个 DataFile 与其关联的 DeleteFile 列表封装为一个扫描任务单元。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/FileScanTask.java`，再联动 `core/src/main/java/org/apache/iceberg/BaseFileScanTask.java`，把"定义-实现-结果"串成一条线。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-6-2"></a>
### 6.2 DeleteFile 关联机制

**问题: Spark/Flink 在构建 `FileScanTask` 时,如何判断哪些 `DeleteFile` 需要关联到某个 `DataFile`？**

**答案（总）:**

不是"所有 Delete 都全局广播"。当前源码是**分层索引匹配**: 先按 delete 类型拆分,再按 `global / partition / path / dv` 四个维度命中候选集,最后再做 sequence 与统计过滤。

**答案（分）:**

1. `ManifestGroup` 先构建 `DeleteFileIndex`,任务生成时按 entry 调 `ctx.deletes().forEntry(entry)`,不是边扫边全量比对。
   - 源码: `core/src/main/java/org/apache/iceberg/ManifestGroup.java` 行 185 构建 `DeleteFileIndex`
   - 源码: `core/src/main/java/org/apache/iceberg/ManifestGroup.java` 行 366 调用 `ctx.deletes().forEntry(entry)` 获取关联的 DeleteFile 数组

2. **DeleteFileIndex 内部五个索引结构**（源码 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java`）：
   ```java
   private final EqualityDeletes globalDeletes;            // 全局 equality deletes (unpartitioned表)
   private final PartitionMap<EqualityDeletes> eqDeletesByPartition;  // 按分区索引的 equality deletes
   private final PartitionMap<PositionDeletes> posDeletesByPartition;  // 按分区索引的 position deletes
   private final Map<String, PositionDeletes> posDeletesByPath;        // 按文件路径索引的 position deletes
   private final Map<String, DeleteFile> dvByPath;          // Deletion Vector (DV), 按文件路径索引
   ```

3. Position delete 优先走 file-scoped 路径索引,否则落到分区索引。
   - 源码: `DeleteFileIndex.Builder#add(Map<String, PositionDeletes>, PartitionMap<PositionDeletes>, DeleteFile)` 行 519-535
   - 先调用 `ContentFileUtil.referencedDataFileLocation(file)` 判断是否 file-scoped
   - 若 `path != null`（file-scoped）,放入 `posDeletesByPath`
   - 若 `path == null`（非 file-scoped）,放入 `posDeletesByPartition`

4. DV（Deletion Vector）是特殊的 position delete,以 Puffin 格式存储,单独走 `dvByPath` 索引。
   - 判断条件: `ContentFileUtil.isDV(file)` 即 `file.format() == FileFormat.PUFFIN`
   - DV 必须通过 `referencedDataFile()` 关联到具体数据文件
   - 源码: `DeleteFileIndex.Builder#add(Map<String, DeleteFile>, DeleteFile)` 行 509-517

5. Equality delete 并非"总是全局": unpartitioned 表进入 `globalDeletes`,partitioned 表进入 `eqDeletesByPartition`。
   - 源码: `DeleteFileIndex.Builder#add(EqualityDeletes, PartitionMap<EqualityDeletes>, DeleteFile)` 行 537-553
   - 判断条件: `spec.isUnpartitioned()`

6. `referencedDataFile == null` 也不等于"必须读内容文件": 会先看 `file_path` 的 lower/upper bound 是否相等来推断 file-scoped。
   - 源码: `core/src/main/java/org/apache/iceberg/util/ContentFileUtil.java` 行 63-89
   - 流程: 先检查 `deleteFile.referencedDataFile()` 是否非空 -> 再检查 `lowerBounds` 和 `upperBounds` 中 PATH_ID 对应的值是否相等 -> 相等则推断为 file-scoped

7. **查找时的匹配逻辑** (`DeleteFileIndex#forDataFile(long, DataFile)` 行 148-165)：
   ```java
   DeleteFile[] forDataFile(long sequenceNumber, DataFile file) {
       DeleteFile[] global = findGlobalDeletes(sequenceNumber, file);
       DeleteFile[] eqPartition = findEqPartitionDeletes(sequenceNumber, file);
       DeleteFile dv = findDV(sequenceNumber, file);
       if (dv != null && global == null && eqPartition == null) {
           return new DeleteFile[] {dv};
       } else if (dv != null) {
           return concat(global, eqPartition, new DeleteFile[] {dv});
       } else {
           // 没有 DV 时才查找 position deletes
           DeleteFile[] posPartition = findPosPartitionDeletes(sequenceNumber, file);
           DeleteFile[] posPath = findPathDeletes(sequenceNumber, file);
           return concat(global, eqPartition, posPartition, posPath);
       }
   }
   ```
   关键设计: DV 和传统 position delete 互斥 -- 如果文件有 DV,就不再查找 posDeletesByPartition/posDeletesByPath。

**答案（总）:**

你在面试里可以一句话收口: **Iceberg 用"类型拆分 + 五维索引(global/partition-eq/partition-pos/path/dv) + 序列号过滤 + 统计裁剪"做 delete 关联,不是粗暴的全局 join。DV 和传统 position delete 互斥设计进一步优化了查找路径。**

**追问与反问:**
- 高频追问: "那 equality delete 到底什么时候是全局的？"
- 你的反问: "我们线上表主要是 unpartitioned 还是 partitioned？这会直接决定 equality delete 的匹配放大倍数。"

#### 30 秒速记（简版）

- 结论先行：不是"所有 Delete 都全局广播"。当前源码是**分层索引匹配**: 先按 delete 类型拆分,再按 global / partition / path / dv 五个维度命中候选集。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/ManifestGroup.java` 行 185 构建索引，再联动 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java` 行 148 查找匹配。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-6-3"></a>
### 6.3 Merge-on-Read 实现原理

**问题: 请详细说明 Spark 读取 Iceberg 时,DeleteFilter 如何实现 Merge-on-Read？**

**答案:**

Merge-on-Read 是 Iceberg 的核心特性,通过在读取时合并 DataFile 和 DeleteFile 来实现行级删除。

#### RowDataReader 读取流程

```java
// 源码: spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/RowDataReader.java
// RowDataReader 继承 BaseRowReader<FileScanTask>, 并实现 PartitionReader<InternalRow>
@Override
protected CloseableIterator<InternalRow> open(FileScanTask task) {
    String filePath = task.file().location();
    LOG.debug("Opening data file {}", filePath);

    // 1. 创建 SparkDeleteFilter (BaseReader 的内部类, 继承 DeleteFilter<InternalRow>)
    SparkDeleteFilter deleteFilter =
        new SparkDeleteFilter(filePath, task.deletes(), counter(), true);

    // 2. 计算需要读取的 Schema(可能包含额外的删除相关列)
    Schema requiredSchema = deleteFilter.requiredSchema();
    Map<Integer, ?> idToConstant = constantsMap(task, requiredSchema);

    // 3. 更新 Spark 当前文件信息(用于 filename() 函数)
    InputFileBlockHolder.set(filePath, task.start(), task.length());

    // 4. 读取 DataFile 并应用 Delete 过滤
    return deleteFilter.filter(open(task, requiredSchema, idToConstant)).iterator();
}
```

注意: `SparkDeleteFilter` 是 `BaseReader` 的**受保护内部类**（定义在 `BaseReader.java` 行 200），不是独立的顶层类。它继承 `DeleteFilter<InternalRow>`，并覆写了 `asStructLike`、`getInputFile`、`markRowDeleted` 和 `newDeleteLoader` 方法。

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
    |
    +-- 1. applyPosDeletes()  <-- 先应用 Position Delete
    |   +-- 使用 PositionDeleteIndex 快速判断行位置
    |
    +-- 2. applyEqDeletes()   <-- 再应用 Equality Delete
        +-- 使用 StructLikeSet 进行集合匹配
```

**为什么先 Position 后 Equality？**
- Position Delete 通常更精确（O(1) 位置查找,基于 Roaring Bitmap）
- Equality Delete 需要 Hash 匹配（O(1) 但常数更大,需要 StructLike 投影 + Hash 计算）
- 先过滤 Position Delete 可以减少 Equality 比较次数
- 此外 Position Delete 只需要行号,不需要从文件中读取额外数据列

**DeleteFilter 的 _deleted 列支持**:

`DeleteFilter` 还支持 `_deleted` 元数据列。当查询包含 `_deleted` 列时,不会真正过滤行,而是通过 `markRowDeleted` 标记行的删除状态:

```java
// 源码: data/src/main/java/org/apache/iceberg/data/DeleteFilter.java 行 260-265
private CloseableIterable<T> createDeleteIterable(
        CloseableIterable<T> records, Predicate<T> isDeleted) {
    return hasIsDeletedColumn
        ? Deletes.markDeleted(records, isDeleted, this::markRowDeleted)
        : Deletes.filterDeleted(records, isDeleted, counter);
}
```

**深入理解**: 两阶段过滤的效率权衡,本质是用确定性换性能:Position Delete 精确到行号(确定性高),Equality Delete 基于值匹配(确定性低),先过滤高确定性的能更快缩小数据集。

#### 面试加分话术

> "Merge-on-Read 的核心是**延迟物化删除**:
> 1. **写入时**: 只写 Delete File,不修改原数据文件,避免写放大
> 2. **读取时**: 通过 DeleteFilter 两阶段过滤合并数据
> 3. **过滤顺序**: Position -> Equality,从高确定性到低确定性
> 4. **_deleted 列**: 支持查询中显式引用删除状态,用于 CDC 等场景
>
> 这是 Iceberg 实现**高吞吐写入 + 正确性保证**的关键设计。
> 代价是读取时有额外开销,但通过 Compaction 可以定期物化删除,消除读放大。"

#### 高频追问

**Q: Merge-on-Read 的读放大如何量化？什么时候需要触发 Compaction？**

**A**: 读放大指标:
```
读放大系数 = (读取的数据量 + 读取的删除文件量) / 实际返回的数据量
```
触发 Compaction 的阈值:
1. **删除比例**: 当分区的删除行数 > 总行数的 10-20%
2. **Delete 文件数**: 当单个 DataFile 关联的 Delete 文件 > 3-5 个
3. **定期策略**: 每日/每周定时 Compaction

**监控方法**: 通过 `table.snapshots()` 和 `table.currentSnapshot().allManifests()` 等 API 监控 Delete 文件增长趋势;也可通过 `FileScanTask.deletes().size()` 在查询时统计关联的删除文件数量。

#### 30 秒速记（简版）

- 结论先行：Merge-on-Read 是 Iceberg 的核心特性,通过在读取时合并 DataFile 和 DeleteFile 来实现行级删除。
- 主链路：先定位 `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/RowDataReader.java` 行 86 的 `open()` 方法,再联动 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java` 行 177 的 `filter()` 方法。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-6-4"></a>
### 6.4 Position Delete 应用机制

**问题: Position Delete 如何实现 O(1) 的删除行判断？请结合源码说明。**

**答案:**

Position Delete 通过 `PositionDeleteIndex` 接口及其实现 `BitmapPositionDeleteIndex` 实现高效的行位置查找。

#### Position Delete 应用逻辑

```java
// 源码: data/src/main/java/org/apache/iceberg/data/DeleteFilter.java 行 250-258
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

// 行 242-248
public PositionDeleteIndex deletedRowPositions() {
    if (deleteRowPositions == null && !posDeletes.isEmpty()) {
        this.deleteRowPositions = deleteLoader().loadPositionDeletes(posDeletes, filePath);
    }
    return deleteRowPositions;
}
```

#### PositionDeleteIndex 接口与实现

**注意**: `PositionDeleteIndex` 是一个**接口**,不是类。核心方法定义在 `core/src/main/java/org/apache/iceberg/deletes/PositionDeleteIndex.java`:

```java
// 源码: core/src/main/java/org/apache/iceberg/deletes/PositionDeleteIndex.java
public interface PositionDeleteIndex {
    void delete(long position);
    void delete(long posStart, long posEnd);  // 范围删除
    boolean isDeleted(long position);         // O(1) 判断
    boolean isEmpty();
    void forEach(LongConsumer consumer);      // 遍历所有被删位置
    long cardinality();                       // 被删行总数
    ByteBuffer serialize();                   // 序列化支持
    // ...
}
```

实际实现是 `BitmapPositionDeleteIndex`（包级别可见,在 `core/src/main/java/org/apache/iceberg/deletes/BitmapPositionDeleteIndex.java`）:

```java
// 源码: core/src/main/java/org/apache/iceberg/deletes/BitmapPositionDeleteIndex.java
class BitmapPositionDeleteIndex implements PositionDeleteIndex {
    // 使用自定义的 RoaringPositionBitmap 存储删除位置
    private final RoaringPositionBitmap bitmap;
    private final List<DeleteFile> deleteFiles;  // 追踪来源的 DeleteFile

    @Override
    public void delete(long position) {
        bitmap.set(position);
    }

    @Override
    public boolean isDeleted(long position) {
        return bitmap.contains(position);  // O(1) 查找
    }

    @Override
    public long cardinality() {
        return bitmap.cardinality();
    }
}
```

#### RoaringPositionBitmap 内部结构

`RoaringPositionBitmap`（在 `core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java`）是 Iceberg 自定义的 64 位 Roaring Bitmap 实现:

```java
// 将 64-bit position 分为高32位 key 和低32位 position
// 内部使用 RoaringBitmap[] 数组,每个 key 对应一个 32-bit RoaringBitmap
class RoaringPositionBitmap {
    private RoaringBitmap[] bitmaps;  // org.roaringbitmap.RoaringBitmap

    boolean contains(long position) {
        int key = key(position);       // 高32位
        int low = low(position);       // 低32位
        if (key >= bitmaps.length || bitmaps[key] == null) {
            return false;
        }
        return bitmaps[key].contains(low);
    }
}
```

这比简单使用 `RoaringBitmap`（只支持32位）更强大,因为 Iceberg 的行位置是 `long` 类型。

#### _pos 元数据列

Iceberg 虚拟列 `_pos` 记录行在文件中的位置:

```java
// 源码: core/src/main/java/org/apache/iceberg/MetadataColumns.java
public static final NestedField ROW_POSITION =
    NestedField.required(
        Integer.MAX_VALUE - 2,    // fieldId = 2147483645
        "_pos",
        Types.LongType.get(),
        "Ordinal position of a row in the source data file"
    );
```

**生成时机**:
- Parquet/ORC/Avro 读取器在读取行时自动计算
- 从文件开头开始递增: 0, 1, 2, 3, ...
- 不存储在数据文件中,是纯虚拟列

#### 执行示例

```
假设 DataFile 有 1000 行,Position Delete 标记了位置 [42, 156, 789]

BitmapPositionDeleteIndex 内部:
  bitmap.set(42), bitmap.set(156), bitmap.set(789)

读取流程:
Row 0:  positionIndex.isDeleted(0)   -> false -> 保留
Row 42: positionIndex.isDeleted(42)  -> true  -> 过滤
Row 156: positionIndex.isDeleted(156) -> true  -> 过滤
Row 789: positionIndex.isDeleted(789) -> true  -> 过滤
...其余行保留
```

#### 性能优化点

1. **RoaringPositionBitmap**: 支持 64 位位置的 Roaring Bitmap,稀疏时内存占用极小（使用 run-length encoding）
2. **O(1) 查找**: Roaring Bitmap 的 contains 操作本质是数组索引 + 位运算
3. **批量加载**: 一次性加载所有 Position Delete 构建索引
4. **序列化支持**: 支持 serialize/deserialize,可用于 DV（Deletion Vector）的持久化存储
5. **合并支持**: `PositionDeleteIndexUtil.merge()` 可合并多个索引

#### 30 秒速记（简版）

- 结论先行：Position Delete 通过 `PositionDeleteIndex` 接口（`BitmapPositionDeleteIndex` 实现）实现 O(1) 的行位置查找,底层是 64 位 Roaring Bitmap。
- 主链路：先定位 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java` 行 250 的 `applyPosDeletes()`，再联动 `core/src/main/java/org/apache/iceberg/deletes/BitmapPositionDeleteIndex.java` 的 `isDeleted()`。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-6-5"></a>
### 6.5 Equality Delete 应用机制

**问题: Equality Delete 如何通过 StructLikeSet 实现高效的等值匹配？**

**答案:**

Equality Delete 通过将删除数据加载到内存 HashSet（`StructLikeSet`）,实现 O(1) 的等值查找。

#### Equality Delete 应用逻辑

```java
// 源码: data/src/main/java/org/apache/iceberg/data/DeleteFilter.java 行 181-213
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

        // 创建投影器: 从 requiredSchema 投影到 deleteSchema
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

**外层调用逻辑**（行 223-227）:

```java
private CloseableIterable<T> applyEqDeletes(CloseableIterable<T> records) {
    // 将所有 Predicate 用 OR 组合: 任一 deleteSet 命中即为删除
    Predicate<T> isEqDeleted = applyEqDeletes().stream()
        .reduce(Predicate::or)
        .orElse(t -> false);
    return createDeleteIterable(records, isEqDeleted);
}
```

#### 分组优化

假设有三个 Equality DeleteFile:

```
DeleteFile1: equalityFieldIds = [1, 5]  // id, email
DeleteFile2: equalityFieldIds = [1, 5]  // 同样的字段
DeleteFile3: equalityFieldIds = [3]     // user_name

分组后:
Group1: [DeleteFile1, DeleteFile2] -> 合并加载成一个 StructLikeSet
Group2: [DeleteFile3]              -> 独立的 StructLikeSet

最终生成两个 Predicate, 用 OR 组合
```

#### 执行示例

```
EqualityDelete 数据(id, email):
| id   | email              |
|------|--------------------|
| 1001 | alice@example.com  |
| 1002 | bob@example.com    |

读取 DataFile 时:
Row: {id: 1001, email: alice@example.com, name: "Alice"}
  -> projectRow 提取 (1001, alice@example.com)
  -> deleteSet.contains((1001, alice@example.com)) -> true
  -> 过滤

Row: {id: 1003, email: carol@example.com, name: "Carol"}
  -> projectRow 提取 (1003, carol@example.com)
  -> deleteSet.contains((1003, carol@example.com)) -> false
  -> 保留
```

#### 性能考量

1. **StructLikeSet 使用 HashSet**: O(1) 平均查找时间,基于 Iceberg 自定义的结构化 Hash
2. **内存开销**: 所有 Equality Delete 需要加载到内存,这是主要的内存压力来源
3. **分组优化**: 相同 equalityFieldIds 的 DeleteFile 共享一个 Set,减少内存和查找次数
4. **投影开销**: 每行需要做 StructProjection 包装,有一定 CPU 开销
5. **eqDeletedRowFilter()**: DeleteFilter 还提供了反向过滤器（取 negate 后用 AND 组合）,用于查找未被删除的行

#### 30 秒速记（简版）

- 结论先行：Equality Delete 通过将删除数据加载到内存 StructLikeSet（HashSet）,实现 O(1) 的等值查找。
- 主链路：先定位 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java` 行 181 的 `applyEqDeletes()`，关注分组逻辑和 StructProjection 投影。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-6-6"></a>
### 6.6 Schema 投影与 Delete 协同

**问题: 为什么应用 Equality Delete 时需要读取额外的列？请从源码角度说明 requiredSchema 的计算逻辑。**

**答案:**

Equality Delete 依赖特定列进行等值匹配,即使用户查询不包含这些列,也必须从 DataFile 读取。

#### requiredSchema 计算

```java
// 源码: data/src/main/java/org/apache/iceberg/data/DeleteFilter.java 行 267-318
private static Schema fileProjection(
    Schema tableSchema,
    Schema requestedSchema,  // 用户查询的列
    List<DeleteFile> posDeletes,
    List<DeleteFile> eqDeletes,
    boolean needRowPosCol) {

    // 0. 如果没有任何 delete 文件,直接返回用户请求的 schema
    if (posDeletes.isEmpty() && eqDeletes.isEmpty()) {
        return requestedSchema;
    }

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
    Set<Integer> missingIds = Sets.newLinkedHashSet(
        Sets.difference(requiredIds, TypeUtil.getProjectedIds(requestedSchema)));

    if (missingIds.isEmpty()) {
        return requestedSchema;  // 无需额外列
    }

    // 4. 添加缺失的列到 Schema
    List<Types.NestedField> columns = Lists.newArrayList(requestedSchema.columns());
    for (int fieldId : missingIds) {
        if (fieldId == MetadataColumns.ROW_POSITION.fieldId()
            || fieldId == MetadataColumns.IS_DELETED.fieldId()) {
            continue;  // _pos 和 _deleted 在最后添加
        }
        Types.NestedField field = tableSchema.asStruct().field(fieldId);
        Preconditions.checkArgument(field != null,
            "Cannot find required field for ID %s", fieldId);
        columns.add(field);
    }

    // 5. 添加 _pos 元数据列(如果需要)
    if (missingIds.contains(MetadataColumns.ROW_POSITION.fieldId())) {
        columns.add(MetadataColumns.ROW_POSITION);
    }

    // 6. 添加 _deleted 元数据列(如果需要)
    if (missingIds.contains(MetadataColumns.IS_DELETED.fieldId())) {
        columns.add(MetadataColumns.IS_DELETED);
    }

    return new Schema(columns);
}
```

注意与原文的区别: 源码中除了处理 `_pos` 列外,还会处理 `_deleted` 列（`IS_DELETED` 元数据列）。这是因为 `DeleteFilter` 支持通过 `_deleted` 列标记删除状态而非真正过滤行。

#### 示例场景

```sql
-- 用户查询
SELECT name, age FROM users;

-- 实际情况
Table Schema: (id, name, age, email, created_at)
Requested Schema: (name, age)
Position DeleteFile 存在: 需要 _pos 列
Equality DeleteFile: equalityFieldIds = [1, 4]  // id, email

-- 计算后的 requiredSchema
Required Schema: (name, age, id, email, _pos)  <-- 添加了 id、email 和 _pos
```

**Flink 侧的额外投影**:

在 Flink 的 `RowDataFileScanTaskReader` 中（`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/RowDataFileScanTaskReader.java` 行 94-101）,如果 `projectedSchema` 和 `requiredSchema` 不同,会在过滤完成后做一次额外投影,去除多余的列:

```java
// Flink 读取时: 先读 requiredSchema, 过滤后投影回 projectedSchema
if (!projectedSchema.sameSchema(deletes.requiredSchema())) {
    RowDataProjection rowDataProjection =
        RowDataProjection.create(
            deletes.requiredRowType(),
            deletes.requiredSchema().asStruct(),
            projectedSchema.asStruct());
    iterable = CloseableIterable.transform(iterable, rowDataProjection::wrap);
}
```

**为什么需要额外列？**
- 用户只查询 `name, age`
- 但 Equality Delete 依赖 `id, email` 进行匹配
- Position Delete 需要 `_pos` 来定位行
- 必须从 DataFile 读取这些列才能判断行是否被删除

**性能影响**:
- 增加读取的列数 -> 更多 IO（尤其是列式存储 Parquet,需要额外读取列 chunk）
- 这是 Merge-on-Read 的本质代价

**深入理解**: Equality Delete 强制读取额外列是 Merge-on-Read 的本质代价。这也解释了为什么频繁 Update 场景下,定期 Compaction 很重要 -- 将 Delete 合并回 DataFile 可以消除这种额外列读取开销。Flink 的额外投影步骤则确保了最终返回给用户的数据只包含请求的列。

#### 30 秒速记（简版）

- 结论先行：Equality Delete 依赖特定列进行等值匹配,即使用户查询不包含这些列,也必须从 DataFile 读取。
- 主链路：先定位 `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java` 行 267 的 `fileProjection()`,理解额外列的计算逻辑。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-6-7"></a>
### 6.7 Sequence Number 并发控制

**问题: `dataSequenceNumber` 和 `fileSequenceNumber` 有什么区别？Delete 的生效规则到底是不是统一 `<=`？**

**答案（总）:**

两类 sequence 要分开看: `fileSequenceNumber` 更偏"文件何时被加入表"（文件的提交序列号）,`dataSequenceNumber` 更偏"这批数据逻辑上属于哪个序列"（数据的逻辑序列号）。Delete 生效规则也**不是一条统一公式**,position 与 equality 在源码中是两套语义。

**答案（分）:**

1. **Position delete 使用 `DeleteFile::dataSequenceNumber` 排序与筛选。**
   - `PositionDeletes` 的排序器直接按 `DeleteFile::dataSequenceNumber` 排序（`DeleteFileIndex.java` 行 650-651）:
     ```java
     private static final Comparator<DeleteFile> SEQ_COMPARATOR =
         Comparator.comparingLong(DeleteFile::dataSequenceNumber);
     ```
   - `filter(seq)` 方法通过 `findStartIndex(seqs, seq)` 返回 `deleteSeq >= dataSeq` 的候选（行 665-681）:
     ```java
     public DeleteFile[] filter(long seq) {
         indexIfNeeded();
         int start = findStartIndex(seqs, seq);
         // start 位置开始的所有 delete files 都满足 deleteSeq >= seq
         // 即: position delete 的 dataSequenceNumber >= data file 的 dataSequenceNumber
     }
     ```
   - 含义: **同序列号的 position delete 可以作用于同序列号的数据文件**

2. **Equality delete 使用 `applySequenceNumber` 而不是 delete 的原始 `dataSequenceNumber`。**
   - `EqualityDeleteFile` 构造时计算（`DeleteFileIndex.java` 行 811-814）:
     ```java
     EqualityDeleteFile(PartitionSpec spec, DeleteFile file) {
         this.spec = spec;
         this.wrapped = file;
         this.applySequenceNumber = wrapped.dataSequenceNumber() - 1;
     }
     ```
   - `EqualityDeletes` 的排序器使用 `applySequenceNumber`（行 725-726）:
     ```java
     private static final Comparator<EqualityDeleteFile> SEQ_COMPARATOR =
         Comparator.comparingLong(EqualityDeleteFile::applySequenceNumber);
     ```
   - `filter(seq, dataFile)` 中的 `findStartIndex` 也基于 `applySequenceNumber`（行 741-744）:
     ```java
     int start = findStartIndex(seqs, seq);
     // seqs 中存储的是 applySequenceNumber = deleteDataSeq - 1
     ```

3. **这意味着 equality delete 的判定是"严格小于":**
   - 因为 `applySequenceNumber = deleteDataSeq - 1`
   - 过滤比较: `applySequenceNumber >= dataSeq`,即 `deleteDataSeq - 1 >= dataSeq`,等价于 `deleteDataSeq > dataSeq`
   - 含义: **同一事务中写入的 equality delete 不会作用于同一事务中写入的数据文件**

4. **Equality Delete 还有额外的统计裁剪**:
   - `filter()` 方法不仅做序列号过滤,还调用 `canContainEqDeletesForFile(dataFile, file)` 做统计信息匹配:
     - 检查 null 值计数
     - 检查等值字段的 lower/upper bounds 是否有重叠
   - 这进一步减少了不必要的 delete 关联

5. `fileSequenceNumber` 仍有价值,但不用于这里的 delete 行匹配主判定。它更多体现文件提交生命周期与快照演进语义（例如,用于判断一个 entry 是否属于某次快照提交）。

**为什么 equality delete 要故意减 1？**

这是 Iceberg 规范定义的语义: equality delete 表达的是"删除满足条件的行",如果同一事务中既写入了数据行又写入了对应的 equality delete,则该数据行不应被删除（因为它们属于同一批操作）。而 position delete 是精确指定某文件某行的删除,同一事务中可以先写数据再精确删除某些行,所以用 `>=`。

**答案（总）:**

面试收口句: **Position delete 是"同序列可见(>=)",equality delete 是"严格前序可见(>)",差别来自 `applySequenceNumber = dataSeq - 1` 的设计。这一区别源于两种 delete 语义的不同: position delete 精确指定行号（同事务感知）,equality delete 是条件匹配（同事务需要排除）。**

**追问与反问:**
- 高频追问: "为什么 equality 要故意减 1？"
- 你的反问: "我们是否验证过跨引擎（Spark/Flink）在同事务写删场景下的序列一致性？"

#### 30 秒速记（简版）

- 结论先行：两类 sequence 要分开看: fileSequenceNumber 更偏"文件何时被加入表",dataSequenceNumber 更偏"这批数据逻辑上属于哪个序列"。Position delete 用 `>=`,equality delete 用 `>`（通过 `-1` 实现）。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java` 行 650 的 `PositionDeletes`，再联动行 811 的 `EqualityDeleteFile` 构造。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-6-8"></a>
### 6.8 Flink Incremental Scan 实现

**问题: Flink 增量读取 Iceberg 的关键路径是什么？`IncrementalAppendScan` 接口在源码里到底长什么样？**

**答案（总）:**

`IncrementalAppendScan` 本体非常薄,只是继承 `IncrementalScan`。真正的行为定义在父接口和实现类里: 起止快照边界、只消费 APPEND 快照、以及流式启动策略（旧 API/FLIP-27）的差异。

**答案（分）:**

1. **`IncrementalAppendScan` 接口没有声明任何方法,只做 extends。**
   ```java
   // 源码: api/src/main/java/org/apache/iceberg/IncrementalAppendScan.java
   public interface IncrementalAppendScan
       extends IncrementalScan<IncrementalAppendScan, FileScanTask, CombinedScanTask> {}
   ```

2. **`fromSnapshotExclusive`、`fromSnapshotInclusive`、`toSnapshot` 等方法定义在父接口 `IncrementalScan`。**
   ```java
   // 源码: api/src/main/java/org/apache/iceberg/IncrementalScan.java
   public interface IncrementalScan<ThisT, T extends ScanTask, G extends ScanTaskGroup<T>>
       extends Scan<ThisT, T, G> {
       ThisT fromSnapshotInclusive(long fromSnapshotId);  // 起始快照(包含)
       ThisT fromSnapshotExclusive(long fromSnapshotId);  // 起始快照(不包含)
       ThisT toSnapshot(long toSnapshotId);               // 结束快照(包含)
       // 还有 ref 版本的重载和 useBranch 方法
   }
   ```

3. **append-only 语义在实现层体现**: `BaseIncrementalAppendScan#appendsBetween` 只保留 `DataOperations.APPEND`。
   ```java
   // 源码: core/src/main/java/org/apache/iceberg/BaseIncrementalAppendScan.java 行 104-116
   private static List<Snapshot> appendsBetween(
       Table table, Long fromSnapshotIdExclusive, long toSnapshotIdInclusive) {
       List<Snapshot> snapshots = Lists.newArrayList();
       for (Snapshot snapshot :
           SnapshotUtil.ancestorsBetween(
               toSnapshotIdInclusive, fromSnapshotIdExclusive, table::snapshot)) {
           if (snapshot.operation().equals(DataOperations.APPEND)) {
               snapshots.add(snapshot);
           }
       }
       return snapshots;
   }
   ```
   关键: 只收集 `DataOperations.APPEND` 操作的快照,忽略 OVERWRITE、REPLACE 等操作。

4. **Flink FLIP-27 的 `ContinuousSplitPlannerImpl` 启动策略**:
   - `TABLE_SCAN_THEN_INCREMENTAL`: 首次做一次全量快照扫描,后续增量
     ```java
     // 源码: flink/v1.20/.../ContinuousSplitPlannerImpl.java 行 170-182
     if (scanContext.streamingStartingStrategy()
         == StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL) {
         splits = FlinkSplitPlanner.planIcebergSourceSplits(
             table, scanContext.copyWithSnapshotId(startSnapshot.snapshotId()), workerPool);
         toPosition = IcebergEnumeratorPosition.of(
             startSnapshot.snapshotId(), startSnapshot.timestampMillis());
     }
     ```
   - `INCREMENTAL_FROM_LATEST_SNAPSHOT_EXCLUSIVE`: 从最新快照开始,跳过历史数据
   - 其他策略（`INCREMENTAL_FROM_LATEST_SNAPSHOT`、`INCREMENTAL_FROM_EARLIEST_SNAPSHOT`）: 起始快照包含消费,通过使用 parentId 实现 inclusive 行为

5. **增量模式下 Delete 文件的处理**:
   - Flink 文件读取阶段仍会构造 `FlinkDeleteFilter`（DeleteFilter 的 Flink 实现）
   - 源码: `flink/v1.20/.../RowDataFileScanTaskReader.java` 行 87-91:
     ```java
     FlinkDeleteFilter deletes =
         new FlinkDeleteFilter(task, tableSchema, projectedSchema, inputFilesDecryptor);
     CloseableIterable<RowData> iterable =
         deletes.filter(
             newIterable(task, deletes.requiredSchema(), idToConstant, inputFilesDecryptor));
     ```
   - **增量并不意味着"不处理 delete"**: 即使是增量读取,每个 FileScanTask 仍可能关联 DeleteFile

6. **`BaseIncrementalAppendScan` 的 ManifestGroup 构建**（行 68-97）:
   ```java
   private CloseableIterable<FileScanTask> appendFilesFromSnapshots(List<Snapshot> snapshots) {
       Set<Long> snapshotIds = Sets.newHashSet(...);
       Set<ManifestFile> manifests = ...; // 只取这些快照新增的 manifest

       ManifestGroup manifestGroup =
           new ManifestGroup(table().io(), manifests)
               .filterManifestEntries(
                   manifestEntry ->
                       snapshotIds.contains(manifestEntry.snapshotId())
                           && manifestEntry.status() == ManifestEntry.Status.ADDED)
               .ignoreDeleted();  // 忽略已删除的 entry
       return manifestGroup.planFiles();
   }
   ```
   注意 `.ignoreDeleted()`: 增量扫描只关注 ADDED 状态的 entry,忽略 DELETED 状态。但 `planFiles()` 内部仍会构建 `DeleteFileIndex` 来关联 delete 文件。

**答案（总）:**

可复述总结: **接口很薄,语义在父接口与实现;增量是快照边界语义,不是"天然不处理 delete"。BaseIncrementalAppendScan 只收集 APPEND 操作的快照,通过 ManifestGroup 的 filterManifestEntries 进一步筛选 ADDED 状态的文件。**

**追问与反问:**
- 高频追问: "为什么我设置了增量,首次还是读了很多历史文件？"
- 你的反问: "当前启动策略是 `TABLE_SCAN_THEN_INCREMENTAL` 还是 `INCREMENTAL_FROM_LATEST_SNAPSHOT_EXCLUSIVE`？前者首轮做全量扫描,后者跳过所有历史。"

#### 30 秒速记（简版）

- 结论先行：IncrementalAppendScan 本体非常薄,只是继承 IncrementalScan。真正的行为定义在父接口和实现类里: 起止快照边界、只消费 APPEND 快照。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/IncrementalAppendScan.java`，再联动 `core/src/main/java/org/apache/iceberg/BaseIncrementalAppendScan.java` 行 104 的 `appendsBetween()`。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-6-9"></a>
### 6.9 Spark DeleteFilter 优化

**问题: Spark 侧对 DeleteFilter 有哪些真实可用的优化？哪些常见说法在源码层面不准确？**

**答案（总）:**

最容易答错的两点:
1) delete 缓存开关不是 DataFrame read option;
2) 缓存策略也不能笼统说"纯 LRU"。
源码里是 session conf + Caffeine（访问过期 + 权重上限）。

**答案（分）:**

1. **读取开关入口在 `SparkReadConf#cacheDeleteFilesOnExecutors`。**
   - 源码: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkReadConf.java` 行 335-338
   ```java
   public boolean cacheDeleteFilesOnExecutors() {
       return executorCacheEnabled() && cacheDeleteFilesOnExecutorsInternal();
   }
   ```
   注意: 必须同时满足 **executor cache 全局开关** 和 **delete files 缓存子开关**。

2. **配置 key 是 Spark Session Conf**:
   - `spark.sql.iceberg.executor-cache.enabled` (默认 true)
   - `spark.sql.iceberg.executor-cache.delete-files.enabled` (默认 true)
   - `spark.sql.iceberg.executor-cache.timeout` (默认 10 分钟)
   - `spark.sql.iceberg.executor-cache.max-entry-size` (默认 64 MB)
   - `spark.sql.iceberg.executor-cache.max-total-size` (默认 128 MB)
   - 源码: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkSQLProperties.java` 行 79-96

3. **Executor 缓存实现基于 Caffeine**:
   ```java
   // 源码: SparkExecutorCache.java 行 162-169
   private Cache<String, CacheValue> initState() {
       return Caffeine.newBuilder()
           .expireAfterAccess(timeout)         // 访问后过期, 不是固定过期
           .maximumWeight(maxTotalSize)         // 按权重限制总大小
           .weigher((key, value) -> ((CacheValue) value).weight())  // 权重 = 数据大小
           .recordStats()                       // 记录命中率等统计
           .removalListener((key, value, cause) -> LOG.debug("Evicted {} ({})", key, cause))
           .build();
   }
   ```
   - **不是纯 LRU**: Caffeine 默认使用 Window TinyLFU 策略,比 LRU 有更好的命中率
   - **权重限制**: 不是按条目数限制,而是按数据总大小限制
   - **超大条目跳过**: 如果单个 delete 文件大小 > `maxEntrySize`(默认 64MB),直接跳过缓存
   ```java
   // SparkExecutorCache.java 行 107-111
   public <V> V getOrLoad(String group, String key, Supplier<V> valueSupplier, long valueSize) {
       if (valueSize > maxEntrySize) {
           return valueSupplier.get();  // 超大条目不缓存
       }
       // ...
   }
   ```

4. **SparkDeleteFilter 中的缓存实现**:
   ```java
   // BaseReader.java 行 230-234
   @Override
   protected DeleteLoader newDeleteLoader() {
       if (cacheDeleteFilesOnExecutors) {
           return new CachingDeleteLoader(this::loadInputFile);
       }
       return new BaseDeleteLoader(this::loadInputFile);
   }
   ```
   `CachingDeleteLoader` 继承 `BaseDeleteLoader`,覆写了 `canCache()` 和 `getOrLoad()` 方法,利用 `SparkExecutorCache` 的单例实例。

5. **SparkExecutorCache 是 JVM 级单例**（行 73-86）:
   - 使用 `volatile + synchronized` 双重检查锁实现
   - 一旦创建,配置不可更改（"the cache will respect the SQL configuration valid at the time of initialization"）
   - 支持按 group 失效: `invalidate(String group)`,用于清理不再需要的表数据

**答案（总）:**

面试一锤定音: **Spark delete 优化的主抓手是"会话级缓存策略（Caffeine Window TinyLFU） + 权重上限 + 超大条目跳过",不是 DataFrame 单次 read option。缓存是 JVM 级单例,跨 task 共享。**

**追问与反问:**
- 高频追问: "为什么开了缓存命中率依然低？"
- 你的反问: "delete 文件大小分布是否超过 `max-entry-size`（默认 64MB）？如果大量超限,命中率天然上不去。也要检查 `max-total-size`（默认 128MB）是否够用。"

#### 30 秒速记（简版）

- 结论先行：1) delete 缓存开关不是 DataFrame read option; 2) 缓存不是纯 LRU,是 Caffeine Window TinyLFU + 权重限制。
- 主链路：先定位 `spark/v3.5/.../SparkReadConf.java` 行 335,再联动 `spark/v3.5/.../SparkExecutorCache.java` 行 162 的缓存初始化。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---

## 七、Catalog 与元数据管理架构
<a id="q-7-1"></a>
### 7.1 Catalog 抽象架构与 SPI 机制

**问题: 请解释 Iceberg Catalog 的 SPI 设计模式,以及如何通过 CatalogUtil 实现 Catalog 的可插拔加载？**

**答案:**

Iceberg 通过 **SPI (Service Provider Interface)** 设计模式实现了 Catalog 的可插拔架构,使得 HiveCatalog、RESTCatalog、JdbcCatalog 等可以无缝替换。

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

    // 还有多个重载: createTable(id, schema, spec, properties),
    // createTable(id, schema, spec), createTable(id, schema)

    /**
     * 检查表是否存在(默认实现通过 loadTable + catch)
     */
    default boolean tableExists(TableIdentifier identifier) {
        try {
            loadTable(identifier);
            return true;
        } catch (NoSuchTableException e) {
            return false;
        }
    }

    /**
     * 删除表(默认删除数据和元数据文件)
     */
    default boolean dropTable(TableIdentifier identifier) {
        return dropTable(identifier, true);
    }

    /**
     * 删除表(purge 控制是否删除数据文件)
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
     * 注册已存在的表(根据 metadata 文件位置)
     */
    default Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
        throw new UnsupportedOperationException("Registering tables is not supported");
    }

    /**
     * 构建表(Builder 模式)
     */
    default TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
        throw new UnsupportedOperationException(
            this.getClass().getName() + " does not implement buildTable");
    }

    /**
     * SPI 初始化方法 - 延迟初始化钩子
     */
    default void initialize(String name, Map<String, String> properties) {}

    /**
     * TableBuilder 内部接口 - 支持创建/替换事务
     */
    interface TableBuilder {
        TableBuilder withPartitionSpec(PartitionSpec spec);
        TableBuilder withSortOrder(SortOrder sortOrder);
        TableBuilder withLocation(String location);
        TableBuilder withProperties(Map<String, String> properties);
        TableBuilder withProperty(String key, String value);
        Table create();
        Transaction createTransaction();
        Transaction replaceTransaction();
        Transaction createOrReplaceTransaction();
    }
}
```

#### SPI 加载机制

Iceberg 通过 `CatalogUtil` 实现 Catalog 的动态加载:

```java
// 源码: core/src/main/java/org/apache/iceberg/CatalogUtil.java
public class CatalogUtil {
    public static final String ICEBERG_CATALOG_TYPE = "type";

    // 支持的 Catalog 类型常量
    public static final String ICEBERG_CATALOG_TYPE_HIVE = "hive";
    public static final String ICEBERG_CATALOG_TYPE_HADOOP = "hadoop";
    public static final String ICEBERG_CATALOG_TYPE_REST = "rest";
    public static final String ICEBERG_CATALOG_TYPE_GLUE = "glue";
    public static final String ICEBERG_CATALOG_TYPE_NESSIE = "nessie";
    public static final String ICEBERG_CATALOG_TYPE_JDBC = "jdbc";
    public static final String ICEBERG_CATALOG_TYPE_BIGQUERY = "bigquery";  // 新增

    // 对应的实现类全路径
    public static final String ICEBERG_CATALOG_HIVE = "org.apache.iceberg.hive.HiveCatalog";
    public static final String ICEBERG_CATALOG_REST = "org.apache.iceberg.rest.RESTCatalog";
    public static final String ICEBERG_CATALOG_GLUE = "org.apache.iceberg.aws.glue.GlueCatalog";
    // ... 等等

    /**
     * 根据配置动态创建 Catalog 实例
     */
    public static Catalog buildIcebergCatalog(
            String name,
            Map<String, String> options,
            Object conf) {

        String catalogImpl = options.get(CatalogProperties.CATALOG_IMPL);

        if (catalogImpl == null) {
            // 方式1: 未指定 impl, 根据 type 属性选择内置实现(默认 hive)
            String catalogType = PropertyUtil.propertyAsString(
                options, ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_TYPE_HIVE);
            switch (catalogType.toLowerCase(Locale.ENGLISH)) {
                case ICEBERG_CATALOG_TYPE_HIVE:
                    catalogImpl = ICEBERG_CATALOG_HIVE;
                    break;
                case ICEBERG_CATALOG_TYPE_REST:
                    catalogImpl = ICEBERG_CATALOG_REST;
                    break;
                // ... 其他类型
                default:
                    throw new UnsupportedOperationException(
                        "Unknown catalog type: " + catalogType);
            }
        } else {
            // 方式2: 显式指定了 impl, 但不允许同时指定 type
            String catalogType = options.get(ICEBERG_CATALOG_TYPE);
            Preconditions.checkArgument(catalogType == null,
                "Cannot create catalog %s, both type and catalog-impl are set",
                name);
        }

        return loadCatalog(catalogImpl, name, options, conf);
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
        DynConstructors.Ctor<Catalog> ctor =
            DynConstructors.builder(Catalog.class)
                .impl(impl)
                .buildChecked();
        Catalog catalog = ctor.newInstance();

        // 2. 如果支持 Hadoop 配置,注入配置
        configureHadoopConf(catalog, hadoopConf);

        // 3. 调用 initialize 完成初始化
        catalog.initialize(catalogName, properties);

        return catalog;
    }
}
```

注意与原文的差异:
- `buildIcebergCatalog` 的逻辑是 **先检查 `catalogImpl` 是否为 null**（即先尝试 type 路径）,而不是先检查是否非空
- **不允许同时指定** `type` 和 `catalog-impl`,会抛出 `IllegalArgumentException`
- `configureHadoopConf` 是通过私有方法处理的,使用的是 `org.apache.iceberg.hadoop.Configurable` 接口（不是 Hadoop 自己的 `Configurable`）
- 新增了 `bigquery` catalog 类型支持

#### SPI 设计模式分析

```
+-------------------------------------------------------------+
|                      Catalog SPI 架构                        |
+-------------------------------------------------------------+
|                                                              |
|  +-------------+    loadCatalog()    +------------------+    |
|  | CatalogUtil | -----------------> |  Catalog 接口     |    |
|  | (工厂类)     |                    |  (SPI 规范)       |    |
|  +-------------+                    +--------+---------+    |
|                                              |               |
|                    +-------------------------+----------+    |
|                    |              |           |          |    |
|            +-------v------+ +----v-----+ +---v------+   |    |
|            | HiveCatalog  | |RESTCatalog| |JdbcCatalog|  |    |
|            | (HMS 实现)   | |(HTTP实现) | |(JDBC实现) |  |    |
|            +--------------+ +----------+ +----------+   |    |
|                    |                          |          |    |
|            +-------v------+          +-------v------+   |    |
|            |HadoopCatalog | |GlueCatalog| |BigQueryCatalog|  |
|            |(HDFS 实现)   | |(AWS实现)  | |(GCP实现)      |  |
|            +--------------+ +----------+ +--------------+   |
+-------------------------------------------------------------+
```

**设计模式应用**:

| 模式 | 应用点 | 优势 |
|------|--------|------|
| **工厂模式** | `CatalogUtil.buildIcebergCatalog()` | 封装创建逻辑,统一入口 |
| **策略模式** | 根据 `type` 选择实现 | 运行时可切换策略 |
| **模板方法** | `initialize()` 钩子 | 子类可定制初始化逻辑 |
| **Builder 模式** | `Catalog.TableBuilder` | 链式调用,流畅 API |
| **双检锁单例** | `SparkExecutorCache` | JVM 级缓存共享 |

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

**深入理解**: SPI 模式是 Iceberg 可扩展性的基石。通过定义清晰的 Catalog 接口契约,任何满足接口的实现都可以无缝集成。`initialize()` 方法作为延迟初始化钩子,使得计算引擎可以先反射创建 Catalog 实例,再通过配置完成初始化。`type` 和 `catalog-impl` 互斥的设计避免了配置歧义。

#### 面试加分话术

> "Iceberg Catalog 采用 SPI 设计,将表操作抽象为统一接口。核心亮点:
> 1. `initialize()` 方法作为延迟初始化钩子,完美适配 Spark/Flink 的配置加载流程
> 2. `type` 和 `catalog-impl` 互斥设计,避免配置歧义
> 3. `registerTable()` 支持从 metadata 文件注册已有表,实现跨 catalog 迁移
> 4. `TableBuilder` 内部接口支持创建/替换事务,保证原子性"

#### 30 秒速记（简版）

- 结论先行：Iceberg 通过 SPI 设计模式实现了 Catalog 的可插拔架构,支持 Hive/REST/JDBC/Glue/Nessie/BigQuery 等多种后端。
- 主链路：先定位 `api/src/main/java/org/apache/iceberg/catalog/Catalog.java`，再联动 `core/src/main/java/org/apache/iceberg/CatalogUtil.java` 行 301 的 `buildIcebergCatalog()`。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-7-2"></a>
### 7.2 TableOperations 的 commit 原子性保证

**问题: TableOperations.commit() 是 Iceberg 事务的核心方法。请详细说明其原子性保证机制,以及 CommitStateUnknownException 的处理策略。**

**答案:**

`TableOperations` 是 Iceberg 元数据操作的 **SPI 核心接口**,其 `commit()` 方法实现了乐观并发控制(OCC)的原子提交。

#### TableOperations 接口定义

```java
// 源码: core/src/main/java/org/apache/iceberg/TableOperations.java
/** SPI interface to abstract table metadata access and updates. */
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
     * 关键约束(源码注释原文):
     * 1. Implementations must check that the base metadata is current to avoid
     *    overwriting updates.
     * 2. Once the atomic commit operation succeeds, implementations must not
     *    perform any operations that may fail.
     * 3. Implementations must throw a CommitStateUnknownException in cases
     *    where it cannot be determined if the commit succeeded or failed.
     */
    void commit(TableMetadata base, TableMetadata metadata);

    /** 返回 FileIO 用于读写数据/元数据文件 */
    FileIO io();

    /** 返回 EncryptionManager (默认 PlaintextEncryptionManager) */
    default EncryptionManager encryption() {
        return PlaintextEncryptionManager.instance();
    }

    /** 给定元数据文件名,获取完整路径 */
    String metadataFileLocation(String fileName);

    /** 返回数据文件 LocationProvider */
    LocationProvider locationProvider();

    /** 返回临时的 TableOperations (用于事务中未提交的元数据) */
    default TableOperations temp(TableMetadata uncommittedMetadata) {
        return this;
    }

    /** 创建新的 Snapshot ID */
    default long newSnapshotId() {
        return SnapshotIdGeneratorUtil.generateSnapshotID();
    }

    /**
     * 是否仅在 CleanableFailure 时才清理
     * 默认 true: 只有明确可清理的失败才会触发清理
     */
    default boolean requireStrictCleanup() {
        return true;
    }
}
```

注意: `TableOperations` 定义在 `core` 模块（不在 `api` 模块），这意味着它是给 Catalog 实现者使用的 SPI,不是给终端用户使用的 API。

#### HiveTableOperations commit 实现

HiveCatalog 的 commit 实现展示了完整的原子性保证机制:

```java
// 源码: hive-metastore/src/main/java/org/apache/iceberg/hive/HiveTableOperations.java
// 注意: 方法名是 doCommit (继承自 BaseMetastoreTableOperations)
@Override
protected void doCommit(TableMetadata base, TableMetadata metadata) {
    boolean newTable = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(newTable, metadata);

    CommitStatus commitStatus = CommitStatus.FAILURE;
    boolean updateHiveTable = false;

    // 1. 获取分布式锁
    HiveLock lock = lockObject(base != null ? base : metadata);
    try {
        lock.lock();  // 悲观锁保护

        Table tbl = loadHmsTable();

        // 新表检查: 如果 metadata location 已存在, 说明并发创建
        if (tbl != null && newTable
            && tbl.getParameters().get(METADATA_LOCATION_PROP) != null) {
            throw new AlreadyExistsException("Table already exists: %s.%s",
                database, tableName);
        }

        // 2. 检查并发冲突 (OCC 核心)
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
            newMetadataLocation, tbl, metadata, removedProps,
            hiveEngineEnabled, maxHiveTablePropertySize, currentMetadataLocation());

        // 4. 确保锁仍然有效
        lock.ensureActive();

        try {
            // 5. 持久化到 HMS
            // 注意: hiveLockEnabled 时不传 baseMetadataLocation
            persistTable(tbl, updateHiveTable,
                hiveLockEnabled(base, conf) ? null : baseMetadataLocation);
            lock.ensureActive();

            commitStatus = CommitStatus.SUCCESS;

        } catch (LockException le) {
            // 锁心跳失败,状态未知!
            commitStatus = CommitStatus.UNKNOWN;
            throw new CommitStateUnknownException(
                "Failed to heartbeat for hive lock while committing changes. " +
                "This can lead to a concurrent commit attempt be able to " +
                "overwrite this commit. Please check the commit history.", le);

        } catch (CommitFailedException | CommitStateUnknownException e) {
            throw e;  // 直接传播这两种已知异常

        } catch (Throwable e) {
            commitStatus = CommitStatus.UNKNOWN;

            if (e.getMessage() != null
                && e.getMessage().contains("The table has been modified")) {
                // HMS 返回并发修改错误,可能是网络重试导致
                // 严格模式检测: 因为没有其他 pending 请求可以成功
                commitStatus = checkCommitStatusStrict(
                    newMetadataLocation, metadata);
                if (commitStatus == CommitStatus.FAILURE) {
                    throw new CommitFailedException(e,
                        "The table %s.%s has been modified concurrently",
                        database, tableName);
                }
            } else {
                // 其他错误,普通模式检测
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
        HiveOperationsBase.cleanupMetadataAndUnlock(
            io(), commitStatus, newMetadataLocation, lock);
    }
}
```

关键实现差异说明:
- 方法名是 `doCommit` 不是 `commit` -- `BaseMetastoreTableOperations.commit()` 调用 `doCommit()` 并处理重试逻辑
- `CommitStatus` 来自 `BaseMetastoreOperations`（不是本类定义的枚举）
- 有两种状态检测模式: `checkCommitStatusStrict` 和 `checkCommitStatus`,前者用于已确认无 pending 请求的场景
- `persistTable` 的第三个参数是 `expectedMetadataLocation`,当启用 hive lock 时传 null（因为锁已保证互斥）

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

    // 两个构造函数
    public CommitStateUnknownException(Throwable cause) {
        super(cause.getMessage() + "\n" + COMMON_INFO, cause);
    }

    public CommitStateUnknownException(String message, Throwable cause) {
        super(message + "\n" + cause.getMessage() + "\n" + COMMON_INFO, cause);
    }
}
```

**为什么需要 CommitStateUnknownException？**

```
场景: 网络分区
+---------+     commit()     +---------+     alterTable()    +---------+
| Client  | --------------> |  Proxy  | ------------------> |  HMS    |
|         |                  |         |                     |         |
|         | <-- 网络超时 ---  |         | <--- SUCCESS -----  |         |
+---------+                  +---------+                     +---------+

Client 无法判断:
- 提交成功: 表已更新,数据文件有效
- 提交失败: 表未更新,数据文件需清理

错误处理:
- 如果假设失败并清理 -> 可能删除已提交的数据(数据丢失!)
- 如果假设成功并重试 -> 可能产生重复数据
- 正确做法: 抛出 CommitStateUnknownException,由上层决定
```

#### 提交状态检测机制

状态检测在 `BaseMetastoreTableOperations` 中实现（HiveTableOperations 继承此类）:

```
            +---------------------------------------------+
            |            Commit 状态机                     |
            +---------------------------------------------+
            |                                             |
            |  +-----------+                              |
            |  |  PENDING  | <--- 初始状态                |
            |  +-----+-----+                              |
            |        | commit()                           |
            |        v                                    |
            |  +-----------+    成功    +-----------+     |
            |  | COMMITTING| -------> |  SUCCESS  |     |
            |  +-----+-----+          +-----------+     |
            |        |                                   |
            |        | 异常                              |
            |        v                                   |
            |  +-----------+   可检测   +-----------+     |
            |  | CHECKING  | -------> |  FAILURE  |     |
            |  +-----+-----+          +-----------+     |
            |        |                                   |
            |        | 无法确定                          |
            |        v                                   |
            |  +-----------+                              |
            |  |  UNKNOWN  | --- 抛 CommitStateUnknown    |
            |  +-----------+                              |
            +---------------------------------------------+
```

检测方式: 刷新表元数据,比对当前 metadata file location 是否等于我们写入的 newMetadataLocation。

#### 不同 Catalog 的原子性保证对比

| Catalog | 原子性机制 | 锁机制 | 状态检测 |
|---------|-----------|--------|---------|
| **HiveCatalog** | HMS alterTable + metadata location 比对 | HiveLock（基于 HMS HIVE_LOCKS 表） | metadata location 比对,支持 strict 模式 |
| **HadoopCatalog** | 文件系统 rename（`version-hint.text`） | 无锁（依赖 FS 原子 rename） | 文件存在性检查 |
| **RESTCatalog** | 服务端 UpdateRequirements 验证 | 服务端处理（客户端无锁） | 快照追加场景可轻量级协调 |
| **GlueCatalog** | AWS API 乐观锁（version 字段） | 无客户端锁 | version 字段比对 |
| **JdbcCatalog** | 数据库事务 + metadata location 比对 | 行级锁（SELECT FOR UPDATE） | metadata location 比对 |

**深入理解**: `CommitStateUnknownException` 体现了 Iceberg 对数据一致性的极致追求。宁可让操作处于"未知"状态需要人工介入,也不冒险做出可能导致数据丢失或重复的决定。HiveTableOperations 额外区分了 `checkCommitStatusStrict` 和 `checkCommitStatus` 两种检测模式,前者在确定无 pending 请求时使用,可更准确地判断状态。

#### 面试加分话术

> "TableOperations.commit() 采用 OCC 乐观并发控制,核心是 base metadata location 比对。关键设计亮点:
> 1. CommitStateUnknownException -- 网络分区等场景下不做假设,显式抛出让上层处理
> 2. HiveTableOperations 的 `doCommit` 中有两种状态检测: strict 模式（无 pending 请求时）和普通模式
> 3. `requireStrictCleanup()` 默认 true,只在 CleanableFailure 时才清理元数据文件
> 4. 提交后不允许执行可能失败的操作（源码注释明确要求）"

#### 高频追问

**Q: 如果收到 CommitStateUnknownException,应该如何处理？**

**A**:
1. **不要立即重试**: 重试可能导致数据重复
2. **检查表状态**: 手动查询表的 snapshot 历史,确认是否提交成功
3. **孤儿文件清理**: 如果确认失败,使用 `removeOrphanFiles` Action 清理
4. **告警通知**: 在生产环境应触发告警,由 DBA 介入

#### 30 秒速记（简版）

- 结论先行：TableOperations 是 Iceberg 元数据操作的 SPI 核心接口,其 `commit()` 方法实现了乐观并发控制(OCC)的原子提交。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/TableOperations.java` 的接口定义，再联动 `hive-metastore/src/main/java/org/apache/iceberg/hive/HiveTableOperations.java` 行 237 的 `doCommit()`。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。

---
<a id="q-7-3"></a>
### 7.3 REST Catalog 与传统 Catalog 的对比

**问题: REST Catalog 相比 HiveCatalog 有哪些架构优势？请从 commit 实现角度分析差异。**

**答案:**

REST Catalog 是 Iceberg 社区推荐的**下一代 Catalog 架构**,通过 HTTP API 实现了更好的解耦、扩展性和安全性。

#### 架构对比

```
+------------------------------------------------------------------------+
|                      传统 Catalog 架构 (HiveCatalog)                    |
+------------------------------------------------------------------------+
|                                                                         |
|  +---------+   Thrift    +---------+   JDBC    +-------------+          |
|  | Spark   | ----------> | HMS     | --------> |  MySQL       |         |
|  +---------+             +---------+           +-------------+          |
|                               ^                                         |
|  +---------+   Thrift         |                                         |
|  | Flink   | -----------------+                                         |
|  +---------+                                                            |
|                                                                         |
|  问题:                                                                  |
|  1. 需要每个客户端配置 HMS 连接                                         |
|  2. 元数据 JSON 文件由客户端写入存储(S3/HDFS)                           |
|  3. 锁管理分散在各客户端                                                |
|  4. 难以实现统一的权限控制                                              |
+------------------------------------------------------------------------+

+------------------------------------------------------------------------+
|                        REST Catalog 架构                                |
+------------------------------------------------------------------------+
|                                                                         |
|  +---------+   HTTP/REST  +-------------+      +-------------+          |
|  | Spark   | -----------> | REST Server | ---> |  Backend    |          |
|  +---------+              |  (集中式)    |      |  (任意存储)  |         |
|                           +-------------+      +-------------+          |
|  +---------+   HTTP/REST       ^                                        |
|  | Flink   | ------------------+                                        |
|  +---------+                                                            |
|                                                                         |
|  优势:                                                                  |
|  1. 客户端仅需配置 REST endpoint                                        |
|  2. 元数据由服务端管理,客户端无需直接访问存储                            |
|  3. 服务端统一处理锁和并发                                              |
|  4. 可实现细粒度权限控制(行级/列级)                                     |
+------------------------------------------------------------------------+
```

#### RESTTableOperations commit 实现

```java
// 源码: core/src/main/java/org/apache/iceberg/rest/RESTTableOperations.java
// RESTTableOperations 直接实现 TableOperations 接口(不继承 BaseMetastoreTableOperations)
@Override
public void commit(TableMetadata base, TableMetadata metadata) {
    Endpoint.check(endpoints, Endpoint.V1_UPDATE_TABLE);

    Consumer<ErrorResponse> errorHandler;
    List<UpdateRequirement> requirements;
    List<MetadataUpdate> updates;

    switch (updateType) {
        case CREATE:
            Preconditions.checkState(base == null,
                "Invalid base metadata for create transaction, expected null: %s", base);
            updates = ImmutableList.<MetadataUpdate>builder()
                .addAll(createChanges)
                .addAll(metadata.changes())
                .build();
            requirements = UpdateRequirements.forCreateTable(updates);
            errorHandler = ErrorHandlers.tableErrorHandler();
            break;

        case REPLACE:
            Preconditions.checkState(base != null, "Invalid base metadata: null");
            updates = ImmutableList.<MetadataUpdate>builder()
                .addAll(createChanges)
                .addAll(metadata.changes())
                .build();
            // 使用原始 replaceBase 计算 requirements
            requirements = UpdateRequirements.forReplaceTable(replaceBase, updates);
            errorHandler = ErrorHandlers.tableCommitHandler();
            break;

        case SIMPLE:
            Preconditions.checkState(base != null, "Invalid base metadata: null");
            updates = metadata.changes();
            requirements = UpdateRequirements.forUpdateTable(base, updates);
            errorHandler = ErrorHandlers.tableCommitHandler();
            break;

        default:
            throw new UnsupportedOperationException(...);
    }

    // 构建请求: requirements + updates
    UpdateTableRequest request = new UpdateTableRequest(requirements, updates);

    LoadTableResponse response;
    try {
        // 发送 POST 请求到服务端
        response = client.post(
            path, request, LoadTableResponse.class, mutationHeaders, errorHandler);
    } catch (CommitStateUnknownException e) {
        // 尝试轻量级协调(仅限快照追加场景)
        if (updateType == UpdateType.SIMPLE && reconcileOnSimpleUpdate(updates, e)) {
            return;  // 协调成功,提交实际上成功了
        }
        throw e;
    }

    // 后续都是 SIMPLE 提交
    this.updateType = UpdateType.SIMPLE;

    updateCurrentMetadata(response);
}
```

关键差异: RESTTableOperations **直接实现 `TableOperations`** 接口（不像 HiveTableOperations 那样继承 `BaseMetastoreTableOperations`）,没有客户端锁,没有 metadata file 写入,所有并发控制由服务端处理。

#### REST Catalog 轻量级协调机制

REST Catalog 独有的优化 -- 对于 **仅追加快照** 的场景,可以尝试协调:

```java
// 源码: RESTTableOperations.java 行 230-244
private boolean reconcileOnSimpleUpdate(
        List<MetadataUpdate> updates,
        CommitStateUnknownException original) {

    // 1. 检查是否为"仅追加快照到main"更新
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
        original.addSuppressed(reconEx);  // 将协调失败作为 suppressed 异常
        return false;
    }
}
```

`expectedSnapshotIdIfSnapshotAddOnly` 的安全检查逻辑（行 251-286）:

```java
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
            return null;  // 其他类型更新(schema变更等),不安全
        }
    }

    if (addedSnapshotId == null) {
        return null;
    }

    // 确保不是 rollback 场景: main 指向的必须是刚添加的快照
    if (mainRefSnapshotId != null && !addedSnapshotId.equals(mainRefSnapshotId)) {
        return null;
    }

    return addedSnapshotId;
}
```

**协调机制的安全性分析**:

| 更新类型 | 可协调 | 原因 |
|---------|--------|------|
| 追加单个快照到 main | 可以 | 快照 ID 唯一,存在即成功 |
| 多个快照追加 | 不可以 | 部分成功状态无法判断 |
| 非 main 分支操作 | 不可以 | 分支指针可能被覆盖 |
| Schema 变更 | 不可以 | 无法通过快照判断 |
| 属性变更 | 不可以 | 无法通过快照判断 |
| Rollback（main 指向非新增快照） | 不可以 | 快照存在不代表 main 指向它 |

#### UpdateRequirements 机制

REST Catalog 使用 `UpdateRequirements` 在服务端验证前置条件:

```java
// 源码: core/src/main/java/org/apache/iceberg/UpdateRequirements.java
public class UpdateRequirements {

    public static List<UpdateRequirement> forUpdateTable(
            TableMetadata base,
            List<MetadataUpdate> metadataUpdates) {

        Builder builder = new Builder(base, false);

        // 1. 要求 UUID 匹配 (不是 AssertTableExists!)
        builder.require(new UpdateRequirement.AssertTableUUID(base.uuid()));

        // 2. 根据更新类型添加额外要求
        for (MetadataUpdate update : metadataUpdates) {
            builder.update(update);
        }

        return builder.build();
    }

    // forCreateTable 使用 AssertTableDoesNotExist
    public static List<UpdateRequirement> forCreateTable(List<MetadataUpdate> metadataUpdates) {
        Builder builder = new Builder(null, false);
        builder.require(new UpdateRequirement.AssertTableDoesNotExist());
        metadataUpdates.forEach(builder::update);
        return builder.build();
    }

    // forReplaceTable 使用 AssertTableUUID
    public static List<UpdateRequirement> forReplaceTable(
            TableMetadata base, List<MetadataUpdate> metadataUpdates) {
        Builder builder = new Builder(base, true);
        builder.require(new UpdateRequirement.AssertTableUUID(base.uuid()));
        metadataUpdates.forEach(builder::update);
        return builder.build();
    }
}
```

Builder 内部根据更新类型自动添加 requirement:
- **SetSnapshotRef**: 添加 `AssertRefSnapshotID(name, baseRef.snapshotId())`
- **AddSchema**: 添加 `AssertLastAssignedFieldId(base.lastColumnId())`
- **SetCurrentSchema**: 添加 `AssertCurrentSchemaID(base.currentSchemaId())`
- **AddPartitionSpec**: 添加 `AssertLastAssignedPartitionId(base.lastAssignedPartitionId())`
- **SetDefaultPartitionSpec**: 添加 `AssertDefaultSpecID(base.defaultSpecId())`
- **SetDefaultSortOrder**: 添加 `AssertDefaultSortOrderID(base.defaultSortOrderId())`

注意: 原文中使用了 `AssertTableExists`,这是不准确的。实际 `forUpdateTable` 使用的是 `AssertTableUUID` -- 它不仅检查表是否存在,还验证 UUID 是否匹配,防止表被删除后重建导致的混淆。

**服务端验证流程**:

```
1. Client 发送: UpdateTableRequest {
     requirements: [AssertTableUUID("xxx"), AssertRefSnapshotID("main", 12345), ...],
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
| **锁机制** | HiveLock（基于 HIVE_LOCKS 表） | 服务端实现（客户端无锁） |
| **并发控制** | metadata location 比对 | UpdateRequirements 服务端验证 |
| **状态协调** | 无（只能检测） | 快照追加场景可轻量级协调 |
| **权限控制** | HMS 级别 | 可细粒度控制 |
| **多租户** | 困难 | 原生支持 |
| **版本兼容** | 需匹配 HMS 版本 | API 版本化（Endpoint 枚举） |
| **父类继承** | BaseMetastoreTableOperations | 直接实现 TableOperations |
| **错误处理** | 自定义异常映射 | ErrorHandlers 统一处理 |

**深入理解**: REST Catalog 代表了 Iceberg 的架构演进方向。通过将元数据操作集中到服务端:
1. 客户端不需要写 metadata 文件 -- 减少了客户端对存储系统的直接访问
2. 并发控制完全服务端化 -- UpdateRequirements 提供了声明式的前置条件验证
3. 轻量级协调机制是 REST Catalog 独有的 -- HiveCatalog 的 `doCommit` 只能检测状态,不能自动恢复
4. Netflix、Apple 等大厂已在生产环境大规模使用 REST Catalog

#### 面试加分话术

> "REST Catalog 的核心优势是**关注点分离**: 客户端只需通过 HTTP 调用 REST API,无需关心底层存储细节。三个关键差异:
> 1. **并发控制**: HiveCatalog 用 metadata location 比对（客户端检测）,REST Catalog 用 UpdateRequirements（服务端验证,更安全）
> 2. **状态恢复**: REST Catalog 独有轻量级协调,在快照追加的 CommitStateUnknown 场景下可自动恢复
> 3. **客户端简化**: REST 客户端不写 metadata 文件、不管理锁、不直接访问数据存储,完美实现了'瘦客户端'架构"

#### 30 秒速记（简版）

- 结论先行：REST Catalog 是 Iceberg 社区推荐的下一代 Catalog 架构,通过 HTTP API 实现了更好的解耦、扩展性和安全性。
- 主链路：先定位 `core/src/main/java/org/apache/iceberg/rest/RESTTableOperations.java` 行 157 的 `commit()`，再联动 `core/src/main/java/org/apache/iceberg/UpdateRequirements.java` 行 50 的 `forUpdateTable()`。
- 面试表达：先回答"是什么"，再补"为什么这样设计"，最后给"边界条件/取舍"。
## 八、SnapshotProducer 提交核心流程
<a id="q-8-1"></a>
### 8.1 SnapshotProducer 模板方法模式

**[红] 问题: SnapshotProducer 是 Iceberg 所有写操作的基类。请详细说明其模板方法模式设计,以及 commit() 方法的重试机制。**

**答案:**

`SnapshotProducer` 是 Iceberg 快照生产的**核心抽象类**,采用**模板方法模式**定义了快照创建的骨架流程,子类只需实现特定的抽象方法。

#### 类继承体系

```
SnapshotProducer<ThisT> (抽象基类)
    |
    +-- MergingSnapshotProducer<ThisT> (合并快照生产者)
    |       |
    |       +-- MergeAppend (合并追加文件)
    |       +-- BaseOverwriteFiles (覆盖文件)
    |       +-- BaseReplacePartitions (替换分区)
    |       +-- StreamingDelete (流式删除文件)
    |       +-- BaseRowDelta (行级变更)
    |       +-- BaseRewriteFiles (重写文件)
    |       +-- CherryPickOperation (快照挑选)
    |
    +-- FastAppend (快速追加,直接继承 SnapshotProducer)
    +-- BaseRewriteManifests (重写 Manifest)
```

#### 模板方法核心结构

```java
// 源码: core/src/main/java/org/apache/iceberg/SnapshotProducer.java
abstract class SnapshotProducer<ThisT> implements SnapshotUpdate<ThisT> {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotProducer.class);

    // Caffeine 缓存: 用于缓存 Manifest 元数据
    private final LoadingCache<ManifestFile, ManifestFile> manifestsWithMetadata;

    private final TableOperations ops;
    private final boolean strictCleanup;                         // 是否严格清理
    private final boolean canInheritSnapshotId;                  // 是否可继承快照 ID
    private final String commitUUID = UUID.randomUUID().toString();
    private final AtomicInteger manifestCount = new AtomicInteger(0); // Manifest 计数器
    private final AtomicInteger attempt = new AtomicInteger(0);  // 重试次数计数器
    private final List<String> manifestLists = Lists.newArrayList();
    private final long targetManifestSizeBytes;

    private volatile Long snapshotId = null;
    private TableMetadata base;
    private boolean stageOnly = false;                           // 是否仅暂存
    private String targetBranch = SnapshotRef.MAIN_BRANCH;       // 目标分支

    protected SnapshotProducer(TableOperations ops) {
        this.ops = ops;
        this.strictCleanup = ops.requireStrictCleanup();
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
            LOG.warn(
                "Failed to load committed table metadata or during cleanup, "
                + "skipping further cleanup", e);
        }
    }

    // 6. 通知事件监听器(外层 try-catch 保护,避免影响主流程)
    try {
        notifyListeners();
    } catch (Throwable e) {
        LOG.warn("Failed to notify event listeners", e);
    }
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
| `commit.retry.max-wait-ms` | 60000 (1分钟) | 最大重试等待时间(ms) |
| `commit.retry.total-timeout-ms` | 1800000 (30分钟) | 总重试超时时间 |

**指数退避示例**(含 10% jitter):
```
公式: sleepTime = min(100 * 2^(attempt-1), 60000) + random(0, delay*0.1)
重试1: ~110ms  (delay=100, jitter=[0,10))
重试2: ~220ms  (delay=200, jitter=[0,20))
重试3: ~440ms  (delay=400, jitter=[0,40))
重试4: ~880ms  (delay=800, jitter=[0,80))
... 直到达到 max-wait-ms(60s 上限)或 total-timeout-ms(30 分钟)
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

> "SnapshotProducer 采用经典的模板方法模式: `commit()` 定义骨架流程,`apply()`、`operation()`、`summary()` 由子类实现。核心亮点是 Caffeine LoadingCache 用于缓存 Manifest 元数据 -- 在 OCC 重试场景下,避免重复读取已处理的 Manifest 文件,同时 Tasks 框架提供了指数退避的重试机制,确保在高并发冲突时能够优雅恢复。"

#### 30 秒速记

- 结论先行: SnapshotProducer 是 Iceberg 快照生产的**核心抽象类**,采用**模板方法模式**定义了快照创建的骨架流程,子类只需实现特定的抽象方法。
- 主链路: `commit()` -> `apply()` -> 子类 `apply(base, parentSnapshot)` -> 写 ManifestList -> OCC 提交
- 面试表达: 先回答"是什么",再补"为什么这样设计",最后给"边界条件/取舍"。

---
<a id="q-8-2"></a>
### 8.2 Manifest 自动合并与分裂机制

**[橙] 问题: Iceberg 如何自动管理 Manifest 文件数量？请从源码角度说明 ManifestMergeManager 的合并策略。**

**答案:**

Iceberg 通过 `ManifestMergeManager` 实现 Manifest 文件的**自动合并与分裂**,在查询性能和写入效率之间取得平衡。

#### 为什么需要 Manifest 合并？

```
场景: 流式写入产生大量小 Manifest

每个 Checkpoint 产生一个 Manifest:
Checkpoint 1 -> manifest-001.avro (100 文件)
Checkpoint 2 -> manifest-002.avro (100 文件)
Checkpoint 3 -> manifest-003.avro (100 文件)
...
Checkpoint 1000 -> manifest-1000.avro (100 文件)

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

+-------------------------------------------------------------+
|  原状态         条件                      新状态             |
+-------------------------------------------------------------+
|  ADDED         snapshotId == 当前快照      ADDED            |
|  ADDED         snapshotId != 当前快照      EXISTING         |
|  EXISTING      -                          EXISTING         |
|  DELETED       snapshotId == 当前快照      DELETED          |
|  DELETED       snapshotId != 当前快照      (抑制,不写入)     |
+-------------------------------------------------------------+

原因:
- 旧快照的 DELETED 已经在该快照的元数据中记录,无需重复
- 旧快照的 ADDED 对当前快照来说是已存在的文件
- 只有当前快照的 ADDED/DELETED 需要保持原状态
```

#### 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `commit.manifest.target-size-bytes` | 8MB (8388608) | 目标 Manifest 大小 |
| `commit.manifest.min-count-to-merge` | 100 | 最小合并数量 |
| `commit.manifest-merge.enabled` | true | 是否启用自动合并 |

#### 合并示例

```
输入 Manifests:
+------------------+---------+----------------+
| Manifest         | Size    | PartitionSpec  |
+------------------+---------+----------------+
| manifest-001.avro| 1MB     | spec-0         |
| manifest-002.avro| 2MB     | spec-0         |
| manifest-003.avro| 3MB     | spec-0         |
| manifest-004.avro| 1MB     | spec-0         |
| manifest-005.avro| 2MB     | spec-0         |
+------------------+---------+----------------+

BinPacking (targetSize=8MB):
Bin 1: [manifest-005, manifest-004, manifest-003] -> 6MB (不足8MB,继续装)
       [manifest-005, manifest-004, manifest-003, manifest-002] -> 8MB
Bin 2: [manifest-001] -> 1MB (剩余单个)

输出 Manifests:
+----------------------+---------+
| Manifest             | Size    |
+----------------------+---------+
| merged-001.avro      | 8MB     | <-- 合并了 4 个
| manifest-001.avro    | 1MB     | <-- 保持原样
+----------------------+---------+
```

**深入理解**: ManifestMergeManager 的设计体现了"空间换时间"的思想。通过缓存已合并的 Manifest,在 OCC 重试场景下避免重复合并。BinPacking 算法确保合并后的 Manifest 接近目标大小,既不会产生过多小文件,也不会生成难以处理的超大文件。

#### 面试加分话术

> "ManifestMergeManager 使用 BinPacking 装箱算法,将多个小 Manifest 合并成接近 target-size-bytes 的大 Manifest。关键设计是 `mergedManifests` 缓存 -- 在 OCC 重试时复用已合并的结果,避免重复 IO。另外,状态转换规则确保旧快照的 DELETED 条目被抑制,避免元数据无限膨胀。"

#### 30 秒速记

- 结论先行: Iceberg 通过 ManifestMergeManager 实现 Manifest 文件的**自动合并与分裂**,在查询性能和写入效率之间取得平衡。
- 关键设计: BinPacking 装箱 + `mergedManifests` 缓存 + Entry 状态转换
- 面试表达: 先回答"是什么",再补"为什么这样设计",最后给"边界条件/取舍"。

---
<a id="q-8-3"></a>
### 8.3 Snapshot Summary 累加器设计

**[黄] 问题: Iceberg 快照的 summary 字段包含丰富的统计信息。请说明 SnapshotSummary 的累加器设计及其在增量计算中的应用。**

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

    // 删除文件统计 (总计)
    public static final String ADDED_DELETE_FILES_PROP = "added-delete-files";
    public static final String REMOVED_DELETE_FILES_PROP = "removed-delete-files";
    public static final String TOTAL_DELETE_FILES_PROP = "total-delete-files";

    // [源码校验修正] 删除文件细分统计 (区分 EQ/POS/DV)
    public static final String ADD_EQ_DELETE_FILES_PROP = "added-equality-delete-files";
    public static final String REMOVED_EQ_DELETE_FILES_PROP = "removed-equality-delete-files";
    public static final String ADD_POS_DELETE_FILES_PROP = "added-position-delete-files";
    public static final String REMOVED_POS_DELETE_FILES_PROP = "removed-position-delete-files";
    public static final String ADDED_DVS_PROP = "added-dvs";
    public static final String REMOVED_DVS_PROP = "removed-dvs";

    // 记录数统计
    public static final String ADDED_RECORDS_PROP = "added-records";
    public static final String DELETED_RECORDS_PROP = "deleted-records";
    public static final String TOTAL_RECORDS_PROP = "total-records";

    // 文件大小统计
    public static final String ADDED_FILE_SIZE_PROP = "added-files-size";
    public static final String REMOVED_FILE_SIZE_PROP = "removed-files-size";
    public static final String TOTAL_FILE_SIZE_PROP = "total-files-size";

    // Position Delete 记录数统计
    public static final String ADDED_POS_DELETES_PROP = "added-position-deletes";
    public static final String REMOVED_POS_DELETES_PROP = "removed-position-deletes";
    public static final String TOTAL_POS_DELETES_PROP = "total-position-deletes";

    // Equality Delete 记录数统计
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
    private boolean trustPartitionMetrics = true;  // [修正] 源码中有此标志位

    public void addedFile(PartitionSpec spec, DataFile file) {
        metrics.addedFile(file);
        updatePartitions(spec, file, true);
    }

    public void addedFile(PartitionSpec spec, DeleteFile file) {
        metrics.addedFile(file);
        updatePartitions(spec, file, true);
    }

    public void deletedFile(PartitionSpec spec, DataFile file) {
        metrics.removedFile(file);
        updatePartitions(spec, file, false);
    }

    public void deletedFile(PartitionSpec spec, DeleteFile file) {
        metrics.removedFile(file);
        updatePartitions(spec, file, false);
    }

    // [源码校验补充] addedManifest 方法 -- 当通过 Manifest 级别添加时
    // 无法区分细粒度删除文件类型,因此 trustSizeAndDeleteCounts 置为 false
    public void addedManifest(ManifestFile manifest) {
        this.trustPartitionMetrics = false;
        partitionMetrics.clear();
        metrics.addedManifest(manifest);
    }
}
```

#### UpdateMetrics 内部累加器

**[源码校验修正]** 实际源码中 `UpdateMetrics` 采用统一的 `addedFile(ContentFile<?>)` 方法通过 `switch` 分发,而非分别接受 `DataFile` 和 `DeleteFile` 参数。同时新增了 DV 追踪、`trustSizeAndDeleteCounts` 标志位,以及使用 `ScanTaskUtil.contentSizeInBytes(file)` 而非 `file.fileSizeInBytes()` 计算大小。

```java
// 源码: SnapshotSummary.java (UpdateMetrics 内部类)
// [修正] 完整字段列表,与源码一致
private static class UpdateMetrics {
    private long addedSize = 0L;
    private long removedSize = 0L;
    private int addedFiles = 0;
    private int removedFiles = 0;
    private int addedEqDeleteFiles = 0;      // [新增] EQ Delete 文件数
    private int removedEqDeleteFiles = 0;    // [新增] EQ Delete 移除数
    private int addedPosDeleteFiles = 0;     // [新增] POS Delete 文件数
    private int removedPosDeleteFiles = 0;   // [新增] POS Delete 移除数
    private int addedDVs = 0;                // [新增] DV 数
    private int removedDVs = 0;              // [新增] DV 移除数
    private int addedDeleteFiles = 0;        // 总删除文件数
    private int removedDeleteFiles = 0;
    private long addedRecords = 0L;
    private long deletedRecords = 0L;
    private long addedPosDeletes = 0L;
    private long removedPosDeletes = 0L;
    private long addedEqDeletes = 0L;
    private long removedEqDeletes = 0L;
    private boolean trustSizeAndDeleteCounts = true;  // [新增] 信任标志

    // [修正] 统一入口方法,通过 switch 分发
    void addedFile(ContentFile<?> file) {
        // [修正] 使用 ScanTaskUtil.contentSizeInBytes(file) 而非 file.fileSizeInBytes()
        this.addedSize += ScanTaskUtil.contentSizeInBytes(file);

        switch (file.content()) {
            case DATA:
                this.addedFiles += 1;
                this.addedRecords += file.recordCount();
                break;

            case POSITION_DELETES:
                DeleteFile deleteFile = (DeleteFile) file;
                // [修正] 区分 DV 和传统 Position Delete 文件
                if (ContentFileUtil.isDV(deleteFile)) {
                    this.addedDVs += 1;
                } else {
                    this.addedPosDeleteFiles += 1;
                }
                this.addedDeleteFiles += 1;
                this.addedPosDeletes += file.recordCount();
                break;

            case EQUALITY_DELETES:
                this.addedDeleteFiles += 1;
                this.addedEqDeleteFiles += 1;
                this.addedEqDeletes += file.recordCount();
                break;

            default:
                throw new UnsupportedOperationException(
                    "Unsupported file content type: " + file.content());
        }
    }

    // [修正] 移除文件同样使用统一 switch 分发
    void removedFile(ContentFile<?> file) {
        this.removedSize += ScanTaskUtil.contentSizeInBytes(file);

        switch (file.content()) {
            case DATA:
                this.removedFiles += 1;
                this.deletedRecords += file.recordCount();
                break;

            case POSITION_DELETES:
                DeleteFile deleteFile = (DeleteFile) file;
                if (ContentFileUtil.isDV(deleteFile)) {
                    this.removedDVs += 1;
                } else {
                    this.removedPosDeleteFiles += 1;
                }
                this.removedDeleteFiles += 1;
                this.removedPosDeletes += file.recordCount();
                break;

            case EQUALITY_DELETES:
                this.removedDeleteFiles += 1;
                this.removedEqDeleteFiles += 1;
                this.removedEqDeletes += file.recordCount();
                break;
        }
    }

    // [新增] addedManifest -- Manifest 级别的粗粒度统计
    void addedManifest(ManifestFile manifest) {
        switch (manifest.content()) {
            case DATA:
                this.addedFiles += manifest.addedFilesCount();
                this.addedRecords += manifest.addedRowsCount();
                this.removedFiles += manifest.deletedFilesCount();
                this.deletedRecords += manifest.deletedRowsCount();
                break;
            case DELETES:
                this.addedDeleteFiles += manifest.addedFilesCount();
                this.removedDeleteFiles += manifest.deletedFilesCount();
                // [关键] 无法区分 EQ/POS/DV,因此标记为不可信
                this.trustSizeAndDeleteCounts = false;
                break;
        }
    }

    // addTo 方法: 仅在 trustSizeAndDeleteCounts 为 true 时输出 size 和 delete 计数
    void addTo(ImmutableMap.Builder<String, String> builder) {
        setIf(addedFiles > 0, builder, ADDED_FILES_PROP, addedFiles);
        setIf(removedFiles > 0, builder, DELETED_FILES_PROP, removedFiles);
        setIf(addedEqDeleteFiles > 0, builder, ADD_EQ_DELETE_FILES_PROP, addedEqDeleteFiles);
        setIf(removedEqDeleteFiles > 0, builder, REMOVED_EQ_DELETE_FILES_PROP, removedEqDeleteFiles);
        setIf(addedPosDeleteFiles > 0, builder, ADD_POS_DELETE_FILES_PROP, addedPosDeleteFiles);
        setIf(removedPosDeleteFiles > 0, builder, REMOVED_POS_DELETE_FILES_PROP, removedPosDeleteFiles);
        setIf(addedDeleteFiles > 0, builder, ADDED_DELETE_FILES_PROP, addedDeleteFiles);
        setIf(removedDeleteFiles > 0, builder, REMOVED_DELETE_FILES_PROP, removedDeleteFiles);
        setIf(addedDVs > 0, builder, ADDED_DVS_PROP, addedDVs);
        setIf(removedDVs > 0, builder, REMOVED_DVS_PROP, removedDVs);
        setIf(addedRecords > 0, builder, ADDED_RECORDS_PROP, addedRecords);
        setIf(deletedRecords > 0, builder, DELETED_RECORDS_PROP, deletedRecords);

        // [关键] 只有在可信时才输出 size 和 delete 计数
        if (trustSizeAndDeleteCounts) {
            setIf(addedSize > 0, builder, ADDED_FILE_SIZE_PROP, addedSize);
            setIf(removedSize > 0, builder, REMOVED_FILE_SIZE_PROP, removedSize);
            setIf(addedPosDeletes > 0, builder, ADDED_POS_DELETES_PROP, addedPosDeletes);
            setIf(removedPosDeletes > 0, builder, REMOVED_POS_DELETES_PROP, removedPosDeletes);
            setIf(addedEqDeletes > 0, builder, ADDED_EQ_DELETES_PROP, addedEqDeletes);
            setIf(removedEqDeletes > 0, builder, REMOVED_EQ_DELETES_PROP, removedEqDeletes);
        }
    }
}
```

#### Total 值增量计算

**[源码校验修正]** `updateTotal` 方法在实际源码中包含 `try-catch NumberFormatException` 保护,并且在**每一步累加/减少**时都检查 `newTotal >= 0`,而非仅在最终检查。

```java
// 源码: SnapshotProducer.java (第 840-870 行)
private static void updateTotal(
        ImmutableMap.Builder<String, String> summaryBuilder,
        Map<String, String> previousSummary,
        String totalProperty,
        Map<String, String> currentSummary,
        String addedProperty,
        String deletedProperty) {

    String totalStr = previousSummary.get(totalProperty);
    if (totalStr != null) {
        try {
            long newTotal = Long.parseLong(totalStr);

            String addedStr = currentSummary.get(addedProperty);
            // [修正] 每步都检查 newTotal >= 0,不仅是最后
            if (newTotal >= 0 && addedStr != null) {
                newTotal += Long.parseLong(addedStr);  // total += added
            }

            String deletedStr = currentSummary.get(deletedProperty);
            // [修正] 减法前也检查 newTotal >= 0
            if (newTotal >= 0 && deletedStr != null) {
                newTotal -= Long.parseLong(deletedStr);  // total -= deleted
            }

            if (newTotal >= 0) {
                summaryBuilder.put(totalProperty, String.valueOf(newTotal));
            }

        } catch (NumberFormatException e) {
            // [修正] 源码中有 try-catch, 解析失败时静默跳过
            // ignore and do not add total
        }
    }
}
```

**关键设计点**:
1. `newTotal >= 0` 在每一步累加/减少**之前**检查,一旦变负即停止后续计算
2. `try-catch NumberFormatException` 防止损坏的 summary 值导致整个提交失败
3. 如果 `previousSummary` 中不存在 `totalProperty`,则完全跳过 total 计算

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
// [修正] 实际代码使用 trustPartitionMetrics 标志控制
private void updatePartitions(PartitionSpec spec, ContentFile<?> file, boolean isAddition) {
    if (trustPartitionMetrics) {
        UpdateMetrics partMetrics =
            partitionMetrics.computeIfAbsent(
                spec.partitionToPath(file.partition()), key -> new UpdateMetrics());

        if (isAddition) {
            partMetrics.addedFile(file);  // 统一调用 addedFile(ContentFile<?>)
        } else {
            partMetrics.removedFile(file);
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
| **可信度控制** | 无 | `trustSizeAndDeleteCounts` 标志 |

**深入理解**: SnapshotSummary 的累加器模式是 Iceberg 高性能的关键设计之一。通过维护 `added` 和 `deleted` 的增量值,结合前一个快照的 `total` 值,可以在 O(1) 时间内计算出新快照的全局统计。`trustSizeAndDeleteCounts` 标志确保在通过 `addedManifest` 粗粒度添加时不会输出不准确的 delete 类型细分统计。

#### 面试加分话术

> "SnapshotSummary 采用累加器模式实现 O(1) 时间复杂度的统计更新: `new_total = old_total + added - deleted`。每个快照只记录增量变化,通过链式累加得到全局统计。值得注意的是,源码中使用了 `trustSizeAndDeleteCounts` 标志 -- 当通过 Manifest 级别添加删除文件时(如 FastAppend),无法区分 EQ/POS/DV 类型,此时该标志置为 false,避免输出不准确的细分统计。updateTotal 方法还有 try-catch NumberFormatException 保护和每步 >= 0 检查,确保损坏的统计值不会传播。"

#### 高频追问

**Q: 如果 Summary 统计值与实际不一致怎么办？**

**A**:
1. **Compaction 可修复**: `rewriteDataFiles` 会重新计算统计
2. **ExpireSnapshots**: 可清理历史数据,重置统计基线
3. **手动修复**: 可通过 `RewriteTableMetadata` 修复元数据
4. **trustSizeAndDeleteCounts 机制**: 源码已内置保护,不可信时不输出可能不准确的统计

#### 30 秒速记

- 结论先行: SnapshotSummary 采用**累加器模式**,通过增量计算维护表的全局统计信息,避免每次提交都需要全表扫描。
- 关键细节: `UpdateMetrics` 统一 `switch` 分发、DV 独立追踪、`trustSizeAndDeleteCounts` 标志、`ScanTaskUtil.contentSizeInBytes()`
- 面试表达: 先回答"是什么",再补"为什么这样设计",最后给"边界条件/取舍"。

---

## 九、DeleteFileIndex 与删除文件匹配优化
<a id="q-9-1"></a>
### 9.1 DeleteFileIndex 多维索引结构

**[红] 问题: DeleteFileIndex 是 Iceberg 读取时关联删除文件的核心组件。请详细说明其多维索引结构及查找算法。**

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
                          |
    +---------------------+---------------------+
    |                     |                     |
    v                     v                     v
+----------------+  +----------------+  +----------------+
| EqualityDeletes|  |PositionDeletes |  | DeletionVector |
+-------+--------+  +-------+--------+  +-------+--------+
        |                    |                    |
        +--globalDeletes     +--byPartition       +--byPath (V3+)
        |  (非分区表)         |  (分区过滤)            (直接映射)
        |                    |
        +--byPartition       +--byPath
           (分区过滤)           (精确匹配)

索引查找优先级:
1. DV (V3) > Position Delete (V2) > Equality Delete
2. 精确路径匹配 > 分区匹配 > 全局匹配
```

#### forDataFile() 查找算法

**[源码校验修正]** 实际源码中,当找到 DV 时会**跳过** posDeletesByPartition 和 posDeletesByPath 的查找,因为 DV 已经完整覆盖了该数据文件的位置删除信息。

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

    // [修正] 仅在没有 DV 时才查找 Position Delete
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
    // [修正] 使用 volatile buffer 实现 Double-Check Locking
    private volatile List<...> buffer = Lists.newArrayList();

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
        // 找到,但需要找到第一个匹配(向前扫描)
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
                    // V3: Deletion Vector (Puffin 格式)
                    add(dvByPath, file);
                } else {
                    // V2: Position Delete
                    // [修正] 使用 ContentFileUtil.referencedDataFileLocation()
                    // 判断是路由到 posDeletesByPath 还是 posDeletesByPartition
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

**[源码校验补充] Position Delete 路由逻辑**: Builder 的 `add` 方法使用 `ContentFileUtil.referencedDataFileLocation(deleteFile)` 判断 Position Delete 是否仅作用于单个数据文件。如果返回非 null(file-scoped),则加入 `posDeletesByPath`;否则加入 `posDeletesByPartition`。

**[源码校验补充] loadDeleteFiles()**: 对于 POSITION_DELETES,保留 `DELETE_FILE_PATH` 列的统计信息;对于 EQUALITY_DELETES,保留 equality field ids 对应列的统计信息。

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

> "DeleteFileIndex 采用多维索引结构: DV 按路径 HashMap O(1) 查找,Position Delete 按路径/分区双索引,Equality Delete 支持全局和分区级别。核心是延迟索引模式 -- buffer 收集文件,首次查询时才排序构建 seqs 数组,通过二分查找实现 O(log n) 的 sequence number 过滤。关键优化是当找到 DV 时跳过 Position Delete 查找,因为 DV 已完整覆盖位置删除信息。"

#### 30 秒速记

- 结论先行: DeleteFileIndex 是 Iceberg 在读取时高效匹配删除文件的**核心数据结构**,它通过多维索引实现 O(log n) 时间复杂度的删除文件查找。
- 关键结构: `globalDeletes` / `eqDeletesByPartition` / `posDeletesByPartition` / `posDeletesByPath` / `dvByPath`
- 面试表达: 先回答"是什么",再补"为什么这样设计",最后给"边界条件/取舍"。

---
<a id="q-9-2"></a>
### 9.2 Equality Delete 的 Range Overlap 剪枝

**[橙] 问题: Equality Delete 的匹配比 Position Delete 更复杂。请说明 canContainEqDeletesForFile() 方法的统计信息剪枝逻辑。**

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

        // 剪枝规则 1: NULL 值检查
        if (containsNull(dataNullCounts, field) &&
            containsNull(deleteNullCounts, field)) {
            continue;  // 数据和删除都有 NULL,必须应用
        }

        // 剪枝规则 2: 全 NULL 数据 vs 非 NULL 删除
        if (allNull(dataNullCounts, dataValueCounts, field) &&
            allNonNull(deleteNullCounts, field)) {
            return false;
        }

        // 剪枝规则 3: 全 NULL 删除 vs 非 NULL 数据
        if (allNull(deleteNullCounts, deleteValueCounts, field) &&
            allNonNull(dataNullCounts, field)) {
            return false;
        }

        if (!checkRanges) {
            continue;
        }

        // 剪枝规则 4: 范围重叠检查
        int id = field.fieldId();
        ByteBuffer dataLower = dataLowers.get(id);
        ByteBuffer dataUpper = dataUppers.get(id);
        Object deleteLower = deleteFile.lowerBound(id);
        Object deleteUpper = deleteFile.upperBound(id);

        if (dataLower == null || dataUpper == null ||
            deleteLower == null || deleteUpper == null) {
            continue;
        }

        if (!rangesOverlap(field, dataLower, dataUpper, deleteLower, deleteUpper)) {
            return false;
        }
    }

    return true;
}
```

#### EqualityDeleteFile 缓存优化

```java
// 源码: DeleteFileIndex.java
private static class EqualityDeleteFile {
    private final PartitionSpec spec;
    private final DeleteFile wrapped;
    private final long applySequenceNumber;

    // 延迟计算并缓存的字段 (Double-Check Locking)
    private volatile List<Types.NestedField> equalityFields = null;
    private volatile Map<Integer, Object> convertedLowerBounds = null;
    private volatile Map<Integer, Object> convertedUpperBounds = null;

    EqualityDeleteFile(PartitionSpec spec, DeleteFile file) {
        this.spec = spec;
        this.wrapped = file;
        // [关键] Equality Delete 应用于 seq - 1 之前的数据
        this.applySequenceNumber = wrapped.dataSequenceNumber() - 1;
    }
}
```

#### 30 秒速记

- 结论先行: Equality Delete 通过**统计信息剪枝**,在文件级别快速判断是否可能包含匹配的删除记录。
- 四层剪枝: NULL 相交 -> 全 NULL 互斥 -> 范围不重叠 -> 嵌套类型保守处理
- 面试表达: 先回答"是什么",再补"为什么这样设计",最后给"边界条件/取舍"。

---
<a id="q-9-3"></a>
### 9.3 Deletion Vector 与 Roaring Bitmap 实现

**[黄] 问题: Iceberg V3 引入了 Deletion Vector (DV) 作为更高效的行级删除方案。请说明 DV 的 Roaring Bitmap 实现原理。**

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
| **文件格式判断** | 非 Puffin | `ContentFileUtil.isDV()`: format == PUFFIN |

#### RoaringPositionBitmap 核心实现

```java
// 源码: core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java
// 包级可见(package-private)
class RoaringPositionBitmap {
    static final long MAX_POSITION = toPosition(Integer.MAX_VALUE - 1, Integer.MIN_VALUE);

    private RoaringBitmap[] bitmaps;

    // 64位位置拆分:
    // key = (int)(pos >> 32)       -- 高32位
    // pos32Bits = (int)pos          -- 低32位
    // toPosition = (long)key << 32 | (long)pos32Bits & 0xFFFFFFFFL

    public void set(long pos) {
        validatePosition(pos);
        int key = key(pos);
        int pos32Bits = pos32Bits(pos);
        allocateBitmapsIfNeeded(key + 1);
        bitmaps[key].add(pos32Bits);
    }

    public boolean contains(long pos) {
        validatePosition(pos);
        int key = key(pos);
        int pos32Bits = pos32Bits(pos);
        return key < bitmaps.length && bitmaps[key].contains(pos32Bits);
    }

    // 序列化: Little-Endian, 8字节 count + (4字节 key + serialized bitmap) 每条目
    public void serialize(ByteBuffer buffer) { ... }
    public static RoaringPositionBitmap deserialize(ByteBuffer buffer) { ... }
}
```

#### 高频追问

**Q: DV 和 Position Delete 可以共存吗？**

**A**:
- V2 表: 只能使用 Position Delete (V2 禁止使用 DV)
- V3 表: 新写入**必须**使用 DV (`MergingSnapshotProducer.validateNewDeleteFile` 强制校验),可以读取旧的 Position Delete
- 升级路径: 通过 Compaction 将 Position Delete 转换为 DV
- DeleteFileIndex 会同时索引两者,`forDataFile()` 找到 DV 后跳过 Position Delete 查找

#### DV 在 Manifest 中的记录格式（V3 新增字段详解）

DV **复用了** delete manifest 的已有 schema,并没有独立的 manifest 格式。DV 作为 `content=1`（POSITION_DELETES）的条目存在于 delete manifest 中,但通过 3 个 V3 新增字段来唯一标识自身。

**V3 新增的 3 个关键字段（源码: `api/.../DataFile.java`）:**

```java
// field id=143 — 此 DV 对应的数据文件路径（DV 必填,position delete 可选）
Types.NestedField REFERENCED_DATA_FILE = optional(143, "referenced_data_file", StringType.get(),
    "Fully qualified location (URI with FS scheme) of a data file that all deletes reference");

// field id=144 — DV blob 在 Puffin 文件中的起始偏移量（DV 必填）
Types.NestedField CONTENT_OFFSET = optional(144, "content_offset", LongType.get(),
    "The offset in the file where the content starts");

// field id=145 — DV blob 的字节长度（DV 必填）
Types.NestedField CONTENT_SIZE = optional(145, "content_size_in_bytes", LongType.get(),
    "The length of referenced content stored in the file");
```

这 3 个字段在 `V3Metadata.fileType()` 中被加入 manifest entry 的 `data_file` struct（行 280-303）:

```java
// 源码: core/src/main/java/org/apache/iceberg/V3Metadata.java
static Types.StructType fileType(Types.StructType partitionType) {
    return Types.StructType.of(
        DataFile.CONTENT.asRequired(),        // 0: content (int)
        DataFile.FILE_PATH,                    // 1: file_path
        DataFile.FILE_FORMAT,                  // 2: file_format
        required(..., partitionType, ...),     // 3: partition
        DataFile.RECORD_COUNT,                 // 4: record_count
        DataFile.FILE_SIZE,                    // 5: file_size_in_bytes
        DataFile.COLUMN_SIZES,                 // 6-11: 统计信息字段...
        DataFile.VALUE_COUNTS,
        DataFile.NULL_VALUE_COUNTS,
        DataFile.NAN_VALUE_COUNTS,
        DataFile.LOWER_BOUNDS,
        DataFile.UPPER_BOUNDS,
        DataFile.KEY_METADATA,                 // 12: key_metadata
        DataFile.SPLIT_OFFSETS,                // 13: split_offsets
        DataFile.EQUALITY_IDS,                 // 14: equality_ids
        DataFile.SORT_ORDER_ID,                // 15: sort_order_id
        DataFile.FIRST_ROW_ID,                 // 16: first_row_id (V3, 仅 data file)
        DataFile.REFERENCED_DATA_FILE,         // 17: ★ referenced_data_file
        DataFile.CONTENT_OFFSET,               // 18: ★ content_offset
        DataFile.CONTENT_SIZE);                // 19: ★ content_size_in_bytes
}
```

写入时的条件判断（`V3Metadata.DataFileWrapper.get()`，行 454-514）:

```java
// 仅 content == POSITION_DELETES 时才写入这 3 个字段
case 17: // referenced_data_file
    if (wrapped.content() == FileContent.POSITION_DELETES) {
        return ((DeleteFile) wrapped).referencedDataFile();
    } else { return null; }

case 18: // content_offset
    if (wrapped.content() == FileContent.POSITION_DELETES) {
        return ((DeleteFile) wrapped).contentOffset();
    } else { return null; }

case 19: // content_size_in_bytes
    if (wrapped.content() == FileContent.POSITION_DELETES) {
        return ((DeleteFile) wrapped).contentSizeInBytes();
    } else { return null; }
```

**DV 的 Delete Manifest Entry 完整结构:**

```
ManifestEntry (Avro record)
├── status              : int        (0=EXISTING, 1=ADDED, 2=DELETED)
├── snapshot_id         : long?      (所属快照 ID)
├── sequence_number     : long?      (数据序列号)
├── file_sequence_number: long?      (文件序列号)
└── data_file           : struct     (核心文件信息)
    ├── content             : int=1     ← POSITION_DELETES
    ├── file_path           : string    ← Puffin 文件路径
    ├── file_format         : string    ← "puffin"
    ├── partition           : struct    ← 分区值
    ├── record_count        : long      ← DV 的基数(被删除的行数)
    ├── file_size_in_bytes  : long      ← Puffin 文件总大小
    ├── column_sizes        : null      (DV 不适用)
    ├── value_counts        : null
    ├── null_value_counts   : null
    ├── nan_value_counts    : null
    ├── lower_bounds        : null
    ├── upper_bounds        : null
    ├── key_metadata        : null
    ├── split_offsets        : null
    ├── equality_ids        : null      (仅 equality delete 使用)
    ├── sort_order_id       : null
    ├── first_row_id        : null      (仅 data file 使用)
    │
    │   ====== V3 新增: DV 专用字段 ======
    ├── referenced_data_file   : string ← ★ 此 DV 对应的数据文件路径 (必填)
    ├── content_offset         : long   ← ★ DV blob 在 Puffin 文件中的偏移量 (必填)
    └── content_size_in_bytes  : long   ← ★ DV blob 的字节长度 (必填)
```

#### DV Manifest Entry 具体示例

**场景**: 表 `db.orders`,分区字段 `order_date`,数据文件含 10000 行,删除第 3、17、42 行。

**Step 1: Puffin 文件结构**

```
s3://bucket/data/order_date=2024-01-15/00001-delete-dv.puffin
├─ Header: magic bytes ("PUF1")
├─ Blob #0 (offset=4, length=38):
│   type: "deletion-vector-v1"   ← StandardBlobTypes.DV_V1
│   properties:
│     referenced-data-file: "s3://bucket/.../00001.parquet"
│     cardinality: "3"
│   data (BitmapPositionDeleteIndex 序列化格式):
│     ┌──────────┬────────────┬──────────────────────┬──────────┐
│     │ len (4B) │ magic (4B) │ Roaring Bitmap (变长) │ CRC (4B) │
│     │ 30       │ 0x6433D364 │ {3, 17, 42}          │ checksum │
│     └──────────┴────────────┴──────────────────────┴──────────┘
└─ Footer: blob 元数据列表 + magic ("PUF1")
```

**Step 2: 对应的 Delete Manifest Entry (JSON 表示)**

```json
{
  "status": 1,
  "snapshot_id": 8899001122,
  "sequence_number": 5,
  "file_sequence_number": 5,
  "data_file": {
    "content": 1,
    "file_path": "s3://bucket/data/order_date=2024-01-15/00001-delete-dv.puffin",
    "file_format": "puffin",
    "partition": { "order_date": "2024-01-15" },
    "record_count": 3,
    "file_size_in_bytes": 256,
    "column_sizes": null,
    "value_counts": null,
    "null_value_counts": null,
    "nan_value_counts": null,
    "lower_bounds": null,
    "upper_bounds": null,
    "key_metadata": null,
    "split_offsets": null,
    "equality_ids": null,
    "sort_order_id": null,
    "first_row_id": null,
    "referenced_data_file": "s3://bucket/data/order_date=2024-01-15/00001.parquet",
    "content_offset": 4,
    "content_size_in_bytes": 38
  }
}
```

**对比: V2 Position Delete File 的 Manifest Entry**

```json
{
  "status": 1,
  "snapshot_id": 8899001122,
  "sequence_number": 5,
  "file_sequence_number": 5,
  "data_file": {
    "content": 1,
    "file_path": "s3://bucket/data/order_date=2024-01-15/00001-pos-del.parquet",
    "file_format": "parquet",
    "partition": { "order_date": "2024-01-15" },
    "record_count": 3,
    "file_size_in_bytes": 1024,
    "referenced_data_file": null,
    "content_offset": null,
    "content_size_in_bytes": null
  }
}
```

Position Delete 文件**内部**还需存储冗余的文件路径:

```
| file_path (string)                                      | pos (long) |
| s3://bucket/data/order_date=2024-01-15/00001.parquet    | 3          |
| s3://bucket/data/order_date=2024-01-15/00001.parquet    | 17         |
| s3://bucket/data/order_date=2024-01-15/00001.parquet    | 42         |
```

#### DV 的读取匹配规则

根据 Iceberg spec,DV 必须应用于数据文件当且仅当同时满足:

```
1. data_file.file_path == deletion_vector.referenced_data_file   ← 1:1 精确匹配
2. data_file.data_sequence_number <= deletion_vector.data_sequence_number
3. data_file.partition == deletion_vector.partition
```

读取引擎执行流程:

```
1. 从 delete manifest 读到 DV entry
2. 用 content_offset + content_size_in_bytes 直接定位 Puffin 文件中的 blob
   → 无需读取整个 Puffin 文件,支持 range read
3. 反序列化为 Roaring Bitmap → {3, 17, 42}
4. 扫描数据文件时: if (bitmap.contains(rowPos)) → skip
```

**关键规则**: 当 DV 存在时,读取器可以安全地忽略同一数据文件匹配的 position delete files。因为 DV 写入时必须合并所有已有的 position deletes,确保 DV 是完整的删除集合。

#### DV vs Position Delete: Manifest 记录差异总结

| Manifest 字段 | Position Delete (V2) | Deletion Vector (V3) |
|---------------|---------------------|---------------------|
| `content` | 1 | 1 |
| `file_path` | `.parquet` / `.avro` | `.puffin` |
| `file_format` | "parquet" / "avro" | "puffin" |
| `record_count` | 删除记录数 | 删除行数（基数） |
| `file_size_in_bytes` | delete 文件大小 | Puffin 文件大小 |
| `referenced_data_file` | null 或可选填写 | **必填** — 1:1 绑定数据文件 |
| `content_offset` | null | **必填** — blob 起始偏移 |
| `content_size_in_bytes` | null | **必填** — blob 字节长度 |
| 文件内部存储 | (file_path, pos) 记录列表 | 序列化 Roaring Bitmap |
| 关联关系 | M:N（一对多/多对多） | **1:1**（每个数据文件最多一个 DV） |

#### 30 秒速记

- 结论先行: DV 使用 Roaring Bitmap,64位拆分为高低32位,三种 Container 自适应选择。
- 核心优势: 稀疏用 Array(2*count字节),密集用 Bitmap(固定8KB),连续用 Run(4*runs字节)
- Manifest 记录: DV 复用 delete manifest schema,通过 `referenced_data_file`(必填)、`content_offset`、`content_size_in_bytes` 三个 V3 新增字段标识,`file_format` 为 `puffin`。
- 面试表达: 先回答"是什么",再补"为什么这样设计",最后给"边界条件/取舍"。

---

## 十、面试必杀技 - 能让你脱颖而出的源码细节

> 本章收集了一些**不太容易被发现但非常有深度**的源码细节。在面试中提及这些内容,可以展示你对 Iceberg 的深入理解,让面试官眼前一亮。
<a id="q-10-1"></a>
### 10.1 Flink CommitDeltaTxn 为何不合并事务

**[红] 问题: 在 Flink 写入 Iceberg 时,如果存在 Delete 文件,IcebergFilesCommitter 会逐 checkpoint 提交而不是合并成一个事务。这是为什么？**

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
            // 关键注释:
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
+-------------------------------------------------------------+
| 单个事务: seq=101                                           |
|                                                             |
| DataFile 1: {id=1, name="Alice"}  seq=101                  |
| DataFile 2: {id=1, name="Bob"}    seq=101                  |
| DeleteFile: id=1                  seq=101                  |
|                                                             |
| 问题: Equality Delete 的 seq=101                           |
|       只删除 seq < 101 的数据                               |
|       但 DataFile 1 的 seq 也是 101!                        |
|       -> Equality Delete 无法删除 DataFile 1 中的 Alice     |
|       -> 结果: 同时存在 Alice 和 Bob (数据重复!)             |
+-------------------------------------------------------------+

正确做法: 逐 checkpoint 提交
+-------------------------------------------------------------+
| 事务1: seq=100                                              |
| DataFile 1: {id=1, name="Alice"}  seq=100                  |
+-------------------------------------------------------------+
                           |
+-------------------------------------------------------------+
| 事务2: seq=101                                              |
| DataFile 2: {id=1, name="Bob"}    seq=101                  |
| DeleteFile: id=1                  seq=101                  |
|                                                             |
| Equality Delete seq=101 删除 seq < 101 的数据              |
| -> DataFile 1 (seq=100) 中的 Alice 被删除                   |
| -> DataFile 2 (seq=101) 中的 Bob 保留                       |
| -> 结果正确: 只有 Bob                                        |
+-------------------------------------------------------------+
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

#### 面试加分话术

> "Flink IcebergFilesCommitter 在有 Delete 文件时必须逐 checkpoint 提交,原因是 Equality Delete 的 Sequence Number 语义: Delete 文件只删除 seq < N 的数据。如果合并提交,所有文件的 seq 相同,Delete 无法删除同一事务中的旧数据,导致 Upsert 语义错误。这是我在阅读源码时发现的一个非常精妙的设计。"

#### 30 秒速记

- 结论先行: 这是 Equality Delete 的 Sequence Number 语义要求,合并提交会导致 Upsert 语义错误。
- 关键判断: `summary.deleteFilesCount() == 0` -> 合并; 否则逐 checkpoint RowDelta 提交
- 面试表达: 先回答"是什么",再补"为什么这样设计",最后给"边界条件/取舍"。

---
<a id="q-10-2"></a>
### 10.2 IcebergFilesCommitter 的 NavigableMap 设计

**[橙] 问题: IcebergFilesCommitter 使用 NavigableMap 来管理待提交的 checkpoint 数据。这个数据结构选择有什么特殊考量？**

**答案:**

`NavigableMap<Long, byte[]>` 的选择是为了处理 **Flink Checkpoint 乱序完成** 和 **故障恢复** 场景。

#### 数据结构定义

**[源码校验修正]** 实际源码中 `dataFilesPerCheckpoint` 存储的是 `byte[]`(序列化后的 DeltaManifests),而不是 `WriteResult` 对象。中间有一个 `writeResultsSinceLastSnapshot` 缓存 `Map<Long, List<WriteResult>>`。

```java
// 源码: IcebergFilesCommitter.java
// [修正] 存储的是序列化后的 DeltaManifests 字节数组
private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

// [补充] 写入结果的中间缓存
private Map<Long, List<WriteResult>> writeResultsSinceLastSnapshot;

// [补充] 初始 checkpoint ID
private static final long INITIAL_CHECKPOINT_ID = -1L;
```

#### 场景1: Checkpoint 乱序完成

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
  - checkpointId=2 > maxCommittedCheckpointId=-1
  - headMap(2, true) = {1: [...], 2: [...]}
  - 同时提交 ckp1 和 ckp2 的数据
  - maxCommittedCheckpointId = 2

步骤4执行时:
  - checkpointId=1 < maxCommittedCheckpointId=2
  - 跳过! (数据已在步骤3提交)
```

#### commitUpToCheckpoint 内部流程

**[源码校验补充]** 该方法内部会反序列化 `byte[]` 为 `DeltaManifests`,从中提取 `WriteResult`,构建 `NavigableMap<Long, WriteResult> pendingResults`,然后调用 `commitDeltaTxn`。

```java
// 源码: IcebergFilesCommitter.java
private void commitUpToCheckpoint(
        NavigableMap<Long, byte[]> deltaManifestsMap,
        String newFlinkJobId,
        String operatorId,
        long checkpointId) throws IOException {

    // 获取 <= checkpointId 的所有待提交数据
    NavigableMap<Long, byte[]> pendingMap = deltaManifestsMap.headMap(checkpointId, true);

    // [修正] 遍历反序列化 DeltaManifests -> WriteResult
    NavigableMap<Long, WriteResult> pendingResults = Maps.newTreeMap();
    for (Map.Entry<Long, byte[]> e : pendingMap.entrySet()) {
        DeltaManifests deltaManifests = SimpleVersionedSerialization
            .readVersionAndDeSerialize(..., e.getValue());
        // ... 构建 WriteResult
    }

    // 提交
    commitDeltaTxn(pendingResults, ...);

    // 提交后清理
    pendingMap.clear();  // 自动从 deltaManifestsMap 中移除
}
```

#### 场景2: 故障恢复

```java
// 源码: IcebergFilesCommitter.java
@Override
public void initializeState(StateInitializationContext context) throws Exception {
    if (context.isRestored()) {
        // 从 state 恢复
        String restoredFlinkJobId = jobIdIterable.iterator().next();

        // 获取已提交的最大 checkpoint id
        this.maxCommittedCheckpointId = SinkUtil.getMaxCommittedCheckpointId(
            table, restoredFlinkJobId, operatorUniqueId, branch);

        // 关键: 使用 tailMap 获取未提交的数据
        NavigableMap<Long, byte[]> uncommittedDataFiles =
            Maps.newTreeMap(checkpointsState.get().iterator().next())
                .tailMap(maxCommittedCheckpointId, false);  // > maxCommittedCheckpointId

        if (!uncommittedDataFiles.isEmpty()) {
            long maxUncommittedCheckpointId = uncommittedDataFiles.lastKey();
            commitUpToCheckpoint(uncommittedDataFiles, restoredFlinkJobId,
                                 operatorUniqueId, maxUncommittedCheckpointId);
        }
    }
}
```

#### 空 Checkpoint 处理

**[源码校验补充]** IcebergFilesCommitter 还有一个 `continuousEmptyCheckpoints` 计数器,当连续空 checkpoint 达到 `maxContinuousEmptyCommits`(默认 10)时,会提交一个空的快照,确保消费端能看到进度推进。

```java
// IcebergFilesCommitter.java
private static final int MAX_CONTINUOUS_EMPTY_COMMITS_DEFAULT = 10;
```

#### NavigableMap vs HashMap 对比

| 特性 | NavigableMap (TreeMap) | HashMap |
|------|------------------------|---------|
| **范围查询** | O(log n) headMap/tailMap | 需遍历 O(n) |
| **有序遍历** | 按 key 排序 | 无序 |
| **插入/查找** | O(log n) | O(1) |
| **适用场景** | checkpoint 管理 | 简单 key-value |

#### 面试加分话术

> "IcebergFilesCommitter 使用 NavigableMap<Long, byte[]> 管理 checkpoint 数据。注意存储的是序列化后的 DeltaManifests 字节数组,不是 WriteResult。核心是利用 headMap/tailMap 处理两个场景: (1) Checkpoint 乱序完成时,headMap(ckpId) 一次性提交所有 <= ckpId 的数据 (2) 故障恢复时,tailMap(maxCommitted) 精确找出未提交的 checkpoint 并重新提交。"

#### 30 秒速记

- 结论先行: `NavigableMap<Long, byte[]>` 利用有序映射处理 Checkpoint 乱序和故障恢复。
- 关键方法: `headMap(ckpId, true)` 批量提交 + `tailMap(maxCommitted, false)` 恢复未提交
- 面试表达: 先回答"是什么",再补"为什么这样设计",最后给"边界条件/取舍"。

---
<a id="q-10-3"></a>
### 10.3 Iceberg 为何选择 Roaring Bitmap

**[黄] 问题: Iceberg 在多处使用 Roaring Bitmap,包括 Deletion Vector 和 Position Delete 缓存。相比其他位图实现,Roaring Bitmap 有什么优势？**

**答案:**

Roaring Bitmap 是 Iceberg 选择的**核心位图数据结构**,在存储效率和查询性能之间取得了最佳平衡。

#### Roaring Bitmap 三种 Container

```
每个 Container 管理 65536 (2^16) 个位置

1. Array Container (稀疏, < 4096 元素)
   存储: sorted short[] 数组
   空间: 2 * cardinality 字节
   查找: 二分查找 O(log n)

2. Bitmap Container (密集, >= 4096 元素)
   存储: 固定 1024 个 long (65536 位)
   空间: 固定 8KB
   查找: O(1) 位运算

3. Run Container (连续范围)
   存储: (start, length) 对的数组
   空间: 4 * runs 字节
   查找: 二分查找 O(log runs)

临界点 4096 的精心计算:
Array 空间 = 4096 * 2 = 8KB = Bitmap 固定空间
-> 超过 4096 切换到 Bitmap 更划算
```

#### Iceberg 中的 Roaring 使用场景

**1. Deletion Vector (V3)**
```java
// 源码: core/src/main/java/org/apache/iceberg/deletes/RoaringPositionBitmap.java
class RoaringPositionBitmap {
    private RoaringBitmap[] bitmaps;  // 64位 -> 高32位 key + 低32位 Roaring
}
```

**2. Position Delete 缓存**: 通过 `PositionDeleteIndex` 接口和 `BitmapPositionDeleteIndex` 实现。

**3. DV 反序列化路径**:
```java
// 源码: data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java
private PositionDeleteIndex readDV(DeleteFile dv) {
    InputFile inputFile = loadInputFile.apply(dv);
    long offset = dv.contentOffset();
    int length = dv.contentSizeInBytes().intValue();
    byte[] bytes = readBytes(inputFile, offset, length);
    return PositionDeleteIndex.deserialize(bytes, dv);
}
```

#### 面试加分话术

> "Roaring Bitmap 是 Iceberg 的核心位图选择,优势在于三种 Container 自适应: Array Container 用于稀疏场景(< 4096),Bitmap Container 用于密集场景(>= 4096),Run Container 用于连续范围(批量删除)。临界点 4096 是精心计算的: Array 空间 = 4096 * 2 字节 = Bitmap 固定 8KB。这种设计使 Roaring 在各种删除模式下都能保持最优性能。"

#### 30 秒速记

- 结论先行: Roaring Bitmap 的核心优势是**自适应**,根据数据密度选择最优存储方式。
- 三种 Container: Array(稀疏) / Bitmap(密集) / Run(连续) -- 临界点 4096
- 面试表达: 先回答"是什么",再补"为什么这样设计",最后给"边界条件/取舍"。

---
<a id="q-10-4"></a>
### 10.4 表格式版本演进 V1->V2->V3 核心变化

**[橙] 问题: Iceberg 表格式从 V1 演进到 V3,每个版本引入了哪些核心特性？如何理解版本兼容性？**

**答案:**

Iceberg 表格式版本演进遵循**向前兼容**原则: 新版本读取器可以读取旧版本数据,但旧版本读取器无法理解新版本特性。

#### 版本核心特性

| 版本 | 核心特性 | 新增能力 |
|------|---------|---------|
| **V1** | 分析表基础格式 | 快照隔离、Schema/分区演进、时间旅行 |
| **V2** | 行级删除 | Position Delete、Equality Delete、Sequence Number |
| **V3** | 扩展类型和 DV | 纳秒时间戳、Variant、DV(Roaring Bitmap)、Geometry |
| **V4** | (开发中) | First Row ID、更多类型扩展 |

#### 版本间的硬约束 (源码级校验)

```java
// 源码: MergingSnapshotProducer.java
protected void validateNewDeleteFile(DeleteFile file) {
    switch (formatVersion()) {
        case 1:
            // V1: 不支持任何删除文件
            throw new IllegalArgumentException("Deletes are supported in V2 and above");

        case 2:
            // V2: Position Delete 不能使用 DV (Puffin 格式)
            Preconditions.checkArgument(
                file.content() == FileContent.EQUALITY_DELETES || !ContentFileUtil.isDV(file),
                "Must not use DVs for position deletes in V2: %s", ...);
            break;

        case 3:
        case 4:
            // V3/V4: Position Delete 必须使用 DV
            Preconditions.checkArgument(
                file.content() == FileContent.EQUALITY_DELETES || ContentFileUtil.isDV(file),
                "Must use DVs for position deletes in V%s: %s", formatVersion(), ...);
            break;
    }
}
```

**关键理解**:
- V2 的 `!ContentFileUtil.isDV(file)`: 确保 Position Delete 不是 DV
- V3 的 `ContentFileUtil.isDV(file)`: 确保 Position Delete 必须是 DV
- Equality Delete 在 V2/V3/V4 都不受 DV 约束

#### 版本兼容性矩阵

| 写入版本 | V1 读取器 | V2 读取器 | V3 读取器 |
|---------|----------|----------|----------|
| V1 | 支持 | 支持 | 支持 |
| V2 | 不支持 (无法理解 Delete) | 支持 | 支持 |
| V3 | 不支持 | 不支持 (无法理解 DV/新类型) | 支持 |

#### 版本选择建议

| 场景 | 推荐版本 | 原因 |
|------|---------|------|
| 只追加数据 | V1 | 简单,兼容性最好 |
| 需要 UPDATE/DELETE | V2 | 支持行级删除 |
| 大量删除操作 | V3 | DV 压缩效率高 |
| 需要新数据类型 | V3 | Variant, 纳秒时间戳 |
| 最大兼容性 | V2 | 主流引擎都支持 |

**注意**: 版本升级是**单向**的,无法降级!

#### 面试加分话术

> "Iceberg 表格式版本演进遵循向前兼容原则: V1 提供分析表基础,V2 引入行级删除和 Sequence Number,V3 添加 Deletion Vector 和新数据类型。关键设计是 V2 的 Sequence Number -- 它不仅用于快照隔离,还控制 Equality Delete 的应用范围(只删除 seq < N 的数据)。V3 的 DV 强制要求 Position Delete 使用 Roaring Bitmap,相比 V2 可减少 50-60000 倍存储。这个约束在 `MergingSnapshotProducer.validateNewDeleteFile` 中硬编码校验。"

#### 30 秒速记

- 结论先行: V1 基础格式 -> V2 行级删除+Sequence Number -> V3 DV+新类型。版本升级不可逆。
- 硬约束: V1 无删除、V2 禁止 DV、V3 强制 DV(Position Delete)
- 面试表达: 先回答"是什么",再补"为什么这样设计",最后给"边界条件/取舍"。

---

## 十一、源码级高阶追问（12题）

> 本章面向"二面/三面追问场景"。每题统一为 **总-分-总 + 追问与反问**,并附源码锚点。
<a id="q-11-1"></a>
### 11.1 DeleteFilter 为什么先 Position 再 Equality

**[红] 问题: `DeleteFilter` 为什么先应用 position delete,再应用 equality delete？顺序可交换吗？**

**答案(总):**

当前实现先按位置过滤,再按值过滤。语义上通常可交换,但性能上先做位置过滤更划算,因为 position 判定更轻量。

**答案(分):**

1. 调用顺序在代码里是写死的: `applyEqDeletes(applyPosDeletes(records))`。
   - 源码: `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:177-179`
2. position 判定依赖 `PositionDeleteIndex#isDeleted(pos)`,是 O(1) 风格位点检查。
   - 源码: `DeleteFilter.java:255-257`
3. equality 判定需要构建 `StructLikeSet` 和行投影,常数开销更高。
   - 源码: `DeleteFilter.java:202-210`

**答案(总):**

30 秒话术: **先用便宜条件快速减行,再用昂贵条件精筛,这是典型分层过滤设计。**

**追问与反问:**
- 追问: "如果 equality delete 远多于 position delete,还应该这个顺序吗？"
- 反问: "我们是否做过 delete 类型占比统计,来验证顺序对真实负载的收益？"

---
<a id="q-11-2"></a>
### 11.2 `requiredSchema` 如何动态扩展以支持 Delete

**[橙] 问题: 用户只查少量列,为什么读取时 Iceberg 可能补读更多列？**

**答案(总):**

因为 delete 过滤需要额外字段。`fileProjection` 会在请求列之外补齐 `_pos` 和 equality 字段,保证删除语义正确。

**答案(分):**

1. 没有 deletes 时直接返回 `requestedSchema`。
   - 源码: `data/src/main/java/org/apache/iceberg/data/DeleteFilter.java:273-275`
2. 存在 position delete 且 `needRowPosCol` 为 true 时补 `ROW_POSITION`。
   - 源码: `DeleteFilter.java:278-280`
3. 对每个 equality delete 收集 `equalityFieldIds` 并补进投影。
   - 源码: `DeleteFilter.java:282-284`
4. 只补"缺失字段",通过 `Sets.difference` 计算需要添加的 ID 集合。
   - 源码: `DeleteFilter.java:286-288`
5. `_pos` 和 `_deleted` 列固定添加到末尾。
   - 源码: `DeleteFilter.java:298-315`

**答案(总):**

一句话: **逻辑查询列不等于物理读取列,Iceberg 会做最小必要扩列来换语义正确。**

---
<a id="q-11-3"></a>
### 11.3 `DeleteFileIndex` 如何避免朴素 O(M*N)

**[红] 问题: 为什么 delete 匹配不是"每个 DataFile 扫所有 DeleteFile"？**

**答案(总):**

Iceberg 先建索引再匹配。`ManifestGroup` 先构建 `DeleteFileIndex`,随后每个 entry 只查候选桶,不做全量笛卡尔遍历。

**答案(分):**

1. 规划阶段先执行 `deleteIndexBuilder.build()`。
   - 源码: `core/src/main/java/org/apache/iceberg/ManifestGroup.java`
2. 任务生成时按 entry 调 `ctx.deletes().forEntry(entry)`。
3. `DeleteFileIndex` 内部按 `global / partition / path / dv` 分桶查找。
4. equality 还会叠加统计边界判断(`canContainEqDeletesForFile`),进一步缩小候选。

**答案(总):**

面试收口: **这是"候选集匹配"而不是"全集匹配",复杂度下降来自索引分层。**

---
<a id="q-11-4"></a>
### 11.4 为什么 Equality Delete 要 `applySequenceNumber = dataSeq - 1`

**[红] 问题: 这个 `-1` 到底在保护什么语义？**

**答案(总):**

它把 equality delete 的生效条件从"<="改成了严格"<",避免删到同序列的新写入数据,是并发正确性的关键细节。

**答案(分):**

1. 构造 `EqualityDeleteFile` 时明确执行 `wrapped.dataSequenceNumber() - 1`。
   - 源码: `core/src/main/java/org/apache/iceberg/DeleteFileIndex.java`
2. equality 过滤按 `applySequenceNumber` 排序比较。
3. 推导后等价于 `deleteDataSeq > dataDataSeq`,自然规避同序列误删。

**答案(总):**

一锤定音: **`-1` 不是实现小技巧,而是"严格前序可见"语义编码。**

**追问与反问:**
- 追问: "position delete 为什么不做 `-1`？"
- 回答: 因为 position delete 通过 (file_path, pos) 精确指定被删除行,不依赖 sequence number 比较,所以无需 -1 调整。

---
<a id="q-11-5"></a>
### 11.5 Position Delete 如何利用 metrics 推断 file-scoped

**[橙] 问题: `referencedDataFile` 为空时,Iceberg 还能否只靠元数据判断作用范围？**

**答案(总):**

可以。若 `file_path` 的 lower/upper bound 相等,源码直接把它判成 file-scoped,无需先读 delete 内容。

**答案(分):**

1. equality delete 直接返回 null,不参与这条推断。
   - 源码: `core/src/main/java/org/apache/iceberg/util/ContentFileUtil.java:63-66`
2. position delete 先看显式 `referencedDataFile()` 字段。
   - 源码: `ContentFileUtil.java:68-70`
3. 若未显式给出,则比较 `file_path`(`DELETE_FILE_PATH` 列) 的 lower/upper bound。
   - 源码: `ContentFileUtil.java:72-84`
4. 上下界相等即返回单文件路径(通过 `Conversions.fromByteBuffer`),否则返回 null 表示"多文件未知"。
   - 源码: `ContentFileUtil.java:84-87`

**答案(总):**

面试收口: **Iceberg 先用统计推断,再决定是否读内容,这是典型"元数据先行"的成本优化。**

---
<a id="q-11-6"></a>
### 11.6 Spark Executor Delete 缓存的真实行为与边界

**[橙] 问题: Spark 的 delete 缓存到底怎么开启、如何淘汰、有什么边界？**

**答案(总):**

这是会话级配置驱动的 executor 缓存,不是 read option;淘汰机制是访问过期 + 权重上限,不是单一 LRU。

**答案(分):**

1. 开关入口: `SparkReadConf#cacheDeleteFilesOnExecutors`。
2. 配置键来自 `SparkSQLProperties` 会话参数。
3. 缓存实现: `expireAfterAccess(timeout) + maximumWeight(maxTotalSize)`。
4. 权重由 entry size 决定,超大对象可能导致低命中或不经济缓存。

**答案(总):**

一句话: **delete 缓存是"容量与时效受约束的机会主义优化",不是万能开关。**

---
<a id="q-11-7"></a>
### 11.7 Flink 流式增量启动策略差异（旧 API vs FLIP-27）

**[红] 问题: 旧 SourceFunction 与 FLIP-27 在"首次启动"阶段有什么关键差异？**

**答案(总):**

两条链路都支持增量,但首轮行为不同。旧 API 在 monitor 中首次 `copyWithSnapshotId`;FLIP-27 的 `TABLE_SCAN_THEN_INCREMENTAL` 会先做一次批量快照扫描再切增量。

**答案(分):**

1. 旧 API 首轮分支: `lastSnapshotId == INIT_LAST_SNAPSHOT_ID(-1L)` 时 `copyWithSnapshotId(snapshotId)`。
   - 源码: `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/StreamingMonitorFunction.java`
2. 后续轮次再用 `copyWithAppendsBetween`。
3. FLIP-27 在 `TABLE_SCAN_THEN_INCREMENTAL` 下显式先做一次全量扫描。
   - 源码: `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/source/enumerator/ContinuousSplitPlannerImpl.java`
4. 其他策略(如 latest-exclusive)直接从定位点进入增量。

**答案(总):**

面试收口: **"增量读取"描述的是持续阶段,不代表首次冷启动一定轻量。**

---
<a id="q-11-8"></a>
### 11.8 `max-planning-snapshot-count` 如何节流并影响吞吐

**[橙] 问题: 这个参数只影响 planner CPU,还是也影响消费节奏？**

**答案(总):**

它不仅影响规划成本,还会改变每轮推进窗口,从而影响 split 发现批量、追赶速度和端到端延迟。

**答案(分):**

1. 旧 API 和 FLIP-27 都用此参数截断本轮终点快照。
2. 截断后通过 position 继续推进,语义连续但节奏改变。
3. 调小 -> 更稳定但追赶变慢;调大 -> 追赶快但规划开销高。

**答案(总):**

一句话: **这是"单轮批量度阀门",本质是在实时性和稳态吞吐间做权衡。**

---
<a id="q-11-9"></a>
### 11.9 Deletion Vector 读取路径与缓存策略为何不同于 position deletes

**[橙] 问题: DV 与 position delete 在读取与缓存上有什么本质区别？**

**答案(总):**

DV 走 Puffin 偏移读取、直接反序列化位图;position delete 则按 delete 文件生成可复用位置索引。两者 IO 形态不同,所以缓存策略也不同。

**答案(分):**

1. `loadPositionDeletes` 检测到单个 DV 时走 `readDV` 分支。
   - 源码: `data/src/main/java/org/apache/iceberg/data/BaseDeleteLoader.java:170-176`
2. `readDV` 依赖 `contentOffset/contentSizeInBytes` 精确读取 blob,使用 `RangeReadable` 优化。
   - 源码: `BaseDeleteLoader.java:181-187`
3. 源码注释明确: DV 当前不走缓存路径,因为 Puffin 读取需要至少 3 次请求,且 task locality 不保证。
   - 源码: `BaseDeleteLoader.java:154-163`
4. position delete 可缓存 `CharSequenceMap<PositionDeleteIndex>` 并按 data file 取索引,因为一个 delete 文件可能关联多个 data file。
   - 源码: `BaseDeleteLoader.java:200-204`

**答案(总):**

面试收口: **DV 是"定点位图读取",position delete 是"可复用索引构建",缓存收益模型不同。**

---
<a id="q-11-10"></a>
### 11.10 何时会强制读取更多统计列（Equality Delete 场景）

**[橙] 问题: 为什么有些查询 manifest 规划时会"多读统计列"？**

**答案(总):**

当存在 equality deletes 时,`ManifestGroup` 会主动保留 stats 列,用于后续候选裁剪,减少无效 delete 应用。

**答案(分):**

1. 先构建 delete index,再判断 `hasEqualityDeletes()`。
2. 命中后执行 `select(ManifestReader.withStatsColumns(columns))`。
3. 这是"规划期多读元数据,执行期少做无效工作"的典型取舍。

**答案(总):**

一句话: **多读统计是为了少做错事,不是 planner 失控。**

---
<a id="q-11-11"></a>
### 11.11 Runtime Filter 在 Spark Iceberg Scan 中如何生效

**[黄] 问题: Spark Join 产生的 runtime filter 如何影响 Iceberg 扫描任务？**

**答案(总):**

它不是"读后过滤",而是在 scan 任务集合上做前置裁剪。命中后会重置任务列表,直接减少后续 split 与 IO。

**答案(分):**

1. 入口是 `SparkBatchQueryScan#filter(Predicate[])`。
2. 先把 Spark predicate 转成 Iceberg expression。
3. 按 `PartitionSpec` 生成 evaluator,过滤 `tasks()`。
4. 仅当任务数减少时才 `resetTasks(filteredTasks)`,避免无效重规划。

**答案(总):**

可背句: **Runtime Filter 在 Iceberg 中的价值,是"计划前移裁剪",不是"执行后补救"。**

---
<a id="q-11-12"></a>
### 11.12 Row-level delete 在格式版本上的约束差异（V2/V3+）

**[红] 问题: V2 与 V3+ 在 position delete 介质上的硬约束是什么？**

**答案(总):**

这是写入时的硬校验,不是文档建议。V1 不支持 delete;V2 禁止 position delete 使用 DV;V3/V4 要求 position delete 必须使用 DV(equality delete 除外)。

**答案(分):**

1. V1 直接抛错: "Deletes are supported in V2 and above"。
   - 源码: `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java`
2. V2 约束: position delete 不能是 DV (`!ContentFileUtil.isDV(file)`)。
3. V3/V4 约束: position delete 必须是 DV (`ContentFileUtil.isDV(file)`)。
4. 这在 `validateNewDeleteFile` 中执行,提交前即失败。

**答案(总):**

一锤定音: **跨版本迁移最危险的点不是语法,而是 row-level delete 介质约束不兼容。**

**追问与反问:**
- 追问: "为什么 V3 要强制 DV？"
- 回答: DV 使用 Roaring Bitmap 存储,相比传统 Position Delete 文件在存储和读取上有数量级的优势。强制 DV 确保 V3 表的删除操作始终使用最优介质,避免新旧 Position Delete 混用导致的复杂性。

---

## 源码校验修正汇总

本文档在对照源码验证后,主要修正和补充了以下内容:

### 修正项

1. **SnapshotSummary.UpdateMetrics**: 原文使用分离的 `addedFile(DataFile)` 和 `addedFile(DeleteFile)` 方法;实际源码使用统一的 `addedFile(ContentFile<?>)` 通过 `switch(file.content())` 分发,新增了 DV 追踪(`addedDVs`/`removedDVs`)、`trustSizeAndDeleteCounts` 标志位,以及使用 `ScanTaskUtil.contentSizeInBytes(file)` 而非 `file.fileSizeInBytes()`。

2. **updateTotal() 方法**: 原文缺少 `try-catch NumberFormatException` 保护,且未说明 `newTotal >= 0` 检查在**每一步**都执行(而非仅最终)。已修正。

3. **IcebergFilesCommitter.dataFilesPerCheckpoint**: 原文部分描述暗示存储 `WriteResult`;实际存储 `byte[]`(序列化后的 DeltaManifests)。`INITIAL_CHECKPOINT_ID = -1L` 和 `maxContinuousEmptyCommits` 机制已补充。

4. **DeleteFileIndex.forDataFile()**: 补充说明当找到 DV 时跳过 Position Delete 查找的逻辑。

5. **Position Delete 路由**: 补充 `ContentFileUtil.referencedDataFileLocation()` 用于判断 file-scoped 并路由到 `posDeletesByPath` vs `posDeletesByPartition`。

6. **DV 共存规则**: V3 表新写入 Position Delete **必须**使用 DV(源码强制校验),而非可选。

### 补充项

1. SnapshotSummary 中 DV 相关统计属性: `ADDED_DVS_PROP`、`REMOVED_DVS_PROP`、`ADD_EQ_DELETE_FILES_PROP`、`REMOVED_EQ_DELETE_FILES_PROP` 等细分属性。
2. `trustPartitionMetrics` 标志位及 `addedManifest()` 导致的分区统计清除。
3. `BaseDeleteLoader` 中 DV 不走缓存的原因(Puffin 读取模式、task locality 不保证)。
4. `continuousEmptyCheckpoints` 空 checkpoint 处理机制。

---

## 十二、Iceberg 元数据结构体系深度解析

> 基于 Apache Iceberg 源码 (main 分支) 的完整元数据结构分析。所有字段均从源码直接提取，覆盖 V1/V2/V3/V4 版本差异。

<a id="12-1"></a>
### 12.1 元数据整体架构 — 多级索引树

Iceberg 的元数据并非扁平的文件列表，而是精心设计的**多级树状索引体系**。自顶向下共 5 层，每一层都提供独立的剪枝能力：

```
TableMetadata (JSON 文件, 位于 metadata/ 目录)
 |
 |-- format-version          # 表格式版本 (1/2/3/4)
 |-- table-uuid              # 表唯一标识
 |-- location                # 表数据存储根路径
 |-- schemas[]               # Schema 历史列表
 |-- partition-specs[]       # 分区规范历史列表
 |-- sort-orders[]           # 排序规范历史列表
 |-- properties              # 表属性键值对
 |-- refs{}                  # 快照引用 (branch/tag)
 |-- current-snapshot-id     # 当前快照ID
 |
 +-- snapshots[]             # 快照列表
      |
      |-- snapshot-id         # 快照唯一ID
      |-- manifest-list       # 指向 Manifest List 文件 (Avro 格式)
      |
      +-- ManifestList        # 清单列表 (Avro 文件)
           |
           +-- ManifestFile[] # 清单文件条目列表
                |
                |-- manifest_path     # 指向 Manifest 文件 (Avro 格式)
                |-- partitions[]      # 分区字段摘要 (第一层剪枝)
                |
                +-- Manifest          # 清单文件 (Avro 文件)
                     |
                     +-- ManifestEntry[]  # 清单条目列表
                          |
                          |-- status          # EXISTING / ADDED / DELETED
                          |-- data_file       # 数据文件元数据
                          |    |
                          |    +-- ContentFile 字段
                          |         |-- file_path
                          |         |-- record_count
                          |         |-- column_sizes
                          |         |-- lower_bounds    # 第二层剪枝
                          |         |-- upper_bounds    # 第二层剪枝
                          |         |-- null_value_counts
                          |         |-- ...
                          |
                          +-- 实际数据文件 (Parquet/ORC/Avro)
```

**核心设计理念**: 通过多级索引,查询引擎在规划阶段逐层剪枝,**无需打开任何数据文件**即可排除绝大部分不相关文件,最终只读取真正需要的数据。

---

<a id="12-2"></a>
### 12.2 TableMetadata — 元数据根节点

**源码**: `core/src/main/java/org/apache/iceberg/TableMetadata.java` (第 243-273 行)
**序列化**: `core/src/main/java/org/apache/iceberg/TableMetadataParser.java`

#### 12.2.1 完整字段列表

| 字段名 | Java 类型 | JSON 键名 | 说明 |
|--------|----------|-----------|------|
| `formatVersion` | `int` | `format-version` | 表格式版本号, 当前支持 1/2/3/4 |
| `uuid` | `String` | `table-uuid` | 表的全局唯一标识符 (UUID), V2+ 必填 |
| `location` | `String` | `location` | 表数据存储的根路径 |
| `lastSequenceNumber` | `long` | `last-sequence-number` | 最近一次提交的序列号, V1 中固定为 0 |
| `lastUpdatedMillis` | `long` | `last-updated-ms` | 最近一次更新时间戳 (毫秒) |
| `lastColumnId` | `int` | `last-column-id` | 已分配的最大列 ID, schema evolution 时保证 ID 唯一性 |
| `currentSchemaId` | `int` | `current-schema-id` | 当前使用的 schema ID |
| `schemas` | `List<Schema>` | `schemas` | 所有历史 schema 的列表 (含当前) |
| `defaultSpecId` | `int` | `default-spec-id` | 当前默认的分区规范 ID |
| `specs` | `List<PartitionSpec>` | `partition-specs` | 所有历史分区规范列表 (含当前) |
| `lastAssignedPartitionId` | `int` | `last-partition-id` | 已分配的最大分区字段 ID |
| `defaultSortOrderId` | `int` | `default-sort-order-id` | 当前默认的排序规范 ID |
| `sortOrders` | `List<SortOrder>` | `sort-orders` | 所有历史排序规范列表 (含当前) |
| `properties` | `Map<String, String>` | `properties` | 表属性键值对 |
| `currentSnapshotId` | `long` | `current-snapshot-id` | 当前快照 ID, 无快照时 V1/V2 为 -1, V3+ 为 null |
| `snapshots` | `List<Snapshot>` | `snapshots` | 快照列表 (支持延迟加载) |
| `refs` | `Map<String, SnapshotRef>` | `refs` | 快照引用映射 (branch 和 tag) |
| `snapshotLog` | `List<HistoryEntry>` | `snapshot-log` | 快照变更历史日志 |
| `previousFiles` | `List<MetadataLogEntry>` | `metadata-log` | 历史元数据文件路径日志 |
| `statisticsFiles` | `List<StatisticsFile>` | `statistics` | Puffin 格式统计信息文件列表 |
| `partitionStatisticsFiles` | `List<PartitionStatisticsFile>` | `partition-statistics` | 分区统计信息文件列表 |
| `nextRowId` | `long` | `next-row-id` | 下一个可分配的行 ID (V3+, 用于 row lineage) |
| `encryptionKeys` | `List<EncryptedKey>` | `encryption-keys` | 加密密钥列表 (V4) |

#### 12.2.2 关键常量

```
INITIAL_SEQUENCE_NUMBER = 0        -- 初始序列号
INVALID_SEQUENCE_NUMBER = -1       -- 无效序列号标记
DEFAULT_TABLE_FORMAT_VERSION = 2   -- 新建表的默认版本
SUPPORTED_TABLE_FORMAT_VERSION = 4 -- 最高支持版本
MIN_FORMAT_VERSION_ROW_LINEAGE = 3 -- Row Lineage 最低要求版本
INITIAL_SPEC_ID = 0                -- 初始分区规范 ID
INITIAL_SORT_ORDER_ID = 1          -- 初始排序规范 ID
INITIAL_SCHEMA_ID = 0              -- 初始 Schema ID
```

#### 12.2.3 版本约束 (构造函数校验)

- **V1**: `uuid` 可选, `lastSequenceNumber` 必须为 0
- **V2+**: `uuid` 必填
- **V3+**: `next-row-id` 必须存在, `current-snapshot-id` 为 null (而非 -1)
- 序列化时 `last-sequence-number` 仅在 V2+ 写入

---

<a id="12-3"></a>
### 12.3 Schema — 表结构定义

**源码**: `api/src/main/java/org/apache/iceberg/Schema.java`

#### 12.3.1 Schema 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `schemaId` | `int` | Schema 的唯一 ID, 默认为 0 |
| `struct` | `Types.StructType` | 由 `NestedField` 列表组成的结构类型,即表的列定义 |
| `identifierFieldIds` | `int[]` | 标识符字段 ID 数组 (用于 upsert 场景的主键定义) |
| `highestFieldId` | `int` | 该 schema 中最大的字段 ID |

#### 12.3.2 NestedField (每个列的定义)

| 属性 | 类型 | 说明 |
|------|------|------|
| `id` | `int` | 全局唯一的字段 ID (**schema evolution 的核心**, 列重命名不变) |
| `name` | `String` | 字段名称 |
| `type` | `Type` | 字段数据类型 |
| `isOptional` | `boolean` | 是否可为 null |
| `doc` | `String` | 字段文档注释 |
| `initialDefault` | `Object` | 初始默认值 (V3+) |
| `writeDefault` | `Object` | 写入默认值 (V3+) |

#### 12.3.3 V3 新增类型

| 数据类型 | 最低版本 |
|---------|----------|
| `TIMESTAMP_NANO` | V3 |
| `VARIANT` | V3 |
| `GEOMETRY` | V3 |
| `GEOGRAPHY` | V3 |
| 字段默认值 | V3 |

---

<a id="12-4"></a>
### 12.4 PartitionSpec — 分区规范

**源码**: `api/src/main/java/org/apache/iceberg/PartitionSpec.java`

#### 12.4.1 字段定义

| 字段 | 类型 | 说明 |
|------|------|------|
| `specId` | `int` | 分区规范的唯一 ID |
| `fields` | `PartitionField[]` | 分区字段数组 |
| `lastAssignedFieldId` | `int` | 本规范中已分配的最大分区字段 ID |

#### 12.4.2 PartitionField (分区字段)

| 字段 | 类型 | 说明 |
|------|------|------|
| `sourceId` | `int` | 源列的字段 ID (指向 Schema 中的列) |
| `fieldId` | `int` | 分区字段自身的 ID (**从 1000 起始, 与数据列 ID 空间隔离**) |
| `name` | `String` | 分区字段名称 |
| `transform` | `Transform` | 分区转换函数 |

#### 12.4.3 支持的 Transform 类型

| Transform | 说明 | 示例 |
|-----------|------|------|
| `identity` | 原值分区 | `identity(region)` |
| `bucket[N]` | 哈希桶分区 | `bucket[16](user_id)` |
| `truncate[W]` | 截断分区 | `truncate[10](name)` |
| `year` | 按年分区 | `year(ts)` |
| `month` | 按月分区 | `month(ts)` |
| `day` | 按日分区 | `day(ts)` |
| `hour` | 按小时分区 | `hour(ts)` |
| `void` | 已废弃的分区字段 | 分区演化中移除字段时使用 |

**关键设计**: 分区字段 ID 从 1000 开始 (`PARTITION_DATA_ID_START = 1000`), 与数据列 ID 空间隔离, 避免冲突。

---

<a id="12-5"></a>
### 12.5 SortOrder — 排序规范

**源码**: `api/src/main/java/org/apache/iceberg/SortOrder.java`

| 字段 | 类型 | 说明 |
|------|------|------|
| `orderId` | `int` | 排序规范的唯一 ID (无排序时为 0) |
| `fields` | `SortField[]` | 排序字段数组 |

**SortField** 字段:

| 字段 | 类型 | 说明 |
|------|------|------|
| `sourceId` | `int` | 源列的字段 ID |
| `transform` | `Transform` | 排序前的转换函数 (可以是 identity) |
| `direction` | `SortDirection` | 排序方向: `ASC` 或 `DESC` |
| `nullOrder` | `NullOrder` | NULL 值排序位置: `NULLS_FIRST` 或 `NULLS_LAST` |

---

<a id="12-6"></a>
### 12.6 Snapshot — 快照

**接口**: `api/src/main/java/org/apache/iceberg/Snapshot.java`
**实现**: `core/src/main/java/org/apache/iceberg/BaseSnapshot.java`

#### 12.6.1 完整字段

| 字段 | 类型 | JSON 键名 | 说明 |
|------|------|-----------|------|
| `snapshotId` | `long` | `snapshot-id` | 快照的全局唯一 ID |
| `parentId` | `Long` | `parent-snapshot-id` | 父快照 ID (首个快照为 null) |
| `sequenceNumber` | `long` | `sequence-number` | 快照的序列号 (V2+, 提交时分配) |
| `timestampMillis` | `long` | `timestamp-ms` | 快照创建时间戳 |
| `manifestListLocation` | `String` | `manifest-list` | Manifest List 文件路径 |
| `operation` | `String` | 嵌入 `summary` | 操作类型 |
| `summary` | `Map<String, String>` | `summary` | 快照摘要 (操作类型 + 统计) |
| `schemaId` | `Integer` | `schema-id` | 创建快照时使用的 schema ID |
| `firstRowId` | `Long` | `first-row-id` | 首个新增行的行 ID (V3+) |
| `addedRows` | `Long` | `added-rows` | 分配了行 ID 的行数上界 (V3+) |

#### 12.6.2 操作类型 (DataOperations)

| 操作 | 值 | 说明 | 对应 API |
|------|-----|------|---------|
| append | `"append"` | 追加新数据, 不删除不替换 | `AppendFiles` |
| replace | `"replace"` | 替换文件但不改变数据 (如 compaction) | `RewriteFiles` |
| overwrite | `"overwrite"` | 用新数据覆盖现有数据 | `OverwriteFiles` |
| delete | `"delete"` | 仅删除数据, 不追加 | `DeleteFiles` |

#### 12.6.3 summary 常见统计条目

| 键 | 说明 |
|----|------|
| `operation` | 操作类型 (必填) |
| `added-data-files` | 新增数据文件数 |
| `deleted-data-files` | 删除数据文件数 |
| `added-delete-files` | 新增删除文件数 |
| `added-records` / `deleted-records` | 新增/删除记录数 |
| `added-files-size` / `removed-files-size` | 新增/移除文件大小 |
| `total-records` | 表总记录数 |
| `total-data-files` / `total-delete-files` | 表总数据/删除文件数 |

#### 12.6.4 Manifest 的惰性加载

`BaseSnapshot` 的设计:
1. 创建时**仅保存** `manifestListLocation` 路径,不读取文件
2. 首次调用 `allManifests(FileIO)` 时才读取 Manifest List
3. 读取后自动分类为 `dataManifests` 和 `deleteManifests`
4. 结果缓存在 transient 字段中,避免重复 I/O

---

<a id="12-7"></a>
### 12.7 SnapshotRef — 快照引用

**源码**: `api/src/main/java/org/apache/iceberg/SnapshotRef.java`

| 字段 | 类型 | 说明 |
|------|------|------|
| `snapshotId` | `long` | 引用指向的快照 ID |
| `type` | `SnapshotRefType` | `BRANCH` (可变, 新提交推进) 或 `TAG` (不可变, 固定快照) |
| `minSnapshotsToKeep` | `Integer` | 最少保留快照数 (仅 BRANCH) |
| `maxSnapshotAgeMs` | `Long` | 快照最大保留时间 (仅 BRANCH) |
| `maxRefAgeMs` | `Long` | 引用自身的最大存活时间 |

**主分支**: `SnapshotRef.MAIN_BRANCH = "main"`, 当 `refs` 字段不存在但 `current-snapshot-id` 有效时, 解析器自动初始化 main branch。

---

<a id="12-8"></a>
### 12.8 ManifestFile — 清单文件元数据

**接口**: `api/src/main/java/org/apache/iceberg/ManifestFile.java`

Manifest List 是 Avro 格式文件, 每行一个 `ManifestFile` 记录。

#### 12.8.1 完整字段 (含 Avro Field ID)

| 字段名 | Field ID | 类型 | 说明 |
|--------|----------|------|------|
| `manifest_path` | 500 | `string` (required) | Manifest 文件的完整 URI |
| `manifest_length` | 501 | `long` (required) | Manifest 文件大小 |
| `partition_spec_id` | 502 | `int` (required) | 写入时使用的分区规范 ID |
| `content` | 517 | `int` (optional) | 内容类型: 0=DATA, 1=DELETES (V2+) |
| `sequence_number` | 515 | `long` (optional) | 添加此 Manifest 的提交序列号 |
| `min_sequence_number` | 516 | `long` (optional) | 所有存活文件的最小数据序列号 |
| `added_snapshot_id` | 503 | `long` (required) | 添加此 Manifest 的快照 ID |
| `added_files_count` | 504 | `int` (optional) | ADDED 状态的文件数 |
| `existing_files_count` | 505 | `int` (optional) | EXISTING 状态的文件数 |
| `deleted_files_count` | 506 | `int` (optional) | DELETED 状态的文件数 |
| `added_rows_count` | 512 | `long` (optional) | ADDED 文件的总行数 |
| `existing_rows_count` | 513 | `long` (optional) | EXISTING 文件的总行数 |
| `deleted_rows_count` | 514 | `long` (optional) | DELETED 文件的总行数 |
| `partitions` | 507 | `list<PartitionFieldSummary>` | **每个分区字段的统计摘要** |
| `key_metadata` | 519 | `binary` (optional) | 加密密钥元数据 |
| `first_row_id` | 520 | `long` (optional) | ADDED 数据文件中起始行 ID (V3+) |

#### 12.8.2 PartitionFieldSummary (分区字段摘要 — 第一层剪枝的关键)

| 字段 | Field ID | 类型 | 说明 |
|------|----------|------|------|
| `contains_null` | 509 | `boolean` | 任一文件是否包含 null 分区值 |
| `contains_nan` | 518 | `boolean` | 任一文件是否包含 NaN 分区值 |
| `lower_bound` | 510 | `binary` | 所有文件的分区值**下界** |
| `upper_bound` | 511 | `binary` | 所有文件的分区值**上界** |

> **读取时作用**: 查询引擎拿到查询谓词后, 首先与 ManifestFile 的 `partitions` 中的 `lower_bound`/`upper_bound` 做范围比较。如果整个 Manifest 的分区范围与谓词不重叠, 直接跳过此 Manifest, **无需打开 Manifest 文件读取其中的条目**。这是最粗粒度的剪枝。

---

<a id="12-9"></a>
### 12.9 ManifestEntry — 清单条目

**源码**: `core/src/main/java/org/apache/iceberg/ManifestEntry.java`

每个 Manifest 文件 (Avro 格式) 中的一行记录:

| 字段 | Field ID | 类型 | 说明 |
|------|----------|------|------|
| `status` | 0 | `int` | 文件状态: EXISTING(0) / ADDED(1) / DELETED(2) |
| `snapshot_id` | 1 | `long` | 将文件添加到表中的快照 ID |
| `sequence_number` | 3 | `long` | 数据序列号 |
| `file_sequence_number` | 4 | `long` | 文件序列号 |
| `data_file` | 2 | `struct` | 嵌套的数据文件/删除文件元数据 (ContentFile) |

#### 两种序列号的语义差异

**data sequence number (数据序列号)**:
- 表示文件中数据**所属的逻辑时间点**
- compaction 新产生的文件保留原文件的数据序列号 (数据内容未变)
- 用于确定 equality delete 的生效范围: delete 仅对 `data_seq <= delete_seq` 的数据文件生效

**file sequence number (文件序列号)**:
- 表示文件被**物理添加到表中**的快照序列号
- commit 时自动分配, 不可手动指定
- compaction 中 `file_seq > data_seq` (新文件,旧数据)

---

<a id="12-10"></a>
### 12.10 ContentFile / DataFile / DeleteFile — 数据文件元数据

**ContentFile**: `api/src/main/java/org/apache/iceberg/ContentFile.java`
**DataFile**: `api/src/main/java/org/apache/iceberg/DataFile.java`

#### 12.10.1 DataFile 的 Avro StructType 完整定义 (Field ID 100-145)

| 字段名 | Field ID | 类型 | 说明 |
|--------|----------|------|------|
| `content` | 134 | `int` | 内容类型: 0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES |
| `file_path` | 100 | `string` | 文件完整 URI |
| `file_format` | 101 | `string` | 文件格式: avro/orc/parquet |
| `partition` | 102 | `struct` | 分区数据元组 |
| `record_count` | 103 | `long` | 文件中的记录数 |
| `file_size_in_bytes` | 104 | `long` | 文件总大小 |
| `column_sizes` | 108 | `map<int, long>` | **列 ID -> 列在磁盘上的大小** |
| `value_counts` | 109 | `map<int, long>` | **列 ID -> 值总数** (含 null 和 NaN) |
| `null_value_counts` | 110 | `map<int, long>` | **列 ID -> null 值数量** |
| `nan_value_counts` | 137 | `map<int, long>` | **列 ID -> NaN 值数量** |
| `lower_bounds` | 125 | `map<int, binary>` | **列 ID -> 列值下界** (第二层剪枝) |
| `upper_bounds` | 128 | `map<int, binary>` | **列 ID -> 列值上界** (第二层剪枝) |
| `key_metadata` | 131 | `binary` | 加密密钥元数据 |
| `split_offsets` | 132 | `list<long>` | 推荐分片偏移量 |
| `equality_ids` | 135 | `list<int>` | 等值比较字段 ID 列表 (equality delete 专用) |
| `sort_order_id` | 140 | `int` | 排序规范 ID |
| `spec_id` | 141 | `int` | 分区规范 ID |
| `first_row_id` | 142 | `long` | 首行的行 ID (V3+) |
| `referenced_data_file` | 143 | `string` | Deletion Vector 引用的数据文件路径 (V3+) |
| `content_offset` | 144 | `long` | Puffin 文件中 DV blob 的起始偏移 (V3+) |
| `content_size_in_bytes` | 145 | `long` | DV blob 的大小 (V3+) |

#### 12.10.2 统计信息字段详解

**lower_bounds / upper_bounds**:
- 键为列 ID, 值为 Iceberg 内部二进制序列化格式
- 序列化规则: `int`/`long` 小端字节序, `string` UTF-8 (可截断到前 16 字节), `timestamp` 以 microseconds 的 long 值, `decimal` 大端无缩放整数
- **截断优化**: bounds 值可截断以节省空间, 截断后 lower_bound 向下取整, upper_bound 向上取整, 保证过滤正确性
- **读取时作用**: 查询引擎用谓词与每个文件的 bounds 比较, 例如 `WHERE ts > '2024-01-01'` 时若某文件 `upper_bound(ts) < '2024-01-01'`, 直接跳过

**column_sizes**:
- 表示该列在磁盘上编码后的大小 (字节)
- **读取时作用**: 查询引擎据此估算读取特定列的 I/O 成本, 辅助代价估计 (cost-based optimization)

**value_counts / null_value_counts / nan_value_counts**:
- `value_counts` 包含所有值 (含 null 和 NaN)
- 引擎可用 `value_counts - null_value_counts` 得到非 null 值计数
- **全 null 列优化**: 若 `null_value_counts == value_counts`, 该列全为 null, 含 `IS NOT NULL` 谓词时可直接跳过此文件

---

<a id="12-11"></a>
### 12.11 StatisticsFile 与 PartitionStatisticsFile

**StatisticsFile**: `api/src/main/java/org/apache/iceberg/StatisticsFile.java`

统计信息文件使用 **Puffin 格式**存储,包含如 NDV (Number of Distinct Values) 等高级统计数据。

| 字段 | JSON 键名 | 说明 |
|------|-----------|------|
| `snapshotId` | `snapshot-id` | 关联的快照 ID |
| `path` | `statistics-path` | 统计文件的完整路径 |
| `fileSizeInBytes` | `file-size-in-bytes` | 文件大小 |
| `fileFooterSizeInBytes` | `file-footer-size-in-bytes` | Puffin footer 大小 |
| `blobMetadata` | `blob-metadata` | 统计 blob 列表 |

**BlobMetadata** 字段:

| 字段 | 说明 |
|------|------|
| `type` | Blob 类型 (如 `"apache-datasketches-theta-v1"`) |
| `sourceSnapshotId` | 计算该 blob 时表的快照 ID |
| `fields` | 计算该 blob 使用的列字段 ID 列表 |
| `properties` | 附加属性 (如 `{"ndv": "95000"}`) |

**PartitionStatisticsFile**: 存储分区级别的聚合统计, 如每个分区的文件数量、记录数、文件大小等。

---

<a id="12-12"></a>
### 12.12 版本差异对比矩阵 (V1/V2/V3/V4)

| 特性 | V1 | V2 | V3 | V4 |
|------|----|----|----|----|
| 序列号 | 不支持 (固定 0) | 支持 | 支持 | 支持 |
| UUID | 可选 | 必填 | 必填 | 必填 |
| Delete 文件 | 不支持 | position + equality | 支持 | 支持 |
| ManifestContent 字段 | 不存在 | DATA/DELETES | DATA/DELETES | DATA/DELETES |
| current-snapshot-id (无快照) | -1 | -1 | null | null |
| Row Lineage (next-row-id) | 不支持 | 不支持 | 支持 | 支持 |
| TIMESTAMP_NANO / VARIANT 等 | 不支持 | 不支持 | 支持 | 支持 |
| 字段默认值 | 不支持 | 不支持 | 支持 | 支持 |
| Deletion Vector | 不支持 | 不支持 | 支持 | 支持 |
| 加密密钥 | 不支持 | 不支持 | 不支持 | 支持 |
| V1 兼容写入 | 同时写 `schema`+`schemas` | 仅 `schemas` | 仅 `schemas` | 仅 `schemas` |

---

<a id="12-13"></a>
### 12.13 元数据在读写场景中的作用

#### 12.13.1 写入流程

```
数据写入 (如 AppendFiles)
    |
    v
1. 写入数据文件 (Parquet/ORC/Avro)
   - 写入过程中收集列级统计:
     column_sizes, value_counts, null_value_counts,
     nan_value_counts, lower_bounds, upper_bounds
   - 记录 record_count, file_size_in_bytes
   - 计算 split_offsets
    |
    v
2. 创建 ManifestEntry (status=ADDED)
   - 填入所有 ContentFile 元数据字段
   - snapshot_id 设为当前快照
    |
    v
3. 写入 Manifest 文件 (Avro 格式)
   - 包含新增的 ManifestEntry 条目
   - 可能继承/合并已有 Manifest
   - 计算 PartitionFieldSummary (分区字段摘要)
   - 统计 added_files_count, added_rows_count 等
    |
    v
4. 写入 Manifest List 文件 (Avro 格式)
   - 包含所有活跃的 ManifestFile 条目
   - 新增 Manifest + 继承的旧 Manifest
    |
    v
5. 创建 Snapshot
   - 分配 snapshot-id, sequence-number
   - 指向新的 Manifest List
   - 计算 summary 统计信息
    |
    v
6. 更新 TableMetadata
   - 添加新 Snapshot 到 snapshots 列表
   - 更新 current-snapshot-id
   - 更新 refs (推进 main branch)
   - 递增 last-sequence-number
   - 更新 last-updated-ms
    |
    v
7. 原子提交 (写入新的 metadata JSON 文件)
   - 乐观并发控制: 比较 base metadata, 冲突时重试
```

#### 12.13.2 读取流程 (查询规划与多层剪枝)

```
查询: SELECT * FROM t WHERE ts > '2024-01-01' AND region = 'US'
    |
    v
1. 读取 TableMetadata
   - 获取 current-snapshot-id (或 time travel 指定历史快照)
   - 获取当前 schema, partition-spec
    |
    v
2. 读取当前 Snapshot
   - 获取 manifest-list 路径
    |
    v
3. 读取 Manifest List → ManifestFile 列表
   ╔═══════════════════════════════════════════╗
   ║  第一层剪枝: Manifest 级别               ║
   ║  - content 字段: 仅读取 DATA manifest    ║
   ║  - partitions[] 的 lower_bound/upper_bound║
   ║    与谓词做范围比较:                      ║
   ║    * day(ts) 的 bounds 是否与              ║
   ║      ts > '2024-01-01' 重叠?              ║
   ║    * identity(region) 的 bounds           ║
   ║      是否包含 'US'?                       ║
   ║  → 跳过不满足条件的整个 Manifest          ║
   ╚═══════════════════════════════════════════╝
    |
    v
4. 读取匹配的 Manifest → ManifestEntry 列表
   ╔═══════════════════════════════════════════╗
   ║  第二层剪枝: 文件级别                    ║
   ║  - 检查 status: 仅关注 ADDED/EXISTING    ║
   ║  - 检查 data_file 的 partition 值:        ║
   ║    精确匹配分区谓词                       ║
   ║  - 检查 lower_bounds/upper_bounds:        ║
   ║    * ts 的 upper_bound < '2024-01-01'     ║
   ║      → 跳过此文件                        ║
   ║    * region 的 bounds 不含 'US'           ║
   ║      → 跳过此文件                        ║
   ║  - 检查 null_value_counts:                ║
   ║    * IS NOT NULL 且列全 null → 跳过       ║
   ╚═══════════════════════════════════════════╝
    |
    v
5. 生成 ScanTask 列表
   - 利用 split_offsets 将大文件拆分
   - 利用 file_size_in_bytes, column_sizes 估算 I/O
   - 关联对应的 delete 文件 (通过 sequence number 匹配)
    |
    v
6. 执行数据读取
   - 读取实际数据文件
   - 应用 delete 文件 (position deletes / equality deletes)
```

#### 12.13.3 Compaction / Maintenance 流程

```
Compaction (RewriteFiles) 流程:
  1. 扫描小文件: 利用 file_size_in_bytes 判断是否需要合并
  2. 合并写入: 新文件的 data_sequence_number 取原文件的最小值
     (逻辑上数据仍属于原始时间点, 保证 delete 文件语义不变)
  3. 创建 Snapshot (operation = "replace"):
     旧文件标记 DELETED, 新文件标记 ADDED

Snapshot 过期 (ExpireSnapshots) 流程:
  1. 确定可过期快照: 考虑 SnapshotRef 保留策略
  2. 清理文件: 比较新旧 Manifest List, 找出无引用的文件
  3. 更新 TableMetadata: 移除过期快照, 更新 snapshot-log
```

---

<a id="12-14"></a>
### 12.14 完整 TableMetadata JSON 示例 (V2 格式)

以下结构严格对照 `TableMetadataParser.toJson()` 的序列化顺序:

```json
{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://warehouse/db/sample_table",
  "last-sequence-number": 5,
  "last-updated-ms": 1710000000000,
  "last-column-id": 5,

  "current-schema-id": 0,
  "schemas": [
    {
      "schema-id": 0,
      "type": "struct",
      "fields": [
        {"id": 1, "name": "id", "required": true, "type": "long"},
        {"id": 2, "name": "data", "required": false, "type": "string"},
        {"id": 3, "name": "ts", "required": true, "type": "timestamptz"},
        {"id": 4, "name": "category", "required": false, "type": "string"},
        {"id": 5, "name": "amount", "required": false, "type": "double"}
      ],
      "identifier-field-ids": [1]
    }
  ],

  "default-spec-id": 0,
  "partition-specs": [
    {
      "spec-id": 0,
      "fields": [
        {"source-id": 3, "field-id": 1000, "name": "ts_day", "transform": "day"},
        {"source-id": 4, "field-id": 1001, "name": "category", "transform": "identity"}
      ]
    }
  ],

  "last-partition-id": 1001,

  "default-sort-order-id": 1,
  "sort-orders": [
    {"order-id": 0, "fields": []},
    {
      "order-id": 1,
      "fields": [
        {"source-id": 3, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}
      ]
    }
  ],

  "properties": {
    "write.parquet.compression-codec": "zstd",
    "commit.retry.num-retries": "4",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "10"
  },

  "current-snapshot-id": 3497810964824022504,

  "refs": {
    "main": {
      "snapshot-id": 3497810964824022504,
      "type": "branch"
    },
    "audit-2024-q1": {
      "snapshot-id": 3497810964824022501,
      "type": "tag",
      "max-ref-age-ms": 15552000000
    }
  },

  "snapshots": [
    {
      "snapshot-id": 3497810964824022501,
      "timestamp-ms": 1709000000000,
      "summary": {
        "operation": "append",
        "added-data-files": "5",
        "added-records": "100000",
        "added-files-size": "31457280",
        "total-records": "100000",
        "total-files-size": "31457280",
        "total-data-files": "5"
      },
      "manifest-list": "s3://warehouse/db/sample_table/metadata/snap-3497810964824022501-0-abc.avro",
      "schema-id": 0
    },
    {
      "sequence-number": 3,
      "snapshot-id": 3497810964824022502,
      "parent-snapshot-id": 3497810964824022501,
      "timestamp-ms": 1709500000000,
      "summary": {
        "operation": "append",
        "added-data-files": "3",
        "added-records": "50000",
        "total-records": "150000",
        "total-data-files": "8"
      },
      "manifest-list": "s3://warehouse/db/sample_table/metadata/snap-3497810964824022502-0-def.avro",
      "schema-id": 0
    },
    {
      "sequence-number": 5,
      "snapshot-id": 3497810964824022504,
      "parent-snapshot-id": 3497810964824022502,
      "timestamp-ms": 1710000000000,
      "summary": {
        "operation": "replace",
        "added-data-files": "2",
        "deleted-data-files": "8",
        "added-records": "150000",
        "deleted-records": "150000",
        "total-records": "150000",
        "total-data-files": "2"
      },
      "manifest-list": "s3://warehouse/db/sample_table/metadata/snap-3497810964824022504-0-ghi.avro",
      "schema-id": 0
    }
  ],

  "statistics": [
    {
      "snapshot-id": 3497810964824022504,
      "statistics-path": "s3://warehouse/db/sample_table/metadata/stats-3497810964824022504.puffin",
      "file-size-in-bytes": 51200,
      "file-footer-size-in-bytes": 256,
      "blob-metadata": [
        {
          "type": "apache-datasketches-theta-v1",
          "snapshot-id": 3497810964824022504,
          "sequence-number": 5,
          "fields": [1],
          "properties": {"ndv": "95000"}
        }
      ]
    }
  ],

  "partition-statistics": [
    {
      "snapshot-id": 3497810964824022504,
      "statistics-path": "s3://warehouse/db/sample_table/metadata/partition-stats.parquet",
      "file-size-in-bytes": 10240
    }
  ],

  "snapshot-log": [
    {"timestamp-ms": 1709000000000, "snapshot-id": 3497810964824022501},
    {"timestamp-ms": 1709500000000, "snapshot-id": 3497810964824022502},
    {"timestamp-ms": 1710000000000, "snapshot-id": 3497810964824022504}
  ],

  "metadata-log": [
    {"timestamp-ms": 1709000000000, "metadata-file": "s3://warehouse/db/sample_table/metadata/00000-uuid-1.metadata.json"},
    {"timestamp-ms": 1709500000000, "metadata-file": "s3://warehouse/db/sample_table/metadata/00001-uuid-2.metadata.json"}
  ]
}
```

#### V3 格式差异示例

V3 在 V2 基础上新增:

```json
{
  "format-version": 3,
  "current-snapshot-id": null,
  "next-row-id": 150000,
  "snapshots": [
    {
      "sequence-number": 5,
      "snapshot-id": 3497810964824022504,
      "first-row-id": 100000,
      "added-rows": 50000,
      "...其余字段与 V2 相同..."
    }
  ]
}
```

---

<a id="12-15"></a>
### 12.15 五大用户特性的元数据架构实现原理

> 结合 Iceberg 官方提出的五大用户体验特性, 分析它们各自是如何基于上述元数据架构实现的。

#### 12.15.1 Schema Evolution — 无副作用的表结构演进

**用户体验**: 支持 add, drop, update, rename 列操作, 不会意外恢复已删除的数据。

**元数据实现原理**:

Iceberg schema evolution 的核心在于**列 ID (field ID) 而非列名**。这一设计贯穿了从 `TableMetadata` 到 `DataFile` 的整个元数据体系:

1. **Schema 中的 `NestedField.id`**: 每个列被分配一个全局唯一的整数 ID, 列重命名只改 `name` 不改 `id`。`TableMetadata.lastColumnId` 保证 ID 只增不减, 因此**删除列后重新添加同名列会得到不同的 ID**。

2. **数据文件中的统计信息以 ID 为键**: `lower_bounds`、`upper_bounds`、`column_sizes` 等映射的键都是 `int` 类型的列 ID, 不是列名。这意味着:
   - 删除列 `status` (id=5) 后, 所有历史文件中 id=5 的统计信息**仍然有效但不再被查询**
   - 新增同名列 `status` 获得 id=8, 与历史数据完全隔离

3. **schemas 列表保存完整历史**: `TableMetadata.schemas[]` 记录了所有历史版本的 schema。每个 `Snapshot.schemaId` 记录了写入时使用的 schema 版本。读取历史快照时, 引擎用该 schemaId 找到正确的 schema 进行数据解释。

4. **不会 un-delete 数据**: 因为列 ID 是单调递增的 (`lastColumnId` 只增不减), 即使删除一列再添加同名列, 旧文件中该列的数据不会被新 schema 重新引用。这从根本上避免了 Hive 等系统中"添加同名列导致已删除数据复活"的问题。

```
Schema V0: {id:1 "user_id", id:2 "status", id:3 "ts"}
  ↓ DROP COLUMN status
Schema V1: {id:1 "user_id", id:3 "ts"}        lastColumnId=3
  ↓ ADD COLUMN status
Schema V2: {id:1 "user_id", id:3 "ts", id:4 "status"}  lastColumnId=4
  → id:4 ≠ id:2, 旧文件中 id:2 的数据永远不会被 id:4 读到
```

#### 12.15.2 Hidden Partitioning — 对用户透明的分区

**用户体验**: 用户无需了解分区细节即可获得快速查询, 避免因分区使用错误导致的静默错误结果或极慢查询。

**元数据实现原理**:

传统 Hive 分区要求用户显式指定分区列并在查询中使用分区列值进行过滤。Iceberg 通过 `PartitionSpec` 中的 **Transform** 机制将分区逻辑与用户查询解耦:

1. **PartitionSpec.fields 中的 Transform**: 分区字段不是数据列本身, 而是通过 transform 函数从源列派生:
   ```
   源列 ts (timestamptz) → transform: day → 分区字段 ts_day
   ```
   用户查询 `WHERE ts > '2024-01-01'` 时, 引擎**自动将谓词映射到分区字段**: `ts_day >= day('2024-01-01')`。

2. **ManifestFile.partitions[] 的 bounds**: 查询引擎将转换后的谓词与 Manifest 级别的分区摘要 (`lower_bound`/`upper_bound`) 比较, 实现剪枝。这一过程完全发生在引擎内部, 用户只需写基于源列的谓词。

3. **DataFile.partition 元组**: 每个数据文件记录了自己的分区值元组, 引擎在文件级别再做一次精确匹配。

4. **分区字段 ID 空间隔离**: 分区字段 ID 从 1000 起始, 与数据列 ID (从 1 起) 完全隔离。这是一个实现细节, 但保证了分区演化不会与 schema evolution 冲突。

**为什么能防止"静默错误"**: Hive 中如果用户忘记用分区列过滤, 查询会扫描全表但仍返回"正确"结果, 只是极慢。如果用户用错误的格式过滤分区列 (如 `month = 'January'` 但实际分区值是 `1`), 会得到空结果。Iceberg 中用户根本不操作分区列, 永远只用源列, 所以不存在这些问题。

#### 12.15.3 Partition Layout Evolution — 分区布局演化

**用户体验**: 当数据量或查询模式变化时, 可以更新表的分区布局, 无需重写已有数据。

**元数据实现原理**:

1. **TableMetadata.specs[] 保存完整历史**: 所有历史分区规范都保留在 `partition-specs[]` 列表中, `default-spec-id` 指向当前使用的规范。新数据使用新规范, 旧数据保持原有分区。

2. **DataFile.spec_id 与 ManifestFile.partition_spec_id**: 每个数据文件和 Manifest 文件都记录了写入时使用的分区规范 ID。读取时, 引擎根据 `spec_id` 找到对应的 PartitionSpec, 正确解释该文件的 partition 元组。

3. **void Transform**: 当移除某个分区字段时, 该字段的 transform 被设置为 `void`, 表示"已废弃"。旧文件中该字段的值仍存在但被忽略。

4. **查询引擎如何处理多规范**: 查询规划时, 引擎遍历所有 ManifestFile, 对每个 Manifest 根据其 `partition_spec_id` 选择对应的规范来解释 `partitions[]` 摘要。不同规范的文件可以在同一次查询中共存。

```
初始: spec-0 = day(ts)
  → 所有文件的 partition_spec_id = 0

演化后: spec-1 = hour(ts)
  → 新写入文件的 partition_spec_id = 1
  → 旧文件仍然是 partition_spec_id = 0, 数据不变
  → 查询时引擎同时处理两种规范的文件
```

#### 12.15.4 Time Travel — 时间旅行查询

**用户体验**: 支持使用完全相同的表快照进行可复现的查询, 或让用户轻松查看数据变更。

**元数据实现原理**:

1. **快照链**: `Snapshot.parentId` 形成一条从当前快照到初始快照的链。每个快照是一个完整的、不可变的表状态视图。

2. **TableMetadata.snapshots[]**: 保存了所有未过期的快照列表。通过 `snapshot-id` 或 `timestamp-ms` 可以定位到任意历史快照。

3. **Manifest List 的不可变性**: 每个 `Snapshot.manifestListLocation` 指向一个不可变的 Manifest List 文件。该文件列出了该时间点所有有效的 Manifest, 进而包含了所有有效的数据文件。因此, **读取一个历史快照等价于读取其 Manifest List 中引用的所有数据文件**。

4. **snapshot-log**: `TableMetadata.snapshotLog[]` 按时间序记录了每次快照变更, 支持 `TIMESTAMP AS OF` 语法 — 引擎在 log 中二分查找对应时间点的快照 ID。

5. **refs (tag)**: 通过 tag 可以给特定快照命名 (如 `audit-2024-q1`), 方便反复查询同一历史状态。tag 引用的快照不会被 `ExpireSnapshots` 清理 (在 `maxRefAgeMs` 内)。

6. **metadata-log**: 记录了历史元数据文件的路径。即使当前 TableMetadata JSON 被更新, 旧的 metadata JSON 仍保留在存储系统中, 可用于极端场景下的恢复。

```
当前 metadata JSON
  └── current-snapshot-id: snap-5
      └── manifest-list → snap-5 的文件集合

Time Travel: TIMESTAMP AS OF '2024-01-15'
  → 在 snapshot-log 中找到 timestamp ≤ '2024-01-15' 的最近 snapshot-id
  → 使用该 snapshot 的 manifest-list 读取该时间点的数据
```

#### 12.15.5 Version Rollback — 版本回滚

**用户体验**: 允许用户将表快速重置到一个已知的良好状态。

**元数据实现原理**:

1. **回滚 = 修改 current-snapshot-id**: 回滚操作本质上只需将 `TableMetadata.currentSnapshotId` 指向一个历史快照 ID, 并更新 `refs.main` 的 `snapshot-id`。

2. **原子性保证**: 回滚通过 `TableOperations.commit()` 提交, 遵循标准的乐观并发控制。整个过程只写一个新的 metadata JSON 文件, 修改的就是 `current-snapshot-id` 和 `refs`。

3. **数据文件不变**: 回滚不删除、不移动任何数据文件。旧快照引用的 Manifest List 和数据文件在存储系统中**一直存在** (只要未被 ExpireSnapshots 清理)。因此回滚是 O(1) 操作, 只涉及一次元数据文件写入。

4. **与快照保留策略的关系**: `SnapshotRef.minSnapshotsToKeep` 和 `maxSnapshotAgeMs` 控制了可以回滚到多远的历史。如果快照已过期并被清理, 则无法回滚到该时间点。

```
回滚前:
  metadata-v5.json → current-snapshot-id: snap-5 (有问题的数据)

回滚操作: ROLLBACK TO SNAPSHOT snap-3

回滚后:
  metadata-v6.json → current-snapshot-id: snap-3 (良好的状态)
  → snap-4, snap-5 的数据文件仍在磁盘上
  → 但当前查询只看到 snap-3 的数据
  → 后续 ExpireSnapshots 可以清理 snap-4, snap-5
```

---

<a id="12-16"></a>
### 12.16 元数据层级关系总图

```
+-----------------------------------------------------------------------+
|                         TableMetadata (JSON)                          |
|  format-version, uuid, location, properties, current-snapshot-id      |
|-----------------------------------------------------------------------|
|  schemas[]          | partition-specs[]    | sort-orders[]             |
|  (Schema 演化历史)  | (分区规范演化历史)   | (排序规范演化历史)        |
|-----------------------------------------------------------------------|
|  refs{}                                                               |
|  (branch/tag → snapshot-id 映射)                                      |
|-----------------------------------------------------------------------|
|  statistics[]                | partition-statistics[]                  |
|  (Puffin 统计文件)           | (分区统计文件)                          |
|-----------------------------------------------------------------------|
|  snapshot-log[]              | metadata-log[]                          |
|  (快照变更历史)              | (元数据文件变更历史)                    |
|-----------------------------------------------------------------------|
|  snapshots[]                                                          |
|  +---------------------------------------------------------------+   |
|  | Snapshot                                                       |   |
|  | snapshot-id, parent-snapshot-id, sequence-number, timestamp-ms |   |
|  | operation, summary{}, schema-id, first-row-id, added-rows     |   |
|  | manifest-list → 指向 Manifest List 文件                       |   |
|  +---------------------------------------------------------------+   |
+-----------------------------------------------------------------------+
          |
          | manifest-list (Avro 文件路径)
          v
+-----------------------------------------------------------------------+
|                    Manifest List (Avro 文件)                          |
|  每行一个 ManifestFile 记录                                           |
|-----------------------------------------------------------------------|
|  ManifestFile                                                         |
|  +---------------------------------------------------------------+   |
|  | manifest_path, manifest_length, partition_spec_id              |   |
|  | content (DATA/DELETES), sequence_number, min_sequence_number   |   |
|  | added_snapshot_id                                              |   |
|  | added_files_count, existing_files_count, deleted_files_count   |   |
|  | added_rows_count, existing_rows_count, deleted_rows_count     |   |
|  | partitions[] (PartitionFieldSummary) ← 第一层剪枝             |   |
|  |   contains_null, contains_nan, lower_bound, upper_bound       |   |
|  +---------------------------------------------------------------+   |
+-----------------------------------------------------------------------+
          |
          | manifest_path (Avro 文件路径)
          v
+-----------------------------------------------------------------------+
|                    Manifest (Avro 文件)                               |
|  每行一个 ManifestEntry 记录                                          |
|-----------------------------------------------------------------------|
|  ManifestEntry                                                        |
|  +---------------------------------------------------------------+   |
|  | status (EXISTING=0 / ADDED=1 / DELETED=2)                     |   |
|  | snapshot_id, sequence_number, file_sequence_number             |   |
|  |                                                               |   |
|  | data_file (ContentFile):                                       |   |
|  |   content, file_path, file_format, partition                  |   |
|  |   record_count, file_size_in_bytes                            |   |
|  |   column_sizes, value_counts, null_value_counts               |   |
|  |   nan_value_counts                                            |   |
|  |   lower_bounds, upper_bounds  ← 第二层剪枝                   |   |
|  |   split_offsets, equality_ids, sort_order_id, spec_id         |   |
|  |   first_row_id (V3+)                                          |   |
|  |   referenced_data_file, content_offset (V3+ DV)               |   |
|  +---------------------------------------------------------------+   |
+-----------------------------------------------------------------------+
          |
          | file_path
          v
+-----------------------------------------------------------------------+
|                实际数据文件 (Parquet / ORC / Avro)                    |
+-----------------------------------------------------------------------+
```

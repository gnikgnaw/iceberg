# Apache Iceberg 表扫描规划（Scan Planning）与谓词下推（Predicate Pushdown）深度源码分析

## 目录

- [1. 概述](#1-概述)
- [2. 表扫描接口体系](#2-表扫描接口体系)
  - [2.1 Scan 顶层接口](#21-scan-顶层接口)
  - [2.2 TableScan 与 DataTableScan](#22-tablescan-与-datatableScan)
  - [2.3 扫描继承链](#23-扫描继承链)
- [3. 三层过滤架构](#3-三层过滤架构)
  - [3.1 第一层：Manifest List 过滤](#31-第一层manifest-list-过滤)
  - [3.2 第二层：Manifest File 内分区过滤](#32-第二层manifest-file-内分区过滤)
  - [3.3 第三层：Data File 统计信息过滤](#33-第三层data-file-统计信息过滤)
- [4. Expression 表达式体系](#4-expression-表达式体系)
  - [4.1 Expression 接口与操作枚举](#41-expression-接口与操作枚举)
  - [4.2 Expressions 工厂](#42-expressions-工厂)
  - [4.3 UnboundPredicate 与 BoundPredicate](#43-unboundpredicate-与-boundpredicate)
  - [4.4 Binder：表达式绑定](#44-binder表达式绑定)
- [5. ManifestGroup：扫描规划引擎](#5-manifestgroup扫描规划引擎)
  - [5.1 核心架构](#51-核心架构)
  - [5.2 Manifest 级别过滤](#52-manifest-级别过滤)
  - [5.3 数据文件级别过滤](#53-数据文件级别过滤)
  - [5.4 并行扫描规划](#54-并行扫描规划)
- [6. ManifestEvaluator：Manifest 分区摘要过滤](#6-manifestevaluatormanifest-分区摘要过滤)
- [7. InclusiveMetricsEvaluator：列统计信息过滤](#7-inclusivemetricsevaluator列统计信息过滤)
  - [7.1 工作原理](#71-工作原理)
  - [7.2 核心过滤逻辑](#72-核心过滤逻辑)
  - [7.3 IN 谓词优化](#73-in-谓词优化)
- [8. StrictMetricsEvaluator vs InclusiveMetricsEvaluator](#8-strictmetricsevaluator-vs-inclusivemetricsevaluator)
  - [8.1 语义差异](#81-语义差异)
  - [8.2 使用场景对比](#82-使用场景对比)
- [9. Projections：表达式投影](#9-projections表达式投影)
  - [9.1 Inclusive Projection](#91-inclusive-projection)
  - [9.2 Strict Projection](#92-strict-projection)
- [10. ResidualEvaluator：残留谓词计算](#10-residualevaluator残留谓词计算)
  - [10.1 核心概念](#101-核心概念)
  - [10.2 实现原理](#102-实现原理)
- [11. 引擎集成：SQL WHERE 到 Iceberg Expression](#11-引擎集成sql-where-到-iceberg-expression)
  - [11.1 Spark 集成](#111-spark-集成)
  - [11.2 Flink 集成](#112-flink-集成)
- [12. 列投影（Column Projection）对性能的影响](#12-列投影column-projection对性能的影响)
- [13. 实际查询场景分析](#13-实际查询场景分析)
- [14. 与 Hive / 传统数据湖扫描方式对比](#14-与-hive--传统数据湖扫描方式对比)
- [15. 总结](#15-总结)

---

## 1. 概述

Apache Iceberg 的表扫描规划（Scan Planning）是其高性能查询的核心机制之一。与传统 Hive 数据湖基于目录遍历的方式不同，Iceberg 通过**元数据驱动的三层过滤架构**，在不读取任何实际数据的情况下，精确裁剪出需要读取的最小文件集合。

谓词下推（Predicate Pushdown）贯穿整个扫描过程，从查询引擎传入的 SQL WHERE 条件，经过表达式转换、分区投影、Manifest 过滤、文件统计信息过滤，最终将无法完全消解的残留谓词下推到 Reader 层做行级过滤。

本文基于 Iceberg 最新源码，深入分析整个扫描规划与谓词下推的完整链路。

---

## 2. 表扫描接口体系

### 2.1 Scan 顶层接口

`Scan` 是 Iceberg 扫描体系的顶层接口，定义了扫描配置的核心方法。

**文件位置：** `api/src/main/java/org/apache/iceberg/Scan.java`

```java
// Scan.java 第38行
public interface Scan<ThisT, T extends ScanTask, G extends ScanTaskGroup<T>> {
  ThisT option(String property, String value);   // 配置覆盖
  ThisT project(Schema schema);                   // 设置投影 Schema
  ThisT select(Collection<String> columns);       // 选择列
  ThisT filter(Expression expr);                  // 设置过滤表达式
  ThisT ignoreResiduals();                        // 忽略残留谓词
  ThisT planWith(ExecutorService executorService); // 并行规划
  CloseableIterable<T> planFiles();               // 文件级规划
  CloseableIterable<G> planTasks();               // 任务级规划
}
```

关键设计：**Scan 对象是不可变的（immutable）**，每个配置方法都返回新的 Scan 实例，因此线程安全且可在多线程间共享。

### 2.2 TableScan 与 DataTableScan

`TableScan` 扩展了 `Scan` 接口，增加了快照相关能力（time travel、增量扫描等）。

**文件位置：** `api/src/main/java/org/apache/iceberg/TableScan.java`

```java
// TableScan.java 第22行
public interface TableScan extends Scan<TableScan, FileScanTask, CombinedScanTask> {
  TableScan useSnapshot(long snapshotId);         // 指定快照
  TableScan useRef(String ref);                   // 使用分支/标签引用
  TableScan asOfTime(long timestampMillis);       // 时间旅行
  Snapshot snapshot();                            // 获取当前快照
}
```

`DataTableScan` 是用于扫描数据表的核心实现类，它的 `doPlanFiles()` 方法是扫描规划的入口。

**文件位置：** `core/src/main/java/org/apache/iceberg/DataTableScan.java`

```java
// DataTableScan.java 第64-92行
@Override
public CloseableIterable<FileScanTask> doPlanFiles() {
    Snapshot snapshot = snapshot();
    FileIO io = table().io();
    List<ManifestFile> dataManifests = snapshot.dataManifests(io);
    List<ManifestFile> deleteManifests = snapshot.deleteManifests(io);

    ManifestGroup manifestGroup =
        new ManifestGroup(io, dataManifests, deleteManifests)
            .caseSensitive(isCaseSensitive())
            .select(scanColumns())
            .filterData(filter())        // ← 传入谓词
            .schemasById(schemas())
            .specsById(specs())
            .scanMetrics(scanMetrics())
            .ignoreDeleted()
            .columnsToKeepStats(columnsToKeepStats());

    if (shouldIgnoreResiduals()) {
        manifestGroup = manifestGroup.ignoreResiduals();
    }

    if (shouldPlanWithExecutor() && (dataManifests.size() > 1 || deleteManifests.size() > 1)) {
        manifestGroup = manifestGroup.planWith(planExecutor());
    }

    return manifestGroup.planFiles();
}
```

### 2.3 扫描继承链

完整的类继承关系如下：

```
Scan (接口)
  └── TableScan (接口)
        └── BaseTableScan (抽象类) extends SnapshotScan
              └── DataTableScan (实现类)

BaseScan (抽象类) implements Scan
  └── SnapshotScan (抽象类)
        └── BaseTableScan
```

- **`BaseScan`**（`core/.../BaseScan.java`）：管理扫描上下文（过滤条件、列选择、大小写敏感性等），实现 `filter()`, `select()`, `project()` 等方法。
- **`SnapshotScan`**（`core/.../SnapshotScan.java`）：实现快照相关的 `planFiles()` 方法，包括扫描指标收集和日志记录。它在 `planFiles()` 中调用子类的 `doPlanFiles()` 抽象方法。
- **`BaseTableScan`**（`core/.../BaseTableScan.java`）：实现 `planTasks()` 方法，将文件级别的 `FileScanTask` 切分和合并为平衡的 `CombinedScanTask`。

**小结：** Iceberg 的扫描接口设计遵循不可变模式，通过继承链实现了层层职责分离：顶层接口定义能力，BaseScan 管理上下文，SnapshotScan 处理快照生命周期，DataTableScan 实现具体的文件规划逻辑。

---

## 3. 三层过滤架构

Iceberg 的扫描规划采用经典的**漏斗式三层过滤**架构，从粗到细逐层裁剪：

```
用户查询条件 (Expression)
    │
    ▼
┌─────────────────────────────────────┐
│ 第一层：Manifest List 过滤          │ ← ManifestEvaluator
│ 利用 Manifest 的分区摘要(PartitionFieldSummary)  │
│ 跳过不可能包含匹配数据的 Manifest    │
└───────────────┬─────────────────────┘
                │
                ▼
┌─────────────────────────────────────┐
│ 第二层：Manifest File 内分区过滤    │ ← Evaluator (分区值)
│ 利用每个 DataFile 的 partition 值   │
│ 跳过不匹配分区条件的数据文件         │
└───────────────┬─────────────────────┘
                │
                ▼
┌─────────────────────────────────────┐
│ 第三层：Data File 统计信息过滤      │ ← InclusiveMetricsEvaluator
│ 利用 min/max/null_count/nan_count   │
│ 跳过不可能包含匹配行的数据文件       │
└───────────────┬─────────────────────┘
                │
                ▼
    剩余数据文件 → 交给 Reader 做行级过滤（Residual）
```

### 3.1 第一层：Manifest List 过滤

每个 Manifest 文件都携带了 `PartitionFieldSummary`，记录了该 Manifest 中所有数据文件的**分区字段汇总统计信息**（包含 `lowerBound`、`upperBound`、`containsNull`、`containsNaN`）。

在 `ManifestGroup.entries()` 方法中（`ManifestGroup.java` 第276-391行），首先构建 `ManifestEvaluator` 缓存：

```java
// ManifestGroup.java 第279-292行
LoadingCache<Integer, ManifestEvaluator> evalCache =
    specsById == null ? null :
    Caffeine.newBuilder().build(
        specId -> {
            PartitionSpec spec = specsById.get(specId);
            return ManifestEvaluator.forPartitionFilter(
                Expressions.and(
                    partitionFilter,
                    Projections.inclusive(spec, caseSensitive).project(dataFilter)),
                spec, caseSensitive);
        });
```

关键操作：**将行级别的 `dataFilter` 通过 `Projections.inclusive()` 投影为分区级别的表达式**，然后与用户显式指定的 `partitionFilter` 组合，用 `ManifestEvaluator` 对每个 Manifest 进行评估：

```java
// ManifestGroup.java 第303-309行
CloseableIterable<ManifestFile> matchingManifests =
    evalCache == null ? closeableDataManifests :
    CloseableIterable.filter(
        scanMetrics.skippedDataManifests(),
        closeableDataManifests,
        manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));
```

### 3.2 第二层：Manifest File 内分区过滤

通过第一层过滤后，对每个匹配的 Manifest 打开 `ManifestReader`，在读取 Manifest 内的数据文件条目时，对每个数据文件的**分区值（partition data）**进行精确匹配。

**文件位置：** `core/src/main/java/org/apache/iceberg/ManifestReader.java` 第237-264行

```java
// ManifestReader.java 第237-258行
private CloseableIterable<ManifestEntry<F>> entries(boolean onlyLive) {
    if (hasRowFilter() || hasPartitionFilter() || partitionSet != null) {
        Evaluator evaluator = evaluator();
        InclusiveMetricsEvaluator metricsEvaluator = metricsEvaluator();

        // 确保统计信息列可用
        boolean requireStatsProjection = requireStatsProjection(rowFilter, columns);
        Collection<String> projectColumns =
            requireStatsProjection ? withStatsColumns(columns) : columns;

        CloseableIterable<ManifestEntry<F>> entries =
            open(projection(fileSchema, fileProjection, projectColumns, caseSensitive));

        return CloseableIterable.filter(
            ..., entries,
            entry -> entry != null
                && evaluator.eval(entry.file().partition())       // ← 分区过滤
                && metricsEvaluator.eval(entry.file())            // ← 统计信息过滤
                && inPartitionSet(entry.file()));
    }
    ...
}
```

其中 `evaluator()` 方法（第347-353行）构建分区评估器：

```java
// ManifestReader.java 第347-353行
private Evaluator evaluator() {
    if (lazyEvaluator == null) {
        Expression projected = Projections.inclusive(spec, caseSensitive).project(rowFilter);
        Expression finalPartFilter = Expressions.and(projected, partFilter);
        this.lazyEvaluator = new Evaluator(spec.partitionType(), finalPartFilter, caseSensitive);
    }
    return lazyEvaluator;
}
```

`Evaluator` 直接在分区数据（`StructLike`）上求值，对每个数据文件的分区值进行精确匹配。

### 3.3 第三层：Data File 统计信息过滤

这是最精细的一层，利用每个数据文件的**列级统计信息**（value count、null count、NaN count、lower bound、upper bound）来判断文件是否可能包含匹配行。

```java
// ManifestReader.java 第356-361行
private InclusiveMetricsEvaluator metricsEvaluator() {
    if (lazyMetricsEvaluator == null) {
        this.lazyMetricsEvaluator =
            new InclusiveMetricsEvaluator(spec.schema(), rowFilter, caseSensitive);
    }
    return lazyMetricsEvaluator;
}
```

详见第 7 节对 `InclusiveMetricsEvaluator` 的深入分析。

**小结：** 三层过滤架构的精妙之处在于：第一层只读 Manifest 元数据（成本最低），第二层读 Manifest 文件条目（中等成本），第三层利用文件内嵌的统计信息（零额外 I/O）。每一层都尽可能多地裁剪掉不相关的数据，使最终交给 Reader 的文件数量最少。

---

## 4. Expression 表达式体系

### 4.1 Expression 接口与操作枚举

**文件位置：** `api/src/main/java/org/apache/iceberg/expressions/Expression.java`

`Expression` 是一个 `Serializable` 接口，代表布尔表达式树。核心枚举 `Operation` 定义了所有支持的操作：

```java
// Expression.java 第27-51行
public interface Expression extends Serializable {
    enum Operation {
        TRUE, FALSE,
        IS_NULL, NOT_NULL, IS_NAN, NOT_NAN,
        LT, LT_EQ, GT, GT_EQ, EQ, NOT_EQ,
        IN, NOT_IN,
        NOT, AND, OR,
        STARTS_WITH, NOT_STARTS_WITH,
        COUNT, COUNT_NULL, COUNT_STAR, MAX, MIN;

        public Operation negate() { ... }   // 取反操作
        public Operation flipLR() { ... }   // 左右互换
    }
}
```

每个 `Operation` 都实现了 `negate()` 方法用于否定重写（例如 `LT.negate()` = `GT_EQ`），以及 `flipLR()` 方法用于左右操作数互换（例如 `LT.flipLR()` = `GT`）。

### 4.2 Expressions 工厂

**文件位置：** `api/src/main/java/org/apache/iceberg/expressions/Expressions.java`

`Expressions` 是一个静态工厂类，提供了构建各种表达式的便捷方法：

```java
// Expressions.java 核心方法
public class Expressions {
    // 逻辑运算 — 带短路优化
    public static Expression and(Expression left, Expression right) {
        if (left == alwaysFalse() || right == alwaysFalse()) return alwaysFalse();
        else if (left == alwaysTrue()) return right;
        else if (right == alwaysTrue()) return left;
        return new And(left, right);
    }

    // 谓词构建
    public static <T> UnboundPredicate<T> equal(String name, T value);
    public static <T> UnboundPredicate<T> lessThan(String name, T value);
    public static <T> UnboundPredicate<T> in(String name, T... values);

    // 分区转换表达式
    public static <T> UnboundTerm<T> bucket(String name, int numBuckets);
    public static <T> UnboundTerm<T> year(String name);
    public static <T> UnboundTerm<T> day(String name);
    public static <T> UnboundTerm<T> hour(String name);
    public static <T> UnboundTerm<T> truncate(String name, int width);

    // NOT 重写：将 NOT 下推到叶子节点
    public static Expression rewriteNot(Expression expr);
}
```

`and()` 和 `or()` 方法包含**常量折叠优化**：如果任一参数为 `alwaysTrue()` 或 `alwaysFalse()`，直接短路返回，避免生成无意义的节点。

### 4.3 UnboundPredicate 与 BoundPredicate

Iceberg 的谓词系统采用**先构建后绑定**的两阶段设计：

- **`UnboundPredicate`**：未绑定的谓词，使用列名（字符串）引用列。
- **`BoundPredicate`**：绑定后的谓词，使用列 ID 和访问器（Accessor）直接访问数据。

**文件位置：** `api/src/main/java/org/apache/iceberg/expressions/BoundPredicate.java`

```java
// BoundPredicate.java 第23-38行
public abstract class BoundPredicate<T> extends Predicate<T, BoundTerm<T>>
    implements Bound<Boolean> {

    public boolean test(StructLike struct) {
        return test(term().eval(struct));  // 先求值 term，再测试谓词
    }

    public abstract boolean test(T value);

    @Override
    public Boolean eval(StructLike struct) {
        return test(term().eval(struct));
    }
}
```

### 4.4 Binder：表达式绑定

`Binder` 负责将 `UnboundPredicate` 绑定到具体的 Schema 上，将列名解析为列 ID，建立列的访问器（Accessor）。绑定过程在每个 Evaluator 创建时自动完成。

```java
// Evaluator.java 第39-41行
public Evaluator(StructType struct, Expression unbound) {
    this.expr = Binder.bind(struct, unbound, true);
}
```

绑定过程还会进行表达式简化和 NOT 重写（通过 `Expressions.rewriteNot()`），确保表达式树中不存在 NOT 节点（NOT 已被下推到叶子）。

**小结：** Expression 体系的"先构建、后绑定"设计是 Iceberg 表达式系统的精髓。它解耦了表达式的语义描述与具体 Schema 的绑定，使得同一个表达式可以在不同上下文（不同的 PartitionSpec、不同的文件 Schema）中复用。

---

## 5. ManifestGroup：扫描规划引擎

### 5.1 核心架构

**文件位置：** `core/src/main/java/org/apache/iceberg/ManifestGroup.java`

`ManifestGroup` 是扫描规划的核心引擎，它协调三层过滤并最终生成 `FileScanTask`。

```java
// ManifestGroup.java 第49-91行
class ManifestGroup {
    private final FileIO io;
    private final Set<ManifestFile> dataManifests;
    private final DeleteFileIndex.Builder deleteIndexBuilder;

    private Expression dataFilter;       // 行级过滤条件
    private Expression fileFilter;       // 文件级过滤条件
    private Expression partitionFilter;  // 分区级过滤条件
    private boolean ignoreDeleted;       // 忽略已删除条目
    private boolean ignoreResiduals;     // 忽略残留谓词
    private List<String> columns;        // 选择的列
    private boolean caseSensitive;
    private ExecutorService executorService;  // 并行规划执行器
    ...
}
```

### 5.2 Manifest 级别过滤

`ManifestGroup.entries()` 方法（第276行起）实现了完整的过滤链：

**步骤 1：构建 ManifestEvaluator 缓存（按 specId）**

```java
// ManifestGroup.java 第279-292行
LoadingCache<Integer, ManifestEvaluator> evalCache =
    Caffeine.newBuilder().build(
        specId -> {
            PartitionSpec spec = specsById.get(specId);
            return ManifestEvaluator.forPartitionFilter(
                Expressions.and(
                    partitionFilter,
                    Projections.inclusive(spec, caseSensitive).project(dataFilter)),
                spec, caseSensitive);
        });
```

使用 Caffeine 缓存是因为一张表可能有多个 `PartitionSpec`（分区演化），不同 specId 对应不同的投影逻辑。

**步骤 2：过滤 Manifest 文件**

```java
// ManifestGroup.java 第303-309行
CloseableIterable<ManifestFile> matchingManifests =
    CloseableIterable.filter(
        scanMetrics.skippedDataManifests(),  // 统计跳过的 Manifest 数量
        closeableDataManifests,
        manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));
```

**步骤 3：进一步过滤（ignoreDeleted / ignoreExisting）**

```java
// ManifestGroup.java 第311-331行
if (ignoreDeleted) {
    matchingManifests = CloseableIterable.filter(
        scanMetrics.skippedDataManifests(),
        matchingManifests,
        manifest -> manifest.hasAddedFiles() || manifest.hasExistingFiles());
}
```

### 5.3 数据文件级别过滤

对每个匹配的 Manifest，创建 `ManifestReader` 并配置行过滤和分区过滤：

```java
// ManifestGroup.java 第344-350行
ManifestReader<DataFile> reader =
    ManifestFiles.read(manifest, io, specsById)
        .filterRows(dataFilter)           // 传递行级过滤条件
        .filterPartitions(partitionFilter) // 传递分区过滤条件
        .caseSensitive(caseSensitive)
        .select(columns)
        .scanMetrics(scanMetrics);
```

### 5.4 并行扫描规划

当数据/删除 Manifest 数量大于 1 时，`ManifestGroup` 使用 `ParallelIterable` 并行扫描多个 Manifest：

```java
// ManifestGroup.java 第216-220行（plan 方法）
if (executorService != null) {
    return new ParallelIterable<>(tasks, executorService);
} else {
    return CloseableIterable.concat(tasks);
}
```

最终为每个匹配的数据文件创建 `FileScanTask`：

```java
// ManifestGroup.java 第393-405行
private static CloseableIterable<FileScanTask> createFileScanTasks(
    CloseableIterable<ManifestEntry<DataFile>> entries, TaskContext ctx) {
    return CloseableIterable.transform(entries, entry -> {
        DataFile dataFile = ContentFileUtil.copy(
            entry.file(), ctx.shouldKeepStats(), ctx.columnsToKeepStats());
        DeleteFile[] deleteFiles = ctx.deletes().forEntry(entry);
        return new BaseFileScanTask(
            dataFile, deleteFiles,
            ctx.schemaAsString(), ctx.specAsString(), ctx.residuals());
    });
}
```

注意：每个 `FileScanTask` 都携带了 `ResidualEvaluator`（残留谓词），供 Reader 做行级过滤。

**小结：** ManifestGroup 是连接元数据过滤与任务生成的桥梁。它将 Manifest 级过滤、分区过滤、文件统计过滤组合在一起，并通过并行执行和缓存机制优化规划性能。

---

## 6. ManifestEvaluator：Manifest 分区摘要过滤

**文件位置：** `api/src/main/java/org/apache/iceberg/expressions/ManifestEvaluator.java`

`ManifestEvaluator` 基于 Manifest 文件的 `PartitionFieldSummary`（每个分区字段的汇总统计信息）进行评估。每个 Manifest 记录了其中所有数据文件分区字段的 `lowerBound`、`upperBound`、`containsNull`、`containsNaN`。

```java
// ManifestEvaluator.java 第55-67行
public static ManifestEvaluator forRowFilter(
    Expression rowFilter, PartitionSpec spec, boolean caseSensitive) {
    return new ManifestEvaluator(
        spec, Projections.inclusive(spec, caseSensitive).project(rowFilter), caseSensitive);
}

public static ManifestEvaluator forPartitionFilter(
    Expression partitionFilter, PartitionSpec spec, boolean caseSensitive) {
    return new ManifestEvaluator(spec, partitionFilter, caseSensitive);
}

private ManifestEvaluator(PartitionSpec spec, Expression partitionFilter, boolean caseSensitive) {
    this.expr = Binder.bind(spec.partitionType(), rewriteNot(partitionFilter), caseSensitive);
}
```

以 `eq` 操作为例（第246-266行）：

```java
// ManifestEvaluator.java 第246-266行
@Override
public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
    int pos = Accessors.toPosition(ref.accessor());
    PartitionFieldSummary fieldStats = stats.get(pos);
    if (fieldStats.lowerBound() == null) {
        return ROWS_CANNOT_MATCH;  // 所有值都是 null
    }

    T lower = Conversions.fromByteBuffer(ref.type(), fieldStats.lowerBound());
    int cmp = lit.comparator().compare(lower, lit.value());
    if (cmp > 0) {
        return ROWS_CANNOT_MATCH;  // 查找值 < 所有分区值的下界
    }

    T upper = Conversions.fromByteBuffer(ref.type(), fieldStats.upperBound());
    cmp = lit.comparator().compare(upper, lit.value());
    if (cmp < 0) {
        return ROWS_CANNOT_MATCH;  // 查找值 > 所有分区值的上界
    }

    return ROWS_MIGHT_MATCH;
}
```

**IN 谓词优化（第284-317行）**：对 IN 谓词设置了 200 的上限（`IN_PREDICATE_LIMIT`），超过后直接返回 `ROWS_MIGHT_MATCH`，避免大量值集合导致评估开销过大。

---

## 7. InclusiveMetricsEvaluator：列统计信息过滤

### 7.1 工作原理

**文件位置：** `api/src/main/java/org/apache/iceberg/expressions/InclusiveMetricsEvaluator.java`

`InclusiveMetricsEvaluator` 是基于数据文件列统计信息（column-level metrics）的**包容性评估器**。它使用每个数据文件的以下信息：

- `valueCounts`：每列的值数量
- `nullCounts`：每列的 null 值数量
- `nanCounts`：每列的 NaN 值数量
- `lowerBounds`：每列的最小值
- `upperBounds`：每列的最大值

评估语义是**包容性的（inclusive）**：返回 `true` 表示文件**可能**包含匹配行；返回 `false` 表示文件**一定不**包含匹配行。只有当评估返回 `false` 时，才能安全地跳过文件。

```java
// InclusiveMetricsEvaluator.java 第55-67行
public class InclusiveMetricsEvaluator {
    private static final int IN_PREDICATE_LIMIT = 200;

    public InclusiveMetricsEvaluator(Schema schema, Expression unbound, boolean caseSensitive) {
        StructType struct = schema.asStruct();
        this.expr = Binder.bind(struct, rewriteNot(unbound), caseSensitive);
    }

    public boolean eval(ContentFile<?> file) {
        return new MetricsEvalVisitor().eval(file);
    }
}
```

### 7.2 核心过滤逻辑

**等值过滤 `eq`（第298-324行）**：

```java
// InclusiveMetricsEvaluator.java 第298-324行
@Override
public <T> Boolean eq(Bound<T> term, Literal<T> lit) {
    int id = term.ref().fieldId();
    if (containsNullsOnly(id) || containsNaNsOnly(id)) {
        return ROWS_CANNOT_MATCH;  // 列只包含 null 或 NaN
    }

    T lower = lowerBound(term);
    if (lower != null && !NaNUtil.isNaN(lower)) {
        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp > 0) {
            return ROWS_CANNOT_MATCH;  // 目标值 < 文件最小值
        }
    }

    T upper = upperBound(term);
    if (null == upper) {
        return ROWS_MIGHT_MATCH;
    }

    int cmp = lit.comparator().compare(upper, lit.value());
    if (cmp < 0) {
        return ROWS_CANNOT_MATCH;  // 目标值 > 文件最大值
    }

    return ROWS_MIGHT_MATCH;
}
```

**小于过滤 `lt`（第202-226行）**：

```java
// InclusiveMetricsEvaluator.java 第202-226行
@Override
public <T> Boolean lt(Bound<T> term, Literal<T> lit) {
    int id = term.ref().fieldId();
    if (containsNullsOnly(id) || containsNaNsOnly(id)) {
        return ROWS_CANNOT_MATCH;
    }

    T lower = lowerBound(term);
    if (null == lower || NaNUtil.isNaN(lower)) {
        return ROWS_MIGHT_MATCH;
    }

    // 关键逻辑注释（原文）：
    // if a transform f is order preserving, a < b means that f(a) <= f(b).
    // because lower <= a for all values of a in the file, f(lower) <= f(a).
    // when f(lower) >= X then f(a) >= f(lower) >= X, so there is no a such that f(a) < X
    int cmp = lit.comparator().compare(lower, lit.value());
    if (cmp >= 0) {
        return ROWS_CANNOT_MATCH;  // 文件最小值 >= 目标值，不可能有 < 目标值的行
    }

    return ROWS_MIGHT_MATCH;
}
```

值得注意的是，`InclusiveMetricsEvaluator` 支持对**保序转换（order preserving transform）**的列进行评估。通过 `BoundTransform`，它可以处理 `day(ts) < '2024-01-01'` 这样的转换表达式。

**不等过滤 `notEq`（第327-339行）**：

```java
// InclusiveMetricsEvaluator.java 第327-339行
@Override
public <T> Boolean notEq(Bound<T> term, Literal<T> lit) {
    // 只有当 min == max 且无 null/NaN 时，才能安全裁剪
    T value = uniqueValue(term);
    if (value != null && lit.comparator().compare(value, lit.value()) == 0) {
        return ROWS_CANNOT_MATCH;
    }
    return ROWS_MIGHT_MATCH;
}
```

### 7.3 IN 谓词优化

```java
// InclusiveMetricsEvaluator.java 第342-386行
@Override
public <T> Boolean in(Bound<T> term, Set<T> literalSet) {
    int id = term.ref().fieldId();
    if (containsNullsOnly(id) || containsNaNsOnly(id)) {
        return ROWS_CANNOT_MATCH;
    }

    Collection<T> literals = literalSet;
    if (literals.size() > IN_PREDICATE_LIMIT) {
        return ROWS_MIGHT_MATCH;  // 值过多，跳过评估
    }

    T lower = lowerBound(term);
    // 过滤掉 < lower 的值
    literals = literals.stream()
        .filter(v -> comparator.compare(lower, v) <= 0)
        .collect(Collectors.toList());
    if (literals.isEmpty()) return ROWS_CANNOT_MATCH;

    T upper = upperBound(term);
    // 过滤掉 > upper 的值
    literals = literals.stream()
        .filter(v -> comparator.compare(upper, v) >= 0)
        .collect(Collectors.toList());
    if (literals.isEmpty()) return ROWS_CANNOT_MATCH;

    return ROWS_MIGHT_MATCH;
}
```

**小结：** InclusiveMetricsEvaluator 的设计秉持"宁可多读、不可漏读"的原则。它利用文件的列统计信息做快速裁剪，可以跳过大量不相关的数据文件，同时保证不会误删任何可能匹配的文件。

---

## 8. StrictMetricsEvaluator vs InclusiveMetricsEvaluator

### 8.1 语义差异

**文件位置：** `api/src/main/java/org/apache/iceberg/expressions/StrictMetricsEvaluator.java`

两者的语义恰好互补：

| 特性 | InclusiveMetricsEvaluator | StrictMetricsEvaluator |
|------|--------------------------|----------------------|
| 返回 true 含义 | 文件**可能**包含匹配行 | 文件中**所有行都**匹配 |
| 返回 false 含义 | 文件**一定不**包含匹配行 | 文件中**可能有行**不匹配 |
| 用途 | 数据裁剪（跳过不相关文件） | 残留谓词消除（判断是否需要行级过滤） |
| 安全方向 | false 时安全跳过 | true 时可以跳过行级过滤 |

以 `lt` 为例对比：

**InclusiveMetricsEvaluator.lt**：检查是否存在行可能 < X
```java
// 当 lower >= X 时，不可能有行 < X → ROWS_CANNOT_MATCH
int cmp = lit.comparator().compare(lower, lit.value());
if (cmp >= 0) return ROWS_CANNOT_MATCH;
```

**StrictMetricsEvaluator.lt**：检查是否所有行都 < X
```java
// StrictMetricsEvaluator.java 第197-218行
// 当 upper < X 时，所有行都 < X → ROWS_MUST_MATCH
if (upperBounds != null && upperBounds.containsKey(id)) {
    T upper = Conversions.fromByteBuffer(ref.type(), upperBounds.get(id));
    int cmp = lit.comparator().compare(upper, lit.value());
    if (cmp < 0) {
        return ROWS_MUST_MATCH;
    }
}
return ROWS_MIGHT_NOT_MATCH;
```

### 8.2 使用场景对比

```
文件统计: min=10, max=50

查询条件: col > 30

InclusiveMetricsEvaluator:
  → upper(50) > 30? 是 → ROWS_MIGHT_MATCH（文件可能有匹配行，不能跳过）

StrictMetricsEvaluator:
  → lower(10) > 30? 否 → ROWS_MIGHT_NOT_MATCH（不是所有行都匹配）

查询条件: col > 60

InclusiveMetricsEvaluator:
  → upper(50) > 60? 否 → ROWS_CANNOT_MATCH（文件一定没有匹配行，可以跳过！）

查询条件: col > 5

StrictMetricsEvaluator:
  → lower(10) > 5? 是 → ROWS_MUST_MATCH（所有行都匹配，无需行级过滤！）
```

`StrictMetricsEvaluator` 还有一个重要限制：**不支持转换表达式**（第101-111行）。对于非直接列引用的 term，它直接返回 `ROWS_MIGHT_NOT_MATCH`：

```java
// StrictMetricsEvaluator.java 第101-111行
@Override
public <T> Boolean handleNonReference(Bound<T> term) {
    return ROWS_MIGHT_NOT_MATCH;
}
```

**小结：** Inclusive 用于"能不能跳过这个文件"（文件裁剪），Strict 用于"能不能省掉行级过滤"（残留谓词消除）。二者配合使用，在 ResidualEvaluator 中共同工作。

---

## 9. Projections：表达式投影

### 9.1 Inclusive Projection

**文件位置：** `api/src/main/java/org/apache/iceberg/expressions/Projections.java`

Inclusive Projection 将行级谓词投影为分区级谓词，保证：**如果原始谓词匹配某一行，那么投影后的谓词一定匹配该行所在的分区**。

```java
// Projections.java 第195-227行
private static class InclusiveProjection extends BaseProjectionEvaluator {
    @Override
    public <T> Expression predicate(BoundPredicate<T> pred) {
        Collection<PartitionField> parts = spec().getFieldsBySourceId(pred.ref().fieldId());
        if (parts == null) {
            return Expressions.alwaysTrue();  // 非分区列，无法投影，返回 true（包容性）
        }

        Expression result = Expressions.alwaysTrue();
        for (PartitionField part : parts) {
            // 对每个分区字段，通过其 Transform 进行投影
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

例如，对于 `day(ts)` 分区和谓词 `ts >= '2024-01-01 12:00:00'`，投影结果是 `ts_day >= day('2024-01-01 12:00:00')` = `ts_day >= '2024-01-01'`。

### 9.2 Strict Projection

Strict Projection 保证：**如果投影后的谓词匹配某个分区，那么该分区中所有行都匹配原始谓词**。

```java
// Projections.java 第229-259行
private static class StrictProjection extends BaseProjectionEvaluator {
    @Override
    public <T> Expression predicate(BoundPredicate<T> pred) {
        Collection<PartitionField> parts = spec().getFieldsBySourceId(pred.ref().fieldId());
        if (parts == null) {
            return Expressions.alwaysFalse();  // 非分区列，返回 false（严格性）
        }

        Expression result = Expressions.alwaysFalse();
        for (PartitionField part : parts) {
            UnboundPredicate<?> strictProjection =
                ((Transform<T, ?>) part.transform()).projectStrict(part.name(), pred);
            if (strictProjection != null) {
                result = Expressions.or(result, strictProjection);  // 注意是 OR
            }
        }
        return result;
    }
}
```

注意两者在组合多个分区字段时的差异：
- **Inclusive**：使用 **AND** 组合（所有分区约束都必须满足）
- **Strict**：使用 **OR** 组合（任一分区约束满足即可，因为只要一个能保证所有行匹配就够了）

**小结：** Projections 是连接行级谓词与分区级谓词的桥梁。Inclusive 投影用于文件裁剪，Strict 投影用于残留谓词消除。两者的安全方向（AND vs OR、alwaysTrue vs alwaysFalse）设计得恰到好处。

---

## 10. ResidualEvaluator：残留谓词计算

### 10.1 核心概念

**文件位置：** `api/src/main/java/org/apache/iceberg/expressions/ResidualEvaluator.java`

残留谓词（Residual）是指**分区过滤之后仍然需要在行级别执行的过滤条件**。当一个谓词跨越分区边界时，分区过滤只能部分消解它。

源码文档中的经典示例（第32-46行）：

```
表按 day(utc_timestamp) 分区
查询条件：utc_timestamp >= a AND utc_timestamp <= b

对于分区值 d，有以下 4 种残留情况：
1. 若 d > day(a) 且 d < day(b)，残留为 alwaysTrue（分区内所有行都匹配）
2. 若 d == day(a) 且 d != day(b)，残留为 utc_timestamp >= a
3. 若 d == day(b) 且 d != day(a)，残留为 utc_timestamp <= b
4. 若 d == day(a) == day(b)，残留为 utc_timestamp >= a AND utc_timestamp <= b
```

### 10.2 实现原理

`ResidualEvaluator` 的核心是 `ResidualVisitor` 内部类（第112行起），它利用 `Strict Projection` 和 `Inclusive Projection` 来判断谓词能否被分区值完全消解：

```java
// ResidualEvaluator.java 第226-288行
@Override
public <T> Expression predicate(BoundPredicate<T> pred) {
    List<PartitionField> parts = spec.getFieldsBySourceId(pred.ref().fieldId());
    if (parts == null) {
        return pred;  // 非分区列，无法消解，返回原谓词
    }

    for (PartitionField part : parts) {
        // 1. 检查 Strict Projection
        UnboundPredicate<?> strictProjection =
            ((Transform<T, ?>) part.transform()).projectStrict(part.name(), pred);
        Expression strictResult = null;
        if (strictProjection != null) {
            Expression bound = strictProjection.bind(spec.partitionType(), caseSensitive);
            strictResult = (bound instanceof BoundPredicate)
                ? super.predicate((BoundPredicate<?>) bound) : bound;
        }

        if (strictResult != null && strictResult.op() == Expression.Operation.TRUE) {
            return Expressions.alwaysTrue();  // Strict 为 true → 所有行都匹配
        }

        // 2. 检查 Inclusive Projection
        UnboundPredicate<?> inclusiveProjection =
            ((Transform<T, ?>) part.transform()).project(part.name(), pred);
        Expression inclusiveResult = null;
        if (inclusiveProjection != null) {
            Expression boundInclusive = inclusiveProjection.bind(spec.partitionType(), caseSensitive);
            inclusiveResult = (boundInclusive instanceof BoundPredicate)
                ? super.predicate((BoundPredicate<?>) boundInclusive) : boundInclusive;
        }

        if (inclusiveResult != null && inclusiveResult.op() == Expression.Operation.FALSE) {
            return Expressions.alwaysFalse();  // Inclusive 为 false → 没有行匹配
        }
    }

    return pred;  // 无法完全消解，返回原谓词作为残留
}
```

处理流程：
1. **尝试 Strict Projection**：对分区值求值，如果结果为 `TRUE`，说明分区内所有行都满足谓词 → 残留为 `alwaysTrue()`
2. **尝试 Inclusive Projection**：对分区值求值，如果结果为 `FALSE`，说明分区内没有行满足谓词 → 残留为 `alwaysFalse()`
3. **两者都不确定**：保留原始谓词作为残留，交给 Reader 做行级过滤

**在 ManifestGroup 中的使用：**

```java
// ManifestGroup.java 第182-189行
LoadingCache<Integer, ResidualEvaluator> residualCache =
    Caffeine.newBuilder().build(
        specId -> {
            PartitionSpec spec = specsById.get(specId);
            Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : dataFilter;
            return ResidualEvaluator.of(spec, filter, caseSensitive);
        });
```

**小结：** ResidualEvaluator 是"过滤-消解"管道的最后一步。它精确计算每个分区需要传递给 Reader 的残留过滤条件，避免了在完全匹配的分区上执行不必要的行级过滤。

---

## 11. 引擎集成：SQL WHERE 到 Iceberg Expression

### 11.1 Spark 集成

#### SparkScanBuilder：过滤下推入口

**文件位置：** `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkScanBuilder.java`

`SparkScanBuilder` 实现了 `SupportsPushDownV2Filters` 接口，在 `pushPredicates()` 方法（第151-195行）中处理谓词下推：

```java
// SparkScanBuilder.java 第151-195行
@Override
public Predicate[] pushPredicates(Predicate[] predicates) {
    // 三种过滤器:
    // (1) 可以完全下推，不需要 Spark 再评估（整个分区匹配的过滤器）
    // (2) 可以部分下推，仍需 Spark 做行级过滤
    // (3) 不能下推，完全由 Spark 评估

    List<Expression> expressions = Lists.newArrayListWithExpectedSize(predicates.length);
    List<Predicate> postScanFilters = Lists.newArrayListWithExpectedSize(predicates.length);

    for (Predicate predicate : predicates) {
        Expression expr = SparkV2Filters.convert(predicate);  // 转换

        if (expr != null) {
            Binder.bind(schema.asStruct(), expr, caseSensitive);  // 验证绑定
            expressions.add(expr);
        }

        // 判断是否可以完全由 Iceberg 处理
        if (expr == null || unpartitioned()
            || !ExpressionUtil.selectsPartitions(expr, table, caseSensitive)) {
            postScanFilters.add(predicate);  // 需要 Spark 做后续过滤
        }
    }

    this.filterExpressions = expressions;
    return postScanFilters.toArray(new Predicate[0]);  // 返回给 Spark 的后扫描过滤
}
```

#### SparkFilters：类型映射

**文件位置：** `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkFilters.java`

`SparkFilters.convert()` 方法（第111行起）将 Spark 的 `Filter` 对象逐一映射为 Iceberg `Expression`：

```java
// SparkFilters.java 第79-98行（映射表）
private static final Map<Class<? extends Filter>, Operation> FILTERS =
    ImmutableMap.<Class<? extends Filter>, Operation>builder()
        .put(EqualTo.class, Operation.EQ)
        .put(GreaterThan.class, Operation.GT)
        .put(LessThan.class, Operation.LT)
        .put(In.class, Operation.IN)
        .put(IsNull.class, Operation.IS_NULL)
        .put(IsNotNull.class, Operation.NOT_NULL)
        .put(And.class, Operation.AND)
        .put(Or.class, Operation.OR)
        .put(StringStartsWith.class, Operation.STARTS_WITH)
        ...
```

特殊处理包括：
- **EqualNullSafe**：如果值为 null，转为 `isNull()`
- **NaN 值**：`equal(col, NaN)` 转为 `isNaN(col)`
- **NOT IN**：额外推导 `notNull()` 谓词（因为 Iceberg 不遵循 SQL 三值逻辑）
- **时间类型**：`Timestamp`, `Date`, `Instant`, `LocalDateTime` 等自动转换为微秒值

#### SparkBatchQueryScan：运行时过滤

**文件位置：** `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkBatchQueryScan.java`

`SparkBatchQueryScan` 还实现了 `SupportsRuntimeV2Filtering`，支持运行时动态过滤（如 broadcast join 生成的 IN 条件）：

```java
// SparkBatchQueryScan.java 第127-163行
@Override
public void filter(Predicate[] predicates) {
    Expression runtimeFilterExpr = convertRuntimeFilters(predicates);

    if (runtimeFilterExpr != Expressions.alwaysTrue()) {
        Map<Integer, Evaluator> evaluatorsBySpecId = Maps.newHashMap();
        for (PartitionSpec spec : specs()) {
            Expression inclusiveExpr =
                Projections.inclusive(spec, caseSensitive()).project(runtimeFilterExpr);
            evaluatorsBySpecId.put(spec.specId(), new Evaluator(spec.partitionType(), inclusiveExpr));
        }

        // 在已规划的任务上进行分区级过滤
        List<PartitionScanTask> filteredTasks = tasks().stream()
            .filter(task -> evaluatorsBySpecId.get(task.spec().specId()).eval(task.partition()))
            .collect(Collectors.toList());

        if (filteredTasks.size() < tasks().size()) {
            resetTasks(filteredTasks);
        }
    }
}
```

### 11.2 Flink 集成

#### FlinkFilters：表达式转换

**文件位置：** `flink/v2.0/flink/src/main/java/org/apache/iceberg/flink/FlinkFilters.java`

`FlinkFilters.convert()` 方法将 Flink 的 `CallExpression` 转换为 Iceberg Expression：

```java
// FlinkFilters.java 第49-63行
private static final Map<FunctionDefinition, Operation> FILTERS =
    ImmutableMap.<FunctionDefinition, Operation>builder()
        .put(BuiltInFunctionDefinitions.EQUALS, Operation.EQ)
        .put(BuiltInFunctionDefinitions.NOT_EQUALS, Operation.NOT_EQ)
        .put(BuiltInFunctionDefinitions.GREATER_THAN, Operation.GT)
        .put(BuiltInFunctionDefinitions.LESS_THAN, Operation.LT)
        .put(BuiltInFunctionDefinitions.IS_NULL, Operation.IS_NULL)
        .put(BuiltInFunctionDefinitions.AND, Operation.AND)
        .put(BuiltInFunctionDefinitions.OR, Operation.OR)
        .put(BuiltInFunctionDefinitions.LIKE, Operation.STARTS_WITH)
        ...
```

特殊处理：
- **LIKE → STARTS_WITH**：仅当 LIKE 模式为 `prefix%`（无 `_` 通配符）时转为 `startsWith`
- **BETWEEN / IN**：Flink 自动将 BETWEEN 转为 `GT_EQ AND LT_EQ`，IN 转为 OR 链，因此无需在 FlinkFilters 中额外处理
- **操作数方向**：支持 `value > field` 形式，自动翻转为 `field < value`

#### IcebergTableSource：过滤集成

**文件位置：** `flink/v2.0/flink/src/main/java/org/apache/iceberg/flink/source/IcebergTableSource.java`

```java
// IcebergTableSource.java 第167-179行
@Override
public Result applyFilters(List<ResolvedExpression> flinkFilters) {
    List<ResolvedExpression> acceptedFilters = Lists.newArrayList();
    List<Expression> expressions = Lists.newArrayList();

    for (ResolvedExpression resolvedExpression : flinkFilters) {
        Optional<Expression> icebergExpression = FlinkFilters.convert(resolvedExpression);
        if (icebergExpression.isPresent()) {
            expressions.add(icebergExpression.get());
            acceptedFilters.add(resolvedExpression);
        }
    }
    this.filters = expressions;
    ...
}
```

**小结：** Spark 和 Flink 的集成模式高度一致：将引擎的过滤表达式转换为 Iceberg Expression → 传入 Scan.filter() → 由 ManifestGroup 在规划阶段应用。区别在于 Spark 还支持运行时动态过滤和聚合下推。

---

## 12. 列投影（Column Projection）对性能的影响

列投影在 Iceberg 扫描中具有**多层面的性能影响**：

### 12.1 Schema 级别的投影

在 `BaseScan.lazyColumnProjection()` 方法（第275-303行）中，投影 Schema 的计算同时考虑了选择的列和过滤条件引用的列：

```java
// BaseScan.java 第275-303行
private static Schema lazyColumnProjection(TableScanContext context, Schema schema) {
    Collection<String> selectedColumns = context.selectedColumns();
    if (selectedColumns != null) {
        Set<Integer> requiredFieldIds = Sets.newHashSet();

        // 过滤条件引用的列也是必需的
        requiredFieldIds.addAll(
            Binder.boundReferences(
                schema.asStruct(),
                Collections.singletonList(context.rowFilter()),
                context.caseSensitive()));

        // 用户选择的列
        Set<Integer> selectedIds = TypeUtil.getProjectedIds(schema.select(selectedColumns));
        requiredFieldIds.addAll(selectedIds);

        return TypeUtil.project(schema, requiredFieldIds);
    }
    ...
}
```

### 12.2 Manifest 读取级别的投影

在扫描规划时，`BaseScan` 定义了两组列集合：

```java
// BaseScan.java 第42-67行
protected static final List<String> SCAN_COLUMNS = ImmutableList.of(
    "snapshot_id", "file_path", "file_ordinal", "file_format",
    "block_size_in_bytes", "file_size_in_bytes", "record_count",
    "partition", "key_metadata", "split_offsets", "sort_order_id");

// 带统计信息的列集合
private static final List<String> STATS_COLUMNS = ImmutableList.of(
    "value_counts", "null_value_counts", "nan_value_counts",
    "lower_bounds", "upper_bounds", "column_sizes");
```

默认扫描**不读取统计信息列**（它们可能很大），仅在需要时才加载。`ManifestReader` 通过 `select()` 方法控制从 Manifest Avro 文件中读取哪些列，减少 I/O 量。

### 12.3 统计信息保留控制

`ManifestReader.dropStats()` 方法（第372-384行）判断是否需要在输出的 `FileScanTask` 中保留统计信息：

```java
// ManifestReader.java 第372-384行
static boolean dropStats(Collection<String> columns) {
    if (columns != null && !columns.containsAll(ManifestReader.ALL_COLUMNS)) {
        Set<String> intersection = Sets.intersection(Sets.newHashSet(columns), STATS_COLUMNS);
        return intersection.isEmpty() || intersection.equals(Sets.newHashSet("record_count"));
    }
    return false;
}
```

### 12.4 性能影响总结

| 投影层面 | 影响 |
|---------|------|
| 只选择需要的列 | 减少 Reader 层的列 I/O，利用列式存储的优势 |
| 排除统计信息列 | 减少 Manifest 读取的数据量（统计信息可能占 Manifest 的 50%+） |
| 包含过滤列 | 确保谓词下推正常工作，即使用户未显式选择这些列 |
| `columnsToKeepStats` | 只保留必要列的统计信息，减少 TaskContext 内存占用 |

---

## 13. 实际查询场景分析

### 场景：订单表按天分区查询

假设有一张订单表，按 `day(order_time)` 分区，查询条件：

```sql
SELECT order_id, amount
FROM orders
WHERE order_time >= '2024-03-15 08:00:00'
  AND order_time <  '2024-03-16 12:00:00'
  AND amount > 1000
```

**谓词下推完整链路：**

**Step 1：引擎层转换**

Spark/Flink 将 SQL WHERE 转换为 Iceberg Expression：
```
AND(
  AND(
    greaterThanOrEqual("order_time", 1710489600000000L),   // 微秒
    lessThan("order_time", 1710590400000000L)
  ),
  greaterThan("amount", 1000)
)
```

**Step 2：Inclusive Projection（行 → 分区）**

对于 `day(order_time)` 分区：
- `order_time >= '2024-03-15 08:00:00'` → `order_time_day >= day('2024-03-15 08:00:00')` = `order_time_day >= 2024-03-15`
- `order_time < '2024-03-16 12:00:00'` → `order_time_day <= day('2024-03-16 12:00:00')` = `order_time_day <= 2024-03-16`
- `amount > 1000` → `alwaysTrue()`（非分区列，无法投影）

投影后的分区表达式：`order_time_day >= 2024-03-15 AND order_time_day <= 2024-03-16`

**Step 3：第一层过滤 — Manifest 级别**

假设有 10 个 Manifest：
- Manifest A: 分区范围 [2024-01-01, 2024-02-28] → **跳过**（upper < 2024-03-15）
- Manifest B: 分区范围 [2024-03-01, 2024-03-31] → **保留**（范围重叠）
- Manifest C: 分区范围 [2024-04-01, 2024-06-30] → **跳过**（lower > 2024-03-16）

**结果：** 10 个 Manifest 跳过 8 个，仅保留 2 个。

**Step 4：第二层过滤 — 分区值过滤**

在保留的 Manifest B 中，逐个检查数据文件的分区值：
- File 1: partition = 2024-03-01 → **跳过**
- File 2: partition = 2024-03-15 → **保留** ✓
- File 3: partition = 2024-03-16 → **保留** ✓
- File 4: partition = 2024-03-31 → **跳过**

**Step 5：第三层过滤 — 列统计信息过滤**

对保留的文件检查 `amount` 列的统计信息：
- File 2 (2024-03-15): amount min=50, max=5000 → **保留**（max > 1000，可能有匹配行）
- File 3a (2024-03-16): amount min=10, max=800 → **跳过**（max < 1000，不可能有匹配行！）
- File 3b (2024-03-16): amount min=500, max=20000 → **保留**

**Step 6：ResidualEvaluator 计算残留谓词**

对于 partition = 2024-03-15 的文件：
- `order_time >= '2024-03-15 08:00:00'`：Strict projection `order_time_day >= 2024-03-16` 对 d=2024-03-15 为 false → 保留原谓词
- `order_time < '2024-03-16 12:00:00'`：Strict projection `order_time_day <= 2024-03-15` 对 d=2024-03-15 为 true → **消解为 alwaysTrue()**
- `amount > 1000`：非分区列，保留

残留谓词：`order_time >= '2024-03-15 08:00:00' AND amount > 1000`

对于 partition = 2024-03-16 的文件：
- 残留谓词：`order_time < '2024-03-16 12:00:00' AND amount > 1000`

**最终效果：** 假设表有 100 个 Manifest 包含 10,000 个数据文件，经过三层过滤后，实际只需读取 2 个数据文件，Reader 层还可以利用残留谓词进一步行级过滤。

---

## 14. 与 Hive / 传统数据湖扫描方式对比

| 维度 | Hive / 传统数据湖 | Apache Iceberg |
|------|--------------------|----------------|
| **元数据存储** | HDFS 目录结构 + Hive Metastore | 独立的 Manifest List / Manifest File |
| **分区发现** | 递归列出文件系统目录（`listStatus`） | 读取 Manifest List → Manifest File（O(1) 次元数据读取） |
| **扫描成本** | 与分区数和文件数成正比（大量 namenode RPC） | 与 Manifest 数成正比（通常远少于文件数） |
| **非分区列过滤** | 不支持（必须全表扫描后在 Reader 过滤） | 支持（利用文件级 min/max 统计信息跳过） |
| **分区演化** | 需要重写数据和目录结构 | 透明支持，新旧分区策略共存 |
| **隐藏分区** | 不支持（分区必须在 WHERE 中显式指定） | 支持（`day(ts)` 等自动从行级谓词投影） |
| **文件级统计** | 依赖文件格式（Parquet footer），需额外 I/O | 内嵌在 Manifest 中，零额外 I/O |
| **并行规划** | 受限于 namenode 并发 | 原生支持多线程并行扫描 Manifest |
| **原子快照** | 不保证（可能读到不一致的文件列表） | 基于快照的 MVCC，读取始终一致 |

### 关键差异解读

1. **Manifest 级过滤是 Iceberg 的独有优势**：Hive 必须列出每个分区目录下的所有文件，而 Iceberg 可以通过 Manifest 的分区摘要一次性跳过成千上万的文件。

2. **非分区列的谓词下推**：这是 Iceberg 相较 Hive 最显著的性能提升点。Hive 对非分区列的过滤只能在 Reader 层执行，而 Iceberg 可以利用每个数据文件的 min/max 统计信息在规划阶段跳过不相关的文件。

3. **隐藏分区消除了用户的心智负担**：在 Hive 中，用户必须知道分区列是 `dt='2024-03-15'` 并在 WHERE 中显式指定。在 Iceberg 中，用户只需写 `WHERE ts > '2024-03-15 08:00:00'`，系统自动将其投影到 `day(ts)` 分区上。

4. **统计信息的零额外 I/O**：Hive 如果要利用 Parquet 文件的 min/max 统计信息，需要读取每个文件的 footer（额外的 seek + read）。Iceberg 将这些信息预先存储在 Manifest 中，在规划阶段就可以使用。

---

## 15. 总结

Apache Iceberg 的表扫描规划与谓词下推机制是一个设计精巧、层次分明的系统，核心要点总结如下：

### 架构设计

1. **三层漏斗过滤**：Manifest List → Manifest File → Data File，从粗到细逐层裁剪，每一层都尽可能减少下一层的输入量。

2. **双重评估器模式**：`InclusiveMetricsEvaluator`（包容性，用于数据裁剪）和 `StrictMetricsEvaluator`（严格性，用于残留消除）配合使用，保证了过滤的正确性和高效性。

3. **表达式投影系统**：通过 `Projections.inclusive()` 和 `Projections.strict()` 将行级谓词自动投影为分区级谓词，支持隐藏分区和分区演化。

### 关键类图

```
用户 SQL WHERE 子句
    │
    ├── SparkFilters.convert() / FlinkFilters.convert()
    │       将引擎 Filter 转为 Iceberg Expression
    ▼
Scan.filter(Expression)
    │
    ├── BaseScan：存储过滤条件到 TableScanContext
    │
    ▼
DataTableScan.doPlanFiles()
    │
    ├── 创建 ManifestGroup，传入 dataFilter
    │
    ▼
ManifestGroup.planFiles()
    │
    ├── Projections.inclusive() → 行谓词投影为分区谓词
    ├── ManifestEvaluator → 第一层：Manifest 级过滤
    ├── ManifestReader:
    │     ├── Evaluator → 第二层：分区值过滤
    │     └── InclusiveMetricsEvaluator → 第三层：列统计过滤
    └── ResidualEvaluator → 计算残留谓词
    │
    ▼
FileScanTask（含残留谓词）→ Reader 做行级过滤
```

### 性能优势

- **零额外 I/O 的文件过滤**：统计信息内嵌在 Manifest 中
- **自动隐藏分区**：用户无需了解分区策略
- **并行规划**：多个 Manifest 可并发处理
- **缓存优化**：Caffeine Cache 缓存评估器，避免重复构建
- **短路求值**：Expression 体系内置常量折叠优化

### 核心源码文件清单

| 文件 | 职责 |
|------|------|
| `api/.../Scan.java` | 扫描顶层接口 |
| `api/.../TableScan.java` | 表扫描接口（含快照能力） |
| `core/.../DataTableScan.java` | 数据表扫描实现 |
| `core/.../ManifestGroup.java` | 扫描规划引擎，三层过滤协调器 |
| `core/.../ManifestReader.java` | Manifest 文件读取器，分区+统计信息过滤 |
| `api/.../expressions/Expression.java` | 表达式接口与操作枚举 |
| `api/.../expressions/Expressions.java` | 表达式工厂 |
| `api/.../expressions/Projections.java` | 行→分区表达式投影 |
| `api/.../expressions/ManifestEvaluator.java` | Manifest 分区摘要评估器 |
| `api/.../expressions/InclusiveMetricsEvaluator.java` | 包容性文件统计评估器 |
| `api/.../expressions/StrictMetricsEvaluator.java` | 严格性文件统计评估器 |
| `api/.../expressions/ResidualEvaluator.java` | 残留谓词计算器 |
| `api/.../expressions/Evaluator.java` | 分区值精确评估器 |
| `spark/v3.5/.../SparkFilters.java` | Spark Filter → Iceberg Expression |
| `spark/v3.5/.../SparkScanBuilder.java` | Spark 扫描构建器（谓词下推入口） |
| `flink/v2.0/.../FlinkFilters.java` | Flink Expression → Iceberg Expression |
| `flink/v2.0/.../IcebergTableSource.java` | Flink 表源（过滤下推入口） |

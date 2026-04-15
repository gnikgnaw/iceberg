# Apache Iceberg 时间旅行与快照生命周期管理深度分析

## 目录

1. [概述](#1-概述)
2. [时间旅行的底层原理](#2-时间旅行的底层原理)
   - 2.1 [Snapshot 链与 parentId 继承](#21-snapshot-链与-parentid-继承)
   - 2.2 [SnapshotLog 与时间戳映射](#22-snapshotlog-与时间戳映射)
   - 2.3 [快照拓扑图与祖先遍历](#23-快照拓扑图与祖先遍历)
3. [SnapshotRef：分支与标签](#3-snapshotref分支与标签)
   - 3.1 [SnapshotRef 数据模型](#31-snapshotref-数据模型)
   - 3.2 [Branch 与 Tag 的区别](#32-branch-与-tag-的区别)
   - 3.3 [WAP (Write-Audit-Publish) 模式](#33-wap-write-audit-publish-模式)
4. [Spark 中的时间旅行实现](#4-spark-中的时间旅行实现)
   - 4.1 [VERSION AS OF 解析路径](#41-version-as-of-解析路径)
   - 4.2 [TIMESTAMP AS OF 解析路径](#42-timestamp-as-of-解析路径)
   - 4.3 [Spark v4.1 的 TimeTravel sealed interface](#43-spark-v41-的-timetravel-sealed-interface)
5. [Flink 中的时间旅行查询](#5-flink-中的时间旅行查询)
6. [Core 层的时间旅行 Scan 实现](#6-core-层的时间旅行-scan-实现)
7. [ExpireSnapshots 操作的完整流程](#7-expiresnapshots-操作的完整流程)
   - 7.1 [快照选择策略](#71-快照选择策略)
   - 7.2 [文件清理策略](#72-文件清理策略)
   - 7.3 [IncrementalFileCleanup 增量清理](#73-incrementalfilecleanup-增量清理)
   - 7.4 [ReachableFileCleanup 可达性清理](#74-reachablefilecleanup-可达性清理)
8. [RemoveOrphanFiles 孤儿文件清理](#8-removeorphanfiles-孤儿文件清理)
9. [cherryPick 与 rollbackTo 操作](#9-cherrypick-与-rollbackto-操作)
   - 9.1 [rollbackTo 实现](#91-rollbackto-实现)
   - 9.2 [cherryPick 实现](#92-cherrypick-实现)
10. [表属性与快照保留策略](#10-表属性与快照保留策略)
11. [快照过多的性能影响与最佳实践](#11-快照过多的性能影响与最佳实践)
12. [总结](#12-总结)

---

## 1. 概述

Apache Iceberg 的时间旅行（Time Travel）能力是其作为数据湖表格式的核心竞争力之一。它允许用户查询表在任意历史时间点或特定快照版本的数据状态，而快照生命周期管理则确保这些历史版本不会无限膨胀从而拖累性能。

本文从 Iceberg 源码出发，系统性地分析时间旅行的底层实现机制、各引擎的集成方式、快照过期清理的完整流程以及相关操作的源码实现。

---

## 2. 时间旅行的底层原理

### 2.1 Snapshot 链与 parentId 继承

Iceberg 的每一次数据变更（append、overwrite、delete 等）都会产生一个新的 `Snapshot`。每个 Snapshot 通过 `parentId` 指向其前驱快照，形成一条单链表结构。

**核心接口 `Snapshot`**（`api/src/main/java/org/apache/iceberg/Snapshot.java`）：

```java
// 第42-66行
public interface Snapshot extends Serializable {
  long sequenceNumber();       // 快照的序列号，提交时分配
  long snapshotId();           // 快照的唯一ID
  Long parentId();             // 父快照ID，首个快照为null
  long timestampMillis();      // 快照创建时间戳（毫秒）
  String operation();          // 产生此快照的操作类型
  Map<String, String> summary(); // 操作摘要信息
  String manifestListLocation(); // manifest list 文件位置
  Integer schemaId();          // 创建此快照时使用的schema ID
}
```

**BaseSnapshot 实现**（`core/src/main/java/org/apache/iceberg/BaseSnapshot.java`，第36-48行）中明确存储了 `parentId`：

```java
class BaseSnapshot implements Snapshot {
  private final long snapshotId;
  private final Long parentId;           // 父快照ID，形成链表
  private final long sequenceNumber;
  private final long timestampMillis;
  private final String manifestListLocation;
  private final String operation;
  private final Map<String, String> summary;
  private final Integer schemaId;
  // ...
}
```

这条 parentId 链构成了快照的"血统"（lineage），是时间旅行和 rollback 操作的基础。

### 2.2 SnapshotLog 与时间戳映射

除了快照链之外，`TableMetadata` 中还维护了一个 **SnapshotLog**（快照日志），记录了每个快照成为"当前快照"时的时间戳。这是时间旅行的另一个关键数据结构。

**SnapshotLogEntry 定义**（`core/src/main/java/org/apache/iceberg/TableMetadata.java`，第156-198行）：

```java
public static class SnapshotLogEntry implements HistoryEntry {
  private final long timestampMillis;  // 快照成为当前状态的时间
  private final long snapshotId;       // 快照ID

  SnapshotLogEntry(long timestampMillis, long snapshotId) {
    this.timestampMillis = timestampMillis;
    this.snapshotId = snapshotId;
  }
}
```

`HistoryEntry` 接口（`api/src/main/java/org/apache/iceberg/HistoryEntry.java`，第29-35行）很简洁：

```java
public interface HistoryEntry extends Serializable {
  long timestampMillis(); // 变更的时间戳
  long snapshotId();      // 新的当前快照ID
}
```

**关键区别**：`Snapshot.timestampMillis()` 是快照创建的时间，而 `HistoryEntry.timestampMillis()` 是快照成为当前状态的时间。正常提交时两者相同，但在 rollback 操作时会不同——rollback 使用当前系统时间作为 HistoryEntry 的时间戳。

在 `TableMetadata.Builder.setRef()` 方法中（第1319-1328行），可以看到这个区分逻辑：

```java
if (SnapshotRef.MAIN_BRANCH.equals(name)) {
  this.currentSnapshotId = ref.snapshotId();
  if (lastUpdatedMillis == null) {
    this.lastUpdatedMillis = System.currentTimeMillis();
  }
  // rollback 到已有快照时使用当前时间，新增快照使用快照自身时间
  long timeOfChange =
      isAddedSnapshot(snapshotId) ? snapshot.timestampMillis() : this.lastUpdatedMillis;
  snapshotLog.add(new SnapshotLogEntry(timeOfChange, ref.snapshotId()));
}
```

**SnapshotLog 的维护**：当快照被删除时，日志需要特殊处理以避免时间旅行查询产生错误结果。`updateSnapshotLog` 方法（第1847-1887行）会在快照被移除后清理日志中的无效条目，并且一旦发现某个历史条目的快照已被删除，就会清除该条目之前的所有日志——这是因为留下不连续的日志会导致时间旅行查询在中间时段返回错误的快照：

```java
// 第1870-1877行
} else if (hasRemovedSnapshots) {
  // 任何无效的条目都会导致它之前的历史被移除。否则，可能出现历史间隙
  // 使时间旅行查询返回不正确的结果。例如：
  // 如果历史是 [(t1, s1), (t2, s2), (t3, s3)] 且 s2 被移除，
  // 历史不能是 [(t1, s1), (t3, s3)]，因为看起来 s3 在 t2 到 t3 之间是当前快照，
  // 但实际上那时 s2 才是当前快照。
  newSnapshotLog.clear();
}
```

### 2.3 快照拓扑图与祖先遍历

`SnapshotUtil`（`core/src/main/java/org/apache/iceberg/util/SnapshotUtil.java`）是快照图遍历的核心工具类。它提供了基于 `parentId` 链的祖先迭代器。

**核心遍历方法**（第231-277行）：

```java
private static Iterable<Snapshot> ancestorsOf(
    Snapshot snapshot, Function<Long, Snapshot> lookup) {
  if (snapshot != null) {
    return () ->
        new Iterator<Snapshot>() {
          private Snapshot next = snapshot;
          private boolean consumed = false;

          @Override
          public boolean hasNext() {
            if (!consumed) return true;
            if (next == null) return false;
            Long parentId = next.parentId();
            if (parentId == null) return false;
            this.next = lookup.apply(parentId);
            if (next != null) {
              this.consumed = false;
              return true;
            }
            return false;
          }

          @Override
          public Snapshot next() {
            if (hasNext()) {
              this.consumed = true;
              return next;
            }
            throw new NoSuchElementException();
          }
        };
  } else {
    return ImmutableList.of();
  }
}
```

遍历逻辑是从给定快照沿着 `parentId` 链向上回溯，直到遇到 `parentId == null`（首个快照）或者 `lookup.apply(parentId)` 返回 `null`（祖先快照已被过期删除）。

**基于时间戳的快照查找**（第380-400行）——这是时间旅行 `TIMESTAMP AS OF` 的核心：

```java
public static long snapshotIdAsOfTime(Table table, long timestampMillis) {
  Long snapshotId = nullableSnapshotIdAsOfTime(table, timestampMillis);
  Preconditions.checkArgument(
      snapshotId != null,
      "Cannot find a snapshot older than %s",
      DateTimeUtil.formatTimestampMillis(timestampMillis));
  return snapshotId;
}

public static Long nullableSnapshotIdAsOfTime(Table table, long timestampMillis) {
  Long snapshotId = null;
  for (HistoryEntry logEntry : table.history()) {
    if (logEntry.timestampMillis() <= timestampMillis) {
      snapshotId = logEntry.snapshotId();  // 找到不超过给定时间戳的最新快照
    }
  }
  return snapshotId;
}
```

这里遍历的是 **SnapshotLog**（通过 `table.history()`），而不是直接遍历快照的 parentId 链。SnapshotLog 是按时间排序的，所以遍历结束后 `snapshotId` 就是在给定时间点最新的当前快照。

**快照拓扑示意图**：

```
时间 →

S1 ← S2 ← S3 ← S4 ← S5 (main branch)
              ↖
               S6 ← S7 (feature branch, 从S3分叉)

标签：release-v1 → S3
标签：release-v2 → S5
```

在上图中：
- main 分支的当前快照是 S5
- S5.parentId = S4, S4.parentId = S3, S3.parentId = S2, S2.parentId = S1, S1.parentId = null
- feature 分支的 S7.parentId = S6, S6.parentId = S3
- 两条链在 S3 处汇合

---

## 3. SnapshotRef：分支与标签

### 3.1 SnapshotRef 数据模型

`SnapshotRef`（`api/src/main/java/org/apache/iceberg/SnapshotRef.java`）是 Iceberg 引入的对快照的"命名引用"机制，类似于 Git 中的 branch 和 tag 概念。

```java
// 第26-47行
public class SnapshotRef implements Serializable {
  public static final String MAIN_BRANCH = "main";  // 主分支固定名称

  private final long snapshotId;            // 指向的快照ID
  private final SnapshotRefType type;       // BRANCH 或 TAG
  private final Integer minSnapshotsToKeep; // 分支上最少保留的快照数（仅branch）
  private final Long maxSnapshotAgeMs;      // 分支上快照的最大年龄（仅branch）
  private final Long maxRefAgeMs;           // 引用本身的最大年龄
}
```

`TableMetadata` 通过 `Map<String, SnapshotRef> refs` 维护所有引用。`main` 分支始终存在，指向当前快照。

### 3.2 Branch 与 Tag 的区别

| 特性 | Branch（分支） | Tag（标签） |
|------|---------------|------------|
| 类型 | `SnapshotRefType.BRANCH` | `SnapshotRefType.TAG` |
| 可写入 | 是 | 否 |
| `minSnapshotsToKeep` | 支持 | 不支持（Builder 中校验） |
| `maxSnapshotAgeMs` | 支持 | 不支持（Builder 中校验） |
| `maxRefAgeMs` | 支持 | 支持 |
| 用途 | 并行写入、WAP 流程 | 标记重要版本 |

Builder 中的限制逻辑（第155-170行）：

```java
public Builder minSnapshotsToKeep(Integer value) {
  Preconditions.checkArgument(
      value == null || !type.equals(SnapshotRefType.TAG),
      "Tags do not support setting minSnapshotsToKeep");
  // ...
}

public Builder maxSnapshotAgeMs(Long value) {
  Preconditions.checkArgument(
      value == null || !type.equals(SnapshotRefType.TAG),
      "Tags do not support setting maxSnapshotAgeMs");
  // ...
}
```

Branch 可以有独立的快照保留策略，这使得不同分支可以有不同的保留期限。

### 3.3 WAP (Write-Audit-Publish) 模式

WAP 模式是一种审计工作流：先将数据写入到一个"暂存"分支，审计通过后再 cherry-pick 到主分支。

**WapUtil**（`core/src/main/java/org/apache/iceberg/util/WapUtil.java`）实现了核心验证逻辑：

```java
// 第30-34行
public static String stagedWapId(Snapshot snapshot) {
  return snapshot.summary() != null
      ? snapshot.summary().get(SnapshotSummary.STAGED_WAP_ID_PROP)
      : null;
}

// 第50-60行 - 验证 WAP ID 是否已被发布
public static String validateWapPublish(TableMetadata current, long wapSnapshotId) {
  Snapshot cherryPickSnapshot = current.snapshot(wapSnapshotId);
  String wapId = stagedWapId(cherryPickSnapshot);
  if (wapId != null && !wapId.isEmpty()) {
    if (WapUtil.isWapIdPublished(current, wapId)) {
      throw new DuplicateWAPCommitException(wapId);  // 防止重复发布
    }
  }
  return wapId;
}
```

WAP 流程：
1. 数据写入到特定分支（写入时在 snapshot summary 中标记 `wap.id`）
2. 审计人员检查分支上的数据
3. 通过 cherry-pick 将变更应用到 main 分支
4. `validateWapPublish` 确保同一个 WAP ID 不会被重复发布

---

## 4. Spark 中的时间旅行实现

### 4.1 VERSION AS OF 解析路径

Spark SQL 的 `VERSION AS OF` 语法会调用 `SparkCatalog.loadTable(Identifier ident, String version)` 方法。

**Spark v3.5 中的实现**（`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkCatalog.java`，第177-209行）：

```java
@Override
public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
  Table table = loadTable(ident);
  if (table instanceof SparkTable) {
    SparkTable sparkTable = (SparkTable) table;
    Preconditions.checkArgument(
        sparkTable.snapshotId() == null && sparkTable.branch() == null,
        "Cannot do time-travel based on both table identifier and AS OF");

    try {
      // 首先尝试将 version 解析为 snapshot ID（数字）
      return sparkTable.copyWithSnapshotId(Long.parseLong(version));
    } catch (NumberFormatException e) {
      // 如果不是数字，则作为 ref 名称查找
      SnapshotRef ref = sparkTable.table().refs().get(version);
      ValidationException.check(ref != null,
          "Cannot find matching snapshot ID or reference name for version %s", version);
      if (ref.isBranch()) {
        return sparkTable.copyWithBranch(version);  // 分支：切换到分支
      } else {
        return sparkTable.copyWithSnapshotId(ref.snapshotId());  // 标签：使用其指向的快照
      }
    }
  }
  // ...
}
```

**解析优先级**：snapshot ID (数字) > ref 名称 (branch/tag)

### 4.2 TIMESTAMP AS OF 解析路径

**Spark v3.5 实现**（`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkCatalog.java`，第212-234行）：

```java
@Override
public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
  Table table = loadTable(ident);
  if (table instanceof SparkTable) {
    SparkTable sparkTable = (SparkTable) table;
    Preconditions.checkArgument(
        sparkTable.snapshotId() == null && sparkTable.branch() == null,
        "Cannot do time-travel based on both table identifier and AS OF");

    // Spark 传递微秒，Iceberg 使用毫秒
    long timestampMillis = TimeUnit.MICROSECONDS.toMillis(timestamp);
    long snapshotId = SnapshotUtil.snapshotIdAsOfTime(sparkTable.table(), timestampMillis);
    return sparkTable.copyWithSnapshotId(snapshotId);
  }
  // ...
}
```

**注意**：Spark 传递的时间戳单位是**微秒**，需要转换为**毫秒**后再调用 `SnapshotUtil.snapshotIdAsOfTime`。

### 4.3 Spark v4.1 的 TimeTravel sealed interface

在 Spark v4.1 中，Iceberg 引入了 `TimeTravel` sealed interface（`spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/TimeTravel.java`）来统一时间旅行的表达：

```java
// 第25-64行
public sealed interface TimeTravel permits TimeTravel.AsOfVersion, TimeTravel.AsOfTimestamp {

  static TimeTravel version(String version) {
    return new AsOfVersion(version);
  }

  static TimeTravel timestampMicros(long timestamp) {
    return new AsOfTimestamp(timestamp);
  }

  /** VERSION AS OF — 可以是 snapshot ID、branch 或 tag */
  record AsOfVersion(String version) implements TimeTravel {
    public boolean isSnapshotId() {
      return Longs.tryParse(version) != null;
    }
  }

  /** TIMESTAMP AS OF — 时间戳单位为微秒 */
  record AsOfTimestamp(long timestampMicros) implements TimeTravel {
    public long timestampMillis() {
      return TimeUnit.MICROSECONDS.toMillis(timestampMicros);
    }
  }
}
```

**SparkTable.create 的统一入口**（`spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkTable.java`，第314-348行）：

```java
public static SparkTable create(Table table, TimeTravel timeTravel) {
  if (timeTravel == null) {
    return new SparkTable(table);
  } else if (timeTravel instanceof AsOfVersion asOfVersion) {
    return createWithVersion(table, asOfVersion);
  } else if (timeTravel instanceof AsOfTimestamp asOfTimestamp) {
    return createWithTimestamp(table, asOfTimestamp);
  }
  throw new IllegalArgumentException("Unknown time travel: " + timeTravel);
}

private static SparkTable createWithVersion(Table table, AsOfVersion timeTravel) {
  if (timeTravel.isSnapshotId()) {
    return new SparkTable(table, Long.parseLong(timeTravel.version()), timeTravel);
  } else {
    SnapshotRef ref = table.refs().get(timeTravel.version());
    // ... 解析 branch 或 tag
  }
}

private static SparkTable createWithTimestamp(Table table, AsOfTimestamp timeTravel) {
  long timestampMillis = timeTravel.timestampMillis();
  long snapshotId = SnapshotUtil.snapshotIdAsOfTime(table, timestampMillis);
  return new SparkTable(table, snapshotId, timeTravel);
}
```

Spark v4.1 中 `SparkCatalog.loadTable` 的时间旅行入口被简化为（第166-172行）：

```java
public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
  return load(ident, TimeTravel.version(version));
}

public Table loadTable(Identifier ident, long timestampMicros) throws NoSuchTableException {
  return load(ident, TimeTravel.timestampMicros(timestampMicros));
}
```

**实际使用的 SQL 语法**：

```sql
-- 按时间戳查询（Spark 将时间字符串转为微秒传递给 loadTable）
SELECT * FROM db.table TIMESTAMP AS OF '2024-01-01 00:00:00';

-- 按快照ID查询
SELECT * FROM db.table VERSION AS OF 123456789;

-- 按 tag 名称查询
SELECT * FROM db.table VERSION AS OF 'release-v1';

-- 按 branch 名称查询
SELECT * FROM db.table VERSION AS OF 'audit-branch';
```

---

## 5. Flink 中的时间旅行查询

Flink 的时间旅行支持通过 **配置选项** 而非 SQL 语法来实现。

**FlinkReadOptions**（`flink/v2.1/flink/src/main/java/org/apache/iceberg/flink/FlinkReadOptions.java`，第33-68行）定义了核心选项：

```java
public static final ConfigOption<Long> SNAPSHOT_ID =
    ConfigOptions.key("snapshot-id").longType().defaultValue(null);

public static final ConfigOption<String> TAG =
    ConfigOptions.key("tag").stringType().defaultValue(null);

public static final ConfigOption<String> BRANCH =
    ConfigOptions.key("branch").stringType().defaultValue(null);

public static final ConfigOption<Long> AS_OF_TIMESTAMP =
    ConfigOptions.key("as-of-timestamp").longType().defaultValue(null);
```

**ScanContext** 封装了这些参数（`flink/v2.1/flink/src/main/java/org/apache/iceberg/flink/source/ScanContext.java`，第46-53行）：

```java
private final Long snapshotId;
private final String branch;
private final String tag;
private final Long asOfTimestamp;
```

**FlinkSplitPlanner** 中的实际执行逻辑（`flink/v2.1/flink/src/main/java/org/apache/iceberg/flink/source/FlinkSplitPlanner.java`，第121-136行）：

```java
TableScan scan = table.newScan();
scan = refineScanWithBaseConfigs(scan, context, workerPool);

if (context.snapshotId() != null) {
  scan = scan.useSnapshot(context.snapshotId());  // 直接指定快照ID
} else if (context.tag() != null) {
  scan = scan.useRef(context.tag());              // 使用 tag
} else if (context.branch() != null) {
  scan = scan.useRef(context.branch());           // 使用 branch
}

if (context.asOfTimestamp() != null) {
  scan = scan.asOfTime(context.asOfTimestamp());  // 按时间戳查询
}

return scan.planTasks();
```

**Flink 批量读取的时间旅行使用方式**：

```java
// 方式1：通过 snapshot-id 指定
TableEnvironment tEnv = ...;
tEnv.executeSql(
    "SELECT * FROM my_table /*+ OPTIONS('snapshot-id'='123456789') */");

// 方式2：通过 as-of-timestamp 指定
tEnv.executeSql(
    "SELECT * FROM my_table /*+ OPTIONS('as-of-timestamp'='1704067200000') */");

// 方式3：通过 tag 指定
tEnv.executeSql(
    "SELECT * FROM my_table /*+ OPTIONS('tag'='release-v1') */");

// 方式4：通过 branch 指定
tEnv.executeSql(
    "SELECT * FROM my_table /*+ OPTIONS('branch'='feature-x') */");
```

**重要限制**（ScanContext 构造函数中的校验，第156-162行）：

```java
// 流式读取不支持 tag、snapshot-id、as-of-timestamp
Preconditions.checkArgument(
    tag == null, "Cannot scan table using ref %s configured for streaming reader", tag);
Preconditions.checkArgument(
    snapshotId == null, "Cannot set snapshot-id option for streaming reader");
Preconditions.checkArgument(
    asOfTimestamp == null, "Cannot set as-of-timestamp option for streaming reader");
```

---

## 6. Core 层的时间旅行 Scan 实现

时间旅行在 Core 层通过 `SnapshotScan` 基类实现（`core/src/main/java/org/apache/iceberg/SnapshotScan.java`）。

**三种时间旅行方式**（第104-136行）：

```java
// 方式1：直接指定快照ID
public ThisT useSnapshot(long scanSnapshotId) {
  Preconditions.checkArgument(
      snapshotId() == null, "Cannot override snapshot, already set snapshot id=%s", snapshotId());
  Preconditions.checkArgument(
      table().snapshot(scanSnapshotId) != null,
      "Cannot find snapshot with ID %s", scanSnapshotId);
  Schema newSchema =
      useSnapshotSchema() ? SnapshotUtil.schemaFor(table(), scanSnapshotId) : tableSchema();
  TableScanContext newContext = context().useSnapshotId(scanSnapshotId);
  return newRefinedScan(table(), newSchema, newContext);
}

// 方式2：使用命名引用（branch 或 tag）
public ThisT useRef(String name) {
  if (SnapshotRef.MAIN_BRANCH.equals(name)) {
    return newRefinedScan(table(), tableSchema(), context());  // main 分支直接返回
  }
  Snapshot snapshot = table().snapshot(name);
  Preconditions.checkArgument(snapshot != null, "Cannot find ref %s", name);
  TableScanContext newContext = context().useSnapshotId(snapshot.snapshotId());
  Schema newSchema = useSnapshotSchema() ? SnapshotUtil.schemaFor(table(), name) : tableSchema();
  return newRefinedScan(table(), newSchema, newContext);
}

// 方式3：按时间戳查找（转化为方式1）
public ThisT asOfTime(long timestampMillis) {
  Preconditions.checkArgument(
      snapshotId() == null, "Cannot override snapshot, already set snapshot id=%s", snapshotId());
  return useSnapshot(SnapshotUtil.snapshotIdAsOfTime(table(), timestampMillis));
}
```

**Schema 回溯**：当 `useSnapshotSchema()` 返回 `true` 时（对于 `DataTableScan` 和 `RESTTableScan`），Scan 会自动使用目标快照时的 Schema，而不是当前 Schema。这通过 `SnapshotUtil.schemaFor()` 实现（第412-430行）：

```java
public static Schema schemaFor(Table table, long snapshotId) {
  if (table instanceof BaseMetadataTable) {
    return table.schema();  // 元数据表不跟随快照Schema
  }
  Snapshot snapshot = table.snapshot(snapshotId);
  Integer schemaId = snapshot.schemaId();
  if (schemaId != null) {
    Schema schema = table.schemas().get(schemaId);
    return schema;
  }
  return table.schema();  // 老版本快照可能没有记录schemaId
}
```

---

## 7. ExpireSnapshots 操作的完整流程

`ExpireSnapshots` 是快照生命周期管理的核心操作，其实现类为 `RemoveSnapshots`（`core/src/main/java/org/apache/iceberg/RemoveSnapshots.java`）。

### 7.1 快照选择策略

**初始化参数**（第84-103行）从表属性中读取默认值：

```java
RemoveSnapshots(TableOperations ops) {
  this.ops = ops;
  this.base = ops.current();
  // 检查GC是否启用
  ValidationException.check(
      PropertyUtil.propertyAsBoolean(base.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
      "Cannot expire snapshots: GC is disabled");

  long defaultMaxSnapshotAgeMs = PropertyUtil.propertyAsLong(
      base.properties(), MAX_SNAPSHOT_AGE_MS, MAX_SNAPSHOT_AGE_MS_DEFAULT);

  this.now = System.currentTimeMillis();
  this.defaultExpireOlderThan = now - defaultMaxSnapshotAgeMs;  // 默认过期时间点
  this.defaultMinNumSnapshots = PropertyUtil.propertyAsInt(
      base.properties(), MIN_SNAPSHOTS_TO_KEEP, MIN_SNAPSHOTS_TO_KEEP_DEFAULT);
  this.defaultMaxRefAgeMs = PropertyUtil.propertyAsLong(
      base.properties(), MAX_REF_AGE_MS, MAX_REF_AGE_MS_DEFAULT);
}
```

**internalApply() 方法**（第196-272行）的完整流程：

**步骤1：计算保留的引用**（`computeRetainedRefs`，第274-297行）

```java
private Map<String, SnapshotRef> computeRetainedRefs(Map<String, SnapshotRef> refs) {
  Map<String, SnapshotRef> retainedRefs = Maps.newHashMap();
  for (Map.Entry<String, SnapshotRef> refEntry : refs.entrySet()) {
    String name = refEntry.getKey();
    SnapshotRef ref = refEntry.getValue();
    if (name.equals(SnapshotRef.MAIN_BRANCH)) {
      retainedRefs.put(name, ref);  // main 分支始终保留
      continue;
    }
    Snapshot snapshot = base.snapshot(ref.snapshotId());
    long maxRefAgeMs = ref.maxRefAgeMs() != null ? ref.maxRefAgeMs() : defaultMaxRefAgeMs;
    if (snapshot != null) {
      long refAgeMs = now - snapshot.timestampMillis();
      if (refAgeMs <= maxRefAgeMs) {
        retainedRefs.put(name, ref);  // 未过期的引用保留
      }
    }
  }
  return retainedRefs;
}
```

**步骤2：计算每个分支需要保留的快照**（`computeAllBranchSnapshotsToRetain`，第299-329行）

每个分支有自己的保留策略（`minSnapshotsToKeep` 和 `maxSnapshotAgeMs`），若未设置则使用全局默认值：

```java
private Set<Long> computeBranchSnapshotsToRetain(
    long snapshot, long expireSnapshotsOlderThan, int minSnapshotsToKeep) {
  Set<Long> idsToRetain = Sets.newHashSet();
  for (Snapshot ancestor : SnapshotUtil.ancestorsOf(snapshot, base::snapshot)) {
    if (idsToRetain.size() < minSnapshotsToKeep
        || ancestor.timestampMillis() >= expireSnapshotsOlderThan) {
      idsToRetain.add(ancestor.snapshotId());  // 满足数量或年龄条件的保留
    } else {
      return idsToRetain;  // 两个条件都不满足，停止遍历
    }
  }
  return idsToRetain;
}
```

**步骤3：保留未被引用但未到过期时间的快照**（`unreferencedSnapshotsToRetain`，第331-353行）

```java
private Set<Long> unreferencedSnapshotsToRetain(Collection<SnapshotRef> refs) {
  Set<Long> referencedSnapshots = Sets.newHashSet();
  for (SnapshotRef ref : refs) {
    if (ref.isBranch()) {
      for (Snapshot snapshot : SnapshotUtil.ancestorsOf(ref.snapshotId(), base::snapshot)) {
        referencedSnapshots.add(snapshot.snapshotId());
      }
    } else {
      referencedSnapshots.add(ref.snapshotId());
    }
  }
  Set<Long> snapshotsToRetain = Sets.newHashSet();
  for (Snapshot snapshot : base.snapshots()) {
    if (!referencedSnapshots.contains(snapshot.snapshotId())     // 未被引用
        && snapshot.timestampMillis() >= defaultExpireOlderThan) { // 但还没过期
      snapshotsToRetain.add(snapshot.snapshotId());
    }
  }
  return snapshotsToRetain;
}
```

**步骤4：汇总并构建新的 TableMetadata**

```java
// 第233-237行
base.snapshots().stream()
    .map(Snapshot::snapshotId)
    .filter(snapshot -> !idsToRetain.contains(snapshot))
    .forEach(idsToRemove::add);
updatedMetaBuilder.removeSnapshots(idsToRemove);
```

### 7.2 文件清理策略

`commit()` 方法（第356-377行）在元数据提交成功后执行文件清理：

```java
public void commit() {
  // ... 重试提交逻辑 ...
  if (CleanupLevel.NONE != cleanupLevel && !base.snapshots().isEmpty()) {
    cleanExpiredSnapshots();  // 提交成功后清理文件
  }
}
```

**清理策略选择**（第384-407行）：

```java
private void cleanExpiredSnapshots() {
  TableMetadata current = ops.refresh();

  // 自动判断是否可以使用增量清理
  if (incrementalCleanup == null) {
    incrementalCleanup =
        !specifiedSnapshotId                         // 没有指定特定快照ID
        && !hasRemovedNonMainAncestors(base, current) // 没有删除非主线祖先
        && !hasNonMainSnapshots(current);             // 没有非主线快照
  }

  FileCleanupStrategy cleanupStrategy =
      incrementalCleanup
          ? new IncrementalFileCleanup(...)   // 增量清理
          : new ReachableFileCleanup(...);    // 可达性清理

  cleanupStrategy.cleanFiles(base, current, cleanupLevel);
}
```

`CleanupLevel` 枚举（`api/src/main/java/org/apache/iceberg/ExpireSnapshots.java`，第42-49行）：

```java
enum CleanupLevel {
  NONE,           // 仅移除快照元数据，不清理文件
  METADATA_ONLY,  // 仅清理元数据文件（manifests、manifest lists、statistics）
  ALL             // 清理所有关联文件（包括数据文件）
}
```

### 7.3 IncrementalFileCleanup 增量清理

`IncrementalFileCleanup`（`core/src/main/java/org/apache/iceberg/IncrementalFileCleanup.java`）是最常用的清理策略，适用于只有主分支且无 rollback 场景。

**核心流程**（第51-280行）：

1. **识别已过期的快照**（第65-78行）：比较 before/after 两份元数据中的快照列表
2. **识别祖先链**（第96行）：确定当前表状态的所有祖先快照
3. **保护 cherry-pick 来源**（第98-109行）：如果某个祖先快照是通过 cherry-pick 而来，保护其源快照
4. **扫描仍被引用的 manifest**（第112-160行）：遍历所有仍存在的快照的 manifest，标记为 valid
5. **识别可删除的 manifest 和数据文件**（第166-259行）

关键的安全逻辑（第149-154行）——只删除**祖先链**上已过期快照标记为 DELETED 的文件：

```java
if (!fromValidSnapshots
    && (isFromAncestor || isPicked)
    && manifest.hasDeletedFiles()) {
  manifestsToScan.add(manifest.copy());
}
```

对于非祖先链上的已过期快照，还会回收其 ADDED 文件（第233-246行）：

```java
if (!isFromAncestor && isFromExpiringSnapshot && manifest.hasAddedFiles()) {
  // 因为 manifest 是由非祖先快照写入的，所以其添加的文件可以安全删除
  manifestsToRevert.add(manifest.copy());
}
```

### 7.4 ReachableFileCleanup 可达性清理

`ReachableFileCleanup`（`core/src/main/java/org/apache/iceberg/ReachableFileCleanup.java`）通过构建完整的"可达文件集"来判断哪些文件可以安全删除，适用于存在分支、rollback 等复杂场景。

**核心流程**（第53-107行）：

1. **收集过期快照的所有 manifest**：作为删除候选
2. **从当前有效快照中移除仍被引用的 manifest**（`pruneReferencedManifests`）
3. **找出仅存在于待删除 manifest 中但不存在于当前 manifest 中的数据文件**
4. **执行删除**

关键的 `findFilesToDelete` 方法（第170-229行）：

```java
private Set<String> findFilesToDelete(
    Set<ManifestFile> manifestFilesToDelete,
    Set<ManifestFile> currentManifestFiles,
    Map<Integer, PartitionSpec> specsById) {
  Set<String> filesToDelete = ConcurrentHashMap.newKeySet();

  // 先收集待删除 manifest 中的所有文件路径
  Tasks.foreach(manifestFilesToDelete).run(manifest -> {
    try (CloseableIterable<String> paths = ManifestFiles.readPaths(manifest, fileIO, specsById)) {
      paths.forEach(filesToDelete::add);
    }
  });

  // 再从当前 manifest 中移除仍被引用的文件
  Tasks.foreach(currentManifestFiles).run(manifest -> {
    try (CloseableIterable<String> paths = ManifestFiles.readPaths(manifest, fileIO, specsById)) {
      paths.forEach(filesToDelete::remove);  // 集合差运算
    }
  });

  return filesToDelete;
}
```

**两种策略的对比**：

| 维度 | IncrementalFileCleanup | ReachableFileCleanup |
|------|----------------------|---------------------|
| 适用场景 | 简单主线场景 | 复杂多分支场景 |
| 内存消耗 | 较低 | 较高（需要全量可达集） |
| 准确性 | 依赖祖先链判断 | 完整的可达性分析 |
| 自动选择条件 | 无指定快照ID、无非主线祖先删除、无非主线快照 | 其他情况 |

---

## 8. RemoveOrphanFiles 孤儿文件清理

**孤儿文件**是指存在于存储层但未被任何有效快照引用的文件。它们可能由以下原因产生：
- 写入操作失败（文件已写入但快照未提交）
- 快照过期后文件清理不完整
- 元数据损坏或手动操作

**DeleteOrphanFiles 接口**（`api/src/main/java/org/apache/iceberg/actions/DeleteOrphanFiles.java`）定义了核心 API：

```java
// 第34行
public interface DeleteOrphanFiles extends Action<DeleteOrphanFiles, DeleteOrphanFiles.Result> {

  DeleteOrphanFiles location(String location);       // 扫描位置
  DeleteOrphanFiles olderThan(long olderThanTimestamp); // 安全期（默认3天）
  DeleteOrphanFiles deleteWith(Consumer<String> deleteFunc);  // 自定义删除函数
}
```

**安全机制**（`olderThan` 默认3天，第129行）：

```java
private long olderThanTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3);
```

**Spark 实现 `DeleteOrphanFilesSparkAction`**（`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/DeleteOrphanFilesSparkAction.java`）的工作原理：

1. **列出存储层所有文件**：通过 Hadoop FileSystem API 递归列出 table location 下的所有文件
2. **收集所有被引用的文件**：遍历所有有效快照的 manifest，收集所有被引用的数据文件、manifest 文件、manifest list 文件和统计文件
3. **取差集**：存储层文件 - 被引用文件 = 孤儿文件
4. **过滤安全期**：只删除创建时间超过 `olderThan` 的孤儿文件
5. **执行删除**

**注意事项**：
- 列举存储层文件是一个**昂贵操作**，尤其在对象存储（S3、GCS等）上
- `olderThan` 的安全期非常重要，因为可能有并发写入操作正在添加新文件
- GC 开关 (`gc.enabled`) 必须为 true 才能执行

---

## 9. cherryPick 与 rollbackTo 操作

### 9.1 rollbackTo 实现

`SetSnapshotOperation`（`core/src/main/java/org/apache/iceberg/SetSnapshotOperation.java`）实现了 rollback 逻辑。

**rollbackTo(long snapshotId)**（第78-89行）：

```java
public SetSnapshotOperation rollbackTo(long snapshotId) {
  TableMetadata current = base;
  ValidationException.check(
      current.snapshot(snapshotId) != null,
      "Cannot roll back to unknown snapshot id: %s", snapshotId);
  ValidationException.check(
      isCurrentAncestor(current, snapshotId),
      "Cannot roll back to snapshot, not an ancestor of the current state: %s", snapshotId);
  return setCurrentSnapshot(snapshotId);
}
```

**关键约束**：只能 rollback 到当前快照的**祖先**。这通过 `isCurrentAncestor` 验证，该方法沿 parentId 链向上遍历。

**rollbackToTime(long timestampMillis)**（第66-76行）：

```java
public SetSnapshotOperation rollbackToTime(long timestampMillis) {
  Snapshot snapshot = findLatestAncestorOlderThan(base, timestampMillis);
  Preconditions.checkArgument(
      snapshot != null, "Cannot roll back, no valid snapshot older than: %s", timestampMillis);
  this.targetSnapshotId = snapshot.snapshotId();
  this.isRollback = true;
  return this;
}
```

`findLatestAncestorOlderThan`（第146-158行）在**祖先链**上找到时间戳小于给定值的最新快照：

```java
private static Snapshot findLatestAncestorOlderThan(TableMetadata meta, long timestampMillis) {
  long snapshotTimestamp = 0;
  Snapshot result = null;
  for (Long snapshotId : currentAncestors(meta)) {
    Snapshot snapshot = meta.snapshot(snapshotId);
    if (snapshot.timestampMillis() < timestampMillis
        && snapshot.timestampMillis() > snapshotTimestamp) {
      result = snapshot;
      snapshotTimestamp = snapshot.timestampMillis();
    }
  }
  return result;
}
```

**commit() 过程**（第109-137行）：将 main 分支的 SnapshotRef 指向目标快照ID：

```java
Snapshot snapshot = apply();
TableMetadata updated =
    TableMetadata.buildFrom(base)
        .setBranchSnapshot(snapshot.snapshotId(), SnapshotRef.MAIN_BRANCH)
        .build();
taskOps.commit(base, updated.withUUID());
```

**重要**：rollback **不会删除**被跳过的快照，它只是将 `main` 分支的指针移回到历史快照。被跳过的快照仍然保留在元数据中，可以通过后续操作或 expire 来处理。

### 9.2 cherryPick 实现

`CherryPickOperation`（`core/src/main/java/org/apache/iceberg/CherryPickOperation.java`）继承自 `MergingSnapshotProducer`，支持两种模式：

**模式1：Fast-Forward**（第173-182行）

如果被 cherry-pick 的快照的 parent 就是当前快照（或两者都为 null），则直接快进：

```java
private boolean isFastForward(TableMetadata base) {
  if (base.currentSnapshot() != null) {
    return cherrypickSnapshot.parentId() != null
        && base.currentSnapshot().snapshotId() == cherrypickSnapshot.parentId();
  } else {
    return cherrypickSnapshot.parentId() == null;
  }
}
```

**模式2：真正的 Cherry-Pick**（第78-141行）

只支持 `APPEND` 操作和带 `REPLACE_PARTITIONS` 标记的 `OVERWRITE` 操作：

```java
if (cherrypickSnapshot.operation().equals(DataOperations.APPEND)) {
  // 提取所有添加的文件，复制到新快照
  String wapId = WapUtil.validateWapPublish(current, snapshotId);
  set(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP, String.valueOf(snapshotId));
  for (DataFile addedFile : changes.addedDataFiles()) {
    add(addedFile);
  }
} else if (/* OVERWRITE with REPLACE_PARTITIONS */) {
  // 验证被替换的分区没有被其他操作修改
  // 复制添加和删除的文件
} else {
  // 尝试 fast-forward
  ValidationException.check(isFastForward(current), "Cannot cherry-pick ...");
}
```

**防重复提交**（`validateNonAncestor`，第207-216行）：

```java
private static void validateNonAncestor(TableMetadata meta, long snapshotId) {
  if (isCurrentAncestor(meta, snapshotId)) {
    throw new CherrypickAncestorCommitException(snapshotId);  // 不能 cherry-pick 已在主线上的快照
  }
  Long ancestorId = lookupAncestorBySourceSnapshot(meta, snapshotId);
  if (ancestorId != null) {
    throw new CherrypickAncestorCommitException(snapshotId, ancestorId);  // 源快照已被 pick 过
  }
}
```

---

## 10. 表属性与快照保留策略

`TableProperties`（`core/src/main/java/org/apache/iceberg/TableProperties.java`，第371-381行）中定义了快照过期相关的核心属性：

| 属性 | 常量名 | 默认值 | 说明 |
|------|-------|--------|------|
| `gc.enabled` | `GC_ENABLED` | `true` | 是否允许垃圾回收。设为 false 可防止 expire 和 orphan 清理操作 |
| `history.expire.max-snapshot-age-ms` | `MAX_SNAPSHOT_AGE_MS` | 5天 (432000000ms) | 快照的最大存活时间。超过此时间的快照可被过期 |
| `history.expire.min-snapshots-to-keep` | `MIN_SNAPSHOTS_TO_KEEP` | 1 | 每个分支上至少保留的快照数量 |
| `history.expire.max-ref-age-ms` | `MAX_REF_AGE_MS` | `Long.MAX_VALUE` | 引用（branch/tag）的最大存活时间。默认永不过期 |

```java
// 第371-381行
public static final String GC_ENABLED = "gc.enabled";
public static final boolean GC_ENABLED_DEFAULT = true;

public static final String MAX_SNAPSHOT_AGE_MS = "history.expire.max-snapshot-age-ms";
public static final long MAX_SNAPSHOT_AGE_MS_DEFAULT = 5 * 24 * 60 * 60 * 1000; // 5 days

public static final String MIN_SNAPSHOTS_TO_KEEP = "history.expire.min-snapshots-to-keep";
public static final int MIN_SNAPSHOTS_TO_KEEP_DEFAULT = 1;

public static final String MAX_REF_AGE_MS = "history.expire.max-ref-age-ms";
public static final long MAX_REF_AGE_MS_DEFAULT = Long.MAX_VALUE;
```

**保留策略的优先级**：
1. `minSnapshotsToKeep` 优先：即使快照已超过 `maxSnapshotAgeMs`，只要保留数量不足 `minSnapshotsToKeep`，就不会被过期
2. 分支级别设置覆盖全局设置：`SnapshotRef` 上的 `minSnapshotsToKeep` 和 `maxSnapshotAgeMs` 会覆盖表级属性
3. `main` 分支永远保留：`computeRetainedRefs` 中对 main 分支直接跳过检查

**实际应用示例**：

```sql
-- 设置保留策略
ALTER TABLE db.my_table SET TBLPROPERTIES (
  'history.expire.max-snapshot-age-ms' = '86400000',     -- 1天
  'history.expire.min-snapshots-to-keep' = '5'           -- 至少保留5个
);

-- 为特定分支设置不同策略
ALTER TABLE db.my_table CREATE BRANCH audit_branch
  RETAIN 10 SNAPSHOTS
  WITH SNAPSHOT RETENTION 7 DAYS;

-- 手动过期快照
CALL catalog_name.system.expire_snapshots(
  table => 'db.my_table',
  older_than => TIMESTAMP '2024-06-01 00:00:00',
  retain_last => 3
);
```

---

## 11. 快照过多的性能影响与最佳实践

### 性能影响

1. **元数据文件膨胀**：每个快照对应一组 manifest list 和 manifest 文件。快照越多，元数据越大。`TableMetadata` 中的 `snapshots` 列表、`snapshotLog` 和 `refs` 都会增长。

2. **规划时间增加**：
   - `ExpireSnapshots` 需要遍历所有快照的 manifest 来判断文件引用关系
   - `ReachableFileCleanup` 需要构建完整的可达文件集，内存和时间消耗与快照数成正比
   - 表的 `planFiles()` 本身不受快照数影响（只读当前快照），但增量扫描（`IncrementalAppendScan`）可能受影响

3. **元数据文件读取**：`TableMetadata` 文件（JSON格式）包含所有快照信息，快照越多文件越大，加载时间越长。v4 格式支持懒加载快照来缓解此问题（`ensureSnapshotsLoaded` 方法，第534-548行）。

4. **时间旅行查询的间接影响**：`snapshotIdAsOfTime` 遍历 `SnapshotLog`，时间复杂度为 O(n)。虽然通常很快，但日志条目过多时也会有开销。

### 最佳实践

**1. 定期执行 ExpireSnapshots**

```sql
-- 推荐：保留7天、至少10个快照
CALL catalog.system.expire_snapshots(
  table => 'db.my_table',
  older_than => TIMESTAMP '${current_date - 7 days}',
  retain_last => 10,
  stream_results => true  -- 流式模式避免 driver OOM
);
```

**2. 定期执行 RemoveOrphanFiles**

```sql
-- 建议在 expire_snapshots 之后执行，使用安全的3天保留期
CALL catalog.system.remove_orphan_files(
  table => 'db.my_table',
  older_than => TIMESTAMP '${current_date - 3 days}'
);
```

**3. 根据业务设置表属性**

```sql
-- 频繁写入的表：短保留、多快照
ALTER TABLE db.high_freq_table SET TBLPROPERTIES (
  'history.expire.max-snapshot-age-ms' = '172800000',  -- 2天
  'history.expire.min-snapshots-to-keep' = '20'
);

-- 审计类表：长保留
ALTER TABLE db.audit_table SET TBLPROPERTIES (
  'history.expire.max-snapshot-age-ms' = '2592000000',  -- 30天
  'history.expire.min-snapshots-to-keep' = '100'
);
```

**4. 利用分支隔离 WAP 工作流**

```sql
-- 创建审计分支（有独立的保留策略）
ALTER TABLE db.my_table CREATE BRANCH audit_branch
  AS OF VERSION 12345
  RETAIN 5 SNAPSHOTS;

-- 审计完成后 fast-forward
CALL catalog.system.fast_forward('db.my_table', 'main', 'audit_branch');
```

**5. 使用 Tag 标记重要版本**

```sql
-- 标记发布版本
ALTER TABLE db.my_table CREATE TAG release_v1
  AS OF VERSION 123456;

-- tag 有自己的过期策略
ALTER TABLE db.my_table ALTER TAG release_v1
  SET MAX REF AGE 90 DAYS;
```

**6. 清理过期的元数据**

```sql
-- 启用元数据清理（移除不再被任何快照引用的 schema 和 partition spec）
CALL catalog.system.expire_snapshots(
  table => 'db.my_table',
  clean_expired_metadata => true
);
```

---

## 12. 总结

Iceberg 的时间旅行与快照生命周期管理是一套精心设计的系统，核心要点如下：

1. **数据模型**：`Snapshot` 通过 `parentId` 形成链表，`SnapshotLog` 记录时间戳到快照的映射，`SnapshotRef` 提供命名引用机制（branch/tag）。三者共同支撑时间旅行能力。

2. **时间旅行查询**：
   - `TIMESTAMP AS OF`：通过 `SnapshotLog` 查找不超过给定时间戳的最新快照
   - `VERSION AS OF`：直接使用 snapshot ID 或解析 ref 名称
   - Core 层统一通过 `SnapshotScan.useSnapshot()`/`useRef()`/`asOfTime()` 实现

3. **引擎集成**：
   - Spark 通过 `loadTable(ident, version/timestamp)` API 和 `TimeTravel` sealed interface（v4.1）
   - Flink 通过 `snapshot-id`/`tag`/`branch`/`as-of-timestamp` 配置选项

4. **快照过期**：`RemoveSnapshots` 综合考虑分支保留策略、引用过期、最小保留数量来决定哪些快照可以被安全删除，然后通过 `IncrementalFileCleanup` 或 `ReachableFileCleanup` 清理关联文件。

5. **安全保障**：
   - `gc.enabled` 全局开关防止误操作
   - `olderThan` 安全期防止删除正在写入的文件
   - SnapshotLog 的 gap 检测防止时间旅行返回错误结果
   - cherry-pick 的重复检测防止 WAP ID 被多次发布
   - rollback 只允许回退到祖先快照

6. **最佳实践**：根据写入频率和业务需求设置合理的快照保留策略，定期执行 `expire_snapshots` 和 `remove_orphan_files`，善用 branch 和 tag 进行工作流管理。

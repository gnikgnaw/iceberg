# Apache Iceberg 乐观并发控制（OCC）与冲突解决机制深度分析

> 基于 Apache Iceberg 源码的深度剖析，版本基于当前 main 分支。

---

## 目录

- [1. 概述：为什么 Iceberg 采用 OCC](#1-概述为什么-iceberg-采用-occ)
- [2. OCC 的基石：TableOperations.commit()](#2-occ-的基石tableoperationscommit)
  - [2.1 接口契约](#21-接口契约)
  - [2.2 BaseMetastoreTableOperations 的实现](#22-basemetastoretableoperations-的实现)
  - [2.3 Hive Metastore 的锁机制](#23-hive-metastore-的锁机制)
- [3. SnapshotProducer：重试循环的核心引擎](#3-snapshotproducer重试循环的核心引擎)
  - [3.1 commit() 方法的重试机制](#31-commit-方法的重试机制)
  - [3.2 apply() 方法：生成新快照](#32-apply-方法生成新快照)
  - [3.3 validate() 方法：冲突检测的入口](#33-validate-方法冲突检测的入口)
- [4. BaseTransaction：多步操作的原子性保证](#4-basetransaction多步操作的原子性保证)
  - [4.1 事务类型](#41-事务类型)
  - [4.2 TransactionTableOperations：内存中的虚拟提交](#42-transactiontableoperations内存中的虚拟提交)
  - [4.3 commitSimpleTransaction：重放机制](#43-commitsimpletransaction重放机制)
- [5. MergingSnapshotProducer：冲突检测的核心逻辑](#5-mergingsnapshotproducer冲突检测的核心逻辑)
  - [5.1 validationHistory：构建验证窗口](#51-validationhistory构建验证窗口)
  - [5.2 validateAddedDataFiles：检测新增数据文件冲突](#52-validateaddeddatafiles检测新增数据文件冲突)
  - [5.3 validateNoNewDeleteFiles：检测新增删除文件冲突](#53-validatenewnewdeletefiles检测新增删除文件冲突)
  - [5.4 validateNoNewDeletesForDataFiles：保护被替换的数据文件](#54-validatenewnewdeletesfordatafiles保护被替换的数据文件)
  - [5.5 validateDeletedDataFiles：检测并发数据文件删除](#55-validatedeleteddatafiles检测并发数据文件删除)
  - [5.6 validateAddedDVs：DV 去重校验](#56-validateaddeddvsdv-去重校验)
  - [5.7 validateDataFilesExist：引用完整性校验](#57-validatedatafilesexist引用完整性校验)
- [6. 各类操作的冲突校验差异](#6-各类操作的冲突校验差异)
  - [6.1 AppendFiles（FastAppend / MergeAppend）](#61-appendfilesfastappend--mergeappend)
  - [6.2 OverwriteFiles（BaseOverwriteFiles）](#62-overwritefilesbaseoverwritefiles)
  - [6.3 RewriteFiles（BaseRewriteFiles）](#63-rewritefilesbaserewritefiles)
  - [6.4 RowDelta（BaseRowDelta）](#64-rowdeltabaserowdelta)
  - [6.5 ReplacePartitions（BaseReplacePartitions）](#65-replacepartitionsbasereplacepartitions)
  - [6.6 DeleteFiles（StreamingDelete）](#66-deletefilesstreamingdelete)
  - [6.7 冲突校验对比总结表](#67-冲突校验对比总结表)
- [7. IsolationLevel：SERIALIZABLE vs SNAPSHOT](#7-isolationlevelserializable-vs-snapshot)
  - [7.1 隔离级别定义](#71-隔离级别定义)
  - [7.2 Spark 引擎中的实际应用](#72-spark-引擎中的实际应用)
  - [7.3 CopyOnWrite 模式下的隔离级别差异](#73-copyonwrite-模式下的隔离级别差异)
  - [7.4 MergeOnRead 模式下的隔离级别差异](#74-mergeonread-模式下的隔离级别差异)
- [8. Tasks 框架：指数退避重试策略](#8-tasks-框架指数退避重试策略)
  - [8.1 重试参数配置](#81-重试参数配置)
  - [8.2 指数退避算法实现](#82-指数退避算法实现)
  - [8.3 重试流程详解](#83-重试流程详解)
- [9. 并发场景分析：何时冲突、何时不冲突](#9-并发场景分析何时冲突何时不冲突)
  - [9.1 不冲突场景](#91-不冲突场景)
  - [9.2 冲突场景](#92-冲突场景)
  - [9.3 条件性冲突场景](#93-条件性冲突场景)
- [10. 与 Delta Lake、Hudi 的 OCC 策略对比](#10-与-delta-lakehudi-的-occ-策略对比)
  - [10.1 核心机制对比](#101-核心机制对比)
  - [10.2 冲突检测粒度对比](#102-冲突检测粒度对比)
  - [10.3 重试策略对比](#103-重试策略对比)
  - [10.4 隔离级别对比](#104-隔离级别对比)
- [11. 总结](#11-总结)

---

## 1. 概述：为什么 Iceberg 采用 OCC

Apache Iceberg 的表格式基于不可变的元数据文件链：每次提交都会生成一个新的 `TableMetadata` 文件（包含新的 Snapshot），旧的元数据文件永远不会被修改。这种设计天然适合乐观并发控制（Optimistic Concurrency Control, OCC），其核心思想是：

1. **读取当前状态**（base metadata）
2. **在本地计算变更**（生成新的 manifest 文件、manifest list 等）
3. **尝试原子性提交**（compare-and-swap：如果 base 仍然是最新的，则提交成功）
4. **冲突时重试**（如果 base 已过时，refresh 后重新 apply + validate）

与悲观锁相比，OCC 的优势在于：
- 不需要集中式的锁管理器
- 并行写入器之间没有相互阻塞
- 特别适合对象存储（S3/GCS/ADLS）等不支持原子 rename 的环境
- 读写之间完全不阻塞（快照隔离）

---

## 2. OCC 的基石：TableOperations.commit()

### 2.1 接口契约

`TableOperations` 是 Iceberg 的 SPI（Service Provider Interface），定义了元数据访问和更新的抽象。其中最核心的是 `commit` 方法：

```java
// 文件：core/src/main/java/org/apache/iceberg/TableOperations.java，第 64 行
void commit(TableMetadata base, TableMetadata metadata);
```

该方法的语义是：**将 base 元数据替换为新的 metadata，但必须保证原子性**。接口文档（第 47-59 行）明确要求：

- **实现必须检查 base metadata 是否为当前版本**，以避免覆盖其他写入者的更新
- 一旦原子提交操作成功，实现**不得执行任何可能失败的操作**
- 当**无法确定提交是否成功时**（如网络分区），必须抛出 `CommitStateUnknownException`
- 其他所有异常都将被视为提交失败

这是一个典型的 CAS（Compare-And-Swap）语义接口。

### 2.2 BaseMetastoreTableOperations 的实现

`BaseMetastoreTableOperations` 是大多数 Catalog 实现的基类，其 `commit` 方法实现了 OCC 的第一道防线：

```java
// 文件：core/src/main/java/org/apache/iceberg/BaseMetastoreTableOperations.java，第 109-135 行
@Override
public void commit(TableMetadata base, TableMetadata metadata) {
    // if the metadata is already out of date, reject it
    if (base != current()) {
        if (base != null) {
            throw new CommitFailedException("Cannot commit: stale table metadata");
        } else {
            throw new AlreadyExistsException("Table already exists: %s", tableName());
        }
    }
    // if the metadata is not changed, return early
    if (base == metadata) {
        LOG.info("Nothing to commit.");
        return;
    }

    long start = System.currentTimeMillis();
    doCommit(base, metadata);
    // ...
}
```

**关键逻辑**：
- 第 111 行使用 **引用相等性**（`!=`）比较 base 和 current，这意味着只有当 base 就是最后一次 refresh 返回的同一个对象时才允许提交
- 如果 base 已经过时，抛出 `CommitFailedException`，触发上层的重试
- `doCommit` 由具体的 Catalog 实现（如 HiveCatalog、RESTCatalog 等）提供原子性保证

### 2.3 Hive Metastore 的锁机制

值得一提的是，HiveTableOperations 在 `doCommit` 中使用了 Hive Metastore 的锁来保证原子性：

```java
// 文件：hive-metastore/src/main/java/org/apache/iceberg/hive/HiveTableOperations.java，约第 266 行
HiveLock lock = lockObject(base != null ? base : tableMetadata);
try {
    lock.lock();
    Table tbl = loadHmsTable();
    // ... 检查并发冲突，执行 alterTable ...
} finally {
    lock.unlock();
}
```

这说明 Iceberg 的 OCC 是**分层设计**的：上层（SnapshotProducer）通过 retry 循环处理冲突，底层（具体 Catalog）通过各自的原子性保证（Hive 锁、REST API 的条件更新、文件系统的 atomic rename 等）确保每次 commit 调用的原子性。

---

## 3. SnapshotProducer：重试循环的核心引擎

`SnapshotProducer` 是所有产生新 Snapshot 的操作的抽象基类。它实现了 OCC 的核心循环：**apply -> validate -> commit -> retry**。

### 3.1 commit() 方法的重试机制

```java
// 文件：core/src/main/java/org/apache/iceberg/SnapshotProducer.java，第 458-550 行
@Override
public void commit() {
    AtomicLong newSnapshotId = new AtomicLong(-1L);
    try (Timed ignore = commitMetrics().totalDuration().start()) {
        try {
            Tasks.foreach(ops)
                .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
                .exponentialBackoff(
                    base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
                    base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
                    base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
                    2.0 /* exponential */)
                .onlyRetryOn(CommitFailedException.class)
                .run(
                    taskOps -> {
                        Snapshot newSnapshot = apply();        // 1. 生成新快照
                        newSnapshotId.set(newSnapshot.snapshotId());
                        TableMetadata.Builder update = TableMetadata.buildFrom(base);
                        // ... 构建新的 metadata ...
                        taskOps.commit(base, updated.withUUID()); // 2. 尝试提交
                    });
        } catch (CommitStateUnknownException commitStateUnknownException) {
            throw commitStateUnknownException;  // 不可重试，直接抛出
        } catch (RuntimeException e) {
            if (!strictCleanup || e instanceof CleanableFailure) {
                Exceptions.suppressAndThrow(e, this::cleanAll);
            }
            throw e;
        }
        // ... 提交成功后的清理 ...
    }
}
```

**核心流程**：

1. **apply()**：读取最新的 base metadata，运行 validate 校验，生成新的 manifest list
2. **commit(base, updated)**：尝试 CAS 提交
3. 如果提交失败抛出 `CommitFailedException`，则自动重试（回到步骤 1）
4. 仅在 `CommitFailedException` 时重试，其他异常（如 `ValidationException`）直接失败
5. `CommitStateUnknownException` 被特殊处理：直接向上抛出，不做任何清理

### 3.2 apply() 方法：生成新快照

每次重试都会调用 `apply()` 重新基于最新的 base 生成快照：

```java
// 文件：core/src/main/java/org/apache/iceberg/SnapshotProducer.java，第 272-348 行
@Override
public Snapshot apply() {
    refresh();  // 刷新 base 到最新状态
    Snapshot parentSnapshot = SnapshotUtil.latestSnapshot(base, targetBranch);
    long sequenceNumber = base.nextSequenceNumber();

    runValidations(parentSnapshot);  // 冲突检测
    List<ManifestFile> manifests = apply(base, parentSnapshot);  // 子类实现

    // 写出 manifest list 文件
    OutputFile manifestList = manifestListPath();
    ManifestListWriter writer = ManifestLists.write(...);
    // ... 写入 manifest files ...

    return new BaseSnapshot(...);
}
```

关键点：
- **第 273 行 `refresh()`**：每次重试都会从底层存储重新读取最新的 metadata，确保基于最新状态进行操作
- **第 279 行 `runValidations()`**：在 apply 之前先做冲突检测，如果发现冲突则抛出 `ValidationException`（**不可重试**，直接失败）
- **第 281 行 `apply(base, parentSnapshot)`**：子类实现具体的 manifest 合并逻辑

### 3.3 validate() 方法：冲突检测的入口

```java
// 文件：core/src/main/java/org/apache/iceberg/SnapshotProducer.java，第 260-261 行
protected void validate(TableMetadata currentMetadata, Snapshot snapshot) {}
```

这是一个空方法，由子类（如 `MergingSnapshotProducer` 的各种操作实现类）根据需要覆盖。`runValidations` 方法在调用 `validate` 后还会验证快照祖先链的有效性（第 350-362 行）。

---

## 4. BaseTransaction：多步操作的原子性保证

`BaseTransaction` 允许将多个表操作（如 schema 变更 + append 文件 + 更新属性）组合为一个原子性提交。

### 4.1 事务类型

```java
// 文件：core/src/main/java/org/apache/iceberg/BaseTransaction.java，第 59-64 行
enum TransactionType {
    CREATE_TABLE,
    REPLACE_TABLE,
    CREATE_OR_REPLACE_TABLE,
    SIMPLE
}
```

大多数业务场景使用的是 `SIMPLE` 类型事务。

### 4.2 TransactionTableOperations：内存中的虚拟提交

事务内部的每个操作（如 `newAppend().appendFile(f).commit()`）并不直接提交到底层存储，而是提交到一个 `TransactionTableOperations` 对象：

```java
// 文件：core/src/main/java/org/apache/iceberg/BaseTransaction.java，第 483-535 行
public class TransactionTableOperations implements TableOperations {
    @Override
    public void commit(TableMetadata underlyingBase, TableMetadata metadata) {
        if (underlyingBase != current) {
            throw new CommitFailedException("Table metadata refresh is required");
        }
        BaseTransaction.this.current = metadata;  // 仅更新内存中的 current
        this.tempOps = ops.temp(metadata);
        BaseTransaction.this.hasLastOpCommitted = true;
    }
    // ...
}
```

关键设计：每个子操作的 "commit" 实际上只是在内存中更新 `current` metadata。真正的 CAS 提交发生在 `commitTransaction()` 时。

### 4.3 commitSimpleTransaction：重放机制

```java
// 文件：core/src/main/java/org/apache/iceberg/BaseTransaction.java，第 351-418 行
private void commitSimpleTransaction() {
    if (base == current) {
        return;  // 无变更则跳过
    }

    Tasks.foreach(ops)
        .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(...)
        .onlyRetryOn(CommitFailedException.class)
        .run(underlyingOps -> {
            applyUpdates(underlyingOps);  // 在最新 base 上重放所有操作
            underlyingOps.commit(base, current);
        });
}
```

`applyUpdates` 方法（第 443-459 行）实现了**操作重放**：

```java
private void applyUpdates(TableOperations underlyingOps) {
    if (base != underlyingOps.refresh()) {
        this.base = underlyingOps.current();
        this.current = underlyingOps.current();
        for (PendingUpdate update : updates) {
            try {
                update.commit();  // 在新的 base 上重新执行每个操作
            } catch (CommitFailedException e) {
                throw new PendingUpdateFailedException(e);
            }
        }
    }
}
```

关键点：
- 如果 base 与底层存储一致（没有并发修改），直接提交
- 如果 base 已过时，会**重放事务中的所有操作**到最新的 base 上
- 如果重放过程中任何操作的 validate 失败（抛出 `CommitFailedException`），则包装为 `PendingUpdateFailedException` **跳出重试循环**（因为逻辑冲突无法通过重试解决）

---

## 5. MergingSnapshotProducer：冲突检测的核心逻辑

`MergingSnapshotProducer` 是 `OverwriteFiles`、`RowDelta`、`RewriteFiles`、`ReplacePartitions`、`DeleteFiles` 等操作的共同基类。它提供了一套丰富的验证方法。

### 5.1 validationHistory：构建验证窗口

所有验证方法的底层都依赖于 `validationHistory`，它负责收集从 startingSnapshot 到当前 parent snapshot 之间的所有相关 manifest：

```java
// 文件：core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java，第 909-949 行
private Pair<List<ManifestFile>, Set<Long>> validationHistory(
    TableMetadata base,
    Long startingSnapshotId,
    Set<String> matchingOperations,
    ManifestContent content,
    Snapshot parent) {
    List<ManifestFile> manifests = Lists.newArrayList();
    Set<Long> newSnapshots = Sets.newHashSet();

    Iterable<Snapshot> snapshots =
        SnapshotUtil.ancestorsBetween(parent.snapshotId(), startingSnapshotId, base::snapshot);
    for (Snapshot currentSnapshot : snapshots) {
        if (matchingOperations.contains(currentSnapshot.operation())) {
            newSnapshots.add(currentSnapshot.snapshotId());
            // 收集该 snapshot 新增的 manifest 文件
            if (content == ManifestContent.DATA) {
                for (ManifestFile manifest : currentSnapshot.dataManifests(ops().io())) {
                    if (manifest.snapshotId() == currentSnapshot.snapshotId()) {
                        manifests.add(manifest);
                    }
                }
            } else {
                for (ManifestFile manifest : currentSnapshot.deleteManifests(ops().io())) {
                    if (manifest.snapshotId() == currentSnapshot.snapshotId()) {
                        manifests.add(manifest);
                    }
                }
            }
        }
    }
    return Pair.of(manifests, newSnapshots);
}
```

关键参数说明：
- **startingSnapshotId**：操作开始时读取的快照 ID（即"我看到的"版本），验证只检查这之后的变更
- **matchingOperations**：只关注特定类型的操作（如 APPEND、OVERWRITE、DELETE、REPLACE）
- **content**：区分数据 manifest 和删除文件 manifest
- 返回值中 `manifests` 是期间新增的 manifest 文件，`newSnapshots` 是相关的 snapshot ID 集合

这种设计意味着：**验证只检查从 startingSnapshot 到当前最新状态之间的增量变更**，而不是全表扫描。

### 5.2 validateAddedDataFiles：检测新增数据文件冲突

检测是否有并发事务在同一范围内新增了数据文件：

```java
// 文件：core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java，第 359-408 行
protected void validateAddedDataFiles(
    TableMetadata base, Long startingSnapshotId,
    Expression conflictDetectionFilter, Snapshot parent) {
    CloseableIterable<ManifestEntry<DataFile>> conflictEntries =
        addedDataFiles(base, startingSnapshotId, conflictDetectionFilter, null, parent);
    try (CloseableIterator<ManifestEntry<DataFile>> conflicts = conflictEntries.iterator()) {
        if (conflicts.hasNext()) {
            throw new ValidationException(
                "Found conflicting files that can contain records matching %s: %s",
                conflictDetectionFilter, ...);
        }
    }
}
```

该方法还有一个基于 PartitionSet 的重载版本，用于分区级别的精确冲突检测。底层通过 `ManifestGroup` 配合 `filterData()` 和 `filterManifestEntries()` 进行高效过滤。

### 5.3 validateNoNewDeleteFiles：检测新增删除文件冲突

```java
// 文件：core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java，第 558-566 行
protected void validateNoNewDeleteFiles(
    TableMetadata base, Long startingSnapshotId, Expression dataFilter, Snapshot parent) {
    DeleteFileIndex deletes = addedDeleteFiles(base, startingSnapshotId, dataFilter, null, parent);
    ValidationException.check(
        deletes.isEmpty(),
        "Found new conflicting delete files that can apply to records matching %s: %s",
        dataFilter,
        Iterables.transform(deletes.referencedDeleteFiles(), ContentFile::location));
}
```

### 5.4 validateNoNewDeletesForDataFiles：保护被替换的数据文件

这个验证确保要替换的数据文件没有被其他并发操作添加了新的 delete file：

```java
// 文件：core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java，第 515-547 行
private void validateNoNewDeletesForDataFiles(
    TableMetadata base, Long startingSnapshotId, Expression dataFilter,
    Iterable<DataFile> dataFiles, boolean ignoreEqualityDeletes, Snapshot parent) {
    DeleteFileIndex deletes = addedDeleteFiles(base, startingSnapshotId, dataFilter, null, parent);
    long startingSequenceNumber = startingSequenceNumber(base, startingSnapshotId);
    for (DataFile dataFile : dataFiles) {
        DeleteFile[] deleteFiles = deletes.forDataFile(startingSequenceNumber, dataFile);
        if (ignoreEqualityDeletes) {
            ValidationException.check(
                Arrays.stream(deleteFiles)
                    .noneMatch(deleteFile -> deleteFile.content() == FileContent.POSITION_DELETES),
                "Cannot commit, found new position delete for replaced data file: %s", dataFile);
        } else {
            ValidationException.check(
                deleteFiles.length == 0,
                "Cannot commit, found new delete for replaced data file: %s", dataFile);
        }
    }
}
```

**特别之处**：当 `ignoreEqualityDeletes` 为 true 时（如 RewriteFiles 操作使用了 `setNewDataFilesDataSequenceNumber`），会跳过 equality delete 的检测。这是因为如果新数据文件保持了与被替换文件相同的序列号，equality deletes 仍然对新文件有效。

### 5.5 validateDeletedDataFiles：检测并发数据文件删除

```java
// 文件：core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java，第 632-650 行
protected void validateDeletedDataFiles(
    TableMetadata base, Long startingSnapshotId, Expression dataFilter, Snapshot parent) {
    CloseableIterable<ManifestEntry<DataFile>> conflictEntries =
        deletedDataFiles(base, startingSnapshotId, dataFilter, null, parent);
    try (CloseableIterator<ManifestEntry<DataFile>> conflicts = conflictEntries.iterator()) {
        if (conflicts.hasNext()) {
            throw new ValidationException(
                "Found conflicting deleted files that can contain records matching %s: %s", ...);
        }
    }
}
```

此验证检查是否有其他事务在相同范围内删除了数据文件，主要用于 `OverwriteFiles` 和 `ReplacePartitions`。

### 5.6 validateAddedDVs：DV 去重校验

Iceberg V3 引入了 Deletion Vectors（DV），对同一个数据文件最多只能有一个 DV。因此需要校验并发事务是否为同一个数据文件添加了 DV：

```java
// 文件：core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java，第 821-874 行
protected void validateAddedDVs(
    TableMetadata base, Long startingSnapshotId,
    Expression conflictDetectionFilter, Snapshot parent) {
    // ... 收集新增的 delete manifest ...
    Tasks.foreach(matchingManifests)
        .run(manifest -> validateAddedDVs(manifest, conflictDetectionFilter, newSnapshotIds));
}

private void validateAddedDVs(ManifestFile manifest, Expression filter, Set<Long> newSnapshotIds) {
    // ... 遍历 manifest entries ...
    for (ManifestEntry<DeleteFile> entry : entries) {
        DeleteFile file = entry.file();
        if (newSnapshotIds.contains(entry.snapshotId()) && ContentFileUtil.isDV(file)) {
            ValidationException.check(
                !dvsByReferencedFile.containsKey(file.referencedDataFile()),
                "Found concurrently added DV for %s: %s", ...);
        }
    }
}
```

### 5.7 validateDataFilesExist：引用完整性校验

确保当前操作引用的数据文件（如要添加 delete file 的目标数据文件）仍然存在，没有被并发操作删除：

```java
// 文件：core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java，第 769-818 行
protected void validateDataFilesExist(
    TableMetadata base, Long startingSnapshotId, CharSequenceSet requiredDataFiles,
    boolean skipDeletes, Expression conflictDetectionFilter, Snapshot parent) {
    // ... 查找期间被删除的文件 ...
    ManifestGroup matchingDeletesGroup = new ManifestGroup(...)
        .filterManifestEntries(entry ->
            entry.status() != ManifestEntry.Status.ADDED
            && newSnapshots.contains(entry.snapshotId())
            && requiredDataFiles.contains(entry.file().location()))
        // ...
    try (CloseableIterator<ManifestEntry<DataFile>> deletes = ...) {
        if (deletes.hasNext()) {
            throw new ValidationException("Cannot commit, missing data files: %s", ...);
        }
    }
}
```

---

## 6. 各类操作的冲突校验差异

### 6.1 AppendFiles（FastAppend / MergeAppend）

**FastAppend** 继承自 `SnapshotProducer`，**没有任何 validate 方法覆盖**。它仅仅向表中追加新文件，不需要检测冲突。

**MergeAppend** 继承自 `MergingSnapshotProducer`，同样**没有覆盖 validate 方法**。

这意味着：**纯追加操作之间永远不会冲突**。多个并发 append 操作可以安全地同时进行，每个操作只需处理 CAS 提交层面的冲突（通过自动重试解决）。

### 6.2 OverwriteFiles（BaseOverwriteFiles）

```java
// 文件：core/src/main/java/org/apache/iceberg/BaseOverwriteFiles.java，第 136-179 行
@Override
protected void validate(TableMetadata base, Snapshot parent) {
    // 1. 验证新增文件匹配覆盖过滤条件
    if (validateAddedFilesMatchOverwriteFilter) { ... }

    // 2. 验证没有并发新增的冲突数据文件
    if (validateNewDataFiles) {
        validateAddedDataFiles(base, startingSnapshotId, dataConflictDetectionFilter(), parent);
    }

    // 3. 验证没有并发新增的删除文件和并发删除的数据文件
    if (validateNewDeletes) {
        if (rowFilter() != Expressions.alwaysFalse()) {
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

OverwriteFiles 的冲突检测最为全面，因为它涉及同时删除旧文件和添加新文件。用户需要**显式调用**：
- `validateNoConflictingData()` 启用数据文件冲突检测
- `validateNoConflictingDeletes()` 启用删除文件冲突检测
- `conflictDetectionFilter(expr)` 设置冲突检测的范围过滤器
- `validateFromSnapshot(id)` 设置验证的起始快照

### 6.3 RewriteFiles（BaseRewriteFiles）

```java
// 文件：core/src/main/java/org/apache/iceberg/BaseRewriteFiles.java，第 135-142 行
@Override
protected void validate(TableMetadata base, Snapshot parent) {
    validateReplacedAndAddedFiles();  // 基础参数校验
    if (!replacedDataFiles.isEmpty()) {
        validateNoNewDeletesForDataFiles(base, startingSnapshotId, replacedDataFiles, parent);
    }
}
```

RewriteFiles（用于 compaction）的验证相对简单：
- 确保被替换的数据文件没有被并发操作添加新的 delete file
- **不检测**并发新增的数据文件（因为 compaction 不改变数据内容，只改变存储布局）
- 构造函数中调用了 `failMissingDeletePaths()`，确保被删除的文件路径必须存在

### 6.4 RowDelta（BaseRowDelta）

```java
// 文件：core/src/main/java/org/apache/iceberg/BaseRowDelta.java，第 132-173 行
@Override
protected void validate(TableMetadata base, Snapshot parent) {
    if (parent != null) {
        // 1. 验证起始快照的祖先关系
        if (startingSnapshotId != null) {
            Preconditions.checkArgument(
                SnapshotUtil.isAncestorOf(parent.snapshotId(), startingSnapshotId, base::snapshot), ...);
        }
        // 2. 验证引用的数据文件仍然存在
        if (!referencedDataFiles.isEmpty()) {
            validateDataFilesExist(base, startingSnapshotId, referencedDataFiles, ...);
        }
        // 3. 验证无并发新增数据文件
        if (validateNewDataFiles) {
            validateAddedDataFiles(base, startingSnapshotId, conflictDetectionFilter, parent);
        }
        // 4. 验证无并发新增/删除的 delete 文件
        if (validateNewDeleteFiles) {
            if (!removedDataFiles.isEmpty()) {
                validateNoNewDeletesForDataFiles(...);
            }
            validateNoNewDeleteFiles(base, startingSnapshotId, conflictDetectionFilter, parent);
        }
        // 5. 验证删除的数据文件与新增 DV 不冲突
        validateNoConflictingFileAndPositionDeletes();
        // 6. 验证并发 DV 不冲突
        validateAddedDVs(base, startingSnapshotId, conflictDetectionFilter, parent);
    }
}
```

RowDelta 是冲突检测最复杂的操作，因为它同时支持添加数据行、添加删除标记、移除数据文件和删除文件。特别需要注意的是：
- **validateAddedDVs** 是 RowDelta 特有的验证（第 172 行），确保同一数据文件不会有两个并发 DV
- **validateNoConflictingFileAndPositionDeletes**（第 180-193 行）确保不会同时删除一个数据文件又为它添加 DV

### 6.5 ReplacePartitions（BaseReplacePartitions）

```java
// 文件：core/src/main/java/org/apache/iceberg/BaseReplacePartitions.java，第 89-110 行
@Override
public void validate(TableMetadata currentMetadata, Snapshot parent) {
    if (validateConflictingData) {
        if (dataSpec().isUnpartitioned()) {
            validateAddedDataFiles(currentMetadata, startingSnapshotId, Expressions.alwaysTrue(), parent);
        } else {
            validateAddedDataFiles(currentMetadata, startingSnapshotId, replacedPartitions, parent);
        }
    }
    if (validateConflictingDeletes) {
        if (dataSpec().isUnpartitioned()) {
            validateDeletedDataFiles(currentMetadata, startingSnapshotId, Expressions.alwaysTrue(), parent);
            validateNoNewDeleteFiles(currentMetadata, startingSnapshotId, Expressions.alwaysTrue(), parent);
        } else {
            validateDeletedDataFiles(currentMetadata, startingSnapshotId, replacedPartitions, parent);
            validateNoNewDeleteFiles(currentMetadata, startingSnapshotId, replacedPartitions, parent);
        }
    }
}
```

ReplacePartitions 的验证基于**分区集合**过滤。对于分区表，只验证被替换分区范围内的冲突；对于非分区表，则验证全表范围。

### 6.6 DeleteFiles（StreamingDelete）

```java
// 文件：core/src/main/java/org/apache/iceberg/StreamingDelete.java，第 72-76 行
@Override
protected void validate(TableMetadata base, Snapshot parent) {
    if (validateFilesToDeleteExist) {
        failMissingDeletePaths();
    }
}
```

StreamingDelete 的验证最为简单——只有一个可选的检查：确保要删除的文件确实存在。它**不检测**任何并发冲突，因为删除操作是幂等的。

### 6.7 冲突校验对比总结表

| 操作 | 新增数据冲突 | 新增 Delete 冲突 | 删除数据冲突 | 引用文件存在 | DV 冲突 | 默认启用 |
|------|:-----------:|:---------------:|:----------:|:----------:|:------:|:------:|
| **FastAppend** | - | - | - | - | - | - |
| **MergeAppend** | - | - | - | - | - | - |
| **OverwriteFiles** | 可选 | 可选 | 可选 | - | - | 否 |
| **RewriteFiles** | - | 部分(DV) | - | - | - | 自动 |
| **RowDelta** | 可选 | 可选 | - | 可选 | 自动 | 部分 |
| **ReplacePartitions** | 可选 | 可选 | 可选 | - | - | 否 |
| **StreamingDelete** | - | - | - | 可选 | - | 否 |

"可选"表示需要显式调用 `validateNoConflictingData()` / `validateNoConflictingDeletes()` 等方法。"自动"表示在特定条件下（如有被替换的数据文件或新增 DV）自动执行。

---

## 7. IsolationLevel：SERIALIZABLE vs SNAPSHOT

### 7.1 隔离级别定义

```java
// 文件：core/src/main/java/org/apache/iceberg/IsolationLevel.java，第 25-41 行
/**
 * The serializable isolation level guarantees that an ongoing UPDATE/DELETE/MERGE operation
 * fails if a concurrent transaction commits a new file that might contain rows matching the
 * condition used in UPDATE/DELETE/MERGE. For example, if there is an ongoing update on a
 * subset of rows and a concurrent transaction adds a new file with records that potentially
 * match the update condition, the update operation must fail under the serializable isolation
 * but can still commit under the snapshot isolation.
 */
public enum IsolationLevel {
    SERIALIZABLE,
    SNAPSHOT;
}
```

两者的核心区别：
- **SERIALIZABLE**：验证并发事务新增的**数据文件**和**删除文件**是否与当前操作的条件范围冲突
- **SNAPSHOT**：仅验证并发事务新增的**删除文件**是否冲突，**允许**并发新增数据文件

### 7.2 Spark 引擎中的实际应用

#### PositionDelta（MergeOnRead）操作

```java
// 文件：spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkPositionDeltaWrite.java，第 274-304 行
if (scan != null) {
    Expression conflictDetectionFilter = scan.filter();
    rowDelta.conflictDetectionFilter(conflictDetectionFilter);
    rowDelta.validateDataFilesExist(referencedDataFiles);

    if (scan.snapshotId() != null) {
        rowDelta.validateFromSnapshot(scan.snapshotId());
    }

    if (command == UPDATE || command == MERGE) {
        rowDelta.validateDeletedFiles();
        rowDelta.validateNoConflictingDeleteFiles();  // 始终验证删除文件冲突
    }

    if (isolationLevel == SERIALIZABLE) {
        rowDelta.validateNoConflictingDataFiles();  // 仅 SERIALIZABLE 额外验证数据文件
    }
}
```

### 7.3 CopyOnWrite 模式下的隔离级别差异

```java
// 文件：spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java，第 477-546 行
switch (isolationLevel) {
    case SERIALIZABLE:
        commitWithSerializableIsolation(overwriteFiles, ...);
        break;
    case SNAPSHOT:
        commitWithSnapshotIsolation(overwriteFiles, ...);
        break;
}
```

**SERIALIZABLE 模式**（第 499-523 行）：
```java
overwriteFiles.conflictDetectionFilter(conflictDetectionFilter);
overwriteFiles.validateNoConflictingData();      // 检测并发新增数据
overwriteFiles.validateNoConflictingDeletes();   // 检测并发新增删除
```

**SNAPSHOT 模式**（第 525-546 行）：
```java
overwriteFiles.conflictDetectionFilter(conflictDetectionFilter);
overwriteFiles.validateNoConflictingDeletes();   // 仅检测并发新增删除
// 注意：不调用 validateNoConflictingData()
```

### 7.4 MergeOnRead 模式下的隔离级别差异

DynamicOverwrite 中同样体现了两种隔离级别的差异：

```java
// 文件：spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java，第 356-368 行
if (isolationLevel == SERIALIZABLE) {
    dynamicOverwrite.validateNoConflictingData();
    dynamicOverwrite.validateNoConflictingDeletes();
} else if (isolationLevel == SNAPSHOT) {
    dynamicOverwrite.validateNoConflictingDeletes();
}
```

**选择建议**：
- **SERIALIZABLE**：适用于对数据一致性要求极高的场景（如金融数据），但并发性较低
- **SNAPSHOT**：适用于 ETL 管道，允许并发写入不同分区或追加新数据，并发性更高

---

## 8. Tasks 框架：指数退避重试策略

### 8.1 重试参数配置

```java
// 文件：core/src/main/java/org/apache/iceberg/TableProperties.java，第 89-99 行
public static final String COMMIT_NUM_RETRIES = "commit.retry.num-retries";
public static final int COMMIT_NUM_RETRIES_DEFAULT = 4;                   // 最多重试 4 次

public static final String COMMIT_MIN_RETRY_WAIT_MS = "commit.retry.min-wait-ms";
public static final int COMMIT_MIN_RETRY_WAIT_MS_DEFAULT = 100;          // 最小等待 100ms

public static final String COMMIT_MAX_RETRY_WAIT_MS = "commit.retry.max-wait-ms";
public static final int COMMIT_MAX_RETRY_WAIT_MS_DEFAULT = 60 * 1000;    // 最大等待 60s

public static final String COMMIT_TOTAL_RETRY_TIME_MS = "commit.retry.total-timeout-ms";
public static final int COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT = 30 * 60 * 1000;  // 总超时 30 分钟
```

### 8.2 指数退避算法实现

```java
// 文件：core/src/main/java/org/apache/iceberg/util/Tasks.java，第 402-470 行
private <E extends Exception> void runTaskWithRetry(Task<I, E> task, I item) throws E {
    long start = System.currentTimeMillis();
    int attempt = 0;
    while (true) {
        attempt += 1;
        try {
            task.run(item);
            break;
        } catch (Exception e) {
            long durationMs = System.currentTimeMillis() - start;
            // 超过最大重试次数或总超时时间，则放弃
            if (attempt >= maxAttempts || (durationMs > maxDurationMs && attempt > 1)) {
                throw e;
            }
            // 检查是否应该重试（onlyRetryOn / stopRetryOn / shouldRetryPredicate）
            // ...

            // 计算退避时间
            int delayMs = (int) Math.min(
                minSleepTimeMs * Math.pow(scaleFactor, attempt - 1),
                (double) maxSleepTimeMs);
            int jitter = ThreadLocalRandom.current().nextInt(Math.max(1, (int) (delayMs * 0.1)));
            int sleepTimeMs = delayMs + jitter;

            TimeUnit.MILLISECONDS.sleep(sleepTimeMs);
        }
    }
}
```

### 8.3 重试流程详解

以默认配置为例，各次重试的等待时间大约为：

| 重试次数 | 基础延迟计算 | 基础延迟 | 加抖动后 |
|---------|-----------|---------|---------|
| 第 1 次 | 100 * 2^0 | 100ms | 100-110ms |
| 第 2 次 | 100 * 2^1 | 200ms | 200-220ms |
| 第 3 次 | 100 * 2^2 | 400ms | 400-440ms |
| 第 4 次 | 100 * 2^3 | 800ms | 800-880ms |

退避公式：`delay = min(minSleep * scaleFactor^(attempt-1), maxSleep) + jitter`

其中 jitter 为 `[0, delay * 0.1)` 的随机值，用于避免**惊群效应**（多个并发写入者在完全相同的时间重试）。

**双重终止条件**：
1. 重试次数达到 `maxAttempts`（默认 5 次，即 1 次初始 + 4 次重试）
2. 总耗时超过 `maxDurationMs`（默认 30 分钟），且至少已尝试 2 次

**异常分类**：
- `CommitFailedException`：可重试（CAS 失败，其他人先提交了）
- `ValidationException`：不可重试（逻辑冲突，重试也不会成功）
- `CommitStateUnknownException`：不可重试，直接透传给调用者

---

## 9. 并发场景分析：何时冲突、何时不冲突

### 9.1 不冲突场景

**场景 1：两个并发 Append 操作**
```
Writer A: table.newAppend().appendFile(fileA).commit()
Writer B: table.newAppend().appendFile(fileB).commit()
```
- **结果**：永远不冲突。两个 append 都没有 validate 逻辑。CAS 层面的冲突通过自动重试解决。后提交的 writer 会 refresh 到前者的结果，然后基于最新的 snapshot 重新 apply。

**场景 2：Append 与 RewriteFiles 并发**
```
Writer A: table.newAppend().appendFile(fileNew).commit()
Writer B: table.newRewrite().deleteFile(fileOld).addFile(fileCompacted).commit()
```
- **结果**：不冲突。Append 不改变已有文件，RewriteFiles 只验证被替换文件是否有新的 delete。两者操作的文件集合不相交。

**场景 3：不同分区的 OverwriteFiles（SNAPSHOT 隔离级别）**
```
Writer A: overwrite 分区 date=2024-01-01
Writer B: overwrite 分区 date=2024-01-02
```
- **结果**：不冲突。每个 overwrite 的 conflictDetectionFilter 过滤了不同的分区范围。

### 9.2 冲突场景

**场景 4：同分区的并发 OverwriteFiles（SERIALIZABLE）**
```
Writer A: overwrite WHERE category='electronics' （添加了 validateNoConflictingData）
Writer B: append 了一个包含 category='electronics' 的文件
```
- Writer A 在提交时发现 Writer B 已追加了匹配过滤条件的数据文件
- **结果**：Writer A 抛出 `ValidationException`，提交失败（不可重试）

**场景 5：RewriteFiles 与 RowDelta 并发**
```
Writer A: rewrite fileX -> fileX'（compaction）
Writer B: 对 fileX 添加了 position delete
```
- Writer A 验证 `validateNoNewDeletesForDataFiles`，发现 fileX 有了新的 position delete
- **结果**：Writer A 抛出 `ValidationException`，因为 compaction 后的 fileX' 不知道这些 delete

**场景 6：并发 RowDelta 对同一数据文件添加 DV**
```
Writer A: 对 data-file-1 添加 DV-A
Writer B: 对 data-file-1 添加 DV-B
```
- `validateAddedDVs` 检测到同一个 `referencedDataFile` 有并发 DV
- **结果**：后提交者抛出 `ValidationException`

### 9.3 条件性冲突场景

**场景 7：OverwriteFiles 的隔离级别差异**
```
Writer A: DELETE FROM table WHERE id = 100（使用 CopyOnWrite）
Writer B: INSERT INTO table VALUES (200, ...)
```
- **SERIALIZABLE**：Writer A 的 conflictDetectionFilter 可能匹配 Writer B 新增的文件（取决于 id=100 的过滤条件是否足够精确），可能冲突
- **SNAPSHOT**：Writer A **不检测**并发新增的数据文件，不冲突

**场景 8：未开启验证的 OverwriteFiles**
```
Writer A: table.newOverwrite().overwriteByRowFilter(expr).addFile(f).commit()
// 未调用 validateNoConflictingData() 和 validateNoConflictingDeletes()
```
- **结果**：不会进行任何冲突检测。如果有并发写入，可能导致数据不一致。
- **教训**：OverwriteFiles 的冲突验证是"opt-in"的，必须显式启用。Spark 等引擎会自动正确配置。

---

## 10. 与 Delta Lake、Hudi 的 OCC 策略对比

### 10.1 核心机制对比

| 维度 | Iceberg | Delta Lake | Hudi |
|------|---------|-----------|------|
| **元数据存储** | 独立 metadata JSON 文件 + manifest 文件 | _delta_log/ 下的 JSON commit 文件 | .hoodie/ 下的 timeline 文件 |
| **原子提交** | CAS on metadata pointer（Catalog 提供） | 文件系统 atomic rename（`_delta_log/N.json`） | 文件系统 atomic rename / Zookeeper 锁 |
| **冲突检测层** | 上层 validate + 底层 CAS | LogStore + ConflictChecker | Timeline Server / 乐观锁 |
| **冲突粒度** | 文件级 + 表达式级过滤 | 文件级 + 分区级 | 文件组级（FileGroup） |
| **Schema 演化** | 独立于 OCC，内置到 metadata | 独立处理 | 独立处理 |

### 10.2 冲突检测粒度对比

**Iceberg 的独特优势**：

Iceberg 的冲突检测支持**表达式级别**的精细过滤（`conflictDetectionFilter`）。例如，两个 writer 分别更新 `WHERE category='A'` 和 `WHERE category='B'` 时，即使在同一个分区内，只要新增文件的 column statistics 证明它们不包含对方范围的数据，就可以避免冲突。

**Delta Lake** 的冲突检测主要基于：
- 读取的文件集合 vs 提交的文件集合是否有交集
- 使用 AddFile/RemoveFile 的 partition 信息做分区级冲突检测
- 不支持 column statistics 级别的精细过滤

**Hudi** 的冲突检测：
- 基于 FileGroup 的并发写入检测
- Copy-On-Write 模式下依赖 timeline 的乐观并发
- Merge-On-Read 模式下使用 log file compaction 减少冲突

### 10.3 重试策略对比

| 维度 | Iceberg | Delta Lake | Hudi |
|------|---------|-----------|------|
| **重试方式** | 上层指数退避 | 内置于 OptimisticTransaction | 基于 Timeline |
| **默认重试次数** | 4 次 | 取决于 LogStore 实现 | 依赖配置 |
| **退避算法** | 指数退避 + 10% jitter | 固定间隔或指数退避 | 固定间隔 |
| **可配置性** | 高（4 个表属性） | 中等 | 中等 |
| **总超时** | 30 分钟 | 无默认总超时 | 依赖配置 |

### 10.4 隔离级别对比

| 维度 | Iceberg | Delta Lake | Hudi |
|------|---------|-----------|------|
| **支持的级别** | SERIALIZABLE, SNAPSHOT | SERIALIZABLE（默认，WriteSerializable 可选） | 依赖底层存储 |
| **隔离级别粒度** | 每次操作可配置 | 表级 | 表级 |
| **SERIALIZABLE 行为** | 检测并发新增数据+删除 | 类似，检测读写冲突 | 不直接对应 |
| **SNAPSHOT 行为** | 仅检测并发删除 | WriteSerializable（不检测 blind append 冲突） | 类似快照隔离 |

**Iceberg 的独特之处**：
- 隔离级别可以**每次操作粒度**配置（而非表级）
- 冲突检测是**按需启用**的（opt-in），给予引擎最大灵活性
- 通过 `conflictDetectionFilter` 可以**精确控制**冲突检测范围

---

## 11. 总结

Apache Iceberg 的乐观并发控制机制是一个精心设计的分层体系：

1. **底层原子性**（TableOperations.commit）：由各 Catalog 实现提供 CAS 保证，是 OCC 的基础
2. **中层重试**（SnapshotProducer.commit / Tasks 框架）：通过指数退避重试处理 CAS 失败，最多 4 次重试，30 分钟总超时
3. **上层验证**（MergingSnapshotProducer.validate）：提供丰富的冲突检测方法，由各操作按需组合调用
4. **事务抽象**（BaseTransaction）：支持多步操作的原子性，通过操作重放处理并发冲突

这种设计的核心优势在于：

- **高并发性**：纯追加操作（最常见的场景）之间从不冲突
- **灵活性**：每种操作精确定义自己需要的验证逻辑，避免过度保守
- **精确性**：通过表达式级冲突过滤，最大限度减少误报
- **可配置性**：隔离级别、重试参数均可按表或按操作粒度配置
- **引擎无关性**：核心 OCC 逻辑在 iceberg-core 中实现，Spark/Flink/Trino 等引擎只需正确配置验证参数

理解这套机制对于在生产环境中正确使用 Iceberg 至关重要——特别是在设计并发写入管道时，需要根据业务需求选择合适的隔离级别和冲突检测策略。

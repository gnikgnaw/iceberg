# CDC 有序场景下 upsert=true 的剩余问题

## 场景设定

- **CDC Source 并行度 = 1**：保证 binlog 读取有序
- **CDC 流严格有序**：UPDATE_BEFORE 和 UPDATE_AFTER 按顺序到达
- **配置**：`equalityFieldColumns = ["id"]`, `upsert = true`

## 已解决的问题

✅ **CDC 流乱序问题**：并行度为 1 保证了顺序性

## 仍然存在的问题

### 问题 1: Flink Sink 并行度导致的乱序 ⚠️ 最关键

#### 问题描述

即使 CDC source 并行度为 1，**Flink sink 的并行度通常大于 1**，数据会被分发到不同的 sink 实例。

#### 具体例子

```java
// Flink Job 配置
env.setParallelism(4);  // 全局并行度 4

FlinkCDC.mysqlSource(...)
    .setParallelism(1)  // ✅ CDC source 并行度 1，保证有序
    .sinkTo(
        FlinkSink.forRowData(...)
            .writeParallelism(4)  // ⚠️ Sink 并行度 4
            .equalityFieldColumns(Arrays.asList("id"))
            .upsert(true)
            .append()
    );
```

#### 数据流转过程

```
CDC Source (并行度=1)
    ↓ 有序流
    ↓ 1. -U {id:1, name:"Alice"}
    ↓ 2. +U {id:1, name:"Bob"}
    ↓ 3. -U {id:1, name:"Bob"}
    ↓ 4. +U {id:1, name:"Charlie"}
    ↓
[Flink 内部 shuffle/rebalance]
    ↓
Sink (并行度=4)
    ├─ Sink-1: 处理 id % 4 == 1 的数据
    ├─ Sink-2: 处理 id % 4 == 2 的数据
    ├─ Sink-3: 处理 id % 4 == 3 的数据
    └─ Sink-4: 处理 id % 4 == 0 的数据
```

**问题**：如果 `id=1` 的所有事件都路由到 Sink-2，那么在 Sink-2 内部仍然是有序的。但是：

1. **不同 key 之间的顺序无法保证**
   ```java
   // CDC Source 产生（有序）
   1. -U {id:1, name:"Alice"}
   2. +U {id:1, name:"Bob"}
   3. -U {id:2, name:"Tom"}
   4. +U {id:2, name:"Jerry"}
   
   // Sink-1 接收（id:1 的事件）
   1. -U {id:1, name:"Alice"}
   2. +U {id:1, name:"Bob"}
   
   // Sink-2 接收（id:2 的事件）
   3. -U {id:2, name:"Tom"}
   4. +U {id:2, name:"Jerry"}
   
   // 两个 Sink 并发写入 Iceberg
   // 写入顺序不确定！
   ```

2. **Checkpoint 对齐问题**
   ```java
   // Checkpoint N
   Sink-1: 处理到事件 100
   Sink-2: 处理到事件 98  ← 慢了
   Sink-3: 处理到事件 102 ← 快了
   Sink-4: 处理到事件 99
   
   // Checkpoint 需要等待所有 Sink 完成
   // 但各个 Sink 的进度不同
   ```

3. **背压导致的乱序**
   ```java
   // Sink-1 处理慢（背压）
   Sink-1: 事件 1, 2, 3 堆积在缓冲区
   
   // Sink-2 处理快
   Sink-2: 事件 4, 5, 6 已经写入
   
   // 实际写入顺序：4, 5, 6, 1, 2, 3
   // 与 CDC 顺序不一致
   ```

#### 解决方案

```java
// 方案 1: Sink 并行度也设置为 1（性能差）
FlinkSink.forRowData(...)
    .writeParallelism(1)  // ⚠️ 性能瓶颈
    .equalityFieldColumns(Arrays.asList("id"))
    .upsert(true)
    .append();

// 方案 2: 使用 upsert=false（推荐）
FlinkSink.forRowData(...)
    .writeParallelism(4)  // 可以并行写入
    .equalityFieldColumns(Arrays.asList("id"))
    .upsert(false)  // 即使乱序也不会丢数据
    .append();
```

---

### 问题 2: Checkpoint 失败重试导致的重复处理

#### 问题描述

即使流是有序的，checkpoint 失败重试可能导致事件重复处理。

#### 具体例子

```java
// Checkpoint N 包含的事件
1. -U {id:1, name:"Alice"}
2. +U {id:1, name:"Bob"}

// upsert=true 处理
1. -U: 忽略
2. +U: deleteKey({id:1}) + write({id:1, name:"Bob"})

// Checkpoint N 提交到 Iceberg
// Iceberg Snapshot 1: {id:1, name:"Bob"}

// ⚠️ Checkpoint N 失败，需要重试
// Flink 从上一个 checkpoint 恢复，重新处理相同的事件

// 重新处理
1. -U: 忽略
2. +U: deleteKey({id:1}) + write({id:1, name:"Bob"})

// Iceberg Snapshot 2: 
// - Delete File: {id:1}
// - Data File: {id:1, name:"Bob"}

// 问题：Snapshot 1 的 {id:1, name:"Bob"} 被 Delete File 删除了！
// 最终表是空的 ❌
```

#### 根本原因

`upsert=true` 的 `deleteKey({id:1})` 会删除**所有** `id=1` 的记录，包括：
- 当前 checkpoint 之前写入的记录
- 其他并发写入的记录

#### upsert=false 的表现

```java
// Checkpoint N 包含的事件
1. -U {id:1, name:"Alice"}
2. +U {id:1, name:"Bob"}

// upsert=false 处理
1. -U: delete({id:1, name:"Alice"})
2. +U: write({id:1, name:"Bob"})

// Checkpoint N 失败，重新处理
1. -U: delete({id:1, name:"Alice"})  ← 精确删除，不会误删 Bob
2. +U: write({id:1, name:"Bob"})     ← 可能重复，但数据不丢失

// 最终：可能有两条 {id:1, name:"Bob"}（重复）
// 但至少数据没有丢失，可以后续清理
```

---

### 问题 3: 丢失变更历史（无法避免）

#### 问题描述

即使数据正确，`upsert=true` 仍然会丢失完整的变更历史。

#### 对比

```java
// MySQL 操作历史
T1: INSERT INTO orders VALUES (1, 'pending', 100);
T2: UPDATE orders SET status='paid' WHERE id=1;
T3: UPDATE orders SET status='shipped' WHERE id=1;
T4: UPDATE orders SET status='delivered' WHERE id=1;

// upsert=false 的 Iceberg 文件
Data File 1: {id:1, status:'pending', amount:100}
Delete File 1: {id:1, status:'pending', amount:100}  ← 完整旧值
Data File 2: {id:1, status:'paid', amount:100}
Delete File 2: {id:1, status:'paid', amount:100}     ← 完整旧值
Data File 3: {id:1, status:'shipped', amount:100}
Delete File 3: {id:1, status:'shipped', amount:100}  ← 完整旧值
Data File 4: {id:1, status:'delivered', amount:100}

// upsert=true 的 Iceberg 文件
Data File 1: {id:1, status:'pending', amount:100}
Delete File 1: {id:1}  ← 只有 key
Data File 2: {id:1, status:'paid', amount:100}
Delete File 2: {id:1}  ← 只有 key
Data File 3: {id:1, status:'shipped', amount:100}
Delete File 3: {id:1}  ← 只有 key
Data File 4: {id:1, status:'delivered', amount:100}
```

#### 业务影响

```sql
-- 需求：查询订单状态变更历史
-- upsert=false: 可以通过 delete files 重建完整历史
SELECT 
    id,
    status,
    change_time,
    'deleted' as action
FROM iceberg_delete_files
WHERE id = 1
UNION ALL
SELECT 
    id,
    status,
    change_time,
    'inserted' as action
FROM iceberg_data_files
WHERE id = 1
ORDER BY change_time;

-- 结果：
-- | id | status    | change_time | action   |
-- |----|-----------|-------------|----------|
-- | 1  | pending   | T1          | inserted |
-- | 1  | pending   | T2          | deleted  |
-- | 1  | paid      | T2          | inserted |
-- | 1  | paid      | T3          | deleted  |
-- | 1  | shipped   | T3          | inserted |
-- | 1  | shipped   | T4          | deleted  |
-- | 1  | delivered | T4          | inserted |

-- upsert=true: 无法重建历史
-- Delete files 只有 {id:1}，不知道删除的是什么状态
```

#### 合规和审计要求

很多行业（金融、医疗、电商）要求：
- **审计日志**：记录所有数据变更
- **合规性**：证明数据变更的合法性
- **回溯能力**：出问题时能追溯原因

`upsert=true` 无法满足这些要求。

---

### 问题 4: UPDATE_BEFORE 的资源浪费

#### 问题描述

如果使用 `upsert=true`，UPDATE_BEFORE 完全被忽略，那为什么还要传输这些数据？

#### 资源消耗

```java
// CDC 流（每秒 10000 条更新）
每秒产生：
- 10000 条 UPDATE_BEFORE
- 10000 条 UPDATE_AFTER
总计：20000 条事件

// upsert=true 处理
- 10000 条 UPDATE_BEFORE: 忽略（浪费）
- 10000 条 UPDATE_AFTER: 处理

// 资源浪费
- 网络带宽：传输了 10000 条无用的 UPDATE_BEFORE
- CPU：反序列化了 10000 条无用的事件
- 内存：缓冲了 10000 条无用的事件
```

#### 优化方案

```java
// 方案 1: 在 CDC source 层面过滤 UPDATE_BEFORE（不推荐）
// 问题：失去了完整的变更信息，无法检测数据问题

// 方案 2: 使用 upsert=false（推荐）
// 充分利用 UPDATE_BEFORE 的信息
```

---

### 问题 5: 与 Iceberg 的 UPSERT 模式不一致

#### Iceberg 的原生 UPSERT 支持

Iceberg 有原生的 UPSERT 支持（通过 `RowDelta` API）：

```java
// Iceberg 原生 UPSERT
table.newRowDelta()
    .addRows(dataFile)           // 新数据
    .addDeletes(deleteFile)      // 删除文件
    .validateDeletedFiles()      // 验证删除的文件存在
    .validateDataFilesExist(...)  // 验证数据文件存在
    .commit();
```

**关键特性**：
- `validateDeletedFiles()`: 确保删除的文件存在
- `validateDataFilesExist()`: 确保数据文件存在
- 原子性提交

#### Flink Sink 的 upsert=true

```java
// Flink Sink 的 upsert=true
// 只是简单地：
1. 写入 delete file（只有 key）
2. 写入 data file
3. 提交

// 没有验证：
- 删除的记录是否真的存在
- 是否误删了其他记录
- 是否有并发冲突
```

**问题**：Flink Sink 的 `upsert=true` 是一个简化的实现，没有 Iceberg 原生 UPSERT 的安全保障。

---

### 问题 6: 调试和故障排查困难

#### 场景

```java
// 生产环境发现数据不一致
// 表中应该有 {id:1, name:"Charlie"}
// 但实际是 {id:1, name:"Bob"}

// upsert=false: 可以通过 delete files 追溯
// 查看 delete files:
// - {id:1, name:"Alice"}  (T1)
// - {id:1, name:"Bob"}    (T2)
// - {id:1, name:"Charlie"} (T3) ← 发现这条被删除了

// 查看 data files:
// - {id:1, name:"Alice"}  (T1)
// - {id:1, name:"Bob"}    (T2, T4) ← 发现 T4 又写入了 Bob
// - {id:1, name:"Charlie"} (T3)

// 结论：T4 时刻有一个错误的写入，覆盖了 Charlie

// upsert=true: 无法追溯
// Delete files 只有 {id:1}，不知道删除的是什么
// 无法判断是哪个环节出了问题
```

---

## 性能对比

### upsert=false

```
优点：
- ✅ 数据安全（不会丢失）
- ✅ 完整的变更历史
- ✅ 可以并行写入
- ✅ 便于调试和审计

缺点：
- ❌ Delete files 更大（存储完整行）
- ❌ 读取时需要过滤更多的 delete records
- ❌ 需要定期 compaction
```

### upsert=true（即使 CDC 有序）

```
优点：
- ✅ Delete files 更小（只存储 key）
- ✅ 读取时过滤更快

缺点：
- ❌ Sink 并行度导致的乱序风险
- ❌ Checkpoint 重试可能误删数据
- ❌ 丢失变更历史
- ❌ 资源浪费（UPDATE_BEFORE 被忽略）
- ❌ 调试困难
- ❌ 不符合审计要求
```

---

## 推荐配置

### 标准配置（推荐）

```java
// CDC Source
FlinkCDC.mysqlSource(...)
    .setParallelism(1)  // 保证有序
    .sinkTo(
        FlinkSink.forRowData(...)
            .tableLoader(tableLoader)
            .equalityFieldColumns(Arrays.asList("id"))
            .upsert(false)  // ✅ 标准 CDC 模式
            .writeParallelism(4)  // 可以并行写入
            .append()
    );

// Checkpoint 配置
env.enableCheckpointing(60000);  // 60 秒
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// 定期 Compaction
table.newRewrite()
    .rewriteFiles(...)
    .commit();
```

### 如果确实需要 UPSERT 语义

```java
// 方案 1: 使用 Flink SQL 的 UPSERT 模式
tableEnv.executeSql(
    "CREATE TABLE iceberg_sink (" +
    "  id INT," +
    "  name STRING," +
    "  PRIMARY KEY (id) NOT ENFORCED" +
    ") WITH (" +
    "  'connector' = 'iceberg'," +
    "  'write.upsert.enabled' = 'true'" +
    ")"
);

// 方案 2: 在 Flink 中先去重
dataStream
    .keyBy(row -> row.getField("id"))
    .process(new KeyedProcessFunction<Integer, RowData, RowData>() {
        private ValueState<RowData> state;
        
        @Override
        public void processElement(RowData row, Context ctx, Collector<RowData> out) {
            // 只保留每个 key 的最新记录
            if (row.getRowKind() == RowKind.INSERT || 
                row.getRowKind() == RowKind.UPDATE_AFTER) {
                state.update(row);
                out.collect(row);
            }
        }
    })
    .sinkTo(
        FlinkSink.forRowData(...)
            .upsert(true)  // 此时可以用 upsert
            .append()
    );

// 方案 3: Sink 并行度设置为 1（性能差）
FlinkSink.forRowData(...)
    .writeParallelism(1)  // 保证写入有序
    .upsert(true)
    .append();
```

---

## 总结

### 即使 CDC 有序（并行度=1），upsert=true 仍然有问题：

1. **Sink 并行度导致的乱序**（最关键）
2. **Checkpoint 重试可能误删数据**
3. **丢失变更历史**（无法审计）
4. **资源浪费**（UPDATE_BEFORE 被忽略）
5. **调试困难**（无法追溯问题）

### 推荐做法：

- ✅ 使用 `upsert=false`（标准 CDC 模式）
- ✅ CDC source 并行度 = 1（保证有序）
- ✅ Sink 并行度 > 1（提高性能）
- ✅ 定期 compaction（清理 delete files）

### 关键点：

**CDC source 有序只是第一步，Flink sink 的并行写入、checkpoint 机制、Iceberg 的异步提交都可能导致顺序问题。upsert=false 虽然可能产生临时的数据重复，但至少保证了数据安全和完整的变更历史。**

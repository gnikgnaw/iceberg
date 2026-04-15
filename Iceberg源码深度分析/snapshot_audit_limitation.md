# Iceberg 快照机制与审计粒度的限制

## 你的观察是正确的！

**Iceberg 是按快照（Snapshot）级别对外展示的**，如果多次变更都在一个快照内，确实无法从快照层面看到中间状态。

## 快照的工作机制

### 什么是快照？

```
Iceberg Snapshot = 一次原子提交（Atomic Commit）

一个快照包含：
- Manifest List（清单列表）
- Manifest Files（清单文件）
- Data Files（数据文件）
- Delete Files（删除文件）
- Metadata（元数据：时间戳、操作类型等）
```

### 快照的粒度

在 Flink CDC 场景下：

```
1 个 Flink Checkpoint = 1 个 Iceberg Snapshot

例如：
- Checkpoint 间隔：60 秒
- 60 秒内的所有变更 → 1 个快照
- 审计粒度：60 秒
```

## 快照内部变更的"黑盒"问题

### 场景示例

```java
// Flink Checkpoint N（60 秒内）
// MySQL 操作：
T1 (10:00:05): UPDATE users SET name='Bob' WHERE id=1;
T2 (10:00:20): UPDATE users SET name='Charlie' WHERE id=1;
T3 (10:00:45): UPDATE users SET name='David' WHERE id=1;

// CDC 流（6 条事件）：
1. -U {id:1, name:"Alice", age:30}      // T1 BEFORE
2. +U {id:1, name:"Bob", age:30}        // T1 AFTER
3. -U {id:1, name:"Bob", age:30}        // T2 BEFORE
4. +U {id:1, name:"Charlie", age:30}    // T2 AFTER
5. -U {id:1, name:"Charlie", age:30}    // T3 BEFORE
6. +U {id:1, name:"David", age:30}      // T3 AFTER

// Flink 在 10:01:00 触发 Checkpoint
// 所有 6 条事件在同一个 Checkpoint 内处理
```

### upsert=false 的快照内容

```java
// Iceberg Snapshot N (10:01:00 提交)

// Data Files（数据文件）
Data File 1: {id:1, name:"David", age:30}  ← 最终状态

// Delete Files（删除文件）
Delete File 1: {id:1, name:"Alice", age:30}    ← 快照内删除的记录
Delete File 2: {id:1, name:"Bob", age:30}      ← 快照内删除的记录
Delete File 3: {id:1, name:"Charlie", age:30}  ← 快照内删除的记录

// Snapshot Metadata
{
    snapshot_id: 12345,
    timestamp: '2024-01-01 10:01:00',
    operation: 'append',
    summary: {
        'added-data-files': 1,
        'added-delete-files': 3,
        'added-records': 1,
        'deleted-records': 3
    }
}
```

### 从快照层面能看到什么？

```sql
-- 查询快照 N 的最终状态
SELECT * FROM iceberg.users FOR SYSTEM_VERSION AS OF 12345;
-- 结果：{id:1, name:"David", age:30}  ← 只能看到最终状态

-- 查询快照 N 的 delete files
SELECT * FROM iceberg.users.delete_files WHERE snapshot_id = 12345;
-- 结果：
-- {id:1, name:"Alice", age:30}
-- {id:1, name:"Bob", age:30}
-- {id:1, name:"Charlie", age:30}
-- ← 可以看到快照内删除了哪些记录

-- ⚠️ 但是无法看到：
-- 1. Alice → Bob → Charlie → David 的变更顺序
-- 2. 每次变更的具体时间（只知道快照时间 10:01:00）
-- 3. Bob 和 Charlie 是中间状态，还是并发写入
```

### upsert=true 的快照内容

```java
// Iceberg Snapshot N (10:01:00 提交)

// Data Files
Data File 1: {id:1, name:"David", age:30}  ← 最终状态

// Delete Files（只有 key）
Delete File 1: {id:1}  ← 不知道删除的是什么
Delete File 2: {id:1}  ← 不知道删除的是什么
Delete File 3: {id:1}  ← 不知道删除的是什么

// ⚠️ 完全无法看到：
// 1. 删除的是 Alice、Bob、Charlie
// 2. 快照内有 3 次变更
// 3. 任何中间状态的信息
```

---

## 审计粒度的限制

### 限制 1: 快照内部的变更顺序不可见

```
快照 N 内的变更：
Alice → Bob → Charlie → David

从快照层面只能看到：
- 快照 N-1: Alice
- 快照 N: David

中间状态 Bob 和 Charlie 不可见 ❌
```

### 限制 2: 快照内部的时间戳不可见

```
实际操作时间：
- T1: 10:00:05 (Alice → Bob)
- T2: 10:00:20 (Bob → Charlie)
- T3: 10:00:45 (Charlie → David)

快照时间戳：
- Snapshot N: 10:01:00

无法区分快照内的操作时间 ❌
```

### 限制 3: 快照内部的操作人不可见

```
实际操作：
- T1: 用户 A 修改
- T2: 用户 B 修改
- T3: 用户 C 修改

快照元数据：
- Operator: Flink Job

无法区分快照内的操作人 ❌
```

---

## upsert=false vs upsert=true 在快照级别的区别

### 即使在快照级别，upsert=false 仍然更好

#### 场景：快照内有 3 次 UPDATE

```java
// upsert=false 的 delete files
Delete File 1: {id:1, name:"Alice", age:30, city:"LA"}
Delete File 2: {id:1, name:"Bob", age:30, city:"LA"}
Delete File 3: {id:1, name:"Charlie", age:30, city:"LA"}

// 可以推断：
// 1. 快照内有 3 次删除操作
// 2. 删除的记录是 Alice、Bob、Charlie
// 3. 最终状态是 David
// 4. 变更路径可能是：Alice → Bob → Charlie → David

// upsert=true 的 delete files
Delete File 1: {id:1}
Delete File 2: {id:1}
Delete File 3: {id:1}

// 只能推断：
// 1. 快照内有 3 次删除操作
// 2. 删除的都是 id=1 的记录
// 3. ❌ 不知道删除的是什么数据
// 4. ❌ 无法推断变更路径
```

#### 场景：快照内有多个 key 的变更

```java
// Checkpoint N 内的操作
UPDATE users SET name='Bob' WHERE id=1;
UPDATE users SET name='Tom' WHERE id=2;
UPDATE users SET name='Charlie' WHERE id=1;

// upsert=false 的 delete files
Delete File 1: {id:1, name:"Alice"}   ← 清楚地知道是 id=1 的 Alice
Delete File 2: {id:2, name:"Jerry"}   ← 清楚地知道是 id=2 的 Jerry
Delete File 3: {id:1, name:"Bob"}     ← 清楚地知道是 id=1 的 Bob

// 可以推断：
// - id=1: Alice → Bob → Charlie
// - id=2: Jerry → Tom

// upsert=true 的 delete files
Delete File 1: {id:1}
Delete File 2: {id:2}
Delete File 3: {id:1}

// 只能推断：
// - id=1 有 2 次变更
// - id=2 有 1 次变更
// ❌ 不知道具体的变更内容
```

---

## 如何提高审计粒度？

### 方案 1: 减小 Checkpoint 间隔（推荐）

```java
// 默认配置（60 秒）
env.enableCheckpointing(60000);  // 审计粒度：60 秒

// 提高审计粒度（10 秒）
env.enableCheckpointing(10000);  // 审计粒度：10 秒

// 极致审计粒度（1 秒）
env.enableCheckpointing(1000);   // 审计粒度：1 秒
```

**权衡**：
- ✅ 更细的审计粒度
- ❌ 更多的快照（存储开销）
- ❌ 更频繁的提交（性能开销）

**推荐**：
- 普通场景：60 秒
- 需要审计：10-30 秒
- 严格审计：5-10 秒

### 方案 2: 应用层审计日志（最可靠）

```java
// 在 Flink 中记录审计日志
dataStream
    .map(new RichMapFunction<RowData, RowData>() {
        @Override
        public RowData map(RowData row) {
            // 记录审计日志到独立的审计表
            auditLogger.log(
                userId,
                operation,
                oldValue,
                newValue,
                timestamp
            );
            return row;
        }
    })
    .sinkTo(icebergSink);

// 审计表结构
CREATE TABLE audit_log (
    audit_id BIGINT,
    user_id BIGINT,
    table_name STRING,
    operation STRING,
    old_value STRING,
    new_value STRING,
    timestamp TIMESTAMP
);
```

**优点**：
- ✅ 完整的审计信息（包括操作人、原因等）
- ✅ 不受快照粒度限制
- ✅ 可以记录业务上下文

**缺点**：
- ❌ 需要额外的存储
- ❌ 需要维护审计表

### 方案 3: 使用 Iceberg 的 Changelog 模式

```java
// Flink SQL 配置
CREATE TABLE iceberg_sink (
    id BIGINT,
    name STRING,
    age INT
) WITH (
    'connector' = 'iceberg',
    'write.format.default' = 'parquet',
    'write.metadata.delete-after-commit.enabled' = 'false',  // 保留所有元数据
    'write.metadata.previous-versions-max' = '100'  // 保留 100 个版本
);

// 读取 changelog
SELECT * FROM iceberg_sink /*+ OPTIONS('read.split.planning-lookback'='100') */;
```

**优点**：
- ✅ 利用 Iceberg 原生能力
- ✅ 不需要额外的表

**缺点**：
- ❌ 仍然受快照粒度限制
- ❌ 无法记录业务上下文

### 方案 4: 混合方案（推荐）

```java
// 1. 使用 upsert=false（保留快照级别的审计）
FlinkSink.forRowData(dataStream)
    .upsert(false)
    .append();

// 2. 调整 checkpoint 间隔（平衡性能和审计粒度）
env.enableCheckpointing(10000);  // 10 秒

// 3. 关键操作记录应用层审计日志
if (isImportantOperation(row)) {
    auditLogger.log(row);
}

// 4. 定期 compaction（清理旧快照，但保留审计日志）
table.expireSnapshots()
    .expireOlderThan(System.currentTimeMillis() - 7 * 24 * 3600 * 1000)  // 保留 7 天
    .retainLast(100)  // 至少保留 100 个快照
    .commit();
```

---

## 实际审计能力对比

### 场景：60 秒内有 10 次 UPDATE

```
MySQL 操作（60 秒内）：
T1: UPDATE users SET name='Bob' WHERE id=1;
T2: UPDATE users SET name='Charlie' WHERE id=1;
...
T10: UPDATE users SET name='Zack' WHERE id=1;

CDC 流：20 条事件（10 个 UPDATE_BEFORE + 10 个UPDATE_AFTER）
```

#### 应用层审计日志（最完整）

```sql
SELECT * FROM audit_log WHERE user_id = 1 ORDER BY timestamp;

-- 结果：10 条记录，每次变更都有
-- | audit_id | operation | old_value | new_value | timestamp           |
-- |----------|-----------|-----------|-----------|---------------------|
-- | 1        | UPDATE    | Alice     | Bob       | 2024-01-01 10:00:05 |
-- | 2        | UPDATE    | Bob       | Charlie   | 2024-01-01 10:00:12 |
-- | ...      | ...       | ...       | ...       | ...                 |
-- | 10       | UPDATE    | Yuki      | Zack      | 2024-01-01 10:00:58 |

-- ✅ 完整的审计信息
```

#### Iceberg upsert=false（快照级别）

```sql
-- 查询快照 N 的 delete files
SELECT * FROM iceberg.users.delete_files WHERE snapshot_id = N;

-- 结果：10 条记录
-- | id | name    | age |
-- |----|---------|-----|
-- | 1  | Alice   | 30  |
-- | 1  | Bob     | 30  |
-- | 1  | Charlie | 30  |
-- | ...| ...     | ... |
-- | 1  | Yuki    | 30  |

-- ⚠️ 可以看到删除了哪些记录，但看不到：
-- 1. 变更顺序（Alice → Bob → Charlie 还是 Alice → Charlie → Bob？）
-- 2. 每次变更的时间（只知道快照时间）
-- 3. 操作人
```

#### Iceberg upsert=true（快照级别）

```sql
-- 查询快照 N 的 delete files
SELECT * FROM iceberg.users.delete_files WHERE snapshot_id = N;

-- 结果：10 条记录
-- | id |
-- |----|
-- | 1  |
-- | 1  |
-- | 1  |
-- | ...| 
-- | 1  |

-- ❌ 只知道有 10 次删除，完全不知道删除的是什么
```

---

## 总结

### 你的观察是正确的

1. **Iceberg 是按快照级别展示的**
2. **快照内部的中间状态不可见**
3. **审计粒度 = Checkpoint 间隔**

### 但是 upsert=false 仍然比 upsert=true 好

即使在快照级别：

| 审计能力 | upsert=false | upsert=true |
|---------|-------------|-------------|
| **快照间的变更** | ✅ 完整的旧值 | ❌ 只有 key |
| **快照内的删除记录** | ✅ 知道删除了什么 | ❌ 只知道删除了几次 |
| **变更路径推断** | ✅ 可以推断 | ❌ 无法推断 |
| **多 key 变更区分** | ✅ 清晰 | ❌ 混乱 |
| **故障排查** | ✅ 可以定位 | ❌ 难以定位 |

### 实际建议

1. **标准场景**：
   - 使用 `upsert=false`
   - Checkpoint 间隔 10-60 秒
   - 快照级别的审计（够用）

2. **严格审计场景**：
   - 使用 `upsert=false`
   - Checkpoint 间隔 5-10 秒
   - 加上应用层审计日志（完整审计）

3. **性能优先场景**：
   - 可以考虑 `upsert=true`
   - 但要接受审计能力的损失
   - 建议仍然保留应用层审计日志

### 关键点

**Iceberg 的快照机制确实限制了审计粒度，但 upsert=false 在快照级别仍然提供了比 upsert=true 更多的审计信息。对于需要完整审计的场景，应该结合应用层审计日志。**

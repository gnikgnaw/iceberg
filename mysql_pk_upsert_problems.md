# MySQL 主键表使用 upsert=true 的问题分析

## 场景设定

- **上游**：MySQL 表，有主键（如 `id`）
- **CDC 工具**：Debezium / Canal / Flink CDC Connector
- **配置**：`equalityFieldColumns = ["id"]`, `upsert = true`

## 表面上看起来没问题

在理想情况下，`upsert=true` 似乎是可以工作的：

```java
// MySQL 表结构
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    city VARCHAR(100)
);

// Flink Sink 配置
FlinkSink.forRowData(dataStream)
    .equalityFieldColumns(Arrays.asList("id"))  // 主键
    .upsert(true)  // 看起来合理？
    .append();
```

### 理想情况下的处理

```java
// MySQL 操作
INSERT INTO users VALUES (1, 'Alice', 30, 'LA');
UPDATE users SET name='Bob', age=31 WHERE id=1;

// CDC 流
{rowKind: +I, id: 1, name: "Alice", age: 30, city: "LA"}
{rowKind: -U, id: 1, name: "Alice", age: 30, city: "LA"}  // UPDATE_BEFORE
{rowKind: +U, id: 1, name: "Bob", age: 31, city: "LA"}    // UPDATE_AFTER

// upsert=true 处理
1. +I: deleteKey({id:1}) + write({id:1, name:"Alice", age:30, city:"LA"})
2. -U: 忽略
3. +U: deleteKey({id:1}) + write({id:1, name:"Bob", age:31, city:"LA"})

// 最终结果：{id:1, name:"Bob", age:31, city:"LA"} ✅ 正确
```

看起来没问题！但实际上有几个**严重的隐藏问题**。

---

## 问题 1: CDC 流乱序导致数据错误

### 问题描述

CDC 流在传输过程中可能因为以下原因乱序：
- 网络延迟不同
- Kafka 分区不同
- Flink 并行度处理
- CDC 工具的多线程采集

### 具体例子

```java
// MySQL 实际操作顺序（时间戳）
T1: UPDATE users SET name='Bob' WHERE id=1;
T2: UPDATE users SET name='Charlie' WHERE id=1;

// CDC 流产生（正确顺序）
T1: -U {id:1, name:"Alice", age:30}
T1: +U {id:1, name:"Bob", age:30}
T2: -U {id:1, name:"Bob", age:30}
T2: +U {id:1, name:"Charlie", age:30}

// 但 Flink 接收到的顺序（乱序！）
1. -U {id:1, name:"Alice", age:30}   (T1 BEFORE)
2. -U {id:1, name:"Bob", age:30}     (T2 BEFORE) ← 先到达
3. +U {id:1, name:"Charlie", age:30} (T2 AFTER)  ← 先到达
4. +U {id:1, name:"Bob", age:30}     (T1 AFTER)  ← 后到达

// upsert=true 处理
1. -U {Alice}: 忽略
2. -U {Bob}: 忽略
3. +U {Charlie}: deleteKey({id:1}) + write({id:1, name:"Charlie"})
4. +U {Bob}: deleteKey({id:1}) + write({id:1, name:"Bob"})
   ⚠️ 这里会删除 Charlie，写入 Bob

// 最终结果：{id:1, name:"Bob"} ❌ 错误！
// 正确结果应该是：{id:1, name:"Charlie"}
```

**问题根源**：`upsert=true` 忽略了 UPDATE_BEFORE，无法通过完整的变更对来检测乱序。

### upsert=false 的表现

```java
// upsert=false 处理（同样的乱序）
1. -U {Alice}: delete({id:1, name:"Alice", age:30})
2. -U {Bob}: delete({id:1, name:"Bob", age:30})
3. +U {Charlie}: write({id:1, name:"Charlie", age:30})
4. +U {Bob}: write({id:1, name:"Bob", age:30})

// 最终结果：
// - {id:1, name:"Charlie", age:30}
// - {id:1, name:"Bob", age:30}
// ⚠️ 数据重复，但至少 Charlie 没有丢失
```

虽然 `upsert=false` 也有问题（数据重复），但至少：
1. 可以通过 delete file 的内容发现乱序问题
2. 数据没有丢失，可以后续清理
3. 保留了完整的变更历史

---

## 问题 2: 快速连续更新导致的数据丢失

### 场景

```java
// MySQL 快速连续更新（在同一秒内）
UPDATE users SET status='processing' WHERE id=1;  // T1
UPDATE users SET status='completed' WHERE id=1;   // T2 (几毫秒后)

// CDC 流
-U {id:1, status:'pending'}      // T1 BEFORE
+U {id:1, status:'processing'}   // T1 AFTER
-U {id:1, status:'processing'}   // T2 BEFORE
+U {id:1, status:'completed'}    // T2 AFTER

// Flink 处理（假设在同一个 mini-batch 中）
// upsert=true:
1. -U {pending}: 忽略
2. +U {processing}: deleteKey({id:1}) + write({id:1, status:'processing'})
3. -U {processing}: 忽略  ← 关键：这条被忽略了
4. +U {completed}: deleteKey({id:1}) + write({id:1, status:'completed'})

// 看起来结果是对的：{id:1, status:'completed'}
```

这个例子看起来没问题，但考虑更复杂的场景：

### 并发写入场景

```java
// 两个 Flink 并行度同时处理
// 并行度 1 处理：
+U {id:1, status:'processing'}  // T1 AFTER

// 并行度 2 处理：
+U {id:1, status:'completed'}   // T2 AFTER

// 如果并行度 2 先完成：
1. 并行度 2: deleteKey({id:1}) + write({id:1, status:'completed'})
2. 并行度 1: deleteKey({id:1}) + write({id:1, status:'processing'})
   ⚠️ 这里会删除 'completed'，写入 'processing'

// 最终结果：{id:1, status:'processing'} ❌ 错误！
// 正确结果应该是：{id:1, status:'completed'}
```

**问题根源**：`upsert=true` 没有 UPDATE_BEFORE 的约束，无法保证更新的顺序性。

---

## 问题 3: 丢失变更历史

### CDC 的本质目的

CDC (Change Data Capture) 的目的是**捕获数据变更历史**，而不仅仅是同步最终状态。

```java
// MySQL 操作历史
T1: INSERT INTO orders VALUES (1, 'pending', 100);
T2: UPDATE orders SET status='paid' WHERE id=1;
T3: UPDATE orders SET status='shipped' WHERE id=1;
T4: UPDATE orders SET status='delivered' WHERE id=1;

// upsert=false 的 delete files（保留完整历史）
Delete File 1: {id:1, status:'pending', amount:100}
Delete File 2: {id:1, status:'paid', amount:100}
Delete File 3: {id:1, status:'shipped', amount:100}

// upsert=true 的 delete files（只有 key）
Delete File 1: {id:1}
Delete File 2: {id:1}
Delete File 3: {id:1}
```

**影响**：
1. **无法审计**：不知道数据是如何变化的
2. **无法回溯**：无法查询历史状态
3. **调试困难**：出问题时无法追溯原因

### 实际业务影响

```java
// 业务需求：查询订单状态变更历史
SELECT * FROM orders_history WHERE order_id = 1;

// upsert=false: 可以通过 delete files 重建历史
// T1: {id:1, status:'pending'}
// T2: {id:1, status:'paid'}      (delete: pending)
// T3: {id:1, status:'shipped'}   (delete: paid)
// T4: {id:1, status:'delivered'} (delete: shipped)

// upsert=true: 无法重建历史
// 只知道有 3 次删除操作，但不知道删除的是什么状态
```

---

## 问题 4: DELETE 操作的语义差异

### 场景

```java
// MySQL 操作
DELETE FROM users WHERE id=1;

// CDC 流
-D {id:1, name:"Alice", age:30, city:"LA"}

// upsert=true 处理
deleteKey({id:1})  // 只删除 key

// upsert=false 处理
delete({id:1, name:"Alice", age:30, city:"LA"})  // 删除完整行
```

**问题**：如果表中有多条 id=1 的记录（虽然理论上不应该有，但在并发写入时可能短暂存在），`upsert=true` 会删除所有记录，而 `upsert=false` 只删除匹配的那条。

---

## 问题 5: 与 Flink CDC 的语义不匹配

### Flink CDC Connector 的设计

Flink CDC Connector 产生的是**完整的变更日志**：

```java
// Flink CDC 产生的 RowKind
public enum RowKind {
    INSERT,          // +I
    UPDATE_BEFORE,   // -U  ← 包含完整的旧值
    UPDATE_AFTER,    // +U  ← 包含完整的新值
    DELETE           // -D
}
```

**设计意图**：
- UPDATE_BEFORE 和 UPDATE_AFTER 是**成对的**
- 应该一起处理，确保原子性
- 提供完整的变更信息

**upsert=true 的问题**：
- 忽略 UPDATE_BEFORE，破坏了成对性
- 无法保证原子性
- 丢失了一半的变更信息

---

## 什么时候可以用 upsert=true？

### 适用场景

1. **数据源不是 CDC 流**
   ```java
   // Kafka Compacted Topic（只有最终状态）
   {id: 1, name: "Alice", age: 30}
   {id: 1, name: "Bob", age: 31}    // 替换 Alice
   {id: 1, name: "Charlie", age: 32} // 替换 Bob
   ```

2. **只关心最终状态，不关心历史**
   ```java
   // 实时仪表盘：只显示当前值
   // 不需要审计和回溯
   ```

3. **数据流已经去重和排序**
   ```java
   // 上游已经保证：
   // - 每个 key 只有最新的一条记录
   // - 记录按时间戳严格排序
   ```

### 不适用场景（包括你的场景）

1. **标准 CDC 流**（包括 MySQL Binlog）
2. **需要审计和合规**
3. **需要时间旅行查询**
4. **数据可能乱序**

---

## 推荐配置

### 对于 MySQL 主键表的 CDC

```java
// ✅ 推荐配置
FlinkSink.forRowData(dataStream)
    .tableLoader(tableLoader)
    .equalityFieldColumns(Arrays.asList("id"))  // 主键
    .upsert(false)  // 标准 CDC 模式
    .append();

// 配合 checkpoint 保证一致性
env.enableCheckpointing(60000);  // 60 秒
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
```

### 如果确实需要 upsert 语义

```java
// 方案 1: 在 Flink 中先转换数据流
dataStream
    .keyBy(row -> row.getField("id"))
    .process(new DeduplicateFunction())  // 去重，只保留最新记录
    .sinkTo(
        FlinkSink.forRowData(...)
            .equalityFieldColumns(Arrays.asList("id"))
            .upsert(true)  // 此时可以用 upsert
            .append()
    );

// 方案 2: 使用 Flink SQL 的 UPSERT 模式
tableEnv.executeSql(
    "CREATE TABLE iceberg_sink (" +
    "  id INT," +
    "  name STRING," +
    "  age INT," +
    "  PRIMARY KEY (id) NOT ENFORCED" +  // 声明主键
    ") WITH (" +
    "  'connector' = 'iceberg'," +
    "  'write.upsert.enabled' = 'true'" +  // 启用 upsert
    ")"
);
```

---

## 总结

### 对于"有主键的 MySQL 表"，使用 upsert=true 的问题：

1. **CDC 流乱序** → 数据错误（最终状态不正确）
2. **并发写入** → 数据丢失（新数据被旧数据覆盖）
3. **丢失变更历史** → 无法审计和回溯
4. **语义不匹配** → 违背 CDC 的设计意图
5. **调试困难** → 出问题时无法追溯原因

### 推荐做法：

- ✅ 使用 `upsert=false`（标准 CDC 模式）
- ✅ 配合 Flink checkpoint 保证一致性
- ✅ 定期运行 compaction 清理 delete files
- ✅ 如果需要去重，在 Flink 中先处理，再写入

### 关键点：

**即使上游有主键约束，CDC 流在传输和处理过程中仍然可能乱序、并发，导致 upsert=true 产生错误的结果。标准 CDC 模式 (upsert=false) 虽然可能产生临时的数据重复，但至少不会丢失数据，且保留了完整的变更历史。**

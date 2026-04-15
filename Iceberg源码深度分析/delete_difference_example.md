# 精确删除 vs 按 Key 删除的区别

## 核心问题

假设表结构：`(id INT, name STRING, age INT, city STRING)`

### 场景：并发写入导致的数据不一致

#### 初始状态
表中有一条记录：
```
{id: 1, name: "Alice", age: 30, city: "LA"}
```

#### 两个并发的 CDC 流

**CDC 流 A**（更新 age）：
```java
// UPDATE users SET age=31 WHERE id=1
{rowKind: -U, id: 1, name: "Alice", age: 30, city: "LA"}   // UPDATE_BEFORE
{rowKind: +U, id: 1, name: "Alice", age: 31, city: "LA"}   // UPDATE_AFTER
```

**CDC 流 B**（更新 city）：
```java
// UPDATE users SET city='NYC' WHERE id=1
{rowKind: -U, id: 1, name: "Alice", age: 30, city: "LA"}   // UPDATE_BEFORE
{rowKind: +U, id: 1, name: "Alice", age: 30, city: "NYC"}  // UPDATE_AFTER
```

---

## 方式 1: 精确删除 (upsert=false)

### 处理流程

假设事件到达顺序：A的-U → B的-U → A的+U → B的+U

```
1. 处理 A的-U: delete({id:1, name:"Alice", age:30, city:"LA"})
   → Delete File 1: {id:1, name:"Alice", age:30, city:"LA"}

2. 处理 B的-U: delete({id:1, name:"Alice", age:30, city:"LA"})
   → Delete File 2: {id:1, name:"Alice", age:30, city:"LA"}
   ⚠️ 注意：两个 delete file 内容相同

3. 处理 A的+U: write({id:1, name:"Alice", age:31, city:"LA"})
   → Data File 1: {id:1, name:"Alice", age:31, city:"LA"}

4. 处理 B的+U: write({id:1, name:"Alice", age:30, city:"NYC"})
   → Data File 2: {id:1, name:"Alice", age:30, city:"NYC"}
```

### 读取时的过滤逻辑

Iceberg 读取时会应用 delete files：

```
Data Files:
  - {id:1, name:"Alice", age:31, city:"LA"}
  - {id:1, name:"Alice", age:30, city:"NYC"}

Delete Files (精确匹配所有字段):
  - {id:1, name:"Alice", age:30, city:"LA"}
  - {id:1, name:"Alice", age:30, city:"LA"}

过滤逻辑：
  - {id:1, name:"Alice", age:31, city:"LA"}  → 不匹配任何 delete，保留 ✅
  - {id:1, name:"Alice", age:30, city:"NYC"} → 不匹配任何 delete，保留 ✅

最终结果：两条记录都保留！❌ 数据重复
```

**问题**：精确删除只能删除**完全相同**的记录，如果字段值有任何不同，就删不掉！

---

## 方式 2: 按 Key 删除 (upsert=true)

### 处理流程

假设事件到达顺序：A的-U → B的-U → A的+U → B的+U

```
1. 处理 A的-U: 忽略（upsert 模式）

2. 处理 B的-U: 忽略（upsert 模式）

3. 处理 A的+U: 
   deleteKey({id:1}) + write({id:1, name:"Alice", age:31, city:"LA"})
   → Delete File 1: {id:1}  ⚠️ 只存储 key
   → Data File 1: {id:1, name:"Alice", age:31, city:"LA"}

4. 处理 B的+U: 
   deleteKey({id:1}) + write({id:1, name:"Alice", age:30, city:"NYC"})
   → Delete File 2: {id:1}
   → Data File 2: {id:1, name:"Alice", age:30, city:"NYC"}
```

### 读取时的过滤逻辑

```
Data Files:
  - {id:1, name:"Alice", age:31, city:"LA"}
  - {id:1, name:"Alice", age:30, city:"NYC"}

Delete Files (只匹配 key):
  - {id:1}
  - {id:1}

过滤逻辑：
  - {id:1, name:"Alice", age:31, city:"LA"}  → id=1 匹配 delete，删除 ✅
  - {id:1, name:"Alice", age:30, city:"NYC"} → id=1 匹配 delete，删除 ✅

最终结果：两条记录都被删除！❌ 数据丢失
```

**问题**：按 key 删除会删除**所有相同 key** 的记录，可能误删新写入的数据！

---

## 真正的问题：CDC 流的语义

### 标准 CDC 的正确处理方式

CDC 流的 UPDATE_BEFORE 和 UPDATE_AFTER 是**成对出现**的，应该在**同一个事务**中处理：

```java
// 正确的处理方式（在数据库层面）
BEGIN TRANSACTION;
  DELETE FROM users WHERE id=1 AND name='Alice' AND age=30 AND city='LA';
  INSERT INTO users VALUES (1, 'Alice', 31, 'LA');
COMMIT;
```

但在 Flink 中，这两个事件可能：
1. 被不同的并行度处理
2. 在不同的 checkpoint 中提交
3. 与其他流的事件交错

### 为什么 upsert=false 更安全

虽然精确删除在并发场景下也有问题，但它至少：

1. **保留了完整的变更信息**
   ```
   Delete File: {id:1, name:"Alice", age:30, city:"LA"}
   ```
   可以看到删除的是哪条具体的记录

2. **不会误删其他记录**
   ```
   如果表中有：
   - {id:1, name:"Alice", age:31, city:"LA"}
   - {id:1, name:"Bob", age:30, city:"LA"}
   
   精确删除 {id:1, name:"Alice", age:30, city:"LA"} 
   → 只会尝试删除完全匹配的记录，不会误删 Bob 的记录
   ```

3. **更容易调试和审计**
   - 可以看到每次删除的完整数据
   - 可以追溯数据变更历史

### 为什么 upsert=true 在 CDC 场景下危险

```java
// 危险场景
{rowKind: -U, id: 1, name: "Alice", age: 30}  // UPDATE_BEFORE（被忽略）
{rowKind: +U, id: 1, name: "Bob", age: 31}    // UPDATE_AFTER

// upsert=true 的处理
// 1. -U: 忽略
// 2. +U: deleteKey({id:1}) + write({id:1, name:"Bob", age:31})

// 问题：如果表中已经有 {id:1, name:"Charlie", age:32}
// deleteKey({id:1}) 会把 Charlie 的记录也删掉！
// 但实际上 CDC 的语义是：把 Alice 改成 Bob，不应该影响 Charlie
```

---

## 最佳实践

### 1. 标准 CDC 流 → upsert=false

```java
FlinkSink.forRowData(dataStream)
    .equalityFieldColumns(Arrays.asList("id"))
    .upsert(false)  // 精确删除，保留完整变更历史
    .append();
```

**优点**：
- 保留完整的变更信息
- 不会误删其他记录
- 更容易调试

**缺点**：
- Delete file 更大（存储完整行）
- 并发场景下可能有数据重复（需要额外处理）

### 2. 简化数据流 → upsert=true

```java
// 数据流只有最终状态，没有 UPDATE_BEFORE
{rowKind: +I, id: 1, name: "Alice", age: 30}
{rowKind: +I, id: 1, name: "Bob", age: 31}  // 替换 Alice

FlinkSink.forRowData(dataStream)
    .equalityFieldColumns(Arrays.asList("id"))
    .upsert(true)  // 按 key 删除，"最后写入获胜"
    .append();
```

**优点**：
- Delete file 更小（只存储 key）
- "最后写入获胜"语义清晰

**缺点**：
- 会删除所有相同 key 的记录
- 丢失变更历史

### 3. 真正的解决方案：事务性写入

Iceberg 的最佳实践是使用**事务性写入**：

```java
// 使用 Flink 的 checkpoint 机制
env.enableCheckpointing(60000);  // 1分钟 checkpoint

FlinkSink.forRowData(dataStream)
    .equalityFieldColumns(Arrays.asList("id"))
    .upsert(false)
    .append();
```

每个 checkpoint 会原子性地提交所有变更，减少并发问题。

---

## 总结

**精确删除 (upsert=false) 的问题**：
- 只能删除完全匹配的记录
- 并发场景下可能导致数据重复

**按 Key 删除 (upsert=true) 的问题**：
- 会删除所有相同 key 的记录
- 可能误删新写入的数据
- 忽略 UPDATE_BEFORE，丢失变更历史

**为什么 CDC 场景用 upsert=false**：
1. CDC 流包含完整的变更信息（UPDATE_BEFORE + UPDATE_AFTER）
2. 精确删除虽然有并发问题，但至少不会误删其他记录
3. 保留完整的变更历史，便于调试和审计
4. 配合 Flink checkpoint 机制，可以实现事务性写入

**关键点**：CDC 的语义是"记录每一次变更"，而不是"确保最终状态正确"。因此需要使用 `upsert=false` 来保留完整的变更历史。

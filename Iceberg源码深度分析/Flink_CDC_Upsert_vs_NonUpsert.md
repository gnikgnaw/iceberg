# Flink CDC 写入 Iceberg：Upsert 与非 Upsert 模式深度解析

本文档深入分析了 Flink CDC 在通过 Iceberg Flink Sink 写入数据时，`upsert` 模式与非 `upsert` (普通) 模式之间的区别，并结合源码详细阐述其内部实现逻辑。

## 1. 核心区别概述

| 特性 | 普通模式 (非 Upsert) | Upsert 模式 |
| :--- | :--- | :--- |
| **设计目标** | 精确反映数据源的每一步变更 (Insert/Update/Delete) | 简化处理，假定流中包含最新的记录状态，自动处理去重 |
| **输入要求** | 需要完整的变更流 (`+I`, `-U`, `+U`, `-D`) | 只需要 `INSERT` / `UPDATE_AFTER` 即可工作 (忽略 `UPDATE_BEFORE`) |
| **Delete 实现** | `UPDATE_BEFORE` -> Pos Delete / Eq Delete<br>`DELETE` -> Eq Delete | `INSERT`/`UPDATE_AFTER` -> 自动触发 Eq Delete (按主键删除旧数据) + Data Insert |
| **性能影响** | 写入逻辑简单，但读取时需要合并更多的 Delete File | 写入时增加了 Delete 操作 (写放大)，但保证了数据的唯一性语义 |

## 2. 源码逻辑分析

Iceberg Flink Sink 的核心写入逻辑位于 `org.apache.iceberg.flink.sink` 包中。关键类包括：

*   **`RowDataTaskWriterFactory`**: 负责创建具体的 `TaskWriter`。
*   **`BaseDeltaTaskWriter`**: 它是 `UnpartitionedDeltaWriter` 和 `PartitionedDeltaWriter` 的基类，包含了处理不同 `RowKind` 的核心逻辑。

### 2.1 Writer 的创建 (RowDataTaskWriterFactory)

在 `RowDataTaskWriterFactory` 中，根据是否启用 `upsert`，`FileWriterFactory` 的配置会有所不同。

*   **Upsert 模式 (`upsert = true`)**:
    *   Delete 文件的 Schema 被裁剪，**只包含 Equality Fields (主键)**。
    *   这是一个优化：既然 Upsert 是通过主键删除旧数据，那么 Delete 文件中只需要存储主键值，不需要存储整行数据。

```java
// RowDataTaskWriterFactory.java 构造函数片段
if (upsert) {
    // ...
    // 在 upsert 模式下，delete file 必须包含正确的主键值。
    // 因此，我们只写入 equality delete fields (主键字段)。
    this.fileWriterFactory = new FlinkFileWriterFactory.Builder(table)
          // ...
          .equalityFieldIds(ArrayUtil.toPrimitive(equalityFieldIds.toArray(new Integer[0])))
          .equalityDeleteRowSchema(TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds))) // 只选主键
          .build();
} else {
    // 普通模式，delete schema 通常包含更多字段
    this.fileWriterFactory = new FlinkFileWriterFactory.Builder(table)
          // ...
          .equalityDeleteRowSchema(schema)
          .build();
}
```

### 2.2 核心写入逻辑 (BaseDeltaTaskWriter)

`BaseDeltaTaskWriter` 中的 `write(RowData row)` 方法展示了两种模式的根本区别。

```java
// BaseDeltaTaskWriter.java

@Override
public void write(RowData row) throws IOException {
    RowDataDeltaWriter writer = route(row); // 根据分区路由到对应的 writer

    switch (row.getRowKind()) {
      case INSERT:
      case UPDATE_AFTER:
        if (upsert) {
          // Upsert 模式关键逻辑：
          // 1. 先尝试删除该主键对应的旧数据 (Delete Key)
          // 2. 再写入新数据 (Write Row)
          writer.deleteKey(keyProjection.wrap(row));
        }
        writer.write(row);
        break;

      case UPDATE_BEFORE:
        if (upsert) {
          // Upsert 模式下，忽略 UPDATE_BEFORE。
          // 因为 INSERT/UPDATE_AFTER 已经包含 "先删后写" 的逻辑，
          // 如果再处理 UPDATE_BEFORE 会导致重复删除，或者不必要的开销。
          break; 
        }
        // 普通模式：UPDATE_BEFORE 产生一个 Delete 操作
        writer.delete(row);
        break;

      case DELETE:
        if (upsert) {
          // Upsert 模式：按主键删除
          writer.deleteKey(keyProjection.wrap(row));
        } else {
          // 普通模式：按配置删除 (可能包含非主键字段的匹配)
          writer.delete(row);
        }
        break;

      default:
        throw new UnsupportedOperationException("Unknown row kind: " + row.getRowKind());
    }
}
```

### 2.3 逻辑流程图解

#### Upsert 模式 (`upsert=true`)
输入流中的数据处理流程：
1.  **收到 `+I` (INSERT) 或 `+U` (UPDATE_AFTER)**:
    *   **Action 1**: 生成一个 `Equality Delete` (针对主键)，标记旧版本数据为删除。
    *   **Action 2**: 生成一个 `Data Insert`，写入新版本数据。
    *   **结果**: 即使之前有相同主键的数据，也会被逻辑覆盖。
2.  **收到 `-U` (UPDATE_BEFORE)**:
    *   **Action**: 直接丢弃 (No-op)。
3.  **收到 `-D` (DELETE)**:
    *   **Action**: 生成一个 `Equality Delete` (针对主键)。

#### 普通模式 (`upsert=false`)
输入流中的数据处理流程：
1.  **收到 `+I` (INSERT) 或 `+U` (UPDATE_AFTER)**:
    *   **Action**: 仅生成 `Data Insert`。
    *   **风险**: 如果表里已有相同主键数据，会导致主键重复 (Duplicates)，除非下游读取时通过合并去重。
2.  **收到 `-U` (UPDATE_BEFORE)**:
    *   **Action**: 生成 `Position Delete` (如果可能) 或 `Equality Delete`，精确删除该版本的旧数据。
3.  **收到 `-D` (DELETE)**:
    *   **Action**: 同上，生成 Delete 记录。

## 3. 总结与建议

*   **使用 Upsert 模式的场景**:
    *   上游数据流可能存在乱序，或者你不能保证 `UPDATE_BEFORE` 和 `UPDATE_AFTER` 总是成对出现且顺序正确。
    *   上游是 Kafka compacted topic，可能丢失了中间状态，只保留了最新值。
    *   你希望 Iceberg 表的行为类似于数据库的 Upsert，只要有新数据来就覆盖旧数据。

*   **使用普通模式的场景**:
    *   上游是严格的 CDC 流 (如 MySQL binlog -> Flink)，完整保留了所有变更历史 (`-U`, `+U`)。
    *   你希望保留数据的每一次变更历史，或者希望减少写入时的 Delete File 开销 (如果确定没有重复数据)。
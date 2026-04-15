# Flink 写入中 equalityFieldColumns 的作用

## 概述

`equalityFieldColumns` 是 Apache Iceberg Flink Sink 中的一个重要配置参数，用于指定**等值删除（Equality Delete）**操作的关键字段。它主要用于支持 CDC（Change Data Capture）场景和 UPSERT 语义。

## 核心概念

### 1. Identifier Fields（标识字段）

在 Iceberg 表的 Schema 中，可以定义一组 **identifier fields**（标识字段），这些字段用于唯一标识表中的一行数据，类似于主键的概念。

- **定义位置**: `Schema.identifierFieldIds()`
- **作用**: 用于等值删除和 UPSERT 操作

### 2. Equality Delete（等值删除）

Iceberg 支持两种删除文件类型：
- **Position Delete**: 基于文件路径 + 行位置的删除
- **Equality Delete**: 基于字段值相等性的删除（通过 equality fields 匹配）

`equalityFieldColumns` 就是用来指定 Equality Delete 的匹配字段。

## 使用场景

### 场景 1: CDC 数据流处理

当从数据库 CDC 流（如 MySQL Binlog、Debezium）写入 Iceberg 表时：

```java
FlinkSink.forRowData(dataStream)
    .tableLoader(tableLoader)
    .equalityFieldColumns(Arrays.asList("id"))  // 指定主键字段
    .upsert(false)  // ⚠️ 标准 CDC 模式必须设置为 false
    .append();
```

**为什么 CDC 场景必须用 `upsert(false)`？**

标准 CDC 流包含**成对的变更事件**（UPDATE_BEFORE 和 UPDATE_AFTER），需要分别处理：

| RowKind | upsert=false (标准 CDC) | upsert=true (UPSERT 模式) |
|---------|------------------------|--------------------------|
| **+I (INSERT)** | 直接写入新记录 | 先按 key 删除旧记录，再写入新记录 |
| **-U (UPDATE_BEFORE)** | 删除旧记录（**完整行数据**） | **忽略**（防止重复删除） |
| **+U (UPDATE_AFTER)** | 直接写入新记录 | 先按 key 删除旧记录，再写入新记录 |
| **-D (DELETE)** | 删除记录（完整行数据） | 按 key 删除记录 |

**关键区别**：
1. **删除方式不同**：
   - `upsert=false`: 使用 `delete(row)` 删除完整行，delete file 存储所有字段
   - `upsert=true`: 使用 `deleteKey(key)` 只按 key 删除，delete file 只存储 equality fields

2. **UPDATE_BEFORE 处理不同**：
   - `upsert=false`: 处理 UPDATE_BEFORE，精确删除旧值
   - `upsert=true`: **忽略** UPDATE_BEFORE（代码注释：`prevent delete one row twice`）

3. **语义不同**：
   - 标准 CDC：记录完整的变更历史（旧值 → 新值）
   - UPSERT：只关心最终状态，不关心中间过程

**示例**：

```java
// MySQL 执行: UPDATE users SET name='Bob', age=31 WHERE id=1
// CDC 流产生：
{rowKind: -U, id: 1, name: "Alice", age: 30}  // UPDATE_BEFORE
{rowKind: +U, id: 1, name: "Bob", age: 31}    // UPDATE_AFTER

// upsert=false 的处理：
// 1. 处理 -U: delete({id:1, name:"Alice", age:30})  → delete file 存储完整行
// 2. 处理 +U: write({id:1, name:"Bob", age:31})     → data file

// upsert=true 的处理：
// 1. 处理 -U: 忽略（什么都不做）
// 2. 处理 +U: deleteKey({id:1}) + write({id:1, name:"Bob", age:31})
//             ⚠️ delete file 只存储 {id:1}，会删除所有 id=1 的记录
```

**工作原理** (upsert=false):
- `+I` (INSERT): 写入新数据文件
- `-U` (UPDATE_BEFORE): 生成 equality delete 文件，删除旧值（**完整行**）
- `+U` (UPDATE_AFTER): 写入新数据文件
- `-D` (DELETE): 生成 equality delete 文件（完整行）

### 场景 2: UPSERT 语义

当需要实现 UPSERT（存在则更新，不存在则插入）语义时，**且数据流中没有 UPDATE_BEFORE 事件**：

```java
FlinkSink.forRowData(dataStream)
    .tableLoader(tableLoader)
    .equalityFieldColumns(Arrays.asList("id"))  // 指定唯一键
    .upsert(true)  // 启用 UPSERT 模式
    .append();
```

**适用场景**：
- 数据流只包含 INSERT 和 DELETE 事件（没有 UPDATE_BEFORE/UPDATE_AFTER）
- 数据流只包含最终状态（如 Kafka Compacted Topic）
- 需要"最后写入获胜"的语义

**工作原理**:
- `+I` / `+U`: 先按 key 删除旧记录（**只存储 key 到 delete file**），再写入新记录
- `-D`: 按 key 删除指定记录
- `-U`: 在 UPSERT 模式下被**忽略**（避免重复删除）

**示例**：

```java
// 数据流（只有 INSERT，没有 UPDATE_BEFORE）
{rowKind: +I, id: 1, name: "Alice", age: 30}
{rowKind: +I, id: 1, name: "Bob", age: 31}     // 相同 id，应该替换
{rowKind: +I, id: 1, name: "Charlie", age: 32} // 相同 id，应该替换

// upsert=true 的处理：
// 1. +I (id:1, Alice): write({id:1, name:"Alice", age:30})
// 2. +I (id:1, Bob):   deleteKey({id:1}) + write({id:1, name:"Bob", age:31})
//                      → delete file 只存储 {id:1}
// 3. +I (id:1, Charlie): deleteKey({id:1}) + write({id:1, name:"Charlie", age:32})

// 最终结果：只有 {id:1, name:"Charlie", age:32}
```

**⚠️ 重要警告**：
- **不要在标准 CDC 流中使用 `upsert=true`**，因为 UPDATE_BEFORE 会被忽略，导致数据不一致
- UPSERT 模式的 delete file 更小（只存储 key），但丢失了完整的变更历史

### 场景 3: 基于业务键的去重

可以指定业务键（而非表的 identifier fields）作为等值字段：

```java
FlinkSink.forRowData(dataStream)
    .tableLoader(tableLoader)
    .equalityFieldColumns(Arrays.asList("order_id", "product_id"))  // 复合业务键
    .upsert(true)
    .append();
```

## 代码实现细节

### 1. 字段 ID 解析

在 `SinkUtil.checkAndGetEqualityFieldIds()` 中：

```java
Set<Integer> checkAndGetEqualityFieldIds(Table table, List<String> equalityFieldColumns) {
    // 默认使用表的 identifier fields
    Set<Integer> equalityFieldIds = Sets.newHashSet(table.schema().identifierFieldIds());
    
    if (equalityFieldColumns != null && !equalityFieldColumns.isEmpty()) {
        // 将列名转换为字段 ID
        Set<Integer> equalityFieldSet = Sets.newHashSetWithExpectedSize(equalityFieldColumns.size());
        for (String column : equalityFieldColumns) {
            NestedField field = table.schema().findField(column);
            equalityFieldSet.add(field.fieldId());
        }
        
        // 如果指定的字段与表的 identifier fields 不一致，使用指定的字段
        if (!equalityFieldSet.equals(table.schema().identifierFieldIds())) {
            LOG.warn("使用作业指定的 equality field columns");
        }
        equalityFieldIds = equalityFieldSet;
    }
    return equalityFieldIds;
}
```

### 2. Delete Schema 生成

在 `BaseDeltaTaskWriter` 中：

```java
BaseDeltaTaskWriter(
    PartitionSpec spec,
    FileFormat format,
    FileWriterFactory<RowData> fileWriterFactory,
    OutputFileFactory fileFactory,
    FileIO io,
    long targetFileSize,
    Schema schema,
    RowType flinkSchema,
    Set<Integer> equalityFieldIds,  // 等值字段 ID
    boolean upsert,
    boolean useDv) {
    
    this.schema = schema;
    // 从完整 schema 中选择 equality fields，生成 delete schema
    this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
    
    // 创建投影，用于提取 equality fields 的值
    this.keyProjection = RowDataProjection.create(
        flinkSchema, 
        schema.asStruct(), 
        deleteSchema.asStruct()
    );
    this.upsert = upsert;
}
```

### 3. 写入逻辑

```java
@Override
public void write(RowData row) throws IOException {
    RowDataDeltaWriter writer = route(row);
    
    switch (row.getRowKind()) {
        case INSERT:
        case UPDATE_AFTER:
            if (upsert) {
                // UPSERT 模式：先删除旧记录
                writer.deleteKey(keyProjection.wrap(row));
            }
            writer.write(row);  // 写入新记录
            break;
            
        case UPDATE_BEFORE:
            if (upsert) {
                break;  // UPSERT 模式忽略 UPDATE_BEFORE
            }
            writer.delete(row);  // 标准 CDC 模式：删除旧记录
            break;
            
        case DELETE:
            if (upsert) {
                writer.deleteKey(keyProjection.wrap(row));
            } else {
                writer.delete(row);
            }
            break;
    }
}
```

## 配置优先级

1. **显式指定 `equalityFieldColumns`**: 使用指定的字段
2. **未指定**: 使用表 Schema 中定义的 `identifierFieldIds()`
3. **都未定义**: 等值字段集合为空（不支持 equality delete）

## 与分区模式的关系

在 **HASH 分布模式** 下，如果设置了 equality fields，则分区字段必须包含在 equality fields 中：

```java
// 错误示例：分区字段 'date' 不在 equality fields 中
table.partitionBy("date");
sink.equalityFieldColumns(Arrays.asList("id"));  // 会抛出异常

// 正确示例
table.partitionBy("date");
sink.equalityFieldColumns(Arrays.asList("id", "date"));  // OK
```

## 性能考虑

### 优点
- 支持高效的行级更新和删除
- 无需重写整个数据文件
- 支持并发写入

### 注意事项
- Equality delete 文件会增加读取时的开销（需要过滤删除的记录）
- 需要定期执行 **compaction** 来合并 delete files 和 data files
- Equality fields 应该选择高选择性的字段（如主键）

## 测试示例

### 示例 1: 基于 ID 的 CDC

```java
// 输入数据
List<Row> input = Arrays.asList(
    row("+I", 1, "aaa"),  // INSERT id=1
    row("-D", 1, "aaa"),  // DELETE id=1
    row("+I", 1, "bbb")   // INSERT id=1
);

// 配置
sink.equalityFieldColumns(Arrays.asList("id"))
    .upsert(false);

// 结果：最终表中只有 (1, "bbb")
```

### 示例 2: 基于 data 字段的 UPSERT

```java
// 输入数据
List<Row> input = Arrays.asList(
    row("+I", 1, "aaa"),  // INSERT
    row("+I", 2, "aaa"),  // INSERT（相同 data）
    row("+U", 3, "aaa")   // UPSERT（相同 data）
);

// 配置
sink.equalityFieldColumns(Arrays.asList("data"))
    .upsert(true);

// 结果：最终表中只有 (3, "aaa")
// 因为 UPSERT 会删除所有 data="aaa" 的旧记录
```

## 相关 API

### Flink Sink Builder API

```java
IcebergSink.forRowData(dataStream)
    .tableLoader(tableLoader)
    .equalityFieldColumns(List<String> columns)  // 设置等值字段
    .upsert(boolean enabled)                     // 启用 UPSERT 模式
    .distributionMode(DistributionMode mode)     // 分布模式
    .append();
```

### Schema API

```java
// 定义表的 identifier fields
Schema schema = new Schema(
    Arrays.asList(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "data", Types.StringType.get())
    ),
    Sets.newHashSet(1)  // 设置 id 为 identifier field
);

// 查询 identifier fields
Set<Integer> identifierIds = schema.identifierFieldIds();
Set<String> identifierNames = schema.identifierFieldNames();
```

## 总结

`equalityFieldColumns` 是 Flink 写入 Iceberg 表时实现 CDC 和 UPSERT 语义的关键配置：

1. **指定唯一标识字段**: 用于匹配和删除旧记录
2. **支持灵活配置**: 可以覆盖表的默认 identifier fields
3. **CDC 场景必备**: 处理 UPDATE/DELETE 操作的基础
4. **UPSERT 语义**: 实现"存在则更新，不存在则插入"的逻辑
5. **性能权衡**: 需要在写入灵活性和读取性能之间平衡

## 参考资料

- **源码位置**:
  - `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergSinkBuilder.java`
  - `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/SinkUtil.java`
  - `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/BaseDeltaTaskWriter.java`
- **测试用例**: `flink/v1.20/flink/src/test/java/org/apache/iceberg/flink/sink/TestFlinkIcebergSinkV2Base.java`
- **Iceberg 规范**: https://iceberg.apache.org/spec/#delete-files

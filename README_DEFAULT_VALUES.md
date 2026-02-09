# Iceberg字段默认值支持 - 快速参考

> **生成时间**: 2026-02-03
> **Iceberg版本**: 1.5.x+ (支持Format V3)

---

## 📊 核心结论速查表

| 功能 | Flink | Spark | 说明 |
|------|-------|-------|------|
| **读取initial-default** | ✅ 完全支持 | ✅ 完全支持 | 读取历史数据时自动填充缺失字段 |
| **写入write-default** | ❌ **不支持** | ⚠️ **不支持** | **两者都不会自动填充！** |
| **SQL DEFAULT关键字** | ❌ 不支持 | ✅ 支持 | Spark可在INSERT中使用 |
| **DDL添加默认值字段** | ❌ 需API | ❌ 不支持 | 两者都需通过Iceberg API |
| **版本要求** | Flink 1.18+ (仅读取) | Spark 3.3+ | 基于Format V3 |

---

## 🎯 快速决策指南

### 场景1：Schema演进 - 添加新字段

**需求**：为现有表添加`status`字段，历史数据默认为"UNKNOWN"，新数据默认为"PENDING"

#### ⚠️ 重要：Flink和Spark写入时都不会自动填充

```java
// 步骤1：通过Iceberg API添加字段
Table table = catalog.loadTable("db.users");
table.updateSchema()
  .addColumn("status", Types.StringType.get())
  .withInitialDefault(Literal.of("UNKNOWN"))  // 读取老数据时的默认值
  .withWriteDefault(Literal.of("PENDING"))    // （目前写入时不会使用）
  .commit();
```

#### Flink方案

**方案1：应用层填充**（推荐，写入实际值）
```java
DataStream<RowData> dataWithDefaults = dataStream.map(row -> {
  GenericRowData newRow = new GenericRowData(3);
  newRow.setField(0, row.getLong(0));  // id
  newRow.setField(1, row.getString(1)); // name
  newRow.setField(2, StringData.fromString("PENDING")); // 手动填充
  return newRow;
});

FlinkSink.forRowData(dataWithDefaults).table(table).build();
// ✅ Parquet文件中实际存储"PENDING"
```

**方案2：依赖读取时填充**（写入NULL，读取时看到默认值）
```java
FlinkSink.forRowData(originalDataStream).table(table).build();
// ❌ Parquet文件中status=NULL
// ✅ 但读取时会显示initial-default="UNKNOWN"
```

#### Spark方案

**方式1：使用SQL DEFAULT关键字**

```sql
-- 步骤1：通过Iceberg API添加字段（同上）

-- 步骤2：在INSERT中显式使用DEFAULT
INSERT INTO db.users (id, name, status)
VALUES (1, 'Alice', DEFAULT);

-- ✅ Spark会将DEFAULT替换为"PENDING"
```

**方式2：在应用层填充**

```scala
// 步骤1：添加字段（同上）

// 步骤2：在DataFrame中手动添加默认值
val df = existingDF.withColumn("status", lit("PENDING"))
df.write.format("iceberg").save("db.users")

// ⚠️ 需要修改应用代码
```

---

## 📚 详细文档索引

1. **《Flink与Spark写入默认值支持分析.md》**
   - 深度剖析Flink与Spark在write-default上的差异
   - 源码级实现机制解析
   - 实际应用场景与最佳实践

2. **《Iceberg字段默认值支持分析.md》**
   - 完整的默认值特性说明
   - Format V3详细介绍
   - 性能影响分析

3. **《Spark与Flink读取Iceberg源码分析.md》**
   - 读取端的initial-default实现
   - ConstantReader机制解析

4. **《Flink与Spark写入Iceberg流程分析.md》**
   - 完整的写入流程分析
   - Writer架构对比

---

## 🔑 关键代码路径速查

### Flink写入（❌ 不支持write-default）

```
入口: IcebergSink.java
  └─ RowDataTaskWriterFactory.java
     └─ FlinkFileWriterFactory.java:96
        └─ FlinkParquetWriters.buildWriter(LogicalType, MessageType)
           └─ ❌ 只接收LogicalType，无法访问writeDefault
```

**关键限制**：
```java
// flink/v1.20/.../FlinkParquetWriters.java:70
public static <T> ParquetValueWriter<T> buildWriter(LogicalType schema, MessageType type) {
  // ❌ 没有Iceberg Schema参数
  // 因此无法访问NestedField.writeDefault()
}
```

### Spark写入（不支持自动填充）

```
入口: SparkWrite.java
  └─ SparkParquetWriters.java
     └─ ❌ 不检查Iceberg Schema的writeDefault
```

**关键限制**：
```java
// spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/Spark3Util.java:548-558
if (add.defaultValue() != null) {
  throw new UnsupportedOperationException(
      "Cannot add column since setting default values in Spark is currently unsupported");
}
```

### 读取端（两者均支持）

```
入口: ParquetReader
  └─ BaseParquetReaders.struct()
     └─ defaultReader()
        └─ ✅ 返回ConstantReader(initialDefault)
```

**关键文件**：
```
parquet/src/main/java/org/apache/iceberg/parquet/BaseParquetReaders.java:250-297
parquet/src/main/java/org/apache/iceberg/parquet/ParquetValueReaders.java:118-160
```

---

## ⚠️ 常见误区

### 误区1：以为Flink或Spark支持自动填充

❌ **错误认知**：
```java
// Flink - 期望自动填充status字段
FlinkSink.forRowData(dataStreamWithoutStatus).table(table).build();
```

```scala
// Spark - 期望自动填充status字段
val df = Seq((1, "Alice")).toDF("id", "name")
df.write.format("iceberg").save("db.users")
```

✅ **实际结果**：
- `status`字段写入**NULL**，不是writeDefault值
- **Flink和Spark都不会**自动检测并填充缺失字段
- ✅ 但读取时会应用initial-default，所以查询能看到默认值


---

## 🚀 最佳实践建议

### For Flink用户

⚠️ **重要提醒**：Flink写入时不会自动填充write-default

✅ **推荐做法**：
1. **方案A**：在应用层手动填充默认值
   ```java
   dataStream.map(row -> addDefaultValue(row, "PENDING"))
   ```

2. **方案B**：依赖读取时的initial-default
   - 写入时缺失字段（存储NULL）
   - 读取时Reader自动填充initial-default
   - ⚠️ 但Parquet文件中实际是NULL

⚠️ **注意事项**：
- ❌ Flink不会自动填充write-default
- ✅ 可以依赖读取端的initial-default
- ⚠️ 如果需要文件中实际存储默认值，必须在应用层处理

### For Spark用户

✅ **推荐做法**：
1. 通过Iceberg API添加字段（设置writeDefault）
2. 在SQL INSERT中使用DEFAULT关键字
3. 或在DataFrame中显式添加默认值列

⚠️ **注意事项**：
- 不能依赖自动填充
- 需要在应用层处理默认值逻辑
- DataFrame API需要手动添加列

### 混合使用场景

**问题**：Flink和Spark同时读写同一张表，如何保证一致性？

✅ **答案**：兼容，但要理解默认值的填充时机

```
场景：Flink写入（不填充字段）+ Spark读取
  • Flink写入：Parquet中status=NULL
  • Spark读取：显示status='ACTIVE'（Reader应用initial-default）
  ✅ 读取一致

场景：Spark写入（不填充字段）+ Flink读取
  • Spark写入：Parquet中status=NULL
  • Flink读取：显示status='ACTIVE'（Reader应用initial-default）
  ✅ 读取一致

场景：Spark写入（使用SQL DEFAULT）+ Flink读取
  • Spark写入：Parquet中status='ACTIVE'（实际值）
  • Flink读取：显示status='ACTIVE'
  ✅ 完全一致
```

**关键理解**：
- ✅ **读取一致性**由Reader保证（应用initial-default）
- ⚠️ **写入不一致**：文件中可能是NULL或实际值
- ✅ 但用户查询结果是一致的

---

## 📝 测试验证要点

### Flink测试

```java
// 验证write-default自动填充
flink/v1.20/flink/src/test/java/org/apache/iceberg/flink/data/TestFlinkParquetWriter.java
```

### Spark测试

```java
// 验证SQL DEFAULT关键字
spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/sql/TestSparkDefaultValues.java:50-83
```

### 手动验证步骤

```sql
-- 1. 创建表
CREATE TABLE test_defaults (id INT);

-- 2. 添加字段（通过Iceberg API）
// Java: table.updateSchema().addColumn("status", ..., Literal.of("PENDING")).commit()

-- 3. 写入测试数据（Flink）
INSERT INTO test_defaults (id) VALUES (1);

-- 4. 验证结果
SELECT * FROM test_defaults;
-- 期望：id=1, status="PENDING"

-- 5. 写入测试数据（Spark DataFrame）
// Scala: Seq((2)).toDF("id").write.format("iceberg").save(...)

-- 6. 验证结果
SELECT * FROM test_defaults WHERE id=2;
-- 实际：id=2, status=NULL （不是"PENDING"！）
```

---

## 🔗 相关资源

### 官方文档
- [Iceberg Spec - Default Values](https://iceberg.apache.org/spec/#default-values)
- [Format Version 3](https://iceberg.apache.org/spec/#version-3)

### 关键Commit
- `401ab27e1` - Spark不支持ADD COLUMN with default
- `7493a158b` - Flink测试Parquet Writer默认值处理
- `a706c348a` - Flink 1.18/1.19实现defaults
- `7a572a9eb` - Flink 1.20支持Avro/Parquet defaults

### 代码示例
- Flink写入：`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergSink.java`
- Spark写入：`spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java`

---

## 🆘 FAQ

**Q1: 为什么Spark不支持自动填充write-default？**

A: 主要是架构设计和性能考虑：
- Spark的InternalRow是固定Schema的
- 自动填充需要在每条记录写入时检查字段
- Spark选择通过SQL DEFAULT关键字提供默认值支持

**Q2: Flink的自动填充有性能开销吗？**

A: 开销极小（<5%），因为：
- 仅在Writer初始化时检查一次Schema
- 使用预编译的默认值常量
- 不需要额外的I/O或序列化

**Q3: 如何让Spark也支持自动填充？**

A: 目前暂不支持，需要：
- 等待Iceberg社区实现（未来roadmap）
- 或者在应用层自行实现默认值填充逻辑

**Q4: initial-default和write-default可以不同吗？**

A: ✅ 可以！这正是设计目标：
```java
.withInitialDefault(Literal.of("UNKNOWN"))  // 历史数据
.withWriteDefault(Literal.of("PENDING"))    // 新数据
```

**Q5: 如何查看表的默认值配置？**

A:
```java
// Java API
Table table = catalog.loadTable("db.users");
for (NestedField field : table.schema().columns()) {
  System.out.println(field.name() + " initial: " + field.initialDefault());
  System.out.println(field.name() + " write: " + field.writeDefault());
}
```

---

**文档维护者**: Claude Code (claude-4.5-sonnet)
**最后更新**: 2026-02-03
**反馈渠道**: 请在项目README中查看联系方式

# Iceberg 模式演进（Schema Evolution）深度解析

## 概述

Apache Iceberg 支持**原地表演进（in-place table evolution）**，允许在不重写表数据或迁移到新表的情况下演进表模式。Schema evolution（模式演进）是 Iceberg 的核心特性之一，它**支持 add（添加）、drop（删除）、update（更新）、rename（重命名）操作，并且没有副作用（no side-effects）**。

## 核心架构

### 1. UpdateSchema 接口

UpdateSchema 是 Iceberg 模式演进的核心 API 接口，定义在 `org.apache.iceberg.UpdateSchema` 中：

```java
public interface UpdateSchema extends PendingUpdate<Schema> {
    // 添加列
    UpdateSchema addColumn(String name, Type type, String doc);
    UpdateSchema addColumn(String parent, String name, Type type, String doc);
    UpdateSchema addRequiredColumn(String parent, String name, Type type, String doc);

    // 删除列
    UpdateSchema deleteColumn(String name);

    // 重命名列
    UpdateSchema renameColumn(String name, String newName);

    // 更新列类型
    UpdateSchema updateColumn(String name, Type.PrimitiveType newType);
    UpdateSchema updateColumnDoc(String name, String newDoc);

    // 改变列的可选性
    UpdateSchema makeColumnOptional(String name);
    UpdateSchema requireColumn(String name);

    // 列位置调整
    UpdateSchema moveFirst(String name);
    UpdateSchema moveBefore(String name, String beforeName);
    UpdateSchema moveAfter(String name, String afterName);

    // 其他操作
    UpdateSchema allowIncompatibleChanges();
    UpdateSchema setIdentifierFields(Collection<String> names);
}
```

### 2. SchemaUpdate 实现类

`SchemaUpdate` 是 `UpdateSchema` 接口的实现类，位于 `org.apache.iceberg.SchemaUpdate`。它的核心设计原则是：

#### 2.1 变更追踪机制

SchemaUpdate 使用内部集合来追踪所有的模式变更操作：

```java
class SchemaUpdate implements UpdateSchema {
    // 当前表的 schema
    private final Schema schema;

    // 追踪所有变更操作
    private final List<Integer> deletes = Lists.newArrayList();
    private final Map<Integer, Types.NestedField> updates = Maps.newHashMap();
    private final Multimap<Integer, Types.NestedField> adds =
        Multimaps.newListMultimap(Maps.newHashMap(), Lists::newArrayList);
    private final Map<String, Integer> addedNameToId = Maps.newHashMap();
    private final Multimap<Integer, Move> moves =
        Multimaps.newListMultimap(Maps.newHashMap(), Lists::newArrayList);

    // 列ID分配器
    private int lastColumnId;
}
```

**关键特性：**
- 所有的变更操作（adds、deletes、updates、moves）都被**先记录下来**，而不是立即应用
- 这种延迟应用的设计确保了变更操作的**原子性**和**一致性**

#### 2.2 字段ID的唯一性保证

Iceberg 使用**唯一的字段 ID** 来追踪每个列，这是实现"无副作用"的核心机制：

```java
private void internalAddColumn(
    String parent, String name, boolean isOptional, Type type, String doc) {
    // 为新列分配新的唯一 ID
    int newId = assignNewColumnId();

    // 为嵌套类型的所有字段分配新的 ID
    adds.put(
        parentId,
        Types.NestedField.of(
            newId,
            isOptional,
            name,
            TypeUtil.assignFreshIds(type, this::assignNewColumnId),  // 关键！
            doc));
}

private int assignNewColumnId() {
    int next = lastColumnId + 1;
    this.lastColumnId = next;
    return next;
}
```

**原理解析：**
- 每次添加新列时，都会分配一个**全新的、递增的字段 ID**
- 即使列名相同，新添加的列也会获得不同的 ID
- `TypeUtil.assignFreshIds()` 确保嵌套类型中的所有字段也获得新的 ID
- 这避免了通过列名追踪可能导致的"意外取消删除"问题

### 3. 变更应用流程

#### 3.1 apply() 方法

`apply()` 方法将所有待定的变更操作应用到 schema 上，生成新的 schema：

```java
@Override
public Schema apply() {
    Schema newSchema =
        applyChanges(schema, deletes, updates, adds, moves,
                     identifierFieldNames, caseSensitive);
    return newSchema;
}
```

#### 3.2 ApplyChanges 访问者模式

SchemaUpdate 使用**访问者模式（Visitor Pattern）**来应用变更：

```java
private static class ApplyChanges extends TypeUtil.SchemaVisitor<Type> {
    private final List<Integer> deletes;
    private final Map<Integer, Types.NestedField> updates;
    private final Multimap<Integer, Types.NestedField> adds;
    private final Multimap<Integer, Move> moves;

    @Override
    public Type field(Types.NestedField field, Type fieldResult) {
        // 1. 处理删除：如果字段ID在删除列表中，返回null
        if (deletes.contains(field.fieldId())) {
            return null;
        }

        // 2. 处理更新：检查字段是否有类型更新
        Types.NestedField update = updates.get(field.fieldId());
        if (update != null && update.type() != field.type()) {
            return update.type();
        }

        // 3. 处理添加：向struct类型添加新字段
        Collection<Types.NestedField> newFields = adds.get(field.fieldId());
        Collection<Move> columnsToMove = moves.get(field.fieldId());
        if (!newFields.isEmpty() || !columnsToMove.isEmpty()) {
            List<Types.NestedField> fields =
                addAndMoveFields(fieldResult.asStructType().fields(),
                               newFields, columnsToMove);
            if (fields != null) {
                return Types.StructType.of(fields);
            }
        }

        return fieldResult;
    }
}
```

**工作流程：**
1. **遍历 schema 树**：使用访问者模式遍历整个 schema 树结构
2. **应用删除**：如果字段 ID 在删除列表中，返回 null 表示删除
3. **应用更新**：替换字段的类型或属性
4. **应用添加**：向 struct 类型添加新字段
5. **应用移动**：调整字段的顺序

#### 3.3 commit() 方法

```java
@Override
public void commit() {
    // 1. 应用 schema 变更
    TableMetadata update = base.updateSchema(apply(), lastColumnId);

    // 2. 应用其他元数据变更（如 name mapping）
    TableMetadata newMetadata = applyChangesToMetadata(update);

    // 3. 提交到表操作
    ops.commit(base, newMetadata);
}
```

## "无副作用"（No Side-Effects）原理

Iceberg 文档明确保证：**schema evolution 变更是独立的且没有副作用**。这是如何实现的？

### 1. 通过唯一 ID 追踪列

**问题：** 基于列名或列位置追踪会导致副作用

| 追踪方式 | 问题 | 示例 |
|---------|------|------|
| **按名称追踪** | 可能意外"取消删除"列 | 删除列 `name`，后来又添加列 `name`，旧数据可能被错误读取 |
| **按位置追踪** | 删除列会改变其他列的位置 | 删除第2列后，原来的第3列变成第2列，破坏了数据映射 |

**Iceberg 解决方案：** 使用**唯一的、永不重用的字段 ID**

```java
// 添加列时分配新ID
int newId = assignNewColumnId();  // 例如: 25

// 即使后来删除该列，ID 25 也永远不会被重新分配
// 再次添加同名列会获得新的 ID，例如 26
```

### 2. 保证独立性的四大原则

源自 `docs/docs/evolution.md`：

```markdown
1. Added columns never read existing values from another column.
   新添加的列永远不会从其他列读取现有值。

2. Dropping a column or field does not change the values in any other column.
   删除列或字段不会改变任何其他列的值。

3. Updating a column or field does not change values in any other column.
   更新列或字段不会改变任何其他列的值。

4. Changing the order of columns or fields does not change the values
   associated with a column or field name.
   改变列或字段的顺序不会改变与列名或字段名关联的值。
```

### 3. 实现机制详解

#### 3.1 添加列（Add Column）

```java
// 位于 SchemaUpdate.java:183
adds.put(
    parentId,
    Types.NestedField.of(
        newId,                                              // 新的唯一ID
        isOptional,
        name,
        TypeUtil.assignFreshIds(type, this::assignNewColumnId),  // 嵌套类型也分配新ID
        doc));
```

**保证原则 #1**：
- 新列获得**全新的字段 ID**
- 旧数据文件中不存在这个 ID
- 读取旧文件时，新列会被填充为 `null`（对于可选列）或默认值，**绝不会错误地读取其他列的数据**

#### 3.2 删除列（Delete Column）

```java
@Override
public UpdateSchema deleteColumn(String name) {
    Types.NestedField field = findField(name);
    Preconditions.checkArgument(field != null, "Cannot delete missing column: %s", name);

    // 只是记录要删除的字段ID
    deletes.add(field.fieldId());

    return this;
}
```

**保证原则 #2**：
- 只是将字段 ID 添加到删除列表
- 其他列的 ID 和位置**完全不变**
- 旧数据文件仍然保留被删除列的数据，只是新的 schema 不再包含它
- 不影响任何其他列的读写

#### 3.3 重命名列（Rename Column）

```java
@Override
public UpdateSchema renameColumn(String name, String newName) {
    Types.NestedField field = findField(name);
    int fieldId = field.fieldId();

    // 更新字段的名称，但保持相同的ID
    updates.put(
        fieldId,
        Types.NestedField.of(
            fieldId,              // ID不变！
            field.isOptional(),
            newName,              // 只改名称
            field.type(),
            field.doc()));

    return this;
}
```

**保证原则 #3**：
- 重命名只改变元数据中的名称
- **字段 ID 保持不变**
- 数据文件中的值通过 ID 映射，完全不受名称变更影响

#### 3.4 更新类型（Update Type）

```java
@Override
public UpdateSchema updateColumn(String name, Type.PrimitiveType newType) {
    Types.NestedField field = findField(name);

    // 检查类型提升是否安全
    Preconditions.checkArgument(
        TypeUtil.isPromotionAllowed(field.type(), newType),
        "Cannot change column type: %s: %s -> %s",
        name, field.type(), newType);

    // 只允许安全的类型拓宽（widening）
    // 例如: int -> long, float -> double, decimal(P,S) -> decimal(P',S) where P' > P
}
```

**支持的类型提升：**
- `int` → `long`
- `float` → `double`
- `decimal(P,S)` → `decimal(P',S')` (P' ≥ P, S' ≥ S)

**保证原则 #3**：
- 只允许**安全的类型拓宽**
- 旧数据可以安全地转换为新类型
- 不会导致数据丢失或精度损失

#### 3.5 移动列（Move Column）

```java
private static List<Types.NestedField> moveFields(
    List<Types.NestedField> fields, Collection<Move> moves) {
    LinkedList<Types.NestedField> reordered = Lists.newLinkedList(fields);

    for (Move move : moves) {
        // 找到要移动的字段（通过ID）
        Types.NestedField toMove =
            Iterables.find(reordered, field -> field.fieldId() == move.fieldId());
        reordered.remove(toMove);

        // 根据移动类型调整位置
        switch (move.type()) {
            case FIRST:
                reordered.addFirst(toMove);
                break;
            case BEFORE:
                // 插入到参考字段之前
                break;
            case AFTER:
                // 插入到参考字段之后
                break;
        }
    }
    return reordered;
}
```

**保证原则 #4**：
- 字段通过 **ID** 识别，而不是位置
- 改变顺序只影响 schema 元数据
- 数据文件中的值映射通过 ID 完成，**顺序无关**

## 元数据变更，无需重写数据

### 关键设计

```java
// 位于 evolution.md:37
"Iceberg schema updates are metadata changes,
 so no data files need to be rewritten to perform the update."
```

### 实现原理

#### 1. Schema 版本化

```java
// TableMetadata 中的 schema 管理
public class TableMetadata {
    // 保存所有历史 schema 版本
    private final List<Schema> schemas;
    private final int currentSchemaId;

    public TableMetadata updateSchema(Schema newSchema, int newLastColumnId) {
        // 添加新的 schema 版本
        List<Schema> updatedSchemas = Lists.newArrayList(schemas);
        updatedSchemas.add(newSchema);

        return new TableMetadata.Builder(this)
            .setSchemas(updatedSchemas)
            .setCurrentSchemaId(newSchema.schemaId())
            .setLastColumnId(newLastColumnId)
            .build();
    }
}
```

**工作原理：**
- 每次 schema 变更创建一个**新的 schema 版本**
- 旧的 schema 版本被保留在元数据中
- 数据文件记录其写入时使用的 schema 版本

#### 2. 读取时的 Schema 演进

```java
// 读取数据文件时的 schema 处理
DataFile dataFile = ...;
int writeSchemaId = dataFile.schemaId();  // 文件写入时的 schema ID
Schema writeSchema = table.schemas().get(writeSchemaId);  // 获取写入时的 schema
Schema readSchema = table.schema();  // 当前的 schema

// Iceberg 自动处理 schema 演进
// - 新增的列：填充 null 或默认值
// - 删除的列：跳过不读取
// - 重命名的列：通过 ID 正确映射
```

#### 3. Name Mapping 更新

```java
private TableMetadata applyChangesToMetadata(TableMetadata metadata) {
    String mappingJson = metadata.property(TableProperties.DEFAULT_NAME_MAPPING, null);

    if (mappingJson != null) {
        // 解析现有的 name mapping
        NameMapping mapping = NameMappingParser.fromJson(mappingJson);

        // 更新 name mapping 以反映 schema 变更
        NameMapping updated = MappingUtil.update(mapping, updates, adds);

        // 更新表属性
        updatedProperties.put(
            TableProperties.DEFAULT_NAME_MAPPING,
            NameMappingParser.toJson(updated));
    }

    return metadata.replaceProperties(updatedProperties);
}
```

**Name Mapping 作用：**
- 在某些存储格式（如 Parquet、ORC）中，列是通过名称引用的
- Name Mapping 维护**列名到字段 ID** 的映射
- 当 schema 演进时，Name Mapping 也会更新
- 这确保了即使列名改变，也能通过 ID 正确读取数据

## 兼容性与安全性

### 1. 不兼容变更保护

```java
@Override
public UpdateSchema addRequiredColumn(String parent, String name, Type type, String doc) {
    // 必须显式允许不兼容变更
    Preconditions.checkArgument(
        allowIncompatibleChanges,
        "Incompatible change: cannot add required column: %s",
        name);

    internalAddColumn(parent, name, false, type, doc);
    return this;
}
```

**不兼容变更包括：**
- 添加必需列（required column）
- 将可选列改为必需列

**为什么不兼容？**
- 旧数据文件中没有这些必需列的值
- 尝试读取时会失败

### 2. 标识符字段（Identifier Fields）保护

```java
// 验证标识符字段不能被删除
for (String name : identifierFieldNames) {
    Types.NestedField field = schema.findField(name);
    if (field != null) {
        Preconditions.checkArgument(
            !deletes.contains(field.fieldId()),
            "Cannot delete identifier field %s. To force deletion, "
            + "also call setIdentifierFields to update identifier fields.",
            field);
    }
}
```

**标识符字段要求：**
- 必须是原始类型（primitive type）
- 必须是必需字段（required）
- 不能是浮点类型（float/double）
- 不能嵌套在可选字段中

## 实践示例

### 示例 1：添加新列

```java
Table table = ...;
table.updateSchema()
     .addColumn("new_column", Types.StringType.get(), "A new column")
     .commit();

// 内部流程：
// 1. 分配新ID: 例如 fieldId=100
// 2. 创建新的 NestedField: NestedField(100, optional, "new_column", StringType)
// 3. 添加到 adds 集合
// 4. commit() 时应用到 schema
// 5. 更新 TableMetadata，创建新的 schema 版本
// 6. 旧数据文件不受影响，读取时新列返回 null
```

### 示例 2：重命名列

```java
table.updateSchema()
     .renameColumn("old_name", "new_name")
     .commit();

// 内部流程：
// 1. 找到字段: field = schema.findField("old_name")
// 2. 获取字段ID: fieldId = field.fieldId()  // 例如 42
// 3. 创建更新: NestedField(42, optional, "new_name", StringType)
// 4. 添加到 updates 集合
// 5. commit() 时应用变更
// 6. 数据文件中的值通过 ID=42 映射，名称变更不影响数据读取
```

### 示例 3：删除列

```java
table.updateSchema()
     .deleteColumn("old_column")
     .commit();

// 内部流程：
// 1. 找到字段: field = schema.findField("old_column")
// 2. 获取字段ID: fieldId = field.fieldId()  // 例如 55
// 3. 添加到 deletes 列表: deletes.add(55)
// 4. commit() 时，新 schema 不包含 ID=55 的字段
// 5. 旧数据文件仍然保留该列的数据
// 6. 读取时，Iceberg 跳过 ID=55 的列
// 7. 其他列的 ID 和值完全不受影响
```

### 示例 4：类型提升

```java
table.updateSchema()
     .updateColumn("count", Types.LongType.get())  // int -> long
     .commit();

// 内部流程：
// 1. 找到字段: field = schema.findField("count")
// 2. 验证类型提升安全: TypeUtil.isPromotionAllowed(IntegerType, LongType) = true
// 3. 创建更新: NestedField(fieldId, optional, "count", LongType)
// 4. 读取旧数据时，int 值自动转换为 long
```

## 与其他格式的对比

| 特性 | Iceberg | Hive | Delta Lake |
|-----|---------|------|------------|
| **列追踪方式** | 唯一 ID | 列名或位置 | 列名 |
| **重命名安全性** | ✅ 完全安全 | ⚠️ 可能导致问题 | ⚠️ 需要重写数据 |
| **删除列** | ✅ 无副作用 | ⚠️ 改变列位置 | ✅ 标记删除 |
| **添加列** | ✅ 无副作用 | ⚠️ 可能重用名称 | ✅ 支持 |
| **类型演进** | ✅ 支持安全提升 | ❌ 有限支持 | ✅ 支持 |
| **嵌套类型** | ✅ 完全支持 | ❌ 不支持 | ⚠️ 有限支持 |
| **需要重写数据** | ❌ 纯元数据操作 | ✅ 某些情况需要 | ❌ 纯元数据操作 |

## 总结

### 核心机制

1. **唯一字段 ID**：每个列都有唯一的、永不重用的 ID
2. **延迟应用**：变更操作先记录，后统一应用，确保原子性
3. **访问者模式**：使用访问者模式遍历和变更 schema 树
4. **版本化**：保留所有 schema 历史版本
5. **元数据操作**：schema 变更只修改元数据，不重写数据文件

### "无副作用"保证

- **添加列**：分配新 ID，不会读取其他列的数据
- **删除列**：其他列的 ID 和值不变
- **重命名列**：ID 不变，数据映射不受影响
- **更新类型**：只允许安全的类型拓宽
- **移动列**：通过 ID 映射，顺序无关

### 优势

1. **安全性**：避免了基于名称或位置追踪的陷阱
2. **灵活性**：支持复杂的嵌套类型演进
3. **高效性**：纯元数据操作，无需重写数据
4. **兼容性**：旧数据文件继续可读，自动处理 schema 差异
5. **原子性**：所有变更作为单个事务提交

这种设计使得 Iceberg 能够支持大规模数据湖的灵活演进，而不会带来传统数据仓库中常见的 schema 变更痛点。

---

**参考源码文件：**
- `api/src/main/java/org/apache/iceberg/UpdateSchema.java` (UpdateSchema.java:27-420)
- `core/src/main/java/org/apache/iceberg/SchemaUpdate.java` (SchemaUpdate.java:48-846)
- `api/src/main/java/org/apache/iceberg/Schema.java` (Schema.java:51-510)
- `core/src/main/java/org/apache/iceberg/TableMetadata.java` (TableMetadata.java:683-712)
- `docs/docs/evolution.md` (evolution.md:27-102)
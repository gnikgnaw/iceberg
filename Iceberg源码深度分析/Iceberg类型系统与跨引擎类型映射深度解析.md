# Apache Iceberg 类型系统与跨引擎类型映射深度解析

## 目录

- [1. 概述](#1-概述)
- [2. 核心类型体系架构](#2-核心类型体系架构)
  - [2.1 Type 接口：类型系统的根基](#21-type-接口类型系统的根基)
  - [2.2 TypeID 枚举：类型标识与 Java 类型映射](#22-typeid-枚举类型标识与-java-类型映射)
  - [2.3 PrimitiveType 抽象基类](#23-primitivetype-抽象基类)
  - [2.4 NestedType 抽象基类](#24-nestedtype-抽象基类)
- [3. 原始类型详解](#3-原始类型详解)
  - [3.1 无参数单例类型](#31-无参数单例类型)
  - [3.2 带参数类型](#32-带参数类型)
  - [3.3 时间戳类型家族](#33-时间戳类型家族)
  - [3.4 特殊类型](#34-特殊类型)
- [4. 复合类型与嵌套结构](#4-复合类型与嵌套结构)
  - [4.1 NestedField：字段的核心抽象](#41-nestedfield字段的核心抽象)
  - [4.2 StructType：结构体类型](#42-structtype结构体类型)
  - [4.3 ListType：列表类型](#43-listtype列表类型)
  - [4.4 MapType：映射类型](#44-maptype映射类型)
  - [4.5 VariantType：半结构化变体类型](#45-varianttype半结构化变体类型)
- [5. Field ID 管理与分配策略](#5-field-id-管理与分配策略)
  - [5.1 为什么 Field ID 而非名称是核心标识](#51-为什么-field-id-而非名称是核心标识)
  - [5.2 Schema 中的 ID 管理](#52-schema-中的-id-管理)
  - [5.3 AssignFreshIds：ID 分配核心实现](#53-assignfreshidsid-分配核心实现)
  - [5.4 reassignIds 与 reassignOrRefreshIds](#54-reassignids-与-reassignorrefreshids)
  - [5.5 ID 冲突解决机制](#55-id-冲突解决机制)
- [6. TypeUtil 工具类深度分析](#6-typeutil-工具类深度分析)
  - [6.1 Schema 投影与裁剪](#61-schema-投影与裁剪)
  - [6.2 Schema 合并 (join)](#62-schema-合并-join)
  - [6.3 索引构建方法族](#63-索引构建方法族)
  - [6.4 SchemaVisitor 访问者模式](#64-schemavisitor-访问者模式)
  - [6.5 内存大小估算](#65-内存大小估算)
- [7. 类型提升规则](#7-类型提升规则)
  - [7.1 isPromotionAllowed 的精确规则](#71-ispromotionallowed-的精确规则)
  - [7.2 类型提升在 Schema 演进中的应用](#72-类型提升在-schema-演进中的应用)
  - [7.3 类型提升的限制与设计考量](#73-类型提升的限制与设计考量)
- [8. Schema 演进中的类型兼容性](#8-schema-演进中的类型兼容性)
  - [8.1 SchemaUpdate 的类型变更校验](#81-schemaupdate-的类型变更校验)
  - [8.2 CheckCompatibility：兼容性检查引擎](#82-checkcompatibility兼容性检查引擎)
  - [8.3 写入兼容性 vs 读取兼容性](#83-写入兼容性-vs-读取兼容性)
- [9. Iceberg 与 Parquet 类型映射](#9-iceberg-与-parquet-类型映射)
  - [9.1 Iceberg 到 Parquet 映射 (TypeToMessageType)](#91-iceberg-到-parquet-映射-typetomessagetype)
  - [9.2 Parquet 到 Iceberg 映射 (MessageTypeToType)](#92-parquet-到-iceberg-映射-messagetypetotype)
  - [9.3 Parquet 映射的特殊处理](#93-parquet-映射的特殊处理)
- [10. Iceberg 与 ORC 类型映射](#10-iceberg-与-orc-类型映射)
  - [10.1 Iceberg 到 ORC 映射 (ORCSchemaUtil.convert)](#101-iceberg-到-orc-映射-orcschemautil-convert)
  - [10.2 ORC 到 Iceberg 映射 (OrcToIcebergVisitor)](#102-orc-到-iceberg-映射-orctoicebergvisitor)
  - [10.3 ORC 特有的属性标注机制](#103-orc-特有的属性标注机制)
  - [10.4 ORC Schema 投影与类型提升](#104-orc-schema-投影与类型提升)
- [11. Iceberg 与 Avro 类型映射](#11-iceberg-与-avro-类型映射)
  - [11.1 Iceberg 到 Avro 映射 (TypeToSchema)](#111-iceberg-到-avro-映射-typetoschema)
  - [11.2 Avro 到 Iceberg 映射 (SchemaToType)](#112-avro-到-iceberg-映射-schematotype)
  - [11.3 Avro 特有的可空性处理](#113-avro-特有的可空性处理)
  - [11.4 Avro 名称兼容性处理](#114-avro-名称兼容性处理)
- [12. Iceberg 与 Spark 类型映射](#12-iceberg-与-spark-类型映射)
  - [12.1 Iceberg 到 Spark 映射 (TypeToSparkType)](#121-iceberg-到-spark-映射-typetosparktype)
  - [12.2 Spark 到 Iceberg 映射 (SparkTypeToType)](#122-spark-到-iceberg-映射-sparktypetotype)
  - [12.3 Spark 版本差异 (v3.4/v3.5/v4.0/v4.1)](#123-spark-版本差异-v34v35v40v41)
- [13. Iceberg 与 Flink 类型映射](#13-iceberg-与-flink-类型映射)
  - [13.1 Iceberg 到 Flink 映射 (TypeToFlinkType)](#131-iceberg-到-flink-映射-typetoflinktype)
  - [13.2 Flink 到 Iceberg 映射 (FlinkTypeToType)](#132-flink-到-iceberg-映射-flinktypetotype)
  - [13.3 Flink 的精度与可空性处理](#133-flink-的精度与可空性处理)
- [14. 跨引擎类型映射全景对照表](#14-跨引擎类型映射全景对照表)
  - [14.1 原始类型完整映射表](#141-原始类型完整映射表)
  - [14.2 复合类型映射表](#142-复合类型映射表)
  - [14.3 Java 运行时类型映射表](#143-java-运行时类型映射表)
- [15. 常见类型不兼容问题与解决方案](#15-常见类型不兼容问题与解决方案)
  - [15.1 时间类型不兼容](#151-时间类型不兼容)
  - [15.2 UUID 类型的引擎差异](#152-uuid-类型的引擎差异)
  - [15.3 Decimal 精度溢出问题](#153-decimal-精度溢出问题)
  - [15.4 TimestampNano 的引擎支持差异](#154-timestampnano-的引擎支持差异)
  - [15.5 Variant 类型的格式版本限制](#155-variant-类型的格式版本限制)
  - [15.6 Fixed 与 Binary 在 ORC 中的二义性](#156-fixed-与-binary-在-orc-中的二义性)
  - [15.7 Spark 中的 Short/Byte 类型损失](#157-spark-中的-shortbyte-类型损失)
- [16. 总结与最佳实践](#16-总结与最佳实践)

---

## 1. 概述

Apache Iceberg 作为一个跨引擎的开放表格式，其类型系统设计的核心挑战在于：如何定义一套统一的、自描述的类型体系，使其能够在 Spark、Flink、Trino、Hive 等不同计算引擎之间无缝映射，同时支持 Parquet、ORC、Avro 三种主流存储格式。

Iceberg 的类型系统具有以下关键设计特征：

1. **独立于引擎的类型定义**：Iceberg 定义了自己的类型层次结构，不依赖于任何特定计算引擎或存储格式
2. **基于 Field ID 的列标识**：使用全局唯一的整数 ID 而非列名来标识字段，这是支持安全 Schema 演进的基础
3. **精确的可空性语义**：通过 optional/required 明确标注每个字段的可空性
4. **双向类型映射**：为每种存储格式和计算引擎都提供了双向的类型转换器
5. **受控的类型提升**：严格限制允许的类型变更，保证数据安全

本文将从源码层面逐一解析这些设计的实现细节。

---

## 2. 核心类型体系架构

### 2.1 Type 接口：类型系统的根基

Iceberg 的整个类型系统构建在 `Type` 接口之上，定义于 `api/src/main/java/org/apache/iceberg/types/Type.java`。这个接口简洁而精确：

```java
// Type.java 第31-114行
public interface Type extends Serializable {
  TypeID typeId();

  default boolean isPrimitiveType() { return false; }
  default PrimitiveType asPrimitiveType() {
    throw new IllegalArgumentException("Not a primitive type: " + this);
  }

  default Types.StructType asStructType() {
    throw new IllegalArgumentException("Not a struct type: " + this);
  }
  default Types.ListType asListType() {
    throw new IllegalArgumentException("Not a list type: " + this);
  }
  default Types.MapType asMapType() {
    throw new IllegalArgumentException("Not a map type: " + this);
  }
  default Types.VariantType asVariantType() {
    throw new IllegalArgumentException("Not a variant type: " + this);
  }

  default boolean isNestedType() { return false; }
  default boolean isStructType() { return false; }
  default boolean isListType() { return false; }
  default boolean isMapType() { return false; }
  default boolean isVariantType() { return false; }

  default NestedType asNestedType() {
    throw new IllegalArgumentException("Not a nested type: " + this);
  }
}
```

**设计要点分析：**

- `Type` 继承 `Serializable`，确保类型可以在分布式环境中序列化传输
- 使用 `default` 方法提供类型转换的安全保护：不正确的类型转换会抛出 `IllegalArgumentException`
- 通过 `isPrimitiveType()` / `isNestedType()` 等方法提供快速类型判断，避免 `instanceof` 检查
- 类型层次结构分为两大类：`PrimitiveType`（原始类型）和 `NestedType`（嵌套类型），但 `VariantType` 是一个例外，它既不是原始类型也不是嵌套类型

### 2.2 TypeID 枚举：类型标识与 Java 类型映射

`TypeID` 枚举定义于 `Type` 接口内部（第32-63行），是 Iceberg 类型系统的身份标识中枢：

```java
enum TypeID {
  BOOLEAN(Boolean.class),
  INTEGER(Integer.class),
  LONG(Long.class),
  FLOAT(Float.class),
  DOUBLE(Double.class),
  DATE(Integer.class),       // 自 epoch 以来的天数
  TIME(Long.class),          // 自午夜以来的微秒数
  TIMESTAMP(Long.class),     // 自 epoch 以来的微秒数
  TIMESTAMP_NANO(Long.class),// 自 epoch 以来的纳秒数
  STRING(CharSequence.class),
  UUID(java.util.UUID.class),
  FIXED(ByteBuffer.class),
  BINARY(ByteBuffer.class),
  DECIMAL(BigDecimal.class),
  GEOMETRY(ByteBuffer.class),
  GEOGRAPHY(ByteBuffer.class),
  STRUCT(StructLike.class),
  LIST(List.class),
  MAP(Map.class),
  VARIANT(Variant.class),
  UNKNOWN(Object.class);
}
```

**关键设计决策：**

1. **DATE 使用 `Integer.class`**：日期存储为自 Unix epoch (1970-01-01) 以来的天数，使用 32 位整数足够表示
2. **TIME 使用 `Long.class`**：时间存储为自午夜以来的微秒数，64 位长整型可以精确表示一天内的任何时刻
3. **TIMESTAMP 和 TIMESTAMP_NANO 都使用 `Long.class`**：前者精度为微秒，后者为纳秒，都以 epoch 为基准
4. **STRING 使用 `CharSequence.class` 而非 `String.class`**：这允许使用 `Utf8` 等高效的字符串实现
5. **FIXED 和 BINARY 都使用 `ByteBuffer.class`**：区别在于 FIXED 有固定长度，BINARY 是可变长度
6. **GEOMETRY 和 GEOGRAPHY**：v3 格式新增的空间类型，底层存储为 `ByteBuffer`
7. **VARIANT**：v3 格式新增的半结构化数据类型，使用专门的 `Variant` 类

### 2.3 PrimitiveType 抽象基类

`PrimitiveType` 定义于 `Type` 接口内部（第116-146行）：

```java
abstract class PrimitiveType implements Type {
  @Override
  public boolean isPrimitiveType() { return true; }

  @Override
  public PrimitiveType asPrimitiveType() { return this; }

  Object writeReplace() throws ObjectStreamException {
    return new PrimitiveLikeHolder(toString());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (!(o instanceof PrimitiveType)) {
      return false;
    }

    PrimitiveType that = (PrimitiveType) o;
    return typeId() == that.typeId();
  }

  @Override
  public int hashCode() {
    return Objects.hash(PrimitiveType.class, typeId());
  }
}
```

**关键设计点：**

- `writeReplace()` 方法使用 `PrimitiveLikeHolder` 进行序列化替换，确保单例模式在反序列化后仍然有效
- 默认的 `equals` 实现仅比较 `typeId()`，对于无参数的原始类型（如 `BooleanType`、`IntegerType`）这是正确的
- 带参数的类型（如 `DecimalType`、`FixedType`）重写了 `equals` 和 `hashCode` 以包含参数比较

### 2.4 NestedType 抽象基类

`NestedType` 是所有复合类型的基类（第149-164行）：

```java
abstract class NestedType implements Type {
  @Override
  public boolean isNestedType() { return true; }

  @Override
  public NestedType asNestedType() { return this; }

  public abstract List<Types.NestedField> fields();
  public abstract Type fieldType(String name);
  public abstract Types.NestedField field(int id);
}
```

`NestedType` 定义了三个核心抽象方法：

1. `fields()`：返回所有子字段列表
2. `fieldType(String name)`：按名称查找子字段类型
3. `field(int id)`：按 ID 查找子字段

这一统一接口使得 `StructType`、`ListType`、`MapType` 可以在遍历算法中被统一处理。

---

## 3. 原始类型详解

### 3.1 无参数单例类型

Iceberg 中大部分原始类型采用单例模式实现，不携带额外参数。它们的实现模式完全一致，以 `BooleanType` 为代表（`Types.java` 第118-134行）：

```java
public static class BooleanType extends PrimitiveType {
  private static final BooleanType INSTANCE = new BooleanType();
  public static BooleanType get() { return INSTANCE; }

  @Override
  public TypeID typeId() { return TypeID.BOOLEAN; }

  @Override
  public String toString() { return "boolean"; }
}
```

以下类型均采用这种单例模式：

| 类型类名 | TypeID | toString() | Java 映射类型 | 语义说明 |
|---------|--------|------------|-------------|---------|
| `BooleanType` | BOOLEAN | "boolean" | `Boolean` | 布尔值 |
| `IntegerType` | INTEGER | "int" | `Integer` | 32位有符号整数 |
| `LongType` | LONG | "long" | `Long` | 64位有符号整数 |
| `FloatType` | FLOAT | "float" | `Float` | 32位IEEE 754浮点 |
| `DoubleType` | DOUBLE | "double" | `Double` | 64位IEEE 754浮点 |
| `DateType` | DATE | "date" | `Integer` | 自epoch天数 |
| `TimeType` | TIME | "time" | `Long` | 自午夜微秒数 |
| `StringType` | STRING | "string" | `CharSequence` | UTF-8字符串 |
| `UUIDType` | UUID | "uuid" | `java.util.UUID` | 128位UUID |
| `BinaryType` | BINARY | "binary" | `ByteBuffer` | 变长二进制 |
| `UnknownType` | UNKNOWN | "unknown" | `Object` | 未知类型（v3+） |

注意 `TimeType` 有一个私有构造方法（第233行），但其他单例类型使用默认构造方法。这并不影响功能，因为外部代码始终通过 `get()` 静态方法获取实例。

### 3.2 带参数类型

**FixedType** (`Types.java` 第389-430行)

`FixedType` 表示固定长度的字节数组，其关键参数是 `length`：

```java
public static class FixedType extends PrimitiveType {
  public static FixedType ofLength(int length) {
    return new FixedType(length);
  }

  private final int length;

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "fixed[%d]", length);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FixedType)) return false;
    FixedType fixedType = (FixedType) o;
    return length == fixedType.length;
  }
}
```

`FixedType` 重写了 `equals` 和 `hashCode`，因为两个不同长度的 `FixedType` 是不同类型。它的字符串表示形式为 `fixed[N]`，如 `fixed[16]`。

**DecimalType** (`Types.java` 第517-571行)

`DecimalType` 包含两个参数：`precision`（精度）和 `scale`（小数位数）：

```java
public static class DecimalType extends PrimitiveType {
  public static DecimalType of(int precision, int scale) {
    return new DecimalType(precision, scale);
  }

  private DecimalType(int precision, int scale) {
    Preconditions.checkArgument(
        precision <= 38,
        "Decimals with precision larger than 38 are not supported: %s", precision);
    this.scale = scale;
    this.precision = precision;
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "decimal(%d, %d)", precision, scale);
  }
}
```

**精度限制**：Iceberg 规定 Decimal 精度最大为 38（与 SQL 标准和大多数数据库一致）。超出此限制会在构造时直接抛出异常。字符串表示为 `decimal(P, S)` 形式，如 `decimal(10, 2)`。

### 3.3 时间戳类型家族

Iceberg 定义了两组时间戳类型，分别对应微秒和纳秒精度。

**TimestampType** (`Types.java` 第246-298行)

```java
public static class TimestampType extends PrimitiveType {
  private static final TimestampType INSTANCE_WITH_ZONE = new TimestampType(true);
  private static final TimestampType INSTANCE_WITHOUT_ZONE = new TimestampType(false);

  public static TimestampType withZone() { return INSTANCE_WITH_ZONE; }
  public static TimestampType withoutZone() { return INSTANCE_WITHOUT_ZONE; }

  private final boolean adjustToUTC;

  public boolean shouldAdjustToUTC() { return adjustToUTC; }

  @Override
  public String toString() {
    return shouldAdjustToUTC() ? "timestamptz" : "timestamp";
  }
}
```

**TimestampNanoType** (`Types.java` 第300-351行)

结构与 `TimestampType` 完全对称：

```java
public static class TimestampNanoType extends PrimitiveType {
  private static final TimestampNanoType INSTANCE_WITH_ZONE = new TimestampNanoType(true);
  private static final TimestampNanoType INSTANCE_WITHOUT_ZONE = new TimestampNanoType(false);

  public static TimestampNanoType withZone() { return INSTANCE_WITH_ZONE; }
  public static TimestampNanoType withoutZone() { return INSTANCE_WITHOUT_ZONE; }

  @Override
  public String toString() {
    return shouldAdjustToUTC() ? "timestamptz_ns" : "timestamp_ns";
  }
}
```

时间戳类型的设计体现了 Iceberg 对时区语义的精确处理：

- **`timestamptz`** / **`timestamptz_ns`**：时间戳将被调整到 UTC（`adjustToUTC = true`），适用于需要跨时区一致性的场景
- **`timestamp`** / **`timestamp_ns`**：本地时间戳（`adjustToUTC = false`），不包含时区信息

两种精度的时间戳类型分别是四个静态单例实例，但 `equals` 方法除了检查类型匹配外还要检查 `adjustToUTC` 标志，因此 `timestamptz` 和 `timestamp` 被视为不同类型。

### 3.4 特殊类型

**GeometryType** (`Types.java` 第573-629行)

空间几何类型，v3 格式新增。携带坐标参考系 (CRS) 参数：

```java
public static class GeometryType extends PrimitiveType {
  public static final String DEFAULT_CRS = "OGC:CRS84";

  public static GeometryType crs84() { return new GeometryType(); }
  public static GeometryType of(String crs) { return new GeometryType(crs); }
}
```

**GeographyType** (`Types.java` 第631-701行)

地理类型，除了 CRS 之外还携带边缘算法参数。

**VariantType** (`Types.java` 第450-497行)

半结构化变体类型，v3 格式新增。`VariantType` 的设计值得特别关注——它没有继承 `PrimitiveType` 或 `NestedType`，而是直接实现了 `Type` 接口：

```java
public static class VariantType implements Type {
  private static final VariantType INSTANCE = new VariantType();
  public static VariantType get() { return INSTANCE; }

  @Override
  public TypeID typeId() { return TypeID.VARIANT; }

  @Override
  public boolean isVariantType() { return true; }

  @Override
  public VariantType asVariantType() { return this; }
}
```

这是因为 Variant 在物理存储上是一种结构化类型（包含 metadata 和 value 两个部分），但在逻辑上是一个原子类型。这种特殊的继承结构使得 `SchemaVisitor` 中需要单独处理 `VARIANT` 分支（`TypeUtil.java` 第789行）。

---

## 4. 复合类型与嵌套结构

### 4.1 NestedField：字段的核心抽象

`NestedField` 是 Iceberg Schema 中每个字段的表示（`Types.java` 第703-999行），它封装了字段的所有元数据：

```java
public static class NestedField implements Serializable {
  private final boolean isOptional;  // 可空性
  private final int id;              // 字段唯一ID
  private final String name;         // 字段名
  private final Type type;           // 字段类型
  private final String doc;          // 文档注释
  private final Literal<?> initialDefault;  // 初始默认值
  private final Literal<?> writeDefault;    // 写入默认值
}
```

**工厂方法：**

```java
public static NestedField optional(int id, String name, Type type) { ... }
public static NestedField optional(int id, String name, Type type, String doc) { ... }
public static NestedField required(int id, String name, Type type) { ... }
public static NestedField required(int id, String name, Type type, String doc) { ... }
```

工厂方法强制在创建时明确可空性，避免了遗漏。注意构造函数中有一个重要的校验（第873-875行）：

```java
Preconditions.checkArgument(
    isOptional || !type.equals(UnknownType.get()),
    "Cannot create required field with unknown type: %s", name);
```

这意味着 `UnknownType` 只能用于 optional 字段，因为 unknown 类型的值无法被确定地提供。

**Builder 模式（第756-852行）：**

从当前代码可以看到，`NestedField` 提供了一个完整的 Builder 模式，支持链式构建：

```java
Types.NestedField.builder()
    .withName("col")
    .withId(1)
    .asOptional()
    .ofType(Types.StringType.get())
    .withDoc("description")
    .withInitialDefault(Expressions.lit("default_value"))
    .withWriteDefault(Expressions.lit("write_default"))
    .build();
```

`from()` 静态方法允许从已有字段创建 Builder，方便进行部分修改。

**默认值处理（第885-897行）：**

```java
private static Literal<?> castDefault(Literal<?> defaultValue, Type type) {
  if (type.isNestedType() && defaultValue != null) {
    throw new IllegalArgumentException(...);
  } else if (defaultValue != null) {
    Literal<?> typedDefault = defaultValue.to(type);
    Preconditions.checkArgument(typedDefault != null, ...);
    return typedDefault;
  }
  return null;
}
```

嵌套类型不允许设置非空默认值，原始类型的默认值会通过 `Literal.to(type)` 进行类型转换和校验。

### 4.2 StructType：结构体类型

`StructType` (`Types.java` 第1001-1146行) 是最常用的复合类型，对应 SQL 中的 ROW 或 STRUCT：

```java
public static class StructType extends NestedType {
  private final NestedField[] fields;

  // 延迟初始化的索引
  private transient Schema schema = null;
  private transient List<NestedField> fieldList = null;
  private transient Map<String, NestedField> fieldsByName = null;
  private transient Map<String, NestedField> fieldsByLowerCaseName = null;
  private transient Map<Integer, NestedField> fieldsById = null;
}
```

**性能优化设计：**

1. 字段存储为数组而非列表，减少内存开销
2. 名称索引、大小写不敏感名称索引、ID 索引均采用延迟初始化（`transient` + 按需构建）
3. `asSchema()` 方法缓存创建的 `Schema` 对象，避免 manifest 评估时的重复转换

**查找方法：**

- `field(String name)`：精确名称匹配
- `field(int id)`：按 ID 匹配
- `caseInsensitiveField(String name)`：大小写不敏感匹配

### 4.3 ListType：列表类型

`ListType` (`Types.java` 第1148-1246行) 表示有序元素集合：

```java
public static class ListType extends NestedType {
  public static ListType ofOptional(int elementId, Type elementType) {
    return new ListType(NestedField.optional(elementId, "element", elementType));
  }
  public static ListType ofRequired(int elementId, Type elementType) {
    return new ListType(NestedField.required(elementId, "element", elementType));
  }

  private final NestedField elementField;
}
```

**关键设计：**

- 列表的元素本身就是一个 `NestedField`，名称固定为 `"element"`
- 元素拥有独立的 `fieldId`，这是 Iceberg 类型系统的核心特征之一
- 元素有自己的可空性语义：`ofOptional` 表示元素可以为 null，`ofRequired` 表示不可以

为什么列表元素需要 Field ID？因为 Iceberg 通过 Field ID 追踪所有字段的演化历史。如果列表的元素类型发生变更（例如从 `int` 提升到 `long`），系统需要通过 Field ID 来关联新旧类型定义。

### 4.4 MapType：映射类型

`MapType` (`Types.java` 第1248-1367行) 表示键值对集合：

```java
public static class MapType extends NestedType {
  public static MapType ofOptional(int keyId, int valueId, Type keyType, Type valueType) {
    return new MapType(
        NestedField.required(keyId, "key", keyType),
        NestedField.optional(valueId, "value", valueType));
  }
  public static MapType ofRequired(int keyId, int valueId, Type keyType, Type valueType) {
    return new MapType(
        NestedField.required(keyId, "key", keyType),
        NestedField.required(valueId, "value", valueType));
  }

  private final NestedField keyField;
  private final NestedField valueField;
}
```

**关键约束：**

1. **键始终是 required**：Map 的 key 不允许为 null，这与 Java Map 的语义一致（第1252行的 `NestedField.required`）
2. **值的可空性由工厂方法控制**：`ofOptional` 的值可以为 null，`ofRequired` 的值不可以
3. **键和值各有独立的 Field ID**：`keyId` 和 `valueId` 分别标识键和值字段
4. **键名固定为 "key"，值名固定为 "value"**

### 4.5 VariantType：半结构化变体类型

`VariantType`（`Types.java` 第450-497行）是 Iceberg v3 格式引入的新类型，用于存储半结构化数据（类似 JSON）。它的独特之处在于：

- 它不是 `PrimitiveType` 也不是 `NestedType`
- 它在物理存储层面由 `metadata`（元数据）和 `value`（值数据）两部分组成
- 支持 Parquet 中的 "shredding" 优化（将部分字段物化为 typed_value 列）

---

## 5. Field ID 管理与分配策略

### 5.1 为什么 Field ID 而非名称是核心标识

这是 Iceberg 类型系统最核心的设计决策之一。在传统的 Hive 表中，列通过名称或位置来标识，这导致了以下问题：

1. **重命名操作是破坏性的**：重命名列会导致所有依赖该列名的查询失败
2. **列删除后无法安全重用名称**：如果删除列 A 后又添加同名列 A，旧数据中的列 A 和新列 A 会被混淆
3. **列重排序可能破坏数据**：基于位置的映射在列重排后会映射到错误的数据

Iceberg 通过 Field ID 解决了这些问题：

- **每个字段分配一个全局唯一的整数 ID**，该 ID 在字段的整个生命周期内不变
- **重命名不改变 ID**：字段被重命名后，通过 ID 仍然能正确关联数据
- **删除后 ID 永不重用**：已分配的 ID 不会被回收，新字段获得新 ID
- **ID 存储在数据文件中**：Parquet 通过 `field_id` metadata、ORC 通过 `iceberg.id` attribute、Avro 通过 `field-id` property

### 5.2 Schema 中的 ID 管理

`Schema` 类（`api/src/main/java/org/apache/iceberg/Schema.java`）维护了 ID 管理的基础设施：

```java
public class Schema implements Serializable {
  private final StructType struct;
  private final int schemaId;
  private final int[] identifierFieldIds;
  private final int highestFieldId;

  // 延迟初始化的索引
  private transient Map<Integer, NestedField> idToField = null;
  private transient Map<String, Integer> nameToId = null;
  private transient Map<Integer, String> idToName = null;
}
```

`highestFieldId` 记录了当前 Schema 中使用的最大 Field ID，新字段的 ID 必须大于此值。这个值在构造函数中通过遍历所有字段 ID 来确定（第160行）：

```java
this.highestFieldId = lazyIdToName().keySet().stream().mapToInt(i -> i).max().orElse(0);
```

### 5.3 AssignFreshIds：ID 分配核心实现

`AssignFreshIds` (`api/src/main/java/org/apache/iceberg/types/AssignFreshIds.java`) 是 ID 分配的核心实现类。它继承 `CustomOrderSchemaVisitor<Type>`，使用前序遍历为所有字段分配新 ID：

```java
class AssignFreshIds extends TypeUtil.CustomOrderSchemaVisitor<Type> {
  private final Schema visitingSchema;
  private final Schema baseSchema;
  private final TypeUtil.NextID nextId;

  // 关键方法：确定字段ID
  private int idFor(String fullName) {
    if (baseSchema != null && fullName != null) {
      Types.NestedField field = baseSchema.findField(fullName);
      if (field != null) {
        return field.fieldId();  // 使用基础Schema中的已有ID
      }
    }
    return nextId.get();  // 分配新ID
  }
}
```

`AssignFreshIds` 有两种工作模式：

1. **纯新分配模式**（只有 `nextId`）：为所有字段分配全新的 ID
2. **基于基础 Schema 分配模式**（有 `visitingSchema` 和 `baseSchema`）：优先从基础 Schema 按名称查找已有 ID，找不到时才分配新 ID

在 `struct` 方法中，ID 分配采用前序遍历——先为当前层级的所有字段分配 ID，再递归处理子类型（第76-95行）：

```java
@Override
public Type struct(Types.StructType struct, Iterable<Type> futures) {
  List<Types.NestedField> fields = struct.fields();
  int length = struct.fields().size();

  // 先分配当前层级的所有ID
  List<Integer> newIds = Lists.newArrayListWithExpectedSize(length);
  for (int i = 0; i < length; i += 1) {
    newIds.add(idFor(name(fields.get(i).fieldId())));
  }

  // 再递归处理子类型
  List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(length);
  Iterator<Type> types = futures.iterator();
  for (int i = 0; i < length; i += 1) {
    Types.NestedField field = fields.get(i);
    Type type = types.next();
    newFields.add(Types.NestedField.from(field).withId(newIds.get(i)).ofType(type).build());
  }

  return Types.StructType.of(newFields);
}
```

### 5.4 reassignIds 与 reassignOrRefreshIds

`TypeUtil` 提供了两种 ID 重分配策略：

**reassignIds**（`TypeUtil.java` 第363-406行）：

```java
public static Schema reassignIds(Schema schema, Schema idSourceSchema, boolean caseSensitive) {
  Types.StructType struct =
      visit(schema, new ReassignIds(idSourceSchema, null, caseSensitive)).asStructType();
  return new Schema(struct.fields(), refreshIdentifierFields(struct, schema));
}
```

此方法从源 Schema 按名称查找 ID，如果找不到则**抛出异常**。适用于已知两个 Schema 结构完全匹配的场景。

**reassignOrRefreshIds**（`TypeUtil.java` 第408-419行）：

```java
public static Schema reassignOrRefreshIds(Schema schema, Schema idSourceSchema, boolean caseSensitive) {
  AtomicInteger highest = new AtomicInteger(idSourceSchema.highestFieldId());
  Types.StructType struct =
      visit(schema, new ReassignIds(idSourceSchema, highest::incrementAndGet, caseSensitive))
          .asStructType();
  return new Schema(struct.fields(), refreshIdentifierFields(struct, schema));
}
```

此方法在源 Schema 中找不到匹配时，从最大 ID 递增分配新 ID。适用于 Schema 演进场景——新增的字段获得新 ID，已有字段保持原 ID。

### 5.5 ID 冲突解决机制

`TypeUtil.reassignConflictingIds` (`TypeUtil.java` 第629-662行) 处理 Schema 合并时的 ID 冲突：

```java
public static GetID reassignConflictingIds(Set<Integer> conflictingIds, Set<Integer> allUsedIds) {
  return new ReassignConflictingIds(conflictingIds, allUsedIds);
}

private static class ReassignConflictingIds implements GetID {
  @Override
  public int get(int oldId) {
    if (conflictingIds.contains(oldId)) {
      return nextAvailableId();  // 冲突ID重新分配
    } else {
      return oldId;  // 非冲突ID保持不变
    }
  }
}
```

---

## 6. TypeUtil 工具类深度分析

`TypeUtil` (`api/src/main/java/org/apache/iceberg/types/TypeUtil.java`) 是 Iceberg 类型系统中最重要的工具类，提供了 Schema 操作的全套方法。

### 6.1 Schema 投影与裁剪

**project** (`TypeUtil.java` 第59-87行)

`project` 方法从 Schema 中精确提取指定 ID 的字段。它只选择被明确枚举的字段——如果选择了一个 struct 字段的 ID 但未选择其子字段，结果将是一个空的 struct：

```java
public static Schema project(Schema schema, Set<Integer> fieldIds) {
  Types.StructType result = project(schema.asStruct(), fieldIds);
  if (schema.asStruct().equals(result)) {
    return schema;
  } else if (result != null) {
    return schema.getAliases() != null
        ? new Schema(result.fields(), schema.getAliases())
        : new Schema(result.fields());
  }
  return new Schema(Collections.emptyList(), schema.getAliases());
}
```

**select** (`TypeUtil.java` 第89-131行)

`select` 方法与 `project` 类似但行为不同——当选择一个 struct 字段时，会自动包含其所有子字段：

```java
public static Types.StructType select(Types.StructType struct, Set<Integer> fieldIds) {
  Type result = visit(struct, new PruneColumns(fieldIds, true));  // true = 选择子树
  ...
}
```

两者的区别在于传递给 `PruneColumns` 的第二个参数：`project` 传递 `false`（不自动选择子树），`select` 传递 `true`（自动选择子树）。

**selectNot** (`TypeUtil.java` 第148-158行)

`selectNot` 执行反向选择——从 Schema 中移除指定 ID 的字段：

```java
public static Types.StructType selectNot(Types.StructType struct, Set<Integer> fieldIds) {
  Set<Integer> projectedIds = getIdsInternal(struct, false);
  projectedIds.removeAll(fieldIds);
  return project(struct, projectedIds);
}
```

### 6.2 Schema 合并 (join)

`join` 方法将两个 Schema 合并为一个（`TypeUtil.java` 第160-177行）：

```java
public static Schema join(Schema left, Schema right) {
  List<Types.NestedField> joinedColumns = Lists.newArrayList(left.columns());
  for (Types.NestedField rightColumn : right.columns()) {
    Types.NestedField leftColumn = left.findField(rightColumn.fieldId());
    if (leftColumn == null) {
      joinedColumns.add(rightColumn);
    } else {
      Preconditions.checkArgument(leftColumn.equals(rightColumn),
          "Schemas have different columns with same id: %s, %s", leftColumn, rightColumn);
    }
  }
  return new Schema(joinedColumns);
}
```

合并规则：
1. 保留 left Schema 的所有字段
2. 添加 right Schema 中不在 left 中的字段（按 Field ID 判断）
3. 如果两个 Schema 中存在相同 ID 但不同定义的字段，抛出异常

### 6.3 索引构建方法族

`TypeUtil` 提供了一系列索引构建方法，用于在 Schema 上快速查找：

```java
// 名称 -> ID 映射
public static Map<String, Integer> indexByName(Types.StructType struct) { ... }

// ID -> 名称 映射
public static Map<Integer, String> indexNameById(Types.StructType struct) { ... }

// 小写名称 -> ID 映射（用于大小写不敏感查询）
public static Map<String, Integer> indexByLowerCaseName(Types.StructType struct) { ... }

// ID -> NestedField 映射
public static Map<Integer, Types.NestedField> indexById(Types.StructType struct) { ... }

// ID -> 父ID 映射
public static Map<Integer, Integer> indexParents(Types.StructType struct) { ... }
```

`indexByLowerCaseName` 方法（第209-229行）包含一个重要的冲突检测：如果两个字段名在转为小写后相同，会抛出异常。这确保了大小写不敏感模式下不会产生歧义。

### 6.4 SchemaVisitor 访问者模式

`TypeUtil` 定义了两种访问者：

**SchemaVisitor**（后序遍历，`TypeUtil.java` 第664-728行）：

子节点先于父节点被访问。这适合需要先处理子类型再组合为父类型的场景（如类型转换）。

**CustomOrderSchemaVisitor**（自定义遍历顺序，第796-824行）：

通过 `Supplier` 延迟计算子节点，允许访问者控制遍历顺序。这适合需要前序遍历的场景（如 ID 分配）。

`visit` 方法的核心逻辑（第734-794行）处理了所有类型分支：

```java
public static <T> T visit(Type type, SchemaVisitor<T> visitor) {
  switch (type.typeId()) {
    case STRUCT:
      // 遍历所有字段，调用 beforeField/afterField 钩子
      ...
    case LIST:
      // 遍历元素类型
      ...
    case MAP:
      // 分别遍历 key 和 value 类型
      ...
    case VARIANT:
      return visitor.variant(type.asVariantType());
    default:
      return visitor.primitive(type.asPrimitiveType());
  }
}
```

### 6.5 内存大小估算

`estimateSize` 方法（`TypeUtil.java` 第550-606行）估算字段值在 JVM 内存中的占用：

```java
private static int estimateSize(Type type) {
  switch (type.typeId()) {
    case BOOLEAN: return 1;
    case INTEGER: case FLOAT: case DATE: return 4;
    case LONG: case DOUBLE: case TIME: case TIMESTAMP: case TIMESTAMP_NANO: return 8;
    case STRING: return 54;  // header + fields + array + 10 chars
    case UUID: return 28;    // header + two longs
    case DECIMAL: return 44; // header + BigInteger + scale
    case STRUCT:
      return HEADER_SIZE + struct.fields().stream().mapToInt(TypeUtil::estimateSize).sum();
    case LIST:
      return HEADER_SIZE + 5 * estimateSize(list.elementType());
    case MAP:
      int entrySize = HEADER_SIZE + estimateSize(map.keyType()) + estimateSize(map.valueType());
      return HEADER_SIZE + 5 * entrySize;
  }
}
```

这里假设 List 平均有 5 个元素，Map 平均有 5 个条目。此估算用于查询计划中的内存预算。

---

## 7. 类型提升规则

### 7.1 isPromotionAllowed 的精确规则

类型提升规则定义于 `TypeUtil.isPromotionAllowed()`（`TypeUtil.java` 第440-466行）：

```java
public static boolean isPromotionAllowed(Type from, Type.PrimitiveType to) {
  if (from.equals(to)) {
    return true;
  }

  switch (from.typeId()) {
    case INTEGER:
      return to.typeId() == Type.TypeID.LONG;
    case FLOAT:
      return to.typeId() == Type.TypeID.DOUBLE;
    case DECIMAL:
      Types.DecimalType fromDecimal = (Types.DecimalType) from;
      if (to.typeId() != Type.TypeID.DECIMAL) {
        return false;
      }
      Types.DecimalType toDecimal = (Types.DecimalType) to;
      return fromDecimal.scale() == toDecimal.scale()
          && fromDecimal.precision() <= toDecimal.precision();
  }

  return false;
}
```

允许的类型提升路径：

| 源类型 | 目标类型 | 条件 |
|--------|---------|------|
| `int` | `long` | 无条件 |
| `float` | `double` | 无条件 |
| `decimal(P1, S)` | `decimal(P2, S)` | P2 >= P1，scale 必须相同 |

**不允许的类型变更（举例）：**

- `int` -> `float`（精度损失）
- `long` -> `double`（精度损失）
- `string` -> `int`（语义不兼容）
- `decimal(10, 2)` -> `decimal(10, 3)`（scale 不同）
- `timestamp` -> `timestamptz`（时区语义不同）
- `timestamp` -> `timestamp_ns`（精度不同）

### 7.2 类型提升在 Schema 演进中的应用

在 `SchemaUpdate.updateColumn()` (`SchemaUpdate.java` 第273-298行) 中，类型提升检查被严格执行：

```java
@Override
public UpdateSchema updateColumn(String name, Type.PrimitiveType newType) {
  Types.NestedField field = findForUpdate(name);
  ...
  if (field.type().equals(newType)) {
    return this;
  }

  Preconditions.checkArgument(
      TypeUtil.isPromotionAllowed(field.type(), newType),
      "Cannot change column type: %s: %s -> %s",
      name, field.type(), newType);

  int fieldId = field.fieldId();
  Types.NestedField newField = Types.NestedField.from(field).ofType(newType).build();
  updates.put(fieldId, newField);
  return this;
}
```

如果类型变更不在允许列表中，`Preconditions.checkArgument` 会直接抛出 `IllegalArgumentException`。

### 7.3 类型提升的限制与设计考量

Iceberg 的类型提升规则非常保守，这是有意为之的：

1. **分区兼容性**：类型提升可能影响分区函数的计算结果。例如，如果某列被用作分区键且进行了 `int -> long` 提升，分区函数的输出不会改变（整数哈希值相同），但某些提升可能导致不兼容
2. **存储格式兼容性**：Parquet 中 `int` 存储为 INT32，`long` 存储为 INT64。类型提升要求读取端能够正确处理旧文件中的原始类型
3. **语义安全性**：不允许可能丢失信息的转换（如 `long -> int`、`double -> float`）

源码中的注释明确警告（第442-443行）：
```java
// Warning! Before changing this function, make sure that the type change doesn't introduce
// compatibility problems in partitioning.
```

---

## 8. Schema 演进中的类型兼容性

### 8.1 SchemaUpdate 的类型变更校验

`SchemaUpdate` 类 (`core/src/main/java/org/apache/iceberg/SchemaUpdate.java`) 是 Schema 演进的 API 实现。除了类型提升外，它还处理以下类型相关的操作：

**可空性变更** (`SchemaUpdate.java` 第241-270行)：

```java
private void internalUpdateColumnRequirement(String name, boolean isOptional) {
  Types.NestedField field = findForUpdate(name);
  ...
  boolean isDefaultedAdd = isAdded(name) && field.initialDefault() != null;

  Preconditions.checkArgument(
      isOptional || isDefaultedAdd || allowIncompatibleChanges,
      "Cannot change column nullability: %s: optional -> required", name);
}
```

将 optional 变为 required 是不兼容变更——已有数据中可能包含 null 值。只有在以下情况下才允许：
1. 显式调用了 `allowIncompatibleChanges()`
2. 字段是新添加的且有初始默认值

**新增字段的 ID 分配** (`SchemaUpdate.java` 第113-187行)：

```java
// assign new IDs in order
int newId = assignNewColumnId();

// 为复合类型的内部字段也分配新ID
Types.NestedField newField = Types.NestedField.builder()
    .withName(name)
    .isOptional(isOptional)
    .withId(newId)
    .ofType(TypeUtil.assignFreshIds(type, this::assignNewColumnId))
    .build();
```

注意 `TypeUtil.assignFreshIds(type, this::assignNewColumnId)` 的调用确保了复合类型（如嵌套 struct）的所有内部字段也获得新的唯一 ID。

### 8.2 CheckCompatibility：兼容性检查引擎

`CheckCompatibility` (`api/src/main/java/org/apache/iceberg/types/CheckCompatibility.java`) 提供了全面的 Schema 兼容性检查：

```java
public class CheckCompatibility extends TypeUtil.CustomOrderSchemaVisitor<List<String>> {
  // 写入兼容性（含可空性检查）
  public static List<String> writeCompatibilityErrors(
      Schema readSchema, Schema writeSchema, boolean checkOrdering) { ... }

  // 类型兼容性（不含可空性检查）
  public static List<String> typeCompatibilityErrors(
      Schema readSchema, Schema writeSchema, boolean checkOrdering) { ... }

  // 读取兼容性
  public static List<String> readCompatibilityErrors(
      Schema readSchema, Schema writeSchema) { ... }
}
```

**原始类型检查** (`CheckCompatibility.java` 第264-283行)：

```java
@Override
public List<String> primitive(Type.PrimitiveType readPrimitive) {
  if (currentType.equals(readPrimitive)) {
    return NO_ERRORS;
  }

  if (!currentType.isPrimitiveType()) {
    return ImmutableList.of(
        String.format(": %s cannot be read as a %s", currentType.typeId()..., readPrimitive));
  }

  if (!TypeUtil.isPromotionAllowed(currentType.asPrimitiveType(), readPrimitive)) {
    return ImmutableList.of(
        String.format(": %s cannot be promoted to %s", currentType, readPrimitive));
  }

  return NO_ERRORS;
}
```

检查逻辑：先检查类型是否完全相同，若不同则检查是否可以提升。不可提升的类型变更将产生错误消息。

**结构体字段排序检查** (`CheckCompatibility.java` 第141-164行)：

当 `checkOrdering` 为 `true` 时，还会检查字段的顺序是否一致。字段顺序不一致会产生如 "column_a is out of order, before column_b" 的错误信息。

### 8.3 写入兼容性 vs 读取兼容性

这两种兼容性检查的差异在于：

- **写入兼容性**：检查写入 Schema 是否能安全地写入到表 Schema 中。要求更严格——optional 字段不能写入 required 位置
- **读取兼容性**：检查读取 Schema 是否能安全地从已有数据中读取。允许 required 数据读为 optional（安全的），但 optional 数据读为 required 会产生错误
- **类型兼容性**：只检查类型匹配，忽略可空性。用于更宽松的场景

---

## 9. Iceberg 与 Parquet 类型映射

### 9.1 Iceberg 到 Parquet 映射 (TypeToMessageType)

`TypeToMessageType` (`parquet/src/main/java/org/apache/iceberg/parquet/TypeToMessageType.java`) 将 Iceberg 类型转换为 Parquet 的 `MessageType`。

**原始类型映射** (`TypeToMessageType.java` 第207-286行)：

```java
public Type primitive(PrimitiveType primitive, Type.Repetition repetition, int id, String name) {
  switch (primitive.typeId()) {
    case BOOLEAN:   return Types.primitive(BOOLEAN, repetition).id(id).named(name);
    case INTEGER:   return Types.primitive(INT32, repetition).id(id).named(name);
    case LONG:      return Types.primitive(INT64, repetition).id(id).named(name);
    case FLOAT:     return Types.primitive(FLOAT, repetition).id(id).named(name);
    case DOUBLE:    return Types.primitive(DOUBLE, repetition).id(id).named(name);
    case DATE:      return Types.primitive(INT32, repetition).as(DATE).id(id).named(name);
    case TIME:      return Types.primitive(INT64, repetition).as(TIME_MICROS).id(id).named(name);
    case TIMESTAMP:
      if (((TimestampType) primitive).shouldAdjustToUTC()) {
        return Types.primitive(INT64, repetition).as(TIMESTAMPTZ_MICROS).id(id).named(name);
      } else {
        return Types.primitive(INT64, repetition).as(TIMESTAMP_MICROS).id(id).named(name);
      }
    case TIMESTAMP_NANO:
      if (((TimestampNanoType) primitive).shouldAdjustToUTC()) {
        return Types.primitive(INT64, repetition).as(TIMESTAMPTZ_NANOS).id(id).named(name);
      } else {
        return Types.primitive(INT64, repetition).as(TIMESTAMP_NANOS).id(id).named(name);
      }
    case STRING:    return Types.primitive(BINARY, repetition).as(STRING).id(id).named(name);
    case BINARY:    return Types.primitive(BINARY, repetition).id(id).named(name);
    case FIXED:     return Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition).length(...).id(id)...;
    case UUID:      return Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition).length(16).as(uuidType())...;
    case DECIMAL:   // 根据精度选择不同的底层类型
      if (precision <= 9)       -> INT32 + decimal annotation
      else if (precision <= 18) -> INT64 + decimal annotation
      else                      -> FIXED_LEN_BYTE_ARRAY + decimal annotation
  }
}
```

**Decimal 的存储优化**：

Parquet 对 Decimal 的物理存储会根据精度选择最紧凑的表示：
- 精度 <= 9：使用 INT32（4字节）
- 精度 <= 18：使用 INT64（8字节）
- 精度 > 18：使用 FIXED_LEN_BYTE_ARRAY（长度由 `TypeUtil.decimalRequiredBytes` 计算）

这是 `TypeToMessageType` 中两个重要常量的含义（第54-55行）：
```java
public static final int DECIMAL_INT32_MAX_DIGITS = 9;
public static final int DECIMAL_INT64_MAX_DIGITS = 18;
```

**可空性映射**：

Iceberg 的 `optional` / `required` 映射到 Parquet 的 `Repetition`：
- `optional` -> `Repetition.OPTIONAL`
- `required` -> `Repetition.REQUIRED`

**Field ID 传递**：每个 Parquet 字段通过 `.id(id)` 方法设置 Field ID，这个 ID 会被写入 Parquet 文件的 schema metadata 中。

**UnknownType 处理** (`TypeToMessageType.java` 第116-117行)：

```java
if (field.type().typeId() == TypeID.UNKNOWN) {
  return null;  // unknown 类型不写入数据文件
}
```

### 9.2 Parquet 到 Iceberg 映射 (MessageTypeToType)

`MessageTypeToType` (`parquet/src/main/java/org/apache/iceberg/parquet/MessageTypeToType.java`) 执行反向转换。

**原始类型映射** (`MessageTypeToType.java` 第149-180行)：

```java
@Override
public Type primitive(PrimitiveType primitive) {
  LogicalTypeAnnotation logicalType = primitive.getLogicalTypeAnnotation();
  if (logicalType != null) {
    Optional<Type> converted = logicalType.accept(ParquetLogicalTypeVisitor.get());
    if (converted.isPresent()) {
      return converted.get();
    }
  }

  switch (primitive.getPrimitiveTypeName()) {
    case BOOLEAN:                return Types.BooleanType.get();
    case INT32:                  return Types.IntegerType.get();
    case INT64:                  return Types.LongType.get();
    case FLOAT:                  return Types.FloatType.get();
    case DOUBLE:                 return Types.DoubleType.get();
    case FIXED_LEN_BYTE_ARRAY:  return Types.FixedType.ofLength(primitive.getTypeLength());
    case INT96:                  return Types.TimestampType.withZone();  // 兼容旧Parquet
    case BINARY:                 return Types.BinaryType.get();
  }
}
```

**INT96 的特殊处理**：Parquet 的 INT96 类型是旧版 Impala/Hive 用于存储时间戳的格式。Iceberg 将其映射为 `TimestampType.withZone()`。

**Logical Type 优先**：转换时先检查 Parquet 的 Logical Type Annotation。例如，一个带有 `StringLogicalTypeAnnotation` 的 `BINARY` 类型会被转换为 `StringType` 而非 `BinaryType`。

**ParquetLogicalTypeVisitor** (`MessageTypeToType.java` 第187-250行) 处理了所有 Parquet 逻辑类型：

```java
// String/Enum -> StringType
// Decimal -> DecimalType
// Date -> DateType
// Time -> TimeType
// Timestamp (microseconds) -> TimestampType (with/without zone)
// Int (bit_width < 32, signed or unsigned) -> IntegerType
// Int (bit_width == 32, signed) -> IntegerType
// Int (bit_width == 32, unsigned) -> LongType
// Int (bit_width > 32) -> LongType
// Json -> StringType
// Bson -> BinaryType
```

### 9.3 Parquet 映射的特殊处理

**名称兼容性**（`TypeToMessageType.java` 第93, 209行）：

Iceberg 字段名可能包含 Avro 不允许的字符。Parquet 使用 Avro 来处理 schema，因此需要通过 `AvroSchemaUtil.makeCompatibleName` 进行名称清理。这意味着写入 Parquet 时字段名可能与 Iceberg schema 中不同，但通过 Field ID 仍然能正确匹配。

**Variant Shredding 支持**（`TypeToMessageType.java` 第166-205行）：

Variant 类型可以选择性地将部分数据"shred"到独立的列中以优化读取性能：

```java
public Type variant(Type.Repetition repetition, int id, String originalName) {
  if (variantShreddingFunc != null) {
    shreddedType = variantShreddingFunc.apply(id, originalName);
  }

  if (shreddedType != null) {
    // 包含 metadata + value + typed_value 三个字段
    return Types.buildGroup(repetition)
        .as(LogicalTypeAnnotation.variantType(...))
        .required(BINARY).named(METADATA)
        .optional(BINARY).named(VALUE)
        .addField(shreddedType)
        .named(name);
  } else {
    // 仅包含 metadata + value 两个字段
    return Types.buildGroup(repetition)
        .as(LogicalTypeAnnotation.variantType(...))
        .required(BINARY).named(METADATA)
        .required(BINARY).named(VALUE)
        .named(name);
  }
}
```

---

## 10. Iceberg 与 ORC 类型映射

### 10.1 Iceberg 到 ORC 映射 (ORCSchemaUtil.convert)

`ORCSchemaUtil.convert()` (`orc/src/main/java/org/apache/iceberg/orc/ORCSchemaUtil.java` 第125-268行) 将 Iceberg Schema 转换为 ORC 的 `TypeDescription`：

```java
private static TypeDescription convert(Integer fieldId, Type type, boolean isRequired) {
  switch (type.typeId()) {
    case BOOLEAN:   orcType = TypeDescription.createBoolean(); break;
    case INTEGER:   orcType = TypeDescription.createInt(); break;
    case TIME:
      orcType = TypeDescription.createLong();
      orcType.setAttribute(ICEBERG_LONG_TYPE_ATTRIBUTE, LongType.TIME.toString());
      break;
    case LONG:
      orcType = TypeDescription.createLong();
      orcType.setAttribute(ICEBERG_LONG_TYPE_ATTRIBUTE, LongType.LONG.toString());
      break;
    case FLOAT:     orcType = TypeDescription.createFloat(); break;
    case DOUBLE:    orcType = TypeDescription.createDouble(); break;
    case DATE:      orcType = TypeDescription.createDate(); break;
    case TIMESTAMP:
      if (tsType.shouldAdjustToUTC()) {
        orcType = TypeDescription.createTimestampInstant();
      } else {
        orcType = TypeDescription.createTimestamp();
      }
      orcType.setAttribute(TIMESTAMP_UNIT, MICROS);
      break;
    case TIMESTAMP_NANO:
      // 同上，但设置 NANOS 属性
      orcType.setAttribute(TIMESTAMP_UNIT, NANOS);
      break;
    case STRING:    orcType = TypeDescription.createString(); break;
    case UUID:
      orcType = TypeDescription.createBinary();
      orcType.setAttribute(ICEBERG_BINARY_TYPE_ATTRIBUTE, BinaryType.UUID.toString());
      break;
    case FIXED:
      orcType = TypeDescription.createBinary();
      orcType.setAttribute(ICEBERG_BINARY_TYPE_ATTRIBUTE, BinaryType.FIXED.toString());
      orcType.setAttribute(ICEBERG_FIELD_LENGTH, Integer.toString(fixedLength));
      break;
    case BINARY:
      orcType = TypeDescription.createBinary();
      orcType.setAttribute(ICEBERG_BINARY_TYPE_ATTRIBUTE, BinaryType.BINARY.toString());
      break;
    case DECIMAL:
      orcType = TypeDescription.createDecimal()
          .withScale(decimal.scale())
          .withPrecision(decimal.precision());
      break;
    case VARIANT:
      orcType = TypeDescription.createStruct();
      orcType.addField(VARIANT_METADATA, TypeDescription.createBinary());
      orcType.addField(VARIANT_VALUE, TypeDescription.createBinary());
      orcType.setAttribute(ICEBERG_STRUCT_TYPE_ATTRIBUTE, VARIANT);
      break;
  }

  orcType.setAttribute(ICEBERG_ID_ATTRIBUTE, String.valueOf(fieldId));
  orcType.setAttribute(ICEBERG_REQUIRED_ATTRIBUTE, String.valueOf(isRequired));
  return orcType;
}
```

### 10.2 ORC 到 Iceberg 映射 (OrcToIcebergVisitor)

`OrcToIcebergVisitor` (`orc/src/main/java/org/apache/iceberg/orc/OrcToIcebergVisitor.java`) 执行反向转换。

**原始类型映射** (`OrcToIcebergVisitor.java` 第126-198行)：

```java
@Override
public Optional<Types.NestedField> primitive(TypeDescription primitive) {
  switch (primitive.getCategory()) {
    case BOOLEAN:   -> BooleanType
    case BYTE/SHORT/INT: -> IntegerType
    case LONG:      -> 检查 ICEBERG_LONG_TYPE_ATTRIBUTE (TIME 或 LONG)
    case FLOAT:     -> FloatType
    case DOUBLE:    -> DoubleType
    case STRING/CHAR/VARCHAR: -> StringType
    case BINARY:    -> 检查 ICEBERG_BINARY_TYPE_ATTRIBUTE (UUID/FIXED/BINARY)
    case DATE:      -> DateType
    case TIMESTAMP: -> 检查 TIMESTAMP_UNIT (MICROS -> TimestampType, NANOS -> TimestampNanoType)
    case TIMESTAMP_INSTANT: -> 同上但 withZone()
    case DECIMAL:   -> DecimalType.of(precision, scale)
  }
}
```

### 10.3 ORC 特有的属性标注机制

ORC 的类型系统不如 Parquet 丰富，因此 Iceberg 通过 ORC 的 attribute 机制补充了缺失的类型信息：

| ORC 属性名 | 值 | 用途 |
|-----------|-----|------|
| `iceberg.id` | 整数 | 字段的 Iceberg Field ID |
| `iceberg.required` | "true"/"false" | 字段是否为必需 |
| `iceberg.binary-type` | "UUID"/"FIXED"/"BINARY" | 区分 ORC binary 的三种 Iceberg 类型 |
| `iceberg.long-type` | "TIME"/"LONG" | 区分 ORC long 的两种 Iceberg 类型 |
| `iceberg.length` | 整数 | FIXED 类型的长度 |
| `iceberg.timestamp-unit` | "MICROS"/"NANOS" | 时间戳精度 |
| `iceberg.struct-type` | "VARIANT" | 标记 struct 实际上是 Variant 类型 |

这些属性是 Iceberg 能在 ORC 格式中精确还原类型信息的关键。没有它们，ORC 的 `BINARY` 无法区分是 UUID、FIXED 还是 BINARY；ORC 的 `LONG` 无法区分是 TIME 还是 LONG。

### 10.4 ORC Schema 投影与类型提升

`ORCSchemaUtil.buildOrcProjection()` (`ORCSchemaUtil.java` 第319-423行) 是 ORC 读取路径中的核心方法。它不仅处理 Schema 投影，还处理 Schema 演进中的类型提升。

**类型提升实现** (`ORCSchemaUtil.java` 第453-477行)：

```java
private static Optional<TypeDescription> getPromotedType(Type icebergType, TypeDescription originalOrcType) {
  if (LONG.equals(icebergType.typeId()) && INT.equals(originalOrcType.getCategory())) {
    return Optional.of(TypeDescription.createLong());   // int -> long
  } else if (DOUBLE.equals(icebergType.typeId()) && FLOAT.equals(originalOrcType.getCategory())) {
    return Optional.of(TypeDescription.createDouble());  // float -> double
  } else if (DECIMAL.equals(icebergType.typeId()) && DECIMAL.equals(originalOrcType.getCategory())) {
    if (newDecimal.scale() == originalOrcType.getScale()
        && newDecimal.precision() > originalOrcType.getPrecision()) {
      return Optional.of(TypeDescription.createDecimal()...);  // decimal precision promotion
    }
  }
  return Optional.empty();
}
```

---

## 11. Iceberg 与 Avro 类型映射

### 11.1 Iceberg 到 Avro 映射 (TypeToSchema)

`TypeToSchema` (`core/src/main/java/org/apache/iceberg/avro/TypeToSchema.java`) 将 Iceberg 类型转换为 Avro Schema。

**原始类型映射** (`TypeToSchema.java` 第213-285行)：

```java
@Override
public Schema primitive(Type.PrimitiveType primitive) {
  switch (primitive.typeId()) {
    case UNKNOWN:         -> Schema.create(Schema.Type.NULL)
    case BOOLEAN:         -> Schema.create(Schema.Type.BOOLEAN)
    case INTEGER:         -> Schema.create(Schema.Type.INT)
    case LONG:            -> Schema.create(Schema.Type.LONG)
    case FLOAT:           -> Schema.create(Schema.Type.FLOAT)
    case DOUBLE:          -> Schema.create(Schema.Type.DOUBLE)
    case DATE:            -> LogicalTypes.date().addToSchema(INT)
    case TIME:            -> LogicalTypes.timeMicros().addToSchema(LONG)
    case TIMESTAMP:       -> LogicalTypes.timestampMicros().addToSchema(LONG) + adjust-to-utc prop
    case TIMESTAMP_NANO:  -> LogicalTypes.timestampNanos().addToSchema(LONG) + adjust-to-utc prop
    case STRING:          -> Schema.create(Schema.Type.STRING)
    case UUID:            -> LogicalTypes.uuid().addToSchema(FIXED(16))
    case FIXED:           -> Schema.createFixed("fixed_N", null, null, length)
    case BINARY:          -> Schema.create(Schema.Type.BYTES)
    case DECIMAL:         -> LogicalTypes.decimal(P, S).addToSchema(FIXED("decimal_P_S", ..., requiredBytes))
  }
}
```

**Avro 中 Timestamp 的时区信息传递**：

Avro 的 `TimestampMicros` 逻辑类型本身不包含时区信息。Iceberg 通过自定义属性 `adjust-to-utc` 来区分 `timestamp` 和 `timestamptz`（第59-63行）：

```java
static {
  TIMESTAMP_SCHEMA.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, false);
  TIMESTAMPTZ_SCHEMA.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, true);
  TIMESTAMP_NANO_SCHEMA.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, false);
  TIMESTAMPTZ_NANO_SCHEMA.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, true);
}
```

**Variant 类型的 Avro 表示** (`TypeToSchema.java` 第198-210行)：

```java
@Override
public Schema variant(Types.VariantType variant) {
  Schema schema = Schema.createRecord(recordName, null, null, false,
      List.of(
          new Schema.Field("metadata", BINARY_SCHEMA),
          new Schema.Field("value", BINARY_SCHEMA)));
  return VariantLogicalType.get().addToSchema(schema);
}
```

Variant 在 Avro 中表示为一个带有 `VariantLogicalType` 注解的 record，包含两个 `bytes` 字段。

### 11.2 Avro 到 Iceberg 映射 (SchemaToType)

`SchemaToType` (`core/src/main/java/org/apache/iceberg/avro/SchemaToType.java`) 执行反向转换。

**逻辑类型处理** (`SchemaToType.java` 第180-213行)：

```java
public Type logicalType(Schema primitive, LogicalType logical) {
  if (logical instanceof LogicalTypes.Decimal) -> DecimalType
  if (logical instanceof LogicalTypes.Date) -> DateType
  if (logical instanceof LogicalTypes.TimeMillis || TimeMicros) -> TimeType
  if (logical instanceof LogicalTypes.TimestampMillis || TimestampMicros) ->
    isTimestamptz(primitive) ? TimestampType.withZone() : TimestampType.withoutZone()
  if (logical instanceof LogicalTypes.TimestampNanos) ->
    isTimestamptz(primitive) ? TimestampNanoType.withZone() : TimestampNanoType.withoutZone()
  if (LogicalTypes.uuid().getName().equals(name)) -> UUIDType
}
```

**Union 类型处理** (`SchemaToType.java` 第106-126行)：

Avro 的 Union 类型有两种处理方式：
1. 如果是 `[null, T]` 形式（option schema），则解包为 T 并标记为 optional
2. 如果是非标准 Union，则转换为 Iceberg 的 StructType，包含一个 `tag` 字段和各个选项字段

**Map 类型的双重表示** (`SchemaToType.java` 第129-173行)：

Avro 原生 Map 的键只能是 String 类型。对于非 String 键的 Map，Iceberg 使用 `LogicalMap`（存储为 array of key-value records）。`SchemaToType` 在 `array` 方法中检测这种特殊表示并还原为 Iceberg 的 MapType。

### 11.3 Avro 特有的可空性处理

Avro 中的可空性通过 Union 类型表示：`[null, T]` 表示可空的 T 类型。Iceberg 的 `AvroSchemaUtil` 提供了相关工具方法：

```java
// 判断是否为 option schema
static boolean isOptionSchema(Schema schema) {
  return schema.getType() == UNION && schema.getTypes().size() == 2
      && (schema.getTypes().get(0).getType() == NULL || schema.getTypes().get(1).getType() == NULL);
}

// 包装为 option
static Schema toOption(Schema schema) { return Schema.createUnion(NULL, schema); }

// 解包 option
static Schema fromOption(Schema schema) { ... }
```

### 11.4 Avro 名称兼容性处理

Avro 要求字段名满足 `[A-Za-z_][A-Za-z0-9_]*` 的格式。Iceberg 字段名可能包含不符合此要求的字符（如中文、特殊符号）。`AvroSchemaUtil.makeCompatibleName` (`AvroSchemaUtil.java` 第505-528行) 负责名称清理：

```java
public static String makeCompatibleName(String name) {
  if (!validAvroName(name)) {
    return sanitize(name);
  }
  return name;
}

static String sanitize(String name) {
  // 将不合法字符替换为 "_xHEX" 格式
  // 如数字开头会添加 "_" 前缀
}
```

当字段名被修改时，通过 `ICEBERG_FIELD_NAME_PROP` 属性保留原始名称（`TypeToSchema.java` 第126行）：

```java
if (!isValidFieldName) {
  field.addProp(AvroSchemaUtil.ICEBERG_FIELD_NAME_PROP, origFieldName);
}
```

---

## 12. Iceberg 与 Spark 类型映射

### 12.1 Iceberg 到 Spark 映射 (TypeToSparkType)

`TypeToSparkType` 存在于不同 Spark 版本的模块中。以 Spark v4.0 (`spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/TypeToSparkType.java`) 为例：

```java
@Override
public DataType primitive(Type.PrimitiveType primitive) {
  switch (primitive.typeId()) {
    case BOOLEAN:   return BooleanType$.MODULE$;
    case INTEGER:   return IntegerType$.MODULE$;
    case LONG:      return LongType$.MODULE$;
    case FLOAT:     return FloatType$.MODULE$;
    case DOUBLE:    return DoubleType$.MODULE$;
    case DATE:      return DateType$.MODULE$;
    case TIME:      throw new UnsupportedOperationException("Spark does not support time fields");
    case TIMESTAMP:
      if (ts.shouldAdjustToUTC()) return TimestampType$.MODULE$;
      else return TimestampNTZType$.MODULE$;
    case STRING:    return StringType$.MODULE$;
    case UUID:      return StringType$.MODULE$;  // UUID 映射为 String
    case FIXED:     return BinaryType$.MODULE$;
    case BINARY:    return BinaryType$.MODULE$;
    case DECIMAL:   return DecimalType$.MODULE$.apply(precision, scale);
    case UNKNOWN:   return NullType$.MODULE$;  // v4.0 新增
  }
}
```

**Spark v4.0 特有的 Variant 支持**（第111-113行）：

```java
@Override
public DataType variant(Types.VariantType variant) {
  return VariantType$.MODULE$;
}
```

Spark 4.0 原生支持 Variant 类型，因此可以直接映射。

**默认值支持**（v4.0 `TypeToSparkType.java` 第75-86行）：

```java
if (field.writeDefault() != null) {
  Object writeDefault = SparkUtil.internalToSpark(field.type(), field.writeDefault());
  sparkField = sparkField.withCurrentDefaultValue(
      Literal$.MODULE$.create(writeDefault, type).sql());
}
if (field.initialDefault() != null) {
  Object initialDefault = SparkUtil.internalToSpark(field.type(), field.initialDefault());
  sparkField = sparkField.withExistenceDefaultValue(
      Literal$.MODULE$.create(initialDefault, type).sql());
}
```

Spark 4.0 支持列默认值，Iceberg 的 `writeDefault` 映射为 `currentDefaultValue`，`initialDefault` 映射为 `existenceDefaultValue`。

### 12.2 Spark 到 Iceberg 映射 (SparkTypeToType)

`SparkTypeToType` (`spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/SparkTypeToType.java`)：

```java
@Override
public Type atomic(DataType atomic) {
  if (atomic instanceof BooleanType)    return Types.BooleanType.get();
  if (atomic instanceof IntegerType
      || atomic instanceof ShortType
      || atomic instanceof ByteType)    return Types.IntegerType.get();  // 窄类型提升
  if (atomic instanceof LongType)       return Types.LongType.get();
  if (atomic instanceof FloatType)      return Types.FloatType.get();
  if (atomic instanceof DoubleType)     return Types.DoubleType.get();
  if (atomic instanceof StringType
      || atomic instanceof CharType
      || atomic instanceof VarcharType) return Types.StringType.get();  // 字符类型统一
  if (atomic instanceof DateType)       return Types.DateType.get();
  if (atomic instanceof TimestampType)  return Types.TimestampType.withZone();
  if (atomic instanceof TimestampNTZType) return Types.TimestampType.withoutZone();
  if (atomic instanceof DecimalType)    return Types.DecimalType.of(precision, scale);
  if (atomic instanceof BinaryType)     return Types.BinaryType.get();
  if (atomic instanceof NullType)       return Types.UnknownType.get();  // v4.0
}
```

**关键映射决策：**

1. **Spark 的 ByteType/ShortType 提升为 IntegerType**：Iceberg 不支持 8 位和 16 位整数
2. **Spark 的 CharType/VarcharType 统一为 StringType**：Iceberg 不区分定长/变长字符串
3. **Spark 的 TimestampType 映射为 TimestampType.withZone()**：Spark 的 Timestamp 默认含时区
4. **Spark 的 TimestampNTZType 映射为 TimestampType.withoutZone()**：NTZ = No Time Zone

**Variant 类型支持**（v4.0 `SparkTypeToType.java` 第122-124行）：

```java
@Override
public Type variant(VariantType variant) {
  return Types.VariantType.get();
}
```

### 12.3 Spark 版本差异 (v3.4/v3.5/v4.0/v4.1)

通过对比不同版本的 `TypeToSparkType` 可以发现：

| 特性 | v3.4 | v3.5 | v4.0 | v4.1 |
|------|------|------|------|------|
| TIME 支持 | 不支持 | 不支持 | 不支持 | 不支持 |
| TimestampNTZ | 支持 | 支持 | 支持 | 支持 |
| UUID 映射 | String | String | String | String |
| Variant 类型 | 不支持 | 不支持 | 支持 (VariantType$) | 支持 |
| UnknownType | 不支持 | 不支持 | NullType$ | NullType$ |
| 默认值 | 不支持 | 不支持 | 支持 | 支持 |

在所有版本中，Spark 始终不支持 Iceberg 的 `TimeType`。如果表中包含 TIME 类型列，尝试在 Spark 中读取会抛出 `UnsupportedOperationException`。

---

## 13. Iceberg 与 Flink 类型映射

### 13.1 Iceberg 到 Flink 映射 (TypeToFlinkType)

`TypeToFlinkType` (`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/TypeToFlinkType.java`)：

```java
@Override
public LogicalType primitive(Type.PrimitiveType primitive) {
  switch (primitive.typeId()) {
    case UNKNOWN:         return new NullType();
    case BOOLEAN:         return new BooleanType();
    case INTEGER:         return new IntType();
    case LONG:            return new BigIntType();
    case FLOAT:           return new FloatType();
    case DOUBLE:          return new DoubleType();
    case DATE:            return new DateType();
    case TIME:            return new TimeType();  // Flink 支持 TIME!
    case TIMESTAMP:
      if (adjustToUTC) return new LocalZonedTimestampType(6);  // 微秒精度
      else             return new TimestampType(6);
    case TIMESTAMP_NANO:
      if (adjustToUTC) return new LocalZonedTimestampType(9);  // 纳秒精度
      else             return new TimestampType(9);
    case STRING:          return new VarCharType(VarCharType.MAX_LENGTH);
    case UUID:            return new BinaryType(16);    // UUID 映射为定长二进制
    case FIXED:           return new BinaryType(length);
    case BINARY:          return new VarBinaryType(VarBinaryType.MAX_LENGTH);
    case DECIMAL:         return new DecimalType(precision, scale);
  }
}
```

**与 Spark 映射的关键差异：**

1. **TIME 类型**：Flink 支持 `TimeType`，Spark 不支持
2. **UUID**：Flink 映射为 `BinaryType(16)`，Spark 映射为 `StringType`
3. **STRING**：Flink 使用 `VarCharType(MAX_LENGTH)`，Spark 使用 `StringType`
4. **BINARY**：Flink 使用 `VarBinaryType(MAX_LENGTH)`，Spark 使用 `BinaryType`
5. **FIXED**：Flink 使用 `BinaryType(length)` 保留长度信息，Spark 丢弃长度映射为 `BinaryType`
6. **TIMESTAMP 精度**：Flink 明确指定精度（6 或 9），Spark 不指定
7. **timestamptz**：Flink 使用 `LocalZonedTimestampType`，Spark 使用 `TimestampType`

### 13.2 Flink 到 Iceberg 映射 (FlinkTypeToType)

`FlinkTypeToType` (`flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/FlinkTypeToType.java`)：

```java
// 各种字符类型统一为 StringType
Type visit(CharType charType)    -> Types.StringType.get()
Type visit(VarCharType varChar)  -> Types.StringType.get()

// 窄整数类型提升
Type visit(TinyIntType)  -> Types.IntegerType.get()
Type visit(SmallIntType) -> Types.IntegerType.get()
Type visit(IntType)      -> Types.IntegerType.get()
Type visit(BigIntType)   -> Types.LongType.get()

// 二进制类型
Type visit(BinaryType)    -> Types.FixedType.ofLength(length)  // 定长映射为 FIXED
Type visit(VarBinaryType) -> Types.BinaryType.get()

// 时间戳类型
Type visit(TimestampType)            -> Types.TimestampType.withoutZone()
Type visit(LocalZonedTimestampType)  -> Types.TimestampType.withZone()

// Multiset 特殊处理
Type visit(MultisetType multisetType) {
  Type elementType = multisetType.getElementType().accept(this);
  return Types.MapType.ofRequired(getNextId(), getNextId(), elementType, Types.IntegerType.get());
}
```

**Flink Multiset 的特殊映射**：Flink 的 `MultisetType` 在 Iceberg 中没有直接对应类型，被转换为 `MapType<元素类型, IntegerType>`，其中 IntegerType 表示元素的出现次数。

**BinaryType 的方向不对称**：

注意一个重要的不对称性：
- Iceberg `FixedType` -> Flink `BinaryType(length)` （TypeToFlinkType）
- Flink `BinaryType(length)` -> Iceberg `FixedType.ofLength(length)` （FlinkTypeToType）

这种映射是双向一致的。但是：
- Iceberg `UUIDType` -> Flink `BinaryType(16)` （TypeToFlinkType）
- Flink `BinaryType(16)` -> Iceberg `FixedType.ofLength(16)` （FlinkTypeToType）

这意味着 UUID 经过 Flink 往返转换后，会变成 `FixedType.ofLength(16)` 而非原始的 `UUIDType`。这是一个已知的信息损失。

### 13.3 Flink 的精度与可空性处理

**时间戳精度**：Flink 的 TimestampType 和 LocalZonedTimestampType 携带精度参数。Iceberg 的 TIMESTAMP 精度为微秒（6位），TIMESTAMP_NANO 为纳秒（9位）。TypeToFlinkType 明确设置了精度：
- `TIMESTAMP` -> `TimestampType(6)` 或 `LocalZonedTimestampType(6)`
- `TIMESTAMP_NANO` -> `TimestampType(9)` 或 `LocalZonedTimestampType(9)`

**可空性传递**：Flink 的 LogicalType 有一个 `copy(boolean nullable)` 方法。TypeToFlinkType 在构建 StructType 时正确传递了可空性（第63行）：

```java
RowType.RowField flinkField =
    new RowType.RowField(field.name(), type.copy(field.isOptional()), field.doc());
```

---

## 14. 跨引擎类型映射全景对照表

### 14.1 原始类型完整映射表

| Iceberg 类型 | Parquet 物理/逻辑类型 | ORC 类型 | Avro 类型 | Spark 类型 | Flink 类型 |
|-------------|---------------------|---------|----------|-----------|-----------|
| `boolean` | BOOLEAN | BOOLEAN | boolean | BooleanType | BooleanType |
| `int` | INT32 | INT | int | IntegerType | IntType |
| `long` | INT64 | LONG + attr(LONG) | long | LongType | BigIntType |
| `float` | FLOAT | FLOAT | float | FloatType | FloatType |
| `double` | DOUBLE | DOUBLE | double | DoubleType | DoubleType |
| `date` | INT32 + DATE | DATE | int + date | DateType | DateType |
| `time` | INT64 + TIME_MICROS | LONG + attr(TIME) | long + timeMicros | **不支持** | TimeType |
| `timestamp` | INT64 + TIMESTAMP_MICROS | TIMESTAMP + attr(MICROS) | long + timestampMicros + adjust=false | TimestampNTZType | TimestampType(6) |
| `timestamptz` | INT64 + TIMESTAMPTZ_MICROS | TIMESTAMP_INSTANT + attr(MICROS) | long + timestampMicros + adjust=true | TimestampType | LocalZonedTimestampType(6) |
| `timestamp_ns` | INT64 + TIMESTAMP_NANOS | TIMESTAMP + attr(NANOS) | long + timestampNanos + adjust=false | **不支持** | TimestampType(9) |
| `timestamptz_ns` | INT64 + TIMESTAMPTZ_NANOS | TIMESTAMP_INSTANT + attr(NANOS) | long + timestampNanos + adjust=true | **不支持** | LocalZonedTimestampType(9) |
| `string` | BINARY + STRING | STRING | string | StringType | VarCharType(MAX) |
| `uuid` | FIXED(16) + UUID | BINARY + attr(UUID) | fixed(16) + uuid | StringType | BinaryType(16) |
| `fixed[N]` | FIXED_LEN_BYTE_ARRAY(N) | BINARY + attr(FIXED) + length | fixed(N) | BinaryType | BinaryType(N) |
| `binary` | BINARY | BINARY + attr(BINARY) | bytes | BinaryType | VarBinaryType(MAX) |
| `decimal(P,S)` | INT32/INT64/FIXED + decimal(P,S) | DECIMAL(P,S) | fixed + decimal(P,S) | DecimalType(P,S) | DecimalType(P,S) |
| `unknown` | 不写入 | 不写入 | null | NullType (v4.0+) | NullType |
| `variant` | Group(metadata+value) + variantType | Struct(metadata+value) + attr(VARIANT) | Record(metadata+value) + VariantLogicalType | VariantType (v4.0+) | **不支持** |
| `geometry` | **不支持** | **不支持** | **不支持** | **不支持** | **不支持** |
| `geography` | **不支持** | **不支持** | **不支持** | **不支持** | **不支持** |

### 14.2 复合类型映射表

| Iceberg 类型 | Parquet | ORC | Avro | Spark | Flink |
|-------------|---------|-----|------|-------|-------|
| `struct<...>` | Group (OPTIONAL/REQUIRED) | Struct | Record | StructType | RowType |
| `list<T>` | Group (LIST) + element | List | Array | ArrayType | ArrayType |
| `map<K,V>` | Group (MAP) + key_value | Map | Map(String key) / Array(kv Record) | MapType | MapType |

**Avro Map 的特殊处理**：

Avro 原生 Map 只支持 String 键。当 Iceberg Map 的键不是 String 时：
- **写入 Avro**：使用 `LogicalMap`，存储为 `Array<Record<key, value>>`
- **读取 Avro**：`SchemaToType.array()` 检测 `LogicalMap` 逻辑类型并还原为 MapType

### 14.3 Java 运行时类型映射表

| Iceberg TypeID | Java 映射类型 | 大小（字节） | 说明 |
|---------------|-------------|------------|------|
| BOOLEAN | `java.lang.Boolean` | ~1 | JVM 实现相关 |
| INTEGER | `java.lang.Integer` | 4 | 32位有符号 |
| LONG | `java.lang.Long` | 8 | 64位有符号 |
| FLOAT | `java.lang.Float` | 4 | IEEE 754 单精度 |
| DOUBLE | `java.lang.Double` | 8 | IEEE 754 双精度 |
| DATE | `java.lang.Integer` | 4 | epoch 天数 |
| TIME | `java.lang.Long` | 8 | 午夜微秒数 |
| TIMESTAMP | `java.lang.Long` | 8 | epoch 微秒数 |
| TIMESTAMP_NANO | `java.lang.Long` | 8 | epoch 纳秒数 |
| STRING | `java.lang.CharSequence` | ~54 (估算) | 通常为 UTF-8 |
| UUID | `java.util.UUID` | ~28 | 128位 |
| FIXED | `java.nio.ByteBuffer` | N | 固定长度 |
| BINARY | `java.nio.ByteBuffer` | ~80 (估算) | 变长 |
| DECIMAL | `java.math.BigDecimal` | ~44 | 最大精度38 |
| STRUCT | `org.apache.iceberg.StructLike` | 可变 | 行数据接口 |
| LIST | `java.util.List` | 可变 | 有序集合 |
| MAP | `java.util.Map` | 可变 | 键值映射 |
| VARIANT | `org.apache.iceberg.variants.Variant` | ~80 (估算) | 半结构化 |

---

## 15. 常见类型不兼容问题与解决方案

### 15.1 时间类型不兼容

**问题**：Spark 不支持 Iceberg 的 `TimeType`。如果 Iceberg 表包含 TIME 类型列，Spark 无法读取。

**源码证据** (`TypeToSparkType.java` 第107行)：
```java
case TIME:
  throw new UnsupportedOperationException("Spark does not support time fields");
```

**解决方案**：
1. 避免在需要 Spark 访问的表中使用 TIME 类型
2. 将 TIME 存储为 LONG（微秒值），在应用层进行转换
3. 使用 Flink 或 Trino 来处理包含 TIME 列的表

### 15.2 UUID 类型的引擎差异

**问题**：Iceberg 的 UUID 在不同引擎中映射为不同类型：
- Spark：StringType（字符串表示）
- Flink：BinaryType(16)（16字节二进制）
- Parquet：FIXED_LEN_BYTE_ARRAY(16) + UUID 逻辑类型
- ORC：BINARY + iceberg.binary-type=UUID 属性

**往返转换风险**：
- 通过 Spark 读出 UUID 后得到 String，再通过 Spark 写回会变成 StringType
- 通过 Flink 读出 UUID 后得到 Binary(16)，再通过 Flink 写回会变成 FixedType(16)

**解决方案**：
1. 创建表时使用 Iceberg API 而非引擎 DDL，确保类型准确
2. 使用 `reassignIds` 而非引擎的 Schema 推断来保持类型一致性
3. 跨引擎操作时，始终以 Iceberg Schema（而非引擎 Schema）作为权威来源

### 15.3 Decimal 精度溢出问题

**问题**：Iceberg 限制 Decimal 精度最大为 38（`Types.java` 第526-529行）：

```java
Preconditions.checkArgument(
    precision <= 38,
    "Decimals with precision larger than 38 are not supported: %s", precision);
```

**Parquet 存储优化带来的问题**：

Decimal 在 Parquet 中根据精度选择不同物理类型：
- 精度 1-9：INT32（4字节）
- 精度 10-18：INT64（8字节）
- 精度 19-38：FIXED_LEN_BYTE_ARRAY

如果在 Schema 演进中将 Decimal 精度从 9 提升到 10，Parquet 的底层物理类型会从 INT32 变为 INT64。ORC reader 需要能够处理这种变化。

**Decimal 提升规则**：只允许增加精度，不允许改变 scale。例如 `decimal(10,2)` 可以提升为 `decimal(15,2)`，但不能提升为 `decimal(15,3)`。

### 15.4 TimestampNano 的引擎支持差异

**问题**：TimestampNano（纳秒精度时间戳）是 Iceberg v3 格式新增的类型。

**引擎支持情况**：
- Flink 1.20+：完全支持（通过 `TimestampType(9)` / `LocalZonedTimestampType(9)`）
- Spark 3.5：不支持（TypeToSparkType 中没有 TIMESTAMP_NANO case）
- Spark 4.0+：不直接支持（仍然只处理微秒精度的 TIMESTAMP）

**格式版本限制** (`Schema.java` 第64-70行)：

```java
static final Map<Type.TypeID, Integer> MIN_FORMAT_VERSIONS = ImmutableMap.of(
    Type.TypeID.TIMESTAMP_NANO, 3,
    Type.TypeID.VARIANT, 3,
    Type.TypeID.UNKNOWN, 3,
    Type.TypeID.GEOMETRY, 3,
    Type.TypeID.GEOGRAPHY, 3);
```

这意味着 `TIMESTAMP_NANO` 只能在 v3 格式的表中使用。在 v1 或 v2 格式的表中添加此类型会失败。

### 15.5 Variant 类型的格式版本限制

**问题**：Variant 类型仅在 v3 格式中可用。

**引擎支持**：
- Spark 4.0+：原生支持 `VariantType`
- Spark 3.x：不支持
- Flink：不支持（TypeToFlinkType 中没有 VARIANT case，会抛出 UnsupportedOperationException）

**存储格式支持**：
- Parquet：支持（通过 variant logical type annotation 的 group 类型）
- ORC：支持（通过带 `iceberg.struct-type=VARIANT` 属性的 struct）
- Avro：支持（通过带 `VariantLogicalType` 的 record）

### 15.6 Fixed 与 Binary 在 ORC 中的二义性

**问题**：ORC 格式没有区分 `FIXED`、`BINARY` 和 `UUID` 的原生类型——它们都映射为 ORC 的 `BINARY`。

**解决机制**：Iceberg 通过 ORC 属性 (`iceberg.binary-type`) 进行区分。但如果 ORC 文件不是由 Iceberg 写入的（缺少这些属性），则无法区分，默认映射为 `BINARY`。

**源码证据** (`OrcToIcebergVisitor.java` 第219-241行)：
```java
private static void convertBinary(TypeDescription binary, Types.NestedField.Builder builder) {
  String binaryAttributeValue = binary.getAttributeValue(ICEBERG_BINARY_TYPE_ATTRIBUTE);
  ORCSchemaUtil.BinaryType binaryType = binaryAttributeValue == null
      ? ORCSchemaUtil.BinaryType.BINARY  // 默认为 BINARY
      : ORCSchemaUtil.BinaryType.valueOf(binaryAttributeValue);
}
```

### 15.7 Spark 中的 Short/Byte 类型损失

**问题**：当从 Spark DataFrame 转换为 Iceberg Schema 时，Spark 的 `ByteType`、`ShortType` 都会被提升为 Iceberg 的 `IntegerType`。

**源码证据** (`SparkTypeToType.java` 第128-131行)：
```java
} else if (atomic instanceof IntegerType
    || atomic instanceof ShortType
    || atomic instanceof ByteType) {
  return Types.IntegerType.get();
```

这意味着：
1. 如果通过 Spark DDL 创建包含 `BYTE` 或 `SHORT` 列的表，它们在 Iceberg 中会变成 `INT`
2. 这是单向的信息损失——从 Iceberg 的 `IntegerType` 无法还原出原始的 `ByteType` 或 `ShortType`

类似地，Spark 的 `CharType` 和 `VarcharType` 都映射为 Iceberg 的 `StringType`，长度信息会丢失。Flink 的 `TinyIntType`、`SmallIntType`、`CharType` 也有类似问题。

---

## 16. 总结与最佳实践

### 16.1 类型系统架构总结

Apache Iceberg 的类型系统是一个精心设计的多层次架构：

1. **核心层**（`api` 模块）：定义了 `Type` 接口、`Types` 中的所有具体类型、`TypeUtil` 工具类。这一层与存储格式和计算引擎完全无关。

2. **格式映射层**（`parquet`/`orc`/`core` 模块）：提供了 Iceberg 类型与 Parquet/ORC/Avro 之间的双向转换。每种格式都有其特殊处理逻辑（如 ORC 的属性标注、Avro 的 Union 可空性、Parquet 的 Decimal 存储优化）。

3. **引擎映射层**（`spark`/`flink` 模块）：提供了 Iceberg 类型与计算引擎类型系统之间的双向转换。不同引擎版本的映射器可能有差异（如 Spark 3.5 vs 4.0 的 Variant 支持）。

### 16.2 Field ID 的核心地位

Field ID 是 Iceberg 类型系统中最关键的概念。它使得以下操作安全可行：

- **列重命名**：不影响数据读取
- **列重排序**：不影响数据映射
- **列删除后重建**：新旧列有不同 ID，不会混淆
- **Schema 演进**：通过 ID 追踪列的完整历史

在所有存储格式中，Field ID 都以不同方式被持久化：
- Parquet：`field_id` 元数据
- ORC：`iceberg.id` attribute
- Avro：`field-id` property

### 16.3 跨引擎使用最佳实践

基于对源码的深入分析，以下是跨引擎使用 Iceberg 类型系统的最佳实践：

1. **使用 Iceberg API 创建表**：避免通过引擎 DDL 创建表，因为引擎的 Schema 推断可能丢失类型信息（如 UUID -> String）

2. **避免使用引擎特有类型**：不要在需要跨引擎访问的表中使用 Iceberg 的 `TimeType`（Spark 不支持）或 `VariantType`（Flink 不支持）

3. **注意类型提升的限制**：只有 `int -> long`、`float -> double`、`decimal(P1,S) -> decimal(P2,S)` 三种提升是允许的。其他类型变更需要重建表。

4. **Decimal 精度规划**：预先规划好 Decimal 的精度和小数位数，因为 scale 不允许变更。宁可初始设置较大的精度。

5. **时间戳类型选择**：
   - 跨时区场景使用 `timestamptz`
   - 本地时间场景使用 `timestamp`
   - 需要纳秒精度时使用 `timestamp_ns`/`timestamptz_ns`（需 v3 格式）
   - 注意 Spark 中 `TimestampType` 对应 `timestamptz`，`TimestampNTZType` 对应 `timestamp`

6. **Format 版本选择**：如果需要使用 `TimestampNanoType`、`VariantType`、`GeometryType`、`GeographyType` 或 `UnknownType`，必须使用 v3 格式。

7. **Schema 演进的安全操作**：
   - 添加 optional 列：始终安全
   - 添加 required 列：需要提供默认值或启用 `allowIncompatibleChanges`
   - 删除列：安全，但 ID 永不重用
   - 重命名列：安全，通过 ID 关联
   - 类型提升：仅限允许的三种路径

### 16.4 类型映射的不对称性总结

以下类型存在跨引擎往返转换的信息损失：

| 原始类型 | 经过的引擎 | 损失的信息 | 转换后类型 |
|---------|----------|-----------|----------|
| UUID | Spark | 类型语义 | StringType -> StringType |
| UUID | Flink | 类型语义 | BinaryType(16) -> FixedType(16) |
| FixedType(N) | Spark | 长度信息 | BinaryType -> BinaryType |
| TimeType | Spark | 完全不支持 | 抛出异常 |
| TimestampNanoType | Spark 3.x | 完全不支持 | 抛出异常 |
| VariantType | Flink | 完全不支持 | 抛出异常 |
| VariantType | Spark 3.x | 完全不支持 | 抛出异常 |
| ByteType(Spark) | Spark -> Iceberg | 窄类型 | IntegerType |
| ShortType(Spark) | Spark -> Iceberg | 窄类型 | IntegerType |
| CharType(Spark/Flink) | Spark/Flink -> Iceberg | 长度信息 | StringType |
| VarcharType(Spark/Flink) | Spark/Flink -> Iceberg | 长度信息 | StringType |
| MultisetType(Flink) | Flink -> Iceberg | 类型语义 | MapType<T, IntegerType> |

### 16.5 源码文件索引

本文分析涉及的核心源码文件：

| 文件路径 | 核心职责 |
|---------|---------|
| `api/src/main/java/org/apache/iceberg/types/Type.java` | Type 接口定义 |
| `api/src/main/java/org/apache/iceberg/types/Types.java` | 所有具体类型实现 |
| `api/src/main/java/org/apache/iceberg/types/TypeUtil.java` | 类型工具类 |
| `api/src/main/java/org/apache/iceberg/types/AssignFreshIds.java` | ID 分配核心 |
| `api/src/main/java/org/apache/iceberg/types/CheckCompatibility.java` | 兼容性检查 |
| `api/src/main/java/org/apache/iceberg/Schema.java` | Schema 定义 |
| `core/src/main/java/org/apache/iceberg/SchemaUpdate.java` | Schema 演进 API |
| `core/src/main/java/org/apache/iceberg/avro/AvroSchemaUtil.java` | Avro 工具类 |
| `core/src/main/java/org/apache/iceberg/avro/SchemaToType.java` | Avro -> Iceberg |
| `core/src/main/java/org/apache/iceberg/avro/TypeToSchema.java` | Iceberg -> Avro |
| `parquet/src/main/java/org/apache/iceberg/parquet/TypeToMessageType.java` | Iceberg -> Parquet |
| `parquet/src/main/java/org/apache/iceberg/parquet/MessageTypeToType.java` | Parquet -> Iceberg |
| `parquet/src/main/java/org/apache/iceberg/parquet/ParquetSchemaUtil.java` | Parquet 工具类 |
| `orc/src/main/java/org/apache/iceberg/orc/ORCSchemaUtil.java` | ORC 映射工具 |
| `orc/src/main/java/org/apache/iceberg/orc/OrcToIcebergVisitor.java` | ORC -> Iceberg |
| `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/TypeToSparkType.java` | Iceberg -> Spark |
| `spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/SparkTypeToType.java` | Spark -> Iceberg |
| `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/TypeToFlinkType.java` | Iceberg -> Flink |
| `flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/FlinkTypeToType.java` | Flink -> Iceberg |

### 16.6 设计哲学

通过对 Iceberg 类型系统源码的全面分析，可以总结出其核心设计哲学：

1. **安全优先**：类型提升规则极其保守，宁可限制灵活性也不冒数据安全风险
2. **独立于引擎**：类型系统完全自包含，不依赖任何引擎的类型定义
3. **ID 即标识**：Field ID 是字段身份的唯一标识，名称只是展示标签
4. **双向可逆**：每种格式/引擎都有完整的双向转换器，尽量减少信息损失
5. **渐进演化**：通过 format version 控制新类型的引入（v3 = TimestampNano + Variant + Geometry + Geography + Unknown），确保向后兼容
6. **访问者模式贯穿始终**：`SchemaVisitor` 和 `CustomOrderSchemaVisitor` 是整个类型系统操作的基础设施，所有类型转换、投影、裁剪、ID 分配都基于此模式实现

这种设计使得 Iceberg 能够作为跨引擎、跨格式的统一数据管理层，在保证数据安全的前提下支持灵活的 Schema 演进。

---

## 文档技术验证记录

**验证日期**: 2026-04-20

**验证范围**: 对照 Apache Iceberg 源码验证文档中的类名、方法名、行号引用和技术描述

**发现的问题及修正**:

1. **Type.java 行号修正**:
   - 原文: "Type 接口第31-115行"
   - 修正: "Type 接口第31-114行"
   - 原因: Type 接口实际在第114行结束，不是115行

2. **TypeID 枚举行号修正**:
   - 原文: "TypeID 枚举第32-64行"
   - 修正: "TypeID 枚举第32-63行"
   - 原因: TypeID 枚举在第63行结束

3. **PrimitiveType 行号修正**:
   - 原文: "PrimitiveType 第116-147行"
   - 修正: "PrimitiveType 第116-146行"
   - 原因: PrimitiveType 抽象类在第146行结束

4. **NestedType 行号修正**:
   - 原文: "NestedType 第149-165行"
   - 修正: "NestedType 第149-164行"
   - 原因: NestedType 抽象类在第164行结束

5. **代码格式优化**:
   - 修正了 Type 接口代码示例中方法的顺序，使其与源码一致
   - 修正了 PrimitiveType 代码示例中的格式，使用源码的完整 if-else 格式

**验证通过的关键技术点**:

1. ✅ TypeID 枚举的所有类型定义与源码完全一致
2. ✅ Schema.java 中 MIN_FORMAT_VERSIONS 的定义准确（第64-70行）
3. ✅ TypeUtil.isPromotionAllowed 方法的类型提升规则准确
4. ✅ Parquet 类型映射（TypeToMessageType）中 TIMESTAMP 和 TIMESTAMP_NANO 的处理逻辑准确
5. ✅ ORC 类型映射中 BinaryType 枚举定义准确（UUID, FIXED, BINARY）
6. ✅ Spark 类型映射中 UUID -> StringType 的映射准确
7. ✅ Flink 类型映射中 UUID -> BinaryType(16) 的映射准确
8. ✅ 跨引擎类型映射对照表中的所有映射关系准确
9. ✅ 常见类型不兼容问题的描述与源码一致
10. ✅ DecimalType 的精度限制（最大38位）准确

**总体评估**: 文档技术内容准确，仅存在少量行号引用偏差（±1行），已全部修正。所有类名、方法名、类型映射规则、设计决策描述均与源码一致。

# Iceberg 表元数据 (`metadata.json`) 完整带注释示例

> **源码忠实性声明**：以下所有 JSON 字段名均从源码 Parser 类的常量定义中提取，不存在任何虚构字段。
> 注释行以 `//` 标注（实际 JSON 不支持注释，此处仅为说明目的）。

## 场景设定

一张电商订单表 `warehouse.orders`，经历了以下 **9 次操作**：

| 操作序号 | 时间 | 操作类型 | 描述 |
|---------|------|---------|------|
| S1 | T+0h | `append` | 初始写入 3 个数据文件 |
| S2 | T+2h | `append` | 追加写入 2 个数据文件 |
| S3 | T+4h | Schema 变更 | 新增 `coupon_code` 列（带 initial-default） |
| S4 | T+6h | `overwrite` | MOR 删除 + 新增 equality delete files |
| S5 | T+8h | `delete` | 纯删除一批过期数据文件 |
| S6 | T+10h | `replace` | 小文件合并（compaction，继承 sequence number） |
| S7 | T+12h | `append` | 按新分区规则（month）追加数据；创建审计分支 |
| S8 | T+14h | Schema 变更 + `append` | **类型提升**：`quantity` int→long, `amount` decimal(12,2)→decimal(16,2) |
| S9 | T+16h | Schema 变更 + `append` | **字段删除**：移除 `product` 列（id=4） |

同时涉及：**分区规格演进**（identity → month）、**排序规则变更**、**创建 branch 和 tag**、**字段类型提升**、**字段删除**。

---

## 完整 metadata.json（带逐行注释）

```jsonc
{
  // ========================= 顶层元信息 =========================
  // 来源: TableMetadataParser.FORMAT_VERSION = "format-version"
  // 值含义: 2 = V2 格式，支持 delete files / sequence number / row-level deletes
  //        V1 不支持 delete files，V3 新增 next-row-id
  "format-version": 2,

  // 来源: TableMetadataParser.TABLE_UUID = "table-uuid"
  // 值含义: 表的全局唯一标识符，创建时生成，整个生命周期不变
  //        用于防止不同 catalog 中同名表被误操作
  "table-uuid": "a8f3d1c2-4e97-4b1a-bc34-0f7e8a12d456",

  // 来源: TableMetadataParser.LOCATION = "location"
  // 值含义: 表数据和元数据的根路径，所有子目录（data/、metadata/）都在其下
  "location": "s3://my-datalake/warehouse/orders",

  // 来源: TableMetadataParser.LAST_SEQUENCE_NUMBER = "last-sequence-number"
  // 值含义: 已分配的最大全局序列号（V2+ 特有）
  //        每次 snapshot commit 时递增 +1，用于 delete file 覆盖判断
  //        当前值为 9，因为经历了 9 次 snapshot 提交
  "last-sequence-number": 9,

  // 来源: TableMetadataParser.LAST_UPDATED_MILLIS = "last-updated-ms"
  // 值含义: 该 metadata.json 文件的最后更新时间（毫秒时间戳）
  "last-updated-ms": 1713628800000,

  // 来源: TableMetadataParser.LAST_COLUMN_ID = "last-column-id"
  // 值含义: 已分配的最大列 ID（全局递增）
  //        初始 schema 有 id=1~6，后来新增了 id=7 的 coupon_code
  //        虽然后续删除了 product 列(id=4)，但 last-column-id 仍为 7
  //        列 ID 只增不减，即使删列也不回收（ID 永不复用）
  "last-column-id": 7,

  // ========================= Schema 演进 =========================
  // 来源: TableMetadataParser.CURRENT_SCHEMA_ID = "current-schema-id"
  // 值含义: 当前生效的 schema ID，指向 schemas 数组中的某个 schema
  //        这里指向 schema-id=3（删除了 product 列后的最终版本）
  "current-schema-id": 3,

  // 来源: TableMetadataParser.SCHEMAS = "schemas"
  // 值含义: 表历史上所有版本的 schema 列表（只增不删）
  //        查询引擎根据 snapshot.schema-id 找到对应 schema 来解析 data file
  "schemas": [
    // ---- Schema 0: 初始版本 ----
    {
      // 来源: SchemaParser.TYPE = "type"
      // 值含义: 必须是 "struct"，Iceberg schema 的顶层类型
      "type": "struct",

      // 来源: SchemaParser.SCHEMA_ID = "schema-id"
      // 值含义: 该 schema 的版本号，在表内唯一，只增不减
      "schema-id": 0,

      // 来源: SchemaParser.IDENTIFIER_FIELD_IDS = "identifier-field-ids"
      // 值含义: 标识符字段 ID 集合（类似主键，但 Iceberg 不强制唯一性）
      //        用于 upsert 操作确定匹配键
      //        要求: 必须是 required 的原始类型，不能是 float/double
      // 验证逻辑: Schema.validateIdentifierField() 确保字段存在且为required原始类型
      "identifier-field-ids": [1],

      // 来源: SchemaParser.FIELDS = "fields"
      "fields": [
        {
          // 来源: SchemaParser.ID = "id" — 字段在表内的唯一 ID，全局递增
          // 来源: SchemaParser.NAME = "name" — 字段名称
          // 来源: SchemaParser.REQUIRED = "required" — 是否必填（true=不可为null）
          // 来源: SchemaParser.TYPE = "type" — 字段类型
          "id": 1, "name": "order_id",    "required": true,  "type": "long"
        },
        {
          "id": 2, "name": "order_date",  "required": true,  "type": "date"
        },
        {
          "id": 3, "name": "customer_id", "required": true,  "type": "long"
        },
        {
          "id": 4, "name": "product",     "required": false, "type": "string",
          // 来源: SchemaParser.DOC = "doc"
          // 值含义: 字段的描述文档，可选字段
          "doc": "Product name or SKU"
        },
        {
          "id": 5, "name": "quantity",    "required": false, "type": "int"
        },
        {
          "id": 6, "name": "amount",      "required": false, "type": "decimal(12, 2)"
        }
      ]
    },

    // ---- Schema 1: 新增 coupon_code 列（T+4h 的 schema 变更）----
    {
      "type": "struct",
      "schema-id": 1,
      "identifier-field-ids": [1],
      "fields": [
        { "id": 1, "name": "order_id",    "required": true,  "type": "long" },
        { "id": 2, "name": "order_date",  "required": true,  "type": "date" },
        { "id": 3, "name": "customer_id", "required": true,  "type": "long" },
        { "id": 4, "name": "product",     "required": false, "type": "string",
          "doc": "Product name or SKU"
        },
        { "id": 5, "name": "quantity",    "required": false, "type": "int" },
        { "id": 6, "name": "amount",      "required": false, "type": "decimal(12, 2)" },
        {
          // 新增列: coupon_code
          // id=7: 由 last-column-id 递增分配
          // required=false: 新增列必须是 optional，否则旧数据文件无法兼容
          "id": 7, "name": "coupon_code", "required": false, "type": "string",

          // 来源: SchemaParser.INITIAL_DEFAULT = "initial-default"
          // 值含义: 对于已存在的数据行，查询时返回此默认值
          //        旧的 data file（schema-id=0 时写入的）没有这个列
          //        读取时 Iceberg 自动用 initial-default 填充
          "initial-default": null,

          // 来源: SchemaParser.WRITE_DEFAULT = "write-default"
          // 值含义: 写入新数据时，如果不提供该列的值，则用此默认值
          "write-default": "NONE",

          "doc": "Coupon code applied to this order"
        }
      ]
    },

    // ---- Schema 2: 类型提升（T+14h 的 schema 变更）----
    // 场景: 业务数据量增长，quantity 需要更大范围，amount 需要更高精度
    // 源码: SchemaUpdate.updateColumn(name, newType)
    //       → 内部调用 TypeUtil.isPromotionAllowed(field.type(), newType) 验证
    //
    // 允许的提升规则（TypeUtil.java:440-466）:
    //   case INTEGER: return to.typeId() == Type.TypeID.LONG          → int → long ✓
    //   case FLOAT:   return to.typeId() == Type.TypeID.DOUBLE        → float → double ✓
    //   case DECIMAL: fromDecimal.scale() == toDecimal.scale()
    //              && fromDecimal.precision() <= toDecimal.precision() → decimal 精度可扩 ✓
    //
    // 关键行为:
    //   1. 字段 ID 保持不变（SchemaUpdate.updateColumn 只更新 type，不改 fieldId）
    //   2. 旧 data file 中的窄类型数据在读取时由 reader 自动拓宽
    //   3. 不允许缩窄（long→int）或跨类型（string→int）
    {
      "type": "struct",
      "schema-id": 2,
      "identifier-field-ids": [1],
      "fields": [
        { "id": 1, "name": "order_id",    "required": true,  "type": "long" },
        { "id": 2, "name": "order_date",  "required": true,  "type": "date" },
        { "id": 3, "name": "customer_id", "required": true,  "type": "long" },
        { "id": 4, "name": "product",     "required": false, "type": "string",
          "doc": "Product name or SKU"
        },
        {
          // ★ 类型提升: int → long
          // 源码验证: TypeUtil.isPromotionAllowed()
          //   case INTEGER: return to.typeId() == Type.TypeID.LONG  → true ✓
          // 字段 ID 不变: SchemaUpdate.updateColumn() 保持 fieldId=5
          // 序列化: SchemaParser.toJson() 输出 "type": "long"（之前是 "int"）
          "id": 5, "name": "quantity",    "required": false, "type": "long"
        },
        {
          // ★ 类型提升: decimal(12, 2) → decimal(16, 2)
          // 源码验证: TypeUtil.isPromotionAllowed()
          //   case DECIMAL:
          //     fromDecimal.scale() == toDecimal.scale()         → 2 == 2 ✓
          //     fromDecimal.precision() <= toDecimal.precision()  → 12 <= 16 ✓
          // 注意: scale（小数位数）必须相同，只能扩大 precision（总位数）
          //       decimal(12,2) → decimal(12,3) 会被拒绝（scale 不同）
          "id": 6, "name": "amount",      "required": false, "type": "decimal(16, 2)"
        },
        { "id": 7, "name": "coupon_code", "required": false, "type": "string",
          "initial-default": null,
          "write-default": "NONE",
          "doc": "Coupon code applied to this order"
        }
      ]
    },

    // ---- Schema 3: 字段删除（T+16h 的 schema 变更）----
    // 场景: product 列已迁移到独立的产品维度表，从订单表中移除
    // 源码: SchemaUpdate.deleteColumn("product")
    //   → deletes.add(field.fieldId())   // fieldId = 4  (SchemaUpdate.java:199)
    //   → ApplyChanges.field() 中:
    //       if (deletes.contains(fieldId)) { return null; }  (SchemaUpdate.java:661)
    //   → struct 方法中: resultType == null → hasChange = true; continue;
    //   → 该字段从新 schema 的 StructType 中彻底消失
    //
    // 关键行为:
    //   1. 被删除的字段直接从 fields 数组中移除（不是标记删除，是物理消失）
    //   2. 字段 ID=4 永远不会被复用（last-column-id 只增不减）
    //   3. 旧 data file 中仍然物理存储着 product 列的数据
    //      但查询时 Iceberg 使用当前 schema 做投影（projection），不会读取该列
    //   4. 时间旅行到 S1-S4 时，由于 snapshot.schema-id=0/1 中仍有 product，
    //      所以旧快照的查询仍能看到 product 列
    //   5. identifier field 不可直接删除（SchemaUpdate.java:548-551 会校验）
    //      必须先调用 setIdentifierFields() 将其从标识符集合中移除
    {
      "type": "struct",
      "schema-id": 3,
      "identifier-field-ids": [1],
      // ★ 注意: product 列（id=4）已不在下面的 fields 中
      //   id 序列: 1, 2, 3, [4已删], 5, 6, 7 — 编号跳跃是正常的
      "fields": [
        { "id": 1, "name": "order_id",    "required": true,  "type": "long" },
        { "id": 2, "name": "order_date",  "required": true,  "type": "date" },
        { "id": 3, "name": "customer_id", "required": true,  "type": "long" },
        // ← id=4 (product) 已被 deleteColumn 删除
        //   ApplyChanges.field() 对 id=4 返回 null → 不出现在 fields 中
        { "id": 5, "name": "quantity",    "required": false, "type": "long" },
        { "id": 6, "name": "amount",      "required": false, "type": "decimal(16, 2)" },
        { "id": 7, "name": "coupon_code", "required": false, "type": "string",
          "initial-default": null,
          "write-default": "NONE",
          "doc": "Coupon code applied to this order"
        }
      ]
    }
  ],

  // ========================= 分区规格演进 =========================
  // 来源: TableMetadataParser.DEFAULT_SPEC_ID = "default-spec-id"
  // 值含义: 当前生效的分区规格 ID
  //        spec-id=1 表示按 month(order_date) 分区（演进后）
  "default-spec-id": 1,

  // 来源: TableMetadataParser.PARTITION_SPECS = "partition-specs"
  // 值含义: 表历史上所有分区规格（只增不删）
  //        旧 spec 的 data file 仍然用旧 spec 读取
  //        新写入的数据用 default-spec-id 指向的 spec
  "partition-specs": [
    // ---- Spec 0: 按 order_date 原值分区（identity）----
    {
      // 来源: PartitionSpecParser.SPEC_ID = "spec-id"
      "spec-id": 0,
      // 来源: PartitionSpecParser.FIELDS = "fields"
      "fields": [
        {
          // 来源: PartitionSpecParser.NAME = "name" — 分区字段在 manifest 中的名称
          "name": "order_date",
          // 来源: PartitionSpecParser.TRANSFORM = "transform" — 分区转换函数
          //        identity = 直接用原始值分区
          //        其他可选: year/month/day/hour/bucket[N]/truncate[W]
          "transform": "identity",
          // 来源: PartitionSpecParser.SOURCE_ID = "source-id" — 来源列的 field ID
          "source-id": 2,
          // 来源: PartitionSpecParser.FIELD_ID = "field-id" — 分区字段自身的 ID
          //        从 1000 开始递增，由 last-partition-id 跟踪
          "field-id": 1000
        }
      ]
    },

    // ---- Spec 1: 演进为按 month(order_date) 分区 ----
    // 场景: 随着数据量增长，每日分区太碎，改为月分区
    // 注意: 已有的旧 data file 仍属于 spec-id=0，不会被重写
    //       只有新写入的 data file 才使用 spec-id=1
    {
      "spec-id": 1,
      "fields": [
        {
          "name": "order_date_month",
          "transform": "month",
          "source-id": 2,
          // field-id=1001: 新分区字段分配了新的 ID
          "field-id": 1001
        }
      ]
    }
  ],

  // 来源: TableMetadataParser.LAST_PARTITION_ID = "last-partition-id"
  // 值含义: 已分配的最大分区字段 ID
  //        spec-0 用了 1000，spec-1 用了 1001，所以这里是 1001
  "last-partition-id": 1001,

  // ========================= 排序规则演进 =========================
  // 来源: TableMetadataParser.DEFAULT_SORT_ORDER_ID = "default-sort-order-id"
  // 值含义: 当前生效的排序规则 ID
  "default-sort-order-id": 1,

  // 来源: TableMetadataParser.SORT_ORDERS = "sort-orders"
  // 值含义: 表历史上所有排序规则（只增不删）
  "sort-orders": [
    // ---- Order 0: 无排序（unsorted）----
    // SortOrder.unsorted() 固定 order-id=0, fields=[]
    {
      // 来源: SortOrderParser.ORDER_ID = "order-id"
      "order-id": 0,
      // 来源: SortOrderParser.FIELDS = "fields"
      "fields": []
    },

    // ---- Order 1: 按 order_date ASC, order_id ASC 排序 ----
    {
      "order-id": 1,
      "fields": [
        {
          // 来源: SortOrderParser.TRANSFORM = "transform"
          "transform": "identity",
          // 来源: SortOrderParser.SOURCE_ID = "source-id"
          "source-id": 2,
          // 来源: SortOrderParser.DIRECTION = "direction"
          // 可选值: "asc" / "desc"
          "direction": "asc",
          // 来源: SortOrderParser.NULL_ORDER = "null-order"
          // 可选值: "nulls-first" / "nulls-last"
          "null-order": "nulls-last"
        },
        {
          "transform": "identity",
          "source-id": 1,
          "direction": "asc",
          "null-order": "nulls-last"
        }
      ]
    }
  ],

  // ========================= 表属性 =========================
  // 来源: TableMetadataParser.PROPERTIES = "properties"
  // 值含义: key-value 形式的表级配置，部分键由 TableProperties 类定义常量
  "properties": {
    "write.format.default": "parquet",
    "write.target-file-size-bytes": "134217728",
    "write.delete.mode": "merge-on-read",
    "write.parquet.compression-codec": "zstd",
    "commit.retry.num-retries": "4",
    "commit.retry.min-wait-ms": "100",
    "history.expire.max-snapshot-age-ms": "432000000",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "10"
  },

  // ========================= 当前快照指针 =========================
  // 来源: TableMetadataParser.CURRENT_SNAPSHOT_ID = "current-snapshot-id"
  // 值含义: main branch 当前指向的快照 ID
  //        这是整个 metadata 中最关键的"指针"
  //        查询引擎默认读此 snapshot（SnapshotScan.snapshot() 逻辑）
  //        原子性切换此值 = 原子性切换表的可见数据集
  "current-snapshot-id": 9000000000000000009,

  // ========================= 快照引用（Branches & Tags）=========================
  // 来源: TableMetadataParser.REFS = "refs"
  // 值含义: 命名的快照引用，类似 Git 的 branch 和 tag
  "refs": {
    // ---- main 分支（默认，始终存在）----
    "main": {
      // 来源: SnapshotRefParser.SNAPSHOT_ID = "snapshot-id"
      "snapshot-id": 9000000000000000009,
      // 来源: SnapshotRefParser.TYPE = "type"
      // 可选值: "branch"（分支，可前进） / "tag"（标签，固定不变）
      "type": "branch",
      // 来源: SnapshotRefParser.MIN_SNAPSHOTS_TO_KEEP = "min-snapshots-to-keep"
      // 值含义: 该分支最少保留的快照数量（过期清理时不会低于此数）
      "min-snapshots-to-keep": 2,
      // 来源: SnapshotRefParser.MAX_SNAPSHOT_AGE_MS = "max-snapshot-age-ms"
      // 值含义: 快照的最大保留时间（毫秒），超过则可被过期清理
      "max-snapshot-age-ms": 432000000
    },

    // ---- audit-branch: 审计分支，保留更长时间用于合规查询 ----
    // 场景: 数据工程师创建分支来隔离 ETL 写入，不影响 main 的读取
    "audit-branch": {
      "snapshot-id": 5000000000000000005,
      "type": "branch",
      "min-snapshots-to-keep": 5,
      "max-snapshot-age-ms": 2592000000
    },

    // ---- release-v1.0: 发布标签，固定指向某个快照 ----
    // 场景: 标记一个数据质量已验证的快照版本
    "release-v1.0": {
      "snapshot-id": 2000000000000000002,
      "type": "tag",
      // 来源: SnapshotRefParser.MAX_REF_AGE_MS = "max-ref-age-ms"
      // 值含义: tag 的最大存活时间（毫秒），超过则可被清理
      "max-ref-age-ms": 7776000000
    }
  },

  // ========================= 快照列表 =========================
  // 来源: TableMetadataParser.SNAPSHOTS = "snapshots"
  // 值含义: 表历史上所有未过期的快照
  //        每个快照是一个不可变的文件集合视图
  //        通过 manifest-list 指向该快照的完整文件清单
  "snapshots": [

    // ---- Snapshot 1: 初始数据写入（append）----
    {
      // 来源: SnapshotParser.SEQUENCE_NUMBER = "sequence-number"
      // 值含义: 该 snapshot 的全局顺序号（V2+），用于 delete file 覆盖语义
      //        规则: delete file 只对 data_sequence_number ≤ delete.sequence_number 的数据生效
      //        注: 仅当 > INITIAL_SEQUENCE_NUMBER(0) 时才序列化
      "sequence-number": 1,

      // 来源: SnapshotParser.SNAPSHOT_ID = "snapshot-id"
      // 值含义: 快照的唯一 ID（全局唯一的 long 值）
      "snapshot-id": 1000000000000000001,

      // 注意: 第一个快照没有 parent-snapshot-id（SnapshotParser 条件: node.has(PARENT_SNAPSHOT_ID)）

      // 来源: SnapshotParser.TIMESTAMP_MS = "timestamp-ms"
      "timestamp-ms": 1713571200000,

      // 来源: SnapshotParser.SUMMARY = "summary"
      // 内部字段来源: SnapshotSummary 类中的常量
      "summary": {
        // 来源: DataOperations.APPEND = "append"
        // 可选值: "append" / "replace" / "overwrite" / "delete"
        // append: 纯追加，由 AppendFiles 操作产生
        "operation": "append",
        // 来源: SnapshotSummary.ADDED_FILES_PROP = "added-data-files"
        "added-data-files": "3",
        // 来源: SnapshotSummary.ADDED_RECORDS_PROP = "added-records"
        "added-records": "1500000",
        // 来源: SnapshotSummary.ADDED_FILE_SIZE_PROP = "added-files-size"
        "added-files-size": "402653184",
        // 来源: SnapshotSummary.TOTAL_DATA_FILES_PROP = "total-data-files"
        "total-data-files": "3",
        // 来源: SnapshotSummary.TOTAL_RECORDS_PROP = "total-records"
        "total-records": "1500000",
        // 来源: SnapshotSummary.TOTAL_FILE_SIZE_PROP = "total-files-size"
        "total-files-size": "402653184",
        // 来源: SnapshotSummary.TOTAL_DELETE_FILES_PROP = "total-delete-files"
        "total-delete-files": "0",
        // 来源: SnapshotSummary.TOTAL_POS_DELETES_PROP = "total-position-deletes"
        "total-position-deletes": "0",
        // 来源: SnapshotSummary.TOTAL_EQ_DELETES_PROP = "total-equality-deletes"
        "total-equality-deletes": "0",
        // 来源: SnapshotSummary.CHANGED_PARTITION_COUNT_PROP = "changed-partition-count"
        "changed-partition-count": "3"
      },

      // 来源: SnapshotParser.MANIFEST_LIST = "manifest-list"
      // 值含义: 指向该 snapshot 的 manifest list 文件（Avro 格式）
      //        manifest list 中每一行描述一个 manifest file
      //        manifest file 中每一行描述一个 data file 或 delete file
      "manifest-list": "s3://my-datalake/warehouse/orders/metadata/snap-1000000000000000001-1-uuid-a.avro",

      // 来源: SnapshotParser.SCHEMA_ID = "schema-id"
      // 值含义: 该快照使用的 schema 版本
      //        时间旅行时，Iceberg 用此 ID 找到正确的 schema 来解析文件
      "schema-id": 0
    },

    // ---- Snapshot 2: 追加更多数据（append）----
    {
      "sequence-number": 2,
      "snapshot-id": 2000000000000000002,
      // 来源: SnapshotParser.PARENT_SNAPSHOT_ID = "parent-snapshot-id"
      // 值含义: 父快照 ID，形成快照链（类似 Git 的 parent commit）
      "parent-snapshot-id": 1000000000000000001,
      "timestamp-ms": 1713578400000,
      "summary": {
        "operation": "append",
        "added-data-files": "2",
        "added-records": "800000",
        "added-files-size": "214748364",
        "total-data-files": "5",
        "total-records": "2300000",
        "total-files-size": "617401548",
        "total-delete-files": "0",
        "total-position-deletes": "0",
        "total-equality-deletes": "0",
        "changed-partition-count": "2"
      },
      "manifest-list": "s3://my-datalake/warehouse/orders/metadata/snap-2000000000000000002-1-uuid-b.avro",
      "schema-id": 0
    },

    // ---- Snapshot 3: Schema 变更（仅元数据更新，无数据操作）----
    // 注意: addColumn 等 schema 变更是通过 UpdateSchema 提交的
    //       它会产生一个新的 metadata.json，但不一定产生新的 snapshot
    //       这里为了呈现完整链路，假设该变更和一次空操作合并提交

    // ---- Snapshot 4: MOR 删除（overwrite）----
    // 场景: 用户发起 DELETE FROM orders WHERE customer_id = 12345
    // MOR 模式下，Iceberg 不修改原数据文件，而是写入 delete files
    {
      "sequence-number": 4,
      "snapshot-id": 4000000000000000004,
      "parent-snapshot-id": 2000000000000000002,
      "timestamp-ms": 1713592800000,
      "summary": {
        // 来源: DataOperations.OVERWRITE = "overwrite"
        // overwrite: 新数据覆盖旧数据，由 OverwriteFiles / ReplacePartitions 产生
        //           这里是 MOR delete，通过 RowDelta 操作添加 delete files
        "operation": "overwrite",
        // 来源: SnapshotSummary.ADDED_DELETE_FILES_PROP = "added-delete-files"
        "added-delete-files": "3",
        // 来源: SnapshotSummary.ADD_EQ_DELETE_FILES_PROP = "added-equality-delete-files"
        // 值含义: 本次新增的 equality delete 文件数量
        //        equality delete = 按列值匹配删除（如 WHERE customer_id = 12345）
        "added-equality-delete-files": "1",
        // 来源: SnapshotSummary.ADD_POS_DELETE_FILES_PROP = "added-position-delete-files"
        // 值含义: 本次新增的 position delete 文件数量
        //        position delete = 按 (file_path, row_position) 精确定位删除
        "added-position-delete-files": "2",
        // 来源: SnapshotSummary.ADDED_EQ_DELETES_PROP = "added-equality-deletes"
        "added-equality-deletes": "5000",
        // 来源: SnapshotSummary.ADDED_POS_DELETES_PROP = "added-position-deletes"
        "added-position-deletes": "12000",
        "total-data-files": "5",
        "total-delete-files": "3",
        "total-records": "2300000",
        "total-position-deletes": "12000",
        "total-equality-deletes": "5000",
        "changed-partition-count": "3"
      },
      "manifest-list": "s3://my-datalake/warehouse/orders/metadata/snap-4000000000000000004-1-uuid-d.avro",
      // schema-id=1: 此快照已关联新 schema（含 coupon_code 列）
      "schema-id": 1
    },

    // ---- Snapshot 5: 纯删除过期数据文件（delete）----
    // 场景: 删除 2024-01-01 之前的全部数据
    {
      "sequence-number": 5,
      "snapshot-id": 5000000000000000005,
      "parent-snapshot-id": 4000000000000000004,
      "timestamp-ms": 1713600000000,
      "summary": {
        // 来源: DataOperations.DELETE = "delete"
        // delete: 纯删除操作，由 DeleteFiles 产生，不新增数据
        "operation": "delete",
        // 来源: SnapshotSummary.DELETED_FILES_PROP = "deleted-data-files"
        "deleted-data-files": "1",
        // 来源: SnapshotSummary.DELETED_RECORDS_PROP = "deleted-records"
        "deleted-records": "500000",
        // 来源: SnapshotSummary.REMOVED_FILE_SIZE_PROP = "removed-files-size"
        "removed-files-size": "134217728",
        // 来源: SnapshotSummary.REMOVED_DELETE_FILES_PROP = "removed-delete-files"
        // 值含义: 伴随删除的 data file 一同移除的 delete files
        //        因为被删除的 data file 关联的 position delete 也不再需要了
        "removed-delete-files": "1",
        // 来源: SnapshotSummary.REMOVED_POS_DELETE_FILES_PROP = "removed-position-delete-files"
        "removed-position-delete-files": "1",
        // 来源: SnapshotSummary.REMOVED_POS_DELETES_PROP = "removed-position-deletes"
        "removed-position-deletes": "4000",
        "total-data-files": "4",
        "total-delete-files": "2",
        "total-records": "1800000",
        "total-position-deletes": "8000",
        "total-equality-deletes": "5000",
        "changed-partition-count": "1"
      },
      "manifest-list": "s3://my-datalake/warehouse/orders/metadata/snap-5000000000000000005-1-uuid-e.avro",
      "schema-id": 1
    },

    // ---- Snapshot 6: 小文件合并 / Compaction（replace）----
    // 场景: RewriteDataFiles 将 4 个小文件合并为 2 个大文件
    //       同时消化并移除所有已生效的 delete files
    {
      "sequence-number": 6,
      "snapshot-id": 6000000000000000006,
      "parent-snapshot-id": 5000000000000000005,
      "timestamp-ms": 1713607200000,
      "summary": {
        // 来源: DataOperations.REPLACE = "replace"
        // replace: 文件替换操作，由 RewriteFiles 产生
        //         语义: 数据内容不变，只是物理文件被替换
        //         commit 时通过 validateNoNewDeletesForDataFiles 检测并发冲突
        "operation": "replace",
        "added-data-files": "2",
        "deleted-data-files": "4",
        "added-records": "1783000",
        "deleted-records": "1800000",
        "added-files-size": "268435456",
        "removed-files-size": "483183820",
        // 所有 delete files 在 compaction 中被消化和移除
        "removed-delete-files": "2",
        "removed-position-delete-files": "1",
        "removed-equality-delete-files": "1",
        "removed-position-deletes": "8000",
        "removed-equality-deletes": "5000",
        "total-data-files": "2",
        "total-delete-files": "0",
        "total-records": "1783000",
        "total-position-deletes": "0",
        "total-equality-deletes": "0",
        "changed-partition-count": "3",
        // 来源: SnapshotSummary.CREATED_MANIFESTS_COUNT = "manifests-created"
        "manifests-created": "1",
        // 来源: SnapshotSummary.REPLACED_MANIFESTS_COUNT = "manifests-replaced"
        "manifests-replaced": "3",
        // 来源: SnapshotSummary.KEPT_MANIFESTS_COUNT = "manifests-kept"
        "manifests-kept": "0"
        // 注意: 新文件的 data_sequence_number 被设为 startingSnapshot.sequenceNumber()
        //       而不是本次 commit 的 sequence-number=6
        //       这是 USE_STARTING_SEQUENCE_NUMBER=true 的效果
        //       源码路径: BaseRewriteDataFilesAction.doReplace()
        //         → rewriteFiles.dataSequenceNumber(sequenceNumber)
        //         → MergingSnapshotProducer.setNewDataFilesDataSequenceNumber()
        //         → ManifestWriter.add(file, dataSequenceNumber)
      },
      "manifest-list": "s3://my-datalake/warehouse/orders/metadata/snap-6000000000000000006-1-uuid-f.avro",
      "schema-id": 1
    },

    // ---- Snapshot 7: 按新分区规则追加数据（append + 新 partition spec）----
    // 场景: 分区从 identity(order_date) 演进为 month(order_date)
    //       新写入的数据使用 spec-id=1，旧数据保持 spec-id=0
    {
      "sequence-number": 7,
      "snapshot-id": 7000000000000000007,
      "parent-snapshot-id": 6000000000000000006,
      "timestamp-ms": 1713614400000,
      "summary": {
        "operation": "append",
        "added-data-files": "1",
        "added-records": "200000",
        "added-files-size": "67108864",
        "total-data-files": "3",
        "total-records": "1983000",
        "total-files-size": "335544320",
        "total-delete-files": "0",
        "total-position-deletes": "0",
        "total-equality-deletes": "0",
        "changed-partition-count": "1"
      },
      "manifest-list": "s3://my-datalake/warehouse/orders/metadata/snap-7000000000000000007-1-uuid-g.avro",
      "schema-id": 1
    },

    // ---- Snapshot 8: 类型提升后追加数据（append + schema=2）----
    // 场景: Schema 演进为 schema-id=2 后，首次数据写入
    //       quantity 现在是 long 类型，amount 是 decimal(16,2)
    //       读取旧 data file（schema-id=0/1）时的行为:
    //         reader 遇到 int 类型的 quantity → 自动拓宽为 long
    //         reader 遇到 decimal(12,2) 的 amount → 自动填充到 decimal(16,2)
    //       这是 Parquet/ORC reader 的内置能力，无需重写旧文件
    {
      "sequence-number": 8,
      "snapshot-id": 8000000000000000008,
      "parent-snapshot-id": 7000000000000000007,
      "timestamp-ms": 1713621600000,
      "summary": {
        "operation": "append",
        "added-data-files": "2",
        "added-records": "350000",
        "added-files-size": "94371840",
        "total-data-files": "5",
        "total-records": "2333000",
        "total-files-size": "429916160",
        "total-delete-files": "0",
        "total-position-deletes": "0",
        "total-equality-deletes": "0",
        "changed-partition-count": "2"
      },
      "manifest-list": "s3://my-datalake/warehouse/orders/metadata/snap-8000000000000000008-1-uuid-h.avro",
      // schema-id=2: 使用类型提升后的 schema
      // 此快照新写入的 data file 中 quantity 物理类型为 long（不再是 int）
      "schema-id": 2
    },

    // ---- Snapshot 9: 删列后追加数据（append + schema=3）----
    // 场景: Schema 演进为 schema-id=3 后，product 列已被移除
    //       新写入的 data file 中不包含 product 列
    //       旧 data file 虽然物理上还有 product 列，但查询时不会投影该列:
    //         ManifestGroup 扫描时，使用 current schema 的 field IDs 做投影
    //         id=4 不在 schema-3 中 → 不请求该列 → reader 跳过
    //       时间旅行到 S1-S4（snapshot.schema-id=0/1）的查询仍能看到 product:
    //         因为 snapshot.schema-id 指向旧 schema，旧 schema 中还有 id=4
    {
      "sequence-number": 9,
      "snapshot-id": 9000000000000000009,
      "parent-snapshot-id": 8000000000000000008,
      "timestamp-ms": 1713628800000,
      "summary": {
        "operation": "append",
        "added-data-files": "1",
        "added-records": "150000",
        "added-files-size": "41943040",
        "total-data-files": "6",
        "total-records": "2483000",
        "total-files-size": "471859200",
        "total-delete-files": "0",
        "total-position-deletes": "0",
        "total-equality-deletes": "0",
        "changed-partition-count": "1"
      },
      "manifest-list": "s3://my-datalake/warehouse/orders/metadata/snap-9000000000000000009-1-uuid-i.avro",
      // schema-id=3: 使用删除了 product 列后的 schema
      // 此快照新写入的 data file 中没有 product 列
      "schema-id": 3
    }
  ],

  // ========================= 统计信息文件 =========================
  // 来源: TableMetadataParser.STATISTICS = "statistics"
  // 值含义: Puffin 格式的列统计文件（如 NDV sketches）
  //        可选，用于查询优化（CBO）
  "statistics": [
    {
      "snapshot-id": 7000000000000000007,
      "statistics-path": "s3://my-datalake/warehouse/orders/metadata/stats-7000000000000000007.puffin",
      "file-size-in-bytes": 8192,
      "file-footer-size-in-bytes": 1024,
      "blob-metadata": [
        {
          "type": "apache-datasketches-theta-v1",
          "snapshot-id": 7000000000000000007,
          "sequence-number": 7,
          "fields": [1],
          "properties": {
            "ndv": "1983000"
          }
        },
        {
          "type": "apache-datasketches-theta-v1",
          "snapshot-id": 7000000000000000007,
          "sequence-number": 7,
          "fields": [3],
          "properties": {
            "ndv": "450000"
          }
        }
      ]
    }
  ],

  // 来源: TableMetadataParser.PARTITION_STATISTICS = "partition-statistics"
  // 值含义: 分区级别的统计信息文件
  "partition-statistics": [
    {
      "snapshot-id": 7000000000000000007,
      "statistics-path": "s3://my-datalake/warehouse/orders/metadata/partition-stats-7000000000000000007.parquet",
      "file-size-in-bytes": 4096
    }
  ],

  // ========================= 快照历史日志 =========================
  // 来源: TableMetadataParser.SNAPSHOT_LOG = "snapshot-log"
  // 值含义: main branch 的快照变更历史（仅记录时间戳和 snapshot ID）
  //        类似 Git reflog，记录 HEAD 的每次移动
  // 注意: 这里只有 snapshot-id 和 timestamp-ms 两个字段
  "snapshot-log": [
    { "timestamp-ms": 1713571200000, "snapshot-id": 1000000000000000001 },
    { "timestamp-ms": 1713578400000, "snapshot-id": 2000000000000000002 },
    { "timestamp-ms": 1713592800000, "snapshot-id": 4000000000000000004 },
    { "timestamp-ms": 1713600000000, "snapshot-id": 5000000000000000005 },
    { "timestamp-ms": 1713607200000, "snapshot-id": 6000000000000000006 },
    { "timestamp-ms": 1713614400000, "snapshot-id": 7000000000000000007 },
    { "timestamp-ms": 1713621600000, "snapshot-id": 8000000000000000008 },
    { "timestamp-ms": 1713628800000, "snapshot-id": 9000000000000000009 }
  ],

  // ========================= 元数据文件历史 =========================
  // 来源: TableMetadataParser.METADATA_LOG = "metadata-log"
  // 值含义: 该表历史上生成的 metadata.json 文件路径
  //        每次 schema 变更、属性变更、snapshot commit 都会生成一个新的 metadata.json
  //        旧的 metadata.json 保留在此日志中，用于审计和恢复
  //        保留数量受 write.metadata.previous-versions-max 控制
  "metadata-log": [
    {
      // 来源: TableMetadataParser.TIMESTAMP_MS = "timestamp-ms"
      "timestamp-ms": 1713571200000,
      // 来源: TableMetadataParser.METADATA_FILE = "metadata-file"
      "metadata-file": "s3://my-datalake/warehouse/orders/metadata/00000-uuid-a.metadata.json"
    },
    {
      "timestamp-ms": 1713578400000,
      "metadata-file": "s3://my-datalake/warehouse/orders/metadata/00001-uuid-b.metadata.json"
    },
    {
      "timestamp-ms": 1713585600000,
      "metadata-file": "s3://my-datalake/warehouse/orders/metadata/00002-uuid-c.metadata.json"
    },
    {
      "timestamp-ms": 1713592800000,
      "metadata-file": "s3://my-datalake/warehouse/orders/metadata/00003-uuid-d.metadata.json"
    },
    {
      "timestamp-ms": 1713600000000,
      "metadata-file": "s3://my-datalake/warehouse/orders/metadata/00004-uuid-e.metadata.json"
    },
    {
      "timestamp-ms": 1713607200000,
      "metadata-file": "s3://my-datalake/warehouse/orders/metadata/00005-uuid-f.metadata.json"
    },
    {
      "timestamp-ms": 1713614400000,
      "metadata-file": "s3://my-datalake/warehouse/orders/metadata/00006-uuid-g.metadata.json"
    },
    {
      // T+14h: schema 类型提升 → 新 metadata.json（无 snapshot，仅元数据变更）
      "timestamp-ms": 1713621600000,
      "metadata-file": "s3://my-datalake/warehouse/orders/metadata/00007-uuid-h.metadata.json"
    },
    {
      // T+16h: schema 删列 + append → 新 metadata.json
      "timestamp-ms": 1713628800000,
      "metadata-file": "s3://my-datalake/warehouse/orders/metadata/00008-uuid-i.metadata.json"
    }
  ]
}
```

---

## 字段来源速查表

所有 JSON key 与源码常量的对应关系：

### TableMetadataParser.java

| JSON Key | 源码常量 | 行号 |
|----------|---------|------|
| `format-version` | `FORMAT_VERSION` | 88 |
| `table-uuid` | `TABLE_UUID` | 89 |
| `location` | `LOCATION` | 90 |
| `last-sequence-number` | `LAST_SEQUENCE_NUMBER` | 91 |
| `last-updated-ms` | `LAST_UPDATED_MILLIS` | 92 |
| `last-column-id` | `LAST_COLUMN_ID` | 93 |
| `current-schema-id` | `CURRENT_SCHEMA_ID` | 96 |
| `schemas` | `SCHEMAS` | 95 |
| `default-spec-id` | `DEFAULT_SPEC_ID` | 99 |
| `partition-specs` | `PARTITION_SPECS` | 98 |
| `last-partition-id` | `LAST_PARTITION_ID` | 100 |
| `default-sort-order-id` | `DEFAULT_SORT_ORDER_ID` | 101 |
| `sort-orders` | `SORT_ORDERS` | 102 |
| `properties` | `PROPERTIES` | 103 |
| `current-snapshot-id` | `CURRENT_SNAPSHOT_ID` | 104 |
| `refs` | `REFS` | 105 |
| `snapshots` | `SNAPSHOTS` | 106 |
| `statistics` | `STATISTICS` | 112 |
| `partition-statistics` | `PARTITION_STATISTICS` | 113 |
| `snapshot-log` | `SNAPSHOT_LOG` | 109 |
| `metadata-log` | `METADATA_LOG` | 111 |

### SchemaParser.java

| JSON Key | 源码常量 | 行号 |
|----------|---------|------|
| `schema-id` | `SCHEMA_ID` | 41 |
| `identifier-field-ids` | `IDENTIFIER_FIELD_IDS` | 42 |
| `type` | `TYPE` | 43 |
| `fields` | `FIELDS` | 47 |
| `id` | `ID` | 53 |
| `name` | `NAME` | 52 |
| `required` | `REQUIRED` | 59 |
| `doc` | `DOC` | 51 |
| `initial-default` | `INITIAL_DEFAULT` | 54 |
| `write-default` | `WRITE_DEFAULT` | 55 |

### TypeUtil.java — 类型提升规则

> 源码位置: [TypeUtil.isPromotionAllowed()](file:///Users/wanghaofeng/IdeaProjects/iceberg/api/src/main/java/org/apache/iceberg/types/TypeUtil.java#L440-L466)

| 源类型 | 目标类型 | 源码条件 |
|--------|---------|----------|
| `int` | `long` | `case INTEGER: return to.typeId() == Type.TypeID.LONG` |
| `float` | `double` | `case FLOAT: return to.typeId() == Type.TypeID.DOUBLE` |
| `decimal(p1, s)` | `decimal(p2, s)` | `fromDecimal.scale() == toDecimal.scale() && fromDecimal.precision() <= toDecimal.precision()` |

> [!WARNING]
> 以下提升**不被允许**（`isPromotionAllowed` 返回 `false`）:
> - 缩窄型：`long→int`、`double→float`
> - 跨类型：`string→int`、`int→decimal`
> - Scale 不同的 decimal：`decimal(12,2)→decimal(12,3)`

### SchemaUpdate.java — 字段删除行为

> 源码位置: [SchemaUpdate.deleteColumn()](file:///Users/wanghaofeng/IdeaProjects/iceberg/core/src/main/java/org/apache/iceberg/SchemaUpdate.java#L190-L201) →
> [ApplyChanges.field()](file:///Users/wanghaofeng/IdeaProjects/iceberg/core/src/main/java/org/apache/iceberg/SchemaUpdate.java#L657-L663)

| 行为 | 源码证据 | 说明 |
|------|---------|------|
| 字段从 fields 数组中消失 | `if (deletes.contains(fieldId)) return null` (L661) | 返回 null 后 struct 方法跳过该字段 |
| 字段 ID 永不复用 | `last-column-id` 只增不减 | 被删的 id=4 永远不会分配给新列 |
| 旧数据文件不受影响 | 物理文件仍含该列 | 查询时新 schema 做投影（projection）不读取 |
| identifier field 受保护 | `!deletes.contains(field.fieldId())` (L549) | 不可直接删除标识符字段 |
| 删除前禁止并发操作 | `!parentToAddedIds.containsKey` (L194) | 不能对有 pending adds/updates 的列执行删除 |

### SnapshotParser.java

| JSON Key | 源码常量 | 行号 |
|----------|---------|------|
| `sequence-number` | `SEQUENCE_NUMBER` | 45 |
| `snapshot-id` | `SNAPSHOT_ID` | 46 |
| `parent-snapshot-id` | `PARENT_SNAPSHOT_ID` | 47 |
| `timestamp-ms` | `TIMESTAMP_MS` | 48 |
| `summary` | `SUMMARY` | 49 |
| `manifest-list` | `MANIFEST_LIST` | 52 |
| `schema-id` | `SCHEMA_ID` | 53 |

### SnapshotSummary.java

| JSON Key | 源码常量 | 行号 |
|----------|---------|------|
| `added-data-files` | `ADDED_FILES_PROP` | 32 |
| `deleted-data-files` | `DELETED_FILES_PROP` | 33 |
| `total-data-files` | `TOTAL_DATA_FILES_PROP` | 34 |
| `added-delete-files` | `ADDED_DELETE_FILES_PROP` | 35 |
| `added-equality-delete-files` | `ADD_EQ_DELETE_FILES_PROP` | 36 |
| `removed-equality-delete-files` | `REMOVED_EQ_DELETE_FILES_PROP` | 37 |
| `added-position-delete-files` | `ADD_POS_DELETE_FILES_PROP` | 38 |
| `removed-position-delete-files` | `REMOVED_POS_DELETE_FILES_PROP` | 39 |
| `added-dvs` | `ADDED_DVS_PROP` | 40 |
| `removed-dvs` | `REMOVED_DVS_PROP` | 41 |
| `removed-delete-files` | `REMOVED_DELETE_FILES_PROP` | 42 |
| `total-delete-files` | `TOTAL_DELETE_FILES_PROP` | 43 |
| `added-records` | `ADDED_RECORDS_PROP` | 44 |
| `deleted-records` | `DELETED_RECORDS_PROP` | 45 |
| `total-records` | `TOTAL_RECORDS_PROP` | 46 |
| `added-files-size` | `ADDED_FILE_SIZE_PROP` | 47 |
| `removed-files-size` | `REMOVED_FILE_SIZE_PROP` | 48 |
| `total-files-size` | `TOTAL_FILE_SIZE_PROP` | 49 |
| `added-position-deletes` | `ADDED_POS_DELETES_PROP` | 50 |
| `removed-position-deletes` | `REMOVED_POS_DELETES_PROP` | 51 |
| `total-position-deletes` | `TOTAL_POS_DELETES_PROP` | 52 |
| `added-equality-deletes` | `ADDED_EQ_DELETES_PROP` | 53 |
| `removed-equality-deletes` | `REMOVED_EQ_DELETES_PROP` | 54 |
| `total-equality-deletes` | `TOTAL_EQ_DELETES_PROP` | 55 |
| `changed-partition-count` | `CHANGED_PARTITION_COUNT_PROP` | 57 |
| `manifests-created` | `CREATED_MANIFESTS_COUNT` | 65 |
| `manifests-replaced` | `REPLACED_MANIFESTS_COUNT` | 66 |
| `manifests-kept` | `KEPT_MANIFESTS_COUNT` | 67 |

### DataOperations.java

| 操作值 | 源码常量 | 描述 |
|--------|---------|------|
| `"append"` | `APPEND` | 纯追加，由 `AppendFiles` 产生 |
| `"replace"` | `REPLACE` | 文件替换不改数据，由 `RewriteFiles` 产生 |
| `"overwrite"` | `OVERWRITE` | 覆盖写，由 `OverwriteFiles`/`ReplacePartitions` 产生 |
| `"delete"` | `DELETE` | 纯删除，由 `DeleteFiles` 产生 |

### PartitionSpecParser.java / SortOrderParser.java / SnapshotRefParser.java

| JSON Key | Parser 类 | 源码常量 |
|----------|----------|---------|
| `spec-id` | PartitionSpecParser | `SPEC_ID` |
| `source-id` | PartitionSpecParser | `SOURCE_ID` |
| `field-id` | PartitionSpecParser | `FIELD_ID` |
| `transform` | PartitionSpecParser | `TRANSFORM` |
| `order-id` | SortOrderParser | `ORDER_ID` |
| `direction` | SortOrderParser | `DIRECTION` |
| `null-order` | SortOrderParser | `NULL_ORDER` |
| `snapshot-id` (ref) | SnapshotRefParser | `SNAPSHOT_ID` |
| `type` (ref) | SnapshotRefParser | `TYPE` |
| `min-snapshots-to-keep` | SnapshotRefParser | `MIN_SNAPSHOTS_TO_KEEP` |
| `max-snapshot-age-ms` | SnapshotRefParser | `MAX_SNAPSHOT_AGE_MS` |
| `max-ref-age-ms` | SnapshotRefParser | `MAX_REF_AGE_MS` |

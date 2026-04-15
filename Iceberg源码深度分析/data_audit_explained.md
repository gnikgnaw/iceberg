# 数据审计（Data Audit）详解

## 什么是数据审计？

**数据审计（Data Audit）** 是指记录和追踪数据的所有变更历史，以便：
1. **证明数据变更的合法性**：谁在什么时候修改了什么数据
2. **追溯问题根源**：当数据出错时，能找到是哪一步出了问题
3. **满足合规要求**：金融、医疗等行业的法律法规要求

## 实际业务场景

### 场景 1: 金融交易审计

#### 业务需求

```sql
-- 银行账户表
CREATE TABLE accounts (
    account_id BIGINT PRIMARY KEY,
    balance DECIMAL(18,2),
    status VARCHAR(20),
    last_updated TIMESTAMP
);

-- 业务操作
T1: INSERT INTO accounts VALUES (1001, 10000.00, 'active', '2024-01-01 10:00:00');
T2: UPDATE accounts SET balance=9500.00 WHERE account_id=1001;  -- 取款 500
T3: UPDATE accounts SET balance=9800.00 WHERE account_id=1001;  -- 存款 300
T4: UPDATE accounts SET status='frozen' WHERE account_id=1001;  -- 冻结账户
```

#### 审计要求

监管机构要求银行能够回答：
1. **账户余额如何变化的？**
   - 初始余额：10000.00
   - 第一次变更：9500.00（减少 500）
   - 第二次变更：9800.00（增加 300）

2. **谁修改了账户状态？**
   - 什么时候从 'active' 变成 'frozen'？
   - 为什么冻结？

3. **如果余额不对，是哪一步出错的？**
   - 需要追溯每一次变更

#### upsert=false 的审计能力

```java
// Iceberg 存储的完整历史

// Data Files（数据文件）
File 1 (T1): {account_id:1001, balance:10000.00, status:'active', ts:'2024-01-01 10:00:00'}
File 2 (T2): {account_id:1001, balance:9500.00, status:'active', ts:'2024-01-01 10:05:00'}
File 3 (T3): {account_id:1001, balance:9800.00, status:'active', ts:'2024-01-01 10:10:00'}
File 4 (T4): {account_id:1001, balance:9800.00, status:'frozen', ts:'2024-01-01 10:15:00'}

// Delete Files（删除文件，记录旧值）
Delete 1 (T2): {account_id:1001, balance:10000.00, status:'active', ts:'2024-01-01 10:00:00'}
Delete 2 (T3): {account_id:1001, balance:9500.00, status:'active', ts:'2024-01-01 10:05:00'}
Delete 3 (T4): {account_id:1001, balance:9800.00, status:'active', ts:'2024-01-01 10:10:00'}
```

**审计查询**：

```sql
-- 查询账户的完整变更历史
SELECT 
    account_id,
    balance,
    status,
    ts,
    'DELETED' as action,
    snapshot_id
FROM iceberg.accounts.delete_files
WHERE account_id = 1001

UNION ALL

SELECT 
    account_id,
    balance,
    status,
    ts,
    'INSERTED' as action,
    snapshot_id
FROM iceberg.accounts.data_files
WHERE account_id = 1001

ORDER BY ts;

-- 结果：
-- | account_id | balance  | status | ts                  | action   | snapshot_id |
-- |------------|----------|--------|---------------------|----------|-------------|
-- | 1001       | 10000.00 | active | 2024-01-01 10:00:00 | INSERTED | 1           |
-- | 1001       | 10000.00 | active | 2024-01-01 10:00:00 | DELETED  | 2           |
-- | 1001       | 9500.00  | active | 2024-01-01 10:05:00 | INSERTED | 2           |
-- | 1001       | 9500.00  | active | 2024-01-01 10:05:00 | DELETED  | 3           |
-- | 1001       | 9800.00  | active | 2024-01-01 10:10:00 | INSERTED | 3           |
-- | 1001       | 9800.00  | active | 2024-01-01 10:10:00 | DELETED  | 4           |
-- | 1001       | 9800.00  | frozen | 2024-01-01 10:15:00 | INSERTED | 4           |

-- 可以清楚地看到：
-- 1. 余额从 10000 → 9500 → 9800
-- 2. 状态从 active → frozen
-- 3. 每次变更的时间和快照 ID
```

#### upsert=true 的审计能力

```java
// Iceberg 存储（丢失了旧值信息）

// Data Files
File 1 (T1): {account_id:1001, balance:10000.00, status:'active', ts:'2024-01-01 10:00:00'}
File 2 (T2): {account_id:1001, balance:9500.00, status:'active', ts:'2024-01-01 10:05:00'}
File 3 (T3): {account_id:1001, balance:9800.00, status:'active', ts:'2024-01-01 10:10:00'}
File 4 (T4): {account_id:1001, balance:9800.00, status:'frozen', ts:'2024-01-01 10:15:00'}

// Delete Files（只有 key，没有旧值）
Delete 1 (T2): {account_id:1001}  ← 不知道删除的是什么
Delete 2 (T3): {account_id:1001}  ← 不知道删除的是什么
Delete 3 (T4): {account_id:1001}  ← 不知道删除的是什么
```

**审计查询**：

```sql
-- 尝试查询变更历史
SELECT 
    account_id,
    balance,
    status,
    ts
FROM iceberg.accounts.data_files
WHERE account_id = 1001
ORDER BY ts;

-- 结果：
-- | account_id | balance  | status | ts                  |
-- |------------|----------|--------|---------------------|
-- | 1001       | 10000.00 | active | 2024-01-01 10:00:00 |
-- | 1001       | 9500.00  | active | 2024-01-01 10:05:00 |
-- | 1001       | 9800.00  | active | 2024-01-01 10:10:00 |
-- | 1001       | 9800.00  | frozen | 2024-01-01 10:15:00 |

-- 问题：
-- 1. 看不到哪些记录被删除了
-- 2. 不知道余额是如何变化的（10000 → 9500 是取款还是扣费？）
-- 3. 无法证明数据变更的合法性
```

---

### 场景 2: 电商订单状态审计

#### 业务需求

```sql
-- 订单表
CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT,
    status VARCHAR(20),
    amount DECIMAL(10,2),
    updated_at TIMESTAMP
);

-- 业务操作
T1: INSERT INTO orders VALUES (2001, 1001, 'pending', 299.00, '2024-01-01 10:00:00');
T2: UPDATE orders SET status='paid' WHERE order_id=2001;
T3: UPDATE orders SET status='shipped' WHERE order_id=2001;
T4: UPDATE orders SET status='delivered' WHERE order_id=2001;
T5: UPDATE orders SET status='refunded', amount=0 WHERE order_id=2001;  -- 退款
```

#### 审计场景

**客户投诉**："我的订单明明已经发货了，为什么显示退款？"

**客服需要查询**：
1. 订单状态变更历史
2. 什么时候从 'shipped' 变成 'refunded'？
3. 是谁操作的退款？

#### upsert=false 的审计查询

```sql
-- 查询订单状态变更历史
WITH history AS (
    SELECT 
        order_id,
        status,
        amount,
        updated_at,
        'DELETED' as action,
        snapshot_id
    FROM iceberg.orders.delete_files
    WHERE order_id = 2001
    
    UNION ALL
    
    SELECT 
        order_id,
        status,
        amount,
        updated_at,
        'INSERTED' as action,
        snapshot_id
    FROM iceberg.orders.data_files
    WHERE order_id = 2001
)
SELECT 
    order_id,
    status,
    amount,
    updated_at,
    action,
    snapshot_id,
    LAG(status) OVER (ORDER BY updated_at) as previous_status
FROM history
ORDER BY updated_at;

-- 结果：
-- | order_id | status    | amount | updated_at          | action   | snapshot_id | previous_status |
-- |----------|-----------|--------|---------------------|----------|-------------|-----------------|
-- | 2001     | pending   | 299.00 | 2024-01-01 10:00:00 | INSERTED | 1           | NULL            |
-- | 2001     | pending   | 299.00 | 2024-01-01 10:00:00 | DELETED  | 2           | pending         |
-- | 2001     | paid      | 299.00 | 2024-01-01 10:05:00 | INSERTED | 2           | pending         |
-- | 2001     | paid      | 299.00 | 2024-01-01 10:05:00 | DELETED  | 3           | paid            |
-- | 2001     | shipped   | 299.00 | 2024-01-01 10:10:00 | INSERTED | 3           | paid            |
-- | 2001     | shipped   | 299.00 | 2024-01-01 10:10:00 | DELETED  | 4           | shipped         |
-- | 2001     | delivered | 299.00 | 2024-01-01 10:15:00 | INSERTED | 4           | shipped         |
-- | 2001     | delivered | 299.00 | 2024-01-01 10:15:00 | DELETED  | 5           | delivered       |
-- | 2001     | refunded  | 0.00   | 2024-01-01 10:20:00 | INSERTED | 5           | delivered       |

-- 客服可以看到：
-- 1. 订单确实经历了 pending → paid → shipped → delivered → refunded
-- 2. 在 10:20 从 delivered 变成 refunded
-- 3. 金额从 299.00 变成 0.00
-- 4. 可以进一步查询 snapshot 5 的元数据，找到操作人
```

#### upsert=true 的审计查询

```sql
-- 尝试查询订单状态变更历史
SELECT 
    order_id,
    status,
    amount,
    updated_at
FROM iceberg.orders.data_files
WHERE order_id = 2001
ORDER BY updated_at;

-- 结果：
-- | order_id | status    | amount | updated_at          |
-- |----------|-----------|--------|---------------------|
-- | 2001     | pending   | 299.00 | 2024-01-01 10:00:00 |
-- | 2001     | paid      | 299.00 | 2024-01-01 10:05:00 |
-- | 2001     | shipped   | 299.00 | 2024-01-01 10:10:00 |
-- | 2001     | delivered | 299.00 | 2024-01-01 10:15:00 |
-- | 2001     | refunded  | 0.00   | 2024-01-01 10:20:00 |

-- 问题：
-- 1. 看起来有完整的历史，但这只是因为每次写入都创建了新的 data file
-- 2. 如果做了 compaction，这些历史就会被合并，只剩下最后一条
-- 3. Delete files 只有 {order_id:2001}，无法证明删除的是哪个状态
```

---

### 场景 3: 医疗数据审计（HIPAA 合规）

#### 业务需求

```sql
-- 患者病历表
CREATE TABLE patient_records (
    patient_id BIGINT PRIMARY KEY,
    diagnosis VARCHAR(200),
    medication VARCHAR(200),
    doctor_id BIGINT,
    updated_at TIMESTAMP
);

-- 业务操作
T1: INSERT INTO patient_records VALUES (3001, 'Hypertension', 'Lisinopril 10mg', 101, '2024-01-01 10:00:00');
T2: UPDATE patient_records SET medication='Lisinopril 20mg' WHERE patient_id=3001;  -- 调整剂量
T3: UPDATE patient_records SET diagnosis='Hypertension, Diabetes' WHERE patient_id=3001;  -- 新增诊断
```

#### 合规要求（HIPAA）

美国医疗保险流通与责任法案（HIPAA）要求：
1. **记录所有对患者数据的访问和修改**
2. **能够追溯谁在什么时候修改了什么数据**
3. **保留完整的变更历史至少 6 年**

#### upsert=false 的合规能力

```sql
-- 审计查询：患者 3001 的诊断和用药变更历史
SELECT 
    patient_id,
    diagnosis,
    medication,
    doctor_id,
    updated_at,
    action,
    snapshot_id
FROM (
    SELECT 
        patient_id,
        diagnosis,
        medication,
        doctor_id,
        updated_at,
        'DELETED' as action,
        snapshot_id
    FROM iceberg.patient_records.delete_files
    WHERE patient_id = 3001
    
    UNION ALL
    
    SELECT 
        patient_id,
        diagnosis,
        medication,
        doctor_id,
        updated_at,
        'INSERTED' as action,
        snapshot_id
    FROM iceberg.patient_records.data_files
    WHERE patient_id = 3001
)
ORDER BY updated_at;

-- 结果：
-- | patient_id | diagnosis              | medication      | doctor_id | updated_at          | action   | snapshot_id |
-- |------------|------------------------|-----------------|-----------|---------------------|----------|-------------|
-- | 3001       | Hypertension           | Lisinopril 10mg | 101       | 2024-01-01 10:00:00 | INSERTED | 1           |
-- | 3001       | Hypertension           | Lisinopril 10mg | 101       | 2024-01-01 10:00:00 | DELETED  | 2           |
-- | 3001       | Hypertension           | Lisinopril 20mg | 101       | 2024-01-01 10:05:00 | INSERTED | 2           |
-- | 3001       | Hypertension           | Lisinopril 20mg | 101       | 2024-01-01 10:05:00 | DELETED  | 3           |
-- | 3001       | Hypertension, Diabetes | Lisinopril 20mg | 101       | 2024-01-01 10:10:00 | INSERTED | 3           |

-- 满足 HIPAA 要求：
-- 1. ✅ 记录了所有变更（剂量调整、新增诊断）
-- 2. ✅ 可以追溯医生 101 的操作
-- 3. ✅ 保留了完整的历史（通过 Iceberg snapshot）
```

#### upsert=true 的合规风险

```sql
-- Delete files 只有 {patient_id:3001}
-- 无法证明：
-- 1. 药物剂量是如何调整的（10mg → 20mg）
-- 2. 诊断是如何变化的（Hypertension → Hypertension, Diabetes）
-- 3. 是否有未授权的修改

-- ❌ 不满足 HIPAA 合规要求
-- 可能面临罚款：每次违规 $100 - $50,000
```

---

## 审计的技术实现

### 1. 时间旅行查询（Time Travel）

```sql
-- 查询某个时间点的数据状态
SELECT * FROM iceberg.accounts
FOR SYSTEM_TIME AS OF '2024-01-01 10:05:00'
WHERE account_id = 1001;

-- 结果：{account_id:1001, balance:9500.00, status:'active'}
-- 可以看到 10:05 时刻的账户状态
```

### 2. 快照对比（Snapshot Diff）

```sql
-- 对比两个快照之间的差异
SELECT 
    a.account_id,
    a.balance as old_balance,
    b.balance as new_balance,
    b.balance - a.balance as change
FROM iceberg.accounts FOR SYSTEM_VERSION AS OF 2 a
JOIN iceberg.accounts FOR SYSTEM_VERSION AS OF 3 b
ON a.account_id = b.account_id
WHERE a.balance != b.balance;

-- 结果：
-- | account_id | old_balance | new_balance | change |
-- |------------|-------------|-------------|--------|
-- | 1001       | 9500.00     | 9800.00     | 300.00 |
```

### 3. 变更日志重建（Change Log Reconstruction）

```sql
-- 重建完整的变更日志
WITH deletes AS (
    SELECT 
        account_id,
        balance,
        status,
        snapshot_id,
        'DELETE' as op
    FROM iceberg.accounts.delete_files
),
inserts AS (
    SELECT 
        account_id,
        balance,
        status,
        snapshot_id,
        'INSERT' as op
    FROM iceberg.accounts.data_files
)
SELECT * FROM deletes
UNION ALL
SELECT * FROM inserts
ORDER BY snapshot_id, op DESC;  -- DELETE 在前，INSERT 在后

-- 结果：完整的 CDC 流
-- | account_id | balance  | status | snapshot_id | op     |
-- |------------|----------|--------|-------------|--------|
-- | 1001       | 10000.00 | active | 1           | INSERT |
-- | 1001       | 10000.00 | active | 2           | DELETE |
-- | 1001       | 9500.00  | active | 2           | INSERT |
-- | 1001       | 9500.00  | active | 3           | DELETE |
-- | 1001       | 9800.00  | active | 3           | INSERT |
```

---

## 审计的业务价值

### 1. 合规性（Compliance）

- **金融行业**：SOX、Basel III、MiFID II
- **医疗行业**：HIPAA、GDPR
- **电商行业**：PCI DSS、消费者权益保护法

### 2. 风险管理（Risk Management）

- **欺诈检测**：发现异常的数据变更模式
- **内部威胁**：追踪未授权的数据修改
- **数据泄露**：确定泄露的范围和时间

### 3. 运营效率（Operational Efficiency）

- **故障排查**：快速定位数据问题的根源
- **客户服务**：回答客户关于数据变更的问题
- **数据质量**：监控数据变更的合理性

### 4. 法律证据（Legal Evidence）

- **诉讼支持**：提供数据变更的证据
- **监管审查**：应对监管机构的检查
- **内部审计**：支持内部审计流程

---

## upsert=false vs upsert=true 的审计能力对比

| 审计需求 | upsert=false | upsert=true |
|---------|-------------|-------------|
| **记录完整的旧值** | ✅ Delete files 存储完整行 | ❌ Delete files 只有 key |
| **追溯数据变更** | ✅ 可以看到每个字段如何变化 | ❌ 只知道有变更，不知道变更内容 |
| **时间旅行查询** | ✅ 可以查询任意时间点的状态 | ⚠️ 部分支持（依赖 data files） |
| **变更日志重建** | ✅ 可以完整重建 CDC 流 | ❌ 无法重建（缺少旧值） |
| **合规性证明** | ✅ 满足大多数合规要求 | ❌ 不满足严格的合规要求 |
| **故障排查** | ✅ 可以追溯问题根源 | ❌ 难以定位问题 |
| **存储成本** | ❌ Delete files 更大 | ✅ Delete files 更小 |

---

## 总结

### 什么是审计？

**审计 = 记录和追踪数据的完整变更历史**

包括：
1. **谁**（操作人）
2. **什么时候**（时间戳）
3. **修改了什么**（旧值 → 新值）
4. **为什么**（业务原因）

### 为什么 upsert=false 对审计重要？

1. **保留完整的旧值**：Delete files 存储完整行，而不只是 key
2. **可以重建变更历史**：通过 delete files 和 data files 重建完整的 CDC 流
3. **满足合规要求**：金融、医疗等行业的法律法规要求
4. **便于故障排查**：出问题时可以追溯根源
5. **支持时间旅行**：可以查询任意时间点的数据状态

### 关键点

**审计不是可选的"nice to have"，而是很多行业的法律要求。upsert=false 通过保留完整的变更历史，提供了强大的审计能力，这是 upsert=true 无法提供的。**

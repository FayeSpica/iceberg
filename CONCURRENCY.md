# Apache Iceberg REST Catalog 并发提交冲突问题分析与解决方案

## 1. 问题描述

### 错误信息

```
org.apache.iceberg.exceptions.CommitFailedException: Requirement failed: branch main has changed: expected id 5207533182135328640 != 8175259835724527679
```

### 现象

- 频繁出现 `CommitFailedException` 异常
- 表的 metadata 目录下产生大量 `metadata.json` 文件
- 写入性能下降，重试增多

---

## 2. 问题原因分析

### 2.1 根本原因

Iceberg 使用 **乐观并发控制（OCC）** 机制来保证数据一致性。当多个写入者同时尝试更新同一个表时：

1. 每个写入者在提交前会记录当前 main 分支的快照 ID
2. 提交时会验证快照 ID 是否发生变化
3. 如果已被其他写入者修改，则抛出 `CommitFailedException`

### 2.2 为什么产生大量 metadata.json

| 阶段 | 行为 | 结果 |
|------|------|------|
| 客户端重试 | `SnapshotProducer.commit()` 默认重试 4 次 | 每次重试生成新的 manifest list |
| 服务端重试 | `CatalogHandlers.commit()` 也有重试逻辑 | 双重重试放大问题 |
| 每次提交尝试 | 写入新的 `metadata.json` 文件 | 文件数量快速增长 |

### 2.3 相关代码位置

```java
// UpdateRequirement.java - 验证快照 ID
class AssertRefSnapshotID implements UpdateRequirement {
    @Override
    public void validate(TableMetadata base) {
        SnapshotRef ref = base.ref(name);
        if (ref != null && snapshotId != null && snapshotId != ref.snapshotId()) {
            throw new CommitFailedException(
                "Requirement failed: %s %s has changed: expected id %s != %s",
                type, name, snapshotId, ref.snapshotId());
        }
    }
}
```

---

## 3. 解决方案

### 3.1 减少并发冲突（治本）

#### 方案 A：分区级别隔离

让不同的写入作业只写入各自负责的分区，避免冲突。

**表结构示例：**

```sql
CREATE TABLE orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10,2),
    order_date DATE
) PARTITIONED BY (order_date);
```

**隔离策略：**

```
❌ 错误：多个作业同时写入所有分区
   Job A, B, C → 同时写入 2024-01-01, 2024-01-02, 2024-01-03

✅ 正确：每个作业只负责特定分区
   Job A → 只写入 2024-01-01
   Job B → 只写入 2024-01-02
   Job C → 只写入 2024-01-03
```

**PySpark 实现：**

```python
target_date = "2024-01-01"  # 每个作业配置不同的日期

df.filter(f"order_date = '{target_date}'") \
  .writeTo("catalog.db.orders") \
  .append()
```

#### 方案 B：时间隔离

错开不同作业的运行时间，避免同时写入同一个表。

#### 方案 C：表隔离

将高并发写入分散到多个临时表，定期合并到主表。

---

### 3.2 调整表属性（缓解症状）

```sql
ALTER TABLE your_table SET TBLPROPERTIES (
  -- 减少重试次数（默认 4）
  'commit.retry.num-retries' = '2',
  
  -- 减少总超时时间（默认 30 分钟）
  'commit.retry.total-timeout-ms' = '60000',
  
  -- 限制保留的历史 metadata 版本数（默认 100）
  'write.metadata.previous-versions-max' = '10',
  
  -- 启用提交后自动删除旧元数据（默认 false）
  'write.metadata.delete-after-commit.enabled' = 'true'
);
```

#### 完整属性参考

| 属性 | 默认值 | 说明 |
|------|--------|------|
| `commit.retry.num-retries` | 4 | 提交重试次数 |
| `commit.retry.min-wait-ms` | 100 | 最小重试等待时间（毫秒） |
| `commit.retry.max-wait-ms` | 60000 | 最大重试等待时间（毫秒） |
| `commit.retry.total-timeout-ms` | 1800000 | 总超时时间（30分钟） |
| `write.metadata.previous-versions-max` | 100 | 保留的历史元数据版本数 |
| `write.metadata.delete-after-commit.enabled` | false | 提交后是否删除旧元数据 |

---

### 3.3 定期清理历史数据

#### 3.3.1 使用 Spark SQL（推荐）

```python
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("IcebergMaintenance") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "rest") \
    .config("spark.sql.catalog.my_catalog.uri", "http://your-rest-catalog:8181") \
    .getOrCreate()

table_name = "my_catalog.db.your_table"

# 1. 过期旧快照（保留最近 7 天，至少保留 5 个）
expire_ts = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
spark.sql(f"""
    CALL my_catalog.system.expire_snapshots(
        table => '{table_name}',
        older_than => TIMESTAMP '{expire_ts}',
        retain_last => 5
    )
""")

# 2. 删除孤立文件（3 天前的）
orphan_ts = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
spark.sql(f"""
    CALL my_catalog.system.remove_orphan_files(
        table => '{table_name}',
        older_than => TIMESTAMP '{orphan_ts}'
    )
""")

# 3. 重写 manifests（合并小的 manifest 文件）
spark.sql(f"""
    CALL my_catalog.system.rewrite_manifests('{table_name}')
""")
```

#### 3.3.2 使用 Py4J 调用 Java API

```python
from pyspark.sql import SparkSession
import time
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# 表名配置
catalog_name = "my_catalog"
database_name = "db"
table_name = "your_table"
full_table_name = f"{catalog_name}.{database_name}.{table_name}"

# 加载表
table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
    spark._jsparkSession, 
    full_table_name
)

# 计算时间戳（示例：10 分钟前）
minutes = 10
older_than_ms = int(time.time() * 1000) - (minutes * 60 * 1000)

print(f"Table: {full_table_name}")
print(f"Deleting orphan files older than: {datetime.fromtimestamp(older_than_ms / 1000)}")

# 获取 SparkActions
actions = spark._jvm.org.apache.iceberg.spark.actions.SparkActions.get(spark._jsparkSession)

# 执行删除孤立文件
result = actions.deleteOrphanFiles(table) \
    .olderThan(older_than_ms) \
    .execute()

# 输出结果
deleted_files = result.orphanFileLocations()
count = deleted_files.size()
print(f"\n✅ Deleted {count} orphan files")

if count > 0:
    print("\nDeleted files:")
    iterator = deleted_files.iterator()
    while iterator.hasNext():
        print(f"  {iterator.next()}")
```

#### 3.3.3 干运行模式（预览不删除）

```python
# 只查看将被删除的文件，不实际删除
spark.sql(f"""
    CALL my_catalog.system.remove_orphan_files(
        table => 'db.your_table',
        older_than => TIMESTAMP '{orphan_ts}',
        dry_run => true
    )
""").show(truncate=False)
```

---

## 4. 完整维护脚本

```python
#!/usr/bin/env python3
"""
Iceberg 表维护脚本
- 过期旧快照
- 删除孤立文件
- 重写 manifests
"""

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import argparse


def create_spark_session():
    return SparkSession.builder \
        .appName("IcebergTableMaintenance") \
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()


def expire_snapshots(spark, table_name, days=7, retain_last=5):
    """过期旧快照"""
    ts = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[1/3] Expiring snapshots older than {ts}, retain last {retain_last}...")
    
    spark.sql(f"""
        CALL system.expire_snapshots(
            table => '{table_name}',
            older_than => TIMESTAMP '{ts}',
            retain_last => {retain_last}
        )
    """)
    print("      ✅ Done")


def remove_orphan_files(spark, table_name, days=3):
    """删除孤立文件"""
    ts = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[2/3] Removing orphan files older than {ts}...")
    
    result = spark.sql(f"""
        CALL system.remove_orphan_files(
            table => '{table_name}',
            older_than => TIMESTAMP '{ts}'
        )
    """)
    count = result.count()
    print(f"      ✅ Removed {count} orphan files")


def rewrite_manifests(spark, table_name):
    """重写 manifests"""
    print(f"[3/3] Rewriting manifests...")
    
    spark.sql(f"""
        CALL system.rewrite_manifests('{table_name}')
    """)
    print("      ✅ Done")


def main():
    parser = argparse.ArgumentParser(description='Iceberg Table Maintenance')
    parser.add_argument('--table', required=True, help='Full table name (catalog.db.table)')
    parser.add_argument('--expire-days', type=int, default=7, help='Expire snapshots older than N days')
    parser.add_argument('--orphan-days', type=int, default=3, help='Remove orphan files older than N days')
    parser.add_argument('--retain-last', type=int, default=5, help='Retain at least N snapshots')
    args = parser.parse_args()

    spark = create_spark_session()
    
    print(f"\n{'='*60}")
    print(f"Iceberg Table Maintenance")
    print(f"Table: {args.table}")
    print(f"{'='*60}\n")

    try:
        expire_snapshots(spark, args.table, args.expire_days, args.retain_last)
        remove_orphan_files(spark, args.table, args.orphan_days)
        rewrite_manifests(spark, args.table)
        
        print(f"\n{'='*60}")
        print("✅ Maintenance completed successfully!")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

**使用方式：**

```bash
spark-submit maintenance.py \
    --table my_catalog.db.your_table \
    --expire-days 7 \
    --orphan-days 3 \
    --retain-last 5
```

---

## 5. 最佳实践建议

### 5.1 预防措施

| 措施 | 说明 |
|------|------|
| 分区隔离写入 | 不同作业写入不同分区 |
| 错开写入时间 | 避免多个作业同时写入同一表 |
| 合理设置重试参数 | 减少不必要的重试 |
| 启用自动清理 | 设置 `write.metadata.delete-after-commit.enabled=true` |

### 5.2 定期维护

| 操作 | 建议频率 | 说明 |
|------|----------|------|
| `expire_snapshots` | 每天 | 清理过期快照 |
| `remove_orphan_files` | 每周 | 删除孤立文件 |
| `rewrite_manifests` | 每周 | 合并小的 manifest 文件 |
| `rewrite_data_files` | 按需 | 合并小文件 |

### 5.3 安全注意事项

⚠️ **删除孤立文件时的注意事项：**

1. `older_than` 时间不要设置太短，建议至少 **1 小时以上**
2. 确保没有正在运行的写入作业
3. 首次执行建议使用 `dry_run => true` 预览
4. 在维护窗口期间执行

---

## 6. 总结

| 问题 | 解决方案 |
|------|----------|
| 并发冲突频繁 | 分区隔离写入、错开写入时间 |
| metadata.json 过多 | 启用自动清理、调整保留版本数 |
| 孤立文件堆积 | 定期执行 `remove_orphan_files` |
| 整体性能下降 | 定期执行完整维护流程 |

**核心建议**：最根本的解决方案是**减少对同一表的并发写入**，而不是仅仅调整重试参数或清理文件。

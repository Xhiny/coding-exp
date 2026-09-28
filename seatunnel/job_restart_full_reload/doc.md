# SeaTunnel 任务重启的重复写入与全量重跑配置

> 适用版本：SeaTunnel 2.3.13（Zeta Engine，Hazelcast 5.1）
> 链路形态：MySQL-CDC → Transform → ClickHouse（Docker Compose 集群，1 master + 2 worker）
> 整理时间：2026-09-28，基于生产配置与 `connector-clickhouse-2.3.13`、`connector-cdc-mysql-2.3.13`、引擎字节码核实

---

## 0. 一句话结论

- **停掉旧 job、重新提交一个新 job**：ClickHouse 会**全量重复插入**。因为 MySQL-CDC 的 `startup.mode` 默认是 `initial`，新 job 会重新做一次全表快照；而 ClickHouse Sink 本身是 **at-least-once**，不存在 exactly-once。
- **用 `-r <jobId>`（原 jobId 从 checkpoint 续跑）**：只重复"最近一次成功 checkpoint 之后"的数据，重复窗口 ≈ `checkpoint.interval` + 内存中未刷批的数据。
- 想让**新 job 自动重建表并重跑全量**：配 `schema_save_mode = "RECREATE_SCHEMA"`，然后用**新 jobId 提交**即可；`-r` 续跑**不会**触发重建/清空（引擎的安全设计）。
- 目标表是 `ReplacingMergeTree` 时，重复行最终会被后台 merge 按主键去重，但**去重是异步的**：查询不加 `FINAL`、或未 `OPTIMIZE TABLE ... FINAL`，短时间内仍会看到重复行。

---

## 1. 三种"重启"方式的重复范围

| 重启方式 | 是否重复 | 重复范围 | 数据来源位置 |
|---|---|---|---|
| 新提交一个 job（新 jobId） | 会，**全量** | 所有现存行重新快照写入，再接着读 binlog | `startup.mode=initial`（默认） |
| 原 jobId 续跑 `--async -r <jobId> -c <conf>` | 会，**小范围** | 最近一次成功 checkpoint 之后已写入的数据被重放 | checkpoint 里的 binlog 位点 |
| `-r` 但 checkpoint 文件已丢 | 会，**全量** | checkpoint 找不到时不报错，**静默降级**为全新任务 → 又走 `initial` 全量快照 | 无 |

判断依据（2.3.13 源码）：

- `MySqlIncrementalSourceOptions.STARTUP_MODE` 默认值 = `StartupMode.INITIAL`（可选 `initial / earliest / latest / specific / timestamp`）。配置里不写 `startup.mode` 就是全量快照。
- 恢复逻辑见 [Checkpoint 持久化与恢复实践](../checkpoint_persistence/doc.md)：`-r` 时从 checkpoint 存储找最近一次成功 checkpoint，source 从其中的 binlog 位点继续。

> 注意：想"全量重刷"就保持 `initial`；**不要**为了"避免重复"改成 `latest`，那会直接跳过停机期间的所有变更（丢数据，不是去重）。

---

## 2. 为什么一定有重复：ClickHouse Sink 是 at-least-once

对 `connector-clickhouse-2.3.13.jar` 反编译核实（`ClickhouseSinkWriter`）：

| 行为 | 结论 |
|---|---|
| `write()` | 攒批，单批达到 `bulk_size`（默认 **20000**）时**立即 flush 到 ClickHouse 目标表** |
| `prepareCommit()` | flush 剩余数据后**直接返回 `Optional.empty()`** |
| `createCommitter()` | Sink 未覆写，即**没有两阶段提交 / 事务**，也没有 staging 表 |

也就是说：**数据在 checkpoint 完成之前就已经落在目标表里了**。恢复时 source 从上一个成功 checkpoint 的位点重读，这段"已落表但未进 checkpoint"的数据必然被再插一次。这就是 at-least-once 的来源。

重复窗口的量级：

```
重放窗口 ≈ checkpoint.interval（生产任务 15s）+ 未达到 bulk_size 的内存数据
```

`checkpoint.interval` 越大、`bulk_size` 越大，单次恢复的重复量越大。

---

## 3. ClickHouse 侧：重复行会不会一直留着

如果目标表是用下面的模板建的：

```hocon
save_mode_create_template = """
CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (
${rowtype_primary_key},
${rowtype_fields}
) ENGINE = ReplacingMergeTree()
ORDER BY (${rowtype_primary_key})
PRIMARY KEY (${rowtype_primary_key})
SETTINGS index_granularity = 8192
"""
```

那么同一主键的重复行会在**后台 merge 时去重**。但要注意：

| 事项 | 说明 |
|---|---|
| 去重时机 | 异步，`SELECT count()` / 不加 `FINAL` 的查询在 merge 前仍会看到重复 |
| 查询正确性 | 要稳定看到去重结果需 `SELECT ... FINAL`，或手动 `OPTIMIZE TABLE db.t FINAL` |
| 保留哪一行 | 裸 `ReplacingMergeTree()`（无 version 列）由写入/合并顺序决定，通常后写入的赢，但不保证确定性；需要确定性语义应加版本列，如 `ReplacingMergeTree(ver)` |
| 前提 | 表必须**确实是 RMT**。`schema_save_mode = CREATE_SCHEMA_WHEN_NOT_EXIST` 不会修改已存在的表；若某张表当初是普通 `MergeTree`，重复就是**永久的** |

---

## 4. 配置"重新起 job 自动重建表 + 重跑全量"

### 4.1 配置示例

```hocon
source {
  MySQL-CDC {
    ...
    startup.mode = "initial"   # 默认值，显式写出来更醒目；新 job 全量快照
  }
}

sink {
  Clickhouse {
    ...
    # 方式 A：启动时 DROP TABLE + 按 save_mode_create_template 重建
    schema_save_mode = "RECREATE_SCHEMA"
    data_save_mode  = "APPEND_DATA"        # 表已被重建，无数据可清

    # 方式 B：保留表结构、只清数据
    # schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    # data_save_mode  = "DROP_DATA"        # TRUNCATE TABLE

    save_mode_create_template = """..."""  # 重建时使用该模板
  }
}
```

### 4.2 两种方案对比

| 配置 | 实际动作 | 适用场景 |
|---|---|---|
| `schema_save_mode = "RECREATE_SCHEMA"` | `DROP TABLE` + 按模板 `CREATE` | 表结构也按模板重建；表上后加的自定义 DDL（额外列、物化视图、TTL 等）会丢 |
| `CREATE_SCHEMA_WHEN_NOT_EXIST` + `data_save_mode = "DROP_DATA"` | 保留表结构，`TRUNCATE TABLE` 清数据 | 想保留现有表定义，只要求每次从零跑全量 |

默认值是 `CREATE_SCHEMA_WHEN_NOT_EXIST` + `APPEND_DATA`，即"只建缺失表、只追加不清理"。

可选防呆/自定义：

- `data_save_mode = "ERROR_WHEN_DATA_EXISTS"`：目标表有数据时直接报错，防止误重复插入。
- `data_save_mode = "CUSTOM_PROCESSING"` + `custom_sql = "..."`：执行自定义清理 SQL。

### 4.3 执行时机（关键）

引擎（`JobMaster`）在任务初始化时按 `isStartWithSavePoint` 走两条不同分支：

| 启动方式 | 执行内容 | 是否会重建/清空 |
|---|---|---|
| 新提交 job（`isStartWithSavePoint=false`） | `handleSchemaSaveMode()` + `handleDataSaveMode()` 完整执行 | 按配置执行 `DROP` / `TRUNCATE` |
| `-r` 续跑（`isStartWithSavePoint=true`） | 只执行 `handleSchemaSaveModeWithRestore()`：`RECREATE_SCHEMA` 被降级为"不存在才建"，`data_save_mode` 完全不执行 | **不会** |

补充说明：

- **执行位置**：`env.savemode.execute.location` 默认 `CLUSTER`，由 master 在任务启动时执行；`CLIENT` 已被标记 deprecated。
- **多表场景**：`table = "${table_name}"` 这种动态模板，在配置解析阶段 `MultipleTableJobConfigParser` 会把 `table-names` 展开成每张表一个 sink，**逐表**执行 save mode，不需要一张张配。
- **重建用的模板**：`ClickhouseCatalog` 的建表语句取自 `save_mode_create_template`，所以 RECREATE 出来的表仍是 RMT。

**结论**：想"重建表 + 全量重跑"，正确姿势是——

```bash
# 1. 先取消/停止旧 job
# 2. 用同一份 conf 重新 submit（新 jobId），不要带 -r
bin/seatunnel.sh --async -c config/mysql_to_clickhouse_donation.conf
```

---

## 5. 坑与注意事项

1. **`DROP` 发生在 job 启动的最开头**。如果全量快照跑到一半失败，目标表就是空的/只有半截数据，要等重新跑成功。期间若有下游在读，先安排好。
2. **`RECREATE_SCHEMA` 每次新提交都会执行**。配置放着不动，之后每次提交都会清库重来；只想重刷一次，跑完后改回 `CREATE_SCHEMA_WHEN_NOT_EXIST` / `APPEND_DATA`。
3. **失败后不要用 `-r` 来"续"一个还没产生 checkpoint 的全量任务**。checkpoint 不存在时它会重新快照，但 `handleSchemaSaveModeWithRestore` 不清表，失败前已写入的部分数据会和新快照重复（RMT 最终能按主键去重，但查询要 `FINAL`）。这种情况直接新提交一个 job，让 RECREATE 把表清干净。
4. **checkpoint 目录必须持久化**，否则 `-r` 会因找不到 checkpoint 静默降级为全量重跑。生产上 checkpoint 默认写在容器内 `/tmp/seatunnel/checkpoint_snapshot`，`docker compose down/up` 或重建容器即丢，需挂载宿主机卷（见 [Checkpoint 持久化与恢复实践](../checkpoint_persistence/doc.md) 第 3 节）。
5. **无 version 列的 RMT 去重不保证"留最新"**。对恢复/重放场景，重放的数据本身按顺序写入，通常最终一致；对时间敏感的表建议加版本列或定期 `OPTIMIZE ... FINAL`。
6. `DROP` / `TRUNCATE` 需要 ClickHouse 写权限（CDC 使用的账号要确认）。
7. `data_save_mode = "DROP_DATA"` 走的是 `TRUNCATE TABLE`；数据量特别大时，`DROP` + `CREATE` 通常比 `TRUNCATE` 更彻底也更快。

---

## 6. 操作与验证清单

```bash
# 全新提交（触发 RECREATE_SCHEMA + initial 全量）
bin/seatunnel.sh --async -c config/mysql_to_clickhouse_donation.conf

# 续跑（不触发 save mode，从最近一次成功 checkpoint 恢复）
bin/seatunnel.sh --async -r <jobId> -c config/mysql_to_clickhouse_donation.conf
```

```bash
# 1) 确认表确实是 ReplacingMergeTree
clickhouse-client --query "
SELECT database, name, engine, sorting_key
FROM system.tables
WHERE database IN ('donation', 'marathon_2')
  AND engine != 'ReplacingMergeTree'"
# 输出为空即全部为 RMT

# 2) 看重复/去重效果（对单表）
clickhouse-client --query "SELECT count() FROM donation.qs_donation"
clickhouse-client --query "SELECT count() FROM donation.qs_donation FINAL"
# 如需要立即收敛，再执行（大表谨慎，开销大）：
# OPTIMIZE TABLE donation.qs_donation FINAL;

# 3) 确认续跑确实从 checkpoint 恢复（restored 计数 +1）
./scripts/show_checkpoints.sh <jobId>
```

REST API 侧也可用 `GET /jobs/checkpoints/<jobId>` 查看 `restored` 计数，`GET /running-jobs` / `/finished-jobs` 查看任务状态。

---

## 7. 推荐做法

| 目标 | 做法 |
|---|---|
| 标准配置（推荐） | 配 `RECREATE_SCHEMA`：每次"停旧 job → 新提交"都是一次干净的全量重建；故障恢复一律用 `-r` 续跑（不重建、不清表）；checkpoint 目录挂到宿主机卷 |
| 新提交时不想清表 | 改用 `CREATE_SCHEMA_WHEN_NOT_EXIST` + `APPEND_DATA`（只追加）；需要重刷时再临时改回 `RECREATE_SCHEMA` |
| 查询侧不受重复影响 | 查询统一带 `FINAL`，或对主键做 `argMax` 聚合；非高频表定期 `OPTIMIZE ... FINAL` |
| 防止误重刷 | 可临时用 `data_save_mode = "ERROR_WHEN_DATA_EXISTS"` 作为防呆 |

---

## 相关文档

- [SeaTunnel Checkpoint 持久化与恢复实践](../checkpoint_persistence/doc.md)
- [使用 SeaTunnel 实现 MySQL CDC 同步数据到 ClickHouse](../mysql_cdc_to_clickhouse/doc.md)

# Print ADX 到 Snowflake 首轮联调步骤

## 1. 本次目标

当前目标不是只建 Snowflake 表，而是把以下整条链路一起打通：

`ADX(print_snapshot_std) -> 导出 Parquet 到 Blob -> 写 manifest.json -> Blob Trigger Function -> 写入 Snowflake`

首轮建议只做 `print_snapshot_std`，先把样板链路跑通。

## 2. 为什么这次 Snowflake 表要加标准字段

如果只是单纯做一张和 ADX 完全同构的业务表，那么只保留业务字段也可以。

但现在我们要做的是日批同步链路，后面一定会遇到这些问题：

- 这批数据来自哪个源表
- 这批数据来自哪个 blob 文件
- 这次批次号是什么
- 这次 Function 跑的是哪一次
- 一条数据如果重复写入，怎么排查
- 这批数据是什么时候从 ADX 导出的
- 这批数据是什么时候真正落到 Snowflake 的

所以建议保留两类字段：

1. 业务字段
2. 链路治理字段

## 2.1 当前设计是全量还是增量

当前这版方案，定位应当是：

- **日批增量**
- **不是每次全量重刷整张表**

这里的“增量”不是指只按“最新一条 watermark 之后的数据”直接追加，而是指：

- 每天只处理一个有限的业务时间窗口
- 这个窗口按 `read_time` 对应的业务日期来切
- Loader 端对该窗口做覆盖式重算，而不是盲目 append

也就是说，推荐口径是：

`增量范围抽取 + 窗口内重算落表`

而不是：

`无限追加`

## 2.2 为什么不建议做纯全量

如果每次都从 ADX 全量导出 `print_snapshot_std` 再整表重写 Snowflake，理论上最直观，但问题也很明显：

- 数据量会越来越大
- 每日导出和装载时间会持续增长
- Blob 临时文件会越来越多
- Function 装载耗时和失败重试成本更高
- 后续扩到 defect / prod_output 时会更重

所以这条链更适合做“按业务日期窗口处理”的增量方案。

## 2.3 如果做增量，怎样保证不漏数

要保证准确，关键不是“只做增量”还是“只做全量”，而是增量策略本身要正确。

推荐同时做 4 件事：

1. **按业务日期切批，不按当前最新态切批**
2. **每次重跑最近 N 天窗口，不只跑昨天 1 天**
3. **Snowflake 对窗口内数据做覆盖式写入，不做纯追加**
4. **保留批次审计和行数对账**

### 1. 按业务日期切批

导出窗口应基于：

- `read_time >= startofday(...)`
- `read_time < startofday(...)`

不能按“当前表里最新一条之后继续取”，因为这类做法对迟到数据不稳，容易漏。

### 2. 重跑最近 N 天窗口

如果只在每天凌晨跑一次“昨天 0 点到 24 点”的数据，那么有两类风险：

- ADX 中有迟到入库
- 上游当天晚到的数据在导出时还没完全落稳

所以建议不要只跑 `T-1`，而是每次跑最近 `N` 天闭区间窗口。

当前最推荐：

- `EXPORT_QUERY_WINDOW_DAYS = 2`

也就是今天跑的时候，重新导出：

- 前天
- 昨天

这样即使昨天还有晚到数据，今天这次也能补回来。

### 3. Snowflake 端不要纯 append

如果 Loader 每次都只是 `insert into`，那么重跑最近 2 天时一定会重复。

因此推荐两种安全方式中的一种：

#### 方式 A：按 `business_date` 删除后重灌

适合当前这张 print 表，最简单直接。

做法：

1. 先从 manifest 识别本批次覆盖的 `business_date`
2. 删除 Snowflake 目标表中这些 `business_date` 的旧数据
3. 再插入本批次新数据

即：

`delete by business_date window + insert current batch`

#### 方式 B：先入 stage，再做 MERGE

适合后续如果要保留更细粒度主键控制。

但当前 `print_snapshot_std` 没有天然单一主键，首轮联调不如方式 A 简洁。

### 4. 做批次审计和对账

至少保留：

- `batch_id`
- `business_date`
- `row_count`
- `load_status`
- `started_at`
- `finished_at`

并建议每批次记录：

- ADX 导出行数
- Snowflake 实际写入行数

如果两边不一致，就标记失败或待复核。

## 2.4 当前最推荐的准确落地方式

对 `print_snapshot_std`，当前建议采用下面这个口径：

1. 每天日批运行一次
2. 每次导出最近 2 天的闭区间数据
3. 为每条记录生成 `business_date`
4. Loader 先删除目标表中这 2 天的数据
5. 再插入本批次 parquet 中的数据
6. 用 `snowflake_load_audit` 记录批次结果

这个方案的核心优点是：

- 仍然是增量，不会整表重刷
- 能覆盖迟到数据
- 不依赖不稳定的“最新 watermark 继续追”
- 逻辑直观，客户也容易理解

## 3. 本次推荐的 Snowflake 表设计

### 3.1 业务字段

直接与 ADX `print_snapshot_std` 对齐：

- `read_time`
- `topic`
- `workcenter`
- `equipment`
- `status`
- `dmc_code`
- `rfid_code`
- `prod_cnt`
- `print_cnt`
- `print_spd`
- `print_stroke`
- `ink_ret_spd`
- `ink_ret_stroke`
- `off_contact_hgt`
- `off_contact_ratio`
- `silk_screen_life`
- `screen_count`
- `silk_screen_status`
- `ir1_maxtemp`
- `ir1_mintemp`
- `ir1_curtemp`
- `ir2_maxtemp`
- `ir2_mintemp`
- `ir2_curtemp`
- `dry1_speed`
- `flood_stroke`
- `flood_spd`
- `screen_height`

### 3.2 链路治理字段

本次建议一起加：

- `source_system`
- `source_table`
- `source_file_name`
- `source_file_path`
- `batch_id`
- `job_run_id`
- `record_hash`
- `business_date`
- `source_export_time`
- `snowflake_load_time`

## 4. 各标准字段建议怎么赋值

- `source_system`
  - 固定写 `ADX`
- `source_table`
  - 固定写 `print_snapshot_std`
- `source_file_name`
  - 当前 parquet 文件名
- `source_file_path`
  - blob 中完整相对路径
- `batch_id`
  - manifest 中的 `batch_id`
- `job_run_id`
  - 当前 Function 执行实例 id
- `record_hash`
  - 对业务字段做 md5/sha256，用于排重
- `business_date`
  - manifest 中的业务日期，或按 `read_time` 所属日期生成
- `source_export_time`
  - manifest 生成时间，或导出完成时间
- `snowflake_load_time`
  - 本次写入 Snowflake 的时间

## 5. 需要执行的对象

### 5.1 Snowflake 业务表

执行：

- `print_snapshot_std_snowflake_current.sql`

### 5.2 Snowflake 审计表

执行：

- `snowflake_load_audit_current.sql`

### 5.3 Loader 事务模板

执行逻辑参考：

- `print_snapshot_std_loader_transaction_template.sql`

说明：

- 这不是需要在 Snowflake 手工单独执行的 ETL 脚本
- 它表示后续 Function 在运行时应执行的 SQL 事务口径

## 6. Function 侧首轮最小实现

首轮不建议一上来做复杂框架，建议先用最小闭环：

### 函数 1：每日导出 ADX

职责：

- 查询 ADX `print_snapshot_std`
- 导出前一天数据到 Blob parquet
- 写 `manifest.json`

建议 manifest 至少包含：

- `pipeline`
- `batch_id`
- `business_date_from`
- `business_date_to`
- `target_table`
- `file_format`
- `source_path`
- `source_export_time`
- `file_count`
- `adx_export_row_count`

### 函数 2：Blob Trigger 装载 Snowflake

职责：

- 只监听 `manifest.json`
- 读取同批次下所有 parquet
- 补齐标准字段
- 直接按窗口删除 `print_snapshot_std` 中旧数据并插入当前批次
- 记录到 `snowflake_load_audit`

## 7. 首轮执行顺序

### 步骤 1：确认 ADX 表已经验收通过

至少确认：

- `print_snapshot_std` 已有最新字段
- update policy 已恢复
- 4 个 topic 都能进标准表

### 步骤 2：在 Snowflake 建两张表

先执行：

- `print_snapshot_std_snowflake_current.sql`
- `snowflake_load_audit_current.sql`

### 步骤 3：准备 Function App Settings

至少准备：

- `ADX_CLUSTER_URL`
- `ADX_DATABASE`
- `ADX_TOKEN_SCOPE`
- `SNOWFLAKE_EXPORT_CONTAINER`
- `SNOWFLAKE_EXPORT_PREFIX`
- `EXPORT_PIPELINE_NAME`
- `EXPORT_SCHEDULE`
- `EXPORT_QUERY_WINDOW_DAYS`
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_TABLE`
- `SNOWFLAKE_AUDIT_TABLE`
- `SNOWFLAKE_PRIVATE_KEY_PEM`
- `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`

### 步骤 4：先手工导出一批数据

建议先不用等定时器，先手工做一批前一天数据：

```kusto
print_snapshot_std
| where read_time >= startofday(ago(1d))
| where read_time < startofday(now())
```

### 步骤 5：确认 Blob 中已有完整批次目录

例如：

```text
snowflake-export/print_snapshot_std/2026/08/17/batch_20260817_010000/
```

目录下至少要有：

- 一个或多个 parquet 文件
- 一个 `manifest.json`

### 步骤 6：触发 Snowflake Loader

Loader 执行后应完成两件事：

1. 删除目标表窗口旧数据并写入 `print_snapshot_std`
2. 写入 `snowflake_load_audit`

### 步骤 7：Snowflake 验收

先看业务表：

```sql
select *
from print_snapshot_std
order by snowflake_load_time desc
limit 50;
```

再看审计表：

```sql
select *
from snowflake_load_audit
order by started_at desc
limit 20;
```

## 8. 首轮联调通过标准

满足以下条件即可认为首轮打通：

1. ADX 前一天数据能成功导出到 Blob
2. `manifest.json` 能成功触发 Loader
3. Loader 能直接完成窗口重算
4. Snowflake 业务表能查到数据
5. Snowflake 审计表能看到本次批次成功记录
6. 同一个 `batch_id` 重复触发时不会重复装载

## 9. 当前最推荐的落地方式

这次建议不要只建“纯业务字段表”，而是直接建“业务字段 + 标准治理字段”版本。

原因很简单：

- 你们现在不是只看表结构
- 是真的要把 ADX 到 Snowflake 这条链跑起来
- 一旦开始联调，没有治理字段，后面排查会很被动

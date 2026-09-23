# Print / 丝网链路升级执行步骤

## 1. 本次为什么要改

客户最新提供的 4 个 topic / payload 样例，与现有 `print_snapshot_std` 设计有四处关键差异：

1. `2CW2` 的 topic 已从 `SGH/WS1/MF180/2CW2/PRINT1` 变为 `SGH/WS1/MF180/2CW2/PRINT3`
2. `2CW2` 与 `2CW6` 现在都已统一为小写字段名，且字段名一致
3. `OFF_CONTACT_RATIO` 已出现 `1.1`，原 `int` 类型不够，需改为 `real`

因此这次建议采用：

`raw 保留不动 -> std 表重建 -> 从 raw 回灌历史 -> 再恢复 update policy`

## 2. 这次不需要改的部分

以下内容可以保持不变：

- `raw_print_eh`
- `raw_print_eh_json`
- 现有 EMQX 外层包装结构
- Snowflake 日批导出整体链路

## 3. 这次需要改的部分

需要变更：

- `print_snapshot_std` 表结构
- `fn_print_snapshot_materialize()` 函数
- `print_snapshot_std` update policy
- Snowflake 目标表字段

## 4. 建议执行窗口

建议在低峰时段执行，并在执行期间短暂停止 print 数据写入 `raw_print_eh` 对应的 Event Hub，避免在“删表到回灌完成”这段窗口出现漏数或重复。

如果现场不方便停流，也可以执行，但需要在回灌条件里加时间边界做精确切分。

## 5. 标准执行步骤

### 步骤 1：确认 raw 中已有最新 topic

先确认 4 个 topic 已经进入 `raw_print_eh`：

```kusto
raw_print_eh
| where topic in (
    "SGH/WS1/NMF180/2CW6/PRINT1",
    "SGH/WS1/NMF180/2CW6/PRINT2",
    "SGH/WS1/NMF180/2CW6/PRINT3",
    "SGH/WS1/MF180/2CW2/PRINT3"
)
| summarize cnt = count(), min_time = min(enqueued_time), max_time = max(enqueued_time) by topic
| order by topic asc
```

如果这里查不到 `2CW2/PRINT3`，先不要改表，先确认上游是否已经把新 topic 发进 Event Hub。

### 步骤 2：移除旧 policy

```kusto
.delete table print_snapshot_std policy update
```

### 步骤 3：删除旧标准化表

说明：这一步是必须的，因为 `off_contact_ratio` 需要从 `int` 改为 `real`，仅靠 `create-merge` 不够。

```kusto
.drop table print_snapshot_std ifexists
```

### 步骤 4：重建标准化表

请执行最新版 `adx/deploy_units/print_current/44_print_snapshot_materialize_current.csl` 中的建表语句，或者直接执行以下语句：

```kusto
.create table print_snapshot_std (
    read_time: datetime,
    topic: string,
    workcenter: string,
    equipment: string,
    status: int,
    dmc_code: string,
    rfid_code: string,
    print_cnt: long,
    print_spd: int,
    print_stroke: int,
    ink_ret_spd: int,
    ink_ret_stroke: int,
    off_contact_hgt: int,
    off_contact_ratio: real,
    silk_screen_life: long,
    silk_screen_status: string,
    ir1_maxtemp: real,
    ir1_mintemp: real,
    ir1_curtemp: real,
    ir2_maxtemp: real,
    ir2_mintemp: real,
    ir2_curtemp: real,
    dry1_speed: real
)
```

### 步骤 5：重建标准化函数

```kusto
.create-or-alter function with (folder = "print/std", docstring = "PRINT/SILK SCREEN standardized rows from raw_print_eh") fn_print_snapshot_materialize() {
    raw_print_eh
    | where topic in (
        "SGH/WS1/NMF180/2CW6/PRINT1",
        "SGH/WS1/NMF180/2CW6/PRINT2",
        "SGH/WS1/NMF180/2CW6/PRINT3",
        "SGH/WS1/MF180/2CW2/PRINT3"
    )
    | extend p = todynamic(payload)
    | project
        read_time = todatetime(tostring(p.read_time)),
        topic,
        workcenter = tostring(p.workcenter),
        equipment = tostring(p.equipment),
        status = toint(p.status),
        dmc_code = tostring(p.dmc_code),
        rfid_code = tostring(p.rfid_code),
        print_cnt = tolong(p.print_cnt),
        print_spd = toint(p.print_spd),
        print_stroke = toint(p.print_stroke),
        ink_ret_spd = toint(p.ink_ret_spd),
        ink_ret_stroke = toint(p.ink_ret_stroke),
        off_contact_hgt = toint(p.off_contact_hgt),
        off_contact_ratio = todouble(p.off_contact_ratio),
        silk_screen_life = tolong(p.silk_screen_life),
        silk_screen_status = tostring(p.silk_screen_status),
        ir1_maxtemp = todouble(p.ir1_maxtemp),
        ir1_mintemp = todouble(p.ir1_mintemp),
        ir1_curtemp = todouble(p.ir1_curtemp),
        ir2_maxtemp = todouble(p.ir2_maxtemp),
        ir2_mintemp = todouble(p.ir2_mintemp),
        ir2_curtemp = todouble(p.ir2_curtemp),
        dry1_speed = todouble(p.dry1_speed)
}
```

### 步骤 6：从 raw 回灌历史数据

```kusto
.set-or-append print_snapshot_std <| fn_print_snapshot_materialize()
```

### 步骤 7：恢复 update policy

```kusto
.alter table print_snapshot_std policy update @'[{"IsEnabled":true,"Source":"raw_print_eh","Query":"fn_print_snapshot_materialize()","IsTransactional":false,"PropagateIngestionProperties":true}]'
```

## 6. 验收 SQL

### 6.1 看 4 个 topic 是否都已落到标准表

```kusto
print_snapshot_std
| summarize cnt = count(), min_time = min(read_time), max_time = max(read_time) by topic
| order by topic asc
```

### 6.2 单独检查 2CW2 新 topic

```kusto
print_snapshot_std
| where topic == "SGH/WS1/MF180/2CW2/PRINT3"
| project read_time, topic, workcenter, equipment, status, dmc_code, rfid_code, silk_screen_life, ink_ret_stroke, ink_ret_spd, off_contact_hgt, off_contact_ratio, silk_screen_status
| take 20
```

### 6.3 单独检查 2CW6 温度字段

```kusto
print_snapshot_std
| where topic == "SGH/WS1/NMF180/2CW6/PRINT1"
| project read_time, topic, workcenter, equipment, silk_screen_life, ir1_maxtemp, ir1_mintemp, ir1_curtemp, ir2_maxtemp, ir2_mintemp, ir2_curtemp, dry1_speed
| take 20
```

## 7. Event Hub / Data Connection 怎么处理

分两种情况：

### 情况 A：还是写入当前 `raw_print_eh` 对应的 Event Hub

这种情况下 ADX data connection 不需要改。

只要上游已经把以下 4 个 topic 的消息送进当前 print Event Hub，ADX 就会继续自动进 `raw_print_eh`：

- `SGH/WS1/NMF180/2CW6/PRINT1`
- `SGH/WS1/NMF180/2CW6/PRINT2`
- `SGH/WS1/NMF180/2CW6/PRINT3`
- `SGH/WS1/MF180/2CW2/PRINT3`

### 情况 B：客户这次为 print 新建了独立 Event Hub

这种情况下只需要新建一条 Event Hub Data Connection，核心配置如下：

- Target table：`raw_print_eh`
- Data format：`JSON`
- Mapping：`raw_print_eh_json`
- Consumer group：print 专用

raw 层不按 topic 建多张表，topic 过滤仍在 `fn_print_snapshot_materialize()` 中完成。

## 8. Snowflake 要同步改什么

Snowflake 目标表也需要同步扩表，尤其是：

- `off_contact_ratio` 改成浮点
- 新增 `silk_screen_life`
- 新增 `screen_count`
- 新增 `silk_screen_status`
- 新增 `ir1_*`
- 新增 `ir2_*`
- 新增 `dry1_speed`
- 新增 `flood_stroke`
- 新增 `flood_spd`
- 新增 `screen_height`

对应脚本已单独整理在：

- `print_snapshot_std_snowflake_current.sql`

## 9. 本次变更后的最终链路

`Event Hub -> raw_print_eh -> fn_print_snapshot_materialize() -> print_snapshot_std -> Snowflake日批导出`

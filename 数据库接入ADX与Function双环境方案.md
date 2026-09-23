# 数据库接入 ADX 与 Function 双环境方案

## 1. 目标

客户当前有两类新增诉求：

1. 本地电脑持续生成 CSV，希望数据能进入 ADX，并尽量减少人工介入。
2. 客户现场已有 MSSQL / MySQL，希望评估是否能绕过 CSV，直接让 ADX 获取数据库更新。
3. 现有 Function 链路希望同时跑 `UAT` 和 `PROD`，而不是一次只指向一个环境。

本文用于说明当前可行方案、推荐方案以及现有代码下需要改造的边界。

## 2. 结论摘要

### 2.1 CSV 场景

- 一次性导数：可直接用 ADX Web UI 的 `Get data -> Local file`。
- 持续滚动文件：不建议手工导入，推荐：
  `本地目录 -> Azure Blob -> ADX 自动摄取`

### 2.2 MSSQL / MySQL 场景

- ADX 不适合直接把 MySQL / MSSQL 当作“实时查询源”来长期使用。
- 如果客户想要“数据更新后 ADX 很快可见”，推荐两类方案：
  1. `数据库 -> 定时增量抽取 -> ADX`
  2. `数据库 CDC -> Event Hub / Kafka -> ADX`

### 2.3 Function 同时跑 UAT 和 PROD

- 当前这套 Function 代码默认是**单环境口径**，也就是一套函数一次只跑一套环境变量。
- 如果要 `UAT + PROD` 同时跑，**可以做**，但不建议直接共用当前这一套单函数配置。
- 推荐优先级：
  1. **推荐**：拆成两套 Function App，分别跑 `UAT` 与 `PROD`
  2. **可做但不优先**：同一个 Function App 内复制函数，分成 `_uat` 和 `_prod` 两套入口

## 3. CSV 进入 ADX 的方案

### 3.1 一次性手工导入

适用场景：

- 只导几份历史文件
- 先验证表结构和字段口径

方式：

- 进入 `https://dataexplorer.azure.com/home`
- 左侧选 `Query`
- 右键目标数据库
- 选 `Get data`
- 选择 `Local file`
- 上传本地 CSV

优点：

- 最快
- 不需要开发

缺点：

- 不适合持续写入的文件
- 不适合客户日常长期使用

### 3.2 持续滚动 CSV 文件

客户当前描述是：

- 文件持续写入
- 单个文件满 `500MB` 后新建下一个 CSV

这种场景推荐：

`本地目录 -> 本地上传脚本 -> Azure Blob -> ADX`

建议步骤：

1. 客户本地程序继续按现有方式写 CSV。
2. 额外部署一个轻量上传脚本，只处理“已经封口”的文件。
3. 脚本将已完成 CSV 上传到 Azure Blob 指定容器。
4. ADX 从 Blob 做 ingestion，并用 CSV mapping 入表。

关键控制点：

- 不要抓正在写入的文件。
- 只上传“文件大小不再变化，且最后修改时间已超过阈值”的文件。
- 文件名建议包含时间戳或序号，确保唯一。
- ADX 表建议保留 `source_file_name` 字段，方便追溯。

适合原因：

- 比本地直连 ADX 更稳
- 比人工上传更可控
- 失败可补传

## 4. MSSQL / MySQL 进入 ADX 的方案

### 4.1 先说能不能直接连

如果客户的问题是：

“ADX 能不能像 BI 工具那样直接去连 MSSQL / MySQL，并把它当实时源表来查？”

结论是：

**不建议这样理解。**

ADX 更适合的是：

- 把外部数据**摄取进来**
- 再在 ADX 内查询、聚合、建函数、做分析

而不是每次查询都回源库做联查。

### 4.2 推荐方案 A：数据库定时增量同步到 ADX

适用场景：

- 分钟级时效可接受
- 希望尽快去掉 CSV
- 先落一个稳定方案

链路建议：

`MSSQL / MySQL -> 定时抽取增量 -> ADX`

实现方式可选：

1. Azure Data Factory / Fabric Data Factory
2. Python 脚本 / Azure Function Timer
3. 其他现有调度平台

增量方式建议：

- 优先使用源表里的 `update_time` / `modified_time` 字段
- 如果没有更新时间字段，则考虑自增主键范围
- 需要单独保存同步水位

优点：

- 架构简单
- 开发量适中
- 对客户数据库侵入较小

缺点：

- 不是秒级实时

### 4.3 推荐方案 B：数据库 CDC / Binlog / Change Tracking 到 ADX

适用场景：

- 客户明确要求近实时
- 库里一更新，ADX 很快要看到

建议链路：

`MSSQL CDC / MySQL Binlog -> Debezium / Kafka / Event Hub -> ADX`

优点：

- 更接近实时
- 适合持续变更捕获

缺点：

- 改造成本高
- 运维复杂度更高

### 4.4 当前项目推荐选择

如果当前目标是：

- 先摆脱 CSV
- 又不想一上来就做 CDC 大改造

推荐优先做：

**数据库定时增量同步到 ADX**

等客户后面把实时性要求抬高，再升级到 CDC。

## 5. Function 同时跑 UAT 和 PROD

### 5.1 当前代码现状

当前这套 Function 代码已经支持多个业务链路，但每条链路默认都是**单环境入口**。

例如：

- `function_glass_master_10m`
- `function_chemical_traceability_5m`
- `function_production_output_10m`

这些函数各自读取的是一套当前生效的环境变量，例如：

- `GLASS_MASTER_FORWARD_TARGET`
- `GLASS_MASTER_WATERMARK_BLOB_NAME`
- `GLASS_MASTER_API_APP_ID`
- `CHEMICAL_TRACEABILITY_FORWARD_TARGET`
- `CHEMICAL_TRACEABILITY_WATERMARK_BLOB_NAME`
- `CHEMICAL_TRACEABILITY_API_APP_ID`

这意味着：

**当前函数一次只会跑一套口径，不会在同一入口里同时把数据发到 UAT 和 PROD。**

### 5.2 为什么不能只靠改环境变量同时跑

原因有 4 类：

1. `forward_target` 是单值
2. watermark blob 名是单值
3. 接口地址 / app_id / app_secret / tenant_id 是单值
4. success / audit 台账默认也是按单一口径落的

所以如果直接把一个函数既想跑 UAT 又想跑 PROD，会出现：

- 水位相互影响
- success 去重相互影响
- 审计台账混在一起
- 失败后难定位到底是哪套环境失败

### 5.3 推荐方案：拆成两套 Function App

推荐程度最高。

建议：

- `Function App A`：只跑 `UAT`
- `Function App B`：只跑 `PROD`

优点：

- 环境隔离最清晰
- watermark / success / audit 完全独立
- 运维和排查最稳
- 不需要在同一个 App 内把函数复制两套

建议同时隔离：

- app settings
- watermark blob name
- forward target
- success / audit 表写入口径
- Function 开关

### 5.4 可选方案：同一个 Function App 内拆两套函数

可以做，但需要代码改造。

示意：

- `function_glass_master_10m_uat`
- `function_glass_master_10m_prod`
- `function_chemical_traceability_5m_uat`
- `function_chemical_traceability_5m_prod`

对应环境变量也要拆成两套，例如：

- `GLASS_MASTER_UAT_FORWARD_TARGET`
- `GLASS_MASTER_UAT_WATERMARK_BLOB_NAME`
- `GLASS_MASTER_UAT_API_APP_ID`
- `GLASS_MASTER_PROD_FORWARD_TARGET`
- `GLASS_MASTER_PROD_WATERMARK_BLOB_NAME`
- `GLASS_MASTER_PROD_API_APP_ID`

同理化学品追溯也要拆成两套。

优点：

- 只维护一个 Function App

缺点：

- 代码复杂度明显增加
- 环境变量数量会翻倍
- 更容易出现配错环境的问题

## 6. 当前建议

### 6.1 对 CSV / 数据库接入 ADX 的建议

优先级建议如下：

1. 如果只是验证：先用 `Get data -> Local file`
2. 如果是持续滚动 CSV：做 `本地 -> Blob -> ADX`
3. 如果客户已经有 MSSQL / MySQL：优先评估 `数据库定时增量同步到 ADX`
4. 如果后面明确要求更实时，再升级 CDC

### 6.2 对 UAT / PROD Function 双跑的建议

优先级建议如下：

1. **推荐**：拆两套 Function App，分别跑 UAT / PROD
2. **次选**：同一 Function App 内复制函数，拆成双入口

不建议：

- 继续让单一函数通过临时改环境变量在 UAT 和 PROD 之间反复切
- 在同一函数里混用一套 watermark / success / audit 去服务两套环境

## 7. 下一步建议

如果客户要继续推进，建议按下面顺序确认：

1. 客户是更倾向继续走 CSV，还是直接改为 MSSQL / MySQL 增量同步。
2. 如果走数据库同步，确认源库类型、表名、增量字段、网络连通方式。
3. 确认 UAT 和 PROD 是否都要长期运行，而不是临时切换。
4. 如果答案是“都要长期运行”，建议立项拆成两套 Function App。

## 8. 适合当前项目的推荐落地

如果现在要尽快给客户一个可执行方向，推荐口径如下：

1. **CSV 不是最终推荐方案**
   - 一次性导数可以
   - 持续文件不建议长期依赖

2. **如果客户已有 MSSQL / MySQL**
   - 更建议做“数据库增量同步到 ADX”
   - 先做分钟级同步
   - 后续再考虑 CDC

3. **Function 双环境**
   - 可以做
   - 但推荐拆成两套 Function App，而不是在当前单套函数上硬共用

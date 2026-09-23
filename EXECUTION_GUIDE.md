# Defect Delivery Pack Execution Guide

## 1. 目标

本执行手册用于客户环境落地下面这条链路：

`EMQX -> Event Hub(iotdefect) -> ADX -> Azure Function -> Tellus/Baiteda 接口`

当前交付包按“全缺陷域架构”设计，但第一批优先完成 `SGH/WS1/Final/5FN6/Defects`。

## 2. 客户侧前置条件

### 2.1 网络

客户 ADX 当前使用内网地址解析，执行 KQL 必须满足以下条件之一：

1. 在客户 VPN 环境下访问
2. 在客户跳板机/堡垒机环境下访问
3. 由客户内部同事代执行 KQL 脚本

### 2.2 权限

执行 ADX 脚本的账号建议至少具备：

1. 数据库 `Admin`
2. 可创建 Data Connection 的 Azure Portal 权限

Azure Function 所需权限：

1. ADX 数据库 `Viewer`
2. Azure Blob `Storage Blob Data Contributor`

## 3. ADX 执行步骤

在客户可访问 ADX 的环境中，连接：

1. Query URI:
   `https://iotdbsekadx01prd.chinanorth3.kusto.chinacloudapi.cn`
2. Database:
   `iotdbsekdb01prd`

按顺序执行：

1. `adx/deploy_units/defect_baseline/01_reference_tables.csl`
2. `adx/deploy_units/defect_baseline/00_raw_tables_and_mappings.csl`
3. `adx/deploy_units/defect_baseline/10_family_raw_functions.csl`
4. `adx/deploy_units/defect_baseline/20_final_standardization.csl`
5. `adx/deploy_units/defect_baseline/30_full_domain_logic_views.csl`
6. `adx/deploy_units/defect_baseline/40_runtime_materialize_and_forward.csl`

## 4. Event Hub -> ADX Data Connection

在 Azure Portal 中进入客户 ADX 数据库，创建 Data Connection：

1. Source type: `Event Hub`
2. Event Hub Namespace: 客户缺陷事件中心命名空间
3. Event Hub: `iotdefect`
4. Consumer Group: 建议 `cg-adx-defect`
5. Target table: `raw_defect_eh`
6. Data format: `JSON`
7. Ingestion mapping: `raw_defect_eh_json`

## 5. EMQX 侧约定

EMQX Rule 当前建议只先订阅：

`SGH/WS1/Final/5FN6/Defects`

推荐输出外壳字段：

1. `enqueued_time`
2. `topic`
3. `device_name`
4. `message_id`
5. `payload`

## 6. 维表初始化

`adx/deploy_units/defect_baseline/01_reference_tables.csl` 中的 `dim_defectlist_5fn6` 已经按现网 `defectlist.xlsx` 第一列固化。

其映射来源与现网 Python 一致，但对未配置 bit 的处理更严格：

1. 只读取 Excel 第一列
2. 行号从 1 开始，对应 `bit_pos`
3. 单元格为空时，该 bit 不写入维表
4. 若标准化时某个 bit 未命中维表，则 `defect_code` 保持空值

如后续客户更新 `defectlist.xlsx`，可重新运行：

`tools/build_5fn6_mapping_from_excel.py`

重新生成 Kusto `datatable` 片段并覆盖维表脚本。

## 7. Function 部署步骤

### 7.1 目录

部署目录：

`function_app/`

预构建打包脚本：

`function_app/build_functionapp_zip.py`

### 7.2 环境变量

必须配置：

1. `ADX_CLUSTER_URL`
2. `ADX_DATABASE`
3. `ADX_TOKEN_SCOPE`
4. `STATE_STORAGE_ACCOUNT_URL`
5. `WATERMARK_CONTAINER_NAME`
6. `INTEGRATION_ACTIVE_ENV`
7. `INTEGRATION_UAT_BASE_URL`
8. `INTEGRATION_PROD_BASE_URL`
9. `INTEGRATION_UAT_APP_ID`
10. `INTEGRATION_UAT_APP_SECRET`
11. `INTEGRATION_PROD_APP_ID`
12. `INTEGRATION_PROD_APP_SECRET`
13. `INTEGRATION_TENANT_ID`

任务级变量请参考：

`APP_SETTINGS_REFERENCE.md`

客户当前中国区 scope 应为：

`https://kusto.kusto.chinacloudapi.cn/.default`

说明：当前默认所有任务共用同一套 ADX 连接；`ADX_CLUSTER_URL`、`ADX_DATABASE`、`ADX_TOKEN_SCOPE` 是公共变量，不是产量单独使用的配置。

`STATE_STORAGE_ACCOUNT_URL` 现在支持三种填法，最终都会按 Blob endpoint 使用：

- `iotdbseksto01prd`
- `https://iotdbseksto01prd.blob.core.chinacloudapi.cn`
- `https://iotdbseksto01prd.dfs.core.chinacloudapi.cn`

如果 `01` 是 Function 可访问的存储账户，就把这个变量改成 `01`，不要再填 `02`。

客户当前接口地址可先填：

`https://sekurit.tellus-uatt.saint-gobain.com.cn`

### 7.3 托管身份授权

Function App 开启 `System Assigned Identity` 后：

1. 在 ADX 数据库授予该身份 `Viewer`
2. 在状态存储账号授予该身份 `Storage Blob Data Contributor`

重要说明：

1. 这里要进入 Function App 左侧的“标识”页面，不是“身份验证”页面。
2. 如果是新建或替换了一个新的 Function App，旧 Function App 上已有的托管标识和授权不会自动继承到新资源。
3. 因此每次更换 Function App 后，都要重新检查：
   - `System Assigned Identity` 是否已开启
   - ADX 权限是否已重新授给新 Function 的身份
   - Blob 权限是否已重新授给新 Function 的身份

ADX 查询页授权示例：

如果只是给查询权限，可在 ADX Query 页面执行：

```kusto
.add database ['iotdbsekdb01prd'] viewers ('aadapp=<新Function托管标识ID>') 'function-mi-viewer'
```

如果当前 Function 还需要执行管理命令、写审计表或按你们现网口径直接给管理员权限，可执行：

```kusto
.add database ['iotdbsekdb01prd'] admins ('aadapp=<新Function托管标识ID>') 'function-mi-admin'
```

说明：

1. `<新Function托管标识ID>` 需替换为新 Function App 在“标识”页中对应的托管标识 ID。
2. 如果 01 Function 之前已经在 ADX 里加过 `admin`，新建的 02 Function 也需要重新单独加一次，不能复用旧资源的授权结果。

### 7.4 预构建部署包

由于客户环境当前 `--build-remote true` 会触发 Azure Oryx 异常，正式包不要依赖远程构建。

应在本机先生成 Linux 兼容部署包，再传到跳板机发布。

在本机执行：

```bash
cd /Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/function_app
python3 build_functionapp_zip.py
```

脚本会：

1. 校验 `host.json`、`function_app.py`、`watermark_store.py` 等必要文件
2. 使用 `docker` 和 `python:3.11-slim` 安装 Linux 兼容依赖
3. 生成预构建 zip：

`customer_rollout/defect_delivery_pack/dist/function_app_prebuilt.zip`

生成后：

1. 将 `function_app_prebuilt.zip` 上传到跳板机桌面
2. 在跳板机执行普通 zip deploy：

```cmd
az functionapp deployment source config-zip --resource-group SekCN-IOTDB-RG01-PRD-N3 --name SekCN-IOTDB-Func01-PRD-N3 --src "%USERPROFILE%\Desktop\function_app_prebuilt.zip"
```

说明：

1. 该 zip 已包含 `.python_packages`
2. 不需要 `--build-remote true`
3. 不会再走有问题的 Oryx remote build

## 8. 验证步骤

### 8.1 验证 EMQX -> Event Hub

1. EMQX 规则命中数增加
2. 动作成功数增加
3. Event Hub `Incoming Messages` 增加

### 8.2 验证 Event Hub -> ADX Raw

执行：

```kusto
raw_defect_eh
| where topic == "SGH/WS1/Final/5FN6/Defects"
| order by enqueued_time desc
| take 20
```

### 8.3 验证 Final 标准化

执行：

```kusto
fn_defect_final_std_v1()
| order by source_time desc
| take 20
```

### 8.4 验证等价层

执行：

```kusto
defect_plc_report_equivalent_v2
| order by source_time desc
| take 20
```

### 8.5 验证下游转发

Function 首轮部署建议：

1. `TRACEABILITY_DEFECT_DRY_RUN=true`
2. 日志确认 ADX 已取到数据
3. 再改为 `TRACEABILITY_DEFECT_DRY_RUN=false`
4. 观察百特搭接口调用结果和 watermark 推进情况
5. 若需要先验证部署成功，可临时查看 Portal 中是否出现 `function_traceability_defect_10m`

### 8.6 Function 命名与频率配置

当前交付包中的 Azure Function 名称已统一改为 `组件_主题_频率` 风格：

1. `function_traceability_defect_10m`
2. `function_glass_master_10m`
3. `function_production_output_10m`
4. `function_production_rollup_2h`

运行频率统一在 Portal 的 App Settings 中维护。
如果客户需要在线上环境覆盖 cron，建议使用新标准变量名：

1. `TRACEABILITY_DEFECT_SCHEDULE`
2. `GLASS_MASTER_SCHEDULE`
3. `PRODUCTION_OUTPUT_SCHEDULE`
4. `PRODUCTION_ROLLUP_SCHEDULE`

单任务停用建议直接使用 Azure 原生变量：

1. `AzureWebJobs.function_traceability_defect_10m.Disabled`
2. `AzureWebJobs.function_glass_master_10m.Disabled`
3. `AzureWebJobs.function_production_output_10m.Disabled`
4. `AzureWebJobs.function_production_rollup_2h.Disabled`

## 9. 当前交付包的边界

### 已完成

1. 全缺陷域架构命名
2. Raw 层
3. 5FN6 Final 标准化
4. 全域统一 equivalent / forward 候选层
5. 方案 B 正式化对象：
   - `defect_std_all_v2`
   - `defect_plc_report_equivalent_v2`
   - `defect_forward_success`
   - `defect_forward_audit`
6. Azure Function 正式骨架

### 后续需要继续补齐

1. `8PA1 EOL` 解析
2. `8PA1 Milling` 解析
3. `WS3` 无缺陷证据逻辑
4. `rfid_prefix28` 业务键回填
5. `defect_name` 中文或业务命名映射

但这些补齐工作都可以在当前架构上继续迭代，不需要推翻对象命名和分层。

# Defect Delivery Pack

## 1. 目录说明

本目录提供客户环境落地缺陷链路的第一批可执行脚本包，遵循下面原则：

1. `ADX` 侧对象命名和分层按“全缺陷域”设计
2. 第一批可执行实现优先覆盖 `SGH/WS1/Final/5FN6/Defects`
3. `8PA1`、`8PA1_Milling`、`WS3` 的对象和函数命名已经预留，后续可直接补逻辑
4. 下游继续复用 `defect_forward_sync.py` 的接口契约和过滤逻辑

## 2. 子目录

### `adx/`

包含 ADX KQL / 管理命令脚本：

1. `shared/`
2. `defect/`
3. `traceability/`
4. `glass_master/`
5. `chemical_traceability/`
6. `print_silk_screen/`
7. `production/`
8. 目录级说明见 `adx/README.md`

### `function_app/`

包含下游转发 Azure Function 的正式骨架：

1. `function_app.py`
2. `runtime_settings.py`
3. `watermark_store.py`
4. `defect_forward_sync.py`
5. `glass_master_forward_sync.py`
6. `prod_output_forward_sync.py`
7. `host.json`
8. `requirements.txt`
9. `local.settings.example.json`
10. `build_functionapp_zip.py`

环境变量标准说明见：

`APP_SETTINGS_REFERENCE.md`

## 3. 执行顺序

### 3.1 ADX 脚本执行顺序

在客户可访问 ADX 的网络环境中，按下面顺序执行：

1. `adx/deploy_units/defect_baseline/01_reference_tables.csl`
2. `adx/deploy_units/defect_baseline/00_raw_tables_and_mappings.csl`
3. `adx/deploy_units/defect_baseline/10_family_raw_functions.csl`
4. `adx/deploy_units/defect_baseline/20_final_standardization.csl`
5. `adx/deploy_units/defect_baseline/30_full_domain_logic_views.csl`
6. `adx/deploy_units/defect_baseline/40_runtime_materialize_and_forward.csl`

### 3.2 Function 部署顺序

1. 在 Function App 中配置环境变量
2. 开启托管身份
3. 给 Function App 授权访问 ADX
4. 给 Function App 授权访问 Azure Blob
5. 在本机执行 `function_app/build_functionapp_zip.py` 生成预构建 zip
6. 将生成的 zip 上传到跳板机
7. 在跳板机上使用普通 `config-zip` 部署

## 4. 当前已实装的部分

### 已实装

1. Raw 表与 JSON mapping
2. 5FN6 专用维表 `dim_defectlist_5fn6`
3. 5FN6 Raw 过滤函数
4. 5FN6 标准化函数
5. 全域统一 schema
6. 全域 union / equivalent / forward candidates 层
7. 方案 B 正式化对象：
   - `defect_std_all_v2`
   - `defect_plc_report_equivalent_v2`
   - `defect_forward_success`
   - `defect_forward_audit`
8. Azure Function 下游转发骨架

其中 `dim_defectlist_5fn6` 已按现网 `defectlist.xlsx` 第一列正式固化：

1. Excel 第 1 行对应 `bit_pos = 1`
2. Excel 第 N 行对应 `bit_pos = N`
3. 空行不入表
4. 若 ADX 标准化时某个 bit 未命中维表，则 `defect_code` 保持空值，不再回退为 `bit_pos`

### 预留但暂未在 KQL 中完全展开

1. `8PA1 EOL` 标准化逻辑
2. `8PA1 Milling` 标准化逻辑
3. `WS3` 标准化逻辑
4. 全域 `rfid_prefix28` 业务键回填逻辑

这些对象已预留固定函数名，后续补逻辑时不需要改架构层对象名。

## 5. 当前建议

先用本脚本包在客户环境完成下面闭环：

1. `EMQX -> Event Hub -> ADX Raw`
2. `5FN6 Final -> 标准化 -> 等价层`
3. `Azure Function -> defect_forward_sync.py -> 下游接口`

等这条链路跑稳，再补 `8PA1`、`WS3` 家族的标准化函数实现。

## 6. 仓库化建议

如果后续把本目录纳入 Git 仓库，建议直接提交下面这些内容：

1. `adx/`
2. `function_app/` 源码
3. `APP_SETTINGS_REFERENCE.md`
4. `EXECUTION_GUIDE.md`
5. `portal_app_settings_标准版.json`
6. `portal_app_settings_仓库模板_脱敏版.json`

不要直接提交带真实线上值的文件或打包产物：

1. `dist/`
2. `adx_delivery_current/`
3. `portal_app_settings_可直接导入_全量兼容版.json`
4. `portal_app_settings_可直接导入_全量兼容版_含print_snowflake.json`
5. `portal_app_settings_print_snowflake_add_only.json`
6. 仓库根外的真实参数文件 `new_parameter.txt`

如果需要基于最新线上参数重新生成一份可入库模板，可以执行：

`python tools/generate_sanitized_app_settings_template.py --input /absolute/path/to/new_parameter.txt --output portal_app_settings_仓库模板_脱敏版.json`

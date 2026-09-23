# ADX KQL 脚本分组说明

本目录现在按“部署单元”分组，不改变任何 ADX 对象名、函数名和业务逻辑。

## 当前链路图

- 源文件：`ADX_当前链路总览.drawio`
- 图片：`ADX_当前链路总览.png`

图里只保留当前正式链路，方便直接看 `table -> function -> result table -> Azure Function/导出` 的层级关系。

## 目录结构

- `deploy_units/defect_baseline/`
  - `01_reference_tables.csl`
  - `00_raw_tables_and_mappings.csl`
  - `10_family_raw_functions.csl`
  - `20_final_standardization.csl`
  - `30_full_domain_logic_views.csl`
  - `40_runtime_materialize_and_forward.csl`
  - 说明：这是缺陷正式主链的基线部署单元，通常一起执行。
- `deploy_units/glass_master/`
  - `42_glass_master_materialize_and_forward.csl`
- `deploy_units/traceability/`
  - `25_traceability_normalize_and_wide_logic.csl`
  - `45_traceability_materialized_tables.csl`
- `deploy_units/chemical_traceability/`
  - `43_chemical_traceability_materialize_and_forward.csl`
- `deploy_units/print_current/`
  - `44_print_snapshot_materialize_current.csl`
- `deploy_units/print_rebuild/`
  - `44_print_snapshot_rebuild_and_backfill.csl`
  - 说明：这不是日常必跑单元，而是 print/std 重建时使用。
- `deploy_units/production/`
  - `00_raw_tables_and_mappings.csl`
  - `01_reference_tables.csl`
  - `20_production_rollup_and_forward.csl`
  - 说明：这里是从 `java_logic_delivery_pack/adx/` 同步过来的产量脚本镜像，当前不改逻辑，只是统一到一个 ADX 目录查看。
- `deploy_units/optional_optimizations/`
  - `90_query_acceleration_policies.csl`
  - 说明：可选运维优化脚本，不属于基线必跑单元。

## 产量链在哪里

当前产量主逻辑可以直接在下面这组里查看：

1. `deploy_units/production/00_raw_tables_and_mappings.csl`
2. `deploy_units/production/01_reference_tables.csl`
3. `deploy_units/production/20_production_rollup_and_forward.csl`

对应链路是：

`raw_ops_eh -> prod_output_minute/hour/day/month -> fn_prod_tellus_forward_candidates_with_watermark_v1 -> Azure Function -> prod_tellus_forward_success / prod_tellus_forward_audit`

这里要特别说明：

1. `prod_output_minute` 不是临时视图，而是产量接口发送的核心业务底表。
2. `prod_output_hour/day/month` 也是这条链沉淀下来的正式结果表。
3. Azure Function 不是替代表存储，它只是读取候选、组接口报文并把发送结果写回成功/审计台账。

## 为什么改成部署单元分组

这次不再继续细拆成数仓层目录，改成部署单元分组，原因是：

1. 更贴近 ADX 官方文档偏“脚本可部署、数量不要过碎”的思路；
2. 现场执行时更清楚“一组脚本一起跑”；
3. 这套项目里 function、table、policy 依赖很深，过细拆分会让执行顺序更脆弱。

## 常用执行顺序

### 缺陷正式链

1. `deploy_units/defect_baseline/01_reference_tables.csl`
2. `deploy_units/defect_baseline/00_raw_tables_and_mappings.csl`
3. `deploy_units/defect_baseline/10_family_raw_functions.csl`
4. `deploy_units/defect_baseline/20_final_standardization.csl`
5. `deploy_units/defect_baseline/30_full_domain_logic_views.csl`
6. `deploy_units/defect_baseline/40_runtime_materialize_and_forward.csl`

### 缺陷主数据链

在缺陷正式链基础对象可用后执行：

1. `deploy_units/glass_master/42_glass_master_materialize_and_forward.csl`

### Traceability 链

依赖缺陷等价层对象：

1. `deploy_units/traceability/25_traceability_normalize_and_wide_logic.csl`
2. `deploy_units/traceability/45_traceability_materialized_tables.csl`

### 化学品追溯链

1. `deploy_units/chemical_traceability/43_chemical_traceability_materialize_and_forward.csl`

### Print / 丝网链

1. `deploy_units/print_current/44_print_snapshot_materialize_current.csl`
2. 如需重建 std 表，再执行 `deploy_units/print_rebuild/44_print_snapshot_rebuild_and_backfill.csl`

### 产量链

1. `deploy_units/production/00_raw_tables_and_mappings.csl`
2. `deploy_units/production/01_reference_tables.csl`
3. `deploy_units/production/20_production_rollup_and_forward.csl`

### 可选优化脚本

1. `deploy_units/optional_optimizations/90_query_acceleration_policies.csl`

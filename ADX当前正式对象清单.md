# ADX 当前正式对象清单

本文件用于说明当前应交付给客户的 ADX 内容范围。

原则：

1. 只交付当前正式主链对应的对象命名。
2. 已删除的旧缺陷单链、视图验证链，不再作为交付内容提供。
3. `print` 链当前以补充脚本方式单独提供。
4. `traceability_wide` 相关对象当前保留，仍可作为交付对象说明。

## 1. 当前正式链路

### 1.1 缺陷正式发送链

当前正式链路：

`raw_defect_eh -> defect_std_all_v2 -> defect_plc_report_equivalent_v2 -> fn_defect_forward_candidates_scheme_b_v2 -> Azure Function`

建议交付对象：

- 表：
  - `raw_defect_eh`
  - `defect_std_all_v2`
  - `defect_plc_report_equivalent_v2`
  - `defect_forward_success`
  - `defect_forward_audit`
- 函数：
  - `fn_defect_forward_candidates_scheme_b_v2`
  - `fn_defect_std_all_v2_materialize_v1`
  - `fn_defect_plc_report_equivalent_v2_materialize_v1`
  - `fn_defect_std_union_v1`
  - 缺陷标准化底座函数 `fn_defect_*_raw_v1` / `fn_defect_*_std_v1`
- 策略：
  - `defect_std_all_v2` update policy
  - `defect_plc_report_equivalent_v2` update policy
- Mapping：
  - `raw_defect_eh_json`

### 1.2 缺陷主数据链

当前正式链路：

`raw_defect_eh -> glass_master -> fn_glass_master_forward_candidates -> Azure Function`

建议交付对象：

- 表：
  - `glass_master`
  - `glass_master_forward_success`
  - `glass_master_forward_audit`
- 函数：
  - `fn_glass_master_message_base`
  - `fn_glass_master_materialize`
  - `fn_glass_master_forward_candidates`
- 策略：
  - `glass_master` update policy

### 1.3 产量发送链

当前正式链路：

`raw_ops_eh -> prod_output_minute/hour/day/month -> fn_prod_tellus_forward_candidates_with_watermark_v1 -> Azure Function -> prod_tellus_forward_success / prod_tellus_forward_audit`

说明：

1. `prod_output_minute` 是产量接口发送的核心业务底表，不是临时函数结果。
2. `prod_output_hour / day / month` 是同一条产量链沉淀下来的汇总结果表。
3. Azure Function 只是读取候选、组包并调用接口，发送成功和审计记录仍然会写回 ADX 台账表。

建议交付对象：

- 表：
  - `raw_ops_eh`
  - `dim_prod_topic_registry`
  - `prod_output_minute`
  - `prod_output_hour`
  - `prod_output_day`
  - `prod_output_month`
  - `prod_recipe_change_event`
  - `prod_recipe_interval`
  - `prod_output_by_recipe`
  - `prod_tellus_forward_success`
  - `prod_tellus_forward_audit`
- 函数：
  - `fn_prod_output_counter_raw_v1`
  - `fn_prod_recipe_change_raw_v1`
  - `fn_prod_recipe_events_v1`
  - `fn_prod_recipe_intervals_v1`
  - `fn_prod_counter_minute_rebuild_v1`
  - `fn_prod_counter_hour_from_minute_v1`
  - `fn_prod_counter_day_from_hour_v1`
  - `fn_prod_counter_month_from_day_v1`
  - `fn_prod_output_by_recipe_v1`
  - `fn_prod_tellus_forward_candidates_v1`
  - `fn_prod_tellus_forward_candidates_with_success_v1`
  - `fn_prod_tellus_forward_candidates_with_watermark_v1`
- Mapping：
  - `raw_ops_eh_json`

### 1.4 Print 链

当前链路：

`raw_print_eh -> print_snapshot_std`

说明：

1. `print` 链当前已经在线上存在 raw / std / function / update policy。
2. 该链尚未完整回写到现有 ADX baseline 目录，因此本次单独补一份脚本：
   [adx/deploy_units/print_current/44_print_snapshot_materialize_current.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/print_current/44_print_snapshot_materialize_current.csl)

建议交付对象：

- 表：
  - `raw_print_eh`
  - `print_snapshot_std`
- 函数：
  - `fn_print_snapshot_materialize`
- 策略：
  - `print_snapshot_std` update policy
- Mapping：
  - `raw_print_eh_json`

### 1.5 Traceability 宽表链

当前决定：保留。

建议交付对象：

- 表：
  - `raw_traceability_eh`
  - `traceability_normalized_events_v2`
  - `traceability_wide_v2`
  - `traceability_final_defect_agg_v1`
  - `traceability_milling_defect_agg_v1`
- 函数：
  - 全部 `fn_traceability_*`
- Mapping：
  - `raw_traceability_eh_json`

## 2. 建议交付给客户的脚本目录

### 2.1 直接交付的 baseline 脚本

缺陷/主数据：

- [adx/deploy_units/defect_baseline/01_reference_tables.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/defect_baseline/01_reference_tables.csl)
- [adx/deploy_units/defect_baseline/00_raw_tables_and_mappings.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/defect_baseline/00_raw_tables_and_mappings.csl)
- [adx/deploy_units/defect_baseline/10_family_raw_functions.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/defect_baseline/10_family_raw_functions.csl)
- [adx/deploy_units/defect_baseline/20_final_standardization.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/defect_baseline/20_final_standardization.csl)
- [adx/deploy_units/traceability/25_traceability_normalize_and_wide_logic.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/traceability/25_traceability_normalize_and_wide_logic.csl)
- [adx/deploy_units/defect_baseline/30_full_domain_logic_views.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/defect_baseline/30_full_domain_logic_views.csl)
- [adx/deploy_units/defect_baseline/40_runtime_materialize_and_forward.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/defect_baseline/40_runtime_materialize_and_forward.csl)
- [adx/deploy_units/glass_master/42_glass_master_materialize_and_forward.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/glass_master/42_glass_master_materialize_and_forward.csl)
- [adx/deploy_units/traceability/45_traceability_materialized_tables.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/traceability/45_traceability_materialized_tables.csl)
- [adx/deploy_units/optional_optimizations/90_query_acceleration_policies.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/optional_optimizations/90_query_acceleration_policies.csl)
- [adx/deploy_units/print_current/44_print_snapshot_materialize_current.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/print_current/44_print_snapshot_materialize_current.csl)
- [adx/deploy_units/chemical_traceability/43_chemical_traceability_materialize_and_forward.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/chemical_traceability/43_chemical_traceability_materialize_and_forward.csl)

产量：

- [adx/deploy_units/production/00_raw_tables_and_mappings.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/production/00_raw_tables_and_mappings.csl)
- [adx/deploy_units/production/01_reference_tables.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/production/01_reference_tables.csl)
- [adx/deploy_units/production/20_production_rollup_and_forward.csl](file:///Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/adx/deploy_units/production/20_production_rollup_and_forward.csl)

## 3. 建议一起交付的线上导出文件

建议保留并交付：

- [export (1).xlsx](file:///Users/gaokuang/Desktop/圣戈班/export%20%281%29.xlsx)：`defect_std_all_v2` update policy
- [export (2).xlsx](file:///Users/gaokuang/Desktop/圣戈班/export%20%282%29.xlsx)：`defect_plc_report_equivalent_v2` update policy
- [export (3).xlsx](file:///Users/gaokuang/Desktop/圣戈班/export%20%283%29.xlsx)：`glass_master` update policy
- [export (4).xlsx](file:///Users/gaokuang/Desktop/圣戈班/export%20%284%29.xlsx)：`print_snapshot_std` update policy
- [export (6).xlsx](file:///Users/gaokuang/Desktop/圣戈班/export%20%286%29.xlsx)：`raw_defect_eh_json`
- [export (7).xlsx](file:///Users/gaokuang/Desktop/圣戈班/export%20%287%29.xlsx)：`raw_ops_eh_json`
- [export (8).xlsx](file:///Users/gaokuang/Desktop/圣戈班/export%20%288%29.xlsx)：`raw_print_eh_json`

不建议直接交付：

- [export.xlsx](file:///Users/gaokuang/Desktop/圣戈班/export.xlsx)：删除前函数全集，仍含旧对象
- [export (5).xlsx](file:///Users/gaokuang/Desktop/圣戈班/export%20%285%29.xlsx)：删除前整库 schema script，噪音较大

## 4. 不建议直接给客户的内容

以下内容不建议作为“当前正式命名”直接给客户：

1. 删除前导出的全函数清单
2. 删除前导出的整库 schema script
3. 历史交付包目录 `delivery_bundle_20260708*`
4. 包含已删除对象的旧说明文档或截图

## 5. 推荐最终交付结构

建议整理成如下目录后再发给客户：

```text
adx_delivery_current/
  01_current_scripts/
    shared/
    defect/
    production/
    traceability/
    glass_master/
    chemical_traceability/
    print_silk_screen/
  02_live_policies_and_mappings/
    defect_std_all_v2_update_policy.xlsx
    defect_plc_report_equivalent_v2_update_policy.xlsx
    glass_master_update_policy.xlsx
    print_snapshot_std_update_policy.xlsx
    raw_defect_eh_mapping.xlsx
    raw_ops_eh_mapping.xlsx
    raw_print_eh_mapping.xlsx
  03_current_object_inventory/
    ADX当前正式对象清单.md
```

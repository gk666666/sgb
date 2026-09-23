# 客户交付 ADX 说明

本目录用于给客户说明当前正式使用的 ADX 链路对象、脚本和线上导出配置。

## 目录说明

### 1. `01_current_scripts`

当前正式使用的 ADX 脚本。

- `deploy_units/`
  - `defect_baseline/`：缺陷正式基线部署单元
  - `glass_master/`：缺陷主数据部署单元
  - `traceability/`：traceability 部署单元
  - `chemical_traceability/`：化学品追溯部署单元
  - `print_current/`：print / 丝网当前脚本
  - `print_rebuild/`：print / 丝网重建脚本
  - `production/`：产量部署单元
  - `optional_optimizations/`：可选优化脚本

### 2. `02_live_policies_and_mappings`

来自当前线上 ADX 的导出文件，已经按用途重命名。

包括：

- 正式 update policy
- raw ingestion mapping

### 3. `03_current_object_inventory`

当前正式对象清单。

建议客户优先查看：

- `ADX当前正式对象清单.md`

## 使用建议

1. 如需了解当前正式命名，请先看 `03_current_object_inventory/ADX当前正式对象清单.md`
2. 如需查看当前脚本，请看 `01_current_scripts`
3. 如需查看线上 policy / mapping，请看 `02_live_policies_and_mappings`

## 说明

1. 本次已删除旧缺陷单链和视图验证链，因此不再提供删除前的全量函数导出和整库 schema 导出。
2. `print` 链原本不在 baseline ADX 目录中，本次已补充为 `deploy_units/print_current/44_print_snapshot_materialize_current.csl`。

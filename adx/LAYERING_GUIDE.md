# ADX 分组说明

这份文件保留为设计说明，但当前目录的正式组织方式已经切换为“按部署单元分组”。

## 当前采用的方式

当前正式目录结构见：

`README.md`

核心思路是：

1. 一条通常需要一起执行的链路，放到一个部署单元里；
2. 可选重建脚本、优化脚本单独成组；
3. 不再继续把目录按数仓层细拆。

## 为什么不继续按数仓层细拆

主要原因不是分层思路不对，而是对 ADX 这套脚本来说，部署单元比纯层次目录更重要：

1. table、function、update policy 强依赖明显；
2. 现场执行更关心“这一组要一起跑哪些文件”；
3. ADX 官方文档更强调脚本幂等、可部署，以及不要把 script 切得太碎。

## 现在的部署单元

- `deploy_units/defect_baseline/`
- `deploy_units/glass_master/`
- `deploy_units/traceability/`
- `deploy_units/chemical_traceability/`
- `deploy_units/print_current/`
- `deploy_units/print_rebuild/`
- `deploy_units/production/`
- `deploy_units/optional_optimizations/`

## 备注

如果以后某条链继续变复杂，仍然可以在该部署单元内部再补充：

1. 对象说明；
2. 执行顺序；
3. 单元内子分组。

但当前不再把整个 `adx/` 目录继续推向“纯数仓层目录”的方向。

# Function 打包与解压检查步骤

## 1. 适用范围

本文档记录当前 `defect_delivery_pack/function_app` 的实际打包方式、生成物位置，以及查看 zip 包内源码和依赖的实际命令。

当前打包脚本：

- `customer_rollout/defect_delivery_pack/function_app/build_functionapp_zip.py`

当前最新包：

- `customer_rollout/defect_delivery_pack/dist/function_app_defect_v2_schemeb2_emqxts_v2.zip`

当前已解压目录：

- `customer_rollout/defect_delivery_pack/dist/function_app_defect_v2_schemeb2_emqxts_v2_unpacked`

## 2. 打包前提

需要本机具备：

- `python3`
- `docker`

打包脚本会：

1. 复制 `function_app/` 下源码到临时目录
2. 使用 `python:3.11-slim` 在 Docker 内安装 Linux 兼容依赖
3. 将源码和 `.python_packages` 一起打成 zip

## 3. 生成预构建包

进入目录：

```bash
cd /Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/function_app
```

使用默认输出名打包：

```bash
python3 build_functionapp_zip.py
```

默认生成：

- `/Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/dist/function_app_prebuilt.zip`

如果要生成带版本名的新包：

```bash
cd /Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/function_app
python3 build_functionapp_zip.py --output /Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/dist/function_app_defect_v2_schemeb2_emqxts_v2.zip
```

## 4. 解压最新包查看内容

把最新包解压到固定目录：

```bash
python3 - <<'PY'
from pathlib import Path
import shutil
import zipfile

zip_path = Path("/Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/dist/function_app_defect_v2_schemeb2_emqxts_v2.zip")
out_dir = Path("/Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/dist/function_app_defect_v2_schemeb2_emqxts_v2_unpacked")

if out_dir.exists():
    shutil.rmtree(out_dir)
out_dir.mkdir(parents=True, exist_ok=True)

with zipfile.ZipFile(zip_path) as zf:
    zf.extractall(out_dir)

print(out_dir)
PY
```

## 5. 查看解压后的顶层文件

```bash
cd /Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/dist/function_app_defect_v2_schemeb2_emqxts_v2_unpacked
ls -la
```

通常可以看到：

- `function_app.py`
- `watermark_store.py`
- `defect_forward_sync.py`
- `glass_master_forward_sync.py`
- `prod_output_forward_sync.py`
- `host.json`
- `requirements.txt`
- `.python_packages/`

## 6. 查看依赖实际装在哪里

依赖实际被打进 zip 的目录是：

- `.python_packages/lib/site-packages`

查看依赖目录：

```bash
cd /Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/dist/function_app_defect_v2_schemeb2_emqxts_v2_unpacked
ls -la .python_packages/lib/site-packages
```

如果只想看前几十项：

```bash
cd /Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/dist/function_app_defect_v2_schemeb2_emqxts_v2_unpacked
find .python_packages/lib/site-packages -maxdepth 1 | sort | head -n 50
```

常见会看到：

- `azure/`
- `requests/`
- `urllib3/`
- `certifi/`
- `charset_normalizer/`
- `idna/`
- 以及对应的 `*.dist-info`

## 7. 查看某个依赖是否真的进包

例如检查 `requests`：

```bash
cd /Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/dist/function_app_defect_v2_schemeb2_emqxts_v2_unpacked
find .python_packages/lib/site-packages -maxdepth 1 -name 'requests*'
```

例如检查 `azure.identity`：

```bash
cd /Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/dist/function_app_defect_v2_schemeb2_emqxts_v2_unpacked
find .python_packages/lib/site-packages -maxdepth 2 -path '*azure/identity*' | head -n 20
```

## 8. 查看 zip 里有哪些文件但不解压

```bash
python3 - <<'PY'
from zipfile import ZipFile

zip_path = "/Users/gaokuang/Desktop/圣戈班/customer_rollout/defect_delivery_pack/dist/function_app_defect_v2_schemeb2_emqxts_v2.zip"
with ZipFile(zip_path) as zf:
    for name in zf.namelist()[:200]:
        print(name)
PY
```

## 9. 部署到 Azure Function

把 zip 上传到目标环境后，执行：

```bash
az functionapp deployment source config-zip \
  --resource-group SekCN-IOTDB-RG01-PRD-N3 \
  --name SekCN-IOTDB-Func01-PRD-N3 \
  --src /path/to/function_app_defect_v2_schemeb2_emqxts_v2.zip
```

## 10. 说明

当前打包方式不是远端构建，而是本地预构建：

1. Docker 只负责本地安装 Linux 兼容依赖
2. 线上 Azure Function 直接运行 zip 内的源码和 `.python_packages`
3. 临时构建目录会在脚本结束后自动删除，因此平时看不到中间目录
4. 如果要检查依赖内容，最直接的方法是解压最终 zip 查看

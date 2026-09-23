#!/usr/bin/env python3
# 这个脚本用于在本地生成 Azure Function 的预构建部署包。
# 设计目标是：
# 1) 把当前 function_app 源码拷到临时 stage 目录；
# 2) 用 Docker 安装 Linux 兼容依赖到 .python_packages；
# 3) 再把 stage 目录整体压成可直接 config-zip 部署的包。
#
# 之所以用 Docker，而不是直接本机 pip install，
# 是为了尽量保证依赖和 Azure Linux Function 运行环境兼容。
import argparse
import shutil
import subprocess
import tempfile
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


ROOT = Path(__file__).resolve().parent
DIST_DIR = ROOT.parent / "dist"
DEFAULT_OUTPUT = DIST_DIR / "function_app_prebuilt.zip"
REQUIRED_FILES = (
    "function_app.py",
    "runtime_settings.py",
    "watermark_store.py",
    "chemical_traceability_forward_sync.py",
    "defect_forward_sync.py",
    "glass_master_forward_sync.py",
    "prod_output_forward_sync.py",
    "silk_screen_forward_sync.py",
    "print_adx_export_to_blob.py",
    "print_blob_to_snowflake_loader.py",
    "host.json",
    "requirements.txt",
)
SKIP_NAMES = {
    "__pycache__",
    ".python_packages",
    ".venv",
    "dist",
    ".DS_Store",
    "function_app.py.zip",
    "build_functionapp_zip.py",
    "local.settings.example.json",
}
SKIP_SUFFIXES = {".pyc", ".pyo"}


def parse_args() -> argparse.Namespace:
    # 输出路径、Docker 镜像、平台都做成参数，
    # 方便后续在不同机器或不同目标环境下复用。
    parser = argparse.ArgumentParser(
        description="Build a Linux-compatible Azure Functions zip package without relying on remote Oryx build."
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help=f"Output zip path. Defaults to {DEFAULT_OUTPUT}",
    )
    parser.add_argument(
        "--docker-image",
        default="python:3.11-slim",
        help="Docker image used to install Linux-compatible dependencies.",
    )
    parser.add_argument(
        "--docker-platform",
        default="linux/amd64",
        help="Docker platform used for dependency build. Defaults to Azure-compatible linux/amd64.",
    )
    return parser.parse_args()


def ensure_prerequisites() -> None:
    # 先校验打包最基本的源码文件和 Docker 是否存在，
    # 避免跑到中途才发现缺文件或本地无法构建 Linux 依赖。
    missing = [name for name in REQUIRED_FILES if not (ROOT / name).exists()]
    if missing:
        raise SystemExit(f"Missing required files in {ROOT}: {', '.join(missing)}")
    if shutil.which("docker") is None:
        raise SystemExit(
            "Docker is required to build Linux-compatible dependencies for the target Function App."
        )


def copy_source_tree(target_dir: Path) -> None:
    # 只拷贝真正要进入部署包的源码和配置，
    # 缓存目录、旧 dist 和本地虚拟环境都直接跳过。
    for item in ROOT.iterdir():
        if item.name in SKIP_NAMES or item.suffix in SKIP_SUFFIXES:
            continue
        if item.is_dir():
            shutil.copytree(item, target_dir / item.name)
        else:
            shutil.copy2(item, target_dir / item.name)


def install_linux_dependencies(stage_dir: Path, docker_image: str, docker_platform: str) -> None:
    # 依赖安装目标目录必须是 Azure Functions 识别的
    # .python_packages/lib/site-packages。
    package_dir = stage_dir / ".python_packages" / "lib" / "site-packages"
    package_dir.mkdir(parents=True, exist_ok=True)

    command = [
        "docker",
        "run",
        "--rm",
        "--platform",
        docker_platform,
        "-v",
        f"{ROOT}:/src:ro",
        "-v",
        f"{stage_dir}:/out",
        docker_image,
        "/bin/sh",
        "-lc",
        (
            "python -m pip install --upgrade pip && "
            "python -m pip install --no-cache-dir "
            "-r /src/requirements.txt "
            "-t /out/.python_packages/lib/site-packages"
        ),
    ]
    subprocess.run(command, check=True)


def create_zip(stage_dir: Path, output_path: Path) -> None:
    # 把 stage 目录内的最终内容完整压入 zip。
    # 这里不再做额外过滤，因为前面拷贝和依赖安装时已经收口过一次。
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output_path, "w", compression=ZIP_DEFLATED) as archive:
        for file_path in sorted(stage_dir.rglob("*")):
            if file_path.is_dir():
                continue
            archive.write(file_path, file_path.relative_to(stage_dir))


def main() -> int:
    # 整体流程：
    # 1) 校验前置条件；
    # 2) 创建临时 stage；
    # 3) 拷源码；
    # 4) 安装 Linux 依赖；
    # 5) 生成最终 zip。
    args = parse_args()
    ensure_prerequisites()
    output_path = Path(args.output).expanduser().resolve()

    with tempfile.TemporaryDirectory(prefix="function-app-build-") as temp_dir:
        stage_dir = Path(temp_dir) / "stage"
        stage_dir.mkdir(parents=True, exist_ok=True)
        copy_source_tree(stage_dir)
        install_linux_dependencies(stage_dir, args.docker_image, args.docker_platform)
        create_zip(stage_dir, output_path)

    print(f"Prebuilt package created: {output_path}")
    print("Upload this zip to the jump host, then deploy it with plain config-zip.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        raise SystemExit(f"Docker build command failed with exit code {exc.returncode}") from exc

from .control import classify_process
from .control import hash_process
from .control import zip_process
from .control import zip_hash_process
from .control import unzip_process
from .control import unzip_hash_process
from .control import upload_process
from .control import delete_process

# 新增的单文件处理模块
from .control import (
    FileProcessorCoordinator,
    CoordinatorConfig,
    get_coordinator
)
from .config import DiskSpaceConfig, PathConfig


def main():
    """
    原始批处理流程 - 保持向后兼容
    """
    result = classify_process()
    result = hash_process()
    result = zip_process()
    result = zip_hash_process()
    result = unzip_process()
    result = unzip_hash_process()
    result = upload_process()
    result = delete_process()
    print(result)


def main_single_file(
    enable_classify: bool = True,
    max_files: int | None = None,
    max_disk_usage_gb: float = 80.0,
    parallel_workers: int = 1,
    upload_workers: int = 2
):
    """
    单文件处理流程 - 新的优化版本

    优势:
    1. 逐个处理文件，降低磁盘空间占用
    2. 磁盘空间管理，防止空间不足
    3. 异步上传，与本地处理解耦
    4. 路径标准化，统一存储结构

    Args:
        enable_classify: 是否启用分类（首次运行需要）
        max_files: 最大处理文件数，None表示处理所有
        max_disk_usage_gb: 最大磁盘使用阈值（GB）
        parallel_workers: 并行处理数量
        upload_workers: 上传服务worker数量
    """
    import logging

    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 创建配置
    coordinator_config = CoordinatorConfig(
        enable_classify=enable_classify,
        parallel_workers=parallel_workers,
        upload_workers=upload_workers,
    )

    disk_config = DiskSpaceConfig(
        max_disk_usage_gb=max_disk_usage_gb,
        enabled=True,
    )

    path_config = PathConfig()

    # 创建并运行协调器
    coordinator = get_coordinator(
        config=coordinator_config,
        disk_config=disk_config,
        path_config=path_config,
    )

    print("=" * 60)
    print("Starting single-file processing workflow")
    print(f"  - Max disk usage: {max_disk_usage_gb} GB")
    print(f"  - Parallel workers: {parallel_workers}")
    print(f"  - Upload workers: {upload_workers}")
    print(f"  - Enable classify: {enable_classify}")
    print("=" * 60)

    stats = coordinator.run_full_cycle(max_files=max_files)

    print("=" * 60)
    print("Processing completed!")
    print(f"  - Total files: {stats.total_files}")
    print(f"  - Success: {stats.success_count}")
    print(f"  - Failed: {stats.failure_count}")
    print(f"  - Skipped: {stats.skipped_count}")
    print(f"  - Elapsed time: {stats.elapsed_time:.2f} seconds")
    print(f"  - Success rate: {stats.success_rate:.2f}%")
    print("=" * 60)

    return stats


if __name__ == '__main__':
    # 默认使用新的单文件处理流程
    main_single_file()

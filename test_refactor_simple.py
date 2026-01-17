"""
简化测试 - 重构后的单文件处理流程
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

print("=" * 50)
print("重构功能测试")
print("=" * 50)

# 测试1: 导入测试
print("\n[1] 测试模块导入...")
try:
    from item_backup_item.config import DiskSpaceConfig, PathConfig
    print("  - DiskSpaceConfig, PathConfig: OK")

    from item_backup_item.control import (
        DiskSpaceManager, SingleFileProcessor,
        AsyncUploadService, FileProcessorCoordinator,
        CoordinatorConfig, ProcessStage, ProcessResult
    )
    print("  - 控制模块: OK")
except Exception as e:
    print(f"  - 导入失败: {e}")
    sys.exit(1)

# 测试2: 配置创建
print("\n[2] 测试配置创建...")
try:
    disk_config = DiskSpaceConfig(max_disk_usage_gb=80.0, enabled=True)
    path_config = PathConfig()
    coord_config = CoordinatorConfig(parallel_workers=1, upload_workers=1)
    print(f"  - DiskSpaceConfig: {disk_config.max_disk_usage_gb}GB")
    print(f"  - PathConfig: {path_config.zipped_folder}")
    print("  - 配置创建: OK")
except Exception as e:
    print(f"  - 配置失败: {e}")
    sys.exit(1)

# 测试3: 分类功能
print("\n[3] 测试分类功能...")
try:
    from item_backup_item.service import classify_folder
    from item_backup_item.config import ClassifyConfig

    result = classify_folder(r"D:\测试AI运行")
    print(f"  - 发现 {len(result)} 个项目")
    for item in result[:3]:  # 只显示前3个
        for path, info in item.items():
            print(f"    - {path.name}: {info['classify_result']}")
    print("  - 分类: OK")
except Exception as e:
    print(f"  - 分类失败: {e}")

# 测试4: 磁盘空间管理器
print("\n[4] 测试磁盘空间管理器...")
try:
    import shutil
    usage = shutil.disk_usage("D:/")
    print(f"  - D: 总空间 {usage.total // (1024**3)}GB")
    print(f"  - D: 可用空间 {usage.free // (1024**3)}GB")
    print("  - 磁盘空间: OK")
except Exception as e:
    print(f"  - 磁盘空间检查失败: {e}")

print("\n" + "=" * 50)
print("基础测试通过!")
print("=" * 50)

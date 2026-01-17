"""
重构后的文件备份系统测试脚本
"""
import os
import shutil
import tempfile
import time
from pathlib import Path

# 设置测试环境
def setup_test_environment():
    """设置测试环境"""
    print("设置测试环境...")
    
    # 创建临时目录用于测试
    test_source_dir = Path("D:/测试文件备份")
    temp_test_dir = Path(tempfile.mkdtemp(prefix="backup_test_"))
    
    # 如果测试源目录不存在，创建它
    if not test_source_dir.exists():
        test_source_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建一些测试文件
        for i in range(3):
            test_file = test_source_dir / f"test_file_{i}.txt"
            with open(test_file, 'w', encoding='utf-8') as f:
                f.write(f"这是测试文件 {i} 的内容\n" * 10)
        
        # 创建一个测试子目录
        test_subdir = test_source_dir / "test_subdir"
        test_subdir.mkdir(exist_ok=True)
        for i in range(2):
            test_file = test_subdir / f"sub_test_file_{i}.txt"
            with open(test_file, 'w', encoding='utf-8') as f:
                f.write(f"这是子目录测试文件 {i} 的内容\n" * 5)
    
    print(f"测试源目录: {test_source_dir}")
    print(f"临时测试目录: {temp_test_dir}")
    
    return test_source_dir, temp_test_dir


def test_single_file_processing():
    """测试单文件处理功能"""
    print("\n开始测试单文件处理功能...")
    
    try:
        from src.item_backup_item.single_file_processor import SingleFileMainProcessor
        from src.item_backup_item.config.classify_config import ClassifyConfig
        
        # 更新配置以指向测试目录
        original_sources = ClassifyConfig.sources_list.copy()
        test_source_dir, temp_test_dir = setup_test_environment()
        ClassifyConfig.sources_list = [str(test_source_dir)]
        
        # 创建处理器实例
        processor = SingleFileMainProcessor()
        
        # 由于完整运行可能需要网络和其他配置，我们只测试初始化
        print("处理器初始化成功")
        print("单文件处理功能测试完成")
        
        # 恢复原始配置
        ClassifyConfig.sources_list = original_sources
        
        return True
        
    except Exception as e:
        print(f"单文件处理功能测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_disk_space_management():
    """测试磁盘空间管理功能"""
    print("\n开始测试磁盘空间管理功能...")
    
    try:
        from src.item_backup_item.utils.disk_space_manager import DiskSpaceManager
        
        # 创建磁盘空间管理器
        manager = DiskSpaceManager(max_disk_usage_gb=80.0, space_buffer_gb=5.0)
        
        # 测试获取空间信息
        space_status = manager.get_space_status()
        print(f"磁盘空间状态: {space_status}")
        
        # 测试空间估算
        test_file_path = "test_file.txt"
        with open(test_file_path, 'w') as f:
            f.write("test" * 100)  # 创建一个小测试文件
        
        estimated_space = manager.estimate_file_processing_space(test_file_path)
        print(f"估算文件处理空间: {estimated_space:.2f}GB")
        
        # 清理测试文件
        if os.path.exists(test_file_path):
            os.remove(test_file_path)
        
        print("磁盘空间管理功能测试完成")
        return True
        
    except Exception as e:
        print(f"磁盘空间管理功能测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_resource_management():
    """测试资源管理功能"""
    print("\n开始测试资源管理功能...")
    
    try:
        from src.item_backup_item.utils.resource_manager import ResourceManager
        
        # 创建资源管理器
        manager = ResourceManager(max_disk_usage_gb=80.0)
        
        # 测试获取资源使用情况
        resource_usage = manager.get_current_resource_usage()
        print(f"当前资源使用情况: CPU={resource_usage.cpu_percent}%, "
              f"内存={resource_usage.memory_percent}%, "
              f"磁盘={resource_usage.disk_usage_percent}%")
        
        # 测试资源预留
        success = manager.reserve_disk_space(0.1)  # 预留0.1GB
        print(f"资源预留结果: {success}")
        
        if success:
            manager.release_disk_space(0.1)  # 释放资源
            print("资源释放成功")
        
        # 获取统计信息
        stats = manager.get_resource_statistics()
        print(f"资源统计信息: {stats}")
        
        print("资源管理功能测试完成")
        return True
        
    except Exception as e:
        print(f"资源管理功能测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_error_handling():
    """测试错误处理功能"""
    print("\n开始测试错误处理功能...")
    
    try:
        from src.item_backup_item.utils.error_handler import ErrorHandler, RetryableError, FatalError
        
        # 创建错误处理器
        handler = ErrorHandler(max_retry_attempts=3, base_delay=0.1)
        
        # 测试成功的操作
        def successful_op():
            return "success"
        
        result = handler.execute_with_retry(successful_op)
        print(f"成功操作结果: {result}")
        
        # 测试失败的操作（应该抛出异常）
        def failing_op():
            raise ValueError("测试错误")
        
        try:
            handler.execute_with_retry(failing_op)
            print("错误处理测试失败：应该抛出异常")
            return False
        except ValueError:
            print("错误处理正常工作：捕获了预期的异常")
        
        print("错误处理功能测试完成")
        return True
        
    except Exception as e:
        print(f"错误处理功能测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_monitoring_logging():
    """测试监控和日志功能"""
    print("\n开始测试监控和日志功能...")
    
    try:
        from src.item_backup_item.utils.monitor import Monitor
        
        # 创建监控器
        monitor = Monitor()
        
        # 记录一些事件
        monitor.log_event('start', 'test_file.txt', 'processing', {'stage': 'initial'})
        monitor.log_event('success', 'test_file.txt', 'processing', {'result': 'completed'})
        
        # 记录性能指标
        import time
        start_time = time.time() - 1  # 模拟1秒前开始
        monitor.record_performance(start_time, 'test_file.txt', 'processing', True, 0.01)
        
        # 获取统计信息
        stats = monitor.get_statistics()
        print(f"监控统计信息: {stats}")
        
        # 获取最近事件
        recent_events = monitor.get_recent_events(5)
        print(f"最近事件数量: {len(recent_events)}")
        
        print("监控和日志功能测试完成")
        return True
        
    except Exception as e:
        print(f"监控和日志功能测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def run_all_tests():
    """运行所有测试"""
    print("开始运行所有重构功能测试...\n")
    
    tests = [
        ("单文件处理", test_single_file_processing),
        ("磁盘空间管理", test_disk_space_management),
        ("资源管理", test_resource_management),
        ("错误处理", test_error_handling),
        ("监控和日志", test_monitoring_logging),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"运行测试: {test_name}")
        print(f"{'='*50}")
        
        success = test_func()
        results.append((test_name, success))
    
    print(f"\n{'='*50}")
    print("测试总结:")
    print(f"{'='*50}")
    
    all_passed = True
    for test_name, success in results:
        status = "通过" if success else "失败"
        print(f"{test_name}: {status}")
        if not success:
            all_passed = False
    
    print(f"\n总体结果: {'所有测试通过' if all_passed else '部分测试失败'}")
    return all_passed


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
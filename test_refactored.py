#!/usr/bin/env python3
"""
测试重构后的单文件处理功能
"""

import os
import sys
import time
from pathlib import Path

# 添加src目录到Python路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

# 使用uv运行时的环境变量设置
os.environ['PYTHONPATH'] = str(Path(__file__).parent / "src")

def test_single_file_processor():
    """测试单文件处理器"""
    print("=" * 60)
    print("开始测试重构后的单文件处理功能")
    print("=" * 60)
    
    try:
        # 导入模块
        from item_backup_item.main import main
        
        print("OK 模块导入成功")
        
        # 测试分类功能
        print("\n1. 测试文件分类...")
        from item_backup_item.control import classify_process
        result = classify_process()
        print(f"   分类完成，处理了 {result} 个文件")
        
        # 检查数据库
        print("\n2. 检查数据库记录...")
        from item_backup_item.database import MySQLClient, ItemProcessRecord
        client = MySQLClient()
        
        # 查询所有记录
        stmt = client.create_query_stmt(ItemProcessRecord, {})
        records = client.query_data(stmt)
        print(f"   数据库中共有 {len(records)} 条记录")
        
        # 显示待处理文件
        pending_stmt = client.create_query_stmt(ItemProcessRecord, {
            "process_status": "classify"
        })
        pending_records = client.query_data(pending_stmt)
        print(f"   待处理文件: {len(pending_records)} 个")
        
        for record in pending_records[:3]:  # 只显示前3个
            print(f"   - {record.item_name} ({record.item_type}, {record.item_size} bytes)")
        
        if len(pending_records) > 0:
            print("\n3. 开始单文件处理测试...")
            print("   (注意: 这将实际处理文件，可能需要一些时间)")
            
            # 启动单文件处理（限制处理1个文件作为测试）
            from item_backup_item.control.single_file_processor import SingleFileProcessor
            
            processor = SingleFileProcessor()
            
            # 修改配置以进行测试
            processor.config.max_disk_usage_gb = 10  # 限制磁盘使用
            processor.config.enable_monitoring = True
            
            print("   启动处理器...")
            
            # 创建测试线程来运行处理器
            import threading
            
            def run_processor():
                try:
                    processor.start_processing()
                except KeyboardInterrupt:
                    processor.stop_processing()
            
            processor_thread = threading.Thread(target=run_processor)
            processor_thread.daemon = True
            processor_thread.start()
            
            # 运行30秒作为测试
            print("   运行30秒测试...")
            time.sleep(30)
            
            # 停止处理器
            processor.stop_processing()
            print("   处理器已停止")
            
            # 检查处理结果
            print("\n4. 检查处理结果...")
            # 查询所有记录，然后过滤
            all_stmt = client.create_query_stmt(ItemProcessRecord, {})
            all_records = client.query_data(all_stmt)
            processed_records = [r for r in all_records if r.process_status != "classify"]
            print(f"   已处理文件: {len(processed_records)} 个")
            
            for record in processed_records[:5]:  # 只显示前5个
                print(f"   - {record.item_name}: {record.process_status}")
        
        print("\nOK 测试完成")
        return True
        
    except Exception as e:
        print(f"\nFAIL 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_components():
    """测试各个组件"""
    print("\n" + "=" * 60)
    print("测试各个组件功能")
    print("=" * 60)
    
    try:
        # 测试配置
        print("\n1. 测试配置模块...")
        from item_backup_item.config import ProcessingConfig, MonitoringConfig
        config = ProcessingConfig()
        print(f"   最大磁盘使用: {config.max_disk_usage_gb}GB")
        print(f"   监控启用: {config.enable_monitoring}")
        
        # 测试路径标准化
        test_path = r"e:\test\file.txt"
        standard_path = config.get_standardized_path(test_path)
        print(f"   路径标准化: {test_path} -> {standard_path}")
        
        # 测试监控
        print("\n2. 测试监控系统...")
        from item_backup_item.monitor import get_monitor
        monitor = get_monitor(config)
        current_metrics = monitor.get_current_metrics()
        print(f"   当前指标: 成功={current_metrics.successful_files}, 失败={current_metrics.failed_files}")
        
        # 测试错误恢复
        print("\n3. 测试错误恢复...")
        from item_backup_item.utils.error_recovery import get_error_recovery_manager
        recovery = get_error_recovery_manager()
        print(f"   错误恢复管理器已创建")
        
        # 测试磁盘空间管理
        print("\n4. 测试磁盘空间管理...")
        from item_backup_item.control.single_file_processor import DiskSpaceManager
        from item_backup_item.config import ProcessingConfig
        
        config = ProcessingConfig()
        space_manager = DiskSpaceManager(config.get_max_usage_bytes())
        
        # 测试D盘空间（测试文件所在盘）
        test_path = config.base_storage_path
        current_usage = space_manager.get_current_usage(test_path)
        print(f"   D盘当前使用: {current_usage.used_bytes/1024/1024/1024:.2f}GB")
        print(f"   D盘剩余空间: {current_usage.free_bytes/1024/1024/1024:.2f}GB")
        
        can_process = space_manager.can_process_file(1024 * 1024, "peak", test_path)  # 1MB文件
        print(f"   是否可以处理1MB文件: {can_process}")
        
        print("\nOK 组件测试完成")
        return True
        
    except Exception as e:
        print(f"\nFAIL 组件测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = True
    
    # 组件测试
    if not test_components():
        success = False
    
    # 功能测试
    if not test_single_file_processor():
        success = False
    
    if success:
        print("\nSUCCESS 所有测试通过!")
        sys.exit(0)
    else:
        print("\nFAILED 测试失败!")
        sys.exit(1)
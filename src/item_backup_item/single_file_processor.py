"""
单文件处理器 - 将批处理模式改为单文件处理模式
"""
import asyncio
import threading
import queue
import time
import os
import logging
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from datetime import datetime
import psutil  # 需要安装: pip install psutil

from .control import classify_process, hash_process, zip_process, zip_hash_process, \
    unzip_process, unzip_hash_process, upload_process, delete_process
from .database import MySQLClient, ItemProcessRecord
from .config import ClassifyConfig
from .utils.disk_space_manager import DiskSpaceManager, SpaceReservationContext
from .utils.upload_manager import UploadManager, UploadTask, get_upload_manager
from .utils.error_handler import get_error_handler, RetryableError, FatalError
from .utils.resource_manager import get_resource_manager, ResourceReservationContext
from .utils.monitor import get_monitor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class FileProcessingConfig:
    """文件处理配置"""
    max_disk_usage_gb: float = 80.0  # 最大磁盘使用量(GB)
    max_concurrent_files: int = 1    # 最大并发处理文件数
    retry_attempts: int = 3          # 重试次数
    space_buffer_gb: float = 5.0     # 空间缓冲区(GB)

class FileProcessor:
    """单文件处理器"""
    
    def __init__(self, config: FileProcessingConfig):
        self.config = config
        self.space_manager = DiskSpaceManager(
            max_disk_usage_gb=config.max_disk_usage_gb,
            space_buffer_gb=config.space_buffer_gb
        )
        self.db_client = MySQLClient()
        self.processed_files = set()
        self.failed_files = set()
        self.error_handler = get_error_handler()
        self.resource_manager = get_resource_manager()
        self.monitor = get_monitor()
        
    def get_next_file_to_process(self) -> Optional[str]:
        """获取下一个待处理的文件"""
        # 从数据库查询尚未处理的文件
        from sqlalchemy import select
        from .database import ItemProcessRecord
        
        stmt = select(ItemProcessRecord).where(
            ItemProcessRecord.process_status.in_(['classify', 'hashed', 'zipped'])
        ).limit(1)
        
        result = self.db_client.query_data(stmt)
        if result:
            return result[0].source_path
        return None
    
    def process_single_file(self, file_path: str) -> bool:
        """处理单个文件"""
        start_time = time.time()
        try:
            logger.info(f"开始处理文件: {file_path}")
            self.monitor.log_event('start', file_path, 'processing', {'stage': 'initial'})
            
            # 查询文件的当前处理状态
            from sqlalchemy import select
            stmt = select(ItemProcessRecord).where(
                ItemProcessRecord.source_path == file_path
            )
            file_record = self.db_client.query_data(stmt)
            
            if not file_record:
                logger.warning(f"文件记录不存在: {file_path}")
                self.monitor.log_event('warning', file_path, 'processing', {'reason': 'record_not_found'})
                return False
                
            current_status = file_record[0].process_status
            logger.info(f"文件 {file_path} 当前状态: {current_status}")
            
            # 根据当前状态决定下一步操作
            success = False
            if current_status == 'classify':
                logger.info(f"对文件 {file_path} 执行哈希计算...")
                self.monitor.log_event('start', file_path, 'hash', {'previous_status': current_status})
                success = self._execute_hash_process(file_path)
                self.monitor.record_performance(start_time, file_path, 'hash', success)
            elif current_status == 'hashed':
                logger.info(f"对文件 {file_path} 执行压缩...")
                self.monitor.log_event('start', file_path, 'zip', {'previous_status': current_status})
                success = self._execute_zip_process(file_path)
                self.monitor.record_performance(start_time, file_path, 'zip', success)
            elif current_status == 'zipped':
                logger.info(f"对文件 {file_path} 执行上传...")
                self.monitor.log_event('start', file_path, 'upload', {'previous_status': current_status})
                success = self._execute_upload_process(file_path)
                self.monitor.record_performance(start_time, file_path, 'upload', success)
            
            if success:
                self.processed_files.add(file_path)
                logger.info(f"成功处理文件: {file_path}")
                self.monitor.log_event('success', file_path, 'processing', {'final_status': 'completed'})
                return True
            else:
                self.failed_files.add(file_path)
                logger.error(f"处理文件失败: {file_path}")
                self.monitor.log_event('failure', file_path, 'processing', {'reason': 'processing_failed'})
                return False
                
        except FatalError as e:
            # 致命错误，不应重试
            logger.error(f"处理文件 {file_path} 时发生致命错误: {str(e)}")
            self.failed_files.add(file_path)
            self.monitor.log_event('failure', file_path, 'processing', {'reason': 'fatal_error', 'error': str(e)})
            return False
        except Exception as e:
            # 可重试的错误
            logger.error(f"处理文件 {file_path} 时发生错误: {str(e)}")
            self.failed_files.add(file_path)
            self.monitor.log_event('failure', file_path, 'processing', {'reason': 'exception', 'error': str(e)})
            return False
    
    def _execute_hash_process(self, file_path: str) -> bool:
        """执行哈希处理过程"""
        def _hash_operation():
            logger.info(f"开始对文件执行哈希计算: {file_path}")
            result = hash_process()
            logger.info(f"哈希计算完成: {file_path}, 结果: {result}")
            return True
        
        try:
            return self.error_handler.execute_with_retry(_hash_operation)
        except Exception as e:
            logger.error(f"哈希计算失败 {file_path}，已达到最大重试次数: {str(e)}")
            return False
    
    def _execute_zip_process(self, file_path: str) -> bool:
        """执行压缩处理过程"""
        def _zip_operation():
            logger.info(f"开始对文件执行压缩: {file_path}")
            result = zip_process()
            logger.info(f"压缩完成: {file_path}, 结果: {result}")
            return True
        
        try:
            return self.error_handler.execute_with_retry(_zip_operation)
        except Exception as e:
            logger.error(f"压缩失败 {file_path}，已达到最大重试次数: {str(e)}")
            return False
    
    def _execute_upload_process(self, file_path: str) -> bool:
        """执行上传处理过程"""
        def _upload_operation():
            logger.info(f"开始上传文件: {file_path}")
            result = upload_process()
            logger.info(f"上传完成: {file_path}, 结果: {result}")
            return True
        
        try:
            return self.error_handler.execute_with_retry(_upload_operation)
        except Exception as e:
            logger.error(f"上传失败 {file_path}，已达到最大重试次数: {str(e)}")
            return False


class SingleFileMainProcessor:
    """单文件主处理器 - 替代原来的main函数"""
    
    def __init__(self):
        self.config = FileProcessingConfig()
        self.processor = FileProcessor(self.config)
        self.upload_manager = get_upload_manager()
        
    def initialize_database(self):
        """初始化数据库"""
        self.processor.db_client.init_schema()
        
    def scan_and_classify_files(self):
        """扫描并分类文件"""
        logger.info("开始扫描和分类文件...")
        classify_process()
        logger.info("文件扫描和分类完成")
        
    def start_upload_manager(self):
        """启动上传管理器"""
        logger.info("启动上传管理器")
        self.upload_manager.start()
    
    def stop_upload_manager(self):
        """停止上传管理器"""
        logger.info("停止上传管理器")
        self.upload_manager.stop()
    
    def _upload_callback(self, task: UploadTask, success: bool):
        """上传完成回调函数"""
        if success:
            logger.info(f"上传成功回调: {task.file_path}")
            # 上传成功后，更新数据库状态并清理本地文件
            self._post_upload_cleanup(task.file_path)
        else:
            logger.error(f"上传失败回调: {task.file_path}")
    
    def _post_upload_cleanup(self, file_path: str):
        """上传完成后清理"""
        try:
            # 更新数据库记录
            from sqlalchemy import update
            from .database import ItemProcessRecord
            stmt = update(ItemProcessRecord).where(
                ItemProcessRecord.zipped_path == file_path
            ).values(
                process_status='uploaded',
                status_result='success'
            )
            self.processor.db_client.execute(stmt)
            
            # 删除本地压缩文件
            if Path(file_path).exists():
                Path(file_path).unlink()
                logger.info(f"已删除上传完成的压缩文件: {file_path}")
            
        except Exception as e:
            logger.error(f"上传后清理失败: {str(e)}, 文件: {file_path}")
    
    def run(self):
        """运行单文件处理流程"""
        logger.info("开始单文件处理流程...")
        self.processor.monitor.log_event('start', 'system', 'main', {'message': 'Starting single file processing flow'})
        
        try:
            # 初始化数据库
            self.initialize_database()
            
            # 扫描并分类文件
            self.scan_and_classify_files()
            
            # 启动上传管理器
            self.start_upload_manager()
            
            # 主处理循环
            while True:
                # 检查资源使用情况
                resource_stats = self.processor.resource_manager.get_resource_statistics()
                logger.info(f"资源使用情况 - CPU: {resource_stats['current_cpu_percent']:.1f}%, "
                            f"内存: {resource_stats['current_memory_percent']:.1f}%, "
                            f"磁盘使用: {resource_stats['current_disk_usage_percent']:.1f}%")
                
                # 检查磁盘空间
                space_status = self.processor.space_manager.get_space_status()
                logger.info(f"磁盘使用情况 - 已用: {space_status['used_space_gb']:.2f}GB, "
                            f"可用: {space_status['available_space_gb']:.2f}GB, "
                            f"预留: {space_status['reserved_space_gb']:.2f}GB")
                
                # 如果超过阈值，等待空间释放
                if space_status['used_space_gb'] >= self.config.max_disk_usage_gb - self.config.space_buffer_gb:
                    logger.warning("磁盘空间接近阈值，等待空间释放...")
                    self.processor.monitor.log_event('warning', 'system', 'main', {'reason': 'disk_threshold_reached'})
                    if not self.processor.space_manager.wait_for_space(1.0, timeout=60):
                        logger.error("等待空间释放超时")
                        self.processor.monitor.log_event('failure', 'system', 'main', {'reason': 'timeout_waiting_for_space'})
                    continue
                
                # 获取下一个待处理的文件
                next_file = self.processor.get_next_file_to_process()
                if not next_file:
                    logger.info("没有更多文件需要处理，退出主循环")
                    self.processor.monitor.log_event('info', 'system', 'main', {'message': 'No more files to process, exiting main loop'})
                    break
                
                # 估算处理该文件所需的空间
                required_space_gb = self.processor.space_manager.estimate_file_processing_space(next_file)
                logger.info(f"处理文件 {next_file} 需要约 {required_space_gb:.2f}GB 空间")
                
                # 检查是否可以分配资源
                if not self.processor.resource_manager.can_allocate_resources(required_space_gb):
                    logger.warning(f"资源不足，等待资源释放: {required_space_gb:.2f}GB")
                    self.processor.monitor.log_event('warning', next_file, 'resource_allocation', {'reason': 'insufficient_resources', 'required_gb': required_space_gb})
                    if not self.processor.resource_manager.wait_for_resources(required_space_gb, timeout=120):
                        logger.error(f"等待资源超时，跳过文件 {next_file}")
                        self.processor.monitor.log_event('failure', next_file, 'resource_allocation', {'reason': 'timeout_waiting_for_resources'})
                        continue
                    # 继续处理下一个文件
                    continue
                
                # 使用上下文管理器来处理空间预留
                try:
                    with SpaceReservationContext(self.processor.space_manager, next_file, required_space_gb):
                        logger.info(f"已为文件 {next_file} 预留 {required_space_gb:.2f}GB 空间")
                        
                        success = self.processor.process_single_file(next_file)
                        if success:
                            # 处理成功，创建上传任务
                            upload_task = UploadTask(
                                file_path=next_file,
                                task_id=f"upload_{next_file}_{int(time.time())}",
                                callback=self._upload_callback
                            )
                            self.upload_manager.add_task(upload_task)
                            logger.info(f"文件 {next_file} 已加入上传任务")
                            self.processor.monitor.log_event('success', next_file, 'queue_add', {'task_id': upload_task.task_id})
                        else:
                            logger.error(f"处理文件 {next_file} 失败")
                            self.processor.monitor.log_event('failure', next_file, 'processing', {'reason': 'processing_failed_in_loop'})
                            
                except RuntimeError as e:
                    logger.warning(f"无法为文件 {next_file} 预留空间: {e}")
                    self.processor.monitor.log_event('warning', next_file, 'space_reservation', {'reason': 'reservation_failed', 'error': str(e)})
                    if not self.processor.space_manager.wait_for_space(required_space_gb, timeout=120):
                        logger.error(f"等待空间释放超时，跳过文件 {next_file}")
                        self.processor.monitor.log_event('failure', next_file, 'space_reservation', {'reason': 'timeout_waiting_for_space'})
                        continue
                    # 重新尝试处理同一个文件
                    continue
                
                # 短暂休眠避免过度占用CPU
                time.sleep(0.1)
            
            logger.info("主处理循环完成，等待上传任务完成...")
            self.processor.monitor.log_event('info', 'system', 'main', {'message': 'Main processing loop completed, waiting for upload tasks'})
            
        finally:
            # 停止上传管理器
            self.stop_upload_manager()
            
            # 输出最终统计信息
            stats = self.processor.monitor.get_statistics()
            logger.info("单文件处理流程完成")
            logger.info(f"成功处理文件数: {len(self.processor.processed_files)}")
            logger.info(f"失败文件数: {len(self.processor.failed_files)}")
            logger.info(f"总体成功率: {stats['success_rate']:.2f}%")
            
            self.processor.monitor.log_event('complete', 'system', 'main', {
                'processed_count': len(self.processor.processed_files),
                'failed_count': len(self.processor.failed_files),
                'success_rate': stats['success_rate']
            })
            
            # 导出日志
            try:
                export_file = f"backup_log_{int(time.time())}.json"
                self.processor.monitor.export_logs(export_file)
                logger.info(f"日志已导出到: {export_file}")
            except Exception as e:
                logger.error(f"导出日志时发生错误: {str(e)}")


def main():
    """新的主函数入口"""
    processor = SingleFileMainProcessor()
    processor.run()


if __name__ == '__main__':
    main()
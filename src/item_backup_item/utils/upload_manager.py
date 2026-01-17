"""
上传管理器模块
"""
import threading
import queue
import time
import logging
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass
from pathlib import Path

from ..database import MySQLClient, ItemProcessRecord
from .error_handler import get_error_handler


logger = logging.getLogger(__name__)


@dataclass
class UploadTask:
    """上传任务数据类"""
    file_path: str
    task_id: str
    priority: int = 1  # 优先级，数字越小优先级越高
    retry_count: int = 0
    max_retries: int = 5
    callback: Optional[Callable] = None  # 上传完成后的回调函数


class UploadManager:
    """上传管理器"""
    
    def __init__(self, max_workers: int = 2, max_retries: int = 5):
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.upload_queue = queue.PriorityQueue()
        self.workers = []
        self.running = False
        self.db_client = MySQLClient()
        self.upload_service = None  # 延迟初始化
        
        # 任务状态跟踪
        self.active_tasks: Dict[str, UploadTask] = {}
        self.completed_tasks: List[UploadTask] = []
        self.failed_tasks: List[UploadTask] = []
        
        # 线程锁
        self.lock = threading.Lock()
        
        # 错误处理
        self.error_handler = get_error_handler()
    
    def _get_upload_service(self):
        """延迟获取上传服务实例"""
        if self.upload_service is None:
            from ..service import UploadService
            self.upload_service = UploadService()
        return self.upload_service
    
    def start(self):
        """启动上传管理器"""
        if self.running:
            return
        
        self.running = True
        logger.info(f"启动上传管理器，最大工作线程数: {self.max_workers}")
        
        # 启动工作线程
        for i in range(self.max_workers):
            worker = threading.Thread(
                target=self._worker,
                name=f"UploadWorker-{i}",
                daemon=True
            )
            worker.start()
            self.workers.append(worker)
    
    def stop(self, timeout: int = 10):
        """停止上传管理器"""
        if not self.running:
            return
        
        logger.info("正在停止上传管理器...")
        self.running = False
        
        # 等待所有工作线程结束
        for worker in self.workers:
            worker.join(timeout=timeout)
        
        self.workers.clear()
        logger.info("上传管理器已停止")
    
    def add_task(self, task: UploadTask):
        """添加上传任务"""
        with self.lock:
            self.upload_queue.put((task.priority, task))
            logger.info(f"添加上传任务: {task.file_path}, 优先级: {task.priority}")
    
    def _worker(self):
        """工作线程函数"""
        while self.running:
            try:
                # 获取任务
                priority, task = self.upload_queue.get(timeout=1)
                
                if not self.running:
                    self.upload_queue.put((priority, task))  # 把任务放回去
                    break
                
                logger.info(f"开始处理上传任务: {task.file_path}")
                
                # 标记任务为活跃状态
                with self.lock:
                    self.active_tasks[task.task_id] = task
                
                # 执行上传
                success = self._execute_upload(task)
                
                if success:
                    # 上传成功
                    logger.info(f"上传成功: {task.file_path}")
                    self._handle_success(task)
                else:
                    # 上传失败
                    logger.error(f"上传失败: {task.file_path}")
                    self._handle_failure(task)
                
                # 标记任务完成
                with self.lock:
                    if task.task_id in self.active_tasks:
                        del self.active_tasks[task.task_id]
                
                self.upload_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"上传工作线程发生异常: {str(e)}")
                time.sleep(1)  # 避免异常导致的忙循环
    
    def _execute_upload(self, task: UploadTask) -> bool:
        """执行上传操作"""
        def _upload_operation():
            logger.info(f"正在上传文件: {task.file_path}")
            
            # 使用上传服务执行上传
            upload_service = self._get_upload_service()
            upload_result = upload_service.upload_file(task.file_path)
            
            # 检查上传结果
            if upload_result and isinstance(upload_result, dict):
                errno = upload_result.get('errno', -1)
                if errno == 0:
                    logger.info(f"文件上传成功: {task.file_path}")
                    return True
                else:
                    logger.error(f"文件上传失败，错误码: {errno}, 文件: {task.file_path}")
                    raise RetryableError(f"上传失败，错误码: {errno}")
            else:
                logger.error(f"上传结果格式错误: {upload_result}, 文件: {task.file_path}")
                raise RetryableError("上传结果格式错误")
                
        try:
            return self.error_handler.execute_with_retry(_upload_operation)
        except FatalError:
            # 致命错误，不应重试
            logger.error(f"上传文件 {task.file_path} 时发生致命错误，已达到最大重试次数")
            return False
        except Exception as e:
            logger.error(f"上传文件 {task.file_path} 时发生错误，已达到最大重试次数: {str(e)}")
            return False
    
    def _handle_success(self, task: UploadTask):
        """处理上传成功的情况"""
        with self.lock:
            self.completed_tasks.append(task)
        
        # 执行回调函数
        if task.callback:
            try:
                task.callback(task, True)
            except Exception as e:
                logger.error(f"执行上传成功回调时发生异常: {str(e)}")
        
        # 上传成功后，更新数据库状态并清理本地文件
        self._post_upload_cleanup(task.file_path)
    
    def _handle_failure(self, task: UploadTask):
        """处理上传失败的情况"""
        if task.retry_count < task.max_retries:
            # 重试任务
            new_task = UploadTask(
                file_path=task.file_path,
                task_id=task.task_id,
                priority=task.priority,
                retry_count=task.retry_count + 1,
                max_retries=task.max_retries,
                callback=task.callback
            )
            logger.info(f"上传失败，准备重试 ({new_task.retry_count}/{new_task.max_retries}): {task.file_path}")
            
            # 延迟重试
            time.sleep(2 ** new_task.retry_count)  # 指数退避
            self.add_task(new_task)
        else:
            # 达到最大重试次数，标记为失败
            with self.lock:
                self.failed_tasks.append(task)
            
            logger.error(f"上传失败达到最大重试次数，放弃: {task.file_path}")
            
            # 执行失败回调
            if task.callback:
                try:
                    task.callback(task, False)
                except Exception as e:
                    logger.error(f"执行上传失败回调时发生异常: {str(e)}")
    
    def _post_upload_cleanup(self, file_path: str):
        """上传完成后清理本地文件"""
        try:
            # 更新数据库记录
            from sqlalchemy import update
            stmt = update(ItemProcessRecord).where(
                ItemProcessRecord.zipped_path == file_path
            ).values(
                process_status='uploaded',
                status_result='success'
            )
            self.db_client.execute(stmt)
            
            # 删除本地压缩文件
            if Path(file_path).exists():
                Path(file_path).unlink()
                logger.info(f"已删除上传完成的压缩文件: {file_path}")
            
        except Exception as e:
            logger.error(f"上传后清理失败: {str(e)}, 文件: {file_path}")
    
    def get_status(self) -> Dict:
        """获取上传管理器状态"""
        with self.lock:
            return {
                'queue_size': self.upload_queue.qsize(),
                'active_tasks': len(self.active_tasks),
                'completed_tasks': len(self.completed_tasks),
                'failed_tasks': len(self.failed_tasks),
                'running': self.running
            }


# 全局上传管理器实例
upload_manager = UploadManager(max_workers=2)


def get_upload_manager() -> UploadManager:
    """获取上传管理器实例"""
    return upload_manager
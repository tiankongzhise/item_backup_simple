import os
import time
import logging
import threading
import queue
from pathlib import Path
from typing import Optional, Callable, Any
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, Future

from ..database import MySQLClient, ItemProcessRecord
from ..service import UploadService

logger = logging.getLogger(__name__)


class UploadStatus(Enum):
    """上传状态枚举"""
    PENDING = "pending"      # 等待中
    UPLOADING = "uploading"  # 上传中
    SUCCESS = "success"      # 上传成功
    FAILED = "failed"        # 上传失败
    RETRYING = "retrying"    # 重试中


@dataclass
class UploadTask:
    """上传任务"""
    item_id: int
    file_path: str
    file_size: int
    retry_count: int = 0
    max_retries: int = 5
    status: UploadStatus = UploadStatus.PENDING
    error_message: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Optional[dict] = None

    def can_retry(self) -> bool:
        """是否可以重试"""
        return self.retry_count < self.max_retries

    def increment_retry(self):
        """增加重试计数"""
        self.retry_count += 1
        self.status = UploadStatus.RETRYING

    @property
    def processing_time(self) -> float:
        """处理时间"""
        if self.started_at is None:
            return 0
        end_time = self.completed_at or time.time()
        return end_time - self.started_at


@dataclass
class UploadCallback:
    """上传回调配置"""
    on_success: Optional[Callable[['UploadTask'], None]] = None
    on_failure: Optional[Callable[['UploadTask'], None]] = None
    on_progress: Optional[Callable[['UploadTask', float], None]] = None
    on_retry: Optional[Callable[['UploadTask', int], None]] = None


class AsyncUploadService:
    """
    异步上传服务

    职责:
    1. 管理上传队列
    2. 后台上传处理
    3. 失败重试机制
    4. 回调通知
    """

    def __init__(
        self,
        max_workers: int = 2,
        max_queue_size: int = 1000,
        callback: Optional[UploadCallback] = None
    ):
        self.max_workers = max_workers
        self.callback = callback or UploadCallback()

        # 任务队列
        self.upload_queue: queue.Queue = queue.Queue(maxsize=max_queue_size)

        # 重试队列
        self.retry_queue: queue.Queue = queue.Queue()

        # 正在运行的任务
        self.running_tasks: dict[int, UploadTask] = {}
        self.running_lock = threading.Lock()

        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        # 控制标志
        self._is_running = False
        self._shutdown_event = threading.Event()

        # 统计信息
        self.stats = {
            'total_submitted': 0,
            'total_success': 0,
            'total_failed': 0,
            'total_retries': 0,
        }

        logger.info(f"AsyncUploadService initialized with {max_workers} workers")

    def start(self):
        """启动上传服务"""
        if self._is_running:
            logger.warning("Upload service is already running")
            return

        self._is_running = True
        self._shutdown_event.clear()

        # 启动工作线程
        for i in range(self.max_workers):
            self.executor.submit(self._upload_worker, i)

        logger.info("Upload service started")

    def stop(self, wait: bool = True):
        """停止上传服务"""
        if not self._is_running:
            return

        self._is_running = False
        self._shutdown_event.set()

        if wait:
            self.executor.shutdown(wait=True)
            logger.info("Upload service stopped")
        else:
            self.executor.shutdown(wait=False)
            logger.info("Upload service shutting down")

    def submit_upload(
        self,
        item_id: int,
        file_path: str,
        file_size: int,
        priority: int = 0
    ) -> UploadTask:
        """
        提交上传任务

        Args:
            item_id: 数据库记录ID
            file_path: 文件路径
            file_size: 文件大小
            priority: 优先级（数字越大优先级越高）

        Returns:
            UploadTask 对象
        """
        task = UploadTask(
            item_id=item_id,
            file_path=file_path,
            file_size=file_size
        )

        self.upload_queue.put((priority, time.time(), task))
        self.stats['total_submitted'] += 1

        logger.debug(f"Submitted upload task: item_id={item_id}, path={file_path}")

        return task

    def submit_batch(self, tasks: list[UploadTask]):
        """批量提交上传任务"""
        for task in tasks:
            self.upload_queue.put((0, time.time(), task))
            self.stats['total_submitted'] += 1

        logger.info(f"Batch submitted {len(tasks)} upload tasks")

    def _upload_worker(self, worker_id: int):
        """上传工作线程"""
        logger.debug(f"Upload worker {worker_id} started")

        while not self._shutdown_event.is_set():
            try:
                # 优先处理重试队列
                if not self.retry_queue.empty():
                    try:
                        task = self.retry_queue.get(timeout=0.1)
                        self._execute_upload(task)
                    except queue.Empty:
                        pass
                else:
                    # 从主队列获取任务
                    try:
                        priority, submit_time, task = self.upload_queue.get(
                            timeout=self._shutdown_event.wait(0.1) and 0.1 or 1.0
                        )
                        self._execute_upload(task)
                    except queue.Empty:
                        continue

            except Exception as e:
                logger.error(f"Upload worker {worker_id} error: {e}")

        logger.debug(f"Upload worker {worker_id} stopped")

    def _execute_upload(self, task: UploadTask):
        """执行上传任务"""
        task.started_at = time.time()
        task.status = UploadStatus.UPLOADING

        with self.running_lock:
            self.running_tasks[task.item_id] = task

        try:
            # 检查文件是否存在
            if not Path(task.file_path).exists():
                raise FileNotFoundError(f"File not found: {task.file_path}")

            # 执行上传
            upload_service = UploadService()
            result = upload_service.upload_file(task.file_path)

            task.completed_at = time.time()
            task.result = result

            if result.get("errno") == 0:
                task.status = UploadStatus.SUCCESS
                self.stats['total_success'] += 1

                # 成功回调
                if self.callback.on_success:
                    try:
                        self.callback.on_success(task)
                    except Exception as e:
                        logger.error(f"Error in on_success callback: {e}")

                # 更新数据库
                self._update_database_success(task)

                logger.info(
                    f"Upload succeeded: item_id={task.item_id}, "
                    f"time={task.processing_time:.2f}s"
                )
            else:
                raise Exception(result.get("errmsg", "Unknown upload error"))

        except Exception as e:
            task.completed_at = time.time()
            task.error_message = str(e)
            task.status = UploadStatus.FAILED
            self.stats['total_failed'] += 1

            # 处理重试
            if task.can_retry():
                task.increment_retry()
                self.stats['total_retries'] += 1
                self.retry_queue.put(task)

                # 重试回调
                if self.callback.on_retry:
                    try:
                        self.callback.on_retry(task, task.retry_count)
                    except Exception as e:
                        logger.error(f"Error in on_retry callback: {e}")

                logger.warning(
                    f"Upload retry: item_id={task.item_id}, "
                    f"attempt={task.retry_count}/{task.max_retries}, "
                    f"error={e}"
                )
            else:
                # 失败回调
                if self.callback.on_failure:
                    try:
                        self.callback.on_failure(task)
                    except Exception as e:
                        logger.error(f"Error in on_failure callback: {e}")

                # 更新数据库失败状态
                self._update_database_failure(task)

                logger.error(
                    f"Upload failed: item_id={task.item_id}, "
                    f"retries={task.retry_count}, error={e}"
                )

        finally:
            # 从运行任务中移除
            with self.running_lock:
                self.running_tasks.pop(task.item_id, None)

    def _update_database_success(self, task: UploadTask):
        """更新数据库 - 上传成功"""
        try:
            client = MySQLClient()
            update_data = [{
                'id': task.item_id,
                'process_status': 'uploaded',
                'status_result': 'success',
            }]
            client.update_data(ItemProcessRecord, update_data)
        except Exception as e:
            logger.error(f"Failed to update database for successful upload: {e}")

    def _update_database_failure(self, task: UploadTask):
        """更新数据库 - 上传失败"""
        try:
            client = MySQLClient()
            update_data = [{
                'id': task.item_id,
                'process_status': 'upload',
                'status_result': 'failure',
                'fail_reason': {'error': task.error_message, 'retries': task.retry_count},
            }]
            client.update_data(ItemProcessRecord, update_data)
        except Exception as e:
            logger.error(f"Failed to update database for failed upload: {e}")

    def get_queue_status(self) -> dict:
        """获取队列状态"""
        return {
            'queue_size': self.upload_queue.qsize(),
            'retry_queue_size': self.retry_queue.qsize(),
            'running_tasks': len(self.running_tasks),
            'is_running': self._is_running,
            'stats': self.stats.copy(),
        }

    def get_pending_tasks(self) -> list[UploadTask]:
        """获取待处理任务列表"""
        tasks = []
        while not self.upload_queue.empty():
            try:
                _, _, task = self.upload_queue.get_nowait()
                tasks.append(task)
            except queue.Empty:
                break
        return tasks

    def get_running_tasks(self) -> list[UploadTask]:
        """获取正在运行的任务"""
        with self.running_lock:
            return list(self.running_tasks.values())

    def wait_for_completion(self, timeout: Optional[float] = None) -> bool:
        """
        等待所有任务完成

        Args:
            timeout: 超时时间（秒），None表示无限等待

        Returns:
            是否在超时前完成
        """
        start_time = time.time()

        while True:
            # 检查是否所有任务都完成
            queue_empty = (
                self.upload_queue.empty() and
                self.retry_queue.empty() and
                len(self.running_tasks) == 0
            )

            if queue_empty:
                return True

            # 检查超时
            if timeout and (time.time() - start_time) >= timeout:
                return False

            time.sleep(0.5)


def get_async_upload_service(
    max_workers: int = 2,
    callback: Optional[UploadCallback] = None
) -> AsyncUploadService:
    """获取异步上传服务"""
    return AsyncUploadService(max_workers=max_workers, callback=callback)

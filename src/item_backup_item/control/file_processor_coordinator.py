import os
import sys
import time
import logging
import threading
from pathlib import Path
from typing import Optional, Callable, Any
from dataclasses import dataclass, field
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import select

from ..database import MySQLClient, ItemProcessRecord
from ..service import classify_folder, StorageService, get_email_notifier
from ..config import ClassifyConfig, DiskSpaceConfig, PathConfig
from .disk_space_manager import DiskSpaceManager, SpaceStatus
from .single_file_processor import SingleFileProcessor, ProcessStage, ProcessResult
from .async_upload_service import AsyncUploadService, UploadTask, UploadStatus, UploadCallback

logger = logging.getLogger(__name__)


def get_host_name():
    """获取主机名"""
    return os.getenv("COMPUTERNAME", "UNKNOWN")


@dataclass
class ProcessingStats:
    """处理统计信息"""
    total_files: int = 0
    processed_files: int = 0
    success_count: int = 0
    failure_count: int = 0
    skipped_count: int = 0
    start_time: float = field(default_factory=time.time)
    last_update_time: float = field(default_factory=time.time)

    @property
    def elapsed_time(self) -> float:
        return time.time() - self.start_time

    @property
    def success_rate(self) -> float:
        if self.processed_files == 0:
            return 0.0
        return self.success_count / self.processed_files * 100

    def to_dict(self) -> dict:
        return {
            'total_files': self.total_files,
            'processed_files': self.processed_files,
            'success_count': self.success_count,
            'failure_count': self.failure_count,
            'skipped_count': self.skipped_count,
            'elapsed_seconds': self.elapsed_time,
            'success_rate_percent': f"{self.success_rate:.2f}%",
        }


@dataclass
class CoordinatorConfig:
    """协调器配置"""
    # 是否启用分类（首次运行需要分类）
    enable_classify: bool = True

    # 并行处理数量
    parallel_workers: int = 1

    # 上传服务worker数量
    upload_workers: int = 2

    # 处理间隔（秒）
    process_interval: float = 0.1

    # 空间检查间隔（秒）
    space_check_interval: float = 1.0

    # 最大等待空间时间（秒）
    max_wait_space_time: float = 3600.0

    # 启用详细日志
    verbose_logging: bool = False


class FileProcessorCoordinator:
    """
    文件处理协调器

    职责:
    1. 协调分类、单文件处理、上传的完整流程
    2. 磁盘空间管理
    3. 异步上传集成
    4. 进度跟踪和统计
    5. 错误恢复
    """

    def __init__(
        self,
        config: Optional[CoordinatorConfig] = None,
        disk_config: Optional[DiskSpaceConfig] = None,
        path_config: Optional[PathConfig] = None,
        classify_config: Optional[ClassifyConfig] = None,
    ):
        self.config = config or CoordinatorConfig()
        self.disk_config = disk_config or DiskSpaceConfig()
        self.path_config = path_config or PathConfig()
        self.classify_config = classify_config or ClassifyConfig()

        # 初始化组件
        self.space_manager = DiskSpaceManager(self.disk_config)
        self.file_processor = SingleFileProcessor(self.disk_config, self.path_config)

        # 初始化上传回调
        upload_callback = UploadCallback(
            on_success=self._on_upload_success,
            on_failure=self._on_upload_failure,
        )
        self.upload_service = AsyncUploadService(
            max_workers=self.config.upload_workers,
            callback=upload_callback
        )

        # 统计信息
        self.stats = ProcessingStats()

        # 控制标志
        self._is_running = False
        self._pause_event = threading.Event()
        self._pause_event.set()  # 不暂停

        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=self.config.parallel_workers + 2)

        # 邮件通知
        self.email_notifier = get_email_notifier()

        logger.info("FileProcessorCoordinator initialized")

    def _on_upload_success(self, task: UploadTask):
        """上传成功回调 - 清理本地文件"""
        try:
            logger.info(f"Upload success callback: item_id={task.item_id}")

            # 获取数据库记录
            client = MySQLClient()
            stmt = select(ItemProcessRecord).where(ItemProcessRecord.id == task.item_id)
            records = client.query_data(stmt)

            if records:
                record = records[0]
                self._cleanup_files(record)

            # 释放空间
            self.space_manager.release_space(self.path_config.zipped_folder, task.file_size)

        except Exception as e:
            logger.error(f"Error in upload success callback: {e}")

    def _on_upload_failure(self, task: UploadTask):
        """上传失败回调"""
        try:
            logger.error(
                f"Upload failure callback: item_id={task.item_id}, "
                f"error={task.error_message}"
            )

            # 发送邮件通知
            self.email_notifier.send_error_notification(
                "Upload Failure",
                {
                    'item_id': task.item_id,
                    'file_path': task.file_path,
                    'error': task.error_message,
                    'retry_count': task.retry_count,
                }
            )

        except Exception as e:
            logger.error(f"Error in upload failure callback: {e}")

    def _cleanup_files(self, record: ItemProcessRecord):
        """清理上传完成的本地文件"""
        try:
            import shutil

            # 删除源文件
            if record.source_path and Path(record.source_path).exists():
                if Path(record.source_path).is_file():
                    Path(record.source_path).unlink()
                else:
                    shutil.rmtree(record.source_path)
                logger.debug(f"Deleted source file: {record.source_path}")

            # 删除压缩包
            if record.zipped_path and Path(record.zipped_path).exists():
                Path(record.zipped_path).unlink()
                logger.debug(f"Deleted zip file: {record.zipped_path}")

            # 删除解压文件
            if record.unzip_path and Path(record.unzip_path).exists():
                shutil.rmtree(record.unzip_path)
                logger.debug(f"Deleted unzip directory: {record.unzip_path}")

            # 更新数据库状态
            client = MySQLClient()
            update_data = [{
                'id': record.id,
                'process_status': 'delete',
                'status_result': 'success',
                'is_compiled': True,
            }]
            client.update_data(ItemProcessRecord, update_data)

        except Exception as e:
            logger.error(f"Error cleaning up files for record {record.id}: {e}")

    def start(self):
        """启动协调器"""
        if self._is_running:
            logger.warning("Coordinator is already running")
            return

        self._is_running = True
        self._pause_event.set()

        # 启动上传服务
        self.upload_service.start()

        logger.info("FileProcessorCoordinator started")

    def stop(self):
        """停止协调器"""
        if not self._is_running:
            return

        self._is_running = False

        # 停止上传服务
        self.upload_service.stop(wait=True)

        # 关闭线程池
        self.executor.shutdown(wait=True)

        logger.info("FileProcessorCoordinator stopped")

    def pause(self):
        """暂停处理"""
        self._pause_event.clear()
        logger.info("Coordinator paused")

    def resume(self):
        """恢复处理"""
        self._pause_event.set()
        logger.info("Coordinator resumed")

    def run_full_cycle(self, max_files: Optional[int] = None) -> ProcessingStats:
        """
        运行完整的处理周期

        Args:
            max_files: 最大处理文件数，None表示处理所有文件

        Returns:
            ProcessingStats 处理统计信息
        """
        self.start()

        try:
            # 步骤1: 分类（如果需要）
            if self.config.enable_classify:
                self._run_classify()

            # 步骤2: 处理文件
            self._process_files(max_files)

            # 步骤3: 等待上传完成
            self._wait_for_uploads()

            return self.stats

        finally:
            self.stop()

    def _run_classify(self):
        """运行分类流程"""
        logger.info("Starting classification...")

        try:
            target_folder_list = self.classify_config.sources_list
            result = []

            for item in target_folder_list:
                result.extend(classify_folder(item))

            # 存储分类结果
            storage_service = StorageService()
            add_rows = storage_service.store_classify_result(result)

            logger.info(f"Classification completed: {add_rows} items added")

        except Exception as e:
            logger.error(f"Classification failed: {e}")
            raise

    def _get_pending_files(self, limit: Optional[int] = None) -> list[ItemProcessRecord]:
        """获取待处理文件列表"""
        client = MySQLClient()

        stmt = (
            select(ItemProcessRecord)
            .where(ItemProcessRecord.host_name == get_host_name())
            .where(ItemProcessRecord.process_status == 'classify')
            .where(ItemProcessRecord.status_result != 'failure')
        )

        if limit:
            stmt = stmt.limit(limit)

        records = client.query_data(stmt)
        return list(records)

    def _process_files(self, max_files: Optional[int] = None):
        """处理文件主循环"""
        logger.info("Starting file processing...")

        self.stats = ProcessingStats()

        while True:
            # 检查暂停
            self._pause_event.wait()

            # 检查是否达到最大文件数
            if max_files and self.stats.processed_files >= max_files:
                logger.info(f"Reached max files limit: {max_files}")
                break

            # 获取待处理文件
            remaining = max_files - self.stats.processed_files if max_files else None
            pending_files = self._get_pending_files(remaining)

            if not pending_files:
                logger.info("No more pending files to process")
                break

            self.stats.total_files = len(pending_files)

            for record in pending_files:
                # 检查暂停
                self._pause_event.wait()

                if not self._is_running:
                    logger.info("Processing stopped")
                    return

                try:
                    self._process_single_file(record)
                except Exception as e:
                    logger.exception(f"Error processing file {record.id}: {e}")
                    self.stats.failure_count += 1

                self.stats.processed_files += 1

                # 处理间隔
                time.sleep(self.config.process_interval)

        logger.info(f"File processing completed: {self.stats.to_dict()}")

    def _process_single_file(self, record: ItemProcessRecord):
        """处理单个文件"""
        file_path = record.source_path

        # 检查磁盘空间
        if self.disk_config.enabled:
            required_space = self.file_processor._calculate_required_space(
                ProcessStage.ZIP,
                self.file_processor._create_context(record)
            )

            can_process, space_info = self.space_manager.can_process_file(
                required_space,
                self.path_config.zipped_folder
            )

            if not can_process:
                logger.warning(
                    f"Insufficient space for {file_path}. "
                    f"Available: {space_info.free_bytes if space_info else 'N/A'} bytes"
                )

                # 等待空间
                success, _ = self.space_manager.wait_for_space(
                    required_space,
                    self.path_config.zipped_folder,
                    timeout=self.config.max_wait_space_time,
                    check_interval=self.config.space_check_interval
                )

                if not success:
                    raise Exception("Timeout waiting for disk space")

        # 处理文件
        result = self.file_processor.process_file(record)

        if result.success:
            self.stats.success_count += 1
            logger.info(
                f"File processed successfully: {file_path} "
                f"(time: {result.processing_time_seconds:.2f}s)"
            )

            # 提交到上传队列
            if result.data and result.data.get('zipped_path'):
                self._submit_for_upload(record, result.data)

        else:
            self.stats.failure_count += 1
            logger.error(
                f"File processing failed: {file_path}, "
                f"stage={result.stage.value}, error={result.error_message}"
            )

            # 更新数据库失败状态
            self._update_record_failure(record, result)

    def _submit_for_upload(self, record: ItemProcessRecord, result_data: dict):
        """提交文件到上传队列"""
        zip_path = result_data.get('zipped_path')
        zip_size = result_data.get('zip_size', 0)

        if not zip_path or not Path(zip_path).exists():
            logger.warning(f"Zip file not found, cannot upload: {zip_path}")
            return

        task = self.upload_service.submit_upload(
            item_id=record.id,
            file_path=zip_path,
            file_size=zip_size
        )

        logger.debug(f"Submitted for upload: item_id={record.id}, path={zip_path}")

    def _wait_for_uploads(self):
        """等待所有上传完成"""
        logger.info("Waiting for uploads to complete...")

        while True:
            queue_status = self.upload_service.get_queue_status()

            if (
                queue_status['queue_size'] == 0 and
                queue_status['retry_queue_size'] == 0 and
                queue_status['running_tasks'] == 0
            ):
                break

            logger.debug(
                f"Upload queue: pending={queue_status['queue_size']}, "
                f"retry={queue_status['retry_queue_size']}, "
                f"running={queue_status['running_tasks']}"
            )

            time.sleep(1.0)

        logger.info("All uploads completed")

    def _update_record_failure(self, record: ItemProcessRecord, result: ProcessResult):
        """更新记录失败状态"""
        try:
            client = MySQLClient()
            update_data = [{
                'id': record.id,
                'process_status': result.stage.value,
                'status_result': 'failure',
                'fail_reason': {
                    'error': result.error_message,
                    'error_details': result.error_details,
                }
            }]
            client.update_data(ItemProcessRecord, update_data)
        except Exception as e:
            logger.error(f"Failed to update record failure: {e}")

    def get_status(self) -> dict:
        """获取当前状态"""
        return {
            'is_running': self._is_running,
            'is_paused': not self._pause_event.is_set(),
            'processing_stats': self.stats.to_dict(),
            'disk_space': self.space_manager.get_status_report(),
            'upload_queue': self.upload_service.get_queue_status(),
        }


def get_coordinator(
    config: Optional[CoordinatorConfig] = None,
    disk_config: Optional[DiskSpaceConfig] = None,
    path_config: Optional[PathConfig] = None,
) -> FileProcessorCoordinator:
    """获取协调器"""
    return FileProcessorCoordinator(config, disk_config, path_config)

import os
import time
import logging
from pathlib import Path
from typing import Optional, Callable, Any
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

from ..database import MySQLClient, ItemProcessRecord
from ..service import ZipService, CalculateHashService, get_email_notifier
from ..config import ZipConfig, DiskSpaceConfig, PathConfig
from .disk_space_manager import DiskSpaceManager, SpaceReservation

logger = logging.getLogger(__name__)


class ProcessStage(Enum):
    """处理阶段枚举"""
    CLASSIFY = "classify"
    HASH = "hash"
    ZIP = "zip"
    ZIP_HASH = "zip_hash"
    UNZIP = "unzip"
    UNZIP_HASH = "unzip_hash"
    UPLOAD_QUEUE = "upload_queue"
    UPLOADED = "uploaded"
    DELETED = "deleted"


@dataclass
class ProcessResult:
    """处理结果"""
    success: bool
    stage: ProcessStage
    item_id: Optional[int] = None
    data: Optional[dict] = None
    error_message: Optional[str] = None
    error_details: Optional[dict] = None
    processing_time_seconds: float = 0.0

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            'success': self.success,
            'stage': self.stage.value,
            'item_id': self.item_id,
            'data': self.data,
            'error_message': self.error_message,
            'error_details': self.error_details,
            'processing_time_seconds': self.processing_time_seconds,
        }


@dataclass
class ProcessingContext:
    """处理上下文 - 跟踪单个文件的处理状态"""
    db_record: ItemProcessRecord
    source_path: str
    item_id: int
    item_name: str
    item_size: int
    classify_result: str

    # 处理过程中产生的路径
    zipped_path: Optional[str] = None
    zip_size: Optional[int] = None
    unzip_path: Optional[str] = None
    unzip_size: Optional[int] = None

    # Hash信息
    source_hash: Optional[dict] = None
    zip_hash: Optional[dict] = None
    unzip_hash: Optional[dict] = None

    # 空间预留
    space_reservation: Optional[SpaceReservation] = None

    # 当前阶段
    current_stage: ProcessStage = ProcessStage.CLASSIFY

    # 处理状态
    is_completed: bool = False
    is_failed: bool = False

    def update_stage(self, stage: ProcessStage):
        """更新处理阶段"""
        self.current_stage = stage
        logger.debug(f"Item {self.item_id} moved to stage: {stage.value}")


class SingleFileProcessor:
    """
    单文件处理器

    职责:
    1. 处理单个文件的完整流程 (Hash -> Zip -> Unzip -> Verify)
    2. 管理空间预留
    3. 更新数据库状态
    4. 支持失败重试
    """

    def __init__(
        self,
        disk_space_config: Optional[DiskSpaceConfig] = None,
        path_config: Optional[PathConfig] = None,
        zip_config: Optional[ZipConfig] = None,
    ):
        self.disk_config = disk_space_config or DiskSpaceConfig()
        self.path_config = path_config or PathConfig()
        self.zip_config = zip_config or ZipConfig()

        self.space_manager = DiskSpaceManager(self.disk_config)
        self.zip_service = ZipService()
        self.hash_service = CalculateHashService()

        self.email_notifier = get_email_notifier()

        # 重试配置
        self.max_retries = 3
        self.retry_delay = 1.0  # 秒

        logger.info("SingleFileProcessor initialized")

    def process_file(
        self,
        db_record: ItemProcessRecord,
        progress_callback: Optional[Callable[[ProcessStage, dict], None]] = None
    ) -> ProcessResult:
        """
        处理单个文件的完整流程

        Args:
            db_record: 数据库记录
            progress_callback: 进度回调函数

        Returns:
            ProcessResult 处理结果
        """
        start_time = time.time()
        context = self._create_context(db_record)

        logger.info(f"Starting to process file: {context.source_path} (ID: {context.item_id})")

        try:
            # 阶段1: Hash计算
            if context.classify_result in ['normal_file', 'normal_folder']:
                result = self._process_hash_stage(context)
                if not result.success:
                    return self._finalize_result(result, start_time)
                if progress_callback:
                    progress_callback(ProcessStage.HASH, result.to_dict())

            # 阶段2: Zip压缩
            if context.classify_result in ['normal_file', 'normal_folder']:
                result = self._process_zip_stage(context)
                if not result.success:
                    return self._finalize_result(result, start_time)
                if progress_callback:
                    progress_callback(ProcessStage.ZIP, result.to_dict())

                # 阶段3: Zip Hash
                result = self._process_zip_hash_stage(context)
                if not result.success:
                    return self._finalize_result(result, start_time)
                if progress_callback:
                    progress_callback(ProcessStage.ZIP_HASH, result.to_dict())

                # 阶段4: Unzip验证
                result = self._process_unzip_stage(context)
                if not result.success:
                    return self._finalize_result(result, start_time)
                if progress_callback:
                    progress_callback(ProcessStage.UNZIP, result.to_dict())

                # 阶段5: Unzip Hash
                result = self._process_unzip_hash_stage(context)
                if not result.success:
                    return self._finalize_result(result, start_time)
                if progress_callback:
                    progress_callback(ProcessStage.UNZIP_HASH, result.to_dict())

            elif context.classify_result == 'zip_file':
                # 对于已压缩的文件，只需要计算hash
                result = self._process_zip_file_stage(context)
                if not result.success:
                    return self._finalize_result(result, start_time)
                if progress_callback:
                    progress_callback(ProcessStage.ZIP_HASH, result.to_dict())

            # 标记为待上传
            self._mark_for_upload(context)

            return ProcessResult(
                success=True,
                stage=ProcessStage.UPLOAD_QUEUE,
                item_id=context.item_id,
                data={
                    'source_path': context.source_path,
                    'zipped_path': context.zipped_path,
                    'zip_size': context.zip_size,
                },
                processing_time_seconds=time.time() - start_time
            )

        except Exception as e:
            logger.exception(f"Error processing file {context.source_path}: {e}")
            return self._create_error_result(context, str(e), start_time)

        finally:
            # 清理空间预留
            if context.space_reservation:
                self.space_manager.release_reservation(context.space_reservation)

    def _create_context(self, db_record: ItemProcessRecord) -> ProcessingContext:
        """创建处理上下文"""
        return ProcessingContext(
            db_record=db_record,
            source_path=db_record.source_path,
            item_id=db_record.id,
            item_name=db_record.item_name,
            item_size=db_record.item_size,
            classify_result=db_record.classify_result,
        )

    def _finalize_result(self, result: ProcessResult, start_time: float) -> ProcessResult:
        """完成处理结果"""
        result.processing_time_seconds = time.time() - start_time
        return result

    def _create_error_result(
        self,
        context: ProcessingContext,
        error_message: str,
        start_time: float
    ) -> ProcessResult:
        """创建错误结果"""
        return ProcessResult(
            success=False,
            stage=context.current_stage,
            item_id=context.item_id,
            error_message=error_message,
            processing_time_seconds=time.time() - start_time
        )

    def _calculate_required_space(self, stage: ProcessStage, context: ProcessingContext) -> int:
        """计算指定阶段所需空间"""
        source_size = context.item_size

        stage_requirements = {
            ProcessStage.HASH: source_size,
            ProcessStage.ZIP: source_size + int(source_size * 0.5),
            ProcessStage.ZIP_HASH: source_size + int(source_size * 0.5),
            ProcessStage.UNZIP: source_size + int(source_size * 0.5) + source_size,
            ProcessStage.UNZIP_HASH: source_size + int(source_size * 0.5) + source_size,
        }

        return stage_requirements.get(stage, source_size)

    def _process_hash_stage(self, context: ProcessingContext) -> ProcessResult:
        """处理Hash计算阶段"""
        context.update_stage(ProcessStage.HASH)

        for retry in range(self.max_retries):
            try:
                logger.debug(f"Calculating hash for: {context.source_path}")

                # 计算源文件hash
                source_path = Path(context.source_path)
                if source_path.is_file():
                    hash_result = self.hash_service.calculate_file_hash(source_path)
                elif source_path.is_dir():
                    hash_result = self.hash_service.calculate_folder_hash(source_path)
                else:
                    raise ValueError(f"Invalid path type: {context.source_path}")

                context.source_hash = hash_result

                # 更新数据库
                self._update_hash_info(context)

                return ProcessResult(
                    success=True,
                    stage=ProcessStage.HASH,
                    item_id=context.item_id,
                    data={'hash': hash_result}
                )

            except Exception as e:
                logger.warning(f"Hash calculation failed (attempt {retry + 1}): {e}")
                if retry < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    return ProcessResult(
                        success=False,
                        stage=ProcessStage.HASH,
                        item_id=context.item_id,
                        error_message=str(e)
                    )

    def _update_hash_info(self, context: ProcessingContext):
        """更新数据库中的hash信息"""
        if context.source_hash is None:
            return

        client = MySQLClient()
        update_data = [{
            'id': context.item_id,
            'md5': context.source_hash.get('md5'),
            'sha1': context.source_hash.get('sha1'),
            'sha256': context.source_hash.get('sha256'),
            'other_hash_info': context.source_hash.get('other'),
            'process_status': 'hashed',
        }]
        client.update_data(ItemProcessRecord, update_data)

    def _process_zip_stage(self, context: ProcessingContext) -> ProcessResult:
        """处理Zip压缩阶段"""
        context.update_stage(ProcessStage.ZIP)

        # 计算所需空间并预留
        required_space = self._calculate_required_space(ProcessStage.ZIP, context)
        context.space_reservation = self.space_manager.reserve_space(
            self.path_config.zipped_folder,
            required_space
        )

        if not context.space_reservation:
            return ProcessResult(
                success=False,
                stage=ProcessStage.ZIP,
                item_id=context.item_id,
                error_message="Failed to reserve disk space for zip operation"
            )

        for retry in range(self.max_retries):
            try:
                logger.debug(f"Compressing: {context.source_path}")

                # 生成压缩路径
                password = getattr(self.zip_config, 'password', None)
                zip_path = self.path_config.get_zip_path(context.source_path, password)

                # 执行压缩
                zipped_path = self.zip_service.zip_item(
                    context.source_path,
                    self.path_config.zipped_folder,
                    password,
                    self.zip_config.zip_level
                )

                context.zipped_path = str(zipped_path)
                context.zip_size = Path(zipped_path).stat().st_size

                # 更新数据库
                self._update_zip_info(context)

                return ProcessResult(
                    success=True,
                    stage=ProcessStage.ZIP,
                    item_id=context.item_id,
                    data={
                        'zipped_path': context.zipped_path,
                        'zip_size': context.zip_size
                    }
                )

            except Exception as e:
                logger.warning(f"Zip compression failed (attempt {retry + 1}): {e}")
                if retry < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    # 清理失败的压缩文件
                    if context.zipped_path and Path(context.zipped_path).exists():
                        Path(context.zipped_path).unlink()
                    return ProcessResult(
                        success=False,
                        stage=ProcessStage.ZIP,
                        item_id=context.item_id,
                        error_message=str(e)
                    )

    def _update_zip_info(self, context: ProcessingContext):
        """更新数据库中的zip信息"""
        if context.zipped_path is None:
            return

        client = MySQLClient()
        update_data = [{
            'id': context.item_id,
            'zipped_path': context.zipped_path,
            'zipped_size': context.zip_size,
            'process_status': 'zipped',
        }]
        client.update_data(ItemProcessRecord, update_data)

    def _process_zip_hash_stage(self, context: ProcessingContext) -> ProcessResult:
        """处理压缩包Hash计算阶段"""
        context.update_stage(ProcessStage.ZIP_HASH)

        try:
            logger.debug(f"Calculating zip hash for: {context.zipped_path}")

            zip_path = Path(context.zipped_path)
            hash_result = self.hash_service.calculate_file_hash(zip_path)

            context.zip_hash = hash_result

            # 更新数据库
            client = MySQLClient()
            update_data = [{
                'id': context.item_id,
                'zipped_md5': hash_result.get('md5'),
                'zipped_sha1': hash_result.get('sha1'),
                'zipped_sha256': hash_result.get('sha256'),
                'other_zipped_hash_info': hash_result.get('other'),
                'process_status': 'zip_file_hashed',
            }]
            client.update_data(ItemProcessRecord, update_data)

            return ProcessResult(
                success=True,
                stage=ProcessStage.ZIP_HASH,
                item_id=context.item_id,
                data={'zip_hash': hash_result}
            )

        except Exception as e:
            logger.error(f"Zip hash calculation failed: {e}")
            return ProcessResult(
                success=False,
                stage=ProcessStage.ZIP_HASH,
                item_id=context.item_id,
                error_message=str(e)
            )

    def _process_unzip_stage(self, context: ProcessingContext) -> ProcessResult:
        """处理解压验证阶段"""
        context.update_stage(ProcessStage.UNZIP)

        # 计算所需空间
        required_space = self._calculate_required_space(ProcessStage.UNZIP, context)
        context.space_reservation = self.space_manager.reserve_space(
            self.path_config.unzip_folder,
            required_space
        )

        if not context.space_reservation:
            return ProcessResult(
                success=False,
                stage=ProcessStage.UNZIP,
                item_id=context.item_id,
                error_message="Failed to reserve disk space for unzip operation"
            )

        try:
            logger.debug(f"Unzipping: {context.zipped_path}")

            password = getattr(self.zip_config, 'password', None)
            unzip_path = self.zip_service.unzip_item(
                context.zipped_path,
                self.path_config.unzip_folder,
                password
            )

            context.unzip_path = str(unzip_path)

            # 计算解压后大小
            unzip_size = self._calculate_directory_size(Path(unzip_path))
            context.unzip_size = unzip_size

            # 验证解压大小
            if context.unzip_size != context.item_size:
                raise ValueError(
                    f"Unzip size mismatch: expected {context.item_size}, got {context.unzip_size}"
                )

            # 更新数据库
            client = MySQLClient()
            update_data = [{
                'id': context.item_id,
                'unzip_path': context.unzip_path,
                'unzip_size': context.unzip_size,
                'process_status': 'unzipped',
            }]
            client.update_data(ItemProcessRecord, update_data)

            # 释放解压空间预留，保留压缩包
            self.space_manager.release_space(self.path_config.unzip_folder, context.item_size)

            return ProcessResult(
                success=True,
                stage=ProcessStage.UNZIP,
                item_id=context.item_id,
                data={
                    'unzip_path': context.unzip_path,
                    'unzip_size': context.unzip_size
                }
            )

        except Exception as e:
            logger.error(f"Unzip failed: {e}")
            return ProcessResult(
                success=False,
                stage=ProcessStage.UNZIP,
                item_id=context.item_id,
                error_message=str(e)
            )

    def _process_unzip_hash_stage(self, context: ProcessingContext) -> ProcessResult:
        """处理解压文件Hash验证阶段"""
        context.update_stage(ProcessStage.UNZIP_HASH)

        try:
            logger.debug(f"Calculating unzip hash for: {context.unzip_path}")

            unzip_path = Path(context.unzip_path)
            if unzip_path.is_file():
                hash_result = self.hash_service.calculate_file_hash(unzip_path)
            elif unzip_path.is_dir():
                hash_result = self.hash_service.calculate_folder_hash(unzip_path)
            else:
                raise ValueError(f"Invalid unzip path: {context.unzip_path}")

            context.unzip_hash = hash_result

            # 验证hash一致性
            if context.source_hash:
                if context.source_hash.get('md5') != hash_result.get('md5'):
                    logger.warning(
                        f"Hash mismatch for {context.item_name}: "
                        f"source={context.source_hash.get('md5')}, "
                        f"unzip={hash_result.get('md5')}"
                    )

            # 更新数据库
            client = MySQLClient()
            update_data = [{
                'id': context.item_id,
                'unzip_md5': hash_result.get('md5'),
                'unzip_sha1': hash_result.get('sha1'),
                'unzip_sha256': hash_result.get('sha256'),
                'other_unzip_hash_info': hash_result.get('other'),
                'process_status': 'unzip_hashed',
            }]
            client.update_data(ItemProcessRecord, update_data)

            return ProcessResult(
                success=True,
                stage=ProcessStage.UNZIP_HASH,
                item_id=context.item_id,
                data={'unzip_hash': hash_result}
            )

        except Exception as e:
            logger.error(f"Unzip hash calculation failed: {e}")
            return ProcessResult(
                success=False,
                stage=ProcessStage.UNZIP_HASH,
                item_id=context.item_id,
                error_message=str(e)
            )

    def _process_zip_file_stage(self, context: ProcessingContext) -> ProcessResult:
        """处理已压缩文件阶段 - 直接标记为已hash"""
        context.update_stage(ProcessStage.ZIP_HASH)

        try:
            # 对于已经是zip的文件，直接更新状态
            client = MySQLClient()
            update_data = [{
                'id': context.item_id,
                'process_status': 'zip_file_hashed',
            }]
            client.update_data(ItemProcessRecord, update_data)

            return ProcessResult(
                success=True,
                stage=ProcessStage.ZIP_HASH,
                item_id=context.item_id,
                data={'note': 'Already a zip file, skipped compression'}
            )

        except Exception as e:
            logger.error(f"Zip file processing failed: {e}")
            return ProcessResult(
                success=False,
                stage=ProcessStage.ZIP_HASH,
                item_id=context.item_id,
                error_message=str(e)
            )

    def _mark_for_upload(self, context: ProcessingContext):
        """标记为待上传"""
        context.update_stage(ProcessStage.UPLOAD_QUEUE)

        client = MySQLClient()
        update_data = [{
            'id': context.item_id,
            'process_status': 'upload',
        }]
        client.update_data(ItemProcessRecord, update_data)

        logger.info(f"File {context.item_name} marked for upload")

    def _calculate_directory_size(self, path: Path) -> int:
        """计算目录大小"""
        if path.is_file():
            return path.stat().st_size
        elif path.is_dir():
            total = 0
            for item in path.rglob("*"):
                if item.is_file():
                    total += item.stat().st_size
            return total
        return 0

    def cleanup_after_upload(self, context: ProcessingContext):
        """上传完成后清理本地文件"""
        try:
            # 删除源文件
            if context.source_path and Path(context.source_path).exists():
                if Path(context.source_path).is_file():
                    Path(context.source_path).unlink()
                else:
                    import shutil
                    shutil.rmtree(context.source_path)
                logger.debug(f"Deleted source file: {context.source_path}")

            # 删除压缩包
            if context.zipped_path and Path(context.zipped_path).exists():
                Path(context.zipped_path).unlink()
                logger.debug(f"Deleted zip file: {context.zipped_path}")

            # 删除解压文件
            if context.unzip_path and Path(context.unzip_path).exists():
                import shutil
                shutil.rmtree(context.unzip_path)
                logger.debug(f"Deleted unzip directory: {context.unzip_path}")

            # 更新数据库状态
            client = MySQLClient()
            update_data = [{
                'id': context.item_id,
                'process_status': 'delete',
                'status_result': 'success',
                'is_compiled': True,
            }]
            client.update_data(ItemProcessRecord, update_data)

        except Exception as e:
            logger.error(f"Error cleaning up files for {context.item_id}: {e}")


def get_single_file_processor(
    disk_space_config: Optional[DiskSpaceConfig] = None,
    path_config: Optional[PathConfig] = None
) -> SingleFileProcessor:
    """获取单文件处理器"""
    return SingleFileProcessor(disk_space_config, path_config)

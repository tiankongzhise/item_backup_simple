from .classify import classify_process
from .source_hash import hash_process
from .zip import zip_process
from .zip_hash import zip_hash_process
from .unzip import unzip_process
from .unzip_hash import unzip_hash_process
from .upload import upload_process
from .delete import delete_process

# 新增的单文件处理模块
from .disk_space_manager import DiskSpaceManager, SpaceStatus, DiskSpaceInfo, get_disk_space_manager
from .single_file_processor import (
    SingleFileProcessor,
    ProcessStage,
    ProcessResult,
    ProcessingContext,
    get_single_file_processor
)
from .async_upload_service import (
    AsyncUploadService,
    UploadTask,
    UploadStatus,
    UploadCallback,
    get_async_upload_service
)
from .file_processor_coordinator import (
    FileProcessorCoordinator,
    CoordinatorConfig,
    ProcessingStats,
    get_coordinator
)

__all__ = [
    # 原有批处理模块
    "classify_process",
    "hash_process",
    "zip_process",
    "zip_hash_process",
    "unzip_process",
    "unzip_hash_process",
    "upload_process",
    "delete_process",
    # 磁盘空间管理
    "DiskSpaceManager",
    "SpaceStatus",
    "DiskSpaceInfo",
    "get_disk_space_manager",
    # 单文件处理
    "SingleFileProcessor",
    "ProcessStage",
    "ProcessResult",
    "ProcessingContext",
    "get_single_file_processor",
    # 异步上传
    "AsyncUploadService",
    "UploadTask",
    "UploadStatus",
    "UploadCallback",
    "get_async_upload_service",
    # 协调器
    "FileProcessorCoordinator",
    "CoordinatorConfig",
    "ProcessingStats",
    "get_coordinator",
]

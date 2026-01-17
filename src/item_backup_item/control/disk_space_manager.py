import os
import time
import threading
import logging
from pathlib import Path
from typing import Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

from ..config import DiskSpaceConfig

logger = logging.getLogger(__name__)


class SpaceStatus(Enum):
    """空间状态枚举"""
    SUFFICIENT = "sufficient"  # 空间充足
    WARNING = "warning"        # 空间不足警告
    CRITICAL = "critical"      # 空间严重不足
    UNAVAILABLE = "unavailable"  # 无法获取空间信息


@dataclass
class DiskSpaceInfo:
    """磁盘空间信息"""
    total_bytes: int
    used_bytes: int
    free_bytes: int
    used_percent: float
    path: str

    @property
    def used_gb(self) -> float:
        return self.used_bytes / (1024 ** 3)

    @property
    def free_gb(self) -> float:
        return self.free_bytes / (1024 ** 3)

    @property
    def total_gb(self) -> float:
        return self.total_bytes / (1024 ** 3)


@dataclass
class SpaceReservation:
    """空间预留信息"""
    reserved_bytes: int
    path: str
    created_at: float = field(default_factory=time.time)
    released: bool = False

    def release(self):
        """释放预留空间"""
        self.released = True
        logger.debug(f"Released space reservation: {self.reserved_bytes} bytes for {self.path}")


class DiskSpaceManager:
    """
    磁盘空间管理器

    职责:
    1. 监控磁盘使用情况
    2. 检查是否有足够空间处理文件
    3. 管理空间预留
    4. 等待空间释放
    """

    _instance: Optional['DiskSpaceManager'] = None
    _lock = threading.Lock()

    def __new__(cls, config: Optional[DiskSpaceConfig] = None):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, config: Optional[DiskSpaceConfig] = None):
        if self._initialized:
            return

        self.config = config or DiskSpaceConfig()
        self._reservations: dict[str, int] = {}  # path -> reserved bytes
        self._reservation_lock = threading.Lock()
        self._callbacks: list[Callable[[SpaceStatus, DiskSpaceInfo], None]] = []

        # 如果没有指定监控路径，使用默认路径
        if not self.config.monitored_paths:
            self.config.monitored_paths = [
                self._get_default_storage_path()
            ]

        self._initialized = True
        logger.info(f"DiskSpaceManager initialized with config: {self.config}")

    def _get_default_storage_path(self) -> str:
        """获取默认存储路径"""
        return str(Path(__file__).resolve().anchor)

    def register_space_callback(self, callback: Callable[[SpaceStatus, DiskSpaceInfo], None]):
        """注册空间状态变化回调"""
        self._callbacks.append(callback)

    def _notify_callbacks(self, status: SpaceStatus, info: DiskSpaceInfo):
        """通知所有回调"""
        for callback in self._callbacks:
            try:
                callback(status, info)
            except Exception as e:
                logger.error(f"Error in space callback: {e}")

    def get_disk_space(self, path: str) -> Optional[DiskSpaceInfo]:
        """
        获取指定路径的磁盘空间信息

        Args:
            path: 任意文件或目录路径

        Returns:
            DiskSpaceInfo对象，如果获取失败返回None
        """
        try:
            # 获取磁盘根目录
            drive_path = Path(path).anchor if Path(path).drive else path

            # 使用 shutil.disk_usage (跨平台兼容)
            import shutil
            usage = shutil.disk_usage(drive_path)

            total_bytes = usage.total
            free_bytes = usage.free
            used_bytes = usage.used
            used_percent = (used_bytes / total_bytes) * 100 if total_bytes > 0 else 100

            return DiskSpaceInfo(
                total_bytes=total_bytes,
                used_bytes=used_bytes,
                free_bytes=free_bytes,
                used_percent=used_percent,
                path=drive_path
            )
        except Exception as e:
            logger.error(f"Failed to get disk space for {path}: {e}")
            return None

    def get_primary_space_info(self) -> Optional[DiskSpaceInfo]:
        """获取主监控路径的磁盘空间信息"""
        if not self.config.monitored_paths:
            return None
        return self.get_disk_space(self.config.monitored_paths[0])

    def check_space_status(self, path: Optional[str] = None) -> tuple[SpaceStatus, Optional[DiskSpaceInfo]]:
        """
        检查空间状态

        Args:
            path: 可选，指定检查路径

        Returns:
            (SpaceStatus, DiskSpaceInfo) 元组
        """
        check_path = path or (self.config.monitored_paths[0] if self.config.monitored_paths else ".")
        space_info = self.get_disk_space(check_path)

        if space_info is None:
            return SpaceStatus.UNAVAILABLE, None

        # 计算实际已用空间（减去预留）
        with self._reservation_lock:
            reserved = self._reservations.get(check_path, 0)
            effective_used = space_info.used_bytes + reserved

        used_percent = (effective_used / space_info.total_bytes) * 100 if space_info.total_bytes > 0 else 100
        effective_free = space_info.total_bytes - effective_used

        # 判断状态
        if effective_free >= self.config.effective_threshold_bytes:
            status = SpaceStatus.SUFFICIENT
        elif effective_free >= self.config.reserved_space_bytes:
            status = SpaceStatus.WARNING
        else:
            status = SpaceStatus.CRITICAL

        # 创建带有有效空闲空间的info对象
        effective_info = DiskSpaceInfo(
            total_bytes=space_info.total_bytes,
            used_bytes=effective_used,
            free_bytes=effective_free,
            used_percent=used_percent,
            path=space_info.path
        )

        self._notify_callbacks(status, effective_info)

        return status, effective_info

    def can_process_file(self, required_bytes: int, path: Optional[str] = None) -> tuple[bool, Optional[DiskSpaceInfo]]:
        """
        检查是否有足够空间处理文件

        Args:
            required_bytes: 所需空间（字节）
            path: 可选，指定检查路径

        Returns:
            (是否可以处理, 当前空间信息)
        """
        check_path = path or (self.config.monitored_paths[0] if self.config.monitored_paths else ".")
        status, space_info = self.check_space_status(check_path)

        if space_info is None:
            return False, None

        # 考虑安全边距
        required_with_margin = self.config.get_required_space_with_margin(required_bytes)

        can_process = space_info.free_bytes >= required_with_margin

        if not can_process:
            logger.warning(
                f"Insufficient space for {required_bytes} bytes. "
                f"Available: {space_info.free_bytes} bytes, "
                f"Required with margin: {required_with_margin} bytes"
            )

        return can_process, space_info

    def reserve_space(self, path: str, bytes: int) -> Optional[SpaceReservation]:
        """
        预留空间

        Args:
            path: 路径
            bytes: 预留字节数

        Returns:
            SpaceReservation对象，如果失败返回None
        """
        print(f'debug:reserve_space is run,self.config.enabled：{self.config.enabled}')
        if not self.config.enabled:
            return SpaceReservation(reserved_bytes=bytes, path=path)

        # 在加锁前先检查空间状态，避免死锁
        can_process, space_info = self.can_process_file(bytes, path)

        if not can_process or space_info is None:
            logger.warning(f"Cannot reserve {bytes} bytes for {path}: insufficient space")
            return None

        with self._reservation_lock:
            # 再次检查（可能空间已被其他线程占用）
            current_reserved = self._reservations.get(path, 0)
            effective_free = space_info.total_bytes - (space_info.used_bytes + current_reserved)
            required_with_margin = self.config.get_required_space_with_margin(bytes)

            if effective_free < required_with_margin:
                logger.warning(f"Cannot reserve {bytes} bytes for {path}: space changed during reservation")
                return None

            # 增加预留
            self._reservations[path] = current_reserved + bytes

            reservation = SpaceReservation(reserved_bytes=bytes, path=path)

            logger.debug(f"Reserved {bytes} bytes for {path}. Total reserved: {self._reservations[path]}")

            return reservation

    def release_space(self, path: str, bytes: int):
        """
        释放预留空间

        Args:
            path: 路径
            bytes: 释放的字节数
        """
        with self._reservation_lock:
            current_reserved = self._reservations.get(path, 0)
            new_reserved = max(0, current_reserved - bytes)
            self._reservations[path] = new_reserved

            if new_reserved == 0:
                del self._reservations[path]

            logger.debug(f"Released {bytes} bytes for {path}. Remaining reserved: {new_reserved}")

    def release_reservation(self, reservation: SpaceReservation):
        """释放预留对象"""
        if reservation.released:
            return

        self.release_space(reservation.path, reservation.reserved_bytes)
        reservation.release()

    def wait_for_space(
        self,
        required_bytes: int,
        path: Optional[str] = None,
        timeout: Optional[float] = None,
        check_interval: float = 1.0
    ) -> tuple[bool, Optional[DiskSpaceInfo]]:
        """
        等待直到有足够空间

        Args:
            required_bytes: 所需空间（字节）
            path: 可选，指定检查路径
            timeout: 超时时间（秒），None表示无限等待
            check_interval: 检查间隔（秒）

        Returns:
            (是否成功获取足够空间, 最终空间信息)
        """
        check_path = path or (self.config.monitored_paths[0] if self.config.monitored_paths else ".")
        timeout = timeout or self.config.max_wait_time_seconds

        start_time = time.time()

        while True:
            can_process, space_info = self.can_process_file(required_bytes, check_path)

            if can_process:
                return True, space_info

            # 检查超时
            elapsed = time.time() - start_time
            if timeout and elapsed >= timeout:
                logger.error(f"Timeout waiting for space after {elapsed} seconds")
                return False, space_info

            # 等待检查间隔
            time.sleep(check_interval)

            logger.debug(
                f"Waiting for space... Elapsed: {elapsed:.1f}s, "
                f"Free: {space_info.free_bytes if space_info else 'N/A'} bytes"
            )

    def calculate_required_space(
        self,
        source_size: int,
        stage: str,
        include_unzip: bool = False
    ) -> int:
        """
        计算各阶段所需空间

        Args:
            source_size: 源文件大小
            stage: 当前阶段
            include_unzip: 是否包含解压文件

        Returns:
            所需空间（字节）
        """
        # 估算压缩包大小（假设最大压缩比1.2，最小0.1）
        # 实际压缩比取决于文件类型，这里使用保守估算
        estimated_zip_ratio = max(0.1, min(1.2, 1.0 - 0.5))  # 简单估算
        zip_size = int(source_size * estimated_zip_ratio)

        # 各阶段空间需求
        stage_requirements = {
            'hash': source_size,  # 源文件Hash计算：占用源文件大小
            'zip': source_size + zip_size,  # 压缩处理：源文件 + 压缩包
            'unzip': source_size + zip_size + source_size,  # 解压验证：源文件 + 压缩包 + 解压文件
            'verify': zip_size,  # 验证完成：只有压缩包
            'upload': 0,  # 上传完成：释放全部空间
        }

        required = stage_requirements.get(stage, source_size)

        # 如果不包含解压阶段，减去解压文件大小
        if not include_unzip and stage == 'unzip':
            required = source_size + zip_size

        return required

    def get_status_report(self) -> dict:
        """获取状态报告"""
        status_info = {}

        for path in self.config.monitored_paths or []:
            status, info = self.check_space_status(path)
            with self._reservation_lock:
                reserved = self._reservations.get(path, 0)

            status_info[path] = {
                'status': status.value,
                'total_gb': info.total_gb if info else 0,
                'used_gb': info.used_gb if info else 0,
                'free_gb': info.free_gb if info else 0,
                'used_percent': info.used_percent if info else 0,
                'reserved_gb': reserved / (1024 ** 3) if reserved else 0,
                'threshold_gb': self.config.max_disk_usage_gb,
            }

        return status_info


def get_disk_space_manager(config: Optional[DiskSpaceConfig] = None) -> DiskSpaceManager:
    """获取磁盘空间管理器单例"""
    return DiskSpaceManager(config)

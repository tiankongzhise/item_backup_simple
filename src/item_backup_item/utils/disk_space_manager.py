"""
磁盘空间管理模块
"""
import os
import threading
import time
from pathlib import Path
from typing import Dict, Optional
import psutil
import logging


logger = logging.getLogger(__name__)


class DiskSpaceManager:
    """磁盘空间管理器"""
    
    def __init__(self, max_disk_usage_gb: float = 80.0, space_buffer_gb: float = 5.0):
        self.max_disk_usage_gb = max_disk_usage_gb
        self.space_buffer_gb = space_buffer_gb
        self.lock = threading.RLock()  # 使用可重入锁
        self.reserved_space = 0  # 已预留空间(字节)
        self.file_space_reservations = {}  # 文件级别的空间预留
        self.last_check_time = 0
        self.check_interval = 1  # 检查间隔(秒)
        
    def get_available_space_gb(self) -> float:
        """获取可用磁盘空间(GB)"""
        disk_usage = psutil.disk_usage('.')
        available_bytes = disk_usage.free
        return available_bytes / (1024**3)
    
    def get_total_space_gb(self) -> float:
        """获取总磁盘空间(GB)"""
        disk_usage = psutil.disk_usage('.')
        total_bytes = disk_usage.total
        return total_bytes / (1024**3)
    
    def get_used_space_gb(self) -> float:
        """获取已用磁盘空间(GB)，包括预留空间"""
        disk_usage = psutil.disk_usage('.')
        used_bytes = disk_usage.used + self.reserved_space
        return used_bytes / (1024**3)
    
    def estimate_file_processing_space(self, file_path: str) -> float:
        """估算处理文件所需的空间(GB)"""
        if os.path.isfile(file_path):
            file_size = os.path.getsize(file_path)
        elif os.path.isdir(file_path):
            file_size = sum(
                os.path.getsize(os.path.join(dirpath, filename))
                for dirpath, _, filenames in os.walk(file_path)
                for filename in filenames
            )
        else:
            return 0
        
        # 估算压缩过程中需要的额外空间：原文件大小 + 压缩包大小 + 解压文件大小
        # 在最坏情况下，这可能是原文件大小的3倍
        estimated_space_gb = (file_size * 3) / (1024**3)
        return estimated_space_gb
    
    def can_reserve_space(self, required_space_gb: float) -> bool:
        """检查是否有足够的空间可以预留"""
        with self.lock:
            current_used = self.get_used_space_gb()
            
            # 检查是否超过最大使用限制
            if current_used + required_space_gb > self.max_disk_usage_gb:
                logger.info(f"空间不足: 当前使用 {current_used:.2f}GB + 需要 {required_space_gb:.2f}GB > 限制 {self.max_disk_usage_gb:.2f}GB")
                return False
            
            # 检查物理磁盘空间是否足够
            available_space = self.get_available_space_gb()
            if available_space < required_space_gb:
                logger.info(f"物理空间不足: 可用 {available_space:.2f}GB < 需要 {required_space_gb:.2f}GB")
                return False
                
            return True
    
    def reserve_space(self, file_path: str, required_space_gb: float) -> bool:
        """为特定文件预留空间"""
        with self.lock:
            if self.can_reserve_space(required_space_gb):
                reserved_bytes = int(required_space_gb * 1024**3)
                self.reserved_space += reserved_bytes
                self.file_space_reservations[file_path] = reserved_bytes
                logger.info(f"为文件 {file_path} 预留 {required_space_gb:.2f}GB 空间")
                return True
            return False
    
    def release_space(self, file_path: str):
        """释放特定文件的空间预留"""
        with self.lock:
            if file_path in self.file_space_reservations:
                released_bytes = self.file_space_reservations[file_path]
                self.reserved_space = max(0, self.reserved_space - released_bytes)
                del self.file_space_reservations[file_path]
                logger.info(f"释放文件 {file_path} 的空间预留: {released_bytes / (1024**3):.2f}GB")
    
    def get_space_status(self) -> Dict[str, float]:
        """获取空间使用状态"""
        with self.lock:
            total_space = self.get_total_space_gb()
            used_space = self.get_used_space_gb()
            available_space = self.get_available_space_gb()
            reserved_space_gb = self.reserved_space / (1024**3)
            
            return {
                'total_space_gb': total_space,
                'used_space_gb': used_space,
                'available_space_gb': available_space,
                'reserved_space_gb': reserved_space_gb,
                'max_allowed_gb': self.max_disk_usage_gb,
                'buffer_gb': self.space_buffer_gb
            }
    
    def wait_for_space(self, required_space_gb: float, timeout: int = 60) -> bool:
        """等待空间释放直到满足要求或超时"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.can_reserve_space(required_space_gb):
                return True
            time.sleep(1)  # 每秒检查一次
        return False


class SpaceReservationContext:
    """空间预留上下文管理器"""
    
    def __init__(self, manager: DiskSpaceManager, file_path: str, required_space_gb: float):
        self.manager = manager
        self.file_path = file_path
        self.required_space_gb = required_space_gb
        self.acquired = False
    
    def __enter__(self):
        self.acquired = self.manager.reserve_space(self.file_path, self.required_space_gb)
        if not self.acquired:
            raise RuntimeError(f"无法为文件 {self.file_path} 预留 {self.required_space_gb}GB 空间")
        return self.acquired
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.acquired:
            self.manager.release_space(self.file_path)
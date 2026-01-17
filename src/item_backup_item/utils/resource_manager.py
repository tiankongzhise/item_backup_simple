"""
资源管理器模块
"""
import threading
import time
import logging
from typing import Dict, Optional, List
from dataclasses import dataclass
import psutil
import os
from ..config import ZipConfig

logger = logging.getLogger(__name__)


@dataclass
class ResourceUsage:
    """资源使用情况数据类"""
    cpu_percent: float
    memory_percent: float
    disk_usage_percent: float
    available_disk_gb: float
    used_disk_gb: float
    reserved_disk_gb: float
    timestamp: float


class ResourceManager:
    """资源管理器"""
    
    def __init__(self, max_cpu_percent: float = 80.0, max_memory_percent: float = 85.0, 
                 max_disk_usage_gb: float = 80.0, disk_buffer_gb: float = 5.0):
        self.max_cpu_percent = max_cpu_percent
        self.max_memory_percent = max_memory_percent
        self.max_disk_usage_gb = max_disk_usage_gb
        self.disk_buffer_gb = disk_buffer_gb
        
        # 资源使用情况跟踪
        self.resource_history: List[ResourceUsage] = []
        self.max_history_size = 100  # 保留最近100次的资源使用情况
        
        # 线程锁
        self.lock = threading.RLock()
        
        # 当前预留资源
        self.currently_reserved_disk_gb = 0.0
        
    def get_current_resource_usage(self) -> ResourceUsage:
        """获取当前资源使用情况"""
        # CPU使用率
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # 内存使用率
        memory_info = psutil.virtual_memory()
        memory_percent = memory_info.percent
        
        # 磁盘使用情况
        disk_usage = psutil.disk_usage(ZipConfig.zipped_folder)
        total_disk_gb = disk_usage.total / (1024**3)
        used_disk_gb = disk_usage.used / (1024**3)
        available_disk_gb = disk_usage.free / (1024**3)
        disk_usage_percent = (used_disk_gb / total_disk_gb) * 100 if total_disk_gb > 0 else 0
        
        # 包含预留空间的磁盘使用情况
        effective_used_gb = used_disk_gb + self.currently_reserved_disk_gb
        
        usage = ResourceUsage(
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            disk_usage_percent=(effective_used_gb / total_disk_gb) * 100 if total_disk_gb > 0 else 0,
            available_disk_gb=available_disk_gb,
            used_disk_gb=used_disk_gb,
            reserved_disk_gb=self.currently_reserved_disk_gb,
            timestamp=time.time()
        )
        
        # 记录资源使用历史
        with self.lock:
            self.resource_history.append(usage)
            if len(self.resource_history) > self.max_history_size:
                self.resource_history.pop(0)
        
        return usage
    
    def can_allocate_resources(self, disk_space_gb: float) -> bool:
        """检查是否可以分配指定的资源"""
        current_usage = self.get_current_resource_usage()
        
        # 检查CPU使用率
        if current_usage.cpu_percent > self.max_cpu_percent:
            logger.warning(f"CPU使用率过高: {current_usage.cpu_percent}% > {self.max_cpu_percent}%")
            return False
        
        # 检查内存使用率
        if current_usage.memory_percent > self.max_memory_percent:
            logger.warning(f"内存使用率过高: {current_usage.memory_percent}% > {self.max_memory_percent}%")
            return False
        
        # 检查磁盘空间
        total_effective_used = current_usage.used_disk_gb + self.currently_reserved_disk_gb + disk_space_gb
        if total_effective_used > self.max_disk_usage_gb:
            logger.warning(
                f"磁盘空间超出限制: 当前使用 {current_usage.used_disk_gb:.2f}GB + "
                f"已预留 {self.currently_reserved_disk_gb:.2f}GB + "
                f"请求 {disk_space_gb:.2f}GB > 限制 {self.max_disk_usage_gb:.2f}GB"
            )
            return False
        
        # 检查可用物理磁盘空间
        if current_usage.available_disk_gb < disk_space_gb:
            logger.warning(
                f"物理磁盘空间不足: 可用 {current_usage.available_disk_gb:.2f}GB < "
                f"请求 {disk_space_gb:.2f}GB"
            )
            return False
        
        return True
    
    def reserve_disk_space(self, disk_space_gb: float) -> bool:
        """预留磁盘空间"""
        with self.lock:
            if self.can_allocate_resources(disk_space_gb):
                self.currently_reserved_disk_gb += disk_space_gb
                logger.info(f"预留 {disk_space_gb:.2f}GB 磁盘空间，当前预留总计: {self.currently_reserved_disk_gb:.2f}GB")
                return True
            else:
                logger.warning(f"无法预留 {disk_space_gb:.2f}GB 磁盘空间")
                return False
    
    def release_disk_space(self, disk_space_gb: float):
        """释放磁盘空间"""
        with self.lock:
            self.currently_reserved_disk_gb = max(0, self.currently_reserved_disk_gb - disk_space_gb)
            logger.info(f"释放 {disk_space_gb:.2f}GB 磁盘空间，当前预留总计: {self.currently_reserved_disk_gb:.2f}GB")
    
    def wait_for_resources(self, disk_space_gb: float, timeout: int = 60) -> bool:
        """等待资源可用"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.can_allocate_resources(disk_space_gb):
                return True
            time.sleep(1)  # 每秒检查一次
        return False
    
    def get_resource_statistics(self) -> Dict[str, float]:
        """获取资源统计信息"""
        current_usage = self.get_current_resource_usage()
        
        avg_cpu = sum(u.cpu_percent for u in self.resource_history) / len(self.resource_history) if self.resource_history else 0
        avg_memory = sum(u.memory_percent for u in self.resource_history) / len(self.resource_history) if self.resource_history else 0
        
        return {
            'current_cpu_percent': current_usage.cpu_percent,
            'average_cpu_percent': avg_cpu,
            'current_memory_percent': current_usage.memory_percent,
            'average_memory_percent': avg_memory,
            'current_disk_usage_percent': current_usage.disk_usage_percent,
            'available_disk_gb': current_usage.available_disk_gb,
            'used_disk_gb': current_usage.used_disk_gb,
            'reserved_disk_gb': current_usage.reserved_disk_gb,
            'max_disk_limit_gb': self.max_disk_usage_gb,
            'history_size': len(self.resource_history)
        }
    
    def cleanup_resources(self):
        """清理资源，重置预留空间"""
        with self.lock:
            if self.currently_reserved_disk_gb > 0:
                logger.warning(f"清理资源: 重置预留磁盘空间 {self.currently_reserved_disk_gb:.2f}GB")
                self.currently_reserved_disk_gb = 0.0


class ResourceReservationContext:
    """资源预留上下文管理器"""
    
    def __init__(self, manager: ResourceManager, disk_space_gb: float):
        self.manager = manager
        self.disk_space_gb = disk_space_gb
        self.acquired = False
    
    def __enter__(self):
        self.acquired = self.manager.reserve_disk_space(self.disk_space_gb)
        if not self.acquired:
            raise RuntimeError(f"无法预留 {self.disk_space_gb}GB 磁盘空间")
        return self.acquired
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.acquired:
            self.manager.release_disk_space(self.disk_space_gb)


# 全局资源管理器实例
resource_manager = ResourceManager()


def get_resource_manager() -> ResourceManager:
    """获取资源管理器实例"""
    return resource_manager
"""
监控和日志记录模块
"""
import logging
import time
import threading
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from pathlib import Path
import json
import psutil


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('backup_system.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ProcessingEvent:
    """处理事件数据类"""
    timestamp: float
    event_type: str  # 'start', 'progress', 'success', 'failure', 'warning'
    file_path: str
    stage: str  # 'classify', 'hash', 'zip', 'upload', etc.
    details: Dict[str, Any]


@dataclass
class PerformanceMetrics:
    """性能指标数据类"""
    start_time: float
    end_time: float
    duration: float
    file_path: str
    stage: str
    success: bool
    file_size: Optional[float] = None  # 文件大小（GB）
    speed: Optional[float] = None  # 处理速度（GB/s）


class Monitor:
    """监控器"""
    
    def __init__(self, log_file: str = 'backup_system_monitor.log'):
        self.events: List[ProcessingEvent] = []
        self.metrics: List[PerformanceMetrics] = []
        self.max_events = 1000  # 最大事件记录数
        self.max_metrics = 500  # 最大性能指标记录数
        
        # 日志配置
        self.log_file = log_file
        self.logger = logging.getLogger('BackupMonitor')
        
        # 统计信息
        self.stats = {
            'total_processed': 0,
            'total_successful': 0,
            'total_failed': 0,
            'total_skipped': 0,
            'start_time': time.time(),
            'current_active_tasks': 0
        }
        
        # 线程锁
        self.lock = threading.RLock()
    
    def log_event(self, event_type: str, file_path: str, stage: str, details: Dict[str, Any] = None):
        """记录处理事件"""
        if details is None:
            details = {}
        
        event = ProcessingEvent(
            timestamp=time.time(),
            event_type=event_type,
            file_path=file_path,
            stage=stage,
            details=details
        )
        
        with self.lock:
            self.events.append(event)
            if len(self.events) > self.max_events:
                self.events.pop(0)
            
            # 更新统计信息
            if event_type == 'success':
                self.stats['total_successful'] += 1
                self.stats['total_processed'] += 1
            elif event_type == 'failure':
                self.stats['total_failed'] += 1
                self.stats['total_processed'] += 1
            elif event_type == 'skipped':
                self.stats['total_skipped'] += 1
        
        # 记录到日志
        self.logger.info(f"[{stage}] {event_type.upper()}: {file_path} - {details}")
    
    def record_performance(self, start_time: float, file_path: str, stage: str, 
                          success: bool, file_size_gb: float = None):
        """记录性能指标"""
        end_time = time.time()
        duration = end_time - start_time
        speed = file_size_gb / duration if file_size_gb and duration > 0 else None
        
        metric = PerformanceMetrics(
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            file_path=file_path,
            stage=stage,
            success=success,
            file_size=file_size_gb,
            speed=speed
        )
        
        with self.lock:
            self.metrics.append(metric)
            if len(self.metrics) > self.max_metrics:
                self.metrics.pop(0)
    
    def get_current_resource_usage(self) -> Dict[str, float]:
        """获取当前资源使用情况"""
        cpu_percent = psutil.cpu_percent(interval=1)
        memory_info = psutil.virtual_memory()
        disk_usage = psutil.disk_usage('.')
        
        return {
            'cpu_percent': cpu_percent,
            'memory_percent': memory_info.percent,
            'memory_used_gb': memory_info.used / (1024**3),
            'memory_total_gb': memory_info.total / (1024**3),
            'disk_used_gb': disk_usage.used / (1024**3),
            'disk_total_gb': disk_usage.total / (1024**3),
            'disk_free_gb': disk_usage.free / (1024**3),
            'timestamp': time.time()
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self.lock:
            processing_rate = self.stats['total_processed'] / (time.time() - self.stats['start_time']) if time.time() - self.stats['start_time'] > 0 else 0
            
            # 计算平均处理时间
            successful_metrics = [m for m in self.metrics if m.success]
            avg_duration = sum(m.duration for m in successful_metrics) / len(successful_metrics) if successful_metrics else 0
            
            return {
                'total_processed': self.stats['total_processed'],
                'total_successful': self.stats['total_successful'],
                'total_failed': self.stats['total_failed'],
                'total_skipped': self.stats['total_skipped'],
                'success_rate': self.stats['total_successful'] / self.stats['total_processed'] * 100 if self.stats['total_processed'] > 0 else 0,
                'processing_rate_per_hour': processing_rate * 3600,
                'average_duration_seconds': avg_duration,
                'current_active_tasks': self.stats['current_active_tasks'],
                'uptime_seconds': time.time() - self.stats['start_time'],
                'events_count': len(self.events),
                'metrics_count': len(self.metrics)
            }
    
    def get_recent_events(self, count: int = 10) -> List[ProcessingEvent]:
        """获取最近的事件"""
        with self.lock:
            return self.events[-count:] if len(self.events) >= count else self.events[:]
    
    def export_logs(self, output_file: str):
        """导出日志到文件"""
        with self.lock:
            export_data = {
                'export_time': datetime.now().isoformat(),
                'statistics': self.get_statistics(),
                'recent_events': [
                    {
                        'timestamp': datetime.fromtimestamp(e.timestamp).isoformat(),
                        'event_type': e.event_type,
                        'file_path': e.file_path,
                        'stage': e.stage,
                        'details': e.details
                    } for e in self.events
                ],
                'performance_metrics': [
                    {
                        'start_time': datetime.fromtimestamp(m.start_time).isoformat(),
                        'end_time': datetime.fromtimestamp(m.end_time).isoformat(),
                        'duration': m.duration,
                        'file_path': m.file_path,
                        'stage': m.stage,
                        'success': m.success,
                        'file_size_gb': m.file_size,
                        'speed_gbs': m.speed
                    } for m in self.metrics
                ]
            }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"日志已导出到: {output_file}")
    
    def reset_statistics(self):
        """重置统计信息"""
        with self.lock:
            self.stats = {
                'total_processed': 0,
                'total_successful': 0,
                'total_failed': 0,
                'total_skipped': 0,
                'start_time': time.time(),
                'current_active_tasks': 0
            }
            self.events.clear()
            self.metrics.clear()
        
        self.logger.info("统计信息已重置")


# 全局监控器实例
monitor = Monitor()


def get_monitor() -> Monitor:
    """获取监控器实例"""
    return monitor
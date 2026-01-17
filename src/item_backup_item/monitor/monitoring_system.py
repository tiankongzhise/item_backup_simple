import time
import threading
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from collections import defaultdict, deque
import json
import os
from pathlib import Path


@dataclass
class ProcessingMetrics:
    """处理指标"""
    timestamp: float
    file_id: int
    file_path: str
    stage: str
    status: str
    processing_time: float
    file_size: int
    error_message: Optional[str] = None


@dataclass
class SystemMetrics:
    """系统指标"""
    timestamp: float
    disk_usage_bytes: int
    disk_total_bytes: int
    disk_free_bytes: int
    queue_length: int
    upload_queue_length: int
    active_processing: int


@dataclass
class AggregatedMetrics:
    """聚合指标"""
    total_files_processed: int = 0
    successful_files: int = 0
    failed_files: int = 0
    avg_processing_time: float = 0.0
    current_disk_usage: int = 0
    queue_length: int = 0
    upload_queue_length: int = 0
    active_processing: int = 0
    
    @property
    def success_rate(self) -> float:
        """成功率"""
        if self.total_files_processed == 0:
            return 0.0
        return self.successful_files / self.total_files_processed
    
    @property
    def failure_rate(self) -> float:
        """失败率"""
        return 1.0 - self.success_rate


class MetricsLogger:
    """指标日志记录器"""
    
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # 配置日志
        self.logger = logging.getLogger("FileBackupMonitor")
        self.logger.setLevel(logging.INFO)
        
        # 文件处理器
        log_file = self.log_dir / f"backup_monitor_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # 格式化器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def log_processing_event(self, metrics: ProcessingMetrics):
        """记录处理事件"""
        log_data = {
            "type": "processing_event",
            "timestamp": metrics.timestamp,
            "file_id": metrics.file_id,
            "file_path": metrics.file_path,
            "stage": metrics.stage,
            "status": metrics.status,
            "processing_time": metrics.processing_time,
            "file_size": metrics.file_size,
            "error": metrics.error_message
        }
        self.logger.info(f"PROCESSING: {json.dumps(log_data, ensure_ascii=False)}")
    
    def log_system_metrics(self, metrics: SystemMetrics):
        """记录系统指标"""
        log_data = {
            "type": "system_metrics",
            "timestamp": metrics.timestamp,
            "disk_usage": metrics.disk_usage_bytes,
            "disk_total": metrics.disk_total_bytes,
            "disk_free": metrics.disk_free_bytes,
            "queue_length": metrics.queue_length,
            "upload_queue_length": metrics.upload_queue_length,
            "active_processing": metrics.active_processing
        }
        self.logger.info(f"SYSTEM: {json.dumps(log_data)}")
    
    def log_warning(self, message: str, details: Dict[str, Any] = None):
        """记录警告"""
        log_data = {
            "type": "warning",
            "message": message,
            "details": details or {}
        }
        self.logger.warning(f"WARNING: {json.dumps(log_data, ensure_ascii=False)}")
    
    def log_error(self, message: str, details: Dict[str, Any] = None):
        """记录错误"""
        log_data = {
            "type": "error", 
            "message": message,
            "details": details or {}
        }
        self.logger.error(f"ERROR: {json.dumps(log_data, ensure_ascii=False)}")


class MonitoringSystem:
    """监控系统"""
    
    def __init__(self, config=None):
        self.config = config
        self.metrics_logger = MetricsLogger()
        
        # 指标存储
        self.processing_history: deque = deque(maxlen=10000)  # 最近10000条处理记录
        self.system_history: deque = deque(maxlen=1000)      # 最近1000条系统指标
        self.active_files: Dict[int, Dict] = {}               # 当前处理的文件
        
        # 聚合统计
        self.daily_stats = defaultdict(lambda: {"total": 0, "success": 0, "failed": 0})
        self.hourly_stats = defaultdict(lambda: {"total": 0, "success": 0, "failed": 0})
        
        # 监控线程
        self.monitoring_thread = None
        self.is_running = False
        
        # 告警状态
        self.last_alert_time = {}
        self.alert_cooldown = 300  # 5分钟告警冷却
    
    def start_monitoring(self):
        """启动监控"""
        if self.is_running:
            return
            
        self.is_running = True
        self.monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitoring_thread.start()
        self.metrics_logger.logger.info("监控系统已启动")
    
    def stop_monitoring(self):
        """停止监控"""
        self.is_running = False
        if self.monitoring_thread:
            self.monitoring_thread.join()
        self.metrics_logger.logger.info("监控系统已停止")
    
    def record_file_start(self, file_id: int, file_path: str, file_size: int):
        """记录文件开始处理"""
        start_time = time.time()
        self.active_files[file_id] = {
            "file_path": file_path,
            "file_size": file_size,
            "start_time": start_time,
            "current_stage": "started"
        }
    
    def record_stage_complete(self, file_id: int, stage: str, status: str = "success", error: str = None):
        """记录阶段完成"""
        if file_id in self.active_files:
            self.active_files[file_id]["current_stage"] = stage
            if error:
                self.active_files[file_id]["error"] = error
    
    def record_file_complete(self, file_id: int, status: str = "success", error: str = None):
        """记录文件处理完成"""
        if file_id not in self.active_files:
            return
            
        file_info = self.active_files[file_id]
        end_time = time.time()
        processing_time = end_time - file_info["start_time"]
        
        # 创建处理指标
        metrics = ProcessingMetrics(
            timestamp=end_time,
            file_id=file_id,
            file_path=file_info["file_path"],
            stage=file_info["current_stage"],
            status=status,
            processing_time=processing_time,
            file_size=file_info["file_size"],
            error_message=error
        )
        
        # 记录指标
        self.processing_history.append(metrics)
        self.metrics_logger.log_processing_event(metrics)
        
        # 更新统计
        self._update_statistics(metrics)
        
        # 清理活跃文件记录
        del self.active_files[file_id]
    
    def get_current_metrics(self) -> AggregatedMetrics:
        """获取当前聚合指标"""
        if not self.processing_history:
            return AggregatedMetrics()
        
        # 计算最近一小时的统计
        one_hour_ago = time.time() - 3600
        recent_metrics = [m for m in self.processing_history if m.timestamp > one_hour_ago]
        
        total = len(recent_metrics)
        successful = len([m for m in recent_metrics if m.status == "success"])
        failed = total - successful
        
        avg_time = 0.0
        if recent_metrics:
            total_time = sum(m.processing_time for m in recent_metrics)
            avg_time = total_time / len(recent_metrics)
        
        # 获取当前系统指标
        current_disk = self._get_disk_usage()
        
        return AggregatedMetrics(
            total_files_processed=total,
            successful_files=successful,
            failed_files=failed,
            avg_processing_time=avg_time,
            current_disk_usage=current_disk["used"],
            queue_length=len(self.active_files),
            upload_queue_length=0,  # 需要从上传队列获取
            active_processing=len(self.active_files)
        )
    
    def _monitoring_loop(self):
        """监控循环"""
        while self.is_running:
            try:
                # 记录系统指标
                self._record_system_metrics()
                
                # 检查告警条件
                self._check_alerts()
                
                # 生成报告
                self._generate_reports()
                
                time.sleep(60)  # 每分钟检查一次
                
            except Exception as e:
                self.metrics_logger.log_error("监控循环异常", {"error": str(e)})
                time.sleep(60)
    
    def _record_system_metrics(self):
        """记录系统指标"""
        disk_usage = self._get_disk_usage()
        
        metrics = SystemMetrics(
            timestamp=time.time(),
            disk_usage_bytes=disk_usage["used"],
            disk_total_bytes=disk_usage["total"],
            disk_free_bytes=disk_usage["free"],
            queue_length=len(self.active_files),
            upload_queue_length=0,  # 需要从上传队列获取
            active_processing=len(self.active_files)
        )
        
        self.system_history.append(metrics)
        self.metrics_logger.log_system_metrics(metrics)
    
    def _get_disk_usage(self) -> Dict[str, int]:
        """获取磁盘使用情况"""
        try:
            import psutil
            disk = psutil.disk_usage('/')
            return {
                "used": disk.used,
                "total": disk.total,
                "free": disk.free
            }
        except ImportError:
            # 如果没有psutil，返回模拟数据
            return {
                "used": 50 * 1024 * 1024 * 1024,  # 50GB
                "total": 100 * 1024 * 1024 * 1024,  # 100GB
                "free": 50 * 1024 * 1024 * 1024   # 50GB
            }
    
    def _check_alerts(self):
        """检查告警条件"""
        current_time = time.time()
        metrics = self.get_current_metrics()
        
        # 磁盘使用率告警
        if self.config:
            disk_usage_ratio = metrics.current_disk_usage / (self.config.max_disk_usage_gb * 1024 * 1024 * 1024)
            if disk_usage_ratio > 0.9:
                if current_time - self.last_alert_time.get("disk_usage", 0) > self.alert_cooldown:
                    self.metrics_logger.log_warning(
                        "磁盘使用率过高",
                        {
                            "current_usage_gb": metrics.current_disk_usage / (1024**3),
                            "max_usage_gb": self.config.max_disk_usage_gb,
                            "usage_ratio": disk_usage_ratio
                        }
                    )
                    self.last_alert_time["disk_usage"] = current_time
        
        # 队列长度告警
        if metrics.queue_length > 100:
            if current_time - self.last_alert_time.get("queue_length", 0) > self.alert_cooldown:
                self.metrics_logger.log_warning(
                    "处理队列过长",
                    {"queue_length": metrics.queue_length}
                )
                self.last_alert_time["queue_length"] = current_time
        
        # 失败率告警
        if metrics.failure_rate > 0.1 and metrics.total_files_processed > 10:
            if current_time - self.last_alert_time.get("failure_rate", 0) > self.alert_cooldown:
                self.metrics_logger.log_warning(
                    "失败率过高",
                    {
                        "failure_rate": metrics.failure_rate,
                        "total_files": metrics.total_files_processed,
                        "failed_files": metrics.failed_files
                    }
                )
                self.last_alert_time["failure_rate"] = current_time
    
    def _update_statistics(self, metrics: ProcessingMetrics):
        """更新统计数据"""
        date_key = datetime.fromtimestamp(metrics.timestamp).strftime("%Y-%m-%d")
        hour_key = datetime.fromtimestamp(metrics.timestamp).strftime("%Y-%m-%d %H")
        
        # 更新日统计
        self.daily_stats[date_key]["total"] += 1
        if metrics.status == "success":
            self.daily_stats[date_key]["success"] += 1
        else:
            self.daily_stats[date_key]["failed"] += 1
        
        # 更新小时统计
        self.hourly_stats[hour_key]["total"] += 1
        if metrics.status == "success":
            self.hourly_stats[hour_key]["success"] += 1
        else:
            self.hourly_stats[hour_key]["failed"] += 1
    
    def _generate_reports(self):
        """生成监控报告"""
        current_time = datetime.now()
        
        # 每小时报告
        if current_time.minute == 0:
            hour_key = current_time.strftime("%Y-%m-%d %H")
            if hour_key in self.hourly_stats:
                self._generate_hourly_report(hour_key)
        
        # 每日报告
        if current_time.hour == 0 and current_time.minute == 0:
            date_key = current_time.strftime("%Y-%m-%d")
            if date_key in self.daily_stats:
                self._generate_daily_report(date_key)
    
    def _generate_hourly_report(self, hour_key: str):
        """生成小时报告"""
        stats = self.hourly_stats[hour_key]
        report_data = {
            "period": hour_key,
            "type": "hourly_report",
            "total_files": stats["total"],
            "successful_files": stats["success"],
            "failed_files": stats["failed"],
            "success_rate": stats["success"] / stats["total"] if stats["total"] > 0 else 0
        }
        
        self.metrics_logger.logger.info(f"HOURLY_REPORT: {json.dumps(report_data, ensure_ascii=False)}")
    
    def _generate_daily_report(self, date_key: str):
        """生成日报告"""
        stats = self.daily_stats[date_key]
        report_data = {
            "period": date_key,
            "type": "daily_report",
            "total_files": stats["total"],
            "successful_files": stats["success"],
            "failed_files": stats["failed"],
            "success_rate": stats["success"] / stats["total"] if stats["total"] > 0 else 0
        }
        
        self.metrics_logger.logger.info(f"DAILY_REPORT: {json.dumps(report_data, ensure_ascii=False)}")


# 全局监控实例
_global_monitor = None


def get_monitor(config=None) -> MonitoringSystem:
    """获取全局监控实例"""
    global _global_monitor
    if _global_monitor is None:
        _global_monitor = MonitoringSystem(config)
    return _global_monitor
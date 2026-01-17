import os
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

from ..database import MySQLClient as Client
from ..database import ItemProcessRecord


class ErrorType(Enum):
    """错误类型枚举"""
    COMPRESSION_FAILED = "compression_failed"
    VERIFICATION_FAILED = "verification_failed"
    UPLOAD_FAILED = "upload_failed"
    SPACE_INSUFFICIENT = "space_insufficient"
    DATABASE_ERROR = "database_error"
    FILE_NOT_FOUND = "file_not_found"
    PERMISSION_DENIED = "permission_denied"
    NETWORK_ERROR = "network_error"
    UNKNOWN_ERROR = "unknown_error"


class RecoveryAction(Enum):
    """恢复动作枚举"""
    RETRY = "retry"
    SKIP = "skip"
    MANUAL_INTERVENTION = "manual_intervention"
    CLEANUP_AND_RETRY = "cleanup_and_retry"
    WAIT_AND_RETRY = "wait_and_retry"


@dataclass
class ErrorRecord:
    """错误记录"""
    file_id: int
    error_type: ErrorType
    error_message: str
    timestamp: datetime
    stage: str
    retry_count: int = 0
    max_retries: int = 3
    next_retry_time: Optional[datetime] = None
    recovery_action: Optional[RecoveryAction] = None
    additional_data: Dict[str, Any] = None


class ErrorRecoveryManager:
    """错误恢复管理器"""
    
    def __init__(self):
        self.client = Client()
        self.error_records: Dict[int, ErrorRecord] = {}
        self.recovery_thread = None
        self.is_running = False
        self.recovery_interval = 300  # 5分钟检查一次
        
        # 错误处理策略配置
        self.retry_strategies = {
            ErrorType.COMPRESSION_FAILED: {
                'max_retries': 3,
                'retry_delay': 60,  # 1分钟
                'recovery_action': RecoveryAction.CLEANUP_AND_RETRY
            },
            ErrorType.VERIFICATION_FAILED: {
                'max_retries': 2,
                'retry_delay': 120,  # 2分钟
                'recovery_action': RecoveryAction.RETRY
            },
            ErrorType.UPLOAD_FAILED: {
                'max_retries': 5,
                'retry_delay': 300,  # 5分钟
                'recovery_action': RecoveryAction.RETRY
            },
            ErrorType.SPACE_INSUFFICIENT: {
                'max_retries': 10,
                'retry_delay': 600,  # 10分钟
                'recovery_action': RecoveryAction.WAIT_AND_RETRY
            },
            ErrorType.DATABASE_ERROR: {
                'max_retries': 3,
                'retry_delay': 30,
                'recovery_action': RecoveryAction.RETRY
            },
            ErrorType.FILE_NOT_FOUND: {
                'max_retries': 1,
                'retry_delay': 0,
                'recovery_action': RecoveryAction.SKIP
            },
            ErrorType.PERMISSION_DENIED: {
                'max_retries': 1,
                'retry_delay': 0,
                'recovery_action': RecoveryAction.MANUAL_INTERVENTION
            },
            ErrorType.NETWORK_ERROR: {
                'max_retries': 5,
                'retry_delay': 180,  # 3分钟
                'recovery_action': RecoveryAction.RETRY
            }
        }
    
    def start_recovery_monitoring(self):
        """启动恢复监控"""
        if self.is_running:
            return
        
        self.is_running = True
        self.recovery_thread = threading.Thread(target=self._recovery_loop, daemon=True)
        self.recovery_thread.start()
        print("错误恢复监控已启动")
    
    def stop_recovery_monitoring(self):
        """停止恢复监控"""
        self.is_running = False
        if self.recovery_thread:
            self.recovery_thread.join()
        print("错误恢复监控已停止")
    
    def record_error(self, file_id: int, error_message: str, stage: str, 
                    additional_data: Dict[str, Any] = None):
        """记录错误"""
        error_type = self._classify_error(error_message)
        
        # 获取错误处理策略
        strategy = self.retry_strategies.get(error_type, self.retry_strategies[ErrorType.UNKNOWN_ERROR])
        
        # 创建错误记录
        error_record = ErrorRecord(
            file_id=file_id,
            error_type=error_type,
            error_message=error_message,
            timestamp=datetime.now(),
            stage=stage,
            max_retries=strategy['max_retries'],
            next_retry_time=datetime.now() + timedelta(seconds=strategy['retry_delay']),
            recovery_action=strategy['recovery_action'],
            additional_data=additional_data or {}
        )
        
        self.error_records[file_id] = error_record
        
        # 更新数据库状态
        self._update_file_error_status(file_id, error_record)
        
        print(f"记录错误: 文件ID {file_id}, 错误类型 {error_type.value}, 阶段 {stage}")
    
    def get_pending_recoveries(self) -> List[ErrorRecord]:
        """获取待恢复的错误记录"""
        current_time = datetime.now()
        pending = []
        
        for record in self.error_records.values():
            if (record.retry_count < record.max_retries and 
                record.next_retry_time and 
                record.next_retry_time <= current_time):
                pending.append(record)
        
        return pending
    
    def _recovery_loop(self):
        """恢复循环"""
        while self.is_running:
            try:
                # 获取待恢复的错误
                pending_recoveries = self.get_pending_recoveries()
                
                for record in pending_recoveries:
                    self._attempt_recovery(record)
                
                # 清理僵尸文件和临时文件
                self._cleanup_zombie_files()
                
                time.sleep(self.recovery_interval)
                
            except Exception as e:
                print(f"恢复循环异常: {e}")
                time.sleep(60)
    
    def _attempt_recovery(self, record: ErrorRecord):
        """尝试恢复"""
        try:
            print(f"尝试恢复: 文件ID {record.file_id}, 重试次数 {record.retry_count + 1}/{record.max_retries}")
            
            # 根据恢复动作执行相应操作
            if record.recovery_action == RecoveryAction.SKIP:
                self._skip_file(record)
            elif record.recovery_action == RecoveryAction.MANUAL_INTERVENTION:
                self._request_manual_intervention(record)
            elif record.recovery_action == RecoveryAction.CLEANUP_AND_RETRY:
                self._cleanup_and_retry(record)
            elif record.recovery_action == RecoveryAction.RETRY:
                self._retry_file(record)
            elif record.recovery_action == RecoveryAction.WAIT_AND_RETRY:
                self._wait_and_retry(record)
            
        except Exception as e:
            print(f"恢复失败: 文件ID {record.file_id}, 错误: {e}")
            record.retry_count += 1
            
            # 更新下次重试时间
            if record.retry_count < record.max_retries:
                delay = self.retry_strategies[record.error_type]['retry_delay'] * (record.retry_count + 1)
                record.next_retry_time = datetime.now() + timedelta(seconds=delay)
            else:
                self._mark_permanent_failure(record)
    
    def _skip_file(self, record: ErrorRecord):
        """跳过文件"""
        self._update_file_status(record.file_id, "skipped", {"reason": "错误恢复跳过"})
        del self.error_records[record.file_id]
        print(f"文件已跳过: {record.file_id}")
    
    def _request_manual_intervention(self, record: ErrorRecord):
        """请求人工干预"""
        self._update_file_status(record.file_id, "manual_intervention_required", {
            "error_type": record.error_type.value,
            "error_message": record.error_message,
            "stage": record.stage
        })
        del self.error_records[record.file_id]
        print(f"需要人工干预: 文件ID {record.file_id}")
        
        # 发送邮件通知
        self._send_intervention_notification(record)
    
    def _cleanup_and_retry(self, record: ErrorRecord):
        """清理并重试"""
        self._cleanup_file_artifacts(record.file_id, record.stage)
        self._reset_file_for_retry(record.file_id)
        
        # 更新重试计数
        record.retry_count += 1
        if record.retry_count < record.max_retries:
            delay = self.retry_strategies[record.error_type]['retry_delay'] * (record.retry_count)
            record.next_retry_time = datetime.now() + timedelta(seconds=delay)
        else:
            self._mark_permanent_failure(record)
    
    def _retry_file(self, record: ErrorRecord):
        """直接重试"""
        self._reset_file_for_retry(record.file_id)
        
        # 更新重试计数
        record.retry_count += 1
        if record.retry_count < record.max_retries:
            delay = self.retry_strategies[record.error_type]['retry_delay'] * (record.retry_count)
            record.next_retry_time = datetime.now() + timedelta(seconds=delay)
        else:
            self._mark_permanent_failure(record)
    
    def _wait_and_retry(self, record: ErrorRecord):
        """等待并重试"""
        record.retry_count += 1
        if record.retry_count < record.max_retries:
            delay = self.retry_strategies[record.error_type]['retry_delay'] * 2  # 指数退避
            record.next_retry_time = datetime.now() + timedelta(seconds=delay)
        else:
            self._mark_permanent_failure(record)
    
    def _cleanup_file_artifacts(self, file_id: int, stage: str):
        """清理文件残留"""
        try:
            # 从数据库获取文件信息
            query_params = {"id": file_id}
            stmt = self.client.create_query_stmt(ItemProcessRecord, query_params)
            result = self.client.query_data(stmt)
            
            if not result:
                return
            
            file_record = result[0]
            
            # 根据阶段清理不同的文件
            if stage in ["zip", "zip_hash", "verify", "upload"]:
                if file_record.zipped_path and os.path.exists(file_record.zipped_path):
                    os.remove(file_record.zipped_path)
                    print(f"清理压缩文件: {file_record.zipped_path}")
            
            if stage in ["verify"]:
                if file_record.unzip_path and os.path.exists(file_record.unzip_path):
                    if os.path.isfile(file_record.unzip_path):
                        os.remove(file_record.unzip_path)
                    else:
                        import shutil
                        shutil.rmtree(file_record.unzip_path)
                    print(f"清理解压文件: {file_record.unzip_path}")
                    
        except Exception as e:
            print(f"清理文件残留失败: {e}")
    
    def _reset_file_for_retry(self, file_id: int):
        """重置文件状态以重试"""
        try:
            update_data = {
                'id': file_id,
                'process_status': 'classify',  # 重置到初始状态
                'fail_reason': None
            }
            self.client.update_data(ItemProcessRecord, [update_data])
            print(f"重置文件状态: {file_id}")
            
        except Exception as e:
            print(f"重置文件状态失败: {e}")
    
    def _mark_permanent_failure(self, record: ErrorRecord):
        """标记永久失败"""
        self._update_file_status(record.file_id, "permanent_failure", {
            "error_type": record.error_type.value,
            "error_message": record.error_message,
            "total_retries": record.retry_count,
            "stage": record.stage
        })
        del self.error_records[record.file_id]
        print(f"标记永久失败: 文件ID {record.file_id}")
    
    def _cleanup_zombie_files(self):
        """清理僵尸文件"""
        try:
            # 查找长时间处于处理状态但无更新的文件
            cutoff_time = datetime.now() - timedelta(hours=24)
            
            query_params = {
                "process_status": ["hashed", "zipped", "zip_hashed", "verified", "uploading"],
                "update_at__lt": cutoff_time.timestamp() * 1000  # 转换为毫秒时间戳
            }
            
            stmt = self.client.create_query_stmt(ItemProcessRecord, query_params)
            zombie_files = self.client.query_data(stmt)
            
            for file_record in zombie_files:
                print(f"发现僵尸文件: {file_record.source_path}, 状态: {file_record.process_status}")
                
                # 清理相关文件
                if file_record.zipped_path and os.path.exists(file_record.zipped_path):
                    os.remove(file_record.zipped_path)
                
                if file_record.unzip_path and os.path.exists(file_record.unzip_path):
                    if os.path.isfile(file_record.unzip_path):
                        os.remove(file_record.unzip_path)
                    else:
                        import shutil
                        shutil.rmtree(file_record.unzip_path)
                
                # 重置状态
                update_data = {
                    'id': file_record.id,
                    'process_status': 'classify',
                    'fail_reason': {"reason": "僵尸文件清理"}
                }
                self.client.update_data(ItemProcessRecord, [update_data])
                
        except Exception as e:
            print(f"清理僵尸文件失败: {e}")
    
    def _classify_error(self, error_message: str) -> ErrorType:
        """分类错误类型"""
        error_message_lower = error_message.lower()
        
        if "compress" in error_message_lower or "zip" in error_message_lower:
            return ErrorType.COMPRESSION_FAILED
        elif "verif" in error_message_lower or "hash" in error_message_lower:
            return ErrorType.VERIFICATION_FAILED
        elif "upload" in error_message_lower or "network" in error_message_lower:
            return ErrorType.UPLOAD_FAILED if "upload" in error_message_lower else ErrorType.NETWORK_ERROR
        elif "space" in error_message_lower or "disk" in error_message_lower:
            return ErrorType.SPACE_INSUFFICIENT
        elif "database" in error_message_lower or "mysql" in error_message_lower:
            return ErrorType.DATABASE_ERROR
        elif "not found" in error_message_lower or "no such file" in error_message_lower:
            return ErrorType.FILE_NOT_FOUND
        elif "permission" in error_message_lower or "access denied" in error_message_lower:
            return ErrorType.PERMISSION_DENIED
        else:
            return ErrorType.UNKNOWN_ERROR
    
    def _update_file_error_status(self, file_id: int, record: ErrorRecord):
        """更新文件错误状态"""
        try:
            update_data = {
                'id': file_id,
                'process_status': f"error_{record.error_type.value}",
                'fail_reason': {
                    "error_type": record.error_type.value,
                    "error_message": record.error_message,
                    "stage": record.stage,
                    "retry_count": record.retry_count,
                    "max_retries": record.max_retries,
                    "next_retry_time": record.next_retry_time.isoformat() if record.next_retry_time else None
                }
            }
            self.client.update_data(ItemProcessRecord, [update_data])
            
        except Exception as e:
            print(f"更新文件错误状态失败: {e}")
    
    def _update_file_status(self, file_id: int, status: str, fail_reason: Dict[str, Any] = None):
        """更新文件状态"""
        try:
            update_data = {
                'id': file_id,
                'process_status': status
            }
            
            if fail_reason:
                update_data['fail_reason'] = fail_reason
                
            self.client.update_data(ItemProcessRecord, [update_data])
            
        except Exception as e:
            print(f"更新文件状态失败: {e}")
    
    def _send_intervention_notification(self, record: ErrorRecord):
        """发送人工干预通知"""
        try:
            from ..service import get_email_notifier
            email_notifier = get_email_notifier()
            
            subject = f"文件备份系统需要人工干预 - 文件ID: {record.file_id}"
            message = f"""
文件ID: {record.file_id}
错误类型: {record.error_type.value}
错误消息: {record.error_message}
发生阶段: {record.stage}
发生时间: {record.timestamp}
重试次数: {record.retry_count}/{record.max_retries}

请及时处理此问题。
            """
            
            email_notifier.send_error_notification(subject, message)
            
        except Exception as e:
            print(f"发送人工干预通知失败: {e}")


# 全局错误恢复管理器实例
_global_recovery_manager = None


def get_error_recovery_manager() -> ErrorRecoveryManager:
    """获取全局错误恢复管理器实例"""
    global _global_recovery_manager
    if _global_recovery_manager is None:
        _global_recovery_manager = ErrorRecoveryManager()
    return _global_recovery_manager
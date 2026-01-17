from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class ProcessingConfig:
    """文件处理配置"""
    
    # 磁盘空间管理配置
    max_disk_usage_gb: int = 300  # 最大磁盘使用量(GB)
    space_check_interval: int = 30  # 空间检查间隔(秒)
    
    # 处理流程配置
    max_concurrent_files: int = 1  # 最大并发处理文件数
    processing_timeout: int = 3600  # 单个文件处理超时时间(秒)
    
    # 上传配置
    max_upload_retries: int = 5  # 最大上传重试次数
    upload_retry_delay: int = 60  # 上传重试延迟(秒)
    
    # 存储路径配置
    base_storage_path: str = r"D:\测试文件备份"  # 基础存储路径
    enable_path_standardization: bool = True  # 启用路径标准化
    
    # 监控配置
    enable_monitoring: bool = True
    log_level: str = "INFO"
    
    @classmethod
    def get_standardized_path(cls, source_path: str, is_cloud_path: bool = False) -> str:
        """
        生成标准化存储路径
        
        格式: base_path/YYYYMMDD/源文件夹名/加密密码/文件名.zip
        """
        base_path = cls.base_storage_path if not is_cloud_path else "/云端存储路径"
        
        # 解析源路径信息
        source_path_obj = Path(source_path)
        folder_name = source_path_obj.parent.name if source_path_obj.parent else "root"
        file_name = source_path_obj.stem  # 不包含扩展名的文件名
        
        # 获取当前日期
        date_str = datetime.now().strftime("%Y%m%d")
        
        # 加密密码（从配置获取或生成）
        password = cls._get_or_generate_password()
        
        # 构建标准化路径
        standardized_path = Path(base_path) / date_str / folder_name / password / f"{file_name}.zip"
        
        return str(standardized_path)
    
    @classmethod
    def _get_or_generate_password(cls) -> str:
        """获取或生成加密密码"""
        # 这里可以从配置文件读取或生成密码
        # 暂时返回默认密码
        return "default_password"
    
    def calculate_required_space(self, file_size: int, stage: str = "peak") -> int:
        """
        计算处理文件所需的磁盘空间
        
        Args:
            file_size: 文件大小(字节)
            stage: 处理阶段 (classify/hash/zip/verify/upload)
            
        Returns:
            所需空间(字节)
        """
        space_multipliers = {
            "classify": 1.0,      # 只需要原文件空间
            "hash": 1.0,          # 只需要原文件空间  
            "zip": 2.5,           # 原文件 + 压缩包 + 临时空间
            "verify": 3.5,        # 原文件 + 压缩包 + 解压文件
            "upload": 1.5         # 压缩包 + 临时空间
        }
        
        multiplier = space_multipliers.get(stage, 3.5)
        return int(file_size * multiplier)
    
    def get_max_usage_bytes(self) -> int:
        """获取最大使用字节数"""
        return self.max_disk_usage_gb * 1024 * 1024 * 1024


@dataclass
class MonitoringConfig:
    """监控配置"""
    
    # 监控指标
    track_disk_usage: bool = True
    track_queue_length: bool = True
    track_processing_time: bool = True
    track_success_rate: bool = True
    
    # 告警配置
    disk_usage_warning_threshold: float = 0.9  # 90%使用率告警
    queue_length_warning_threshold: int = 100  # 队列长度告警
    failure_rate_warning_threshold: float = 0.1  # 10%失败率告警
    
    # 报告配置
    generate_hourly_report: bool = True
    generate_daily_report: bool = True
    report_retention_days: int = 7
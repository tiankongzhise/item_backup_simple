from dataclasses import dataclass
from typing import Optional


@dataclass
class DiskSpaceConfig:
    """磁盘空间管理配置"""

    # 磁盘使用阈值 (GB) - 超过此值时暂停处理
    max_disk_usage_gb: float = 80.0

    # 预留空间阈值 (GB) - 低于此值时等待空间释放
    reserved_space_gb: float = 5.0

    # 检查磁盘空间的间隔时间 (秒)
    check_interval_seconds: float = 1.0

    # 最大等待时间 (秒) - 等待空间释放的超时时间
    max_wait_time_seconds: float = 3600.0

    # 要监控的磁盘路径列表 (None表示自动检测)
    monitored_paths: Optional[list[str]] = None

    # 空间计算安全边率 (预留额外空间百分比)
    safety_margin_percent: float = 10.0

    # 启用空间管理
    enabled: bool = True

    @property
    def max_disk_usage_bytes(self) -> int:
        """将GB转换为字节"""
        return int(self.max_disk_usage_gb * 1024 * 1024 * 1024)

    @property
    def reserved_space_bytes(self) -> int:
        """将GB转换为字节"""
        return int(self.reserved_space_gb * 1024 * 1024 * 1024)

    @property
    def effective_threshold_bytes(self) -> int:
        """有效阈值 = 最大使用量 - 预留空间"""
        return self.max_disk_usage_bytes - self.reserved_space_bytes

    def get_required_space_with_margin(self, required_bytes: int) -> int:
        """计算考虑安全边距后的所需空间"""
        margin = int(required_bytes * (self.safety_margin_percent / 100))
        return required_bytes + margin


@dataclass
class PathConfig:
    """路径标准化配置"""

    # 存储根目录
    storage_root: str = r"D:\测试AI运行\备份文件"

    # 临时文件目录
    temp_dir: str = r"D:\测试AI运行\备份临时"

    # 压缩文件存放目录
    zipped_folder: str = r"D:\测试AI运行\压缩结果"

    # 解压验证目录
    unzip_folder: str = r"D:\测试AI运行\解压测试"

    # 日志目录
    logs_folder: str = r"D:\测试AI运行\备份日志"

    # 是否按日期创建子目录
    create_date_subdir: bool = True

    # 日期格式
    date_format: str = "%Y%m%d"

    def get_storage_path(self, source_name: str, password: Optional[str] = None) -> str:
        """
        生成标准化的存储路径
        格式: storage_root/YYYYMMDD/源文件夹名/加密密码/文件名.zip
        """
        from datetime import datetime
        import os

        date_str = datetime.now().strftime(self.date_format)

        if self.create_date_subdir:
            base_path = os.path.join(self.storage_root, date_str)
        else:
            base_path = self.storage_root

        if password:
            # 使用带密码标记的目录名
            password_dir = f"加密密码_{password}"
            base_path = os.path.join(base_path, source_name, password_dir)
        else:
            base_path = os.path.join(base_path, source_name)

        return base_path

    def get_zip_path(self, source_path: str, password: Optional[str] = None) -> str:
        """
        生成压缩包的完整路径
        """
        import os
        from pathlib import Path

        source = Path(source_path)
        storage_dir = self.get_storage_path(source.name, password)

        # 确保目录存在
        os.makedirs(storage_dir, exist_ok=True)

        # 生成压缩包路径
        zip_name = f"{source.name}.zip"
        return os.path.join(storage_dir, zip_name)

    def get_unzip_path(self, zip_path: str) -> str:
        """
        生成解压目录的路径
        """
        import os
        from pathlib import Path

        zip_file = Path(zip_path)
        unzip_dir = os.path.join(self.unzip_folder, zip_file.stem)
        os.makedirs(unzip_dir, exist_ok=True)
        return unzip_dir

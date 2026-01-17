import psutil 
from item_backup_item.config import ZipConfig

def get_current_resource_usage():
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
    
    print(f"CPU使用率: {cpu_percent}%")
    print(f"内存使用率: {memory_percent}%")
    print(f"磁盘使用情况: {used_disk_gb:.2f}GB / {total_disk_gb:.2f}GB ({disk_usage_percent:.2f}%)")

if __name__ == "__main__":
    get_current_resource_usage()

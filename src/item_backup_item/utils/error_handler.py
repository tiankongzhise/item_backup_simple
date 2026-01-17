"""
错误处理和恢复机制模块
"""
import logging
import time
import traceback
from typing import Callable, Any, Optional
from pathlib import Path


logger = logging.getLogger(__name__)


class ErrorHandler:
    """错误处理和恢复机制"""
    
    def __init__(self, max_retry_attempts: int = 5, base_delay: float = 1.0, max_delay: float = 60.0):
        self.max_retry_attempts = max_retry_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
    
    def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """
        执行带重试的函数
        
        Args:
            func: 要执行的函数
            *args: 函数的位置参数
            **kwargs: 函数的关键字参数
            
        Returns:
            函数执行结果
            
        Raises:
            Exception: 当达到最大重试次数后仍失败时抛出异常
        """
        last_exception = None
        
        for attempt in range(self.max_retry_attempts):
            try:
                result = func(*args, **kwargs)
                if attempt > 0:
                    logger.info(f"函数 {func.__name__} 在第 {attempt + 1} 次尝试后成功")
                return result
            except Exception as e:
                last_exception = e
                if attempt == self.max_retry_attempts - 1:
                    # 最后一次尝试失败
                    logger.error(f"函数 {func.__name__} 在 {self.max_retry_attempts} 次尝试后仍然失败: {str(e)}")
                    logger.error(f"错误堆栈: {traceback.format_exc()}")
                    raise e
                else:
                    # 计算延迟时间（指数退避）
                    delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                    logger.warning(
                        f"函数 {func.__name__} 第 {attempt + 1} 次尝试失败: {str(e)}, "
                        f"将在 {delay} 秒后重试..."
                    )
                    time.sleep(delay)
        
        # 理论上不会到达这里，但为了类型安全
        raise last_exception
    
    def handle_file_operation_error(self, file_path: str, operation: str, error: Exception):
        """
        处理文件操作错误
        
        Args:
            file_path: 文件路径
            operation: 操作类型
            error: 错误对象
        """
        logger.error(f"{operation} 操作失败 - 文件: {file_path}, 错误: {str(error)}")
        logger.error(f"错误类型: {type(error).__name__}")
        logger.error(f"错误堆栈: {traceback.format_exc()}")
        
        # 尝试检查文件是否存在
        if Path(file_path).exists():
            logger.info(f"文件 {file_path} 存在")
            file_size = Path(file_path).stat().st_size
            logger.info(f"文件大小: {file_size} 字节")
        else:
            logger.warning(f"文件 {file_path} 不存在")
    
    def cleanup_temp_files(self, temp_dir: str, preserve_recent: bool = True):
        """
        清理临时文件
        
        Args:
            temp_dir: 临时目录路径
            preserve_recent: 是否保留最近的文件
        """
        try:
            import os
            from datetime import datetime, timedelta
            
            temp_path = Path(temp_dir)
            if not temp_path.exists():
                logger.warning(f"临时目录不存在: {temp_dir}")
                return
            
            # 获取当前时间，用于判断是否为最近文件
            current_time = datetime.now()
            
            cleaned_count = 0
            for file_path in temp_path.rglob('*'):
                if file_path.is_file():
                    # 检查是否为最近创建的文件（如果是保留最近文件的话）
                    if preserve_recent:
                        file_modified = datetime.fromtimestamp(file_path.stat().st_mtime)
                        if current_time - file_modified < timedelta(hours=1):
                            continue  # 跳过最近1小时内的文件
                    
                    # 删除文件
                    try:
                        file_path.unlink()
                        logger.debug(f"已删除临时文件: {file_path}")
                        cleaned_count += 1
                    except OSError as e:
                        logger.warning(f"删除临时文件失败: {file_path}, 错误: {str(e)}")
            
            logger.info(f"临时文件清理完成，共删除 {cleaned_count} 个文件")
        except Exception as e:
            logger.error(f"清理临时文件时发生错误: {str(e)}")
    
    def validate_file_integrity(self, file_path: str, expected_size: Optional[int] = None) -> bool:
        """
        验证文件完整性
        
        Args:
            file_path: 文件路径
            expected_size: 期望的文件大小
            
        Returns:
            文件是否完整
        """
        try:
            path = Path(file_path)
            if not path.exists():
                logger.error(f"文件不存在: {file_path}")
                return False
            
            actual_size = path.stat().st_size
            
            if expected_size is not None and actual_size != expected_size:
                logger.error(f"文件大小不匹配: {file_path}, 期望: {expected_size}, 实际: {actual_size}")
                return False
            
            logger.debug(f"文件完整性验证通过: {file_path}")
            return True
        except Exception as e:
            logger.error(f"验证文件完整性时发生错误: {file_path}, 错误: {str(e)}")
            return False


class RetryableError(Exception):
    """可重试的错误"""
    pass


class FatalError(Exception):
    """致命错误，不应重试"""
    pass


# 全局错误处理器实例
error_handler = ErrorHandler()


def get_error_handler() -> ErrorHandler:
    """获取错误处理器实例"""
    return error_handler
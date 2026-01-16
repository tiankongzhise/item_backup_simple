# ... existing code ...

import re
from pathlib import Path
from typing import Tuple, Optional


def extract_date_and_password_from_path(path: str) -> Tuple[Optional[str], Optional[str]]:
    """
    从路径中提取日期和压缩密码
    路径格式如: D:\\压缩测试\\20260115\\解压密码_H_x123456789
    
    Args:
        path: 输入路径
        
    Returns:
        tuple: (date, password)，如果未找到则返回(None, None)
    """
    path_obj = Path(path)
    
    # 获取路径的所有部分
    parts = path_obj.parts
    
    date_pattern = r'(\d{8})'  # 匹配8位数字的日期格式
    password_pattern = r'(?:解压密码|密码)[_\-]([A-Za-z_\d]+)'  # 匹配"解压密码_"或"密码-"后的内容
    
    date = None
    password = None
    
    for part in parts:
        # 查找日期
        date_match = re.search(date_pattern, part)
        if date_match and not date:
            potential_date = date_match.group(1)
            # 验证是否是有效的日期格式 YYYYMMDD
            if len(potential_date) == 8:
                try:
                    year = int(potential_date[:4])
                    month = int(potential_date[4:6])
                    day = int(potential_date[6:8])
                    if 1 <= month <= 12 and 1 <= day <= 31 and 1900 <= year <= 2100:
                        date = potential_date
                except ValueError:
                    pass
        
        # 查找密码
        password_match = re.search(password_pattern, part)
        if password_match and not password:
            password = password_match.group(1)
    
    return date, password


def extract_date_and_password_from_path_strict(path: str) -> Tuple[Optional[str], Optional[str]]:
    """
    从路径中严格提取日期和压缩密码
    路径格式如: D:\\压缩测试\\20260115\\解压密码_H_x123456789
    
    Args:
        path: 输入路径
        
    Returns:
        tuple: (date, password)，如果未找到则返回(None, None)
    """
    path_obj = Path(path)
    parts = path_obj.parts
    
    date = None
    password = None
    
    # 逐个检查路径部分
    for i, part in enumerate(parts):
        # 检查是否是8位数字的日期
        if re.match(r'^\d{8}$', part):
            date = part
            
        # 检查是否包含密码信息
        if '解压密码' in part or '密码' in part:
            # 提取"解压密码_"或类似格式后的部分
            password_match = re.search(r'(?:解压密码|密码)[_\-]([A-Za-z_\d]+)', part)
            if password_match:
                password = password_match.group(1)
    
    return date, password


# 示例使用
if __name__ == "__main__":
    test_path = r"D:\压缩测试\20260115\解压密码_H_x123456789"
    date, password = extract_date_and_password_from_path(test_path)
    print(f"路径: {test_path}")
    print(f"提取的日期: {date}")
    print(f"提取的密码: {password}")
    
    date2, password2 = extract_date_and_password_from_path_strict(test_path)
    print("\n严格模式:")
    print(f"提取的日期: {date2}")
    print(f"提取的密码: {password2}")

# ... existing code ...

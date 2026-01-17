#!/usr/bin/env python3
"""
清空数据库脚本
"""

import sys
from pathlib import Path

# 添加src目录到Python路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

def clear_database():
    """清空数据库"""
    try:
        from item_backup_item.database import MySQLClient
        from item_backup_item.database.mysql.models import ItemProcessRecord
        
        client = MySQLClient()
        
        # 删除所有数据
        client.drop_schema()
        print("数据库已清空")
        return True
        
    except Exception as e:
        print(f"清空数据库失败: {e}")
        return False

if __name__ == "__main__":
    success = clear_database()
    if success:
        print("SUCCESS 数据库清空成功!")
        sys.exit(0)
    else:
        print("FAILED 数据库清空失败!")
        sys.exit(1)
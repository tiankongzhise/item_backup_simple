import hashlib
import pathlib

def calculate_file_hash_base(file_path: pathlib.Path|str, algorithm: str = 'sha256'):
    """计算单个文件的哈希值"""
    hash_obj = hashlib.new(algorithm)
    with open(file_path, 'rb') as f:
        # 大文件 一次读取500MB 内存占用低
        for chunk in iter(lambda: f.read(500*1024*1024), b''):
            hash_obj.update(chunk)
    return hash_obj
from .core import calculate_file_hash_base
from pathlib import Path
from ...config import HashConfig,ClassifyConfig

def _is_oversize(file_info: str|Path|dict) -> bool:
    match file_info:
        case dict():
            return file_info['classify_result'] == 'oversize_file'
        case _:
            file_info = Path(file_info)
            return file_info.stat().st_size > ClassifyConfig.file_oversize

def _is_file(file_info: str|Path|dict) -> bool:
    match file_info:
        case Path():
            return file_info.is_file()
        case dict():
            return Path(file_info['source_path']).is_file()
        case str():
            return Path(file_info).is_file()
        case _:
            raise ValueError(f"Invalid file_info: {file_info}")

def _verify_file_for_hashing(file_info: str|Path|dict) -> Path:
    if _is_oversize(file_info):
        raise ValueError("file_info is oversize")
    if not _is_file(file_info):
        print(file_info)
        raise ValueError("file_info is not a file")
    return Path(file_info['source_path']) if isinstance(file_info, dict) else Path(file_info)

def calculate_file_hash(file_info:str|Path|dict, algorithm:list|None= None):
    print(f'file_info in calculate_file_hash: {file_info}')
    file_path = _verify_file_for_hashing(file_info)
    hash_result = {}
    algorithm = algorithm or HashConfig.required_hash_algorithms
    for alg in algorithm:
        hash_result[alg] = calculate_file_hash_base(file_path, alg).hexdigest().upper()
    return hash_result


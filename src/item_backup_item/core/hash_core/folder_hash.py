from .core import calculate_file_hash_base
from ...config import HashConfig,ClassifyConfig
from pathlib import Path
from tqdm import tqdm
import hashlib

def _is_empty_folder(folder_info: Path|dict) -> bool:
    match folder_info:
        case Path():
            return not any(folder_info.iterdir())
        case dict():
            return folder_info['classify_result'] == 'empty_folder'
        case _:
            raise TypeError("folder_info must be Path or dict")

def _is_overcount(folder_info: list[Path]|dict) -> bool:
    match folder_info:
        case list():
            return len(folder_info) > ClassifyConfig.overcount
        case dict():
            return folder_info['classify_result'] == 'overcount'
        case _:
            raise TypeError("folder_info must be Path, list or dict")

def _is_oversize(folder_info: list[Path]|dict) -> bool:
    match folder_info:
        case list():
            return sum(item.stat().st_size for item in folder_info) > ClassifyConfig.folder_oversize
        case dict():
            return folder_info['classify_result'] == 'oversize'
        case _:
            raise TypeError("folder_info must be Path, list or dict")
def _verify_folder_for_hashing(folder_info: Path|dict|str) -> list:
    '''
    校验文件夹是否符合 hashing 条件
    如果不合规则， raise ValueError，否则返回经过排序的待 hashing 的文件列表
    :param folder_info: 文件夹路径或文件夹信息
    :return: 文件列表
    '''
    file_list = []
    match folder_info:
        case dict():
            if _is_empty_folder(folder_info):
                raise ValueError("folder_info is empty")
            if _is_overcount(folder_info):
                raise ValueError("folder_info is overcount")
            if _is_oversize(folder_info):
                raise ValueError("folder_info is oversize")
            folder_path = Path(folder_info['source_path'])
            file_list = [item for item in folder_path.rglob("*") if item.is_file()]
        case _:
            file_path = Path(folder_info)
            if _is_empty_folder(file_path):
                raise ValueError("folder_info is empty")    
            file_list = [item for item in file_path.rglob("*") if item.is_file()]
            if _is_overcount(file_list):
                raise ValueError("folder_info is overcount")
            if _is_oversize(file_list):
                raise ValueError("folder_info is oversize")
    sorted_file_list = sorted(file_list)
    return sorted_file_list

def _display_hash_progress(file_list: list, alg:str):
    hash_obj = hashlib.new(alg)
    for file in tqdm(file_list, desc=f"Calculating {alg} hash", unit="file"):
        hash_obj.update(calculate_file_hash_base(file, alg).digest())
    hash_result = hash_obj.hexdigest().upper()
    return hash_result
def _not_display_hash_progress(file_list: list, alg:str):
    hash_obj = hashlib.new(alg)
    for file in file_list:
        hash_obj.update(calculate_file_hash_base(file, alg).digest())
    hash_result = hash_obj.hexdigest().upper()
    return hash_result

def calculate_folder_hash(folder_info: str|Path|dict, algorithm: list|None = None,display_hash_progress: bool = True) -> dict:
    file_list = _verify_folder_for_hashing(folder_info)
    algorithm = algorithm or HashConfig.required_hash_algorithms
    hash_result = {}
    for alg in algorithm:
        if display_hash_progress:
            hash_result[alg] = _display_hash_progress(file_list, alg)
        else:
            hash_result[alg] = _not_display_hash_progress(file_list, alg)
    return hash_result

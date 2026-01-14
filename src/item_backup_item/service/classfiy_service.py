
import pathlib
from os import PathLike
from ..config import ZipConfig,ClassifyConfig



def is_zip_file(item:pathlib.Path):
    """
    判断文件是否为压缩文件
    """
    return item.suffix in ZipConfig.zipped_suffix

def is_file_oversize(item:pathlib.Path):
    """
    判断文件大小是否合规，由CLASSIFY_CONFIG.file_oversize决定
    """
    return item.is_file() and item.stat().st_size > ClassifyConfig.file_oversize

def is_empty_folder(item:pathlib.Path):
    return item.is_dir() and not any(item.iterdir())


def is_folder_oversize(item:list[pathlib.Path]):
    """
    判断文件夹大小是否合规，由CLASSIFY_CONFIG.folder_oversize决定
    """
    return sum([i.stat().st_size for i in item]) > ClassifyConfig.folder_oversize


def is_folder_overcount(item:list[pathlib.Path]):
    """
    判断文件夹是否包含超过CLASSIFY_CONFIG.overcount限定的文件数量
    """
    return len(item) > ClassifyConfig.overcount



def classify_item(item:PathLike):
    '''
    对文件或文件夹进行分类
    @param item: 文件或文件夹路径, str or pathlib.Path
    @return: 分类结果, dict, key为文件路径, value为分类结果和项目大小
    '''


    item = pathlib.Path(item)
    if not item.exists():
        raise FileNotFoundError(f"item {item} not exists")
    
    if item.is_file():
        if is_file_oversize(item):
            return {item:{'classify_result':'oversize_file','item_type':'file','item_size':item.stat().st_size}}
        elif is_zip_file(item):
            return {item:{'classify_result':'zip_file','item_type':'file','item_size':item.stat().st_size}}
        else:
            return {item:{'classify_result':'normal_file','item_type':'file','item_size':item.stat().st_size}}
    elif item.is_dir():
        if is_empty_folder(item):
            return {item:{'classify_result':'empty_folder','item_type':'folder','item_size':0}}
        all_file_iter = [i for i in item.rglob("*") if i.is_file()]
        if is_folder_overcount(all_file_iter):
            return {item:{'classify_result':'overcount_folder','item_type':'folder','item_size':sum([i.stat().st_size for i in all_file_iter])}}
        elif is_folder_oversize(all_file_iter):
            return {item:{'classify_result':'oversize_folder','item_type':'folder','item_size':sum([i.stat().st_size for i in all_file_iter])}}
        else:
            return {item:{'classify_result':'normal_folder','item_type':'folder','item_size':sum([i.stat().st_size for i in all_file_iter])}}


def classify_folder(folder:PathLike):
    '''
    对文件夹进行分类,仅对第一层进行分类，不递归分类
    @param folder: 文件夹路径, str or pathlib.Path
    @return: 分类结果, list[dict], key为文件或文件夹路径, value为分类结果和项目大小，格式为{'item_type': 'normal_file', 'size': 123456}
    '''
    folder = pathlib.Path(folder)
    print(folder)
    if not folder.exists():
        raise FileNotFoundError(f"folder {folder} not exists")
    return [classify_item(i) for i in folder.glob("*")]

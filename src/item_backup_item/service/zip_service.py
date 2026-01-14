from dowhen import when
import pyzipper
from os import PathLike
import pathlib
import datetime
from ..config import ZipConfig



def add_self_salt(self):
    self.salt = ZipConfig.salt[self.salt_length]
    
def _add_file_to_zip(zipf: pyzipper.ZipFile, file_path: pathlib.Path, arcname: str) -> None:
    """添加单个文件到ZIP压缩包"""
    zipf.write(file_path, arcname)

def _add_directory_to_zip(zipf: pyzipper.ZipFile, directory_path: pathlib.Path) -> None:
    """
    添加整个目录到ZIP压缩包，确保文件按路径排序[6](@ref)
    """
    all_files = []
    
    # 递归收集所有文件路径[1,2](@ref)
    for file_path in directory_path.rglob('*'):
        if file_path.is_file():
            # 计算在ZIP中的相对路径[11](@ref)
            relative_path = file_path.relative_to(directory_path.parent 
                if directory_path.parent != directory_path 
                else directory_path)
            all_files.append((file_path, str(relative_path)))
    
    # 按路径排序确保一致性[6](@ref)
    all_files.sort(key=lambda x: x[1].lower())
    
    # 按排序后的顺序添加文件
    for file_path, arcname in all_files:
        _add_file_to_zip(zipf, file_path, arcname)


class ZipService:
    """ZIP压缩和解压服务"""
    @staticmethod
    def zip_item(source_item:PathLike,target_dir:PathLike,password:str|None=None,compress_level:int=6):
        source_item = pathlib.Path(source_item)
        target_dir = pathlib.Path(target_dir)
        if not source_item.exists():
            raise FileNotFoundError(f"source_item {source_item} not exists")
        if target_dir.is_file():
            raise IsADirectoryError(f"target_dir {target_dir} is a file,shoud be a folder")
        if not isinstance(compress_level,int):
            raise TypeError(f"compress_level {compress_level} should be int")
        
        if password:
            ziped_item = target_dir / datetime.datetime.now().strftime("%Y%m%d") / f'解压密码_{password}' / f'{source_item.name}.zip'
        else:
            ziped_item = target_dir / datetime.datetime.now().strftime("%Y%m%d") / f'{source_item.name}.zip'
        ziped_item.parent.mkdir(parents=True, exist_ok=True)
        
        compress_level = max(0, min(9, compress_level))
        
        with when(pyzipper.zipfile_aes.AESZipEncrypter,'pwd_verify_length = 2').do(add_self_salt):
            try:
                with pyzipper.AESZipFile(
                    ziped_item,
                    'w',
                    compression=pyzipper.ZIP_DEFLATED,
                    compresslevel=compress_level,
                ) as zipf:
                    # 设置加密密码
                    if password:
                        zipf.setpassword(password.encode('utf-8'))
                        # 设置加密方法（AES加密）
                        zipf.encryption = pyzipper.WZ_AES
                    if source_item.is_file():
                        _add_file_to_zip(zipf, source_item, source_item.name)
                    elif source_item.is_dir():
                        _add_directory_to_zip(zipf, source_item)
                    else:
                        raise ValueError(f"不支持的源路径类型: {source_item}")
                    return ziped_item
            except Exception as e:
                if ziped_item.exists():
                    ziped_item.unlink()
                raise e
    @staticmethod
    def unzip_item(zip_path: PathLike, target_dir: PathLike = None, password: str | None = None) -> pathlib.Path:
        """
        解压ZIP文件到目标目录

        Args:
            zip_path: ZIP文件路径
            target_dir: 目标目录，默认为 ClassifyConfig.unzip_folder
            password: 解压密码（可选）

        Returns:
            解压后的根目录路径
        """
        zip_path = pathlib.Path(zip_path)
        if target_dir is None:
            target_dir = pathlib.Path(ZipConfig.unzip_folder)
        else:
            target_dir = pathlib.Path(target_dir)

        if not zip_path.exists():
            raise FileNotFoundError(f"ZIP文件不存在: {zip_path}")

        target_dir.mkdir(parents=True, exist_ok=True)

        try:
            with pyzipper.AESZipFile(zip_path, 'r') as zipf:
                if password:
                    zipf.setpassword(password.encode('utf-8'))

                # 获取ZIP内的根目录名作为解压后的顶层目录
                all_names = zipf.namelist()
                if all_names:
                    root_prefix = all_names[0].split('/')[0]
                    extract_to = target_dir / root_prefix
                else:
                    extract_to = target_dir / zip_path.stem
                zipf.extractall(target_dir)
                return extract_to
        except Exception as e:
            raise e

from pathlib import Path
import os
from dotenv import load_dotenv
import hashlib
from pprint import pprint
from .openapi_client.api import fileupload_api
from . import openapi_client
from datetime import datetime
from .utils import extract_date_and_password_from_path
load_dotenv()


class UploadService:
    def __init__(self, file_path:str|Path = '',chunk_size: int = 20*1024*1024, rtype: int = 1,env_path:str|Path ='upload.env',temp_dir:str|Path|None = None):
        self.file_path = Path(file_path)
        self.remote_path:str = None # type: ignore
        self.chunk_size:int = chunk_size
        self.rtype:int = rtype
        self.size:int = None # type: ignore
        self.block_list_jsonstr:str = None # type: ignore
        self.block_list:list[str] = None # type: ignore
        self.upload_id:str = None # type: ignore
        self.temp_dir = temp_dir
        self.tmp_list:list[Path] = None # type: ignore
        self.load_env(env_path)


    def load_env(self,env_path:str|Path ='upload.env'):
        if not Path(env_path).exists():
            raise FileNotFoundError(f"env file:{env_path} not found,can not access")
        load_dotenv(env_path)

    def _set_remote_path(self):
        temp_path = '/item_backup/'
        date, password = extract_date_and_password_from_path(self.file_path.absolute().as_posix())
        date = date or datetime.now().strftime("%Y%m%d")
        if not password:
            raise ValueError("upload info Password is missing, please check your file path")
        temp_path = f"{temp_path}{date}/{password}/{self.file_path.name}"
        self.remote_path = temp_path

    def _split_file(self):
        '''
        分片文件，设置self.tmp_list为分片文件列表，并且返回self.tmp_list
        '''
        import shutil

        # 检查文件是否存在
        if not Path(self.file_path).exists():
            raise FileNotFoundError(f"file_path:{self.file_path} not exists")


        # 如果文件大小小于等于块大小，则直接返回文件路径,无需分块
        if self.file_path.stat().st_size <= self.chunk_size:
            self.tmp_list = [self.file_path]
            return self.tmp_list

        # 确保临时目录存在
        if not self.temp_dir:
            self.temp_dir = self.file_path.parent / f"temp_{self.file_path.name}"
        self.temp_dir = Path(self.temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        # 清空临时目录，确保没有残留文件
        shutil.rmtree(self.temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        
        # 4. 初始化存储分块文件路径的列表和分块计数器
        paths:list[Path] = []
        partnum = 0
        
        # 5. 以二进制读模式打开源文件
        with open(self.file_path, 'rb') as inputfile:
            # 6. 从源文件路径中提取文件名（不包含目录）
            file_name = self.file_path.name
            
            # 7. 循环读取源文件，直到文件结束
            while True:
                # 8. 读取指定大小的数据块
                chunk = inputfile.read(self.chunk_size)
                
                # 9. 如果读取到的数据为空，说明已到文件末尾，退出循环
                if not chunk:
                    break
                
                # 10. 构建分块文件的完整路径，使用路径拼接运算符 `/`
                filename = self.temp_dir / f'{file_name}.part{partnum:04d}'
                
                # 11. 将分块文件路径（Path对象）添加到列表中
                paths.append(filename)
                
                # 12. 以二进制写模式创建并打开分块文件，写入数据块
                with open(filename, 'wb') as fileobj:
                    fileobj.write(chunk)
                
                # 13. 分块计数器加1，为下一个分块文件准备
                partnum += 1
        
        # 14. 返回所有分块文件的路径列表
        self.tmp_list = paths
        return self.tmp_list


    def _calculate_md5(self,data):
        """
        计算数据的MD5值
        :param data: 数据
        :return: MD5值
        """
        md5_hash = hashlib.md5()
        md5_hash.update(data)
        return md5_hash.hexdigest().lower()

    def _create_block_list(self):
        import json
        block_list = []
        self._split_file()
        for file_path in self.tmp_list:
            with open(file_path, 'rb') as f:
                md5 = self._calculate_md5(f.read())
                block_list.append(md5)
        return json.dumps(block_list)
    

    def precreate(self):
        """
        precreate
        """
        if not self.file_path.exists():
            raise FileNotFoundError(f"file_path:{self.file_path} not exists!")
        if not self.file_path.is_file():
            raise ValueError(f"file_path:{self.file_path} is not a file,folder is not supported")
        self._set_remote_path()
        #    Enter a context with an instance of the API client
        with openapi_client.ApiClient() as api_client:
            # Create an instance of the API class
            api_instance = fileupload_api.FileuploadApi(api_client)
            access_token = os.getenv("BAIDU_PAN_ACCESS_TOKEN")  # str |
            path = self.remote_path  # str | 对于一般的第三方软件应用，路径以 "/apps/your-app-name/" 开头。对于小度等硬件应用，路径一般 "/来自：小度设备/" 开头。对于定制化配置的硬件应用，根据配置情况进行填写。
            isdir = 0  # int | isdir
            self.size = self.file_path.stat().st_size  # int | size
            autoinit = 1  # int | autoinit
            self.block_list_jsonstr = self._create_block_list() # str | 由MD5字符串组成的list
            rtype = self.rtype  # int | rtype (optional)
            # example passing only required values which don't have defaults set
            # and optional values
            try:
                api_response = api_instance.xpanfileprecreate(
                    access_token, path, isdir, self.size, autoinit, self.block_list_jsonstr, rtype=rtype)
                print(api_response)
                self.upload_id = api_response['uploadid']
                self.block_list = api_response['block_list']
                return self
            except openapi_client.ApiException as e:
                print("Exception when calling FileuploadApi->xpanfileprecreate: %s\n" % e)
                exit(-1)
    
    def _get_file(self, partseq):
        try:
            file = open(self.tmp_list[partseq], 'rb')
            return file
        except Exception as e:
            print(f"Exception when open file:{e}")
            exit(-1)
    def upload(self):
        """
        upload
        """
        # Enter a context with an instance of the API client
        with openapi_client.ApiClient() as api_client:
            # Create an instance of the API class
            api_instance = fileupload_api.FileuploadApi(api_client)
            access_token = os.getenv("BAIDU_PAN_ACCESS_TOKEN")  # str |
            for partseq in self.block_list:
                path = self.remote_path  # str |
                uploadid = self.upload_id  # str |
                type = "tmpfile"  # str |
                file = self._get_file(partseq)  # file_type | 要进行传送的本地文件分片
                # example passing only required values which don't have defaults set
                # and optional values
                try:
                    api_response = api_instance.pcssuperfile2(
                        access_token, str(partseq), path, uploadid, type, file=file)
                    pprint(api_response)
                except openapi_client.ApiException as e:
                    print("Exception when calling FileuploadApi->pcssuperfile2: %s\n" % e)
        print("upload done")
        return self

    def create(self):
        """
        create
        """
        # Enter a context with an instance of the API client
        with openapi_client.ApiClient() as api_client:
            # Create an instance of the API class
            api_instance = fileupload_api.FileuploadApi(api_client)
            access_token = os.getenv("BAIDU_PAN_ACCESS_TOKEN")  # str |
            path = self.remote_path  # str | 与precreate的path值保持一致
            isdir = 0  # int | isdir
            size = self.size # int | 与precreate的size值保持一致
            uploadid = self.upload_id  # str | precreate返回的uploadid
            block_list = self.block_list_jsonstr  # str | 与precreate的block_list值保持一致
            rtype = self.rtype  # int | rtype (optional)

            # example passing only required values which don't have defaults set
            # and optional values
            try:
                api_response = api_instance.xpanfilecreate(
                    access_token, path, isdir, size, uploadid, block_list, rtype=rtype)
                pprint(api_response)
                return api_response
            except openapi_client.ApiException as e:
                print("Exception when calling FileuploadApi->xpanfilecreate: %s\n" % e)
    def _clean_tmp(self):
        if self.temp_dir:
            import shutil
            shutil.rmtree(self.temp_dir)

    def upload_file(self,file_path:str|Path,chunk_size: int = 20*1024*1024, rtype: int = 1,temp_dir:str|Path|None = None):
        self.file_path = Path(file_path)
        self.chunk_size = chunk_size
        self.rtype = rtype
        self.temp_dir = temp_dir
        self.precreate()
        self.upload()
        result = self.create()
        self._clean_tmp()
        return result
        
    def __del__(self):
        import shutil
        if self.temp_dir:
            shutil.rmtree(self.temp_dir)



if __name__ == "__main__":
    params = {
        'file_path':r'd:\压缩测试\20260115\解压密码_H_x123456789\normal_folder.zip',
        'chunk_size': 20*1024*1024,
        'rtype': 1
    }
    upload_service = UploadService(**params)
    upload_service.precreate().upload().create()

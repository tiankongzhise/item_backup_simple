from pathlib import Path
import os
from dotenv import load_dotenv
from pathlib import Path
import hashlib
from pprint import pprint
from .openapi_client.api import fileupload_api
from . import openapi_client
from io import BytesIO
load_dotenv()


class UploadService:
    def __init__(self, file_path:str|Path, remote_path:str="/apps/test_upload/",chunk_size: int = 20*1024*1024, rtype: int = 1,env_path:str|Path ='upload.env'):
        self.file_path = Path(file_path)
        self.remote_path = self._create_remote_path(remote_path)
        self.chunk_size = chunk_size
        self.rtype = rtype
        self.size = None # type: ignore
        self.block_list_jsonstr = None # type: ignore
        self.block_list:list = None # type: ignore
        self.upload_id:str = None # type: ignore
        self.load_env(env_path)
        

    def load_env(self,env_path:str|Path ='upload.env'):
        if not Path(env_path).exists():
            raise FileNotFoundError(f"env file:{env_path} not found,can not access")
        load_dotenv(env_path)

    def _create_remote_path(self,remote_path:str):
        temp = remote_path if remote_path.endswith("/") else f"{remote_path}/"
        temp = temp if temp.startswith("/") else f"/{temp}"
        return temp


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
        file_size = self.file_path.stat().st_size
        with open(self.file_path, 'rb') as f:
            for i in range(0, file_size, self.chunk_size):
                data = f.read(self.chunk_size)
                md5 = self._calculate_md5(data)
                block_list.append(md5)
        return json.dumps(block_list)
    def _get_file_data(self,offset:int):
        """
        从指定偏移量读取指定大小的文件块
        
        Args:
            file_path: 文件路径
            offset: 偏移量（字节）
            chunk_size: 要读取的块大小（字节）
        
        Returns:
            读取的数据（字节串）
        """
        try:
            with open(self.file_path, 'rb') as f:  # 二进制模式打开
                f.seek(offset)  # 移动到指定偏移量
                data = f.read(self.chunk_size)  # 读取指定大小的数据
            return BytesIO(data)
        except Exception as e:
            print("Exception when open file: %s\n" % e)
            exit(-1)
    def precreate(self):
        """
        precreate
        """
        if not self.file_path.exists():
            raise FileNotFoundError(f"file_path:{self.file_path} not exists!")
        if not self.file_path.is_file():
            raise ValueError(f"file_path:{self.file_path} is not a file,folder is not supported")
        #    Enter a context with an instance of the API client
        with openapi_client.ApiClient() as api_client:
            # Create an instance of the API class
            api_instance = fileupload_api.FileuploadApi(api_client)
            access_token = os.getenv("BAIDU_PAN_ACCESS_TOKEN")  # str |
            path = f"{self.remote_path}{self.file_path.name}"  # str | 对于一般的第三方软件应用，路径以 "/apps/your-app-name/" 开头。对于小度等硬件应用，路径一般 "/来自：小度设备/" 开头。对于定制化配置的硬件应用，根据配置情况进行填写。
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
                path = f'{self.remote_path}{self.file_path.name}'  # str |
                uploadid = self.upload_id  # str |
                type = "tmpfile"  # str |
                file = self._get_file_data(partseq)  # file_type | 要进行传送的本地文件分片
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
            path = f"{self.remote_path}{self.file_path.name}"  # str | 与precreate的path值保持一致
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
                return self
            except openapi_client.ApiException as e:
                print("Exception when calling FileuploadApi->xpanfilecreate: %s\n" % e)


if __name__ == "__main__":
    params = {
        'file_path':r'd:\压缩测试\20260115\解压密码_H_x123456789\normal_folder.zip',
        'remote_path': '/apps/test_upload/',
        'chunk_size': 20*1024*1024,
        'rtype': 1
    }
    upload_service = UploadService(**params)
    upload_service.precreate().upload().create()

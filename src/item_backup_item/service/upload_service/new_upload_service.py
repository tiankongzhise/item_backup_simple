import os
import json
import hashlib
import asyncio
import aiohttp
import requests
from pathlib import Path
from typing import Optional, Dict, List, Callable
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import logging
from dotenv import load_dotenv

@dataclass
class UploadConfig:
    """上传配置类"""
    chunk_size: int = 4 * 1024 * 1024  # 默认分片大小4MB
    max_workers: int = 4  # 最大并发数
    retry_times: int = 3  # 重试次数
    timeout: int = 30  # 超时时间
    use_async: bool = True  # 是否使用异步模式

@dataclass
class UploadProgress:
    """上传进度记录"""
    file_path: str
    remote_path: str
    file_size: int
    uploaded_size: int = 0
    chunks_uploaded: List[int] = None
    upload_id: str = None
    block_list: List[str] = None
    
    def __post_init__(self):
        if self.chunks_uploaded is None:
            self.chunks_uploaded = []
        if self.block_list is None:
            self.block_list = []

class BaiduPanUploadService:
    """百度网盘上传服务核心类"""
    
    def __init__(self, access_token: str, config: UploadConfig = None):
        self.access_token = access_token
        self.config = config or UploadConfig()
        self.progress_file = "upload_progress.json"
        self.upload_progress: Dict[str, UploadProgress] = {}
        
        # API端点 [1,3](@ref)
        self.precreate_api = "https://pan.baidu.com/rest/2.0/xpan/file?method=precreate"
        self.upload_api = "https://d.pcs.baidu.com/rest/2.0/pcs/superfile2?method=upload"
        self.create_api = "https://pan.baidu.com/rest/2.0/xpan/file?method=create"
        
        self.load_progress()
        self._setup_logging()
    
    def _setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def load_progress(self):
        """加载上传进度"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r') as f:
                    data = json.load(f)
                    for key, progress_data in data.items():
                        self.upload_progress[key] = UploadProgress(**progress_data)
                self.logger.info(f"加载历史上传进度: {len(self.upload_progress)}个文件")
        except Exception as e:
            self.logger.warning(f"加载进度文件失败: {e}")
    
    def save_progress(self):
        """保存上传进度"""
        try:
            with open(self.progress_file, 'w') as f:
                data = {k: v.__dict__ for k, v in self.upload_progress.items()}
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"保存进度文件失败: {e}")
    
    def _get_file_key(self, local_path: str, remote_path: str) -> str:
        """生成文件唯一标识"""
        return f"{local_path}:{remote_path}"
    
    def calculate_file_md5(self, file_path: str) -> str:
        """计算文件MD5 [1,3](@ref)"""
        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()
    
    def calculate_chunk_md5(self, data: bytes) -> str:
        """计算分片MD5"""
        return hashlib.md5(data).hexdigest()
    
    def split_file_chunks(self, file_path: str) -> List[int]:
        """计算文件分片信息"""
        file_size = os.path.getsize(file_path)
        chunks = []
        for offset in range(0, file_size, self.config.chunk_size):
            chunks.append(offset)
        return chunks

    def upload_file_sync(self, local_path: str, remote_path: str, 
                            progress_callback: Callable = None) -> bool:
            """
            同步上传文件 [1,3](@ref)
            支持断点续传和大文件分片上传
            """
            if not os.path.exists(local_path):
                self.logger.error(f"文件不存在: {local_path}")
                return False
            
            file_key = self._get_file_key(local_path, remote_path)
            file_size = os.path.getsize(local_path)
            
            # 检查是否存在上传进度
            if file_key in self.upload_progress:
                progress = self.upload_progress[file_key]
                self.logger.info(f"发现上传进度，继续上传: {local_path}")
            else:
                progress = UploadProgress(local_path, remote_path, file_size)
                self.upload_progress[file_key] = progress
            
            try:
                # 1. 预创建文件 [3](@ref)
                if not progress.upload_id:
                    upload_id = self._precreate_file(remote_path, file_size, local_path)
                    if not upload_id:
                        return False
                    progress.upload_id = upload_id
                    self.save_progress()
                
                # 2. 分片上传
                chunks = self.split_file_chunks(local_path)
                for i, offset in enumerate(chunks):
                    if i in progress.chunks_uploaded:
                        continue  # 跳过已上传分片
                    
                    success = self._upload_chunk_sync(local_path, remote_path, 
                                                    progress.upload_id, i, offset)
                    if success:
                        progress.chunks_uploaded.append(i)
                        progress.uploaded_size = min(progress.uploaded_size + self.config.chunk_size, file_size)
                        self.save_progress()
                        
                        if progress_callback:
                            progress_callback(progress.uploaded_size, file_size)
                    else:
                        self.logger.error(f"分片 {i} 上传失败")
                        return False
                
                # 3. 创建文件 [1](@ref)
                return self._create_file(remote_path, progress.upload_id, file_size, progress.block_list)
                
            except Exception as e:
                self.logger.error(f"上传过程中发生错误: {e}")
                return False
            finally:
                # 上传完成清理进度
                if progress.uploaded_size >= file_size:
                    self.upload_progress.pop(file_key, None)
                    self.save_progress()
        
    def _precreate_file(self, remote_path: str, file_size: int, file_path: str) -> Optional[str]:
        """预创建文件 [3](@ref)"""
        # 计算文件分片MD5列表
        block_list = []
        with open(file_path, 'rb') as f:
            while True:
                chunk_data = f.read(self.config.chunk_size)
                if not chunk_data:
                    break
                chunk_md5 = self.calculate_chunk_md5(chunk_data)
                block_list.append(chunk_md5)
        
        params = {
            'access_token': self.access_token,
            'method': 'precreate'
        }
        
        data = {
            'path': remote_path,
            'size': file_size,
            'isdir': 0,
            'autoinit': 1,
            'block_list': json.dumps(block_list)
        }
        
        try:
            response = requests.post(self.precreate_api, params=params, data=data)
            result = response.json()
            
            if result.get('errno') == 0:
                self.logger.info(f"文件预创建成功: {remote_path}")
                return result['uploadid']
            else:
                self.logger.error(f"文件预创建失败: {result}")
                return None
        except Exception as e:
            self.logger.error(f"预创建请求失败: {e}")
            return None
    
    def _upload_chunk_sync(self, local_path: str, remote_path: str, 
                          upload_id: str, partseq: int, offset: int) -> bool:
        """同步上传分片"""
        for attempt in range(self.config.retry_times):
            try:
                with open(local_path, 'rb') as f:
                    f.seek(offset)
                    chunk_data = f.read(self.config.chunk_size)
                
                params = {
                    'access_token': self.access_token,
                    'method': 'upload',
                    'type': 'tmpfile',
                    'path': remote_path,
                    'uploadid': upload_id,
                    'partseq': partseq
                }
                
                files = {'file': chunk_data}
                response = requests.post(self.upload_api, params=params, files=files)
                result = response.json()
                
                if result.get('errno') == 0:
                    self.logger.debug(f"分片 {partseq} 上传成功")
                    return True
                else:
                    self.logger.warning(f"分片 {partseq} 上传失败，尝试重试: {result}")
            
            except Exception as e:
                self.logger.warning(f"分片 {partseq} 上传异常: {e}")
        
        return False
    
    def _create_file(self, remote_path: str, upload_id: str, 
                    file_size: int, block_list: List[str]) -> bool:
        """创建文件 [1](@ref)"""
        params = {
            'access_token': self.access_token,
            'method': 'create'
        }
        
        data = {
            'path': remote_path,
            'size': file_size,
            'isdir': 0,
            'uploadid': upload_id,
            'block_list': json.dumps(block_list)
        }
        
        try:
            response = requests.post(self.create_api, params=params, data=data)
            result = response.json()
            
            if result.get('errno') == 0:
                self.logger.info(f"文件创建成功: {remote_path}")
                return True
            else:
                self.logger.error(f"文件创建失败: {result}")
                return False
        except Exception as e:
            self.logger.error(f"创建文件请求失败: {e}")
            return False
    async def upload_file_async(self, local_path: str, remote_path: str,
                              progress_callback: Callable = None) -> bool:
        """
        异步上传文件
        支持并发分片上传，提高大文件上传效率 [8](@ref)
        """
        if not os.path.exists(local_path):
            self.logger.error(f"文件不存在: {local_path}")
            return False
        
        file_key = self._get_file_key(local_path, remote_path)
        file_size = os.path.getsize(local_path)
        
        if file_key in self.upload_progress:
            progress = self.upload_progress[file_key]
            self.logger.info(f"发现上传进度，继续上传: {local_path}")
        else:
            progress = UploadProgress(local_path, remote_path, file_size)
            self.upload_progress[file_key] = progress
        
        try:
            # 预创建文件
            if not progress.upload_id:
                upload_id = await self._precreate_file_async(remote_path, file_size, local_path)
                if not upload_id:
                    return False
                progress.upload_id = upload_id
                self.save_progress()
            
            # 并发上传分片
            chunks = self.split_file_chunks(local_path)
            upload_tasks = []
            
            for i, offset in enumerate(chunks):
                if i in progress.chunks_uploaded:
                    continue
                
                task = self._upload_chunk_async(local_path, remote_path, 
                                              progress.upload_id, i, offset, progress)
                upload_tasks.append(task)
            
            # 等待所有分片上传完成
            results = await asyncio.gather(*upload_tasks, return_exceptions=True)
            
            # 检查结果
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.error(f"分片上传异常: {result}")
                    return False
                elif not result:
                    self.logger.error(f"分片 {i} 上传失败")
                    return False
            
            # 创建文件
            success = await self._create_file_async(remote_path, progress.upload_id, 
                                                 file_size, progress.block_list)
            
            if success and progress_callback:
                progress_callback(file_size, file_size)
                
            return success
            
        except Exception as e:
            self.logger.error(f"异步上传过程中发生错误: {e}")
            return False
        finally:
            if progress.uploaded_size >= file_size:
                self.upload_progress.pop(file_key, None)
                self.save_progress()
    
    async def _precreate_file_async(self, remote_path: str, file_size: int, 
                                  file_path: str) -> Optional[str]:
        """异步预创建文件"""
        block_list = []
        
        # 使用线程池计算MD5，避免阻塞事件循环
        with ThreadPoolExecutor() as executor:
            loop = asyncio.get_event_loop()
            with open(file_path, 'rb') as f:
                while True:
                    chunk_data = await loop.run_in_executor(executor, f.read, self.config.chunk_size)
                    if not chunk_data:
                        break
                    chunk_md5 = await loop.run_in_executor(executor, self.calculate_chunk_md5, chunk_data)
                    block_list.append(chunk_md5)
        
        params = {
            'access_token': self.access_token,
            'method': 'precreate'
        }
        
        data = {
            'path': remote_path,
            'size': file_size,
            'isdir': 0,
            'autoinit': 1,
            'block_list': json.dumps(block_list)
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.precreate_api, params=params, data=data) as response:
                    result = await response.json()
                    
                    if result.get('errno') == 0:
                        self.logger.info(f"文件预创建成功: {remote_path}")
                        return result['uploadid']
                    else:
                        self.logger.error(f"文件预创建失败: {result}")
                        return None
        except Exception as e:
            self.logger.error(f"异步预创建请求失败: {e}")
            return None
    
    async def _upload_chunk_async(self, local_path: str, remote_path: str,
                                upload_id: str, partseq: int, offset: int,
                                progress: UploadProgress) -> bool:
        """异步上传分片"""
        for attempt in range(self.config.retry_times):
            try:
                # 异步读取文件分片
                loop = asyncio.get_event_loop()
                with open(local_path, 'rb') as f:
                    f.seek(offset)
                    chunk_data = await loop.run_in_executor(None, f.read, self.config.chunk_size)
                
                params = {
                    'access_token': self.access_token,
                    'method': 'upload',
                    'type': 'tmpfile',
                    'path': remote_path,
                    'uploadid': upload_id,
                    'partseq': partseq
                }
                
                async with aiohttp.ClientSession() as session:
                    form_data = aiohttp.FormData()
                    form_data.add_field('file', chunk_data, 
                                      filename=f'chunk_{partseq}',
                                      content_type='application/octet-stream')
                    
                    async with session.post(self.upload_api, params=params, data=form_data) as response:
                        result = await response.json()
                        
                        if result.get('errno') == 0:
                            progress.uploaded_size = min(progress.uploaded_size + len(chunk_data), progress.file_size)
                            progress.chunks_uploaded.append(partseq)
                            self.save_progress()
                            return True
                        else:
                            self.logger.warning(f"分片 {partseq} 上传失败: {result}")
            
            except Exception as e:
                self.logger.warning(f"分片 {partseq} 上传异常: {e}")
        
        return False
    
    async def _create_file_async(self, remote_path: str, upload_id: str,
                               file_size: int, block_list: List[str]) -> bool:
        """异步创建文件"""
        params = {
            'access_token': self.access_token,
            'method': 'create'
        }
        
        data = {
            'path': remote_path,
            'size': file_size,
            'isdir': 0,
            'uploadid': upload_id,
            'block_list': json.dumps(block_list)
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self.create_api, params=params, data=data) as response:
                    result = await response.json()
                    
                    if result.get('errno') == 0:
                        self.logger.info(f"文件创建成功: {remote_path}")
                        return True
                    else:
                        self.logger.error(f"文件创建失败: {result}")
                        return False
        except Exception as e:
            self.logger.error(f"异步创建文件请求失败: {e}")
            return False

    def upload_files(self, file_pairs: List[tuple], use_async: bool = None) -> Dict[str, bool]:
            """
            批量上传文件
            file_pairs: [(local_path, remote_path), ...]
            """
            use_async = use_async if use_async is not None else self.config.use_async
            results = {}
            
            if use_async:
                # 异步批量上传
                async def batch_upload():
                    tasks = []
                    for local_path, remote_path in file_pairs:
                        task = self.upload_file_async(local_path, remote_path)
                        tasks.append((local_path, remote_path, task))
                    
                    for local_path, remote_path, task in tasks:
                        try:
                            results[f"{local_path}->{remote_path}"] = await task
                        except Exception as e:
                            self.logger.error(f"上传失败 {local_path}: {e}")
                            results[f"{local_path}->{remote_path}"] = False
                    
                    return results
                
                return asyncio.run(batch_upload())
            else:
                # 同步批量上传
                for local_path, remote_path in file_pairs:
                    try:
                        success = self.upload_file_sync(local_path, remote_path)
                        results[f"{local_path}->{remote_path}"] = success
                    except Exception as e:
                        self.logger.error(f"上传失败 {local_path}: {e}")
                        results[f"{local_path}->{remote_path}"] = False
                
                return results
    
    def resume_upload(self, local_path: str, remote_path: str) -> bool:
        """恢复上传（断点续传）"""
        file_key = self._get_file_key(local_path, remote_path)
        if file_key not in self.upload_progress:
            self.logger.info("未找到上传进度，开始新上传")
            return self.upload_file_sync(local_path, remote_path)
        
        progress = self.upload_progress[file_key]
        self.logger.info(f"恢复上传: {local_path}, 已上传: {progress.uploaded_size}/{progress.file_size} bytes")
        
        if self.config.use_async:
            return asyncio.run(self.upload_file_async(local_path, remote_path))
        else:
            return self.upload_file_sync(local_path, remote_path)
    
    def set_chunk_size(self, chunk_size: int):
        """设置分片大小（必须是4MB的倍数）[13](@ref)"""
        if chunk_size % (4 * 1024 * 1024) != 0:
            self.logger.warning("分片大小建议设置为4MB的倍数以获得最佳性能")
        self.config.chunk_size = chunk_size
    
    def get_upload_stats(self) -> Dict:
        """获取上传统计信息"""
        total_files = len(self.upload_progress)
        in_progress = sum(1 for p in self.upload_progress.values() 
                         if p.uploaded_size < p.file_size)
        
        return {
            'total_files': total_files,
            'in_progress': in_progress,
            'completed': total_files - in_progress,
            'progress_files': list(self.upload_progress.keys())
        }



def main():
    load_dotenv('upload.env')
    """使用示例"""
    # 初始化上传服务
    access_token = os.getenv('BAIDU_PAN_ACCESS_TOKEN')
    config = UploadConfig(
        chunk_size=20 * 1024 * 1024,  # 4MB分片
        max_workers=4,
        use_async=True
    )
    
    upload_service = BaiduPanUploadService(access_token, config)
    
    def progress_callback(uploaded, total):
        percent = (uploaded / total) * 100
        print(f"\r上传进度: {uploaded}/{total} ({percent:.1f}%)", end='')
    
    # 单个文件上传
    try:
        # 同步上传
        success = upload_service.upload_file_sync(
            r'l:\QQDownload\bns_1.63.3865.2_setup_bin.7z.005', 
            r'/apps/your_app/bns_1.63.3865.2_setup_bin.7z.005',
            progress_callback=progress_callback
        )
        
        # 异步上传
        success = asyncio.run(upload_service.upload_file_async(
            r'l:\QQDownload\bns_1.63.3865.2_setup_bin.7z.005',
            r'/apps/your_app/bns_1.63.3865.2_setup_bin.7z.0051', 
            progress_callback=progress_callback
        ))
        
    except Exception as e:
        print(f"上传失败: {e}")
    
    # # 批量上传
    # file_pairs = [
    #     ("file1.txt", "/apps/your_app/file1.txt"),
    #     ("file2.jpg", "/apps/your_app/file2.jpg"),
    #     ("file3.pdf", "/apps/your_app/file3.pdf")
    # ]
    
    # results = upload_service.upload_files(file_pairs)
    # for file_pair, success in results.items():
    #     status = "成功" if success else "失败"
    #     print(f"{file_pair}: {status}")

if __name__ == "__main__":
    main()
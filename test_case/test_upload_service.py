from item_backup_item.service.upload_service import UploadService




def test_upload_service():
    params = {
        # 'file_path':r'l:\QQDownload\bns_1.63.3865.2_setup_bin.7z.005',
        'file_path': r'd:\压缩测试\20260115\解压密码_H_x123456789\normal_folder.zip',
        'chunk_size': 20*1024*1024,
        'rtype': 1
    }
    upload_service = UploadService(**params)
    upload_service.precreate().upload().create()

def new_test_upload_service():
    params = {
        'file_path': r'd:\压缩测试\20260115\解压密码_H_x123456789\normal_folder.zip',
        'chunk_size': 20*1024*1024,
        'rtype': 1
    }
    params1 = {
        'file_path': r'd:\压缩测试\20260115\解压密码_H_x123456789\normal_file.exe.zip',
        'chunk_size': 20*1024*1024,
        'rtype': 1
    }
    upload_service = UploadService()
    result = upload_service.upload_file(**params)
    print(f"result: {result}")
    result1 = upload_service.upload_file(**params1)
    print(f"result1: {result1}")

if __name__ == "__main__":
    test_upload_service()
    # new_test_upload_service()

from item_backup_item.service.upload_service import UploadService


def test_upload_service():
    params = {
        'file_path':r'd:\压缩测试\20260115\解压密码_H_x123456789\normal_folder.zip',
        'remote_path': '/apps/test_upload/',
        'chunk_size': 20*1024*1024,
        'rtype': 1
    }
    upload_service = UploadService(**params)
    upload_service.precreate().upload().create()

if __name__ == "__main__":
    test_upload_service()

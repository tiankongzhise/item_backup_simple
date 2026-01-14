from item_backup_item.service.zip_service import ZipService


def test_zip_service(path,target_dir,password,compress_type):
    zip_service = ZipService()
    zip_service.zip_item(path, target_dir, password, compress_type)


def test_unzip_service(zip_path, target_dir, password):
    zip_service = ZipService()
    zip_service.unzip_item(zip_path, target_dir, password)

if __name__ == "__main__":
    # test_zip_service(r'E:\移动文件测试源', r'E:\压缩结果', 'H_x123456789', 0)
    test_unzip_service(r'e:\压缩结果\20260114\解压密码_H_x123456789\移动文件测试源.zip', r'E:\移动文件测试源', 'H_x123456789')

from item_backup_item.service.calculate_hash_service import CalculateHashService


def test_oversize_file(path):
    print("Testing oversize file: ", path)
    service = CalculateHashService()
    try:
        service.calculate_file_hash(path)
    except ValueError as e:
        print("File is oversize")

def test_oversize_folder(path):
    print("Testing oversize folder: ", path)
    service = CalculateHashService()
    try:
        service.calculate_folder_hash(path)
    except ValueError as e:
        print("Folder is oversize")

def test_empty_folder(path):
    print("Testing empty folder: ", path)
    service = CalculateHashService()
    try:
        service.calculate_folder_hash(path)
    except ValueError as e:
        print("Folder is empty")

def test_overcount_folder(path):
    print("Testing overcount folder: ", path)
    service = CalculateHashService()
    try:
        service.calculate_folder_hash(path)
    except ValueError as e:
        print("Folder has too many files")

def test_hash_file(path):
    print("Testing hash file: ", path)
    service = CalculateHashService()
    result = service.calculate_file_hash(path)
    print("File hash: ", result)

if __name__ == "__main__":
    # oversize_file = r'd:\测试用例\oversize_file.zip'
    # oversize_folder = r'd:\测试用例\oversize_folder'
    # empty_folder = r'd:\测试用例\empty_folder'
    # overcount_folder = r'd:\测试用例\overcount_folder'
    # test_oversize_file(oversize_file)
    # test_oversize_folder(oversize_folder)
    # test_empty_folder(empty_folder)
    # test_overcount_folder(overcount_folder)
    test_hash_file(r'e:\压缩结果\20260114\解压密码_H_x123456789\移动文件测试源.zip')

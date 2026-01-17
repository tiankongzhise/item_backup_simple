from .control import classify_process
from .control import hash_process
from .control import zip_process
from .control import zip_hash_process
from .control import unzip_process
from .control import unzip_hash_process
from .control import upload_process
from .control import delete_process
from .single_file_processor import SingleFileMainProcessor


def batch_main():
    """原有的批处理模式主函数"""
    result = classify_process()
    result = hash_process()
    result = zip_process()
    result = zip_hash_process()
    result = unzip_process()
    result = unzip_hash_process()
    result = upload_process()   
    result = delete_process()
    print(result)

def single_file_main():
    """新的单文件处理模式主函数"""
    processor = SingleFileMainProcessor()
    processor.run()

def main(mode="single"):
    """主函数入口，支持多种模式
    
    Args:
        mode (str): 处理模式，"single"为单文件处理，"batch"为批处理
    """
    if mode == "single":
        print("使用单文件处理模式")
        single_file_main()
    elif mode == "batch":
        print("使用批处理模式")
        batch_main()
    else:
        raise ValueError(f"不支持的模式: {mode}")


if __name__ == '__main__':
    # 默认使用单文件处理模式
    main(mode="single")
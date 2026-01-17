from .control import classify_process, single_file_process
from .config import ProcessingConfig

def main():
    """主处理函数 - 使用新的单文件处理模式"""
    config = ProcessingConfig()
    
    # 首先执行分类流程，生成待处理文件列表
    print("开始文件分类...")
    result = classify_process()
    print(f"分类完成，共处理 {result} 个文件")
    
    # 启动单文件处理流程
    print("启动单文件处理流程...")
    single_file_process()
    
    print("所有处理流程完成")



if __name__ == '__main__':
    main()

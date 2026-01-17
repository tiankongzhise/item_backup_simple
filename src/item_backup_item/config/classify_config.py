from dataclasses import dataclass

@dataclass
class ClassifyConfig:
    '''
    项目分配配置，参数说明
    args:
        sources_list: list[str] = 记录需要处理的源文件夹或者源文件
        zipped_folder: str = 压缩后的结果保存到此文件夹
        unzip_folder: str = 将压缩后的文件解压到此文件夹与源文件进行比较
        max_byte_size: int = 压缩时每次压缩的文件大小阈值，单位字节
        overcount: int = hash文件夹时，单个文件夹下最多允许的子项数量,仅计算有效文件，不计算文件夹
        file_oversize: int = hash计算时，单个文件大小阈值，单位字节,建议此大小不要超过百度网盘上传限制,
        folder_oversize: int = hash计算时，单个文件夹大小阈值，单位字节,建议此大小不要超过百度网盘上传限制
        baidu_pan_upload_max_size: int = 百度网盘上传文件大小限制，SVIP为20G,VIP为10G,普通用户为4G
    
    '''


    # 记录需要处理的源文件夹或者源文件
    sources_list = [r'D:\测试AI运行']

    # 压缩时每次压缩的文件大小阈值，单位字节
    max_byte_size = 1024 * 1024 * 500 # 500M

    # hash文件夹时，单个文件夹下最多允许的子项数量,仅计算有效文件，不计算文件夹
    overcount = 100

    # hash计算时，单个文件大小阈值，单位字节,建议此大小不要超过百度网盘上传限制
    file_oversize = 1024 * 1024 * 1024 * 19

    # hash计算时，单个文件夹大小阈值，单位字节,建议此大小不要超过百度网盘上传限制
    folder_oversize = 1024 * 1024 * 1024 * 19

    # 百度网盘上传文件大小限制，SVIP为20G,VIP为10G,普通用户为4G
    baidu_pan_upload_max_size = 1024 * 1024 * 1024 * 20

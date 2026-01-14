from dataclasses import dataclass
@dataclass
class ZipConfig:
    # 压缩文件后缀
    zipped_suffix= [
        ".zip",
        ".rar",
        ".7z",
        ".tar.gz",
        ".gz",
        ".tar.bz2",
        ".tar.xz",
        ".tgz",
        ".tar",
        ".bz2",
    ]

    # 压缩密码盐
    salt = {
        8: b"\xaa%\xec\xec[\x94\xbex",
        12: b"}y\xd5\x19A\xa2\xf6\x1b\xce\x86\x7f\x85",
        16: b"\xd1\x12_\xd7\xd7\n\x92\xfdC\x84\re\xcdxD\x0b",
    }

    # 压缩后的结果保存到此文件夹
    zipped_folder= r'D:\压缩测试'

    # 将压缩后的文件解压到此文件夹与源文件进行比较
    unzip_folder = r'D:\解压测试'
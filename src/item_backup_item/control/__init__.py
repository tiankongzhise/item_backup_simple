from .classify import classify_process
from .source_hash import hash_process
from .zip import zip_process
from .zip_hash import zip_hash_process
from .unzip import unzip_process
from .unzip_hash import unzip_hash_process
from .upload import upload_process
from .delete import delete_process
__all__ = [
    "classify_process",
    "hash_process",
    "zip_process",
    "zip_hash_process",
    "unzip_process",
    "unzip_hash_process",   
    "upload_process",
    "delete_process",
]

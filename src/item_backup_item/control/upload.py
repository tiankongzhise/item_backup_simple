from ..database import MySQLClient as Client
from ..database import ItemProcessRecord as UnzipHashProcessTable
from ..service import CalculateHashService, get_email_notifier
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Type
from copy import deepcopy

def get_host_name():
    import os

    return os.getenv("COMPUTERNAME")


def _create_calculate_info(db_data):
    result = {}
    for item in db_data:
        result[item.id] = {
            'file'
        }
    return result


def _need_upload_records(client: Client, table: Type[UnzipHashProcessTable]):
    from sqlalchemy import select , and_,or_
    stmt = (
        select(table)
        .where(
            or_(
                and_(
                    table.process_status == "zip_file_hashed",
                    table.classify_result == "zip_file",
                    ),
                table.process_status == "unzip_hashed",
            )
        )
    )
    result = client.query_data(stmt)
    return result


def _calculate_hash(item_info):
    hash_service = CalculateHashService()
    if item_info["file_type"] == "file":
        return hash_service.calculate_file_hash(item_info["info"])
    else:
        return hash_service.calculate_folder_hash(item_info["info"])


class UnzipHashResult(BaseModel):
    id: int
    unzip_md5: str = Field(
        ..., max_length=32, min_length=32, description="MD5 hash of the file"
    )
    unzip_sha1: str = Field(
        ..., max_length=40, min_length=40, description="SHA1 hash of the file"
    )
    unzip_sha256: str = Field(
        ..., max_length=64, min_length=64, description="SHA256 hash of the file"
    )
    process_status: str = "unzip_hashed"


def _update_unzip_hash_info(
    client: Client, table: Type[UnzipHashProcessTable], source_item_info:dict, hash_result: dict
):
    checked_hash_result = UnzipHashResult(
        id=source_item_info["id"],
        unzip_md5=hash_result["md5"],
        unzip_sha1=hash_result["sha1"],
        unzip_sha256=hash_result["sha256"],
    )
    if not (source_item_info['md5'] == checked_hash_result.unzip_md5 and
            source_item_info['sha1'] == checked_hash_result.unzip_sha1 and
            source_item_info['sha256'] == checked_hash_result.unzip_sha256):
            return {"result": "failure", "error_message": {
                "错误类型": "解压文件与源文件hash不一致",
                "数据库模型": "UnzipHashProcessTable",
                "记录ID": source_item_info["id"],
                "错误时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "错误信息": f"数据库记录的哈希值与计算的哈希值不匹配，源hash记录为:md5:{source_item_info['md5']} sha1:{source_item_info['sha1']} sha256:{source_item_info['sha256']},解压 hash结果为:md5:{checked_hash_result.unzip_md5} sha1:{checked_hash_result.unzip_sha1} sha256:{checked_hash_result.unzip_sha256}",
            }}


    try:
        client.update_data(table, [checked_hash_result.model_dump()])
        return {"result": "success", "error_message": ""}
    except Exception as e:
        return {
            "result": "failure",
            "error_message": {
                "错误类型": "数据库更新失败",
                "数据库模型": "UnzipHashProcessTable",
                "记录ID": source_item_info["id"],
                "错误时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "错误信息": str(e),
            },
        }


def _send_error_notification(error_message):
    email_notifier = get_email_notifier()
    email_notifier.send_error_notification("Unzip Hash Process", error_message)


def upload_process():
    client = Client()

    unhashed_records = _need_upload_records(client, UnzipHashProcessTable)

    calculate_info = _create_calculate_info(unhashed_records)

    error_message = []

    for item_id, item_value in calculate_info.items():
        temp_item_info = deepcopy(item_value)
        md5 = temp_item_info.pop("md5")
        sha1 = temp_item_info.pop("sha1")
        sha256 = temp_item_info.pop("sha256")
        source_item_info ={
            "id": item_id,
            "md5": md5,
            "sha1": sha1,
            "sha256": sha256,
        }
        hash_result = _calculate_hash(temp_item_info)
        update_result = _update_unzip_hash_info(
            client, UnzipHashProcessTable, source_item_info, hash_result
        )
        if update_result["result"] == "failure":
            error_message.append(update_result["error_message"])
    if error_message:
        _send_error_notification(error_message)
    return update_result

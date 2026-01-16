from ..database import MySQLClient as Client
from ..database import ItemProcessRecord as UnzipHashProcessTable
from ..core import CalculateHashService, get_email_notifier
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
            "file_type": item.item_type,
            "info": item.unzip_path,
            "md5": item.md5,
            "sha1": item.sha1,
            "sha256": item.sha256,
            
        }
    return result


def _fetch_unzip_records(client: Client, table: Type[UnzipHashProcessTable]):
    from sqlalchemy import select
    stmt = (
        select(table)
        .where(table.host_name == get_host_name())
        .where(table.process_status == "unzipped")
        .where(table.classify_result != "zip_file")
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

def _pre_source_zip_file_process(client: Client, table: Type[UnzipHashProcessTable]):
    from ..utils.state_machine import get_state_machine
    unzip_hash_machine = get_state_machine()
    unzip_hash_machine.set_state(unzip_hash_machine.get_state_by_index(6))
    params = {
        "host_name": get_host_name(),
        "process_status": unzip_hash_machine.get_previous_state(),
        "classify_result": ["zip_file"],
    }
    stmt = client.create_query_stmt(table, params)
    records = client.query_data(stmt)
    changed_records = []
    changed_ids = []
    for record in records:
        changed_records.append({
            'id': record.id,
            'process_status': unzip_hash_machine.get_current_state(),
        })
        changed_ids.append(record.id)
    try:
        client.update_data(table, changed_records)
        return {"result": "success", "error_message": ""}
    except Exception as e:
        return {
            "result": "failure",
            "error_message": {
                "错误类型": "数据库更新失败",
                "数据库模型": "UnzipHashProcessTable",
                "记录ID": ','.join(map(str, changed_ids)),
                "错误时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "错误信息": str(e),
            },
        }

def _pre_unzip_hash_process(client: Client, table: Type[UnzipHashProcessTable]):
    return _pre_source_zip_file_process(client, table)

def unzip_hash_process():
    client = Client()

    unhashed_records = _fetch_unzip_records(client, UnzipHashProcessTable)

    calculate_info = _create_calculate_info(unhashed_records)

    pre_unzip_hash_process_result = _pre_unzip_hash_process(client, UnzipHashProcessTable)

    error_message = []

    if pre_unzip_hash_process_result["result"] == "failure":
        error_message.append(pre_unzip_hash_process_result["error_message"])

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

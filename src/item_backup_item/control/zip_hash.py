from ..database import MySQLClient as Client
from ..database import ItemProcessRecord as UnzipProcessTable
from ..service import CalculateHashService, get_email_notifier
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Type


def get_host_name():
    import os

    return os.getenv("COMPUTERNAME")


def _create_calculate_info(db_data):
    result = {}
    for item in db_data:
        result[item.id] = {
            "file_path": item.source_path
            if item.classify_result == "zip_file"
            else item.zipped_path,
            "classify_result": item.classify_result,
            "hash_info": {"md5": item.md5, "sha1": item.sha1, "sha256": item.sha256}
            if item.classify_result == "zip_file"
            else None,
        }
    return result


def _fetch_ziped_records(client: Client, table: Type[UnzipProcessTable]):
    from sqlalchemy import select, and_, or_

    stmt = (
        select(table)
        .where(table.host_name == get_host_name())
        .where(
            or_(
                UnzipProcessTable.process_status == "zipped",
                and_(
                    UnzipProcessTable.process_status == "hashed",
                    UnzipProcessTable.classify_result == "zip_file",
                ),
            )
        )
    )

    result = client.query_data(stmt)

    return result


def _calculate_hash(item_info):
    if item_info["classify_result"] == "zip_file":
        if item_info["hash_info"] is None:
            raise Exception("Hash info is None for zip file")
        return item_info["hash_info"]
    hash_service = CalculateHashService()
    hash_result = hash_service.calculate_file_hash(item_info["file_path"])
    return hash_result


class HashResult(BaseModel):
    id: int
    zipped_md5: str = Field(
        ..., max_length=32, min_length=32, description="Zipped MD5 hash of the file"
    )
    zipped_sha1: str = Field(
        ..., max_length=40, min_length=40, description="Zipped SHA1 hash of the file"
    )
    zipped_sha256: str = Field(
        ..., max_length=64, min_length=64, description="Zipped SHA256 hash of the file"
    )
    process_status: str = "zip_file_hashed"


def _update_zipped_hash_info(
    client: Client, table: Type[UnzipProcessTable], item_id: int, hash_result: dict
):
    checked_hash_result = HashResult(
        id=item_id,
        zipped_md5=hash_result["md5"],
        zipped_sha1=hash_result["sha1"],
        zipped_sha256=hash_result["sha256"],
    )
    try:
        client.update_data(table, [checked_hash_result.model_dump()])
        return {"result": "success", "error_message": ""}
    except Exception as e:
        return {
            "result": "failure",
            "error_message": {
                "错误类型": "数据库更新失败",
                "数据库模型": "ZipProcessTable",
                "记录ID": item_id,
                "错误时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "错误信息": str(e),
            },
        }


def _send_error_notification(error_message):
    email_notifier = get_email_notifier()
    email_notifier.send_error_notification("ZIP Hash Process", error_message)


def zip_hash_process():
    client = Client()

    zip_records = _fetch_ziped_records(client, UnzipProcessTable)

    calculate_info = _create_calculate_info(zip_records)

    error_message = []

    if not calculate_info:
        error_message.append({"警告": "无压缩文件信息被查询到", "警告时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})

    update_result = None
    for item_id, item_value in calculate_info.items():
        hash_result = _calculate_hash(item_value)
        update_result = _update_zipped_hash_info(
            client, UnzipProcessTable, item_id, hash_result
        )
        if update_result["result"] == "failure":
            error_message.append(update_result["error_message"])
    if error_message:
        _send_error_notification(error_message)
    return update_result 

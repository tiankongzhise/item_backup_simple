from ..database import MySQLClient as Client
from ..database import ItemProcessRecord as ZipProcessTable
from ..service import CalculateHashService, get_email_notifier
from pydantic import BaseModel, Field
from datetime import datetime


def get_host_name():
    import os

    return os.getenv("COMPUTERNAME")


def _create_calculate_info(db_data):
    result = {}
    for item in db_data:
        result[item.id] = {
            "file_type": item.item_type,
            "info": {
                "source_path": item.source_path,
                "classify_result": item.classify_result,
            },
        }
    return result


def _fetch_unhashed_records(client: Client, table: ZipProcessTable):
    query_params = {
        "host_name": get_host_name(),
        "classify_result": ["normal_file", "normal_folder", "zip_file"],
        "process_status": "classify",
    }
    stmt = client.create_query_stmt(ZipProcessTable, query_params)
    result = client.query_data(stmt)
    return result


def calculate_hash(item_info):
    hash_service = CalculateHashService()
    if item_info["file_type"] == "file":
        return hash_service.calculate_file_hash(item_info["info"])
    else:
        return hash_service.calculate_folder_hash(item_info["info"])


class HashResult(BaseModel):
    id: int
    md5: str = Field(
        ..., max_length=32, min_length=32, description="MD5 hash of the file"
    )
    sha1: str = Field(
        ..., max_length=40, min_length=40, description="SHA1 hash of the file"
    )
    sha256: str = Field(
        ..., max_length=64, min_length=64, description="SHA256 hash of the file"
    )
    process_status: str = "hashed"


def _update_hash_info(
    client: Client, table: ZipProcessTable, item_id: int, hash_result: dict
):
    checked_hash_result = HashResult(
        id=item_id,
        md5=hash_result["md5"],
        sha1=hash_result["sha1"],
        sha256=hash_result["sha256"],
    )
    try:
        client.update_data(ZipProcessTable, [checked_hash_result.model_dump()])
        return {"result": "success", "error_message": ""}
    except Exception as e:
        return {
            "result": "failure",
            "error_message": {
                "错误类型": "数据库更新失败",
                "数据库模型": "HashProcessTable",
                "记录ID": item_id,
                "错误时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "错误信息": str(e),
            },
        }


def _send_error_notification(error_message):
    email_notifier = get_email_notifier()
    email_notifier.send_error_notification("Hash Process", error_message)


def hash_process():
    client = Client()

    unhashed_records = _fetch_unhashed_records(client, ZipProcessTable)

    calculate_info = _create_calculate_info(unhashed_records)

    error_message = []

    for item_id, item_value in calculate_info.items():
        hash_result = calculate_hash(item_value)
        update_result = _update_hash_info(
            client, ZipProcessTable, item_id, hash_result
        )
        if update_result["result"] == "failure":
            error_message.append(update_result["error_message"])
    if error_message:
        _send_error_notification(error_message)
    return update_result

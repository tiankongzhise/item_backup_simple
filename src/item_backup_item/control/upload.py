from ..database import MySQLClient as Client
from ..database import ItemProcessRecord as UploadProcessTable
from ..service import UploadService, get_email_notifier
from pydantic import BaseModel, Field, model_validator
from datetime import datetime
from typing import Type,Literal
from copy import deepcopy

def get_host_name():
    import os

    return os.getenv("COMPUTERNAME")


def _create_upload_info(db_data):
    result = {}
    for item in db_data:
        result[item.id] = item.zipped_path
    return result


def _need_upload_records(client: Client, table: Type[UploadProcessTable]):
    params = {
        "host_name": get_host_name(),
        "process_status": "unzip_hashed",
    }
    stmt = client.create_query_stmt(table, params)
    result = client.query_data(stmt)
    return result


def _upload_file(file_path):
    upload_service = UploadService()
    upload_result = upload_service.upload_file(file_path)
    if upload_result["errno"] == 0:
        return {"result": "success", "error_message": ""}
    else:
        return {
            "result": "failure",
            "error_message": str(upload_result)
        }


class UploadResult(BaseModel):
    id: int
    fail_reason: str|None = None
    process_status: str = "upload"
    status_result :Literal["success","failure"]

    @model_validator(mode="after")
    def check_fail_reason(self):
        if self.status_result == "success" and self.fail_reason is not None:
            raise ValueError("fail_reason must be None when status_result is success")
        if self.status_result == "failure" and self.fail_reason is None:
            raise ValueError("fail_reason must be set when status_result is failure")
        return self



def _update_upload_info(
    client: Client, table: Type[UploadProcessTable], item_id: int, upload_result: dict):
    if upload_result["result"] == "success":
        checked_upload_result = UploadResult(
            id=item_id,
            status_result="success"
        )
    else:
        checked_upload_result = UploadResult(
            id=item_id,
            fail_reason=upload_result["error_message"],
            status_result="failure"
        )

    try:
        client.update_data(table, [checked_upload_result.model_dump()])
        return {"result": "success", "error_message": ""}
    except Exception as e:
        return {
            "result": "failure",
            "error_message": {
                "错误类型": "数据库更新失败",
                "数据库模型": "UploadProcessTable",
                "记录ID": item_id,
                "错误时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "错误信息": str(e),
            },
        }


def _send_error_notification(error_message):
    email_notifier = get_email_notifier()
    email_notifier.send_error_notification("Upload Process", error_message)


def upload_process():
    client = Client()

    need_upload_records = _need_upload_records(client, UploadProcessTable)

    upload_info = _create_upload_info(need_upload_records)

    error_message = []
    for item_id, path in upload_info.items():
        upload_result = _upload_file(path)
        update_result = _update_upload_info(
            client, UploadProcessTable, item_id, upload_result
        )
        if update_result["result"] == "failure":
            error_message.append(update_result["error_message"])
    if error_message:
        _send_error_notification(error_message)
    return update_result

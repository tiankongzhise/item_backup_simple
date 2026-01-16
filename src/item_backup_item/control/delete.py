from ..database import MySQLClient as Client
from ..database import ItemProcessRecord as DeleteProcessTable
from ..service import UploadService, get_email_notifier
from pydantic import BaseModel, Field, model_validator
from datetime import datetime
from typing import Type,Literal
from copy import deepcopy
from pathlib import Path

def get_host_name():
    import os

    return os.getenv("COMPUTERNAME")


def _create_delete_info(db_data):
    result = {}
    for item in db_data:
        result[item.id] = {
            "source_path": item.source_path,
            "classify_result": item.classify_result,
            "zipped_path": item.zipped_path,
            "unzipped_path": item.unzip_path,
        }
    return result


def _need_delete_records(client: Client, table: Type[DeleteProcessTable]):
    from sqlalchemy import select , or_ ,and_
    stmt = (
        select(table).
        where(table.host_name == get_host_name())
        .where(
        or_(
            table.process_status == "upload",
            and_(
                table.process_status == "classify",
                table.classify_result == "empty_folder"
            )
        ))
    )
    result = client.query_data(stmt)
    return result

def _del_unzipped_file(file_path):
    import shutil
    if file_path:
        unzipped_path = Path(file_path)
        if unzipped_path.exists():
            shutil.rmtree(unzipped_path)
def _zip_file(file_path):
    import shutil
    if file_path:
        zipped_path = Path(file_path)
        if zipped_path.exists():
            shutil.rmtree(zipped_path)

def _source_file(file_path):
    import shutil
    if file_path:
        source_path = Path(file_path)
        if source_path.exists():
            shutil.rmtree(source_path)

def _delete_file(file_path):
    try:
        _del_unzipped_file(file_path['unzipped_path'])
    except Exception as e:
        print(f"Error deleting unzipped file: {e}")
        return {"result": "failure", "error_message": str(e)}
    try:
        if file_path['classify_result'] != "zip_file":
            _zip_file(file_path['zipped_path'])
    except Exception as e:
        print(f"Error deleting source file: {e}")
        return {"result": "failure", "error_message": str(e)}
    try:
        _source_file(file_path['source_path'])
    except Exception as e:
        print(f"Error deleting source file: {e}")
        return {"result": "failure", "error_message": str(e)}
    return {"result": "success", "error_message": ""}

class DeleteResult(BaseModel):
    id: int
    fail_reason: str|None = None
    process_status: str = "delete"
    status_result :Literal["success","failure"]

    @model_validator(mode="after")
    def check_fail_reason(self):
        if self.status_result == "success" and self.fail_reason is not None:
            raise ValueError("fail_reason must be None when status_result is success")
        if self.status_result == "failure" and self.fail_reason is None:
            raise ValueError("fail_reason must be set when status_result is failure")
        return self



def _update_delete_info(
    client: Client, table: Type[DeleteProcessTable], item_id: int, delete_result: dict):
    if delete_result["result"] == "success":
        checked_upload_result = DeleteResult(
            id=item_id,
            status_result="success"
        )
    else:
        checked_upload_result = DeleteResult(
            id=item_id,
            fail_reason=delete_result["error_message"],
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
                "数据库模型": "DeleteProcessTable",
                "记录ID": item_id,
                "错误时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "错误信息": str(e),
            },
        }


def _send_error_notification(error_message):
    email_notifier = get_email_notifier()
    email_notifier.send_error_notification("Upload Process", error_message)


def delete_process():
    client = Client()

    need_delete_records = _need_delete_records(client, DeleteProcessTable)

    delete_info = _create_delete_info(need_delete_records)

    error_message = []
    for item_id, path in delete_info.items():
        upload_result = _delete_file(path)
        update_result = _update_delete_info(
            client, DeleteProcessTable, item_id, upload_result
        )
        if update_result["result"] == "failure":
            error_message.append(update_result["error_message"])
    if error_message:
        _send_error_notification(error_message)
    return update_result

from ..database import MySQLClient as Client
from ..database import ItemProcessRecord as UnzipProcessTable
from ..service import ZipService, get_email_notifier
from ..config import ZipConfig
from pydantic import BaseModel,field_validator
from datetime import datetime
from pathlib import Path
from typing import Literal

def get_host_name():
    import os

    return os.getenv("COMPUTERNAME")


def _create_zip_item_info(db_data):
    result = {}
    for item in db_data:
        result[item.id] = {
            'source_path': item.source_path,
            'target_path': ZipConfig.zipped_folder,
            'zip_level': ZipConfig.zip_level
        }
        if hasattr(ZipConfig, 'password'):
            result[item.id]['password'] = ZipConfig.password
    return result


def _fetch_need_zip_records(client: Client, table: UnzipProcessTable):
    query_params = {
        "host_name": get_host_name(),
        "classify_result": ["normal_file", "normal_folder"],
        "process_status": "hashed",
    }
    stmt = client.create_query_stmt(UnzipProcessTable, query_params)
    result = client.query_data(stmt)
    return result


def _zip_item(item_id,item_info):
    try:
        hash_service = ZipService()
        zip_result = hash_service.zip_item(
            item_info['source_path'], 
            item_info['target_path'], 
            item_info.get('password'),
            item_info['zip_level']
            )
        return {'status': 'success', 'zipped_path': zip_result}
    except Exception as e:
        return {'status': 'failure', "error_message": {
                "错误类型": "压缩文件失败",
                "数据库模型": "ZipProcessTable",
                "记录ID": item_id,
                "错误时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "错误信息": str(e),
            }}

class _ZipResultCheck(BaseModel):
    id: int
    zipped_path:str
    zipped_size:int
    process_status:Literal['zipped'] = 'zipped'

    @field_validator('zipped_path')
    def check_zipped_path(cls, value):

        temp_path = Path(value)
        if not temp_path.exists():
            raise ValueError(f"Zipped path does not exist: {value}")
        if temp_path.suffix != ".zip":
            raise ValueError(f"Zipped path is not a zip file: {value}")
        return value




def _update_zip_info(
    client: Client, table: UnzipProcessTable, item_id: int, zip_result: Path
):
    checked_zip_result = _ZipResultCheck(
        id=item_id,
        zipped_path=zip_result.absolute().as_posix(),
        zipped_size=zip_result.stat().st_size,
    )
    try:
        client.update_data(UnzipProcessTable, [checked_zip_result.model_dump()])
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
    email_notifier.send_error_notification("Hash Process", error_message)


def zip_process():
    client = Client()

    need_zip_records = _fetch_need_zip_records(client, UnzipProcessTable)

    need_zip_info = _create_zip_item_info(need_zip_records)

    error_message = []

    for item_id, item_value in need_zip_info.items():
        zip_result = _zip_item(item_id, item_value)

        if zip_result['status'] == 'failure':
            error_message.append(zip_result['error_message'])
            continue


        update_result = _update_zip_info(
            client, UnzipProcessTable, item_id, zip_result['zipped_path']
        )
        if update_result["result"] == "failure":
            error_message.append(update_result["error_message"])
    if error_message:
        _send_error_notification(error_message)
    return update_result

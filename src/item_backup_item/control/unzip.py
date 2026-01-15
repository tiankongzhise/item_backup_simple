from ..database import MySQLClient as Client
from ..database import ItemProcessRecord as UnzipProcessTable
from ..service import ZipService, get_email_notifier
from ..config import ZipConfig
from pydantic import BaseModel,field_validator,Field
from datetime import datetime
from pathlib import Path
from typing import Type
from typing import Literal

def get_host_name():
    import os

    return os.getenv("COMPUTERNAME")


def _create_unzip_item_info(db_data):
    result = {}
    for item in db_data:
        result[item.id] = {
            'zipped_path': item.zipped_path,
            'target_path': ZipConfig.unzip_folder,
            'source_item_size': item.item_size
        }
        if hasattr(ZipConfig, 'password'):
            result[item.id]['password'] = ZipConfig.password
    return result


def _fetch_need_unzip_records(client: Client, table: Type[UnzipProcessTable]):
    from sqlalchemy import select
    stmt = (
        select(table)
        .where(table.host_name == get_host_name())
        .where(table.process_status == "zip_file_hashed")
        .where(table.classify_result != "zip_file")
    )
    result = client.query_data(stmt)
    return result


def _unzip_item(item_id,item_info):
    try:
        hash_service = ZipService()
        zip_result = hash_service.unzip_item(
            item_info['zipped_path'], 
            item_info['target_path'], 
            item_info.get('password'),
            )
        return {'status': 'success', 'zipped_path': zip_result}
    except Exception as e:
        return {'status': 'failure', "error_message": {
                "错误类型": "解压文件失败",
                "数据库模型": "UnzipProcessTable",
                "记录ID": item_id,
                "错误时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "错误信息": str(e),
            }}

class _UnzipResultCheck(BaseModel):
    id: int
    unzip_path:str
    unzip_size:int = Field(..., gt=0) # unzip size greater than 0
    process_status:Literal['unzipped'] = 'unzipped'

    @field_validator('unzip_path')
    def check_unzip_path(cls, value):

        temp_path = Path(value)
        if not temp_path.exists():
            raise ValueError(f"Unzip path does not exist: {value}")
        return value


def _calculate_unzip_item_size(unzip_result: Path)->int:
    if not unzip_result.exists():
        raise ValueError(f"Unzip path does not exist: {unzip_result}")
    if unzip_result.is_file():
        return unzip_result.stat().st_size
    elif unzip_result.is_dir():
        return sum([i.stat().st_size for i in unzip_result.rglob("*") if i.is_file()])
    else:
        raise ValueError(f"Unzip path is not a file or directory: {unzip_result}")
def _update_unzip_info(
    client: Client, table: Type[UnzipProcessTable], item_id: int, source_item_size: int, unzip_result: Path
):
    checked_zip_result = _UnzipResultCheck(
        id=item_id,
        unzip_path=unzip_result.absolute().as_posix(),
        unzip_size=_calculate_unzip_item_size(unzip_result),
    )

    if checked_zip_result.unzip_size != source_item_size:
        return {
            "result": "failure",
            "error_message": {
                "错误类型": "解压文件大小错误",
                "数据库模型": "UnzipProcessTable",
                "记录ID": item_id,
                "错误时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "错误信息": f"解压文件大小错误，预期大小: {source_item_size}, 实际大小: {checked_zip_result.unzip_size}",
            },
        }

    try:
        client.update_data(table, [checked_zip_result.model_dump()])
        return {"result": "success", "error_message": ""}
    except Exception as e:
        return {
            "result": "failure",
            "error_message": {
                "错误类型": "数据库更新失败",
                "数据库模型": "UnzipProcessTable",
                "记录ID": item_id,
                "错误时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "错误信息": str(e),
            },
        }


def _send_error_notification(error_message):
    email_notifier = get_email_notifier()
    email_notifier.send_error_notification("Hash Process", error_message)


def unzip_process():
    client = Client()

    need_zip_records = _fetch_need_unzip_records(client, UnzipProcessTable)

    need_zip_info = _create_unzip_item_info(need_zip_records)

    error_message = []

    for item_id, item_value in need_zip_info.items():
        source_item_size = item_value.pop('source_item_size')
        zip_result = _unzip_item(item_id, item_value)

        if zip_result['status'] == 'failure':
            error_message.append(zip_result['error_message'])
            continue


        update_result = _update_unzip_info(
            client, UnzipProcessTable, item_id,source_item_size, zip_result['zipped_path']
        )
        if update_result["result"] == "failure":
            error_message.append(update_result["error_message"])
    if error_message:
        _send_error_notification(error_message)
    return update_result

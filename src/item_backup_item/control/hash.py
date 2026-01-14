from ..database import MySQLClient as Client
from ..database import ItemProcessRecord as HashProcessTable
from ..service import CalculateHashService,get_email_notifier
from pydantic import BaseModel, Field
from datetime import datetime

def get_host_name():
    import os

    return os.getenv("COMPUTERNAME")


def _create_hash_info(db_data):
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


def _fetch_unhashed_records(client: Client, table: HashProcessTable):
    query_params = {
        "host_name": get_host_name(),
        "classify_result": ["normal_file", "normal_folder",'zip_file'],
        "process_status": "classify",
    }
    stmt = client.create_query_stmt(HashProcessTable, query_params)
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
    md5: str = Field(..., max_length=32, min_length=32, description="MD5 hash of the file")
    sha1: str = Field(..., max_length=40, min_length=40, description="SHA1 hash of the file")
    sha256: str = Field(..., max_length=64, min_length=64, description="SHA256 hash of the file")
    process_status: str = 'hashed'

def hash_process():
    client = Client()
    unhashed_records = _fetch_unhashed_records(client, HashProcessTable)
    hash_info = _create_hash_info(unhashed_records)
    
    update_result = {
        'success':[],
        'failure':{}
    }
    for item_id, item_value in hash_info.items():
        hash_result = calculate_hash(item_value)
        checked_hash_result = HashResult(
            id=item_id,
            md5=hash_result["md5"],
            sha1=hash_result["sha1"],
            sha256=hash_result["sha256"],
        )
        try:
            client.update_data(HashProcessTable, [checked_hash_result.model_dump()])
            update_result['success'].append(item_id)
        except Exception as e:
            print(f"Error updating record {item_id}: {e}")
            update_result['failure'][item_id] = {'错误时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '错误信息': str(e)}
    if update_result['failure']:
        err_msg = []
        for item_id, error_info in update_result['failure'].items():
            err_msg.append({
                '错误类型': '更新失败',
                '数据库模型': 'HashProcessTable',
                '记录ID': item_id,
                '错误时间': error_info['错误时间'],
                '错误信息': error_info['错误信息']
            })
        email_notifier = get_email_notifier()
        email_notifier.send_error_notification("Hash Process", err_msg)
    return update_result

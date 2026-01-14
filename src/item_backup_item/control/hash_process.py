from ..database import MySQLClient as Client
from ..database import ItemProcessRecord as HashProcessTable
from ..service import CalculateHashService


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


def hash_process():
    client = Client()
    unhashed_records = _fetch_unhashed_records(client, HashProcessTable)
    hash_info = _create_hash_info(unhashed_records)
    hash_service = CalculateHashService()
    hash_result = {}
    for item_id, item_value in hash_info.items():
        print(item_id, item_value)
        if item_value["file_type"] == "file":
            hash_result[item_id] = hash_service.calculate_file_hash(item_value["info"])
        else:
            hash_result[item_id] = hash_service.calculate_folder_hash(item_value["info"])
    return hash_result

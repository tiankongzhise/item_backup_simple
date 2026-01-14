
from ..database import MySQLClient as Client
from ..database import ItemProcessRecord as ItemProcessRecordTable
from pydantic import BaseModel
from typing import Literal
from pathlib import Path

def get_host_name():
    import os
    hostname = os.getenv('COMPUTERNAME')
    return hostname


class StoreClassifyResult(BaseModel):
    item_name: str
    host_name: str
    source_path: str
    item_type: Literal['file','folder']
    item_size:int
    classify_result:Literal['normal_file','normal_folder','zip_file','overcount_folder','empty_folder','oversize_folder','oversize_file']
    process_status:Literal['classify']
    status_result:Literal['success']

class StorageService:
    def __init__(self, client = None):
        self.client = client or Client()
        self.register_table()
        self.register_check_schema()

    def _default_tag_table_map(self):
        return {
            "item_process_record":ItemProcessRecordTable,
            'classify':ItemProcessRecordTable

        }

    def register_table(self,tag:str = None,model = None):
        if not hasattr(self,'_table_manager'):
            self._table_manager = self._default_tag_table_map()
        if tag is None and model is None:
            return self
        if any([tag,model]):
            raise ValueError("tag and model must be either both None or both have specific values, not one has a value and the other is None")
        self._table_manager[tag] = model
        return self

    def get_table(self, tag):
        return self._table_manager.get(tag)
    
    def _default_check_schema(self):
        return {
            'classify':StoreClassifyResult
        }

    def register_check_schema(self, tag:str = None, schema = None):
        if not hasattr(self,'_check_schema'):
            self._check_schema = self._default_check_schema()
        if tag is None and schema is None:
            return self
        if any([tag,schema]):
            raise ValueError("tag and model must be either both None or both have specific values, not one has a value and the other is None")
        self._check_schema[tag] = schema
        return self

    def get_check_schema(self, tag):
        return self._check_schema.get(tag)
    


    def store_classify_result(self, classify_data:list[dict[Path,dict]]):
        result = []
        for item in classify_data:
            for key, value in item.items():
                temp_dict = {
                    "item_name": key.name,
                    "host_name": get_host_name(),
                    "source_path": key.resolve().as_posix(),
                    "item_type": value['item_type'],
                    "item_size": value['item_size'],
                    "classify_result": value['classify_result'],
                    "process_status": "classify",
                    "status_result": "success"
                }
                check_schema = self.get_check_schema('classify')
                if check_schema:
                    check_params = check_schema(**temp_dict)
                else:
                    raise ValueError("No check schema registered for tag 'classify'")
                table_model = self.get_table('classify')
                if table_model:
                    result.append(table_model(**check_params.model_dump()))
                else:
                    raise ValueError("No table registered for tag 'classify'")
        return self.client.add_all(result)

from ..config import ClassifyConfig
from ..service import classify_folder
from ..service import StorageService
from typing import Optional, Type

def classify_process(config: Optional[Type[ClassifyConfig]] = None):
    if config is None:
        config = ClassifyConfig
    target_folder_list = config.sources_list
    print(f'target_folder_list: {target_folder_list}')
    result = []
    for item in target_folder_list:
        result.extend(classify_folder(item))
    print(f'result: {result}')
    storage_service = StorageService()
    add_rows = storage_service.store_classify_result(result)
    return add_rows


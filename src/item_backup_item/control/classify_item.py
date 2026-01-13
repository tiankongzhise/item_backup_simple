from ..config import ClassifyConfig
from ..service import classify_folder
from typing import Optional

def classify_process(config: Optional[ClassifyConfig] = None):
    if config is None:
        config = ClassifyConfig
    target_folder_list = config.sources_list
    print(target_folder_list)
    result = []
    for item in target_folder_list:
        result.extend(classify_folder(item))
    return result


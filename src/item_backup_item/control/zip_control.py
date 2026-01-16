from ..service.zip_service import ZipService
from ..utils import state_machine
from pydantic import BaseModel

class ZippedItem(BaseModel):
    ...

class _CheckXXParams(BaseModel):
    ...

class ZipControl:
    def __init__(self):
        self.db_client = None
        self.state = None
        self.table =None
        self.service = None
    
    def fetch_pre_state_records(self):
        ...
    
  
    def preprocess_special_records(self):
        ...
    
    def transform_to_zip_format(self):
        ...

    def zip_record_item(self):
        ...

    def persist_zip_result(self):
        ...

    def run(self):
        ...

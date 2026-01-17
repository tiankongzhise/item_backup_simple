from item_backup_item.service.fetch_service import FetchRecordsService




class BackupItem:
    def get_local_source_items(self):
        ...

    def fetch_remote_source_items(self):
        ...

    def get_new_items_to_add(self):
        ...

    def update_remote_source_items(self):
        ...
    

    def backup_items(self):
        self._fetch_object_to_backup()
        self._calculate_object_hash()
        self._persist_object_hash_result()


    def _fetch_object_to_backup(self):
        ...
    def _calculate_object_hash(self):
        ...
    def _persist_object_hash_result(self):
        ...
    def _fetch_object_to_zip(self):
        ...
    def _zip_object(self):
        ...
    def _persist_object_zip_result(self):
        ...
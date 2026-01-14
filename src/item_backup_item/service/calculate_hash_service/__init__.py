from .folder_hash import calculate_folder_hash
from .file_hash import calculate_file_hash

class CalculateHashService:
    @staticmethod
    def calculate_folder_hash(*args, **kwargs) -> dict:
        '''Calculate the hash of a folder.
        params:
            folder_info: str|Path|dict
            algorithm: list
        return: dict
        '''
        return calculate_folder_hash(*args, **kwargs)

    @staticmethod
    def calculate_file_hash(*args, **kwargs) -> dict:
        '''Calculate the hash of a file.
        params:
            file_info: str|Path|dict
            algorithm: list
        return: dict
        '''
        return calculate_file_hash(*args, **kwargs)
__all__ = [
    'CalculateHashService'
]

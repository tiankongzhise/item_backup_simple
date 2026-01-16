from .control import classify_process
from .control import hash_process
from .control import zip_process
from .control import zip_hash_process
from .control import unzip_process
from .control import unzip_hash_process
from .control import upload_process
def main():
    # result = classify_process()
    # result = hash_process()
    # result = zip_process()
    # result = zip_hash_process()
    # result = unzip_process()
    # result = unzip_hash_process()
    result = upload_process()   
    print(result)



if __name__ == '__main__':
    main()

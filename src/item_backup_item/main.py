from .control import classify_process
from .control import hash_process
from .control import zip_process

def main():
    classify_process()
    hash_process()
    result = zip_process()
    print(result)



if __name__ == '__main__':
    main()

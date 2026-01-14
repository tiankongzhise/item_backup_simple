from .control import classify_process
from .control import hash_process


def main():
    classify_process()
    result = hash_process()
    print(result)



if __name__ == '__main__':
    main()

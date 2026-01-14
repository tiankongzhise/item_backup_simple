from collections.abc import Iterator, Iterable

def check_iterable_type(obj):
    """检查对象的可迭代类型"""
    is_iterable = isinstance(obj, Iterable)  # 可迭代对象
    is_iterator = isinstance(obj, Iterator)  # 迭代器
    
    return {
        "object": obj,
        "is_iterable": is_iterable,
        "is_iterator": is_iterator,
        "type": type(obj).__name__
    }

# 测试各种对象
test_objects = [
    [1, 2, 3],           # 列表
    (1, 2, 3),           # 元组
    {1, 2, 3},           # 集合
    {"a": 1, "b": 2},    # 字典
    "hello",             # 字符串
    iter([1, 2, 3]),     # 列表迭代器
    (x for x in range(3)),  # 生成器表达式
    open(__file__),      # 文件对象
    42,                  # 整数
]

for obj in test_objects:
    result = check_iterable_type(obj)
    print(f"{result['type']:15} iterable={result['is_iterable']} iterator={result['is_iterator']}")
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import MetaData

class MysqlBase(DeclarativeBase):
    metadata = MetaData()
    __abstract__ = True

    @property
    def create_at_local_time(self) -> str | None:
        """创建时间戳转本地时间"""
        return timestamp_to_local_time(self.create_at, ms=True)
    
    @property
    def update_at_local_time(self) -> str | None:
        """更新时间戳转本地时间"""
        return timestamp_to_local_time(self.update_at, ms=True)

    def __repr__(self) -> str:
        # 运行时，动态解析全部字段，以str形式返回
        return f"<{self.__class__.__name__}{','.join([f'{k}={v}' for k,v in self.__dict__.items() if not k.startswith('_')])},create_at_local_time={self.create_at_local_time},update_at_local_time={self.update_at_local_time}>"\r

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import MetaData, Integer, String, Text, JSON, Boolean, UniqueConstraint,BigInteger
from datetime import timezone,datetime



def get_current_timestamp(ms: bool = True) -> int:
    """获取当前时间戳，默认毫秒级，ms=False则返回秒级"""
    if ms:
        return int(datetime.now().timestamp() * 1000)
    return int(datetime.now().timestamp())

def timestamp_to_local_time(ts: int, fmt: str = "%Y-%m-%d %H:%M:%S", ms: bool = True) -> str | None:
    """
    时间戳转本地时区格式化时间
    :param ts: 时间戳(秒/毫秒)
    :param fmt: 格式化字符串，默认：年-月-日 时:分:秒
    :param ms: 是否是毫秒级时间戳
    :return: 本地时间字符串 / None
    """
    if not ts:
        return None
    _ts = ts / 1000 if ms else ts
    # 强制转【本地时区】，杜绝时区偏差
    local_dt = datetime.fromtimestamp(_ts).astimezone(timezone.utc).astimezone()
    # 如果是毫秒级时间戳,应当显示毫秒部分
    if ms:
        fmt += ".%f"
    return local_dt.strftime(fmt)[:-3] if ms else local_dt.strftime(fmt)


class MysqlBase(DeclarativeBase):
    metadata = MetaData()
    __abstract__ = True

    create_at: Mapped[int] = mapped_column(BigInteger, default=get_current_timestamp)
    update_at: Mapped[int] = mapped_column(BigInteger, onupdate=get_current_timestamp,default=None,nullable=True)

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
        return f"<{self.__class__.__name__}{','.join([f'{k}={v}' for k,v in self.__dict__.items() if not k.startswith('_')])},create_at_local_time={self.create_at_local_time},update_at_local_time={self.update_at_local_time}>"

class ItemProcessRecord(MysqlBase):
    __tablename__ = "item_process_record"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    item_name: Mapped[str] = mapped_column(String(255))
    source_path: Mapped[str] = mapped_column(String(300))
    item_type: Mapped[str] = mapped_column(String(10))
    item_size: Mapped[int] = mapped_column(Integer)
    classify_result: Mapped[str] = mapped_column(String(10))
    process_status: Mapped[str] = mapped_column(String(32))
    status_result: Mapped[str] = mapped_column(String(10))
    md5: Mapped[str] = mapped_column(String(32), nullable=True)
    sha1: Mapped[str] = mapped_column(String(40), nullable=True)
    sha256: Mapped[str] = mapped_column(String(64), nullable=True)
    other_hash_info: Mapped[dict] = mapped_column(JSON, nullable=True)
    zipped_path: Mapped[str] = mapped_column(Text, nullable=True)
    zipped_size: Mapped[int] = mapped_column(Integer)
    zipped_md5: Mapped[str] = mapped_column(String(32))
    zipped_sha1: Mapped[str] = mapped_column(String(40))
    zipped_sha256: Mapped[str] = mapped_column(String(64))
    other_zipped_hash_info: Mapped[dict] = mapped_column(JSON, nullable=True)
    unzip_path: Mapped[str] = mapped_column(Text, nullable=True)
    unzip_size: Mapped[int] = mapped_column(Integer, nullable=True)
    unzip_md5: Mapped[str] = mapped_column(String(32), nullable=True)
    unzip_sha1: Mapped[str] = mapped_column(String(40), nullable=True)
    unzip_sha256: Mapped[str] = mapped_column(String(64), nullable=True)
    other_unzip_hash_info: Mapped[dict] = mapped_column(JSON, nullable=True)
    is_compiled: Mapped[bool] = mapped_column(Boolean, default=False)
    fail_reason: Mapped[dict] = mapped_column(JSON, nullable=True)

    __table_args__ = (
        UniqueConstraint('source_path', name='uix_source_path'),
    )



if __name__ == '__main__':
    from client import MySQLClient
    client = MySQLClient()
    engine = client.get_engine()
    # ItemProcessRecord.metadata.drop_all(engine)
    ItemProcessRecord.metadata.create_all(engine)
    print("Table created")
    from sqlalchemy.orm import Session
    def add_item():
        with Session(engine) as session:
            item = ItemProcessRecord(
                item_name="test",
                source_path="test",
                item_type="test",
                item_size=123,
                classify_result="test",
                process_status="test",
                status_result="test",
                md5="test",
                sha1="test",
                sha256="test",
                other_hash_info={"test": "test"},
                zipped_path="test",
                zipped_size=123,
                zipped_md5="test",
                zipped_sha1="test",
                zipped_sha256="test",
                other_zipped_hash_info={"test": "test"},
                unzip_path="test",
                unzip_size=123,
                unzip_md5="test",
                unzip_sha1="test",
                unzip_sha256="test",
                other_unzip_hash_info={"test": "test"},
                is_compiled=True,
                fail_reason={"test": "test"}
            )
            session.add(item)
            session.commit()
            print("Item added")
            print(item)

    def query_item():
        from sqlalchemy import select
        with Session(engine) as session:
            item = session.execute(select(ItemProcessRecord)).scalars().all()
            print(item)
    # add_item()
    query_item()
    
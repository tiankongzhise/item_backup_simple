from dataclasses import dataclass

@dataclass
class MysqlConfig:
    pool_pre_ping: bool = True

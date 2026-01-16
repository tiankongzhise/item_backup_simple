from dataclasses import dataclass

@dataclass
class MysqlConfig:
    engine_config={
        "pool_pre_ping": True,
        "pool_size": 10,
        "max_overflow": 20,
        "pool_recycle": 3600,
        "pool_timeout": 30,
    }

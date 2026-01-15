from sqlalchemy import create_engine
from pathlib import Path
from sqlalchemy.orm import Session
from .models import MysqlBase
from sqlalchemy import select,update,Table
from ...config import MysqlConfig
from typing import Type,cast,Sequence


class MySQLClient:
    def __init__(self, database:str|None = None,env_file:str|Path|None = None,db_config:dict|None=None):
        self.database = database
        self.env_file = env_file or 'mysql.env'
        self.db_config = db_config or MysqlConfig.engine_config
        self.engine = None

    def get_engine(self):
        import os
        from dotenv import load_dotenv
        load_dotenv(self.env_file)
        host = os.getenv('MYSQL_HOST')
        port = os.getenv('MYSQL_PORT')
        user = os.getenv('MYSQL_USER')
        password = os.getenv('MYSQL_PASSWORD')
        database = self.database or os.getenv('MYSQL_DATABASE')
        self.engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', **self.db_config)
        try:
            self.engine.connect()
        except Exception as e:
            raise e
        return self.engine

    def init_schema(self):
        engine = self.get_engine()
        MysqlBase.metadata.create_all(engine)

    def reset_schema(self):
        engine = self.get_engine()
        MysqlBase.metadata.drop_all(engine)
        MysqlBase.metadata.create_all(engine)

    def create_table(self,model:Type[MysqlBase]|MysqlBase):
        engine = self.get_engine()

        MysqlBase.metadata.create_all(engine,tables=cast(Sequence[Table],[model.__table__]))

    def reset_table(self,model:Type[MysqlBase]):
        engine = self.get_engine()
        MysqlBase.metadata.drop_all(engine,tables=cast(Sequence[Table],[model.__table__]))
        MysqlBase.metadata.create_all(engine,tables=cast(Sequence[Table],[model.__table__]))
    
    def drop_table(self,model:Type[MysqlBase]):
        engine = self.get_engine()
        MysqlBase.metadata.drop_all(engine,tables=cast(Sequence[Table],[model.__table__]))

    def drop_schema(self):
        engine = self.get_engine()
        MysqlBase.metadata.drop_all(engine)
    def add_all(self, data:list[MysqlBase]):
        '''Add all data to mysql
        args:
            data (list[MysqlBase]): data to add
        return (int)
            number of rows added
        '''
        if not data:
            return 0
        engine = self.get_engine()
        self.create_table(data[0])
        with Session(engine) as session:
            session.add_all(data)
            session.commit()
        return len(data)

    def get_all_data(self, model:Type[MysqlBase]):
        engine = self.get_engine()
        with Session(engine) as session:
            stmt = select(model)
            return session.scalars(stmt).all()

    def update_data(self, model:Type[MysqlBase], data:list[dict]):
        engine = self.get_engine()
        changed_rows = 0
        with Session(engine) as session:
            results = session.execute(update(model), data)
            session.commit()
            for result in results:
                changed_rows += result.rowcount
        return changed_rows

    def create_query_stmt(self, model:Type[MysqlBase], query_params:dict):
        stmt = select(model)
        for key, value in query_params.items():
            if isinstance(value, (list, tuple)):
                stmt = stmt.where(model.__table__.c[key].in_(value))
            else:
                stmt = stmt.where(model.__table__.c[key] == value)
        return stmt

    def query_data(self, stmt):
        engine = self.get_engine()
        with Session(engine) as session:
            return session.scalars(stmt).all()
        
if __name__ == '__main__':
    client = MySQLClient(env_file=r'./mysql.env')
    engine = client.get_engine()
    print(engine)

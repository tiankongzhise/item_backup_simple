from sqlalchemy import create_engine
from pathlib import Path

class MySQLClient:
    def __init__(self, database:str|None = None,env_file:str|Path|None = None):
        self.database = database
        self.env_file = env_file or 'mysql.env'
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
        self.engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
        try:
            self.engine.connect()
        except Exception as e:
            raise e
        return self.engine




if __name__ == '__main__':
    client = MySQLClient(env_file=r'J:\uv_code_lib\item_backup_simple\mysql.env')
    engine = client.get_engine()
    print(engine)

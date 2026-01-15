from item_backup_item.main import main
from item_backup_item.database import MySQLClient
from item_backup_item.control import zip_hash
from dowhen import when

if __name__ == '__main__':
    db_client = MySQLClient()
    db_client.drop_schema()
    with when(zip_hash,"return hash_result").do("print(f'\\nhash_result:{hash_result}\\n')"):
        main()
    print("All processes completed.")

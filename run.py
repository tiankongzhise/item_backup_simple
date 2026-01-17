from item_backup_item.main import main_single_file
from item_backup_item.database import MySQLClient


if __name__ == '__main__':
    db_client = MySQLClient()
    db_client.drop_schema()

    main_single_file()
    print("All processes completed.")

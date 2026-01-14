from item_backup_item.main import main
from item_backup_item.database import MySQLClient
from item_backup_item.database.mysql.models import ItemProcessRecord

if __name__ == '__main__':
    db_client = MySQLClient()
    db_client.drop_schema()
    main()
    print("All processes completed.")

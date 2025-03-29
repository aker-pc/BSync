import sqlite3
import BFile
import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def dump_folder_info(bill_platform, bill_date):
    dump_folder_path = os.getcwd()
    if bill_date:
        dump_folder_path = f"{dump_folder_path}/bsync_save/archives/{bill_platform}/{bill_date}"
    else:
        dump_folder_path = f"{dump_folder_path}/bsync_save/archives/{bill_platform}"

    def traverse_directory(root_dir):
        path = Path(root_dir)
        path_list = []
        for item in path.rglob("*"):
            if item.is_file():
                path_list.append(str(item))
        return path_list
    return traverse_directory(dump_folder_path)


def dump_sqlite(bill_platform, dump_path):
    sqlite_create = """
    CREATE TABLE IF NOT EXISTS trade_transactions (
        trade_id TEXT,
        trade_date TEXT,
        trade_info TEXT,
        price REAL,
        trade_platform TEXT,
        trade_type TEXT,
        trade_method TEXT,
        PRIMARY KEY (trade_id, trade_platform)
    );
    """

    sqlite_insert = """
    INSERT INTO trade_transactions VALUES ('{}', '{}', '{}', {}, '{}', '{}', '{}');
    """

    dump_save_path = os.getcwd()

    is_alipay = bill_platform == "alipay"
    csv_data = BFile.BDataLoader.bill_data(dump_path, is_alipay)

    con = sqlite3.connect(f"{dump_save_path}/bsync_save/bills.db")
    cur = con.cursor()
    cur.execute(sqlite_create)
    con.commit()

    for bill in csv_data:
        bill_trade_id = bill[8+int(is_alipay)].replace('\t', '').replace(' ', '')
        bill_date = bill[0]
        bill_price = float(bill[5+int(is_alipay)].replace('¥', ''))
        bill_type = bill[4+int(is_alipay)]
        if not is_alipay:
            if bill_type == '支出':
                bill_method = bill[6]
                bill_info = bill[1] if bill[3] == '"/"' else bill[3]
            else:
                bill_method = bill[7]
                bill_info = bill[1]
        else:
            bill_info = bill[4]
            bill_method = bill[7] if bill_type == '支出' else bill[1]

        sqlite_insert_real = sqlite_insert.format(bill_trade_id, bill_date, bill_info, bill_price, bill_platform, bill_type, bill_method)
        logger.info(sqlite_insert_real)
        try:
            cur.execute(sqlite_insert_real)
            con.commit()
        except sqlite3.IntegrityError:
            con.rollback()
            logger.info(sqlite_insert_real)
        finally:
            continue
    logger.info(csv_data)


# 支持dump所有历史文件至sqlite
if __name__ == '__main__':
    os.chdir("..")
    bill_platforms = ["alipay", "wechat"]
    for bill_platform in bill_platforms:
        dump_paths = dump_folder_info(bill_platform, None)
        for dump_path in dump_paths:
            dump_sqlite(bill_platform, dump_path)
import os
import pymysql


def get_db_conn():
    from api.app import _env_int
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1").strip(),
        user=os.getenv("MYSQL_USER", "root").strip(),
        password=os.getenv("MYSQL_PASSWORD", "").strip(),
        port=_env_int("MYSQL_PORT", 3306),
        database=os.getenv("MYSQL_DB", "bike_db").strip(),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

import os
from api.db import get_db_conn


def verify():
    print("Connecting to database to verify pipeline results...\n")
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Check stats table
            print("=== TABLE: stats ===")
            cur.execute("SELECT COUNT(*) as n FROM stats")
            print(f"Total rows: {cur.fetchone()['n']}")
            cur.execute("SELECT * FROM stats LIMIT 5")
            for row in cur.fetchall():
                print(row)

            # Check bike_forecast table
            print("\n=== TABLE: bike_forecast ===")
            cur.execute("SELECT COUNT(*) as n FROM bike_forecast")
            print(f"Total rows: {cur.fetchone()['n']}")
            cur.execute("SELECT * FROM bike_forecast LIMIT 5")
            for row in cur.fetchall():
                print(row)
    finally:
        conn.close()


if __name__ == '__main__':
    verify()

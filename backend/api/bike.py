# backend/api/bike.py
import os
import time
import datetime as dt
from typing import Dict, List, Any, Optional

import requests
import pymysql


class BikeRepository:
    """
    Dublin Bikes ingest + storage (MySQL/MariaDB):
      - bike_current: latest snapshot (1 row per station) for frontend
      - bike_history: time-series, partitioned by day, retention by dropping partitions

    Key points:
      - bike_history uses generated STORED column scraped_date = DATE(scraped_at)
      - partitions are daily RANGE COLUMNS(scraped_date) with a final MAXVALUE partition (pmax)
      - we "split" pmax each day using REORGANIZE PARTITION pmax INTO (pYYYYMMDD, pmax)
      - retention removes old partitions (fast) instead of DELETE millions of rows
    """

    def __init__(
        self,
        mysql_host: str,
        mysql_user: str,
        mysql_password: str,
        mysql_port: int = 3306,
        db_name: str = "bike_db",
        jcdecaux_api_key: Optional[str] = None,
        jcdecaux_contract: str = "dublin",
        retention_days: int = 7,
        create_partitions_ahead_days: int = 1,
        request_timeout: int = 15,
    ):
        self.mysql_host = mysql_host
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_port = int(mysql_port)
        self.db_name = db_name

        self.api_key = jcdecaux_api_key or os.getenv("JCDECAUX_API_KEY")
        if not self.api_key:
            raise ValueError(
                "JCDECAUX_API_KEY is required (pass jcdecaux_api_key or set env var).")

        self.contract = jcdecaux_contract
        self.retention_days = int(retention_days)
        self.ahead_days = int(create_partitions_ahead_days)
        self.request_timeout = int(request_timeout)

        # 1) connect to server (no DB) to create DB if needed
        self._conn = self._connect(database=None)
        self._create_database_if_not_exists()
        self._conn.close()

        # 2) connect to the target DB
        self._conn = self._connect(database=self.db_name)

        # 3) create tables (MUST happen before partition ops)
        self._create_tables()

        # 4) ensure partitions exist for recent past + near future
        today = dt.date.today()
        start = today - dt.timedelta(days=self.retention_days + 1)  # buffer
        end = today + dt.timedelta(days=self.ahead_days)
        # self.ensure_partitions(start_date=start, end_date=end)

        # 5) enforce retention now
        self.drop_old_partitions()

    # ----------------------------
    # MySQL connection + helpers
    # ----------------------------
    def _connect(self, database: Optional[str]):
        return pymysql.connect(
            host=self.mysql_host,
            user=self.mysql_user,
            password=self.mysql_password,
            port=self.mysql_port,
            database=database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )

    def _execute(self, sql: str, params: Optional[tuple] = None):
        with self._conn.cursor() as cur:
            cur.execute(sql, params or ())

    def _fetchall(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()

    def _create_database_if_not_exists(self):
        sql = f"CREATE DATABASE IF NOT EXISTS `{self.db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        self._execute(sql)
        self._conn.commit()

    def _table_exists(self, table_name: str) -> bool:
        rows = self._fetchall(
            """
            SELECT 1 AS ok
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
            LIMIT 1;
            """,
            (self.db_name, table_name),
        )
        return len(rows) > 0

    # ----------------------------
    # Table creation
    # ----------------------------
    def _create_tables(self):
        create_current = """
        CREATE TABLE IF NOT EXISTS bike_current (
            station_id INT NOT NULL PRIMARY KEY,
            name VARCHAR(255) NULL,
            address VARCHAR(255) NULL,
            latitude DOUBLE NULL,
            longitude DOUBLE NULL,
            banking TINYINT NULL,
            bonus TINYINT NULL,

            available_bikes INT NOT NULL,
            available_stands INT NOT NULL,
            capacity INT NOT NULL,
            status VARCHAR(32) NULL,

            scraped_at DATETIME NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB;
        """

        # NOTE:
        # - partition key scraped_date MUST be included in every UNIQUE/PRIMARY KEY
        # - pmax is required to allow reorganize/splitting in the future
        create_history = """
        CREATE TABLE IF NOT EXISTS bike_history (
            station_id INT NOT NULL,
            scraped_at DATETIME NOT NULL,
            scraped_date DATE GENERATED ALWAYS AS (DATE(scraped_at)) STORED NOT NULL,

            available_bikes INT NOT NULL,
            available_stands INT NOT NULL,
            capacity INT NOT NULL,
            status VARCHAR(32) NULL,

            PRIMARY KEY (station_id, scraped_date, scraped_at),
            INDEX idx_station_time (station_id, scraped_at),
            INDEX idx_scraped_at (scraped_at)
        ) ENGINE=InnoDB
        PARTITION BY RANGE COLUMNS (scraped_date) (
            PARTITION pmin VALUES LESS THAN ('2000-01-01'),
            PARTITION pmax VALUES LESS THAN (MAXVALUE)
        );
        """

        try:
            self._execute(create_current)
            self._execute(create_history)
            self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            raise RuntimeError(f"Failed creating tables: {e}") from e

        if not self._table_exists("bike_history"):
            raise RuntimeError(
                "bike_history table still does not exist after creation.")

    # ----------------------------
    # Partition management (Option B)
    # ----------------------------
    @staticmethod
    def _pname(d: dt.date) -> str:
        return f"p{d.strftime('%Y%m%d')}"  # p20260221

    def _existing_partitions(self) -> Dict[str, str]:
        rows = self._fetchall(
            """
            SELECT PARTITION_NAME, PARTITION_DESCRIPTION
            FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'bike_history'
              AND PARTITION_NAME IS NOT NULL
            ORDER BY PARTITION_ORDINAL_POSITION;
            """,
            (self.db_name,),
        )
        return {r["PARTITION_NAME"]: str(r["PARTITION_DESCRIPTION"]) for r in rows}

    def _maxvalue_partition_name(self) -> str:
        # Usually it's 'pmax', but we'll detect it to be safe.
        rows = self._fetchall(
            """
            SELECT PARTITION_NAME, PARTITION_DESCRIPTION
            FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'bike_history'
              AND PARTITION_NAME IS NOT NULL
            ORDER BY PARTITION_ORDINAL_POSITION;
            """,
            (self.db_name,),
        )
        for r in rows:
            desc = str(r["PARTITION_DESCRIPTION"]).upper()
            if "MAXVALUE" in desc:
                return r["PARTITION_NAME"]
        # fallback if INFORMATION_SCHEMA returns NULL-ish for MAXVALUE (rare)
        if "pmax" in self._existing_partitions():
            return "pmax"
        raise RuntimeError(
            "No MAXVALUE partition found (expected something like pmax).")

    def ensure_partitions(self, start_date: dt.date, end_date: dt.date):
        """
        Ensure daily partitions exist for [start_date, end_date] inclusive.
        For day D, partition boundary is VALUES LESS THAN (D+1).
        """
        if not self._table_exists("bike_history"):
            raise RuntimeError(
                "bike_history table does not exist. Table creation failed.")

        existing = self._existing_partitions()
        d = start_date
        while d <= end_date:
            pname = self._pname(d)
            if pname not in existing:
                self._add_partition_for_day(d)
                existing[pname] = ""  # avoid re-check
            d += dt.timedelta(days=1)

        self._conn.commit()

    def _add_partition_for_day(self, day: dt.date):
        """
        Split the MAXVALUE partition into (new day partition + MAXVALUE partition).
        """
        pname = self._pname(day)
        less_than = (day + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        pmax = self._maxvalue_partition_name()

        sql = f"""
        ALTER TABLE bike_history
        REORGANIZE PARTITION {pmax} INTO (
            PARTITION {pname} VALUES LESS THAN ('{less_than}'),
            PARTITION {pmax} VALUES LESS THAN (MAXVALUE)
        );
        """
        self._execute(sql)

    def drop_old_partitions(self):
        """
        Drop partitions older than retention_days.
        Keep partitions with day >= (today - retention_days).
        """
        if not self._table_exists("bike_history"):
            return

        today = dt.date.today()
        keep_from = today - dt.timedelta(days=self.retention_days)  # inclusive

        existing = self._existing_partitions()
        to_drop: List[str] = []

        for pname in existing.keys():
            if pname in ("pmin", "pmax"):
                continue
            if not pname.startswith("p") or len(pname) != 9:
                continue
            try:
                day = dt.datetime.strptime(pname[1:], "%Y%m%d").date()
            except ValueError:
                continue
            if day < keep_from:
                to_drop.append(pname)

        if not to_drop:
            return

        sql = f"ALTER TABLE bike_history DROP PARTITION {', '.join(to_drop)};"
        self._execute(sql)
        self._conn.commit()

    # ----------------------------
    # Scrape + store
    # ----------------------------
    def scrape(self) -> List[Dict[str, Any]]:
        url = "https://api.jcdecaux.com/vls/v1/stations"
        params = {"contract": self.contract, "apiKey": self.api_key}
        resp = requests.get(url, params=params, timeout=self.request_timeout)
        resp.raise_for_status()
        data = resp.json()

        scraped_at = dt.datetime.utcnow().replace(microsecond=0)
        rows: List[Dict[str, Any]] = []

        for s in data:
            pos = s.get("position") or {}
            rows.append(
                {
                    "station_id": int(s.get("number")),
                    "name": s.get("name"),
                    "address": s.get("address"),
                    "latitude": float(pos["lat"]) if "lat" in pos else None,
                    "longitude": float(pos["lng"]) if "lng" in pos else None,
                    "banking": 1 if s.get("banking") else 0 if s.get("banking") is not None else None,
                    "bonus": 1 if s.get("bonus") else 0 if s.get("bonus") is not None else None,
                    "available_bikes": int(s.get("available_bikes", 0)),
                    "available_stands": int(s.get("available_bike_stands", 0)),
                    "capacity": int(s.get("bike_stands", 0)),
                    "status": s.get("status"),
                    "scraped_at": scraped_at,
                }
            )

        return rows

    def push_in(self, rows: List[Dict[str, Any]]):
        if not rows:
            return

        scraped_at: dt.datetime = rows[0]["scraped_at"]
        day = scraped_at.date()

        # Ensure partitions exist for insert day (and a bit around it)
        self.ensure_partitions(
            start_date=day - dt.timedelta(days=1),
            end_date=day + dt.timedelta(days=self.ahead_days),
        )

        history_sql = """
        INSERT INTO bike_history
            (station_id, scraped_at, available_bikes, available_stands, capacity, status)
        VALUES
            (%s, %s, %s, %s, %s, %s);
        """

        history_params = [
            (
                r["station_id"],
                r["scraped_at"],
                r["available_bikes"],
                r["available_stands"],
                r["capacity"],
                r["status"],
            )
            for r in rows
        ]

        current_sql = """
        INSERT INTO bike_current
            (station_id, name, address, latitude, longitude, banking, bonus,
             available_bikes, available_stands, capacity, status, scraped_at)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name=VALUES(name),
            address=VALUES(address),
            latitude=VALUES(latitude),
            longitude=VALUES(longitude),
            banking=VALUES(banking),
            bonus=VALUES(bonus),
            available_bikes=VALUES(available_bikes),
            available_stands=VALUES(available_stands),
            capacity=VALUES(capacity),
            status=VALUES(status),
            scraped_at=VALUES(scraped_at);
        """

        current_params = [
            (
                r["station_id"],
                r["name"],
                r["address"],
                r["latitude"],
                r["longitude"],
                r["banking"],
                r["bonus"],
                r["available_bikes"],
                r["available_stands"],
                r["capacity"],
                r["status"],
                r["scraped_at"],
            )
            for r in rows
        ]

        try:
            with self._conn.cursor() as cur:
                cur.executemany(history_sql, history_params)
                cur.executemany(current_sql, current_params)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def push_out(self):
        self.drop_old_partitions()

    # ----------------------------
    # Convenience
    # ----------------------------
    def run_once(self):
        rows = self.scrape()
        self.push_in(rows)
        self.push_out()

    def run_forever(self, interval_seconds: int = 300):
        interval_seconds = int(interval_seconds)
        while True:
            self.run_once()
            time.sleep(interval_seconds)


if __name__ == "__main__":
    repo = BikeRepository(
        mysql_host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        mysql_user=os.getenv("MYSQL_USER", "root"),
        mysql_password=os.getenv("MYSQL_PASSWORD", ""),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
        db_name=os.getenv("MYSQL_DB", "bike_db"),
        jcdecaux_api_key=os.getenv("JCDECAUX_API_KEY"),
        jcdecaux_contract=os.getenv("JCDECAUX_CONTRACT", "dublin"),
        retention_days=7,
        create_partitions_ahead_days=1,  # 1 is enough for local dev
    )
    repo.run_forever(interval_seconds=300)

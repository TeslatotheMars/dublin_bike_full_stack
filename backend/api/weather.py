import os
import time
import datetime as dt
from typing import Dict, List, Any, Optional, Tuple

import requests
import pymysql


class WeatherRepository:
    """
    OpenWeather ingest + storage (MySQL/MariaDB), similar to bike.py:

      - weather_current: latest observed snapshot for frontend
      - weather_history: time-series of observed hourly data (partitioned by day)
      - weather_forecast: hourly forecast rows for next N hours (partitioned by day)

    Partition strategy:
      - generated STORED date columns (obs_date / forecast_date)
      - daily RANGE COLUMNS partitions + a MAXVALUE partition (pmax)
      - retention drops old partitions quickly

    IMPORTANT:
      MySQL cannot "insert" an older partition boundary by splitting pmax.
      So we only extend partitions FORWARD from the latest existing partition day.
    """

    def __init__(
        self,
        mysql_host: str,
        mysql_user: str,
        mysql_password: str,
        mysql_port: int = 3306,
        db_name: str = "bike_db",
        openweather_api_key: Optional[str] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        units: str = "metric",
        retention_days_history: int = 7,
        retention_days_forecast: int = 3,
        forecast_horizon_hours: int = 24,
        create_partitions_ahead_days: int = 2,
        request_timeout: int = 15,
    ):
        self.mysql_host = mysql_host
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_port = int(mysql_port)
        self.db_name = db_name

        # DO NOT hardcode API keys in code. Use env var.
        self.api_key = openweather_api_key or os.getenv("OPENWEATHER_API_KEY")
        if not self.api_key:
            raise ValueError("OPENWEATHER_API_KEY is required (pass openweather_api_key or set env var).")

        self.lat = float(latitude if latitude is not None else os.getenv("OWM_LAT", "53.3498"))
        self.lon = float(longitude if longitude is not None else os.getenv("OWM_LON", "-6.2603"))
        self.units = units

        self.retention_days_history = int(retention_days_history)
        self.retention_days_forecast = int(retention_days_forecast)
        self.forecast_horizon_hours = int(forecast_horizon_hours)
        self.ahead_days = int(create_partitions_ahead_days)
        self.request_timeout = int(request_timeout)

        # 1) connect to server (no DB) to create DB if needed
        self._conn = self._connect(database=None)
        self._create_database_if_not_exists()
        self._conn.close()

        # 2) connect to the target DB
        self._conn = self._connect(database=self.db_name)

        # 3) create tables (must happen before partition ops)
        self._create_tables()

        # 4) ensure partitions exist (FORWARD ONLY)
        today = dt.date.today()

        # Only need to ensure from today to a bit ahead. Retention will prune old partitions.
        self.ensure_partitions_history(start_date=today, end_date=today + dt.timedelta(days=self.ahead_days))
        self.ensure_partitions_forecast(start_date=today, end_date=today + dt.timedelta(days=max(self.ahead_days, 2)))

        # 5) enforce retention now
        self.drop_old_partitions_history()
        self.drop_old_partitions_forecast()

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
        CREATE TABLE IF NOT EXISTS weather_current (
            location_key VARCHAR(64) NOT NULL PRIMARY KEY,
            latitude DOUBLE NOT NULL,
            longitude DOUBLE NOT NULL,

            observed_at DATETIME NOT NULL,
            timezone_offset_seconds INT NULL,

            temperature DOUBLE NULL,
            feels_like DOUBLE NULL,
            humidity INT NULL,
            pressure INT NULL,
            wind_speed DOUBLE NULL,
            wind_deg INT NULL,
            clouds INT NULL,
            visibility INT NULL,

            weather_main VARCHAR(64) NULL,
            weather_description VARCHAR(255) NULL,

            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB;
        """

        create_history = """
        CREATE TABLE IF NOT EXISTS weather_history (
            location_key VARCHAR(64) NOT NULL,
            observed_at DATETIME NOT NULL,
            obs_date DATE GENERATED ALWAYS AS (DATE(observed_at)) STORED NOT NULL,

            timezone_offset_seconds INT NULL,

            temperature DOUBLE NULL,
            feels_like DOUBLE NULL,
            humidity INT NULL,
            pressure INT NULL,
            wind_speed DOUBLE NULL,
            wind_deg INT NULL,
            clouds INT NULL,
            visibility INT NULL,

            weather_main VARCHAR(64) NULL,
            weather_description VARCHAR(255) NULL,

            PRIMARY KEY (location_key, obs_date, observed_at),
            INDEX idx_loc_time (location_key, observed_at),
            INDEX idx_observed_at (observed_at)
        ) ENGINE=InnoDB
        PARTITION BY RANGE COLUMNS (obs_date) (
            PARTITION pmin VALUES LESS THAN ('2000-01-01'),
            PARTITION pmax VALUES LESS THAN (MAXVALUE)
        );
        """

        create_forecast = """
        CREATE TABLE IF NOT EXISTS weather_forecast (
            location_key VARCHAR(64) NOT NULL,
            forecast_time DATETIME NOT NULL,
            forecast_date DATE GENERATED ALWAYS AS (DATE(forecast_time)) STORED NOT NULL,

            generated_at DATETIME NOT NULL,

            timezone_offset_seconds INT NULL,

            temperature DOUBLE NULL,
            feels_like DOUBLE NULL,
            humidity INT NULL,
            pressure INT NULL,
            wind_speed DOUBLE NULL,
            wind_deg INT NULL,
            clouds INT NULL,
            pop DOUBLE NULL,

            weather_main VARCHAR(64) NULL,
            weather_description VARCHAR(255) NULL,

            PRIMARY KEY (location_key, forecast_date, forecast_time),
            INDEX idx_loc_forecast_time (location_key, forecast_time),
            INDEX idx_generated_at (generated_at)
        ) ENGINE=InnoDB
        PARTITION BY RANGE COLUMNS (forecast_date) (
            PARTITION pmin VALUES LESS THAN ('2000-01-01'),
            PARTITION pmax VALUES LESS THAN (MAXVALUE)
        );
        """

        try:
            self._execute(create_current)
            self._execute(create_history)
            self._execute(create_forecast)
            self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            raise RuntimeError(f"Failed creating weather tables: {e}") from e

        for t in ("weather_current", "weather_history", "weather_forecast"):
            if not self._table_exists(t):
                raise RuntimeError(f"{t} table still does not exist after creation.")

    # ----------------------------
    # Partition helpers
    # ----------------------------
    @staticmethod
    def _pname(d: dt.date) -> str:
        return f"p{d.strftime('%Y%m%d')}"

    def _existing_partitions(self, table_name: str) -> Dict[str, str]:
        rows = self._fetchall(
            """
            SELECT PARTITION_NAME, PARTITION_DESCRIPTION
            FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
              AND PARTITION_NAME IS NOT NULL
            ORDER BY PARTITION_ORDINAL_POSITION;
            """,
            (self.db_name, table_name),
        )
        return {r["PARTITION_NAME"]: str(r["PARTITION_DESCRIPTION"]) for r in rows}

    def _maxvalue_partition_name(self, table_name: str) -> str:
        rows = self._fetchall(
            """
            SELECT PARTITION_NAME, PARTITION_DESCRIPTION
            FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
              AND PARTITION_NAME IS NOT NULL
            ORDER BY PARTITION_ORDINAL_POSITION;
            """,
            (self.db_name, table_name),
        )
        for r in rows:
            desc = str(r["PARTITION_DESCRIPTION"]).upper()
            if "MAXVALUE" in desc:
                return r["PARTITION_NAME"]

        existing = self._existing_partitions(table_name)
        if "pmax" in existing:
            return "pmax"
        raise RuntimeError(f"No MAXVALUE partition found on {table_name} (expected pmax).")

    def _latest_partition_day(self, table_name: str) -> Optional[dt.date]:
        existing = self._existing_partitions(table_name)
        days: List[dt.date] = []
        for pname in existing.keys():
            if pname in ("pmin", "pmax"):
                continue
            if pname.startswith("p") and len(pname) == 9:
                try:
                    days.append(dt.datetime.strptime(pname[1:], "%Y%m%d").date())
                except ValueError:
                    pass
        return max(days) if days else None

    def _add_partition_for_day(self, table_name: str, day: dt.date):
        """
        Adds a single day partition by splitting pmax.
        This ONLY works if `day` is AFTER all existing day partitions.
        """
        pname = self._pname(day)
        less_than = (day + dt.timedelta(days=1)).strftime("%Y-%m-%d")
        pmax = self._maxvalue_partition_name(table_name)
        sql = f"""
        ALTER TABLE {table_name}
        REORGANIZE PARTITION {pmax} INTO (
            PARTITION {pname} VALUES LESS THAN ('{less_than}'),
            PARTITION {pmax} VALUES LESS THAN (MAXVALUE)
        );
        """
        self._execute(sql)

    def _ensure_partitions_forward_only(self, table_name: str, start_date: dt.date, end_date: dt.date):
        """
        Make sure partitions exist from start_date..end_date, but only extend forward.
        If the table already has day partitions up to D, we only create (D+1..end_date).
        """
        if not self._table_exists(table_name):
            raise RuntimeError(f"{table_name} table does not exist.")

        latest = self._latest_partition_day(table_name)
        if latest is not None:
            start_date = max(start_date, latest + dt.timedelta(days=1))

        if start_date > end_date:
            return

        d = start_date
        while d <= end_date:
            self._add_partition_for_day(table_name, d)
            d += dt.timedelta(days=1)

        self._conn.commit()

    # ---- history partitions
    def ensure_partitions_history(self, start_date: dt.date, end_date: dt.date):
        self._ensure_partitions_forward_only("weather_history", start_date, end_date)

    def drop_old_partitions_history(self):
        if not self._table_exists("weather_history"):
            return
        today = dt.date.today()
        keep_from = today - dt.timedelta(days=self.retention_days_history)

        existing = self._existing_partitions("weather_history")
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

        if to_drop:
            sql = f"ALTER TABLE weather_history DROP PARTITION {', '.join(to_drop)};"
            self._execute(sql)
            self._conn.commit()

    # ---- forecast partitions
    def ensure_partitions_forecast(self, start_date: dt.date, end_date: dt.date):
        self._ensure_partitions_forward_only("weather_forecast", start_date, end_date)

    def drop_old_partitions_forecast(self):
        if not self._table_exists("weather_forecast"):
            return
        today = dt.date.today()
        keep_from = today - dt.timedelta(days=self.retention_days_forecast)

        existing = self._existing_partitions("weather_forecast")
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

        if to_drop:
            sql = f"ALTER TABLE weather_forecast DROP PARTITION {', '.join(to_drop)};"
            self._execute(sql)
            self._conn.commit()

    # ----------------------------
    # OpenWeather scrape
    # ----------------------------
    def _location_key(self) -> str:
        return f"dublin_{self.lat:.4f}_{self.lon:.4f}"

    @staticmethod
    def _to_utc_datetime(unix_ts: int) -> dt.datetime:
        return dt.datetime.utcfromtimestamp(int(unix_ts)).replace(microsecond=0)

    def scrape(self) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Uses One Call 3.0 endpoint:
          https://api.openweathermap.org/data/3.0/onecall

        Returns:
          current_row: dict for weather_current + weather_history
          forecast_rows: list for weather_forecast (next forecast_horizon_hours)
        """
        url = "https://api.openweathermap.org/data/3.0/onecall"
        params = {
            "lat": self.lat,
            "lon": self.lon,
            "appid": self.api_key,
            "units": self.units,
            "exclude": "minutely,daily,alerts",
        }

        resp = requests.get(url, params=params, timeout=self.request_timeout)

        if resp.status_code == 401:
            # Give a useful message (invalid key, not activated, or One Call not enabled)
            raise RuntimeError(
                "OpenWeather 401 Unauthorized. "
                "Common causes: invalid API key, key not activated yet, or your plan/key lacks One Call 3.0 access. "
                f"Response: {resp.text}"
            )

        resp.raise_for_status()
        data = resp.json()

        location_key = self._location_key()
        tz_offset = data.get("timezone_offset")
        generated_at = dt.datetime.utcnow().replace(microsecond=0)

        cur = data.get("current") or {}
        observed_at = self._to_utc_datetime(cur.get("dt", int(generated_at.timestamp())))

        w0 = (cur.get("weather") or [{}])[0] or {}
        current_row = {
            "location_key": location_key,
            "latitude": self.lat,
            "longitude": self.lon,
            "observed_at": observed_at,
            "timezone_offset_seconds": tz_offset,
            "temperature": cur.get("temp"),
            "feels_like": cur.get("feels_like"),
            "humidity": cur.get("humidity"),
            "pressure": cur.get("pressure"),
            "wind_speed": cur.get("wind_speed"),
            "wind_deg": cur.get("wind_deg"),
            "clouds": cur.get("clouds"),
            "visibility": cur.get("visibility"),
            "weather_main": w0.get("main"),
            "weather_description": w0.get("description"),
        }

        hourly = data.get("hourly") or []
        forecast_rows: List[Dict[str, Any]] = []
        for h in hourly[: self.forecast_horizon_hours]:
            ft = self._to_utc_datetime(h.get("dt"))
            hw0 = (h.get("weather") or [{}])[0] or {}
            forecast_rows.append(
                {
                    "location_key": location_key,
                    "forecast_time": ft,
                    "generated_at": generated_at,
                    "timezone_offset_seconds": tz_offset,
                    "temperature": h.get("temp"),
                    "feels_like": h.get("feels_like"),
                    "humidity": h.get("humidity"),
                    "pressure": h.get("pressure"),
                    "wind_speed": h.get("wind_speed"),
                    "wind_deg": h.get("wind_deg"),
                    "clouds": h.get("clouds"),
                    "pop": h.get("pop"),
                    "weather_main": hw0.get("main"),
                    "weather_description": hw0.get("description"),
                }
            )

        return current_row, forecast_rows

    # ----------------------------
    # Store
    # ----------------------------
    def push_in(self, current_row: Dict[str, Any], forecast_rows: List[Dict[str, Any]]):
        if not current_row:
            return

        loc = current_row["location_key"]
        obs_at: dt.datetime = current_row["observed_at"]
        day = obs_at.date()

        # Ensure partitions around this day (forward-only will no-op for older)
        self.ensure_partitions_history(
            start_date=day,
            end_date=day + dt.timedelta(days=self.ahead_days),
        )

        if forecast_rows:
            min_day = min(r["forecast_time"].date() for r in forecast_rows)
            max_day = max(r["forecast_time"].date() for r in forecast_rows)
            self.ensure_partitions_forecast(
                start_date=min_day,
                end_date=max_day + dt.timedelta(days=self.ahead_days),
            )

        current_sql = """
        INSERT INTO weather_current
            (location_key, latitude, longitude, observed_at, timezone_offset_seconds,
             temperature, feels_like, humidity, pressure, wind_speed, wind_deg,
             clouds, visibility, weather_main, weather_description)
        VALUES
            (%s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            latitude=VALUES(latitude),
            longitude=VALUES(longitude),
            observed_at=VALUES(observed_at),
            timezone_offset_seconds=VALUES(timezone_offset_seconds),
            temperature=VALUES(temperature),
            feels_like=VALUES(feels_like),
            humidity=VALUES(humidity),
            pressure=VALUES(pressure),
            wind_speed=VALUES(wind_speed),
            wind_deg=VALUES(wind_deg),
            clouds=VALUES(clouds),
            visibility=VALUES(visibility),
            weather_main=VALUES(weather_main),
            weather_description=VALUES(weather_description);
        """

        history_sql = """
        INSERT INTO weather_history
            (location_key, observed_at, timezone_offset_seconds,
             temperature, feels_like, humidity, pressure, wind_speed, wind_deg,
             clouds, visibility, weather_main, weather_description)
        VALUES
            (%s, %s, %s,
             %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s);
        """

        forecast_sql = """
        INSERT INTO weather_forecast
            (location_key, forecast_time, generated_at, timezone_offset_seconds,
             temperature, feels_like, humidity, pressure, wind_speed, wind_deg,
             clouds, pop, weather_main, weather_description)
        VALUES
            (%s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            generated_at=VALUES(generated_at),
            timezone_offset_seconds=VALUES(timezone_offset_seconds),
            temperature=VALUES(temperature),
            feels_like=VALUES(feels_like),
            humidity=VALUES(humidity),
            pressure=VALUES(pressure),
            wind_speed=VALUES(wind_speed),
            wind_deg=VALUES(wind_deg),
            clouds=VALUES(clouds),
            pop=VALUES(pop),
            weather_main=VALUES(weather_main),
            weather_description=VALUES(weather_description);
        """

        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    current_sql,
                    (
                        current_row["location_key"],
                        current_row["latitude"],
                        current_row["longitude"],
                        current_row["observed_at"],
                        current_row["timezone_offset_seconds"],
                        current_row["temperature"],
                        current_row["feels_like"],
                        current_row["humidity"],
                        current_row["pressure"],
                        current_row["wind_speed"],
                        current_row["wind_deg"],
                        current_row["clouds"],
                        current_row["visibility"],
                        current_row["weather_main"],
                        current_row["weather_description"],
                    ),
                )

                cur.execute(
                    history_sql,
                    (
                        loc,
                        current_row["observed_at"],
                        current_row["timezone_offset_seconds"],
                        current_row["temperature"],
                        current_row["feels_like"],
                        current_row["humidity"],
                        current_row["pressure"],
                        current_row["wind_speed"],
                        current_row["wind_deg"],
                        current_row["clouds"],
                        current_row["visibility"],
                        current_row["weather_main"],
                        current_row["weather_description"],
                    ),
                )

                if forecast_rows:
                    params = [
                        (
                            r["location_key"],
                            r["forecast_time"],
                            r["generated_at"],
                            r["timezone_offset_seconds"],
                            r["temperature"],
                            r["feels_like"],
                            r["humidity"],
                            r["pressure"],
                            r["wind_speed"],
                            r["wind_deg"],
                            r["clouds"],
                            r["pop"],
                            r["weather_main"],
                            r["weather_description"],
                        )
                        for r in forecast_rows
                    ]
                    cur.executemany(forecast_sql, params)

            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def push_out(self):
        self.drop_old_partitions_history()
        self.drop_old_partitions_forecast()

    # ----------------------------
    # Convenience
    # ----------------------------
    def run_once(self):
        current_row, forecast_rows = self.scrape()
        self.push_in(current_row, forecast_rows)
        self.push_out()

    def run_forever(self, interval_seconds: int = 3600):
        interval_seconds = int(interval_seconds)
        while True:
            self.run_once()
            time.sleep(interval_seconds)


if __name__ == "__main__":
    repo = WeatherRepository(
        mysql_host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        mysql_user=os.getenv("MYSQL_USER", "root"),
        mysql_password=os.getenv("MYSQL_PASSWORD", ""),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
        db_name=os.getenv("MYSQL_DB", "bike_db"),
        openweather_api_key=os.getenv("OPENWEATHER_API_KEY"),
        latitude=float(os.getenv("OWM_LAT", "53.3498")),
        longitude=float(os.getenv("OWM_LON", "-6.2603")),
        units=os.getenv("OWM_UNITS", "metric"),
        retention_days_history=int(os.getenv("WEATHER_HISTORY_RETENTION_DAYS", "7")),
        retention_days_forecast=int(os.getenv("WEATHER_FORECAST_RETENTION_DAYS", "3")),
        forecast_horizon_hours=int(os.getenv("WEATHER_FORECAST_HORIZON_HOURS", "24")),
        create_partitions_ahead_days=int(os.getenv("WEATHER_PARTITIONS_AHEAD_DAYS", "2")),
        request_timeout=int(os.getenv("REQUEST_TIMEOUT", "15")),
        )

    repo.run_forever(interval_seconds=int(os.getenv("WEATHER_INTERVAL_SECONDS", "3600")))
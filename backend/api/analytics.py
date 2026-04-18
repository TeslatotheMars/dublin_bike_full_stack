# analytics.py
import os
import math
import datetime as dt
from typing import Optional, Tuple, Dict, Any, List

import pymysql
import pandas as pd

# pip install scikit-learn joblib pandas pymysql
from sklearn.linear_model import Ridge
import joblib


class AnalyticsRepository:
    """
    What this module does:
      1) Build descriptive statistics table:
           stats(station_id, day, hour, average)
         - day: Monday..Sunday
         - hour: 0..23
         - average: average available bikes for that station/day/hour

      2) Build ML training dataset (hourly):
         - aggregate bike_history to hourly mean per station
         - join with weather_history (hourly)

      3) Train model on ALL data (no train/test split)
         Features:
           - wind_speed
           - temp, temp^2
           - rain (weather_main == 'Rain')
           - sin_hour, cos_hour
           - weekday dummies (6 vars, Monday dropped)
           - optional station fixed effect dummies

      4) Use trained model + weather_forecast to generate next 24 hours (every 5 minutes)
         and store into:
           bike_forecast(station_id, forecast_time, available_bike)
    """

    def __init__(
        self,
        mysql_host: str,
        mysql_user: str,
        mysql_password: str,
        mysql_port: int = 3306,
        db_name: str = "bike_db",
    ):
        self._conn = pymysql.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            port=int(mysql_port),
            database=db_name,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )

    # ----------------------------
    # Generic helpers
    # ----------------------------
    def _execute(self, sql: str, params: Optional[tuple] = None) -> None:
        with self._conn.cursor() as cur:
            cur.execute(sql, params or ())

    def _fetchall(self, sql: str, params: Optional[tuple] = None):
        with self._conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    # ----------------------------
    # 1) Stats table (descriptive statistics)
    # ----------------------------
    def create_stats_table(self) -> None:
        sql = """
        CREATE TABLE IF NOT EXISTS stats (
            station_id INT NOT NULL,
            day VARCHAR(16) NOT NULL,
            hour TINYINT NOT NULL,
            average DOUBLE NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (station_id, day, hour),
            INDEX idx_day_hour (day, hour)
        ) ENGINE=InnoDB;
        """
        self._execute(sql)
        self._conn.commit()

    def refresh_stats(self, lookback_days: int = 7) -> None:
        """
        Refresh stats based on bike_history in last N days.
        Uses derived table to avoid ONLY_FULL_GROUP_BY issues.
        """
        self.create_stats_table()

        sql = """
        INSERT INTO stats (station_id, day, hour, average)
        SELECT
            x.station_id,
            x.day_name,
            x.hour_val,
            AVG(x.available_bikes) AS avg_bikes
        FROM (
            SELECT
                station_id,
                CASE WEEKDAY(scraped_at)
                    WHEN 0 THEN 'Monday'
                    WHEN 1 THEN 'Tuesday'
                    WHEN 2 THEN 'Wednesday'
                    WHEN 3 THEN 'Thursday'
                    WHEN 4 THEN 'Friday'
                    WHEN 5 THEN 'Saturday'
                    WHEN 6 THEN 'Sunday'
                END AS day_name,
                HOUR(scraped_at) AS hour_val,
                available_bikes
            FROM bike_history
            WHERE scraped_at >= (UTC_TIMESTAMP() - INTERVAL %s DAY)
        ) x
        GROUP BY x.station_id, x.day_name, x.hour_val
        ON DUPLICATE KEY UPDATE
            average = VALUES(average);
        """
        try:
            self._execute(sql, (int(lookback_days),))
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    # ----------------------------
    # 2) Dataset builder (hourly) for training
    # ----------------------------
    def fetch_hourly_dataset(self, lookback_days: int = 7) -> pd.DataFrame:
        """
        Returns hourly dataset with:
          station_id, hour_ts, y_available_bikes, wind_speed, temperature, weather_main

        Implementation avoids CTE for broad MySQL compatibility.
        """
        sql = """
        SELECT
            b.station_id,
            b.hour_ts,
            b.y_available_bikes,
            w.wind_speed,
            w.temperature,
            w.weather_main
        FROM
        (
            SELECT
                station_id,
                DATE_FORMAT(scraped_at, '%%Y-%%m-%%d %%H:00:00') AS hour_ts,
                AVG(available_bikes) AS y_available_bikes
            FROM bike_history
            WHERE scraped_at >= (UTC_TIMESTAMP() - INTERVAL %s DAY)
            GROUP BY station_id, DATE_FORMAT(scraped_at, '%%Y-%%m-%%d %%H:00:00')
        ) b
        JOIN
        (
            SELECT
                DATE_FORMAT(observed_at, '%%Y-%%m-%%d %%H:00:00') AS hour_ts,
                AVG(wind_speed) AS wind_speed,
                AVG(temperature) AS temperature,
                MAX(weather_main) AS weather_main
            FROM weather_history
            WHERE observed_at >= (UTC_TIMESTAMP() - INTERVAL %s DAY)
            GROUP BY DATE_FORMAT(observed_at, '%%Y-%%m-%%d %%H:00:00')
        ) w
        ON b.hour_ts = w.hour_ts
        WHERE
            w.wind_speed IS NOT NULL
            AND w.temperature IS NOT NULL;
        """

        rows = self._fetchall(sql, (int(lookback_days), int(lookback_days)))
        df = pd.DataFrame(rows)
        if df.empty:
            return df

        df["hour_ts"] = pd.to_datetime(df["hour_ts"], utc=True)
        # Add lag feature: previous hour's available bikes
        df = df.sort_values(["station_id", "hour_ts"])
        df["lag_1h"] = df.groupby("station_id")["y_available_bikes"].shift(1)
        # Drop rows with NaN lag (first hour for each station)
        df = df.dropna(subset=["lag_1h"])
        return df

    def _feature_engineer(
        self,
        df: pd.DataFrame,
        include_station_fe: bool = True
    ) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Features:
          - lag_1h (previous hour's available bikes)
          - wind_speed
          - temp, temp2
          - rain
          - sin_hour, cos_hour
          - weekday dummies (6, Monday dropped)
          - station fixed effects (optional)
        """
        work = df.copy()

        # target
        y = work["y_available_bikes"].astype(float)

        # lag feature
        work["lag_1h"] = work["lag_1h"].astype(float)

        # weather
        work["wind_speed"] = work["wind_speed"].astype(float)
        work["temp"] = work["temperature"].astype(float)
        work["temp2"] = work["temp"] ** 2
        work["rain"] = (work["weather_main"].astype(
            str).str.lower() == "rain").astype(int)

        # time
        work["hour"] = work["hour_ts"].dt.hour
        work["weekday"] = work["hour_ts"].dt.dayofweek  # Monday=0..Sunday=6

        work["sin_hour"] = work["hour"].apply(
            lambda h: math.sin(2.0 * math.pi * h / 24.0))
        work["cos_hour"] = work["hour"].apply(
            lambda h: math.cos(2.0 * math.pi * h / 24.0))

        # weekday dummies: ensure wd_0..wd_6 exist, then drop wd_0 (Monday baseline)
        wd = pd.get_dummies(work["weekday"], prefix="wd", dtype=int)
        for k in range(7):
            col = f"wd_{k}"
            if col not in wd.columns:
                wd[col] = 0
        wd = wd[[f"wd_{k}" for k in range(7)]].drop(columns=["wd_0"])

        X_parts = [
            work[["lag_1h", "wind_speed", "temp", "temp2", "rain",
                  "sin_hour", "cos_hour"]].reset_index(drop=True),
            wd.reset_index(drop=True),
        ]

        if include_station_fe:
            st = pd.get_dummies(work["station_id"], prefix="st", dtype=int)
            X_parts.append(st.reset_index(drop=True))

        X = pd.concat(X_parts, axis=1)
        return X, y

    # ----------------------------
    # 3) Train on ALL data and save model bundle
    # ----------------------------
    def train_and_save_model_all_data(
        self,
        lookback_days: int = 7,
        model_path: str = "bike_availability_model.joblib",
        include_station_fe: bool = True,
        alpha: float = 1.0,
    ) -> Dict[str, Any]:
        df = self.fetch_hourly_dataset(lookback_days=lookback_days)
        if df.empty:
            raise RuntimeError(
                "No training data available. "
                "Check bike_history/weather_history and lookback_days."
            )

        X, y = self._feature_engineer(
            df, include_station_fe=include_station_fe)

        model = Ridge(alpha=float(alpha), random_state=42)
        model.fit(X, y)

        bundle = {
            "model": model,
            "feature_columns": list(X.columns),
            "include_station_fe": bool(include_station_fe),
            "alpha": float(alpha),
            "trained_at_utc": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "lookback_days": int(lookback_days),
            "n_rows": int(len(df)),
        }
        joblib.dump(bundle, model_path)
        return bundle

    # ----------------------------
    # 4) Forecast pipeline: weather_forecast -> bike_forecast
    # ----------------------------
    def create_bike_forecast_table(self) -> None:
        sql = """
        CREATE TABLE IF NOT EXISTS bike_forecast (
            station_id INT NOT NULL,
            forecast_time DATETIME NOT NULL,
            available_bike DOUBLE NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (station_id, forecast_time),
            INDEX idx_forecast_time (forecast_time)
        ) ENGINE=InnoDB;
        """
        self._execute(sql)
        self._conn.commit()

    def _get_station_ids(self) -> List[int]:
        """
        Get all station IDs from bike_current (preferred) or bike_history.
        """
        try:
            rows = self._fetchall(
                "SELECT station_id FROM bike_current ORDER BY station_id;")
            if rows:
                return [int(r["station_id"]) for r in rows]
        except Exception:
            pass
        rows = self._fetchall(
            "SELECT DISTINCT station_id FROM bike_history ORDER BY station_id;")
        return [int(r["station_id"]) for r in rows]

    def _get_latest_available_bikes(self) -> Dict[int, float]:
        """
        Get the latest available bikes for each station from bike_current or bike_history.
        Returns dict[station_id -> available_bikes]
        """
        # Try bike_current first
        try:
            rows = self._fetchall(
                "SELECT station_id, available_bikes FROM bike_current;")
            if rows:
                return {int(r["station_id"]): float(r["available_bikes"]) for r in rows}
        except Exception:
            pass

        # Fallback to latest from bike_history
        sql = """
        SELECT station_id, available_bikes
        FROM bike_history
        WHERE (station_id, scraped_at) IN (
            SELECT station_id, MAX(scraped_at)
            FROM bike_history
            GROUP BY station_id
        );
        """
        rows = self._fetchall(sql)
        return {int(r["station_id"]): float(r["available_bikes"]) for r in rows}

    def fetch_forecast_weather_hourly(
        self,
        start_utc: dt.datetime,
        end_utc: dt.datetime
    ) -> pd.DataFrame:
        """
        Assumed weather_forecast columns:
          - forecast_at (DATETIME/TIMESTAMP, UTC)
          - wind_speed
          - temperature
          - weather_main

        If your column names differ, adjust this SQL accordingly.
        """
        sql = """
        SELECT
            DATE_FORMAT(forecast_time, '%%Y-%%m-%%d %%H:00:00') AS hour_ts,
            AVG(wind_speed) AS wind_speed,
            AVG(temperature) AS temperature,
            MAX(weather_main) AS weather_main
        FROM weather_forecast
        WHERE forecast_time >= %s AND forecast_time < %s
        GROUP BY DATE_FORMAT(forecast_time, '%%Y-%%m-%%d %%H:00:00');
        """
        rows = self._fetchall(
            sql,
            (
                start_utc.strftime("%Y-%m-%d %H:%M:%S"),
                end_utc.strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["hour_ts"] = pd.to_datetime(df["hour_ts"], utc=True)
        return df

    def predict_and_store_bike_forecast(
        self,
        model_path: str = "bike_availability_model.joblib",
        next_hours: int = 24,
        step_minutes: int = 5,
        start_time_utc: Optional[dt.datetime] = None,
        clamp_to_non_negative: bool = True,
    ) -> int:
        """
        Predict available bikes for each station for next_hours, every step_minutes,
        using weather_forecast and trained model, store into bike_forecast.

        Returns number of rows written.
        """
        self.create_bike_forecast_table()

        bundle = joblib.load(model_path)
        model = bundle["model"]
        feature_columns = bundle["feature_columns"]
        include_station_fe = bool(bundle.get("include_station_fe", True))

        if start_time_utc is None:
            start_time_utc = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        end_time_utc = start_time_utc + dt.timedelta(hours=int(next_hours))

        # Build hourly timestamps for prediction
        hours = pd.date_range(
            start=pd.Timestamp(start_time_utc, tz="UTC"),
            end=pd.Timestamp(end_time_utc, tz="UTC"),
            freq="h",
            inclusive="left",
        )
        if len(hours) == 0:
            return 0

        # Load hourly forecast weather
        weather_h = self.fetch_forecast_weather_hourly(
            start_time_utc, end_time_utc)
        if weather_h.empty:
            raise RuntimeError(
                "weather_forecast has no rows for the requested forecast window.")

        # Stations
        station_ids = self._get_station_ids()
        if not station_ids:
            raise RuntimeError(
                "No stations found (stations empty and bike_history empty).")

        # Get latest available bikes for initial lag
        latest_bikes = self._get_latest_available_bikes()

        # Predict hour by hour
        predictions = []
        for station_id in station_ids:
            lag_value = latest_bikes.get(
                station_id, 0.0)  # default to 0 if no data

            for hour_ts in hours:
                # Get weather for this hour
                weather_row = weather_h[weather_h["hour_ts"] == hour_ts]
                if weather_row.empty:
                    continue

                wind_speed = float(weather_row["wind_speed"].iloc[0])
                temperature = float(weather_row["temperature"].iloc[0])
                weather_main = str(weather_row["weather_main"].iloc[0])

                # Feature engineering for this prediction
                hour = hour_ts.hour
                weekday = hour_ts.dayofweek
                sin_hour = math.sin(2.0 * math.pi * hour / 24.0)
                cos_hour = math.cos(2.0 * math.pi * hour / 24.0)
                temp = temperature
                temp2 = temp ** 2
                rain = 1 if weather_main.lower() == "rain" else 0

                # Build feature dictionary safely
                feature_dict = {
                    "lag_1h": lag_value,
                    "wind_speed": wind_speed,
                    "temp": temp,
                    "temp2": temp2,
                    "rain": rain,
                    "sin_hour": sin_hour,
                    "cos_hour": cos_hour
                }

                # weekday dummies
                for k in range(1, 7):
                    feature_dict[f"wd_{k}"] = 1 if weekday == k else 0

                # station dummies (only those known to the model)
                if include_station_fe:
                    for col in feature_columns:
                        if col.startswith("st_"):
                            feature_dict[col] = 1 if col == f"st_{station_id}" else 0

                # Create DataFrame for prediction
                X_pred = pd.DataFrame([{c: feature_dict.get(c, 0) for c in feature_columns}])[
                    feature_columns]

                # Predict
                pred = float(model.predict(X_pred)[0])
                if clamp_to_non_negative:
                    pred = max(0, pred)

                predictions.append({
                    "station_id": station_id,
                    "forecast_time": hour_ts,
                    "available_bike": pred
                })

                # Update lag for next hour
                lag_value = pred

        # Now interpolate to step_minutes intervals
        if not predictions:
            return 0

        pred_df = pd.DataFrame(predictions)
        pred_df["forecast_time"] = pd.to_datetime(
            pred_df["forecast_time"], utc=True)

        # Build target timestamps
        times = pd.date_range(
            start=pd.Timestamp(start_time_utc, tz="UTC"),
            end=pd.Timestamp(end_time_utc, tz="UTC"),
            freq=f"{int(step_minutes)}min",
            inclusive="left",
        )

        # Interpolate predictions to target times
        out_rows = []
        for station_id in station_ids:
            station_preds = pred_df[pred_df["station_id"] == station_id].copy()
            if station_preds.empty:
                continue

            station_preds = station_preds.set_index("forecast_time")
            station_preds = station_preds.reindex(
                times, method="ffill")  # forward fill hourly predictions

            for ts, row in station_preds.iterrows():
                out_rows.append({
                    "station_id": int(station_id),
                    "forecast_time": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "available_bike": float(row["available_bike"])
                })

        insert_sql = """
        INSERT INTO bike_forecast (station_id, forecast_time, available_bike)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            available_bike = VALUES(available_bike);
        """

        try:
            with self._conn.cursor() as cur:
                cur.executemany(insert_sql, [
                                (r["station_id"], r["forecast_time"], r["available_bike"]) for r in out_rows])
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

        return len(out_rows)


if __name__ == "__main__":
    repo = AnalyticsRepository(
        mysql_host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        mysql_user=os.getenv("MYSQL_USER", "root"),
        mysql_password=os.getenv("MYSQL_PASSWORD", ""),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
        db_name=os.getenv("MYSQL_DB", "bike_db"),
    )

    try:
        # 1) stats
        stats_days = int(os.getenv("STATS_LOOKBACK_DAYS", "7"))
        repo.refresh_stats(lookback_days=stats_days)
        print(f"stats table refreshed (lookback_days={stats_days}).")

        # 2) train model (all data)
        ml_days = int(os.getenv("ML_LOOKBACK_DAYS", "7"))
        include_fe = (os.getenv("INCLUDE_STATION_FE", "1") == "1")
        model_path = os.getenv("MODEL_PATH", "bike_availability_model.joblib")
        alpha = float(os.getenv("RIDGE_ALPHA", "1.0"))

        bundle = repo.train_and_save_model_all_data(
            lookback_days=ml_days,
            model_path=model_path,
            include_station_fe=include_fe,
            alpha=alpha,
        )
        print(
            f"model trained on ALL data and saved to {model_path}. rows used={bundle['n_rows']}")

        # 3) forecast next 24 hours every 5 minutes and store
        forecast_hours = int(os.getenv("FORECAST_HOURS", "24"))
        forecast_step = int(os.getenv("FORECAST_STEP_MINUTES", "5"))

        written = repo.predict_and_store_bike_forecast(
            model_path=model_path,
            next_hours=forecast_hours,
            step_minutes=forecast_step,
        )
        print(
            f"bike_forecast updated: {written} rows. (next_hours={forecast_hours}, step={forecast_step}min)")
        print("PYTHON DB:", repo._fetchall(
            "SELECT DATABASE() AS db, @@port AS port, @@hostname AS host;")[0])
        print("PYTHON bike_forecast count:", repo._fetchall(
            "SELECT COUNT(*) AS n FROM bike_forecast;")[0]["n"])
    finally:
        repo.close()

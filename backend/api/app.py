import os
import json
import datetime as dt
from typing import Any, Dict, List, Optional

import pymysql
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
app = Flask(__name__)
CORS(app) 

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def get_db_conn():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        port=_env_int("MYSQL_PORT", 3306),
        database=os.getenv("MYSQL_DB", "bike_db"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def to_iso_z(value: Any) -> Any:
    """Serialize datetime to ISO8601 with 'Z' if it's naive (assumed UTC)."""
    if isinstance(value, (dt.datetime,)):
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")
        return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, (dt.date,)):
        return value.isoformat()
    return value


def jsonify_rows(rows: List[Dict[str, Any]]):
    out = []
    for r in rows:
        out.append({k: to_iso_z(v) for k, v in r.items()})
    return jsonify(out)


def create_app() -> Flask:
    app = Flask(__name__, static_folder=None)
    CORS(app)

    @app.get("/api/health")
    def health():
        try:
            conn = get_db_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT UTC_TIMESTAMP() AS now_utc;")
                now_utc = cur.fetchone()["now_utc"]
                cur.execute("SELECT COUNT(*) AS n FROM bike_current;")
                bikes = cur.fetchone()["n"]
                cur.execute("SELECT COUNT(*) AS n FROM weather_current;")
                weather = cur.fetchone()["n"]
            conn.close()
            return jsonify({
                "status": "ok",
                "db": os.getenv("MYSQL_DB", "bike_db"),
                "now_utc": to_iso_z(now_utc),
                "bike_current_rows": bikes,
                "weather_current_rows": weather,
            })
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    @app.get("/api/bikes/current")
    def bikes_current():
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        station_id,
                        name,
                        address,
                        latitude,
                        longitude,
                        available_bikes,
                        available_stands,
                        capacity,
                        status,
                        scraped_at
                    FROM bike_current
                    ORDER BY station_id;
                    """
                )
                rows = cur.fetchall()
            return jsonify_rows(rows)
        finally:
            conn.close()

    @app.get("/api/bikes/forecast")
    def bikes_forecast():
        station_id = request.args.get("station_id", type=int)
        hours = request.args.get("hours", default=24, type=int)
        if not station_id:
            return jsonify({"error": "station_id is required"}), 400
        hours = max(1, min(hours, 168))

        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT station_id, forecast_time, available_bike
                    FROM bike_forecast
                    WHERE station_id=%s
                      AND forecast_time >= UTC_TIMESTAMP()
                      AND forecast_time < (UTC_TIMESTAMP() + INTERVAL %s HOUR)
                    ORDER BY forecast_time;
                    """,
                    (station_id, hours),
                )
                rows = cur.fetchall()
            return jsonify_rows(rows)
        finally:
            conn.close()

    @app.get("/api/weather/current")
    def weather_current():
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        observed_at,
                        timezone_offset_seconds,
                        temperature,
                        feels_like,
                        humidity,
                        pressure,
                        wind_speed,
                        wind_deg,
                        clouds,
                        weather_main,
                        weather_description
                    FROM weather_current
                    ORDER BY observed_at DESC
                    LIMIT 1;
                    """
                )
                row = cur.fetchone()
            if not row:
                return jsonify({"error": "no weather_current data"}), 404
            return jsonify({k: to_iso_z(v) for k, v in row.items()})
        finally:
            conn.close()

    @app.get("/api/weather/forecast")
    def weather_forecast():
        hours = request.args.get("hours", default=72, type=int)
        hours = max(1, min(hours, 168))
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        forecast_time,
                        temperature,
                        feels_like,
                        humidity,
                        pressure,
                        wind_speed,
                        wind_deg,
                        clouds,
                        pop,
                        weather_main,
                        weather_description
                    FROM weather_forecast
                    WHERE forecast_time >= UTC_TIMESTAMP()
                      AND forecast_time < (UTC_TIMESTAMP() + INTERVAL %s HOUR)
                    ORDER BY forecast_time;
                    """,
                    (hours,),
                )
                rows = cur.fetchall()
            return jsonify_rows(rows)
        finally:
            conn.close()

    # Optional: serve frontend from Flask if you want one-command demo.
    FRONTEND_DIR = os.getenv("FRONTEND_DIR")

    @app.get("/")
    def index():
        if not FRONTEND_DIR:
            return jsonify({
                "message": "Frontend not served by Flask. Set FRONTEND_DIR env var to serve ./frontend." 
            })
        return send_from_directory(FRONTEND_DIR, "index.html")

    @app.get("/<path:path>")
    def static_proxy(path: str):
        if not FRONTEND_DIR:
            return jsonify({"error": "FRONTEND_DIR not set"}), 404
        return send_from_directory(FRONTEND_DIR, path)

    return app


if __name__ == "__main__":
    app = create_app()
    port = _env_int("API_PORT", 5000)
    app.run(host="0.0.0.0", port=port, debug=(os.getenv("FLASK_DEBUG", "0") == "1"))

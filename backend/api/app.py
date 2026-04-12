import os
import datetime as dt
from typing import Any, Dict, List
from api.db import get_db_conn
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from api.auth import auth_bp
from api.chat_api import chat_bp


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def to_iso_z(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z")
        return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, dt.date):
        return value.isoformat()
    return value


def jsonify_rows(rows: List[Dict[str, Any]]):
    return jsonify([{k: to_iso_z(v) for k, v in r.items()} for r in rows])


def create_app() -> Flask:
    app = Flask(__name__, static_folder=None)

    # JWT — required for login/register/profile
    app.config["JWT_SECRET_KEY"] = os.getenv("JWT_SECRET_KEY", "cityflow-dev-secret-2025")
    JWTManager(app)

    # CORS — allows frontend to call backend
    CORS(app, resources={r"/api/*": {"origins": "*"}})

    # Blueprints
    app.register_blueprint(auth_bp, url_prefix="/api/auth")
    app.register_blueprint(chat_bp)

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
            return jsonify({"status": "ok", "bike_current_rows": bikes, "weather_current_rows": weather})
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)}), 500

    @app.get("/api/bikes/current")
    def bikes_current():
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT station_id, name, address, latitude, longitude,
                           available_bikes, available_stands, capacity, status, scraped_at
                    FROM bike_current ORDER BY station_id;
                """)
                rows = cur.fetchall()
            return jsonify_rows(rows)
        finally:
            conn.close()

    @app.get("/api/bikes/forecast")
    def bikes_forecast():
        station_id = request.args.get("station_id", type=int)
        hours = max(1, min(request.args.get("hours", default=24, type=int), 168))
        if not station_id:
            return jsonify({"error": "station_id is required"}), 400
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT station_id, forecast_time, available_bike
                    FROM bike_forecast
                    WHERE station_id = %s
                      AND forecast_time >= UTC_TIMESTAMP()
                      AND forecast_time < (UTC_TIMESTAMP() + INTERVAL %s HOUR)
                    ORDER BY forecast_time;
                """, (station_id, hours))
                rows = cur.fetchall()
            return jsonify_rows(rows)
        finally:
            conn.close()

    @app.get("/api/weather/current")
    def weather_current():
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT observed_at, timezone_offset_seconds, temperature, feels_like,
                           humidity, pressure, wind_speed, wind_deg, clouds,
                           weather_main, weather_description
                    FROM weather_current ORDER BY observed_at DESC LIMIT 1;
                """)
                row = cur.fetchone()
            if not row:
                return jsonify({"error": "no weather_current data"}), 404
            return jsonify({k: to_iso_z(v) for k, v in row.items()})
        finally:
            conn.close()

    @app.get("/api/weather/forecast")
    def weather_forecast():
        hours = max(1, min(request.args.get("hours", default=72, type=int), 168))
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT forecast_time, temperature, feels_like, humidity, pressure,
                           wind_speed, wind_deg, clouds, pop, weather_main, weather_description
                    FROM weather_forecast
                    WHERE forecast_time >= UTC_TIMESTAMP()
                      AND forecast_time < (UTC_TIMESTAMP() + INTERVAL %s HOUR)
                    ORDER BY forecast_time;
                """, (hours,))
                rows = cur.fetchall()
            return jsonify_rows(rows)
        finally:
            conn.close()

    FRONTEND_DIR = os.getenv("FRONTEND_DIR")

    @app.get("/")
    def index():
        if not FRONTEND_DIR:
            return jsonify({"message": "Set FRONTEND_DIR to serve frontend."})
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
# backend/api/bike_api.py
from flask import Blueprint, request, jsonify
from api.db import get_db_conn
from datetime import datetime

# Create a blueprint for bike-related web routes
bike_api_bp = Blueprint("bike_api", __name__)


@bike_api_bp.route("/predict", methods=["GET"])
def predict_bikes():
    """
    Handle prediction requests from the frontend.
    Expected URL parameters: ?station_id=1&target_time=2026-04-08T15:00:00Z
    """
    station_id = request.args.get("station_id")
    target_time_str = request.args.get("target_time")

    # Data validation
    if not station_id or not target_time_str:
        return jsonify({"error": "Missing station_id or target_time"}), 400

    try:
        # Convert ISO format time string from frontend to Python datetime object
        target_time = datetime.fromisoformat(
            target_time_str.replace("Z", "+00:00"))

        conn = get_db_conn()
        with conn.cursor() as cursor:
            # Query using the exact column name 'available_bike'
            cursor.execute("""
                SELECT available_bike 
                FROM bike_forecast 
                WHERE station_id = %s AND forecast_time >= %s
                ORDER BY forecast_time ASC 
                LIMIT 1
            """, (station_id, target_time))
            result = cursor.fetchone()

        conn.close()

        if result:
            # Support both dictionary and tuple cursor return formats
            predicted_bikes = result['available_bike'] if isinstance(
                result, dict) else result[0]
            return jsonify({
                "station_id": int(station_id),
                "target_time": target_time_str,
                "predicted_bikes": int(predicted_bikes)
            }), 200
        else:
            return jsonify({"error": "No forecast data available for this time window"}), 404

    except ValueError:
        return jsonify({"error": "Invalid date format. Use ISO format (e.g., 2026-04-08T15:00:00Z)"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bike_api_bp.route("/forecast", methods=["GET"])
def get_forecast():
    """
    Handle 24-hour forecast requests for the frontend Chart.js modal.
    Expected URL: ?station_id=1&hours=24
    """
    station_id = request.args.get("station_id")
    hours = int(request.args.get("hours", 24))

    if not station_id:
        return jsonify({"error": "Missing station_id parameter"}), 400

    try:
        conn = get_db_conn()
        with conn.cursor() as cursor:
            # Query future data up to 'hours' limit
            cursor.execute("""
                SELECT forecast_time, available_bike 
                FROM bike_forecast 
                WHERE station_id = %s 
                  AND forecast_time >= NOW() 
                  AND forecast_time <= DATE_ADD(NOW(), INTERVAL %s HOUR)
                ORDER BY forecast_time ASC
            """, (station_id, hours))
            results = cursor.fetchall()

        conn.close()

        # Format output to match frontend expectations
        formatted = []
        for row in results:
            if isinstance(row, dict):
                ft = row["forecast_time"]
                formatted.append({
                    "forecast_time": ft.isoformat() + "Z" if isinstance(ft, datetime) else ft,
                    "available_bike": row["available_bike"]
                })
            else:
                ft = row[0]
                formatted.append({
                    "forecast_time": ft.isoformat() + "Z" if isinstance(ft, datetime) else ft,
                    "available_bike": row[1]
                })

        return jsonify(formatted), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

from pathlib import Path
import os
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

print("Loaded .env from:", ENV_PATH)
print("OPENWEATHER_API_KEY:", os.getenv("OPENWEATHER_API_KEY"))

from api.weather import WeatherRepository

def main():
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
        forecast_horizon_hours=int(os.getenv("WEATHER_FORECAST_HORIZON_HOURS", "72")),
        create_partitions_ahead_days=int(os.getenv("WEATHER_PARTITIONS_AHEAD_DAYS", "2")),
        request_timeout=int(os.getenv("REQUEST_TIMEOUT", "15")),
    )
    # run once (use cron/Task Scheduler to run every hour)
    repo.run_once()


if __name__ == "__main__":
    main()
